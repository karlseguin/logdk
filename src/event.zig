const std = @import("std");
const zul = @import("zul");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

const json = std.json;
const Allocator = std.mem.Allocator;
const ParseOptions = json.ParseOptions;

const MAX_FLATTEN_DEPTH = 1;

pub const Event = struct {
	map: std.StringHashMapUnmanaged(Value),

	pub const List = struct {
		created: i64,
		events: []Event,
		arena: *std.heap.ArenaAllocator,

		pub fn deinit(self: *const List) void {
			const arena = self.arena;
			const allocator = arena.child_allocator;
			arena.deinit();
			allocator.destroy(arena);
		}
	};

	pub fn get(self: *const Event, field: []const u8) ?Value {
		return self.map.get(field);
	}

	pub fn fieldCount(self: *const Event) usize {
		return @intCast(self.map.count());
	}

	pub const DataType = enum {
		tinyint,
		smallint,
		integer,
		bigint,
		utinyint,
		usmallint,
		uinteger,
		ubigint,
		double,
		bool,
		null,
		string,
		json,
		list,
		date,
		time,
		timestamp,
		uuid,
	};

	pub const Value = union(DataType) {
		tinyint: i8,
		smallint: i16,
		integer: i32,
		bigint: i64,
		utinyint: u8,
		usmallint: u16,
		uinteger: u32,
		ubigint: u64,
		double: f64,
		bool: bool,
		null: void,
		string: []const u8,
		json: []const u8,
		list: Value.List,
		// might be nice to have more abstract types here, like a zul.Date, zul.Time and zul.DateTime
		// but in the scale of things, it's the same and having the duckdb type directly
		// here makes inserting into the dataset easier (which is nice, because that code
		// is already complicated)
		date: zuckdb.Date,
		time: zuckdb.Time,
		timestamp: i64,

		// we store this direclty as an i128 (which is what we insert into the appender)
		// because we parse this from a string using `zuckdb.encodeUUID` and that's
		// what it gives us. We could store the []const u8, but then we'd end up
		// parsing it twice: once to see if it IS a uuid and then once to store it.
		uuid: i128,

		pub const List = struct {
			json: []const u8,
			values: []Value,
		};

		pub fn tryParse(self: *Value) ?Value {
			switch (self.*) {
				.string => |s| return tryParseValue(s),
				.list => |l| {
					// Wish we didn't have to do this, but without it, it would be impossible
					// to insert a date[], time[] or timestamptz[].
					for (l.values, 0..) |v, i| {
						switch (v) {
							.string => |s| if (tryParseValue(s)) |new| {
								l.values[i] = new;
							},
							else => {},
						}
					}
					// This is a bit hackish. Even if we parsed members of the list,
					// we still return null, because we internally mutated the list.
					return null;
				},
				else => return null,
			}
		}
	};

	pub fn parse(allocator: Allocator, input: []const u8) !Event.List {
		const created = std.time.microTimestamp();
		const arena = try allocator.create(std.heap.ArenaAllocator);
		errdefer allocator.destroy(arena);

		arena.* = std.heap.ArenaAllocator.init(allocator);
		errdefer arena.deinit();

		const aa = arena.allocator();

		const owned = try aa.dupe(u8, input);
		var scanner = json.Scanner.initCompleteInput(aa, owned);
		defer scanner.deinit();

		const events = switch (try scanner.next()) {
			.array_begin => try Parser.bulk(aa, &scanner),
			.object_begin => try Parser.singleAsList(aa, &scanner),
			else => return error.UnexpectedToken,
		};

		return .{
			.created = created,
			.arena = arena,
			.events = events,
		};
	}
};

const Parser = struct {
	depth: usize = 0,
	allocator: Allocator,
	stack: [MAX_FLATTEN_DEPTH][]const u8 = undefined,
	map: std.StringHashMapUnmanaged(Event.Value),

	const Error = json.ParseError(json.Scanner);

	fn bulk(aa: Allocator, scanner: *json.Scanner) ![]Event {
		var count: usize = 0;
		var events = try aa.alloc(Event, 10);

		while (true) {
			switch (try scanner.next()) {
				.object_begin => {},
				.array_end => break,
				else => return error.UnexpectedToken,
			}

			if (try single(aa, scanner)) |event| {
				if (count == events.len) {
					events = try aa.realloc(events, events.len + 10);
				}
				events[count] = event;
				count += 1;
			}
		}

		switch (try scanner.next()) {
			.end_of_document => return events[0..count],
			else => return error.UnexpectedToken,
		}
	}

	fn singleAsList(aa: Allocator, scanner: *json.Scanner) ![]Event {
		const event = try single(aa, scanner) orelse return &[_]Event{};
		var events = try aa.alloc(Event, 1);
		events[0] = event;
		return events;
	}

	fn single(aa: Allocator, scanner: *json.Scanner) !?Event {
		var parser = Parser{
			.allocator = aa,
			.map = std.StringHashMapUnmanaged(Event.Value){},
		};
		try parser.parseObject(aa, scanner);
		return if (parser.map.count() == 0) null else .{.map = parser.map};
	}

	fn add(self: *Parser, value: Event.Value) !void {
		const depth = self.depth;
		const field_name = if (depth == 0) self.stack[0] else try std.mem.join(self.allocator, ".", self.stack[0..depth+1]);
		try self.map.put(self.allocator, field_name, value);
	}

	fn parseObject(self: *Parser, allocator: Allocator, scanner: *json.Scanner) Error!void {
		while (true) {
			switch (try scanner.nextAlloc(allocator, .alloc_if_needed)) {
				.string, .allocated_string => |field| {
					const depth = self.depth;
					self.stack[depth] = field;
					const break_on_object = depth < MAX_FLATTEN_DEPTH - 1;
					const token = try scanner.nextAlloc(allocator, .alloc_if_needed);
					switch (try parseValue(allocator, scanner, break_on_object, false, token)) {
						.value => |v| try self.add(v),
						.nested_object => {
							// if break_on_object is true, then pareValue won't parse an object and will
							// simply return nested_object.
							self.depth = depth + 1;
							try self.parseObject(allocator, scanner);
						},
						.nested_list => unreachable, // since break_on_list is false, this cannot happen, as a list will always be returned as a value
					}
				},
				.object_end => {
					const depth = self.depth;
					if (depth != 0) self.depth = depth - 1;
					break;
				},
				.end_of_document => return,
				else => unreachable,
			}
		}
	}

	const ParseValueResult = union(enum) {
		value: Event.Value,
		nested_list: void,
		nested_object: void,
	};

	fn parseValue(allocator: Allocator, scanner: *json.Scanner, break_on_object: bool,  break_on_list: bool, token: json.Token) Error!ParseValueResult {
		switch (token) {
			.string, .allocated_string => |value| return .{.value = .{.string = value}},
			.null => return .{.value = .{.null = {}}},
			.true => return .{.value = .{.bool = true}},
			.false => return .{.value = .{.bool = false}},
			.number, .allocated_number => |str| {
				const result = try parseInteger(str);
				return .{.value = try result.toNumericValue(str)};
			},
			.array_begin => {
				if (break_on_list) {
					// Our caller doesn't want us to parse a list. So we return, telling
					// our caller that we have a list
					return .{.nested_list = {}};
				}

				// This is messy, but we don't know if this should be treated as a list
				// or as a json. We treat it as json if it has a nested list.

				// in case we need to treat this as json, we'll grab the position and
				// stack height of the parser
				// -1 because we alreayd consumed the '['
				const array_start = scanner.cursor - 1;

				// -1 becuase we're already increased the scanner's stack height from the array_begin
				const stack_height = scanner.stackHeight() - 1;

				var arr = std.ArrayList(Event.Value).init(allocator);
				while (true) {
					const sub_token = try scanner.nextAlloc(allocator, .alloc_if_needed);
					switch (sub_token) {
						.array_end => break,
						else => switch (try parseValue(allocator, scanner, false, true, sub_token)) {
							.value => |v| try arr.append(v),
							.nested_object => unreachable,  // since break_on_object is false, a nested object will always be returned as a value
							.nested_list => {
								if (break_on_list) {
									// If this is true, then we aren't the root list, so we just propagate this up
									return .{.nested_list = {}};
								}

								try scanner.skipUntilStackHeight(stack_height);
								return .{.value = .{.json = scanner.input[array_start..scanner.cursor]}};
							}
						}
					}
				}

				return .{.value = .{.list = .{.json = scanner.input[array_start..scanner.cursor], .values = arr.items}}};
			},
			.object_begin => {
				if (break_on_object) {
					// Our caller doesn't want us to parse an object. So we return, telling
					// our caller that we have a object
					return .{.nested_object = {}};
				}

				// The caller doesn't want this nested object to be flattened. Instead,
				// the entire object is going to be taken as-is (a string literal) and
				// treated as a json blob.

				// -1 because we already consumed the '{'
				const object_start = scanner.cursor - 1;

				// -1 because we've already increased the scanner's stack height from this object_begin
				try scanner.skipUntilStackHeight(scanner.stackHeight() - 1);
				return .{.value = .{.json = scanner.input[object_start..scanner.cursor]}};
			},
			else => unreachable,
		}
	}
};

// parseInteger is used both when parsing the JSON input as well as when trying to
// parse a string value. When parsing the JSON input, we _know_ this has to be a number
// but when parsing a string value, we're only trying to see if it works.
// We don't use std.fmt.parseInt because we don't know the type of the integer.
// If we use `u64`, we won't be able to represent negatives. If we use `i64` we
// won't be able to fully represent `u64.
const JsonIntegerResult = struct {
	value: u64,
	negative: bool,
	rest: []const u8,

	// Try to turn the JsonIntegerResult into a Event.Value.
	fn toNumericValue(self: JsonIntegerResult, input: []const u8) !Event.Value {
		if (self.rest.len > 0) {
			// We had more data than just our integer.
			// A bit of a waste to throw away self.value, but parsing floats is hard
			// and I rather just use std at this point.
			return .{.double = try std.fmt.parseFloat(f64, input)};
		}

		const value = self.value;
		if (self.negative) {
			if (value == 0) return .{.double = -0.0};
			if (value <= 128) return .{.tinyint = @intCast(-@as(i64, @intCast(value)))};
			if (value <= 32768) return .{.smallint = @intCast(-@as(i64, @intCast(value)))};
			if (value <= 2147483648) return .{.integer = @intCast(-@as(i64, @intCast(value)))};
			if (value <= 9223372036854775807) return .{.bigint = -@as(i64, @intCast(value))};
			// as far as I can tell, this is the only way to cast a 9223372036854775808 u64 into an i64 -9223372036854775808
			if (value == 9223372036854775808) return .{.bigint = @intCast(-@as(i128, @intCast(value)))};
			return error.InvalidNumber;
		}
		if (value <= 255) return .{.utinyint = @intCast(value)};
		if (value <= 65535) return .{.usmallint = @intCast(value)};
		if (value <= 4294967295) return .{.uinteger = @intCast(value)};
		if (value <= 18446744073709551615) return .{.ubigint = @intCast(value)};
		return error.InvalidNumber;
	}
};

fn parseInteger(str: []const u8) error{InvalidNumber}!JsonIntegerResult {
	std.debug.assert(str.len != 0);

	var pos: usize = 0;
	var negative = false;
	if (str[0] == '-') {
		pos = 1;
		negative = true;
	}

	var n: u64 = 0;
	for (str[pos..]) |b| {
		if (b < '0' or b > '9') {
			break;
		}

		pos += 1;
		{
			n, const overflowed = @mulWithOverflow(n, 10);
			if (overflowed != 0) {
				return error.InvalidNumber;
			}
		}
		{
			n, const overflowed = @addWithOverflow(n, @as(u64, @intCast(b - '0')));
			if (overflowed != 0) {
				return error.InvalidNumber;
			}
		}
	}

	return .{
		.value = n,
		.negative = negative,
		.rest = str[pos..],
	};
}

fn tryParseValue(s: []const u8) ?Event.Value {
	if (s.len == 0) return null;

	if (s.len >= 20) {
		// this does its own length check, but we put it in the >= 20 since it'll
		// avoid the function call for common short strings
		if (zuckdb.encodeUUID(s)) |v| {
			return .{.uuid = v};
		} else |_| {}

		if (zul.DateTime.parse(s, .rfc3339)) |v| {
			return .{.timestamp = v.unix(.microseconds)};
		} else |_| {}
	}

	if (s.len == 10) {
		if (zul.Date.parse(s, .rfc3339)) |v| {
			return .{.date = .{.year = v.year, .month = @intCast(v.month), .day = @intCast(v.day)}};
		} else |_| {}
	}

	if (s.len >= 5) {
		if (zul.Time.parse(s, .rfc3339)) |v| {
			return .{.time = .{.hour = @intCast(v.hour), .min = @intCast(v.min), .sec = @intCast(v.sec), .micros = @intCast(v.micros)}};
		} else |_| {}
	}

	if (parseInteger(s)) |result| {
		// parseInteger can return a result that is NOT an integer/float.
		// given "1234abc", it'll parse "1234" and set .rest to "abc". It's only
		// the combination of parseInteger and toNumericValue that definitively
		// detect an integer or float.
		if (result.toNumericValue(s)) |value| {
			return value;
		} else |_| {}
	} else |_| {}

	if (std.ascii.eqlIgnoreCase(s, "true")) {
		return .{.bool = true};
	}

	if (std.ascii.eqlIgnoreCase(s, "false")) {
		return .{.bool = false};
	}

	return null;
}

const t = logdk.testing;
test "Event: parse non-array or non-object" {
	try t.expectError(error.UnexpectedToken, Event.parse(t.allocator, "123"));
}

test "Event: parse empty" {
	const event_list = try Event.parse(t.allocator, "{}");
	defer event_list.deinit();
	try t.expectEqual(0, event_list.events.len);
}

test "Event: parse empty list" {
	const event_list = try Event.parse(t.allocator, "[]");
	defer event_list.deinit();
	try t.expectEqual(0, event_list.events.len);
}

test "Event: parse list of empty events" {
	const event_list = try Event.parse(t.allocator, "[{}, {}, {}]");
	defer event_list.deinit();
	try t.expectEqual(0, event_list.events.len);
}

test "Event: parse simple" {
	const event_list = try Event.parse(t.allocator, \\{
		\\ "key_1": true, "another_key": false,
		\\ "key_3": null,
		\\ "key_4": "over 9000!!",
		\\ "a": 0, "b": 1, "c": 6999384751, "d": -1, "e": -867211,
		\\ "f1": 0.0, "f2": -0, "f3": 99.33929191, "f4": -1.49E10
	\\}
	);
	defer event_list.deinit();

	try assertEvent(.{
		.key_1 = Event.Value{.bool = true}, .another_key = Event.Value{.bool = false},
		.key_3 = Event.Value{.null = {}}, .key_4 = Event.Value{.string = "over 9000!!"},
		.a = Event.Value{.utinyint = 0}, .b = Event.Value{.utinyint = 1}, .c = Event.Value{.ubigint = 6999384751}, .d = Event.Value{.tinyint = -1}, .e = Event.Value{.integer = -867211}, // ints
		.f1 = Event.Value{.double = 0.0}, .f2 = Event.Value{.double = -0.0}, .f3 = Event.Value{.double = 99.33929191}, .f4 = Event.Value{.double = -1.49E10}
	}, event_list.events[0]);
}

test "Event: parse array" {
	const event_list = try Event.parse(t.allocator, "[{\"id\":1},{\"id\":2},{\"id\":3}]");
	defer event_list.deinit();

	try t.expectEqual(3, event_list.events.len);
	try assertEvent(.{.id = Event.Value{.utinyint = 1}}, event_list.events[0]);
	try assertEvent(.{.id = Event.Value{.utinyint = 2}}, event_list.events[1]);
	try assertEvent(.{.id = Event.Value{.utinyint = 3}}, event_list.events[2]);
}

test "Event: parse array past initial size" {
	const event_list = try Event.parse(t.allocator,
		\\ [
		\\   {"id":1}, {"id":2}, {"id":3}, {"id":4}, {"id":5}, {"id":6}, {"id":7}, {"id":8}, {"id":9}, {"id":10},
		\\   {"id":11}, {"id":12}, {"id":13}, {"id":14}, {"id":15}, {"id":16}, {"id":17}, {"id":18}, {"id":19}, {"id":20},
		\\   {"id":21}, {"id":22}, {"id":23}, {"id":24}, {"id":25}, {"id":26}, {"id":27}, {"id":28}, {"id":29}, {"id":30},
		\\   {"id":31}, {"id":32}, {"id":33}, {"id":34}
		\\ ]
	);
	defer event_list.deinit();

	try t.expectEqual(34, event_list.events.len);
	for (0..34) |i| {
		try assertEvent(.{.id = Event.Value{.utinyint = @intCast(i + 1)}}, event_list.events[i]);
	}
}

// test "Event: parse nesting flatten" {
// 	if (MAX_FLATTEN_DEPTH != 3) return error.SkipZigTest;

// 	const event = try Event.parse(t.allocator, \\{
// 		\\ "key_1": {},
// 		\\ "key_2": {
// 		\\   "sub_1": true,
// 		\\   "sub_2": "hello",
// 		\\   "sub_3": {
// 		\\      "other": 12345,
// 		\\      "too_deep":  {  "handle ":  1, "x": {"even": "more", "ok": true}}
// 		\\   }
// 		\\ }
// 	\\}
// 	);
// 	defer event.deinit();

// 	try assertEvent(.{
// 		.@"key_2.sub_1" = Event.Value{.bool = true},
// 		.@"key_2.sub_2" = Event.Value{.string = "hello"},
// 		.@"key_2.sub_3.other" = Event.Value{.usmallint = 12345},
// 		.@"key_2.sub_3.too_deep" = Event.Value{.json =  "{  \"handle \":  1, \"x\": {\"even\": \"more\", \"ok\": true}}"},
// 	}, event);
// }

test "Event: parse nesting" {
	if (MAX_FLATTEN_DEPTH != 1) return error.SkipZigTest;
	const event_list = try Event.parse(t.allocator, \\{
		\\ "key_1": {},
		\\ "key_2": {
		\\   "sub_1": true,
		\\   "sub_2": {  "handle ":  1, "x": {"even": "more", "ok": true}}
		\\}
	\\}
	);
	defer event_list.deinit();

	try assertEvent(.{
		.@"key_1" = Event.Value{.json =  "{}"},
		.@"key_2" = Event.Value{.json =  "{\n   \"sub_1\": true,\n   \"sub_2\": {  \"handle \":  1, \"x\": {\"even\": \"more\", \"ok\": true}}\n}"},
	}, event_list.events[0]);
}

test "Event: parse list" {
	const event_list = try Event.parse(t.allocator, "{\"a\": [1, -9000], \"b\": [true, 56.78912, null, {\"abc\": \"123\"}]}");
	defer event_list.deinit();
	try assertEvent(.{
		.a = Event.Value{.list = .{
			.json = "[1, -9000]",
			.values = @constCast(&[_]Event.Value{.{.utinyint = 1}, .{.smallint = -9000}})
		}},
		.b = Event.Value{.list = .{
			.json = "[true, 56.78912, null, {\"abc\": \"123\"}]",
			.values = @constCast(&[_]Event.Value{.{.bool = true}, .{.double = 56.78912}, .{.null = {}}, .{.json = "{\"abc\": \"123\"}"}})
		}},
	}, event_list.events[0]);
}

test "Event: parse nested list" {
	const event_list = try Event.parse(t.allocator, "{\"a\": [1, [true, null, \"hi\"]]}");
	defer event_list.deinit();
	try assertEvent(.{.a = Event.Value{.json = "[1, [true, null, \"hi\"]]"}}, event_list.events[0]);
}

test "Event: parse list simple" {
	const event_list = try Event.parse(t.allocator, "{\"a\": [9999, -128]}");
	defer event_list.deinit();
	try assertEvent(.{.a = Event.Value{.list = .{
		.json = "[9999, -128]",
		.values = @constCast(&[_]Event.Value{.{.usmallint = 9999}, .{.tinyint = -128}})
	}}}, event_list.events[0]);
}

test "Event: parse positive integer" {
	const event_list = try Event.parse(t.allocator, "{\"pos\": [0, 1, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615]}");
	defer event_list.deinit();
	try assertEvent(.{
		.pos = Event.Value{.list = .{
			.json = "[0, 1, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615]",
			.values = @constCast(&[_]Event.Value{
				.{.utinyint = 0}, .{.utinyint = 1}, .{.utinyint = 255},
				.{.usmallint = 256}, .{.usmallint = 65535},
				.{.uinteger = 65536}, .{.uinteger = 4294967295},
				.{.ubigint = 4294967296}, .{.ubigint = 18446744073709551615}
			})
		}}
	}, event_list.events[0]);
}

test "Event: parse negative integer" {
	const event_list = try Event.parse(t.allocator, "{\"neg\": [-0, -1, -128, -129, -32768 , -32769, -2147483648, -2147483649, -9223372036854775807, -9223372036854775808]}");
	defer event_list.deinit();
	try assertEvent(.{
		.neg = Event.Value{.list = .{
			.json = "[-0, -1, -128, -129, -32768 , -32769, -2147483648, -2147483649, -9223372036854775807, -9223372036854775808]",
			.values = @constCast(&[_]Event.Value{
				.{.double = -0.0}, .{.tinyint = -1}, .{.tinyint = -128},
				.{.smallint = -129}, .{.smallint = -32768},
				.{.integer = -32769}, .{.integer = -2147483648},
				.{.bigint = -2147483649}, .{.bigint = -9223372036854775807}, .{.bigint = -9223372036854775808}
			})
		}}
	}, event_list.events[0]);
}

test "Event: parse integer overflow" {
	try t.expectError(error.InvalidNumber, Event.parse(t.allocator, "{\"overflow\": 18446744073709551616}"));
}

test "Event: tryParse string" {
	// just sneak this in here
	var non_string = Event.Value{.bool = true};
	try t.expectEqual(null, non_string.tryParse());

	try t.expectEqual(null, testTryParseString(""));
	try t.expectEqual(null, testTryParseString("over 9000!"));
	try t.expectEqual(null, testTryParseString("9000!"));
	try t.expectEqual(null, testTryParseString("t"));
	try t.expectEqual(null, testTryParseString("falsey"));
	try t.expectEqual(null, testTryParseString("2005-1-1"));
	try t.expectEqual(null, testTryParseString("2025-13-01"));
	try t.expectEqual(null, testTryParseString("2025-11-31"));
	try t.expectEqual(null, testTryParseString("2025-11-31T00:00:00Z"));
	try t.expectEqual(null, testTryParseString("2025-12-31T00:00:00+12:30"));

	try t.expectEqual(true, testTryParseString("true").?.bool);
	try t.expectEqual(true, testTryParseString("TRUE").?.bool);
	try t.expectEqual(true, testTryParseString("True").?.bool);
	try t.expectEqual(true, testTryParseString("TrUe").?.bool);

	try t.expectEqual(false, testTryParseString("false").?.bool);
	try t.expectEqual(false, testTryParseString("FALSE").?.bool);
	try t.expectEqual(false, testTryParseString("False").?.bool);
	try t.expectEqual(false, testTryParseString("FaLsE").?.bool);

	try t.expectEqual(0, testTryParseString("0").?.utinyint);
	try t.expectEqual(-128, testTryParseString("-128").?.tinyint);
	try t.expectEqual(10000, testTryParseString("10000").?.usmallint);
	try t.expectEqual(-1234, testTryParseString("-1234").?.smallint);
	try t.expectEqual(394918485, testTryParseString("394918485").?.uinteger);
	try t.expectEqual(-999999912, testTryParseString("-999999912").?.integer);
	try t.expectEqual(7891235891098352, testTryParseString("7891235891098352").?.ubigint);
	try t.expectEqual(-111123456698832, testTryParseString("-111123456698832").?.bigint);
	try t.expectEqual(-323993.3231332, testTryParseString("-323993.3231332").?.double);

	try t.expectEqual(.{.year = 2025, .month = 1, .day = 2}, testTryParseString("2025-01-02").?.date);
	try t.expectEqual(.{.hour = 10, .min = 22, .sec = 0, .micros = 0}, testTryParseString("10:22").?.time);
	try t.expectEqual(.{.hour = 15, .min = 3, .sec = 59, .micros = 123456}, testTryParseString("15:03:59.123456").?.time);
	try t.expectEqual(1737385439123456, testTryParseString("2025-01-20T15:03:59.123456Z").?.timestamp);

	try t.expectEqual(-119391408245701198339858421598325797365, testTryParseString("262e0d19-d9d8-4892-8fe1-e421fe188e0b").?.uuid);
	try t.expectEqual(-119391408245701198339858421598325797365, testTryParseString("262E0D19-D9D8-4892-8FE1-E421FE188E0B").?.uuid);
}


test "Event: tryParse list" {
	{
		var l = Event.Value{.list = .{
			.json = "",
			.values = @constCast(&[_]Event.Value{.{.string = "200"}, .{.bool = true}, .{.string = "9000!"}})
		}};
		try t.expectEqual(null, l.tryParse());
		try t.expectEqual(200, l.list.values[0].utinyint);
		try t.expectEqual(true, l.list.values[1].bool);
		try t.expectEqual("9000!", l.list.values[2].string);
	}
}

fn testTryParseString(str: []const u8) ?Event.Value {
	var v = Event.Value{.string = str};
	return v.tryParse();
}

fn assertEvent(expected: anytype, actual: Event) !void {
	const fields = @typeInfo(@TypeOf(expected)).Struct.fields;
	try t.expectEqual(fields.len, actual.fieldCount());

	inline for (fields) |f| {
		const field_name = f.name;
		switch (@field(expected, field_name)) {
			.list => |expected_list| {
				const actual_list = actual.map.get(field_name).?.list;
				for (expected_list.values, actual_list.values) |expect_list_value, actual_list_value| {
					try t.expectEqual(expect_list_value, actual_list_value);
				}
				try t.expectEqual(expected_list.json, actual_list.json);
			},
			else => |expected_value| try t.expectEqual(expected_value, actual.map.get(field_name).?),
		}
	}
}
