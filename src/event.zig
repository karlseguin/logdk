const std = @import("std");
const logdk = @import("logdk.zig");

const json = std.json;
const Allocator = std.mem.Allocator;
const ParseOptions = json.ParseOptions;

const MAX_FLATTEN_DEPTH = 3;

pub const Event = struct {
	values: []const Value,
	fields: []const []const u8,
	_arena: *std.heap.ArenaAllocator,

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
		text,
		json,
		list,
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
		text: []const u8,
		json: []const u8,
		list: []const Value,
	};

	pub fn deinit(self: *const Event) void {
		const arena = self._arena;
		const allocator = arena.child_allocator;
		arena.deinit();
		allocator.destroy(arena);
	}

	pub fn parse(allocator: Allocator, input: []const u8) !*Event {
		const arena = try allocator.create(std.heap.ArenaAllocator);
		errdefer allocator.destroy(arena);

		arena.* = std.heap.ArenaAllocator.init(allocator);
		errdefer arena.deinit();

		const aa = arena.allocator();
		var scanner = json.Scanner.initCompleteInput(allocator, input);
		defer scanner.deinit();

		if (try scanner.next() != .object_begin) {
			return error.UnexpectedToken;
		}

		var parser = Parser{
			.allocator = aa,
			.values = std.ArrayList(Value).init(aa),
			.fields = std.ArrayList([]const u8).init(aa),
		};

		try parser.parseObject(aa, &scanner);
		const event = try aa.create(Event);
		event.* = .{
			._arena = arena,
			.values = parser.values.items,
			.fields = parser.fields.items,
		};

		return event;
	}
};

const Parser = struct {
	depth: usize = 0,
	allocator: Allocator,
	stack: [MAX_FLATTEN_DEPTH][]const u8 = undefined,
	fields: std.ArrayList([]const u8),
	values: std.ArrayList(Event.Value),

	const Error = json.ParseError(json.Scanner);

	fn add(self: *Parser, value: Event.Value) !void {
		try self.values.append(value);
		const depth = self.depth;
		if (depth == 0) {
			try self.fields.append(self.stack[0]);
		} else {
			try self.fields.append(try std.mem.join(self.allocator, ".", self.stack[0..depth+1]));
		}
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
			.string, .allocated_string => |value| return .{.value = .{.text = value}},
			.null => return .{.value = .{.null = {}}},
			.true => return .{.value = .{.bool = true}},
			.false => return .{.value = .{.bool = false}},
			.number, .allocated_number => |str| {
				const result = try parseInteger(str);
				if (result.rest.len == 0) {
					const value = result.value;
					if (result.negative) {
						if (value == 0) return .{.value = .{.double = -0.0}};
						if (value <= 128) return .{.value = .{.tinyint = @intCast(-@as(i64, @intCast(value)))}};
						if (value <= 32768) return .{.value = .{.smallint = @intCast(-@as(i64, @intCast(value)))}};
						if (value <= 2147483648) return .{.value = .{.integer = @intCast(-@as(i64, @intCast(value)))}};
						if (value <= 9223372036854775807) return .{.value = .{.bigint = -@as(i64, @intCast(value))}};
						// as far as I can tell, this is the only way to cast a 9223372036854775808 u64 into an i64 -9223372036854775808
						if (value == 9223372036854775808) return .{.value = .{.bigint = @intCast(-@as(i128, @intCast(value)))}};
						return error.InvalidNumber;
					}
					if (value <= 255) return .{.value = .{.utinyint = @intCast(value)}};
					if (value <= 65535) return .{.value = .{.usmallint = @intCast(value)}};
					if (value <= 4294967295) return .{.value = .{.uinteger = @intCast(value)}};
					if (value <= 18446744073709551615) return .{.value = .{.ubigint = @intCast(value)}};
					return error.InvalidNumber;
				} else {
					return .{.value = .{.double = std.fmt.parseFloat(f64, str) catch unreachable}};
				}
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
				return .{.value = .{.list = arr.items}};
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

	const JsonIntegerResult = struct {
		value: u64,
		negative: bool,
		rest: []const u8,
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
};

const t = logdk.testing;
test "Event: parse empty array" {
	try t.expectError(error.UnexpectedToken, Event.parse(t.allocator, "[]"));
}

test "Event: parse empty" {
	const event = try Event.parse(t.allocator, "{}");
	defer event.deinit();
	try t.expectEqual(0, event.fields.len);
}

test "Event: parse simple" {
	const event = try Event.parse(t.allocator, \\{
		\\ "key_1": true, "another_key": false,
		\\ "key_3": null,
		\\ "key_4": "over 9000!!",
		\\ "a": 0, "b": 1, "c": 6999384751, "d": -1, "e": -867211,
		\\ "f1": 0.0, "f2": -0, "f3": 99.33929191, "f4": -1.49E10
	\\}
	);
	defer event.deinit();

	try assertEvent(.{
		.fields = &[_][]const u8{
			"key_1", "another_key", "key_3", "key_4",
			"a", "b", "c", "d", "e",   // ints
			"f1", "f2", "f3", "f4"
		},
		.values = &[_]Event.Value{
			.{.bool = true},
			.{.bool = false},
			.{.null = {}},
			.{.text = "over 9000!!"},
			.{.utinyint = 0},
			.{.utinyint = 1},
			.{.ubigint = 6999384751},
			.{.tinyint = -1},
			.{.integer = -867211},
			.{.double = 0.0},
			.{.double = -0.0},
			.{.double = 99.33929191},
			.{.double = -1.49E10},
		},
	}, event);
}

test "Event: parse nesting" {
	const event = try Event.parse(t.allocator, \\{
		\\ "key_1": {},
		\\ "key_2": {
		\\   "sub_1": true,
		\\   "sub_2": "hello",
		\\   "sub_3": {
		\\      "other": 12345,
		\\      "too_deep":  {  "handle ":  1, "x": {"even": "more", "ok": true}}
		\\   }
		\\ }
	\\}
	);
	defer event.deinit();

	try assertEvent(.{
		.fields = &[_][]const u8{
			"key_2.sub_1",
			"key_2.sub_2",
			"key_2.sub_3.other",
			"key_2.sub_3.too_deep"
		},
		.values = &[_]Event.Value{
			.{.bool = true},
			.{.text = "hello"},
			.{.usmallint = 12345},
			.{.json =  "{  \"handle \":  1, \"x\": {\"even\": \"more\", \"ok\": true}}"},
		},
	}, event);
}

test "Event: parse list" {
	const event = try Event.parse(t.allocator, "{\"a\": [1, -9000], \"b\": {\"values\": [true, 56.78912, null, {\"abc\": \"123\"}]}}");
	defer event.deinit();
	try assertEvent(.{
		.fields = &[_][]const u8{"a", "b.values"},
		.values = &[_]Event.Value{
			.{.list = &[_]Event.Value{.{.utinyint = 1}, .{.smallint = -9000}}},
			.{.list = &[_]Event.Value{.{.bool = true}, .{.double = 56.78912}, .{.null = {}}, .{.json = "{\"abc\": \"123\"}"}}}
		},
	}, event);
}

test "Event: parse nested list" {
	const event = try Event.parse(t.allocator, "{\"a\": [1, [true, null, \"hi\"]]}");
	defer event.deinit();
	try assertEvent(.{
		.fields = &[_][]const u8{"a"},
		.values = &[_]Event.Value{.{.json = "[1, [true, null, \"hi\"]]"}},
	}, event);
}

test "Event: parse list simple" {
	const event = try Event.parse(t.allocator, "{\"a\": [9999, -128]}");
	defer event.deinit();
	try assertEvent(.{
		.fields = &[_][]const u8{"a"},
		.values = &[_]Event.Value{
			.{.list = &[_]Event.Value{.{.usmallint = 9999}, .{.tinyint = -128}}},
		},
	}, event);
}

test "Event: parse positive integer" {
	const event = try Event.parse(t.allocator, "{\"pos\": [0, 1, 255, 256, 65535, 65536, 4294967295, 4294967296, 18446744073709551615]}");
	defer event.deinit();
	try assertEvent(.{
		.fields = &[_][]const u8{"pos"},
		.values = &[_]Event.Value{
			.{.list = &[_]Event.Value{
				.{.utinyint = 0}, .{.utinyint = 1}, .{.utinyint = 255},
				.{.usmallint = 256}, .{.usmallint = 65535},
				.{.uinteger = 65536}, .{.uinteger = 4294967295},
				.{.ubigint = 4294967296}, .{.ubigint = 18446744073709551615}
			}},
		},
	}, event);
}

test "Event: parse negative integer" {
	const event = try Event.parse(t.allocator, "{\"neg\": [-0, -1, -128, -129, -32768 , -32769, -2147483648, -2147483649, -9223372036854775807, -9223372036854775808]}");
	defer event.deinit();
	try assertEvent(.{
		.fields = &[_][]const u8{"neg"},
		.values = &[_]Event.Value{
			.{.list = &[_]Event.Value{
				.{.double = -0.0}, .{.tinyint = -1}, .{.tinyint = -128},
				.{.smallint = -129}, .{.smallint = -32768},
				.{.integer = -32769}, .{.integer = -2147483648},
				.{.bigint = -2147483649}, .{.bigint = -9223372036854775807}, .{.bigint = -9223372036854775808}
			}},
		},
	}, event);
}

test "Event: parse integer overflow" {
	try t.expectError(error.InvalidNumber, Event.parse(t.allocator, "{\"overflow\": 18446744073709551616}"));
}

fn assertEvent(expected: anytype, actual: *Event) !void {
	try t.expectEqual(actual.fields.len, actual.values.len);
	try t.expectEqual(expected.fields.len, actual.fields.len);

	for (0..expected.fields.len) |i| {
		try t.expectEqual(expected.fields[i], actual.fields[i]);
		switch (expected.values[i]) {
			.list => |expected_list| {
				for (expected_list, actual.values[i].list) |expect_list_value, actual_list_value| {
					try t.expectEqual(expect_list_value, actual_list_value);
				}
			},
			else => |ex| try t.expectEqual(ex, actual.values[i]),
		}

	}
}