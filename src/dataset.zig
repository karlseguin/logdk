const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

const App = logdk.App;
const Event = logdk.Event;

const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const DataSet = struct {
	// DataSet doesn't usually need anything from the app, except when it modifies
	// itself, it needs to inform app.meta of the change.
	app: *App,

	// misc scrap, currently used to generate alter table T alter column C DDLs as needed
	buffer: zul.StringBuilder,

	// This entire DataSet is contained in this arena. After initialization, it will
	// keep growing if new columns are added, but that should be pretty minor.
	arena: *ArenaAllocator,

	// The name of the dataset, this is the name of the table.
	name: []const u8,

	// The DataSet is a pseudo Actor. Its main methods always run on the same
	// threads. This makes _everything_ so much easier. It also means all the
	// datasets running on 1 thread can share a connection. This makes prepared
	// statements easy to manage.
	conn: *zuckdb.Conn,

	// Our prepared statement for inserting 1 row.
	insert_one: zuckdb.Stmt,

	// The columns. In an ArrayList because we might have to add columns, and
	// that makes everything easier.
	columns: std.ArrayList(Column),

	// Used when seeing what column we need to add, key is owned by the Column
	// in the columns array.
	columnLookup: std.StringHashMapUnmanaged(void),

	// row could be either a *zuckdb.Row or a *zuckdb.OwningRow
	pub fn init(app: *App, row: anytype) !DataSet {
		const allocator = app.allocator;
		const arena = try allocator.create(ArenaAllocator);
		errdefer allocator.destroy(arena);

		arena.* = ArenaAllocator.init(allocator);
		errdefer arena.deinit();

		const aa = arena.allocator();

		const conn = try aa.create(zuckdb.Conn);
		conn.* = try app.db.newConn();
		errdefer conn.deinit();

		const name = try aa.dupe(u8, row.get([]const u8, 0));

		const columns = try std.json.parseFromSliceLeaky([]Column, aa, row.get([]const u8, 1), .{});
		const insert_one = try generateInsertOnePrepared(allocator, conn, name, columns);

		var columnLookup = std.StringHashMapUnmanaged(void){};
		try columnLookup.ensureTotalCapacity(aa, @intCast(columns.len));
		for (columns) |c| {
			columnLookup.putAssumeCapacity(c.name, {});
		}

		return .{
			.app = app,
			.name = name,
			.conn = conn,
			.arena = arena,
			.insert_one = insert_one,
			.columnLookup = columnLookup,
			.columns = std.ArrayList(Column).fromOwnedSlice(aa, columns),
			.buffer = zul.StringBuilder.fromOwnedSlice(aa, try aa.alloc(u8, logdk.MAX_IDENTIFIER_LEN * 2 + 512)), // more than enough for an alter table T alter column C ...
		};
	}

	pub fn deinit(self: *DataSet) void {
		self.insert_one.deinit();
		self.conn.deinit();

		const arena = self.arena;
		const allocator = arena.child_allocator;
		arena.deinit();
		allocator.destroy(arena);
	}

	pub fn columnsFromEvent(allocator: Allocator, event: *const Event) ![]Column {
		var columns = try allocator.alloc(Column, event.fieldCount());
		errdefer allocator.free(columns);

		var i: usize = 0;
		var it = event.map.iterator();
		while (it.next()) |kv| {
			columns[i] = Column.fromEventValue(kv.key_ptr.*, kv.value_ptr.*);
			i += 1;
		}

		std.mem.sort(Column, columns, {}, sortColumns);

		return columns;
	}

	pub const Message = union(enum) {
		record: *Event,
	};

	pub fn handle(self: *DataSet, message: Message) !void {
		switch (message) {
			.record => |event| return self.record(event) catch |err| {
				logdk.metrics.recordError();
				return err;
			},
		}
	}

	// This is part of the hot path. It should be optimized in the future to
	// do bulk-inserts (via a multi-values prepared statement) and possibly using
	// duckdb's appender if the C api ever solves a number of issues (no support for
	// defaults or complex types).
	// What makes this code complicated is that we might need to alter the dataset
	// and thus the underlying table. We might need to make a column nullable, or
	// change its type, or add a column.
	// But, because this is the hot path and because we expect most events not
	// to require any alterations, it's written in a way that assumes that the
	// event can be inserted as-is. We don't loop through the event first to
	// detect if there's a alteration. Instead, we start to bind our prepare
	// statement, and if, in doing so, we need to alter, things get a bit messy.
	// One issue with altering is that we need to re-prepare our insert statement
	// so it pretty much resets everything. Also, detecting new columns (fields in
	// event that aren't a known column) is particularly inefficient, but again
	// something we don't expect to have to do often.
	fn record(self: *DataSet, event: *Event) !void {
		defer event.deinit();

		var insert = &self.insert_one;
		try insert.clearBindings();


		// first_pass is an attempt to bind the event values to our prepared
		// statement as-is. This is the optimized case where we don't need to
		// alter the dataset and underlying table. If we succeed, we'll return
		// at the end of first_pass and are done.
		// If we do detect the need for an alteration, we switch to a slow-path.
		// Specifically, we'll apply any alternation needed, then break out of
		// first_pass, and execute our second pass (which should be guaranteed to
		// work since all necessary alterations should have been applied)
		first_pass: {
			// Used to track if we've used up all of the events fields. If we haven't
			// then we had new columns to add.
			var used_fields: usize = 0;

			for (self.columns.items, 0..) |*column, param_index| {
				const value = if (event.get(column.name)) |value| blk: {
					used_fields += 1;
					break :blk value;
				} else blk: {
					break :blk Event.Value{.null = {}};
				};

				switch (value) {
					.list => unreachable, // TODO
					.null => {
						if (column.nullable == false) {
							try self.alter(param_index, value, event, used_fields);
							break :first_pass;
						}
						try insert.bindValue(param_index, null);
					},
					inline else => |scalar| {
						const target_type = compatibleDataType(column.data_type, value);
						if (target_type != column.data_type) {
							try self.alter(param_index, value, event, used_fields);
							break :first_pass;
						}
						try insert.bindValue(param_index, scalar);
					},
				}
			}

			if (used_fields < event.fieldCount()) {
				try self.alter(self.columns.items.len, null, event, used_fields);
				break: first_pass;
			}

			// If we made it all the way here, then the event fit into our dataset
			// as-is (without any alternation) and thus we can finish our insert)
			const inserted = try insert.exec();
			std.debug.assert(inserted == 1);
			return;
		}

		// We can only be here because first_pass made alterations to the dataset
		// and underlying table. This also means our insert_one prepared statement
		// was re-generated. At this point, it should be possible to insert our
		// event as-is, using the new prepared statement.

		insert = &self.insert_one;
		try insert.clearBindings();
		for (self.columns.items, 0..) |*column, param_index| {
			const value = event.get(column.name) orelse Event.Value{.null = {}};
			switch (value) {
				.list => unreachable, // TODO
				.null => {
					std.debug.assert(column.nullable);
					try insert.bindValue(param_index, null);
				},
				inline else => |scalar| {
					std.debug.assert(compatibleDataType(column.data_type, value) == column.data_type);
					try insert.bindValue(param_index, scalar);
				},
			}
		}

		const inserted = try insert.exec();
		std.debug.assert(inserted == 1);

		logdk.metrics.alterDataSet();
		try self.app.meta.datasetChanged(self);
	}

	fn alter(self: *DataSet, start_index: usize, value_: ?Event.Value, event: *const Event, used_fields_: usize) !void {
		_ = try self.exec("begin", .{});
		errdefer _ = self.exec("rollback", .{}) catch {};

		const original_column_count = self.columns.items.len;

		var columns_added = false;
		errdefer if (columns_added) {
			// We need to revere potential changes we made to our columns and columnsLookup
			self.columns.shrinkRetainingCapacity(original_column_count);
			self.columnLookup.clearRetainingCapacity();
			for (self.columns.items) |*c| {
				self.columnLookup.putAssumeCapacity(c.name, {});
			}
		};

		var used_fields = used_fields_;

		// this is null if the only change is column addition. At this point, we don't
		// know what columns to add, but we know there's at least 1.
		if (value_) |value| {
			// we know this column/value mismatch since that's what triggered entering
			// this alter function.
			try self.alterColumn(&self.columns.items[start_index], value);

			for (self.columns.items[start_index+1..]) |*column| {
				const v = if (event.get(column.name)) |v| blk: {
					used_fields += 1;
					break :blk v;
				} else blk: {
					break :blk Event.Value{.null = {}};
				};
				try self.alterColumn(column, v);
			}
		}

		const new_column_count = event.fieldCount() - used_fields;
		if (new_column_count > 0) {
			columns_added = true;

			var buffer = &self.buffer;
			buffer.clearRetainingCapacity();

			// this is shared by all columns being added
			try buffer.write("alter table \"");
			try buffer.write(self.name);
			try buffer.write("\" add column ");
			const buffer_pos = buffer.len();

			var added: usize = 0;

			const aa = self.arena.allocator();
			var it = event.map.iterator();
			while (it.next()) |kv| {
				if (self.columnLookup.contains(kv.key_ptr.*)) {
					// we already know this field/column
					continue;
				}

				var column = Column.fromEventValue(kv.key_ptr.*, kv.value_ptr.*);

				// a column added after initial creation is always nullable, since
				// existing events won't have a value.
				column.nullable = true;

				buffer.pos = buffer_pos;
				try column.writeDDL(buffer.writer());

				_ = try self.exec(buffer.string(), .{});
				try self.columns.append(column);
				try self.columnLookup.put(aa, column.name, {});

				added += 1;
				if (added == new_column_count) {
					// optimization, we know there are `new_column_count` columns to add
					// and once we've added that number, we can stop iterating through
					// events
					break;
				}
			}
		}

		// This is bad. This is our app.allocator GPA, but what an awful way to get it
		const allocator = self.arena.child_allocator;
		const serialized_columns = try std.json.stringifyAlloc(allocator, self.columns.items, .{});
		defer allocator.free(serialized_columns);

		const n = try self.exec("update logdk.datasets set columns = $2 where name = $1", .{self.name, serialized_columns});
		std.debug.assert(n == 1);

		_ = try self.exec("commit", .{});

		const insert_one = try generateInsertOnePrepared(allocator, self.conn, self.name, self.columns.items);
		// safe to delete our existing one now
		self.insert_one.deinit();
		self.insert_one = insert_one;
	}

	fn alterColumn(self: *DataSet, column: *Column, value: Event.Value) !void {
		switch (value) {
			.list => unreachable, // TODO
			.null => if (column.nullable == false) {
				_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" drop not null", .{self.name, column.name}, .{});
				column.nullable = true;
			},
			else => {
				const target_type = compatibleDataType(column.data_type, value);
				if (target_type != column.data_type) {
					_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" set type {s}", .{self.name, column.name, @tagName(target_type)}, .{});
					column.data_type = target_type;
				}
			},
		}
	}

	fn exec(self: *DataSet, sql: []const u8, args: anytype) !usize {
		return self.conn.exec(sql, args) catch |err| {
			return logdk.dbErr("DataSet.exec", err, self.conn, logz.err().string("sql", sql));
		};
	}

	fn execFmt(self: *DataSet, comptime fmt: []const u8, fmt_args: anytype, args: anytype) !usize {
		var buffer = &self.buffer;
		buffer.clearRetainingCapacity();
		std.fmt.format(buffer.writer(), fmt, fmt_args) catch |err| {
			logz.err().ctx("DataSet.execFmt").string("fmt", fmt).err(err).log();
			return err;
		};
		return self.exec(buffer.string(), args);
	}
};

const Column = struct {
	name: []const u8,
	nullable: bool,
	is_list: bool,
	data_type: DataType,

	pub fn writeDDL(self: *const Column, writer: anytype) !void {
		// name should always be a valid identifier without quoting
		try writer.writeByte('"');
		try writer.writeAll(self.name);
		try writer.writeAll("\" ");

		switch (self.data_type) {
			.unknown => try writer.writeAll("text"),
			else => try writer.writeAll(@tagName(self.data_type)),
		}

		if (self.is_list) {
			try writer.writeAll("[]");
		}

		if (self.nullable) {
			try writer.writeAll(" null");
		} else {
			try writer.writeAll(" not null");
		}
	}

	pub fn fromEventValue(name: []const u8, value: Event.Value) Column {
		const event_type = std.meta.activeTag(value);
		const column_type = switch (value) {
			.list => |list| columnTypeForEventList(list),
			else => columnTypeFromEventScalar(event_type),
		};

		return .{
			.name = name,
			.is_list = event_type == .list,
			.nullable = event_type == .null,
			.data_type = column_type,
		};
	}
};

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
	text,
	json,
	unknown,
};

fn columnTypeFromEventScalar(event_type: Event.DataType) DataType {
	return switch (event_type) {
		.tinyint => .tinyint,
		.smallint => .smallint,
		.integer => .integer,
		.bigint => .bigint,
		.utinyint => .utinyint,
		.usmallint => .usmallint,
		.uinteger => .uinteger,
		.ubigint => .ubigint,
		.double => .double,
		.bool => .bool,
		.text => .text,
		.json => .json,
		.null => .unknown,
		.list => unreachable,
	};
}

fn columnTypeForEventList(list: []const Event.Value) DataType {
	if (list.len == 0) return .text;
	const first = list[0];
	var candidate = columnTypeFromEventScalar(std.meta.activeTag(first));
	for (list[1..]) |value| {
		candidate = compatibleDataType(candidate, value);
	}
	return candidate;
}

// It's impossible to get a null or list value since we expect that to already be
// special-cased by the caller.
//
// This code can lead to unecesasrily widening a column. If a column is `tinyint`
// and our value is `.{.utinyint = 10}`, then we know the column can stay `tinyint`.
// However, if the inverse happens, and or column is utinyint and the value is
// `.{.tinyint = -32}`, we have no way to know whether we can safely use a `tinyint`.
// This is because we don't know the max value of the column. As a utinyint, it
// could be holding values from 0-255. if max(column) <= 127, we could use a tinyint.
// If max(column) >= 128, we have to use a smallint. This could be solved by
// tracking (or fetching) max(column).
fn compatibleDataType(column_type: DataType, value: Event.Value) DataType {
	switch (column_type) {
		.bool => switch (value) {
			.bool => return .bool,
			.json => return .json,
			.null, .list => unreachable,
			else => return .text,
		},
		.tinyint => switch (value) {
			.tinyint => return .tinyint,
			.smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.utinyint => |v| return if (v <= 127) .tinyint else .smallint,
			.usmallint => |v| return if (v <= 32767) .smallint else .integer,
			.uinteger => |v| return if (v <= 2147483647) .integer else .bigint,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .text,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.utinyint => switch (value) {
			.utinyint => return .utinyint,
			.usmallint => return .usmallint,
			.uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.smallint => switch (value) {
			.utinyint, .tinyint, .smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.usmallint => |v| return if (v <= 32767) .smallint else .integer,
			.uinteger => |v| return if (v <= 2147483647) .integer else .bigint,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .text,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.usmallint => switch (value) {
			.utinyint, .usmallint => return .usmallint,
			.uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer => return .integer,
			.bigint => return .bigint,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.integer => switch (value) {
			.utinyint, .tinyint, .smallint, .usmallint, .integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.uinteger => |v| return if (v <= 2147483647) .integer else .bigint,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .text,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.uinteger => switch (value) {
			.utinyint, .usmallint, .uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer, .bigint => return .bigint,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.bigint => switch (value) {
			.utinyint, .tinyint, .smallint, .usmallint, .integer, .uinteger, .bigint => return .bigint,
			.double => return .double,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .text,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.ubigint => switch (value) {
			.utinyint, .usmallint, .uinteger, .ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer, .bigint => return .bigint,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.double => switch (value) {
			.tinyint, .utinyint, .smallint, .usmallint, .integer, .uinteger, .bigint, .ubigint, .double => return .double,
			.text, .bool => return .text,
			.json => return .json,
			.null, .list => unreachable,
		},
		.text => return .text,
		.json => return .json,
		.unknown => switch (value) {
			.tinyint => return .tinyint,
			.utinyint => return .utinyint,
			.smallint => return .smallint,
			.usmallint => return .usmallint,
			.integer => return .integer,
			.uinteger => return .uinteger,
			.bigint => return .bigint,
			.ubigint => return .ubigint,
			.double => return .double,
			.text => return .text,
			.bool => return .bool,
			.json => return .json,
			.null, .list => unreachable,
		}
	}
}

fn generateInsertOnePrepared(allocator: Allocator, conn: *zuckdb.Conn, name: []const u8, columns: []Column) !zuckdb.Stmt {
	var sb = zul.StringBuilder.init(allocator);
	defer sb.deinit();

	try sb.write("insert into ");
	try sb.write(name);
	try sb.write(" (");
	for (columns) |c| {
		try sb.writeByte('"');
		try sb.write(c.name);
		try sb.write("\", ");
	}
	// strip out the trailing comma + space
	sb.truncate(2);

	try sb.write(")\nvalues (");
	const writer = sb.writer();
	for (0..columns.len) |i| {
		try std.fmt.format(writer, "${d}, ", .{i+1});
	}
	// strip out the trailing comma + space
	sb.truncate(2);
	try sb.writeByte(')');

	return conn.prepare(sb.string(), .{.auto_release = false}) catch |err| {
		return logdk.dbErr("dataSetFromRow", err, conn, logz.err().string("sql", sb.string()));
	};
}

fn sortColumns(_: void, a: Column, b: Column) bool {
	return std.ascii.lessThanIgnoreCase(a.name, b.name);
}

const t = logdk.testing;
test "Column: writeDDL" {
	var buf = zul.StringBuilder.init(t.allocator);
	defer buf.deinit();

	{
		buf.clearRetainingCapacity();
		const c = Column{.name = "id", .nullable = false, .is_list = false, .data_type = .integer};
		try c.writeDDL(buf.writer());
		try t.expectEqual("\"id\" integer not null", buf.string());
	}

	{
		buf.clearRetainingCapacity();
		const c = Column{.name = "names", .nullable = true, .is_list = true, .data_type = .text};
		try c.writeDDL(buf.writer());
		try t.expectEqual("\"names\" text[] null", buf.string());
	}

	{
		buf.clearRetainingCapacity();
		const c = Column{.name = "details", .nullable = false, .is_list = false, .data_type = .unknown};
		try c.writeDDL(buf.writer());
		try t.expectEqual("\"details\" text not null", buf.string());
	}
}

test "columnTypeForEventList" {
	try t.expectEqual(.text, columnTypeForEventList(&.{}));
	{
		// tinyint transation
		try t.expectEqual(.tinyint, testColumnTypeEventList("-1, -20, -128"));
		try t.expectEqual(.tinyint, testColumnTypeEventList("-1, -20, 127"));
		try t.expectEqual(.smallint, testColumnTypeEventList("-1, -20, 128"));
		try t.expectEqual(.integer, testColumnTypeEventList("-1, -20, 32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-1, -20, 2147483648"));
		try t.expectEqual(.text, testColumnTypeEventList("-1, -20, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-1, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("-1, true"));
	}

	{
		// utinyint transation
		try t.expectEqual(.utinyint, testColumnTypeEventList("1, 128, 255"));
		try t.expectEqual(.usmallint, testColumnTypeEventList("1, 65535"));
		try t.expectEqual(.uinteger, testColumnTypeEventList("1, 65536"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("1, 4294967296"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("1, 18446744073709551615"));
		try t.expectEqual(.smallint, testColumnTypeEventList("1, -1"));
		try t.expectEqual(.smallint, testColumnTypeEventList("1, -32768"));
		try t.expectEqual(.integer, testColumnTypeEventList("1, -2147483648"));
		try t.expectEqual(.bigint, testColumnTypeEventList("1, -2147483649"));
		try t.expectEqual(.bigint, testColumnTypeEventList("1, -9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("1, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("1, false"));
	}

	{
		// smallint transition
		try t.expectEqual(.smallint, testColumnTypeEventList("-129, -20, -32768"));
		try t.expectEqual(.smallint, testColumnTypeEventList("-129, -4832, 32767"));
		try t.expectEqual(.integer, testColumnTypeEventList("-129, -4832, 32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-129, -4832, 2147483648"));
		try t.expectEqual(.text, testColumnTypeEventList("-129, 4, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-129, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("-129, true"));
	}

	{
		// usmallint transation
		try t.expectEqual(.usmallint, testColumnTypeEventList("256, 128, 255"));
		try t.expectEqual(.usmallint, testColumnTypeEventList("256, 65535"));
		try t.expectEqual(.uinteger, testColumnTypeEventList("256, 65536"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("256, 4294967296"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("256, 18446744073709551615"));
		try t.expectEqual(.integer, testColumnTypeEventList("256, -1"));
		try t.expectEqual(.integer, testColumnTypeEventList("256, -32768"));
		try t.expectEqual(.integer, testColumnTypeEventList("256, -2147483648"));
		try t.expectEqual(.bigint, testColumnTypeEventList("256, -2147483649"));
		try t.expectEqual(.bigint, testColumnTypeEventList("256, -9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("256, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("256, false"));
	}

	{
		// integer transition
		try t.expectEqual(.integer, testColumnTypeEventList("-32769, -20, -2147483647"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-32769, -4832, 2147483648"));
		try t.expectEqual(.text, testColumnTypeEventList("-32769, 4, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-32769, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("-32769, true"));
	}

	{
		// uinteger transation
		try t.expectEqual(.uinteger, testColumnTypeEventList("65536, 128, 255"));
		try t.expectEqual(.uinteger, testColumnTypeEventList("65536, 4294967295"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("65536, 4294967296"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("65536, 18446744073709551615"));
		try t.expectEqual(.bigint, testColumnTypeEventList("65536, -1"));
		try t.expectEqual(.bigint, testColumnTypeEventList("65536, -32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("65536, -2147483648"));
		try t.expectEqual(.bigint, testColumnTypeEventList("65536, -2147483649"));
		try t.expectEqual(.bigint, testColumnTypeEventList("65536, -9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("65536, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("65536, false"));
	}

	{
		// bigint transition
		try t.expectEqual(.bigint, testColumnTypeEventList("-2147483649, -20, -9223372036854775808"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-2147483649, -4832, 9223372036854775807"));
		try t.expectEqual(.text, testColumnTypeEventList("-2147483649, 4, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-2147483649, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("-2147483649, true"));
	}

	{
		// ubigint transation
		try t.expectEqual(.ubigint, testColumnTypeEventList("4294967296, 128, 255"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("4294967296, 4294967295"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("4294967296, 18446744073709551615"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -1"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -2147483648"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -2147483649"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("4294967296, 1.2"));
		try t.expectEqual(.text, testColumnTypeEventList("4294967296, false"));
	}

	{
		// double transition
		try t.expectEqual(.double, testColumnTypeEventList("1.02, -20, 43384848"));
		try t.expectEqual(.text, testColumnTypeEventList("-2147483649, 4, 9223372036854775808"));
		try t.expectEqual(.text, testColumnTypeEventList("-2147483649, true"));
	}

	{
		// bool transition
		try t.expectEqual(.bool, testColumnTypeEventList("true, false, true"));
		try t.expectEqual(.text, testColumnTypeEventList("false, 0"));
		try t.expectEqual(.text, testColumnTypeEventList("true, \"hello\""));
	}

	{
		// text transition
		try t.expectEqual(.text, testColumnTypeEventList("\"a\", \"abc\", \"213\""));
		try t.expectEqual(.text, testColumnTypeEventList("\"a\", 0"));
		try t.expectEqual(.text, testColumnTypeEventList("\"a\", 123.4, true"));
	}
}

fn testColumnTypeEventList(comptime event_value: []const u8) DataType {
	const event = Event.parse(t.allocator, "{\"list\": [" ++ event_value ++ "]}") catch unreachable;
	defer event.deinit();
	return columnTypeForEventList(event.map.get("list").?.list);
}

test "DataSet: columnsFromEvent" {
	const event = try Event.parse(t.allocator,
	  \\ {
	  \\   "id": 99999, "name": "teg", "details": {"handle": 9001},
	  \\   "l1": [1, -9000, 293000], "l2": [true, [123]]
	  \\ }
	);
	defer event.deinit();

	const columns = try DataSet.columnsFromEvent(t.allocator, event);
	defer t.allocator.free(columns);

	try t.expectEqual(5, columns.len);
	try t.expectEqual(.{.name = "details.handle", .is_list = false, .nullable = false, .data_type = .usmallint}, columns[0]);
	try t.expectEqual(.{.name = "id", .is_list = false, .nullable = false, .data_type = .uinteger}, columns[1]);
	try t.expectEqual(.{.name = "l1", .is_list = true, .nullable = false, .data_type = .integer}, columns[2]);
	try t.expectEqual(.{.name = "l2", .is_list = false, .nullable = false, .data_type = .json}, columns[3]);
	try t.expectEqual(.{.name = "name", .is_list = false, .nullable = false, .data_type = .text}, columns[4]);
}

test "DataSet: record simple" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSet(tc);

	{
		const event = try Event.parse(t.allocator, "{\"id\": 1, \"system\": \"catalog\", \"active\": true, \"record\": 0.932, \"category\": null}");
		try ds.record(event);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category from dataset_test where id =  1", .{})).?;
		defer row.deinit();

		try t.expectEqual(1, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(1, row.get(u16, 2));
		try t.expectEqual("catalog", row.get([]const u8, 3));
		try t.expectEqual(true, row.get(bool, 4));
		try t.expectEqual(0.932, row.get(f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
	}

	{
		// infer null from missing event field
		const event = try Event.parse(t.allocator, "{\"id\": 2, \"system\": \"other\", \"active\": false, \"record\": 4}");
		try ds.record(event);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category from dataset_test where id =  2", .{})).?;
		defer row.deinit();

		try t.expectEqual(2, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(2, row.get(u16, 2));
		try t.expectEqual("other", row.get([]const u8, 3));
		try t.expectEqual(false, row.get(bool, 4));
		try t.expectEqual(4, row.get(f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
	}

	{
		// makes a column nullable
		const event = try Event.parse(t.allocator, "{\"id\": null, \"system\": null, \"active\": null, \"record\": null}");
		try ds.record(event);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category from dataset_test where id is null", .{})).?;
		defer row.deinit();

		try t.expectEqual(3, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(null, row.get(?u16, 2));
		try t.expectEqual(null, row.get(?[]const u8, 3));
		try t.expectEqual(null, row.get(?bool, 4));
		try t.expectEqual(null, row.get(?f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));

		for (ds.columns.items) |c| {
			try t.expectEqual(true, c.nullable);
		}
	}

	{
		// alter type
		const event = try Event.parse(t.allocator, "{\"id\": -1003843293448, \"system\": 43, \"active\": \"maybe\", \"record\": 229, \"category\": -2}");
		try ds.record(event);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category from dataset_test where id = -1003843293448", .{})).?;
		defer row.deinit();

		try t.expectEqual(4, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(-1003843293448, row.get(i64, 2));
		try t.expectEqual("43", row.get([]const u8, 3));
		try t.expectEqual("maybe", row.get([]const u8, 4));
		try t.expectEqual(229, row.get(f64, 5));
		try t.expectEqual(-2, row.get(i8, 6));
	}
}

test "DataSet: record add column" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSet(tc);

	{
		const event = try Event.parse(t.allocator, "{\"id\": 5, \"new\": true}");
		try ds.record(event);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category, new from dataset_test where id =  5", .{})).?;
		defer row.deinit();

		try t.expectEqual(1, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(5, row.get(u16, 2));
		try t.expectEqual(null, row.get(?[]const u8, 3));
		try t.expectEqual(null, row.get(?bool, 4));
		try t.expectEqual(null, row.get(?f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
		try t.expectEqual(true, row.get(bool, 7));

		try t.expectEqual("new", ds.columns.items[5].name);
		try t.expectEqual(true, ds.columns.items[5].nullable);
		try t.expectEqual(false, ds.columns.items[5].is_list);
		try t.expectEqual(.bool, ds.columns.items[5].data_type);
		try t.expectEqual(true, ds.columnLookup.contains("new"));
	}

	{
		// no other difference, except for 2 new columns
		const event = try Event.parse(t.allocator, \\ {
		\\    "id": 6,
		\\    "system": "catalog",
		\\    "active": true,
		\\    "record": 0.932,
		\\    "category": null,
		\\    "new": false,
		\\    "tag1": "ok",
		\\    "tag2": -9999
		\\ }
		);
		try ds.record(event);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category, new, tag1, tag2 from dataset_test where id =  6", .{})).?;
		defer row.deinit();

		try t.expectEqual(2, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(6, row.get(u16, 2));
		try t.expectEqual("catalog", row.get([]const u8, 3));
		try t.expectEqual(true, row.get(bool, 4));
		try t.expectEqual(0.932, row.get(f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
		try t.expectEqual(false, row.get(bool, 7));
		try t.expectEqual("ok", row.get([]const u8, 8));
		try t.expectEqual(-9999, row.get(i16, 9));

		try t.expectEqual("tag1", ds.columns.items[6].name);
		try t.expectEqual(true, ds.columns.items[6].nullable);
		try t.expectEqual(false, ds.columns.items[6].is_list);
		try t.expectEqual(.text, ds.columns.items[6].data_type);
		try t.expectEqual(true, ds.columnLookup.contains("tag1"));

		try t.expectEqual("tag2", ds.columns.items[7].name);
		try t.expectEqual(true, ds.columns.items[7].nullable);
		try t.expectEqual(false, ds.columns.items[7].is_list);
		try t.expectEqual(.smallint, ds.columns.items[7].data_type);
		try t.expectEqual(true, ds.columnLookup.contains("tag2"));
	}
}

// This is one of those things. It's hard to create a DataSet since it requires
// a lot of setup. It needs a real table, since it prepares a statement. Tempting
// to think we can just fake create a dataset, ala, `return DataSet{....}`...but
// it's simpler and has better fidelity if we use the real APIs
fn testDataSet(tc: *t.Context) !*DataSet {
	const event = try Event.parse(t.allocator,
		\\ {
		\\    "id": 393,
		\\    "system": "catalog",
		\\    "active": true,
		\\    "record": 0.932,
		\\    "category": null
		\\ }
	);
	defer event.deinit();
	const actor_id = try tc.app.createDataSet(tc.env(), "dataset_test", event);
	return tc.app.dispatcher.unsafeInstance(DataSet, actor_id);
}
