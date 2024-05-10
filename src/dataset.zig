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
	logger: logz.Logger,

	// the next ldk_id value to insert
	next_id: u64,

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
	// threads. Rather than interacting with the DB pool, we just create a
	// connection for this dataset to use exclusively. This menas we can have
	// a long-living appender, which simplifies our hot path (inserting new events)
	conn: zuckdb.Conn,

	// Used to efficiently insert rows into a table
	appender: zuckdb.Appender,

	// The columns. In an ArrayList because we might have to add columns, and
	// that makes everything easier.
	columns: std.ArrayList(Column),

	// Used when seeing what column we need to add, key is owned by the Column
	// in the columns array.
	columnLookup: std.StringHashMapUnmanaged(void),

	// The number of unflushed records we have.
	unflushed: usize,

	// This datasets own actor_id, used with the dispatcher. The dataset needs
	// this in order to dispatch messages to itself (which sounds dumb, but
	// it allows us to schedule messages for later processing)
	actor_id: usize,

	// force a flush every 5 seconds (todo: make configurable, per dataset)
	max_unflushed_ms: i64 = 5000,

	// force a flush every 10000 records (todo: make configurable, per dataset)
	max_unflushed_count: usize = 10000,


	// row could be either a *zuckdb.Row or a *zuckdb.OwningRow
	pub fn init(app: *App, row: anytype) !DataSet {
		const allocator = app.allocator;
		const arena = try allocator.create(ArenaAllocator);
		errdefer allocator.destroy(arena);

		arena.* = ArenaAllocator.init(allocator);
		errdefer arena.deinit();

		const aa = arena.allocator();

		// const conn = try aa.create(zuckdb.Conn);
		var conn = try app.db.newConn();
		errdefer conn.deinit();

		const name = try aa.dupe(u8, row.get([]const u8, 0));
		const column_data = try aa.dupe(u8, row.get([]const u8, 1));
		const columns = try std.json.parseFromSliceLeaky([]Column, aa, column_data, .{.allocate = .alloc_if_needed});

		var columnLookup = std.StringHashMapUnmanaged(void){};
		try columnLookup.ensureTotalCapacity(aa, @intCast(columns.len));
		for (columns) |c| {
			columnLookup.putAssumeCapacity(c.name, {});
		}

		const current_max_id = blk: {
			var buf: [logdk.MAX_IDENTIFIER_LEN + 50]u8 = undefined;
			const sql = try std.fmt.bufPrint(&buf, "select max(ldk_id) from \"{s}\"", .{name});
			const max_id_row = try conn.row(sql, .{}) orelse break :blk 0;
			defer max_id_row.deinit();
			break :blk max_id_row.get(?u64, 0) orelse 0;
		};

		var logger = (try logz.newLogger()).string("name", name).multiuse();
		errdefer logger.deinit();

		var appender = try conn.appender(null, name);
		errdefer appender.deinit();

		return .{
			.app = app,
			.name = name,
			.conn = conn,
			.arena = arena,
			.actor_id = 0,
			.unflushed = 0,
			.logger = logger,
			.appender = appender,
			.columnLookup = columnLookup,
			.next_id = current_max_id + 1,
			.columns = std.ArrayList(Column).fromOwnedSlice(aa, columns),
			.buffer = zul.StringBuilder.fromOwnedSlice(aa, try aa.alloc(u8, logdk.MAX_IDENTIFIER_LEN * 2 + 512)), // more than enough for an alter table T alter column C ...
		};
	}

	pub fn deinit(self: *DataSet) void {
		self.appender.deinit();
		self.conn.deinit();
		self.logger.deinit();

		const arena = self.arena;
		const allocator = arena.child_allocator;
		arena.deinit();
		allocator.destroy(arena);
	}

	// allocator is the ArenaAllocator, so we can be a little sloppy
	pub fn columnsFromEvent(allocator: Allocator, event: Event, logger: logz.Logger) ![]Column {
		var columns = try allocator.alloc(Column, event.fieldCount());
		errdefer allocator.free(columns);

		var i: usize = 0;
		var it = event.map.iterator();
		while (it.next()) |kv| {
			const name = kv.key_ptr.*;
			if (Column.isValidName(name) == false) {
				_ = logger.string("name", name);
				continue;
			}
			columns[i] = Column.fromEventValue(try allocator.dupe(u8, name), kv.value_ptr.*);
			i += 1;
		}

		columns = columns[0..i];
		std.mem.sort(Column, columns, {}, sortColumns);
		return columns;
	}

	pub const Message = union(enum) {
		flush: void,
		record: Event.List,
	};

	pub fn handle(self: *DataSet, message: Message) !void {
		switch (message) {
			.record => |event_list| return self.record(event_list),
			.flush => self.flushAppender() catch {}, // already logged
		}
	}

	// This is part of the hot path.
	// What makes this code complicated is that we might need to alter the dataset
	// and thus the underlying table. We might need to make a column nullable, or
	// change its type, or add a column.
	// But, because this is the hot path and because we expect most events not
	// to require any alterations, it's written in a way that assumes that the
	// event can be inserted as-is. We don't loop through the event first to
	// detect if there's a alteration. Instead, we start to bind values to our appender
	// and if, in doing so, we need to alter, things get a bit messy.
	// We'll flush whatever rows we've appended up until this point, alter the table
	// and re-create the appender (which will be based on the altered table)
	// This works because if we call appender.flush() without first calling
	// appender.endRow(), the current row is abandoned.
	fn record(self: *DataSet, event_list: Event.List) void {
		defer event_list.deinit();

		var next_id = self.next_id;
		defer self.next_id = next_id;

		var alter_count: usize = 0;
		var error_count: usize = 0;
		const created = event_list.created;

		for (event_list.events) |event| {
			defer next_id += 1;
			const a = self.recordEvent(next_id, created, event) catch |err| blk: {
				if (error_count == 0) {
					// Just log 1 of these per batch
					// self.appender.err might be null, but that's ok
					self.logger.level(.Error).ctx("DataSet.recordEvent").err(err).string("details", self.appender.err).log();
				}
				error_count += 1;
				break :blk false;
			};
			if (a) {
				alter_count += 1;
			}
		}

		var appended = event_list.events.len;
		if (error_count > 0) {
			logdk.metrics.recordError(error_count);
			appended -= error_count;
		}

		if (appended > 0) {
			const pre_unflushed = self.unflushed;
			const new_unflushed = pre_unflushed + appended;

			if (new_unflushed >= self.max_unflushed_count) {
				self.flushAppender() catch {}; // already logged, not much point in failing now
			} else {
				if (pre_unflushed == 0) {
					// these are the first events (either since program start, or since
					// the last flush), schedule a flush.
					self.app.scheduler.scheduleIn(.{.flush_dataset = self.actor_id}, self.max_unflushed_ms) catch |err| {
						self.logger.level(.Error).ctx("DataSet.schedule.flush").err(err).log();
					};
				}
				self.unflushed = new_unflushed;
			}
		}

		if (alter_count > 0) {
			self.app.meta.datasetChanged(self) catch |err| {
				self.logger.level(.Error).ctx("Meta.datasetChanged").err(err).log();
			};
			logdk.metrics.alterDataSet(alter_count);
		}
	}

	fn recordEvent(self: *DataSet, next_id: u64, created: i64, event: Event) !bool {
		var appender = &self.appender;
		appender.beginRow();
		try appender.appendValue(next_id, 0);
		try appender.appendValue(created, 1);

		// first_pass is an attempt to bind the event values to our appender as-is.
		// This is the optimized case where we don't need to alter the dataset and
		// underlying table. If we succeed, we'll return at the end of first_pass and are done.
		// If we do detect the need for an alteration, we switch to a slow-path.
		// Specifically, we'll apply any alternation needed, then break out of
		// first_pass, and execute our second pass (which should be guaranteed to
		// work since all necessary alterations should have been applied)
		first_pass: {
			// Used to track if we've used up all of the events fields. If we haven't
			// then we had new columns to add.
			var used_fields: usize = 0;

			for (self.columns.items, 2..) |*column, param_index| {
				const value = if (event.get(column.name)) |value| blk: {
					used_fields += 1;
					break :blk value;
				} else blk: {
					break :blk Event.Value{.null = {}};
				};

				switch (value) {
					.null => {
						if (column.nullable == false) {
							try self.alter(param_index - 2, value, event, used_fields);
							break :first_pass;
						}
						try appender.appendValue(null, param_index);
					},
					.list => |list| {
						if (column.data_type == .json) {
							try appender.appendValue(list.json, param_index);
							continue;
						}

						if (column.is_list == false) {
							try self.alter(param_index - 2, value, event, used_fields);
							break :first_pass;
						}

						const target_type = compatibleListDataType(column.data_type, columnTypeForEventList(list.values));
						if (target_type != column.data_type) {
							try self.alter(param_index - 2, value, event, used_fields);
							break :first_pass;
						}

						try appendList(target_type, appender, list, param_index);
					},
					inline else => |scalar| {
						const target_type = compatibleDataType(column.data_type, value);
						if (target_type != column.data_type) {
							try self.alter(param_index - 2, value, event, used_fields);
							break :first_pass;
						}
						if (column.is_list) {
							// the column is a list, but we were given a single value. This is
							// a problem given DuckDB's lack of list binding support.
							// At this point, list.json should not be needed (fingers crossed);
							const list = Event.Value.List{.values = &.{value}, .json = "[]"};
							try appendList(target_type, appender, list, param_index);
						} else {
							try appender.appendValue(scalar, param_index);
						}
					},
				}
			}

			if (used_fields < event.fieldCount()) {
				try self.alter(self.columns.items.len, null, event, used_fields);
				break: first_pass;
			}

			// If we made it all the way here, then the event fit into our dataset
			// as-is (without any alternation) and thus we can finish our insert)
			try appender.endRow();
			return false;
		}

		// We can only be here because first_pass made alterations to the dataset
		// and underlying table. This also means that self.appender is new.
		// At this point, it should be possible to insert our event as-is,
		// since our new appender exists based on the updated schema.

		appender = &self.appender;
		appender.beginRow();
		try appender.appendValue(next_id, 0);
		try appender.appendValue(created, 1);

		for (self.columns.items, 2..) |*column, param_index| {
			const value = event.get(column.name) orelse Event.Value{.null = {}};
			switch (value) {
				.list => |list| try appendList(column.data_type, appender, list, param_index),
				.null => {
					std.debug.assert(column.nullable);
					try appender.appendValue(null, param_index);
				},
				inline else => |scalar| {
					std.debug.assert(compatibleDataType(column.data_type, value) == column.data_type);
					if (column.is_list) {
						// the column is a list, but we were given a single value. This is
						// a problem given DuckDB's lack of list binding support.
						// At this point, list.json should not be needed (fingers crossed);
						const list = Event.Value.List{.values = &.{value}, .json = "[]"};
						try appendList(column.data_type, appender, list, param_index);
					} else {
						try appender.appendValue(scalar, param_index);
					}
				},
			}
		}

		try appender.endRow();
		return true;
	}

	fn alter(self: *DataSet, start_index: usize, value_: ?Event.Value, event: Event, used_fields_: usize) !void {
		// Flush any pending rows we have, this is safe to do even if we've bound values
		// for this existing event since it'll flush only data up until the last endRow;
		try self.flushAppender();

		_ = try self.exec("begin", .{});
		errdefer _ = self.exec("rollback", .{}) catch {};

		const original_column_count = self.columns.items.len;

		var columns_added = false;
		errdefer if (columns_added) {
			// We need to reverse potential changes we made to our columns and columnsLookup
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
				const name = kv.key_ptr.*;
				if (self.columnLookup.contains(name)) {
					// we already know this field/column
					continue;
				}

				if (Column.isValidName(name) == false) {
					self.logger.level(.Warn).ctx("DataSet.alter.invalid_name").string("name", name).log();
					continue;
				}

				var column = Column.fromEventValue(try aa.dupe(u8, name), kv.value_ptr.*);

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

		const allocator = self.app.allocator;
		const serialized_columns = try std.json.stringifyAlloc(allocator, self.columns.items, .{});
		defer allocator.free(serialized_columns);

		const n = try self.exec("update logdk.datasets set columns = $2 where name = $1", .{self.name, serialized_columns});
		std.debug.assert(n == 1);

		_ = try self.exec("commit", .{});

		const appender = try self.conn.appender(null, self.name);
		// safe to delete our existing one now
		self.appender.deinit();
		self.appender = appender;
	}

	fn alterColumn(self: *DataSet, column: *Column, value: Event.Value) !void {
		switch (value) {
			.null => if (column.nullable == false) {
				_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" drop not null", .{self.name, column.name}, .{});
				column.nullable = true;
			},
			.list => |list| {
				const target_type = compatibleListDataType(column.data_type, columnTypeForEventList(list.values));
				if (target_type == .json and column.data_type != .json) {
					_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" set type json", .{self.name, column.name}, .{});
					column.is_list = false;
					column.data_type = .json;
				} else if (column.is_list == false) {
					_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" set type {s}[] using array[\"{s}\"]", .{self.name, column.name, @tagName(target_type), column.name}, .{});
					column.is_list = true;
					column.data_type = target_type;
				} else if (target_type != column.data_type) {
					_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" set type {s}[]", .{self.name, column.name, @tagName(target_type)}, .{});
					column.is_list = true;
					column.data_type = target_type;
				}
			},
			else => {
				const target_type = compatibleDataType(column.data_type, value);
				if (target_type != column.data_type) {
					const extra = if (column.is_list) "[]" else "";
					_ = try self.execFmt("alter table \"{s}\" alter column \"{s}\" set type {s}{s}", .{self.name, column.name, @tagName(target_type), extra}, .{});
					column.data_type = target_type;
				}
			},
		}
	}

	fn flushAppender(self: *DataSet) !void {
		self.appender.flush() catch |err| {
			self.logger.level(.Error).ctx("DataSet.flushAppender").err(err).string("details", self.appender.err).log();
			return err;
		};
		self.unflushed = 0;
	}

	fn exec(self: *DataSet, sql: []const u8, args: anytype) !usize {
		return self.conn.exec(sql, args) catch |err| {
			return logdk.dbErr("DataSet.exec", err, &self.conn, self.logger.level(.Error).string("sql", sql));
		};
	}

	fn execFmt(self: *DataSet, comptime fmt: []const u8, fmt_args: anytype, args: anytype) !usize {
		var buffer = &self.buffer;
		buffer.clearRetainingCapacity();
		std.fmt.format(buffer.writer(), fmt, fmt_args) catch |err| {
			self.logger.level(.Error).ctx("DataSet.execFmt").string("fmt", fmt).err(err).log();
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

	pub fn isValidName(name: []const u8) bool {
		return logdk.Validate.identifierIsValid(name);
	}

	pub fn writeDDL(self: *const Column, writer: anytype) !void {
		// name should always be a valid identifier without quoting
		try writer.writeByte('"');
		try writer.writeAll(self.name);
		try writer.writeAll("\" ");

		switch (self.data_type) {
			.unknown => try writer.writeAll("varchar"),
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
		var event_type = std.meta.activeTag(value);
		const column_type = switch (value) {
			.list => |list| columnTypeForEventList(list.values),
			else => columnTypeFromEventScalar(event_type),
		};

		// instead of having a json[] column, we're going to have a json column
		// which holds an array. I *think* this is more usable.
		if (event_type == .list and column_type == .json) {
			event_type = .json;
		}


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
	varchar,
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
		.string => .varchar,
		.json => .json,
		.null => .unknown,
		.list => unreachable,
	};
}

// Only very obvious expansions (like smallint[] -> bigint[]) are done. Otherwise,
// when list types mismatch, say a column that's a varchar[], but an event that
// has a tinyint[], we convert the column to JSON.
fn columnTypeForEventList(list: []const Event.Value) DataType {
	if (list.len == 0) return .varchar;
	const first = list[0];
	var candidate = columnTypeFromEventScalar(std.meta.activeTag(first));
	for (list[1..]) |value| {
		const maybe = compatibleDataType(candidate, value);
		if ((maybe == .varchar and candidate != .varchar) or (maybe == .varchar and std.meta.activeTag(value) != .string)) {
			// A JSON column never converts to anything else. So once we have that as
			// a candiadte, we don't need to look any further.
			return .json;
		}
		candidate = maybe;
	}
	return candidate;
}

// This code can lead to unnecessarily widening a column. If a column is `tinyint`
// and our value is `.{.utinyint = 10}`, then we know the column can stay `tinyint`.
// However, if the inverse happens, and or column is utinyint and the value is
// `.{.tinyint = -32}`, we have no way to know whether we can safely use a `tinyint`.
// This is because we don't know the max value of the column. As a utinyint, it
// could be holding values from 0-255. if max(column) <= 127, we could use a tinyint.
// If max(column) >= 128, we have to use a smallint. This could be solved by
// tracking max(column).
fn compatibleDataType(column_type: DataType, value: Event.Value) DataType {
	switch (column_type) {
		.bool => switch (value) {
			.null, .bool => return .bool,
			.json => return .json,
			.list => unreachable,
			else => return .varchar,
		},
		.tinyint => switch (value) {
			.null, .tinyint => return .tinyint,
			.smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.utinyint => |v| return if (v <= 127) .tinyint else .smallint,
			.usmallint => |v| return if (v <= 32767) .smallint else .integer,
			.uinteger => |v| return if (v <= 2147483647) .integer else .bigint,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .varchar,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.utinyint => switch (value) {
			.null, .utinyint => return .utinyint,
			.usmallint => return .usmallint,
			.uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.smallint => switch (value) {
			.null, .utinyint, .tinyint, .smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.usmallint => |v| return if (v <= 32767) .smallint else .integer,
			.uinteger => |v| return if (v <= 2147483647) .integer else .bigint,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .varchar,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.usmallint => switch (value) {
			.null, .utinyint, .usmallint => return .usmallint,
			.uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer => return .integer,
			.bigint => return .bigint,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.integer => switch (value) {
			.null, .utinyint, .tinyint, .smallint, .usmallint, .integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.uinteger => |v| return if (v <= 2147483647) .integer else .bigint,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .varchar,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.uinteger => switch (value) {
			.null, .utinyint, .usmallint, .uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer, .bigint => return .bigint,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.bigint => switch (value) {
			.null, .utinyint, .tinyint, .smallint, .usmallint, .integer, .uinteger, .bigint => return .bigint,
			.double => return .double,
			.ubigint => |v| return if (v <= 9223372036854775807) .bigint else .varchar,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.ubigint => switch (value) {
			.null, .utinyint, .usmallint, .uinteger, .ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer, .bigint => return .bigint,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.double => switch (value) {
			.null, .tinyint, .utinyint, .smallint, .usmallint, .integer, .uinteger, .bigint, .ubigint, .double => return .double,
			.string, .bool => return .varchar,
			.json => return .json,
			.list => unreachable,
		},
		.varchar => switch (value) {
			.null => return .varchar,
			.json => return .json,
			else => return .varchar,
		},
		.json => return .json,
		.unknown => switch (value) {
			.null => return .unknown,
			.tinyint => return .tinyint,
			.utinyint => return .utinyint,
			.smallint => return .smallint,
			.usmallint => return .usmallint,
			.integer => return .integer,
			.uinteger => return .uinteger,
			.bigint => return .bigint,
			.ubigint => return .ubigint,
			.double => return .double,
			.string => return .varchar,
			.bool => return .bool,
			.json => return .json,
			.list => unreachable,
		}
	}
}

fn compatibleListDataType(column_type: DataType, list_type: DataType) DataType {
	switch (column_type) {
		.bool => switch (list_type) {
			.bool => return .bool,
			else => return .json,
		},
		.tinyint => switch (list_type) {
			.tinyint, .unknown => return .tinyint,
			.smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.utinyint => return .smallint,
			.usmallint => return .integer,
			.uinteger => return .bigint,
			.ubigint => return .varchar,
			.varchar, .bool, .json => return .json,
		},
		.utinyint => switch (list_type) {
			.utinyint, .unknown => return .utinyint,
			.usmallint => return .usmallint,
			.uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.varchar, .bool, .json => return .json,
		},
		.smallint => switch (list_type) {
			.utinyint, .tinyint, .smallint, .unknown => return .smallint,
			.integer => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.usmallint => return .integer,
			.uinteger => return .bigint,
			.ubigint => return .varchar,
			.varchar, .bool, .json => return .json,
		},
		.usmallint => switch (list_type) {
			.utinyint, .usmallint, .unknown => return .usmallint,
			.uinteger => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer => return .integer,
			.bigint => return .bigint,
			.varchar, .bool, .json => return .json,
		},
		.integer => switch (list_type) {
			.utinyint, .tinyint, .smallint, .usmallint, .integer, .unknown => return .integer,
			.bigint => return .bigint,
			.double => return .double,
			.uinteger => return .bigint,
			.ubigint => return .varchar,
			.varchar, .bool, .json => return .json,
		},
		.uinteger => switch (list_type) {
			.utinyint, .usmallint, .uinteger, .unknown => return .uinteger,
			.ubigint => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer, .bigint => return .bigint,
			.varchar, .json, .bool => return .json,
		},
		.bigint => switch (list_type) {
			.utinyint, .tinyint, .smallint, .usmallint, .integer, .uinteger, .bigint, .unknown => return .bigint,
			.double => return .double,
			.ubigint => return .varchar,
			.varchar, .json, .bool => return .json,
		},
		.ubigint => switch (list_type) {
			.utinyint, .usmallint, .uinteger, .ubigint, .unknown => return .ubigint,
			.double => return .double,
			.tinyint, .smallint, .integer, .bigint => return .bigint,
			.varchar, .json, .bool => return .json,
		},
		.double => switch (list_type) {
			.tinyint, .utinyint, .smallint, .usmallint, .integer, .uinteger, .bigint, .ubigint, .double, .unknown => return .double,
			.varchar, .bool, .json => return .json,
		},
		.varchar => switch (list_type) {
			.varchar => return .varchar,
			else => return .json,
		},
		.json => return .json,
		.unknown => return list_type,
	}}

// Appending a list is, sadly, complicated. The issue is that the DuckDB driver
// reasonably expect a proper list, say an []i32 or a []?bool. But what we
// have is an []Event.Value, i.e. a slice of custom union values.
//
// We _could_ change Event.Value.List to itself be a union of concrete types. So
// instead of a slice of unions ([]Event.Value), it could be a union of slices
// (e.g. Event.Value.List.i32 would be an []i32). But this wouldn't work in all
// cases since the DuckDB driver expect an _exact_ match. If we try to bind an
// []u16 into an integer[], it'll fail. And we very well might be trying to do that.
//
// The simplest approach is to take our []Event.Value and, using the target_type,
// create an appropriate []T. We're aided by the fact that, for any great mismatch,
// the column type is json and we can insert the raw list json as-is. So, for example,
// we know that we won't have to covert an Event list of integers, bools or floats to
// strings. We also know that, by the type appendList has been called, our values
// are compatible with target_type. So if our target_type is `bigint`, we know that
// ever value in our list can be converted to an ?i64.
//
// It's worth nothing that we could do this allocation-free. We could iterate our
// []Event.Value and append them directly into the underlying DuckDB vectors.

fn appendList(target_type: DataType, appender: *zuckdb.Appender, list: Event.Value.List, param_index: usize) !void {
	if (target_type == .json) {
		// Our event thankfully stores the raw array json. For a json column
		// we can just insert this as-is.
		return appender.appendValue(list.json, param_index);
	}

	switch (target_type) {
		.tinyint => return appender.appendListMap(Event.Value, i8, param_index, list.values, listItemMapTinyInt),
		.smallint => return appender.appendListMap(Event.Value, i16, param_index, list.values, listItemMapSmallInt),
		.integer => return appender.appendListMap(Event.Value, i32, param_index, list.values, listItemMapInteger),
		.bigint => return appender.appendListMap(Event.Value, i64, param_index, list.values, listItemMapBigInt),
		.utinyint => return appender.appendListMap(Event.Value, u8, param_index, list.values, listItemMapUTinyInt),
		.usmallint => return appender.appendListMap(Event.Value, u16, param_index, list.values, listItemMapUSmallInt),
		.uinteger => return appender.appendListMap(Event.Value, u32, param_index, list.values, listItemMapUInteger),
		.ubigint => return appender.appendListMap(Event.Value, u64, param_index, list.values, listItemMapUBigInt),
		.double => return appender.appendListMap(Event.Value, f64, param_index, list.values, listItemMapDouble),
		.bool => return appender.appendListMap(Event.Value, bool, param_index, list.values, listItemMapBool),
		.varchar => return appender.appendListMap(Event.Value, []const u8, param_index, list.values, listItemMapText),
		.json, .unknown => unreachable,
	}
}

// For all of these, the unsigned to signed cast, such as
//   .utinyint => |v| return @intCast(v)
// ought to be safe, because if we're asking for an i8, then we've already validated
// that the value fits.
fn listItemMapTinyInt(value: Event.Value) ?i8 {
	switch (value) {
		.null => return null,
		.tinyint => |v| return v,
		.utinyint => |v| return @intCast(v),
		else => unreachable,
	}
}

fn listItemMapSmallInt(value: Event.Value) ?i16 {
	switch (value) {
		.null => return null,
		.tinyint, .utinyint, .smallint => |v| return v,
		.usmallint => |v| return @intCast(v),
		else => unreachable,
	}
}

fn listItemMapInteger(value: Event.Value) ?i32 {
	switch (value) {
		.null => return null,
		.tinyint, .utinyint, .smallint, .usmallint, .integer  => |v| return v,
		.uinteger => |v| return @intCast(v),
		else => unreachable,
	}
}

fn listItemMapBigInt(value: Event.Value) ?i64 {
	switch (value) {
		.null => return null,
		.tinyint, .utinyint, .smallint, .usmallint, .integer, .uinteger, .bigint  => |v| return v,
		.ubigint => |v| return @intCast(v),
		else => unreachable,
	}
}

fn listItemMapUTinyInt(value: Event.Value) ?u8 {
	switch (value) {
		.null => return null,
		.utinyint => |v| return v,
		else => unreachable,
	}
}

fn listItemMapUSmallInt(value: Event.Value) ?u16 {
	switch (value) {
		.null => return null,
		.utinyint, .usmallint => |v| return v,
		else => unreachable,
	}
}

fn listItemMapUInteger(value: Event.Value) ?u32 {
	switch (value) {
		.null => return null,
		.utinyint, .usmallint, .uinteger  => |v| return v,
		else => unreachable,
	}
}

fn listItemMapUBigInt(value: Event.Value) ?u64 {
	switch (value) {
		.null => return null,
		.utinyint, .usmallint, .uinteger, .ubigint  => |v| return v,
		else => unreachable,
	}
}

fn listItemMapDouble(value: Event.Value) ?f64 {
	switch (value) {
		.null => return null,
		.double => |v| return v,
		.tinyint, .smallint, .integer, .bigint,
		.utinyint, .usmallint, .uinteger => |v| return @floatFromInt(v),
		.ubigint => |v| return @floatFromInt(v),
		else => unreachable,
	}
}

fn listItemMapBool(value: Event.Value) ?bool {
	switch (value) {
		.null => return null,
		.bool => |v| return v,
		else => unreachable,
	}
}

fn listItemMapText(value: Event.Value) ?[]const u8 {
	switch (value) {
		.null => return null,
		.string => |v| return v,
		else => unreachable,
	}
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
		const c = Column{.name = "names", .nullable = true, .is_list = true, .data_type = .varchar};
		try c.writeDDL(buf.writer());
		try t.expectEqual("\"names\" varchar[] null", buf.string());
	}

	{
		buf.clearRetainingCapacity();
		const c = Column{.name = "details", .nullable = false, .is_list = false, .data_type = .unknown};
		try c.writeDDL(buf.writer());
		try t.expectEqual("\"details\" varchar not null", buf.string());
	}
}

test "columnTypeForEventList" {
	try t.expectEqual(.varchar, columnTypeForEventList(&.{}));
	{
		// tinyint
		try t.expectEqual(.tinyint, testColumnTypeEventList("-1, -20, -128"));
		try t.expectEqual(.tinyint, testColumnTypeEventList("-1, -20, 127"));
		try t.expectEqual(.smallint, testColumnTypeEventList("-1, -20, 128"));
		try t.expectEqual(.integer, testColumnTypeEventList("-1, -20, 32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-1, -20, 2147483648"));
		try t.expectEqual(.json, testColumnTypeEventList("-1, -20, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-1, 1.2"));
		try t.expectEqual(.json, testColumnTypeEventList("-1, true"));
	}

	{
		// utinyint
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
		try t.expectEqual(.json, testColumnTypeEventList("1, false"));
	}

	{
		// smallint
		try t.expectEqual(.smallint, testColumnTypeEventList("-129, -20, -32768"));
		try t.expectEqual(.smallint, testColumnTypeEventList("-129, -4832, 32767"));
		try t.expectEqual(.integer, testColumnTypeEventList("-129, -4832, 32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-129, -4832, 2147483648"));
		try t.expectEqual(.json, testColumnTypeEventList("-129, 4, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-129, 1.2"));
		try t.expectEqual(.json, testColumnTypeEventList("-129, true"));
	}

	{
		// usmallint
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
		try t.expectEqual(.json, testColumnTypeEventList("256, false"));
	}

	{
		// integer
		try t.expectEqual(.integer, testColumnTypeEventList("-32769, -20, -2147483647"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-32769, -4832, 2147483648"));
		try t.expectEqual(.json, testColumnTypeEventList("-32769, 4, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-32769, 1.2"));
		try t.expectEqual(.json, testColumnTypeEventList("-32769, true"));
	}

	{
		// uinteger
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
		try t.expectEqual(.json, testColumnTypeEventList("65536, false"));
	}

	{
		// bigint
		try t.expectEqual(.bigint, testColumnTypeEventList("-2147483649, -20, -9223372036854775808"));
		try t.expectEqual(.bigint, testColumnTypeEventList("-2147483649, -4832, 9223372036854775807"));
		try t.expectEqual(.json, testColumnTypeEventList("-2147483649, 4, 9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("-2147483649, 1.2"));
		try t.expectEqual(.json, testColumnTypeEventList("-2147483649, true"));
	}

	{
		// ubigint
		try t.expectEqual(.ubigint, testColumnTypeEventList("4294967296, 128, 255"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("4294967296, 4294967295"));
		try t.expectEqual(.ubigint, testColumnTypeEventList("4294967296, 18446744073709551615"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -1"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -32768"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -2147483648"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -2147483649"));
		try t.expectEqual(.bigint, testColumnTypeEventList("4294967296, -9223372036854775808"));
		try t.expectEqual(.double, testColumnTypeEventList("4294967296, 1.2"));
		try t.expectEqual(.json, testColumnTypeEventList("4294967296, false"));
	}

	{
		// double
		try t.expectEqual(.double, testColumnTypeEventList("1.02, -20, 43384848"));
		try t.expectEqual(.json, testColumnTypeEventList("-2147483649, 4, 9223372036854775808"));
		try t.expectEqual(.json, testColumnTypeEventList("-2147483649, true"));
	}

	{
		// bool
		try t.expectEqual(.bool, testColumnTypeEventList("true, false, true"));
		try t.expectEqual(.json, testColumnTypeEventList("false, 0"));
		try t.expectEqual(.json, testColumnTypeEventList("true, \"hello\""));
	}

	{
		// varchar
		try t.expectEqual(.varchar, testColumnTypeEventList("\"a\", \"abc\", \"213\""));
		try t.expectEqual(.json, testColumnTypeEventList("\"a\", 0"));
		try t.expectEqual(.json, testColumnTypeEventList("\"a\", 123.4, true"));
	}

	{
		// with null
		try t.expectEqual(.utinyint, testColumnTypeEventList("1, 2, null, 3"));
	}
}

fn testColumnTypeEventList(comptime event_value: []const u8) DataType {
	const event_list = Event.parse(t.allocator, "{\"list\": [" ++ event_value ++ "]}") catch unreachable;
	defer event_list.deinit();
	return columnTypeForEventList(event_list.events[0].map.get("list").?.list.values);
}

test "DataSet: columnsFromEvent" {
	const event_list = try Event.parse(t.allocator,
	  \\ {
	  \\   "id": 99999, "name": "teg", "details": {"handle": 9001},
	  \\   "l1": [1, -9000, 293000], "l2": [true, [123]]
	  \\ }
	);
	defer event_list.deinit();

	const columns = try DataSet.columnsFromEvent(t.allocator, event_list.events[0], logz.logger());
	defer {
		// this is normally managed by an arena
		for (columns) |c| {
			t.allocator.free(c.name);
		}
		t.allocator.free(columns);
	}

	try t.expectEqual(5, columns.len);
	try t.expectEqual(.{.name = "details", .is_list = false, .nullable = false, .data_type = .json}, columns[0]);
	try t.expectEqual(.{.name = "id", .is_list = false, .nullable = false, .data_type = .uinteger}, columns[1]);
	try t.expectEqual(.{.name = "l1", .is_list = true, .nullable = false, .data_type = .integer}, columns[2]);
	try t.expectEqual(.{.name = "l2", .is_list = false, .nullable = false, .data_type = .json}, columns[3]);
	try t.expectEqual(.{.name = "name", .is_list = false, .nullable = false, .data_type = .varchar}, columns[4]);
}

test "DataSet: record simple" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSet(tc);

	{
		const event_list = try Event.parse(t.allocator, "{\"id\": 1, \"system\": \"catalog\", \"active\": true, \"record\": 0.932, \"category\": null}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, system, active, record, category from dataset_test where id =  1", .{})).?;
		defer row.deinit();

		try t.expectEqual(1, row.get(u64, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(1, row.get(u16, 2));
		try t.expectEqual("catalog", row.get([]const u8, 3));
		try t.expectEqual(true, row.get(bool, 4));
		try t.expectEqual(0.932, row.get(f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
	}

	{
		// infer null from missing event field
		const event_list = try Event.parse(t.allocator, "{\"id\": 2, \"system\": \"other\", \"active\": false, \"record\": 4}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, system, active, record, category from dataset_test where id =  2", .{})).?;
		defer row.deinit();

		try t.expectEqual(2, row.get(u64, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(2, row.get(u16, 2));
		try t.expectEqual("other", row.get([]const u8, 3));
		try t.expectEqual(false, row.get(bool, 4));
		try t.expectEqual(4, row.get(f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
	}

	{
		// makes a column nullable
		const event_list = try Event.parse(t.allocator, "{\"id\": null, \"system\": null, \"active\": null, \"record\": null}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, system, active, record, category from dataset_test where id is null", .{})).?;
		defer row.deinit();

		try t.expectEqual(3, row.get(u64, 0));
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
		const event_list = try Event.parse(t.allocator, "{\"id\": -1003843293448, \"system\": 43, \"active\": \"maybe\", \"record\": 229, \"category\": -2}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, system, active, record, category from dataset_test where id = -1003843293448", .{})).?;
		defer row.deinit();

		try t.expectEqual(4, row.get(u64, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(-1003843293448, row.get(i64, 2));
		try t.expectEqual("43", row.get([]const u8, 3));
		try t.expectEqual("maybe", row.get([]const u8, 4));
		try t.expectEqual(229, row.get(f64, 5));
		try t.expectEqual(-2, row.get(i8, 6));
	}
}

test "DataSet: record with list" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSetWithList(tc);

	{
		// history is added for another part of this test later
		const event_list = try Event.parse(t.allocator, "{\"id\": 1, \"tags\": [1, 9], \"history\": 9991}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, tags from dataset_list_test where id =  1", .{})).?;
		defer row.deinit();

		try t.expectEqual(1, row.get(u64, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(1, row.get(u16, 2));

		const list = row.list(u8, 3).?;
		try t.expectEqual(2, list.len);
		try t.expectEqual(1, list.get(0));
		try t.expectEqual(9, list.get(1));
	}

	{
		//expand int type (utinyint => integer)
		const event_list = try Event.parse(t.allocator, "{\"id\": 2, \"tags\": [-99384823, 200000, 0]}");
		ds.record(event_list);
		try ds.flushAppender();

		{
			var row = (try ds.conn.row("select tags from dataset_list_test where id =  2", .{})).?;
			defer row.deinit();

			const list = row.list(i32, 0).?;
			try t.expectEqual(3, list.len);
			try t.expectEqual(-99384823, list.get(0));
			try t.expectEqual(200000, list.get(1));
			try t.expectEqual(0, list.get(2));
		}

		{
			// check previous row
			var row = (try ds.conn.row("select tags from dataset_list_test where id =  1", .{})).?;
			defer row.deinit();

			const list = row.list(i32, 0).?;
			try t.expectEqual(2, list.len);
			try t.expectEqual(1, list.get(0));
			try t.expectEqual(9, list.get(1));
		}
	}

	{
		// alter list type to json
		const event_list = try Event.parse(t.allocator, "{\"id\": 3, \"tags\": [\"hello\"]}");
		ds.record(event_list);
		try ds.flushAppender();

		{
			var row = (try ds.conn.row("select tags from dataset_list_test where id =  3", .{})).?;
			defer row.deinit();
			try t.expectEqual("[\"hello\"]", row.get([]u8, 0));
		}

		{
			var row = (try ds.conn.row("select tags from dataset_list_test where id = 2", .{})).?;
			defer row.deinit();
			try t.expectEqual("[-99384823,200000,0]", row.get([]u8, 0));
		}

		{
			var row = (try ds.conn.row("select tags from dataset_list_test where id = 1", .{})).?;
			defer row.deinit();
			try t.expectEqual("[1,9]", row.get([]u8, 0));
		}
	}

	{
		// add a new list
		const event_list = try Event.parse(t.allocator, "{\"id\": 4, \"state\": [true, false]}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select state from dataset_list_test where id = 4", .{})).?;
		defer row.deinit();
		const list = row.list(bool, 0).?;
		try t.expectEqual(2, list.len);
		try t.expectEqual(true, list.get(0));
		try t.expectEqual(false, list.get(1));
	}

	{
		// convert scalar to list
		const event_list = try Event.parse(t.allocator, "{\"id\": 5, \"history\": [1.1, 0.9]}");
		ds.record(event_list);
		try ds.flushAppender();

		{
			var row = (try ds.conn.row("select history from dataset_list_test where id = 5", .{})).?;
			defer row.deinit();
			const list = row.list(f64, 0).?;
			try t.expectEqual(2, list.len);
			try t.expectEqual(1.1, list.get(0));
			try t.expectEqual(0.9, list.get(1));
		}

		{
			// check the original row too
			var row = (try ds.conn.row("select history from dataset_list_test where id = 1", .{})).?;
			defer row.deinit();
			const list = row.list(f64, 0).?;
			try t.expectEqual(1, list.len);
			try t.expectEqual(9991, list.get(0));
		}
	}

	{
		// insert scalar in json (which came from list)
		const event_list = try Event.parse(t.allocator, "{\"id\": 6, \"tags\": \"ouch\"}");
		ds.record(event_list);
		try ds.flushAppender();

		{
			var row = (try ds.conn.row("select tags from dataset_list_test where id = 6", .{})).?;
			defer row.deinit();
			try t.expectEqual("ouch", row.get([]u8, 0));
		}

		{
			var row = (try ds.conn.row("select tags from dataset_list_test where id = 1", .{})).?;
			defer row.deinit();
			try t.expectEqual("[1,9]", row.get([]u8, 0));
		}
	}

	{
		// insert scalar in list
		const event_list = try Event.parse(t.allocator, "{\"id\": 8, \"history\": 9.998}");
		ds.record(event_list);
		try ds.flushAppender();

		{
			var row = (try ds.conn.row("select history from dataset_list_test where id = 8", .{})).?;
			defer row.deinit();
			const list = row.list(f64, 0).?;
			try t.expectEqual(1, list.len);
			try t.expectEqual(9.998, list.get(0));
		}

		{
			var row = (try ds.conn.row("select history from dataset_list_test where id = 5", .{})).?;
			defer row.deinit();
			const list = row.list(f64, 0).?;
			try t.expectEqual(2, list.len);
			try t.expectEqual(1.1, list.get(0));
			try t.expectEqual(0.9, list.get(1));
		}
	}

	{
		// insert narrower in list
		const event_list = try Event.parse(t.allocator, "{\"id\": 9, \"history\": -331}");
		ds.record(event_list);
		try ds.flushAppender();

		{
			var row = (try ds.conn.row("select history from dataset_list_test where id = 9", .{})).?;
			defer row.deinit();
			const list = row.list(f64, 0).?;
			try t.expectEqual(1, list.len);
			try t.expectEqual(-331, list.get(0));
		}

		{
			var row = (try ds.conn.row("select history from dataset_list_test where id = 5", .{})).?;
			defer row.deinit();
			const list = row.list(f64, 0).?;
			try t.expectEqual(2, list.len);
			try t.expectEqual(1.1, list.get(0));
			try t.expectEqual(0.9, list.get(1));
		}
	}
}

test "DataSet: record add column" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSet(tc);

	{
		const event_list = try Event.parse(t.allocator, "{\"id\": 5, \"new\": true}");
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, system, active, record, category, new from dataset_test where id =  5", .{})).?;
		defer row.deinit();

		try t.expectEqual(1, row.get(u64, 0));
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
		tc.silenceLogs();
		// no other difference, except for 2 new columns
		const event_list = try Event.parse(t.allocator, \\ {
		\\    "id": 6,
		\\    "system": "catalog",
		\\    "active": true,
		\\    "record": 0.932,
		\\    "category": null,
		\\    "new": false,
		\\    "tag1": "ok",
		\\    "tag2": -9999,
		\\    "\"invalid\"": "will not get added"
		\\ }
		);
		ds.record(event_list);
		try ds.flushAppender();

		var row = (try ds.conn.row("select ldk_id, ldk_ts, id, system, active, record, category, new, tag1, tag2 from dataset_test where id =  6", .{})).?;
		defer row.deinit();

		try t.expectEqual(2, row.get(u64, 0));
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
		try t.expectEqual(.varchar, ds.columns.items[6].data_type);
		try t.expectEqual(true, ds.columnLookup.contains("tag1"));

		try t.expectEqual("tag2", ds.columns.items[7].name);
		try t.expectEqual(true, ds.columns.items[7].nullable);
		try t.expectEqual(false, ds.columns.items[7].is_list);
		try t.expectEqual(.smallint, ds.columns.items[7].data_type);
		try t.expectEqual(true, ds.columnLookup.contains("tag2"));

		try t.expectEqual(false, ds.columnLookup.contains("\"invalid\""));
	}
}

test "DataSet: autoflush after configured time" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.withScheduler();

	const ds = try testDataSet(tc);
	ds.max_unflushed_ms = 5;

	{
		const event_list = try Event.parse(t.allocator, "[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]");
		ds.record(event_list);
	}

	try t.expectEqual(0, (try tc.scalar(i64, "select count(*) from dataset_test", .{})));
	std.time.sleep(std.time.ns_per_ms * 100);
	try t.expectEqual(3, (try tc.scalar(i64, "select count(*) from dataset_test", .{})));
	try t.expectEqual(0, ds.unflushed);
}

test "DataSet: autoflush after configured size" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.withScheduler();

	const ds = try testDataSet(tc);
	ds.max_unflushed_count = 5;

	{
		const event_list = try Event.parse(t.allocator, "[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]");
		ds.record(event_list);
		try t.expectEqual(0, (try tc.scalar(i64, "select count(*) from dataset_test", .{})));
	}


	{
		const event_list = try Event.parse(t.allocator, "[{\"id\": 4}, {\"id\": 5}]");
		ds.record(event_list);
		try t.expectEqual(5, (try tc.scalar(i64, "select count(*) from dataset_test", .{})));
	}

	try t.expectEqual(0, ds.unflushed);
}

// This is one of those things. It's hard to create a DataSet since it requires
// a lot of setup. It needs a real table, since it prepares a statement. Tempting
// to think we can just fake create a dataset, ala, `return DataSet{....}`...but
// it's simpler and has better fidelity if we use the real APIs
fn testDataSet(tc: *t.Context) !*DataSet {
	const event_list = try Event.parse(t.allocator,
		\\ {
		\\    "id": 393,
		\\    "system": "catalog",
		\\    "active": true,
		\\    "record": 0.932,
		\\    "category": null
		\\ }
	);
	defer event_list.deinit();
	const actor_id = try tc.app.createDataSet(tc.env(), "dataset_test", event_list.events[0]);
	var ds = tc.app.dispatcher.unsafeInstance(DataSet, actor_id);
	ds.actor_id = actor_id;
	return ds;
}

fn testDataSetWithList(tc: *t.Context) !*DataSet {
	const event_list = try Event.parse(t.allocator,
		\\ {
		\\    "id": 393,
		\\    "tags": [1, 9],
		\\    "history": 7.2
		\\ }
	);
	defer event_list.deinit();
	const actor_id = try tc.app.createDataSet(tc.env(), "dataset_list_test", event_list.events[0]);
	return tc.app.dispatcher.unsafeInstance(DataSet, actor_id);
}
