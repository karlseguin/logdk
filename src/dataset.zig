const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

const Event = logdk.Event;

const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const module = @This();

pub const DataSet = struct {
	arena: *ArenaAllocator,
	name: []const u8,
	columns: []Column,
	conn: *zuckdb.Conn,
	insert_one: zuckdb.Stmt,
	scrap: []u8,

	pub const Dispatcher = module.Dispatcher;

	pub fn deinit(self: *DataSet) void {
		self.insert_one.deinit();

		const arena = self.arena;
		const allocator = arena.child_allocator;
		arena.deinit();
		allocator.destroy(arena);
		allocator.destroy(self);
	}

	pub fn columnsFromEvent(allocator: Allocator, event: *const Event) ![]Column {
		var columns = try allocator.alloc(Column, event.fieldCount());
		errdefer allocator.free(columns);

		var i: usize = 0;
		var it = event.map.iterator();
		while (it.next()) |kv| {
			const value = kv.value_ptr.*;
			const event_type = std.meta.activeTag(value);
			const column_type = switch (value) {
				.list => |list| columnTypeForEventList(list),
				else => columnTypeFromEventScalar(event_type),
			};

			columns[i] = .{
				.name = kv.key_ptr.*,
				.is_list = event_type == .list,
				.nullable = event_type == .null,
				.data_type = column_type,
			};
			i += 1;
		}

		return columns;
	}

	fn record(self: *DataSet, event: *Event, retry: bool) !void {
		defer if (retry == false) event.deinit();

		const insert = &self.insert_one;
		try insert.clearBindings();

		var mutator = Mutator{.dataset = self};
		errdefer if (mutator.mutated) mutator.rollback();

		for (self.columns, 0..) |*column, param_index| {
			const value = event.get(column.name) orelse Event.Value{.null = {}};
			switch (value) {
				.list => unreachable, // TODO
				.null => {
					if (column.nullable == false) {
						std.debug.assert(retry == false);
						try mutator.nullable(column);
					} else {
						try insert.bindValue(param_index, null);
					}
				},
				inline else => |scalar| {
					const target_type = compatibleDataType(column.data_type, value);
					if (target_type == column.data_type) {
						try insert.bindValue(param_index, scalar);
					} else {
						std.debug.assert(retry == false);
						try mutator.alterType(column, target_type);
					}
				},
			}
		}

		if (mutator.mutated == false) {
			const inserted = try insert.exec();
			std.debug.assert(inserted == 1);
			return;
		}

		if (retry) {
			unreachable;
		}

		try mutator.finalize();

		// We should be able to record this event now. We could slightly optimize
		// this, because at this point the event should be insertable without any
		// mutation. But it's easier to call this recursively.
		return self.record(event, true);
	}

	const Mutator = struct {
		mutated: bool = false,
		dataset: *DataSet,

		fn nullable(self: *Mutator, column: *Column) !void {
			try self.begin();
			const sql = try self.print("alter table \"{s}\" alter column \"{s}\" drop not null", .{self.dataset.name, column.name});
			_ = try self.exec(sql, .{});
			column.nullable = true;
		}

		fn alterType(self: *Mutator, column: *Column, target_type: Column.DataType) !void {
			try self.begin();
			std.debug.assert(target_type != .unknown);
			const sql = try self.print("alter table \"{s}\" alter column \"{s}\" set type {s}", .{self.dataset.name, column.name, @tagName(target_type)});
			_ = try self.exec(sql, .{});
			column.data_type = target_type;
		}

		fn begin(self: *Mutator) !void {
			if (self.mutated) {
				// mutation was already started from a previous field, nothing to do
				return;
			}

			self.mutated = true;
			_ = try self.exec("begin", .{});
		}

		fn print(self: *Mutator, comptime format: []const u8, args: anytype) ![]const u8 {
			return std.fmt.bufPrint(self.dataset.scrap, format, args) catch |err| {
				logz.err().ctx("Mutator.print").string("name", self.dataset.name).string("format", format).err(err).log();
				return err;
			};
		}

		fn exec(self: *Mutator, sql: []const u8, args: anytype) !usize {
			const dataset = self.dataset;
			return dataset.conn.exec(sql, args) catch |err| {
				return logdk.dbErr("Mutator.exec", err, dataset.conn, logz.err().string("sql", sql));
			};
		}

		fn rollback(self: *Mutator) void {
			std.debug.assert(self.mutated);
			_ = self.exec("rollback", .{}) catch {};
		}

		// This function is a bit much. First, the above methods mutated the dataset's columns.
		// These changes now need to be saved in logdk.datasets. Second, we need to commit
		// all the DDLs we did, as well as the logdk.datasets update. Finally, we have
		// to destroy our existing prepared statement and re-create it (this might not be
		// needed for all mutations, I'm not sure, but we do it all the time for now).s
		fn finalize(self: *Mutator) !void {
			std.debug.assert(self.mutated);

			var dataset = self.dataset;

			// This is bad. This is our app.allocator GPA, but what an awful way to get it
			const allocator = dataset.arena.child_allocator;
			const serialized_columns = try std.json.stringifyAlloc(allocator, dataset.columns, .{});
			defer allocator.free(serialized_columns);

			const n = try self.exec("update logdk.datasets set columns = $2 where name = $1", .{dataset.name, serialized_columns});
			std.debug.assert(n == 1);

			_ = try self.exec("commit", .{});

			const insert_one = try generateInsertOnePrepared(allocator, dataset.conn, dataset.name, dataset.columns);

			// safe to delete our existing one now
			dataset.insert_one.deinit();

			dataset.insert_one = insert_one;
		}
	};
};

const Column = struct {
	name: []const u8,
	nullable: bool,
	is_list: bool,
	data_type: DataType,

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
};

fn columnTypeFromEventScalar(event_type: Event.DataType) Column.DataType {
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

fn columnTypeForEventList(list: []const Event.Value) Column.DataType {
	if (list.len == 0) return .text;
	const first = list[0];
	var candidate = columnTypeFromEventScalar(std.meta.activeTag(first));
	for (list[1..]) |value| {
		candidate = compatibleDataType(candidate, value);
	}
	return candidate;
}

const TypeCompatibilityResult = struct {
	target: Column.DataType,
};

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
fn compatibleDataType(column_type: Column.DataType, value: Event.Value) Column.DataType {
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

// We have N worker threads handling M datasets. For every worker, we have a
// Queue and a zuckdb.Conn. Messages for a dataset are always processed on
// the same thread, which makes thread-safety a non-issue. A dataset doesn't
// "own" a Queue, but it is associated with its Worker's Queue.
// The zuckdb.Conn is important for a few reasons. First, we avoid contention
// on the zuckdb.Pool. But, more importantly, it allows us to easily manage
// prepared statements - because, again, 1 worker owns 1 conn, which means
// a dataset always gets the same conn.
pub const Dispatcher = struct {
	queues: []Queue,
	threads: []Thread,
	arena: ArenaAllocator,
	conns: []zuckdb.Conn,

	// the index @mod(next, queues.len) to add the next dataset to.
	next: usize,

	pub const Actor = struct {
		queue: *Queue,
		dataset: *DataSet,
	};

	pub fn init(allocator: Allocator, db: *const zuckdb.DB, worker_count: usize) !Dispatcher {
		var arena = ArenaAllocator.init(allocator);
		errdefer arena.deinit();

		const aa = arena.allocator();

		const count = @max(worker_count, 1);
		var conns = try aa.alloc(zuckdb.Conn, count);
		var queues = try aa.alloc(Queue, count);
		var threads = try aa.alloc(Thread, count);

		var started: usize = 0;
		errdefer for (0..started) |i| {
			queues[i].enqueue(.{.dataset = null, .task = .{.stop = {}}});
			threads[i].join();
			conns[i].deinit();
		};

		for (queues, threads, conns) |*queue, *thread, *conn| {
			conn.* = try db.conn();
			errdefer conn.deinit();

			queue.* = try Queue.init(aa, 1000);
			thread.* = try Thread.spawn(.{}, Dispatcher.workerLoop, .{queue});
			started += 1;
		}

		logz.info().ctx("Dispatcher.init").int("workers", count).log();

		return .{
			.next = 0,
			.conns = conns,
			.arena = arena,
			.queues = queues,
			.threads = threads,
		};
	}

	pub fn deinit(self: *Dispatcher) void {
		for (self.queues, self.threads, self.conns) |*queue, thread, *conn| {
			queue.enqueue(.{.dataset = null, .task = .{.stop = {}}});
			thread.join();
			conn.deinit();
		}
		self.arena.deinit();
	}

	const AddResult = struct {
		name: []const u8,
		actor_id: usize,
	};

	pub fn add(self: *Dispatcher, allocator: Allocator, row: anytype) !AddResult {
		const next = self.next;
		const index = @mod(next, self.queues.len);

		const dataset = try dataSetFromRow(allocator, row, &self.conns[index]);
		errdefer dataset.deinit();

		const actor = try self.arena.allocator().create(Actor);
		actor.* = .{
			.dataset = dataset,
			.queue = &self.queues[index],
		};

		self.next = next + 1;
		return .{
			.name = dataset.name,
			.actor_id = @intFromPtr(actor),
		};
	}

	pub fn send(_: *Dispatcher, actor_id: usize, task: Message.Task) void {
		const actor: *Actor = @ptrFromInt(actor_id);
		actor.queue.enqueue(.{
			.task = task,
			.dataset = actor.dataset,
		});
	}

	fn workerLoop(queue: *Queue) void {
		while (true) {
			for (queue.next()) |msg| {
				switch (msg.task) {
					.stop => return,
					.event => |event| {
						var ds = msg.dataset.?;
						ds.record(event, false) catch |err| logdk.dbErr("DataSet.record.event", err, ds.conn, logz.err().string("name", ds.name)) catch {};
					}
				}
			}
		}
	}
};

// row could be either a *zuckdb.Row or a *zuckdb.OwningRow
fn dataSetFromRow(allocator: Allocator, row: anytype, conn: *zuckdb.Conn) !*DataSet {
	const arena = try allocator.create(ArenaAllocator);
	errdefer allocator.destroy(arena);

	arena.* = ArenaAllocator.init(allocator);
	errdefer arena.deinit();

	const aa = arena.allocator();

	const name = try aa.dupe(u8, row.get([]const u8, 0));

	const columns = try std.json.parseFromSliceLeaky([]Column, aa, row.get([]const u8, 1), .{});
	const insert_one = try generateInsertOnePrepared(allocator, conn, name, columns);

	const dataset = try allocator.create(DataSet);
	dataset.* = .{
		.name = name,
		.conn = conn,
		.arena = arena,
		.columns = columns,
		.insert_one = insert_one,
		.scrap = try aa.alloc(u8, logdk.MAX_IDENTIFIER_LEN * 2 + 512), // more than enough for an alter table T alter column C ...
	};
	return dataset;
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

const Message = struct {
	task: Task,
	dataset: ?*DataSet,

	const Task = union(enum) {
		stop: void,
		event: *Event,
	};
};

const Queue = struct {
	// Where pending messages are stored. Acts as a circular buffer.
	messages: []Message,

	// Index in `messages` where we'll enqueue a new messages (see enqueue function)
	push: usize,

	// Index in `messages` where we'll read to ge the next pendin messages (see next function)
	pull: usize,

	// The number of pending messages we have
	pending: usize,

	// messages.len - 1. When `push` or `pull` hit this, they need to go back to 0
	queue_end: usize,

	// protects messages
	mutex: Thread.Mutex,

	// signals the consumer that there are messages waiting
	cond: Thread.Condition,

	// limits the producers. This sem permits (messages.len - pending) messages from being
	// queued. When `messages` is full, producers block.
	sem: Thread.Semaphore,

	fn init(allocator: Allocator, len: usize) !Queue {
		// we expect allcator to be an arena!
		const messages = try allocator.alloc(Message, len);
		errdefer allocator.free(messages);

		return .{
			.pull = 0,
			.push = 0,
			.pending = 0,
			.cond = .{},
			.mutex = .{},
			.messages = messages,
			.queue_end = len - 1,
			.sem = .{.permits = len}
		};
	}
	// This is only ever called by a single thread (the same thread each time)
	// Essentially the "consumer"
	fn next(self: *Queue) []Message {
		// pull is only ever written to from this thread
		const pull = self.pull;

		while (true) {
			self.mutex.lock();
			while (self.pending == 0) {
				self.cond.wait(&self.mutex);
			}
			const push = self.push;
			const end = if (push > pull) push else self.messages.len;
			const messages = self.messages[pull..end];

			self.pull = if (end == self.messages.len) 0 else push;
			self.pending -= messages.len;
			self.mutex.unlock();
			var sem = &self.sem;
			sem.mutex.lock();
			defer sem.mutex.unlock();
			sem.permits += messages.len;
			sem.cond.signal();
			return messages;
		}
	}

	// This can be called by multiple threads, the "producers"
	fn enqueue(self: *Queue, message: Message) void {
		self.sem.wait();
		self.mutex.lock();
		const push = self.push;
		self.messages[push] = message;
		self.push = if (push == self.queue_end) 0 else push + 1;
		self.pending += 1;
		self.mutex.unlock();
		self.cond.signal();
	}
};

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

fn testColumnTypeEventList(comptime event_value: []const u8) Column.DataType {
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
	try t.expectEqual(.{.name = "l2", .is_list = false, .nullable = false, .data_type = .json}, columns[0]);
	try t.expectEqual(.{.name = "id", .is_list = false, .nullable = false, .data_type = .uinteger}, columns[1]);
	try t.expectEqual(.{.name = "name", .is_list = false, .nullable = false, .data_type = .text}, columns[2]);
	try t.expectEqual(.{.name = "l1", .is_list = true, .nullable = false, .data_type = .integer}, columns[3]);
	try t.expectEqual(.{.name = "details.handle", .is_list = false, .nullable = false, .data_type = .usmallint}, columns[4]);
}

test "DataSet: record" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSet(tc);

	{
		const event = try Event.parse(t.allocator, "{\"id\": 1, \"system\": \"catalog\", \"active\": true, \"record\": 0.932, \"category\": null}");
		try ds.record(event, false);

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
		try ds.record(event, false);

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
		try ds.record(event, false);

		var row = (try ds.conn.row("select \"$id\", \"$inserted\", id, system, active, record, category from dataset_test where id is null", .{})).?;
		defer row.deinit();

		try t.expectEqual(3, row.get(i32, 0));
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);
		try t.expectEqual(null, row.get(?u16, 2));
		try t.expectEqual(null, row.get(?[]const u8, 3));
		try t.expectEqual(null, row.get(?bool, 4));
		try t.expectEqual(null, row.get(?f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));

		for (ds.columns) |c| {
			try t.expectEqual(true, c.nullable);
		}
	}

	{
		// alter type
		const event = try Event.parse(t.allocator, "{\"id\": -1003843293448, \"system\": 43, \"active\": \"maybe\", \"record\": 229, \"category\": -2}");
		try ds.record(event, false);

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
	return @as(*Dispatcher.Actor, @ptrFromInt(actor_id)).dataset;
}
