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
	name: []const u8,
	conn: *zuckdb.Conn,
	insert_one: zuckdb.Stmt,
	arena: *ArenaAllocator,
	columns: []Column,

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
		var columns = try allocator.alloc(Column, event.fields.len);
		errdefer allocator.free(columns);

		for (event.fields, event.values, 0..) |field, value, i| {
			const event_type = std.meta.activeTag(value);
			const column_type = switch (value) {
				.list => |list| columnTypeForEventList(list),
				else => columnTypeFromEventScalar(event_type),
			};

			columns[i] = .{
				.name = field,
				.is_list = event_type == .list,
				.nullable = event_type == .null,
				.data_type = column_type,
			};
		}

		return columns;
	}

	fn record(self: *DataSet, event: *Event) !void {
		defer event.deinit();

		const insert = &self.insert_one;
		try insert.clearBindings();

		var dataset_changed = false;
		for (self.columns, 0..) |column, param_index| {
			const value = event.get(column.name) orelse Event.Value{.null = {}};
			switch (value) {
				.list => unreachable, // TODO
				.null => {
					if (column.nullable == false) {
						dataset_changed = true;
					} else {
						try insert.bindValue(param_index, null);
					}
				},
				inline else => |scalar| {
					const ct = compatibleDataType(column.data_type, value);
					if (ct.target == column.data_type) {
						try insert.bindValue(param_index, scalar);
					} else {
						dataset_changed = true;
					}
				},
			}
		}

		if (dataset_changed == false) {
			const inserted = try insert.exec();
			std.debug.assert(inserted == 1);
			return;
		}
	}
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
		candidate = compatibleDataType(candidate, value).target;
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
fn compatibleDataType(column_type: Column.DataType, value: Event.Value) TypeCompatibilityResult {
	switch (column_type) {
		.bool => switch (value) {
			.bool => return .{.target = .bool},
			.json => return .{.target = .json},
			.null, .list => unreachable,
			else => return .{.target = .text},
		},
		.tinyint => switch (value) {
			.tinyint => return .{.target = .tinyint},
			.smallint => return .{.target = .smallint},
			.integer => return .{.target = .integer},
			.bigint => return .{.target = .bigint},
			.double => return .{.target = .double},
			.utinyint => |v| return if (v <= 127) .{.target = .tinyint} else .{.target = .smallint},
			.usmallint => |v| return if (v <= 32767) .{.target = .smallint} else .{.target = .integer},
			.uinteger => |v| return if (v <= 2147483647) .{.target = .integer} else .{.target = .bigint},
			.ubigint => |v| return if (v <= 9223372036854775807) .{.target = .bigint} else .{.target = .text},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.utinyint => switch (value) {
			.utinyint => return .{.target = .utinyint},
			.usmallint => return .{.target = .usmallint},
			.uinteger => return .{.target = .uinteger},
			.ubigint => return .{.target = .ubigint},
			.double => return .{.target = .double},
			.tinyint, .smallint => return .{.target = .smallint},
			.integer => return .{.target = .integer},
			.bigint => return .{.target = .bigint},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.smallint => switch (value) {
			.utinyint, .tinyint, .smallint => return .{.target = .smallint},
			.integer => return .{.target = .integer},
			.bigint => return .{.target = .bigint},
			.double => return .{.target = .double},
			.usmallint => |v| return if (v <= 32767) .{.target = .smallint} else .{.target = .integer},
			.uinteger => |v| return if (v <= 2147483647) .{.target = .integer} else .{.target = .bigint},
			.ubigint => |v| return if (v <= 9223372036854775807) .{.target = .bigint} else .{.target = .text},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.usmallint => switch (value) {
			.utinyint, .usmallint => return .{.target = .usmallint},
			.uinteger => return .{.target = .uinteger},
			.ubigint => return .{.target = .ubigint},
			.double => return .{.target = .double},
			.tinyint, .smallint, .integer => return .{.target = .integer},
			.bigint => return .{.target = .bigint},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.integer => switch (value) {
			.utinyint, .tinyint, .smallint, .usmallint, .integer => return .{.target = .integer},
			.bigint => return .{.target = .bigint},
			.double => return .{.target = .double},
			.uinteger => |v| return if (v <= 2147483647) .{.target = .integer} else .{.target = .bigint},
			.ubigint => |v| return if (v <= 9223372036854775807) .{.target = .bigint} else .{.target = .text},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.uinteger => switch (value) {
			.utinyint, .usmallint, .uinteger => return .{.target = .uinteger},
			.ubigint => return .{.target = .ubigint},
			.double => return .{.target = .double},
			.tinyint, .smallint, .integer, .bigint => return .{.target = .bigint},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.bigint => switch (value) {
			.utinyint, .tinyint, .smallint, .usmallint, .integer, .uinteger, .bigint => return .{.target = .bigint},
			.double => return .{.target = .double},
			.ubigint => |v| return if (v <= 9223372036854775807) .{.target = .bigint} else .{.target = .text},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.ubigint => switch (value) {
			.utinyint, .usmallint, .uinteger, .ubigint => return .{.target = .ubigint},
			.double => return .{.target = .double},
			.tinyint, .smallint, .integer, .bigint => return .{.target = .bigint},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.double => switch (value) {
			.tinyint, .utinyint, .smallint, .usmallint, .integer, .uinteger, .bigint, .ubigint, .double => return .{.target = .double},
			.text, .bool => return .{.target = .text},
			.json => return .{.target = .json},
			.null, .list => unreachable,
		},
		.text => return .{.target = .text},
		.json => return .{.target = .json},
		.unknown => switch (value) {
			.tinyint => return .{.target = .tinyint},
			.utinyint => return .{.target = .utinyint},
			.smallint => return .{.target = .smallint},
			.usmallint => return .{.target = .usmallint},
			.integer => return .{.target = .integer},
			.uinteger => return .{.target = .uinteger},
			.bigint => return .{.target = .bigint},
			.ubigint => return .{.target = .ubigint},
			.double => return .{.target = .double},
			.text => return .{.target = .text},
			.bool => return .{.target = .bool},
			.json => return .{.target = .json},
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
					ds.record(event) catch |err| logdk.dbErr("DataSet.record.event", err, ds.conn, logz.err().string("name", ds.name)) catch {};
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
	const insert_one = blk: {
		var sb = zul.StringBuilder.init(allocator);
		defer sb.deinit();
		try generateInsertSQL(&sb, name, columns);
		break :blk conn.prepare(sb.string(), .{.auto_release = false}) catch |err| {
			return logdk.dbErr("dataSetFromRow", err, conn, logz.err().string("sql", sb.string()));
		};
	};

	const dataset = try allocator.create(DataSet);
	dataset.* = .{
		.name = name,
		.conn = conn,
		.arena = arena,
		.columns = columns,
		.insert_one = insert_one,
	};
	return dataset;
}

fn generateInsertSQL(sb: *zul.StringBuilder, name: []const u8, columns: []Column) !void {
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
	return columnTypeForEventList(event.values[0].list);
}

test "DataSet: columnsFromEvent" {
	const columns = try DataSet.columnsFromEvent(t.allocator, &.{
		._arena = undefined,
		.fields = &[_][]const u8{"id", "name", "details", "l1", "l2"},
		.values = &[_]Event.Value{
			.{.ubigint = 99999},
			.{.text = "teg"},
			.{.json =  "{\"handle \": 9001}"},
			.{.list = &[_]Event.Value{.{.utinyint = 1}, .{.smallint = -9000}, .{.integer = 293000}}},
			.{.list = &[_]Event.Value{.{.bool = true}, .{.integer = 123}}},
		}
	});
	defer t.allocator.free(columns);

	try t.expectEqual(5, columns.len);
	try t.expectEqual(.{.name = "id", .is_list = false, .nullable = false, .data_type = .ubigint}, columns[0]);
	try t.expectEqual(.{.name = "name", .is_list = false, .nullable = false, .data_type = .text}, columns[1]);
	try t.expectEqual(.{.name = "details", .is_list = false, .nullable = false, .data_type = .json}, columns[2]);
}

test "DataSet: record simple" {
	var tc = t.context(.{});
	defer tc.deinit();

	const ds = try testDataSet(tc);

	{
		const event = try Event.parse(t.allocator, "{\"id\": 1, \"system\": \"catalog\", \"active\": true, \"record\": 0.932, \"category\": null}");
		try ds.record(event);

		var row = (try ds.conn.row("select * from dataset_test where id =  1", .{})).?;
		defer row.deinit();

		// $id column
		try t.expectEqual(1, row.get(i32, 0));
		// $inserted
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

		var row = (try ds.conn.row("select * from dataset_test where id =  2", .{})).?;
		defer row.deinit();

		// $id column
		try t.expectEqual(2, row.get(i32, 0));
		// $inserted
		try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);

		try t.expectEqual(2, row.get(u16, 2));
		try t.expectEqual("other", row.get([]const u8, 3));
		try t.expectEqual(false, row.get(bool, 4));
		try t.expectEqual(4, row.get(f64, 5));
		try t.expectEqual(null, row.get(?[]u8, 6));
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
