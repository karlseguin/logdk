const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const httpz = @import("httpz");
const logdk = @import("logdk.zig");

const d = logdk.dispatcher;

const RwLock = std.Thread.RwLock;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

// Describes the system. Used in the GET /describe and GET /info endpoints.
//
// For Describe, in the scale of things, the system doesn't change often, so
// this data is pretty static. If we were to get this on-demand, we'd have to
// deal with all types of thread-safety issues. Instead, when a dataset changes,
// it sends a message to Meta (via our actor-ish dispatcher).
//
// For Info, the data changes a bit more (e.g. size of the database), so we load
// the info, track the timestamp, and if we try getInfo() again and the timestamp
// is old, we refresh it. It's a bit like a cache, EXCEPT, the old info is always
// available, so if updating info fails, we can always return the old one. B
//
// There's still some thread-safety issues. describe or info might get rebuilt while
// we're writing the response to the socket. So we use thread-safe reference
// counter. This is solved by keeping the values on the heap, behind an atomic
// reference counter (arc). Only when the arc reaches 0 do we free the memory.
pub const Meta = struct {
	_describe: Payload,
	_describe_lock: RwLock,
	_info: Payload,
	_info_lock: RwLock,
	_info_expires: i64,
	_allocator: Allocator,
	_queue: *d.Queue(Meta),
	_datasets: std.ArrayListUnmanaged(DataSet),

	pub const Payload = *ArenaArc([]const u8);

	pub fn init(allocator: Allocator, queue: *d.Queue(Meta)) !Meta {
		const info = try ArenaArc([]const u8).init(allocator, 1);
		info.value = "{}"; //load this later
		errdefer info.release();

		const describe = try loadDescribe(allocator, .{.datasets = &[_]DataSet{}});
		errdefer describe.release();

		return .{
			._queue = queue,
			._info = info,
			._info_lock = .{},
			._info_expires = 0,
			._describe = describe,
			._describe_lock = .{},
			._allocator = allocator,
			._datasets = std.ArrayListUnmanaged(DataSet){},
		};
	}

	pub fn deinit(self: *Meta) void {
		self._info.release();
		self._describe.release();
		for (self._datasets.items) |ds| {
			ds.deinit();
		}
		self._datasets.deinit(self._allocator);
	}

	// The mix of lock + atomic seems weird, but I believe it's correct/necessary.
	// The lock protects the self._describe reference, wheras the _rc protects the
	// describe itself. `arc._rc` is mutated in arc.release() which isn't
	// lock-protected.
	// So the lock here makes sure that we can get an arc instance, and increase its
	// rc, without another thread being able to swap out the version in between
	// those two operations. The atomic keeps `arc._rc` consistent across
	// multiple-threads which aren't all under lock.
	// I think.
	pub fn getDescribe(self: *Meta) Payload {
		self._describe_lock.lockShared();
		defer self._describe_lock.unlockShared();
		self._describe.acquire();
		return self._describe;
	}

	pub fn getInfo(self: *Meta, app: *logdk.App) Payload {
		const now = std.time.timestamp();
		self._info_lock.lockShared();
		defer self._info_lock.unlockShared();
		if (self._info_expires < now) blk: {
			const info = loadInfo(self._allocator, app) catch |err| {
				// if this fails (unlikely), we can return the previous stored info
				// we'll extend the expiry a little to avoid to avoid flooding the
				// system with this error
				self._info_expires = now + 5;
				logz.err().err(err).ctx("Meta.loadInfo").log();
				break :blk;
			};
			self._info.release();
			self._info = info;
		}
		self._info.acquire();
		return self._info;
	}

	// This is being called from the DataSet's worker thread, so we can
	// safely access the dataset here and now. We'll create our meta version
	// of the dataset.
	pub fn datasetChanged(self: *Meta, dataset: *logdk.DataSet) !void {
		var arena = ArenaAllocator.init(self._allocator);
		errdefer arena.deinit();
		const allocator = arena.allocator();

		var columns = try allocator.alloc(Column, dataset.columns.items.len);
		for (dataset.columns.items, 0..) |c, i| {
			columns[i] = .{
				.name = try allocator.dupe(u8, c.name),
				.is_list = c.is_list,
				.nullable = c.nullable,
				.data_type = @tagName(c.data_type),
			};
		}

		// Now we want to tranfer to Meta's own worker thread. We largely want to
		// do this because we want to limit how much time we spend blocking the
		// dataset thread. We still need to regenerate the entire payload based
		// on this new/changed dataset, and there's no reason to do that while
		// blocking hte worker.
		self._queue.send(self, .{
			.dataset = .{
				.arena = arena,
				.name = try allocator.dupe(u8, dataset.name),
				.columns = columns,
			}
		});
	}

	pub const Message = union(enum) {
		dataset: DataSet,
	};

	pub fn handle(self: *Meta, message: Message) !void {
		switch (message) {
			.dataset => |dataset| return self.updateDataSet(dataset),
		}
	}

	fn updateDataSet(self: *Meta, dataset: DataSet) !void {
		const allocator = self._allocator;

		blk: {
			for (self._datasets.items) |*ds| {
				if (std.mem.eql(u8, ds.name, dataset.name)) {
					ds.deinit();
					ds.* = dataset;
					break :blk;
				}
			}
			try self._datasets.append(allocator, dataset);
		}

		const describe = try loadDescribe(allocator, .{.datasets = self._datasets.items});
		errdefer describe.release();

		self._describe_lock.lock();
		defer self._describe_lock.unlock();
		self._describe.release();
		self._describe = describe;
	}

	fn loadDescribe(allocator: Allocator, data: anytype) !Payload {
		const arc = try ArenaArc([]const u8).init(allocator, 1);
		errdefer arc.release();

		const json = try std.json.stringifyAlloc(arc._arena.allocator(), data, .{});
		arc.value = json;
		return arc;
	}

	fn loadInfo(allocator: Allocator, app: *logdk.App) !Payload {
		var conn = try app.db.acquire();
		defer conn.release();

		const arc = try ArenaArc([]const u8).init(allocator, 1);
		errdefer arc.release();

		const aa = arc._arena.allocator();

		const duckdb_version = blk: {
			const row = conn.row("select to_json(t) from pragma_version() t", .{}) catch |err| {
				return logdk.dbErr("Meta.info.version", err, conn, logz.err());
			} orelse return error.PragmaNoRow;
			defer row.deinit();
			break :blk try aa.dupe(u8, row.get([]const u8, 0));
		};

		const duckdb_size = blk: {
			const row = conn.row("select to_json(t) from pragma_database_size() t", .{}) catch |err| {
				return logdk.dbErr("Meta.info.version", err, conn, logz.err());
			} orelse return error.PragmaNoRow;
			defer row.deinit();
			break :blk try aa.dupe(u8, row.get([]const u8, 0));
		};

		const json = try std.json.stringifyAlloc(aa, .{
			.logdk = .{
				.version = logdk.version,
				.httpz_blocking = httpz.blockingMode(),
			},
			.duckdb = .{
				.size = zul.jsonString(duckdb_size),
				.version = zul.jsonString(duckdb_version),
			},
		}, .{});

		arc.value = json;
		return arc;
	}
};

fn ArenaArc(comptime T: type) type {
	return struct {
		value: T,
		_rc: u16,
		_arena: ArenaAllocator,

		const Self = @This();

		pub fn init(allocator: Allocator, initial: u16) !*Self {
			var arena = ArenaAllocator.init(allocator);
			errdefer arena.deinit();

			const arc = try arena.allocator().create(Self);

			arc.* = .{
				._rc = initial,
				._arena = arena,
				.value = undefined,
			};

			return arc;
		}

		pub fn acquire(self: *Self) void {
			_ = @atomicRmw(u16, &self._rc, .Add, 1, .monotonic);
		}

		pub fn release(self: *Self) void {
			// returns the value before the sub, so if the value before the sub was 1,
			// then we have no more references to object
			if (@atomicRmw(u16, &self._rc, .Sub, 1, .monotonic) == 1) {
				self._arena.deinit();
			}
		}
	};
}

// a "meta" representation of a DataSet. This owns all its fields, since
// references to the real dataset are not thread-safe
const DataSet = struct {
	arena: ArenaAllocator,
	name: []const u8,
	columns: []Column,

	pub fn deinit(self: *const DataSet) void {
		self.arena.deinit();
	}

	pub fn jsonStringify(self: *const DataSet, jws: anytype) !void {
		// necessary, else it'll try (and fail) to stringify the arena
		try jws.beginObject();
		try jws.objectField("name");
		try jws.write(self.name);
		try jws.objectField("columns");
		try jws.write(self.columns);
		try jws.endObject();
	}
};

const Column = struct {
	name: []const u8,
	nullable: bool,
	is_list: bool,
	data_type: []const u8,
};
