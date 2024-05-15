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
	_info: zul.LockRefArenaArc(Info),
	_describe: zul.LockRefArenaArc([]const u8),
	_allocator: Allocator,
	_queue: *d.Queue(Meta),
	_datasets: std.ArrayListUnmanaged(DataSet),

	pub const InfoValue = *zul.LockRefArenaArc(Info).Value;
	pub const DescribeValue = *zul.LockRefArenaArc([]const u8).Value;

	pub fn init(allocator: Allocator, queue: *d.Queue(Meta)) !Meta {
		var ic = try zul.LockRefArenaArc(Info).create(allocator);
		errdefer ic.deinit();
		// default value, because expires is 0, it'll get reloaded on demand on the first request
		ic.value_ptr.* = .{.json = "{}", .expires = 0};

		var dc = try zul.LockRefArenaArc([]const u8).create(allocator);
		errdefer dc.deinit();
		dc.value_ptr.* = try loadDescribe(dc.arena, .{.datasets = &[_]DataSet{}});

		return .{
			._queue = queue,
			._info = ic.ref,
			._describe = dc.ref,
			._allocator = allocator,
			._datasets = std.ArrayListUnmanaged(DataSet){},
		};
	}

	pub fn deinit(self: *Meta) void {
		self._info.deinit();
		self._describe.deinit();

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
	pub fn getDescribe(self: *Meta) DescribeValue {
		return self._describe.acquire();
	}

	pub fn getInfo(self: *Meta, app: *logdk.App) InfoValue {
		const arc = self._info.acquire();
		const info = arc.value;

		const now = std.time.timestamp();
		if (info.expires > now) {
			return arc;
		}

		var new = self._info.new() catch |err| {
			logz.err().err(err).ctx("Meta.getInfo.create").log();
			return arc;
		};

		new.value_ptr.* = loadInfo(new.arena, app) catch |err| {
			new.deinit();
			logz.err().err(err).ctx("Meta.loadInfo").log();
			return arc; // return the previous value
		};

		// release the old one, now that we have a new
		arc.release();

		self._info.swap(new);
		return new.acquire();
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

		var new = try self._describe.new();
		errdefer new.deinit();

		new.value_ptr.* = try loadDescribe(new.arena, .{.datasets = self._datasets.items});
		self._describe.swap(new);
	}

	fn loadDescribe(allocator: Allocator, data: anytype) ![]const u8 {
		return try std.json.stringifyAlloc(allocator, data, .{});
	}

	fn loadInfo(allocator: Allocator, app: *logdk.App) !Info {
		var conn = try app.db.acquire();
		defer conn.release();

		const duckdb_version = blk: {
			const row = conn.row("select to_json(t) from pragma_version() t", .{}) catch |err| {
				return logdk.dbErr("Meta.info.version", err, conn, logz.err());
			} orelse return error.PragmaNoRow;
			defer row.deinit();
			break :blk try allocator.dupe(u8, row.get([]const u8, 0));
		};

		const duckdb_size = blk: {
			const row = conn.row("select to_json(t) from pragma_database_size() t", .{}) catch |err| {
				return logdk.dbErr("Meta.info.version", err, conn, logz.err());
			} orelse return error.PragmaNoRow;
			defer row.deinit();
			break :blk try allocator.dupe(u8, row.get([]const u8, 0));
		};

		const json = try std.json.stringifyAlloc(allocator, .{
			.logdk = .{
				.version = logdk.version,
				.httpz_blocking = httpz.blockingMode(),
			},
			.duckdb = .{
				.size = zul.jsonString(duckdb_size),
				.version = zul.jsonString(duckdb_version),
			},
		}, .{});

		return .{
			.json = json,
			.expires = std.time.timestamp() + 10,
		};
	}
};

const Info = struct {
	json: []const u8,
	expires: i64,
};

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
