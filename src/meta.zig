const std = @import("std");
const logdk = @import("logdk.zig");

const d = logdk.dispatcher;

const RwLock = std.Thread.RwLock;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

// Describes the system. Used in the GET /describe endpoint. In the scale of
// things, the system doesn't change often, so this data is pretty static. If
// we were to get this on-demand, we'd have to deal with all types of thread-safety
// issues. Instead, when a dataset changes, it sends a message to Meta (via
// our actor-ish dispatcher).
//
// There's still some thread-safety issues. _payload might get rebuilt while
// we're writing the response to the socket. So we use thread-safe reference
// counter. _payload is on the heap, and Meta holds 1 reference. Anyone who
// calls meta.payload() also holds a reference. When no longer in use, Meta and
// the callers need to call payload.release(). When the internal rc becomes 0
// the payload is freed. This allows Meta to swap out its reference without
// invalidating any other references out there.
pub const Meta = struct {
	_lock: RwLock,
	_payload: *Payload,
	_allocator: Allocator,
	_queue: *d.Queue(Meta),
	_datasets: std.ArrayListUnmanaged(DataSet),

	pub fn init(allocator: Allocator, queue: *d.Queue(Meta)) !Meta {
		const pl = try Payload.init(allocator, .{.datasets = &[_]DataSet{}});
		errdefer pl.release();

		return .{
			._lock = .{},
			._payload = pl,
			._queue = queue,
			._allocator = allocator,
			._datasets = std.ArrayListUnmanaged(DataSet){},
		};
	}

	pub fn deinit(self: *Meta) void {
		self._payload.release();
		for (self._datasets.items) |ds| {
			ds.deinit();
		}
		self._datasets.deinit(self._allocator);
	}

	// The mix of lock + atomic seems weird, but I believe it's correct/necessary.
	// The lock protects the self._payload reference, wheras the _rc protects the
	// payload itself. `payload._rc` is mutated in payload.release() which isn't
	// lock-protected.
	// So the lock here makes sure that get a payload instance, and increase its
	// rc, without another thread being able to swap out the version in between
	// those two operations. The atomic keeps `payload._rc` consistent across
	// multiple-threads which aren't all under lock.
	// I think.
	pub fn payload(self: *Meta) *Payload {
		self._lock.lockShared();
		const p = self._payload;
		_ = @atomicRmw(u16, &p._rc, .Add, 1, .monotonic);
		self._lock.unlockShared();
		return p;
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

		const pl = try Payload.init(allocator, .{.datasets = self._datasets.items});
		errdefer pl.release();


		self._lock.lock();
		defer self._lock.unlock();
		self._payload.release();
		self._payload = pl;
	}

	pub const Payload = struct {
		_rc: u16,
		json: []const u8,
		_allocator: Allocator,

		fn init(allocator: Allocator, describe: anytype) !*Payload {
			const json = try std.json.stringifyAlloc(allocator, describe, .{});
			errdefer allocator.free(json);

			const pl = try allocator.create(Payload);
			errdefer allocator.destroy(pl);

			pl.* = .{
				.json = json,
				._rc = 1,  // Meta's reference always counts as 1
				._allocator = allocator,
			};

			return pl;
		}

		pub fn release(self: *Payload) void {
			// returns the value before the sub, so if the value before the sub was 1,
			// then we have no more references to this payload
			if (@atomicRmw(u16, &self._rc, .Sub, 1, .monotonic) == 1) {
				self._allocator.free(self.json);
				self._allocator.destroy(self);
			}
		}
	};
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
