const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const httpz = @import("httpz");
const logdk = @import("logdk.zig");

const M = @This();

const d = logdk.dispatcher;

const RwLock = std.Thread.RwLock;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

// Describes the system. logdk.DataSet is an important part of this system, but
// its record() method is also on the hot path, and it's pretty complicated.
// A challenge for this system is that a logdk.DataSet can change, but how do
// we handle such changes concurrently, without over-complicated and slowing
// the record() hot path? Ideally, logdk.DataSet would be lock-free, doing its
// own thing and unconcerned with what the rest of the code is doing.
//
// And what's what we do.
//
// We have 2 versions of a "DataSet". The first is is logdk.DataSet,
// it's authoratative and its main purpose is to insert events, and mutate
// the underlying table (and itself) whenever columns need changing. logdk.DataSet
// is a bit like an "Actor", almost all of its code runs on a single thread and
// the only way to iteract with it is by sending messages via the disaptcher.
//
// But, various HTTP endpoints need details about the datasets and using [async]
// messages through the dispatcher to get this information isn't only slow, it
// puts a burden on logdk.DataSet. Hence we have "Meta" which has its own DataSet.
// the DataSet that you'll find in this file is almost purely data (no behavior).
// Each meta DataSet is a deep copy of a logdk.DataSet, and it's thread safe. It
// might be a few seconds out of date, but that's ok.
//
// We use something like a Mutex<Arc<DataSet>> to protect and share Meta's information.
pub const Meta = struct {
	_allocator: Allocator,

	_info: zul.LockRefArenaArc(Info),
	_describe: zul.LockRefArenaArc(Describe),

	_datasets_lock: RwLock,
	_datasets: std.StringHashMapUnmanaged(zul.LockRefArenaArc(DataSet)),

	pub const InfoArc = *zul.LockRefArenaArc(Info).Arc;
	pub const DataSetArc = *zul.LockRefArenaArc(DataSet).Arc;
	pub const DescribeArc = *zul.LockRefArenaArc(Describe).Arc;

	pub fn init(allocator: Allocator) !Meta {
		// with expired set to 0, this is reloaded on the first call to getInfo
		var info = try zul.LockRefArenaArc(Info).initWithValue(allocator, .{.json = "{}", .expires = 0});
		errdefer info.deinit();

		// with stale set to true, this will get reloaded on the first call to getDescribe
		var describe = try zul.LockRefArenaArc(Describe).initWithValue(allocator, .{
			.stale = true,
			.json = "{}",
			.gzip = &[_]u8{31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 171, 174, 5, 0, 67, 191, 166, 163, 2, 0, 0, 0}, //awful
		});
		errdefer describe.deinit();

		const datasets = std.StringHashMapUnmanaged(zul.LockRefArenaArc(DataSet)){};
		return .{
			._info = info,
			._describe = describe,
			._allocator = allocator,
			._datasets_lock = .{},
			._datasets = datasets,
		};
	}

	pub fn deinit(self: *Meta) void {
		// Shutdown is normally be called before deinit. The only time this isn't
		// true is during a startup failure, where no dataset has been loaded,
		// and no events can possibly need flushing (because the web endpoints
		// aren't event up).
		self._info.deinit();
		self._describe.deinit();
		self._datasets.deinit(self._allocator);
	}

	// The logdk.DataSets aren't owned by anything. They're just floating
	// on the heap. Someone has to be responsible for cleaning them up. Ideally
	// that would be the logdk.App, but it's a lot easier to do here because
	// this is where we have all the actor_ids we need to flush and then
	// deinint them.
	pub fn shutdown(self: *Meta, dispatcher: anytype) void {
		{
			// Because we're under lock, it's safe to access the arc values directly
			self._datasets_lock.lock();
			defer self._datasets_lock.unlock();

			// First, send a flush to each dataset
			var it = self._datasets.valueIterator();
			while (it.next()) |ds| {
				dispatcher.send(logdk.DataSet, ds.arc.value.actor_id, .{.flush = {}});
			}
		}

		// Next, stop the dispatcher, this waits until all message are processed
		dispatcher.stop();

		{
			// Finally cleanup all the datsets
			var it = self._datasets.valueIterator();
			while (it.next()) |ds| {
				dispatcher.unsafeInstance(logdk.DataSet, ds.arc.value.actor_id).deinit();
				ds.deinit();
			}
		}
	}

	pub fn getDataSet(self: *Meta, name: []const u8) ?DataSetArc {
		self._datasets_lock.lockShared();
		defer self._datasets_lock.unlockShared();
		var arc = self._datasets.get(name) orelse return null;
		return arc.acquire();
	}

	pub fn dataSetExists(self: *Meta, name: []const u8) bool {
		self._datasets_lock.lockShared();
		defer self._datasets_lock.unlockShared();
		return self._datasets.contains(name);
	}

	// Returns an ARC-wrapped Info. This allows us to update _info
	// without invalidating the reference that we just handed out.
	// Unlike describe which is updated whenever a dataset changes or is added
	// info gets updated periodically (because things like DB size change
	// periodically). getInfo checks if its time to update the reference and, if
	// so, does just that.
	pub fn getInfo(self: *Meta, app: *logdk.App) InfoArc {
		const arc = self._info.acquire();
		const info = arc.value;

		const now = std.time.timestamp();
		if (info.expires > now) {
			return arc;
		}

		// our current info has expired. We'll try to generate a new one, but
		// if that fails, we can always return the expired one.s
		self._info.setValue(.{app}) catch |err| {
			logz.err().err(err).ctx("Meta.getInfo").log();
			return arc; // return the previous value
		};

		// we no longer need the old value
		arc.release();
		return self._info.acquire();
	}

	pub fn getDescribe(self: *Meta, app: *logdk.App) DescribeArc {
		const arc = self._describe.acquire();
		if (@atomicLoad(bool, &arc.value.stale, .monotonic) == false) {
			return arc;
		}

		// our describe is stale, regenerate!
		self._datasets_lock.lockShared();
		defer self._datasets_lock.unlockShared();
		self._describe.setValue(.{self._datasets, app}) catch |err| {
			logz.err().err(err).ctx("Meta.getDescribe").log();
			return arc; // return the previous value
		};

		// we no longer need the old value
		arc.release();
		return self._describe.acquire();
	}

	// When dataset is new, then this is being called	This is being called from the DataSet's worker thread, so we can
	// safely access the dataset here and now. We'll create our meta version
	// of the dataset.
	pub fn datasetChanged(self: *Meta, dataset: *logdk.DataSet) !void {
		// we want to create the arc here, because we want its arena to clone
		// the dataset.
		{
			const allocator = self._allocator;

			self._datasets_lock.lock();
			defer self._datasets_lock.unlock();

			const gop = try self._datasets.getOrPut(allocator, dataset.name);
			if (gop.found_existing == false) {
				gop.value_ptr.* = try zul.LockRefArenaArc(DataSet).init(allocator, .{dataset});
			} else {
				try gop.value_ptr.setValue(.{dataset});
			}

			// not strictly necessary since out logdk.DataSet.name should be immutable
			// but better safe than sorry
			const arc = gop.value_ptr.acquire();
			gop.key_ptr.* = arc.value.name;
			arc.release();
		}

		const describe = self._describe.acquire();
		defer describe.release();
		@atomicStore(bool, &describe.value.stale, true, .monotonic);
	}

	// a "meta" representation of a DataSet. This owns all its fields, since
	// references to the real dataset are not thread-safe
	pub const DataSet = struct {
		name: []const u8,
		columns: []Column,
		actor_id: usize,

		// A list of the fields who were parsed. Of course, all fields are parsed
		// from the raw JSON, but here we mean a text field which we parsed into
		// a number, bool, date, uuid, etc. For the first event of a new dataset,
		// of a newly added column, we'll try to parse every string field. But
		// for subsequent events, we only need to parse those string fields which
		// were previously successfully parsed (I think this is a pretty important
		// optimization if we're ingesting a large number of events).
		parsed_fields: [][]const u8,

		// allocator is an Arena, we can be sloppy
		pub fn init(allocator: Allocator, dataset: *logdk.DataSet) !DataSet {
			var columns = try allocator.alloc(Column, dataset.columns.items.len);

			var parsed_count: usize = 0;
			for (dataset.columns.items, 0..) |c, i| {
				if (c.parsed) {
					parsed_count += 1;
				}

				columns[i] = .{
					.parsed = c.parsed,
					.name = try allocator.dupe(u8, c.name),
					.is_list = c.is_list,
					.nullable = c.nullable,
					.data_type = @tagName(c.data_type),
				};
			}

			var parsed_index: usize = 0;
			const parsed_fields = try allocator.alloc([]const u8, parsed_count);
			for (columns) |c| {
				if (c.parsed) {
					parsed_fields[parsed_index] = c.name;
					parsed_index += 1;
				}
			}

			return .{
				.name = try allocator.dupe(u8, dataset.name),
				.parsed_fields = parsed_fields,
				.actor_id = dataset.actor_id,
				.columns = columns,
			};
		}

		pub fn jsonStringify(self: *const DataSet, jws: anytype) !void {
			// stupid, but don't want to serialize the actor_id
			try jws.beginObject();
			try jws.objectField("name");
			try jws.write(self.name);
			try jws.objectField("columns");
			try jws.write(self.columns);
			try jws.endObject();
		}
	};
};

const Column = struct {
	name: []const u8,
	nullable: bool,
	is_list: bool,
	parsed: bool,
	data_type: []const u8,
};

const Describe = struct {
	stale: bool,
	json: []const u8,
	gzip: []const u8,

	// when this is called, meta._datasets_lock is under a read-lock.
	// allocator is an arena that we aren't responsible for, so we can be sloppy.
	pub fn init(allocator: Allocator, datasets: std.StringHashMapUnmanaged(zul.LockRefArenaArc(Meta.DataSet)), app: *logdk.App) !Describe {
		// Zig doesn't have a good way to serialize a StringHashMap
		// So we're doing all of this by hand.
		var arr = std.ArrayList(u8).init(allocator);
		var writer = arr.writer();
		try writer.writeAll("{\"datasets\": [");

		var it = datasets.valueIterator();
		if (it.next()) |first| {
			try std.json.stringify(first.arc.value, .{}, writer);
			while (it.next()) |ref| {
				try writer.writeByte(',');
				try std.json.stringify(ref.arc.value, .{}, writer);
			}
		}
		try writer.writeAll("],\"settings\":{");
		try writer.writeAll("\"single_user\":");
		try writer.writeAll(if (app.isSingleUser()) "true" else "false");
		try writer.writeAll("}}");

		var compressed = std.ArrayList(u8).init(allocator);
		var fbs = std.io.fixedBufferStream(arr.items);
		try std.compress.gzip.compress(fbs.reader(), compressed.writer(), .{.level = .best});

		return .{
			.stale = false,
			.json = arr.items,
			.gzip = compressed.items,
		};
	}
};

// A pre-serialized payload of the system information (mosty versions and stuff)
// Expires indicates the unix timestamp in seconds, where a new info should be
// generated.
// allocator is an arena that we aren't responsible for, so we can be sloppy.
const Info = struct {
	json: []const u8,
	expires: i64,

	pub fn init(allocator: Allocator, app: *logdk.App) !Info {
		var conn = try app.db.acquire();
		defer conn.release();

		const duckdb_version = blk: {
			const row = conn.row("select to_json(t) from pragma_version() t", .{}) catch |err| {
				return logdk.dbErr("Meta.Info.version", err, conn, logz.err());
			} orelse return error.PragmaNoRow;
			defer row.deinit();
			break :blk try allocator.dupe(u8, row.get([]const u8, 0));
		};

		const duckdb_size = blk: {
			const row = conn.row("select to_json(t) from pragma_database_size() t", .{}) catch |err| {
				return logdk.dbErr("Meta.Info.database_size", err, conn, logz.err());
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
