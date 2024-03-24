const std = @import("std");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

const Env = logdk.Env;
const Event = logdk.Event;
const DataSet = logdk.DataSet;

const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const ValidatorPool = @import("validate").Pool;
const BufferPool = @import("zul").StringBuilder.Pool;

pub const App = struct {
	settings: Settings,
	allocator: Allocator,
	db: *zuckdb.Pool,
	create_lock: std.Thread.Mutex,
	buffers: *BufferPool,
	validators: ValidatorPool(void),

	// protects the dispatcher And the datasets lookup when adding datasets
	lock: Thread.RwLock,

	// We have a pseudo actor-model thing going on. DataSets can only be modified
	// or acted on from a single thread. This ensures that we can mutate the
	// dataset (e.g. add a column) while safely processing inserts. To achieve
	// this, all behavior is sent as Mesasges, to the DataSet, via the dispatcher.
	// This is a name => actor_id lookup, we use the actor_id to send a message
	// to the dataset via the dispatcher.
	dispatcher: DataSet.Dispatcher,
	datasets: std.StringHashMap(usize),

	pub fn init(allocator: Allocator, config: logdk.Config) !App{
		var open_err: ?[]u8 = null;
		const db = zuckdb.DB.initWithErr(allocator, config.db.path, .{.enable_external_access = false}, &open_err) catch |err| {
			if (err == error.OpenDB) {
				defer allocator.free(open_err.?);
				logz.err().ctx("App.init.db.open").string("err", open_err).string("path", config.db.path).log();
			}
			return err;
		};
		var db_pool = try db.pool(.{.size = config.db.pool_size, .timeout = config.db.pool_timeout});
		errdefer db_pool.deinit();

		{
			var conn = try db_pool.acquire();
			conn.release();
			try @import("migrations/migrations.zig").run(conn);
		}

		const dispatcher = try DataSet.Dispatcher.init(allocator, &db, config.workers);

		var validator_pool = try ValidatorPool(void).init(allocator, config.validator);
		errdefer validator_pool.deinit();

		// A pool of pre-generated []u8 for whatever we need
		var buffers = try BufferPool.init(allocator, 32, 8_192);
		errdefer buffers.deinit();

		return .{
			.create_lock = .{},
			.allocator = allocator,
			.validators = validator_pool,
			.settings = .{
				._dynamic_dataset_creation = false,
			},
			.lock = .{},
			.db = db_pool,
			.buffers = buffers,
			.dispatcher = dispatcher,
			.datasets = std.StringHashMap(usize).init(allocator),
		};
	}

	pub fn deinit(self: *App) void {
		// This is a bit ugly. The DataSets are somewhat intentionally, not easily
		// reachable. (because we really want to make sure any modifications are done
		// via the disaptcher to ensure single-threaded access). So we have this.
		var it = self.datasets.valueIterator();
		while (it.next()) |value| {
			actorToDataSet(value.*).deinit();
		}

		self.datasets.deinit();
		self.dispatcher.deinit();

		self.db.deinit();
		self.buffers.deinit();
		self.validators.deinit();
	}

	pub fn createDataSet(self: *App, env: *Env, name: []const u8, event: *const Event) !usize {
		const validator = try env.validator();
		try logdk.Validate.TableName("dataset", name, validator);

		var arena = ArenaAllocator.init(self.allocator);
		defer arena.deinit();

		const allocator = arena.allocator();

		{
			// We could prepare everything we need before taking this lock, like
			// building our columns, turning them into json, and writing our SQL strings.
			// But this lock is under very low contention, and if there IS contention,
			// it very well could be multiple threads trying to create the same dataset
			// so a long lock seems fine.

			self.create_lock.lock();
			defer self.create_lock.unlock();

			// under our create_lock, this check is definitive.
			if (self.datasets.get(name)) |q| {
				return q;
			}

			const columns = try DataSet.columnsFromEvent(allocator, event);
			for (columns) |c| {
				logdk.Validate.ColumnName(c.name, validator) catch {};
			}
			if (validator.isValid() == false) {
				return error.Validation;
			}

			const serialized_columns = try std.json.stringifyAlloc(allocator, columns, .{});

			var sequence = try self.buffers.acquire();
			defer sequence.release();
			try std.fmt.format(sequence.writer(), "create sequence {s}_id_seq start 1;", .{name});

			var create = try self.buffers.acquire();
			defer create.release();

			{
				try std.fmt.format(create.writer(),
					\\ create table {s} (
					\\  "$id" integer not null default(nextval('{s}_id_seq')),
					\\  "$inserted" timestamptz not null default(now())
				, .{name, name});

				const writer = create.writer();
				for (columns) |c| {
					try create.write(",\n  ");
					try c.writeDDL(writer);
				}
			}
			try create.write("\n)");

			{
				var conn = try self.db.acquire();
				defer conn.release();

				const insert_sql = "insert into logdk.datasets (name, columns, created) values ($1, $2, now())";
				try conn.begin();
				errdefer conn.rollback() catch {};
				_ = conn.exec(sequence.string(), .{}) catch |err| return env.dbErr("App.createDataSet.sequence", err, conn);
				_ = conn.exec(create.string(), .{}) catch |err| return env.dbErr("App.createDataSet.create", err, conn);
				_ = conn.exec(insert_sql, .{name, serialized_columns}) catch |err| return env.dbErr("App.createDataSet.insert", err, conn);
				conn.commit() catch |err| return env.dbErr("App.createDataSet.commit", err, conn);
			}
		}

		// This has to happen under our create_lock, else another thread can come in
		// and create the same dataset.
		// This could easily be done without hitting the DB. We have everything we need
		// right here to build the dataset. But, this should happen rarely, and
		// having a single path to load a dataset is more than worth it.
		return try self.loadDataSet(env, name);
	}

	fn loadDataSet(self: *App, env: *Env, name: []const u8) !usize {
		const result = blk: {
			var conn = try self.db.acquire();
			defer conn.release();

			var row = conn.row("select name, columns from logdk.datasets where name = $1", .{name}) catch |err| {
				return env.dbErr("app.loadDataSet", err, conn);
			} orelse return error.DataSetNotFound;
			defer row.deinit();

			break :blk try self.dispatcher.add(self.allocator, row);
		};

		self.lock.lock();
		defer self.lock.unlock();
		try self.datasets.put(result.name, result.actor_id);
		return result.actor_id;
	}

	pub fn loadDataSets(self: *App) !void {
		var conn = try self.db.acquire();
		defer conn.release();

		var rows = conn.query("select name, columns from logdk.datasets", .{}) catch |err| {
			return logdk.dbErr("App.loadDataSets", err, conn, logz.err());
		};
		defer rows.deinit();

		self.lock.lock();
		defer self.lock.unlock();

		while (try rows.next()) |row| {
			const result = try self.dispatcher.add(self.allocator, row);
			try self.datasets.put(result.name, result.actor_id);
		}

		logz.info().ctx("App.loadDataSets").int("count", self.datasets.count()).log();
	}

	// The app settings can be changed during runtime, so we need to encapsulate
	// all access in order to enforce thread-safety
	const Settings = struct {
		_dynamic_dataset_creation: bool,

		pub fn dynamicDataSetCreation(self: *const Settings) bool {
			return @atomicLoad(bool, &self._dynamic_dataset_creation, .monotonic);
		}
	};
};

// THIS SHOULD NOT BE CALLED.
// It's should only be used in `app.deinit`, and tests.
fn actorToDataSet(actor_id: usize) *DataSet {
	return @as(*DataSet.Dispatcher.Actor, @ptrFromInt(actor_id)).dataset;
}

const t = logdk.testing;
test "App: loadDataSets" {
	var tc = t.context(.{});
	defer tc.deinit();

	const columns = \\ [
		\\{"name": "id", "nullable": false, "is_list": false, "data_type": "integer"},
		\\{"name": "type", "nullable": false, "is_list": false, "data_type": "text"},
		\\{"name": "value", "nullable": true, "is_list": false, "data_type": "double"},
		\\{"name": "tags", "nullable": false, "is_list": true, "data_type": "text"}
	\\]
	;
	try tc.exec("insert into logdk.datasets (name, columns) values ($1, $2)", .{"system", columns});
	try tc.exec("create table system (id integer, type text, value double null, tags text[])", .{});

	var app = tc.app;
	try app.loadDataSets();

	const ds = actorToDataSet(app.datasets.get("system").?);
	try t.expectEqual("system", ds.name);
	try t.expectEqual(4, ds.columns.len);
	try t.expectEqual(.{.name = "id", .nullable = false, .is_list = false, .data_type = .integer}, ds.columns[0]);
	try t.expectEqual(.{.name = "type", .nullable = false, .is_list = false, .data_type = .text}, ds.columns[1]);
	try t.expectEqual(.{.name = "value", .nullable = true, .is_list = false, .data_type = .double}, ds.columns[2]);
	try t.expectEqual(.{.name = "tags", .nullable = false, .is_list = true, .data_type = .text}, ds.columns[3]);
}

test "App: createDataSet invalid dataset name" {
	var tc = t.context(.{});
	defer tc.deinit();

	{
		try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "", undefined));
		try tc.expectInvalid(.{.code = 1, .field = "dataset"});
	}

	{
		tc.reset();
		try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "a" ** 251, undefined));
		try tc.expectInvalid(.{.code = 5001, .field = "dataset"});
	}

	{
		tc.reset();
		try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "_hello", undefined));
		try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
	}

	{
		tc.reset();
		try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "1hello", undefined));
		try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
	}

	{
		tc.reset();
		try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "he-llo", undefined));
		try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
	}

	{
		tc.reset();
		try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "hello$", undefined));
		try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
	}
}

test "App: createDataSet invalid column names" {
	var tc = t.context(.{});
	defer tc.deinit();

	const long = "a" ** 251;
	var event = try Event.parse(t.allocator, "{\"\": 1, \"1a\": 2, \"_a\": 3, \".a\": 4, \"a$\": 5, \"a b\": 6, \"a[b]\": 7, \"" ++ long ++ "\": 8}");
	defer event.deinit();

	try t.expectError(error.Validation, tc.app.createDataSet(tc.env(), "ds", event));
	try tc.expectInvalid(.{.code = 1, .field = ""});
	try tc.expectInvalid(.{.code = 5000, .field = "1a"});
	try tc.expectInvalid(.{.code = 5000, .field = "_a"});
	try tc.expectInvalid(.{.code = 5000, .field = ".a"});
	try tc.expectInvalid(.{.code = 5000, .field = "a$"});
	try tc.expectInvalid(.{.code = 5000, .field = "a b"});
	try tc.expectInvalid(.{.code = 5000, .field = "a[b]"});
	try tc.expectInvalid(.{.code = 5001, .field = long});
}

test "App: createDataSet success" {
	var tc = t.context(.{});
	defer tc.deinit();

	var event = try Event.parse(t.allocator, "{\"id\": \"cx_312\", \"tags\": null, \"monitor\": false, \"flags\": [2, 2394, -3]}");
	defer event.deinit();

	{
		const actor_id = try tc.app.createDataSet(tc.env(), "metrics_1", event);
		try t.expectEqual(actor_id, tc.app.datasets.get("metrics_1").?);

		const ds = actorToDataSet(actor_id);
		try t.expectEqual("metrics_1", ds.name);

		try t.expectEqual(4, ds.columns.len);
		try t.expectEqual(.{.name = "id", .nullable = false, .is_list = false, .data_type = .text}, ds.columns[0]);
		try t.expectEqual(.{.name = "tags", .nullable = true, .is_list = false, .data_type = .unknown}, ds.columns[1]);
		try t.expectEqual(.{.name = "monitor", .nullable = false, .is_list = false, .data_type = .bool}, ds.columns[2]);
		try t.expectEqual(.{.name = "flags", .nullable = false, .is_list = true, .data_type = .integer}, ds.columns[3]);
	}

	try t.expectEqual(1, tc.scalar(i64, "select nextval('metrics_1_id_seq')", .{}));
	try t.expectEqual(2, tc.scalar(i64, "select nextval('metrics_1_id_seq')", .{}));

	var rows = try tc.query("describe metrics_1", .{});
	defer rows.deinit();
	{
		const row = (try rows.next()).?;
		try t.expectEqual("$id", row.get([]u8, 0));  // name
		try t.expectEqual("INTEGER", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual("nextval('metrics_1_id_seq')", row.get([]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("$inserted", row.get([]u8, 0));  // name
		try t.expectEqual("TIMESTAMP WITH TIME ZONE", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual("now()", row.get([]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("id", row.get([]u8, 0));  // name
		try t.expectEqual("VARCHAR", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("tags", row.get([]u8, 0));  // name
		try t.expectEqual("VARCHAR", row.get([]u8, 1));  // type
		try t.expectEqual("YES", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("monitor", row.get([]u8, 0));  // name
		try t.expectEqual("BOOLEAN", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("flags", row.get([]u8, 0));  // name
		try t.expectEqual("INTEGER[]", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	try t.expectEqual(null, try rows.next());
}
