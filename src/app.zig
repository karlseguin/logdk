const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

const Env = logdk.Env;
const Meta = logdk.Meta;
const Event = logdk.Event;
const DataSet = logdk.DataSet;
const d = logdk.dispatcher;

const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

const ValidatorPool = @import("validate").Pool;
const BufferPool = @import("zul").StringBuilder.Pool;

pub const Queues = struct {
	dataset: d.Queues(DataSet),

	pub fn init(allocator: Allocator) !Queues {
		return .{
			.dataset = try d.createQueues(allocator, DataSet, 4),
		};
	}
};

const Scheduler = zul.Scheduler(logdk.Tasks, *App);

pub const App = struct {
	// Because of the actor-ish nature of DataSets, our meta holds a snapshot
	// of the current configuration.
	meta: Meta,

	// Pool of DuckDB Connections. Note that each DataSet worker (which handles
	// the inserts and any alter statement) has its own dedicate connetion that
	// lives outside of the pool.
	db: *zuckdb.Pool,

	allocator: Allocator,

	// We can only create 1 dataset at a time. This ensures we don't create the
	// same dataset (same name) from 2 concurrent requests.
	_create_lock: std.Thread.Mutex,

	// Thread-safe pool of pre-generated & growable []u8 for whatever we need.
	buffers: *BufferPool,

	validators: ValidatorPool(void),

	// We have a pseudo actor-model thing going on. DataSets can only be modified
	// or acted on from a single thread. This ensures that we can mutate the
	// dataset (e.g. add a column) while safely processing inserts. To achieve
	// this, all behavior is sent as Messages to the DataSet via the dispatcher.
	dispatcher: d.Dispatcher(Queues),

	// Ephemeral background task scheduler. All scheduled task for this scheduler
	// run in the scheduler thread. Currently only used to periodically flush the
	// dataset appender.
	scheduler: zul.Scheduler(logdk.Tasks, *App),

	// only used for shutting down
	_webserver: ?*logdk.web.Server = null,

	// must always be access via one of the getters/setters to synchronize access
	_settings: Settings,

	// serialize persisting the settings
	_settings_lock: Thread.Mutex,

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

		const settings = blk: {
			var conn = try db_pool.acquire();
			conn.release();
			try @import("migrations/migrations.zig").run(conn);

			const row = (try conn.row("select settings from logdk.settings where id = 1", .{})) orelse break :blk Settings{};
			defer row.deinit();

			const parsed = try std.json.parseFromSlice(Settings, allocator, row.get([]const u8, 0), .{});
			defer parsed.deinit();

			// Yes, we deinit parsed, but settings has no allocated fields.
			break :blk parsed.value;
		};

		var validator_pool = try ValidatorPool(void).init(allocator, config.validator);
		errdefer validator_pool.deinit();

		var buffers = try BufferPool.init(allocator, config.buffers.count, config.buffers.size);
		errdefer buffers.deinit();

		var dispatcher = try d.Dispatcher(Queues).init(allocator);
		errdefer dispatcher.deinit();

		const meta = try Meta.init(allocator);
		errdefer meta.value.deinit();

		var scheduler = Scheduler.init(allocator);
		errdefer scheduler.deinit();

		return .{
			.meta = meta,
			._create_lock = .{},
			._settings = settings,
			._settings_lock = .{},
			.allocator = allocator,
			.validators = validator_pool,
			.db = db_pool,
			.buffers = buffers,
			.scheduler = scheduler,
			.dispatcher = dispatcher,
		};
	}

	pub fn deinit(self: *App) void {
		self.scheduler.deinit();

		// No one really owns the logdk.DataSets, but we make Meta responsible
		// for cleaning them up since it knows the most about them.
		self.meta.shutdown(&self.dispatcher);
		self.meta.deinit();

		self.dispatcher.deinit();

		self.db.deinit();
		self.buffers.deinit();
		self.validators.deinit();
	}

	pub fn getDataSet(self: *App, name: []const u8) ?Meta.DataSetArc {
		return self.meta.getDataSet(name);
	}

	pub fn dataSetExists(self: *App, name: []const u8) bool {
		return self.meta.dataSetExists(name);
	}

	pub fn createDataSet(self: *App, env: *Env, name: []const u8, event: Event) !Meta.DataSetArc {
		var arena = ArenaAllocator.init(self.allocator);
		defer arena.deinit();

		const aa = arena.allocator();

		// We could prepare everything we need before taking this lock, like
		// building our columns, turning them into json, and writing our SQL strings.
		// But this lock is under very low contention, and if there IS contention,
		// it very well could be multiple threads trying to create the same dataset
		// so a coarse lock seems fine.
		self._create_lock.lock();
		defer self._create_lock.unlock();

		// under our create_lock, this check is definitive.
		if (self.getDataSet(name)) |ds| {
			return ds;
		}

		const columns = try DataSet.columnsFromEvent(aa, event, env.logger);
		const serialized_columns = try std.json.stringifyAlloc(aa, columns, .{});

		if (columns.len < event.fieldCount()) {
			// columnsFromEvent added the invalid column names to this logger already, without
			// actually logging it
			env.logger.level(.Warn).ctx("validation.column.name").string("dataset", name).log();
		}

		var create = try self.buffers.acquire();
		defer create.release();
		const writer = create.writer();

		try std.fmt.format(writer,
			\\ create table {s} (
			\\  ldk_id ubigint not null primary key,
			\\  ldk_ts timestamp not null
		, .{name});

		for (columns) |c| {
			try create.write(",\n  ");
			try c.writeDDL(writer);
		}
		try create.write("\n)");

		const insert_sql =
			\\ insert into logdk.datasets (name, columns, created)
			\\ values ($1, $2, now())
			\\ returning name, columns
		;

		var conn = try self.db.acquire();
		defer conn.release();

		try conn.begin();
		errdefer conn.rollback() catch {};

		_ = conn.exec(create.string(), .{}) catch |err| return env.dbErr("App.createDataSet.create", err, conn);
		var row = conn.row(insert_sql, .{name, serialized_columns}) catch |err| return env.dbErr("App.createDataSet.insert", err, conn);
		defer row.?.deinit();

		conn.commit() catch |err| return env.dbErr("App.createDataSet.commit", err, conn);

		logdk.metrics.addDataSet();
		try self.loadDataSet(row.?);
		return self.getDataSet(name) orelse unreachable;
	}

	pub fn loadDataSets(self: *App) !void {
		var conn = try self.db.acquire();
		defer conn.release();

		var rows = conn.query("select name, columns from logdk.datasets", .{}) catch |err| {
			return logdk.dbErr("App.loadDataSets", err, conn, logz.err());
		};
		defer rows.deinit();

		var count: usize = 0;
		while (try rows.next()) |row| {
			try self.loadDataSet(row);
			count += 1;
		}
		logz.info().ctx("App.loadDataSets").int("count", count).log();
	}

	// row could be an zuckdb.Row or a zuckdb.OwningRow
	fn loadDataSet(self: *App, row: anytype) !void {
		var dataset = try DataSet.init(self, row);
		errdefer dataset.deinit();

		const actor_id = try self.dispatcher.add(dataset);
		dataset.actor_id = actor_id;

		// this dataset is going to move (and be owned by our actor), but this
		// is safe because meta.datasetChanged clones all the data it needs for its
		// own meta copy
		try self.meta.datasetChanged(&dataset);
	}

	pub fn allowDataSetCreation(self: *App) bool {
		return self.readSetting(bool, "allow_dataset_creation");
	}
	pub fn setDataSetCreation(self: *App, value: bool) !void {
		return self.writeSetting(bool, "allow_dataset_creation", value);
	}

	pub fn isSingleUser(self: *App) bool {
		return self.readSetting(bool, "single_user");
	}
	pub fn setSingleUser(self: *App, value: bool) !void {
		return self.writeSetting(bool, "single_user", value);
	}

	fn readSetting(self: *App, comptime T: type, comptime name: []const u8) T {
		return @atomicLoad(T, &@field(self._settings, name), .monotonic);
	}

	// Thread safety around _settings is weird. We only use our lock to synchronize writes
	// (i.e. this function). When reading a value, we use an @atomicLoad. However, in this
	// function, when we serialize _settings (which obviously has to read each value) we
	// don't use an @atomicLoad. What gives?
	// We need an @atomicLoad in `isSingleUser` (for example), because another thread
	// could be calling `writeSetting` at the same time and issuing an @atomicStore.
	// However, once a thread is in `writeSetting` and has the lock, no other thread
	// could be writing to the settings. Other threads might be reading a value, but
	// as far as I know, it's fine for some threads to use @atomicLoad while other
	// threads just read the value directly - as long as no thread is writing, which
	// our lock guarantees.
	fn writeSetting(self: *App, comptime T: type, comptime name: []const u8, value: T) !void {
		self._settings_lock.lock();
		defer self._settings_lock.unlock();

		@atomicStore(T, &@field(self._settings, name), value, .monotonic);

		var buf = try self.buffers.acquire();
		defer buf.release();

		try std.json.stringify(self._settings, .{}, buf.writer());

		var conn = try self.db.acquire();
		defer conn.release();

		const affected = try conn.exec(
			\\ insert into logdk.settings (id, settings) values (1, $1)
			\\ on conflict do update set settings = $1
		, .{buf.string()});

		std.debug.assert(affected == 1);
	}

	const Settings = struct {
		single_user: bool = true,
		allow_dataset_creation: bool = true,
	};
};

const t = logdk.testing;
test "App: loadDataSets" {
	var tc = t.context(.{});
	defer tc.deinit();

	const columns = \\ [
		\\{"name": "id", "nullable": false, "is_list": false, "data_type": "integer"},
		\\{"name": "tags", "nullable": false, "is_list": true, "data_type": "varchar"},
		\\{"name": "type", "nullable": false, "is_list": false, "data_type": "varchar", "parsed": false},
		\\{"name": "value", "nullable": true, "is_list": false, "data_type": "double", "parsed": true}
	\\]
	;
	try tc.exec("insert into logdk.datasets (name, columns) values ($1, $2)", .{"system", columns});
	try tc.exec("create table system (ldk_id ubigint not null primary key, ldk_ts timestamp not null, id integer, type text, value double null, tags text[])", .{});

	var app = tc.app;
	try app.loadDataSets();

	{
		// assert our "real" logdk.DataSet
		const ds = tc.unsafeDataSet("system");
		try t.expectEqual("system", ds.name);
		try t.expectEqual(4, ds.columns.items.len);
		try t.expectEqual(.{.name = "id", .nullable = false, .is_list = false, .data_type = .integer, .parsed = false}, ds.columns.items[0]);
		try t.expectEqual(.{.name = "tags", .nullable = false, .is_list = true, .data_type = .varchar, .parsed = false}, ds.columns.items[1]);
		try t.expectEqual(.{.name = "type", .nullable = false, .is_list = false, .data_type = .varchar, .parsed = false}, ds.columns.items[2]);
		try t.expectEqual(.{.name = "value", .nullable = true, .is_list = false, .data_type = .double, .parsed = true}, ds.columns.items[3]);
	}

	{
		// assert our "not real??" meta.DataSet
		const arc = app.getDataSet("system").?;
		defer arc.release();

		const ds = arc.value;
		try t.expectEqual("system", ds.name);
		try t.expectEqual(4, ds.columns.len);
		try t.expectEqual(.{.name = "id", .nullable = false, .is_list = false, .data_type = "integer", .parsed = false}, ds.columns[0]);
		try t.expectEqual(.{.name = "tags", .nullable = false, .is_list = true, .data_type = "varchar", .parsed = false}, ds.columns[1]);
		try t.expectEqual(.{.name = "type", .nullable = false, .is_list = false, .data_type = "varchar", .parsed = false}, ds.columns[2]);
		try t.expectEqual(.{.name = "value", .nullable = true, .is_list = false, .data_type = "double", .parsed = true}, ds.columns[3]);

		try t.expectEqual(1, ds.parsed_fields.len);
		try t.expectEqual("value", ds.parsed_fields[0]);
	}
}

test "App: createDataSet success" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.silenceLogs();

	// sanity check
	try t.expectEqual(false, tc.app.dataSetExists("metrics_1"));

	// the invalid field names are ignored
	var event_list = try Event.parse(t.allocator, "{\"id\": \"cx_312\", \"tags\": null, \"monitor\": false, \"flags\": [2, 2394, -3], \"inv\\\"alid\": 8, \"\": 9, \"value\": \"1234\", \"at\": \"2024-05-16T08:57:33Z\"}");
	defer event_list.deinit();

	{
		const arc = try tc.app.createDataSet(tc.env(), "metrics_1", event_list.events[0]);
		defer arc.release();
		try t.expectEqual(true, tc.app.dataSetExists("metrics_1"));

		const ds = tc.app.dispatcher.unsafeInstance(DataSet, arc.value.actor_id);
		try t.expectEqual("metrics_1", ds.name);

		try t.expectEqual(6, ds.columns.items.len);

		try t.expectEqual(.{.name = "at", .nullable = false, .is_list = false, .data_type = .timestamptz, .parsed = true}, ds.columns.items[0]);
		try t.expectEqual(.{.name = "flags", .nullable = false, .is_list = true, .data_type = .integer, .parsed = false}, ds.columns.items[1]);
		try t.expectEqual(.{.name = "id", .nullable = false, .is_list = false, .data_type = .varchar, .parsed = false}, ds.columns.items[2]);
		try t.expectEqual(.{.name = "monitor", .nullable = false, .is_list = false, .data_type = .bool, .parsed = false}, ds.columns.items[3]);
		try t.expectEqual(.{.name = "tags", .nullable = true, .is_list = false, .data_type = .unknown, .parsed = false}, ds.columns.items[4]);
		try t.expectEqual(.{.name = "value", .nullable = false, .is_list = false, .data_type = .usmallint, .parsed = true}, ds.columns.items[5]);
	}

	var rows = try tc.query("describe metrics_1", .{});
	defer rows.deinit();
	{
		const row = (try rows.next()).?;
		try t.expectEqual("ldk_id", row.get([]u8, 0));  // name
		try t.expectEqual("UBIGINT", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual("PRI", row.get([]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("ldk_ts", row.get([]u8, 0));  // name
		try t.expectEqual("TIMESTAMP", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	{
		const row = (try rows.next()).?;
		try t.expectEqual("at", row.get([]u8, 0));  // name
		try t.expectEqual("TIMESTAMP WITH TIME ZONE", row.get([]u8, 1));  // type
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
		try t.expectEqual("monitor", row.get([]u8, 0));  // name
		try t.expectEqual("BOOLEAN", row.get([]u8, 1));  // type
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
		try t.expectEqual("value", row.get([]u8, 0));  // name
		try t.expectEqual("USMALLINT", row.get([]u8, 1));  // type
		try t.expectEqual("NO", row.get([]u8, 2));  // nullable
		try t.expectEqual(null, row.get(?[]u8, 3));  // key
		try t.expectEqual(null, row.get(?[]u8, 4));  // default
		try t.expectEqual(null, row.get(?[]u8, 5));  // extra
	}

	try t.expectEqual(null, try rows.next());
}

test "App: settings" {
	var tc = t.context(.{});
	defer tc.deinit();

	// defaults
	try t.expectEqual(true, tc.app.isSingleUser());
	try t.expectEqual(true, tc.app.allowDataSetCreation());

	{
		try tc.app.setSingleUser(false);
		try t.expectEqual(false, tc.app.isSingleUser());
		try t.expectEqual(true, tc.app.allowDataSetCreation());
		try t.expectEqual(false, try tc.scalar(bool, "select (settings->'single_user')::bool from logdk.settings where id = 1", .{}));
	}

	{
		try tc.app.setDataSetCreation(false);
		try t.expectEqual(false, tc.app.isSingleUser());
		try t.expectEqual(false, tc.app.allowDataSetCreation());
		try t.expectEqual(false, try tc.scalar(bool, "select (settings->'allow_dataset_creation')::bool from logdk.settings where id = 1", .{}));
	}
}
