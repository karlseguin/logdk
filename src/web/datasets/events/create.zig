const std = @import("std");
const httpz = @import("httpz");

const logdk = @import("../../../logdk.zig");
const web = logdk.web;
const Event = logdk.Event;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const app = env.app;
	const name = req.params.get("name").?;

	var arc = app.getDataSet(name) orelse blk: {
		if (app.settings.dynamicDataSetCreation() == false) {
			return web.notFound(res, "dataset not found and dynamic creation is disabled");
		}
		break :blk null;
	};

	defer if (arc) |arc_| arc_.release();

	const event_list = Event.parse(app.allocator, req.body() orelse "") catch return error.InvalidJson;
	if (event_list.events.len == 0) {
		event_list.deinit();
		res.status = 204;
		return;
	}

	// once passed to the dispatcher, it becomes the datasets job to release this
	errdefer event_list.deinit();

	if (arc == null) {
		const validator = try env.validator();
		logdk.Validate.validateIdentifier("dataset", name, validator) catch |err| {
			// Even though this is (most likely) user-input error, we want to log it
			// since creating datasets is fairly uncommon and intentional and the creator
			// would probably be surprised to find out that it failed.
			env.logger.level(.Warn).ctx("validation.dataset.name").string("name", name).log();
			return err;
		};
		arc = try app.createDataSet(env, name, event_list.events[0]);
	}
	app.dispatcher.send(logdk.DataSet, arc.?.value.actor_id, .{.record = event_list});
	res.status = 204;
}

const t = logdk.testing;
test "events.create: unknown dataset, dynamic creation disabled" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.app.settings._dynamic_dataset_creation = false;

	tc.web.param("name", "nope");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.expectNotFound("dataset not found and dynamic creation is disabled");
}

test "events.create: empty body" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "table_x");
	tc.web.body("{}");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try t.expectEqual(null, try tc.row("select * from duckdb_tables where table_name = 'table_x'", .{}));
}

test "events.create: invalid json body" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.web.param("name", "table_x");

	tc.web.body("{hi");
	try t.expectError(error.InvalidJson, handler(tc.env(), tc.web.req, tc.web.res));
}

test "events.create: unknown dataset, dynamic create invalid dataset name" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.silenceLogs();

	tc.web.param("name", "n\"0p3!");
	tc.web.body("{\"id\": 1}");
	try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
}

test "events.create: creates dataset and event" {
	const zuckdb = @import("zuckdb");

	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "logx_x");
	tc.web.json(.{
		.id = 552,
		.active = true,
		.category = "system",
		.tags = &[_]std.json.Value{.{.string = "teg"}, .{.float = 1.32}},
		.zrecord = &[_]std.json.Value{.{.integer = 394}, .{.float = 590.1}},
		.details = .{
			.over = 9000.1,
		},
		.date = "2024-10-30",
		.uuid = "d772b20f-6948-4b20-9485-369399e97068",
	});
	try handler(tc.env(), tc.web.req, tc.web.res);

	tc.flushMessages();

	const row = (try tc.row("select * from logx_x", .{})).?;
	defer row.deinit();

	try t.expectEqual(1, row.get(u64, 0));                                               // ldk_id
	try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);                 // ldk_ts
	try t.expectEqual(true, row.get(bool, 2));                                           // active
	try t.expectEqual("system", row.get([]const u8, 3));                                 // category
	try t.expectEqual(.{.year = 2024, .month = 10, .day = 30}, row.get(zuckdb.Date, 4));    // date
	try t.expectEqual("{\"over\":9.0001e3}", row.get([]const u8, 5));                    // details
	try t.expectEqual(552, row.get(u16, 6));                                             // id
	try t.expectEqual("[\"teg\",1.32e0]", row.get([]const u8, 7));                       // tags
	try t.expectEqual("d772b20f-6948-4b20-9485-369399e97068", &row.get(zuckdb.UUID, 8)); // uuid

	const list = row.list(f64, 9).?;                                                     // zrecord
	try t.expectEqual(2, list.len);
	try t.expectEqual(394, list.get(0));
	try t.expectEqual(590.1, list.get(1));
}
