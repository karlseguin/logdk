const std = @import("std");
const httpz = @import("httpz");

const logdk = @import("../../../logdk.zig");
const web = logdk.web;
const Event = logdk.Event;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const app = env.app;
	const name = req.params.get("name").?;

	var dataset_id = app.getDataSet(name) orelse blk: {
		if (app.settings.dynamicDataSetCreation() == false) {
			return web.notFound(res, "dataset not found and dynamic creation is disabled");
		}
		break :blk null;
	};

	const event = Event.parse(app.allocator, req.body() orelse "") catch return error.InvalidJson;
	if (event.fieldCount() == 0) {
		event.deinit();
		res.status = 204;
		return;
	}

	// once passed to the dispatcher, it because the datasets job to release this
	errdefer event.deinit();

	if (dataset_id == null) {
		dataset_id = try app.createDataSet(env, name, event);
	}

	app.dispatcher.send(logdk.DataSet, dataset_id.?, .{.record = event});
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

	tc.web.param("name", "n0p3!");
	tc.web.body("{\"id\": 1}");
	try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
}

test "events.create: unknown dataset, invalid column name" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "table_x");
	tc.web.body("{\"id$x\": 1}");
	try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = 5000, .field = "id$x"});
}

test "events.create: creates dataset and event" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "logx_x");
	tc.web.json(.{
		.id = 1,
		.active = true,
		.category = "system",
		.tags = &[_][]const u8{"teg", "leto"},
		.details = .{
			.over = 9000.1,
		}
	});
	try handler(tc.env(), tc.web.req, tc.web.res);
}
