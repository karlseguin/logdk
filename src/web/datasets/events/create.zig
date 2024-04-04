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

	// once passed to the dispatcher, it because the datasets job to release this
	const event = Event.parse(app.allocator, req.body() orelse "") catch return error.InvalidJson;
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

	tc.web.param("name", "nope");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.expectNotFound("dataset not found and dynamic creation is disabled");
}

test "events.create: unknown dataset, dynamic create invalid name" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "nope");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.expectNotFound("dataset not found and dynamic creation is disabled");
}


// test "events.create: empty body" {
// 	var tc = t.context(.{});
// 	defer tc.deinit();
// 	try t.expectError(error.InvalidJson, handler(tc.env(), tc.web.req, tc.web.res));
// }

// test "events.create: invalid json body" {
// 	var tc = t.context(.{});
// 	defer tc.deinit();

// 	tc.web.body("{hi");
// 	try t.expectError(error.InvalidJson, handler(tc.env(), tc.web.req, tc.web.res));
// }
