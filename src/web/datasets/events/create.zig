const std = @import("std");
const httpz = @import("httpz");

const logdk = @import("../../../logdk.zig");
const web = logdk.web;
const Event = logdk.Event;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const app = env.app;
	const name = req.params.get("name").?;

	var dataset = app.datasets.get(name) orelse blk: {
		if (app.settings.dynamicDataSetCreation() == false) {
			return web.notFound(res, "dataset not found and dynamic creation is disabled");
		}
		break :blk null;
	};

	const event = Event.parse(res.arena, req.body() orelse "") catch return error.InvalidJson;
	defer event.deinit();

	if (dataset == null) {
		dataset = try app.createDataSet(env, name, event);
	}

	// dataset.?.record(event);
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
