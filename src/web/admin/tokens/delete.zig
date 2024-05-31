const std = @import("std");
const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const logdk = @import("../../../logdk.zig");

const web = logdk.web;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const id = req.param("id").?;
	_ = try env.app.db.exec("delete from logdk.tokens where id = $1", .{id});
	res.status = 204;
}

const t = logdk.testing;
test "tokens.delete: unknown id" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("id", "123");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204); //noop
}

test "tokens.delete: delete" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.factory.token(.{.id = "x1"});

	tc.web.param("id", "x1");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204);

	try t.expectEqual(true, try tc.scalar(bool, "select count(*) = 0 from logdk.tokens where id = 'x1'", .{}));
}
