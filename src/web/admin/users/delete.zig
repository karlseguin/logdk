const std = @import("std");
const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const logdk = @import("../../../logdk.zig");

const web = logdk.web;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const id = try web.parseInt(u32, "id", req.param("id").?, env);

	var app = env.app;

	var conn = try app.db.acquire();
	defer conn.release();

	try conn.begin();
	errdefer conn.rollback() catch {};
	_ = try conn.exec("delete from logdk.users where id = $1", .{id});
	_ = try conn.exec("delete from logdk.sessions where user_id = $1", .{id});

	try conn.commit();
	res.status = 204;
}

const t = logdk.testing;
test "users.delete: invalid id" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("id", "nope");
	try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = logdk.Validate.TYPE_INT, .field = "id"});
}

test "users.delete: unknown id" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("id", "123");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204); //noop
}

test "users.delete: delete" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.factory.user(.{.id = 3});

	tc.web.param("id", "3");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204);

	try t.expectEqual(true, try tc.scalar(bool, "select count(*) = 0 from logdk.users where id = 3", .{}));
}
