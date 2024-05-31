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

	const maybe = try conn.row("delete from logdk.users where id = $1 returning list_contains(permissions, 'admin')", .{id});
	if (maybe) |row| {
		defer row.deinit();

		_ = try conn.exec("delete from logdk.sessions where user_id = $1", .{id});

		if (row.get(bool, 0) == true and (try hasAdmin(conn)) == false) {
			// we're trying to delete the last admin
			try conn.rollback();
			res.status = 400;
			return res.json(.{
				.code = logdk.codes.MUST_HAVE_ONE_ADMIN,
				.err = "cannot delete last admin"
			}, .{});
		}
	}

	try conn.commit();
	res.status = 204;
}

fn hasAdmin(conn: *zuckdb.Conn) !bool {
	var row = (try conn.row("select count(*) from logdk.users where enabled and list_contains(permissions, 'admin')", .{})) orelse unreachable;
	defer row.deinit();
	return row.get(i64, 0) > 0;
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

test "users.delete: delete non-admin" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.factory.user(.{.id = 3});

	tc.web.param("id", "3");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204);

	try t.expectEqual(true, try tc.scalar(bool, "select count(*) = 0 from logdk.users where id = 3", .{}));
}

test "users.delete: cannot delete last admin" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.factory.user(.{.id = 4, .permissions = &[_][]const u8{"admin"}});

	tc.web.param("id", "4");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(400);
	try tc.web.expectJson(.{
		.code = 11,
		.err = "cannot delete last admin",
	});
}

test "users.delete: delete non-last admin" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.factory.user(.{.id = 4, .permissions = &[_][]const u8{"admin"}});
	tc.factory.user(.{.id = 5, .permissions = &[_][]const u8{"admin"}});

	tc.web.param("id", "4");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204);

	try t.expectEqual(true, try tc.scalar(bool, "select count(*) = 0 from logdk.users where id = 4", .{}));
	try t.expectEqual(false, try tc.scalar(bool, "select count(*) = 0 from logdk.users where id = 5", .{}));
}
