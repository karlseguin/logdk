const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
	var app = env.app;

	const sql =
		\\ select json_object('id', id, 'username', username, 'enabled', enabled, 'permissions', permissions)
		\\ from logdk.users
		\\ order by lower(username)
	;
	var rows = try app.db.query(sql, .{});
	defer rows.deinit();

	res.content_type = .JSON;
	var writer = res.writer();
	try writer.writeAll("{\"users\":[\n  ");
	if (try rows.next()) |first| {
		try writer.writeAll(first.get([]const u8, 0));

		while (try rows.next()) |row| {
			try writer.writeAll(",\n  ");
			try writer.writeAll(row.get([]const u8, 0));
		}
	}
	try writer.writeAll("\n]}");
}

const t = logdk.testing;
test "users.list: empty" {
	var tc = t.context(.{});
	defer tc.deinit();

	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.users = &[_]i32{}, // i33 because we don't care, as long as its empty
	});
}

test "users.list: list" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.factory.user(.{.id = 1, .username = "Teg", .enabled = true, .created = 12345678});
	tc.factory.user(.{.id = 2, .username = "alia", .enabled = false, .created = 1717127247274335, .permissions = [_][]const u8{"admin"}});

	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.users = &[_]struct{id: u32, username: []const u8, enabled: bool, permissions: []const []const u8} {
			.{.id = 2, .username = "alia", .enabled = false, .permissions = &[_][]const u8{"admin"}},
			.{.id = 1, .username = "Teg", .enabled = true, .permissions = &[_][]const u8{}},
		}
	});
}
