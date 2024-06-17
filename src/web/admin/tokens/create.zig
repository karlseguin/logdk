const std = @import("std");
const builtin = @import("builtin");

const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const logdk = @import("../../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
	const CHARS = "abcdefghijklmnopqrtuvwxyz0123456789";
	var id: [30]u8 = undefined;
	std.crypto.random.bytes(&id);
	for (&id) |*b| {
		b.* = CHARS[@mod(b.*, CHARS.len)];
	}

	{
		var conn = try env.app.db.acquire();
		defer conn.release();
		_ = conn.exec("insert into logdk.tokens (id) values ($1)", .{&id}) catch |err| {
			return logdk.dbErr("Tokens.create", err, conn, env.logger);
		};
	}

	res.status = 201;
	return res.json(.{.id = id}, .{});
}

const t = logdk.testing;
test "tokens.create" {
	var tc = t.context(.{});
	defer tc.deinit();
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(201);

	const id = (try tc.web.getJson()).object.get("id").?.string;
	try t.expectEqual(30, id.len);
	try t.expectEqual(1, tc.scalar(i64, "select count(*) from logdk.tokens where id = $1", .{id}));
}
