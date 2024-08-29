const std = @import("std");
const builtin = @import("builtin");

const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const logdk = @import("../../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
    const id = try env.app.tokens.create(env);
    res.status = 201;
    return res.json(.{ .id = id }, .{});
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
