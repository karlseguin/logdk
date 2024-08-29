const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
    var app = env.app;

    const sql =
        \\ select json_object('id', id, 'created', epoch_us(created))
        \\ from logdk.tokens
        \\ order by id
    ;
    var conn = try app.db.acquire();
    defer conn.release();

    var rows = conn.query(sql, .{}) catch |err| {
        return env.dbErr("Tokens.list", err, conn);
    };
    defer rows.deinit();

    res.content_type = .JSON;
    var writer = res.writer();
    try writer.writeAll("{\"tokens\":[\n  ");
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
test "tokens.list: empty" {
    var tc = t.context(.{});
    defer tc.deinit();

    try handler(tc.env(), tc.web.req, tc.web.res);
    try tc.web.expectJson(.{
        .tokens = &[_]i32{}, // i33 because we don't care, as long as its empty
    });
}

test "tokens.list: list" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.factory.token(.{ .id = "tok2" });
    tc.factory.token(.{ .id = "tok1" });

    try handler(tc.env(), tc.web.req, tc.web.res);
    try tc.web.expectJson(.{ .tokens = &[_]struct { id: []const u8 }{
        .{ .id = "tok1" },
        .{ .id = "tok2" },
    } });
}
