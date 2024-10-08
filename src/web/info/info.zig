const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
    const info = env.app.meta.getInfo(env.app);
    defer info.release();

    res.content_type = .JSON;
    res.body = info.value.json;

    // explicitly write, so that we can release our describe at the end of this function
    try res.write();
}

const t = logdk.testing;
test "info: json" {
    var tc = t.context(.{});
    defer tc.deinit();

    try handler(tc.env(), tc.web.req, tc.web.res);
    try tc.web.expectJson(.{ .logdk = .{
        .version = "commit: local-dev\nzig: local-dev\nui: local-dev\n",
        .httpz_blocking = httpz.blockingMode(),
    }, .duckdb = .{
        .size = .{
            .database_name = "memory",
            .database_size = "0 bytes",
            .block_size = 0,
            .total_blocks = 0,
            .used_blocks = 0,
            .free_blocks = 0,
            .wal_size = "0 bytes",
        },
        .version = .{ .library_version = "v0.10.2", .source_id = "1601d94f94" },
    } });
}
