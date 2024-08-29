const logz = @import("logz");
const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

// env is undefined for this route, do not use!
pub fn handler(_: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
    const writer = res.writer();
    try httpz.writeMetrics(writer);
    try logz.writeMetrics(writer);
    try logdk.metrics.write(writer);
}
