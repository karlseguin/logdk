const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

// env is undefined for this route, do not use!
pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
	const payload = env.app.meta.payload();
	res.callback(releasePayload, @ptrCast(payload));
	res.content_type = .JSON;
	res.body = payload.json;
}

fn releasePayload(state: *anyopaque) void {
	const payload: *logdk.Meta.Payload = @alignCast(@ptrCast(state));
	payload.release();
}
