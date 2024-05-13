const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
	const info = env.app.meta.getInfo(env.app);
	res.callback(releasePayload, @ptrCast(info));
	res.content_type = .JSON;
	res.body = info.value;
}

fn releasePayload(state: *anyopaque) void {
	const info: logdk.Meta.Payload = @alignCast(@ptrCast(state));
	info.release();
}
