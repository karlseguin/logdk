const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
	const describe = env.app.meta.getDescribe();
	res.callback(releasePayload, @ptrCast(describe));
	res.content_type = .JSON;
	res.body = describe.value;
}

fn releasePayload(state: *anyopaque) void {
	const describe: logdk.Meta.DescribeValue = @alignCast(@ptrCast(state));
	describe.release();
}
