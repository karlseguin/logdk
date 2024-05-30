const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

const web = logdk.web;

var input_validator: *logdk.Validate.Object = undefined;
pub fn init(builder: *logdk.Validate.Builder) !void {
	input_validator = builder.object(&.{
		builder.field("create_tokens", builder.boolean(.{.parse = true})),
		builder.field("dataset_creation", builder.boolean(.{.parse = true})),
	}, .{});
}

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const input = try web.validateJson(req, input_validator, env);
	const existing = env.settings;

	try env.app.saveSettings(.{
		.create_tokens = if (input.get("create_tokens")) |v| v.bool else existing.create_tokens,
		.dataset_creation = if (input.get("dataset_creation")) |v| v.bool else existing.dataset_creation,
	});

	res.status = 204;
}

// const t = logdk.testing;
// test "settings.update: unknown dataset, dynamic creation disabled" {
