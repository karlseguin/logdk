const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../../../logdk.zig");

const web = logdk.web;

var input_validator: *logdk.Validate.Object = undefined;
pub fn init(builder: *logdk.Validate.Builder) !void {
	input_validator = builder.object(&.{
		builder.field("create_tokens", builder.boolean(.{})),
		builder.field("dataset_creation", builder.boolean(.{})),
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

const t = logdk.testing;
test "settings.update: validation" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{
		.create_tokens = "123",
		.dataset_creation = null,
	});

	try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = logdk.Validate.TYPE_BOOL, .field = "create_tokens"});
	try tc.expectInvalid(.{.code = logdk.Validate.TYPE_BOOL, .field = "dataset_creation"});
}

test "settings.update: success" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.json(.{
		.create_tokens = true,
		.dataset_creation = false,
		.single_user = false, // ignored
		.misc = 123, // ignored
	});

	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(204);

	{
		const settings = tc.app.getSettings();
		defer settings.release();

		try t.expectEqual(true, settings.value.single_user);
		try t.expectEqual(true, settings.value.create_tokens);
		try t.expectEqual(false, settings.value.dataset_creation);
	}
}
