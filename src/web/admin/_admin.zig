const logdk = @import("../../logdk.zig");

const s = @import("settings.zig");

pub const settings = struct {
	pub const update = s.handler;
};

pub fn init(builder: *logdk.Validate.Builder) !void {
	try s.init(builder);
}
