const logdk = @import("../../logdk.zig");

pub const events_index = @import("events/index.zig");
pub const events_create = @import("events/create.zig");

pub fn init(builder: *logdk.Validate.Builder) !void {
	try events_index.init(builder);
}
