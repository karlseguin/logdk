const logdk = @import("../../logdk.zig");

const events_list = @import("events/index.zig");
const events_create = @import("events/create.zig");

pub const events = struct {
	pub const list = events_list.handler;
	pub const create = events_create.handler;
};

pub fn init(builder: *logdk.Validate.Builder) !void {
	try events_list.init(builder);
}
