const logdk = @import("../../logdk.zig");

const web = logdk.web;

const events_index = @import("events/index.zig");
const events_create = @import("events/create.zig");

pub fn init(builder: *web.Validate.Builder) !void {
	try events_index.init(builder);
}

// anytype because the type is a complex generic, and I'm lazy.
pub fn routes(r: anytype) void {
	r.get("/datasets/:name/events", events_index.handler);
	r.post("/datasets/:name/events", events_create.handler);
}
