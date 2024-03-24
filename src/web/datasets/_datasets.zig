const logdk = @import("../../logdk.zig");

const web = logdk.web;

const events_create = @import("events/create.zig");

// anytype because the type is a complex generic, and I'm lazy.
pub fn routes(r: anytype) void {
	r.post("/datasets/:name/events", events_create.handler);
}
