const std = @import("std");
const validate = @import("validate");
const logdk = @import("logdk.zig");

// There's no facility to do initialization on startup (like Go's init), so
// we'll just hard-code this ourselves. The reason we extract this out is
// largely so that our tests can call this too for setup.
pub fn init(allocator: std.mem.Allocator) !void {
    // allocator is an arena
    const builder = try allocator.create(logdk.Validate.Builder);
    builder.* = try logdk.Validate.Builder.init(allocator);

    try @import("web/web.zig").init(builder);
    try @import("binder.zig").init(builder);
}
