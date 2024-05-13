const logdk = @import("../../logdk.zig");

const web = logdk.web;

pub const info = @import("info.zig").handler;
pub const metrics = @import("metrics.zig").handler;
pub const describe = @import("describe.zig").handler;
