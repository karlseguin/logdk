const logdk = @import("../../logdk.zig");

const tokens_list = @import("tokens/list.zig");
const tokens_create = @import("tokens/create.zig");
const tokens_delete = @import("tokens/delete.zig");

pub const tokens = struct {
    pub const list = tokens_list.handler;
    pub const create = tokens_create.handler;
    pub const delete = tokens_delete.handler;
};

const settings_update = @import("settings/update.zig");
pub const settings = struct {
    pub const update = settings_update.handler;
};

pub fn init(builder: *logdk.Validate.Builder) !void {
    try settings_update.init(builder);
}
