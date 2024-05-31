const logdk = @import("../../logdk.zig");

const users_list = @import("users/list.zig");
const users_create = @import("users/create.zig");
const users_update = @import("users/update.zig");
const users_delete = @import("users/delete.zig");

pub const users = struct {
	pub const list = users_list.handler;
	pub const create = users_create.handler;
	pub const update = users_update.handler;
	pub const delete = users_delete.handler;
};

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
	try users_create.init(builder);
	try users_update.init(builder);
	try settings_update.init(builder);
}
