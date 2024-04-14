const std = @import("std");
pub const web = @import("web/web.zig");
pub const metrics = @import("metrics.zig");
pub const dispatcher = @import("dispatcher.zig");

pub const App = @import("app.zig").App;
pub const Env = @import("env.zig").Env;
pub const Meta = @import("meta.zig").Meta;
pub const Event = @import("event.zig").Event;
pub const Config = @import("config.zig").Config;
pub const DataSet = @import("dataset.zig").DataSet;

pub const testing = @import("t.zig");

pub const MAX_IDENTIFIER_LEN = 100;

pub const codes = struct {
	pub const CONNECTION_RESET = 0;
	pub const INTERNAL_SERVER_ERROR_CAUGHT = 1;
	pub const INTERNAL_SERVER_ERROR_UNCAUGHT = 2;
	pub const ROUTER_NOT_FOUND = 3;
	pub const NOT_FOUND = 4;
	pub const VALIDATION_ERROR = 5;
	pub const INVALID_JSON = 6;
	pub const INVALID_SQL = 7;
};

const logz = @import("logz");
const zuckdb = @import("zuckdb");
pub fn dbErr(ctx: []const u8, err: anyerror, conn: *const zuckdb.Conn, logger: logz.Logger) anyerror {
	logger.level(.Error).ctx(ctx).err(err).string("details", conn.err).log();
	return err;
}

pub const Validate = struct {
	const validate = @import("validate");

	const INVALID_IDENTIFIER = 5000;
	const INVALID_IDENTIFIER_LEN = 5001;

	pub fn TableName(field: []const u8, name: []const u8, context: *validate.Context(void)) !void {
		if (name.len == 0) {
			context.addInvalidField(.{
				.field = field,
				.err = "is required",
				.code = validate.codes.REQUIRED,
			});
			return error.Validation;
		}

		if (name.len > MAX_IDENTIFIER_LEN) {
			context.addInvalidField(.{
				.field = field,
				.code = INVALID_IDENTIFIER_LEN,
				.err = std.fmt.comptimePrint("name cannot be longer than {d} characters", .{MAX_IDENTIFIER_LEN}),
			});
			return error.Validation;
		}

		if (std.mem.indexOfScalar(u8, name, '"') != null) {
			context.addInvalidField(.{
				.field = field,
				.code = INVALID_IDENTIFIER,
				.err = "cannot contain double quote",
			});
			return error.Validation;
		}
	}
};
