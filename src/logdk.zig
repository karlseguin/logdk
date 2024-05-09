const std = @import("std");
pub const web = @import("web/web.zig");
pub const metrics = @import("metrics.zig");
pub const dispatcher = @import("dispatcher.zig");

pub const hrm = @import("hrm.zig");
pub const binder = @import("binder.zig");

pub const App = @import("app.zig").App;
pub const Env = @import("env.zig").Env;
pub const Meta = @import("meta.zig").Meta;
pub const Tasks = @import("tasks.zig").Tasks;
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
	pub const ILLEGAL_DB_WRITE = 8;
};

const logz = @import("logz");
const zuckdb = @import("zuckdb");
pub fn dbErr(ctx: []const u8, err: anyerror, conn: *const zuckdb.Conn, logger: logz.Logger) anyerror {
	logger.level(.Error).ctx(ctx).err(err).string("details", conn.err).log();
	return err;
}

pub const Validate = struct {
	const validate = @import("validate");

	pub const Object = validate.Object(void);
	pub const Builder = validate.Builder(void);
	pub const Context = validate.Context(void);

	pub const REQUIRED = validate.codes.REQUIRED;
	pub const STRING_LEN = validate.codes.STRING_LEN;
	pub const TYPE_STRING = validate.codes.TYPE_STRING;
	pub const STRING_CHOICE = validate.codes.STRING_CHOICE;
	pub const INT_MIN = validate.codes.INT_MIN;
	pub const INT_MAX  = validate.codes.INT_MAX ;
	pub const TYPE_BOOL = validate.codes.TYPE_BOOL;
	pub const TYPE_ARRAY = validate.codes.TYPE_ARRAY;
	pub const ARRAY_LEN_MIN = validate.codes.ARRAY_LEN_MIN;

	pub const INVALID_IDENTIFIER = 5000;
	pub const INVALID_IDENTIFIER_LEN = 5001;
	pub const INVALID_FILTER_VALUE_COUNT = 5003;
	pub const UNSUPPORTED_PARAMETER_TYPE = 5004;
	pub const INVALID_BITSTRING = 5005;
	pub const INVALID_RELATIVE_TIME = 5006;

	pub fn identifier(value: []const u8) !void {
		if (value.len == 0) return error.Required;
		if (value.len > MAX_IDENTIFIER_LEN) return error.TooLong;
		if (std.mem.indexOfScalar(u8, value, '"') != null) return error.Invalid;
	}

	pub fn identifierIsValid(value: []const u8) bool {
		identifier(value) catch return false;
		return true;
	}

	// when field is null, it means this code is (probably) being executed within
	// an input validation, and the field is being implictly tracked by the context
	// object.
	// When field isn't null, it (probably) means we're calling this directly.
	pub fn validateIdentifier(field: ?[]const u8, value: []const u8, context: *validate.Context(void)) !void {
		identifier(value) catch |err| {
			const invalid: validate.Invalid = switch (err) {
				error.Required => .{.err = "is required", .code = validate.codes.REQUIRED},
				error.Invalid => .{.err = "cannot contain double quote", .code = INVALID_IDENTIFIER},
				error.TooLong => .{
					.code = INVALID_IDENTIFIER_LEN,
					.err = std.fmt.comptimePrint("name cannot be longer than {d} characters", .{MAX_IDENTIFIER_LEN}),
					.data = try context.dataBuilder().put("max", MAX_IDENTIFIER_LEN).done(),
				},
			};

			if (field) |f| {
				context.addInvalidField(.{
					.field = f,
					.err = invalid.err,
					.code = invalid.code,
				});
			} else {
				try context.add(invalid);
			}
			return error.Validation;
		};
	}
};
