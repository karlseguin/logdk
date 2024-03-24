const std = @import("std");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");
const validate = @import("validate");

const App = logdk.App;

pub const Env = struct {
	app: *App,

	// This logger has the "$rid=REQUEST_ID" attributes (and maybe more) automatically
	// added to any generated log. Managed by the dispatcher.
	logger: logz.Logger,

	// should be loaded via the env.validator() function
	_validator: ?*validate.Context(void) = null,

	pub fn deinit(self: Env) void {
		self.logger.release();
		if (self._validator) |val| {
			self.app.validators.release(val);
		}
	}

	pub fn validator(self: *Env) !*validate.Context(void) {
		if (self._validator) |val| {
			return val;
		}
		const val = try self.app.validators.acquire({});
		self._validator = val;
		return val;
	}

	pub fn dbErr(self: *const Env, ctx: []const u8, err: anyerror, conn: *const zuckdb.Conn) anyerror {
		return logdk.dbErr(ctx, err, conn, self.logger);
	}
};
