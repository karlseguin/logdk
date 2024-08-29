const std = @import("std");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

const App = logdk.App;

pub const Env = struct {
    app: *App,

    // This logger has the "$rid=REQUEST_ID" attributes (and maybe more) automatically
    // added to any generated log. Managed by the dispatcher.
    logger: logz.Logger,

    // should be loaded via the env.validator() function
    _validator: ?*logdk.Validate.Context = null,

    // App.Settings can change (settings can be updated), but this is a snapshot
    // of settings when the request began (achieved through an Arc Mutext). This
    // ensures a request has a consistent view of settings, and allows accessing
    // fields without thread-safety issues.
    settings: *const App.Settings,

    pub fn deinit(self: Env) void {
        if (self._validator) |val| {
            self.app.validators.release(val);
        }
    }

    pub fn validator(self: *Env) !*logdk.Validate.Context {
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
