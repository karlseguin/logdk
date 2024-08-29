const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");
pub const web = @import("httpz").testing;

pub usingnamespace zul.testing;

const App = logdk.App;
const Env = logdk.Env;
const allocator = std.testing.allocator;

// We will _very_ rarely use this. Zig test doesn't have lifecycle hooks. We
// can setup globals on startup, but we can't clean this up properly. If we use
// std.testing.allocator for these, it'll report a leak. So, we create a gpa
// without any leak reporting, and use that for the few globals that we have.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const leaking_allocator = gpa.allocator();

pub fn setup() !void {
    logz.setup(allocator, .{ .pool_size = 2, .level = .Warn, .output = .stderr }) catch unreachable;
    try @import("init.zig").init(leaking_allocator);
}

// Our Test.Context exists to help us write tests. It does this by:
// - Exposing the httpz.testing helpers
// - Giving us an arena for any ad-hoc allocation we need
// - Having a working *App
// - Exposing a database factory
// - Creating envs and users as needed
pub fn context(_: Context.Config) *Context {
    const arena = allocator.create(std.heap.ArenaAllocator) catch unreachable;
    arena.* = std.heap.ArenaAllocator.init(allocator);

    const aa = arena.allocator();
    const app = aa.create(App) catch unreachable;
    app.* = App.init(allocator, .{
        .log_http = .none,
        .db = .{
            .pool_size = 1,
            .pool_timeout = 1000,
            .path = ":memory:",
        },
    }) catch unreachable;

    const ctx = allocator.create(Context) catch unreachable;
    ctx.* = .{
        ._env = null,
        ._arena = arena,
        .arena = aa,
        .app = app,
        .web = web.init(.{}),
        .factory = .{ .ctx = ctx },
    };
    return ctx;
}

pub const Context = struct {
    _arena: *std.heap.ArenaAllocator,
    _env: ?*Env,
    app: *App,
    web: web.Testing,
    arena: std.mem.Allocator,
    reset_log_level: bool = false,
    factory: Factory,

    const Config = struct {};

    pub fn deinit(self: *Context) void {
        self.web.deinit();
        if (self._env) |e| {
            e.logger.release();
            e.deinit();
        }
        self.app.deinit();
        self._arena.deinit();

        if (self.reset_log_level) {
            logz.setLevel(.Warn);
        }

        allocator.destroy(self._arena);
        allocator.destroy(self);
    }

    pub fn reset(self: *Context) void {
        if (self._env) |e| {
            if (e._validator) |val| {
                val.reset();
            }
        }
        self.web.deinit();
        self.web = web.init(.{});
    }

    pub fn withScheduler(self: *Context) void {
        self.app.scheduler.start(self.app) catch unreachable;
    }

    pub fn silenceLogs(self: *Context) void {
        logz.setLevel(.None);
        self.reset_log_level = true;
    }

    pub fn flushMessages(self: *Context) void {
        // brute force, since we can't process any more messages after this
        // but should be good enough for most cases. We need to flush every dataset
        // to make sure any pending appends are written.
        const app = self.app;
        var it = app.meta._datasets.valueIterator();
        while (it.next()) |ds| {
            app.dispatcher.send(logdk.DataSet, ds.arc.value.actor_id, .{ .flush = {} });
        }
        app.dispatcher.stop();
    }

    pub fn unsafeDataSet(self: *Context, name: []const u8) *logdk.DataSet {
        const arc = self.app.getDataSet(name).?;
        defer arc.release();
        return self.app.dispatcher.unsafeInstance(logdk.DataSet, arc.value.actor_id);
    }

    pub fn env(self: *Context) *Env {
        if (self._env) |e| {
            return e;
        }
        const app = self.app;
        const e = self.arena.create(Env) catch unreachable;
        e.* = Env{
            .app = app,
            .settings = &app._settings.arc.value,
            .logger = logz.logger().multiuse(),
        };
        self._env = e;
        return e;
    }

    pub fn exec(self: *Context, sql: []const u8, args: anytype) !void {
        var c = self.conn();
        defer c.release();
        _ = c.exec(sql, args) catch |err| {
            if (c.err) |e| std.debug.print("err: {s}\n", .{e});
            return err;
        };
    }

    pub fn row(self: *Context, sql: []const u8, args: anytype) !?zuckdb.OwningRow {
        var c = self.conn();
        defer c.release();
        return c.row(sql, args) catch |err| {
            if (c.err) |e| std.debug.print("err: {s}\n", .{e});
            return err;
        };
    }

    pub fn scalar(self: *Context, comptime T: type, sql: []const u8, args: anytype) !T {
        var c = self.conn();
        defer c.release();
        const r = c.row(sql, args) catch |err| {
            if (c.err) |e| std.debug.print("err: {s}\n", .{e});
            return err;
        } orelse unreachable;

        defer r.deinit();
        return r.get(T, 0);
    }

    pub fn query(self: *Context, sql: []const u8, args: anytype) !zuckdb.Rows {
        var c = self.conn();
        defer c.release();
        return c.query(sql, args) catch |err| {
            if (c.err) |e| std.debug.print("err: {s}\n", .{e});
            return err;
        };
    }

    pub fn conn(self: *Context) *zuckdb.Conn {
        return self.app.db.acquire() catch unreachable;
    }

    pub fn createDataSet(self: *Context, name: []const u8, event_json: []const u8, insert_event: bool) !void {
        const event_list = try logdk.Event.parse(allocator, event_json);

        const arc = try self.app.createDataSet(self.env(), name, event_list.events[0]);
        arc.release();

        if (insert_event) {
            const dataset = self.unsafeDataSet(name);
            try dataset.handle(.{ .record = event_list });
            try dataset.handle(.{ .flush = {} });
        } else {
            event_list.deinit();
        }
    }

    pub fn recordEvent(self: *Context, name: []const u8, event_json: []const u8) !void {
        const event = try logdk.Event.parse(allocator, event_json);
        const arc = self.app.getDataSet(name).?;
        arc.release();

        const dataset = self.unsafeDataSet(name);
        try dataset.handle(.{ .record = event });
        try dataset.handle(.{ .flush = {} });
    }

    // pub fn event(self: *const Context, s: anytype) typed.Map {
    //  const str = std.json.stringifyAlloc(self.arena, s, .{}) catch unreachable;
    //  return std.json.parseFromSliceLeaky(typed.Map, self.arena, str, .{}) catch unreachable;
    // }

    pub fn expectNotFound(self: *Context, desc: []const u8) !void {
        try self.web.expectStatus(404);
        try self.web.expectJson(.{
            .code = 4,
            .desc = desc,
            .err = "not found",
        });
    }

    pub fn expectInvalid(self: *const Context, expectation: anytype) !void {
        const validate = @import("validate");
        return validate.testing.expectInvalid(expectation, self._env.?._validator.?);
    }
};

const Factory = struct {
    ctx: *Context,

    pub fn token(self: Factory, args: anytype) void {
        const T = @TypeOf(args);

        self.ctx.exec("insert into logdk.tokens (id, created) values ($1, $2)", .{
            args.id,
            if (@hasField(T, "created")) args.created else std.time.microTimestamp(),
        }) catch unreachable;
    }
};
