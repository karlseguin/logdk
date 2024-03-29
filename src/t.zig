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

pub fn setup() !void {
	logz.setup(allocator, .{.pool_size = 2, .level = .Warn, .output = .stderr}) catch unreachable;
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
		.log_http = false,
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
	};
	return ctx;
}

pub const Context = struct {
	_arena: *std.heap.ArenaAllocator,
	_env: ?*Env,
	app: *App,
	web: web.Testing,
	arena: std.mem.Allocator,

	const Config = struct {
	};

	pub fn deinit(self: *Context) void {
		self.web.deinit();
		if (self._env) |e| {
			e.deinit();
		}
		self.app.deinit();
		self._arena.deinit();

		allocator.destroy(self._arena);
		allocator.destroy(self);
	}

	pub fn reset(self: *Context) void {
		if (self._env) |e| {
			if (e._validator) |val| {
				val.reset();
			}
		}
	}

	pub fn env(self: *Context) *Env {
		if (self._env) |e| {
			return e;
		}

		const app = self.app;
		const e = self.arena.create(Env) catch unreachable;
		e.* = Env{
			.app = app,
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

	pub fn scalar(self: *Context, comptime T: type, sql: []const u8, args: anytype) !T {
		var c = self.conn();
		defer c.release();
		const row = c.row(sql, args) catch |err| {
			if (c.err) |e| std.debug.print("err: {s}\n", .{e});
			return err;
		} orelse unreachable;

		defer row.deinit();
		return row.get(T, 0);
	}

	pub fn query(self: *Context, sql: []const u8, args: anytype) !zuckdb.Rows {
		var c = self.conn();
		defer c.release();
		return c.query(sql, args) catch |err| {
			if (c.err) |e| std.debug.print("err: {s}\n", .{e});
			return err;
		};
	}

	pub fn conn(self: *Context) *zuckdb.Conn  {
		return self.app.db.acquire() catch unreachable;
	}

	// pub fn event(self: *const Context, s: anytype) typed.Map {
	// 	const str = std.json.stringifyAlloc(self.arena, s, .{}) catch unreachable;
	// 	return std.json.parseFromSliceLeaky(typed.Map, self.arena, str, .{}) catch unreachable;
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
