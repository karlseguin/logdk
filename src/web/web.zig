const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const cache = @import("cache");
const httpz = @import("httpz");
const typed = @import("typed");
const validate = @import("validate");

const logdk = @import("../logdk.zig");

const App = logdk.App;
const Env = logdk.Env;
const Config = logdk.Config;
const User = logdk.auth.User;
const Permission = logdk.auth.Permission;

const ui = @import("ui.zig");
const exec = @import("exec.zig");
const info = @import("info/_info.zig");
const admin = @import("admin/_admin.zig");
const session = @import("session/_session.zig");
const datasets = @import("datasets/_datasets.zig");

const SessionCache = cache.Cache(User);

pub const Server = httpz.ServerCtx(*const Dispatcher, *Env);

// every request gets a request_id, any log message created from a request-specific
// logger will include this id
var request_counter: u32 = 0;

pub fn init(builder: *logdk.Validate.Builder) !void {
	try datasets.init(builder);
	try exec.init(builder);
	try admin.init(builder);
}

pub fn start(app: *App, config: *const Config) !void {
	const allocator = app.allocator;

	var session_cache = try SessionCache.init(allocator, .{
		.max_size = 1000,
		.gets_per_promote = 10,
	});
	defer session_cache.deinit();

	const http_config = config.http;
	var server = try Server.init(allocator, http_config, undefined);
	defer server.deinit();

	app._webserver = &server;

	server.dispatcher(Dispatcher.dispatch);
	server.notFound(routerNotFound);
	server.errorHandler(errorHandler);

	const router = server.router();

	// We create a dispatcher per route, since each has a unique route name, each
	// may have its own permission, and who knows what else. But in all cases
	// it's a &Dispatcher, just with a 1 or 2 different fields. This factory just
	// makes creating the dispatchers a little cleaner.
	const df = Dispatcher.Factory.init(app, &session_cache, config);

	{
		var routes = router.group("/api/1", .{});
		router.getC("/metrics", info.metrics, .{.dispatcher = server.dispatchUndefined()});

		routes.getC("/datasets/:name/events", datasets.events.list, .{.ctx = &df.create("events_list", null)});
		routes.postC("/datasets/:name/events", datasets.events.create, .{.ctx = &df.create("events_create", null)});
		routes.getC("/exec", exec.handler, .{.ctx = &df.create("exec_sql", .raw_query)});
		routes.getC("/info", info.info, .{.ctx = &df.create("info", null)});
		routes.getC("/describe", info.describe, .{.ctx = &df.create("describe", null)});
		routes.getC("/session", session.show, .{.ctx = &df.create("session_show.handler", null)});

		routes.postC("/settings", admin.settings.update, .{.ctx = &df.create("settigs", .admin)});

		routes.getC("/users", admin.users.list, .{.ctx = &df.create("users_list", .admin)});
		routes.postC("/users", admin.users.create, .{.ctx = &df.create("users_create", .admin)});
		// see the handler for why this route is di
		// routes.putC("/users/:id", admin.users.update, .{.ctx = &df.create("users_update", .admin)});
		routes.deleteC("/users/:id", admin.users.delete, .{.ctx = &df.create("users_delete", .admin)});

		routes.getC("/tokens", admin.tokens.list, .{.ctx = &df.create("tokens_list", .admin)});
		routes.postC("/tokens", admin.tokens.create, .{.ctx = &df.create("tokens_create", .admin)});
		routes.deleteC("/tokens/:id", admin.tokens.delete, .{.ctx = &df.create("tokens_delete", .admin)});
	}

	router.getC("/*", ui.handler, .{.dispatcher = server.dispatchUndefined()});

	logz.info().ctx("http.listen")
		.fmt("address", "http://{s}:{d}", .{server.config.address.?, server.config.port.?})
		.stringSafe("log_http", @tagName(config.log_http))
		.boolean("blocking", httpz.blockingMode())
		.log();

	// blocks
	try server.listen();
}

const Dispatcher = struct {
	app: *App,

	// The canonical name for the route. This is added to the request info log
	// entry, as well as set in Route header of the response.
	route: []const u8,

	// Whether or not to log the request info. In case of an unhandled error, the
	// request info is always logged and have a code value of
	// `logdk.codes.INTERNAL_SERVER_ERROR_CAUGHT` (i.e. 1).
	log_request: bool = false,

	// the permission needed to access the route associated with this dispatcher
	// null means no permissions are needed
	permission: ?Permission,

	// session_id => logdk.auth.User
	session_cache: *SessionCache,

	pub fn dispatch(self: *const Dispatcher, action: httpz.Action(*Env), req: *httpz.Request, res: *httpz.Response) !void {
		const start_time = std.time.milliTimestamp();

		res.header("route", self.route);

		const request_id = @atomicRmw(u32, &request_counter, .Add, 1, .monotonic);
		// every log message generated with env.logger will include this $rid
		var logger = logz.logger().int("$rid", request_id).multiuse();
		defer logger.release();


		var code: i32 = 0;
		var user_id: ?u32 = null;
		var log_request = self.log_request;

		const app = self.app;
		var arc_settings = app.getSettings();
		defer arc_settings.release();

		const settings = &arc_settings.value;

		// Block exists only so we can break out of it should we have an authentication
		// or authorization issue.
		action: {
			// default user
			// when in single_user mode, the default user is an admin
			var user = User{.id = 0, .permission_admin = settings.single_user};
			var cache_user_entry: ?*cache.Entry(User) = null;
			if (getSessionId(req)) |session_id| {
				if (try self.session_cache.fetch(*const Dispatcher, session_id, loadUserFromSessionId, self, .{.ttl = 1800})) |entry| {
					user = entry.value;
					user_id = user.id;
					cache_user_entry = entry;
				} else {
					// we had a session_id, but it wasn't valid, this is always an error
					code = errors.InvalidAuthenticationToken.write(res);
					break: action;
				}
			}
			defer if (cache_user_entry) |entry| entry.release();
			if (self.permission) |required_permission| {
				if (user.hasPermission(required_permission) == false) {
					code = errors.PermissionDenied.write(res);
					break :action;
				}
			}

			var env = Env{
				.app = app,
				.user = user,
				.logger = logger,
				.settings = settings,
			};
			defer env.deinit();

			action(&env, req, res) catch |err| switch (err) {
				error.BrokenPipe, error.ConnectionResetByPeer => code = logdk.codes.CONNECTION_RESET,
				error.InvalidJson => code = errors.InvalidJson.write(res),
				error.Validation => {
					code = logdk.codes.VALIDATION_ERROR;
					res.status = 400;
					try res.json(.{
						.err = "validation error",
						.code = code,
						.validation = env._validator.?.errors(),
					}, .{.emit_null_optional_fields = false});
				},
				else => {
					code = logdk.codes.INTERNAL_SERVER_ERROR_CAUGHT;
					const error_id = zul.UUID.v4().toHexAlloc(res.arena, .lower) catch "00000000-0000-0000-0000-000000000000";
					res.status = 500;
					res.header("Error-Id", error_id);
					try res.json(.{
						.code = code,
						.error_id = error_id,
						.err = "internal server error",
					}, .{});

					log_request = true;
					_ = logger.stringSafe("error_id", error_id).err(err);
				}
			};
		}

		if (log_request) {
			logger.
				stringSafe("@l", "REQ").
				stringSafe("method", @tagName(req.method)).
				stringSafe("route", self.route).
				string("path", req.url.path).
				int("status", res.status).
				int("code", code).
				int("uid", user_id).
				int("ms", std.time.milliTimestamp() - start_time).
				log();
		}
	}

	fn loadUserFromSessionId(self: *const Dispatcher, session_id: []const u8) !?User {
		const sql =
			\\ select s.user_id, u.permissions
			\\ from logdk.sessions s join logdk.users u on s.user_id = u.id
			\\ where u.enabled and s.id = $1 and s.expires > now()
		;

		const conn = try self.app.db.acquire();
		defer conn.release();

		const row = conn.row(sql, .{session_id}) catch |err| {
			return logdk.dbErr("Dispatcher.loadUserFromSessionId", err, conn, logz.err());
		} orelse return null;

		defer row.deinit();

		const permissions = row.list([]const u8, 1).?;

		var user = User{.id = row.get(u32, 0)};
		for (0..permissions.len) |_| {
			const permission = std.meta.stringToEnum(Permission, permissions.get(0)) orelse continue;
			switch (permission) {
				.admin => user.permission_admin = true,
				.raw_query => user.permission_raw_query = true,
			}
		}
		return user;
	}

	// We need to create a dispatch, but each one is quite similar, so this
	// little factor makes it easier to create them without having to repeat
	// their common parameters
	const Factory = struct {
		app: *App,
		config: *const Config,
		session_cache: *SessionCache,

		fn init(app: *App, session_cache: *SessionCache, config: *const Config) Factory {
			return .{
				.app = app,
				.config = config,
				.session_cache = session_cache,
			};
		}

		fn create(self: *const Factory, route: []const u8, permission: ?Permission) Dispatcher {
			return .{
				.app = self.app,
				.route = route,
				.permission = permission,
				.session_cache = self.session_cache,
				.log_request = switch (self.config.log_http) {
					.all => true,
					.none => false,
					.smart => std.mem.eql(u8, route, "events_create") == false,
				}
			};
		}
	};
};

// public because it's used by our logout handler
pub fn getSessionId(req: *httpz.Request) ?[]const u8 {
	return req.header("authorization");
}

pub fn parseInt(comptime T: type, field: []const u8, value: []const u8, env: *Env) !T {
	return std.fmt.parseInt(T, value, 10) catch {
		(try env.validator()).addInvalidField(.{
			.field = field,
			.err = "is not a valid number",
			.code = logdk.Validate.TYPE_INT,
		});
		return error.Validation;
	};
}

// This isn't great, but we turn out querystring args into a typed.Map where every
// value is a typed.Value.string. Validators can be configured to parse strings.
pub fn validateQuery(req: *httpz.Request, v: *logdk.Validate.Object, env: *Env) !typed.Map {
	const q = try req.query();

	var map = typed.Map.init(req.arena);
	try map.ensureTotalCapacity(@intCast(q.len));

	for (q.keys[0..q.len], q.values[0..q.len]) |name, value| {
		try map.putAssumeCapacity(name, value);
	}

	var validator = try env.validator();
	const input = try v.validate(map, validator);
	if (!validator.isValid()) {
		return error.Validation;
	}
	return input orelse typed.Map.readonlyEmpty();
}

pub fn validateJson(req: *httpz.Request, v: *logdk.Validate.Object, env: *Env) !typed.Map {
	const body = req.body() orelse return error.InvalidJson;
	var validator = try env.validator();
	const input = try v.validateJsonS(body, validator);
	if (!validator.isValid()) {
		return error.Validation;
	}
	return input;
}

pub fn metrics(_: *Env, _: *httpz.Request, res: *httpz.Response) !void {
	const writer = res.writer();
	try httpz.writeMetrics(writer);
	try logz.writeMetrics(writer);
}

pub fn invalidSQL(res: *httpz.Response, desc: ?[]const u8, sql: []const u8) !void {
	res.status = 400;
	return res.json(.{
		.code = logdk.codes.INVALID_SQL,
		.@"error" = desc orelse "invalid sql",
		.sql = sql,
	}, .{});
}

// An application-related 404. The given method+path was valid. This could be
// something like trying to request a non-existent package.
pub fn notFound(res: *httpz.Response, comptime desc: []const u8) !void {
	const body = std.fmt.comptimePrint("{{\"code\": {d}, \"err\": \"not found\", \"desc\": \"{s}\"}}", .{logdk.codes.NOT_FOUND, desc});
	res.content_type = .JSON;
	res.status = 404;
	res.body = body;
}

// A routing-related 404 (as opposed to an application-specific one, like a
// request for a non-existent package). This is passed to the httpz library
// as a fallback route when no matching route is found.
fn routerNotFound(_: *const Dispatcher, _: *httpz.Request, res: *httpz.Response) !void {
	_ = errors.RouterNotFound.write(res);
}

// Dispatcher.dispatch handles any action-related errors (it executes the action
// with a catch and handles all possibilities.) This is for an error that happens
// outside of the action, either in the Dispatcher itself or in the underlying
// httpz library. This should not happen.
fn errorHandler(_: *const Dispatcher, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
	const code = errors.ServerError.write(res);
	logz.err().err(err).ctx("errorHandler").string("path", req.url.raw).int("code", code).log();
}

// pre-generated error messages
pub const Error = struct {
	code: i32,
	status: u16,
	body: []const u8,

	pub fn init(status: u16, comptime code: i32, comptime message: []const u8) Error {
		const body = std.fmt.comptimePrint("{{\"code\": {d}, \"err\": \"{s}\"}}", .{code, message});
		return .{
			.code = code,
			.body = body,
			.status = status,
		};
	}

	pub fn write(self: Error, res: *httpz.Response) i32 {
		res.status = self.status;
		res.content_type = httpz.ContentType.JSON;
		res.body = self.body;
		return self.code;
	}
};

// bunch of static errors that we can serialize at comptime
pub const errors = struct {
	const codes = logdk.codes;
	pub const ServerError = Error.init(500, codes.INTERNAL_SERVER_ERROR_UNCAUGHT, "internal server error");
	pub const RouterNotFound = Error.init(404, codes.ROUTER_NOT_FOUND, "not found");
	pub const InvalidJson = Error.init(400, codes.INVALID_JSON, "invalid JSON");
	pub const IllegalDBWrite = Error.init(400, codes.ILLEGAL_DB_WRITE, "only read statements are allowed (e.g. select)");
	pub const InvalidAuthenticationToken = Error.init(401, codes.INVALID_AUTHENTICATION_TOKEN, "invalid authentication token");
	pub const PermissionDenied = Error.init(403, codes.PERMISSION_DENIED, "permission denied");
};

const t = logdk.testing;
test "dispatcher: handles action error" {
	var tc = t.context(.{});
	defer tc.deinit();

	logz.setLevel(.None);
	defer logz.setLevel(.Warn);

	const dispatcher = testDispatcher(tc, .{});
	try dispatcher.dispatch(testErrorAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(500);
	try tc.web.expectJson(.{.code = 1});
}

test "dispatcher: dispatch to actions" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.url("/test_1");

	request_counter = 958589;
	const dispatcher = testDispatcher(tc, .{});
	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(200);
	try tc.web.expectJson(.{.url = "/test_1"});
}

test "dispatcher: permission required with no auth in single user mode" {
	var tc = t.context(.{});
	defer tc.deinit();
	try tc.app._settings.setValue(.{.single_user = true});

	tc.web.url("/user_0");

	const dispatcher = testDispatcher(tc, .{.permission = .admin});
	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(200);
	try tc.web.expectJson(.{.user_id = 0});
}

test "dispatcher: permission required with no auth not in single user mode" {
	var tc = t.context(.{});
	defer tc.deinit();
	try tc.app._settings.setValue(.{.single_user = false});

	tc.web.url("/");

	const dispatcher = testDispatcher(tc, .{.permission = .admin});
	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(403);
}

test "dispatcher: invalid auth token" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.url("/");
	tc.web.header("authorization", "nope");
	const dispatcher = testDispatcher(tc, .{});
	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(401);
}

test "dispatcher: valid auth but with wrong permission" {
	var tc = t.context(.{});
	defer tc.deinit();
try 	tc.app._settings.setValue(.{.single_user = false});

	tc.web.url("/");
	tc.web.header("authorization", "token1");

	const dispatcher = testDispatcher(tc, .{.permission = .admin});
	dispatcher.session_cache.put("token1", User{.id = 1, .permission_raw_query = true}, .{.ttl = 300}) catch unreachable;

	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(403);
}

test "dispatcher: expired session" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.exec("insert into logdk.users (id, username, password, enabled, permissions) values (2, 'leto', '', true, array['raw_query'])", .{});
	try tc.exec("insert into logdk.sessions (id, user_id, expires) values ('token-x2', 2, now() - interval '1 second')", .{});

	const dispatcher = testDispatcher(tc, .{.permission = .raw_query});
	// this loads the user from the DB
	tc.web.url("/user_2");
	tc.web.header("authorization", "token-x3");

	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(401);
}

test "dispatcher: loads user" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.exec("insert into logdk.users (id, username, password, enabled, permissions) values (3, 'leto', '', true, array['raw_query'])", .{});
	try tc.exec("insert into logdk.sessions (id, user_id, expires) values ('token-x3', 3, now() + interval '1 minute')", .{});

	const dispatcher = testDispatcher(tc, .{.permission = .raw_query});

	{
		// this loads the user from the DB
		tc.web.url("/user_3");
		tc.web.header("authorization", "token-x3");

		try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
		try tc.web.expectJson(.{.user_id = 3});
	}

	try tc.exec("delete from logdk.users", .{});
	try tc.exec("delete from logdk.sessions", .{});
	{
		// this loads the user from the cache
		tc.reset();
		tc.web.url("/user_3");
		tc.web.header("authorization", "token-x3");

		try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
		try tc.web.expectJson(.{.user_id = 3});
	}
}

test "web: Error.write" {
	var tc = t.context(.{});
	defer tc.deinit();

	try t.expectEqual(2, errors.ServerError.write(tc.web.res));
	try tc.web.expectStatus(500);
	try tc.web.expectJson(.{.code = 2, .err = "internal server error"});
}

test "web: notFound" {
	var tc = t.context(.{});
	defer tc.deinit();

	try notFound(tc.web.res, "no spice");
	try tc.web.expectStatus(404);
	try tc.web.expectJson(.{.code = 4, .err = "not found", .desc = "no spice"});
}

fn testDispatcher(tc: *t.Context, opts: anytype) Dispatcher {
	const T = @TypeOf(opts);

	const session_cache = tc.arena.create(SessionCache) catch unreachable;
	session_cache.* = SessionCache.init(tc.arena, .{
		.max_size = 20,
		.gets_per_promote = 10,
	}) catch unreachable;

	return .{
		.app = tc.app,
		.permission = if (@hasField(T, "permission")) opts.permission else null,
		.log_request = false,
		.route = "test-disaptcher",
		.session_cache = session_cache,
	};
}

fn testErrorAction(_: *Env, _: *httpz.Request, _: *httpz.Response) !void {
	return error.Nope;
}

fn callableAction(env: *Env, req: *httpz.Request, res: *httpz.Response) !void {
	var arr = std.ArrayList(u8).init(t.allocator);
	defer arr.deinit();

	if (std.mem.eql(u8, req.url.path, "/test_1")) {
		try env.logger.logTo(arr.writer());
		try t.expectEqual("@ts=9999999999999 $rid=958589\n", arr.items);
		try t.expectEqual(0, env.user.id);
		return res.json(.{.url = req.url.path}, .{});
	}

	if (std.mem.eql(u8, req.url.path, "/user_0")) {
		try t.expectEqual(0, env.user.id);
		try t.expectEqual(true, env.user.permission_admin);
		try t.expectEqual(false, env.user.permission_raw_query);
		return res.json(.{.user_id = env.user.id}, .{});
	}

	if (std.mem.eql(u8, req.url.path, "/user_3")) {
		try t.expectEqual(3, env.user.id);
		try t.expectEqual(false, env.user.permission_admin);
		try t.expectEqual(true, env.user.permission_raw_query);
		return res.json(.{.user_id = env.user.id}, .{});
	}

	unreachable;
}
