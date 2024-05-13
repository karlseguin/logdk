const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const httpz = @import("httpz");
const typed = @import("typed");
const validate = @import("validate");

const logdk = @import("../logdk.zig");

const App = logdk.App;
const Env = logdk.Env;

const ui = @import("ui.zig");
const exec = @import("exec.zig");
const info = @import("info/_info.zig");
const datasets = @import("datasets/_datasets.zig");

// every request gets a request_id, any log message created from a request-specific
// logger will include this id
var request_counter: u32 = 0;

pub fn init(builder: *logdk.Validate.Builder) !void {
	try datasets.init(builder);
	try exec.init(builder);
}

pub fn start(app: *App, config: *const logdk.Config) !void {
	const allocator = app.allocator;

	const http_config = config.http;
	var server = try httpz.ServerCtx(*const Dispatcher, *Env).init(allocator, http_config, undefined);
	defer server.deinit();

	server.dispatcher(Dispatcher.dispatch);
	server.notFound(routerNotFound);
	server.errorHandler(errorHandler);

	const router = server.router();

	{
		var routes = router.group("/api/1", .{});
		routes.getC("/datasets/:name/events", datasets.events_index.handler, .{.ctx = &Dispatcher.init(app, "events_list", config)});
		routes.postC("/datasets/:name/events", datasets.events_create.handler, .{.ctx = &Dispatcher.init(app, "events_create", config)});
		routes.getC("/exec", exec.handler, .{.ctx = &Dispatcher.init(app, "exec_sql", config)});
		routes.getC("/info", info.info, .{.ctx = &Dispatcher.init(app, "info", config)});
		routes.getC("/describe", info.describe, .{.ctx = &Dispatcher.init(app, "describe", config)});
	}

	router.getC("/metrics", info.metrics, .{.dispatcher = server.dispatchUndefined()});
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
	route: []const u8,
	log_request: bool = false,

	fn init(app: *App, route: []const u8, config: *const logdk.Config) Dispatcher {
		return .{
			.app = app,
			.route = route,
			.log_request = switch (config.log_http) {
				.all => true,
				.none => false,
				.smart => std.mem.eql(u8, route, "events_create") == false,
			}
		};
	}

	pub fn dispatch(self: *const Dispatcher, action: httpz.Action(*Env), req: *httpz.Request, res: *httpz.Response) !void {
		const start_time = std.time.milliTimestamp();

		res.header("route", self.route);

		const request_id = @atomicRmw(u32, &request_counter, .Add, 1, .monotonic);
		// every log message generated with env.logger will include this $rid
		var logger = logz.logger().int("$rid", request_id).multiuse();

		const app = self.app;

		var env = Env{
			.app = app,
			.logger = logger,
			.arena = res.arena,
		};
		defer env.deinit();

		var code: i32 = 0;
		var log_request = self.log_request;

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

		if (log_request) {
			logger.
				stringSafe("@l", "REQ").
				stringSafe("method", @tagName(req.method)).
				stringSafe("route", self.route).
				string("path", req.url.path).
				int("status", res.status).
				int("code", code).
				int("ms", std.time.milliTimestamp() - start_time).
				log();
		}
	}
};

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

	fn init(status: u16, comptime code: i32, comptime message: []const u8) Error {
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
};

const t = logdk.testing;
test "dispatcher: handles action error" {
	var tc = t.context(.{});
	defer tc.deinit();

	logz.setLevel(.None);
	defer logz.setLevel(.Warn);

	const dispatcher = testDispatcher(tc);
	try dispatcher.dispatch(testErrorAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(500);
	try tc.web.expectJson(.{.code = 1});
}

test "dispatcher: dispatch to actions" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.url("/test_1");

	request_counter = 958589;
	const dispatcher = testDispatcher(tc);
	try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
	try tc.web.expectStatus(200);
	try tc.web.expectJson(.{.url = "/test_1"});
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

fn testDispatcher(tc: *t.Context) Dispatcher {
	return .{
		.app = tc.app,
		.log_request = false,
		.route = "test-disaptcher",
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
	} else {
		unreachable;
	}
	return res.json(.{.url = req.url.path}, .{});
}
