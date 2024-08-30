const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const httpz = @import("httpz");
const typed = @import("typed");
const validate = @import("validate");

const logdk = @import("../logdk.zig");

const App = logdk.App;
const Env = logdk.Env;
const Config = logdk.Config;

const ui = @import("ui.zig");
const exec = @import("exec.zig");
const info = @import("info/_info.zig");
const admin = @import("admin/_admin.zig");
const datasets = @import("datasets/_datasets.zig");

pub const Server = httpz.Server(*const Dispatcher);

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

    const http_config = config.http;
    var dispatcher = Dispatcher{
        .app = app,
    };

    var server = try httpz.Server(*const Dispatcher).init(allocator, http_config, &dispatcher);
    defer server.deinit();

    app._webserver = &server;

    const router = server.router();

    const log_request = config.log_http != .none;

    router.get("/metrics", info.metrics, .{ .dispatcher = Dispatcher.direct });

    {
        var api_routes = router.group("/api/", .{});
        api_routes.get("/1/datasets/:name/events", datasets.events.list, .{ .data = &Dispatcher.Route{.name = "events_list", .log = log_request }});
        api_routes.post("/1/datasets/:name/events", datasets.events.create, .{ .data = &Dispatcher.Route{.name = "events_create", .log = config.log_http == .all  }});
        api_routes.get("/1/exec", exec.handler, .{ .data = &Dispatcher.Route{.name = "exec_sql", .log = log_request }});
        api_routes.get("/1/info", info.info, .{ .data = &Dispatcher.Route{.name = "info", .log = log_request }});
        api_routes.get("/1/describe", info.describe, .{ .data = &Dispatcher.Route{.name = "describe", .log = log_request }});

        api_routes.post("/1/settings", admin.settings.update, .{ .data = &Dispatcher.Route{.name = "settigs", .log = log_request }});

        api_routes.get("/1/tokens", admin.tokens.list, .{ .data = &Dispatcher.Route{.name = "tokens_list", .log = log_request }});
        api_routes.post("/1/tokens", admin.tokens.create, .{ .data = &Dispatcher.Route{.name = "tokens_create", .log = log_request }});
        api_routes.delete("/1/tokens/:id", admin.tokens.delete, .{ .data = &Dispatcher.Route{.name = "tokens_delete", .log = log_request }});
    }

    router.get("/*", ui.handler, .{ .dispatcher = Dispatcher.direct });

    logz.info().ctx("http.listen")
        .fmt("address", "http://{s}:{d}", .{ server.config.address.?, server.config.port.? })
        .stringSafe("log_http", @tagName(config.log_http))
        .boolean("blocking", httpz.blockingMode())
        .log();

    // blocks
    try server.listen();
}

const Dispatcher = struct {
    app: *App,

    const Route = struct {
        name: []const u8,
        log: bool = true,
    };

    pub fn dispatch(self: *const Dispatcher, action: httpz.Action(*Env), req: *httpz.Request, res: *httpz.Response) !void {
        const start_time = std.time.milliTimestamp();

        const route_data: *const Route = @ptrCast(@alignCast(req.route_data));
        res.header("route", route_data.name);

        const request_id = @atomicRmw(u32, &request_counter, .Add, 1, .monotonic);
        // every log message generated with env.logger will include this $rid
        var logger = logz.logger().int("$rid", request_id).multiuse();
        defer logger.release();

        var code: i32 = 0;
        var log_request = route_data.log;

        const app = self.app;
        var arc_settings = app.getSettings();
        defer arc_settings.release();

        const settings = &arc_settings.value;

        var env = Env{
            .app = app,
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
                }, .{ .emit_null_optional_fields = false });
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
            },
        };

        if (log_request) {
            logger
                .stringSafe("@l", "REQ")
                .stringSafe("method", @tagName(req.method))
                .stringSafe("route", route_data.name)
                .string("path", req.url.path)
                .int("status", res.status)
                .int("code", code)
                .int("ms", std.time.milliTimestamp() - start_time)
                .log();
        }
    }

    pub fn direct(_: *const Dispatcher, action: httpz.Action(*Env), req: *httpz.Request, res: *httpz.Response) !void {
        return action(undefined, req, res);
    }

    // A routing-related 404 (as opposed to an application-specific one, like a
    // request for a non-existent package). This is passed to the httpz library
    // as a fallback route when no matching route is found.
    pub fn notFound(_: *const Dispatcher, _: *httpz.Request, res: *httpz.Response) !void {
        _ = errors.RouterNotFound.write(res);
    }

    // Dispatcher.dispatch handles any action-related errors (it executes the action
    // with a catch and handles all possibilities.) This is for an error that happens
    // outside of the action, either in the Dispatcher itself or in the underlying
    // httpz library. This should not happen.
    pub fn uncaughtError(_: *const Dispatcher, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        const code = errors.ServerError.write(res);
        logz.err().err(err).ctx("errorHandler").string("path", req.url.raw).int("code", code).log();
    }
};

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
    const body = std.fmt.comptimePrint("{{\"code\": {d}, \"err\": \"not found\", \"desc\": \"{s}\"}}", .{ logdk.codes.NOT_FOUND, desc });
    res.content_type = .JSON;
    res.status = 404;
    res.body = body;
}

// pre-generated error messages
pub const Error = struct {
    code: i32,
    status: u16,
    body: []const u8,

    pub fn init(status: u16, comptime code: i32, comptime message: []const u8) Error {
        const body = std.fmt.comptimePrint("{{\"code\": {d}, \"err\": \"{s}\"}}", .{ code, message });
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
    try tc.web.expectJson(.{ .code = 1 });
}

test "dispatcher: dispatch to actions" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.web.url("/test_1");

    request_counter = 958589;
    const dispatcher = testDispatcher(tc, .{});
    try dispatcher.dispatch(callableAction, tc.web.req, tc.web.res);
    try tc.web.expectStatus(200);
    try tc.web.expectJson(.{ .url = "/test_1" });
}

test "web: Error.write" {
    var tc = t.context(.{});
    defer tc.deinit();

    try t.expectEqual(2, errors.ServerError.write(tc.web.res));
    try tc.web.expectStatus(500);
    try tc.web.expectJson(.{ .code = 2, .err = "internal server error" });
}

test "web: notFound" {
    var tc = t.context(.{});
    defer tc.deinit();

    try notFound(tc.web.res, "no spice");
    try tc.web.expectStatus(404);
    try tc.web.expectJson(.{ .code = 4, .err = "not found", .desc = "no spice" });
}

fn testDispatcher(tc: *t.Context, opts: anytype) Dispatcher {
    _ = opts;
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
        return res.json(.{ .url = req.url.path }, .{});
    }

    unreachable;
}
