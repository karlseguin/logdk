const std = @import("std");
const builtin = @import("builtin");

const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const logdk = @import("../../../logdk.zig");

const web = logdk.web;
const argon2 = std.crypto.pwhash.argon2;

const ARGON_CONFIG = if (builtin.is_test) argon2.Params.fromLimits(1, 1024) else argon2.Params.interactive_2id;

var input_validator: *logdk.Validate.Object = undefined;
pub fn init(builder: *logdk.Validate.Builder) !void {
    const permission = builder.string(.{ .choices = &.{ "admin", "raw_query" } });
    input_validator = builder.object(&.{
        builder.field("username", builder.string(.{ .required = true, .min = 1, .max = 50 })),
        builder.field("password", builder.string(.{ .required = true, .min = 6, .max = 50 })),
        builder.field("permissions", builder.array(permission, .{ .required = true })),
    }, .{});
}

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
    const input = try web.validateJson(req, input_validator, env);

    const username = input.get("username").?.string;
    const permissions = try std.json.stringifyAlloc(req.arena, input.get("permissions").?.array.items, .{});

    var pw_buf: [200]u8 = undefined;
    const password = argon2.strHash(input.get("password").?.string, .{
        .allocator = req.arena,
        .params = ARGON_CONFIG,
    }, &pw_buf) catch unreachable;

    // we can't bind arrays to duckdb prepared statements (yet), but we can insert
    // json and let it convert.

    var app = env.app;
    var conn = try app.db.acquire();
    defer conn.release();

    _ = conn.exec("insert into logdk.users (username, password, permissions, enabled) values ($1, $2, $3::json, true)", .{ username, password, permissions }) catch |err| {
        if (conn.err) |duckdb_err| {
            if (zuckdb.isDuplicate(duckdb_err)) {
                (try env.validator()).addInvalidField(.{
                    .field = "username",
                    .err = "is in use",
                    .code = logdk.Validate.USERNAME_IN_USE,
                });
                return error.Validation;
            }
        }
        return logdk.dbErr("Users.create", err, conn, env.logger);
    };

    res.status = 201;
}

const t = logdk.testing;
test "users.create: validation required" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.web.body("{}");
    try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
    try tc.expectInvalid(.{ .code = logdk.Validate.REQUIRED, .field = "username" });
    try tc.expectInvalid(.{ .code = logdk.Validate.REQUIRED, .field = "password" });
    try tc.expectInvalid(.{ .code = logdk.Validate.REQUIRED, .field = "permissions" });
}

test "users.create: validation type" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.web.json(.{ .username = 123, .password = true, .permissions = "nope" });
    try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
    try tc.expectInvalid(.{ .code = logdk.Validate.TYPE_STRING, .field = "username" });
    try tc.expectInvalid(.{ .code = logdk.Validate.TYPE_STRING, .field = "password" });
    try tc.expectInvalid(.{ .code = logdk.Validate.TYPE_ARRAY, .field = "permissions" });
}

test "users.create: validation ranges" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.web.json(.{
        .username = "a" ** 51,
        .password = "12345",
        .permissions = &[_][]const u8{ "admin", "wrong" },
    });
    try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
    try tc.expectInvalid(.{ .code = logdk.Validate.STRING_LEN, .field = "username" });
    try tc.expectInvalid(.{ .code = logdk.Validate.STRING_LEN, .field = "password" });
    try tc.expectInvalid(.{ .code = logdk.Validate.STRING_CHOICE, .field = "permissions.1" });
}

test "users.create: duplicate username" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.factory.user(.{ .id = 10, .username = "paul" });

    tc.web.json(.{
        .username = "paul",
        .password = "atreides",
        .permissions = &[_][]const u8{},
    });

    try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
    try tc.expectInvalid(.{ .code = logdk.Validate.USERNAME_IN_USE, .field = "username" });
}

test "users.create: success" {
    var tc = t.context(.{});
    defer tc.deinit();

    tc.web.json(.{
        .username = "leto",
        .password = "ghanima",
        .permissions = &[_][]const u8{"raw_query"},
    });

    try handler(tc.env(), tc.web.req, tc.web.res);

    var row = (try tc.row("select id, username, password, enabled, permissions::json, created from logdk.users", .{})) orelse unreachable;
    defer row.deinit();

    try t.expectEqual(1, row.get(u32, 0));
    try t.expectEqual("leto", row.get([]const u8, 1));
    try argon2.strVerify(row.get([]const u8, 2), "ghanima", .{ .allocator = tc.arena });
    try t.expectEqual(true, row.get(bool, 3));
    try t.expectEqual("[\"raw_query\"]", row.get([]const u8, 4));
    try t.expectDelta(std.time.microTimestamp(), row.get(i64, 5), 10_000);
}
