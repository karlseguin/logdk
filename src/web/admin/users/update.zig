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
        builder.field("password", builder.string(.{ .min = 6, .max = 50 })),
        builder.field("permissions", builder.array(permission, .{ .required = true })),
        builder.field("enabled", builder.boolean(.{ .required = true })),
    }, .{});
}

// Updates in DuckDB have a bunch of limitations. So for now, this route is
// disabled.
// https://duckdb.org/docs/sql/indexes#over-eager-unique-constraint-checking
// (there are other, poorly documented cases, where this also causes issues
// such as updating a list, which we do with permissions)
pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
    const input = try web.validateJson(req, input_validator, env);
    const id = try web.parseInt(u32, "id", req.param("id").?, env);

    const username = input.get("username").?.string;
    const permissions = try std.json.stringifyAlloc(req.arena, input.get("permissions").?.array.items, .{});
    const enabled = input.get("enabled").?.bool;

    var pw_buf: [200]u8 = undefined;
    var password: ?[]const u8 = null;
    if (input.get("password")) |pw| {
        password = argon2.strHash(pw.string, .{ .allocator = req.arena, .params = ARGON_CONFIG }, &pw_buf) catch unreachable;
    }

    var conn = try env.app.db.acquire();
    defer conn.release();

    const count = conn.exec("update logdk.users set username = $1, password = coalesce($2, password), permissions = $3::json, enabled = $4 where id = $5", .{ username, password, permissions, enabled, id }) catch |err| {
        return logdk.dbErr("Users.update", err, conn, env.logger);
    };

    if (count == 0) {
        return web.notFound(res, "user not found");
    }

    res.status = 204;
}

// const t = logdk.testing;
// test "users.update: validation required" {
//  var tc = t.context(.{});
//  defer tc.deinit();

//  tc.web.body("{}");
//  tc.web.param("id", "1");
//  try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
//  try tc.expectInvalid(.{.code = logdk.Validate.REQUIRED, .field = "username"});
//  try tc.expectInvalid(.{.code = logdk.Validate.REQUIRED, .field = "permissions"});
//  try tc.expectInvalid(.{.code = logdk.Validate.REQUIRED, .field = "enabled"});
// }

// test "users.update: validation type" {
//  var tc = t.context(.{});
//  defer tc.deinit();

//  tc.web.json(.{
//    .username = 123,
//    .password = true,
//    .permissions = "nope",
//    .enabled = 2.2,
//  });
//  try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
//  try tc.expectInvalid(.{.code = logdk.Validate.TYPE_STRING, .field = "username"});
//  try tc.expectInvalid(.{.code = logdk.Validate.TYPE_STRING, .field = "password"});
//  try tc.expectInvalid(.{.code = logdk.Validate.TYPE_ARRAY, .field = "permissions"});
//  try tc.expectInvalid(.{.code = logdk.Validate.TYPE_BOOL, .field = "enabled"});
// }

// test "users.update: validation ranges" {
//  var tc = t.context(.{});
//  defer tc.deinit();

//  tc.web.json(.{
//    .username = "a" ** 51,
//    .password = "12345",
//    .permissions = &[_][]const u8{"admin", "wrong"},
//  });
//  try t.expectEqual(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
//  try tc.expectInvalid(.{.code = logdk.Validate.STRING_LEN, .field = "username"});
//  try tc.expectInvalid(.{.code = logdk.Validate.STRING_LEN, .field = "password"});
//  try tc.expectInvalid(.{.code = logdk.Validate.STRING_CHOICE, .field = "permissions.1"});
// }

// test "users.update: unknown id" {
//  var tc = t.context(.{});
//  defer tc.deinit();

//  tc.web.param("id", "3123");
//  tc.web.json(.{
//    .enabled = true,
//    .username = "paul",
//    .password = "atreides",
//    .permissions = &[_][]const u8{},
//  });

//  try handler(tc.env(), tc.web.req, tc.web.res);
//  try tc.web.expectStatus(404);
// }

// test "users.update: without password" {
//  var tc = t.context(.{});
//  defer tc.deinit();

//  tc.factory.user(.{.id = 955, .enabled = false, .password = "origianl"});

//  tc.web.param("id", "955");
//  tc.web.json(.{
//    .enabled = true,
//    .username = "paul",
//    .permissions = &[_][]const u8{"admin"},
//  });

//  try handler(tc.env(), tc.web.req, tc.web.res);
//  try tc.web.expectStatus(200);

//  var row = (try tc.row("select username, password, enabled, permissions::json from logdk.users where id = 955", .{})) orelse unreachable;
//  defer row.deinit();

//  try t.expectEqual("paul", row.get([]const u8, 0));
//  try argon2.strVerify(row.get([]const u8, 1), "original", .{.allocator = tc.arena});
//  try t.expectEqual(false, row.get(bool, 2));
//  try t.expectEqual("[\"admin\"]", row.get([]const u8, 3));
// }

// test "users.update: with password" {
//  var tc = t.context(.{});
//  defer tc.deinit();

//  tc.factory.user(.{.id = 1, .enabled = true, .password = "original", .permissions = &[_][]const u8{"admin"}});

//  tc.web.param("id", "1");
//  tc.web.json(.{
//    .enabled = false,
//    .username = "leto",
//    .password = "newnew",
//    .permissions = &[_][]const u8{},
//  });

//  try handler(tc.env(), tc.web.req, tc.web.res);
//  try tc.web.expectStatus(200);

//  var row = (try tc.row("select username, password, enabled, permissions::json from logdk.users where id = 1", .{})) orelse unreachable;
//  defer row.deinit();

//  try t.expectEqual("leto", row.get([]const u8, 0));
//  try argon2.strVerify(row.get([]const u8, 1), "newnew", .{.allocator = tc.arena});
//  try t.expectEqual(true, row.get(bool, 2));
//  try t.expectEqual("[]", row.get([]const u8, 3));
// }
