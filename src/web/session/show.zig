const std = @import("std");
const httpz = @import("httpz");

const logdk = @import("../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
    const user = env.user;
    // in single user mode (the default) there is no auth and every request
    // is treated as though it was an admin user.
    return res.json(.{ .user_id = user.id, .permissions = .{
        .admin = user.permission_admin or env.settings.single_user,
        .raw_query = user.permission_raw_query,
    } }, .{});
}

const t = logdk.testing;
test "session show: null user single user mode OFF" {
    var tc = t.context(.{});
    defer tc.deinit();
    try tc.app._settings.setValue(.{ .single_user = false });

    try handler(tc.env(), tc.web.req, tc.web.res);
    try tc.web.expectJson(.{
        .user_id = 0,
        .permissions = .{
            .admin = false,
            .raw_query = false,
        },
    });
}

test "session show: null user single user mode ON" {
    var tc = t.context(.{});
    defer tc.deinit();
    try tc.app._settings.setValue(.{ .single_user = true });

    try handler(tc.env(), tc.web.req, tc.web.res);
    try tc.web.expectJson(.{
        .user_id = 0,
        .permissions = .{
            .admin = true,
            .raw_query = false,
        },
    });
}

test "session show: logged in user" {
    var tc = t.context(.{});
    defer tc.deinit();
    try tc.app._settings.setValue(.{ .single_user = false });

    {
        try handler(tc.envWithUser(.{ .id = 33, .permission_admin = true }), tc.web.req, tc.web.res);
        try tc.web.expectJson(.{
            .user_id = 33,
            .permissions = .{
                .admin = true,
                .raw_query = false,
            },
        });
    }

    {
        tc.reset();
        try handler(tc.envWithUser(.{ .id = 34, .permission_raw_query = true }), tc.web.req, tc.web.res);
        try tc.web.expectJson(.{
            .user_id = 34,
            .permissions = .{
                .admin = false,
                .raw_query = true,
            },
        });
    }
}
