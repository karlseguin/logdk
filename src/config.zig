const std = @import("std");
const logz = @import("logz");
const httpz = @import("httpz");
const validate = @import("validate");

pub const Config = struct {
    db: DB = .{
        .path = "db.duckdb",
        .pool_size = 20,
    },

    http: httpz.Config = .{
        .port = 7724,
        .address = "127.0.0.1",
    },

    buffers: Buffer = .{
        .count = 32,
        .size = 32_768,
    },

    log_http: LogHTTP = .smart,
    logger: logz.Config = .{},
    validator: validate.Config = .{},

    pub const LogHTTP = enum {
        none,
        all,
        smart,
    };

    const Buffer = struct {
        count: u16 = 32,
        size: u32 = 32_768,
    };

    const DB = struct {
        path: []const u8,
        pool_size: u16 = 20,
        pool_timeout: u32 = 10 * std.time.ms_per_s,
    };
};
