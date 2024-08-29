const std = @import("std");
const typed = @import("typed");
const zuckdb = @import("zuckdb");
const validate = @import("validate");
const logdk = @import("logdk.zig");

const Allocator = std.mem.Allocator;

// Given a []typed.Value, we could just iterate through and bind each value to
// a prepared statement. If the value was invalid for the given parameter, we'd
// get an error. But, our error would be a generic DuckDB error string and we much
// prefer our more specific validation errors. They are more structured, and we can
// return an error for all invalid values, not just the first one.
//
// So for each value, we look at the parameter type and apply the appropriate validation
// (which might also do some conversion, e.g. and int64 => i8 if the parameter type is a
// tinyint and the value fits).
//
// In addition to the above, we want to execute 2 prepared statements using the same
// parameters: one for the results and one for the total count. Once validated and
// converted after the first binding, we don't need to validate/convert again for
// the second binding (the total).
//
// Therefore, first you'd call `validateAndBind` on the main prepared statement.
// This would validate/convert the values, bind then, and it mutates the passed in
// []typed.Values with the converted value. Next, if you do want the total count
// (using a different prepared statement with the same paremters), you call `bindValues`
// which only binds (does not validate) the converted values.
pub fn validateAndBind(allocator: Allocator, stmt: zuckdb.Stmt, values: []typed.Value, validator: *logdk.Validate.Context) !void {
    for (values, 0..) |value, index| {
        values[index] = try validateAndBindValue(allocator, stmt, value, index, validator);
    }
}

// assumes values been mutated by first calling validateAndBind
pub fn bindValues(stmt: zuckdb.Stmt, values: []typed.Value) !void {
    for (values, 0..) |value, index| {
        try bindValue(stmt, value, index);
    }
}

fn validateAndBindValue(allocator: Allocator, stmt: zuckdb.Stmt, value: typed.Value, index: usize, validator: *logdk.Validate.Context) !typed.Value {
    if (std.meta.activeTag(value) == typed.Value.null) {
        try stmt.bindValue(null, index);
        return value;
    }

    validator.field = null;
    validator.force_prefix = try fieldName(allocator, index);
    const validated_value = switch (stmt.dataType(index)) {
        .boolean => try bool_validator.validateValue(value, validator),
        .uuid => try uuid_validator.validateValue(value, validator),
        .tinyint => try i8_validator.validateValue(value, validator),
        .smallint => try i16_validator.validateValue(value, validator),
        .integer => try i32_validator.validateValue(value, validator),
        .bigint => try i64_validator.validateValue(value, validator),
        .hugeint => try i128_validator.validateValue(value, validator),
        .utinyint => try u8_validator.validateValue(value, validator),
        .usmallint => try u16_validator.validateValue(value, validator),
        .uinteger => try u32_validator.validateValue(value, validator),
        .ubigint => try u64_validator.validateValue(value, validator),
        .uhugeint => try u128_validator.validateValue(value, validator),
        .real => try f32_validator.validateValue(value, validator),
        .double => try f64_validator.validateValue(value, validator),
        .decimal => try f64_validator.validateValue(value, validator),
        .timestamp, .timestamptz => try i64_validator.validateValue(value, validator),
        .varchar => try string_validator.validateValue(value, validator),
        .blob => try blob_validator.validateValue(value, validator),
        .bit => try bitstring_validator.validateValue(value, validator),
        .@"enum" => try string_validator.validateValue(value, validator),
        .date => try date_validator.validateValue(value, validator),
        .time => try time_validator.validateValue(value, validator),
        .interval => switch (value) {
            .string => try string_validator.validateValue(value, validator),
            else => try interval_validator.validateValue(value, validator),
        },
        else => |tpe| {
            const type_name = @tagName(tpe);
            try validator.add(.{
                .code = logdk.Validate.UNSUPPORTED_PARAMETER_TYPE,
                .err = try std.fmt.allocPrint(allocator, "Unsupported parameter type: {s}", .{type_name}),
                .data = try validator.dataBuilder().put("index", index).put("type", type_name).done(),
            });
            return .{ .null = {} };
        },
    };

    try bindValue(stmt, validated_value, index);
    return validated_value;
}

fn bindValue(stmt: zuckdb.Stmt, value: typed.Value, index: usize) !void {
    switch (value) {
        .date => |v| return stmt.bindValue(zuckdb.Date{
            .year = v.year,
            .month = @intCast(v.month),
            .day = @intCast(v.day),
        }, index),
        .time => |v| return stmt.bindValue(zuckdb.Time{
            .hour = @intCast(v.hour),
            .min = @intCast(v.min),
            .sec = @intCast(v.sec),
            .micros = @intCast(v.micros),
        }, index),
        .map => |v| {
            switch (stmt.dataType(index)) {
                .interval => {
                    return stmt.bindValue(zuckdb.Interval{
                        .months = v.get("months").?.i32,
                        .days = v.get("days").?.i32,
                        .micros = v.get("micros").?.i32,
                    }, index);
                },
                else => unreachable,
            }
        },
        .null => return stmt.bindValue(null, index),
        .array, .timestamp, .datetime => unreachable,
        inline else => |v| return stmt.bindValue(v, index),
    }
}

// As much as possible, we want our validation error message to help pinpoint
// the error. Normally, the validation framework would handle this, but here
// we're validating manually based on the parameter types. So it's a bit more
// tedious.
fn fieldName(allocator: Allocator, i: usize) ![]const u8 {
    return switch (i) {
        0 => "filters.0",
        1 => "filters.1",
        2 => "filters.2",
        3 => "filters.3",
        4 => "filters.4",
        5 => "filters.5",
        6 => "filters.6",
        7 => "filters.7",
        8 => "filters.8",
        9 => "filters.9",
        10 => "filters.10",
        11 => "filters.11",
        12 => "filters.12",
        13 => "filters.13",
        14 => "filters.14",
        15 => "filters.15",
        16 => "filters.16",
        17 => "filters.17",
        18 => "filters.18",
        19 => "filters.19",
        20 => "filters.20",
        21 => "filters.21",
        22 => "filters.22",
        23 => "filters.23",
        24 => "filters.24",
        25 => "filters.25",
        26 => "filters.26",
        27 => "filters.27",
        28 => "filters.28",
        29 => "filters.29",
        30 => "filters.30",
        31 => "filters.31",
        32 => "filters.32",
        33 => "filters.33",
        34 => "filters.34",
        35 => "filters.35",
        36 => "filters.36",
        37 => "filters.37",
        38 => "filters.38",
        39 => "filters.39",
        40 => "filters.40",
        41 => "filters.41",
        42 => "filters.42",
        43 => "filters.43",
        44 => "filters.44",
        45 => "filters.45",
        46 => "filters.46",
        47 => "filters.47",
        48 => "filters.48",
        49 => "filters.49",
        50 => "filters.50",
        else => std.fmt.allocPrint(allocator, "filters.{d}", .{i}),
    };
}

var i8_validator: *validate.Int(i8, void) = undefined;
var i16_validator: *validate.Int(i16, void) = undefined;
var i32_validator: *validate.Int(i32, void) = undefined;
var i64_validator: *validate.Int(i64, void) = undefined;
var i128_validator: *validate.Int(i128, void) = undefined;
var u8_validator: *validate.Int(u8, void) = undefined;
var u16_validator: *validate.Int(u16, void) = undefined;
var u32_validator: *validate.Int(u32, void) = undefined;
var u64_validator: *validate.Int(u64, void) = undefined;
var u128_validator: *validate.Int(u128, void) = undefined;
var f32_validator: *validate.Float(f32, void) = undefined;
var f64_validator: *validate.Float(f64, void) = undefined;
var bool_validator: *validate.Bool(void) = undefined;
var uuid_validator: *validate.UUID(void) = undefined;
var date_validator: *validate.Date(void) = undefined;
var time_validator: *validate.Time(void) = undefined;
var string_validator: *validate.String(void) = undefined;
var blob_validator: *validate.String(void) = undefined;
var bitstring_validator: *validate.String(void) = undefined;
var interval_validator: *validate.Object(void) = undefined;

// Called in init.zig
pub fn init(builder: *validate.Builder(void)) !void {
    // All of these validators are very simple. They largely just assert type
    // correctness.

    // std.json represents large integers are strings (fail), so we need to enable
    // test parsing for those.
    i8_validator = builder.int(i8, .{});
    i16_validator = builder.int(i16, .{});
    i32_validator = builder.int(i32, .{});
    i64_validator = builder.int(i64, .{ .parse = true });
    i128_validator = builder.int(i128, .{ .parse = true });
    u8_validator = builder.int(u8, .{});
    u16_validator = builder.int(u16, .{});
    u32_validator = builder.int(u32, .{});
    u64_validator = builder.int(u64, .{ .parse = true });
    u128_validator = builder.int(u128, .{ .parse = true });
    f32_validator = builder.float(f32, .{});
    f64_validator = builder.float(f64, .{});
    bool_validator = builder.boolean(.{});
    uuid_validator = builder.uuid(.{});
    date_validator = builder.date(.{ .parse = true });
    time_validator = builder.time(.{ .parse = true });
    string_validator = builder.string(.{});
    blob_validator = builder.string(.{ .decode = .base64 });
    bitstring_validator = builder.string(.{ .function = validateBitstring });
    interval_validator = builder.object(&.{
        builder.field("months", builder.int(i32, .{ .default = 0 })),
        builder.field("days", builder.int(i32, .{ .default = 0 })),
        builder.field("micros", builder.int(i64, .{ .default = 0 })),
    }, .{});
}

fn validateBitstring(optional_value: ?[]const u8, context: *validate.Context(void)) !?[]const u8 {
    const value = optional_value orelse return null;
    for (value) |b| {
        if (b != '0' and b != '1') {
            try context.add(.{
                .code = logdk.Validate.INVALID_BITSTRING,
                .err = "bitstring must contain only 0s and 1s",
            });
            return null;
        }
    }
    return value;
}
