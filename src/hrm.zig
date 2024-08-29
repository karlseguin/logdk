const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const logdk = @import("logdk.zig");

// hrm -> http relationalmapping.
//
// This project is essentially taking input from HTTP and mapping it to an SQL
// query, or taking a result from DuckDB and writing it as an HTTP response.
// The code for that is sprinkled throughout, but for any piece of this mapping
// that is needed in more than one place, we put it here.

pub fn writePaging(writer: anytype, page: u16, limit: u16) !void {
    const adjusted_page = if (page == 0) page else page - 1;
    const offset: u32 = @as(u32, adjusted_page) * limit;
    try std.fmt.format(writer, " limit {d} offset {d}", .{ limit, offset });
}

pub fn writeRows(res: *httpz.Response, rows: *zuckdb.Rows, buf: *zul.StringBuilder, logger: logz.Logger) !usize {
    res.content_type = .JSON;
    const writer = buf.writer();
    const vectors = rows.vectors;
    try buf.write("{\n \"cols\": [");
    for (0..vectors.len) |i| {
        try std.json.encodeJsonString(std.mem.span(rows.columnName(i)), .{}, writer);
        try buf.writeByte(',');
    }
    // strip out the last comma
    buf.truncate(1);
    try buf.write("],\n \"types\": [");

    for (vectors) |*vector| {
        try buf.writeByte('"');
        try vector.writeType(writer);
        try buf.write("\",");
    }
    buf.truncate(1);

    try buf.write("],\n \"rows\": [");
    const first_row = (try rows.next()) orelse return 0;

    try buf.write("\n  [");
    try writeRow(&first_row, buf, vectors, logger);

    var row_count: usize = 1;
    while (try rows.next()) |row| {
        buf.writeAssumeCapacity("],\n  [");
        try writeRow(&row, buf, vectors, logger);
        if (@mod(row_count, 50) == 0) {
            try res.chunk(buf.string());
            buf.clearRetainingCapacity();
        }
        row_count += 1;
    }
    try buf.writeByte(']');
    return row_count;
}

fn writeRow(row: *const zuckdb.Row, buf: *zul.StringBuilder, vectors: []zuckdb.Vector, logger: logz.Logger) !void {
    const writer = buf.writer();

    for (vectors, 0..) |*vector, i| {
        switch (vector.type) {
            .list => |list_vector| {
                const list = row.lazyList(i) orelse {
                    try buf.write("null,");
                    continue;
                };
                if (list.len == 0) {
                    try buf.write("[],");
                    continue;
                }

                const child_type = list_vector.child;
                try buf.writeByte('[');
                for (0..list.len) |list_index| {
                    try translateScalar(&list, child_type, list_index, writer, logger);
                    try buf.writeByte(',');
                }
                // overwrite the last trailing comma
                buf.truncate(1);
                try buf.write("],");
            },
            .scalar => |s| {
                try translateScalar(row, s, i, writer, logger);
                try buf.writeByte(',');
            },
        }
    }
    // overwrite the last trailing comma
    buf.truncate(1);
}

// src can either be a zuckdb.Row or a zuckdb.LazyList
fn translateScalar(src: anytype, column_type: zuckdb.Vector.Type.Scalar, i: usize, writer: anytype, logger: logz.Logger) !void {
    if (src.isNull(i)) {
        return writer.writeAll("null");
    }

    switch (column_type) {
        .decimal => try std.fmt.format(writer, "{d}", .{src.get(f64, i)}),
        .@"enum" => unreachable, // TODO
        .simple => |s| switch (s) {
            zuckdb.c.DUCKDB_TYPE_VARCHAR => try std.json.encodeJsonString(src.get([]const u8, i), .{}, writer),
            zuckdb.c.DUCKDB_TYPE_BOOLEAN => try writer.writeAll(if (src.get(bool, i)) "true" else "false"),
            zuckdb.c.DUCKDB_TYPE_TINYINT => try std.fmt.formatInt(src.get(i8, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_SMALLINT => try std.fmt.formatInt(src.get(i16, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_INTEGER => try std.fmt.formatInt(src.get(i32, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_BIGINT => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_HUGEINT => try std.fmt.formatInt(src.get(i128, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_UTINYINT => try std.fmt.formatInt(src.get(u8, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_USMALLINT => try std.fmt.formatInt(src.get(u16, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_UINTEGER => try std.fmt.formatInt(src.get(u32, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_UBIGINT => try std.fmt.formatInt(src.get(u64, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_UHUGEINT => try std.fmt.formatInt(src.get(u128, i), 10, .lower, .{}, writer),
            zuckdb.c.DUCKDB_TYPE_FLOAT => try std.fmt.format(writer, "{d}", .{src.get(f32, i)}),
            zuckdb.c.DUCKDB_TYPE_DOUBLE => try std.fmt.format(writer, "{d}", .{src.get(f64, i)}),
            zuckdb.c.DUCKDB_TYPE_UUID => try std.json.encodeJsonString(&src.get(zuckdb.UUID, i), .{}, writer),
            zuckdb.c.DUCKDB_TYPE_DATE => {
                // std.fmt's integer formatting is broken when dealing with signed integers
                // we use our own formatter
                // https://github.com/ziglang/zig/issues/19488
                const date = src.get(zuckdb.Date, i);
                try std.fmt.format(writer, "\"{d}-{s}-{s}\"", .{ date.year, paddingTwoDigits(date.month), paddingTwoDigits(date.day) });
            },
            zuckdb.c.DUCKDB_TYPE_TIME => {
                // std.fmt's integer formatting is broken when dealing with signed integers
                // we use our own formatter. But for micros, I'm lazy and cast it to unsigned,
                // which std.fmt handles better.
                const time = src.get(zuckdb.Time, i);
                try std.fmt.format(writer, "\"{s}:{s}:{s}.{d:6>0}\"", .{ paddingTwoDigits(time.hour), paddingTwoDigits(time.min), paddingTwoDigits(time.sec), @as(u32, @intCast(time.micros)) });
            },
            zuckdb.c.DUCKDB_TYPE_TIMESTAMP, zuckdb.c.DUCKDB_TYPE_TIMESTAMP_TZ => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
            else => |duckdb_type| {
                try writer.writeAll("\"???\"");
                logger.level(.Warn).ctx("serialize.unknown_type").int("duckdb_type", duckdb_type).log();
            },
        },
    }
}

fn writeVarcharType(result: *zuckdb.c.duckdb_result, column_index: usize, buf: *zul.StringBuilder) !void {
    var logical_type = zuckdb.c.duckdb_column_logical_type(result, column_index);
    defer zuckdb.c.duckdb_destroy_logical_type(&logical_type);
    const alias = zuckdb.c.duckdb_logical_type_get_alias(logical_type);
    if (alias == null) {
        return buf.write("varchar");
    }
    defer zuckdb.c.duckdb_free(alias);
    return buf.write(std.mem.span(alias));
}

fn paddingTwoDigits(value: i8) [2]u8 {
    std.debug.assert(value < 61 and value > 0);
    const digits = "0001020304050607080910111213141516171819" ++
        "2021222324252627282930313233343536373839" ++
        "4041424344454647484950515253545556575859" ++
        "60";
    const index: usize = @intCast(value);
    return digits[index * 2 ..][0..2].*;
}
