const std = @import("std");
const zul = @import("zul");
const httpz = @import("httpz");
const zuckdb = @import("zuckdb");

const logdk = @import("../../../logdk.zig");
const web = logdk.web;
const Event = logdk.Event;

const Allocator = std.mem.Allocator;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const app = env.app;
	const name = req.params.get("name").?;
	_ = app.getDataSet(name) orelse return web.notFound(res, "dataset not found");

	const query = try req.query();
	const include_total = if (query.get("total")) |t| std.mem.eql(u8, t, "true") else false;
	_ = include_total;

	const buf = try app.buffers.acquire();
	defer buf.release();


	// This is relatively safe because the name was validated when the dataset was
	// created. If `name` was invalid/unsafe, then the dataset never wouldn't exist
	// and app.getDataSet above would have returned null;
	try buf.write("select * from \"");
	try buf.write(name);
	try buf.write("\" where \"$id\" >= 56071478 order by \"$id\" desc limit 100");

	var conn = try app.db.acquire();
	defer conn.release();

	var stmt = conn.prepare(buf.string(), .{}) catch |err| switch (err) {
		error.DuckDBError => return web.invalidSQL(res, conn.err, buf.string()),
		else => return err,
	};
	defer stmt.deinit();

	var rows = stmt.query(null) catch |err| switch (err) {
		error.DuckDBError => return web.invalidSQL(res, conn.err, buf.string()),
		else => return err,
	};
	defer rows.deinit();

	const first_row = (try rows.next()) orelse {
		res.body = "{\"cols\":[],\"rows\":[]}";
		return;
	};

	buf.clearRetainingCapacity();
	const writer = buf.writer();

	const aa = res.arena;
	var column_types = try aa.alloc(zuckdb.ParameterType, rows.column_count);
	for (0..column_types.len) |i| {
		column_types[i] = rows.columnType(i);
	}

	try buf.write("{\n \"cols\": [");
	for (0..column_types.len) |i| {
		try std.json.encodeJsonString(std.mem.span(rows.columnName(i)), .{}, writer);
		try buf.writeByte(',');
	}
	// strip out the last comma
	buf.truncate(1);
	try buf.write("],\n \"rows\": [\n  [");
	try res.chunk(buf.string());

	buf.clearRetainingCapacity();
	try serializeRow(&first_row, column_types, buf);
	try res.chunk(buf.string());

	while (try rows.next()) |row| {
		buf.clearRetainingCapacity();
		buf.writeAssumeCapacity("],\n  [");
		try serializeRow(&row, column_types, buf);
		try res.chunk(buf.string());
	}

	try res.chunk("]\n ]\n}");
}

fn serializeRow(row: *const zuckdb.Row, column_types: []zuckdb.ParameterType, buf: *zul.StringBuilder) !void {
	const writer = buf.writer();

	for (column_types, 0..) |column_type, i| {
		if (row.isNull(i)) {
			try buf.write("null,");
		}

		switch (column_type) {
			.list => {
				const list = row.lazyList(i) orelse {
					try buf.write("null,");
					continue;
				};
				_ = list;
				// var typed_list = typed.Array.init(aa);
				// try typed_list.ensureTotalCapacity(list.len);
				// for (0..list.len) |list_index| {
				// 	if (list.isNull(list_index)) {
				// 		typed_list.appendAssumeCapacity(.{.null = {}});
				// 	} else {
				// 		typed_list.appendAssumeCapacity(try translateScalar(aa, &list, list.type, list_index));
				// 	}
				// }
			},
			else => {
				try translateScalar(row, column_type, i, writer);
				try buf.writeByte(',');
			}
		}
	}
	// overwrite the last trailing comma
	buf.truncate(1);
}


// src can either be a zuckdb.Row or a zuckdb.LazyList
fn translateScalar(src: anytype, column_type: zuckdb.ParameterType, i: usize, writer: anytype) !void {
	switch (column_type) {
		.varchar => try std.json.encodeJsonString(src.get([]const u8, i), .{}, writer),
		.bool => try writer.writeAll(if (src.get(bool, i)) "true" else "false"),
		.i8 => try std.fmt.formatInt(src.get(i8, i), 10, .lower, .{}, writer),
		.i16 => try std.fmt.formatInt(src.get(i16, i), 10, .lower, .{}, writer),
		.i32 => try std.fmt.formatInt(src.get(i32, i), 10, .lower, .{}, writer),
		.i64 => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
		.i128 => try std.fmt.formatInt(src.get(i128, i), 10, .lower, .{}, writer),
		.u8 => try std.fmt.formatInt(src.get(u8, i), 10, .lower, .{}, writer),
		.u16 => try std.fmt.formatInt(src.get(u16, i), 10, .lower, .{}, writer),
		.u32 => try std.fmt.formatInt(src.get(u32, i), 10, .lower, .{}, writer),
		.u64 => try std.fmt.formatInt(src.get(u64, i), 10, .lower, .{}, writer),
		.u128 => try std.fmt.formatInt(src.get(u128, i), 10, .lower, .{}, writer),
		.f32 => try std.fmt.format(writer, "{d}", .{src.get(f32, i)}),
		.f64, .decimal => try std.fmt.format(writer, "{d}", .{src.get(f64, i)}),
		.uuid => try std.json.encodeJsonString(&src.get(zuckdb.UUID, i), .{}, writer),
		.date => {
			const date = src.get(zuckdb.Date, i);
			try std.json.stringify(.{
				.year = date.year,
				.month = date.month,
				.day = date.day,
			}, .{}, writer);
		},
		.time, .timetz => {
			const time = src.get(zuckdb.Time, i);
			try std.json.stringify(.{
				.hour = time.hour,
				.min = time.min,
				.sec = time.sec,
			}, .{}, writer);
		},
		.timestamp, .timestamptz => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
		else => try std.fmt.format(writer,  "\"cannot serialize {any}\"", .{column_type}),
	}
}

// fn serializeRow(row: []typed.Value, prefix: []const u8, sb: *zul.StringBuilder, writer: anytype) ![]const u8 {
// 	sb.clearRetainingCapacity();
// 	try sb.write(prefix);
// 	try std.json.stringify(row, .{}, writer);
// 	return sb.string();
// }

// fn releaseBuf(state: *anyopaque) void {
// 	const buf: *zul.StringBuilder = @alignCast(@ptrCast(state));
// 	buf.release();
// }
