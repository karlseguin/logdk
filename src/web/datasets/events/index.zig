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
	const include_total = if (query.get("total")) |value| std.mem.eql(u8, value, "true") else false;
	_ = include_total;

	const buf = try app.buffers.acquire();
	defer buf.release();

	// This is relatively safe because the name was validated when the dataset was
	// created. If `name` was invalid/unsafe, then the dataset never wouldn't exist
	// and app.getDataSet above would have returned null;
	try buf.write("select * from \"");
	try buf.write(name);
	try buf.write("\"");

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

	res.content_type = .JSON;

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

	if (try rows.next()) |first_row| {
		try buf.write("],\n \"types\": [");

		for (column_types, 0..) |column_type, i| {
			try buf.writeByte('"');
			switch (column_type) {
				.list => try writeListType(rows.result, i, buf),
				.varchar => try writeVarcharType(rows.result, i, buf),
				else => try buf.write(@tagName(column_type)),
			}
			try buf.write("\",");
		}
		buf.truncate(1);
		try buf.write("],\n \"rows\": [");

		try buf.write("\n  [");
		try serializeRow(&first_row, column_types, buf);
		try res.chunk(buf.string());
		buf.clearRetainingCapacity();

		while (try rows.next()) |row| {
			buf.writeAssumeCapacity("],\n  [");
			try serializeRow(&row, column_types, buf);
			try res.chunk(buf.string());
			buf.clearRetainingCapacity();
		}
		try buf.writeByte(']');
	} else {
		try buf.write("],\"rows\":[");
	}

	try buf.write("\n]\n}");
	try res.chunk(buf.string());
}

fn serializeRow(row: *const zuckdb.Row, column_types: []zuckdb.ParameterType, buf: *zul.StringBuilder) !void {
	const writer = buf.writer();

	for (column_types, 0..) |column_type, i| {
		switch (column_type) {
			.list => {
				const list = row.lazyList(i) orelse {
					try buf.write("null,");
					continue;
				};
				if (list.len == 0) {
					try buf.write("[],");
					continue;
				}
				try buf.writeByte('[');
				for (0..list.len) |list_index| {
					try translateScalar(&list, list.type, list_index, writer);
					try buf.writeByte(',');
				}
				// overwrite the last trailing comma
				buf.truncate(1);
				try buf.write("],");
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
	if (src.isNull(i)) {
		return writer.writeAll("null");
	}

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

fn writeListType(result: *zuckdb.c.duckdb_result, column_index: usize, buf: *zul.StringBuilder) !void {
	var logical_type = zuckdb.c.duckdb_column_logical_type(result, column_index);
	defer zuckdb.c.duckdb_destroy_logical_type(&logical_type);

	var child_type = zuckdb.c.duckdb_list_type_child_type(logical_type);
	defer zuckdb.c.duckdb_destroy_logical_type(&child_type);

	const duckdb_type = zuckdb.c.duckdb_get_type_id(child_type);
	const named_type = zuckdb.ParameterType.fromDuckDBType(duckdb_type);
	try buf.write(@tagName(named_type));
	try buf.write("[]");
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

const t = logdk.testing;
test "events.index: unknown dataset" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "nope");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.expectNotFound("dataset not found");
}

test "events.index: empty result" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("ds1", "{\"id\": 1}", false);

	tc.web.param("name", "ds1");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = &[_][]const u8{"$id", "$inserted", "id"},
		.rows = &[_][]const u8{},
	});
}

test "events.index: single row" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet(
		"ds1",
		\\ {
		\\  "int": -30029,
		\\  "uint": 98823,
		\\  "float_pos": 0.392,
		\\  "float_neg": -9949283.44221,
		\\  "true": true,
		\\  "false": false,
		\\  "text": "over 9000",
		\\  "null": null,
		\\  "details": {"message": "1", "tags": [1, 2, 3]},
		\\  "mixed_list": [1, "two", true],
		\\  "list": [0.1, 2.2, -33.33]
		\\ }
	, true);

	tc.web.param("name", "ds1");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = &[_][]const u8{"$id", "$inserted", "details", "false", "float_neg", "float_pos", "int", "list", "mixed_list", "null", "text", "true", "uint"},
		.types = &[_][]const u8{"u32", "timestamptz", "JSON", "bool", "f64", "f64", "i16", "f64[]", "JSON", "varchar", "varchar", "bool", "u32"},
		.rows = &[_][]const std.json.Value{
			&[_]std.json.Value{
				.{.integer = 1},
				.{.integer = try tc.scalar(i64, "select \"$inserted\" from ds1 where \"$id\" = 1", .{})},
				.{.string = "{\"message\": \"1\", \"tags\": [1, 2, 3]}"},
				.{.bool = false},
				.{.float = -9949283.44221},
				.{.float = 0.392},
				.{.integer = -30029},
				.{.array = std.json.Array.fromOwnedSlice(undefined, @constCast(&[_]std.json.Value{
					.{.float = 0.1}, .{.float = 2.2}, .{.float = -33.33},
				}))},
				.{.string = "[1, \"two\", true]"},
				.{.null = {}},
				.{.string = "over 9000"},
				.{.bool = true},
				.{.integer = 98823},
			}
		},
	});
}

test "events.index: multiple rows" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("ds1", "{\"int\": 99}", true);
	try tc.recordEvent("ds1", "{\"int\": 4913}");

	tc.web.param("name", "ds1");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = &[_][]const u8{"$id", "$inserted", "int"},
		.types = &[_][]const u8{"u32", "timestamptz", "u16"},
		.rows = &[_][]const std.json.Value{
			&[_]std.json.Value{
				.{.integer = 1},
				.{.integer = try tc.scalar(i64, "select \"$inserted\" from ds1 where \"$id\" = 1", .{})},
				.{.integer = 99},
			},
			&[_]std.json.Value{
				.{.integer = 2},
				.{.integer = try tc.scalar(i64, "select \"$inserted\" from ds1 where \"$id\" = 2", .{})},
				.{.integer = 4913},
			}
		},
	});
}
