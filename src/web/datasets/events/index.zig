const std = @import("std");
const zul = @import("zul");
const typed = @import("typed");
const httpz = @import("httpz");
const zuckdb = @import("zuckdb");
const validate = @import("validate");

const logdk = @import("../../../logdk.zig");
const web = logdk.web;
const Event = logdk.Event;

const Allocator = std.mem.Allocator;

var input_validator: *logdk.Validate.Object = undefined;
pub fn init(builder: *logdk.Validate.Builder) !void {
	// The "filters" field should be an arary of arrays.
	//   [["id", "e", 10], ["name", "nn"]]
	//
	// translates to:
	//   id = 10 and name is not null
	//
	// Although it's dynamic, it's fairly structured and can mostly be validated
	// by our validation framework.
	// 1 - Filters must be an array
	// 2 - It must contain arrays
	// 3 - Each fo these child arrays must have at least 2 values
	// 4 - The first value must be a column name (string)
	// 5 - The second value must be a valid operation (string)
	// 6 - Any other value depends on the second value (the operation)
	// Further validation can only be done later, once we've prepared the statement
	// and know the parameter types.
	const filter_validator = builder.array(null, .{.function = validateFilter, .min = 2});
	input_validator = builder.object(&.{
		builder.field("page", builder.int(u16, .{.parse = true, .min = 1, .default = 1})),
		builder.field("limit", builder.int(u16, .{.parse = true, .min = 1, .max = 5000, .default = 100})),
		builder.field("total", builder.boolean(.{.parse = true, .default = false})),
		builder.field("order", builder.string(.{.min = 1, .max = logdk.MAX_IDENTIFIER_LEN, .function = validateOrder})),
		builder.field("filters", builder.array(filter_validator, .{.parse = true})),
	}, .{});
}

fn validateOrder(opt_value: ?[]const u8, ctx: *logdk.Validate.Context) anyerror!?[]const u8 {
	const name = opt_value orelse return null;
	logdk.Validate.validateIdentifier("order", name, ctx) catch {};
	return name;
}

// Validates 1 filter. Once we've reached this point, we're sure that
// (a) value is not null
// (b) has a length of at least 2
fn validateFilter(opt_value: ?typed.Array, context: *logdk.Validate.Context) !?typed.Array {
	context.startArray();
	defer context.endArray();

	const filter = opt_value orelse unreachable;
	const items = filter.items;
	std.debug.assert(items.len > 1);

	context.arrayIndex(0);
	switch (items[0]) {
		.string => |column_name| logdk.Validate.validateIdentifier(null, column_name, context) catch {},
		else => try context.add(.{.code = logdk.Validate.TYPE_STRING, .err = "column name must be a string"}),
	}

	context.arrayIndex(1);
	const op = switch (items[1]) {
		.string => |str| std.meta.stringToEnum(QueryBuilder.Operator, str) orelse {
			try context.add(.{.code = logdk.Validate.STRING_CHOICE, .err = "must be one of: e, n, l, le, g, ge, rel"});
			return null;
		},
		else => {
			try context.add(.{.code = logdk.Validate.TYPE_STRING, .err = "must be a string"});
			return null;
		}
	};

	const expected_value_count = switch (op) {
		.e, .n, .l, .le, .g, .ge, .rel => 1,
	};

	// +1 for the column name, +1 for the operator
	const expected_filter_length = 2 + expected_value_count;

	context.arrayIndex(2);
	if (items.len != expected_filter_length) {
		const plural = if (expected_filter_length == 1) "" else "s";
		try context.add(.{
			.code = logdk.Validate.INVALID_FILTER_VALUE_COUNT,
			.data = try context.dataBuilder().put("length", expected_value_count).done(),
			.err = try std.fmt.allocPrint(context.allocator, "must have {d} value{s}", .{expected_value_count, plural}),
		});
		return null;
	}

	// Replace the string operation value with an integer, this is cheaper to turn
	// back tinto a QueryBuikler.Operator later.
	items[1] = .{.i64 = @intFromEnum(op)};
	return filter;
}

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const app = env.app;
	const name = req.params.get("name").?;
	_ = app.getDataSet(name) orelse return web.notFound(res, "dataset not found");

	const input = try web.validateQuery(req, input_validator, env);
	const include_total = input.get("total").?.bool;
	const page = input.get("page").?.u16;
	const limit = input.get("limit").?.u16;

	var builder = try QueryBuilder.init(res.arena, env);
	defer builder.deinit();

	try builder.select("*");
	try builder.from(name);
	if (input.get("filters")) |filters| {
		try builder.filters(filters.array);
	} else {
		try builder.filters(null);
	}

	if (input.get("order")) |value| {
		const order = value.string;
		if (order[0] == '-') {
			try builder.order(order[1..], false);
		} else if (order[0] == '+') {
			try builder.order(order[1..], true);
		} else {
			try builder.order(order, true);
		}
	}
	try builder.paging(page, limit);

	// we can't re-use the builder buf, because we might need it, intact, to get
	// total count
	const buf = try app.buffers.acquire();
	defer buf.release();
	const writer = buf.writer();

	var conn = try app.db.acquire();
	defer conn.release();

	{
		var stmt = conn.prepare(builder.string(), .{}) catch |err| switch (err) {
			error.DuckDBError => return web.invalidSQL(res, conn.err, builder.string()),
			else => return err,
		};
		defer stmt.deinit();

		const validator = try env.validator();
		try logdk.binder.validateAndBind(res.arena, stmt, builder.values.items, validator);
		if (!validator.isValid()) {
			return error.Validation;
		}

		var rows = stmt.query(null) catch |err| switch (err) {
			error.DuckDBError => return web.invalidSQL(res, conn.err, builder.string()),
			else => return err,
		};
		defer rows.deinit();

		res.content_type = .JSON;

		const vectors = rows.vectors;
		try buf.write("{\n \"cols\": [");
		for (0..vectors.len) |i| {
			try std.json.encodeJsonString(std.mem.span(rows.columnName(i)), .{}, writer);
			try buf.writeByte(',');
		}
		// strip out the last comma
		buf.truncate(1);

		if (try rows.next()) |first_row| {
			try buf.write("],\n \"types\": [");

			for (vectors) |*vector| {
				try buf.writeByte('"');
				try vector.writeType(writer);
				try buf.write("\",");
			}
			buf.truncate(1);
			try buf.write("],\n \"rows\": [");

			try buf.write("\n  [");
			try serializeRow(env, &first_row, vectors, buf);
			try res.chunk(buf.string());
			buf.clearRetainingCapacity();

			while (try rows.next()) |row| {
				buf.writeAssumeCapacity("],\n  [");
				try serializeRow(env, &row, vectors, buf);
				try res.chunk(buf.string());
				buf.clearRetainingCapacity();
			}
			try buf.writeByte(']');
		} else {
			try buf.write("],\"rows\":[");
		}
	}

	if (include_total) {
		const select_count_sql = builder.toCount();
		var stmt = conn.prepare(select_count_sql, .{}) catch |err| switch (err) {
			error.DuckDBError => return web.invalidSQL(res, conn.err, select_count_sql),
			else => return err,
		};
		defer stmt.deinit();

		// don't need to revalidate this
		try logdk.binder.bindValues(stmt, builder.values.items);

		var rows = stmt.query(null) catch |err| switch (err) {
			error.DuckDBError => return web.invalidSQL(res, conn.err, select_count_sql),
			else => return err,
		};
		defer rows.deinit();

		// count must return a row
		const row = (try rows.next()) orelse unreachable;

		try buf.write("\n ],\n \"total\": ");
		try std.fmt.formatInt(row.get(i64, 0), 10, .lower, .{}, buf.writer());
		try buf.write("\n}");
	} else {
		try buf.write("\n]\n}");
	}

	try res.chunk(buf.string());
}

fn serializeRow(env: *logdk.Env, row: *const zuckdb.Row, vectors: []zuckdb.Vector, buf: *zul.StringBuilder) !void {
	const writer = buf.writer();

	for (vectors, 0..) |*vector, i| {
		switch (vector.type) {
			.list => |child_type| {
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
					try translateScalar(env, &list, child_type, list_index, writer);
					try buf.writeByte(',');
				}
				// overwrite the last trailing comma
				buf.truncate(1);
				try buf.write("],");
			},
			.scalar => |s| {
				try translateScalar(env, row, s, i, writer);
				try buf.writeByte(',');
			}
		}
	}
	// overwrite the last trailing comma
	buf.truncate(1);
}

// src can either be a zuckdb.Row or a zuckdb.LazyList
fn translateScalar(env: *logdk.Env, src: anytype, column_type: zuckdb.Vector.Type.Scalar, i: usize, writer: anytype) !void {
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
			zuckdb.c.DUCKDB_TYPE_SMALLINT  => try std.fmt.formatInt(src.get(i16, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_INTEGER  => try std.fmt.formatInt(src.get(i32, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_BIGINT  => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_HUGEINT  => try std.fmt.formatInt(src.get(i128, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UTINYINT  => try std.fmt.formatInt(src.get(u8, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_USMALLINT  => try std.fmt.formatInt(src.get(u16, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UINTEGER  => try std.fmt.formatInt(src.get(u32, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UBIGINT  => try std.fmt.formatInt(src.get(u64, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_UHUGEINT  => try std.fmt.formatInt(src.get(u128, i), 10, .lower, .{}, writer),
			zuckdb.c.DUCKDB_TYPE_FLOAT  => try std.fmt.format(writer, "{d}", .{src.get(f32, i)}),
			zuckdb.c.DUCKDB_TYPE_DOUBLE  => try std.fmt.format(writer, "{d}", .{src.get(f64, i)}),
			zuckdb.c.DUCKDB_TYPE_UUID  => try std.json.encodeJsonString(&src.get(zuckdb.UUID, i), .{}, writer),
			zuckdb.c.DUCKDB_TYPE_DATE  => {
				const date = src.get(zuckdb.Date, i);
				try std.json.stringify(.{
					.year = date.year,
					.month = date.month,
					.day = date.day,
				}, .{}, writer);
			},
			zuckdb.c.DUCKDB_TYPE_TIME, zuckdb.c.DUCKDB_TYPE_TIME_TZ => {
				const time = src.get(zuckdb.Time, i);
				try std.json.stringify(.{
					.hour = time.hour,
					.min = time.min,
					.sec = time.sec,
				}, .{}, writer);
			},
			zuckdb.c.DUCKDB_TYPE_TIMESTAMP, zuckdb.c.DUCKDB_TYPE_TIMESTAMP_TZ  => try std.fmt.formatInt(src.get(i64, i), 10, .lower, .{}, writer),
			else => |duckdb_type| {
				try writer.writeAll("???");
				env.logger.level(.Warn).ctx("serialize.unknown_type").int("duckdb_type", duckdb_type).log();
			}
		},
	}
}

fn writeListType(result: *zuckdb.c.duckdb_result, column_index: usize, buf: *zul.StringBuilder) !void {
	var logical_type = zuckdb.c.duckdb_column_logical_type(result, column_index);
	defer zuckdb.c.duckdb_destroy_logical_type(&logical_type);

	var child_type = zuckdb.c.duckdb_list_type_child_type(logical_type);
	defer zuckdb.c.duckdb_destroy_logical_type(&child_type);

	const duckdb_type = zuckdb.c.duckdb_get_type_id(child_type);
	const named_type = zuckdb.DataType.fromDuckDBType(duckdb_type);
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

fn parseInt(comptime T: type, value: ?[]const u8) ?T {
	const v = value orelse return null;
	return std.fmt.parseInt(T, v, 10) catch return null;
}

// The main reason we have a struct for building the query is to support paging.
// If we didn't have paging, we could just glue our query together using a StringBuilder.
// And while that's exactly what we do, to efficiently support paging, we want
// to re-use _most_ of that query. For example, say our query ends up looking like:
//
//  select * from my table where size > $1 order by name desc limit $2 offset $3
//
// We'd like to reuse a good chunk of that and turn it into:
//
//  select count(*) from my table where size > $1
//
// In other words, replace the column list with a count(*) and eliminate any order by
// or limit/offsets.
//
// Thus, the main goal of the QueryBuilder is to efficiently (e.g. reusing
// as much of the buffer as possible) facilitate this.
//
// As a side note: yes, cursor paging has many advantages, but also has drawbacks
// Also, selecting count _with_ the main query, via a window function, is much
// more expensive than doing 2 separate calls.
const QueryBuilder = struct {
	env: *logdk.Env,
	allocator: Allocator,
	cols_end: usize,
	where_end: usize,
	buf: *zul.StringBuilder,
	values: std.ArrayList(typed.Value),

	const Operator = enum {
		e,
		n,
		l,
		le,
		g,
		ge,
		rel,
	};

	fn init(allocator: Allocator, env: *logdk.Env) !QueryBuilder {
		const buf = try env.app.buffers.acquire();
		errdefer buf.release();

		const values = std.ArrayList(typed.Value).init(allocator);
		errdefer values.deinit();

		return .{
			.env = env,
			.buf = buf,
			.cols_end = 0,
			.where_end = 0,
			.values = values,
			.allocator = allocator,
		};
	}

	fn deinit(self: *QueryBuilder) void {
		self.buf.release();
		self.values.deinit();
	}

	fn string(self: *const QueryBuilder) []const u8 {
		return self.buf.string();
	}

	fn select(self: *QueryBuilder, columns: []const u8) !void {
		var buf = self.buf;
		// we want to make sure we have enough space to overwrite this with
		// "select count(*)" if we need to convert this query using toCount()
		try buf.write("         select ");
		try buf.write(columns);
		self.cols_end = buf.len();
	}

	// We expect the table name to have been validated one way or another, and thus
	// this injection to be safe. In fact, the table name is the dataset name
	// and we made sure it existed before ever executing this. And, it can only
	// exist if its valid, thus this should always be safe.
	fn from(self: *QueryBuilder, table: []const u8) !void {
		var buf = self.buf;
		try buf.write(" from \"");
		try buf.write(table);
		try buf.write("\" ");
	}

	fn filters(self: *QueryBuilder, opt_filters: ?typed.Array) !void {
		var buf = self.buf;

		const filters_ = opt_filters orelse {
			self.where_end = buf.len();
			return;
		};

		if (filters_.items.len == 0) {
			self.where_end = buf.len();
			return;
		}

		// reasonable assumption
		try self.values.ensureTotalCapacity(filters_.items.len);

		try buf.write("where ");
		// Much of this has already been validated
		for (filters_.items) |untyped| {
			const filter = untyped.array.items;

			try buf.writeByte('"');
			try buf.write(filter[0].string);
			try buf.writeByte('"');

			const operator: Operator = @enumFromInt(filter[1].i64);
			switch (operator) {
				.e => {
					try buf.write(" = ");
					try self.writePlaceHolderFor(filter[2]);
				},
				.n => {
					try buf.write(" != ");
					try self.writePlaceHolderFor(filter[2]);
				},
				.l => {
					try buf.write(" < ");
					try self.writePlaceHolderFor(filter[2]);
				},
				.le => {
					try buf.write(" <= ");
					try self.writePlaceHolderFor(filter[2]);
				},
				.g => {
					try buf.write(" > ");
					try self.writePlaceHolderFor(filter[2]);
				},
				.ge => {
					try buf.write(" >= ");
					try self.writePlaceHolderFor(filter[2]);
				},
				.rel => unreachable, // todo
			}

			try buf.write(" and ");
		}
		// truncate the trailing " and "
		// we wouldn't be here if we didn't have at least 1 filter
		buf.truncate(5);
		self.where_end = buf.len();
	}

	fn writePlaceHolderFor(self: *QueryBuilder, value: typed.Value) !void {
		try self.values.append(value);
		switch (self.values.items.len) {
			1 => try self.buf.write(" $1 "),
			2 => try self.buf.write(" $2 "),
			3 => try self.buf.write(" $3 "),
			4 => try self.buf.write(" $4 "),
			5 => try self.buf.write(" $5 "),
			6 => try self.buf.write(" $6 "),
			7 => try self.buf.write(" $7 "),
			8 => try self.buf.write(" $8 "),
			9 => try self.buf.write(" $9 "),
			10 => try self.buf.write(" $10 "),
			11 => try self.buf.write(" $11 "),
			12 => try self.buf.write(" $12 "),
			13 => try self.buf.write(" $13 "),
			14 => try self.buf.write(" $14 "),
			15 => try self.buf.write(" $15 "),
			16 => try self.buf.write(" $16 "),
			17 => try self.buf.write(" $17 "),
			18 => try self.buf.write(" $18 "),
			19 => try self.buf.write(" $19 "),
			else => |n| try std.fmt.format(self.buf.writer(), " ${d} ", .{n}),
		}
	}

	fn order(self: *QueryBuilder, column: []const u8, asc: bool) !void {
		var buf = self.buf;
		try buf.write(" order by \"");
		try buf.write(column);
		if (asc) {
			try buf.writeByte('"');
		} else {
			try buf.write("\" desc");
		}
	}

	fn paging(self: *QueryBuilder, page: u16, limit: u16) !void {
		const adjusted_page = if (page == 0) page else page - 1;
		try std.fmt.format(self.buf.writer(), " limit {d} offset {d}", .{limit, adjusted_page * limit});
	}

	fn toCount(self: *QueryBuilder) []const u8 {
		// the underlying []u8 of our StringBuilder
		var buf = self.buf.buf;

		const select_count = "select count(*) ";
		const start = self.cols_end - select_count.len;
		// We wrote our select statament with padding so that even if it was a
		// "select *", we have enough space now for "select count(*) "
		std.debug.assert(start > 0);
		@memcpy(buf[start..start + select_count.len], select_count);

		return buf[start..self.where_end];
	}
};

const t = logdk.testing;
test "events.index: unknown dataset" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "nope");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.expectNotFound("dataset not found");
}

test "events.index: simple validation" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("ds1", "{\"id\": 1}", false);
	tc.web.param("name", "ds1");
	tc.web.query("page", "0");
	tc.web.query("limit", "10001");
	tc.web.query("total", "what");
	tc.web.query("order", "ha\"ck");

	try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = logdk.Validate.INT_MIN, .field = "page"});
	try tc.expectInvalid(.{.code = logdk.Validate.INT_MAX, .field = "limit"});
	try tc.expectInvalid(.{.code = logdk.Validate.TYPE_BOOL, .field = "total"});
	try tc.expectInvalid(.{.code = logdk.Validate.INVALID_IDENTIFIER, .field = "order"});
}

test "events.index: filter validation" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("ds1", "{\"id\": 1}", false);

	{
		tc.web.param("name", "ds1");
		tc.web.query("filters", "{}");
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.TYPE_ARRAY, .field = "filters"});
	}

	{
		tc.reset();
		tc.web.param("name", "ds1");
		tc.web.query("filters", "[[]]");
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.ARRAY_LEN_MIN, .field = "filters.0", .data = .{.min = 2}});
	}

	{
		tc.reset();
		tc.web.param("name", "ds1");
		tc.web.query("filters", "[[\"inv\\\"alid\", null]]");
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.INVALID_IDENTIFIER, .field = "filters.0.0"});
		try tc.expectInvalid(.{.code = logdk.Validate.TYPE_STRING, .field = "filters.0.1"});
	}

	{
		tc.reset();
		tc.web.param("name", "ds1");
		tc.web.query("filters", "[[23, \"xx\"]]");
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.TYPE_STRING, .field = "filters.0.0"});
		try tc.expectInvalid(.{.code = logdk.Validate.STRING_CHOICE, .field = "filters.0.1"});
	}

	{
		// not enough values
		tc.reset();
		tc.web.param("name", "ds1");
		tc.web.query("filters", "[[\"a\", \"e\"]]");
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.INVALID_FILTER_VALUE_COUNT, .field = "filters.0.2"});
	}

	{
		// too many values
		tc.reset();
		tc.web.param("name", "ds1");
		tc.web.query("filters", "[[\"a\", \"e\", 0, 1]]");
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.INVALID_FILTER_VALUE_COUNT, .field = "filters.0.2"});
	}
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
		.types = &[_][]const u8{"ubigint","timestamp","varchar","boolean","double","double","smallint","double[]","varchar","varchar","varchar","boolean","uinteger"},
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
	tc.web.query("order", "-$id");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectJson(.{
		.cols = &[_][]const u8{"$id", "$inserted", "int"},
		.types = &[_][]const u8{"ubigint", "timestamp", "usmallint"},
		.rows = &[_][]const std.json.Value{
			&[_]std.json.Value{
				.{.integer = 2},
				.{.integer = try tc.scalar(i64, "select \"$inserted\" from ds1 where \"$id\" = 2", .{})},
				.{.integer = 4913},
			},
			&[_]std.json.Value{
				.{.integer = 1},
				.{.integer = try tc.scalar(i64, "select \"$inserted\" from ds1 where \"$id\" = 1", .{})},
				.{.integer = 99},
			},
		},
	});
}

test "events.index: single row with total" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("ds_count", "{\"int\": 44}", true);
	tc.web.param("name", "ds_count");
	tc.web.query("total", "true");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectJson(.{
		.cols = &[_][]const u8{"$id", "$inserted", "int"},
		.total = 1,
	});
}

test "events.index: multiple row with total" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("ds_count", "{\"int\": 44}", true);
	try tc.recordEvent("ds_count", "{\"int\": 22}");
	try tc.recordEvent("ds_count", "{\"int\": 144}");
	tc.web.param("name", "ds_count");
	tc.web.query("total", "true");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectJson(.{
		.cols = &[_][]const u8{"$id", "$inserted", "int"},
		.total = 3,
	});
}

test "events.index: paging" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("events", "[{\"x\": 1}, {\"x\": 2}, {\"x\": 3}, {\"x\": 4}, {\"x\": 5}]", true);

	{
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("order", "-x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(5, rows[0].array.items[2].i64);
		try t.expectEqual(4, rows[1].array.items[2].i64);
	}

	{
		// same as above, but with an explicit page
		tc.reset();
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("page", "1");
		tc.web.query("order", "-x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(5, rows[0].array.items[2].i64);
		try t.expectEqual(4, rows[1].array.items[2].i64);
	}

	{
		// page 2
		tc.reset();
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("page", "2");
		tc.web.query("order", "-x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(3, rows[0].array.items[2].i64);
		try t.expectEqual(2, rows[1].array.items[2].i64);
	}
}

test "events.index: order" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("events", "[{\"x\": 2}, {\"x\": 1}, {\"x\": 5}, {\"x\": 4}, {\"x\": 3}]", true);

	{
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("order", "-x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(5, rows[0].array.items[2].i64);
		try t.expectEqual(4, rows[1].array.items[2].i64);
	}

	{
		tc.reset();
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("order", "+x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(1, rows[0].array.items[2].i64);
		try t.expectEqual(2, rows[1].array.items[2].i64);
	}

	{
		tc.reset();
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("page", "2");
		tc.web.query("order", "x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(3, rows[0].array.items[2].i64);
		try t.expectEqual(4, rows[1].array.items[2].i64);
	}
}

test "events.index: filter" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("events", "[{\"x\": 2}, {\"x\": 1}, {\"x\": 5}, {\"x\": 4}, {\"x\": 3}]", true);

	{
		tc.web.param("name", "events");
		tc.web.query("limit", "2");
		tc.web.query("filters", "[[\"x\", \"g\", 2]]");
		tc.web.query("order", "x");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(2, rows.len);
		try t.expectEqual(3, rows[0].array.items[2].i64);
		try t.expectEqual(4, rows[1].array.items[2].i64);
	}

	{
		tc.reset();
		tc.web.param("name", "events");
		tc.web.query("limit", "3");
		tc.web.query("order", "+x");
		tc.web.query("total", "true");
		tc.web.query("filters", "[[\"x\", \"ge\", 2], [\"x\", \"n\", 4]]");
		try handler(tc.env(), tc.web.req, tc.web.res);
		const res = try typed.fromJson(tc.arena, try tc.web.getJson());
		const rows = res.map.get("rows").?.array.items;
		try t.expectEqual(3, rows.len);
		try t.expectEqual(2, rows[0].array.items[2].i64);
		try t.expectEqual(3, rows[1].array.items[2].i64);
		try t.expectEqual(5, rows[2].array.items[2].i64);
	}
}
