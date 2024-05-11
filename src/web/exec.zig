const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../logdk.zig");

const web = logdk.web;

var input_validator: *logdk.Validate.Object = undefined;
pub fn init(builder: *logdk.Validate.Builder) !void {
	input_validator = builder.object(&.{
		builder.field("page", builder.int(u16, .{.parse = true, .min = 1, .default = 1})),
		builder.field("limit", builder.int(u16, .{.parse = true, .min = 1, .max = 1000, .default = 100})),
		builder.field("total", builder.boolean(.{.parse = true, .default = false})),
		builder.field("sql", builder.string(.{.min = 1, .max = 5000, .required = true})),
	}, .{});
}

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const input = try web.validateQuery(req, input_validator, env);

	const page = input.get("page").?.u16;
	const limit = input.get("limit").?.u16;
	const include_total = input.get("total").?.bool;

	// we're going to wrap this in a CTE, so need to strip out any trailing comma
	const sql = stripTrailingSemicolon(input.get("sql").?.string);

	var app = env.app;
	const query = try app.buffers.acquire();
	defer query.release();

	// + 100 because we wrap the SQL in a CTE and add paging
	try query.ensureTotalCapacity(sql.len + 100);
	query.writeAssumeCapacity("with ldk as(");
	query.writeAssumeCapacity(sql);
	query.writeAssumeCapacity(") select ");

	// This position marks the spot where we can change the nature of the query.
	// Initially we'll finish the query off with:
	//   * from ldk limit X offset Y
	// But, if "total=true" in the querystring, we'll issue another query, that'll
	// finish with:
	//    count(*) from ldk"
	// So by marking this spot where the two queries will diverge, we're able
	// to re-use the chunk of buf we've written up until this point
	const magic_buf_pos = query.len();

	query.writeAssumeCapacity("* from ldk");
	try logdk.hrm.writePaging(query.writer(), page, limit);

	// we can't re-use the query buf, because we might need it, intact, to get
	// total count
	const buf = try app.buffers.acquire();
	defer buf.release();

	var conn = try app.db.acquire();
	defer conn.release();

	{
		// Check if (a) the provided SQL is valid and (b) is a read-onl statement.
		// Notice that we do this against the sql input, and not our query buffer.
		// We do this for two reasons.
		//  1 - If the SQL is invalid, the error returned will be based on the
		//      user input, rather than our CTE wrapper. This will result in a less
		//      confusing error message to users.
		//
		//  2 - The statementType() of our builder SQL is always a select statement
		//      even if it wraps a delete/update/whatever, because it's a CTE
		//      which ends in "select * from ldk".
		//
		// Our approach is less efficient, because we prepare the statement twice,
		// here only to validate it, and then we'll prepare+execute it wrapped in our
		// CTE.
		//
		// We could skip this first step and just execute builder.string(). If the
		// SQL is invalid, the error message would still largely be clear.
		// Further, DuckDB only allows CTEs around select statements (for now),
		// so we'd still be guaranteed that the sql is a select. But this approach
		// is cleaner, more explicit and more future proof (the error message
		// when using delete/update/insert in a CTE implies they will be supported
		// at some point)
		var stmt = conn.prepare(sql, .{}) catch |err| switch (err) {
			error.DuckDBError => return web.invalidSQL(res, conn.err, sql),
			else => return err,
		};
		defer stmt.deinit();
		switch (stmt.statementType()) {
			.select, .explain => {},
			else => {
				_ = web.errors.IllegalDBWrite.write(res);
				return;
			},
		}
	}

	{
		var stmt = conn.prepare(try query.stringZ(), .{}) catch |err| {
			// This should not be possible, since we already prepared the query above
			// All we've done is wrapped it in a CTE, which should not fail.
			return env.dbErr("exec", err, conn);
		};
		defer stmt.deinit();

		var rows = stmt.query(null) catch |err| switch (err) {
			error.DuckDBError => return web.invalidSQL(res, conn.err, sql),
			else => return err,
		};
		defer rows.deinit();

		try logdk.hrm.writeRows(res, &rows, buf, env.logger);
	}

	if (include_total) {
		query.pos = magic_buf_pos;
		query.writeAssumeCapacity("count(*) from ldk");
		var row = conn.row(try query.stringZ(), .{}) catch |err| {
			// don't use web.invalidSQL here for two reasons
			// 1 - the response is already partially written
			// 2 - this isn't a user/sql error, it shouldn't be possible for this to fail
			return env.dbErr("exec.count", err, conn);
		} orelse unreachable;
		defer row.deinit();

		try buf.write("\n ],\n \"total\": ");
		try std.fmt.formatInt(row.get(i64, 0), 10, .lower, .{}, buf.writer());
		try buf.write("\n}");
	} else {
		try buf.write("\n]\n}");
	}

	try res.chunk(buf.string());
}

fn stripTrailingSemicolon(sql: []const u8) []const u8 {
	var i : usize = sql.len-1;
	while (i >= 0) : (i -= 1) {
		if (!std.ascii.isWhitespace(sql[i])) break;
	}

	while (i >= 0) : (i -= 1) {
		if (sql[i] != ';') break;
	}
	return sql[0..i+1];
}

const t = logdk.testing;
test "exec: validation" {
	var tc = t.context(.{});
	defer tc.deinit();

	{
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.REQUIRED, .field = "sql"});
	}

	{
		tc.reset();
		tc.web.query("sql", "a" ** 5001);
		try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
		try tc.expectInvalid(.{.code = logdk.Validate.STRING_LEN, .field = "sql"});
	}
}

test "exec: invalid SQL" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.query("sql", "a" ** 5000);
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.web.expectStatus(400);
	try tc.web.expectJson(.{.code = logdk.codes.INVALID_SQL});
}

test "exec: can't mutate" {
	var tc = t.context(.{});
	defer tc.deinit();

	{
		tc.web.query("sql", "delete from logdk.datasets");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectStatus(400);
		try tc.web.expectJson(.{.code = logdk.codes.ILLEGAL_DB_WRITE});
	}

	{
		tc.reset();
		tc.web.query("sql", "update logdk.datasets set name = 'x'");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectStatus(400);
		try tc.web.expectJson(.{.code = logdk.codes.ILLEGAL_DB_WRITE});
	}

	{
		tc.reset();
		tc.web.query("sql", "create table xx (id int)");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectStatus(400);
		try tc.web.expectJson(.{.code = logdk.codes.ILLEGAL_DB_WRITE});
	}

	{
		tc.reset();
		tc.web.query("sql", "drop table logdk.datasets");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectStatus(400);
		try tc.web.expectJson(.{.code = logdk.codes.ILLEGAL_DB_WRITE});
	}
}

test "exec: rows" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.exec("create table data (id integer, name text)", .{});
	try tc.exec("insert into data values (1, 'leto'), (2, 'ghanima')", .{});

	{
		// empty
		tc.web.query("sql", "select * from data where id = 0");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectJson(.{
			.cols = &[_][]const u8{"id", "name"},
			.types = &[_][]const u8{"integer", "varchar"},
			.rows = &[_][]const u8{},
		});
	}

	{
		// single row
		tc.reset();
		tc.web.query("sql", "select * from data where id = 1");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectJson(.{
			.cols = &[_][]const u8{"id", "name"},
			.types = &[_][]const u8{"integer", "varchar"},
			.rows = &[_][]const std.json.Value{
				&[_]std.json.Value{.{.integer = 1}, .{.string = "leto"}}
			},
		});
	}

	{
		// multiple rows
		tc.reset();
		tc.web.query("sql", "select * from data order by id desc");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectJson(.{
			.cols = &[_][]const u8{"id", "name"},
			.types = &[_][]const u8{"integer", "varchar"},
			.rows = &[_][]const std.json.Value{
				&[_]std.json.Value{.{.integer = 2}, .{.string = "ghanima"}},
				&[_]std.json.Value{.{.integer = 1}, .{.string = "leto"}}
			},
		});
	}
}

test "exec: paging" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.exec("create table data (id integer)", .{});
	try tc.exec("insert into data values (1), (2), (3), (4), (5)", .{});

	{
		// empty
		tc.web.query("total", "true");
		tc.web.query("sql", "select * from data where id = 0");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectJson(.{
			.cols = &[_][]const u8{"id"},
			.types = &[_][]const u8{"integer"},
			.rows = &[_][]const u8{},
			.total = 0,
		});
	}

	{
		// single row
		tc.reset();
		tc.web.query("total", "true");
		tc.web.query("sql", "select * from data where id = 1");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectJson(.{
			.cols = &[_][]const u8{"id"},
			.types = &[_][]const u8{"integer"},
			.rows = &[_][]const std.json.Value{
				&[_]std.json.Value{.{.integer = 1}}
			},
			.total = 1,
		});
	}

	{
		tc.reset();
		tc.web.query("total", "true");
		tc.web.query("page", "2");
		tc.web.query("limit", "2");
		tc.web.query("sql", "select * from data order by id desc");
		try handler(tc.env(), tc.web.req, tc.web.res);
		try tc.web.expectJson(.{
			.cols = &[_][]const u8{"id"},
			.types = &[_][]const u8{"integer"},
			.rows = &[_][]const std.json.Value{
				&[_]std.json.Value{.{.integer = 3}},
				&[_]std.json.Value{.{.integer = 2}},
			},
			.total = 5,
		});
	}
}
