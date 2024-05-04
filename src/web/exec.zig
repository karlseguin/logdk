const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../logdk.zig");

const web = logdk.web;

var input_validator: *logdk.Validate.Object = undefined;
pub fn init(builder: *logdk.Validate.Builder) !void {
	input_validator = builder.object(&.{
		builder.field("sql", builder.string(.{.min = 1, .max = 5000, .required = true})),
	}, .{});
}

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const input = try web.validateQuery(req, input_validator, env);

	// we're going to wrap this in a CTE (to prevent update/delete/insert)
	// so we need to strip any trailing comma
	const sql = input.get("sql").?.string;

	var app = env.app;
	var conn = try app.db.acquire();
	defer conn.release();

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

	var rows = stmt.query(null) catch |err| switch (err) {
		error.DuckDBError => return web.invalidSQL(res, conn.err, sql),
		else => return err,
	};
	defer rows.deinit();

	const buf = try app.buffers.acquire();
	defer buf.release();
	try logdk.hrm.writeRows(res, &rows, buf, env.logger);
	try buf.write("\n]\n}");
	try res.chunk(buf.string());
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
