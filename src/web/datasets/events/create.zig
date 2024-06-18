const std = @import("std");
const httpz = @import("httpz");
const zuckdb = @import("zuckdb");

const Allocator = std.mem.Allocator;

const logdk = @import("../../../logdk.zig");
const web = logdk.web;
const Event = logdk.Event;

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	if (env.settings.create_tokens and validateToken(env, req, res) == false) {
		// validateToken writes the response
		return;
	}

	const app = env.app;
	const name = req.params.get("name").?;

	var arc = app.getDataSet(name) orelse blk: {
		if (env.settings.dataset_creation == false) {
			return web.notFound(res, "dataset not found and dynamic creation is disabled");
		}
		break :blk null;
	};

	defer if (arc) |a| a.release();

	var body = req.body() orelse {
		res.status = 204;
		return;
	};

	if (req.header("content-encoding")) |ce| {
		// decodeBody either returns the decoded body, or null on error
		// If null is returned, decodeBody wrote the error response already
		body = decodeBody(env, req.arena, body, ce, res) orelse return;
	}


	const event_list = Event.parse(app.allocator, body) catch return error.InvalidJson;
	if (event_list.events.len == 0) {
		event_list.deinit();
		res.status = 204;
		return;
	}

	// once passed to the dispatcher, it becomes the datasets job to release this
	errdefer event_list.deinit();

	if (arc) |a| {
		// This DataSet already exists. That means we have a list of fields which we've
		// previously been able to parse. Let's try to parse them again.
		const ds = a.value;
		for (ds.parsed_fields) |field| {
			for (event_list.events) |event| {
				if (event.map.getPtr(field)) |value| {
					if (value.tryParse()) |parsed| {
						value.* = parsed;
					}
				}
			}
		}
	} else {
		const validator = try env.validator();
		logdk.Validate.validateIdentifier("dataset", name, validator) catch |err| {
			// Even though this is (most likely) user-input error, we want to log it
			// since creating datasets is fairly uncommon and intentional and the creator
			// would probably be surprised to find out that it failed.
			env.logger.level(.Warn).ctx("events.create.dataset.name").string("name", name).log();
			return err;
		};
		arc = try app.createDataSet(env, name, event_list.events[0]);
	}

	app.dispatcher.send(logdk.DataSet, arc.?.value.actor_id, .{.record = event_list});
	res.status = 204;
}

const MISSING_TOKEN = web.Error.init(401, logdk.codes.MISSING_TOKEN, "access token required to create events");
const INVALID_TOKEN = web.Error.init(403, logdk.codes.INVALID_TOKEN, "access token is invalid");
const UNSUPPORTED_CONTENT_ENCODING = web.Error.init(400, logdk.codes.UNSUPPORTED_CONTENT_ENCODING, "content-encoding is not supported");
const DECOMPRESSION_ERROR = web.Error.init(400, logdk.codes.DECOMPRESSION_ERROR, "failed to decompress event(s)");

fn validateToken(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) bool {
	const token = req.header("token") orelse {
		_ = MISSING_TOKEN.write(res);
		return false;
	};

	if (env.app.tokens.contains(token) == false) {
		_ = INVALID_TOKEN.write(res);
		return false;
	}
	return true;
}

fn decodeBody(env: *logdk.Env, allocator: Allocator, body: []const u8, encoding: []const u8, res: *httpz.Response) ?[]const u8 {
	if (std.mem.eql(u8, encoding, "zstd")) {
		return std.compress.zstd.decompress.decodeAlloc(allocator, body, true, 8_388_608) catch |err| {
			env.logger.level(.Error).ctx("events.create.decode").stringSafe("type", "zstd").err(err).log();
			_ = DECOMPRESSION_ERROR.write(res);
			return null;
		};
	}

	env.logger.level(.Warn).ctx("events.create.content-encoding").string("content-encoding", encoding).log();
	_ = UNSUPPORTED_CONTENT_ENCODING.write(res);
	return null;
}

const t = logdk.testing;
test "events.create: unknown dataset, dynamic creation disabled" {
	var tc = t.context(.{});
	defer tc.deinit();
	try tc.app._settings.setValue(.{.dataset_creation = false});

	tc.web.param("name", "nope");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try tc.expectNotFound("dataset not found and dynamic creation is disabled");
}

test "events.create: empty body" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "table_x");
	tc.web.body("{}");
	try handler(tc.env(), tc.web.req, tc.web.res);
	try t.expectEqual(null, try tc.row("select * from duckdb_tables where table_name = 'table_x'", .{}));
}

test "events.create: invalid json body" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.web.param("name", "table_x");

	tc.web.body("{hi");
	try t.expectError(error.InvalidJson, handler(tc.env(), tc.web.req, tc.web.res));
}

test "events.create: unknown dataset, dynamic create invalid dataset name" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.silenceLogs();

	tc.web.param("name", "n\"0p3!");
	tc.web.body("{\"id\": 1}");
	try t.expectError(error.Validation, handler(tc.env(), tc.web.req, tc.web.res));
	try tc.expectInvalid(.{.code = 5000, .field = "dataset"});
}

test "events.create: creates dataset and event" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "logx_x");
	tc.web.json(.{
		.id = 552,
		.active = true,
		.category = "system",
		.tags = &[_]std.json.Value{.{.string = "teg"}, .{.float = 1.32}},
		.zrecord = &[_]std.json.Value{.{.integer = 394}, .{.float = 590.1}},
		.details = .{
			.over = 9000.1,
		},
		.date = "2024-10-30",
		.uuid = "d772b20f-6948-4b20-9485-369399e97068",
	});
	try handler(tc.env(), tc.web.req, tc.web.res);

	tc.flushMessages();

	const row = (try tc.row("select * from logx_x", .{})).?;
	defer row.deinit();

	try t.expectEqual(1, row.get(u64, 0));                                               // ldk_id
	try t.expectDelta(std.time.microTimestamp(), row.get(i64, 1), 5000);                 // ldk_ts
	try t.expectEqual(true, row.get(bool, 2));                                           // active
	try t.expectEqual("system", row.get([]const u8, 3));                                 // category
	try t.expectEqual(.{.year = 2024, .month = 10, .day = 30}, row.get(zuckdb.Date, 4));    // date
	try t.expectEqual("{\"over\":9.0001e3}", row.get([]const u8, 5));                    // details
	try t.expectEqual(552, row.get(u16, 6));                                             // id
	try t.expectEqual("[\"teg\",1.32e0]", row.get([]const u8, 7));                       // tags
	try t.expectEqual("d772b20f-6948-4b20-9485-369399e97068", &row.get(zuckdb.UUID, 8)); // uuid

	const list = row.list(f64, 9).?;                                                     // zrecord
	try t.expectEqual(2, list.len);
	try t.expectEqual(394, list.get(0));
	try t.expectEqual(590.1, list.get(1));
}

test "events.create: event fields are case-insensitive" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "casing");
	tc.web.body(
		\\ [
		\\  {"Id": 1, "name": "teg"},
		\\  {"id": 2, "NAME": "duncan"}
		\\ ]
	);
	try handler(tc.env(), tc.web.req, tc.web.res);
	tc.flushMessages();

	var rows = try tc.query("select id, name from casing order by id", .{});
	defer rows.deinit();

	{
		var row = (try rows.next()).?;
		try t.expectEqual(1, row.get(u8, 0));
		try t.expectEqual("teg", row.get([]const u8, 1));
	}

	{
		var row = (try rows.next()).?;
		try t.expectEqual(2, row.get(u8, 0));
		try t.expectEqual("duncan", row.get([]const u8, 1));
	}
}

test "events.create: attemps to re-parse previously parsed fields" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("reparser", "{\"id\": 1, \"active\": \"true\", \"value\": \"223\", \"at\":\"2024-05-19\", \"over\": \"9000!\"}", true);

	tc.web.param("name", "reparser");
	tc.web.body(
		\\ [
		\\  {"id": 2, "active": "false", "value": "-991944", "over": 4},
		\\  {"id": 3, "at": "2030-02-10", "over": 123.33}
		\\ ]
	);
	try handler(tc.env(), tc.web.req, tc.web.res);
	tc.flushMessages();

	{
		var row = (try tc.row("select active, value, at, over from reparser where id = 2", .{})).?;
		defer row.deinit();
		try t.expectEqual(false, row.get(bool, 0));
		try t.expectEqual(-991944, row.get(i32, 1));
		try t.expectEqual(null, row.get(?zuckdb.Date, 2));
		try t.expectEqual("4", row.get([]u8, 3));
	}

	{
		var row = (try tc.row("select active, value, at, over from reparser where id = 3", .{})).?;
		defer row.deinit();
		try t.expectEqual(null, row.get(?bool, 0));
		try t.expectEqual(null, row.get(?i32, 1));
		try t.expectEqual(.{.year = 2030, .month = 2, .day = 10}, row.get(zuckdb.Date, 2));
		try t.expectEqual("1.2333e2", row.get([]u8, 3));
	}
}

test "events.create: unparsable field disabled column parsed flag" {
	var tc = t.context(.{});
	defer tc.deinit();

	try tc.createDataSet("unparser", "{\"id\": 1, \"active\": \"true\", \"value\": \"223\"}", true);

	tc.web.param("name", "unparser");
	tc.web.body("{\"id\": 2, \"active\": \"false\", \"value\": \"nope\"}");
	try handler(tc.env(), tc.web.req, tc.web.res);
	tc.flushMessages();

	{
		var row = (try tc.row("select active, value from unparser where id = 2", .{})).?;
		defer row.deinit();
		try t.expectEqual(false, row.get(bool, 0));
		try t.expectEqual("nope", row.get([]u8, 1));
	}

	{
		// verify our original while we're at it
		var row = (try tc.row("select active, value from unparser where id = 1", .{})).?;
		defer row.deinit();
		try t.expectEqual(true, row.get(bool, 0));
		try t.expectEqual("223", row.get([]u8, 1));
	}

	{
		// check the ds is updated
		var arc = tc.app.getDataSet("unparser").?;
		defer arc.release();

		const ds = arc.value;
		try t.expectEqual(1, ds.parsed_fields.len);
		try t.expectEqual("active", ds.parsed_fields[0]);
	}
}

test "events.create: no token when token required" {
	var tc = t.context(.{});
	defer tc.deinit();
	try tc.app._settings.setValue(.{.create_tokens = true});

	tc.web.param("name", "logx_y");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(401);
	try tc.web.expectJson(.{
		.code = 11,
		.err = "access token required to create events",
	});
}

test "events.create: invalid token when token required" {
	var tc = t.context(.{});
	defer tc.deinit();
	try tc.app._settings.setValue(.{.create_tokens = true});

	tc.web.header("token", "hack");
	tc.web.param("name", "logx_y");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(403);
	try tc.web.expectJson(.{
		.code = 12,
		.err = "access token is invalid",
	});
}

test "events.create: create event with correct access token" {
	var tc = t.context(.{});
	defer tc.deinit();
	try tc.app._settings.setValue(.{.create_tokens = true});
	const token = try tc.app.tokens.create(tc.env());

	tc.web.header("token", &token);
	tc.web.param("name", "logx_y");
	tc.web.json(.{.id = 999});
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(204);
}

test "events.create: content-encoding unsupported" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.silenceLogs();

	tc.web.param("name", "logx_y");
	tc.web.body("{\"id\": 1}");
	tc.web.header("content-encoding", "gzip");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(400);
	try tc.web.expectJson(.{
		.code = 13,
		.err = "content-encoding is not supported",
	});
}

test "events.create: content-encoding zstd invalid" {
	var tc = t.context(.{});
	defer tc.deinit();
	tc.silenceLogs();

	tc.web.param("name", "logx_y");
	tc.web.body("{\"id\": 1}");
	tc.web.header("content-encoding", "zstd");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(400);
	try tc.web.expectJson(.{
		.code = 14,
		.err = "failed to decompress event(s)",
	});
}

test "events.create: content-encoding zstd ok" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "logx_y");
	tc.web.body(&.{0x28, 0xb5, 0x2f, 0xfd, 0x24, 0x15, 0xa9, 0x00, 0x00, 0x7b, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x20, 0x22, 0x7a, 0x73, 0x74, 0x64, 0x2d, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x22, 0x7d, 0x0a, 0x17, 0x51, 0xc6, 0x1d});
	tc.web.header("content-encoding", "zstd");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(204);
}

test "events.create: content-encoding zstd" {
	var tc = t.context(.{});
	defer tc.deinit();

	tc.web.param("name", "log_ce");
	// {"id": "zstd-event"}
	tc.web.body(&.{0x28, 0xb5, 0x2f, 0xfd, 0x24, 0x15, 0xa9, 0x00, 0x00, 0x7b, 0x22, 0x69, 0x64, 0x22, 0x3a, 0x20, 0x22, 0x7a, 0x73, 0x74, 0x64, 0x2d, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x22, 0x7d, 0x0a, 0x17, 0x51, 0xc6, 0x1d});
	tc.web.header("content-encoding", "zstd");
	try handler(tc.env(), tc.web.req, tc.web.res);

	try tc.web.expectStatus(204);

	tc.flushMessages();

	const row = (try tc.row("select id from log_ce", .{})).?;
	defer row.deinit();
	try t.expectEqual("zstd-event", row.get([]const u8, 0));
}
