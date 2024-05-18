const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

pub fn handler(env: *logdk.Env, _: *httpz.Request, res: *httpz.Response) !void {
	const describe = env.app.meta.getDescribe();
	res.callback(releasePayload, @ptrCast(describe));
	res.content_type = .JSON;
	res.body = describe.value.json;
}

fn releasePayload(state: *anyopaque) void {
	const describe: logdk.Meta.DescribeArc = @alignCast(@ptrCast(state));
	describe.release();
}

const t = logdk.testing;
test "describe: json" {
	var tc = t.context(.{});
	defer tc.deinit();

	{
		// no datasets
		try handler(tc.env(), tc.web.req, tc.web.res);
		tc.web.conn.doCallback();
		try tc.web.expectJson(.{
			// wrong dataset type, but we don't care, we just want an empty array
			.datasets = &[_]i32{}
		});
	}

	{
		tc.reset();

		try tc.createDataSet("events", "{\"id\": 1235, \"date\": [\"2024-05-10\"]}", false);
		try tc.createDataSet("app_logs", "{\"level\": \"INFO\", \"@ctx\": \"listen\", \"blocking\": true}", false);

		// necessary to make sure meta._describe is updated
		tc.flushMessages();


		try handler(tc.env(), tc.web.req, tc.web.res);
		tc.web.conn.doCallback();

		try tc.web.expectJson(
			\\ {
			\\   "datasets": [
			\\     {"name": "events", "columns": [
			\\       {"name": "date", "nullable": false, "is_list": true, "data_type": "date"},
			\\       {"name": "id", "nullable": false, "is_list": false, "data_type": "usmallint"}
			\\     ]},
			\\     {"name": "app_logs", "columns": [
			\\       {"name": "@ctx", "nullable": false, "is_list": false, "data_type": "varchar"},
			\\       {"name": "blocking", "nullable": false, "is_list": false, "data_type": "bool"},
			\\       {"name": "level", "nullable": false, "is_list": false, "data_type": "varchar"}
			\\     ]}
			\\   ]
			\\ }
		);
	}
}
