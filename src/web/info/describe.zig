const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../../logdk.zig");

pub fn handler(env: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
    const describe = env.app.meta.getDescribe(env.settings);
    defer describe.release();

    res.content_type = .JSON;
    if (servedCompressed(req)) {
        res.header("Content-Encoding", "gzip");
        res.body = describe.value.gzip;
    } else {
        res.body = describe.value.json;
    }

    // explicitly write, so that we can release our describe at the end of this function
    try res.write();
}

fn servedCompressed(req: *httpz.Request) bool {
    const ae = req.header("accept-encoding") orelse return false;
    return std.mem.indexOf(u8, ae, "gzip") != null;
}

const t = logdk.testing;
test "describe: json" {
    var tc = t.context(.{});
    defer tc.deinit();

    {
        // no datasets
        try handler(tc.env(), tc.web.req, tc.web.res);
        try tc.web.expectJson(.{
            // wrong dataset type, but we don't care, we just want an empty array
            .datasets = &[_]i32{},
        });
    }

    try tc.createDataSet("events", "{\"id\": 1235, \"date\": [\"2024-05-10\"]}", false);
    try tc.createDataSet("app_logs", "{\"level\": \"INFO\", \"@ctx\": \"listen\", \"blocking\": true}", false);

    // necessary to make sure meta._describe is updated
    tc.flushMessages();

    {
        tc.reset();

        try handler(tc.env(), tc.web.req, tc.web.res);
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

    {
        tc.reset();

        tc.web.header("accept-encoding", "br,gzip");
        try handler(tc.env(), tc.web.req, tc.web.res);
        try tc.web.expectGzip();

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
