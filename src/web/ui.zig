const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../logdk.zig");

const files = @import("ui_files");

const Response = struct {
	body: []const u8,
	content_type: httpz.ContentType,
};

const file_lookup = std.ComptimeStringMap(Response, blk: {
	// our name=>content KV list that's passed to the ComptimeString
	var kv: [files.names.len]struct{[]const u8, Response} = undefined;
	for (files.names, 0..) |name, i| {
		if (std.mem.eql(u8, name, "ui/index.html")) {
			kv[i] = .{"/", .{.body = @embedFile(name), .content_type = .HTML}};
		} else {
			var content_type: httpz.ContentType = undefined;
			if (std.mem.endsWith(u8, name, ".js")) {
				content_type = .JS;
			} else {
				unreachable;
			}
			kv[i] = .{"/" ++ name[3..], .{.body = @embedFile(name), .content_type = content_type }};
		}
	}
	break :blk &kv;
});

// env is undefined for this route, do not use!
pub fn handler(_: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const response = file_lookup.get(req.url.path) orelse {
		res.status = 404;
		return;
	};
	res.content_type = response.content_type;
	res.body = response.body;
}
