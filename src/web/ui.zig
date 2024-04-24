const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../logdk.zig");

const files = @import("ui_files");
const file_count = files.names.len;

const Response = struct {
	body: []const u8,
	content_type: httpz.ContentType,
};

const compressed_lookup = std.StaticStringMap(Response).initComptime(loadFiles(true));
const uncompressed_lookup = std.StaticStringMap(Response).initComptime(loadFiles(false));

fn loadFiles(c: bool) [file_count/2]struct{[]const u8, Response} {
	var kv: [file_count/2]struct{[]const u8, Response} = undefined;

	var i: usize = 0;
	for (files.names, files.compressed, files.contents) |name, compressed, content| {
		if (compressed != c) continue;

		var content_type: httpz.ContentType = undefined;
		if (std.mem.endsWith(u8, name, ".js")) {
			content_type = .JS;
		} else if (std.mem.endsWith(u8, name, ".html")) {
			content_type = .HTML;
		} else {
			unreachable;
		}

		const key = if (std.mem.eql(u8, name, "ui/index.html")) "/" else "/" ++ name[3..];
		kv[i] = .{key, .{
			.body = content,
			.content_type = content_type,
		}};
		i += 1;
	}

	return kv;
}

// env is undefined for this route, do not use!
pub fn handler(_: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
	const compressed = clientSupportsBrotli(req);
	const lookup = if (compressed) &compressed_lookup else &uncompressed_lookup;

	const response = lookup.get(req.url.path) orelse {
		res.status = 404;
		return;
	};

	res.header("cache-control", "public, max-age=604800, immutable");
	if (compressed) {
		res.header("Content-Encoding", "br");
	}
	res.content_type = response.content_type;
	res.body = response.body;
}

fn clientSupportsBrotli(req: *httpz.Request) bool {
	const ae = req.header("accept-encoding") orelse return false;
	return std.mem.indexOf(u8, ae, "br") != null;
}
