const std = @import("std");
const httpz = @import("httpz");
const logdk = @import("../logdk.zig");

const files = @import("ui_files");

const Response = struct {
    body: []const u8,
    cache: []const u8,
    content_type: httpz.ContentType,
};

const compressed_lookup = std.StaticStringMap(Response).initComptime(loadFiles(true));
const uncompressed_lookup = std.StaticStringMap(Response).initComptime(loadFiles(false));

fn matchCount(c: bool) usize {
    var count: usize = 0;
    for (files.compressed) |compressed| {
        if (c == compressed) count += 1;
    }
    return count;
}

fn loadFiles(comptime c: bool) []struct { []const u8, Response } {
    var kv: [matchCount(c)]struct { []const u8, Response } = undefined;

    var i: usize = 0;
    for (files.names, files.compressed, files.contents) |name, compressed, content| {
        if (compressed != c) continue;

        var content_type: httpz.ContentType = undefined;
        var cache: []const u8 = "public, max-age=300";
        if (std.mem.endsWith(u8, name, ".js")) {
            content_type = .JS;
            cache = "public, max-age=604800, immutable";
        } else if (std.mem.endsWith(u8, name, ".png")) {
            cache = "public, max-age=604800, immutable";
            content_type = .PNG;
        } else if (std.mem.endsWith(u8, name, ".html")) {
            content_type = .HTML;
        } else {
            unreachable;
        }

        const key = if (std.mem.eql(u8, name, "ui/index.html")) "/" else "/" ++ name[3..];
        kv[i] = .{ key, .{
            .body = content,
            .cache = cache,
            .content_type = content_type,
        } };
        i += 1;
    }

    return kv[0..kv.len];
}

// env is undefined for this route, do not use!
pub fn handler(_: *logdk.Env, req: *httpz.Request, res: *httpz.Response) !void {
    const compressed = serveCompressed(req);
    const lookup = if (compressed) &compressed_lookup else &uncompressed_lookup;

    var path = req.url.path;
    if (std.fs.path.extension(path).len == 0) {
        // SPA. If we're not requesting an asset (png/js), serve up the index
        // and let the spa's routing handle the rest
        path = "/";
    }

    const response = lookup.get(path) orelse {
        res.status = 404;
        return;
    };

    res.header("cache-control", response.cache);
    if (compressed) {
        res.header("Content-Encoding", "br");
    }
    res.content_type = response.content_type;
    res.body = response.body;
}

fn serveCompressed(req: *httpz.Request) bool {
    if (std.ascii.endsWithIgnoreCase(req.url.path, ".png")) {
        return false;
    }

    const ae = req.header("accept-encoding") orelse return false;
    return std.mem.indexOf(u8, ae, "br") != null;
}
