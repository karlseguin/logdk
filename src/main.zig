const std = @import("std");
const zul = @import("zul");
const logz = @import("logz");
const builtin = @import("builtin");

const logdk = @import("logdk.zig");

const App = logdk.App;
const Config = logdk.Config;
const Allocator = std.mem.Allocator;

// global, only used for shutting down from signal
var shutdown_app: ?*App = null;

pub fn main() !void {
	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;

	defer if (builtin.mode == .Debug) {
		_ = gpa.detectLeaks();
	};

	if (comptime builtin.os.tag != .windows) {
		try std.posix.sigaction(std.posix.SIG.INT, &.{
			.handler = .{.handler = shutdown},
			.mask = std.posix.empty_sigset,
			.flags = 0,
		}, null);
		try std.posix.sigaction(std.posix.SIG.TERM, &.{
			.handler = .{.handler = shutdown},
			.mask = std.posix.empty_sigset,
			.flags = 0,
		}, null);
	}

	// Some data exists for the entire lifetime of the project. We could just
	// use the gpa allocator, but if we don't properly clean it up, it'll cause
	// tests to report leaks.
	var arena = std.heap.ArenaAllocator.init(allocator);
	defer arena.deinit();
	const aa = arena.allocator();

	const config = loadConfig(aa) catch |err| {
		std.log.err("Failed to read config file: {}", .{err});
		std.process.exit(1);
	};

	try logz.setup(allocator, config.logger);
	defer logz.deinit();

	logz.info().ctx("init").stringSafe("log_level", @tagName(logz.level())).log();

	var app = try App.init(allocator, config);
	defer app.deinit();
	shutdown_app = &app;

	try app.loadDataSets();

	try app.scheduler.start(&app);

	try @import("init.zig").init(aa);
	try logdk.web.start(&app, &config);
	logz.info().ctx("shutdown").log();
}

fn loadConfig(allocator: Allocator) !logdk.Config {
	var args = try zul.CommandLineArgs.parse(allocator);
	defer args.deinit();

	if (args.contains("version")) {
		try std.io.getStdOut().writer().print("{s}", .{logdk.version});
		std.posix.exit(0);
	}

	var config_file: []const u8 = undefined;
	var explicit_config_file: bool = undefined;

	if (args.get("config")) |cf| {
		config_file = cf;
		explicit_config_file = true;
	} else {
		config_file = "config.json";
		explicit_config_file = false;
	}

	const managed = zul.fs.readJson(logdk.Config, allocator, config_file, .{}) catch |err| switch (err) {
		error.FileNotFound => {
			if (explicit_config_file == true) {
				return err;
			}
			// this means no config file was specified and we just tried config.json
			// let's start up with sane defaults.
			return Config{};
		},
		else => return err,
	};

	// it's ok to discard the managed value, because we know Allocator comes
	// from an Arena which main will cleanup.
	return managed.value;
}

fn shutdown(_: c_int) callconv(.C) void {
	if (shutdown_app) |app| {
		if (app._webserver) |web| {
			// this will unblock the main thread, which will clean everything up
			logz.info().ctx("shutdown").boolean("started", true).log();
			web.stop();
			return;
		}
	}
	logz.info().ctx("shutdown").boolean("started", false).log();
}

const t = logdk.testing;
test {
	try t.setup();
	std.testing.refAllDecls(@This());
}
