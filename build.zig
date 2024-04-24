const std = @import("std");

const Allocator = std.mem.Allocator;
const ModuleMap = std.StringArrayHashMap(*std.Build.Module);

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();

	var modules = ModuleMap.init(allocator);
	defer modules.deinit();

	const dep_opts = .{.target = target, .optimize = optimize};
	const static_link = b.option(bool, "static", "static link duckdb") orelse false;

	try modules.put("httpz", b.dependency("httpz", dep_opts).module("httpz"));
	try modules.put("typed", b.dependency("typed", dep_opts).module("typed"));
	try modules.put("metrics", b.dependency("metrics", dep_opts).module("metrics"));
	try modules.put("logz", b.dependency("logz", dep_opts).module("logz"));
	try modules.put("validate", b.dependency("validate", dep_opts).module("validate"));
	try modules.put("zul", b.dependency("zul", dep_opts).module("zul"));

	const zuckdb = b.dependency("zuckdb", dep_opts).module("zuckdb");
	try modules.put("zuckdb",  zuckdb);


	// try modules.put("logz", b.addModule("logz", .{
	// 	.root_source_file = .{.path = "lib/log.zig/src/logz.zig"},
	// 	.imports = &.{
	// 		.{.name = "metrics", .module = b.addModule("metrics", .{.root_source_file = .{.path = "lib/metrics.zig/src/metrics.zig"}})},
	// 	},
	// }));

	zuckdb.addIncludePath(b.path("lib"));

	// setup executable
	const exe = b.addExecutable(.{
		.name = "logdk",
		.root_source_file = .{ .path = "src/main.zig" },
		.target = target,
		.optimize = optimize,
	});
	try addLibs(exe, modules, static_link);
	try addUIFiles(allocator, b, exe);
	b.installArtifact(exe);

	const run_cmd = b.addRunArtifact(exe);
	run_cmd.step.dependOn(b.getInstallStep());
	if (b.args) |args| {
		run_cmd.addArgs(args);
	}

	const run_step = b.step("run", "Run the app");
	run_step.dependOn(&run_cmd.step);

	// setup tests
	const tests = b.addTest(.{
		.root_source_file = .{ .path = "src/main.zig" },
		.target = target,
		.optimize = optimize,
		.test_runner = .{.path = "test_runner.zig"},
	});

	try addLibs(tests, modules, static_link);
	try addUIFiles(allocator, b, tests);
	const run_test = b.addRunArtifact(tests);
	run_test.has_side_effects = true;

	const test_step = b.step("test", "Run tests");
	test_step.dependOn(&run_test.step);
}

fn addLibs(c: *std.Build.Step.Compile, modules: ModuleMap, static_link: bool) !void {
	var it = modules.iterator();
	while (it.next()) |m| {
		c.root_module.addImport(m.key_ptr.*, m.value_ptr.*);
	}

	c.linkLibC();
	if (static_link) {
		c.linkSystemLibrary("stdc++");
	} else {
		c.addRPath(c.step.owner.path("lib"));
	}
	c.linkSystemLibrary("duckdb");
	c.addLibraryPath(c.step.owner.path("lib"));
}

fn addUIFiles(allocator: Allocator, b: *std.Build, c: *std.Build.Step.Compile) !void {
	// We use a MultiArryaList because addOption is broken when adding a struct
	// https://github.com/ziglang/zig/issues/19594
	// So instead of adding 1 option of type []StaticFile, we add 1 option for each field
	var files = std.MultiArrayList(StaticFile){};
	defer files.deinit(allocator);

	try addUIFilesFrom(allocator, c, "ui", &files);

	var files_option = b.addOptions();
	files_option.addOption([]const []const u8, "names", files.items(.name));
	files_option.addOption([]const []const u8, "contents", files.items(.content));
	files_option.addOption([]const bool, "compressed", files.items(.compressed));
	c.root_module.addOptions("ui_files", files_option);
}

fn addUIFilesFrom(allocator: Allocator, c: *std.Build.Step.Compile, root: []const u8, files: *std.MultiArrayList(StaticFile)) !void {
	var dir = try std.fs.cwd().openDir(root, .{.iterate = true});
	var it = dir.iterate();

	while (try it.next()) |file| {
		const name = try std.fmt.allocPrint(allocator, "{s}/{s}", .{root, file.name});
		switch (file.kind) {
			.directory => try addUIFilesFrom(allocator, c, name, files),
			.file => {
				const compressed = std.mem.endsWith(u8, name, ".br");
				try files.append(allocator, .{
					.name = if (compressed) name[0..name.len - 3] else name,
					.compressed = compressed,
					.content = try dir.readFileAlloc(allocator, file.name, 1_048_576),
				});
				// c.root_module.addAnonymousImport(name, .{.root_source_file = c.step.owner.path(name)});
			},
			else => unreachable,
		}
	}
}

pub const StaticFile = struct {
	name: []const u8,
	compressed: bool,
	content: []const u8,
};


	// const httpz = b.addModule("httpz", .{
	// 	.root_source_file = .{.path = "lib/http.zig/src/httpz.zig"},
	// 	.imports = &.{
	// 		.{.name = "metrics", .module = b.addModule("metrics", .{.root_source_file = .{.path = "lib/metrics.zig/src/metrics.zig"}})},
	// 		.{.name = "websocket", .module = b.addModule("websocket", .{.root_source_file = .{.path = "lib/websocket.zig/src/websocket.zig"}})},
	// 	},
	// });
	// try modules.put("httpz", httpz);

	// const zuckdb = b.addModule("zuckdb", .{
	// 	.root_source_file = .{.path = "lib/zuckdb.zig/src/zuckdb.zig"},
	// 	.imports = &.{},
	// });

	// try modules.put("zul", b.addModule("zul", .{
	// 	.root_source_file = .{.path = "lib/zul/src/zul.zig"},
	// }));
