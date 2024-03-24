const std = @import("std");

const LazyPath = std.Build.LazyPath;
const ModuleMap = std.StringArrayHashMap(*std.Build.Module);

pub fn build(b: *std.Build) !void {
	const target = b.standardTargetOptions(.{});
	const optimize = b.standardOptimizeOption(.{});

	var gpa = std.heap.GeneralPurposeAllocator(.{}){};
	const allocator = gpa.allocator();

	var modules = ModuleMap.init(allocator);
	defer modules.deinit();

	const dep_opts = .{.target = target,.optimize = optimize};

	try modules.put("httpz", b.dependency("httpz", dep_opts).module("httpz"));
	try modules.put("metrics", b.dependency("metrics", dep_opts).module("metrics"));
	try modules.put("logz", b.dependency("logz", dep_opts).module("logz"));
	try modules.put("validate", b.dependency("validate", dep_opts).module("validate"));
	// try modules.put("zul", b.dependency("zul", dep_opts).module("zul"));

	// const zuckdb = b.dependency("zuckdb", dep_opts).module("zuckdb");
	const zuckdb = b.addModule("zuckdb", .{
		.root_source_file = .{.path = "lib/zuckdb.zig/src/zuckdb.zig"},
		.imports = &.{},
	});
	zuckdb.addIncludePath(LazyPath.relative("lib"));
	try modules.put("zuckdb",  zuckdb);

	try modules.put("zul", b.addModule("zul", .{
		.root_source_file = .{.path = "lib/zul/src/zul.zig"},
	}));

	// setup executable
	const exe = b.addExecutable(.{
		.name = "logdk",
		.root_source_file = .{ .path = "src/main.zig" },
		.target = target,
		.optimize = optimize,
	});
	try addLibs(exe, modules);
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
		.test_runner = "test_runner.zig",
	});

	try addLibs(tests, modules);
	const run_test = b.addRunArtifact(tests);
	run_test.has_side_effects = true;

	const test_step = b.step("test", "Run tests");
	test_step.dependOn(&run_test.step);
}

fn addLibs(step: *std.Build.Step.Compile, modules: ModuleMap) !void {
	var it = modules.iterator();
	while (it.next()) |m| {
		step.root_module.addImport(m.key_ptr.*, m.value_ptr.*);
	}
	step.linkLibC();
	step.linkSystemLibrary("duckdb");
	step.addRPath(LazyPath.relative("lib"));
	step.addLibraryPath(LazyPath.relative("lib"));
}
