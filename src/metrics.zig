const m = @import("metrics");

// This is an advanced usage of metrics.zig, largely done because we aren't
// using any vectored metrics and thus can do everything at comptime.
var metrics = Metrics{
	.add_dataset = m.Counter(usize).Impl.init("add_dataset", .{}),
	.alter_dataset = m.Counter(usize).Impl.init("alter_dataset", .{}),
	.record_error = m.Counter(usize).Impl.init("record_error", .{}),
};

const Metrics = struct {
	add_dataset: m.Counter(usize).Impl,
	alter_dataset: m.Counter(usize).Impl,
	record_error: m.Counter(usize).Impl,
};

pub fn write(writer: anytype) !void {
	try metrics.add_dataset.write(writer);
	try metrics.alter_dataset.write(writer);
	try metrics.record_error.write(writer);
}

pub fn addDataSet() void {
	metrics.add_dataset.incr();
}

pub fn alterDataSet(c: usize) void {
	metrics.alter_dataset.incrBy(c);
}

pub fn recordError(c: usize) void {
	metrics.record_error.incrBy(c);
}
