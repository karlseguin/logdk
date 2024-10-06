const logdk = @import("logdk.zig");
const App = logdk.App;

pub const Tasks = union(enum) {
    purge: usize,
    flush_dataset: usize,

    pub fn run(self: Tasks, app: *App, _: i64) void {
        switch (self) {
            .purge => |actor_id| app.dispatcher.send(logdk.DataSet, actor_id, .{ .purge = {} }),
            .flush_dataset => |actor_id| app.dispatcher.send(logdk.DataSet, actor_id, .{ .flush = {} }),
        }
    }
};
