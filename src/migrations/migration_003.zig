const zuckdb = @import("zuckdb");

pub fn run(conn: *zuckdb.Conn) !void {
    _ = try conn.exec(
        \\ create table logdk.tokens (
        \\  id text not null primary key,
        \\  created timestamptz not null default(now())
        \\ )
    , .{});
}
