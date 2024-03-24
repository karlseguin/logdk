const zuckdb = @import("zuckdb");

pub fn run(conn: *zuckdb.Conn) !void {
	_ = try conn.exec(
		\\ create table logdk.datasets (
		\\   name text not null primary key,
		\\   columns json not null default('[]'),
		\\   created timestamptz not null default(now())
		\\ )
	, .{});
}
