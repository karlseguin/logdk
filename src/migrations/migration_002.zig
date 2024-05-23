const zuckdb = @import("zuckdb");

pub fn run(conn: *zuckdb.Conn) !void {
	// We should only ever have 1 row in this table. The primary key + check
	// constraint exist to enforce that
	_ = try conn.exec(
		\\ create table logdk.settings (
		\\   id uinteger not null primary key check (id = 1),
		\\   settings json not null,
		\\ )
	, .{});
}
