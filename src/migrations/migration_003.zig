const zuckdb = @import("zuckdb");

pub fn run(conn: *zuckdb.Conn) !void {
	_ = try conn.exec(
		\\ create table logdk.users (
		\\   id uinteger primary key,
		\\   username text not null,
		\\   password text not null,
		\\   enabled bool not null,
		\\   permissions text[] not null,
		\\   created timestamptz not null default(now())
		\\ )
	, .{});

	_ = try conn.exec(
		\\ create table logdk.sessions (
		\\  id text not null primary key,
		\\  user_id uinteger not null,
		\\  expires timestamptz not null,
		\\  created timestamptz not null default(now())
		\\ )
	, .{});
}