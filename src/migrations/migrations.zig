const std = @import("std");
const logz = @import("logz");
const zuckdb = @import("zuckdb");

var migrations = [_]*const fn (*zuckdb.Conn) anyerror!void{
    @import("migration_001.zig").run,
    @import("migration_002.zig").run,
    @import("migration_003.zig").run,
};

pub fn run(conn: *zuckdb.Conn) !void {
    conn.err = null; // should not be necessary, but let's be safe

    try createMigrationsTable(conn);
    const current_migration = try getCurrentMigration(conn);

    logz.info().ctx("migrations.start").int("current_migration_id", current_migration).log();

    for (migrations[current_migration..], current_migration..) |m, id| {
        try conn.begin();
        m(conn) catch |err| {
            logz.err().ctx("migration.run").int("migration_id", id).err(err).string("details", conn.err).log();
            conn.rollback() catch {};
            return err;
        };
        migrated(conn, @intCast(id)) catch |err| {
            conn.rollback() catch {};
            return err;
        };
        try conn.commit();
    }

    logz.info().ctx("migrations.end").int("current_migration_id", migrations.len).log();
}

fn createMigrationsTable(conn: *zuckdb.Conn) !void {
    _ = conn.exec("create schema if not exists logdk", .{}) catch |err| {
        logz.err().ctx("migration.run.create_migrations").string("details", conn.err).err(err).log();
        return err;
    };

    _ = conn.exec("create table if not exists logdk.migrations (id usmallint not null primary key, created timestamptz not null)", .{}) catch |err| {
        logz.err().ctx("migration.run.create_migrations").string("details", conn.err).err(err).log();
        return err;
    };
}

fn getCurrentMigration(conn: *zuckdb.Conn) !u16 {
    var row = (try conn.row("select max(id) from logdk.migrations", .{})) orelse return 0;
    defer row.deinit();
    const current = row.get(?u16, 0) orelse return 0;
    return current + 1;
}

fn migrated(conn: *zuckdb.Conn, migration_id: u16) !void {
    _ = conn.exec("insert into logdk.migrations (id, created) values ($1, now())", .{migration_id}) catch |err| {
        logz.err().ctx("migration.run.migrations_insert").string("details", conn.err).int("migration_id", migration_id).err(err).log();
        return err;
    };
}
