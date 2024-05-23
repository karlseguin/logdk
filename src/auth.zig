const std = @import("std");

const Allocator = std.mem.Allocator;

pub const User = struct {
	id: u32,
	// Although we persist permissions as an array, we store them this way so that
	// we don't have to dynamically allocate a []Permission
	permission_admin: bool = false,
	permission_raw_query: bool = false,

	pub fn hasPermission(self: User, required_permission: Permission) bool {
		if (self.permission_admin) {
			return true;
		}
		switch (required_permission) {
			.raw_query => return self.permission_raw_query,
			.admin => return false, // (the if (self.admin) clause would have caught this if it was true)
		}
	}
};

pub const Permission = enum {
	admin,
	raw_query,
};

const t = @import("t.zig");
test "User: hasPermission" {
	{
		const u = User{.id = 0};
		try t.expectEqual(false, u.hasPermission(.admin));
		try t.expectEqual(false, u.hasPermission(.raw_query));
	}

	{
		const u = User{.id = 0, .permission_admin = true};
		try t.expectEqual(true, u.hasPermission(.admin));
		try t.expectEqual(true, u.hasPermission(.raw_query));
	}

	{
		const u = User{.id = 0, .permission_raw_query = true};
		try t.expectEqual(false, u.hasPermission(.admin));
		try t.expectEqual(true, u.hasPermission(.raw_query));
	}
}
