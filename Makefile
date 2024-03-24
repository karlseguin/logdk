F=
.PHONY: t
t:
	TEST_FILTER="${F}" zig build test --summary all -freference-trace

.PHONY: s
s:
	zig build run -freference-trace
