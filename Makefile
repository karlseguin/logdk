F=
.PHONY: t
t:
	TEST_FILTER="${F}" zig build test --summary all -freference-trace

.PHONY: s
s:
	zig build run -freference-trace

.PHONY: ui
ui:
	rm -fr ui
	cd ../ui && make d
	cp -R ../ui/dist ui/
	brotli -9 --keep ui/*
