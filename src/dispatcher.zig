const std = @import("std");
const logz = @import("logz");

const Thread = std.Thread;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub fn createQueues(allocator: Allocator, comptime T: type, worker_count: usize) !Queues(T) {
	// we expect this to be the ArenaAllocator created in dispatcher.init and passed
	// back here
	const queues = try allocator.alloc(Queue(T), worker_count);
	errdefer allocator.free(queues);

	for (queues) |*q| {
		q.* = try Queue(T).init(allocator, 100);
	}

	return .{.list = queues};
}

pub fn Dispatcher(comptime Q: type) type {
	return struct {
		queues: Q,
		threads: []Thread,
		arena: *ArenaAllocator,

		const Self = @This();

		pub fn init(allocator: Allocator) !Self {
			const arena = try allocator.create(ArenaAllocator);
			errdefer allocator.destroy(arena);

			arena.* = ArenaAllocator.init(allocator);
			errdefer arena.deinit();

			const queues = try Q.init(arena.allocator());
			const threads = try startWorkers(arena.allocator(), queues);

			return .{
				.arena = arena,
				.queues = queues,
				.threads = threads,
			};
		}

		fn startWorkers(allocator: Allocator, queues: Q) ![]Thread {
			var queue_count: usize = 0;
			inline for (@typeInfo(Q).Struct.fields) |field| {
				queue_count += @field(queues, field.name).list.len;
			}

			var started: usize = 0;
			var threads = try allocator.alloc(Thread, queue_count);
			errdefer blk: {
				var j: usize = 0;
				inline for (@typeInfo(Q).Struct.fields) |field| {
					for (@field(queues, field.name).list) |*tq| {
						tq.enqueue(.{.stop = {}});
						threads[j].join();
						j += 1;
						if (j == started) break :blk;
					}
				}
			}

			inline for (@typeInfo(Q).Struct.fields) |field| {
				for (@field(queues, field.name).list) |*tq| {
					threads[started] = try Thread.spawn(.{}, @TypeOf(tq.*).run, .{tq});
					started += 1;
				}
			}

			return threads;
		}

		// The caller (the App) wants to call stop and deinit separately. First
		// it wants to call dispatcher.stop, which will drain the queues.
		// Then it wants to do other cleanup and then call dispatcher.deinit().
		// The reason for this is that dispatcher.deinit() clears the dispatcher
		// arena, which owns all the actors. That's fine for cleaning the actual
		// *Actor, but the values might need to be de-inited, which we leave up
		// to the app.
		pub fn stop(self: *Self) void {
			// we don't usually expect stop to be called twice, but it is in some tests
			// as a brute force way to ensure all messages are flushed. So we guard against
			// it being called twice
			if (self.threads.len == 0) return;

			var i: usize = 0;
			inline for (@typeInfo(Q).Struct.fields) |field| {
				for (@field(self.queues, field.name).list) |*tq| {
					tq.enqueue(.{.stop = {}});
					self.threads[i].join();
					i += 1;
				}
			}
			self.threads = &[_]Thread{};
		}

		pub fn deinit(self: *Self) void {
			const allocator = self.arena.child_allocator;
			self.arena.deinit();
			allocator.destroy(self.arena);
		}

		pub fn add(self: *Self, instance: anytype) !usize {
			const T = @TypeOf(instance);
			var actor = try self.create(T);
			actor.value = instance;
			return @intFromPtr(actor);
		}

		pub fn create(self: *Self, comptime T: type) !*Actor(T) {
			const allocator = self.arena.allocator();
			const actor = try allocator.create(Actor(T));
			errdefer allocator.destroy(actor);

			const queues = &@field(self.queues, typeToFieldName(T));
			const actor_count = @atomicRmw(usize, &queues.actor_count, .Add, 1, .monotonic);
			const queue_index = @mod(actor_count, queues.list.len);

			actor.* = .{
				.value = undefined,
				.queue = &queues.list[queue_index],
			};
			return actor;
		}

		pub fn unsafeInstance(_: *const Self, comptime T: type, actor_id: usize) *T {
			const actor: *Actor(T) = @ptrFromInt(actor_id);
			return &actor.value;
		}

		pub fn send(_: *const Self, comptime T: type, actor_id: usize, message: T.Message) void {
			const actor: *Actor(T) = @ptrFromInt(actor_id);
			actor.queue.enqueue(.{.dispatch = .{.message = message, .recipient = &actor.value}});
		}
	};
}

pub fn Queues(comptime T: type) type {
	return struct {
		list: []Queue(T),
		actor_count: usize = 0,
	};
}

pub fn Queue(comptime T: type) type {
	return struct {
		// Where pending messages are stored. Acts as a circular buffer.
		messages: []Message,

		// Index in `messages` where we'll enqueue a new messages (see enqueue function)
		push: usize,

		// Index in `messages` where we'll read to ge the next pendin messages (see next function)
		pull: usize,

		// The number of pending messages we have
		pending: usize,

		// messages.len - 1. When `push` or `pull` hit this, they need to go back to 0
		queue_end: usize,

		// protects messages
		mutex: Thread.Mutex,

		// signals the consumer that there are messages waiting
		cond: Thread.Condition,

		// limits the producers. This sem permits (messages.len - pending) messages from being
		// queued. When `messages` is full, producers block.
		sem: Thread.Semaphore,

		const Message = union(enum) {
			stop: void,
			dispatch: Dispatch,

			const Dispatch = struct {
				recipient: *T,
				message: T.Message,
			};
		};

		const Self = @This();

		fn init(allocator: Allocator, len: usize) !Self {
			// we expect allcator to be an arena!
			const messages = try allocator.alloc(Message, len);
			errdefer allocator.free(messages);

			return .{
				.pull = 0,
				.push = 0,
				.pending = 0,
				.cond = .{},
				.mutex = .{},
				.messages = messages,
				.queue_end = len - 1,
				.sem = .{.permits = len}
			};
		}

		pub fn send(self: *Self, recipient: *T, message: T.Message) void {
			self.enqueue(.{.dispatch = .{.recipient = recipient, .message = message}});
		}

		// This can be called from multiple threads, the "producers"
		fn enqueue(self: *Self, message: Message) void {
			self.sem.wait();
			self.mutex.lock();
			const push = self.push;
			self.messages[push] = message;
			self.push = if (push == self.queue_end) 0 else push + 1;
			self.pending += 1;
			self.mutex.unlock();
			self.cond.signal();
		}

		// This is only ever called from a single thread (the same thread each time)
		// which is running our run loop. Essentially the "consumer"
		fn next(self: *Self) []Message {
			// pull is only ever written to from this thread
			const pull = self.pull;

			while (true) {
				self.mutex.lock();
				while (self.pending == 0) {
					self.cond.wait(&self.mutex);
				}
				const push = self.push;
				const end = if (push > pull) push else self.messages.len;
				const messages = self.messages[pull..end];

				self.pull = if (end == self.messages.len) 0 else push;
				self.pending -= messages.len;
				self.mutex.unlock();
				var sem = &self.sem;
				sem.mutex.lock();
				defer sem.mutex.unlock();
				sem.permits += messages.len;
				sem.cond.signal();
				return messages;
			}
		}

		fn run(self: *Self) void {
			while (true) {
				for (self.next()) |message| {
					switch (message) {
						.stop => return,
						.dispatch => |d| {
							var recipient = d.recipient;
							recipient.handle(d.message) catch |err| {
								logz.err().ctx("Dispatcher.handle").stringSafe("recipient", @typeName(@TypeOf(recipient))).stringSafe("message", @tagName(d.message)).err(err).log();
							};
						}
					}
				}
			}
		}
	};
}

fn Actor(comptime T: type) type {
	return struct {
		value: T,
		queue: *Queue(T),
	};
}

fn typeToFieldName(comptime T: type) []const u8 {
	const full_name = @typeName(T);
	const sep = std.mem.lastIndexOfScalar(u8, full_name, '.').?;
	const type_name = full_name[sep+1..];

	var lower: [type_name.len]u8 = undefined;
	for (type_name, 0..) |c, i| {
		lower[i] = std.ascii.toLower(c);
	}

	return &lower;
}
