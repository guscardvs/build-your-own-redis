const std = @import("std");
const net = std.net;
const binned_allocator = @import("binned_allocator");
const Store = @import("Store.zig");
const Redis = @import("Redis.zig");

const default_host = [4]u8{ 127, 0, 0, 1 };
const default_port = 6379;

const Result = struct {
    data: ?[]Store.Value,
    error_tag: ?[]const u8,
    messages: ?[][]const u8,
};

pub fn main() !void {
    var state = binned_allocator.BinnedAllocator(.{}){};
    defer state.deinit();
    const allocator = state.allocator();
    var args = std.process.args();
    // skip
    _ = args.skip();
    var address = std.net.Address.initIp4(default_host, default_port);
    if (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--host")) {
            if (args.next()) |host| {
                address = std.net.Address.resolveIp(host, address.getPort()) catch |err| {
                    std.log.err("Unexpected value for host: {s}, expected ip address.", .{host});
                    return err;
                };
            } else {
                std.log.err("Received --host but no argument was found next", .{});
            }
        } else if (std.mem.eql(u8, arg, "--port")) {
            if (args.next()) |port| {
                const int_port = std.fmt.parseInt(u16, port, 10) catch |err| {
                    std.log.err("Unexpected value for port {s}, expected u16 number", .{port});
                    return err;
                };
                address.setPort(int_port);
            } else {
                std.log.err("Received --port but no argument was found next", .{});
            }
        }
    }
    var inmem = Store.InMemoryStore.init(allocator);
    defer inmem.deinit();
    var store = inmem.store();
    var redis = Redis.init(allocator, &store);

    var server = std.net.StreamServer.init(.{ .reuse_address = true });
    defer server.deinit();
    try server.listen(address);
    std.log.info("Listening for connections on {}", .{address});

    while (true) {
        const conn = try server.accept();
        defer conn.stream.close();
        var msg = std.ArrayList(u8).init(allocator);
        defer msg.deinit();

        var reader = conn.stream.reader();
        reader.streamUntilDelimiter(msg.writer(), 0, null) catch |err| switch (err) {
            error.EndOfStream => {},
            else => return err,
        };
        const msg_slice = if (msg.items[msg.items.len - 1] == 0) msg.items[0 .. msg.items.len - 1] else msg.items;

        var result_message = std.ArrayList(u8).init(allocator);
        defer result_message.deinit();
        var results = redis.run(msg_slice, result_message.writer());
        std.debug.print("{any}", .{results});
        if (results) |response| {
            defer allocator.free(response);
            try std.json.stringify(
                Result{ .data = response, .error_tag = null, .messages = null },
                .{ .whitespace = .indent_2 },
                conn.stream.writer(),
            );
        } else |err| {
            var messages = std.ArrayList([]const u8).init(allocator);
            defer messages.deinit();
            const newlines = std.mem.count(u8, result_message.items, "\n");
            try messages.ensureTotalCapacity(newlines + 1);
            var iterator = std.mem.split(u8, result_message.items, "\n");
            while (iterator.next()) |item| {
                try messages.append(item);
            }
            const response = Result{ .data = null, .error_tag = @errorName(err), .messages = messages.items };
            try std.json.stringify(
                response,
                .{ .whitespace = .indent_2 },
                conn.stream.writer(),
            );
        }
    }
}
