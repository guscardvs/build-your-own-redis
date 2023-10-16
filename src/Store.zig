const std = @import("std");

const Store = @This();

pub const Value = union(enum) {
    bytearray: []const u8,
    number: f64,
    nil,

    pub fn format(self: Value, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        switch (self) {
            .bytearray => |bytearray| try writer.print("{s}", .{bytearray}),
            .number => |number| try writer.print("{d}", .{number}),
            .nil => try writer.print("NULL", .{}),
        }
    }
    pub fn jsonStringify(self: Value, jw: anytype) !void {
        switch (self) {
            .bytearray => |arr| try jw.print("\"{s}\"", .{arr}),
            .number => |number| try jw.print("{d}", .{number}),
            .nil => try jw.print("null", .{}),
        }
    }
};

pub const StoreError = error{ CacheMiss, InvalidBuffer, BufferOverflow, UnexpectedValue };
pub const VTable = struct {
    get: *const fn (*anyopaque, []const u8) Value,
    set: *const fn (*anyopaque, []const u8, Value) anyerror!void,
    hget: *const fn (*anyopaque, []const u8, []const u8) Value,
    hset: *const fn (*anyopaque, []const u8, []const u8, Value) anyerror!void,
    hashArrKey: *const fn (*anyopaque, []const u8, usize) anyerror![]const u8,
    releaseHash: *const fn (*anyopaque, []const u8) void,
    arrKeyLength: *const fn (*anyopaque, []const u8) anyerror!usize,
    cleanup: *const fn (*anyopaque, [][]const u8) anyerror!usize,
};
ctx: *anyopaque,
vtable: *const VTable,

const Mapper = struct {
    context: *anyopaque,
    hkey: []const u8,

    pub fn get(self: Mapper, key: []const u8) Value {
        const store: *Store = @ptrCast(@alignCast(self.context));
        return store.hget(self.hkey, key);
    }
    pub fn set(self: Mapper, key: []const u8, value: Value) anyerror!void {
        const store: *Store = @ptrCast(@alignCast(self.context));
        return store.hset(self.hkey, key, value);
    }
};

pub fn mapper(self: *Store, hkey: []const u8) Mapper {
    return Mapper{ .context = self, .hkey = hkey };
}

pub fn get(self: *Store, key: []const u8) Value {
    return self.vtable.get(self.ctx, key);
}
pub fn set(self: *Store, key: []const u8, value: Value) !void {
    return self.vtable.set(self.ctx, key, value);
}
pub fn hget(self: *Store, map: []const u8, key: []const u8) Value {
    return self.vtable.hget(self.ctx, map, key);
}
pub fn hset(self: *Store, map: []const u8, key: []const u8, value: Value) !void {
    return self.vtable.hset(self.ctx, map, key, value);
}
pub fn cleanup(self: *Store, keys: [][]const u8) !usize {
    return self.vtable.cleanup(self.ctx, keys);
}

pub fn arrSet(self: *Store, arrkey: []const u8, values: []Value) anyerror!void {
    var i: usize = 0;
    const length_hash = try self.vtable.arrKeyLength(self.ctx, arrkey);

    while (i < values.len) : (i += 1) {
        var hash = try self.vtable.hashArrKey(self.ctx, arrkey, i);
        defer self.vtable.releaseHash(self.ctx, hash);
        try self.vtable.set(self.ctx, hash, values[i]);
    }

    try self.vtable.set(self.ctx, length_hash, i);
}

// caller owns the memory
pub fn arrGet(self: *Store, arrkey: []const u8, allocator: std.mem.Allocator) anyerror![]Value {
    var array = std.ArrayList(Value).init(allocator);
    const length: usize = try self.vtable.arrKeyLength(self.ctx, arrkey);
    var i = 0;
    array.ensureTotalCapacity(length);
    while (i < length) : (i += 1) {
        const hash = try self.vtable.hashArrKey(self.ctx, arrkey, i);
        defer self.vtable.releaseHash(hash);
        const val = self.vtable.get(self.ctx, hash);
        try array.append(val);
    }

    return array.toOwnedSlice();
}

pub const InMemoryStore = struct {
    map: std.StringHashMap(Value),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) InMemoryStore {
        return .{
            .map = std.StringHashMap(Value).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *InMemoryStore) void {
        self.map.deinit();
    }

    pub fn get(ctx: *anyopaque, key: []const u8) Value {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        return self.map.get(key) orelse .nil;
    }

    pub fn set(ctx: *anyopaque, key: []const u8, value: Value) anyerror!void {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        try self.map.put(key, value);
    }

    pub fn hget(ctx: *anyopaque, key: []const u8, field: []const u8) Value {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        var compositeKey = std.ArrayList(u8).init(self.allocator);
        defer compositeKey.deinit();
        compositeKey.appendSlice(key) catch return .nil;
        compositeKey.appendSlice(field) catch return .nil;

        const value = self.map.get(compositeKey.allocatedSlice()) orelse .nil;
        return value;
    }

    pub fn hset(ctx: *anyopaque, key: []const u8, field: []const u8, value: Value) anyerror!void {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        var compositeKey = std.ArrayList(u8).init(self.allocator);
        defer compositeKey.deinit();
        try compositeKey.appendSlice(key);
        try compositeKey.appendSlice(field);

        try self.map.put(compositeKey.allocatedSlice(), value);
    }

    // caller owns memory
    pub fn hashArrKey(ctx: *anyopaque, key: []const u8, index: usize) anyerror![]const u8 {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        var compositeKey = std.ArrayList(u8).init(self.allocator);
        defer compositeKey.deinit();
        try compositeKey.appendSlice(key);
        try compositeKey.writer().print("{d}", .{index});
        const hash = compositeKey.toOwnedSlice();
        return hash;
    }

    pub fn releaseHash(ctx: *anyopaque, hash: []const u8) void {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        self.allocator.free(hash);
    }

    pub fn arrKeyLength(ctx: *anyopaque, key: []const u8) anyerror!usize {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        var lengthKey = std.ArrayList(u8).init(self.allocator);
        try lengthKey.appendSlice(key);
        try lengthKey.appendSlice("|len|");
        const lengthValue = self.map.get(lengthKey.allocatedSlice()) orelse .nil;
        lengthKey.deinit();

        switch (lengthValue) {
            .number => |num| return @intFromFloat(num),
            else => return 0,
        }
    }

    pub fn cleanup(ctx: *anyopaque, keys: [][]const u8) anyerror!usize {
        const self: *InMemoryStore = @ptrCast(@alignCast(ctx));
        if (keys.len == 1) {
            return self.removeOne(keys[0]);
        }
        var removed: usize = 0;
        for (keys) |key| {
            if (self.map.remove(key)) removed += 1;
        }
        return removed;
    }

    fn removeOne(self: *InMemoryStore, key: []const u8) anyerror!usize {
        if (key.len == 1 and key[0] == '*') {
            return self.reset();
        }
        return if (self.map.remove(key)) 1 else 0;
    }

    fn reset(self: *InMemoryStore) anyerror!usize {
        const length = self.map.count();
        self.map.clearAndFree();
        return length;
    }

    pub fn store(self: *InMemoryStore) Store {
        return Store{ .ctx = self, .vtable = &.{
            .get = InMemoryStore.get,
            .set = InMemoryStore.set,
            .hget = InMemoryStore.hget,
            .hset = InMemoryStore.hset,
            .hashArrKey = InMemoryStore.hashArrKey,
            .arrKeyLength = InMemoryStore.arrKeyLength,
            .releaseHash = InMemoryStore.releaseHash,
            .cleanup = InMemoryStore.cleanup,
        } };
    }
};
