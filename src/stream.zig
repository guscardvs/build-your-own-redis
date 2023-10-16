const std = @import("std");

pub fn Stream(comptime T: type) type {
    return struct {
        const CharStream = @This();

        slice: []const T,
        position: usize,

        pub fn subslice(self: *CharStream, from: usize, upto: usize) []const T {
            const fromval = @min(from, self.slice.len);
            const uptoval = @min(upto, self.slice.len);
            return self.slice[fromval..uptoval];
        }
        pub fn moveNext(self: *CharStream) void {
            _ = self.goto(1);
        }

        pub fn goto(self: *CharStream, offset: usize) T {
            defer self.position += offset;
            return self.current();
        }

        pub fn advance(self: *CharStream) T {
            defer self.position += 1;
            return self.current();
        }

        pub fn current(self: CharStream) T {
            return self.getItem(self.position);
        }

        pub fn lookAhead(self: CharStream, offset: usize) T {
            return self.getItem(self.position + offset);
        }
        pub fn lookBehind(self: CharStream, offset: usize) T {
            // returns first if out of bounds
            if (self.position < offset) return self.getItem(0);
            return self.getItem(self.position - offset);
        }

        pub fn length(self: CharStream) usize {
            return self.slice.len;
        }

        pub fn previous(self: CharStream) T {
            return self.lookBehind(1);
        }

        pub fn next(self: CharStream) T {
            return self.getItem(self.position + 1);
        }

        fn getItem(self: CharStream, pos: usize) T {
            // returns last if out of bounds
            return if (pos >= self.slice.len) self.slice[self.slice.len - 1] else self.slice[pos];
        }

        pub fn isEoS(self: CharStream) bool {
            return self.slice.len == 0 or self.position >= self.slice.len;
        }
    };
}
