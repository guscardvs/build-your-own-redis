const std = @import("std");
const Store = @import("Store.zig");
const stream = @import("stream.zig");
const Redis = @This();
const builtin = @import("builtin");

store: *Store,
allocator: std.mem.Allocator,
const RedisError = error{ UnexpectedName, MissingValue, UnexpectedValue };
const TokenTypes = enum {
    Keyword,
    Literal,
    NewLine,
    Identifier,
};

pub const Operation = enum {
    GET,
    SET,
    HGET,
    HSET,
    MGET,
    MSET,
    DELETE,
    FLUSH,

    pub fn fromString(val: []const u8) ?Operation {
        inline for (@typeInfo(Operation).Enum.fields) |field| {
            if (std.mem.eql(u8, field.name, val)) {
                return @enumFromInt(field.value);
            }
        }
        return null;
    }

    pub fn asU8(self: *Operation) u8 {
        return @intFromEnum(self);
    }
};
const KwFields = @typeInfo(Operation).Enum.fields;
const Token = struct {
    token_type: TokenTypes,
    lexeme: []const u8,
    kwd: ?Operation,
    start: usize,
    length: usize,

    pub fn format(self: Token, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        try writer.print("{s} at start: {d} with length: {d}", .{
            self.lexeme,
            self.start,
            self.length,
        });
    }
};

const TokenError = error{ UnterminatedString, InvalidName };
pub const Tokenizer = struct {
    char_stream: stream.Stream(u8),
    start: usize = 0,
    in_many: bool = false,
    tokens: std.ArrayList(Token),
    allocator: std.mem.Allocator,
    message_stream: std.ArrayList(u8),

    pub fn init(
        allocator: std.mem.Allocator,
        source: []const u8,
    ) Tokenizer {
        return Tokenizer{
            .char_stream = stream.Stream(u8){ .position = 0, .slice = source },
            .tokens = std.ArrayList(Token).init(allocator),
            .allocator = allocator,
            .message_stream = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(self: Tokenizer) void {
        self.tokens.deinit();
        self.message_stream.deinit();
    }

    pub fn tokenize(self: *Tokenizer) !void {
        var next_is_identifier = false;
        while (!self.isEoS()) {
            self.start = self.char_stream.position;
            const token = switch (self.advance()) {
                '\n' => blk: {
                    self.in_many = false;
                    break :blk self.getToken(.NewLine);
                },
                ' ', '\t' => continue,
                else => |char| char_blk: {
                    if (char == 'G' and self.matchSubstring("ET", self.start + 1)) {
                        next_is_identifier = true;
                        break :char_blk self.getToken(.Keyword);
                    } else if (char == 'S' and self.matchSubstring("ET", self.start + 1)) {
                        next_is_identifier = true;
                        break :char_blk self.getToken(.Keyword);
                    } else if ((char == 'M' or char == 'H') and self.matchEither("GET", "SET", self.start + 1)) {
                        next_is_identifier = true;
                        if (char == 'M') self.in_many = true;
                        break :char_blk self.getToken(.Keyword);
                    } else if ((char == 'D' and self.matchSubstring("ELETE", self.start + 1)) or (char == 'F' and self.matchSubstring("LUSH", self.start + 1))) {
                        next_is_identifier = true;
                        break :char_blk self.getToken(.Keyword);
                    } else if (next_is_identifier) {
                        break :char_blk try self.getIdentifier(&next_is_identifier);
                    } else {
                        if (self.in_many) next_is_identifier = true;
                        break :char_blk try self.getLiteral();
                    }
                },
            };
            try self.tokens.append(token);
        }
    }

    fn getIdentifier(self: *Tokenizer, next_is_id: *bool) !Token {
        const token = try self.getIdentifierName();
        const last: Token = self.tokens.getLast();
        switch (last.token_type) {
            .Keyword => {
                if (!std.mem.eql(u8, "HGET", last.lexeme) and !std.mem.eql(u8, "HSET", last.lexeme)) {
                    next_is_id.* = false;
                }
            },
            else => {
                next_is_id.* = false;
            },
        }
        return token;
    }

    fn getIdentifierName(self: *Tokenizer) !Token {
        if (!std.ascii.isAlphabetic(self.char_stream.current()) and self.char_stream.current() != '_') {
            try self.message_stream.writer().print(
                "First letter of name started with invalid char {c}, expected letter or '_'.\n",
                .{self.char_stream.current()},
            );
            return TokenError.InvalidName;
        }
        while (self.char_stream.current() != '\n' and self.char_stream.current() != ' ' and self.char_stream.current() != '\t' and !self.isEoS()) {
            const next = self.advance();
            if (!std.ascii.isAlphanumeric(next) and next != '_') {
                try self.message_stream.writer().print(
                    "Name must be alphanumeric or '_', received char {c} at position {d}.\n",
                    .{ self.char_stream.current(), self.char_stream.position },
                );
                return TokenError.InvalidName;
            }
        }
        return self.getToken(.Identifier);
    }

    fn getToken(self: *Tokenizer, token_type: TokenTypes) Token {
        const lexeme = self.getLexeme();
        return Token{
            .start = self.start,
            .length = self.char_stream.position - self.start,
            .lexeme = lexeme,
            .kwd = if (token_type == .Keyword) Operation.fromString(lexeme) else null,
            .token_type = token_type,
        };
    }

    fn getLiteral(self: *Tokenizer) !Token {
        _ = self.advance();
        switch (self.char_stream.lookBehind(2)) {
            '"' => return try self.getString(),
            else => |char| {
                var next = char;

                while (next != '\n' and next != ' ' and next != '\t' and !self.isEoS()) {
                    next = self.advance();
                }
                if (!self.isEoS()) self.char_stream.position -= 1;
                return self.getToken(.Literal);
            },
        }
    }

    fn getString(self: *Tokenizer) !Token {
        while (!self.isEoS()) {
            if (self.char_stream.next() == '"' and self.char_stream.current() != '\\') {
                _ = self.char_stream.goto(2);
                return self.getToken(.Literal);
            }
            self.char_stream.moveNext();
        }

        return TokenError.UnterminatedString;
    }

    fn advance(self: *Tokenizer) u8 {
        return self.char_stream.advance();
    }

    fn previous(self: *Tokenizer) u8 {
        return self.char_stream.previous();
    }

    fn matchSubstring(self: *Tokenizer, substr: []const u8, starts_at: usize) bool {
        const upto = starts_at + substr.len;

        if (upto >= self.char_stream.length()) return false;
        const slice = self.char_stream.subslice(starts_at, upto);
        if (std.mem.eql(u8, substr, slice)) {
            _ = self.char_stream.goto(substr.len);
            return true;
        }
        return false;
    }

    fn matchEither(self: *Tokenizer, left: []const u8, right: []const u8, starts_at: usize) bool {
        if (self.matchSubstring(left, starts_at)) return true;
        if (self.matchSubstring(right, starts_at)) return true;
        return false;
    }

    fn getLexeme(self: *Tokenizer) []const u8 {
        return self.char_stream.subslice(self.start, self.char_stream.position);
    }

    pub fn toOwnedTokens(self: *Tokenizer) ![]Token {
        return self.tokens.toOwnedSlice();
    }

    fn isEoS(self: Tokenizer) bool {
        return self.char_stream.isEoS();
    }
};

pub fn literalAsValue(literal: []const u8) !Store.Value {
    if (std.mem.eql(u8, literal, "NULL")) {
        return .nil;
    } else if (std.ascii.isDigit(literal[0])) {
        return .{ .number = try std.fmt.parseFloat(f64, literal) };
    } else if (literal[0] == '"') {
        return .{ .bytearray = literal[1 .. literal.len - 1] };
    } else {
        return .{ .bytearray = literal };
    }
}

pub const ParseError = error{
    InvalidToken,
};

pub const Parser = struct {
    token_stream: stream.Stream(Token),
    operation_array: std.ArrayList(Operation),
    operation_stack: std.ArrayList(Store.Value),
    message_stream: std.ArrayList(u8),

    pub fn init(
        allocator: std.mem.Allocator,
        tokens: []Token,
    ) Parser {
        return Parser{
            .token_stream = stream.Stream(Token){ .slice = tokens, .position = 0 },
            .operation_array = std.ArrayList(Operation).init(allocator),
            .operation_stack = std.ArrayList(Store.Value).init(allocator),
            .message_stream = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn deinit(self: Parser) void {
        self.operation_array.deinit();
        self.operation_stack.deinit();
        self.message_stream.deinit();
    }

    pub fn parse(self: *Parser) !void {
        while (!self.token_stream.isEoS()) {
            const token = self.token_stream.advance();
            switch (token.token_type) {
                .Keyword => {
                    try self.handleKeyword();
                },
                else => unreachable,
            }
        }
    }

    fn expects(self: *Parser, comptime token_type: TokenTypes) !Token {
        const token = self.token_stream.advance();
        if (token.token_type != token_type) {
            if (token_type == .NewLine and self.token_stream.isEoS()) return token;
            try self.message_stream.writer().print("Received invalid token, expected {s}, received {}.\n", .{ @tagName(token_type), token });
            return ParseError.InvalidToken;
        }
        return token;
    }

    fn handleKeyword(self: *Parser) !void {
        const token = self.token_stream.previous();
        switch (token.kwd.?) {
            .GET => {
                const id: Token = try self.expects(.Identifier);
                _ = try self.expects(.NewLine);
                try self.operation_array.append(token.kwd.?);
                try self.operation_stack.insert(0, try literalAsValue(id.lexeme));
            },
            .HGET => {
                const map = try self.expects(.Identifier);
                const id = try self.expects(.Identifier);
                _ = try self.expects(.NewLine);
                try self.operation_array.append(token.kwd.?);
                try self.operation_stack.insert(0, try literalAsValue(map.lexeme));
                try self.operation_stack.insert(0, try literalAsValue(id.lexeme));
            },
            .SET => {
                const id: Token = try self.expects(.Identifier);
                const val: Token = try self.expects(.Literal);
                _ = try self.expects(.NewLine);
                try self.operation_array.append(token.kwd.?);
                try self.operation_stack.insert(0, try literalAsValue(id.lexeme));
                try self.operation_stack.insert(0, try literalAsValue(val.lexeme));
            },
            .HSET => {
                const map = try self.expects(.Identifier);
                const id = try self.expects(.Identifier);
                const val = try self.expects(.Literal);
                _ = try self.expects(.NewLine);
                try self.operation_array.append(token.kwd.?);
                try self.operation_stack.insert(0, try literalAsValue(map.lexeme));
                try self.operation_stack.insert(0, try literalAsValue(id.lexeme));
                try self.operation_stack.insert(0, try literalAsValue(val.lexeme));
            },
            .DELETE => {
                const id = try self.expects(.Identifier);
                _ = try self.expects(.NewLine);
                try self.operation_array.append(token.kwd.?);
                try self.operation_stack.insert(0, try literalAsValue(id.lexeme));
            },
            .FLUSH => {
                try self.operation_array.append(token.kwd.?);
                _ = try self.expects(.NewLine);
            },
            .MGET => {
                while (self.token_stream.current().token_type != .NewLine and !self.token_stream.isEoS()) {
                    self.token_stream.moveNext();
                    const current: Token = try self.expects(.Identifier);
                    try self.operation_array.append(.GET);
                    try self.operation_stack.insert(0, try literalAsValue(current.lexeme));
                }
            },
            .MSET => {
                while (self.token_stream.current().token_type != .NewLine and !self.token_stream.isEoS()) {
                    const current: Token = try self.expects(.Identifier);
                    const next = self.token_stream.advance();
                    try self.operation_array.append(.SET);
                    try self.operation_stack.insert(0, try literalAsValue(current.lexeme));
                    switch (next.token_type) {
                        .Literal => try self.operation_stack.insert(0, try literalAsValue(next.lexeme)),
                        else => {
                            try self.message_stream.writer().print("Received invalid token, expected Literal, received {}.\n", .{current});
                            return ParseError.InvalidToken;
                        },
                    }
                }
            },
        }
    }
};

pub fn init(allocator: std.mem.Allocator, store: *Store) Redis {
    return .{ .store = store, .allocator = allocator };
}

// Caller owns slice memory
pub fn run(self: Redis, source: []const u8, writer: anytype) ![]Store.Value {
    var tokenizer = Tokenizer.init(
        self.allocator,
        source,
    );
    defer tokenizer.deinit();
    tokenizer.tokenize() catch |err| {
        try writer.writeAll(tokenizer.message_stream.items);
        return err;
    };
    var parser = Parser.init(
        self.allocator,
        tokenizer.tokens.items,
    );
    defer parser.deinit();
    parser.parse() catch |err| {
        try writer.writeAll(parser.message_stream.items);
        return err;
    };

    var results = std.ArrayList(Store.Value).init(self.allocator);

    for (parser.operation_array.items) |op| {
        switch (op) {
            .GET => {
                const name: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected name after operation.\n", .{});
                    return RedisError.MissingValue;
                };
                const result = switch (name) {
                    .nil => .nil,
                    .bytearray => |val| self.store.get(val),
                    else => {
                        try writer.print("Expected string value for name.\n", .{});
                        return RedisError.UnexpectedValue;
                    },
                };
                try results.append(result);
            },
            .SET => {
                const name: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected name after operation.\n", .{});
                    return RedisError.MissingValue;
                };
                const value: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected value after name.\n", .{});
                    return RedisError.MissingValue;
                };
                switch (name) {
                    .bytearray => |id| try self.store.set(id, value),
                    else => {
                        try writer.print("Expected string value for name.\n", .{});
                        return RedisError.UnexpectedValue;
                    },
                }
            },
            .HGET => {
                const map: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected map after operation.\n", .{});
                    return RedisError.MissingValue;
                };
                const name: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected name after map.\n", .{});
                    return RedisError.MissingValue;
                };
                const result = switch (map) {
                    .nil => .nil,
                    .bytearray => |mapid| switch (name) {
                        .nil => .nil,
                        .bytearray => |val| self.store.hget(mapid, val),
                        else => {
                            try writer.print("Expected string value for name.\n", .{});
                            return RedisError.UnexpectedValue;
                        },
                    },
                    else => {
                        try writer.print("Expected string value for name.\n", .{});
                        return RedisError.UnexpectedValue;
                    },
                };
                try results.append(result);
            },
            .HSET => {
                const map: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected map after operation.\n", .{});
                    return RedisError.MissingValue;
                };
                const name: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected name after map.\n", .{});
                    return RedisError.MissingValue;
                };
                const value: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected value after name.\n", .{});
                    return RedisError.MissingValue;
                };
                switch (map) {
                    .bytearray => |mapid| switch (name) {
                        .bytearray => |id| try self.store.hset(mapid, id, value),
                        else => {
                            try writer.print("Expected string value for name.\n", .{});
                            return RedisError.UnexpectedValue;
                        },
                    },
                    else => {
                        try writer.print("Expected string value for name.\n", .{});
                        return RedisError.UnexpectedValue;
                    },
                }
            },
            .DELETE => {
                const name: Store.Value = parser.operation_stack.popOrNull() orelse {
                    try writer.print("Expected name after operation.\n", .{});
                    return RedisError.MissingValue;
                };
                switch (name) {
                    .bytearray => |val| {
                        var valarr = [_][]const u8{val};
                        const delresult = try self.store.cleanup(&valarr);
                        try results.append(.{ .number = @floatFromInt(delresult) });
                    },
                    else => {
                        try writer.print("Expected string value for name.\n", .{});
                        return RedisError.UnexpectedValue;
                    },
                }
            },
            .FLUSH => {
                var delval = [_][]const u8{"*"};
                const delresult = try self.store.cleanup(&delval);
                try results.append(.{ .number = @floatFromInt(delresult) });
            },
            else => unreachable,
        }
    }

    return try results.toOwnedSlice();
}
