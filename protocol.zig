//! https://kafka.apache.org/42/design/protocol/

const std = @import("std");
const net = std.Io.net;
const Io = std.Io;

const BE = std.builtin.Endian.big;

/// Request Header v2 => request_api_key request_api_version correlation_id client_id
///   request_api_key => INT16
///   request_api_version => INT16
///   correlation_id => INT32
///   client_id => NULLABLE_STRING
pub const RequestHeader = struct {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: ?[]const u8,
};

pub const reader = struct {
    const Error = error{
        BufferTooSmall,
        ProtocolError,
    };

    /// RequestOrResponse => Size (RequestMessage | ResponseMessage)
    ///   Size => int32
    pub fn msg_size(r: *Io.Reader) !i32 {
        return try r.takeInt(i32, BE);
    }

    pub fn req_header(r: *Io.Reader, buf: []u8) !RequestHeader {
        return RequestHeader{
            .request_api_key = try r.takeInt(i16, BE),
            .request_api_version = try r.takeInt(i16, BE),
            .correlation_id = try r.takeInt(i32, BE),
            .client_id = try nullable_str(r, buf),
        };
    }

    /// NULLABLE_STRING
    /// Represents a sequence of characters or null. For non-null strings, first the
    /// length N is given as an INT16. Then N bytes follow which are the UTF-8
    /// encoding of the character sequence. A null value is encoded with length of
    /// -1 and there are no following bytes.
    fn nullable_str(r: *Io.Reader, buf: []u8) !?[]u8 {
        const len = try r.takeInt(i16, BE);
        if (len == -1) return null;
        if (len > buf.len) return Error.BufferTooSmall;
        if (len < 0) return Error.ProtocolError;
        const l: usize = @intCast(len);
        try r.readSliceAll(buf[0..l]);
        return buf[0..l];
    }
};

pub const writer = struct {
    pub fn msg_size(w: *Io.Writer, size: i32) !void {
        try w.writeInt(i32, size, BE);
    }

    pub fn req_header(w: *Io.Writer, header: RequestHeader) !void {
        try w.writeInt(i16, header.request_api_key, BE);
        try w.writeInt(i16, header.request_api_version, BE);
        try w.writeInt(i32, header.correlation_id, BE);
        try nullable_str(w, header.client_id);
    }

    fn nullable_str(w: *Io.Writer, str: ?[]const u8) !void {
        if (str) |s| {
            const len: i16 = @intCast(s.len);
            try w.writeInt(i16, len, BE);
            try w.writeAll(s);
        } else {
            try w.writeInt(i16, -1, BE);
        }
    }
};

// Response Header v1 => correlation_id
//  correlation_id => INT32

const testing = std.testing;

test "msg_size round-trip" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.msg_size(&w, 1024);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(@as(i32, 1024), try reader.msg_size(&r));
}

test "msg_size round-trip zero" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.msg_size(&w, 0);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(@as(i32, 0), try reader.msg_size(&r));
}

test "nullable_str round-trip non-null" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, "test-client");

    var out: [64]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const got = try reader.nullable_str(&r, &out);
    try testing.expect(got != null);
    try testing.expectEqualStrings("test-client", got.?);
}

test "nullable_str round-trip null" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, null);

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const got = try reader.nullable_str(&r, &out);
    try testing.expect(got == null);
}

test "nullable_str round-trip empty string" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, "");

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const got = try reader.nullable_str(&r, &out);
    try testing.expect(got != null);
    try testing.expectEqualStrings("", got.?);
}

test "req_header round-trip with client_id" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const original = RequestHeader{
        .request_api_key = 0,
        .request_api_version = 13,
        .correlation_id = 42,
        .client_id = "test-client",
    };
    try writer.req_header(&w, original);

    var out: [64]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const got = try reader.req_header(&r, &out);
    try testing.expect(got.client_id != null);
    try testing.expectEqual(original.request_api_key, got.request_api_key);
    try testing.expectEqual(original.request_api_version, got.request_api_version);
    try testing.expectEqual(original.correlation_id, got.correlation_id);
    try testing.expectEqualStrings(original.client_id.?, got.client_id.?);
}

test "req_header round-trip with null client_id" {
    var buf: [32]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const original = RequestHeader{
        .request_api_key = 1,
        .request_api_version = 7,
        .correlation_id = -1,
        .client_id = null,
    };
    try writer.req_header(&w, original);

    var out: [32]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const got = try reader.req_header(&r, &out);
    try testing.expect(got.client_id == null);
}
