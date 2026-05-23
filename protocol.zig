//! https://kafka.apache.org/42/design/protocol/

const std = @import("std");
const net = std.Io.net;
const Io = std.Io;

const BE = std.builtin.Endian.big;

// TODO namespace into protocol rader and writer

/// RequestOrResponse => Size (RequestMessage | ResponseMessage)
///   Size => int32
pub fn read_msg_size(r: *Io.Reader) !i32 {
    return try r.takeInt(i32, BE);
}

pub fn write_msg_size(w: *Io.Writer, size: i32) !void {
    try w.writeInt(i32, size, BE);
}

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

pub fn read_req_header(r: *Io.Reader, buf: []u8) !RequestHeader {
    return RequestHeader{
        .request_api_key = try r.takeInt(i16, BE),
        .request_api_version = try r.takeInt(i16, BE),
        .correlation_id = try r.takeInt(i32, BE),
        .client_id = try read_nullable_str(r, buf),
    };
}

pub fn write_req_header(w: *Io.Writer, header: RequestHeader) !void {
    try w.writeInt(i16, header.request_api_key, BE);
    try w.writeInt(i16, header.request_api_version, BE);
    try w.writeInt(i32, header.correlation_id, BE);
    try write_nullable_str(w, header.client_id);
}

const ReadError = error{
    BufferTooSmall,
    ProtocolError,
};

// NULLABLE_STRING
// Represents a sequence of characters or null. For non-null strings, first the
// length N is given as an INT16. Then N bytes follow which are the UTF-8
// encoding of the character sequence. A null value is encoded with length of
// -1 and there are no following bytes.
fn read_nullable_str(r: *Io.Reader, buf: []u8) !?[]u8 {
    const len = try r.takeInt(i16, BE);
    if (len == -1) return null;
    if (len > buf.len) return ReadError.BufferTooSmall;
    if (len < 0) return ReadError.ProtocolError;
    const l: usize = @intCast(len);
    try r.readSliceAll(buf[0..l]);
    return buf[0..l];
}

fn write_nullable_str(w: *Io.Writer, str: ?[]const u8) !void {
    if (str) |s| {
        const len: i16 = @intCast(s.len);
        try w.writeInt(i16, len, BE);
        try w.writeAll(s);
    } else {
        try w.writeInt(i16, -1, BE);
    }
}

// Response Header v1 => correlation_id
//  correlation_id => INT32
