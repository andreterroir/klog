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
const RequestHeader = struct {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: ?[]u8,
};

pub fn read_req_header(r: *Io.Reader, buf: []u8) RequestHeader {
    return RequestHeader{
        .request_api_key = r.takeInt(i16),
        .request_api_version = r.takeInt(i16),
        .correlation_id = r.takeInt(i32),
        .client_id = read_nullable_str(buf),
    };
}

const ReadError = error{
    BufferTooSmall,
};

// NULLABLE_STRING
// Represents a sequence of characters or null. For non-null strings, first the
// length N is given as an INT16. Then N bytes follow which are the UTF-8
// encoding of the character sequence. A null value is encoded with length of
// -1 and there are no following bytes.
pub fn read_nullable_str(r: *Io.Reader, buf: []u8) !?[]u8 {
    const len = r.takeInt(i16);
    if (len == -1) return null;
    if (len > buf.len) return ReadError.BufferTooSmall;
    r.readSliceAll(buf[0..len]);
    return buf[0..len];
}

// Response Header v1 => correlation_id
//  correlation_id => INT32
