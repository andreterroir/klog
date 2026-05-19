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

// Request Header v2 => request_api_key request_api_version correlation_id client_id
//  request_api_key => INT16
//  request_api_version => INT16
//  correlation_id => INT32
//  client_id => NULLABLE_STRING

// Response Header v1 => correlation_id
//  correlation_id => INT32
