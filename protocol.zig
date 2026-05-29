//! https://kafka.apache.org/43/design/protocol/

const std = @import("std");
const net = std.Io.net;
const Io = std.Io;

const BE = std.builtin.Endian.big;

/// https://kafka.apache.org/43/design/protocol/#api-keys
pub const ApiKey = enum(i16) {
    produce = 0,
    fetch = 1,
    _,
};

/// Request Header v2 => request_api_key request_api_version correlation_id client_id
///   request_api_key => INT16
///   request_api_version => INT16
///   correlation_id => INT32
///   client_id => NULLABLE_STRING
pub const RequestHeader = struct {
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: ?[]const u8,
};

// Response Header v1 => correlation_id
//  correlation_id => INT32
pub const ResponseHeader = struct {};

/// Represents the fixed part of Produce API request.
///
/// Produce Request (Version: 13) => { transactional_id acks timeout_ms (topic_data) }
///   transactional_id => COMPACT_NULLABLE_STRING
///   acks => INT16
///   timeout_ms => INT32
///   topic_data => { topic_id (partition_data) }
///     topic_id => UUID
///     partition_data => { index records }
///       index => INT32
///       records => COMPACT_NULLABLE_RECORDS
pub const ProduceRequest = struct {
    transactional_id: ?[]const u8,
    acks: i16,
    timeout_ms: i32,
    // TODO implement UUID type
    topic_id: [16]u8 = undefined,
    partition_index: i32,
};

/// Produce Response (Version: 12) => [responses] throttle_time_ms node_endpoints]<tag: 0>
///   responses => name [partition_responses]
///     name => COMPACT_STRING
///     partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0>
///       index => INT32
///       error_code => INT16
///       base_offset => INT64
///       log_append_time_ms => INT64
///       log_start_offset => INT64
///       record_errors => batch_index batch_index_error_message
///         batch_index => INT32
///         batch_index_error_message => COMPACT_NULLABLE_STRING
///       error_message => COMPACT_NULLABLE_STRING
///       current_leader<tag: 0> => leader_id leader_epoch
///         leader_id => INT32
///         leader_epoch => INT32
///   throttle_time_ms => INT32
///   node_endpoints<tag: 0> => node_id host port rack
///     node_id => INT32
///     host => COMPACT_STRING
///     port => INT32
///     rack => COMPACT_NULLABLE_STRING[
pub const ProduceResponse = struct {};

/// Fetch Request (Version: 18) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id cluster_id<tag: 0> replica_state<tag: 1>
///   max_wait_ms => INT32
///   min_bytes => INT32
///   max_bytes => INT32
///   isolation_level => INT8
///   session_id => INT32
///   session_epoch => INT32
///   topics => topic_id [partitions]
///     topic_id => UUID
///     partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes replica_directory_id<tag: 0> high_watermark<tag: 1>
///       partition => INT32
///       current_leader_epoch => INT32
///       fetch_offset => INT64
///       last_fetched_epoch => INT32
///       log_start_offset => INT64
///       partition_max_bytes => INT32
///       replica_directory_id<tag: 0> => UUID
///       high_watermark<tag: 1> => INT64
///   forgotten_topics_data => topic_id [partitions]
///     topic_id => UUID
///     partitions => INT32
///   rack_id => COMPACT_STRING
///   cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
///   replica_state<tag: 1> => replica_id replica_epoch
///     replica_id => INT32
///     replica_epoch => INT64
pub const FetchRequest = struct {};

/// Fetch Response (Version: 18) => throttle_time_ms error_code session_id [responses] [node_endpoints]<tag: 0>
///   throttle_time_ms => INT32
///   error_code => INT16
///   session_id => INT32
///   responses => topic_id [partitions]
///     topic_id => UUID
///     partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2>
///       partition_index => INT32
///       error_code => INT16
///       high_watermark => INT64
///       last_stable_offset => INT64
///       log_start_offset => INT64
///       aborted_transactions => producer_id first_offset
///         producer_id => INT64
///         first_offset => INT64
///       preferred_read_replica => INT32
///       records => COMPACT_RECORDS
///       diverging_epoch<tag: 0> => epoch end_offset
///         epoch => INT32
///         end_offset => INT64
///       current_leader<tag: 1> => leader_id leader_epoch
///         leader_id => INT32
///         leader_epoch => INT32
///       snapshot_id<tag: 2> => end_offset epoch
///         end_offset => INT64
///         epoch => INT32
///   node_endpoints<tag: 0> => node_id host port rack
///     node_id => INT32
///     host => COMPACT_STRING
///     port => INT32
///     rack => COMPACT_NULLABLE_STRING
pub const FetchResponse = struct {};

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
            .api_key = try api_key(r),
            .api_version = try r.takeInt(i16, BE),
            .correlation_id = try r.takeInt(i32, BE),
            .client_id = try nullable_str(r, buf),
        };
    }

    /// buf has to be large enough to hold transactional_id
    pub fn produce_req(r: *Io.Reader, buf: []u8) !ProduceRequest {
        var req = ProduceRequest{
            .transactional_id = try compact_nullable_str(r, buf),
            .acks = try r.takeInt(i16, BE),
            .timeout_ms = try r.takeInt(i32, BE),
            .partition_index = try r.takeInt(i32, BE),
        };
        // FIXME must be read before partition index
        try r.readSliceAll(&req.topic_id);
        return req;
    }

    fn api_key(r: *Io.Reader) !ApiKey {
        return @enumFromInt(try r.takeInt(i16, BE));
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

    /// COMPACT_NULLABLE_STRING Represents a sequence of characters.
    //First the length N + 1 is given as an UNSIGNED_VARINT . Then N
    //bytes follow which are the UTF-8 encoding of the character
    //sequence. A null string is represented with a length of 0.
    fn compact_nullable_str(r: *Io.Reader, buf: []u8) !?[]u8 {
        // XXX
        _ = r;
        _ = buf;
        return null;
    }

    /// https://protobuf.dev/programming-guides/encoding/#varints
    fn unsigned_varint(r: *Io.Reader) !u64 {
        var res: u64 = 0;
        var i: usize = 0;
        var b: u8 = 0;
        while (i < 10) : (i += 1) {
            b = try r.takeByte();
            res |= @as(u64, b & 0x7f) << @intCast(7 * i);
            if (b & 0x80 == 0) break;
        }
        // Reject over-long input and 10th-byte payloads that don't fit in u64.
        if (b & 0x80 != 0 or (i == 9 and b & 0x7f > 1)) return Error.ProtocolError;
        return res;
    }
};

pub const writer = struct {
    pub fn msg_size(w: *Io.Writer, size: i32) !void {
        try w.writeInt(i32, size, BE);
    }

    pub fn req_header(w: *Io.Writer, header: RequestHeader) !void {
        try w.writeInt(i16, @intFromEnum(header.api_key), BE);
        try w.writeInt(i16, header.api_version, BE);
        try w.writeInt(i32, header.correlation_id, BE);
        try nullable_str(w, header.client_id);
    }

    pub fn produce_req(w: *Io.Writer, req: ProduceRequest) !void {
        try compact_nullable_str(w, req.transactional_id);
        try w.writeInt(i16, req.acks, BE);
        try w.writeInt(i32, req.timeout_ms, BE);
        try w.writeAll(&req.topic_id);
        try w.writeInt(i32, req.partition_index, BE);
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

    fn compact_nullable_str(w: *Io.Writer, str: ?[]const u8) !void {
        // XXX
        _ = w;
        _ = str;
    }

    fn unsigned_varint(w: *Io.Writer, v: u64) !void {
        // XXX
        _ = w;
        _ = v;
    }
};

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

test "readInt" {
    var buf = [_]u8{0} ** 8;
    buf[0] = 1;
    try testing.expectEqual(1, std.mem.readInt(u64, &buf, std.builtin.Endian.little));
}

test "unsigned_varint" {
    try expectVarintBytes(&[_]u8{0x00}, 0);
    try expectVarintBytes(&[_]u8{0x01}, 1);
    // largest 1-byte varint
    try expectVarintBytes(&[_]u8{0x7f}, 127);
    // smallest 2-byte varint
    try expectVarintBytes(&[_]u8{ 0x80, 0x01 }, 128);
    try expectVarintBytes(&[_]u8{ 0x96, 0x01 }, 150);
    // largest 2-byte varint
    try expectVarintBytes(&[_]u8{ 0xff, 0x7f }, 16383);
    // smallest 3-byte varint
    try expectVarintBytes(&[_]u8{ 0x80, 0x80, 0x01 }, 16384);
    try expectVarintBytes(
        &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0x0f },
        std.math.maxInt(u32),
    );
    // largest u64; uses the full 10-byte encoding
    try expectVarintBytes(
        &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 },
        std.math.maxInt(u64),
    );

    // Bytes following the varint must not be consumed even when their
    // continuation bit is set.
    const buf = [_]u8{ 0x96, 0x01, 0xff, 0xff };
    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(@as(u64, 150), try reader.unsigned_varint(&r));
    try testing.expectEqual(@as(u8, 0xff), try r.takeByte());
    try testing.expectEqual(@as(u8, 0xff), try r.takeByte());
}

fn expectVarintBytes(bytes: []const u8, v: u64) !void {
    var r = Io.Reader.fixed(bytes);
    try testing.expectEqual(v, try reader.unsigned_varint(&r));
}

test "unsigned_varint rejects over-long input" {
    const buf = [_]u8{0xff} ** 11;
    var r = Io.Reader.fixed(&buf);
    try testing.expectError(error.ProtocolError, reader.unsigned_varint(&r));
}

test "unsigned_varint rejects 10th-byte payload overflow" {
    // Nine continuation bytes followed by a 10th byte whose payload (0x02)
    // does not fit in the single remaining bit of a u64.
    const buf = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02 };
    var r = Io.Reader.fixed(&buf);
    try testing.expectError(error.ProtocolError, reader.unsigned_varint(&r));
}

test "nullable_str round-trip non-null" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, "test-client");

    var out: [64]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.nullable_str(&r, &out);
    try testing.expect(read != null);
    try testing.expectEqualStrings("test-client", read.?);
}

test "nullable_str round-trip null" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, null);

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.nullable_str(&r, &out);
    try testing.expect(read == null);
}

test "nullable_str round-trip empty string" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, "");

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.nullable_str(&r, &out);
    try testing.expect(read != null);
    try testing.expectEqualStrings("", read.?);
}

test "compact_nullable_str round-trip" {}

test "compact_nullable_str round-trip null" {}

test "compact_nullable_str round-trip empty string" {}

test "req_header round-trip with client_id" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const written = RequestHeader{
        .api_key = .produce,
        .api_version = 13,
        .correlation_id = 42,
        .client_id = "test-client",
    };
    try writer.req_header(&w, written);

    var out: [64]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.req_header(&r, &out);
    try testing.expectEqual(written.api_key, read.api_key);
    try testing.expectEqual(written.api_version, read.api_version);
    try testing.expectEqual(written.correlation_id, read.correlation_id);
    try testing.expect(read.client_id != null);
    try testing.expectEqualStrings("test-client", read.client_id.?);
}

test "req_header round-trip with null client_id" {
    var buf: [32]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const written = RequestHeader{
        .api_key = .fetch,
        .api_version = 7,
        .correlation_id = -1,
        .client_id = null,
    };
    try writer.req_header(&w, written);

    var out: [32]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.req_header(&r, &out);
    try testing.expect(read.client_id == null);
}

test "req_header round-trip with unsupported api_key" {
    var buf: [32]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const unsupported_api_key = 255; // not mapped in ApiKey
    try w.writeInt(i16, unsupported_api_key, BE);

    var r = Io.Reader.fixed(&buf);
    const read = try reader.api_key(&r);
    switch (read) {
        _ => {},
        else => try std.testing.expect(false),
    }
}

test "produce request" {
    // XXX
}

test "produce request without transactional_id" {
    // XXX
}
