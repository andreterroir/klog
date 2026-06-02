//! https://kafka.apache.org/43/design/protocol/
//!
//! `reader`, `writer`, and the test block at the bottom of this file are
//! kept in the same order. Append new fields and messages in
//! implementation order, and keep `reader`/`writer` mirror images of each
//! other: a field added to one gets a matching entry in the other, with a
//! test alongside it in the same position.

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

/// Represents the fixed part of the Produce API request.
/// The trailing topic_data array is read separately and not part of this struct.
///
/// Produce Request (Version: 13) => { transactional_id acks timeout_ms (topic_data) }
///   transactional_id => COMPACT_NULLABLE_STRING
///   acks => INT16
///   timeout_ms => INT32
///   topic_data => { topic_id (partition_data) }
pub const ProduceRequest = struct {
    /// The transactional ID, or null if the producer is not
    /// transactional.
    transactional_id: ?[]const u8 = null,
    /// The number of acknowledgments the producer requires the leader to have
    /// received before considering a request complete. Allowed values: 0 for no
    /// acknowledgments, 1 for only the leader and -1 for the full ISR.
    acks: i16,
    /// The timeout to await a response in milliseconds.
    timeout_ms: i32,
    /// The number of topics a request contains data for.
    topic_data_size: u64,
};

// TODO consider client writing the data piece by piece instead

/// topic_id => UUID
/// partition_data => { index records }
pub const TopicData = struct {
    topic_id: [16]u8, // UUID
    partition_data: []const PartitionData,
};

/// index => INT32
/// records => COMPACT_NULLABLE_RECORDS
pub const PartitionData = struct {
    index: i32,
    records: ?[]const u8, // ?[]Record,
};

pub const Record = struct {
    // XXX
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

    fn api_key(r: *Io.Reader) !ApiKey {
        return @enumFromInt(try r.takeInt(i16, BE));
    }

    /// buf has to be large enough to hold transactional_id.
    pub fn produce_req(r: *Io.Reader, buf: []u8) !ProduceRequest {
        return .{
            .transactional_id = try compact_nullable_str(r, buf),
            .acks = try r.takeInt(i16, BE),
            .timeout_ms = try r.takeInt(i32, BE),
            .topic_data_size = try compact_arr_size(r),
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

    /// COMPACT_NULLABLE_STRING Represents a sequence of characters.
    /// First the length N + 1 is given as an UNSIGNED_VARINT . Then N
    /// bytes follow which are the UTF-8 encoding of the character
    /// sequence. A null string is represented with a length of 0.
    fn compact_nullable_str(r: *Io.Reader, buf: []u8) !?[]u8 {
        const len = try unsigned_varint(r);
        if (len == 0) return null;
        const n: usize = @intCast(len - 1);
        if (n > buf.len) return Error.BufferTooSmall;
        try r.readSliceAll(buf[0..n]);
        return buf[0..n];
    }

    /// Represents a sequence of objects of a given
    /// type T. Type T can be either a primitive type
    /// (e.g. STRING) or a structure. First, the length N
    /// + 1 is given as an UNSIGNED_VARINT. Then N
    /// instances of type T follow. In protocol
    /// documentation a compact array of T instances
    /// is referred to as (T).
    fn compact_arr_size(r: *Io.Reader) !u64 {
        return try unsigned_varint(r) - 1;
    }

    /// Unsigned LEB128, as used by Protocol Buffers.
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
        try unsigned_varint(w, req.topic_data_size + 1);
    }

    /// Writes a COMPACT array of partition_data: the element count as a
    /// compact array size, followed by each { index records } entry.
    fn partition_data(w: *Io.Writer, arr: []const PartitionData) !void {
        try compact_arr_size(w, arr.len);
        for (arr) |partition| {
            try w.writeInt(i32, partition.index, BE);
            if (partition.records) |records| {
                try compact_arr_size(w, records.len);
                try w.writeAll(records);
            } else {
                try compact_arr_size(w, null);
            }
        }
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
        if (str) |s| {
            try unsigned_varint(w, @as(u64, s.len) + 1);
            try w.writeAll(s);
        } else {
            try unsigned_varint(w, 0);
        }
    }

    /// Writes a COMPACT array size: length N+1 as an unsigned varint, or 0
    /// for a null array. Also the length prefix for COMPACT_NULLABLE_RECORDS,
    /// where the N raw record-batch bytes follow.
    fn compact_arr_size(w: *Io.Writer, size: ?u64) !void {
        if (size) |s| {
            try unsigned_varint(w, s + 1);
        } else {
            try unsigned_varint(w, 0);
        }
    }

    /// Unsigned LEB128, as used by Protocol Buffers.
    /// https://protobuf.dev/programming-guides/encoding/#varints
    fn unsigned_varint(w: *Io.Writer, v: u64) !void {
        var n = v;
        while (n >= 0x80) : (n >>= 7) {
            try w.writeByte(@as(u8, @intCast(n & 0x7f)) | 0x80);
        }
        try w.writeByte(@intCast(n));
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

test "api_key reads unknown key" {
    var buf: [32]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const unsupported_api_key = 255; // not mapped in ApiKey
    try w.writeInt(i16, unsupported_api_key, BE);

    var r = Io.Reader.fixed(&buf);
    const read = try reader.api_key(&r);
    switch (read) {
        _ => {},
        else => try testing.expect(false),
    }
}

test "produce_req round-trip" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const written = ProduceRequest{
        .transactional_id = "txn-42",
        .acks = -1,
        .timeout_ms = 30_000,
        .topic_data_size = 3,
    };
    try writer.produce_req(&w, written);

    var out: [64]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.produce_req(&r, &out);
    try testing.expect(read.transactional_id != null);
    try testing.expectEqualStrings("txn-42", read.transactional_id.?);
    try testing.expectEqual(-1, read.acks);
    try testing.expectEqual(30_000, read.timeout_ms);
    try testing.expectEqual(3, read.topic_data_size);
}

test "produce_req round-trip without transactional_id" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const written = ProduceRequest{
        .acks = 1,
        .timeout_ms = 50,
        .topic_data_size = 2,
    };
    try writer.produce_req(&w, written);

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.produce_req(&r, &out);
    try testing.expect(read.transactional_id == null);
}

test "partition_data writes count, indices, and records" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const partitions = [_]PartitionData{
        .{ .index = 1, .records = &[_]u8{ 0xde, 0xad, 0xbe } },
        .{ .index = 2, .records = null },
    };
    try writer.partition_data(&w, &partitions);

    try testing.expectEqualSlices(u8, &[_]u8{
        0x03, // compact array size: 2 partitions (N+1)
        0x00, 0x00, 0x00, 0x01, // index 1
        0x04, 0xde, 0xad, 0xbe, // records: size 3 (N+1) then the bytes
        0x00, 0x00, 0x00, 0x02, // index 2
        0x00, // null records
    }, w.buffered());
}

test "partition_data writes empty array" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const partitions = [_]PartitionData{};
    try writer.partition_data(&w, &partitions);

    // Just the compact array size for zero elements (N+1).
    try testing.expectEqualSlices(u8, &[_]u8{0x01}, w.buffered());
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

test "nullable_str rejects too-small buffer" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.nullable_str(&w, "test-client");

    var out: [4]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    try testing.expectError(error.BufferTooSmall, reader.nullable_str(&r, &out));
}

test "nullable_str rejects negative length" {
    // A length below -1 is not a valid null marker and must be rejected.
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try w.writeInt(i16, -2, BE);

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    try testing.expectError(error.ProtocolError, reader.nullable_str(&r, &out));
}

test "compact_nullable_str round-trip" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_nullable_str(&w, "test-client");

    var out: [64]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.compact_nullable_str(&r, &out);
    try testing.expect(read != null);
    try testing.expectEqualStrings("test-client", read.?);
}

test "compact_nullable_str round-trip null" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_nullable_str(&w, null);

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.compact_nullable_str(&r, &out);
    try testing.expect(read == null);
}

test "compact_nullable_str round-trip empty string" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_nullable_str(&w, "");

    var out: [16]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    const read = try reader.compact_nullable_str(&r, &out);
    try testing.expect(read != null);
    try testing.expectEqualStrings("", read.?);
}

test "compact_nullable_str rejects too-small buffer" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_nullable_str(&w, "test-client");

    var out: [4]u8 = undefined;
    var r = Io.Reader.fixed(&buf);
    try testing.expectError(error.BufferTooSmall, reader.compact_nullable_str(&r, &out));
}

test "compact_arr_size round-trip" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);

    const size: u64 = 7;
    try writer.compact_arr_size(&w, size);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(size, try reader.compact_arr_size(&r));
}

test "compact_arr_size encodes null as zero" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_arr_size(&w, null);
    try testing.expectEqualSlices(u8, &[_]u8{0x00}, w.buffered());
}

test "unsigned_varint round-trip" {
    try expectVarint(0, &[_]u8{0x00});
    try expectVarint(1, &[_]u8{0x01});
    // largest 1-byte varint
    try expectVarint(127, &[_]u8{0x7f});
    // smallest 2-byte varint
    try expectVarint(128, &[_]u8{ 0x80, 0x01 });
    // worked example from the protobuf docs
    try expectVarint(150, &[_]u8{ 0x96, 0x01 });
    // largest 2-byte varint
    try expectVarint(16383, &[_]u8{ 0xff, 0x7f });
    // smallest 3-byte varint
    try expectVarint(16384, &[_]u8{ 0x80, 0x80, 0x01 });
    try expectVarint(std.math.maxInt(u32), &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0x0f });
    // largest u64; uses the full 10-byte encoding
    try expectVarint(
        std.math.maxInt(u64),
        &[_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 },
    );
}

/// Asserts `encoded` decodes to `v` and that `v` re-encodes to `encoded`,
/// pinning the exact wire bytes in both directions.
fn expectVarint(v: u64, encoded: []const u8) !void {
    var r = Io.Reader.fixed(encoded);
    try testing.expectEqual(v, try reader.unsigned_varint(&r));

    var buf: [10]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.unsigned_varint(&w, v);
    try testing.expectEqualSlices(u8, encoded, w.buffered());
}

test "unsigned_varint stops at varint boundary" {
    // Bytes following the varint must not be consumed even when their
    // continuation bit is set.
    const buf = [_]u8{ 0x96, 0x01, 0xff, 0xff };
    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(@as(u64, 150), try reader.unsigned_varint(&r));
    try testing.expectEqual(@as(u8, 0xff), try r.takeByte());
    try testing.expectEqual(@as(u8, 0xff), try r.takeByte());
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
