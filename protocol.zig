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
    // Unlike proposed in the KIP, client ID is a strring rather than a compact
    // string for compatibility reasons.
    // https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-FlexibleVersions
    // https://github.com/apache/kafka/blob/9956b49816deb77e5d022f2a42505bd9cfe23fc9/clients/src/main/resources/common/message/RequestHeader.json#L36-L40
    client_id: ?[]const u8,
};

// Response Header v1 => correlation_id
//  correlation_id => INT32
pub const ResponseHeader = struct {
    correlation_id: i32,
};

// TODO support tagged struct fields
// https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=120722234#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-TagHeaders

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

/// The fixed part of a topic_data entry, handled up front so the
/// partitions can be streamed one at a time without buffering the whole
/// entry. The topic array length itself lives in
/// `ProduceRequest.topic_data_size`.
///
/// topic_id => UUID
/// partition_data => { index records }
pub const TopicData = struct {
    topic_id: [16]u8, // UUID
    /// The number of partition_data entries that follow.
    partition_data_size: u64,
};

/// The fixed part of a partition_data entry: the partition index and the
/// size of the records that follow. The records are a COMPACT_NULLABLE byte
/// sequence whose bytes live outside this struct, consumed by the caller
/// after the entry is read or written.
///
/// index => INT32
/// records => COMPACT_NULLABLE_RECORDS
pub const PartitionData = struct {
    index: i32,
    /// The number of record bytes that follow, or null for the
    /// COMPACT_NULLABLE null marker.
    records_size: ?u64,
};

/// Produce Response (Version: 13) => { (responses) throttle_time_ms node_endpoints<tag: 0> }
///   responses => { topic_id (partition_responses) }
///     topic_id => UUID
///     partition_responses => { index error_code base_offset log_append_time_ms log_start_offset (record_errors) error_message current_leader<tag: 0> }
///       index => INT32
///       error_code => INT16
///       base_offset => INT64
///       log_append_time_ms => INT64
///       log_start_offset => INT64
///       record_errors => { batch_index batch_index_error_message }
///         batch_index => INT32
///         batch_index_error_message => COMPACT_NULLABLE_STRING
///       error_message => COMPACT_NULLABLE_STRING
///       current_leader<tag: 0> => { leader_id leader_epoch }
///         leader_id => INT32
///         leader_epoch => INT32
///   throttle_time_ms => INT32
///   node_endpoints<tag: 0> => { node_id host port rack }
///     node_id => INT32
///     host => COMPACT_STRING
///     port => INT32
///     rack => COMPACT_NULLABLE_STRING
///
/// Unlike the request's partition `records` — record batches that can be
/// arbitrarily large and are streamed a chunk at a time, never collected in
/// memory — a produce response carries only small metadata arrays (no record
/// batches), and the topic/partition counts per response are expected to be
/// small. So the response is modeled as a single nested data structure that is
/// read/written at once and kept in memory, rather than streamed. The reader
/// allocates the arrays (and the nullable strings) into a caller-provided
/// allocator; the writer just walks slices the caller already holds.
pub const ProduceResponse = struct {
    responses: []const TopicResponse,
    /// The duration in milliseconds for which the request was throttled.
    throttle_time_ms: i32,
};

/// One topic_id entry in a produce response's `responses` array, with the
/// partition responses held inline rather than streamed.
pub const TopicResponse = struct {
    topic_id: [16]u8, // UUID
    partition_responses: []const PartitionResponse,
};

/// One partition entry in a topic response's `partition_responses` array.
pub const PartitionResponse = struct {
    /// The partition index.
    index: i32,
    /// The error code, or 0 if there was no error.
    error_code: i16,
    /// The base offset assigned to the first record in the batch.
    base_offset: i64,
    /// The timestamp the broker appended the batch, or -1 if not set.
    log_append_time_ms: i64,
    /// The log start offset.
    log_start_offset: i64,
    record_errors: []const RecordError,
    /// The global error message summarizing the failure, or null.
    error_message: ?[]const u8,
};

/// One entry in a partition response's `record_errors` array, describing a
/// record that caused a batch to be rejected.
pub const RecordError = struct {
    /// The batch index of the record that caused the batch to be dropped.
    batch_index: i32,
    /// The error message of the record, or null.
    batch_index_error_message: ?[]const u8,
};

/// Fetch Request (Version: 18) => { max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch (topics) (forgotten_topics_data) rack_id cluster_id<tag: 0> replica_state<tag: 1> }
///   max_wait_ms => INT32
///   min_bytes => INT32
///   max_bytes => INT32
///   isolation_level => INT8
///   session_id => INT32
///   session_epoch => INT32
///   topics => { topic_id (partitions) }
///     topic_id => UUID
///     partitions => { partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes replica_directory_id<tag: 0> high_watermark<tag: 1> }
///       partition => INT32
///       current_leader_epoch => INT32
///       fetch_offset => INT64
///       last_fetched_epoch => INT32
///       log_start_offset => INT64
///       partition_max_bytes => INT32
///       replica_directory_id<tag: 0> => UUID
///       high_watermark<tag: 1> => INT64
///   forgotten_topics_data => { topic_id (partitions) }
///     topic_id => UUID
///     partitions => INT32
///   rack_id => COMPACT_STRING
///   cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
///   replica_state<tag: 1> => { replica_id replica_epoch }
///     replica_id => INT32
///     replica_epoch => INT64
pub const FetchRequest = struct {};

/// Fetch Response (Version: 18) => { throttle_time_ms error_code session_id (responses) node_endpoints<tag: 0> }
///   throttle_time_ms => INT32
///   error_code => INT16
///   session_id => INT32
///   responses => { topic_id (partitions) }
///     topic_id => UUID
///     partitions => { partition_index error_code high_watermark last_stable_offset log_start_offset ?(aborted_transactions) preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> }
///       partition_index => INT32
///       error_code => INT16
///       high_watermark => INT64
///       last_stable_offset => INT64
///       log_start_offset => INT64
///       aborted_transactions => { producer_id first_offset }
///         producer_id => INT64
///         first_offset => INT64
///       preferred_read_replica => INT32
///       records => COMPACT_NULLABLE_RECORDS
///       diverging_epoch<tag: 0> => { epoch end_offset }
///         epoch => INT32
///         end_offset => INT64
///       current_leader<tag: 1> => { leader_id leader_epoch }
///         leader_id => INT32
///         leader_epoch => INT32
///       snapshot_id<tag: 2> => { end_offset epoch }
///         end_offset => INT64
///         epoch => INT32
///   node_endpoints<tag: 0> => { node_id host port rack }
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
            // topic_data is a non-nullable COMPACT array; reject the null marker.
            .topic_data_size = (try compact_size(r)) orelse return Error.ProtocolError,
        };
    }

    /// Reads one topic_data entry's fixed part: the topic_id UUID followed
    /// by the partition_data array length. The caller then reads
    /// `partition_data_size` partitions with `partition_data`. There are
    /// `ProduceRequest.topic_data_size` of these entries in a request.
    pub fn topic_data(r: *Io.Reader) !TopicData {
        var topic_id: [16]u8 = undefined;
        try r.readSliceAll(&topic_id);
        return .{
            .topic_id = topic_id,
            // partition_data is a non-nullable COMPACT array; reject the null marker.
            .partition_data_size = (try compact_size(r)) orelse return Error.ProtocolError,
        };
    }

    /// Reads one partition_data entry's fixed part: the partition index and
    /// the records length. The caller then consumes `records_size` record
    /// bytes (or none when null) before reading the next partition.
    pub fn partition_data(r: *Io.Reader) !PartitionData {
        return .{
            .index = try r.takeInt(i32, BE),
            .records_size = try compact_size(r),
        };
    }

    pub fn resp_header(r: *Io.Reader) !ResponseHeader {
        return .{ .correlation_id = try r.takeInt(i32, BE) };
    }

    /// Reads a whole produce response into a single nested structure, with the
    /// `responses` array (and everything below it) allocated into `gpa`. Pass
    /// an arena to free the entire tree at once.
    pub fn produce_resp(r: *Io.Reader, gpa: std.mem.Allocator) !ProduceResponse {
        const responses = try gpa.alloc(TopicResponse, try array_len(r));
        for (responses) |*resp| resp.* = try topic_response(r, gpa);
        return .{
            .responses = responses,
            .throttle_time_ms = try r.takeInt(i32, BE),
        };
    }

    fn topic_response(r: *Io.Reader, gpa: std.mem.Allocator) !TopicResponse {
        var topic_id: [16]u8 = undefined;
        try r.readSliceAll(&topic_id);
        const partitions = try gpa.alloc(PartitionResponse, try array_len(r));
        for (partitions) |*p| p.* = try partition_response(r, gpa);
        return .{ .topic_id = topic_id, .partition_responses = partitions };
    }

    fn partition_response(r: *Io.Reader, gpa: std.mem.Allocator) !PartitionResponse {
        const index = try r.takeInt(i32, BE);
        const error_code = try r.takeInt(i16, BE);
        const base_offset = try r.takeInt(i64, BE);
        const log_append_time_ms = try r.takeInt(i64, BE);
        const log_start_offset = try r.takeInt(i64, BE);
        const record_errors = try gpa.alloc(RecordError, try array_len(r));
        for (record_errors) |*e| e.* = try record_error(r, gpa);
        return .{
            .index = index,
            .error_code = error_code,
            .base_offset = base_offset,
            .log_append_time_ms = log_append_time_ms,
            .log_start_offset = log_start_offset,
            .record_errors = record_errors,
            .error_message = try compact_nullable_str_alloc(r, gpa),
        };
    }

    fn record_error(r: *Io.Reader, gpa: std.mem.Allocator) !RecordError {
        return .{
            .batch_index = try r.takeInt(i32, BE),
            .batch_index_error_message = try compact_nullable_str_alloc(r, gpa),
        };
    }

    /// Reads the length of a non-nullable COMPACT array, rejecting the null
    /// marker. The response arrays (responses, partition_responses,
    /// record_errors) are all non-nullable.
    fn array_len(r: *Io.Reader) !usize {
        return @intCast((try compact_size(r)) orelse return Error.ProtocolError);
    }

    /// Like `compact_nullable_str`, but allocates the string into `gpa` instead
    /// of borrowing a caller buffer — used by the produce response, which is
    /// read into an owned data structure rather than streamed.
    fn compact_nullable_str_alloc(r: *Io.Reader, gpa: std.mem.Allocator) !?[]const u8 {
        const n: usize = @intCast((try compact_size(r)) orelse return null);
        const buf = try gpa.alloc(u8, n);
        try r.readSliceAll(buf);
        return buf;
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
        const n: usize = @intCast((try compact_size(r)) orelse return null);
        if (n > buf.len) return Error.BufferTooSmall;
        try r.readSliceAll(buf[0..n]);
        return buf[0..n];
    }

    /// Reads the length N of a COMPACT array, encoded as N + 1 in an
    /// UNSIGNED_VARINT, or null when the length is 0 — the COMPACT_NULLABLE
    /// null marker. The same length-prefix scheme is used for compact byte
    /// sequences (e.g. partition records), so this applies to both. In
    /// protocol documentation a compact array of T instances is referred to
    /// as (T). Mirrors the writer's `compact_size`; non-nullable callers
    /// reject the null marker.
    pub fn compact_size(r: *Io.Reader) !?u64 {
        const len = try unsigned_varint(r);
        if (len == 0) return null;
        return len - 1;
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
        // topic_data is a non-nullable COMPACT array; the count writes as N+1.
        try compact_size(w, req.topic_data_size);
    }

    /// Writes one topic_data entry's fixed part: the topic_id UUID followed
    /// by the partition_data array length. Each of the `partition_data_size`
    /// partitions is then written with `partition_data`. The topic array
    /// length itself is written by `produce_req` (topic_data_size).
    pub fn topic_data(w: *Io.Writer, topic: TopicData) !void {
        try w.writeAll(&topic.topic_id);
        // partition_data is a non-nullable COMPACT array, like topic_data_size.
        try compact_size(w, topic.partition_data_size);
    }

    /// Writes one partition_data entry's fixed part: the partition index and
    /// the records length prefix. The caller then writes the `records_size`
    /// record bytes (none when null), mirroring how `reader.partition_data`
    /// reads the index and length and leaves the records for the caller.
    pub fn partition_data(w: *Io.Writer, partition: PartitionData) !void {
        try w.writeInt(i32, partition.index, BE);
        // records is a COMPACT_NULLABLE byte sequence; its bytes follow,
        // written by the caller.
        try compact_size(w, partition.records_size);
    }

    pub fn resp_header(w: *Io.Writer, header: ResponseHeader) !void {
        try w.writeInt(i32, header.correlation_id, BE);
    }

    /// Writes a whole produce response from a single nested structure. Unlike
    /// the streamed request, the response's small metadata arrays are held in
    /// memory, so the caller passes the fully populated struct and it is
    /// written at once. Mirrors `produce_resp` on the reader.
    pub fn produce_resp(w: *Io.Writer, resp: ProduceResponse) !void {
        try array_len(w, resp.responses.len);
        for (resp.responses) |topic| try topic_response(w, topic);
        try w.writeInt(i32, resp.throttle_time_ms, BE);
    }

    fn topic_response(w: *Io.Writer, topic: TopicResponse) !void {
        try w.writeAll(&topic.topic_id);
        try array_len(w, topic.partition_responses.len);
        for (topic.partition_responses) |p| try partition_response(w, p);
    }

    fn partition_response(w: *Io.Writer, partition: PartitionResponse) !void {
        try w.writeInt(i32, partition.index, BE);
        try w.writeInt(i16, partition.error_code, BE);
        try w.writeInt(i64, partition.base_offset, BE);
        try w.writeInt(i64, partition.log_append_time_ms, BE);
        try w.writeInt(i64, partition.log_start_offset, BE);
        try array_len(w, partition.record_errors.len);
        for (partition.record_errors) |e| try record_error(w, e);
        try compact_nullable_str(w, partition.error_message);
    }

    fn record_error(w: *Io.Writer, e: RecordError) !void {
        try w.writeInt(i32, e.batch_index, BE);
        try compact_nullable_str(w, e.batch_index_error_message);
    }

    /// Writes the length of a non-nullable COMPACT array. Mirrors the reader's
    /// `array_len`.
    fn array_len(w: *Io.Writer, len: usize) !void {
        try compact_size(w, @as(u64, @intCast(len)));
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
        try compact_size_of(w, str);
        if (str) |s| try w.writeAll(s);
    }

    /// Writes the length N of a COMPACT array as N+1 in an UNSIGNED_VARINT,
    /// or 0 for null — the COMPACT_NULLABLE null marker. The inverse of the
    /// reader's `compact_size`, which turns the same encoding back into
    /// `?u64`. Non-nullable callers pass the count directly (it coerces to
    /// the optional and never writes the null marker). The caller writes the
    /// N elements/bytes afterwards.
    pub fn compact_size(w: *Io.Writer, len: ?u64) !void {
        if (len) |n| {
            try unsigned_varint(w, n + 1);
        } else {
            try unsigned_varint(w, 0);
        }
    }

    /// Writes the COMPACT_NULLABLE length prefix of a (possibly null) slice,
    /// deferring to `compact_size`. Pass the slice itself — e.g. a compact
    /// string or a partition's records — rather than a precomputed count.
    pub fn compact_size_of(w: *Io.Writer, arr: ?[]const u8) !void {
        const len: ?u64 = if (arr) |a| @intCast(a.len) else null;
        try compact_size(w, len);
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
    try testing.expectEqual(1024, try reader.msg_size(&r));
}

test "msg_size round-trip zero" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.msg_size(&w, 0);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(0, try reader.msg_size(&r));
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

test "topic_data round-trip" {
    var buf: [32]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const topic_id = [16]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    try writer.topic_data(&w, .{ .topic_id = topic_id, .partition_data_size = 2 });

    var r = Io.Reader.fixed(&buf);
    const read = try reader.topic_data(&r);
    try testing.expectEqualSlices(u8, &topic_id, &read.topic_id);
    try testing.expectEqual(2, read.partition_data_size);
}

test "topic_data round-trip with no partitions" {
    var buf: [32]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const topic_id = [_]u8{0xab} ** 16;
    try writer.topic_data(&w, .{ .topic_id = topic_id, .partition_data_size = 0 });

    var r = Io.Reader.fixed(&buf);
    const read = try reader.topic_data(&r);
    try testing.expectEqualSlices(u8, &topic_id, &read.topic_id);
    try testing.expectEqual(0, read.partition_data_size);
}

test "partition round-trip with records" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const records = [_]u8{ 0xde, 0xad, 0xbe };
    // index and records length, then the record bytes.
    try writer.partition_data(&w, .{ .index = 1, .records_size = records.len });
    try w.writeAll(&records);

    var r = Io.Reader.fixed(&buf);
    const read = try reader.partition_data(&r);
    try testing.expectEqual(1, read.index);
    try testing.expectEqual(records.len, read.records_size.?);
    // The record bytes are left in the stream for the caller to consume.
    var out: [3]u8 = undefined;
    try r.readSliceAll(&out);
    try testing.expectEqualSlices(u8, &records, &out);
}

test "partition round-trip with null records" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.partition_data(&w, .{ .index = 2, .records_size = null });

    var r = Io.Reader.fixed(&buf);
    const read = try reader.partition_data(&r);
    try testing.expectEqual(2, read.index);
    try testing.expectEqual(null, read.records_size);
}

test "resp_header round-trip" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.resp_header(&w, .{ .correlation_id = 42 });

    var r = Io.Reader.fixed(&buf);
    const read = try reader.resp_header(&r);
    try testing.expectEqual(42, read.correlation_id);
}

test "record_error round-trip" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.record_error(&w, .{ .batch_index = 7, .batch_index_error_message = "bad record" });

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var r = Io.Reader.fixed(&buf);
    const read = try reader.record_error(&r, arena.allocator());
    try testing.expectEqual(7, read.batch_index);
    try testing.expect(read.batch_index_error_message != null);
    try testing.expectEqualStrings("bad record", read.batch_index_error_message.?);
}

test "record_error round-trip with null message" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.record_error(&w, .{ .batch_index = 0, .batch_index_error_message = null });

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var r = Io.Reader.fixed(&buf);
    const read = try reader.record_error(&r, arena.allocator());
    try testing.expectEqual(0, read.batch_index);
    try testing.expect(read.batch_index_error_message == null);
}

test "partition_response round-trip with record_errors" {
    var buf: [128]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const written = PartitionResponse{
        .index = 3,
        .error_code = 9,
        .base_offset = 100,
        .log_append_time_ms = -1,
        .log_start_offset = 50,
        .record_errors = &.{
            .{ .batch_index = 1, .batch_index_error_message = "oops" },
        },
        .error_message = "partition failed",
    };
    try writer.partition_response(&w, written);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var r = Io.Reader.fixed(&buf);
    const read = try reader.partition_response(&r, arena.allocator());
    try testing.expectEqual(3, read.index);
    try testing.expectEqual(9, read.error_code);
    try testing.expectEqual(100, read.base_offset);
    try testing.expectEqual(-1, read.log_append_time_ms);
    try testing.expectEqual(50, read.log_start_offset);
    try testing.expectEqual(1, read.record_errors.len);
    try testing.expectEqual(1, read.record_errors[0].batch_index);
    try testing.expectEqualStrings("oops", read.record_errors[0].batch_index_error_message.?);
    try testing.expect(read.error_message != null);
    try testing.expectEqualStrings("partition failed", read.error_message.?);
}

test "partition_response round-trip without errors" {
    var buf: [64]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const written = PartitionResponse{
        .index = 0,
        .error_code = 0,
        .base_offset = 0,
        .log_append_time_ms = -1,
        .log_start_offset = 0,
        .record_errors = &.{},
        .error_message = null,
    };
    try writer.partition_response(&w, written);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var r = Io.Reader.fixed(&buf);
    const read = try reader.partition_response(&r, arena.allocator());
    try testing.expectEqual(0, read.error_code);
    try testing.expectEqual(0, read.record_errors.len);
    try testing.expect(read.error_message == null);
}

test "topic_response round-trip" {
    var buf: [128]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const topic_id = [_]u8{0xab} ** 16;
    const written = TopicResponse{
        .topic_id = topic_id,
        .partition_responses = &.{
            .{
                .index = 0,
                .error_code = 0,
                .base_offset = 10,
                .log_append_time_ms = -1,
                .log_start_offset = 0,
                .record_errors = &.{},
                .error_message = null,
            },
            .{
                .index = 1,
                .error_code = 0,
                .base_offset = 20,
                .log_append_time_ms = -1,
                .log_start_offset = 0,
                .record_errors = &.{},
                .error_message = null,
            },
        },
    };
    try writer.topic_response(&w, written);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var r = Io.Reader.fixed(&buf);
    const read = try reader.topic_response(&r, arena.allocator());
    try testing.expectEqualSlices(u8, &topic_id, &read.topic_id);
    try testing.expectEqual(2, read.partition_responses.len);
    try testing.expectEqual(0, read.partition_responses[0].index);
    try testing.expectEqual(10, read.partition_responses[0].base_offset);
    try testing.expectEqual(1, read.partition_responses[1].index);
    try testing.expectEqual(20, read.partition_responses[1].base_offset);
}

test "produce_resp round-trip" {
    var buf: [256]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    const topic1 = [_]u8{0x11} ** 16;
    const topic2 = [_]u8{0x22} ** 16;
    const written = ProduceResponse{
        .responses = &.{
            .{
                .topic_id = topic1,
                .partition_responses = &.{
                    .{
                        .index = 0,
                        .error_code = 0,
                        .base_offset = 0,
                        .log_append_time_ms = -1,
                        .log_start_offset = 0,
                        .record_errors = &.{},
                        .error_message = null,
                    },
                },
            },
            .{
                .topic_id = topic2,
                .partition_responses = &.{},
            },
        },
        .throttle_time_ms = 100,
    };
    try writer.produce_resp(&w, written);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var r = Io.Reader.fixed(&buf);
    const read = try reader.produce_resp(&r, arena.allocator());
    try testing.expectEqual(2, read.responses.len);
    try testing.expectEqualSlices(u8, &topic1, &read.responses[0].topic_id);
    try testing.expectEqual(1, read.responses[0].partition_responses.len);
    try testing.expectEqualSlices(u8, &topic2, &read.responses[1].topic_id);
    try testing.expectEqual(0, read.responses[1].partition_responses.len);
    try testing.expectEqual(100, read.throttle_time_ms);
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

test "compact_size round-trip" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_size(&w, 7);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(7, try reader.compact_size(&r));
}

test "compact_size_of round-trip" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);

    // compact_size_of takes the slice and writes its length; here that is 7.
    const arr = [_]u8{0} ** 7;
    try writer.compact_size_of(&w, &arr);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(arr.len, try reader.compact_size(&r));
}

test "compact_size round-trip null" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_size(&w, null);

    var r = Io.Reader.fixed(&buf);
    try testing.expectEqual(null, try reader.compact_size(&r));
}

test "compact_size encodes null as zero" {
    var buf: [16]u8 = undefined;
    var w = Io.Writer.fixed(&buf);
    try writer.compact_size(&w, null);
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
    try testing.expectEqual(150, try reader.unsigned_varint(&r));
    try testing.expectEqual(0xff, try r.takeByte());
    try testing.expectEqual(0xff, try r.takeByte());
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
