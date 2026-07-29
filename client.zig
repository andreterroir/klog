const std = @import("std");
const log = std.log;
const net = std.Io.net;

const proto = @import("protocol");

const server_port = 8080;

pub fn main(init: std.process.Init) !void {
    const writer = proto.writer;

    const io = init.io;

    // Resolve the host name and connect, letting the standard library race
    // IPv6 and IPv4 candidates (Happy Eyeballs). On dual-stack hosts IPv6 is
    // attempted first; IPv4-only hosts fall back transparently.
    const host = try net.HostName.init("localhost");

    const LookupResult = net.HostName.LookupResult;
    var lookup_buf: [16]LookupResult = undefined;
    var lookup_queue: std.Io.Queue(LookupResult) = .init(&lookup_buf);
    // FIXME: can lookup block when more than 16 results are returned?
    // https://sheran.io/blog/porting-dns-from-zig-0.15-to-0.16/
    // https://andrewkelley.me/post/zig-new-async-io-text-version.html
    try host.lookup(io, &lookup_queue, .{ .port = server_port });

    const IpAddress = std.Io.net.IpAddress;
    var connect_addr: ?IpAddress = null;
    while (lookup_queue.getOne(io)) |res| switch (res) {
        .canonical_name => |name| log.debug("resolved canonical name: {}", .{name}),
        .address => |addr| {
            log.debug("resolved address: {f}", .{addr});
            switch (addr) {
                .ip6 => {
                    connect_addr = addr;
                    break;
                },
                .ip4 => {
                    if (connect_addr == null) connect_addr = addr;
                },
            }
        },
    } else |err| switch (err) {
        error.Canceled => |e| return e,
        // FIXME is the queue guaranteed to be drained at this point or do we
        // need to await completion?
        // In other words, does getOne block when there's pending work but the
        // queue is closed?
        error.Closed => {},
    }
    var stream = if (connect_addr) |addr| blk: {
        log.debug("selected address {f}", .{addr});
        break :blk try addr.connect(io, .{ .mode = .stream });
    } else return net.HostName.LookupError.UnknownHostName;
    defer stream.close(io);
    log.info("connected from {f}", .{stream.socket.address});

    // unbuffered writer
    var stream_writer = stream.writer(io, &.{});
    const io_writer = &stream_writer.interface;

    try writer.msg_size(io_writer, 0);
    const req_header = proto.RequestHeader{
        .api_key = proto.ApiKey.produce,
        .api_version = 13, // latest produce
        .correlation_id = 42,
        .client_id = "test-client",
    };
    try writer.req_header(io_writer, req_header);

    const req = proto.ProduceRequest{
        .acks = -1, // ISR
        .timeout_ms = 1000,
        .topic_data_size = 2,
    };
    try writer.produce_req(io_writer, req);

    // Example data: topic one with a single partition, topic two with three
    // (the first with null records, so a non-null partition follows it across
    // the records boundary). The record bytes live outside the protocol
    // structs, so each partition is a header write followed by its records.
    const topic1_uuid = [_]u8{0x11} ** 16;
    const topic2_uuid = [_]u8{0x22} ** 16;

    try writer.topic_data(io_writer, .{ .topic_id = topic1_uuid, .partition_data_size = 1 });
    try write_partition(io_writer, 0, &[_]u8{ 0xca, 0xfe, 0xba, 0xbe });

    try writer.topic_data(io_writer, .{ .topic_id = topic2_uuid, .partition_data_size = 3 });
    try write_partition(io_writer, 0, null);
    try write_partition(io_writer, 1, &[_]u8{ 0xde, 0xad, 0xbe, 0xef });
    try write_partition(io_writer, 2, &[_]u8{ 0xfe, 0xed, 0xfa, 0xce });
    try io_writer.flush();

    // Read the response the server sends back. Unlike the request, the produce
    // response carries only small metadata, so it is read at once into a single
    // nested structure. An arena backs that structure and frees it all at once.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const gpa = arena.allocator();

    const reader = proto.reader;
    var rbuf: [1024]u8 = undefined;
    var stream_reader = stream.reader(io, &rbuf);
    const io_reader = &stream_reader.interface;

    const resp_size = try reader.msg_size(io_reader);
    log.info("response size: {d} bytes", .{resp_size});
    const resp_header = try reader.resp_header(io_reader);
    log.info("response correlation_id: {d}", .{resp_header.correlation_id});

    const resp = try reader.produce_resp(io_reader, gpa);
    log.info("produce response: {d} topic(s), throttle {d}ms", .{
        resp.responses.len,
        resp.throttle_time_ms,
    });
    for (resp.responses) |topic| {
        log.info("topic {x}: {d} partition response(s)", .{
            topic.topic_id,
            topic.partition_responses.len,
        });
        for (topic.partition_responses) |p| {
            log.info(
                "  partition {d}: error_code={d} base_offset={d} log_append_time_ms={d} log_start_offset={d} record_errors={d} error_message={?s}",
                .{ p.index, p.error_code, p.base_offset, p.log_append_time_ms, p.log_start_offset, p.record_errors.len, p.error_message },
            );
        }
    }
}

/// Writes a partition_data entry: the index and records length prefix (both
/// written by partition_data, the size taken from the slice), then the record
/// bytes. The bytes live outside the struct, so the size and the bytes are
/// written from the same slice and always agree.
fn write_partition(w: *std.Io.Writer, index: i32, records: ?[]const u8) !void {
    const records_size: ?u64 = if (records) |r| @intCast(r.len) else null;
    try proto.writer.partition_data(w, .{ .index = index, .records_size = records_size });
    if (records) |r| try w.writeAll(r);
}
