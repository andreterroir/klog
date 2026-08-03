const std = @import("std");
const log = std.log;
const net = std.Io.net;
const Io = std.Io;
const Thread = std.Thread;

const proto = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const port = 8080;
    // Bind the IPv6 wildcard, which on dual-stack hosts also accepts IPv4
    // clients (IPv4-mapped addresses). Fall back to the IPv4 wildcard on hosts
    // without an IPv6 stack.
    var addr: net.IpAddress = .{ .ip6 = .unspecified(port) };
    var server = addr.listen(io, .{ .reuse_address = true }) catch |err| switch (err) {
        error.AddressFamilyUnsupported => blk: {
            addr = .{ .ip4 = .unspecified(port) };
            log.info("falling back to IPv4: {}", .{addr});
            break :blk try addr.listen(io, .{ .reuse_address = true });
        },
        else => return err,
    };
    defer server.deinit(io);
    log.info("started server at {f}", .{addr});

    while (true) {
        var stream = try server.accept(io);
        errdefer stream.close(io);
        const thread = try Thread.spawn(.{}, serve, .{ io, stream });
        thread.detach();
    }
}

fn serve(io: Io, stream: net.Stream) !void {
    log.debug("serving a new connection from {f} on thread {}", .{ stream.socket.address, Thread.getCurrentId() });
    var buf: [1024]u8 = undefined;
    var stream_reader = stream.reader(io, &buf);

    try respond(io, &stream_reader.interface, stream);
}

fn respond(io: Io, io_reader: *Io.Reader, stream: net.Stream) !void {
    const reader = proto.reader;

    log.debug("responding to {f}", .{stream.socket.address});

    const req_size = try reader.msg_size(io_reader);
    log.info("expecting a request of {d} bytes", .{req_size});

    const max_client_id = 1024;
    var buf: [max_client_id]u8 = undefined;
    const req_header = try reader.req_header(io_reader, &buf);
    log.debug("request header: {}", .{req_header});
    log.debug("request API key: {}", .{req_header.api_key});

    var wbuf: [1024]u8 = undefined;
    var stream_writer = stream.writer(io, &wbuf);
    const io_writer = &stream_writer.interface;

    try switch (req_header.api_key) {
        .produce => produce(io, io_reader, io_writer, req_header.correlation_id),
        else => std.log.err("unsupported API: {}", .{req_header.api_key}),
    };

    try io_writer.flush();
}

fn produce(io: Io, io_reader: *Io.Reader, io_writer: *Io.Writer, correlation_id: i32) !void {
    const reader = proto.reader;

    const max_transactional_id = 1024;
    var buf: [max_transactional_id]u8 = undefined;
    const req = try reader.produce_req(io_reader, &buf);
    log.debug("produce request: {}", .{req});

    // The response echoes the request's topic_ids and partition indices, so
    // collect that small metadata while streaming the request. Only the record
    // bytes are large; they are still drained a chunk at a time and never kept
    // in memory. An arena holds the response tree until it is written below.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const gpa = arena.allocator();

    // Read the topic_data array one entry at a time, using the counts and
    // sizes that prefix each level rather than buffering the whole request.
    const responses = try gpa.alloc(proto.TopicResponse, @intCast(req.topic_data_size));
    for (responses) |*resp| {
        const topic = try reader.topic_data(io_reader);
        log.info("topic {x}: {d} partition(s)", .{ topic.topic_id, topic.partition_data_size });
        const partitions = try gpa.alloc(proto.PartitionResponse, @intCast(topic.partition_data_size));
        for (partitions) |*p| {
            const partition = try reader.partition_data(io_reader);
            try log_records(io, io_reader, topic.topic_id, partition);
            // index is taken from the request; the rest are example results a
            // real broker would fill in after persisting the records.
            p.* = .{
                .index = partition.index,
                .error_code = 0,
                .base_offset = 0,
                .log_append_time_ms = -1,
                .log_start_offset = 0,
                .record_errors = &.{},
                .error_message = null,
            };
        }
        resp.* = .{ .topic_id = topic.topic_id, .partition_responses = partitions };
    }

    // TODO fsync before responding to the client

    const writer = proto.writer;

    // Reply with a success response. topic_ids and partition indices mirror the
    // request; the per-partition results are examples. Unlike the request, the
    // response carries only small metadata, so it is written at once. msg_size
    // is a placeholder 0, matching how the request prefixes its size.
    try writer.msg_size(io_writer, 0);
    try writer.resp_header(io_writer, .{ .correlation_id = correlation_id });
    try writer.produce_resp(io_writer, .{
        .responses = responses,
        .throttle_time_ms = 0,
    });
}

/// Logs a partition's records, consuming them from the stream in
/// fixed-size chunks so an arbitrarily large records blob never has to be
/// held in memory at once. Logs the byte count and a short hex preview of
/// the first chunk.
fn log_records(io: Io, io_reader: *Io.Reader, topic_id: [16]u8, partition: proto.PartitionData) !void {
    const size = partition.records_size orelse {
        log.info("  partition {d}: empty batch", .{partition.index});
        return;
    };

    var buf: [16]u8 = undefined;
    const b = buf[0..@min(buf.len, size)];
    const read = try io_reader.readSliceShort(b);
    log.info("  partition {d}: {d} record byte(s), first {d}: {x}", .{ partition.index, size, read, b });

    // create data directory, if does not exist
    const data_path = "/tmp/klog/data/";
    Io.Dir.cwd().createDirPath(io, data_path) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };
    const data_dir = try Io.Dir.openDirAbsolute(io, data_path, .{});
    defer data_dir.close(io);
    std.debug.assert(partition.index < 999_999);
    // ffffffffffffffffffffffffffffffff-999999
    var path_buf: [39]u8 = undefined;
    // TODO is there a way to format an i32 without a sign using specifiers?
    const file_path = try std.fmt.bufPrint(&path_buf, "{x}-{d:06}", .{ topic_id, @as(u64, @intCast(partition.index)) });
    const data_file = try data_dir.createFile(io, file_path, .{ .truncate = false });
    defer data_file.close(io);

    var file_buf: [1024]u8 = undefined;
    var file_writer = data_file.writer(io, &file_buf);
    var file_io_writer = &file_writer.interface;
    // write preview bytes first
    const length = try data_file.length(io);
    try data_file.writePositionalAll(io, b, length);

    var to_read = size - @min(buf.len, size);
    var read_buf: [1024]u8 = undefined;
    while (to_read > 0) {
        const read_size = @min(read_buf.len, to_read);
        try io_reader.readSliceAll(read_buf[0..read_size]);
        try file_io_writer.writeAll(read_buf[0..read_size]);
        to_read -= read_size;
    }
    try file_writer.flush();
}
