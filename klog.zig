const std = @import("std");
const log = std.log;
const net = std.Io.net;
const Io = std.Io;

const proto = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const addr = try net.IpAddress.parse("::1", 8080);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    log.info("started server at {f}", .{addr});

    while (true) {
        var stream = try server.accept(io);
        errdefer stream.close(io);
        const thread = try std.Thread.spawn(.{}, serve, .{ io, stream });
        thread.detach();
    }
}

fn serve(io: Io, stream: net.Stream) !void {
    var buf: [1024]u8 = undefined;
    var stream_reader = stream.reader(io, &buf);

    try respond(io, &stream_reader.interface, stream);
}

fn respond(io: Io, io_reader: *Io.Reader, stream: net.Stream) !void {
    const reader = proto.reader;

    const req_size = try reader.msg_size(io_reader);
    log.info("expecting a request of {d} bytes", .{req_size});

    const max_client_id = 1024;
    var buf: [max_client_id]u8 = undefined;
    const req_header = try reader.req_header(io_reader, &buf);
    log.debug("request header: {}", .{req_header});
    log.debug("request API key: {}", .{req_header.api_key});

    const stream_writer = stream.writer(io, &.{});
    _ = stream_writer;

    try switch (req_header.api_key) {
        .produce => produce(io, io_reader, stream),
        else => std.log.err("unsupported API: {}", .{req_header.api_key}),
    };
}

fn produce(io: Io, io_reader: *Io.Reader, stream: Io.net.Stream) !void {
    _ = stream;
    _ = io;

    const reader = proto.reader;

    const max_transactional_id = 1024;
    var buf: [max_transactional_id]u8 = undefined;
    const req = try reader.produce_req(io_reader, &buf);
    log.debug("produce request: {}", .{req});

    // Read the topic_data array one entry at a time, using the counts and
    // sizes that prefix each level rather than buffering the whole request.
    for (0..req.topic_data_size) |_| {
        const topic = try reader.topic_data(io_reader);
        log.info("topic {x}: {d} partition(s)", .{ topic.topic_id, topic.partition_count });
        for (0..topic.partition_count) |_| {
            const partition = try reader.partition_data(io_reader);
            try log_records(io_reader, partition);
        }
    }
}

/// Logs a partition's records, consuming them from the stream in
/// fixed-size chunks so an arbitrarily large records blob never has to be
/// held in memory at once. Logs the byte count and a short hex preview of
/// the first chunk.
fn log_records(io_reader: *Io.Reader, partition: proto.PartitionData) !void {
    const size = partition.records_size orelse {
        log.info("  partition {d}: null records", .{partition.index});
        return;
    };

    var chunk: [4096]u8 = undefined;
    const first_len: usize = @intCast(@min(size, chunk.len));
    try io_reader.readSliceAll(chunk[0..first_len]);
    const preview = chunk[0..@min(first_len, 16)];
    log.info("  partition {d}: {d} record byte(s), first {d}: {x}", .{
        partition.index,
        size,
        preview.len,
        preview,
    });

    // Drain the remaining bytes, again one chunk at a time.
    var remaining = size - first_len;
    while (remaining > 0) {
        const take: usize = @intCast(@min(remaining, chunk.len));
        try io_reader.readSliceAll(chunk[0..take]);
        remaining -= take;
    }
}
