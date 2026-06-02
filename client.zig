const std = @import("std");
const log = std.log;
const net = std.Io.net;

const proto = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const writer = proto.writer;

    const io = init.io;
    const addr = try net.IpAddress.parse("::1", 8080);
    var stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    log.info("connected to {f}", .{addr});

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

    // Example data: topic one with a single partition, topic two with two.
    // The record bytes live outside the protocol structs, so each partition
    // is a header write followed by its records.
    const topic1_uuid = [_]u8{0x11} ** 16;
    const topic2_uuid = [_]u8{0x22} ** 16;

    try writer.topic_data(io_writer, .{ .topic_id = topic1_uuid, .partition_count = 1 });
    try write_partition(io_writer, 0, &[_]u8{ 0xca, 0xfe, 0xba, 0xbe });

    try writer.topic_data(io_writer, .{ .topic_id = topic2_uuid, .partition_count = 2 });
    try write_partition(io_writer, 0, &[_]u8{ 0xde, 0xad, 0xbe, 0xef });
    try write_partition(io_writer, 1, &[_]u8{ 0xfe, 0xed, 0xfa, 0xce });
}

/// Writes a partition_data header followed by its record bytes, deriving
/// the records size from the slice so the two always agree.
fn write_partition(w: *std.Io.Writer, index: i32, records: ?[]const u8) !void {
    try proto.writer.partition_data(w, .{
        .index = index,
        .records_size = if (records) |r| r.len else null,
    });
    if (records) |r| try w.writeAll(r);
}
