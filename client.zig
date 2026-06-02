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

    // Topic one: a single partition.
    const topic_one = [_]u8{0x11} ** 16;
    try writer.topic_data(io_writer, topic_one, 1);
    try writer.partition_data(io_writer, .{ .index = 0, .records = &[_]u8{ 0xca, 0xfe, 0xba, 0xbe } });

    // Topic two: two partitions.
    const topic_two = [_]u8{0x22} ** 16;
    try writer.topic_data(io_writer, topic_two, 2);
    try writer.partition_data(io_writer, .{ .index = 0, .records = &[_]u8{ 0xde, 0xad, 0xbe, 0xef } });
    try writer.partition_data(io_writer, .{ .index = 1, .records = &[_]u8{ 0x01, 0x02, 0x03 } });
}
