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

    // Example data: topic one with a single partition, topic two with two.
    const topic1_uuid = [_]u8{0x11} ** 16;
    const topic2_uuid = [_]u8{0x22} ** 16;
    const topics = [_]proto.TopicData{
        .{
            .topic_id = topic1_uuid,
            .partition_data = &.{
                .{ .index = 0, .records = &[_]u8{ 0xca, 0xfe, 0xba, 0xbe } },
            },
        },
        .{
            .topic_id = topic2_uuid,
            .partition_data = &.{
                .{ .index = 0, .records = &[_]u8{ 0xde, 0xad, 0xbe, 0xef } },
                .{ .index = 1, .records = &[_]u8{ 0x01, 0x02, 0x03 } },
            },
        },
    };

    const req = proto.ProduceRequest{
        .acks = -1, // ISR
        .timeout_ms = 1000,
        .topic_data_size = topics.len,
    };
    try writer.produce_req(io_writer, req);

    // Write each topic header followed by its partitions, mirroring how the
    // server streams them back out.
    for (topics) |topic| {
        try writer.topic_data(io_writer, .{
            .topic_id = topic.topic_id,
            .partition_count = topic.partition_data.len,
        });
        for (topic.partition_data) |partition| {
            try writer.partition_data(io_writer, partition);
        }
    }
}
