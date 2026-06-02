const std = @import("std");
const log = std.log;
const net = std.Io.net;

const proto = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const writer = proto.writer;

    const io = init.io;
    // Resolve the host name and connect, letting the standard library race
    // IPv6 and IPv4 candidates (Happy Eyeballs). On dual-stack hosts IPv6 is
    // attempted first; IPv4-only hosts fall back transparently.
    const host = try net.HostName.init("localhost");
    var stream = try host.connect(io, 8080, .{ .mode = .stream });
    defer stream.close(io);
    log.info("connected to {s}", .{host.bytes});

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
        .topic_data_size = 1,
    };
    try writer.produce_req(io_writer, req);
}
