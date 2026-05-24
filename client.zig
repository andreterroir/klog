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
        .request_api_key = proto.ApiKey.produce,
        .request_api_version = 13, // latest produce
        .correlation_id = 42,
        .client_id = "test-client",
    };
    try writer.req_header(io_writer, req_header);
}
