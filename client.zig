const std = @import("std");
const log = std.log;
const net = std.Io.net;

const proto = @import("protocol.zig");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const addr = try net.IpAddress.parse("::1", 8080);
    var stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);
    log.info("connected to {f}", .{addr});

    // unbuffered writer
    var writer = stream.writer(io, &.{});

    try proto.writer.msg_size(&writer.interface, 0);
    const req_header = proto.RequestHeader{
        .request_api_key = 0, // produce
        .request_api_version = 13, // latest produce
        .correlation_id = 42,
        .client_id = "test-client",
    };
    try proto.writer.req_header(&writer.interface, req_header);
}
