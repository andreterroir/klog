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

    try proto.write_msg_size(&writer.interface, 0);
}
