const std = @import("std");
const net = std.Io.net;
const log = std.log;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const addr = try net.IpAddress.parse("::1", 8080);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    std.log.info("started server at {f}", .{addr});

    var stream = try server.accept(io);
    defer stream.close(io);

    var buf: [1024]u8 = undefined;
    var reader = stream.reader(io, &buf);
    // unbuffered writer
    var writer = stream.writer(io, &.{});

    while (true) {
        const msg = reader.interface.takeDelimiter('\n') catch |err| {
            if (err == error.EndOfStream) {
                log.info("client closed connection\n", .{});
                return;
            } else {
                return err;
            }
        };
        if (msg) |m| {
            log.info("data received ({d} bytes): '{s}'", .{ m.len, m });
            try writer.interface.writeAll(m);
        } else {
            return;
        }
    }
}
