const std = @import("std");
const log = std.log;
const net = std.Io.net;
const Io = std.Io;

const proto = @import("protocol.zig");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const addr = try net.IpAddress.parse("::1", 8080);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    log.info("started server at {f}", .{addr});

    while (true) {
        var stream = try server.accept(io);
        errdefer stream.close(io);
        const thread = try std.Thread.spawn(.{}, serve, .{ stream, io });
        thread.detach();
    }
}

fn serve(stream: net.Stream, io: std.Io) !void {
    var buf: [1024]u8 = undefined;
    var reader = stream.reader(io, &buf);

    // parse message
    try respond(stream, &reader.interface, io);
}

fn respond(stream: net.Stream, reader: *Io.Reader, io: std.Io) !void {
    const req_size = try proto.reader.msg_size(reader);
    log.info("expecting a request of {d} bytes", .{req_size});

    const writer = stream.writer(io, &.{});
    _ = writer;

    const max_client_id = 1024;
    var buf: [max_client_id]u8 = undefined;
    const req_header = try proto.reader.req_header(reader, &buf);
    log.debug("request header: {}", .{req_header});

    // produce()
    // fetch()
}

fn produce(stream: std.Io.net.Stream, reader: std.Io.net.Stream.Reader, io: std.Io) !void {
    _ = stream;
    _ = reader;
    _ = io;
}
