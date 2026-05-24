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
        const thread = try std.Thread.spawn(.{}, serve, .{ stream, io });
        thread.detach();
    }
}

fn serve(stream: net.Stream, io: std.Io) !void {
    var buf: [1024]u8 = undefined;
    var stream_reader = stream.reader(io, &buf);

    try respond(stream, &stream_reader.interface, io);
}

fn respond(stream: net.Stream, io_reader: *Io.Reader, io: std.Io) !void {
    const reader = proto.reader;

    const req_size = try reader.msg_size(io_reader);
    log.info("expecting a request of {d} bytes", .{req_size});

    const max_client_id = 1024;
    var buf: [max_client_id]u8 = undefined;
    const req_header = try reader.req_header(io_reader, &buf);
    log.debug("request header: {}", .{req_header});
    log.debug("request API key: {}", .{req_header.request_api_key});

    const stream_writer = stream.writer(io, &.{});
    _ = stream_writer;

    try switch (req_header.request_api_key) {
        .produce => produce(stream, io_reader, io),
        else => std.log.err("unsupported API: {}", .{req_header.request_api_key}),
    };
}

fn produce(stream: std.Io.net.Stream, io_reader: *Io.Reader, io: std.Io) !void {
    _ = stream;
    _ = io_reader;
    _ = io;
}
