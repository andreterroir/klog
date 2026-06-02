const std = @import("std");
const log = std.log;
const net = std.Io.net;
const Io = std.Io;

const proto = @import("protocol");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const port = 8080;
    // Bind the IPv6 wildcard, which on dual-stack hosts also accepts IPv4
    // clients (IPv4-mapped addresses). Fall back to the IPv4 wildcard on hosts
    // without an IPv6 stack.
    var addr: net.IpAddress = .{ .ip6 = .unspecified(port) };
    var server = addr.listen(io, .{ .reuse_address = true }) catch |err| switch (err) {
        error.AddressFamilyUnsupported => blk: {
            addr = .{ .ip4 = .unspecified(port) };
            break :blk try addr.listen(io, .{ .reuse_address = true });
        },
        else => return err,
    };
    defer server.deinit(io);
    log.info("started server at {f}", .{addr});

    while (true) {
        var stream = try server.accept(io);
        errdefer stream.close(io);
        const thread = try std.Thread.spawn(.{}, serve, .{ io, stream });
        thread.detach();
    }
}

fn serve(io: Io, stream: net.Stream) !void {
    var buf: [1024]u8 = undefined;
    var stream_reader = stream.reader(io, &buf);

    try respond(io, &stream_reader.interface, stream);
}

fn respond(io: Io, io_reader: *Io.Reader, stream: net.Stream) !void {
    const reader = proto.reader;

    const req_size = try reader.msg_size(io_reader);
    log.info("expecting a request of {d} bytes", .{req_size});

    const max_client_id = 1024;
    var buf: [max_client_id]u8 = undefined;
    const req_header = try reader.req_header(io_reader, &buf);
    log.debug("request header: {}", .{req_header});
    log.debug("request API key: {}", .{req_header.api_key});

    const stream_writer = stream.writer(io, &.{});
    _ = stream_writer;

    try switch (req_header.api_key) {
        .produce => produce(io, io_reader, stream),
        else => std.log.err("unsupported API: {}", .{req_header.api_key}),
    };
}

fn produce(io: Io, io_reader: *Io.Reader, stream: Io.net.Stream) !void {
    _ = stream;
    _ = io;

    const reader = proto.reader;

    const max_transactional_id = 1024;
    var buf: [max_transactional_id]u8 = undefined;
    const req = try reader.produce_req(io_reader, &buf);
    log.debug("produce request: {}", .{req});
}
