const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const protocol = b.createModule(.{
        .root_source_file = b.path("protocol.zig"),
        .target = target,
        .optimize = optimize,
    });

    const klog = b.addExecutable(.{
        .name = "klog",
        .root_module = b.createModule(.{
            .root_source_file = b.path("klog.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "protocol", .module = protocol },
            },
        }),
    });
    b.installArtifact(klog);

    const client = b.addExecutable(.{
        .name = "client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "protocol", .module = protocol },
            },
        }),
    });
    b.installArtifact(client);

    const protocol_tests = b.addTest(.{ .root_module = protocol });
    const run_protocol_tests = b.addRunArtifact(protocol_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_protocol_tests.step);
}
