# klog

A Kafka-protocol compatible log server and a Zig client.

## Requirements

[Zig](https://ziglang.org/download/) **0.16.0** or newer.

## Build

```sh
zig build
```

Produces both binaries:

- `zig-out/bin/klog` — the server
- `zig-out/bin/client` — the example client

### Optimize options

Use `-Doptimize` to pick a release mode (default is `Debug`):

```sh
zig build                              # Debug
zig build -Doptimize=ReleaseSafe       # Optimized, runtime safety on
zig build -Doptimize=ReleaseFast       # Optimized, runtime safety off
zig build -Doptimize=ReleaseSmall      # Optimized for binary size
```

`-Dtarget=<triple>` cross-compiles, e.g. `-Dtarget=aarch64-linux`.

Run `zig build --help` to see every option.
