# klog

A Kafka-protocol compatible log server and a Zig client.

> [!NOTE]
> Status: proof of concept

## Requirements

[Zig](https://ziglang.org/download/) — see `minimum_zig_version` in
[`build.zig.zon`](build.zig.zon) for the required version.

To install the required version automatically (local dev or non-GitHub CI):

```sh
./scripts/install-zig.sh   # downloads the version from build.zig.zon
```

Then add the printed path to your `PATH`. On GitHub Actions,
`mlugg/setup-zig` reads the same version from `build.zig.zon`.

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
