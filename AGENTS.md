# AGENTS.md

## Setup

Requires Zig **0.16.0** or newer. The 0.16 `std.Io` API is recent and
differs significantly from 0.15 — verify symbols against the installed
stdlib rather than older examples.

## Build & test

```sh
zig build              # build server (klog) and example client
zig build test         # run unit tests (currently: protocol.zig tests)
```

CI runs `zig build test --summary all` on Zig 0.16.0.

## Layout

Sources live at the repo root, not under `src/`:

- `klog.zig` — server entrypoint
- `client.zig` — example client entrypoint
- `protocol.zig` — Kafka wire protocol, exposed to the executables as the
  `protocol` module (see `build.zig`); import it as `@import("protocol")`,
  not by relative path.

## Conventions

- Unit tests live next to the code they cover, in the same file via Zig's
  `test` blocks. Only `protocol.zig` is wired into the `test` step today;
  add new test sources to `build.zig` when you create them.

## Keeping this file current

Update `AGENTS.md` in the same change that alters anything it describes:
build or test commands, the file layout, module names exposed via
`build.zig`, or the conventions section. If a change makes a statement
here wrong, the change is incomplete until this file matches.

<!--
Suggested additions once the project grows past proof-of-concept (useful
for agents as well as humans, so worth promoting into this file):

- Architecture overview: request lifecycle from accept → header parse →
  API dispatch, and where state will live once persistence exists.
- Supported Kafka APIs and versions, with a pointer to the spec sections
  each one implements.
- Error-handling and allocator conventions (who owns buffers, when to
  use fixed-size stack buffers vs. an allocator).
- Logging conventions (levels, what belongs at `debug` vs `info`).
-->
