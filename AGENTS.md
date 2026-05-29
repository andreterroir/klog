# AGENTS.md

## Setup

If `zig` is not on `PATH`, run `./scripts/install-zig.sh` before building.
The script reads the required version from `build.zig.zon` (the single
source of truth) and prints a `PATH` line to source. The `std.Io` API is
still churning across Zig releases — verify symbols against the stdlib
shipped with the installed version rather than older examples.

## Build & test

```sh
zig build              # build server (klog) and example client
zig build test         # run unit tests (currently: protocol.zig tests)
```

CI runs `zig build test --summary all`.

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

## Pull requests

If later commits invalidate the PR's title or description, update them to
match the final diff before asking for review.
