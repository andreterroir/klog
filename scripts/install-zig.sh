#!/bin/sh
# install-zig.sh — install the Zig version required by build.zig.zon.
#
# Reads `minimum_zig_version` from build.zig.zon, detects the host OS/arch,
# downloads the matching Zig tarball from ziglang.org, and extracts it.
#
# Intended for build environments OTHER than GitHub Actions (local dev, other
# CI). On GitHub Actions, mlugg/setup-zig reads the same field automatically.
#
# Usage:
#   ./scripts/install-zig.sh
#
# Env vars:
#   ZIG_INSTALL_DIR   where to extract Zig (default: <repo>/.zig)
set -eu

# Resolve repo root relative to this script so it works from any cwd.
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
ZON="$REPO_ROOT/build.zig.zon"
INSTALL_DIR="${ZIG_INSTALL_DIR:-$REPO_ROOT/.zig}"

# Single source of truth: the version declared in build.zig.zon.
VERSION=$(sed -n 's/.*\.minimum_zig_version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$ZON")
if [ -z "$VERSION" ]; then
    echo "error: could not parse minimum_zig_version from $ZON" >&2
    exit 1
fi

# Detect OS and map to Zig's naming.
case "$(uname -s)" in
    Linux)  OS=linux ;;
    Darwin) OS=macos ;;
    *) echo "error: unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

# Detect arch and map to Zig's naming.
case "$(uname -m)" in
    x86_64|amd64)  ARCH=x86_64 ;;
    aarch64|arm64) ARCH=aarch64 ;;
    *) echo "error: unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

# Tarball is named zig-<arch>-<os>-<version> and unpacks to a dir of that name.
NAME="zig-${ARCH}-${OS}-${VERSION}"
URL="https://ziglang.org/download/${VERSION}/${NAME}.tar.xz"
DEST="$INSTALL_DIR/$NAME"

if [ -x "$DEST/zig" ]; then
    echo "Zig $VERSION already installed at $DEST"
else
    echo "Downloading $URL"
    mkdir -p "$INSTALL_DIR"
    TARBALL="$INSTALL_DIR/$NAME.tar.xz"
    if command -v curl >/dev/null 2>&1; then
        curl -fSL -o "$TARBALL" "$URL"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$TARBALL" "$URL"
    else
        echo "error: need curl or wget to download Zig" >&2
        exit 1
    fi
    tar -xJf "$TARBALL" -C "$INSTALL_DIR"
    rm -f "$TARBALL"
    echo "Installed Zig $VERSION to $DEST"
fi

echo
echo "Add Zig to your PATH for this shell:"
echo "  export PATH=\"$DEST:\$PATH\""
