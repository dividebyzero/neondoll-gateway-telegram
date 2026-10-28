#!/bin/sh
set -eu

: "${CORE_REPO:?set CORE_REPO to a checkout of Neon-Dolls/neondoll main}"
TELEGRAM_REPO=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TELEGRAM_WASM=${TELEGRAM_WASM:-$TELEGRAM_REPO/wasm/build/gateway.wasm}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/telegram-corecompat.XXXXXX")
trap 'rm -rf "$WORK"' EXIT HUP INT TERM

cp "$(dirname -- "$0")/go.mod" "$(dirname -- "$0")/core_compat_test.go" "$WORK/"
cd "$WORK"
go mod edit -replace "github.com/Neon-Dolls/neondoll=$CORE_REPO"
go mod tidy
TELEGRAM_WASM="$TELEGRAM_WASM" go test -count=1 -v ./...
