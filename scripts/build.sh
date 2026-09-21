#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CMAKE=${CMAKE:-cmake}
NINJA=${NINJA:-ninja}
NINJA_BIN="$(command -v "$NINJA")"

cd "$ROOT_DIR"

mkdir -p ./cmake-build-release
"$CMAKE" -DCMAKE_BUILD_TYPE=Release -DCMAKE_MAKE_PROGRAM="$NINJA_BIN" -G Ninja -S . -B ./cmake-build-release

"$CMAKE" --build ./cmake-build-release --target clean -j 4
"$CMAKE" --build ./cmake-build-release --target all -j 4

mkdir -p ./cmake-build-debug
"$CMAKE" -DCMAKE_BUILD_TYPE=Debug -DCMAKE_MAKE_PROGRAM="$NINJA_BIN" -G Ninja -S . -B ./cmake-build-debug

"$CMAKE" --build ./cmake-build-debug --target clean -j 4
"$CMAKE" --build ./cmake-build-debug --target all -j 4
