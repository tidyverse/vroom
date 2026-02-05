#!/bin/bash
# Update the vendored libvroom subtree and trim files not needed for the R package.
#
# Usage:
#   ./scripts/update-libvroom.sh [REPO] [BRANCH]   # pull + trim
#   ./scripts/update-libvroom.sh --trim-only        # trim without pulling
#
# REPO defaults to the GitHub URL; BRANCH defaults to main.

set -euo pipefail

LIBVROOM_DIR="src/libvroom"
DEFAULT_REPO="https://github.com/jimhester/libvroom.git"
DEFAULT_BRANCH="main"

trim_only=false
repo="$DEFAULT_REPO"
branch="$DEFAULT_BRANCH"

for arg in "$@"; do
  case "$arg" in
    --trim-only) trim_only=true ;;
    -*) echo "Unknown option: $arg" >&2; exit 1 ;;
    *)
      if [ "$repo" = "$DEFAULT_REPO" ] && [ "$arg" != "$DEFAULT_BRANCH" ]; then
        repo="$arg"
      else
        branch="$arg"
      fi
      ;;
  esac
done

# Ensure we're at the repo root
if [ ! -d "$LIBVROOM_DIR" ]; then
  echo "Error: $LIBVROOM_DIR not found. Run from the vroom repo root." >&2
  exit 1
fi

# Step 1: Pull latest changes (unless --trim-only)
if [ "$trim_only" = false ]; then
  # Preserve vendored simdutf (not in upstream libvroom, added for R package)
  simdutf_backup=""
  if [ -d "$LIBVROOM_DIR/third_party/simdutf" ]; then
    simdutf_backup=$(mktemp -d)
    cp -a "$LIBVROOM_DIR/third_party/simdutf" "$simdutf_backup/"
    echo "Backed up vendored simdutf."
  fi

  echo "Pulling libvroom from $repo ($branch)..."
  git subtree pull --prefix="$LIBVROOM_DIR" "$repo" "$branch" --squash -m \
    "chore: update vendored libvroom from $branch"

  # Restore vendored simdutf
  if [ -n "$simdutf_backup" ]; then
    mkdir -p "$LIBVROOM_DIR/third_party/simdutf"
    cp -a "$simdutf_backup/simdutf/"* "$LIBVROOM_DIR/third_party/simdutf/"
    rm -rf "$simdutf_backup"
    echo "Restored vendored simdutf."
  fi
fi

# Step 2: Trim files not needed for the R package build.
# Uses a denylist approach: remove known-unneeded directories and files.
echo "Trimming unneeded files from $LIBVROOM_DIR..."

removed=0

remove() {
  local path="$LIBVROOM_DIR/$1"
  if [ -e "$path" ]; then
    rm -rf "$path"
    echo "  removed $1"
    removed=$((removed + 1))
  fi
}

# Directories not needed for R package
remove test
remove benchmark
remove docs
remove python
remove scripts
remove bench
remove fuzz
remove oss-fuzz
remove cmake
remove .github

# Source files not compiled by Makevars
remove src/cli.cpp
remove src/reader/streaming_parser.cpp

# Root-level config and documentation
remove CMakeLists.txt
remove Dockerfile
remove codecov.yml
remove .clang-format
remove .gitignore
remove .lcovrc
remove .work.toml
remove tsan_suppressions.txt
remove CLAUDE.md
remove README.md
remove PRODUCTION_READINESS_PLAN.md
remove baseline_benchmarks.txt

if [ "$removed" -eq 0 ]; then
  echo "Nothing to trim."
else
  echo "Removed $removed items."
fi
