#!/bin/bash
# Update the vendored libvroom subtree and its third-party dependencies.
#
# Usage:
#   ./scripts/update-libvroom.sh [REPO] [BRANCH]   # pull + trim + vendor deps
#   ./scripts/update-libvroom.sh --trim-only        # trim without pulling
#   ./scripts/update-libvroom.sh --deps-only        # re-vendor deps without pulling
#
# REPO defaults to the GitHub URL; BRANCH defaults to main.
# Requires: cmake (for fetching third-party deps via FetchContent)

set -euo pipefail

LIBVROOM_DIR="src/libvroom"
DEFAULT_REPO="https://github.com/jimhester/libvroom.git"
DEFAULT_BRANCH="main"

trim_only=false
deps_only=false
repo="$DEFAULT_REPO"
branch="$DEFAULT_BRANCH"

for arg in "$@"; do
  case "$arg" in
    --trim-only) trim_only=true ;;
    --deps-only) deps_only=true ;;
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

# Step 1: Pull latest changes (unless --trim-only or --deps-only)
if [ "$trim_only" = false ] && [ "$deps_only" = false ]; then
  echo "Pulling libvroom from $repo ($branch)..."
  git subtree pull --prefix="$LIBVROOM_DIR" "$repo" "$branch" --squash -m \
    "chore: update vendored libvroom from $branch"
fi

# Step 2: Trim files not needed for the R package build.
if [ "$deps_only" = false ]; then
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

  # Source files not compiled by Makevars (CLI-only, uses std::cerr)
  remove src/cli.cpp
  remove src/convert.cpp
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
  remove compile_commands.json
  remove CLAUDE.md
  remove README.md
  remove PRODUCTION_READINESS_PLAN.md
  remove baseline_benchmarks.txt

  if [ "$removed" -eq 0 ]; then
    echo "Nothing to trim."
  else
    echo "Removed $removed items."
  fi
fi

# Step 3: Vendor third-party dependencies using cmake FetchContent.
# libvroom uses cmake FetchContent to download these at build time, but
# R packages can't fetch from the network during install. We run cmake
# configure in a temp directory to download the deps, then copy the
# needed files into src/libvroom/third_party/.
echo "Vendoring third-party dependencies..."

# Find the upstream CMakeLists.txt (we need it temporarily for FetchContent)
# Use the libvroom repo directly if available, otherwise reconstruct
LIBVROOM_REPO=""
if [ -d "$repo/.git" ] 2>/dev/null; then
  LIBVROOM_REPO="$repo"
elif git remote get-url libvroom-upstream &>/dev/null; then
  LIBVROOM_REPO=$(git remote get-url libvroom-upstream)
fi

# Use cmake to fetch dependencies. If a local repo is provided and has an
# existing build dir with _deps, reuse it. Otherwise, run cmake configure
# in a temp directory to trigger FetchContent downloads.
BUILD_DIR=""
CLEANUP_BUILD=false

if [ -n "$LIBVROOM_REPO" ] && [ -d "$LIBVROOM_REPO" ]; then
  # Look for existing build dir with fetched deps
  for candidate in "$LIBVROOM_REPO/build-release" "$LIBVROOM_REPO/build"; do
    if [ -d "$candidate/_deps" ]; then
      BUILD_DIR="$candidate"
      echo "  Using existing build at $BUILD_DIR"
      break
    fi
  done

  if [ -z "$BUILD_DIR" ]; then
    BUILD_DIR=$(mktemp -d)
    CLEANUP_BUILD=true
    echo "  Running cmake configure to fetch deps..."
    cmake -S "$LIBVROOM_REPO" -B "$BUILD_DIR" \
      -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF \
      -DFETCHCONTENT_QUIET=ON 2>&1 | grep -E "^(--|Fetching|Populating)" || true
  fi
else
  BUILD_DIR=$(mktemp -d)
  CLEANUP_BUILD=true
  echo "  Cloning libvroom for dep fetching..."
  git clone --depth=1 -b "$branch" "$repo" "$BUILD_DIR/libvroom-src" 2>&1 | tail -1
  cmake -S "$BUILD_DIR/libvroom-src" -B "$BUILD_DIR/build" \
    -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF \
    -DFETCHCONTENT_QUIET=ON 2>&1 | grep -E "^(--|Fetching|Populating)" || true
  BUILD_DIR="$BUILD_DIR/build"
fi

if [ "$CLEANUP_BUILD" = true ]; then
  trap "rm -rf $BUILD_DIR" EXIT
fi

THIRD_PARTY="$LIBVROOM_DIR/third_party"

# Highway (SIMD abstraction) — headers + 3 compiled .cc files
echo "  Vendoring Highway..."
HWY_SRC="$BUILD_DIR/_deps/highway-src/hwy"
rm -rf "$THIRD_PARTY/hwy"
mkdir -p "$THIRD_PARTY/hwy/ops" "$THIRD_PARTY/hwy/contrib/algo" \
  "$THIRD_PARTY/hwy/contrib/bit_pack" "$THIRD_PARTY/hwy/contrib/dot" \
  "$THIRD_PARTY/hwy/contrib/math" "$THIRD_PARTY/hwy/contrib/matvec" \
  "$THIRD_PARTY/hwy/contrib/random" "$THIRD_PARTY/hwy/contrib/thread_pool"

# Headers
cp "$HWY_SRC"/*.h "$THIRD_PARTY/hwy/"
cp "$HWY_SRC"/ops/*.h "$THIRD_PARTY/hwy/ops/"
cp "$HWY_SRC"/contrib/algo/*.h "$THIRD_PARTY/hwy/contrib/algo/"
cp "$HWY_SRC"/contrib/bit_pack/*.h "$THIRD_PARTY/hwy/contrib/bit_pack/"
cp "$HWY_SRC"/contrib/dot/*.h "$THIRD_PARTY/hwy/contrib/dot/"
cp "$HWY_SRC"/contrib/math/*.h "$THIRD_PARTY/hwy/contrib/math/"
cp "$HWY_SRC"/contrib/matvec/*.h "$THIRD_PARTY/hwy/contrib/matvec/"
cp "$HWY_SRC"/contrib/random/*.h "$THIRD_PARTY/hwy/contrib/random/"
cp "$HWY_SRC"/contrib/thread_pool/*.h "$THIRD_PARTY/hwy/contrib/thread_pool/"
# Compiled sources (only the 3 we need in Makevars)
cp "$HWY_SRC"/aligned_allocator.cc "$THIRD_PARTY/hwy/"
cp "$HWY_SRC"/per_target.cc "$THIRD_PARTY/hwy/"
cp "$HWY_SRC"/targets.cc "$THIRD_PARTY/hwy/"
# topology.cc needed by thread_pool
if [ -f "$HWY_SRC/contrib/thread_pool/topology.cc" ]; then
  cp "$HWY_SRC"/contrib/thread_pool/topology.cc "$THIRD_PARTY/hwy/contrib/thread_pool/"
fi

# fast_float (header-only)
echo "  Vendoring fast_float..."
FAST_FLOAT_SRC="$BUILD_DIR/_deps/fast_float-src/include/fast_float"
rm -rf "$THIRD_PARTY/fast_float"
mkdir -p "$THIRD_PARTY/fast_float"
cp "$FAST_FLOAT_SRC"/*.h "$THIRD_PARTY/fast_float/"

# BS::thread_pool (single header)
echo "  Vendoring BS_thread_pool..."
THREAD_POOL_SRC="$BUILD_DIR/_deps/thread_pool-src/include"
cp "$THREAD_POOL_SRC"/BS_thread_pool.hpp "$THIRD_PARTY/"

# simdutf (single-header amalgamation)
echo "  Vendoring simdutf..."
SIMDUTF_SRC="$BUILD_DIR/_deps/simdutf-src/singleheader"
rm -rf "$THIRD_PARTY/simdutf"
mkdir -p "$THIRD_PARTY/simdutf"
# If amalgamation doesn't exist yet, generate it
if [ ! -f "$SIMDUTF_SRC/simdutf.h" ]; then
  echo "    Generating amalgamation..."
  (cd "$BUILD_DIR/_deps/simdutf-src/singleheader" && python3 amalgamate.py) 2>&1 | tail -3
fi
cp "$SIMDUTF_SRC"/simdutf.h "$THIRD_PARTY/simdutf/"
cp "$SIMDUTF_SRC"/simdutf.cpp "$THIRD_PARTY/simdutf/"

echo "Done. Third-party deps vendored to $THIRD_PARTY/"
echo ""
echo "Remember to:"
echo "  1. Check if Makevars OBJECTS list needs updating"
echo "  2. git add -A && git commit"
