# GitHub Actions CI

This directory contains GitHub Actions workflows for libvroom continuous integration.

## Workflows

### python.yml - Python Bindings Tests

Tests Python bindings for vroom-csv across multiple Python versions and platforms.

**Triggers:**
- Push to main/master branches
- Pull requests to main/master branches

**Test Matrix:**
- **Platforms**: Ubuntu (latest), macOS (latest)
- **Python Versions**: 3.9, 3.10, 3.11, 3.12
- **Total Jobs**: 8 combinations

**Build Steps:**
1. Checkout code
2. Set up Python with specified version
3. Install build dependencies (CMake, build tools)
4. Install Python package with test dependencies via `pip install .[test]`
5. Run pytest on Python tests

**Tests Covered:**
- `read_csv()` function (basic parsing, delimiters, headers)
- `Table` class functionality (column/row access, properties)
- Arrow PyCapsule interface (`__arrow_c_schema__`, `__arrow_c_stream__`)
- PyArrow interoperability (conversion to PyArrow Table)
- Polars interoperability (conversion to Polars DataFrame)

### python-wheels.yml - Python Wheel Distribution

Builds Python wheels for distribution on PyPI.

**Triggers:**
- Push to main branch (fast builds for TestPyPI)
- Push of release tags (`v*`) (full builds for PyPI)
- Manual dispatch via workflow_dispatch

**Note:** PRs do NOT trigger wheel builds to save CI time. Regular Python testing is handled by `python.yml`.

**Build Strategy:**
- **Main branch**: Fast builds only (x86_64 Linux, both macOS archs)
- **Release tags**: Full builds including slow Linux ARM64 via QEMU (~20+ min)

**Build Matrix:**
- **Platforms**: Ubuntu (latest), macOS 14 (ARM)
- **Python Versions**: 3.9, 3.10, 3.11, 3.12
- **Architectures**: x86_64 always, ARM64 on macOS (native), ARM64 on Linux (releases only)

**Jobs:**
1. `build_wheels` - Build wheels using cibuildwheel
2. `build_sdist` - Build source distribution
3. `publish` - Publish to PyPI (only on release tags)
4. `publish-testpypi` - Publish to TestPyPI (only on main branch)

### ci.yml - Main CI Pipeline

Runs on every push and pull request to main/master branches.

**Build Matrix:**
- **Platforms**: Ubuntu (latest), macOS (latest)
- **Build Types**: Release, Debug (Debug skipped on macOS)
- **Total Jobs**: 3 build combinations

**Build Steps:**
1. Checkout code
2. Install dependencies (CMake, build tools)
3. Configure CMake with specified build type
4. Build all targets (libvroom, libvroom_test, error_handling_test)
5. Run well-formed CSV tests (42 tests)
6. Run error handling tests (37 tests)
7. Run full CTest suite (79 tests)

### Additional CI Jobs

**Code Coverage** (runs on every push/PR):
- Builds with `-DENABLE_COVERAGE=ON`
- Generates coverage with lcov
- Uploads to Codecov
- **Note**: Header file coverage may appear artificially low due to gcov limitations. See [docs/coverage.md](../../docs/coverage.md) for details.

**Minimal Release Build** (main branch only):
- Builds with `-DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF`
- Verifies library and CLI binary are built
- Confirms test/benchmark executables are NOT built

**Shared Library Build** (runs on every push/PR):
- Builds with `-DBUILD_SHARED_LIBS=ON`
- Verifies shared library (.so) is created
- Tests vroom binary links correctly

## CI Badge

Add to README.md:
```markdown
![CI](https://github.com/jimhester/libvroom/workflows/CI/badge.svg)
```

## Local Testing

To test locally before pushing:

```bash
# Run the same commands as CI
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
./build/libvroom_test
./build/error_handling_test
cd build && ctest --output-on-failure
```

## Adding New Tests

When adding new test files:

1. Add test file to appropriate `test/data/` subdirectory
2. Update test harness (`csv_parser_test.cpp` or `error_handling_test.cpp`)
3. CI will automatically run new tests on next push

## Platform-Specific Notes

### Linux (Ubuntu)
- Uses GCC 13.3.0
- Installs: cmake, build-essential
- AVX2 support via `-march=native`

### macOS
- Uses Apple Clang
- Installs: cmake (via Homebrew)
- ARM64 (M1/M2) and x86_64 support

### Future Platforms
- **Windows**: MSVC support planned (requires adjustments for AVX2 intrinsics)
- **ARM Linux**: For testing ARM NEON/SVE implementations

## Debugging CI Failures

### Build Failures
1. Check CMake configuration output
2. Verify all source files compile
3. Check for missing dependencies

### Test Failures
1. Review test output (shown via `--output-on-failure`)
2. Check if test data files are present
3. Verify file permissions and line endings

## Performance Considerations

- **Caching**: FetchContent dependencies are cached to speed up rebuilds
- **Parallel builds**: CMake uses multiple cores by default
- **Test parallelization**: CTest can run tests in parallel

### fuzz.yml - Fuzz Testing

Continuous fuzz testing using libFuzzer with sanitizers.

**Triggers:**
- **Scheduled**: Weekly on Sundays at 2:00 UTC
- **Manual**: On-demand via workflow_dispatch

**Fuzz Targets:**
- `fuzz_csv_parser` - Core two-pass CSV parser
- `fuzz_dialect_detection` - Dialect detection
- `fuzz_parse_auto` - Integrated parse_auto()

**Configuration:**
- Default duration: 10 minutes per target (scheduled runs)
- Manual runs: Configurable 1-30 minutes
- Sanitizers: AddressSanitizer, UndefinedBehaviorSanitizer

**On Crash Detection:**
1. Workflow fails with error message
2. Crash artifacts uploaded (30-day retention)
3. Details in workflow logs

See `fuzz/README.md` for local fuzzing instructions.

### benchmark.yml - Performance Regression Detection

Automatically detects performance regressions on every push and PR.

**Triggers:**
- Push to main/master branches
- Pull requests to main/master branches

**How it Works:**
1. Builds the benchmark executable in Release mode
2. Runs a subset of benchmarks from `parser_overhead_benchmarks.cpp`:
   - `BM_RawFirstPass` - Raw SIMD first pass scanning performance
   - `BM_RawTwoPassComplete` - Complete two-pass index building
   - `BM_ParserWithExplicitDialect` - Full parser overhead with known dialect
   - `BM_ParserBranchless` - Branchless algorithm variant
   - `BM_ParserSpeculative` - Speculative multi-threaded algorithm
   - `BM_ParserMultiThread/1` - Single-threaded parser
   - `BM_ParserMultiThread/4` - Multi-threaded parser (4 threads)
3. Compares results against a cached baseline (from main branch)
4. Fails if any benchmark regresses by more than 10%

**Baseline Management:**
- Baseline is cached per-OS with a version key (e.g., `benchmark-baseline-v3-Linux-main`)
- On main branch pushes, the baseline is updated with current results
- PRs compare against the cached main branch baseline
- If benchmark names change, increment the cache version in `benchmark.yml`

**Cache Key Versioning:**
If you rename benchmarks or change benchmark methodology, you need to reset the baseline:
1. Edit `.github/workflows/benchmark.yml`
2. Increment the version in the cache key (e.g., `v2` → `v3`)
3. The next main branch push will establish a new baseline

**Artifacts:**
- `benchmark-results` artifact contains JSON results (30-day retention)
- Both current results and baseline are uploaded for debugging

### benchmarks.yml - Full Benchmark Suite

Runs the complete benchmark suite for releases and manual comparison.

**Triggers:**
- Push of release tags (`v*`)
- Manual dispatch with optional comparison ref

**Features:**
- Multi-platform: Ubuntu and macOS
- Full benchmark suite (not just regression subset)
- Historical result storage for releases
- Optional comparison against any git ref

## Future Enhancements

Potential workflow additions:

1. **Static analysis**: clang-tidy, cppcheck
2. **Format checking**: clang-format validation
3. **Windows builds**: MSVC support
4. **ARM64 builds**: Native ARM testing
5. **Release automation**: Automatic tagging and releases
