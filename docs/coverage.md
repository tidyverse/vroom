# Code Coverage

This document explains libvroom's code coverage setup, known limitations, and how to interpret coverage reports.

## Overview

libvroom uses two complementary coverage instrumentation methods:

| Method | Compiler | Tool | Best For |
|--------|----------|------|----------|
| **gcov/lcov** | GCC | lcov | Traditional coverage, broad compatibility |
| **llvm-cov** | Clang | llvm-cov | Accurate header file attribution |

Both methods are run in CI and upload results to Codecov with separate flags (`gcov` and `llvm`).

## Header File Coverage Limitations

### The Problem

GCC's gcov-based coverage has a known limitation: **template and inline code defined in header files is often attributed to the `.cpp` files that include them**, rather than to the header files themselves.

This leads to artificially low coverage numbers for header-only components like:
- `two_pass.h` - Core two-pass parsing algorithm
- `simd_highway.h` - Portable SIMD operations
- `type_detector.h` - Type detection for columns
- `value_extraction.h` - Value extraction utilities
- `branchless_state_machine.h` - CSV state machine

For example, `two_pass.h` may show only 6% line coverage in gcov-based reports, while the actual coverage (as measured by llvm-cov) is 70-90%.

### Why This Happens

1. **Template Instantiation**: When a template function is instantiated in a `.cpp` file, gcov attributes the coverage to that `.cpp` file, not the header where the template is defined.

2. **Inline Functions**: Functions marked `inline` (explicitly or implicitly, like member functions defined in class bodies) may have their coverage attributed to the translation unit that includes them.

3. **Test File Filtering**: Test files are excluded from coverage reports. If a header is primarily included from test files, its coverage data may be filtered out.

### The Solution

We run both gcov-based and llvm-cov source-based coverage:

- **llvm-cov source-based coverage** operates on AST and preprocessor information directly, correctly attributing template and inline code to the header files where they are defined.

- Coverage results are uploaded to Codecov with separate flags, allowing you to compare both views.

## Running Coverage Locally

### GCC/lcov Coverage

```bash
# Configure with coverage
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON

# Build
cmake --build build -j

# Run tests
cd build && ctest --output-on-failure

# Generate coverage report
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '*/test/*' '*/benchmark/*' '*/build/_deps/*' '/usr/*' --output-file coverage.info

# View summary
lcov --summary coverage.info

# Generate HTML report (optional)
genhtml coverage.info --output-directory coverage_html
```

### Clang/llvm-cov Coverage

```bash
# Configure with LLVM coverage (requires Clang)
cmake -B build \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DENABLE_LLVM_COVERAGE=ON

# Build
cmake --build build -j

# Run tests with profile collection
cd build
export LLVM_PROFILE_FILE="coverage-%p.profraw"
ctest --output-on-failure

# Merge profiles
llvm-profdata merge -sparse coverage-*.profraw -o coverage.profdata

# View coverage report
llvm-cov report ./libvroom_test -instr-profile=coverage.profdata

# View specific file coverage
llvm-cov show ./libvroom_test -instr-profile=coverage.profdata -name-regex='.*' include/two_pass.h

# Generate HTML report (optional)
llvm-cov show ./libvroom_test -instr-profile=coverage.profdata -format=html -output-dir=coverage_html
```

## Python Bindings Coverage

The Python extension module (`python/src/bindings.cpp`) is covered separately from the core library:

| Flag | Tool | What It Measures |
|------|------|-----------------|
| **python** | pytest-cov | Python source code (`__init__.py`) |
| **python-bindings** | llvm-cov | C++ bindings code (`bindings.cpp`) |

### What's Not Measured

- **Type stub files (`.pyi`)**: These files contain type annotations only, no executable code. Coverage tools cannot and should not measure them.
- **Third-party code**: pybind11 headers and other dependencies are excluded.

### Running Python Bindings Coverage Locally

```bash
cd python

# Build with coverage instrumentation
CC=clang CXX=clang++ CMAKE_ARGS="-DENABLE_LLVM_COVERAGE=ON" \
  pip install --no-build-isolation -v -e .[test]

# Run tests with profiling
LLVM_PROFILE_FILE="coverage-%p.profraw" pytest tests/ -v

# Generate coverage report
llvm-profdata merge -sparse coverage-*.profraw -o coverage.profdata
EXTENSION=$(find . -name '_core*.so' -type f | head -1)
llvm-cov report "$EXTENSION" -instr-profile=coverage.profdata
```

## Interpreting Codecov Reports

### Using Flags

In Codecov, you can filter coverage by flag:
- **gcov**: Traditional GCC-based coverage (core library)
- **llvm**: Clang source-based coverage (core library, more accurate for headers)
- **python**: Python source code coverage
- **python-bindings**: C++ bindings coverage (bindings.cpp)

To see accurate header coverage, select the `llvm` flag in the Codecov UI.

### Understanding Discrepancies

If you see a large discrepancy between gcov and llvm coverage for a header file:
- The **llvm** number is more accurate
- The **gcov** number reflects a tool limitation, not actual test coverage

### Example Comparison

| File | gcov Coverage | llvm Coverage | Notes |
|------|--------------|---------------|-------|
| `src/dialect.cpp` | 85% | 85% | Source files are similar |
| `include/two_pass.h` | 6% | 75% | Header attribution issue |
| `include/simd_highway.h` | 0% | 68% | Template-heavy header |

## Configuration Files

| File | Purpose |
|------|---------|
| `.lcovrc` | lcov configuration (exclusion patterns, branch coverage) |
| `codecov.yml` | Codecov configuration (flags, thresholds, components) |
| `CMakeLists.txt` | Coverage compiler flags (`ENABLE_COVERAGE`, `ENABLE_LLVM_COVERAGE`) |

## CI Jobs

Three coverage jobs run in CI:

| Job | Workflow | Flag(s) | What It Measures |
|-----|----------|---------|------------------|
| **Code Coverage (GCC/lcov)** | ci.yml | `gcov` | Core library (traditional) |
| **Code Coverage (Clang/llvm-cov)** | ci.yml | `llvm` | Core library (accurate headers) |
| **Python Coverage** | python.yml | `python`, `python-bindings` | Python source + C++ bindings |

The Python Coverage job is a combined job that builds with LLVM coverage instrumentation
and runs pytest with pytest-cov, collecting both Python and C++ coverage in a single run.

All jobs are informational and won't fail the build if coverage thresholds aren't met.

## References

- [Clang Source-Based Code Coverage](https://clang.llvm.org/docs/SourceBasedCodeCoverage.html)
- [LCOV Project](https://github.com/linux-test-project/lcov)
- [Codecov Documentation](https://docs.codecov.com/)
- [GCC gcov Documentation](https://gcc.gnu.org/onlinedocs/gcc/Gcov.html)
