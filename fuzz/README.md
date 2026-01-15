# Fuzz Testing for libvroom

Fuzz testing infrastructure using libFuzzer with AddressSanitizer and
UndefinedBehaviorSanitizer.

## Fuzz Targets

| Target | Description | Max Input |
|--------|-------------|-----------|
| `fuzz_csv_parser` | Core two-pass CSV parser | 64 KB |
| `fuzz_dialect_detection` | Dialect detection (delimiter, quoting, line endings) | 16 KB |
| `fuzz_parse_auto` | Integrated parse_auto() combining detection + parsing | 64 KB |

## Building Locally

Requires Clang compiler:

```bash
cmake -B build-fuzz -DENABLE_FUZZING=ON \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++
cmake --build build-fuzz
```

## Running Locally

```bash
# Run with corpus (recommended)
./build-fuzz/fuzz_csv_parser build-fuzz/fuzz_corpus -max_len=65536

# Run for a specific duration (e.g., 60 seconds)
timeout 60s ./build-fuzz/fuzz_csv_parser build-fuzz/fuzz_corpus -max_len=65536

# Run all targets
for t in fuzz_csv_parser fuzz_dialect_detection fuzz_parse_auto; do
  timeout 60s ./build-fuzz/$t build-fuzz/fuzz_corpus -max_len=65536
done
```

## Seed Corpus

The seed corpus is located in `test/data/fuzz/` and includes edge cases:
- Binary data and null bytes
- Invalid UTF-8 sequences
- Mixed line endings (CR/LF/CRLF)
- Deep quoting and escape sequences
- Quote handling at EOF

The corpus is automatically copied to `build/fuzz_corpus/` during the build.

## CI Integration

Fuzzing runs automatically in GitHub Actions:

- **Weekly scheduled runs**: Every Sunday at 2:00 UTC (10 minutes per target)
- **Manual dispatch**: Run on-demand via Actions UI with configurable duration

See `.github/workflows/fuzz.yml` for the workflow configuration.

### Viewing Results

If crashes are found:
1. The workflow fails with an error message
2. Crash artifacts are uploaded and available for 30 days
3. Download artifacts to reproduce locally

## OSS-Fuzz

See `oss-fuzz/` for Google OSS-Fuzz integration files.
