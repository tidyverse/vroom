/**
 * @file fuzz_csv_parser.cpp
 * @brief LibFuzzer target for fuzz testing the CSV parser.
 */

#include "libvroom.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0)
    return 0;
  // 64KB limit: Large enough to test SIMD chunking (64-byte lanes) and
  // multi-record parsing, small enough for fast fuzzing iterations
  constexpr size_t MAX_INPUT_SIZE = 64 * 1024;
  if (size > MAX_INPUT_SIZE)
    size = MAX_INPUT_SIZE;

  try {
    // Default options (comma-separated, error collection disabled)
    {
      libvroom::AlignedBuffer buf = libvroom::AlignedBuffer::allocate(size);
      std::memcpy(buf.data(), data, size);

      libvroom::CsvOptions opts;
      opts.num_threads = 1;
      libvroom::CsvReader reader(opts);
      auto open_result = reader.open_from_buffer(std::move(buf));
      if (open_result.ok) {
        auto parse_result = reader.read_all();
        (void)parse_result.ok;
        (void)reader.row_count();
      }
    }

    // Permissive error mode
    {
      libvroom::AlignedBuffer buf = libvroom::AlignedBuffer::allocate(size);
      std::memcpy(buf.data(), data, size);

      libvroom::CsvOptions opts;
      opts.num_threads = 1;
      opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
      libvroom::CsvReader reader(opts);
      auto open_result = reader.open_from_buffer(std::move(buf));
      if (open_result.ok) {
        auto parse_result = reader.read_all();
        (void)parse_result.ok;
        (void)reader.has_errors();
      }
    }
  } catch (...) {
    // Exceptions are valid behavior for malformed input
  }

  return 0;
}
