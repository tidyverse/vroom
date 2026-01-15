/**
 * @file fuzz_csv_parser.cpp
 * @brief LibFuzzer target for fuzz testing the CSV parser.
 */

#include "error.h"
#include "mem_util.h"
#include "two_pass.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>

struct AlignedDeleter {
  void operator()(uint8_t* ptr) const {
    if (ptr)
      aligned_free(ptr);
  }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0)
    return 0;
  // 64KB limit: Large enough to test SIMD chunking (64-byte lanes) and
  // multi-record parsing, small enough for fast fuzzing iterations
  constexpr size_t MAX_INPUT_SIZE = 64 * 1024;
  if (size > MAX_INPUT_SIZE)
    size = MAX_INPUT_SIZE;

  // Use immediate RAII to prevent leaks if an exception occurs
  std::unique_ptr<uint8_t, AlignedDeleter> guard(
      static_cast<uint8_t*>(aligned_malloc(64, size + 64)));
  if (!guard)
    return 0;
  uint8_t* buf = guard.get();
  std::memcpy(buf, data, size);
  std::memset(buf + size, 0, 64);

  libvroom::TwoPass parser;

  { // Single-threaded parsing
    libvroom::ParseIndex idx = parser.init(size, 1);
    parser.parse(buf, idx, size);
  }

  { // Error collection mode
    libvroom::ParseIndex idx = parser.init(size, 1);
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    parser.parse_with_errors(buf, idx, size, errors);
  }

  return 0;
}
