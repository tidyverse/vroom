/**
 * @file fuzz_parse_auto.cpp
 * @brief LibFuzzer target for fuzz testing auto-detected CSV parsing.
 *
 * Exercises the integrated flow: dialect detection followed by parsing
 * with the detected dialect settings.
 */

#include "libvroom.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0)
    return 0;
  // 64KB limit: Matches fuzz_csv_parser since this exercises both
  // dialect detection and full parsing paths
  constexpr size_t MAX_INPUT_SIZE = 64 * 1024;
  if (size > MAX_INPUT_SIZE)
    size = MAX_INPUT_SIZE;

  try {
    // Step 1: Detect dialect
    libvroom::AlignedBuffer detect_buf = libvroom::AlignedBuffer::allocate(size);
    std::memcpy(detect_buf.data(), data, size);

    libvroom::DialectDetector detector;
    libvroom::DetectionResult detected = detector.detect(detect_buf.data(), size);

    // Step 2: Parse with detected dialect (or defaults if detection failed)
    libvroom::AlignedBuffer parse_buf = libvroom::AlignedBuffer::allocate(size);
    std::memcpy(parse_buf.data(), data, size);

    libvroom::CsvOptions opts;
    opts.num_threads = 1;
    opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    if (detected.success()) {
      opts.separator = detected.dialect.delimiter;
      opts.quote = detected.dialect.quote_char;
      opts.has_header = detected.has_header;
      if (detected.dialect.comment_char != '\0') {
        opts.comment = detected.dialect.comment_char;
      }
    }

    libvroom::CsvReader reader(opts);
    auto open_result = reader.open_from_buffer(std::move(parse_buf));
    if (open_result.ok) {
      auto parse_result = reader.read_all();
      if (parse_result.ok) {
        (void)reader.row_count();
        (void)reader.schema().size();
      }
      (void)reader.has_errors();
    }
  } catch (...) {
    // Exceptions are valid behavior for malformed input
  }

  return 0;
}
