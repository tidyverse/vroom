/**
 * @file fuzz_dialect_detection.cpp
 * @brief LibFuzzer target for fuzz testing dialect detection.
 */

#include "libvroom.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0)
    return 0;
  // 16KB limit: Dialect detection only examines the first portion of data,
  // so a smaller limit allows faster fuzzing without losing coverage
  constexpr size_t MAX_INPUT_SIZE = 16 * 1024;
  if (size > MAX_INPUT_SIZE)
    size = MAX_INPUT_SIZE;

  try {
    libvroom::AlignedBuffer buf = libvroom::AlignedBuffer::allocate(size);
    std::memcpy(buf.data(), data, size);

    libvroom::DialectDetector detector;
    libvroom::DetectionResult result = detector.detect(buf.data(), size);
    (void)result.success();
    (void)result.detected_columns;
    (void)result.confidence;
  } catch (...) {
    // Exceptions are valid behavior for malformed input
  }

  return 0;
}
