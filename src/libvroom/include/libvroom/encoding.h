/**
 * @file encoding.h
 * @brief Character encoding detection and transcoding to UTF-8.
 *
 * Detects file encoding (via BOM or heuristic analysis) and transcodes
 * non-UTF-8 content to UTF-8 before CSV parsing. Uses simdutf for
 * SIMD-accelerated conversion.
 *
 * The common case (UTF-8/ASCII) has essentially zero overhead: just a
 * 4-byte BOM check with no allocation or copy.
 */

#pragma once

#include "io_util.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace libvroom {

/// Character encoding types for CSV file input.
/// Named CharEncoding to avoid collision with Parquet Encoding enum in types.h.
enum class CharEncoding : uint8_t {
  UTF8 = 0,
  UTF8_BOM = 1,
  UTF16_LE = 2,
  UTF16_BE = 3,
  UTF32_LE = 4,
  UTF32_BE = 5,
  LATIN1 = 6,
  WINDOWS_1252 = 7,
  UNKNOWN = 255
};

/// Result of encoding detection.
struct EncodingResult {
  CharEncoding encoding = CharEncoding::UTF8;
  size_t bom_length = 0;
  double confidence = 1.0;
  bool needs_transcoding = false;

  /// True if detection succeeded (encoding is not UNKNOWN).
  bool success() const { return encoding != CharEncoding::UNKNOWN; }
};

/// Get the string name of an encoding (e.g., "UTF-8", "UTF-16LE").
const char* encoding_to_string(CharEncoding enc);

/// Parse an encoding name string to CharEncoding.
/// Accepts various forms: "utf-8", "UTF8", "utf-16le", "UTF-16LE",
/// "latin1", "iso-8859-1", "windows-1252", "cp1252", etc.
/// Returns CharEncoding::UNKNOWN for unrecognized names.
CharEncoding parse_encoding_name(std::string_view name);

/// Detect encoding of a data buffer.
/// Checks BOM first, then uses heuristic analysis if no BOM is found.
/// For UTF-8/ASCII data (the common case), returns immediately with no
/// allocation or heavy computation.
EncodingResult detect_encoding(const uint8_t* data, size_t size);

/// Transcode a buffer from the given encoding to UTF-8.
/// Returns a new AlignedBuffer with the transcoded content.
/// The BOM (if any) is stripped from the output.
///
/// @param data   Pointer to the raw input data.
/// @param size   Size of the input data in bytes.
/// @param enc    Source encoding.
/// @param bom_length  Number of BOM bytes to skip.
/// @return AlignedBuffer containing UTF-8 data with SIMD padding.
/// @throws std::runtime_error on transcoding failure.
AlignedBuffer transcode_to_utf8(const uint8_t* data, size_t size, CharEncoding enc,
                                size_t bom_length);

} // namespace libvroom
