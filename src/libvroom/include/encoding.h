/**
 * @file encoding.h
 * @brief Character encoding detection and transcoding for CSV files.
 *
 * This header provides encoding detection (via BOM and heuristics) and
 * transcoding functionality for UTF-16 and UTF-32 encoded CSV files.
 * Files are transcoded to UTF-8 for processing by the parser.
 *
 * Supported encodings:
 * - UTF-8 (with or without BOM)
 * - UTF-16 LE (Little Endian)
 * - UTF-16 BE (Big Endian)
 * - UTF-32 LE (Little Endian)
 * - UTF-32 BE (Big Endian)
 * - Latin-1 (ISO-8859-1, pass-through detection only)
 *
 * @see detect_encoding() for encoding detection
 * @see transcode_to_utf8() for transcoding
 */

#ifndef LIBVROOM_ENCODING_H
#define LIBVROOM_ENCODING_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace libvroom {

/**
 * @brief Character encodings supported by the parser.
 */
enum class Encoding {
  UTF8,         ///< UTF-8 (default)
  UTF8_BOM,     ///< UTF-8 with BOM (EF BB BF)
  UTF16_LE,     ///< UTF-16 Little Endian
  UTF16_BE,     ///< UTF-16 Big Endian
  UTF32_LE,     ///< UTF-32 Little Endian
  UTF32_BE,     ///< UTF-32 Big Endian
  LATIN1,       ///< Latin-1 (ISO-8859-1)
  WINDOWS_1252, ///< Windows-1252 (Western European)
  UNKNOWN       ///< Unknown encoding
};

/**
 * @brief Result of encoding detection.
 */
struct EncodingResult {
  Encoding encoding = Encoding::UTF8; ///< Detected encoding
  size_t bom_length = 0;              ///< Length of BOM in bytes (0 if no BOM)
  double confidence = 1.0;            ///< Detection confidence [0.0, 1.0]
  bool needs_transcoding = false;     ///< True if transcoding to UTF-8 is needed

  /// Returns true if detection was successful
  bool success() const { return encoding != Encoding::UNKNOWN; }
};

/**
 * @brief Result of transcoding operation.
 */
struct TranscodeResult {
  uint8_t* data = nullptr; ///< Pointer to transcoded data (caller owns)
  size_t length = 0;       ///< Length of transcoded data in bytes
  bool success = false;    ///< True if transcoding succeeded
  std::string error;       ///< Error message if failed

  /// Check if the result is valid
  operator bool() const { return success && data != nullptr; }
};

/**
 * @brief Convert Encoding enum to human-readable string.
 *
 * @param enc The encoding to convert
 * @return C-string name of the encoding (e.g., "UTF-16LE", "UTF-8")
 */
const char* encoding_to_string(Encoding enc);

/**
 * @brief Parse an encoding name string to Encoding enum.
 *
 * Accepts various common aliases for each encoding:
 * - UTF-8: "utf-8", "utf8"
 * - UTF-16LE: "utf-16le", "utf16le", "utf-16-le"
 * - UTF-16BE: "utf-16be", "utf16be", "utf-16-be"
 * - UTF-32LE: "utf-32le", "utf32le", "utf-32-le"
 * - UTF-32BE: "utf-32be", "utf32be", "utf-32-be"
 * - Latin-1: "latin1", "latin-1", "iso-8859-1", "iso88591"
 * - Windows-1252: "windows-1252", "windows1252", "cp1252"
 *
 * @param name The encoding name (case-insensitive)
 * @return The corresponding Encoding enum, or Encoding::UNKNOWN if not recognized
 */
Encoding parse_encoding_name(std::string_view name);

/**
 * @brief Detect the encoding of a byte buffer.
 *
 * Detection strategy:
 * 1. Check for BOM (Byte Order Mark) - most reliable
 * 2. If no BOM, use heuristics based on null byte patterns
 *
 * BOM patterns:
 * - UTF-8:    EF BB BF
 * - UTF-16 LE: FF FE (and not FF FE 00 00)
 * - UTF-16 BE: FE FF
 * - UTF-32 LE: FF FE 00 00
 * - UTF-32 BE: 00 00 FE FF
 *
 * Heuristics (when no BOM):
 * - UTF-16: Alternating null bytes with ASCII characters
 * - UTF-32: Three null bytes between ASCII characters
 * - Latin-1: Bytes in 0x80-0xFF range with no null bytes
 * - UTF-8: Valid UTF-8 sequences or ASCII only
 *
 * @param buf Pointer to the byte buffer
 * @param len Length of the buffer in bytes
 * @return EncodingResult with detected encoding and confidence
 */
EncodingResult detect_encoding(const uint8_t* buf, size_t len);

/**
 * @brief Transcode a buffer from detected encoding to UTF-8.
 *
 * If the source is already UTF-8 (with or without BOM), this function
 * strips the BOM if present and returns a copy of the data.
 *
 * For UTF-16 and UTF-32, this performs full transcoding to UTF-8.
 *
 * @param buf Pointer to the source buffer
 * @param len Length of the source buffer in bytes
 * @param enc The detected encoding of the source
 * @param bom_length Length of BOM to skip (from EncodingResult)
 * @param padding Extra bytes to allocate after the transcoded data
 *                (for SIMD safety)
 * @return TranscodeResult with transcoded data, or error on failure
 *
 * @note The caller is responsible for freeing the returned data using
 *       aligned_free() from mem_util.h
 *
 * @example
 * @code
 * auto enc_result = detect_encoding(buf, len);
 * if (enc_result.needs_transcoding) {
 *     auto result = transcode_to_utf8(buf, len, enc_result.encoding,
 *                                     enc_result.bom_length, 64);
 *     if (result) {
 *         // Use result.data and result.length
 *         // ...
 *         aligned_free(result.data);
 *     }
 * }
 * @endcode
 */
TranscodeResult transcode_to_utf8(const uint8_t* buf, size_t len, Encoding enc, size_t bom_length,
                                  size_t padding);

/**
 * @brief Calculate the UTF-8 length needed for a UTF-16 buffer.
 *
 * @param buf Pointer to UTF-16 data
 * @param len Length in bytes (not code units)
 * @param is_big_endian True for UTF-16 BE, false for UTF-16 LE
 * @return Number of bytes needed for UTF-8 output
 */
size_t utf16_to_utf8_length(const uint8_t* buf, size_t len, bool is_big_endian);

/**
 * @brief Calculate the UTF-8 length needed for a UTF-32 buffer.
 *
 * @param buf Pointer to UTF-32 data
 * @param len Length in bytes (not code units)
 * @param is_big_endian True for UTF-32 BE, false for UTF-32 LE
 * @return Number of bytes needed for UTF-8 output
 */
size_t utf32_to_utf8_length(const uint8_t* buf, size_t len, bool is_big_endian);

} // namespace libvroom

#endif // LIBVROOM_ENCODING_H
