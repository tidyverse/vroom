/**
 * @file encoding.cpp
 * @brief Character encoding detection and transcoding implementation.
 *
 * Uses simdutf for SIMD-accelerated encoding conversion. Custom handling
 * for Windows-1252 (0x80-0x9F range) which simdutf doesn't support.
 */

#include "libvroom/encoding.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <simdutf.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace libvroom {

const char* encoding_to_string(CharEncoding enc) {
  switch (enc) {
  case CharEncoding::UTF8:
    return "UTF-8";
  case CharEncoding::UTF8_BOM:
    return "UTF-8 (BOM)";
  case CharEncoding::UTF16_LE:
    return "UTF-16LE";
  case CharEncoding::UTF16_BE:
    return "UTF-16BE";
  case CharEncoding::UTF32_LE:
    return "UTF-32LE";
  case CharEncoding::UTF32_BE:
    return "UTF-32BE";
  case CharEncoding::LATIN1:
    return "Latin-1";
  case CharEncoding::WINDOWS_1252:
    return "Windows-1252";
  case CharEncoding::UNKNOWN:
    return "Unknown";
  }
  return "Unknown";
}

CharEncoding parse_encoding_name(std::string_view name) {
  // Normalize: lowercase, remove hyphens and underscores
  std::string normalized;
  normalized.reserve(name.size());
  for (char c : name) {
    if (c != '-' && c != '_') {
      normalized += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }

  if (normalized == "utf8")
    return CharEncoding::UTF8;
  if (normalized == "utf16le")
    return CharEncoding::UTF16_LE;
  if (normalized == "utf16be")
    return CharEncoding::UTF16_BE;
  if (normalized == "utf32le")
    return CharEncoding::UTF32_LE;
  if (normalized == "utf32be")
    return CharEncoding::UTF32_BE;
  if (normalized == "latin1" || normalized == "iso88591")
    return CharEncoding::LATIN1;
  if (normalized == "windows1252" || normalized == "cp1252" || normalized == "win1252")
    return CharEncoding::WINDOWS_1252;

  return CharEncoding::UNKNOWN;
}

// Windows-1252 lookup table for bytes 0x80-0x9F.
// These map to Unicode code points that differ from Latin-1.
// Entries of 0 indicate undefined bytes (mapped to U+FFFD replacement char).
static const uint32_t windows1252_to_unicode[32] = {
    0x20AC, // 0x80 -> Euro sign
    0,      // 0x81 -> undefined
    0x201A, // 0x82 -> Single low-9 quotation mark
    0x0192, // 0x83 -> Latin small letter f with hook
    0x201E, // 0x84 -> Double low-9 quotation mark
    0x2026, // 0x85 -> Horizontal ellipsis
    0x2020, // 0x86 -> Dagger
    0x2021, // 0x87 -> Double dagger
    0x02C6, // 0x88 -> Modifier letter circumflex accent
    0x2030, // 0x89 -> Per mille sign
    0x0160, // 0x8A -> Latin capital letter S with caron
    0x2039, // 0x8B -> Single left-pointing angle quotation mark
    0x0152, // 0x8C -> Latin capital ligature OE
    0,      // 0x8D -> undefined
    0x017D, // 0x8E -> Latin capital letter Z with caron
    0,      // 0x8F -> undefined
    0,      // 0x90 -> undefined
    0x2018, // 0x91 -> Left single quotation mark
    0x2019, // 0x92 -> Right single quotation mark
    0x201C, // 0x93 -> Left double quotation mark
    0x201D, // 0x94 -> Right double quotation mark
    0x2022, // 0x95 -> Bullet
    0x2013, // 0x96 -> En dash
    0x2014, // 0x97 -> Em dash
    0x02DC, // 0x98 -> Small tilde
    0x2122, // 0x99 -> Trade mark sign
    0x0161, // 0x9A -> Latin small letter s with caron
    0x203A, // 0x9B -> Single right-pointing angle quotation mark
    0x0153, // 0x9C -> Latin small ligature oe
    0,      // 0x9D -> undefined
    0x017E, // 0x9E -> Latin small letter z with caron
    0x0178, // 0x9F -> Latin capital letter Y with diaeresis
};

// Encode a Unicode code point as UTF-8 bytes. Returns number of bytes written.
static size_t encode_utf8(uint32_t cp, uint8_t* out) {
  if (cp < 0x80) {
    out[0] = static_cast<uint8_t>(cp);
    return 1;
  } else if (cp < 0x800) {
    out[0] = static_cast<uint8_t>(0xC0 | (cp >> 6));
    out[1] = static_cast<uint8_t>(0x80 | (cp & 0x3F));
    return 2;
  } else if (cp < 0x10000) {
    out[0] = static_cast<uint8_t>(0xE0 | (cp >> 12));
    out[1] = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F));
    out[2] = static_cast<uint8_t>(0x80 | (cp & 0x3F));
    return 3;
  } else {
    out[0] = static_cast<uint8_t>(0xF0 | (cp >> 18));
    out[1] = static_cast<uint8_t>(0x80 | ((cp >> 12) & 0x3F));
    out[2] = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F));
    out[3] = static_cast<uint8_t>(0x80 | (cp & 0x3F));
    return 4;
  }
}

// Check if the data contains any bytes in the Windows-1252 0x80-0x9F range.
static bool has_windows1252_bytes(const uint8_t* data, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    if (data[i] >= 0x80 && data[i] <= 0x9F) {
      return true;
    }
  }
  return false;
}

EncodingResult detect_encoding(const uint8_t* data, size_t size) {
  EncodingResult result;

  if (size == 0) {
    result.encoding = CharEncoding::UTF8;
    result.confidence = 1.0;
    return result;
  }

  // Check BOM (byte order mark)
  // Check UTF-32 BOMs first (4 bytes) before UTF-16 (2 bytes)
  if (size >= 4) {
    if (data[0] == 0xFF && data[1] == 0xFE && data[2] == 0x00 && data[3] == 0x00) {
      result.encoding = CharEncoding::UTF32_LE;
      result.bom_length = 4;
      result.confidence = 1.0;
      result.needs_transcoding = true;
      return result;
    }
    if (data[0] == 0x00 && data[1] == 0x00 && data[2] == 0xFE && data[3] == 0xFF) {
      result.encoding = CharEncoding::UTF32_BE;
      result.bom_length = 4;
      result.confidence = 1.0;
      result.needs_transcoding = true;
      return result;
    }
  }

  if (size >= 3) {
    if (data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF) {
      result.encoding = CharEncoding::UTF8_BOM;
      result.bom_length = 3;
      result.confidence = 1.0;
      result.needs_transcoding = false; // Just skip BOM bytes
      return result;
    }
  }

  if (size >= 2) {
    if (data[0] == 0xFF && data[1] == 0xFE) {
      result.encoding = CharEncoding::UTF16_LE;
      result.bom_length = 2;
      result.confidence = 1.0;
      result.needs_transcoding = true;
      return result;
    }
    if (data[0] == 0xFE && data[1] == 0xFF) {
      result.encoding = CharEncoding::UTF16_BE;
      result.bom_length = 2;
      result.confidence = 1.0;
      result.needs_transcoding = true;
      return result;
    }
  }

  // No BOM found — heuristic detection.
  // Check for null byte patterns first, since null bytes are valid UTF-8
  // but strongly indicate UTF-16/32 encoding in text data.
  if (size >= 4) {
    // UTF-32: every 4th byte pattern
    bool could_be_utf32_le = true;
    bool could_be_utf32_be = true;
    size_t check_bytes = std::min(size, size_t(256));
    // Ensure we check a multiple of 4 bytes
    check_bytes = (check_bytes / 4) * 4;

    if (check_bytes >= 4) {
      for (size_t i = 0; i < check_bytes; i += 4) {
        // UTF-32 LE: significant byte first, then nulls
        if (data[i + 2] != 0 || data[i + 3] != 0) {
          could_be_utf32_le = false;
        }
        // UTF-32 BE: nulls first, then significant byte
        if (data[i] != 0 || data[i + 1] != 0) {
          could_be_utf32_be = false;
        }
        if (!could_be_utf32_le && !could_be_utf32_be)
          break;
      }

      if (could_be_utf32_le) {
        result.encoding = CharEncoding::UTF32_LE;
        result.confidence = 0.8;
        result.needs_transcoding = true;
        return result;
      }
      if (could_be_utf32_be) {
        result.encoding = CharEncoding::UTF32_BE;
        result.confidence = 0.8;
        result.needs_transcoding = true;
        return result;
      }
    }
  }

  // Check for UTF-16 null byte patterns
  if (size >= 2) {
    size_t null_even = 0; // Nulls at even positions (UTF-16BE pattern)
    size_t null_odd = 0;  // Nulls at odd positions (UTF-16LE pattern)
    size_t check_bytes = std::min(size, size_t(256));
    // Ensure we check a multiple of 2 bytes
    check_bytes = (check_bytes / 2) * 2;

    for (size_t i = 0; i < check_bytes; i += 2) {
      if (data[i] == 0)
        null_even++;
      if (data[i + 1] == 0)
        null_odd++;
    }

    size_t pairs = check_bytes / 2;
    // If most odd bytes are null -> UTF-16LE (ASCII range)
    if (null_odd > pairs / 2 && null_even < pairs / 4) {
      result.encoding = CharEncoding::UTF16_LE;
      result.confidence = 0.7;
      result.needs_transcoding = true;
      return result;
    }
    // If most even bytes are null -> UTF-16BE
    if (null_even > pairs / 2 && null_odd < pairs / 4) {
      result.encoding = CharEncoding::UTF16_BE;
      result.confidence = 0.7;
      result.needs_transcoding = true;
      return result;
    }
  }

  // No null byte patterns found. Check if it's valid UTF-8.
  if (simdutf::validate_utf8(reinterpret_cast<const char*>(data), size)) {
    result.encoding = CharEncoding::UTF8;
    result.confidence = 1.0;
    result.needs_transcoding = false;
    return result;
  }

  // Not valid UTF-8 and not UTF-16/32 — likely a single-byte encoding.
  // Check for Windows-1252 bytes (0x80-0x9F range differs from Latin-1).
  if (has_windows1252_bytes(data, std::min(size, size_t(4096)))) {
    result.encoding = CharEncoding::WINDOWS_1252;
    result.confidence = 0.6;
    result.needs_transcoding = true;
    return result;
  }

  // Default to Latin-1 for any non-UTF-8 single-byte data
  result.encoding = CharEncoding::LATIN1;
  result.confidence = 0.5;
  result.needs_transcoding = true;
  return result;
}

// Transcode Windows-1252 to UTF-8.
// Handles the 0x80-0x9F range via lookup table, delegates 0xA0-0xFF to Latin-1 path.
static AlignedBuffer transcode_windows1252_to_utf8(const uint8_t* data, size_t size) {
  // Worst case: each byte could become 3 UTF-8 bytes (for BMP code points)
  AlignedBuffer buf = AlignedBuffer::allocate(size * 3);
  uint8_t* out = buf.data();
  size_t out_len = 0;

  for (size_t i = 0; i < size; ++i) {
    uint8_t byte = data[i];
    if (byte < 0x80) {
      // ASCII: pass through
      out[out_len++] = byte;
    } else if (byte >= 0x80 && byte <= 0x9F) {
      // Windows-1252 specific range
      uint32_t cp = windows1252_to_unicode[byte - 0x80];
      if (cp == 0) {
        // Undefined: use U+FFFD replacement character
        cp = 0xFFFD;
      }
      out_len += encode_utf8(cp, out + out_len);
    } else {
      // 0xA0-0xFF: same as Latin-1 (Unicode code point == byte value)
      out_len += encode_utf8(static_cast<uint32_t>(byte), out + out_len);
    }
  }

  // AlignedBuffer has no resize(), so we allocate a right-sized copy.
  // Acceptable since Windows-1252 files are typically small.
  AlignedBuffer result = AlignedBuffer::allocate(out_len);
  std::memcpy(result.data(), buf.data(), out_len);
  return result;
}

AlignedBuffer transcode_to_utf8(const uint8_t* data, size_t size, CharEncoding enc,
                                size_t bom_length) {
  // Skip BOM
  const uint8_t* src = data + bom_length;
  size_t src_size = size - bom_length;

  switch (enc) {
  case CharEncoding::UTF8:
  case CharEncoding::UTF8_BOM: {
    // Just copy without BOM
    AlignedBuffer buf = AlignedBuffer::allocate(src_size);
    std::memcpy(buf.data(), src, src_size);
    return buf;
  }

  case CharEncoding::UTF16_LE: {
    // Copy into aligned char16_t buffer to avoid UB from misaligned reinterpret_cast.
    // Current callers (mmap, AlignedBuffer) are 64-byte aligned with even BOM sizes,
    // but this is a public API so we don't assume alignment.
    size_t src16_len = src_size / 2;
    std::vector<char16_t> aligned16(src16_len);
    std::memcpy(aligned16.data(), src, src16_len * sizeof(char16_t));

    size_t utf8_len = simdutf::utf8_length_from_utf16le(aligned16.data(), src16_len);
    AlignedBuffer buf = AlignedBuffer::allocate(utf8_len);

    size_t written = simdutf::convert_utf16le_to_utf8(aligned16.data(), src16_len,
                                                      reinterpret_cast<char*>(buf.data()));
    if (written == 0 && src16_len > 0) {
      throw std::runtime_error("Failed to transcode UTF-16LE to UTF-8");
    }

    if (written != utf8_len) {
      AlignedBuffer result = AlignedBuffer::allocate(written);
      std::memcpy(result.data(), buf.data(), written);
      return result;
    }
    return buf;
  }

  case CharEncoding::UTF16_BE: {
    size_t src16_len = src_size / 2;
    std::vector<char16_t> aligned16(src16_len);
    std::memcpy(aligned16.data(), src, src16_len * sizeof(char16_t));

    size_t utf8_len = simdutf::utf8_length_from_utf16be(aligned16.data(), src16_len);
    AlignedBuffer buf = AlignedBuffer::allocate(utf8_len);

    size_t written = simdutf::convert_utf16be_to_utf8(aligned16.data(), src16_len,
                                                      reinterpret_cast<char*>(buf.data()));
    if (written == 0 && src16_len > 0) {
      throw std::runtime_error("Failed to transcode UTF-16BE to UTF-8");
    }

    if (written != utf8_len) {
      AlignedBuffer result = AlignedBuffer::allocate(written);
      std::memcpy(result.data(), buf.data(), written);
      return result;
    }
    return buf;
  }

  case CharEncoding::UTF32_LE: {
    size_t src32_len = src_size / 4;
    std::vector<char32_t> aligned32(src32_len);
    std::memcpy(aligned32.data(), src, src32_len * sizeof(char32_t));

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    // Byte-swap for big-endian machines
    for (size_t i = 0; i < src32_len; ++i) {
      uint32_t val = static_cast<uint32_t>(aligned32[i]);
      aligned32[i] = static_cast<char32_t>(__builtin_bswap32(val));
    }
#endif

    size_t utf8_len = simdutf::utf8_length_from_utf32(aligned32.data(), src32_len);
    AlignedBuffer buf = AlignedBuffer::allocate(utf8_len);

    size_t written = simdutf::convert_utf32_to_utf8(aligned32.data(), src32_len,
                                                    reinterpret_cast<char*>(buf.data()));
    if (written == 0 && src32_len > 0) {
      throw std::runtime_error("Failed to transcode UTF-32LE to UTF-8");
    }

    if (written != utf8_len) {
      AlignedBuffer result = AlignedBuffer::allocate(written);
      std::memcpy(result.data(), buf.data(), written);
      return result;
    }
    return buf;
  }

  case CharEncoding::UTF32_BE: {
    size_t src32_len = src_size / 4;
    std::vector<char32_t> aligned32(src32_len);
    std::memcpy(aligned32.data(), src, src32_len * sizeof(char32_t));

#if __BYTE_ORDER__ != __ORDER_BIG_ENDIAN__
    // Byte-swap for little-endian machines (common case)
    for (size_t i = 0; i < src32_len; ++i) {
      uint32_t val = static_cast<uint32_t>(aligned32[i]);
      aligned32[i] = static_cast<char32_t>(__builtin_bswap32(val));
    }
#endif

    size_t utf8_len = simdutf::utf8_length_from_utf32(aligned32.data(), src32_len);
    AlignedBuffer buf = AlignedBuffer::allocate(utf8_len);

    size_t written = simdutf::convert_utf32_to_utf8(aligned32.data(), src32_len,
                                                    reinterpret_cast<char*>(buf.data()));
    if (written == 0 && src32_len > 0) {
      throw std::runtime_error("Failed to transcode UTF-32BE to UTF-8");
    }

    if (written != utf8_len) {
      AlignedBuffer result = AlignedBuffer::allocate(written);
      std::memcpy(result.data(), buf.data(), written);
      return result;
    }
    return buf;
  }

  case CharEncoding::LATIN1: {
    size_t utf8_len =
        simdutf::utf8_length_from_latin1(reinterpret_cast<const char*>(src), src_size);
    AlignedBuffer buf = AlignedBuffer::allocate(utf8_len);

    size_t written = simdutf::convert_latin1_to_utf8(reinterpret_cast<const char*>(src), src_size,
                                                     reinterpret_cast<char*>(buf.data()));
    if (written == 0 && src_size > 0) {
      throw std::runtime_error("Failed to transcode Latin-1 to UTF-8");
    }

    if (written != utf8_len) {
      AlignedBuffer result = AlignedBuffer::allocate(written);
      std::memcpy(result.data(), buf.data(), written);
      return result;
    }
    return buf;
  }

  case CharEncoding::WINDOWS_1252: {
    return transcode_windows1252_to_utf8(src, src_size);
  }

  case CharEncoding::UNKNOWN:
    throw std::runtime_error("Cannot transcode from unknown encoding");
  }

  throw std::runtime_error("Unsupported encoding for transcoding");
}

} // namespace libvroom
