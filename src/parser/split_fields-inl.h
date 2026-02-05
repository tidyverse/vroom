// SIMD-accelerated field splitting for CSV parsing using Google Highway.
//
// This file is included multiple times by split_fields.cpp with different
// SIMD targets defined by Highway's foreach_target.h mechanism.

#include "libvroom/quote_parity.h"
#include "libvroom/types.h"

#include "hwy/highway.h"

#include <cstring>
#include <vector>

HWY_BEFORE_NAMESPACE();
namespace libvroom {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Helper: post-process a field view (trim quotes and whitespace)
HWY_INLINE void PostProcessField(FieldView& field, char quote) {
  // Remove quotes if present
  if (field.size >= 2 && field.data[0] == quote && field.data[field.size - 1] == quote) {
    field.quoted = true;
    field.data++;
    field.size -= 2;
  }

  // Trim leading whitespace
  while (field.size > 0 && (field.data[0] == ' ' || field.data[0] == '\t')) {
    field.data++;
    field.size--;
  }

  // Trim trailing whitespace
  while (field.size > 0 &&
         (field.data[field.size - 1] == ' ' || field.data[field.size - 1] == '\t')) {
    field.size--;
  }
}

// SIMD field splitter implementation for 64-byte blocks
// Returns the positions of valid field ends (separators/EOL outside quotes)
HWY_NOINLINE std::vector<FieldView> SplitFieldsSimdImpl(const char* data, size_t size,
                                                        char separator, char quote) {
  std::vector<FieldView> fields;
  if (size == 0) {
    return fields;
  }

  // Reserve reasonable space (typical CSV has ~10 fields per line)
  fields.reserve(16);

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // Create broadcast vectors for comparison
  const auto sep_vec = hn::Set(d, static_cast<uint8_t>(separator));
  const auto quote_vec = hn::Set(d, static_cast<uint8_t>(quote));
  const auto newline_vec = hn::Set(d, static_cast<uint8_t>('\n'));
  const auto cr_vec = hn::Set(d, static_cast<uint8_t>('\r'));

  size_t field_start = 0;
  size_t pos = 0;
  uint64_t prev_quote_state = 0; // Start outside quotes

  // Process 64-byte blocks for SIMD efficiency
  // We need exactly 64 bytes to get a 64-bit mask for quote parity
  while (pos + 64 <= size) {
    // Load 64 bytes in chunks based on vector width
    uint64_t sep_mask = 0;
    uint64_t quote_mask = 0;
    uint64_t eol_mask = 0;

    // Process 64 bytes in N-byte chunks
    for (size_t chunk = 0; chunk < 64; chunk += N) {
      auto chunk_data = hn::LoadU(d, reinterpret_cast<const uint8_t*>(data + pos + chunk));

      // Find structural characters
      auto is_sep = hn::VecFromMask(d, hn::Eq(chunk_data, sep_vec));
      auto is_quote = hn::VecFromMask(d, hn::Eq(chunk_data, quote_vec));
      auto is_nl = hn::VecFromMask(d, hn::Eq(chunk_data, newline_vec));
      auto is_cr = hn::VecFromMask(d, hn::Eq(chunk_data, cr_vec));
      auto is_eol = hn::Or(is_nl, is_cr);

      // Convert to bitmasks using StoreMaskBits
      // StoreMaskBits returns the number of bytes written
      alignas(64) uint8_t mask_bytes[8];
      (void)hn::StoreMaskBits(d, hn::MaskFromVec(is_sep), mask_bytes);
      uint64_t chunk_sep = 0;
      std::memcpy(&chunk_sep, mask_bytes, (N + 7) / 8);

      (void)hn::StoreMaskBits(d, hn::MaskFromVec(is_quote), mask_bytes);
      uint64_t chunk_quote = 0;
      std::memcpy(&chunk_quote, mask_bytes, (N + 7) / 8);

      (void)hn::StoreMaskBits(d, hn::MaskFromVec(is_eol), mask_bytes);
      uint64_t chunk_eol = 0;
      std::memcpy(&chunk_eol, mask_bytes, (N + 7) / 8);

      // Accumulate into 64-bit masks
      sep_mask |= chunk_sep << chunk;
      quote_mask |= chunk_quote << chunk;
      eol_mask |= chunk_eol << chunk;
    }

    // Compute quote parity using CLMUL
    uint64_t inside_quote_mask = find_quote_mask(quote_mask, prev_quote_state);

    // Valid field ends are separators or EOL that are outside quotes
    uint64_t valid_ends = (sep_mask | eol_mask) & ~inside_quote_mask;

    // Extract positions using bit manipulation
    while (valid_ends != 0) {
      size_t bit_pos = __builtin_ctzll(valid_ends); // Position of lowest set bit
      size_t abs_pos = pos + bit_pos;

      // Check if this is EOL
      char c = data[abs_pos];
      if (c == '\n' || c == '\r') {
        // End of line - emit final field and return
        FieldView field;
        field.data = data + field_start;
        field.size = abs_pos - field_start;
        field.quoted = false;
        PostProcessField(field, quote);
        fields.push_back(field);

        return fields;
      }

      // It's a separator - emit field
      FieldView field;
      field.data = data + field_start;
      field.size = abs_pos - field_start;
      field.quoted = false;
      PostProcessField(field, quote);
      fields.push_back(field);

      field_start = abs_pos + 1;

      // Clear the lowest set bit
      valid_ends &= valid_ends - 1;
    }

    pos += 64;
  }

  // Handle remaining bytes with scalar code
  bool in_quote = (prev_quote_state != 0);

  for (size_t i = pos; i < size; ++i) {
    char c = data[i];

    if (c == quote) {
      // Check for escaped quote
      if (in_quote && i + 1 < size && data[i + 1] == quote) {
        ++i; // Skip escaped quote
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote) {
      if (c == separator) {
        FieldView field;
        field.data = data + field_start;
        field.size = i - field_start;
        field.quoted = false;
        PostProcessField(field, quote);
        fields.push_back(field);
        field_start = i + 1;
      } else if (c == '\n' || c == '\r') {
        // End of line
        break;
      }
    }
  }

  // Handle last field
  FieldView field;
  field.data = data + field_start;
  field.size = size - field_start;

  // Remove trailing newline
  while (field.size > 0 &&
         (field.data[field.size - 1] == '\n' || field.data[field.size - 1] == '\r')) {
    field.size--;
  }

  field.quoted = false;
  PostProcessField(field, quote);
  fields.push_back(field);

  return fields;
}

// SIMD field splitter with buffer reuse (avoids allocation per call)
HWY_NOINLINE void SplitFieldsSimdIntoImpl(const char* data, size_t size, char separator, char quote,
                                          std::vector<FieldView>& fields) {
  fields.clear();
  if (size == 0) {
    return;
  }

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto sep_vec = hn::Set(d, static_cast<uint8_t>(separator));
  const auto quote_vec = hn::Set(d, static_cast<uint8_t>(quote));
  const auto newline_vec = hn::Set(d, static_cast<uint8_t>('\n'));
  const auto cr_vec = hn::Set(d, static_cast<uint8_t>('\r'));

  size_t field_start = 0;
  size_t pos = 0;
  uint64_t prev_quote_state = 0;

  // Process 64-byte blocks for SIMD efficiency
  while (pos + 64 <= size) {
    uint64_t sep_mask = 0;
    uint64_t quote_mask = 0;
    uint64_t eol_mask = 0;

    for (size_t chunk = 0; chunk < 64; chunk += N) {
      auto chunk_data = hn::LoadU(d, reinterpret_cast<const uint8_t*>(data + pos + chunk));

      auto is_sep = hn::VecFromMask(d, hn::Eq(chunk_data, sep_vec));
      auto is_quote = hn::VecFromMask(d, hn::Eq(chunk_data, quote_vec));
      auto is_nl = hn::VecFromMask(d, hn::Eq(chunk_data, newline_vec));
      auto is_cr = hn::VecFromMask(d, hn::Eq(chunk_data, cr_vec));
      auto is_eol = hn::Or(is_nl, is_cr);

      alignas(64) uint8_t mask_bytes[8];
      (void)hn::StoreMaskBits(d, hn::MaskFromVec(is_sep), mask_bytes);
      uint64_t chunk_sep = 0;
      std::memcpy(&chunk_sep, mask_bytes, (N + 7) / 8);

      (void)hn::StoreMaskBits(d, hn::MaskFromVec(is_quote), mask_bytes);
      uint64_t chunk_quote = 0;
      std::memcpy(&chunk_quote, mask_bytes, (N + 7) / 8);

      (void)hn::StoreMaskBits(d, hn::MaskFromVec(is_eol), mask_bytes);
      uint64_t chunk_eol = 0;
      std::memcpy(&chunk_eol, mask_bytes, (N + 7) / 8);

      sep_mask |= chunk_sep << chunk;
      quote_mask |= chunk_quote << chunk;
      eol_mask |= chunk_eol << chunk;
    }

    uint64_t inside_quote_mask = find_quote_mask(quote_mask, prev_quote_state);
    uint64_t valid_ends = (sep_mask | eol_mask) & ~inside_quote_mask;

    while (valid_ends != 0) {
      size_t bit_pos = __builtin_ctzll(valid_ends);
      size_t abs_pos = pos + bit_pos;

      char c = data[abs_pos];
      if (c == '\n' || c == '\r') {
        FieldView field;
        field.data = data + field_start;
        field.size = abs_pos - field_start;
        field.quoted = false;
        PostProcessField(field, quote);
        fields.push_back(field);
        return;
      }

      FieldView field;
      field.data = data + field_start;
      field.size = abs_pos - field_start;
      field.quoted = false;
      PostProcessField(field, quote);
      fields.push_back(field);
      field_start = abs_pos + 1;

      valid_ends &= valid_ends - 1;
    }

    pos += 64;
  }

  // Handle remaining bytes with scalar code
  bool in_quote = (prev_quote_state != 0);

  for (size_t i = pos; i < size; ++i) {
    char c = data[i];

    if (c == quote) {
      if (in_quote && i + 1 < size && data[i + 1] == quote) {
        ++i;
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote) {
      if (c == separator) {
        FieldView field;
        field.data = data + field_start;
        field.size = i - field_start;
        field.quoted = false;
        PostProcessField(field, quote);
        fields.push_back(field);
        field_start = i + 1;
      } else if (c == '\n' || c == '\r') {
        break;
      }
    }
  }

  // Handle last field
  FieldView field;
  field.data = data + field_start;
  field.size = size - field_start;

  while (field.size > 0 &&
         (field.data[field.size - 1] == '\n' || field.data[field.size - 1] == '\r')) {
    field.size--;
  }

  field.quoted = false;
  PostProcessField(field, quote);
  fields.push_back(field);
}

} // namespace HWY_NAMESPACE
} // namespace libvroom
HWY_AFTER_NAMESPACE();
