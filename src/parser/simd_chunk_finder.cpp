// SIMD chunk boundary finder using Google Highway.
//
// This file uses Highway's dynamic dispatch to select the optimal
// SIMD implementation at runtime based on CPU capabilities.
// The core algorithm processes 64 bytes at a time:
// 1. Load and compare for quote and newline characters
// 2. Compute quote parity using CLMUL (from quote_parity.cpp)
// 3. Mask out newlines inside quotes
// 4. Count valid row endings

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "src/parser/simd_chunk_finder.cpp"
#include "libvroom/quote_parity.h"

#include "hwy/foreach_target.h"
#include "hwy/highway.h"

#include <cstdint>
#include <tuple>
#include <utility>

// Dual-state result struct must be defined BEFORE HWY_BEFORE_NAMESPACE
// so it's consistent across all SIMD targets for dynamic dispatch
// Use #ifndef guard to prevent redefinition across foreach_target.h iterations
#ifndef VROOM_DUAL_STATE_RESULT_DEFINED
#define VROOM_DUAL_STATE_RESULT_DEFINED
namespace libvroom {
struct DualStateResultInternal {
  size_t row_count_outside;
  size_t last_row_end_outside;
  size_t row_count_inside;
  size_t last_row_end_inside;
  int ends_inside_quote; // 1 if ends inside (from outside start), 0 otherwise
};
} // namespace libvroom
#endif

HWY_BEFORE_NAMESPACE();
namespace libvroom {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// Analyze data and count rows using SIMD.
// Returns (row_count, last_row_end_offset, ends_inside_quote_as_0_or_1)
HWY_NOINLINE std::tuple<size_t, size_t, int>
AnalyzeChunkSimdImpl(const char* data, size_t size, char quote_char,
                     uint64_t initial_quote_state // 0 = outside, ~0ULL = inside
) {
  size_t row_count = 0;
  size_t last_row_end = 0;

  if (size == 0) {
    return {0, 0, (initial_quote_state != 0) ? 1 : 0};
  }

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto quote_vec = hn::Set(d, static_cast<uint8_t>(quote_char));
  const auto newline_vec = hn::Set(d, static_cast<uint8_t>('\n'));
  const auto cr_vec = hn::Set(d, static_cast<uint8_t>('\r'));

  uint64_t quote_state = initial_quote_state;
  size_t offset = 0;

  // Process in 64-byte blocks for consistent quote parity tracking
  while (offset + 64 <= size) {
    uint64_t quote_bits = 0;
    uint64_t newline_bits = 0;
    uint64_t cr_bits = 0;

    // Process 64 bytes in chunks of vector width N
    for (size_t chunk_offset = 0; chunk_offset < 64; chunk_offset += N) {
      const auto* ptr = reinterpret_cast<const uint8_t*>(data + offset + chunk_offset);
      auto block = hn::LoadU(d, ptr);

      auto qm = hn::Eq(block, quote_vec);
      auto nm = hn::Eq(block, newline_vec);
      auto cm = hn::Eq(block, cr_vec);

      // StoreMaskBits packs mask into bytes
      uint8_t q_bytes[HWY_MAX_BYTES / 8] = {0};
      uint8_t n_bytes[HWY_MAX_BYTES / 8] = {0};
      uint8_t c_bytes[HWY_MAX_BYTES / 8] = {0};

      hn::StoreMaskBits(d, qm, q_bytes);
      hn::StoreMaskBits(d, nm, n_bytes);
      hn::StoreMaskBits(d, cm, c_bytes);

      // Combine bytes into bits at correct position
      const size_t num_mask_bytes = (N + 7) / 8;
      for (size_t b = 0; b < num_mask_bytes && chunk_offset + b * 8 < 64; ++b) {
        size_t bit_offset = chunk_offset + b * 8;
        if (bit_offset < 64) {
          quote_bits |= static_cast<uint64_t>(q_bytes[b]) << bit_offset;
          newline_bits |= static_cast<uint64_t>(n_bytes[b]) << bit_offset;
          cr_bits |= static_cast<uint64_t>(c_bytes[b]) << bit_offset;
        }
      }
    }

    // Compute quote mask using CLMUL-based prefix XOR.
    // Escaped quotes ("") are handled correctly: each quote toggles state,
    // so "" toggles twice and ends up in the same state. Since newlines
    // cannot appear between adjacent quotes, the brief "wrong" state during
    // the pair doesn't affect row detection. Tests verify SIMD matches scalar.
    uint64_t inside_quote = find_quote_mask(quote_bits, quote_state);

    // Valid line endings are newlines NOT inside quotes
    uint64_t valid_lf = newline_bits & ~inside_quote;

    // Standalone CR: CR not followed by LF, and not inside quotes.
    // CR at position i is standalone if there's no LF at position i+1.
    uint64_t valid_cr = cr_bits & ~inside_quote;
    // Remove CRs that are followed by LF (CRLF pairs - we count the LF).
    // If LF is at position i+1, then (newline_bits >> 1) has bit i set.
    // So (valid_cr & (newline_bits >> 1)) gives CRs that are part of CRLF.
    uint64_t crlf_cr = valid_cr & (newline_bits >> 1);
    uint64_t standalone_cr = valid_cr & ~crlf_cr;

    // Handle CR at position 63 - need to check next block's first byte
    if (standalone_cr & (1ULL << 63)) {
      // CR at last position - check if next byte is LF
      if (offset + 64 < size && data[offset + 64] == '\n') {
        standalone_cr &= ~(1ULL << 63); // It's CRLF, don't count
      }
    }

    // Total valid line endings
    uint64_t valid_eol = valid_lf | standalone_cr;

    size_t block_rows = __builtin_popcountll(valid_eol);
    row_count += block_rows;

    // Track last row ending position
    if (valid_eol != 0) {
      // Find highest set bit position
      int last_eol_bit = 63 - __builtin_clzll(valid_eol);
      last_row_end = offset + static_cast<size_t>(last_eol_bit) + 1;
    }

    offset += 64;
  }

  // Handle remaining bytes with scalar code
  while (offset < size) {
    char c = data[offset];

    if (c == quote_char) {
      // Check for escaped quote (doubled)
      bool in_quote = (quote_state != 0);
      if (in_quote && offset + 1 < size && data[offset + 1] == quote_char) {
        offset += 2; // Skip escaped quote pair
        continue;
      }
      // Toggle quote state
      quote_state = quote_state ? 0 : ~0ULL;
    } else if (quote_state == 0) {
      // Not inside quote - check for line endings
      if (c == '\n') {
        row_count++;
        last_row_end = offset + 1;
      } else if (c == '\r') {
        // CRLF or standalone CR
        if (offset + 1 < size && data[offset + 1] == '\n') {
          // CRLF - will count on the LF
        } else {
          // Standalone CR as line ending
          row_count++;
          last_row_end = offset + 1;
        }
      }
    }
    offset++;
  }

  return {row_count, last_row_end, (quote_state != 0) ? 1 : 0};
}

// Count rows in data using SIMD.
// Returns (row_count, offset_after_last_complete_row).
HWY_NOINLINE std::pair<size_t, size_t> CountRowsSimdImpl(const char* data, size_t size,
                                                         char quote_char) {
  auto [count, last_end, ends_in_quote] = AnalyzeChunkSimdImpl(data, size, quote_char, 0);
  (void)ends_in_quote;
  return {count, last_end};
}

// POLARS ALGORITHM: Single-pass dual-state chunk analysis
// Computes stats for BOTH starting states (inside/outside quote) simultaneously.
// Key insight: quote_parity mask tells us which EOLs are valid for each starting state.
// - !quote_parity = valid EOLs for starting OUTSIDE quotes
// - quote_parity = valid EOLs for starting INSIDE quotes
//
// Returns: row counts and last row offsets for both states, plus ending quote state.
HWY_NOINLINE DualStateResultInternal AnalyzeChunkDualStateSimdImpl(const char* data, size_t size,
                                                                   char quote_char) {
  DualStateResultInternal result = {0, 0, 0, 0, 0};

  if (size == 0) {
    return result;
  }

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto quote_vec = hn::Set(d, static_cast<uint8_t>(quote_char));
  const auto newline_vec = hn::Set(d, static_cast<uint8_t>('\n'));
  const auto cr_vec = hn::Set(d, static_cast<uint8_t>('\r'));

  // Global quote parity: 0 means even quotes seen so far
  uint64_t global_quote_parity_mask = 0;
  size_t offset = 0;

  // Process in 64-byte blocks for consistent quote parity tracking
  while (offset + 64 <= size) {
    uint64_t quote_bits = 0;
    uint64_t newline_bits = 0;
    uint64_t cr_bits = 0;

    // Process 64 bytes in chunks of vector width N
    for (size_t chunk_offset = 0; chunk_offset < 64; chunk_offset += N) {
      const auto* ptr = reinterpret_cast<const uint8_t*>(data + offset + chunk_offset);
      auto block = hn::LoadU(d, ptr);

      auto qm = hn::Eq(block, quote_vec);
      auto nm = hn::Eq(block, newline_vec);
      auto cm = hn::Eq(block, cr_vec);

      uint8_t q_bytes[HWY_MAX_BYTES / 8] = {0};
      uint8_t n_bytes[HWY_MAX_BYTES / 8] = {0};
      uint8_t c_bytes[HWY_MAX_BYTES / 8] = {0};

      hn::StoreMaskBits(d, qm, q_bytes);
      hn::StoreMaskBits(d, nm, n_bytes);
      hn::StoreMaskBits(d, cm, c_bytes);

      const size_t num_mask_bytes = (N + 7) / 8;
      for (size_t b = 0; b < num_mask_bytes && chunk_offset + b * 8 < 64; ++b) {
        size_t bit_offset = chunk_offset + b * 8;
        if (bit_offset < 64) {
          quote_bits |= static_cast<uint64_t>(q_bytes[b]) << bit_offset;
          newline_bits |= static_cast<uint64_t>(n_bytes[b]) << bit_offset;
          cr_bits |= static_cast<uint64_t>(c_bytes[b]) << bit_offset;
        }
      }
    }

    // Compute quote parity using CLMUL (XOR with global state to handle continuation)
    uint64_t quote_parity = prefix_xorsum_inclusive(quote_bits) ^ global_quote_parity_mask;

    // Update global state for next block: sign-extend bit 63 to all bits
    global_quote_parity_mask = static_cast<uint64_t>(static_cast<int64_t>(quote_parity) >> 63);

    // Valid line endings are newlines NOT inside quotes
    // Also handle CRLF: don't count CR if followed by LF
    uint64_t crlf_cr = cr_bits & (newline_bits >> 1);

    // For state 0 (started outside): valid EOLs where !quote_parity
    uint64_t outside_quote_mask = ~quote_parity;
    uint64_t valid_lf_outside = newline_bits & outside_quote_mask;
    uint64_t valid_cr_outside = cr_bits & outside_quote_mask & ~crlf_cr;
    uint64_t valid_eol_outside = valid_lf_outside | valid_cr_outside;

    // For state 1 (started inside): valid EOLs where quote_parity
    uint64_t inside_quote_mask = quote_parity;
    uint64_t valid_lf_inside = newline_bits & inside_quote_mask;
    uint64_t valid_cr_inside = cr_bits & inside_quote_mask & ~crlf_cr;
    uint64_t valid_eol_inside = valid_lf_inside | valid_cr_inside;

    // Handle CR at position 63 - need to check next block's first byte
    if ((cr_bits & (1ULL << 63)) && offset + 64 < size && data[offset + 64] == '\n') {
      // It's part of CRLF, remove from both states
      valid_eol_outside &= ~(1ULL << 63);
      valid_eol_inside &= ~(1ULL << 63);
    }

    // Count rows for each state
    result.row_count_outside += __builtin_popcountll(valid_eol_outside);
    result.row_count_inside += __builtin_popcountll(valid_eol_inside);

    // Track last row ending position for each state
    if (valid_eol_outside != 0) {
      int last_eol_bit = 63 - __builtin_clzll(valid_eol_outside);
      result.last_row_end_outside = offset + static_cast<size_t>(last_eol_bit) + 1;
    }
    if (valid_eol_inside != 0) {
      int last_eol_bit = 63 - __builtin_clzll(valid_eol_inside);
      result.last_row_end_inside = offset + static_cast<size_t>(last_eol_bit) + 1;
    }

    offset += 64;
  }

  // Handle remaining bytes with scalar code
  // At this point, global_quote_parity_mask is 0 or ~0ULL
  bool global_quote_parity = (global_quote_parity_mask != 0);

  while (offset < size) {
    char c = data[offset];

    if (c == quote_char) {
      // Toggle global parity (don't handle escaped quotes specially in counting)
      global_quote_parity = !global_quote_parity;
    } else if (c == '\n') {
      // LF is valid EOL when parity indicates "outside" for that state
      // For state 0: valid when !global_quote_parity
      // For state 1: valid when global_quote_parity
      if (!global_quote_parity) {
        result.row_count_outside++;
        result.last_row_end_outside = offset + 1;
      } else {
        result.row_count_inside++;
        result.last_row_end_inside = offset + 1;
      }
    } else if (c == '\r') {
      // Check for CRLF
      bool is_crlf = (offset + 1 < size && data[offset + 1] == '\n');
      if (!is_crlf) {
        // Standalone CR
        if (!global_quote_parity) {
          result.row_count_outside++;
          result.last_row_end_outside = offset + 1;
        } else {
          result.row_count_inside++;
          result.last_row_end_inside = offset + 1;
        }
      }
      // If CRLF, we'll count on the LF
    }

    offset++;
  }

  result.ends_inside_quote = global_quote_parity ? 1 : 0;
  return result;
}

// Find the end of the row starting from 'start' position using SIMD.
// Returns offset of first byte after row terminator (newline or CRLF).
// If no newline found, returns size.
HWY_NOINLINE size_t FindRowEndSimdImpl(const char* data, size_t size, size_t start,
                                       char quote_char) {
  if (start >= size) {
    return size;
  }

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  const auto quote_vec = hn::Set(d, static_cast<uint8_t>(quote_char));
  const auto newline_vec = hn::Set(d, static_cast<uint8_t>('\n'));
  const auto cr_vec = hn::Set(d, static_cast<uint8_t>('\r'));

  // Start scanning from the given position
  // Assume we start outside quotes (consistent with scalar implementation)
  uint64_t quote_state = 0;
  size_t offset = start;

  // Align to 64-byte boundary for efficient SIMD processing
  // First, handle bytes before the next 64-byte aligned block
  size_t aligned_start = ((start + 63) / 64) * 64;
  if (aligned_start > size) {
    aligned_start = size;
  }

  // Process unaligned prefix with scalar
  bool in_quote = false;
  while (offset < aligned_start && offset < size) {
    char c = data[offset];
    if (c == quote_char) {
      // Check for escaped quote
      if (in_quote && offset + 1 < size && data[offset + 1] == quote_char) {
        offset += 2;
        continue;
      }
      in_quote = !in_quote;
    } else if (!in_quote) {
      if (c == '\n') {
        return offset + 1;
      }
      if (c == '\r') {
        if (offset + 1 < size && data[offset + 1] == '\n') {
          return offset + 2; // CRLF
        }
        return offset + 1; // Standalone CR
      }
    }
    offset++;
  }

  // Update quote_state for SIMD processing
  quote_state = in_quote ? ~0ULL : 0;

  // Process 64-byte blocks with SIMD
  while (offset + 64 <= size) {
    uint64_t quote_bits = 0;
    uint64_t newline_bits = 0;
    uint64_t cr_bits = 0;

    // Process 64 bytes in chunks of vector width N
    for (size_t chunk_offset = 0; chunk_offset < 64; chunk_offset += N) {
      const auto* ptr = reinterpret_cast<const uint8_t*>(data + offset + chunk_offset);
      auto block = hn::LoadU(d, ptr);

      auto qm = hn::Eq(block, quote_vec);
      auto nm = hn::Eq(block, newline_vec);
      auto cm = hn::Eq(block, cr_vec);

      uint8_t q_bytes[HWY_MAX_BYTES / 8] = {0};
      uint8_t n_bytes[HWY_MAX_BYTES / 8] = {0};
      uint8_t c_bytes[HWY_MAX_BYTES / 8] = {0};

      hn::StoreMaskBits(d, qm, q_bytes);
      hn::StoreMaskBits(d, nm, n_bytes);
      hn::StoreMaskBits(d, cm, c_bytes);

      const size_t num_mask_bytes = (N + 7) / 8;
      for (size_t b = 0; b < num_mask_bytes && chunk_offset + b * 8 < 64; ++b) {
        size_t bit_offset = chunk_offset + b * 8;
        if (bit_offset < 64) {
          quote_bits |= static_cast<uint64_t>(q_bytes[b]) << bit_offset;
          newline_bits |= static_cast<uint64_t>(n_bytes[b]) << bit_offset;
          cr_bits |= static_cast<uint64_t>(c_bytes[b]) << bit_offset;
        }
      }
    }

    // Compute quote mask
    uint64_t inside_quote = find_quote_mask(quote_bits, quote_state);

    // Valid line endings are newlines NOT inside quotes
    uint64_t valid_lf = newline_bits & ~inside_quote;

    // Handle standalone CR (not part of CRLF, not inside quotes)
    uint64_t valid_cr = cr_bits & ~inside_quote;
    uint64_t crlf_cr = valid_cr & (newline_bits >> 1);
    uint64_t standalone_cr = valid_cr & ~crlf_cr;

    // Handle CR at position 63 - check next block's first byte
    if (standalone_cr & (1ULL << 63)) {
      if (offset + 64 < size && data[offset + 64] == '\n') {
        standalone_cr &= ~(1ULL << 63); // It's CRLF
      }
    }

    // Total valid line endings
    uint64_t valid_eol = valid_lf | standalone_cr;

    // Find FIRST valid end-of-line (lowest set bit)
    if (valid_eol != 0) {
      int first_eol_bit = __builtin_ctzll(valid_eol);
      size_t eol_pos = offset + static_cast<size_t>(first_eol_bit);

      // Check if it's LF or CR
      if (data[eol_pos] == '\n') {
        return eol_pos + 1;
      } else {
        // CR - check for CRLF
        if (eol_pos + 1 < size && data[eol_pos + 1] == '\n') {
          return eol_pos + 2;
        }
        return eol_pos + 1;
      }
    }

    offset += 64;
  }

  // Handle remaining bytes with scalar
  in_quote = (quote_state != 0);
  while (offset < size) {
    char c = data[offset];
    if (c == quote_char) {
      if (in_quote && offset + 1 < size && data[offset + 1] == quote_char) {
        offset += 2;
        continue;
      }
      in_quote = !in_quote;
    } else if (!in_quote) {
      if (c == '\n') {
        return offset + 1;
      }
      if (c == '\r') {
        if (offset + 1 < size && data[offset + 1] == '\n') {
          return offset + 2;
        }
        return offset + 1;
      }
    }
    offset++;
  }

  return size; // No newline found
}

} // namespace HWY_NAMESPACE
} // namespace libvroom
HWY_AFTER_NAMESPACE();

// Generate dispatch tables and public API (only once)
#if HWY_ONCE

#include "libvroom/vroom.h"

#include <cstring>

namespace libvroom {

// Export implementations for dynamic dispatch
HWY_EXPORT(CountRowsSimdImpl);
HWY_EXPORT(AnalyzeChunkSimdImpl);
HWY_EXPORT(AnalyzeChunkDualStateSimdImpl);
HWY_EXPORT(FindRowEndSimdImpl);

// Public API: Count rows using SIMD
// Returns (row_count, offset_after_last_complete_row)
std::pair<size_t, size_t> count_rows_simd(const char* data, size_t size, char quote_char) {
  return HWY_DYNAMIC_DISPATCH(CountRowsSimdImpl)(data, size, quote_char);
}

// Analyze chunk with known starting quote state
// Returns (row_count, last_row_end_offset, ends_inside_quote)
std::tuple<size_t, size_t, bool> analyze_chunk_simd(const char* data, size_t size, char quote_char,
                                                    bool start_inside_quote) {
  auto [count, last_end, ends_in_quote] = HWY_DYNAMIC_DISPATCH(AnalyzeChunkSimdImpl)(
      data, size, quote_char, start_inside_quote ? ~0ULL : 0);
  return {count, last_end, ends_in_quote != 0};
}

// Single-pass dual-state chunk analysis (Polars algorithm)
// Computes stats for BOTH starting states simultaneously using SIMD
DualStateChunkStats analyze_chunk_dual_state_simd(const char* data, size_t size, char quote_char) {
  auto result = HWY_DYNAMIC_DISPATCH(AnalyzeChunkDualStateSimdImpl)(data, size, quote_char);

  DualStateChunkStats stats;
  stats.row_count_outside = result.row_count_outside;
  stats.last_row_end_outside = result.last_row_end_outside;
  stats.row_count_inside = result.row_count_inside;
  stats.last_row_end_inside = result.last_row_end_inside;
  stats.ends_inside_quote_from_outside = (result.ends_inside_quote != 0);
  return stats;
}

// Find the end of the row starting from 'start' position using SIMD.
// Returns offset of first byte after row terminator.
size_t find_row_end_simd(const char* data, size_t size, size_t start, char quote_char) {
  return HWY_DYNAMIC_DISPATCH(FindRowEndSimdImpl)(data, size, start, quote_char);
}

// Scalar fallback for small data or verification
std::pair<size_t, size_t> count_rows_scalar(const char* data, size_t size, char quote_char) {
  size_t row_count = 0;
  size_t last_row_end = 0;
  bool in_quote = false;

  for (size_t i = 0; i < size; ++i) {
    char c = data[i];

    if (c == quote_char) {
      // Check for escaped quote
      if (in_quote && i + 1 < size && data[i + 1] == quote_char) {
        ++i; // Skip escaped quote
        continue;
      }
      in_quote = !in_quote;
    } else if (!in_quote) {
      if (c == '\n') {
        ++row_count;
        last_row_end = i + 1;
      } else if (c == '\r') {
        // CRLF or standalone CR
        if (i + 1 < size && data[i + 1] == '\n') {
          // CRLF - will count on the LF
        } else {
          // Standalone CR
          ++row_count;
          last_row_end = i + 1;
        }
      }
    }
  }

  return {row_count, last_row_end};
}

// Scalar find_row_end for verification and small data
size_t find_row_end_scalar(const char* data, size_t size, size_t start, char quote_char) {
  bool in_quote = false;
  size_t i = start;

  while (i < size) {
    char c = data[i];

    if (c == quote_char) {
      // Check for escaped quote (doubled quote inside quoted field)
      if (in_quote && i + 1 < size && data[i + 1] == quote_char) {
        i += 2; // Skip escaped quote
        continue;
      }
      in_quote = !in_quote;
    } else if (!in_quote) {
      // Check for line terminator
      if (c == '\n') {
        return i + 1; // Past the newline
      }
      if (c == '\r') {
        // Handle \r\n or standalone \r
        if (i + 1 < size && data[i + 1] == '\n') {
          return i + 2; // Past \r\n
        }
        return i + 1; // Past \r
      }
    }

    ++i;
  }

  // Reached end of data (last row without newline)
  return size;
}

} // namespace libvroom

#endif // HWY_ONCE
