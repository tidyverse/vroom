#include "libvroom/vroom.h"

#include <algorithm>

namespace libvroom {

// Threshold for using SIMD vs scalar - SIMD has setup overhead
constexpr size_t kSimdThreshold = 64;

ChunkFinder::ChunkFinder(char separator, char quote, bool escape_backslash)
    : separator_(separator), quote_(quote), escape_backslash_(escape_backslash) {}

size_t ChunkFinder::find_row_end(const char* data, size_t size, size_t start) {
  // Use SIMD for large enough remaining data
  size_t remaining = size - start;
  if (remaining >= kSimdThreshold) {
    return find_row_end_simd(data, size, start, quote_, escape_backslash_);
  }
  // Fall back to scalar for small data
  return find_row_end_scalar(data, size, start, quote_, escape_backslash_);
}

std::vector<ChunkBoundary> ChunkFinder::find_chunks(const char* data, size_t size,
                                                    size_t target_chunk_size) {
  std::vector<ChunkBoundary> chunks;

  if (size == 0) {
    return chunks;
  }

  size_t offset = 0;

  while (offset < size) {
    ChunkBoundary chunk;
    chunk.start_offset = offset;

    // Calculate target end for this chunk
    size_t target_end = std::min(offset + target_chunk_size, size);

    // If we're at the target end, find the actual row boundary
    if (target_end < size) {
      // Scan forward to find a proper row boundary
      size_t row_end = find_row_end(data, size, target_end);

      // If find_row_end returned the same position, we might be stuck
      // This can happen if we're in the middle of a very long quoted field
      // In that case, keep scanning until we find a row boundary
      while (row_end == target_end && row_end < size) {
        target_end = std::min(target_end + target_chunk_size, size);
        row_end = find_row_end(data, size, target_end);
      }

      chunk.end_offset = row_end;
    } else {
      chunk.end_offset = size;
    }

    // Count rows in this chunk
    size_t row_count = 0;
    size_t pos = chunk.start_offset;
    while (pos < chunk.end_offset) {
      size_t row_end = find_row_end(data, chunk.end_offset, pos);
      if (row_end > pos) {
        ++row_count;
        pos = row_end;
      } else {
        break; // Shouldn't happen, but safety
      }
    }
    chunk.row_count = row_count;

    // Check if chunk ends inside a quote
    // (This is a simplified check - full implementation would track quote parity)
    chunk.ends_in_quote = false;

    chunks.push_back(chunk);
    offset = chunk.end_offset;
  }

  return chunks;
}

std::pair<size_t, size_t> ChunkFinder::count_rows(const char* data, size_t size) {
  if (size < kSimdThreshold) {
    return count_rows_scalar(data, size, quote_, escape_backslash_);
  }
  return count_rows_simd(data, size, quote_, escape_backslash_);
}

} // namespace libvroom
