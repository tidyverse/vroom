/**
 * @file two_pass.cpp
 * @brief Implementation of TwoPass parser methods.
 *
 * This file contains non-performance-critical implementations from two_pass.h,
 * including scalar parsing fallbacks, helper functions, and serialization.
 * SIMD hot-path functions remain in the header for inlining.
 */

#include "two_pass.h"

#include <algorithm>
#include <thread>
#include <vector>

namespace libvroom {

//-----------------------------------------------------------------------------
// ParseIndex class implementations
//-----------------------------------------------------------------------------

// Index file format version for backward compatibility
// Version 1 (legacy): columns (uint64_t), n_threads (uint8_t), n_indexes,
// indexes Version 2: version (uint8_t=2), columns (uint64_t), n_threads
// (uint16_t), n_indexes, indexes
// Version 3: version (uint8_t=3), source_mtime (uint64_t), source_size (uint64_t),
// columns (uint64_t), n_threads (uint16_t), n_indexes[], indexes[]
static constexpr uint8_t INDEX_FORMAT_VERSION = 2;
static constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;

// V3 header layout (32 bytes, 8-byte aligned for direct mmap pointer access):
// - version:    1 byte
// - padding:    7 bytes (alignment padding)
// - mtime:      8 bytes (uint64_t)
// - size:       8 bytes (uint64_t)
// - columns:    8 bytes (uint64_t)
// - n_threads:  2 bytes (uint16_t)
// - padding2:   6 bytes (alignment padding to ensure arrays are 8-byte aligned)
// Total: 40 bytes
static constexpr size_t INDEX_V3_HEADER_SIZE = 40;

void ParseIndex::write(const std::string& filename) {
  std::FILE* fp = std::fopen(filename.c_str(), "wb");
  if (!fp) {
    throw std::runtime_error("error opening file for writing");
  }
  // Write version 2 format: version byte, columns, n_threads (16-bit),
  // n_indexes, indexes
  uint8_t version = INDEX_FORMAT_VERSION;
  if (!((std::fwrite(&version, sizeof(uint8_t), 1, fp) == 1) &&
        (std::fwrite(&columns, sizeof(uint64_t), 1, fp) == 1) &&
        (std::fwrite(&n_threads, sizeof(uint16_t), 1, fp) == 1) &&
        (std::fwrite(n_indexes, sizeof(uint64_t), n_threads, fp) == n_threads))) {
    std::fclose(fp);
    throw std::runtime_error("error writing index");
  }

  // Write indexes: linearize from per-thread regions to contiguous format.
  // Determine where each thread's data is stored:
  // - region_offsets != nullptr: per-thread right-sized regions (from init_counted_per_thread)
  // - region_size > 0: uniform per-thread regions (from init/init_counted)
  // - otherwise: already contiguous layout (from deserialized cache)
  for (uint16_t t = 0; t < n_threads; ++t) {
    uint64_t* thread_base;
    if (region_offsets != nullptr) {
      // Right-sized per-thread regions
      thread_base = indexes + region_offsets[t];
    } else if (region_size > 0) {
      // Uniform per-thread regions
      thread_base = indexes + t * region_size;
    } else {
      // Already contiguous: compute offset for this thread
      size_t offset = 0;
      for (uint16_t i = 0; i < t; ++i) {
        offset += n_indexes[i];
      }
      thread_base = indexes + offset;
    }
    if (std::fwrite(thread_base, sizeof(uint64_t), n_indexes[t], fp) != n_indexes[t]) {
      std::fclose(fp);
      throw std::runtime_error("error writing index2");
    }
  }

  std::fclose(fp);
}

void ParseIndex::read(const std::string& filename) {
  std::FILE* fp = std::fopen(filename.c_str(), "rb");
  if (!fp) {
    throw std::runtime_error("error opening file for reading");
  }
  // Read first byte to detect version
  // Version 2 starts with version byte (value 2)
  // Version 1 (legacy) starts with columns (uint64_t), so first byte is part of
  // that
  uint8_t first_byte;
  if (std::fread(&first_byte, sizeof(uint8_t), 1, fp) != 1) {
    std::fclose(fp);
    throw std::runtime_error("error reading index version");
  }

  if (first_byte == INDEX_FORMAT_VERSION) {
    // Version 2 format: read columns, n_threads (16-bit), n_indexes, indexes
    if (!((std::fread(&columns, sizeof(uint64_t), 1, fp) == 1) &&
          (std::fread(&n_threads, sizeof(uint16_t), 1, fp) == 1) &&
          (std::fread(n_indexes, sizeof(uint64_t), n_threads, fp) == n_threads))) {
      std::fclose(fp);
      throw std::runtime_error("error reading index v2");
    }
  } else {
    // Version 1 (legacy) format: first_byte is part of columns
    // Read remaining 7 bytes of columns, then n_threads (8-bit)
    uint8_t columns_rest[7];
    if (std::fread(columns_rest, 1, 7, fp) != 7) {
      std::fclose(fp);
      throw std::runtime_error("error reading index v1 columns");
    }
    // Reconstruct columns from first_byte + columns_rest (little-endian)
    columns = first_byte;
    for (int i = 0; i < 7; ++i) {
      columns |= static_cast<uint64_t>(columns_rest[i]) << (8 * (i + 1));
    }
    // Read n_threads as uint8_t and convert to uint16_t
    uint8_t n_threads_v1;
    if (std::fread(&n_threads_v1, sizeof(uint8_t), 1, fp) != 1) {
      std::fclose(fp);
      throw std::runtime_error("error reading index v1 n_threads");
    }
    n_threads = n_threads_v1;
    if (std::fread(n_indexes, sizeof(uint64_t), n_threads, fp) != n_threads) {
      std::fclose(fp);
      throw std::runtime_error("error reading index v1 n_indexes");
    }
  }

  size_t total_size = 0;
  for (uint16_t i = 0; i < n_threads; ++i) {
    total_size += n_indexes[i];
  }
  if (std::fread(indexes, sizeof(uint64_t), total_size, fp) != total_size) {
    std::fclose(fp);
    throw std::runtime_error("error reading index2");
  }

  // Deserialized indexes are stored contiguously, not in per-thread regions.
  // Set region_size = 0 to indicate this layout to readers.
  region_size = 0;

  // Deserialized data is already in flat file-order format, so point
  // flat_indexes directly at it for O(1) access without calling compact()
  flat_indexes = indexes;
  flat_indexes_count = total_size;

  std::fclose(fp);
}

FieldSpan ParseIndex::get_field_span(uint64_t global_field_idx) const {
  // The indexes array stores the byte positions of field separators (delimiters
  // and newlines). To get a field's content:
  // - Field 0: start = 0, end = indexes[0]
  // - Field N: start = indexes[N-1] + 1, end = indexes[N]

  if (!is_valid() || n_threads == 0) {
    return FieldSpan(); // Invalid
  }

  // O(1) path: Use flat index if available
  if (is_flat()) {
    if (global_field_idx >= flat_indexes_count) {
      return FieldSpan(); // Out of bounds
    }
    uint64_t end_pos = flat_indexes[global_field_idx];
    uint64_t start_pos = (global_field_idx == 0) ? 0 : flat_indexes[global_field_idx - 1] + 1;
    return FieldSpan(start_pos, end_pos);
  }

  // O(n_threads) fallback: iterate through per-thread regions
  // (Used when compact() hasn't been called or for single-threaded parsing)

  // Handle special case: first field starts at position 0
  if (global_field_idx == 0) {
    // Find the first separator (end of first field)
    auto view = thread_data(0);
    if (view.empty()) {
      return FieldSpan(); // No separators found
    }
    return FieldSpan(0, view[0]);
  }

  // Find which thread contains this field by iterating through thread counts
  uint64_t fields_seen = 0;
  for (uint16_t t = 0; t < n_threads; ++t) {
    uint64_t thread_count = n_indexes[t];
    if (fields_seen + thread_count > global_field_idx) {
      // This thread contains our target field
      uint64_t local_idx = global_field_idx - fields_seen;
      auto view = thread_data(t);

      // Get the end position (separator position) for this field
      uint64_t end_pos = view[local_idx];

      // Get the start position (position after previous separator)
      uint64_t start_pos;
      if (local_idx == 0) {
        // First field in this thread's chunk
        if (t == 0) {
          // Very first field in the file
          start_pos = 0;
        } else {
          // Start of this thread's chunk: position after last separator of previous thread
          // Get the last separator from the previous thread
          auto prev_view = thread_data(t - 1);
          if (!prev_view.empty()) {
            start_pos = prev_view[prev_view.size() - 1] + 1;
          } else {
            // Previous thread had no separators - use chunk_starts if available
            // This shouldn't happen in well-formed CSVs, but handle gracefully
            if (chunk_starts != nullptr) {
              start_pos = chunk_starts[t];
            } else {
              return FieldSpan(); // Cannot determine start position
            }
          }
        }
      } else {
        // Start is after the previous separator in this thread
        start_pos = view[local_idx - 1] + 1;
      }

      return FieldSpan(start_pos, end_pos);
    }
    fields_seen += thread_count;
  }

  // Index out of bounds
  return FieldSpan();
}

FieldSpan ParseIndex::get_field_span(uint64_t row, uint64_t col) const {
  // Convert (row, col) to global field index
  // Each row has 'columns' fields
  if (columns == 0) {
    return FieldSpan(); // Column count not set
  }

  // Validate column index
  if (col >= columns) {
    return FieldSpan(); // Column out of bounds
  }

  // Calculate global field index
  uint64_t global_idx = row * columns + col;

  return get_field_span(global_idx);
}

void ParseIndex::compact() {
  // Idempotent: skip if already compacted
  if (is_flat()) {
    return;
  }

  // Must have valid data
  if (!is_valid() || n_threads == 0) {
    return;
  }

  // Calculate total number of separators
  uint64_t total = total_indexes();
  if (total == 0) {
    return;
  }

  // Allocate flat index array
  flat_indexes_ptr_ = std::make_unique<uint64_t[]>(total);
  flat_indexes = flat_indexes_ptr_.get();
  flat_indexes_count = total;

  // For single-threaded parsing, the indexes are already in file order
  // Just copy them directly
  if (n_threads == 1) {
    auto view = thread_data(0);
    std::copy(view.begin(), view.end(), flat_indexes);
    return;
  }

  // For multi-threaded parsing, we need to merge the per-thread arrays
  // in file order. Each thread's chunk is in a contiguous byte range,
  // and within each chunk, separators are already sorted by position.
  //
  // Strategy: Since chunks are non-overlapping and ordered by chunk_starts,
  // we can merge by interleaving based on chunk positions.
  //
  // Note: Thread 0 always starts at byte 0, then thread 1, etc.
  // The separators within each thread's data are already in ascending order.

  // Build a list of (chunk_start, thread_id) pairs and sort by chunk_start
  // to determine the order in which to merge threads
  std::vector<std::pair<uint64_t, uint16_t>> thread_order;
  thread_order.reserve(n_threads);

  for (uint16_t t = 0; t < n_threads; ++t) {
    if (n_indexes[t] > 0) {
      // Get the first separator position for this thread to determine ordering
      auto view = thread_data(t);
      uint64_t first_sep = view[0];
      thread_order.emplace_back(first_sep, t);
    }
  }

  // Sort threads by their first separator position (file order)
  std::sort(thread_order.begin(), thread_order.end());

  // Now merge all threads' data in order
  uint64_t write_idx = 0;
  for (const auto& [first_sep, t] : thread_order) {
    auto view = thread_data(t);
    std::copy(view.begin(), view.end(), flat_indexes + write_idx);
    write_idx += view.size();
  }
}

void ParseIndex::compact_column_major(size_t requested_threads) {
  // Idempotent: skip if already column-major
  if (is_column_major()) {
    return;
  }

  // Must have valid data and columns must be set
  if (!is_valid() || n_threads == 0 || columns == 0) {
    return;
  }

  // Calculate total number of separators and rows
  uint64_t total = total_indexes();
  if (total == 0) {
    return;
  }

  uint64_t nrows = total / columns;
  if (nrows == 0) {
    return;
  }

  // Ensure we have a flat row-major index first (needed for transpose)
  // This is temporary and will be freed after transpose
  compact();
  if (!is_flat()) {
    return;
  }

  // Determine thread count
  size_t num_transpose_threads = requested_threads > 0 ? requested_threads : n_threads;
  if (num_transpose_threads > nrows) {
    num_transpose_threads = static_cast<size_t>(nrows);
  }
  if (num_transpose_threads == 0) {
    num_transpose_threads = 1;
  }

  // Allocate column-major array
  col_indexes_ptr_ = std::make_unique<uint64_t[]>(total);
  col_indexes = col_indexes_ptr_.get();
  col_indexes_count = total;

  // Transpose: row-major flat_indexes to column-major col_indexes
  // Using row-first pattern (sequential reads, strided writes) with multi-threading
  // Based on benchmarking results from issue #599
  if (num_transpose_threads == 1) {
    // Single-threaded: simple row-first transpose
    for (uint64_t row = 0; row < nrows; ++row) {
      for (uint64_t col = 0; col < columns; ++col) {
        col_indexes[col * nrows + row] = flat_indexes[row * columns + col];
      }
    }
  } else {
    // Multi-threaded: each thread handles a range of rows
    std::vector<std::thread> threads;
    threads.reserve(num_transpose_threads);

    uint64_t rows_per_thread = (nrows + num_transpose_threads - 1) / num_transpose_threads;

    for (size_t t = 0; t < num_transpose_threads; ++t) {
      uint64_t row_start = t * rows_per_thread;
      uint64_t row_end = std::min(row_start + rows_per_thread, nrows);

      if (row_start >= nrows)
        break;

      threads.emplace_back([this, row_start, row_end, nrows]() {
        for (uint64_t row = row_start; row < row_end; ++row) {
          for (uint64_t col = 0; col < this->columns; ++col) {
            this->col_indexes[col * nrows + row] = this->flat_indexes[row * this->columns + col];
          }
        }
      });
    }

    for (auto& th : threads) {
      th.join();
    }
  }

  // Free the row-major flat_indexes to save memory (1x instead of 2x)
  // The column-major layout is now the primary index
  flat_indexes_ptr_.reset();
  flat_indexes = nullptr;
  flat_indexes_count = 0;
}

void ParseIndex::write(const std::string& filename, const SourceMetadata& source_meta) {
  // Write to temp file, then atomic rename for crash safety
  std::string temp_path = filename + ".tmp";

  std::FILE* fp = std::fopen(temp_path.c_str(), "wb");
  if (!fp) {
    throw std::runtime_error("error opening file for writing: " + filename);
  }

  bool success = true;

  // Write v3 header with alignment padding (40 bytes total)
  // Layout: version(1) + padding(7) + mtime(8) + size(8) + columns(8) + n_threads(2) + padding(6)
  uint8_t version = INDEX_FORMAT_VERSION_V3;
  uint8_t padding[7] = {0};
  uint8_t padding2[6] = {0};

  success = success && (std::fwrite(&version, sizeof(uint8_t), 1, fp) == 1);
  success = success && (std::fwrite(padding, 1, 7, fp) == 7); // Align mtime to 8 bytes
  success = success && (std::fwrite(&source_meta.mtime, sizeof(uint64_t), 1, fp) == 1);
  success = success && (std::fwrite(&source_meta.size, sizeof(uint64_t), 1, fp) == 1);
  success = success && (std::fwrite(&columns, sizeof(uint64_t), 1, fp) == 1);
  success = success && (std::fwrite(&n_threads, sizeof(uint16_t), 1, fp) == 1);
  success = success && (std::fwrite(padding2, 1, 6, fp) == 6); // Align n_indexes array to 8 bytes

  // Write n_indexes array (skip if empty/null)
  if (n_threads > 0 && n_indexes != nullptr) {
    success = success && (std::fwrite(n_indexes, sizeof(uint64_t), n_threads, fp) == n_threads);

    // Write indexes array: linearize from per-thread regions to contiguous format.
    // Determine where each thread's data is stored:
    // - region_offsets != nullptr: per-thread right-sized regions (from init_counted_per_thread)
    // - region_size > 0: uniform per-thread regions (from init/init_counted)
    // - otherwise: already contiguous layout (from deserialized cache)
    if (indexes != nullptr) {
      for (uint16_t t = 0; t < n_threads && success; ++t) {
        if (n_indexes[t] == 0)
          continue;
        uint64_t* thread_base;
        if (region_offsets != nullptr) {
          // Right-sized per-thread regions
          thread_base = indexes + region_offsets[t];
        } else if (region_size > 0) {
          // Uniform per-thread regions
          thread_base = indexes + t * region_size;
        } else {
          // Already contiguous: compute offset for this thread
          size_t offset = 0;
          for (uint16_t i = 0; i < t; ++i) {
            offset += n_indexes[i];
          }
          thread_base = indexes + offset;
        }
        success = success &&
                  (std::fwrite(thread_base, sizeof(uint64_t), n_indexes[t], fp) == n_indexes[t]);
      }
    }
  }

  std::fclose(fp);

  if (!success) {
    std::remove(temp_path.c_str());
    throw std::runtime_error("error writing index v3");
  }

  // Atomic rename
  if (std::rename(temp_path.c_str(), filename.c_str()) != 0) {
    std::remove(temp_path.c_str());
    throw std::runtime_error("error renaming temp file to: " + filename);
  }
}

ParseIndex ParseIndex::from_mmap(const std::string& cache_path, const SourceMetadata& source_meta) {
  ParseIndex result;

  // Allocate MmapBuffer
  auto mmap = std::make_unique<MmapBuffer>();
  if (!mmap->open(cache_path)) {
    return result; // Return empty, invalid index
  }

  const uint8_t* data = mmap->data();
  size_t file_size = mmap->size();

  // Validate minimum size for header
  if (file_size < INDEX_V3_HEADER_SIZE) {
    return result;
  }

  size_t offset = 0;

  // Read and validate version
  uint8_t version = data[offset];
  offset += 1;
  if (version != INDEX_FORMAT_VERSION_V3) {
    return result; // Not a v3 format file
  }

  // Skip padding (7 bytes to align mtime to 8-byte boundary)
  offset += 7;

  // Read source file metadata (now 8-byte aligned)
  uint64_t cached_mtime;
  std::memcpy(&cached_mtime, data + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  uint64_t cached_size;
  std::memcpy(&cached_size, data + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  // Validate source file hasn't changed
  if (cached_mtime != source_meta.mtime || cached_size != source_meta.size) {
    return result; // Source file changed, cache is stale
  }

  // Read columns (8-byte aligned)
  std::memcpy(&result.columns, data + offset, sizeof(uint64_t));
  offset += sizeof(uint64_t);

  // Read n_threads
  std::memcpy(&result.n_threads, data + offset, sizeof(uint16_t));
  offset += sizeof(uint16_t);

  // Skip padding2 (6 bytes to align n_indexes array to 8-byte boundary)
  offset += 6;

  // Validate we have enough data for n_indexes array
  size_t n_indexes_size = static_cast<size_t>(result.n_threads) * sizeof(uint64_t);
  if (offset + n_indexes_size > file_size) {
    result.n_threads = 0;
    return result;
  }

  // Set n_indexes to point directly into mmap'd data
  // const_cast is safe because we're only reading, and ParseIndex needs non-const
  result.n_indexes = const_cast<uint64_t*>(reinterpret_cast<const uint64_t*>(data + offset));
  offset += n_indexes_size;

  // Calculate total indexes and validate with overflow checks
  // Remaining bytes after n_indexes array
  size_t remaining_bytes = file_size - offset;
  size_t max_possible_indexes = remaining_bytes / sizeof(uint64_t);

  size_t total_indexes = 0;
  for (uint16_t i = 0; i < result.n_threads; ++i) {
    uint64_t n_idx = result.n_indexes[i];
    // Validate each value is within bounds (prevents overflow and ensures sanity)
    if (n_idx > max_possible_indexes) {
      result.n_indexes = nullptr;
      result.n_threads = 0;
      return result;
    }
    // Check for overflow before adding
    if (total_indexes > SIZE_MAX - static_cast<size_t>(n_idx)) {
      result.n_indexes = nullptr;
      result.n_threads = 0;
      return result;
    }
    total_indexes += static_cast<size_t>(n_idx);
  }

  // Check for multiplication overflow before computing indexes_size
  if (total_indexes > SIZE_MAX / sizeof(uint64_t)) {
    result.n_indexes = nullptr;
    result.n_threads = 0;
    return result;
  }
  size_t indexes_size = total_indexes * sizeof(uint64_t);

  // Final bounds check (using subtraction to avoid overflow in comparison)
  if (indexes_size > remaining_bytes) {
    result.n_indexes = nullptr;
    result.n_threads = 0;
    return result;
  }

  // Set indexes to point directly into mmap'd data
  result.indexes = const_cast<uint64_t*>(reinterpret_cast<const uint64_t*>(data + offset));

  // Mmap'd data is already in flat file-order format, so point
  // flat_indexes directly at it for O(1) access without calling compact()
  result.flat_indexes = result.indexes;
  result.flat_indexes_count = total_indexes;

  // Transfer mmap ownership to the ParseIndex
  result.mmap_buffer_ = std::move(mmap);

  return result;
}

//-----------------------------------------------------------------------------
// TwoPass scalar first pass implementations
//-----------------------------------------------------------------------------

TwoPass::Stats TwoPass::first_pass_chunk(const uint8_t* buf, size_t start, size_t end,
                                         char quote_char, char delimiter) {
  Stats out;
  uint64_t i = start;
  bool needs_even = out.first_even_nl == null_pos;
  bool needs_odd = out.first_odd_nl == null_pos;
  bool inside_quote = false; // Track quote state for separator counting
  while (i < end) {
    // Support LF, CRLF, and CR-only line endings
    // Check for line ending: \n, or \r not followed by \n
    bool is_line_ending = false;
    if (buf[i] == '\n') {
      is_line_ending = true;
    } else if (buf[i] == '\r') {
      // CR is a line ending only if not followed by LF
      if (i + 1 >= end || buf[i + 1] != '\n') {
        is_line_ending = true;
      }
      // If followed by LF, skip this CR (the LF will be the line ending)
    }

    if (is_line_ending) {
      // Count separator if not inside quote
      if (!inside_quote) {
        ++out.n_separators;
      }

      bool is_even = (out.n_quotes % 2) == 0;
      if (needs_even && is_even) {
        out.first_even_nl = i;
        needs_even = false;
      } else if (needs_odd && !is_even) {
        out.first_odd_nl = i;
        needs_odd = false;
      }
    } else if (buf[i] == static_cast<uint8_t>(quote_char)) {
      ++out.n_quotes;
      inside_quote = !inside_quote;
    } else if (buf[i] == static_cast<uint8_t>(delimiter)) {
      // Count delimiter if not inside quote
      if (!inside_quote) {
        ++out.n_separators;
      }
    }
    ++i;
  }
  return out;
}

TwoPass::Stats TwoPass::first_pass_naive(const uint8_t* buf, size_t start, size_t end) {
  Stats out;
  uint64_t i = start;
  while (i < end) {
    // Support LF, CRLF, and CR-only line endings
    if (buf[i] == '\n') {
      out.first_even_nl = i;
      return out;
    } else if (buf[i] == '\r') {
      // CR is a line ending only if not followed by LF
      if (i + 1 >= end || buf[i + 1] != '\n') {
        out.first_even_nl = i;
        return out;
      }
      // If followed by LF, continue - the LF will be the line ending
    }
    ++i;
  }
  return out;
}

TwoPass::quote_state TwoPass::get_quotation_state(const uint8_t* buf, size_t start, char delimiter,
                                                  char quote_char) {
  // 64kb
  constexpr int SPECULATION_SIZE = 1 << 16;

  if (start == 0) {
    return UNQUOTED;
  }

  size_t end = start > SPECULATION_SIZE ? start - SPECULATION_SIZE : 0;
  size_t i = start;
  size_t num_quotes = 0;

  // FIXED: Use i > end to avoid unsigned underflow when i reaches 0
  while (i > end) {
    if (buf[i] == static_cast<uint8_t>(quote_char)) {
      // q-o case
      if (i + 1 < start && is_other(buf[i + 1], delimiter, quote_char)) {
        return num_quotes % 2 == 0 ? QUOTED : UNQUOTED;
      }

      // o-q case
      else if (i > end && is_other(buf[i - 1], delimiter, quote_char)) {
        return num_quotes % 2 == 0 ? UNQUOTED : QUOTED;
      }
      ++num_quotes;
    }
    --i;
  }
  // Check the last position (i == end)
  if (buf[end] == static_cast<uint8_t>(quote_char)) {
    ++num_quotes;
  }
  return AMBIGUOUS;
}

TwoPass::Stats TwoPass::first_pass_speculate(const uint8_t* buf, size_t start, size_t end,
                                             char delimiter, char quote_char) {
  auto is_quoted = get_quotation_state(buf, start, delimiter, quote_char);

  for (size_t i = start; i < end; ++i) {
    // Support LF, CRLF, and CR-only line endings
    bool is_line_ending = false;
    if (buf[i] == '\n') {
      is_line_ending = true;
    } else if (buf[i] == '\r') {
      // CR is a line ending only if not followed by LF
      if (i + 1 >= end || buf[i + 1] != '\n') {
        is_line_ending = true;
      }
    }

    if (is_line_ending) {
      if (is_quoted == UNQUOTED || is_quoted == AMBIGUOUS) {
        return {0, i, null_pos};
      } else {
        return {1, null_pos, i};
      }
    } else if (buf[i] == static_cast<uint8_t>(quote_char)) {
      is_quoted = is_quoted == UNQUOTED ? QUOTED : UNQUOTED;
    }
  }
  return {0, null_pos, null_pos};
}

//-----------------------------------------------------------------------------
// TwoPass helper functions
//-----------------------------------------------------------------------------

std::string TwoPass::get_context(const uint8_t* buf, size_t len, size_t pos, size_t context_size) {
  // Handle empty buffer case
  if (len == 0 || buf == nullptr)
    return "";

  // Bounds check
  size_t safe_pos = pos < len ? pos : len - 1;
  size_t ctx_start = safe_pos > context_size ? safe_pos - context_size : 0;
  size_t ctx_end = std::min(safe_pos + context_size, len);

  std::string ctx;
  // Reserve space to avoid reallocations (worst case: every char becomes 2
  // chars like \n)
  ctx.reserve((ctx_end - ctx_start) * 2);

  for (size_t i = ctx_start; i < ctx_end; ++i) {
    char c = static_cast<char>(buf[i]);
    if (c == '\n')
      ctx += "\\n";
    else if (c == '\r')
      ctx += "\\r";
    else if (c == '\0')
      ctx += "\\0";
    else if (c >= 32 && c < 127)
      ctx += c;
    else
      ctx += "?";
  }
  return ctx;
}

void TwoPass::get_line_column(const uint8_t* buf, size_t buf_len, size_t offset, size_t& line,
                              size_t& column) {
  line = 1;
  column = 1;
  // Ensure we don't read past buffer bounds
  size_t safe_offset = offset < buf_len ? offset : buf_len;
  for (size_t i = 0; i < safe_offset; ++i) {
    if (buf[i] == '\n') {
      ++line;
      column = 1;
    } else if (buf[i] != '\r') {
      ++column;
    }
  }
}

//-----------------------------------------------------------------------------
// TwoPass comment line helper functions
//-----------------------------------------------------------------------------

bool TwoPass::is_comment_line(const uint8_t* buf, size_t pos, size_t end, char comment_char) {
  if (comment_char == '\0' || pos >= end) {
    return false;
  }

  // Skip leading whitespace (spaces and tabs only)
  while (pos < end && (buf[pos] == ' ' || buf[pos] == '\t')) {
    pos++;
  }

  // Check if first non-whitespace character is the comment character
  return pos < end && buf[pos] == static_cast<uint8_t>(comment_char);
}

size_t TwoPass::skip_to_line_end(const uint8_t* buf, size_t pos, size_t end) {
  // Skip to end of line
  while (pos < end && buf[pos] != '\n' && buf[pos] != '\r') {
    pos++;
  }

  // Skip line ending (LF, CR, or CRLF)
  if (pos < end) {
    if (buf[pos] == '\r') {
      pos++;
      if (pos < end && buf[pos] == '\n') {
        pos++;
      }
    } else if (buf[pos] == '\n') {
      pos++;
    }
  }

  return pos;
}

//-----------------------------------------------------------------------------
// TwoPass scalar second pass implementations
//-----------------------------------------------------------------------------

uint64_t TwoPass::second_pass_chunk(const uint8_t* buf, size_t start, size_t end, ParseIndex* out,
                                    size_t thread_id, ErrorCollector* errors, size_t total_len,
                                    char delimiter, char quote_char, char comment_char) {
  uint64_t pos = start;
  size_t n_indexes = 0;
  // Use contiguous per-thread storage.
  // Use region_offsets if available (per-thread right-sized allocation),
  // otherwise fall back to uniform region_size.
  size_t i = out->region_offsets != nullptr ? out->region_offsets[thread_id]
                                            : thread_id * out->region_size;
  csv_state s = RECORD_START;
  bool at_line_start = true; // Track if we're at the start of a line

  while (pos < end) {
    // Check for comment lines at the start of a record/line
    if (at_line_start && comment_char != '\0' && is_comment_line(buf, pos, end, comment_char)) {
      // Skip the entire comment line
      pos = skip_to_line_end(buf, pos, end);
      // Stay in RECORD_START state, still at line start
      continue;
    }
    at_line_start = false;

    uint8_t value = buf[pos];

    // Use effective buffer length for bounds checking
    size_t buf_len = total_len > 0 ? total_len : end;

    // Check for null bytes
    if (value == '\0' && errors) {
      size_t line, col;
      get_line_column(buf, buf_len, pos, line, col);
      errors->add_error(ErrorCode::NULL_BYTE, ErrorSeverity::RECOVERABLE, line, col, pos,
                        "Null byte in data", get_context(buf, buf_len, pos));
      if (errors->should_stop())
        return n_indexes;
      ++pos;
      continue;
    }

    StateResult result;
    if (value == static_cast<uint8_t>(quote_char)) {
      result = quoted_state(s);
      if (result.error != ErrorCode::NONE && errors) {
        size_t line, col;
        get_line_column(buf, buf_len, pos, line, col);
        std::string msg = "Quote character '";
        msg += quote_char;
        msg += "' in unquoted field";
        errors->add_error(result.error, ErrorSeverity::RECOVERABLE, line, col, pos, msg,
                          get_context(buf, buf_len, pos));
        if (errors->should_stop())
          return n_indexes;
      }
      s = result.state;
    } else if (value == static_cast<uint8_t>(delimiter)) {
      if (s != QUOTED_FIELD) {
        i = add_position(out, i, pos);
        ++n_indexes;
      }
      result = comma_state(s);
      s = result.state;
    } else if (value == '\n') {
      if (s != QUOTED_FIELD) {
        i = add_position(out, i, pos);
        ++n_indexes;
        at_line_start = true; // Next position is start of new line
      }
      result = newline_state(s);
      s = result.state;
    } else if (value == '\r') {
      // Support CR-only line endings: CR is a line ending if not followed by LF
      bool is_line_ending_char = (pos + 1 >= end || buf[pos + 1] != '\n');
      if (is_line_ending_char && s != QUOTED_FIELD) {
        i = add_position(out, i, pos);
        ++n_indexes;
        result = newline_state(s);
        s = result.state;
        at_line_start = true; // Next position is start of new line
      }
      // If CR is followed by LF (CRLF), treat CR as regular character
      // The LF will be the line ending; CR will be stripped during value
      // extraction
    } else {
      result = other_state(s);
      if (result.error != ErrorCode::NONE && errors) {
        size_t line, col;
        get_line_column(buf, buf_len, pos, line, col);
        std::string msg = "Invalid character after closing quote '";
        msg += quote_char;
        msg += "'";
        errors->add_error(result.error, ErrorSeverity::RECOVERABLE, line, col, pos, msg,
                          get_context(buf, buf_len, pos));
        if (errors->should_stop())
          return n_indexes;
      }
      s = result.state;
    }
    ++pos;
  }

  // Use effective buffer length for bounds checking
  size_t buf_len = total_len > 0 ? total_len : end;

  // Check for unclosed quote at end of chunk
  if (s == QUOTED_FIELD && errors && end == buf_len) {
    size_t line, col;
    get_line_column(buf, buf_len, pos > 0 ? pos - 1 : 0, line, col);
    std::string msg = "Unclosed quote '";
    msg += quote_char;
    msg += "' at end of file";
    errors->add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, line, col, pos, msg,
                      get_context(buf, buf_len, pos > 20 ? pos - 20 : 0));
  }

  return n_indexes;
}

uint64_t TwoPass::second_pass_chunk_throwing(const uint8_t* buf, size_t start, size_t end,
                                             ParseIndex* out, size_t thread_id, char delimiter,
                                             char quote_char, char comment_char) {
  uint64_t pos = start;
  size_t n_indexes = 0;
  // Use contiguous per-thread storage.
  // Use region_offsets if available (per-thread right-sized allocation),
  // otherwise fall back to uniform region_size.
  size_t i = out->region_offsets != nullptr ? out->region_offsets[thread_id]
                                            : thread_id * out->region_size;
  csv_state s = RECORD_START;
  bool at_line_start = true; // Track if we're at the start of a line

  while (pos < end) {
    // Check for comment lines at the start of a record/line
    if (at_line_start && comment_char != '\0' && is_comment_line(buf, pos, end, comment_char)) {
      // Skip the entire comment line
      pos = skip_to_line_end(buf, pos, end);
      // Stay in RECORD_START state, still at line start
      continue;
    }
    at_line_start = false;

    uint8_t value = buf[pos];
    StateResult result;
    if (value == static_cast<uint8_t>(quote_char)) {
      result = quoted_state(s);
      if (result.error != ErrorCode::NONE) {
        std::string msg = "Quote character '";
        msg += quote_char;
        msg += "' in unquoted field";
        throw std::runtime_error(msg);
      }
      s = result.state;
    } else if (value == static_cast<uint8_t>(delimiter)) {
      if (s != QUOTED_FIELD) {
        i = add_position(out, i, pos);
        ++n_indexes;
      }
      s = comma_state(s).state;
    } else if (value == '\n') {
      if (s != QUOTED_FIELD) {
        i = add_position(out, i, pos);
        ++n_indexes;
        at_line_start = true; // Next position is start of new line
      }
      s = newline_state(s).state;
    } else if (value == '\r') {
      // Support CR-only line endings: CR is a line ending if not followed by LF
      bool is_line_ending_char = (pos + 1 >= end || buf[pos + 1] != '\n');
      if (is_line_ending_char && s != QUOTED_FIELD) {
        i = add_position(out, i, pos);
        ++n_indexes;
        s = newline_state(s).state;
        at_line_start = true; // Next position is start of new line
      }
      // If CR is followed by LF (CRLF), treat CR as regular character
    } else {
      result = other_state(s);
      if (result.error != ErrorCode::NONE) {
        std::string msg = "Invalid character after closing quote '";
        msg += quote_char;
        msg += "'";
        throw std::runtime_error(msg);
      }
      s = result.state;
    }
    ++pos;
  }
  return n_indexes;
}

//-----------------------------------------------------------------------------
// TwoPass orchestration methods
//-----------------------------------------------------------------------------

bool TwoPass::parse_speculate(const uint8_t* buf, ParseIndex& out, size_t len,
                              const Dialect& dialect, const SecondPassProgressCallback& progress) {
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;
  uint16_t n_threads = out.n_threads;
  // Validate n_threads: treat 0 as single-threaded to avoid division by zero
  if (n_threads == 0)
    n_threads = 1;
  if (n_threads == 1) {
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    // Report full progress for single-threaded case
    if (progress && !progress(len)) {
      return false; // Cancelled
    }
    return true;
  }
  size_t chunk_size = len / n_threads;
  // If chunk size is too small, small chunks may not contain any newlines,
  // causing first_pass_speculate to return null_pos. Fall back to
  // single-threaded.
  if (chunk_size < 64) {
    // CRITICAL: Must update n_threads to 1 for correct stride in write()
    out.n_threads = 1;
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    // Report full progress for single-threaded fallback
    if (progress && !progress(len)) {
      return false; // Cancelled
    }
    return true;
  }
  std::vector<uint64_t> chunk_pos(n_threads + 1);
  std::vector<std::future<Stats>> first_pass_fut(n_threads);
  std::vector<std::future<SecondPassResult>> second_pass_fut(n_threads);

  for (int i = 0; i < n_threads; ++i) {
    first_pass_fut[i] = std::async(std::launch::async, [buf, chunk_size, i, delim, quote]() {
      return first_pass_speculate(buf, chunk_size * i, chunk_size * (i + 1), delim, quote);
    });
  }

  auto st = first_pass_fut[0].get();
  chunk_pos[0] = 0;
  for (int i = 1; i < n_threads; ++i) {
    auto st = first_pass_fut[i].get();
    chunk_pos[i] = st.n_quotes == 0 ? st.first_even_nl : st.first_odd_nl;
  }
  chunk_pos[n_threads] = len;

  // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
  for (int i = 1; i < n_threads; ++i) {
    if (chunk_pos[i] == null_pos) {
      // CRITICAL: Must update n_threads to 1 for correct stride in write()
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      // Set chunk_starts for single-threaded case
      if (out.chunk_starts != nullptr) {
        out.chunk_starts[0] = 0;
      }
      // Report full progress for single-threaded fallback
      if (progress && !progress(len)) {
        return false; // Cancelled
      }
      return true;
    }
  }

  // Store chunk start positions for later use by get_field_span()
  if (out.chunk_starts != nullptr) {
    for (int i = 0; i < n_threads; ++i) {
      out.chunk_starts[i] = chunk_pos[i];
    }
  }

  // Launch second pass with state tracking for validation
  for (int i = 0; i < n_threads; ++i) {
    second_pass_fut[i] = std::async(std::launch::async, [buf, &out, &chunk_pos, i, delim, quote]() {
      return second_pass_simd_with_state(buf, chunk_pos[i], chunk_pos[i + 1], &out, i, delim,
                                         quote);
    });
  }

  // Collect results and validate per Chang et al. Algorithm 1:
  // Each chunk must end at a record boundary for speculation to be valid.
  // If any chunk ends inside a quoted field, the speculation was incorrect
  // and we must fall back to the reliable two-pass algorithm.
  std::vector<SecondPassResult> results(n_threads);
  bool speculation_valid = true;
  bool cancelled = false;

  for (int i = 0; i < n_threads; ++i) {
    results[i] = second_pass_fut[i].get();
    out.n_indexes[i] = results[i].n_indexes;

    // Report progress after each chunk completes
    if (progress && !cancelled) {
      size_t chunk_bytes = chunk_pos[i + 1] - chunk_pos[i];
      if (!progress(chunk_bytes)) {
        cancelled = true;
        // Continue collecting futures to avoid dangling references
      }
    }

    // For all chunks except the last, verify they ended at a record boundary.
    // The last chunk (i == n_threads - 1) may end mid-record if the file
    // doesn't end with a newline, which is valid.
    if (i < n_threads - 1 && !results[i].at_record_boundary) {
      speculation_valid = false;
    }
  }

  if (cancelled) {
    return false;
  }

  // If speculation was invalid, fall back to reliable two-pass parsing.
  // This should be extremely rare (< 1 in 10 million chunks per the paper).
  if (!speculation_valid) {
    // Reset the index and re-parse using the two-pass algorithm
    // Note: Progress was already reported during speculation, so don't report
    // again during fallback to avoid double-counting
    return parse_two_pass(buf, out, len, dialect, nullptr);
  }

  return true;
}

bool TwoPass::parse_two_pass(const uint8_t* buf, ParseIndex& out, size_t len,
                             const Dialect& dialect, const SecondPassProgressCallback& progress) {
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;
  uint16_t n_threads = out.n_threads;
  // Validate n_threads: treat 0 as single-threaded to avoid division by zero
  if (n_threads == 0)
    n_threads = 1;
  if (n_threads == 1) {
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    // Report full progress for single-threaded case
    if (progress && !progress(len)) {
      return false; // Cancelled
    }
    return true;
  }
  size_t chunk_size = len / n_threads;
  // If chunk size is too small, small chunks may not contain any newlines,
  // causing first_pass_chunk to return null_pos. Fall back to single-threaded.
  if (chunk_size < 64) {
    // CRITICAL: Must update n_threads to 1 for correct stride in write()
    out.n_threads = 1;
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    // Report full progress for single-threaded fallback
    if (progress && !progress(len)) {
      return false; // Cancelled
    }
    return true;
  }
  std::vector<uint64_t> chunk_pos(n_threads + 1);
  std::vector<std::future<Stats>> first_pass_fut(n_threads);
  std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

  for (int i = 0; i < n_threads; ++i) {
    first_pass_fut[i] = std::async(std::launch::async, [buf, chunk_size, i, quote]() {
      return first_pass_chunk(buf, chunk_size * i, chunk_size * (i + 1), quote);
    });
  }

  auto st = first_pass_fut[0].get();
  size_t n_quotes = st.n_quotes;
  chunk_pos[0] = 0;
  for (int i = 1; i < n_threads; ++i) {
    auto st = first_pass_fut[i].get();
    chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
    n_quotes += st.n_quotes;
  }
  chunk_pos[n_threads] = len;

  // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
  for (int i = 1; i < n_threads; ++i) {
    if (chunk_pos[i] == null_pos) {
      // CRITICAL: Must update n_threads to 1 for correct stride in write()
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      // Set chunk_starts for single-threaded case
      if (out.chunk_starts != nullptr) {
        out.chunk_starts[0] = 0;
      }
      // Report full progress for single-threaded fallback
      if (progress && !progress(len)) {
        return false; // Cancelled
      }
      return true;
    }
  }

  // Store chunk start positions for later use by get_field_span()
  if (out.chunk_starts != nullptr) {
    for (int i = 0; i < n_threads; ++i) {
      out.chunk_starts[i] = chunk_pos[i];
    }
  }

  for (int i = 0; i < n_threads; ++i) {
    second_pass_fut[i] = std::async(std::launch::async, [buf, &out, &chunk_pos, i, delim, quote]() {
      return second_pass_chunk_throwing(buf, chunk_pos[i], chunk_pos[i + 1], &out, i, delim, quote);
    });
  }

  bool cancelled = false;
  for (int i = 0; i < n_threads; ++i) {
    out.n_indexes[i] = second_pass_fut[i].get();

    // Report progress after each chunk completes
    if (progress && !cancelled) {
      size_t chunk_bytes = chunk_pos[i + 1] - chunk_pos[i];
      if (!progress(chunk_bytes)) {
        cancelled = true;
        // Continue collecting futures to avoid dangling references
      }
    }
  }

  return !cancelled;
}

bool TwoPass::parse(const uint8_t* buf, ParseIndex& out, size_t len, const Dialect& dialect,
                    const SecondPassProgressCallback& progress) {
  return parse_speculate(buf, out, len, dialect, progress);
}

ParseIndex TwoPass::parse_optimized(const uint8_t* buf, size_t len, size_t n_threads,
                                    const Dialect& dialect,
                                    const SecondPassProgressCallback& progress) {
  ParseIndex out;
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;

  // Ensure at least 1 thread
  if (n_threads == 0)
    n_threads = 1;

  // Single-threaded fast path - no need for per-thread allocation
  if (n_threads == 1) {
    // Count separators first
    auto stats = first_pass_simd(buf, 0, len, quote, delim);
    out = init_counted(stats.n_separators, 1);
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    if (progress && !progress(len)) {
      // Cancelled, but return what we have
    }
    return out;
  }

  // Multi-threaded path with optimized per-thread allocation
  size_t chunk_size = len / n_threads;

  // If chunk size is too small, fall back to single-threaded
  if (chunk_size < 64) {
    auto stats = first_pass_simd(buf, 0, len, quote, delim);
    out = init_counted(stats.n_separators, 1);
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    if (progress && !progress(len)) {
      // Cancelled
    }
    return out;
  }

  // Phase 1: Find chunk boundaries using speculative first pass
  std::vector<uint64_t> chunk_pos(n_threads + 1);
  std::vector<std::future<Stats>> first_pass_fut(n_threads);

  for (size_t i = 0; i < n_threads; ++i) {
    first_pass_fut[i] = std::async(std::launch::async, [buf, chunk_size, i, delim, quote]() {
      return first_pass_speculate(buf, chunk_size * i, chunk_size * (i + 1), delim, quote);
    });
  }

  auto st0 = first_pass_fut[0].get();
  chunk_pos[0] = 0;
  for (size_t i = 1; i < n_threads; ++i) {
    auto st = first_pass_fut[i].get();
    chunk_pos[i] = st.n_quotes == 0 ? st.first_even_nl : st.first_odd_nl;
  }
  chunk_pos[n_threads] = len;

  // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
  for (size_t i = 1; i < n_threads; ++i) {
    if (chunk_pos[i] == null_pos) {
      auto stats = first_pass_simd(buf, 0, len, quote, delim);
      out = init_counted(stats.n_separators, 1);
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      if (out.chunk_starts != nullptr) {
        out.chunk_starts[0] = 0;
      }
      if (progress && !progress(len)) {
        // Cancelled
      }
      return out;
    }
  }

  // Phase 2: Count separators in each actual chunk (in parallel)
  std::vector<std::future<Stats>> count_fut(n_threads);
  for (size_t i = 0; i < n_threads; ++i) {
    count_fut[i] = std::async(std::launch::async, [buf, &chunk_pos, i, delim, quote]() {
      return first_pass_simd(buf, chunk_pos[i], chunk_pos[i + 1], quote, delim);
    });
  }

  std::vector<uint64_t> per_thread_counts(n_threads);
  for (size_t i = 0; i < n_threads; ++i) {
    auto stats = count_fut[i].get();
    per_thread_counts[i] = stats.n_separators;
  }

  // Phase 3: Allocate with per-thread right-sizing
  out = init_counted_per_thread(per_thread_counts, n_threads);

  // Store chunk start positions
  if (out.chunk_starts != nullptr) {
    for (size_t i = 0; i < n_threads; ++i) {
      out.chunk_starts[i] = chunk_pos[i];
    }
  }

  // Phase 4: Second pass - parse each chunk (in parallel)
  std::vector<std::future<SecondPassResult>> second_pass_fut(n_threads);
  for (size_t i = 0; i < n_threads; ++i) {
    second_pass_fut[i] = std::async(std::launch::async, [buf, &out, &chunk_pos, i, delim, quote]() {
      return second_pass_simd_with_state(buf, chunk_pos[i], chunk_pos[i + 1], &out, i, delim,
                                         quote);
    });
  }

  // Collect results
  std::vector<SecondPassResult> results(n_threads);
  bool speculation_valid = true;
  bool cancelled = false;

  for (size_t i = 0; i < n_threads; ++i) {
    results[i] = second_pass_fut[i].get();
    out.n_indexes[i] = results[i].n_indexes;

    // Report progress
    if (progress && !cancelled) {
      size_t chunk_bytes = chunk_pos[i + 1] - chunk_pos[i];
      if (!progress(chunk_bytes)) {
        cancelled = true;
      }
    }

    // Validate speculation (all chunks except last must end at record boundary)
    if (i < n_threads - 1 && !results[i].at_record_boundary) {
      speculation_valid = false;
    }
  }

  // If speculation failed, fall back to single-threaded (rare)
  if (!speculation_valid) {
    auto stats = first_pass_simd(buf, 0, len, quote, delim);
    out = init_counted(stats.n_separators, 1);
    out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
    if (out.chunk_starts != nullptr) {
      out.chunk_starts[0] = 0;
    }
  }

  return out;
}

TwoPass::branchless_chunk_result TwoPass::second_pass_branchless_chunk_with_errors(
    const BranchlessStateMachine& sm, const uint8_t* buf, size_t start, size_t end, ParseIndex* out,
    size_t thread_id, size_t total_len, ErrorMode mode) {
  branchless_chunk_result result;
  result.errors.set_mode(mode);
  // Calculate per-thread base pointer for contiguous storage
  // Use region_offsets if available (per-thread right-sized allocation)
  uint64_t* thread_base = out->region_offsets != nullptr
                              ? out->indexes + out->region_offsets[thread_id]
                              : out->indexes + thread_id * out->region_size;
  // Use SIMD-optimized version for better performance
  result.n_indexes = second_pass_simd_branchless_with_errors(
      sm, buf, start, end, thread_base, thread_id, out->n_threads, &result.errors, total_len);
  return result;
}

bool TwoPass::parse_branchless_with_errors(const uint8_t* buf, ParseIndex& out, size_t len,
                                           ErrorCollector& errors, const Dialect& dialect) {
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;
  char escape = dialect.escape_char;
  bool double_quote = dialect.double_quote;

  // Handle empty input
  if (len == 0)
    return true;

  // Check structural issues first (single-threaded, fast)
  check_empty_header(buf, len, errors);
  if (errors.should_stop())
    return false;

  check_duplicate_columns(buf, len, errors, delim, quote);
  if (errors.should_stop())
    return false;

  check_line_endings(buf, len, errors);
  if (errors.should_stop())
    return false;

  BranchlessStateMachine sm(delim, quote, escape, double_quote);
  uint16_t n_threads = out.n_threads;

  // Validate n_threads: treat 0 as single-threaded to avoid division by zero
  if (n_threads == 0)
    n_threads = 1;

  // For single-threaded, use the simpler path
  if (n_threads == 1) {
    // Use SIMD-optimized version for better performance
    out.n_indexes[0] =
        second_pass_simd_branchless_with_errors(sm, buf, 0, len, out.indexes, 0, 1, &errors, len);
    check_field_counts(buf, len, errors, delim, quote);
    return !errors.has_fatal_errors();
  }

  size_t chunk_size = len / n_threads;

  // If chunk size is too small, fall back to single-threaded
  if (chunk_size < 64) {
    out.n_threads = 1;
    // Use SIMD-optimized version for better performance
    out.n_indexes[0] =
        second_pass_simd_branchless_with_errors(sm, buf, 0, len, out.indexes, 0, 1, &errors, len);
    check_field_counts(buf, len, errors, delim, quote);
    return !errors.has_fatal_errors();
  }

  std::vector<uint64_t> chunk_pos(n_threads + 1);
  std::vector<std::future<Stats>> first_pass_fut(n_threads);
  std::vector<std::future<branchless_chunk_result>> second_pass_fut(n_threads);

  // First pass: find chunk boundaries
  for (int i = 0; i < n_threads; ++i) {
    first_pass_fut[i] = std::async(std::launch::async, [buf, chunk_size, i, quote]() {
      return first_pass_chunk(buf, chunk_size * i, chunk_size * (i + 1), quote);
    });
  }

  auto st = first_pass_fut[0].get();
  size_t n_quotes = st.n_quotes;
  chunk_pos[0] = 0;
  for (int i = 1; i < n_threads; ++i) {
    auto st = first_pass_fut[i].get();
    chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
    n_quotes += st.n_quotes;
  }
  chunk_pos[n_threads] = len;

  // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
  for (int i = 1; i < n_threads; ++i) {
    if (chunk_pos[i] == null_pos) {
      out.n_threads = 1;
      // Use SIMD-optimized version for better performance
      out.n_indexes[0] =
          second_pass_simd_branchless_with_errors(sm, buf, 0, len, out.indexes, 0, 1, &errors, len);
      // Set chunk_starts for single-threaded case
      if (out.chunk_starts != nullptr) {
        out.chunk_starts[0] = 0;
      }
      check_field_counts(buf, len, errors, delim, quote);
      return !errors.has_fatal_errors();
    }
  }

  // Store chunk start positions for later use by get_field_span()
  if (out.chunk_starts != nullptr) {
    for (int i = 0; i < n_threads; ++i) {
      out.chunk_starts[i] = chunk_pos[i];
    }
  }

  // Second pass: parse with thread-local error collectors using branchless
  ErrorMode mode = errors.mode();
  for (int i = 0; i < n_threads; ++i) {
    second_pass_fut[i] =
        std::async(std::launch::async, [sm, buf, &out, &chunk_pos, i, len, mode]() {
          return second_pass_branchless_chunk_with_errors(sm, buf, chunk_pos[i], chunk_pos[i + 1],
                                                          &out, i, len, mode);
        });
  }

  // Collect results and merge errors
  std::vector<ErrorCollector> thread_errors;
  thread_errors.reserve(n_threads);

  for (int i = 0; i < n_threads; ++i) {
    auto result = second_pass_fut[i].get();
    out.n_indexes[i] = result.n_indexes;
    thread_errors.push_back(std::move(result.errors));
  }

  // Merge all thread-local errors, sorted by byte offset
  errors.merge_sorted(thread_errors);

  // Check field counts after parsing (single-threaded, scans file linearly)
  check_field_counts(buf, len, errors, delim, quote);

  return !errors.has_fatal_errors();
}

bool TwoPass::parse_branchless(const uint8_t* buf, ParseIndex& out, size_t len,
                               const Dialect& dialect) {
  BranchlessStateMachine sm(dialect.delimiter, dialect.quote_char, dialect.escape_char,
                            dialect.double_quote);
  uint16_t n_threads = out.n_threads;

  // Validate n_threads: treat 0 as single-threaded to avoid division by zero
  if (n_threads == 0)
    n_threads = 1;

  if (n_threads == 1) {
    out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
    return true;
  }

  // Multi-threaded parsing with branchless second pass
  size_t chunk_size = len / n_threads;

  // If chunk size is too small, fall back to single-threaded
  if (chunk_size < 64) {
    out.n_threads = 1;
    out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
    return true;
  }

  std::vector<uint64_t> chunk_pos(n_threads + 1);
  std::vector<std::future<Stats>> first_pass_fut(n_threads);
  std::vector<std::future<SecondPassResult>> second_pass_fut(n_threads);

  char delim = dialect.delimiter;
  char quote = dialect.quote_char;

  // First pass: find chunk boundaries using speculative approach
  for (int i = 0; i < n_threads; ++i) {
    first_pass_fut[i] = std::async(std::launch::async, [buf, chunk_size, i, delim, quote]() {
      return first_pass_speculate(buf, chunk_size * i, chunk_size * (i + 1), delim, quote);
    });
  }

  auto st = first_pass_fut[0].get();
  chunk_pos[0] = 0;
  for (int i = 1; i < n_threads; ++i) {
    auto st = first_pass_fut[i].get();
    chunk_pos[i] = st.n_quotes == 0 ? st.first_even_nl : st.first_odd_nl;
  }
  chunk_pos[n_threads] = len;

  // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
  for (int i = 1; i < n_threads; ++i) {
    if (chunk_pos[i] == null_pos) {
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
      // Set chunk_starts for single-threaded case
      if (out.chunk_starts != nullptr) {
        out.chunk_starts[0] = 0;
      }
      return true;
    }
  }

  // Store chunk start positions for later use by get_field_span()
  if (out.chunk_starts != nullptr) {
    for (int i = 0; i < n_threads; ++i) {
      out.chunk_starts[i] = chunk_pos[i];
    }
  }

  // Second pass: branchless parsing with state tracking for validation
  // Capture sm by value since it's small (~300 bytes) and we need thread safety
  for (int i = 0; i < n_threads; ++i) {
    second_pass_fut[i] = std::async(std::launch::async, [sm, buf, &out, &chunk_pos, i]() {
      return second_pass_simd_branchless_with_state(sm, buf, chunk_pos[i], chunk_pos[i + 1], &out,
                                                    i);
    });
  }

  // Collect results and validate per Chang et al. Algorithm 1:
  // Each chunk must end at a record boundary for speculation to be valid.
  // If any chunk ends inside a quoted field, the speculation was incorrect
  // and we must fall back to reliable single-threaded parsing.
  std::vector<SecondPassResult> results(n_threads);
  bool speculation_valid = true;

  for (int i = 0; i < n_threads; ++i) {
    results[i] = second_pass_fut[i].get();
    out.n_indexes[i] = results[i].n_indexes;

    // For all chunks except the last, verify they ended at a record boundary.
    // The last chunk (i == n_threads - 1) may end mid-record if the file
    // doesn't end with a newline, which is valid.
    if (i < n_threads - 1 && !results[i].at_record_boundary) {
      speculation_valid = false;
    }
  }

  // If speculation was invalid, fall back to reliable single-threaded parsing.
  // This should be extremely rare (< 1 in 10 million chunks per the paper).
  if (!speculation_valid) {
    // Reset the index and re-parse single-threaded
    out.n_threads = 1;
    out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
    // Update chunk_starts for single-threaded fallback
    if (out.chunk_starts != nullptr) {
      out.chunk_starts[0] = 0;
    }
  }

  return true;
}

bool TwoPass::parse_auto(const uint8_t* buf, ParseIndex& out, size_t len, ErrorCollector& errors,
                         DetectionResult* detected, const DetectionOptions& detection_options) {
  // Perform dialect detection
  DialectDetector detector(detection_options);
  DetectionResult result = detector.detect(buf, len);

  // Store detection result if requested
  if (detected != nullptr) {
    *detected = result;
  }

  // Use detected dialect if successful, otherwise fall back to standard CSV
  Dialect dialect = result.success() ? result.dialect : Dialect::csv();

  // Add info message about detected dialect
  if (result.success()) {
    Dialect csv = Dialect::csv();
    if (result.dialect.delimiter != csv.delimiter || result.dialect.quote_char != csv.quote_char) {
      std::string msg = "Auto-detected dialect: " + result.dialect.to_string();
      errors.add_error(ErrorCode::NONE, ErrorSeverity::WARNING, 1, 1, 0, msg, "");
    }
  }

  // Parse with detected dialect
  return parse_two_pass_with_errors(buf, out, len, errors, dialect);
}

DetectionResult TwoPass::detect_dialect(const uint8_t* buf, size_t len,
                                        const DetectionOptions& options) {
  DialectDetector detector(options);
  return detector.detect(buf, len);
}

TwoPass::chunk_result TwoPass::second_pass_chunk_with_errors(const uint8_t* buf, size_t start,
                                                             size_t end, ParseIndex* out,
                                                             size_t thread_id, size_t total_len,
                                                             ErrorMode mode, char delimiter,
                                                             char quote_char, char comment_char) {
  chunk_result result;
  result.errors.set_mode(mode);
  result.n_indexes = second_pass_chunk(buf, start, end, out, thread_id, &result.errors, total_len,
                                       delimiter, quote_char, comment_char);
  return result;
}

bool TwoPass::parse_two_pass_with_errors(const uint8_t* buf, ParseIndex& out, size_t len,
                                         ErrorCollector& errors, const Dialect& dialect) {
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;
  char comment = dialect.comment_char;

  // Handle empty input
  if (len == 0)
    return true;

  // Check structural issues first (single-threaded, fast)
  check_empty_header(buf, len, errors, comment);
  if (errors.should_stop())
    return false;

  check_duplicate_columns(buf, len, errors, delim, quote, comment);
  if (errors.should_stop())
    return false;

  check_line_endings(buf, len, errors);
  if (errors.should_stop())
    return false;

  uint16_t n_threads = out.n_threads;

  // Validate n_threads: treat 0 as single-threaded to avoid division by zero
  if (n_threads == 0)
    n_threads = 1;

  // For single-threaded, use the simpler path
  if (n_threads == 1) {
    out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote, comment);
    check_field_counts(buf, len, errors, delim, quote, comment);
    return !errors.has_fatal_errors();
  }

  size_t chunk_size = len / n_threads;
  std::vector<uint64_t> chunk_pos(n_threads + 1);
  std::vector<std::future<Stats>> first_pass_fut(n_threads);
  std::vector<std::future<chunk_result>> second_pass_fut(n_threads);

  // First pass: find chunk boundaries
  for (int i = 0; i < n_threads; ++i) {
    first_pass_fut[i] = std::async(std::launch::async, [buf, chunk_size, i, quote]() {
      return first_pass_chunk(buf, chunk_size * i, chunk_size * (i + 1), quote);
    });
  }

  auto st = first_pass_fut[0].get();
  size_t n_quotes = st.n_quotes;
  chunk_pos[0] = 0;
  for (int i = 1; i < n_threads; ++i) {
    auto st = first_pass_fut[i].get();
    chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
    n_quotes += st.n_quotes;
  }
  chunk_pos[n_threads] = len;

  // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
  for (int i = 1; i < n_threads; ++i) {
    if (chunk_pos[i] == null_pos) {
      out.n_threads = 1;
      out.n_indexes[0] =
          second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote, comment);
      // Set chunk_starts for single-threaded case
      if (out.chunk_starts != nullptr) {
        out.chunk_starts[0] = 0;
      }
      check_field_counts(buf, len, errors, delim, quote, comment);
      return !errors.has_fatal_errors();
    }
  }

  // Store chunk start positions for later use by get_field_span()
  if (out.chunk_starts != nullptr) {
    for (int i = 0; i < n_threads; ++i) {
      out.chunk_starts[i] = chunk_pos[i];
    }
  }

  // Second pass: parse with thread-local error collectors
  ErrorMode mode = errors.mode();
  for (int i = 0; i < n_threads; ++i) {
    second_pass_fut[i] = std::async(
        std::launch::async, [buf, &out, &chunk_pos, i, len, mode, delim, quote, comment]() {
          return second_pass_chunk_with_errors(buf, chunk_pos[i], chunk_pos[i + 1], &out, i, len,
                                               mode, delim, quote, comment);
        });
  }

  // Collect results and merge errors
  std::vector<ErrorCollector> thread_errors;
  thread_errors.reserve(n_threads);

  for (int i = 0; i < n_threads; ++i) {
    auto result = second_pass_fut[i].get();
    out.n_indexes[i] = result.n_indexes;
    thread_errors.push_back(std::move(result.errors));
  }

  // Merge all thread-local errors, sorted by byte offset
  errors.merge_sorted(thread_errors);

  // Check field counts after parsing (single-threaded, scans file linearly)
  check_field_counts(buf, len, errors, delim, quote, comment);

  return !errors.has_fatal_errors();
}

bool TwoPass::parse_with_errors(const uint8_t* buf, ParseIndex& out, size_t len,
                                ErrorCollector& errors, const Dialect& dialect) {
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;
  char comment = dialect.comment_char;

  // Handle empty input
  if (len == 0)
    return true;

  // Check structural issues first
  check_empty_header(buf, len, errors, comment);
  if (errors.should_stop())
    return false;

  check_duplicate_columns(buf, len, errors, delim, quote, comment);
  if (errors.should_stop())
    return false;

  check_line_endings(buf, len, errors);
  if (errors.should_stop())
    return false;

  // Single-threaded parsing for accurate error position tracking
  out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote, comment);

  // Check field counts after parsing
  check_field_counts(buf, len, errors, delim, quote, comment);

  return !errors.has_fatal_errors();
}

bool TwoPass::parse_validate(const uint8_t* buf, ParseIndex& out, size_t len,
                             ErrorCollector& errors, const Dialect& dialect) {
  char delim = dialect.delimiter;
  char quote = dialect.quote_char;
  char comment = dialect.comment_char;

  // Handle empty input
  if (len == 0)
    return true;

  // Check structural issues first
  check_empty_header(buf, len, errors, comment);
  if (errors.should_stop())
    return false;

  check_duplicate_columns(buf, len, errors, delim, quote, comment);
  if (errors.should_stop())
    return false;

  check_line_endings(buf, len, errors);
  if (errors.should_stop())
    return false;

  // Parse with error collection
  out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote, comment);

  // Check field counts after parsing
  check_field_counts(buf, len, errors, delim, quote, comment);

  return !errors.has_fatal_errors();
}

//-----------------------------------------------------------------------------
// TwoPass validation functions
//-----------------------------------------------------------------------------

bool TwoPass::check_empty_header(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                 char comment_char) {
  if (len == 0)
    return true;

  // Skip leading comment lines if comment_char is set
  size_t pos = 0;
  while (pos < len && is_comment_line(buf, pos, len, comment_char)) {
    pos = skip_to_line_end(buf, pos, len);
  }

  // Check if we've consumed everything or hit an empty line
  if (pos >= len || buf[pos] == '\n' || buf[pos] == '\r') {
    errors.add_error(ErrorCode::EMPTY_HEADER, ErrorSeverity::RECOVERABLE, 1, 1, 0,
                     "Header row is empty", "");
    return false;
  }
  return true;
}

void TwoPass::check_duplicate_columns(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                      char delimiter, char quote_char, char comment_char) {
  if (len == 0)
    return;

  // Skip leading comment lines if comment_char is set
  size_t header_start = 0;
  while (header_start < len && is_comment_line(buf, header_start, len, comment_char)) {
    header_start = skip_to_line_end(buf, header_start, len);
  }

  if (header_start >= len)
    return;

  // Find end of first (non-comment) line
  size_t header_end = header_start;
  bool in_quote = false;
  while (header_end < len) {
    if (buf[header_end] == static_cast<uint8_t>(quote_char))
      in_quote = !in_quote;
    else if (!in_quote && (buf[header_end] == '\n' || buf[header_end] == '\r'))
      break;
    ++header_end;
  }

  // Parse header fields
  std::vector<std::string> fields;
  std::string current;
  in_quote = false;
  for (size_t i = header_start; i < header_end; ++i) {
    if (buf[i] == static_cast<uint8_t>(quote_char)) {
      in_quote = !in_quote;
    } else if (!in_quote && buf[i] == static_cast<uint8_t>(delimiter)) {
      fields.push_back(current);
      current.clear();
    } else if (buf[i] != '\r') {
      current += static_cast<char>(buf[i]);
    }
  }
  fields.push_back(current);

  // Check for duplicates
  std::unordered_set<std::string> seen;
  for (size_t i = 0; i < fields.size(); ++i) {
    if (seen.count(fields[i]) > 0) {
      errors.add_error(ErrorCode::DUPLICATE_COLUMN_NAMES, ErrorSeverity::WARNING, 1, i + 1, 0,
                       "Duplicate column name: '" + fields[i] + "'", fields[i]);
    }
    seen.insert(fields[i]);
  }
}

void TwoPass::check_field_counts(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                 char delimiter, char quote_char, char comment_char) {
  if (len == 0)
    return;

  size_t expected_fields = 0;
  size_t current_fields = 1;
  size_t current_line = 1;
  size_t line_start = 0;
  bool in_quote = false;
  bool header_done = false;
  bool at_line_start = true;

  for (size_t i = 0; i < len; ++i) {
    // Skip comment lines
    if (at_line_start && is_comment_line(buf, i, len, comment_char)) {
      size_t line_end = skip_to_line_end(buf, i, len);
      i = line_end - 1; // -1 because loop will increment
      ++current_line;
      line_start = line_end;
      continue;
    }
    at_line_start = false;

    if (buf[i] == static_cast<uint8_t>(quote_char)) {
      in_quote = !in_quote;
    } else if (!in_quote) {
      if (buf[i] == static_cast<uint8_t>(delimiter)) {
        ++current_fields;
      } else if (buf[i] == '\n') {
        if (!header_done) {
          expected_fields = current_fields;
          header_done = true;
        } else if (current_fields != expected_fields) {
          std::ostringstream msg;
          msg << "Expected " << expected_fields << " fields but found " << current_fields;
          errors.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE,
                           current_line, 1, line_start, msg.str(),
                           get_context(buf, len, line_start, 40));
          if (errors.should_stop())
            return;
        }
        current_fields = 1;
        ++current_line;
        line_start = i + 1;
        at_line_start = true;
      } else if (buf[i] == '\r') {
        // Support CR-only line endings: CR is a line ending if not followed by LF
        bool is_line_ending = (i + 1 >= len || buf[i + 1] != '\n');
        if (is_line_ending) {
          if (!header_done) {
            expected_fields = current_fields;
            header_done = true;
          } else if (current_fields != expected_fields) {
            std::ostringstream msg;
            msg << "Expected " << expected_fields << " fields but found " << current_fields;
            errors.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE,
                             current_line, 1, line_start, msg.str(),
                             get_context(buf, len, line_start, 40));
            if (errors.should_stop())
              return;
          }
          current_fields = 1;
          ++current_line;
          line_start = i + 1;
          at_line_start = true;
        }
        // If CR is followed by LF, the LF will handle the line ending
      }
    }
  }

  // Check last line if no trailing newline
  if (header_done && current_fields != expected_fields && line_start < len) {
    std::ostringstream msg;
    msg << "Expected " << expected_fields << " fields but found " << current_fields;
    errors.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::RECOVERABLE, current_line,
                     1, line_start, msg.str(), get_context(buf, len, line_start, 40));
  }
}

void TwoPass::check_line_endings(const uint8_t* buf, size_t len, ErrorCollector& errors) {
  bool has_crlf = false;
  bool has_lf = false;
  bool has_cr = false;

  for (size_t i = 0; i < len; ++i) {
    if (buf[i] == '\r') {
      if (i + 1 < len && buf[i + 1] == '\n') {
        has_crlf = true;
        ++i;
      } else {
        has_cr = true;
      }
    } else if (buf[i] == '\n') {
      has_lf = true;
    }
  }

  int types = (has_crlf ? 1 : 0) + (has_lf ? 1 : 0) + (has_cr ? 1 : 0);
  if (types > 1) {
    errors.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING, 1, 1, 0,
                     "Mixed line endings detected", "");
  }
}

//-----------------------------------------------------------------------------
// TwoPass initialization functions
//-----------------------------------------------------------------------------

ParseIndex TwoPass::init(size_t len, size_t n_threads) {
  ParseIndex out;
  // Ensure at least 1 thread for valid memory allocation
  if (n_threads == 0)
    n_threads = 1;
  out.n_threads = n_threads;

  // Set region size for contiguous per-thread storage
  out.region_size = len + 8;

  // Allocate n_indexes array with RAII ownership
  out.n_indexes_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.n_indexes = out.n_indexes_ptr_.get();

  // Allocate chunk_starts array with RAII ownership
  // This tracks where each thread's chunk starts in the source file
  out.chunk_starts_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.chunk_starts = out.chunk_starts_ptr_.get();
  // Initialize chunk_starts (will be set during parsing)
  for (size_t i = 0; i < n_threads; ++i) {
    out.chunk_starts[i] = 0;
  }

  // Allocate space for contiguous per-thread index storage.
  // Each thread gets its own region of size region_size to avoid false sharing.
  size_t allocation_size;
  if (n_threads == 1) {
    // Single-threaded: simple allocation with padding for speculative writes
    allocation_size = len + 8;
  } else {
    // Multi-threaded: contiguous per-thread regions
    allocation_size = out.region_size * n_threads;
  }

  // Allocate indexes array with RAII ownership
  out.indexes_ptr_ = std::make_unique<uint64_t[]>(allocation_size);
  out.indexes = out.indexes_ptr_.get();

  return out;
}

ParseIndex TwoPass::init_safe(size_t len, size_t n_threads, ErrorCollector* errors) {
  ParseIndex out;
  // Ensure at least 1 thread for valid memory allocation
  if (n_threads == 0)
    n_threads = 1;
  out.n_threads = n_threads;

  // Set region size for contiguous per-thread storage
  out.region_size = len + 8;

  // Calculate allocation size with overflow checking
  size_t allocation_size;
  bool overflow = false;

  if (n_threads == 1) {
    // Single-threaded: simple allocation with padding for speculative writes
    // allocation_size = len + 8
    if (len > std::numeric_limits<size_t>::max() - 8) {
      overflow = true;
    } else {
      allocation_size = len + 8;
    }
  } else {
    // Multi-threaded: contiguous per-thread regions
    // allocation_size = region_size * n_threads
    size_t len_plus_8;
    if (len > std::numeric_limits<size_t>::max() - 8) {
      overflow = true;
    } else {
      len_plus_8 = len + 8;
      // Check region_size * n_threads for overflow
      if (len_plus_8 > std::numeric_limits<size_t>::max() / n_threads) {
        overflow = true;
      } else {
        allocation_size = len_plus_8 * n_threads;
      }
    }
  }

  // Check final allocation: allocation_size * sizeof(uint64_t)
  if (!overflow && allocation_size > std::numeric_limits<size_t>::max() / sizeof(uint64_t)) {
    overflow = true;
  }

  if (overflow) {
    std::string msg = "Index allocation would overflow: len=" + std::to_string(len) +
                      ", n_threads=" + std::to_string(n_threads);
    if (errors != nullptr) {
      errors->add_error(ErrorCode::INDEX_ALLOCATION_OVERFLOW, ErrorSeverity::FATAL, 1, 1, 0, msg);
      // Return empty index to signal failure
      return out;
    } else {
      throw std::runtime_error(msg);
    }
  }

  // Safe to allocate - use RAII to ensure proper cleanup
  out.n_indexes_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.n_indexes = out.n_indexes_ptr_.get();

  // Allocate chunk_starts array with RAII ownership
  out.chunk_starts_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.chunk_starts = out.chunk_starts_ptr_.get();
  for (size_t i = 0; i < n_threads; ++i) {
    out.chunk_starts[i] = 0;
  }

  out.indexes_ptr_ = std::make_unique<uint64_t[]>(allocation_size);
  out.indexes = out.indexes_ptr_.get();

  return out;
}

ParseIndex TwoPass::init_counted(uint64_t total_separators, size_t n_threads) {
  ParseIndex out;
  // Ensure at least 1 thread for valid memory allocation
  if (n_threads == 0)
    n_threads = 1;
  out.n_threads = n_threads;

  // Set region size for contiguous per-thread storage.
  // Each thread may get all separators in the worst case.
  out.region_size = total_separators + 8;

  // Allocate n_indexes array with RAII ownership
  out.n_indexes_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.n_indexes = out.n_indexes_ptr_.get();

  // Allocate chunk_starts array with RAII ownership
  out.chunk_starts_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.chunk_starts = out.chunk_starts_ptr_.get();
  for (size_t i = 0; i < n_threads; ++i) {
    out.chunk_starts[i] = 0;
  }

  // Allocate space for separator positions.
  // Add padding for speculative writes in the write() function (writes up to 8 extra).
  //
  // With contiguous per-thread storage, thread i writes to positions:
  //   indexes[i * region_size], indexes[i * region_size + 1], ...
  //
  // Since we don't know exact per-thread distribution until after chunking,
  // we conservatively assume all separators could end up in one thread's chunk.
  // However, this is still much better than allocating based on file size.
  size_t allocation_size;
  if (n_threads == 1) {
    // Single-threaded: simple allocation with padding
    allocation_size = total_separators + 8;
  } else {
    // Multi-threaded: contiguous per-thread regions
    // Allocation: region_size * n_threads
    allocation_size = out.region_size * n_threads;
  }

  // Allocate indexes array with RAII ownership
  out.indexes_ptr_ = std::make_unique<uint64_t[]>(allocation_size);
  out.indexes = out.indexes_ptr_.get();

  return out;
}

ParseIndex TwoPass::init_counted_safe(uint64_t total_separators, size_t n_threads,
                                      ErrorCollector* errors, uint64_t n_quotes, size_t len) {
  ParseIndex out;
  // Ensure at least 1 thread for valid memory allocation
  if (n_threads == 0)
    n_threads = 1;
  out.n_threads = n_threads;

  // Calculate allocation size with overflow checking
  // See init_counted for explanation of the allocation strategy
  //
  // SECURITY FIX: When there are quote characters in the file, the first pass
  // separator count may be too low due to error recovery behavior differences.
  //
  // The first pass uses SIMD quote masking: a quote toggles "inside quote" state,
  // and separators inside quotes are not counted. But the second pass uses a
  // state machine that handles error recovery differently. For example:
  //
  //   Input: a"b,c,d\n
  //   First pass: Quote toggles state, so ",c,d\n" is "inside quotes" = 0 seps
  //   Second pass: "quote in unquoted field" error, stays UNQUOTED_FIELD, so
  //                ",c,d\n" counts as 3 separators
  //
  // When n_quotes is odd (unpaired), ALL separators after the last unpaired quote
  // could be missed by the first pass but found by the second pass.
  //
  // Solution: When quotes are present, use the file length as an upper bound,
  // since the maximum possible separators in a file equals its length (every
  // byte could be a separator in the pathological case).
  uint64_t safe_separators = total_separators;
  if (n_quotes > 0 && len > 0) {
    // With quotes present and file length known, use max(counted, len) as bound
    safe_separators = std::max(total_separators, static_cast<uint64_t>(len));
  } else if (n_quotes > 0) {
    // Quotes but no len provided - use conservative 2x multiplier
    safe_separators = total_separators * 2 + n_quotes;
  }

  // Set region size for contiguous per-thread storage
  out.region_size = safe_separators + 8;

  size_t allocation_size;
  bool overflow = false;

  if (n_threads == 1) {
    // Single-threaded: allocation_size = safe_separators + 8
    if (safe_separators > std::numeric_limits<size_t>::max() - 8) {
      overflow = true;
    } else {
      allocation_size = static_cast<size_t>(safe_separators) + 8;
    }
  } else {
    // Multi-threaded: contiguous per-thread regions
    // region_size * n_threads
    size_t sep_plus_8;
    if (safe_separators > std::numeric_limits<size_t>::max() - 8) {
      overflow = true;
    } else {
      sep_plus_8 = static_cast<size_t>(safe_separators) + 8;
      if (sep_plus_8 > std::numeric_limits<size_t>::max() / n_threads) {
        overflow = true;
      } else {
        allocation_size = sep_plus_8 * n_threads;
      }
    }
  }

  // Check final allocation: allocation_size * sizeof(uint64_t)
  if (!overflow && allocation_size > std::numeric_limits<size_t>::max() / sizeof(uint64_t)) {
    overflow = true;
  }

  if (overflow) {
    std::string msg =
        "Index allocation would overflow: total_separators=" + std::to_string(total_separators) +
        ", n_threads=" + std::to_string(n_threads);
    if (errors != nullptr) {
      errors->add_error(ErrorCode::INDEX_ALLOCATION_OVERFLOW, ErrorSeverity::FATAL, 1, 1, 0, msg);
      // Return empty index to signal failure
      return out;
    } else {
      throw std::runtime_error(msg);
    }
  }

  // Safe to allocate - use RAII to ensure proper cleanup
  out.n_indexes_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.n_indexes = out.n_indexes_ptr_.get();

  // Allocate chunk_starts array with RAII ownership
  out.chunk_starts_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.chunk_starts = out.chunk_starts_ptr_.get();
  for (size_t i = 0; i < n_threads; ++i) {
    out.chunk_starts[i] = 0;
  }

  out.indexes_ptr_ = std::make_unique<uint64_t[]>(allocation_size);
  out.indexes = out.indexes_ptr_.get();

  return out;
}

ParseIndex TwoPass::init_counted_per_thread(const std::vector<uint64_t>& thread_separator_counts,
                                            size_t n_threads, size_t padding_per_thread) {
  ParseIndex out;
  // Ensure at least 1 thread for valid memory allocation
  if (n_threads == 0)
    n_threads = 1;

  // Validate vector size matches n_threads
  if (thread_separator_counts.size() != n_threads) {
    throw std::runtime_error("thread_separator_counts size (" +
                             std::to_string(thread_separator_counts.size()) +
                             ") must match n_threads (" + std::to_string(n_threads) + ")");
  }

  out.n_threads = n_threads;

  // Allocate n_indexes array with RAII ownership
  out.n_indexes_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.n_indexes = out.n_indexes_ptr_.get();

  // Allocate chunk_starts array with RAII ownership
  out.chunk_starts_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.chunk_starts = out.chunk_starts_ptr_.get();
  for (size_t i = 0; i < n_threads; ++i) {
    out.chunk_starts[i] = 0;
  }

  // Allocate region_offsets array with RAII ownership
  out.region_offsets_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.region_offsets = out.region_offsets_ptr_.get();

  // Calculate total allocation and per-thread offsets
  // Each thread gets: its separator count + padding
  uint64_t total_allocation = 0;
  for (size_t t = 0; t < n_threads; ++t) {
    out.region_offsets[t] = total_allocation;
    total_allocation += thread_separator_counts[t] + padding_per_thread;
  }

  // Set region_size to 0 to indicate per-thread variable sizing
  // (readers should use region_offsets instead)
  out.region_size = 0;

  // Allocate indexes array with RAII ownership
  out.indexes_ptr_ = std::make_unique<uint64_t[]>(static_cast<size_t>(total_allocation));
  out.indexes = out.indexes_ptr_.get();

  return out;
}

ParseIndex
TwoPass::init_counted_per_thread_safe(const std::vector<uint64_t>& thread_separator_counts,
                                      size_t n_threads, ErrorCollector* errors,
                                      size_t padding_per_thread) {
  ParseIndex out;
  // Ensure at least 1 thread for valid memory allocation
  if (n_threads == 0)
    n_threads = 1;

  // Validate vector size matches n_threads
  if (thread_separator_counts.size() != n_threads) {
    std::string msg = "thread_separator_counts size (" +
                      std::to_string(thread_separator_counts.size()) + ") must match n_threads (" +
                      std::to_string(n_threads) + ")";
    if (errors != nullptr) {
      errors->add_error(ErrorCode::INTERNAL_ERROR, ErrorSeverity::FATAL, 1, 1, 0, msg);
      return out;
    } else {
      throw std::runtime_error(msg);
    }
  }

  out.n_threads = n_threads;

  // Calculate total allocation with overflow checking
  uint64_t total_allocation = 0;
  bool overflow = false;

  for (size_t t = 0; t < n_threads && !overflow; ++t) {
    uint64_t count = thread_separator_counts[t];
    // Check count + padding overflow
    if (count > std::numeric_limits<uint64_t>::max() - padding_per_thread) {
      overflow = true;
      break;
    }
    uint64_t padded_count = count + padding_per_thread;

    // Check accumulation overflow
    if (total_allocation > std::numeric_limits<uint64_t>::max() - padded_count) {
      overflow = true;
      break;
    }
    total_allocation += padded_count;
  }

  // Check final allocation: total_allocation * sizeof(uint64_t)
  if (!overflow && total_allocation > std::numeric_limits<size_t>::max() / sizeof(uint64_t)) {
    overflow = true;
  }

  if (overflow) {
    std::string msg = "Index allocation would overflow: n_threads=" + std::to_string(n_threads);
    if (errors != nullptr) {
      errors->add_error(ErrorCode::INDEX_ALLOCATION_OVERFLOW, ErrorSeverity::FATAL, 1, 1, 0, msg);
      return out;
    } else {
      throw std::runtime_error(msg);
    }
  }

  // Safe to allocate - use RAII to ensure proper cleanup
  out.n_indexes_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.n_indexes = out.n_indexes_ptr_.get();

  // Allocate chunk_starts array with RAII ownership
  out.chunk_starts_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.chunk_starts = out.chunk_starts_ptr_.get();
  for (size_t i = 0; i < n_threads; ++i) {
    out.chunk_starts[i] = 0;
  }

  // Allocate region_offsets array with RAII ownership
  out.region_offsets_ptr_ = std::make_unique<uint64_t[]>(n_threads);
  out.region_offsets = out.region_offsets_ptr_.get();

  // Calculate per-thread offsets
  uint64_t offset = 0;
  for (size_t t = 0; t < n_threads; ++t) {
    out.region_offsets[t] = offset;
    offset += thread_separator_counts[t] + padding_per_thread;
  }

  // Set region_size to 0 to indicate per-thread variable sizing
  out.region_size = 0;

  // Allocate indexes array with RAII ownership
  out.indexes_ptr_ = std::make_unique<uint64_t[]>(static_cast<size_t>(total_allocation));
  out.indexes = out.indexes_ptr_.get();

  return out;
}

//-----------------------------------------------------------------------------
// ParseIndex shared ownership implementation
//-----------------------------------------------------------------------------

std::shared_ptr<const ParseIndex> ParseIndex::share() {
  // Create a new ParseIndex that shares ownership of the underlying data
  auto shared = std::make_shared<ParseIndex>();

  // Copy scalar members
  shared->columns = columns;
  shared->n_threads = n_threads;
  shared->region_size = region_size;
  shared->flat_indexes_count = flat_indexes_count;
  shared->col_indexes_count = col_indexes_count;

  // Share the buffer reference (if set)
  shared->buffer_ = buffer_;

  // Handle different ownership modes
  if (mmap_buffer_) {
    // Convert unique_ptr mmap to shared_ptr
    if (!mmap_buffer_shared_) {
      mmap_buffer_shared_ = std::move(mmap_buffer_);
    }
    shared->mmap_buffer_shared_ = mmap_buffer_shared_;
    // n_indexes, indexes, and flat_indexes already point into mmap'd memory
    shared->n_indexes = n_indexes;
    shared->indexes = indexes;
    shared->flat_indexes = flat_indexes;
    // col_indexes is separately allocated, handle it
    if (col_indexes_ptr_) {
      col_indexes_shared_ = std::shared_ptr<uint64_t[]>(col_indexes_ptr_.release());
    }
    shared->col_indexes_shared_ = col_indexes_shared_;
    shared->col_indexes = col_indexes;
  } else if (n_indexes_shared_ || indexes_shared_) {
    // Already using shared ownership - just copy the shared_ptrs
    // But check if flat_indexes_ptr_ was created after the first share() call
    // (e.g., compact() called after share()) - convert it to shared if needed
    if (flat_indexes_ptr_) {
      flat_indexes_shared_ = std::shared_ptr<uint64_t[]>(flat_indexes_ptr_.release());
    }
    if (col_indexes_ptr_) {
      col_indexes_shared_ = std::shared_ptr<uint64_t[]>(col_indexes_ptr_.release());
    }
    shared->n_indexes_shared_ = n_indexes_shared_;
    shared->indexes_shared_ = indexes_shared_;
    shared->flat_indexes_shared_ = flat_indexes_shared_;
    shared->col_indexes_shared_ = col_indexes_shared_;
    shared->n_indexes = n_indexes;
    shared->indexes = indexes;
    shared->flat_indexes = flat_indexes;
    shared->col_indexes = col_indexes;
  } else if (n_indexes_ptr_ || indexes_ptr_) {
    // Convert unique_ptr to shared_ptr for sharing
    if (n_indexes_ptr_) {
      n_indexes_shared_ = std::shared_ptr<uint64_t[]>(n_indexes_ptr_.release());
    }
    if (indexes_ptr_) {
      indexes_shared_ = std::shared_ptr<uint64_t[]>(indexes_ptr_.release());
    }
    if (flat_indexes_ptr_) {
      flat_indexes_shared_ = std::shared_ptr<uint64_t[]>(flat_indexes_ptr_.release());
    }
    if (col_indexes_ptr_) {
      col_indexes_shared_ = std::shared_ptr<uint64_t[]>(col_indexes_ptr_.release());
    }
    shared->n_indexes_shared_ = n_indexes_shared_;
    shared->indexes_shared_ = indexes_shared_;
    shared->flat_indexes_shared_ = flat_indexes_shared_;
    shared->col_indexes_shared_ = col_indexes_shared_;
    shared->n_indexes = n_indexes;
    shared->indexes = indexes;
    shared->flat_indexes = flat_indexes;
    shared->col_indexes = col_indexes;
  } else {
    // No data to share - empty index
    shared->n_indexes = nullptr;
    shared->indexes = nullptr;
    shared->flat_indexes = nullptr;
    shared->col_indexes = nullptr;
  }

  return shared;
}

} // namespace libvroom
