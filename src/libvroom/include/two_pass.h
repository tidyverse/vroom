#ifndef TWO_PASS_H
#define TWO_PASS_H

/**
 * @file two_pass.h
 * @brief Internal implementation of the high-performance CSV parser.
 *
 * @warning **Do not include this header directly.** Use `#include "libvroom.h"`
 *          and the `Parser` class instead. This header contains internal
 *          implementation details that may change without notice.
 *
 * This header provides the core parsing functionality of the libvroom library.
 * The parser uses a speculative multi-threaded two-pass algorithm based on
 * research by Chang et al. (SIGMOD 2019) combined with SIMD techniques from
 * Langdale & Lemire (simdjson).
 *
 * ## Recommended Usage
 *
 * ```cpp
 * #include "libvroom.h"
 *
 * libvroom::Parser parser(num_threads);
 *
 * // Auto-detect dialect, throw on errors (simplest)
 * auto result = parser.parse(buf, len);
 *
 * // With error collection
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 * auto result = parser.parse(buf, len, {.errors = &errors});
 *
 * // Explicit dialect
 * auto result = parser.parse(buf, len, {.dialect = libvroom::Dialect::tsv()});
 * ```
 *
 * ## Algorithm Overview
 *
 * 1. **First Pass**: Scans for line boundaries while tracking quote parity.
 *    Finds safe split points where the file can be divided for parallel
 * processing.
 *
 * 2. **Speculative Chunking**: The file is divided into chunks based on quote
 *    parity analysis. Multiple threads can speculatively parse chunks.
 *
 * 3. **Second Pass**: SIMD-based field indexing using a state machine.
 * Processes 64 bytes at a time using Google Highway portable SIMD intrinsics.
 *
 * @see libvroom::Parser for the unified public API
 * @see libvroom::ParseOptions for configuration options
 * @see libvroom::ParseIndex for the result structure containing field positions
 */

#include "branchless_state_machine.h"
#include "dialect.h"
#include "error.h"
#include "mmap_util.h"
#include "simd_highway.h"

#include <cassert>
#include <cstdint>
#include <cstring> // for memcpy
#include <functional>
#include <future>
#include <limits>
#include <memory>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace libvroom {

/**
 * @brief Progress callback for second-pass field indexing.
 *
 * Called periodically during parsing to report progress. The callback receives
 * the number of bytes just processed. Return true to continue, false to cancel.
 *
 * This is used internally by the TwoPass parser to report chunk completion
 * to the ProgressTracker in libvroom.h.
 */
using SecondPassProgressCallback = std::function<bool(size_t bytes_processed)>;

/// Sentinel value indicating an invalid or unset position.
constexpr static uint64_t null_pos = std::numeric_limits<uint64_t>::max();

/**
 * @brief Represents a field's byte boundaries in the source buffer.
 *
 * FieldSpan provides the byte range for a single CSV field, enabling
 * efficient value extraction without re-parsing the entire file.
 *
 * @note The `start` offset points to the first byte of the field content.
 *       The `end` offset points to the delimiter/newline byte (exclusive),
 *       so the field content is buf[start..end).
 *
 * @example
 * @code
 * // For CSV: "hello,world\n"
 * //          ^     ^
 * //          0     6
 * // Field 0: FieldSpan{0, 5}   -> "hello"
 * // Field 1: FieldSpan{6, 11}  -> "world"
 * @endcode
 */
struct FieldSpan {
  uint64_t start; ///< Byte offset of field start (inclusive)
  uint64_t end;   ///< Byte offset of field end (exclusive, at delimiter/newline)

  /// Default constructor, creates an invalid span.
  FieldSpan() : start(null_pos), end(null_pos) {}

  /// Construct with explicit start and end positions.
  FieldSpan(uint64_t start, uint64_t end) : start(start), end(end) {}

  /// Check if this span is valid.
  bool is_valid() const { return start != null_pos && end != null_pos; }

  /// Get the length of the field in bytes.
  uint64_t length() const { return is_valid() ? end - start : 0; }
};

/**
 * @brief Read-only view into a contiguous array of uint64_t values.
 *
 * This is a lightweight, non-owning view similar to std::span<const uint64_t>
 * but available in C++17. Used to provide O(1) access to per-thread index
 * regions without copying.
 *
 * @note This view does not own the data it points to. The caller must ensure
 *       the underlying data remains valid for the lifetime of the view.
 */
struct IndexView {
  const uint64_t* data_; ///< Pointer to the first element
  size_t size_;          ///< Number of elements

  /// Default constructor, creates an empty view.
  IndexView() : data_(nullptr), size_(0) {}

  /// Construct a view over [data, data + size).
  IndexView(const uint64_t* data, size_t size) : data_(data), size_(size) {}

  /// Get pointer to the first element.
  const uint64_t* data() const { return data_; }

  /// Get the number of elements.
  size_t size() const { return size_; }

  /// Check if the view is empty.
  bool empty() const { return size_ == 0; }

  /// Access element at index i (no bounds checking).
  const uint64_t& operator[](size_t i) const { return data_[i]; }

  /// Iterator support for range-based for loops.
  const uint64_t* begin() const { return data_; }
  const uint64_t* end() const { return data_ + size_; }
};

/**
 * @brief Result structure containing parsed CSV field positions.
 *
 * The ParseIndex class stores the byte offsets of field separators (commas and
 * newlines) found during CSV parsing. These positions enable efficient random
 * access to individual fields without re-parsing the entire file.
 *
 * When using multi-threaded parsing, field positions are stored in contiguous
 * per-thread regions to avoid false sharing. Each thread writes to its own
 * region: indexes[thread_id * region_size ... thread_id * region_size + count - 1].
 * Use n_indexes[thread_id] to get the count for each thread.
 *
 * @note This class is move-only. Copy operations are deleted to prevent
 * accidental expensive copies of large index arrays.
 *
 * @note Memory management uses std::unique_ptr for automatic RAII cleanup. The
 *       raw pointer accessors (n_indexes, indexes) are provided for
 * compatibility with existing parsing code but ownership remains with the
 * unique_ptr members.
 *
 * @warning The caller must ensure the index remains valid while accessing the
 *          underlying buffer data. The index stores byte offsets, not the data
 * itself.
 *
 * @example
 * @code
 * // Create parser and initialize index
 * libvroom::Parser parser(num_threads);
 * auto result = parser.parse(buffer, length);
 *
 * // Access field positions from result.idx
 * // For single-threaded: positions are at result.idx.indexes[0..n_indexes[0]-1]
 * // For multi-threaded: thread t's positions are at
 * //   indexes[t * region_size .. t * region_size + n_indexes[t] - 1]
 * @endcode
 */
class ParseIndex {
public:
  /// Number of columns detected in the CSV (set after parsing header).
  uint64_t columns{0};

  /// Number of threads used for parsing. Determines the interleave stride.
  /// Changed from uint8_t to uint16_t to support systems with >255 cores.
  uint16_t n_threads{0};

  /// Size of each thread's contiguous index region. Used for per-thread
  /// storage to avoid false sharing. Each thread writes to:
  ///   indexes[thread_id * region_size .. thread_id * region_size + n_indexes[thread_id] - 1]
  uint64_t region_size{0};

  /// Array of size n_threads containing the count of indexes found by each
  /// thread.
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by n_indexes_ptr_.
  uint64_t* n_indexes{nullptr};

  /// Array of field separator positions (byte offsets). Stored in contiguous
  /// per-thread regions: thread t's data is at indexes[t * region_size].
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by indexes_ptr_.
  uint64_t* indexes{nullptr};

  /// Array of size n_threads containing the starting byte offset of each
  /// thread's chunk in the source file. Used for computing field start
  /// positions across thread boundaries.
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by chunk_starts_ptr_.
  uint64_t* chunk_starts{nullptr};

  /// Array of size n_threads containing the starting offset within the indexes
  /// array for each thread's region. Used for right-sized per-thread allocation
  /// where each thread gets a region sized for its actual separator count.
  /// When nullptr, uniform region_size is used (thread t starts at t * region_size).
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by region_offsets_ptr_.
  uint64_t* region_offsets{nullptr};

  /// Flat index array containing all separator positions in file order.
  /// When populated (via compact()), enables O(1) field access instead of
  /// O(n_threads) iteration. Each entry stores the byte position of a field
  /// separator (delimiter or newline).
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by flat_indexes_ptr_.
  uint64_t* flat_indexes{nullptr};

  /// Total number of fields in the flat index (sum of all n_indexes[]).
  /// Set when flat_indexes is populated.
  uint64_t flat_indexes_count{0};

  /// Column-major index array for efficient column-oriented access (ALTREP, Arrow).
  /// When populated (via compact_column_major()), enables O(1) column access:
  /// col_indexes[col * num_rows() + row] gives the byte position of field (row, col).
  /// @note This is a raw pointer accessor; memory is managed by col_indexes_ptr_.
  uint64_t* col_indexes{nullptr};

  /// Total number of fields in the column-major index.
  /// Should equal flat_indexes_count when both are populated.
  uint64_t col_indexes_count{0};

  /// Default constructor. Creates an empty, uninitialized index.
  ParseIndex() = default;

  /**
   * @brief Move constructor.
   *
   * Transfers ownership of index arrays from another ParseIndex object.
   *
   * @param other The ParseIndex to move from. Will be left in a valid but empty
   * state.
   */
  ParseIndex(ParseIndex&& other) noexcept
      : columns(other.columns), n_threads(other.n_threads), region_size(other.region_size),
        n_indexes(other.n_indexes), indexes(other.indexes), chunk_starts(other.chunk_starts),
        region_offsets(other.region_offsets), flat_indexes(other.flat_indexes),
        flat_indexes_count(other.flat_indexes_count), col_indexes(other.col_indexes),
        col_indexes_count(other.col_indexes_count), n_indexes_ptr_(std::move(other.n_indexes_ptr_)),
        indexes_ptr_(std::move(other.indexes_ptr_)),
        chunk_starts_ptr_(std::move(other.chunk_starts_ptr_)),
        region_offsets_ptr_(std::move(other.region_offsets_ptr_)),
        flat_indexes_ptr_(std::move(other.flat_indexes_ptr_)),
        col_indexes_ptr_(std::move(other.col_indexes_ptr_)),
        mmap_buffer_(std::move(other.mmap_buffer_)), buffer_(std::move(other.buffer_)),
        n_indexes_shared_(std::move(other.n_indexes_shared_)),
        indexes_shared_(std::move(other.indexes_shared_)),
        flat_indexes_shared_(std::move(other.flat_indexes_shared_)),
        col_indexes_shared_(std::move(other.col_indexes_shared_)),
        mmap_buffer_shared_(std::move(other.mmap_buffer_shared_)) {
    other.n_indexes = nullptr;
    other.indexes = nullptr;
    other.chunk_starts = nullptr;
    other.region_offsets = nullptr;
    other.flat_indexes = nullptr;
    other.flat_indexes_count = 0;
    other.col_indexes = nullptr;
    other.col_indexes_count = 0;
  }

  /**
   * @brief Move assignment operator.
   *
   * Releases current resources and takes ownership from another ParseIndex.
   *
   * @param other The ParseIndex to move from. Will be left in a valid but empty
   * state.
   * @return Reference to this ParseIndex.
   */
  ParseIndex& operator=(ParseIndex&& other) noexcept {
    if (this != &other) {
      columns = other.columns;
      n_threads = other.n_threads;
      region_size = other.region_size;
      n_indexes = other.n_indexes;
      indexes = other.indexes;
      chunk_starts = other.chunk_starts;
      region_offsets = other.region_offsets;
      flat_indexes = other.flat_indexes;
      flat_indexes_count = other.flat_indexes_count;
      col_indexes = other.col_indexes;
      col_indexes_count = other.col_indexes_count;
      n_indexes_ptr_ = std::move(other.n_indexes_ptr_);
      indexes_ptr_ = std::move(other.indexes_ptr_);
      chunk_starts_ptr_ = std::move(other.chunk_starts_ptr_);
      region_offsets_ptr_ = std::move(other.region_offsets_ptr_);
      flat_indexes_ptr_ = std::move(other.flat_indexes_ptr_);
      col_indexes_ptr_ = std::move(other.col_indexes_ptr_);
      mmap_buffer_ = std::move(other.mmap_buffer_);
      buffer_ = std::move(other.buffer_);
      n_indexes_shared_ = std::move(other.n_indexes_shared_);
      indexes_shared_ = std::move(other.indexes_shared_);
      flat_indexes_shared_ = std::move(other.flat_indexes_shared_);
      col_indexes_shared_ = std::move(other.col_indexes_shared_);
      mmap_buffer_shared_ = std::move(other.mmap_buffer_shared_);
      other.n_indexes = nullptr;
      other.indexes = nullptr;
      other.chunk_starts = nullptr;
      other.region_offsets = nullptr;
      other.flat_indexes = nullptr;
      other.flat_indexes_count = 0;
      other.col_indexes = nullptr;
      other.col_indexes_count = 0;
    }
    return *this;
  }

  // Delete copy operations to prevent accidental copies
  ParseIndex(const ParseIndex&) = delete;
  ParseIndex& operator=(const ParseIndex&) = delete;

  /**
   * @brief Serialize the index to a binary file (v2 format).
   *
   * Writes the index structure to disk for later retrieval, avoiding the need
   * to re-parse large CSV files.
   *
   * @param filename Path to the output file.
   * @throws std::runtime_error If writing fails.
   */
  void write(const std::string& filename);

  /**
   * @brief Serialize the index to a binary file (v3 format with source metadata).
   *
   * Writes the index structure with source file metadata (mtime, size) for
   * cache validation. This format supports mmap-based loading.
   *
   * @param filename Path to the output file.
   * @param source_meta Metadata of the source CSV file for cache validation.
   * @throws std::runtime_error If writing fails.
   */
  void write(const std::string& filename, const SourceMetadata& source_meta);

  /**
   * @brief Deserialize the index from a binary file.
   *
   * Reads a previously saved index structure from disk.
   *
   * @param filename Path to the input file.
   * @throws std::runtime_error If reading fails.
   *
   * @warning The n_indexes and indexes arrays must be pre-allocated before
   * calling.
   */
  void read(const std::string& filename);

  /**
   * @brief Load index from a cache file using memory-mapping.
   *
   * This factory method creates a ParseIndex that directly references data
   * in a memory-mapped file, avoiding any copying. The returned ParseIndex
   * owns the memory mapping and will release it on destruction.
   *
   * @param cache_path Path to the cache file (.vidx format).
   * @param source_meta Metadata of the source CSV file for validation.
   * @return ParseIndex with mmap-backed data, or empty ParseIndex if
   *         the cache is invalid or cannot be loaded.
   *
   * @note If the cache file is missing, invalid, or the source file has changed,
   *       an empty ParseIndex is returned (check with is_valid()).
   */
  static ParseIndex from_mmap(const std::string& cache_path, const SourceMetadata& source_meta);

  /**
   * @brief Check if this index contains valid data.
   *
   * @return true if the index has been populated (either by parsing or loading).
   */
  bool is_valid() const { return n_indexes != nullptr && indexes != nullptr; }

  /**
   * @brief Check if this index is backed by memory-mapped data.
   *
   * @return true if the index data is memory-mapped (from from_mmap()).
   */
  bool is_mmap_backed() const { return mmap_buffer_ != nullptr; }

  /**
   * @brief Check if this index has a flat index for O(1) field access.
   *
   * @return true if the flat index has been built (via compact()).
   */
  bool is_flat() const { return flat_indexes != nullptr && flat_indexes_count > 0; }

  /**
   * @brief Compact the per-thread index regions into a flat array for O(1) access.
   *
   * After parsing, field separators are stored in per-thread regions which
   * require O(n_threads) iteration to find a specific field. This method
   * consolidates all separators into a single flat array sorted by file order,
   * enabling O(1) random access via `flat_indexes[row * columns + col]`.
   *
   * Memory usage: 8 bytes per field separator (same as before, just reorganized).
   *
   * @note This method is idempotent - calling it multiple times has no effect
   *       after the first successful call.
   *
   * @note The original per-thread indexes are retained for backward compatibility
   *       and serialization. The flat index is a derived view.
   *
   * @example
   * @code
   * auto result = parser.parse(buf, len);
   *
   * // Before compact(): O(n_threads) per field access
   * FieldSpan slow = result.idx.get_field_span(1000);
   *
   * // After compact(): O(1) per field access
   * result.idx.compact();
   * FieldSpan fast = result.idx.get_field_span(1000);
   * @endcode
   */
  void compact();

  /**
   * @brief Compact and transpose to column-major layout for ALTREP/Arrow access.
   *
   * Consolidates per-thread separator positions into a column-major array
   * optimized for column-at-a-time access patterns (R ALTREP, Arrow conversion).
   *
   * Layout: col_indexes[col * num_rows() + row] = byte position of field (row, col)
   *
   * This method uses multi-threaded row-first transpose for optimal cache
   * performance based on benchmarking (see issue #599).
   *
   * Memory usage: 8 bytes per field separator (same as compact(), different layout).
   *
   * @param n_threads Number of threads for parallel transpose (0 = use parsing threads).
   *
   * @note This method is idempotent - calling it multiple times has no effect
   *       after the first successful call.
   *
   * @note Unlike compact(), this method does NOT preserve the row-major flat_indexes.
   *       Use compact() if you need row-major access, or compact_column_major() for
   *       column-major access. The two layouts use the same memory (not additive).
   *
   * @example
   * @code
   * auto result = parser.parse(buf, len);
   * result.idx.compact_column_major();  // Enable O(1) column access
   *
   * // Get column 5's data for all rows
   * const uint64_t* col5 = result.idx.column(5);
   * for (size_t row = 0; row < result.idx.num_rows(); ++row) {
   *   uint64_t field_end = col5[row];
   *   // ...
   * }
   * @endcode
   */
  void compact_column_major(size_t n_threads = 0);

  /**
   * @brief Check if column-major index is available.
   *
   * @return true if compact_column_major() has been called successfully.
   */
  bool is_column_major() const { return col_indexes != nullptr && col_indexes_count > 0; }

  /**
   * @brief Get the number of rows in the parsed CSV.
   *
   * @return Number of rows (total_indexes / columns), or 0 if columns is 0.
   */
  uint64_t num_rows() const {
    if (columns == 0)
      return 0;
    return total_indexes() / columns;
  }

  /**
   * @brief Get O(1) access to a column's field positions.
   *
   * Returns a pointer to the start of the column's data in the column-major index.
   * The returned pointer is valid for num_rows() consecutive uint64_t values.
   *
   * @param col Column index (0 to columns - 1).
   * @return Pointer to column data, or nullptr if column-major index not available
   *         or col is out of bounds.
   *
   * @note Requires compact_column_major() to have been called first.
   *
   * @example
   * @code
   * result.idx.compact_column_major();
   * const uint64_t* col0 = result.idx.column(0);
   * for (size_t r = 0; r < result.idx.num_rows(); ++r) {
   *   uint64_t end_pos = col0[r];
   *   uint64_t start_pos = (r == 0 && 0 == 0) ? 0 : col0[r-1] + 1;
   *   // Field bytes are at buf[start_pos..end_pos)
   * }
   * @endcode
   */
  const uint64_t* column(size_t col) const {
    if (!is_column_major() || col >= columns)
      return nullptr;
    return &col_indexes[col * num_rows()];
  }

  /**
   * @brief Get field positions for a single row (O(cols) operation).
   *
   * Extracts field positions for all columns in a row from the column-major index.
   * This is an O(columns) operation with strided memory access, suitable for
   * occasional row access (CLI head/tail, type detection) but not for bulk row
   * iteration.
   *
   * @param row Row index (0 to num_rows() - 1).
   * @param[out] out Vector to receive field positions. Will be resized to columns.
   * @return true if successful, false if row is out of bounds or index not available.
   *
   * @note Requires compact_column_major() to have been called first.
   *
   * @example
   * @code
   * result.idx.compact_column_major();
   * std::vector<uint64_t> row_fields;
   * if (result.idx.get_row_fields(0, row_fields)) {
   *   for (size_t col = 0; col < row_fields.size(); ++col) {
   *     // row_fields[col] is the end position of field (0, col)
   *   }
   * }
   * @endcode
   */
  bool get_row_fields(size_t row, std::vector<uint64_t>& out) const {
    if (!is_column_major() || row >= num_rows())
      return false;
    out.resize(columns);
    uint64_t nrows = num_rows();
    for (size_t col = 0; col < columns; ++col) {
      out[col] = col_indexes[col * nrows + row];
    }
    return true;
  }

  /**
   * @brief Get O(1) read-only access to a thread's index region.
   *
   * Returns a view into the contiguous region of field separator positions
   * written by the specified thread. Each thread's indexes are in sorted
   * order within that thread's region (file order within its chunk).
   *
   * @param t Thread ID (0 to n_threads - 1).
   * @return IndexView of the thread's field separator positions.
   *
   * @note For region_size == 0 (contiguous/deserialized layout), this computes
   *       the offset by summing n_indexes[0..t-1].
   *
   * @example
   * @code
   * for (uint16_t t = 0; t < idx.n_threads; ++t) {
   *   auto view = idx.thread_data(t);
   *   for (size_t i = 0; i < view.size(); ++i) {
   *     std::cout << "Thread " << t << " separator at " << view[i] << "\n";
   *   }
   * }
   * @endcode
   */
  IndexView thread_data(uint16_t t) const {
    if (t >= n_threads || indexes == nullptr || n_indexes == nullptr) {
      return IndexView(); // Empty view
    }
    if (region_offsets != nullptr) {
      // Right-sized per-thread regions: O(1) access via offset array
      return IndexView(indexes + region_offsets[t], n_indexes[t]);
    } else if (region_size > 0) {
      // Uniform per-thread regions: direct O(1) access
      return IndexView(indexes + t * region_size, n_indexes[t]);
    } else {
      // Contiguous layout (deserialized): compute offset by summing prior counts
      size_t offset = 0;
      for (uint16_t i = 0; i < t; ++i) {
        offset += n_indexes[i];
      }
      return IndexView(indexes + offset, n_indexes[t]);
    }
  }

  /**
   * @brief Get total number of field separators across all threads.
   *
   * @return Sum of n_indexes[0..n_threads-1].
   */
  uint64_t total_indexes() const {
    if (n_indexes == nullptr || n_threads == 0)
      return 0;
    uint64_t total = 0;
    for (uint16_t t = 0; t < n_threads; ++t) {
      total += n_indexes[t];
    }
    return total;
  }

  /**
   * @brief Get field span by global field index without sorting.
   *
   * Iterates through threads in file order to find the field at the given
   * global index. This is O(n_threads) in the worst case but avoids the
   * O(n log n) sorting required by ValueExtractor.
   *
   * @param global_field_idx Zero-based index of the field across the entire file.
   * @return FieldSpan with byte boundaries, or invalid span if out of bounds.
   *
   * @note For the first field (global_field_idx == 0), the start position is
   *       always 0 (beginning of file).
   *
   * @example
   * @code
   * // Get the 100th field in the file
   * FieldSpan span = idx.get_field_span(100);
   * if (span.is_valid()) {
   *   std::string_view field(buf + span.start, span.length());
   * }
   * @endcode
   */
  FieldSpan get_field_span(uint64_t global_field_idx) const;

  /**
   * @brief Get field span by row and column without sorting.
   *
   * Converts (row, col) to a global field index and delegates to the
   * global field index overload. The number of columns must be set
   * (idx.columns > 0) for this method to work.
   *
   * @param row Zero-based row index.
   * @param col Zero-based column index.
   * @return FieldSpan with byte boundaries, or invalid span if out of bounds.
   *
   * @note Row 0 is the first data row (or header if has_header is false).
   *
   * @example
   * @code
   * // Get the field at row 5, column 2
   * FieldSpan span = idx.get_field_span(5, 2);
   * if (span.is_valid()) {
   *   std::string_view field(buf + span.start, span.length());
   * }
   * @endcode
   */
  FieldSpan get_field_span(uint64_t row, uint64_t col) const;

  /**
   * @brief Destructor. Releases allocated index arrays via RAII.
   *
   * Memory is automatically freed when the unique_ptr members are destroyed.
   * For mmap-backed indexes, the memory mapping is released instead.
   */
  ~ParseIndex() = default;

  void fill_double_array(ParseIndex* idx, uint64_t column, double* out) {}

  // =========================================================================
  // Shared ownership API for buffer lifetime safety
  // =========================================================================

  /**
   * @brief Set the shared buffer reference.
   *
   * Associates this ParseIndex with a shared buffer. This enables safe sharing
   * of the underlying CSV data buffer between multiple consumers (e.g.,
   * ValueExtractor, lazy columns) that may outlive the original ParseIndex.
   *
   * @param buffer Shared pointer to the CSV data buffer.
   *
   * @note The buffer should contain the same data that was used during parsing.
   *       The ParseIndex stores byte offsets into this buffer.
   *
   * @example
   * @code
   * // Create a shared buffer
   * auto buffer = std::make_shared<std::vector<uint8_t>>(data, data + len);
   *
   * // Parse and associate the buffer
   * auto result = parser.parse(buffer->data(), buffer->size());
   * result.idx.set_buffer(buffer);
   *
   * // Now ValueExtractor and lazy columns can safely share the buffer
   * @endcode
   */
  void set_buffer(std::shared_ptr<const std::vector<uint8_t>> buffer) {
    buffer_ = std::move(buffer);
  }

  /**
   * @brief Get the shared buffer reference.
   *
   * @return Shared pointer to the CSV data buffer, or nullptr if not set.
   */
  std::shared_ptr<const std::vector<uint8_t>> buffer() const { return buffer_; }

  /**
   * @brief Check if this index has a shared buffer reference.
   *
   * @return true if a shared buffer has been set via set_buffer().
   */
  bool has_buffer() const { return buffer_ != nullptr; }

  /**
   * @brief Get a pointer to the buffer data.
   *
   * Returns a pointer to the underlying buffer data if a shared buffer is set.
   * This is a convenience method for accessing the data without manually
   * checking has_buffer() and dereferencing.
   *
   * @return Pointer to the buffer data, or nullptr if no buffer is set.
   */
  const uint8_t* buffer_data() const { return buffer_ ? buffer_->data() : nullptr; }

  /**
   * @brief Get the size of the buffer.
   *
   * @return Size of the buffer in bytes, or 0 if no buffer is set.
   */
  size_t buffer_size() const { return buffer_ ? buffer_->size() : 0; }

  /**
   * @brief Create a shared reference to this ParseIndex.
   *
   * This factory method creates a shared_ptr that shares ownership of this
   * ParseIndex's internal data. Multiple shared ParseIndex instances can
   * coexist, and the underlying data is freed only when all references are
   * released.
   *
   * This is the recommended way to share ParseIndex data between components
   * that may have independent lifetimes (e.g., lazy columns in R's ALTREP).
   *
   * @return A new shared_ptr<const ParseIndex> that shares this index's data.
   *
   * @note The returned ParseIndex shares the same underlying index arrays.
   *       Modifications to the original ParseIndex after calling share()
   *       may affect the shared copy, so the original should not be modified.
   *
   * @warning After calling share(), the original ParseIndex should be
   *          considered immutable. Moving or modifying it may invalidate
   *          the shared copy.
   *
   * @example
   * @code
   * // Parse a CSV file
   * auto result = parser.parse(buf, len);
   *
   * // Create a shared reference for a lazy column
   * auto shared_idx = result.idx.share();
   *
   * // The original can now be moved/destroyed without affecting shared_idx
   * ParseIndex temp = std::move(result.idx);
   *
   * // shared_idx still has valid data
   * @endcode
   */
  std::shared_ptr<const ParseIndex> share();

  /**
   * @brief Check if this index is using shared ownership.
   *
   * @return true if this index was created via share() and uses shared
   *         ownership semantics for its internal data.
   */
  bool is_shared() const { return n_indexes_shared_ != nullptr || indexes_shared_ != nullptr; }

private:
  /// RAII owner for n_indexes array. Memory freed automatically on destruction.
  /// Null when using mmap-backed data or shared ownership.
  std::unique_ptr<uint64_t[]> n_indexes_ptr_;

  /// RAII owner for indexes array. Memory freed automatically on destruction.
  /// Null when using mmap-backed data or shared ownership.
  std::unique_ptr<uint64_t[]> indexes_ptr_;

  /// RAII owner for chunk_starts array. Memory freed automatically on destruction.
  std::unique_ptr<uint64_t[]> chunk_starts_ptr_;

  /// RAII owner for region_offsets array. Memory freed automatically on destruction.
  std::unique_ptr<uint64_t[]> region_offsets_ptr_;

  /// RAII owner for flat_indexes array. Memory freed automatically on destruction.
  /// Null when using mmap-backed data or shared ownership.
  std::unique_ptr<uint64_t[]> flat_indexes_ptr_;

  /// RAII owner for col_indexes array (column-major layout).
  /// Memory freed automatically on destruction.
  std::unique_ptr<uint64_t[]> col_indexes_ptr_;

  /// Memory-mapped buffer for mmap-backed indexes.
  /// When set, n_indexes and indexes point directly into this buffer's data.
  std::unique_ptr<MmapBuffer> mmap_buffer_;

  /// Shared reference to the CSV data buffer.
  /// When set, the buffer's lifetime is managed by reference counting,
  /// allowing safe sharing between ParseIndex and consumers like ValueExtractor.
  std::shared_ptr<const std::vector<uint8_t>> buffer_;

  /// Shared owner for n_indexes array (used when share() is called).
  /// When set, n_indexes_ptr_ is null and this manages the memory.
  std::shared_ptr<uint64_t[]> n_indexes_shared_;

  /// Shared owner for indexes array (used when share() is called).
  /// When set, indexes_ptr_ is null and this manages the memory.
  std::shared_ptr<uint64_t[]> indexes_shared_;

  /// Shared owner for flat_indexes array (used when share() is called).
  /// When set, flat_indexes_ptr_ is null and this manages the memory.
  std::shared_ptr<uint64_t[]> flat_indexes_shared_;

  /// Shared owner for col_indexes array (used when share() is called).
  /// When set, col_indexes_ptr_ is null and this manages the memory.
  std::shared_ptr<uint64_t[]> col_indexes_shared_;

  /// Shared reference to mmap buffer for shared ParseIndex instances.
  std::shared_ptr<MmapBuffer> mmap_buffer_shared_;

  // Allow TwoPass::init() to set the private members
  friend class TwoPass;
};

/**
 * @brief High-performance CSV parser using a speculative two-pass algorithm.
 *
 * The TwoPass class implements a multi-threaded CSV parsing algorithm that
 * achieves high performance through SIMD operations and speculative parallel
 * processing. The algorithm is based on research by Chang et al. (SIGMOD 2019)
 * combined with SIMD techniques from Langdale & Lemire (simdjson).
 *
 * The parsing algorithm works in two phases:
 *
 * 1. **First Pass**: Scans the file to find safe split points where the file
 *    can be divided for parallel processing. Tracks quote parity to ensure
 *    chunks don't split in the middle of quoted fields.
 *
 * 2. **Second Pass**: Each thread parses its assigned chunk using a state
 *    machine to identify field boundaries. Results are stored in an interleaved
 *    format in the index structure.
 *
 * @note Thread Safety: The parser itself is stateless and thread-safe.
 *       However, each index object should only be accessed by one thread
 *       during parsing. Multiple parsers can run concurrently with separate
 *       index objects.
 *
 * @example
 * @code
 * #include "two_pass.h"
 * #include "io_util.h"
 *
 * // Load CSV file with SIMD-aligned padding
 * auto [buffer, length] = libvroom::load_file("data.csv");
 *
 * // Create parser and initialize index
 * libvroom::TwoPass parser;
 * libvroom::ParseIndex idx = parser.init(length, 4);  // 4 threads
 *
 * // Parse without error collection (throws on error)
 * parser.parse(buffer, idx, length);
 *
 * // Or parse with error collection
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 * bool success = parser.parse_with_errors(buffer, idx, length, errors);
 *
 * if (!success || errors.error_count() > 0) {
 *     for (const auto& err : errors.get_errors()) {
 *         std::cerr << "Line " << err.line << ": " << err.message << "\n";
 *     }
 * }
 * @endcode
 *
 * @see index For the result structure containing field positions
 * @see ErrorCollector For error handling during parsing
 */
class TwoPass {
public:
  /**
   * @brief Statistics from the first pass of parsing.
   *
   * The Stats structure contains information gathered during the first pass
   * that is used to determine safe chunk boundaries for multi-threaded parsing.
   *
   * @note These statistics are primarily for internal use by the parser's
   *       multi-threading logic.
   */
  struct Stats {
    /// Total number of quote characters found in the chunk.
    uint64_t n_quotes{0};

    /// Position of first newline at even quote count (safe split point if
    /// unquoted). Set to null_pos if no such newline exists.
    uint64_t first_even_nl{null_pos};

    /// Position of first newline at odd quote count (safe split point if
    /// quoted). Set to null_pos if no such newline exists.
    uint64_t first_odd_nl{null_pos};

    /// Total number of field separators (delimiters + newlines) found in the
    /// chunk, excluding those inside quoted fields. Used for right-sized index
    /// allocation.
    uint64_t n_separators{0};
  };
  /**
   * @brief First pass SIMD scan with dialect-aware quote and delimiter
   * characters.
   *
   * This function scans the buffer to:
   * 1. Count total quote characters (for chunk boundary detection)
   * 2. Find first newline at even/odd quote count (for safe split points)
   * 3. Count field separators outside quotes (for right-sized allocation)
   *
   * @param buf Input buffer
   * @param start Start position in buffer
   * @param end End position in buffer
   * @param quote_char Quote character (default: '"')
   * @param delimiter Field delimiter character (default: ',')
   * @return Stats with quote count, newline positions, and separator count
   */
  static Stats first_pass_simd(const uint8_t* buf, size_t start, size_t end, char quote_char = '"',
                               char delimiter = ',') {
    Stats out;
    assert(end >= start && "Invalid range: end must be >= start");
    size_t len = end - start;
    size_t idx = 0;
    bool needs_even = out.first_even_nl == null_pos;
    bool needs_odd = out.first_odd_nl == null_pos;
    uint64_t prev_iter_inside_quote = 0ULL; // Track quote state across iterations
    buf += start;
    for (; idx < len; idx += 64) {
      libvroom_prefetch(buf + idx + 128);

      size_t remaining = len - idx;
      simd_input in = fill_input_safe(buf + idx, remaining);
      uint64_t mask = ~0ULL;

      /* TODO: look into removing branches if possible */
      if (remaining < 64) {
        mask = blsmsk_u64(1ULL << remaining);
      }

      uint64_t quotes = cmp_mask_against_input(in, static_cast<uint8_t>(quote_char)) & mask;

      // Compute separator positions (delimiters + newlines) outside quotes
      uint64_t delims = cmp_mask_against_input(in, static_cast<uint8_t>(delimiter)) & mask;
      uint64_t nl = compute_line_ending_mask_simple(in, mask);
      uint64_t quote_mask = find_quote_mask2(quotes, prev_iter_inside_quote);
      uint64_t field_seps = (delims | nl) & ~quote_mask & mask;
      out.n_separators += count_ones(field_seps);

      if (needs_even || needs_odd) {
        if (nl != 0) {
          if (needs_even) {
            uint64_t quote_mask2 = find_quote_mask(quotes, ~0ULL) & mask;
            uint64_t even_nl = quote_mask2 & nl;
            if (even_nl > 0) {
              out.first_even_nl = start + idx + trailing_zeroes(even_nl);
            }
            needs_even = false;
          }
          if (needs_odd) {
            uint64_t quote_mask_odd = find_quote_mask(quotes, 0ULL) & mask;
            uint64_t odd_nl = quote_mask_odd & nl & mask;
            if (odd_nl > 0) {
              out.first_odd_nl = start + idx + trailing_zeroes(odd_nl);
            }
            needs_odd = false;
          }
        }
      }

      out.n_quotes += count_ones(quotes);
    }
    return out;
  }

  /**
   * @brief First pass scalar scan with dialect-aware quote and delimiter
   * characters.
   *
   * Scalar fallback version of first_pass_simd. Used when SIMD is not available
   * or for small chunks.
   *
   * @param buf Input buffer
   * @param start Start position in buffer
   * @param end End position in buffer
   * @param quote_char Quote character (default: '"')
   * @param delimiter Field delimiter character (default: ',')
   * @return Stats with quote count, newline positions, and separator count
   */
  static Stats first_pass_chunk(const uint8_t* buf, size_t start, size_t end, char quote_char = '"',
                                char delimiter = ',');

  static Stats first_pass_naive(const uint8_t* buf, size_t start, size_t end);

  /**
   * @brief Check if character is not a delimiter, newline (LF or CR), or quote.
   */
  static bool is_other(uint8_t c, char delimiter = ',', char quote_char = '"') {
    return c != static_cast<uint8_t>(delimiter) && c != '\n' && c != '\r' &&
           c != static_cast<uint8_t>(quote_char);
  }

  enum quote_state { AMBIGUOUS, QUOTED, UNQUOTED };

  /**
   * @brief Determine quote state at a position using backward scanning.
   */
  static quote_state get_quotation_state(const uint8_t* buf, size_t start, char delimiter = ',',
                                         char quote_char = '"');

  /**
   * @brief Speculative first pass with dialect-aware quote character.
   */
  static Stats first_pass_speculate(const uint8_t* buf, size_t start, size_t end,
                                    char delimiter = ',', char quote_char = '"');

  /**
   * @brief Result structure from second pass SIMD scan.
   *
   * Contains both the number of indexes found and whether parsing ended
   * at a record boundary. This is used for speculation validation in
   * Algorithm 1 from Chang et al. - if a chunk doesn't end at a record
   * boundary, the speculation was incorrect.
   */
  struct SecondPassResult {
    uint64_t n_indexes;      ///< Number of field separators found
    bool at_record_boundary; ///< True if parsing ended at a record boundary
  };

  /**
   * @brief Second pass SIMD scan with dialect-aware delimiter and quote
   * character.
   */
  static uint64_t second_pass_simd(const uint8_t* buf, size_t start, size_t end, ParseIndex* out,
                                   size_t thread_id, char delimiter = ',', char quote_char = '"') {
    return second_pass_simd_with_state(buf, start, end, out, thread_id, delimiter, quote_char)
        .n_indexes;
  }

  /**
   * @brief Second pass SIMD scan that also returns ending state.
   *
   * This version returns both the index count and whether parsing ended at
   * a record boundary. Used for speculation validation per Chang et al.
   * Algorithm 1 - chunks must end at record boundaries for speculation
   * to be valid.
   *
   * A chunk ends at a record boundary if the final quote parity is even
   * (not inside a quoted field). If we end inside a quote, the speculation
   * was definitely wrong and we need to fall back to two-pass parsing.
   */
  static SecondPassResult second_pass_simd_with_state(const uint8_t* buf, size_t start, size_t end,
                                                      ParseIndex* out, size_t thread_id,
                                                      char delimiter = ',', char quote_char = '"') {
    assert(end >= start && "Invalid range: end must be >= start");
    size_t len = end - start;
    uint64_t idx = 0;
    size_t n_indexes = 0;
    uint64_t prev_iter_inside_quote = 0ULL; // either all zeros or all ones
    uint64_t base = 0;
    const uint8_t* data = buf + start;

    // Calculate per-thread base pointer for contiguous storage.
    // Each thread writes to its own region to avoid false sharing.
    // Use region_offsets if available (per-thread right-sized allocation),
    // otherwise fall back to uniform region_size.
    uint64_t* thread_base = out->region_offsets != nullptr
                                ? out->indexes + out->region_offsets[thread_id]
                                : out->indexes + thread_id * out->region_size;

    for (; idx < len; idx += 64) {
      libvroom_prefetch(data + idx + 128);
      size_t remaining = len - idx;
      simd_input in = fill_input_safe(data + idx, remaining);

      uint64_t mask = ~0ULL;

      if (remaining < 64) {
        mask = blsmsk_u64(1ULL << remaining);
      }

      uint64_t quotes = cmp_mask_against_input(in, static_cast<uint8_t>(quote_char)) & mask;

      uint64_t quote_mask = find_quote_mask2(quotes, prev_iter_inside_quote);
      uint64_t sep = cmp_mask_against_input(in, static_cast<uint8_t>(delimiter)) & mask;
      // Support LF, CRLF, and CR-only line endings
      uint64_t end_mask = compute_line_ending_mask_simple(in, mask);
      uint64_t field_sep = (end_mask | sep) & ~quote_mask;

      n_indexes += write(thread_base, base, start + idx, out->n_threads, field_sep);
    }

    // Check if we ended at a record boundary:
    // Not inside a quoted field (prev_iter_inside_quote == 0)
    //
    // The key insight from Chang et al. Algorithm 1: if speculative chunk
    // boundary detection was wrong, parsing this chunk will end inside a
    // quoted field. The next chunk would then start mid-quote, leading to
    // incorrect parsing. By checking the ending state, we can detect this
    // misprediction and fall back to reliable two-pass parsing.
    bool at_record_boundary = (prev_iter_inside_quote == 0);

    return {n_indexes, at_record_boundary};
  }

  /**
   * @brief Branchless SIMD second pass using lookup table state machine.
   *
   * This method uses the branchless state machine implementation which
   * eliminates branch mispredictions by using precomputed lookup tables for
   * character classification and state transitions.
   *
   * Performance characteristics:
   * - Eliminates 90%+ of branches in the parsing hot path
   * - Uses SIMD for parallel character classification
   * - Single memory access per character for classification
   * - Single memory access per character for state transition
   *
   * @param sm Pre-initialized branchless state machine
   * @param buf Input buffer
   * @param start Start position in buffer
   * @param end End position in buffer
   * @param out Index structure to store results
   * @param thread_id Thread ID for contiguous per-thread storage
   * @return Number of field separators found
   */
  static uint64_t second_pass_simd_branchless(const BranchlessStateMachine& sm, const uint8_t* buf,
                                              size_t start, size_t end, ParseIndex* out,
                                              size_t thread_id) {
    // Calculate per-thread base pointer for contiguous storage
    // Use region_offsets if available (per-thread right-sized allocation)
    uint64_t* thread_base = out->region_offsets != nullptr
                                ? out->indexes + out->region_offsets[thread_id]
                                : out->indexes + thread_id * out->region_size;
    return libvroom::second_pass_simd_branchless(sm, buf, start, end, thread_base, thread_id,
                                                 out->n_threads);
  }

  /**
   * @brief Branchless SIMD second pass that also returns ending state.
   *
   * This version returns both the index count and whether parsing ended at
   * a record boundary. Used for speculation validation per Chang et al.
   * Algorithm 1 - chunks must end at record boundaries for speculation
   * to be valid.
   *
   * @param sm Pre-initialized branchless state machine
   * @param buf Input buffer
   * @param start Start position in buffer
   * @param end End position in buffer
   * @param out Index structure to store results
   * @param thread_id Thread ID for contiguous per-thread storage
   * @return SecondPassResult with count and boundary status
   */
  static SecondPassResult second_pass_simd_branchless_with_state(const BranchlessStateMachine& sm,
                                                                 const uint8_t* buf, size_t start,
                                                                 size_t end, ParseIndex* out,
                                                                 size_t thread_id) {
    // Calculate per-thread base pointer for contiguous storage
    // Use region_offsets if available (per-thread right-sized allocation)
    uint64_t* thread_base = out->region_offsets != nullptr
                                ? out->indexes + out->region_offsets[thread_id]
                                : out->indexes + thread_id * out->region_size;
    auto result = libvroom::second_pass_simd_branchless_with_state(sm, buf, start, end, thread_base,
                                                                   thread_id, out->n_threads);
    return {result.n_indexes, result.at_record_boundary};
  }

  /**
   * @brief Parser state machine states for CSV field parsing.
   *
   * The CSV parser uses a finite state machine to track its position within
   * the CSV structure. Each character transition updates the state based on
   * whether it's a quote, comma, newline, or other character.
   *
   * State transitions:
   * - RECORD_START + '"' -> QUOTED_FIELD
   * - RECORD_START + ',' -> FIELD_START
   * - RECORD_START + '\n' -> RECORD_START
   * - RECORD_START + other -> UNQUOTED_FIELD
   * - QUOTED_FIELD + '"' -> QUOTED_END (potential close or escape)
   * - QUOTED_END + '"' -> QUOTED_FIELD (escaped quote)
   * - QUOTED_END + ',' -> FIELD_START (field ended)
   * - QUOTED_END + '\n' -> RECORD_START (record ended)
   */
  enum csv_state {
    RECORD_START,   ///< At the beginning of a new record (row).
    FIELD_START,    ///< At the beginning of a new field (after comma).
    UNQUOTED_FIELD, ///< Inside an unquoted field.
    QUOTED_FIELD,   ///< Inside a quoted field.
    QUOTED_END      ///< Just saw a quote inside a quoted field (might be closing or
                    ///< escape).
  };

  // Error result from state transitions
  struct StateResult {
    csv_state state;
    ErrorCode error;
  };

  really_inline static StateResult quoted_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case FIELD_START:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      // Quote in middle of unquoted field
      return {UNQUOTED_FIELD, ErrorCode::QUOTE_IN_UNQUOTED_FIELD};
    case QUOTED_FIELD:
      return {QUOTED_END, ErrorCode::NONE};
    case QUOTED_END:
      return {QUOTED_FIELD, ErrorCode::NONE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static StateResult comma_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {FIELD_START, ErrorCode::NONE};
    case FIELD_START:
      return {FIELD_START, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      return {FIELD_START, ErrorCode::NONE};
    case QUOTED_FIELD:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_END:
      return {FIELD_START, ErrorCode::NONE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static StateResult newline_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {RECORD_START, ErrorCode::NONE};
    case FIELD_START:
      return {RECORD_START, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      return {RECORD_START, ErrorCode::NONE};
    case QUOTED_FIELD:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_END:
      return {RECORD_START, ErrorCode::NONE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static StateResult other_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {UNQUOTED_FIELD, ErrorCode::NONE};
    case FIELD_START:
      return {UNQUOTED_FIELD, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      return {UNQUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_FIELD:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_END:
      // Invalid character after closing quote
      return {UNQUOTED_FIELD, ErrorCode::INVALID_QUOTE_ESCAPE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  // Add a position to the index array using contiguous per-thread storage.
  // The caller must initialize i to thread_id * region_size, then this function
  // increments by 1 for each call.
  really_inline static size_t add_position(ParseIndex* out, size_t i, size_t pos) {
    out->indexes[i] = pos;
    return i + 1; // Contiguous: increment by 1, not n_threads
  }

  // Default context size for error messages (characters before/after error
  // position)
  static constexpr size_t DEFAULT_ERROR_CONTEXT_SIZE = 20;

  // Helper to get context around an error position
  // Returns a string representation of the buffer content near the given
  // position
  static std::string get_context(const uint8_t* buf, size_t len, size_t pos,
                                 size_t context_size = DEFAULT_ERROR_CONTEXT_SIZE);

  // Helper to calculate line and column from byte offset
  // SECURITY: buf_len parameter ensures we never read past buffer bounds
  static void get_line_column(const uint8_t* buf, size_t buf_len, size_t offset, size_t& line,
                              size_t& column);

  /**
   * @brief Check if position is at the start of a comment line.
   *
   * A comment line is a line that starts with the comment character,
   * optionally preceded by whitespace (spaces or tabs).
   *
   * @param buf Buffer to check
   * @param pos Position to check (must be at start of line)
   * @param end End of buffer
   * @param comment_char Comment character ('\0' means no comments)
   * @return true if this is a comment line
   */
  static bool is_comment_line(const uint8_t* buf, size_t pos, size_t end, char comment_char);

  /**
   * @brief Skip to the end of the current line.
   *
   * @param buf Buffer to scan
   * @param pos Current position
   * @param end End of buffer
   * @return Position after the line ending (or end if no newline found)
   */
  static size_t skip_to_line_end(const uint8_t* buf, size_t pos, size_t end);

  /**
   * @brief Second pass with error collection and dialect support.
   */
  static uint64_t second_pass_chunk(const uint8_t* buf, size_t start, size_t end, ParseIndex* out,
                                    size_t thread_id, ErrorCollector* errors = nullptr,
                                    size_t total_len = 0, char delimiter = ',',
                                    char quote_char = '"', char comment_char = '\0');

  /**
   * @brief Second pass that throws on error (backward compatible), with dialect
   * support.
   */
  static uint64_t second_pass_chunk_throwing(const uint8_t* buf, size_t start, size_t end,
                                             ParseIndex* out, size_t thread_id,
                                             char delimiter = ',', char quote_char = '"',
                                             char comment_char = '\0');

  /**
   * @brief Parse using speculative multi-threading with dialect support.
   *
   * @param buf Input buffer
   * @param out Output index to populate
   * @param len Buffer length
   * @param dialect CSV dialect settings
   * @param progress Optional progress callback (called after each chunk completes)
   * @return true if parsing succeeded
   */
  bool parse_speculate(const uint8_t* buf, ParseIndex& out, size_t len,
                       const Dialect& dialect = Dialect::csv(),
                       const SecondPassProgressCallback& progress = nullptr);

  /**
   * @brief Parse using two-pass algorithm with dialect support.
   *
   * @param buf Input buffer
   * @param out Output index to populate
   * @param len Buffer length
   * @param dialect CSV dialect settings
   * @param progress Optional progress callback (called after each chunk completes)
   * @return true if parsing succeeded
   */
  bool parse_two_pass(const uint8_t* buf, ParseIndex& out, size_t len,
                      const Dialect& dialect = Dialect::csv(),
                      const SecondPassProgressCallback& progress = nullptr);

  /**
   * @brief Parse a CSV buffer and build the field index.
   *
   * @param buf Input buffer
   * @param out Output index to populate
   * @param len Buffer length
   * @param dialect CSV dialect settings
   * @param progress Optional progress callback (called after each chunk completes)
   * @return true if parsing succeeded
   */
  bool parse(const uint8_t* buf, ParseIndex& out, size_t len,
             const Dialect& dialect = Dialect::csv(),
             const SecondPassProgressCallback& progress = nullptr);

  /**
   * @brief Parse a CSV buffer with optimized per-thread memory allocation.
   *
   * This method combines chunk boundary detection, per-chunk separator counting,
   * and parsing into a single operation that allocates only the memory needed
   * for each thread's actual separator count. This dramatically reduces memory
   * usage for multi-threaded parsing compared to the default worst-case allocation.
   *
   * Memory savings: For N separators evenly distributed across T threads:
   * - Default: T * N (each thread gets space for all separators)
   * - Optimized: ~N (each thread gets space for its ~N/T separators)
   *
   * @param buf Input buffer
   * @param len Buffer length
   * @param n_threads Number of threads to use for parsing
   * @param dialect CSV dialect settings
   * @param progress Optional progress callback (called after each chunk completes)
   * @return ParseIndex with parsed data and optimized memory allocation
   */
  ParseIndex parse_optimized(const uint8_t* buf, size_t len, size_t n_threads,
                             const Dialect& dialect = Dialect::csv(),
                             const SecondPassProgressCallback& progress = nullptr);

  // Result from multi-threaded branchless parsing with error collection
  struct branchless_chunk_result {
    uint64_t n_indexes;
    ErrorCollector errors;

    branchless_chunk_result() : n_indexes(0), errors(ErrorMode::PERMISSIVE) {}
  };

  /**
   * @brief Static wrapper for thread-safe branchless parsing with error
   * collection.
   */
  static branchless_chunk_result
  second_pass_branchless_chunk_with_errors(const BranchlessStateMachine& sm, const uint8_t* buf,
                                           size_t start, size_t end, ParseIndex* out,
                                           size_t thread_id, size_t total_len, ErrorMode mode);

  /**
   * @brief Parse a CSV buffer using branchless state machine with error
   * collection.
   */
  bool parse_branchless_with_errors(const uint8_t* buf, ParseIndex& out, size_t len,
                                    ErrorCollector& errors,
                                    const Dialect& dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer using branchless state machine (optimized).
   */
  bool parse_branchless(const uint8_t* buf, ParseIndex& out, size_t len,
                        const Dialect& dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer with automatic dialect detection.
   */
  bool parse_auto(const uint8_t* buf, ParseIndex& out, size_t len, ErrorCollector& errors,
                  DetectionResult* detected = nullptr,
                  const DetectionOptions& detection_options = DetectionOptions());

  /**
   * @brief Detect the dialect of a CSV buffer without parsing.
   */
  static DetectionResult detect_dialect(const uint8_t* buf, size_t len,
                                        const DetectionOptions& options = DetectionOptions());

  // Result from multi-threaded parsing with error collection
  struct chunk_result {
    uint64_t n_indexes;
    ErrorCollector errors;

    chunk_result() : n_indexes(0), errors(ErrorMode::PERMISSIVE) {}
  };

  /**
   * @brief Static wrapper for thread-safe parsing with error collection and
   * dialect.
   */
  static chunk_result second_pass_chunk_with_errors(const uint8_t* buf, size_t start, size_t end,
                                                    ParseIndex* out, size_t thread_id,
                                                    size_t total_len, ErrorMode mode,
                                                    char delimiter = ',', char quote_char = '"',
                                                    char comment_char = '\0');

  /**
   * @brief Parse a CSV buffer with error collection using multi-threading.
   */
  bool parse_two_pass_with_errors(const uint8_t* buf, ParseIndex& out, size_t len,
                                  ErrorCollector& errors, const Dialect& dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer with detailed error collection (single-threaded).
   */
  bool parse_with_errors(const uint8_t* buf, ParseIndex& out, size_t len, ErrorCollector& errors,
                         const Dialect& dialect = Dialect::csv());

  // Check for empty header (skips leading comment lines if comment_char is set)
  static bool check_empty_header(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                 char comment_char = '\0');

  /**
   * @brief Check for duplicate column names in header with dialect support.
   */
  static void check_duplicate_columns(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                      char delimiter = ',', char quote_char = '"',
                                      char comment_char = '\0');

  /**
   * @brief Check for inconsistent field counts with dialect support.
   */
  static void check_field_counts(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                 char delimiter = ',', char quote_char = '"',
                                 char comment_char = '\0');

  // Check for mixed line endings
  static void check_line_endings(const uint8_t* buf, size_t len, ErrorCollector& errors);

  /**
   * @brief Perform full CSV validation with comprehensive error checking.
   */
  bool parse_validate(const uint8_t* buf, ParseIndex& out, size_t len, ErrorCollector& errors,
                      const Dialect& dialect = Dialect::csv());

  /**
   * @brief Initialize an index structure for parsing.
   */
  ParseIndex init(size_t len, size_t n_threads);

  /**
   * @brief Initialize an index structure with overflow validation.
   */
  ParseIndex init_safe(size_t len, size_t n_threads, ErrorCollector* errors = nullptr);

  /**
   * @brief Initialize an index structure with exact-sized allocation.
   *
   * This method uses the separator count from a first pass to allocate
   * exactly the right amount of memory, reducing memory usage by 2-10x
   * for typical CSV files compared to the worst-case allocation in init().
   *
   * @param total_separators Total number of separators found in first pass.
   * @param n_threads Number of threads for parsing.
   * @return ParseIndex with right-sized allocation.
   */
  ParseIndex init_counted(uint64_t total_separators, size_t n_threads);

  /**
   * @brief Initialize an index structure with exact-sized allocation and
   * overflow validation.
   *
   * @param total_separators Total number of separators found in first pass.
   * @param n_threads Number of threads for parsing.
   * @param errors Optional error collector for overflow errors.
   * @param n_quotes Number of quote characters found in first pass. Used to
   *        determine if safety padding is needed for error recovery scenarios.
   * @param len File length in bytes. Used as upper bound when n_quotes > 0
   *        to ensure sufficient allocation for error recovery scenarios.
   * @return ParseIndex with right-sized allocation, or empty on error.
   */
  ParseIndex init_counted_safe(uint64_t total_separators, size_t n_threads,
                               ErrorCollector* errors = nullptr, uint64_t n_quotes = 0,
                               size_t len = 0);

  /**
   * @brief Initialize an index structure with per-thread right-sized allocation.
   *
   * This method uses per-thread separator counts from a first pass to allocate
   * exactly the right amount of memory for each thread's region. This provides
   * optimal memory usage by avoiding the worst-case assumption that all
   * separators could end up in any single thread's chunk.
   *
   * Memory savings: For a file with N separators evenly distributed across T
   * threads, this allocates ~N entries instead of ~N*T entries.
   *
   * @param thread_separator_counts Vector of separator counts per thread.
   *        Size must match n_threads parameter.
   * @param n_threads Number of threads for parsing.
   * @param padding_per_thread Extra slots per thread for safety (default: 8).
   * @return ParseIndex with right-sized per-thread allocation.
   *
   * @example
   * @code
   * // After first pass that collected per-thread stats:
   * std::vector<uint64_t> counts = {1000, 1200, 800, 1100}; // 4 threads
   * auto idx = parser.init_counted_per_thread(counts, 4);
   * // Total allocation: (1000+8) + (1200+8) + (800+8) + (1100+8) = 4132 slots
   * // vs init_counted: 4*4100 = 16400 slots with uniform regions
   * @endcode
   */
  ParseIndex init_counted_per_thread(const std::vector<uint64_t>& thread_separator_counts,
                                     size_t n_threads, size_t padding_per_thread = 8);

  /**
   * @brief Initialize an index structure with per-thread right-sized allocation
   *        and overflow validation.
   *
   * Same as init_counted_per_thread but with overflow checking and error
   * reporting.
   *
   * @param thread_separator_counts Vector of separator counts per thread.
   * @param n_threads Number of threads for parsing.
   * @param errors Optional error collector for overflow errors.
   * @param padding_per_thread Extra slots per thread for safety (default: 8).
   * @return ParseIndex with right-sized allocation, or empty on error.
   */
  ParseIndex init_counted_per_thread_safe(const std::vector<uint64_t>& thread_separator_counts,
                                          size_t n_threads, ErrorCollector* errors = nullptr,
                                          size_t padding_per_thread = 8);
};

} // namespace libvroom

#endif // TWO_PASS_H
