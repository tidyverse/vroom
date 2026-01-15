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
 * @brief Result structure containing parsed CSV field positions.
 *
 * The ParseIndex class stores the byte offsets of field separators (commas and
 * newlines) found during CSV parsing. These positions enable efficient random
 * access to individual fields without re-parsing the entire file.
 *
 * When using multi-threaded parsing, field positions are interleaved across
 * threads. For example, with 4 threads: thread 0 stores positions at indices 0,
 * 4, 8, ...; thread 1 stores at indices 1, 5, 9, ...; and so on.
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
 * // For single-threaded: positions are at result.idx.indexes[0],
 * result.idx.indexes[1], ...
 * // For multi-threaded: use stride of result.idx.n_threads
 * @endcode
 */
class ParseIndex {
public:
  /// Number of columns detected in the CSV (set after parsing header).
  uint64_t columns{0};

  /// Number of threads used for parsing. Determines the interleave stride.
  /// Changed from uint8_t to uint16_t to support systems with >255 cores.
  uint16_t n_threads{0};

  /// Array of size n_threads containing the count of indexes found by each
  /// thread.
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by n_indexes_ptr_.
  uint64_t* n_indexes{nullptr};

  /// Array of field separator positions (byte offsets). Interleaved by thread.
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by indexes_ptr_.
  uint64_t* indexes{nullptr};

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
      : columns(other.columns), n_threads(other.n_threads), n_indexes(other.n_indexes),
        indexes(other.indexes), n_indexes_ptr_(std::move(other.n_indexes_ptr_)),
        indexes_ptr_(std::move(other.indexes_ptr_)), mmap_buffer_(std::move(other.mmap_buffer_)) {
    other.n_indexes = nullptr;
    other.indexes = nullptr;
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
      n_indexes = other.n_indexes;
      indexes = other.indexes;
      n_indexes_ptr_ = std::move(other.n_indexes_ptr_);
      indexes_ptr_ = std::move(other.indexes_ptr_);
      mmap_buffer_ = std::move(other.mmap_buffer_);
      other.n_indexes = nullptr;
      other.indexes = nullptr;
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
   * @brief Destructor. Releases allocated index arrays via RAII.
   *
   * Memory is automatically freed when the unique_ptr members are destroyed.
   * For mmap-backed indexes, the memory mapping is released instead.
   */
  ~ParseIndex() = default;

  void fill_double_array(ParseIndex* idx, uint64_t column, double* out) {}

private:
  /// RAII owner for n_indexes array. Memory freed automatically on destruction.
  /// Null when using mmap-backed data.
  std::unique_ptr<uint64_t[]> n_indexes_ptr_;

  /// RAII owner for indexes array. Memory freed automatically on destruction.
  /// Null when using mmap-backed data.
  std::unique_ptr<uint64_t[]> indexes_ptr_;

  /// Memory-mapped buffer for mmap-backed indexes.
  /// When set, n_indexes and indexes point directly into this buffer's data.
  std::unique_ptr<MmapBuffer> mmap_buffer_;

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

      n_indexes += write(out->indexes + thread_id, base, start + idx, out->n_threads, field_sep);
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
   * @param thread_id Thread ID for interleaved storage
   * @return Number of field separators found
   */
  static uint64_t second_pass_simd_branchless(const BranchlessStateMachine& sm, const uint8_t* buf,
                                              size_t start, size_t end, ParseIndex* out,
                                              size_t thread_id) {
    return libvroom::second_pass_simd_branchless(sm, buf, start, end, out->indexes, thread_id,
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
   * @param thread_id Thread ID for interleaved storage
   * @return SecondPassResult with count and boundary status
   */
  static SecondPassResult second_pass_simd_branchless_with_state(const BranchlessStateMachine& sm,
                                                                 const uint8_t* buf, size_t start,
                                                                 size_t end, ParseIndex* out,
                                                                 size_t thread_id) {
    auto result = libvroom::second_pass_simd_branchless_with_state(
        sm, buf, start, end, out->indexes, thread_id, out->n_threads);
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

  really_inline static size_t add_position(ParseIndex* out, size_t i, size_t pos) {
    out->indexes[i] = pos;
    return i + out->n_threads;
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
};

} // namespace libvroom

#endif // TWO_PASS_H
