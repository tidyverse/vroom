/**
 * @file libvroom.h
 * @brief libvroom - High-performance CSV parser using portable SIMD
 * instructions.
 * @version 0.1.0
 *
 * This is the main public header for the libvroom library. Include this single
 * header to access all public functionality.
 */

#ifndef LIBVROOM_H
#define LIBVROOM_H

#define LIBVROOM_VERSION_MAJOR 0
#define LIBVROOM_VERSION_MINOR 1
#define LIBVROOM_VERSION_PATCH 0
#define LIBVROOM_VERSION_STRING "0.1.0"

#include "dialect.h"
#include "error.h"
#include "index_cache.h"
#include "io_util.h"
#include "mem_util.h"
#include "mmap_util.h"
#include "two_pass.h"
#include "value_extraction.h"

#include <atomic>
#include <functional>
#include <future>
#include <optional>
#include <unordered_map>

namespace libvroom {

/**
 * @brief Callback signature for progress reporting during parsing.
 *
 * This callback is invoked periodically during parsing to report progress.
 * It can be used to display a progress bar, update a UI, or implement
 * cancellation logic.
 *
 * @param bytes_processed Number of bytes processed so far.
 * @param total_bytes Total number of bytes to process.
 * @return true to continue parsing, false to abort.
 *
 * @note The callback should return quickly to avoid slowing down parsing.
 *       For very large files, it may be called thousands of times.
 *
 * @note Progress is reported at chunk boundaries (typically every 1-4MB).
 *       The final callback may report bytes_processed < total_bytes if
 *       parsing is aborted.
 *
 * @example
 * @code
 * // Simple console progress bar
 * auto progress = [](size_t processed, size_t total) {
 *     int percent = total > 0 ? (processed * 100 / total) : 0;
 *     std::cerr << "\rProgress: " << percent << "%" << std::flush;
 *     return true;  // continue parsing
 * };
 *
 * ParseOptions opts;
 * opts.progress_callback = progress;
 * auto result = parser.parse(buf, len, opts);
 * std::cerr << std::endl;  // newline after progress
 * @endcode
 *
 * @example
 * @code
 * // Cancellable parsing
 * std::atomic<bool> cancel_requested{false};
 * auto progress = [&cancel_requested](size_t, size_t) {
 *     return !cancel_requested.load();
 * };
 * @endcode
 */
using ProgressCallback = std::function<bool(size_t bytes_processed, size_t total_bytes)>;

/**
 * @brief Thread-safe progress tracker for multi-threaded parsing.
 *
 * This class wraps a progress callback and provides thread-safe progress
 * updates with automatic throttling to avoid excessive callback invocations.
 *
 * Design:
 * - Uses atomic counter for thread-safe progress accumulation
 * - Throttles callback to ~1% granularity (100 updates max)
 * - Supports cancellation by checking callback return value
 *
 * @note This is an internal implementation detail; users interact via
 *       ProgressCallback directly through ParseOptions.
 */
class ProgressTracker {
public:
  /**
   * @brief Create a progress tracker.
   *
   * @param callback User's progress callback (may be null)
   * @param total_bytes Total bytes to process
   * @param first_pass_weight Weight of first pass (0.0-1.0), default 0.1 (10%)
   */
  explicit ProgressTracker(ProgressCallback callback, size_t total_bytes,
                           double first_pass_weight = 0.1)
      : callback_(std::move(callback)), total_bytes_(total_bytes),
        first_pass_weight_(first_pass_weight), bytes_processed_(0), last_reported_percent_(-1),
        cancelled_(false) {}

  /**
   * @brief Report progress from first pass (chunk boundary detection).
   *
   * Thread-safe. Progress is weighted by first_pass_weight.
   *
   * @param bytes Bytes processed in this update
   * @return true to continue, false if cancelled
   */
  bool add_first_pass_progress(size_t bytes) {
    if (!callback_ || cancelled_)
      return !cancelled_;

    // First pass contributes first_pass_weight_ of total progress
    size_t weighted = static_cast<size_t>(bytes * first_pass_weight_);
    return add_progress_internal(weighted);
  }

  /**
   * @brief Report progress from second pass (field indexing).
   *
   * Thread-safe. Progress is weighted by (1 - first_pass_weight).
   *
   * @param bytes Bytes processed in this update
   * @return true to continue, false if cancelled
   */
  bool add_second_pass_progress(size_t bytes) {
    if (!callback_ || cancelled_)
      return !cancelled_;

    // Second pass contributes (1 - first_pass_weight_) of total progress
    // First pass already added its weighted bytes to bytes_processed_,
    // so we just add the second pass weighted bytes.
    double second_weight = 1.0 - first_pass_weight_;
    size_t weighted = static_cast<size_t>(bytes * second_weight);
    return add_progress_internal(weighted);
  }

  /**
   * @brief Report completion (100%).
   */
  void complete() {
    if (callback_ && !cancelled_) {
      callback_(total_bytes_, total_bytes_);
    }
  }

  /**
   * @brief Check if parsing was cancelled.
   */
  bool is_cancelled() const { return cancelled_.load(std::memory_order_acquire); }

  /**
   * @brief Check if a callback is registered.
   */
  bool has_callback() const { return static_cast<bool>(callback_); }

private:
  bool add_progress_internal(size_t weighted_bytes) {
    size_t old_val = bytes_processed_.fetch_add(weighted_bytes, std::memory_order_relaxed);
    size_t new_val = old_val + weighted_bytes;

    // Calculate percentage (0-100)
    int new_percent = total_bytes_ > 0 ? static_cast<int>((new_val * 100) / total_bytes_) : 0;
    new_percent = std::min(new_percent, 100);

    // Only call callback if percentage changed (throttling)
    int last = last_reported_percent_.load(std::memory_order_relaxed);
    if (new_percent > last) {
      // Try to update last_reported_percent_ atomically
      if (last_reported_percent_.compare_exchange_strong(last, new_percent,
                                                         std::memory_order_relaxed)) {
        // We won the race to report this percentage
        bool should_continue = callback_(new_val, total_bytes_);
        if (!should_continue) {
          cancelled_.store(true, std::memory_order_release);
          return false;
        }
      }
    }
    return true;
  }

  ProgressCallback callback_;
  size_t total_bytes_;
  double first_pass_weight_;
  std::atomic<size_t> bytes_processed_;
  std::atomic<int> last_reported_percent_;
  std::atomic<bool> cancelled_;
};

/**
 * @brief Algorithm selection for parsing.
 *
 * Allows choosing between different parsing implementations that offer
 * different performance characteristics.
 */
enum class ParseAlgorithm {
  /**
   * @brief Automatic algorithm selection (default).
   *
   * The parser chooses the best algorithm based on the data and options.
   * Currently uses the speculative multi-threaded algorithm.
   */
  AUTO,

  /**
   * @brief Speculative multi-threaded parsing.
   *
   * Uses speculative execution to find safe chunk boundaries for parallel
   * processing. Good general-purpose choice for large files.
   */
  SPECULATIVE,

  /**
   * @brief Two-pass algorithm with quote tracking.
   *
   * Traditional two-pass approach that tracks quote parity across chunks.
   * More predictable than speculative but may be slower for some files.
   */
  TWO_PASS,

  /**
   * @brief Branchless state machine implementation.
   *
   * Uses lookup tables to eliminate branch mispredictions in the parsing
   * hot path. Can provide significant speedups on data with many special
   * characters (quotes, delimiters) that cause branch mispredictions.
   *
   * Performance characteristics:
   * - Eliminates 90%+ of branches in parsing
   * - Single memory access per character for classification and transition
   * - Best for files with high quote/delimiter density
   */
  BRANCHLESS
};

/**
 * @brief Size limits for secure CSV parsing.
 *
 * These limits prevent denial-of-service attacks through excessive memory
 * allocation. They can be configured based on the expected data and available
 * system resources.
 *
 * ## Security Considerations
 *
 * Without size limits, a malicious CSV file could cause:
 * - **Memory exhaustion**: The parser allocates index arrays proportional to
 *   file size. A 1GB file allocates ~8GB for indexes (one uint64_t per byte).
 * - **Integer overflow**: Unchecked size calculations could overflow, leading
 *   to undersized allocations and buffer overflows.
 *
 * ## Defaults
 *
 * Default limits are chosen to handle most legitimate use cases while
 * providing protection against malicious inputs:
 * - max_file_size: 10GB (handles very large datasets)
 * - max_field_size: 16MB (larger than most legitimate fields)
 *
 * @example
 * @code
 * // Use default limits
 * libvroom::Parser parser;
 * auto result = parser.parse(buf, len);
 *
 * // Use custom limits for large file processing
 * libvroom::SizeLimits limits;
 * limits.max_file_size = 50ULL * 1024 * 1024 * 1024;  // 50GB
 * auto result = parser.parse(buf, len, {.limits = limits});
 *
 * // Disable limits (NOT RECOMMENDED for untrusted input)
 * auto result = parser.parse(buf, len, {.limits = SizeLimits::unlimited()});
 * @endcode
 */
struct SizeLimits {
  /**
   * @brief Maximum file size in bytes (default: 10GB).
   *
   * Files larger than this limit will be rejected with FILE_TOO_LARGE error.
   * Set to 0 to disable the file size check (not recommended).
   */
  size_t max_file_size = 10ULL * 1024 * 1024 * 1024; // 10GB default

  /**
   * @brief Maximum field size in bytes (default: 16MB).
   *
   * Individual fields larger than this will trigger FIELD_TOO_LARGE error.
   * Set to 0 to disable field size checks.
   */
  size_t max_field_size = 16ULL * 1024 * 1024; // 16MB default

  /**
   * @brief Enable UTF-8 validation (default: false for performance).
   *
   * When true, the parser validates that all byte sequences are valid UTF-8.
   * Invalid sequences are reported as INVALID_UTF8 errors. This has a
   * performance cost, so only enable when parsing untrusted input that
   * claims to be UTF-8 encoded.
   */
  bool validate_utf8 = false;

  /**
   * @brief Factory for default limits (10GB file, 16MB field, no UTF-8
   * validation).
   */
  static SizeLimits defaults() { return SizeLimits{}; }

  /**
   * @brief Factory for unlimited parsing (disables all size checks).
   *
   * @warning Using unlimited limits with untrusted input is dangerous
   *          and may lead to denial-of-service through memory exhaustion.
   */
  static SizeLimits unlimited() {
    SizeLimits limits;
    limits.max_file_size = 0;
    limits.max_field_size = 0;
    return limits;
  }

  /**
   * @brief Factory for strict limits (suitable for web services).
   *
   * @param max_file Maximum file size in bytes (default: 100MB)
   * @param max_field Maximum field size in bytes (default: 1MB)
   */
  static SizeLimits strict(size_t max_file = 100ULL * 1024 * 1024,
                           size_t max_field = 1ULL * 1024 * 1024) {
    SizeLimits limits;
    limits.max_file_size = max_file;
    limits.max_field_size = max_field;
    limits.validate_utf8 = true;
    return limits;
  }
};

/**
 * @brief Check if a size multiplication would overflow.
 *
 * This function safely checks if multiplying two size_t values would overflow
 * before performing the multiplication. Used internally to prevent integer
 * overflow in memory allocation calculations.
 *
 * @param a First operand
 * @param b Second operand
 * @return true if multiplication would overflow, false if safe
 */
inline bool would_overflow_multiply(size_t a, size_t b) {
  if (a == 0 || b == 0)
    return false;
  return a > std::numeric_limits<size_t>::max() / b;
}

/**
 * @brief Check if a size addition would overflow.
 *
 * @param a First operand
 * @param b Second operand
 * @return true if addition would overflow, false if safe
 */
inline bool would_overflow_add(size_t a, size_t b) {
  return a > std::numeric_limits<size_t>::max() - b;
}

/**
 * @brief Configuration options for parsing.
 *
 * ParseOptions provides a unified way to configure CSV parsing, combining
 * dialect selection, error handling, and algorithm selection into a single
 * structure. This enables a single parse() method to handle all use cases.
 *
 * **Key Design Principle**: Parser::parse() never throws for parse errors.
 * All errors are collected in the Result object's internal ErrorCollector,
 * accessible via result.errors(). Exceptions are reserved for truly exceptional
 * conditions (e.g., system-level memory allocation failures).
 *
 * Key behaviors:
 * - **Dialect**: If dialect is nullopt (default), the dialect is auto-detected
 *   from the data. Set an explicit dialect (e.g., Dialect::csv()) to skip
 * detection.
 * - **Error collection**: Errors are always collected in result.errors().
 *   An external ErrorCollector can optionally be provided for backward
 * compatibility.
 * - **Algorithm**: Choose parsing algorithm for performance tuning. Default
 * (AUTO) uses speculative multi-threaded parsing.
 * - **Detection options**: Only used when dialect is nullopt and auto-detection
 * runs.
 *
 * @example
 * @code
 * Parser parser;
 *
 * // Simple usage - check result for errors
 * auto result = parser.parse(buf, len);
 * if (!result.success() || result.has_errors()) {
 *     std::cerr << result.error_summary() << std::endl;
 * }
 *
 * // Explicit CSV dialect
 * auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});
 *
 * // Explicit dialect (backward compatible with external collector)
 * ErrorCollector errors(ErrorMode::PERMISSIVE);
 * auto result = parser.parse(buf, len, {
 *     .dialect = Dialect::tsv(),
 *     .errors = &errors
 * });
 *
 * // Use branchless algorithm for maximum performance
 * auto result = parser.parse(buf, len, {
 *     .dialect = Dialect::csv(),
 *     .algorithm = ParseAlgorithm::BRANCHLESS
 * });
 * @endcode
 *
 * @see Parser::parse() for the unified parsing method
 * @see Dialect for dialect configuration options
 * @see Result::errors() for accessing parse errors
 * @see ParseAlgorithm for algorithm selection
 */
struct ParseOptions {
  /**
   * @brief Dialect configuration for parsing.
   *
   * If nullopt (default), the dialect is auto-detected from the data using
   * the CleverCSV-inspired algorithm. Set to an explicit dialect to skip
   * detection and use the specified format.
   *
   * Common explicit dialects:
   * - Dialect::csv() - Standard comma-separated values
   * - Dialect::tsv() - Tab-separated values
   * - Dialect::semicolon() - Semicolon-separated (European style)
   * - Dialect::pipe() - Pipe-separated
   */
  std::optional<Dialect> dialect = std::nullopt;

  /**
   * @brief Optional external error collector for error-tolerant parsing.
   *
   * If nullptr (default), errors are collected in Result's internal collector.
   * If a pointer is provided, errors go to both the external collector and
   * the Result's internal collector.
   *
   * **Performance note**: When an explicit dialect is provided and errors is
   * nullptr, the parser uses a fast path that achieves ~370 MB/s vs ~205 MB/s
   * with comprehensive validation. The fast path only detects fatal quote
   * errors; it does NOT detect inconsistent field counts or duplicate column
   * names. Provide an external error collector if you need comprehensive
   * validation.
   *
   * @note The ErrorCollector must remain valid for the duration of parsing.
   *
   * For most use cases, the Result's internal error collector is sufficient:
   * @code
   * auto result = parser.parse(buf, len);
   * if (result.has_errors()) {
   *     for (const auto& err : result.errors()) { ... }
   * }
   * @endcode
   */
  ErrorCollector* errors = nullptr;

  /**
   * @brief Options for dialect auto-detection.
   *
   * Only used when dialect is nullopt and auto-detection runs.
   * Allows customizing detection sample size, candidate delimiters, etc.
   */
  DetectionOptions detection_options = DetectionOptions();

  /**
   * @brief Algorithm to use for parsing.
   *
   * Allows selecting different parsing implementations for performance tuning.
   * Default is AUTO which currently uses the speculative multi-threaded
   * algorithm.
   *
   * Note: When errors is non-null, some algorithms may fall back to simpler
   * implementations to ensure accurate error position tracking.
   */
  ParseAlgorithm algorithm = ParseAlgorithm::AUTO;

  /**
   * @brief Size limits for secure parsing.
   *
   * Controls maximum file and field sizes to prevent denial-of-service attacks.
   * Default limits are 10GB for files and 16MB for fields, which handles
   * most legitimate use cases while providing security protection.
   *
   * @see SizeLimits for configuration options
   */
  SizeLimits limits = SizeLimits::defaults();

  /**
   * @brief Maximum number of errors to collect before suppressing.
   *
   * When parsing malformed files, error collection can consume significant
   * memory if thousands of errors are generated (e.g., wrong delimiter causing
   * field count errors on every row). This limit prevents memory exhaustion
   * by stopping error collection after the limit is reached.
   *
   * Errors beyond the limit are counted but not stored. The count of suppressed
   * errors is available via result.error_collector().suppressed_count() and
   * is included in the error summary.
   *
   * Default: 10000 errors (ErrorCollector::DEFAULT_MAX_ERRORS)
   */
  size_t max_errors = ErrorCollector::DEFAULT_MAX_ERRORS;

  /**
   * @brief Index caching configuration.
   *
   * When set (has_value()), the parser will attempt to load a cached index from
   * disk on cache hit, or write the parsed index to disk on cache miss. This
   * can dramatically speed up repeated reads of the same file.
   *
   * When nullopt (default), caching is disabled.
   *
   * @note Caching requires a file path to be provided via source_path.
   *       Caching is not supported for stdin or memory-only buffers.
   *
   * @see CacheConfig for location options (SAME_DIR, XDG_CACHE, CUSTOM)
   */
  std::optional<CacheConfig> cache = std::nullopt;

  /**
   * @brief Source file path for caching.
   *
   * Required when caching is enabled. Used to compute the cache file path
   * and to validate cache freshness against the source file metadata.
   * If empty when caching is enabled, caching will be silently disabled.
   */
  std::string source_path;

  /**
   * @brief Force re-parsing even if a valid cache exists.
   *
   * When true, ignores any existing cache and re-parses the file, then
   * writes a new cache file. Useful for refreshing stale caches or
   * debugging cache issues.
   *
   * Only applies when caching is enabled (cache.has_value()).
   */
  bool force_cache_refresh = false;

  /**
   * @brief Optional callback for progress reporting during parsing.
   *
   * When set, this callback is invoked periodically to report parsing
   * progress. It can be used to display progress bars, update UIs, or
   * implement cancellation by returning false.
   *
   * The callback receives:
   * - bytes_processed: Number of bytes processed so far
   * - total_bytes: Total size being parsed
   *
   * Returns true to continue, false to abort parsing.
   *
   * @note Progress is approximate and reported at chunk boundaries.
   * @see ProgressCallback for detailed documentation and examples.
   */
  ProgressCallback progress_callback = nullptr;

  // =========================================================================
  // Row Filtering Options
  // =========================================================================

  /**
   * @brief Number of data rows to skip at the beginning.
   *
   * After reading any header row (if has_header is true), skip this many
   * data rows before starting to index rows. This is applied during parsing
   * to avoid indexing rows that will be discarded.
   *
   * Default: 0 (no rows skipped)
   *
   * @note Comment lines and empty lines (when skip_empty_rows is true) are
   *       not counted towards skip - only actual data rows are counted.
   *
   * @example
   * @code
   * // Skip first 10 data rows
   * auto result = parser.parse(buf, len, {.skip = 10});
   * @endcode
   */
  size_t skip = 0;

  /**
   * @brief Maximum number of data rows to read.
   *
   * After skipping rows (if skip > 0), read at most this many data rows.
   * Parsing stops early once n_max rows have been indexed. This is applied
   * during parsing to avoid processing the entire file when only a subset
   * is needed.
   *
   * A value of 0 means no limit (read all rows).
   *
   * Default: 0 (no limit)
   *
   * @note The header row (if present) is not counted towards n_max.
   *
   * @example
   * @code
   * // Read only the first 100 data rows
   * auto result = parser.parse(buf, len, {.n_max = 100});
   *
   * // Skip 10 rows, then read 100
   * auto result = parser.parse(buf, len, {.skip = 10, .n_max = 100});
   * @endcode
   */
  size_t n_max = 0;

  /**
   * @brief Comment character for line skipping.
   *
   * Lines starting with this character (optionally preceded by whitespace)
   * are treated as comments and excluded from parsing. Comment lines are
   * not counted towards skip or n_max limits.
   *
   * A value of '\0' (null character) means no comment handling.
   *
   * Default: '\0' (no comment handling)
   *
   * @note This overrides the comment_char in the Dialect if both are set.
   *       If only dialect.comment_char is set, that will be used.
   *
   * @example
   * @code
   * // Skip lines starting with #
   * auto result = parser.parse(buf, len, {.comment = '#'});
   * @endcode
   */
  char comment = '\0';

  /**
   * @brief Whether to skip empty rows during parsing.
   *
   * When true, rows that contain only whitespace or are completely empty
   * are excluded from the index. Empty rows are not counted towards skip
   * or n_max limits.
   *
   * Default: false (empty rows are preserved)
   *
   * @example
   * @code
   * // Skip blank lines in the CSV
   * auto result = parser.parse(buf, len, {.skip_empty_rows = true});
   * @endcode
   */
  bool skip_empty_rows = false;

  /**
   * @brief Per-column configuration overrides for value extraction.
   *
   * When set, these configurations are used during value extraction to
   * override the global ExtractionConfig for specific columns. This enables:
   * - Column-specific NA value definitions
   * - Column-specific type hints (force integer, double, string, etc.)
   * - Column-specific boolean true/false values
   * - Skipping specific columns during extraction
   *
   * Columns can be specified by index (0-based) or by name (resolved using
   * the header row). Name-based configurations are resolved after parsing
   * when headers are available.
   *
   * @example
   * @code
   * ParseOptions opts;
   * opts.column_configs.set("id", ColumnConfig::as_integer());
   * opts.column_configs.set("price", ColumnConfig::as_double());
   * opts.column_configs.set(5, ColumnConfig::skip());  // Skip column 5
   *
   * auto result = parser.parse(buf, len, opts);
   * @endcode
   *
   * @see ColumnConfig for available per-column options
   * @see ColumnConfigMap for managing column configurations
   */
  ColumnConfigMap column_configs;

  /**
   * @brief Global extraction configuration for value parsing.
   *
   * These settings apply to all columns that don't have specific overrides
   * in column_configs. Controls NA value detection, boolean parsing,
   * whitespace handling, and integer parsing options.
   *
   * @see ExtractionConfig for available options
   */
  ExtractionConfig extraction_config = ExtractionConfig::defaults();

  /**
   * @brief Factory for default options (auto-detect dialect, fast path).
   *
   * Equivalent to standard(). Both methods create identical options.
   */
  static ParseOptions defaults() { return ParseOptions{}; }

  /**
   * @brief Factory for standard options (auto-detect dialect, fast path).
   *
   * Creates default parsing options: auto-detect dialect, no error collection.
   * This is the recommended entry point for simple parsing use cases.
   *
   * Equivalent to defaults(). Both methods create identical options.
   */
  static ParseOptions standard() { return ParseOptions{}; }

  /**
   * @brief Factory for options with explicit dialect.
   */
  static ParseOptions with_dialect(const Dialect& d) {
    ParseOptions opts;
    opts.dialect = d;
    return opts;
  }

  /**
   * @brief Factory for options with error collection.
   */
  static ParseOptions with_errors(ErrorCollector& e) {
    ParseOptions opts;
    opts.errors = &e;
    return opts;
  }

  /**
   * @brief Factory for auto-detection mode with explicit intent.
   *
   * Returns default options configured for dialect auto-detection.
   * This is functionally equivalent to defaults() and standard(), but
   * provides more self-documenting code when auto-detection is the
   * explicit requirement.
   *
   * @return ParseOptions configured for auto-detection.
   *
   * @example
   * @code
   * auto result = parser.parse(buf, len, ParseOptions::auto_detect());
   * @endcode
   */
  static ParseOptions auto_detect() { return ParseOptions{}; }

  /**
   * @brief Factory for auto-detection with error collection.
   *
   * Convenience method combining dialect auto-detection with error
   * collection. This provides more self-documenting code compared to
   * using with_errors() when auto-detection is the explicit intent.
   *
   * @param e Reference to an ErrorCollector for collecting parse errors.
   * @return ParseOptions configured for auto-detection with error collection.
   *
   * @example
   * @code
   * ErrorCollector errors(ErrorMode::PERMISSIVE);
   * auto result = parser.parse(buf, len, ParseOptions::auto_detect_with_errors(errors));
   * @endcode
   */
  static ParseOptions auto_detect_with_errors(ErrorCollector& e) { return with_errors(e); }

  /**
   * @brief Factory for options with both dialect and error collection.
   */
  static ParseOptions with_dialect_and_errors(const Dialect& d, ErrorCollector& e) {
    ParseOptions opts;
    opts.dialect = d;
    opts.errors = &e;
    return opts;
  }

  /**
   * @brief Factory for options with specific algorithm.
   */
  static ParseOptions with_algorithm(ParseAlgorithm algo) {
    ParseOptions opts;
    opts.algorithm = algo;
    return opts;
  }

  /**
   * @brief Factory for branchless parsing (performance optimization).
   *
   * Convenience factory for using the branchless state machine algorithm
   * with an explicit dialect. This combination provides the best performance
   * for files with known format.
   */
  static ParseOptions branchless(const Dialect& d = Dialect::csv()) {
    ParseOptions opts;
    opts.dialect = d;
    opts.algorithm = ParseAlgorithm::BRANCHLESS;
    return opts;
  }

  /**
   * @brief Factory for options with caching enabled.
   *
   * Creates options with automatic caching enabled. Cache files are placed
   * in the default location (same directory as source, or XDG_CACHE as fallback).
   *
   * @param file_path Path to the source CSV file (required for caching)
   */
  static ParseOptions with_cache(const std::string& file_path) {
    ParseOptions opts;
    opts.cache = CacheConfig::defaults();
    opts.source_path = file_path;
    return opts;
  }

  /**
   * @brief Factory for options with caching to a custom directory.
   *
   * Creates options with caching enabled, storing cache files in the
   * specified directory.
   *
   * @param file_path Path to the source CSV file
   * @param cache_dir Directory to store cache files
   */
  static ParseOptions with_cache_dir(const std::string& file_path, const std::string& cache_dir) {
    ParseOptions opts;
    opts.cache = CacheConfig::custom(cache_dir);
    opts.source_path = file_path;
    return opts;
  }

  /**
   * @brief Factory for options with progress callback.
   *
   * Creates options with a progress callback for monitoring parse progress.
   *
   * @param callback Progress callback function
   */
  static ParseOptions with_progress(ProgressCallback callback) {
    ParseOptions opts;
    opts.progress_callback = std::move(callback);
    return opts;
  }

  /**
   * @brief Factory for options with per-column configuration.
   *
   * Creates options with per-column configuration overrides for value
   * extraction. Use this to specify different parsing settings for
   * specific columns.
   *
   * @param configs ColumnConfigMap with per-column settings
   * @return ParseOptions configured with the column configs
   *
   * @example
   * @code
   * ColumnConfigMap configs;
   * configs.set("id", ColumnConfig::as_integer());
   * configs.set("price", ColumnConfig::as_double());
   *
   * auto result = parser.parse(buf, len, ParseOptions::with_column_configs(configs));
   * @endcode
   */
  static ParseOptions with_column_configs(const ColumnConfigMap& configs) {
    ParseOptions opts;
    opts.column_configs = configs;
    return opts;
  }

  /**
   * @brief Factory for options with extraction configuration.
   *
   * Creates options with custom global extraction settings.
   *
   * @param config ExtractionConfig with parsing settings
   * @return ParseOptions configured with the extraction config
   */
  static ParseOptions with_extraction_config(const ExtractionConfig& config) {
    ParseOptions opts;
    opts.extraction_config = config;
    return opts;
  }
};

/**
 * @brief RAII wrapper for SIMD-aligned file buffers.
 *
 * FileBuffer provides automatic memory management for buffers loaded with
 * load_file() or allocated with allocate_padded_buffer(). It ensures proper
 * cleanup using aligned_free() and supports move semantics for efficient
 * transfer of ownership.
 *
 * The buffer is cache-line aligned (64 bytes) with additional padding for
 * safe SIMD overreads. This allows SIMD operations to read beyond the actual
 * data length without bounds checking.
 *
 * @note FileBuffer is move-only. Copy operations are deleted to prevent
 *       accidental double-free or shallow copy issues.
 *
 * @example
 * @code
 * // Load a file using the convenience function
 * libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
 *
 * if (buffer) {  // Check if valid using operator bool
 *     std::cout << "Loaded " << buffer.size() << " bytes\n";
 *
 *     // Access data
 *     const uint8_t* data = buffer.data();
 *
 *     // Parse with libvroom
 *     libvroom::Parser parser;
 *     auto result = parser.parse(data, buffer.size());
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see load_file() To create a FileBuffer from a file path.
 * @see allocate_padded_buffer() For manual buffer allocation.
 */
class FileBuffer {
public:
  /// Default constructor. Creates an empty, invalid buffer.
  FileBuffer() : data_(nullptr), size_(0) {}
  /**
   * @brief Construct a FileBuffer from raw data.
   * @param data Pointer to SIMD-aligned buffer (takes ownership).
   * @param size Size of the data in bytes.
   * @warning The data pointer must have been allocated with aligned_malloc()
   *          or allocate_padded_buffer(). The FileBuffer takes ownership.
   */
  FileBuffer(uint8_t* data, size_t size) : data_(data), size_(size) {}

  /**
   * @brief Move constructor.
   * @param other The FileBuffer to move from.
   */
  FileBuffer(FileBuffer&& other) noexcept : data_(other.data_), size_(other.size_) {
    other.data_ = nullptr;
    other.size_ = 0;
  }
  /**
   * @brief Move assignment operator.
   * @param other The FileBuffer to move from.
   * @return Reference to this buffer.
   */
  FileBuffer& operator=(FileBuffer&& other) noexcept {
    if (this != &other) {
      free();
      data_ = other.data_;
      size_ = other.size_;
      other.data_ = nullptr;
      other.size_ = 0;
    }
    return *this;
  }

  // Copy operations deleted to prevent double-free
  FileBuffer(const FileBuffer&) = delete;
  FileBuffer& operator=(const FileBuffer&) = delete;

  /// Destructor. Frees the buffer using aligned_free().
  ~FileBuffer() { free(); }

  /// @return Const pointer to the buffer data.
  const uint8_t* data() const { return data_; }

  /// @return Mutable pointer to the buffer data.
  uint8_t* data() { return data_; }

  /// @return Size of the data in bytes (not including padding).
  size_t size() const { return size_; }

  /// @return true if the buffer contains valid data.
  bool valid() const { return data_ != nullptr; }

  /// @return true if the buffer is empty (size == 0).
  bool empty() const { return size_ == 0; }

  /// @return true if the buffer is valid, enabling `if (buffer)` syntax.
  explicit operator bool() const { return valid(); }

  /**
   * Release ownership of the buffer and return the raw pointer.
   * After calling this method, the FileBuffer no longer owns the memory
   * and the caller is responsible for freeing it using aligned_free().
   * @return The raw pointer to the buffer data, or nullptr if the buffer was
   * empty/invalid.
   */
  uint8_t* release() {
    uint8_t* ptr = data_;
    data_ = nullptr;
    size_ = 0;
    return ptr;
  }

private:
  void free() {
    if (data_) {
      aligned_free(data_);
      data_ = nullptr;
      size_ = 0;
    }
  }
  uint8_t* data_;
  size_t size_;
};

/**
 * @brief Loads a file into a FileBuffer with SIMD-aligned memory.
 *
 * This function loads the entire file contents into a newly allocated buffer
 * that is cache-line aligned with padding for safe SIMD operations. The
 * FileBuffer takes ownership of the allocated memory and will free it when
 * destroyed.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return FileBuffer containing the file data. Check valid() for success.
 * @throws std::runtime_error if file cannot be opened or read.
 *
 * @note Memory ownership is transferred to FileBuffer - do not manually free.
 */
inline FileBuffer load_file(const std::string& filename, size_t padding = 64) {
  auto [buffer, size] = read_file(filename, padding);
  return FileBuffer(buffer.release(), size);
}

/**
 * @brief Result of loading a file with RAII memory management.
 *
 * Combines an AlignedPtr (owning the buffer) with size information.
 * This provides RAII semantics while also tracking the data size
 * (since padding is allocated but not counted in the logical size).
 *
 * @example
 * @code
 * auto [buffer, size] = libvroom::load_file_to_ptr("data.csv");
 * if (buffer) {
 *     parser.parse(buffer.get(), size);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 */
struct AlignedBuffer {
  AlignedPtr ptr; ///< Smart pointer owning the buffer
  size_t size{0}; ///< Size of the data (not including padding)

  /// Default constructor creates an empty, invalid buffer.
  AlignedBuffer() = default;

  /// Construct from pointer and size.
  AlignedBuffer(AlignedPtr p, size_t s) : ptr(std::move(p)), size(s) {}

  /// Move constructor.
  AlignedBuffer(AlignedBuffer&&) = default;

  /// Move assignment.
  AlignedBuffer& operator=(AlignedBuffer&&) = default;

  // Non-copyable
  AlignedBuffer(const AlignedBuffer&) = delete;
  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  /// @return true if the buffer is valid.
  explicit operator bool() const { return ptr != nullptr; }

  /// @return Pointer to the buffer data.
  uint8_t* data() { return ptr.get(); }

  /// @return Const pointer to the buffer data.
  const uint8_t* data() const { return ptr.get(); }

  /// @return true if the buffer is empty.
  bool empty() const { return size == 0; }

  /// @return true if the buffer is valid.
  bool valid() const { return ptr != nullptr; }

  /// Release ownership and return the raw pointer.
  uint8_t* release() {
    size = 0;
    return ptr.release();
  }
};

/**
 * @brief Loads a file into an AlignedBuffer with RAII memory management.
 *
 * This function provides an alternative to load_file() that returns an
 * AlignedBuffer (using AlignedPtr internally) instead of FileBuffer.
 * Both approaches provide automatic memory management; AlignedBuffer
 * exposes the underlying smart pointer type for compatibility with
 * code that works with AlignedPtr directly.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return AlignedBuffer containing the file data. Check with if(buffer) or
 * valid().
 * @throws std::runtime_error if file cannot be opened or read.
 *
 * @example
 * @code
 * auto buffer = libvroom::load_file_to_ptr("data.csv");
 * if (buffer) {
 *     libvroom::Parser parser;
 *     auto result = parser.parse(buffer.data(), buffer.size);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see load_file() For FileBuffer-based loading.
 * @see load_stdin_to_ptr() For reading from stdin.
 */
inline AlignedBuffer load_file_to_ptr(const std::string& filename, size_t padding = 64) {
  auto [ptr, size] = read_file(filename, padding);
  return AlignedBuffer(std::move(ptr), size);
}

/**
 * @brief Loads stdin into an AlignedBuffer with RAII memory management.
 *
 * Reads all data from standard input into an RAII-managed buffer.
 * Useful for piping data into CSV processing tools.
 *
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return AlignedBuffer containing the stdin data. Check with if(buffer) or
 * valid().
 * @throws std::runtime_error if reading fails or allocation fails.
 *
 * @example
 * @code
 * // cat data.csv | ./my_program
 * auto buffer = libvroom::load_stdin_to_ptr();
 * if (buffer) {
 *     libvroom::Parser parser;
 *     auto result = parser.parse(buffer.data(), buffer.size);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see load_file_to_ptr() For loading from files.
 * @see read_stdin() For lower-level access.
 */
inline AlignedBuffer load_stdin_to_ptr(size_t padding = 64) {
  auto [ptr, size] = read_stdin(padding);
  return AlignedBuffer(std::move(ptr), size);
}

/**
 * @brief Internal UTF-8 validation function.
 *
 * Validates UTF-8 encoding and reports any invalid byte sequences to the
 * error collector. This function implements the UTF-8 state machine to
 * detect encoding errors including:
 * - Invalid leading bytes
 * - Truncated multi-byte sequences
 * - Overlong encodings
 * - Surrogate code points (U+D800-U+DFFF)
 * - Code points exceeding U+10FFFF
 *
 * @param buf Pointer to data buffer
 * @param len Length of data in bytes
 * @param errors ErrorCollector to receive validation errors
 *
 * @note This is an internal function used by Parser when
 * SizeLimits::validate_utf8 is true.
 */
inline void validate_utf8_internal(const uint8_t* buf, size_t len, ErrorCollector& errors) {
  size_t line = 1;
  size_t column = 1;
  size_t i = 0;

  while (i < len) {
    // Track line/column for error reporting
    if (buf[i] == '\n') {
      line++;
      column = 1;
      i++;
      continue;
    }
    if (buf[i] == '\r') {
      // Handle CRLF
      if (i + 1 < len && buf[i + 1] == '\n') {
        i++; // Skip \r, let \n be handled next iteration
      } else {
        line++;
        column = 1;
      }
      i++;
      continue;
    }

    // Check for valid UTF-8 sequences
    uint8_t byte = buf[i];

    if ((byte & 0x80) == 0) {
      // Single-byte ASCII (0xxxxxxx)
      column++;
      i++;
    } else if ((byte & 0xE0) == 0xC0) {
      // Two-byte sequence (110xxxxx 10xxxxxx)
      if (i + 1 >= len || (buf[i + 1] & 0xC0) != 0x80) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: truncated 2-byte sequence");
        if (errors.should_stop())
          return;
        column++;
        i++;
        continue;
      }
      // Check for overlong encoding (code points < 0x80 encoded as 2 bytes)
      if ((byte & 0x1E) == 0) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: overlong 2-byte encoding");
        if (errors.should_stop())
          return;
      }
      column++;
      i += 2;
    } else if ((byte & 0xF0) == 0xE0) {
      // Three-byte sequence (1110xxxx 10xxxxxx 10xxxxxx)
      if (i + 2 >= len || (buf[i + 1] & 0xC0) != 0x80 || (buf[i + 2] & 0xC0) != 0x80) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: truncated 3-byte sequence");
        if (errors.should_stop())
          return;
        column++;
        i++;
        continue;
      }
      // Check for overlong encoding and surrogate code points
      uint32_t cp = ((byte & 0x0F) << 12) | ((buf[i + 1] & 0x3F) << 6) | (buf[i + 2] & 0x3F);
      if (cp < 0x800) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: overlong 3-byte encoding");
        if (errors.should_stop())
          return;
      } else if (cp >= 0xD800 && cp <= 0xDFFF) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: surrogate code point");
        if (errors.should_stop())
          return;
      }
      column++;
      i += 3;
    } else if ((byte & 0xF8) == 0xF0) {
      // Four-byte sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
      if (i + 3 >= len || (buf[i + 1] & 0xC0) != 0x80 || (buf[i + 2] & 0xC0) != 0x80 ||
          (buf[i + 3] & 0xC0) != 0x80) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: truncated 4-byte sequence");
        if (errors.should_stop())
          return;
        column++;
        i++;
        continue;
      }
      // Check for overlong encoding and code points > U+10FFFF
      uint32_t cp = ((byte & 0x07) << 18) | ((buf[i + 1] & 0x3F) << 12) |
                    ((buf[i + 2] & 0x3F) << 6) | (buf[i + 3] & 0x3F);
      if (cp < 0x10000) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: overlong 4-byte encoding");
        if (errors.should_stop())
          return;
      } else if (cp > 0x10FFFF) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                         "Invalid UTF-8 sequence: code point exceeds U+10FFFF");
        if (errors.should_stop())
          return;
      }
      column++;
      i += 4;
    } else {
      // Invalid leading byte (10xxxxxx continuation byte without leading byte,
      // or invalid 5/6-byte sequence starts 111110xx/1111110x)
      errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::RECOVERABLE, line, column, i,
                       "Invalid UTF-8 sequence: invalid leading byte");
      if (errors.should_stop())
        return;
      column++;
      i++;
    }
  }
}

/**
 * @brief High-level CSV parser with automatic index management.
 *
 * Parser provides a simplified interface over the lower-level TwoPass class.
 * It manages index allocation internally and returns a Result object containing
 * the parsed index, dialect information, and success status.
 *
 * The Parser supports:
 * - Single-threaded and multi-threaded parsing
 * - Explicit dialect specification or auto-detection
 * - Error collection in permissive mode
 *
 * @note For maximum performance with manual control, use TwoPass directly.
 *       Parser is designed for convenience and typical use cases.
 *
 * @example
 * @code
 * #include "libvroom.h"
 *
 * // Load CSV file
 * libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
 *
 * // Create parser with 4 threads
 * libvroom::Parser parser(4);
 *
 * // Parse with default CSV dialect
 * auto result = parser.parse(buffer.data(), buffer.size());
 *
 * if (result.success()) {
 *     std::cout << "Parsed " << result.num_columns() << " columns\n";
 *     std::cout << "Total indexes: " << result.total_indexes() << "\n";
 * }
 * @endcode
 *
 * @example
 * @code
 * // Parse with auto-detection and error collection
 * libvroom::Parser parser(4);
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 *
 * auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors});
 *
 * std::cout << "Detected dialect: " << result.dialect.to_string() << "\n";
 *
 * if (result.has_errors()) {
 *     for (const auto& err : result.errors()) {
 *         std::cerr << err.to_string() << "\n";
 *     }
 * }
 * @endcode
 *
 * @see TwoPass For lower-level parsing with full control.
 * @see FileBuffer For loading CSV files.
 * @see Dialect For dialect configuration options.
 */
class Parser {
public:
  /**
   * @brief A single row in a parsed CSV result.
   *
   * Row provides access to individual fields within a row by column index or
   * name. It supports type-safe value extraction with automatic type
   * conversion.
   *
   * @note Row objects are lightweight views that do not own the underlying
   * data. They remain valid only as long as the parent Result object exists.
   */
  class Row {
  public:
    Row(const ValueExtractor* extractor, size_t row_index,
        const std::unordered_map<std::string, size_t>* column_map)
        : extractor_(extractor), row_index_(row_index), column_map_(column_map) {}

    /**
     * @brief Get a field value by column index with type conversion.
     *
     * @tparam T The type to convert to (int32_t, int64_t, double, bool,
     * std::string)
     * @param col Column index (0-based)
     * @return ExtractResult<T> containing the value or error/NA status
     *
     * @example
     * @code
     * auto age = row.get<int>(1);
     * if (age.ok()) {
     *     std::cout << "Age: " << age.get() << "\n";
     * }
     * @endcode
     */
    template <typename T> ExtractResult<T> get(size_t col) const {
      return extractor_->get<T>(row_index_, col);
    }

    /**
     * @brief Get a field value by column name with type conversion.
     *
     * @tparam T The type to convert to (int32_t, int64_t, double, bool,
     * std::string)
     * @param name Column name (must match header exactly)
     * @return ExtractResult<T> containing the value or error/NA status
     * @throws std::out_of_range if column name is not found
     *
     * @example
     * @code
     * auto name = row.get<std::string>("name");
     * auto age = row.get<int>("age");
     * @endcode
     */
    template <typename T> ExtractResult<T> get(const std::string& name) const {
      auto it = column_map_->find(name);
      if (it == column_map_->end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return extractor_->get<T>(row_index_, it->second);
    }

    /**
     * @brief Get a string view of a field by column index.
     *
     * This is the most efficient way to access string data as it avoids
     * copying. The returned view is valid only as long as the parent Result
     * exists.
     *
     * @param col Column index (0-based)
     * @return std::string_view of the field contents (quotes stripped)
     */
    std::string_view get_string_view(size_t col) const {
      return extractor_->get_string_view(row_index_, col);
    }

    /**
     * @brief Get a string view of a field by column name.
     * @param name Column name
     * @return std::string_view of the field contents
     * @throws std::out_of_range if column name is not found
     */
    std::string_view get_string_view(const std::string& name) const {
      auto it = column_map_->find(name);
      if (it == column_map_->end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return extractor_->get_string_view(row_index_, it->second);
    }

    /**
     * @brief Get a copy of a field as a string by column index.
     *
     * This handles unescaping of quoted fields (converting "" to ").
     *
     * @param col Column index (0-based)
     * @return std::string with the field value
     */
    std::string get_string(size_t col) const { return extractor_->get_string(row_index_, col); }

    /**
     * @brief Get a copy of a field as a string by column name.
     * @param name Column name
     * @return std::string with the field value
     * @throws std::out_of_range if column name is not found
     */
    std::string get_string(const std::string& name) const {
      auto it = column_map_->find(name);
      if (it == column_map_->end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return extractor_->get_string(row_index_, it->second);
    }

    /// @return The number of columns in this row.
    size_t num_columns() const { return extractor_->num_columns(); }

    /// @return The 0-based row index.
    size_t row_index() const { return row_index_; }

  private:
    const ValueExtractor* extractor_;
    size_t row_index_;
    const std::unordered_map<std::string, size_t>* column_map_;
  };

  /**
   * @brief Iterator for iterating over rows in a parsed CSV result.
   *
   * RowIterator is a forward iterator that yields Row objects for each data
   * row. It skips the header row automatically when has_header is true.
   */
  class ResultRowIterator {
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = Row;
    using difference_type = std::ptrdiff_t;
    using pointer = Row*;
    using reference = Row;

    ResultRowIterator(const ValueExtractor* extractor, size_t row,
                      const std::unordered_map<std::string, size_t>* column_map)
        : extractor_(extractor), row_(row), column_map_(column_map) {}

    Row operator*() const { return Row(extractor_, row_, column_map_); }

    ResultRowIterator& operator++() {
      ++row_;
      return *this;
    }
    ResultRowIterator operator++(int) {
      auto tmp = *this;
      ++row_;
      return tmp;
    }

    bool operator==(const ResultRowIterator& other) const { return row_ == other.row_; }
    bool operator!=(const ResultRowIterator& other) const { return row_ != other.row_; }

  private:
    const ValueExtractor* extractor_;
    size_t row_;
    const std::unordered_map<std::string, size_t>* column_map_;
  };

  /**
   * @brief Iterable view over rows in a parsed CSV result.
   *
   * RowView provides begin() and end() iterators for use in range-based for
   * loops.
   */
  class RowView {
  public:
    RowView(const ValueExtractor* extractor,
            const std::unordered_map<std::string, size_t>* column_map)
        : extractor_(extractor), column_map_(column_map) {}

    ResultRowIterator begin() const { return ResultRowIterator(extractor_, 0, column_map_); }

    ResultRowIterator end() const {
      return ResultRowIterator(extractor_, extractor_->num_rows(), column_map_);
    }

    /// @return The number of rows in this view.
    size_t size() const { return extractor_->num_rows(); }

    /// @return true if there are no data rows.
    bool empty() const { return extractor_->num_rows() == 0; }

  private:
    const ValueExtractor* extractor_;
    const std::unordered_map<std::string, size_t>* column_map_;
  };

  // Forward declaration for FilteredRowView
  class FilteredRowView;

  /**
   * @brief Iterator for filtered row view (supports skip/n_max/skip_empty_rows).
   *
   * This iterator is used internally by FilteredRowView to iterate over rows
   * with filtering applied.
   */
  class FilteredRowIterator {
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = Row;
    using difference_type = std::ptrdiff_t;
    using pointer = Row*;
    using reference = Row;

    FilteredRowIterator(const ValueExtractor* extractor, size_t idx, size_t total,
                        const std::unordered_map<std::string, size_t>* column_map, size_t skip,
                        size_t n_max, bool skip_empty_rows)
        : extractor_(extractor), idx_(idx), total_(total), column_map_(column_map), skip_(skip),
          n_max_(n_max), skip_empty_rows_(skip_empty_rows) {
      advance_to_valid();
    }

    Row operator*() const { return Row(extractor_, current_actual_, column_map_); }

    FilteredRowIterator& operator++() {
      ++idx_;
      advance_to_valid();
      return *this;
    }

    FilteredRowIterator operator++(int) {
      FilteredRowIterator tmp = *this;
      ++(*this);
      return tmp;
    }

    bool operator==(const FilteredRowIterator& other) const { return idx_ == other.idx_; }
    bool operator!=(const FilteredRowIterator& other) const { return idx_ != other.idx_; }

  private:
    const ValueExtractor* extractor_;
    size_t idx_;   // Filtered index (0..n_max_)
    size_t total_; // Total rows available after skip
    const std::unordered_map<std::string, size_t>* column_map_;
    size_t skip_;
    size_t n_max_;
    bool skip_empty_rows_;
    size_t current_actual_{0}; // Actual row index in extractor

    bool is_row_empty(size_t actual_idx) const {
      if (!extractor_)
        return true;
      size_t ncols = extractor_->num_columns();
      for (size_t c = 0; c < ncols; ++c) {
        std::string val = extractor_->get_string(actual_idx, c);
        for (char ch : val) {
          if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n') {
            return false;
          }
        }
      }
      return true;
    }

    void advance_to_valid() {
      // Reached end?
      if (n_max_ > 0 && idx_ >= n_max_) {
        idx_ = total_; // Mark as end
        return;
      }

      if (!skip_empty_rows_) {
        // Simple case: direct mapping
        size_t actual = skip_ + idx_;
        size_t extractor_total = extractor_ ? extractor_->num_rows() : 0;
        if (actual >= extractor_total) {
          idx_ = total_;
          return;
        }
        current_actual_ = actual;
        return;
      }

      // Complex case: skip empty rows
      size_t extractor_total = extractor_ ? extractor_->num_rows() : 0;
      size_t count = 0;
      for (size_t i = skip_; i < extractor_total; ++i) {
        if (!is_row_empty(i)) {
          if (count == idx_) {
            current_actual_ = i;
            return;
          }
          ++count;
        }
      }
      // No valid row found
      idx_ = total_;
    }
  };

  /**
   * @brief Filtered iterable view over rows with skip/n_max/skip_empty_rows support.
   *
   * FilteredRowView applies row filtering before iteration, respecting:
   * - skip: Number of data rows to skip from the beginning
   * - n_max: Maximum number of rows to return (0 = unlimited)
   * - skip_empty_rows: Whether to exclude rows containing only whitespace
   */
  class FilteredRowView {
  public:
    FilteredRowView(const ValueExtractor* extractor,
                    const std::unordered_map<std::string, size_t>* column_map, size_t skip,
                    size_t n_max, bool skip_empty_rows)
        : extractor_(extractor), column_map_(column_map), skip_(skip), n_max_(n_max),
          skip_empty_rows_(skip_empty_rows) {
      compute_size();
    }

    FilteredRowIterator begin() const {
      return FilteredRowIterator(extractor_, 0, size_, column_map_, skip_, n_max_,
                                 skip_empty_rows_);
    }

    FilteredRowIterator end() const {
      return FilteredRowIterator(extractor_, size_, size_, column_map_, skip_, n_max_,
                                 skip_empty_rows_);
    }

    /// @return The number of rows after filtering.
    size_t size() const { return size_; }

    /// @return true if there are no rows after filtering.
    bool empty() const { return size_ == 0; }

  private:
    const ValueExtractor* extractor_;
    const std::unordered_map<std::string, size_t>* column_map_;
    size_t skip_;
    size_t n_max_;
    bool skip_empty_rows_;
    size_t size_{0};

    bool is_row_empty(size_t actual_idx) const {
      if (!extractor_)
        return true;
      size_t ncols = extractor_->num_columns();
      for (size_t c = 0; c < ncols; ++c) {
        std::string val = extractor_->get_string(actual_idx, c);
        for (char ch : val) {
          if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n') {
            return false;
          }
        }
      }
      return true;
    }

    void compute_size() {
      if (!extractor_) {
        size_ = 0;
        return;
      }

      size_t total = extractor_->num_rows();
      if (skip_ >= total) {
        size_ = 0;
        return;
      }
      size_t available = total - skip_;

      if (!skip_empty_rows_) {
        size_ = (n_max_ > 0 && n_max_ < available) ? n_max_ : available;
        return;
      }

      // Count non-empty rows
      size_t count = 0;
      size_t max_to_count = (n_max_ > 0) ? n_max_ : SIZE_MAX;
      for (size_t i = skip_; i < total && count < max_to_count; ++i) {
        if (!is_row_empty(i)) {
          ++count;
        }
      }
      size_ = count;
    }
  };

  /**
   * @brief Result of a parsing operation.
   *
   * Contains the parsed index, dialect used (or detected), and success status.
   * This structure is move-only since the underlying index contains raw
   * pointers.
   *
   * Result provides a convenient API for iterating over rows and accessing
   * columns, as well as integrated error handling through the built-in
   * ErrorCollector.
   *
   * @example Row iteration
   * @code
   * auto result = parser.parse(buffer.data(), buffer.size());
   * for (auto row : result.rows()) {
   *     auto name = row.get<std::string>("name");
   *     auto age = row.get<int>("age");
   *     if (name.ok() && age.ok()) {
   *         std::cout << name.get() << " is " << age.get() << " years old\n";
   *     }
   * }
   * @endcode
   *
   * @example Column extraction
   * @code
   * auto names = result.column<std::string>("name");
   * auto ages = result.column<int64_t>("age");
   * @endcode
   *
   * @example Error handling (unified API)
   * @code
   * auto result = parser.parse(buffer.data(), buffer.size());
   * if (result.has_errors()) {
   *     std::cerr << result.error_summary() << std::endl;
   *     for (const auto& err : result.errors()) {
   *         std::cerr << err.to_string() << std::endl;
   *     }
   * }
   * @endcode
   */
  struct Result {
    ParseIndex idx;            ///< The parsed field index.
    bool successful{false};    ///< Whether parsing completed without fatal errors.
    Dialect dialect;           ///< The dialect used for parsing.
    DetectionResult detection; ///< Detection result (populated by parse_auto).
    bool used_cache{false};    ///< True if index was loaded from cache.
    std::string cache_path;    ///< Path to cache file (empty if caching disabled).

    // Row filtering options (from ParseOptions, applied during iteration)
    size_t skip_{0};              ///< Number of data rows to skip
    size_t n_max_{0};             ///< Maximum rows to return (0 = unlimited)
    bool skip_empty_rows_{false}; ///< Whether to skip empty rows

  private:
    const uint8_t* buf_{nullptr};                                ///< Pointer to the parsed buffer.
    size_t len_{0};                                              ///< Length of the parsed buffer.
    mutable std::unique_ptr<ValueExtractor> extractor_;          ///< Lazy-initialized extractor.
    mutable std::unordered_map<std::string, size_t> column_map_; ///< Column name to index map.
    mutable bool column_map_initialized_{false};
    /// Internal error collector for unified error handling.
    /// Uses PERMISSIVE mode by default to collect all errors without stopping.
    ErrorCollector error_collector_{ErrorMode::PERMISSIVE};
    /// Global extraction configuration for value parsing.
    ExtractionConfig extraction_config_;
    /// Per-column configuration overrides.
    ColumnConfigMap column_configs_;

    void ensure_extractor() const {
      if (!extractor_ && buf_ && len_ > 0) {
        if (!column_configs_.empty()) {
          extractor_ = std::make_unique<ValueExtractor>(buf_, len_, idx, dialect,
                                                        extraction_config_, column_configs_);
        } else {
          extractor_ =
              std::make_unique<ValueExtractor>(buf_, len_, idx, dialect, extraction_config_);
        }
      }
    }

    void ensure_column_map() const {
      if (!column_map_initialized_) {
        ensure_extractor();
        if (extractor_ && extractor_->has_header()) {
          auto headers = extractor_->get_header();
          for (size_t i = 0; i < headers.size(); ++i) {
            column_map_[headers[i]] = i;
          }
        }
        column_map_initialized_ = true;
      }
    }

    /// Check if a row (by actual index in extractor) is empty or whitespace-only
    bool is_row_empty(size_t actual_row_idx) const {
      if (!extractor_)
        return true;
      size_t ncols = extractor_->num_columns();
      for (size_t c = 0; c < ncols; ++c) {
        std::string val = extractor_->get_string(actual_row_idx, c);
        // Check if any character is non-whitespace
        for (char ch : val) {
          if (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n') {
            return false;
          }
        }
      }
      return true;
    }

    /// Translate a filtered row index to actual extractor row index
    /// Returns SIZE_MAX if the index is out of range
    size_t translate_row_index(size_t filtered_idx) const {
      if (!skip_empty_rows_) {
        // Simple case: just add skip offset
        size_t actual = skip_ + filtered_idx;
        size_t total = extractor_ ? extractor_->num_rows() : 0;
        if (actual >= total)
          return SIZE_MAX;
        if (n_max_ > 0 && filtered_idx >= n_max_)
          return SIZE_MAX;
        return actual;
      }

      // Complex case: need to skip empty rows
      size_t total = extractor_ ? extractor_->num_rows() : 0;
      size_t count = 0;
      size_t limit = (n_max_ > 0) ? n_max_ : SIZE_MAX;
      for (size_t i = skip_; i < total && count < limit; ++i) {
        if (!is_row_empty(i)) {
          if (count == filtered_idx) {
            return i;
          }
          ++count;
        }
      }
      return SIZE_MAX;
    }

  public:
    Result() = default;

    // Custom move operations to reset the extractor (its idx_ptr_ would become dangling)
    Result(Result&& other) noexcept
        : idx(std::move(other.idx)), successful(other.successful), dialect(other.dialect),
          detection(std::move(other.detection)), used_cache(other.used_cache),
          cache_path(std::move(other.cache_path)), skip_(other.skip_), n_max_(other.n_max_),
          skip_empty_rows_(other.skip_empty_rows_), buf_(other.buf_), len_(other.len_),
          extractor_(nullptr), // Reset - will be recreated lazily with correct pointer
          column_map_(std::move(other.column_map_)),
          column_map_initialized_(other.column_map_initialized_),
          error_collector_(std::move(other.error_collector_)),
          extraction_config_(other.extraction_config_),
          column_configs_(std::move(other.column_configs_)) {
      other.buf_ = nullptr;
      other.len_ = 0;
    }

    Result& operator=(Result&& other) noexcept {
      if (this != &other) {
        idx = std::move(other.idx);
        successful = other.successful;
        dialect = other.dialect;
        detection = std::move(other.detection);
        used_cache = other.used_cache;
        cache_path = std::move(other.cache_path);
        skip_ = other.skip_;
        n_max_ = other.n_max_;
        skip_empty_rows_ = other.skip_empty_rows_;
        buf_ = other.buf_;
        len_ = other.len_;
        extractor_ = nullptr; // Reset - will be recreated lazily with correct pointer
        column_map_ = std::move(other.column_map_);
        column_map_initialized_ = other.column_map_initialized_;
        error_collector_ = std::move(other.error_collector_);
        extraction_config_ = other.extraction_config_;
        column_configs_ = std::move(other.column_configs_);
        other.buf_ = nullptr;
        other.len_ = 0;
      }
      return *this;
    }

    // Prevent copying - index contains raw pointers
    Result(const Result&) = delete;
    Result& operator=(const Result&) = delete;

    /**
     * @brief Store buffer reference and row filtering options for later iteration.
     *
     * This is called internally by Parser::parse() to enable row iteration.
     * Users should not call this directly.
     *
     * @param buf Pointer to the CSV data buffer.
     * @param len Length of the buffer.
     * @param skip Number of data rows to skip (default: 0)
     * @param n_max Maximum rows to return, 0 = unlimited (default: 0)
     * @param skip_empty_rows Whether to skip empty rows (default: false)
     */
    void set_buffer(const uint8_t* buf, size_t len, size_t skip = 0, size_t n_max = 0,
                    bool skip_empty_rows = false) {
      buf_ = buf;
      len_ = len;
      skip_ = skip;
      n_max_ = n_max;
      skip_empty_rows_ = skip_empty_rows;
      // Reset extractor and column map since buffer changed
      extractor_.reset();
      column_map_.clear();
      column_map_initialized_ = false;
    }

    /**
     * @brief Set extraction configuration options.
     *
     * This is called internally by Parser::parse() to pass configuration from
     * ParseOptions. Users should set options on ParseOptions before parsing.
     *
     * @param config Global extraction configuration
     * @param column_configs Per-column configuration overrides
     */
    void set_extraction_options(const ExtractionConfig& config,
                                const ColumnConfigMap& column_configs) {
      extraction_config_ = config;
      column_configs_ = column_configs;
      // Reset extractor since config changed
      if (extractor_) {
        extractor_.reset();
      }
    }

    // =========================================================================
    // Per-column configuration API
    // =========================================================================

    /**
     * @brief Get the per-column configuration map.
     * @return Reference to the ColumnConfigMap
     */
    const ColumnConfigMap& column_configs() const { return column_configs_; }

    /**
     * @brief Set per-column configuration after parsing.
     *
     * This allows modifying column configurations after parsing,
     * which will affect subsequent value extraction.
     *
     * @param configs ColumnConfigMap with per-column settings
     */
    void set_column_configs(const ColumnConfigMap& configs) {
      column_configs_ = configs;
      // Reset extractor so new configs take effect
      if (extractor_) {
        extractor_.reset();
      }
    }

    /**
     * @brief Set configuration for a specific column by index.
     * @param col_index 0-based column index
     * @param config Configuration to apply
     */
    void set_column_config(size_t col_index, const ColumnConfig& config) {
      column_configs_.set(col_index, config);
      // Update extractor if it exists
      if (extractor_) {
        extractor_->set_column_config(col_index, config);
      }
    }

    /**
     * @brief Set configuration for a specific column by name.
     * @param col_name Column name (case-sensitive)
     * @param config Configuration to apply
     */
    void set_column_config(const std::string& col_name, const ColumnConfig& config) {
      column_configs_.set(col_name, config);
      // Update extractor if it exists
      if (extractor_) {
        extractor_->set_column_config(col_name, config);
      }
    }

    /**
     * @brief Get the type hint for a specific column.
     * @param col_index 0-based column index
     * @return The type hint, or TypeHint::AUTO if none is set
     */
    TypeHint get_type_hint(size_t col_index) const {
      ensure_extractor();
      return extractor_ ? extractor_->get_type_hint(col_index) : TypeHint::AUTO;
    }

    /**
     * @brief Check if a column should be skipped during extraction.
     * @param col_index 0-based column index
     * @return true if the column has TypeHint::SKIP
     */
    bool should_skip_column(size_t col_index) const {
      return get_type_hint(col_index) == TypeHint::SKIP;
    }

    /**
     * @brief Get the global extraction configuration.
     * @return Reference to ExtractionConfig
     */
    const ExtractionConfig& extraction_config() const { return extraction_config_; }

    /// @return true if parsing was successful.
    bool success() const { return successful; }

    /// @return Number of columns detected in the CSV.
    size_t num_columns() const {
      ensure_extractor();
      return extractor_ ? extractor_->num_columns() : idx.columns;
    }

    /**
     * @brief Get total number of field separator positions found.
     * @return Sum of indexes across all parsing threads.
     */
    size_t total_indexes() const {
      if (!idx.n_indexes)
        return 0;
      size_t total = 0;
      for (uint16_t t = 0; t < idx.n_threads; ++t) {
        total += idx.n_indexes[t];
      }
      return total;
    }

    /**
     * @brief Compact the index for O(1) field access.
     *
     * After parsing, field separators are stored in per-thread regions which
     * require O(n_threads) iteration to find a specific field. This method
     * consolidates all separators into a single flat array sorted by file order,
     * enabling O(1) random access.
     *
     * This is particularly beneficial for ALTREP-style lazy column access where
     * fields are accessed randomly. For sequential column extraction, the
     * performance improvement is less significant.
     *
     * @note This method is idempotent - calling it multiple times has no effect
     *       after the first successful call.
     *
     * @note Results loaded from cache are automatically in flat format and
     *       don't need explicit compaction.
     *
     * @example
     * @code
     * auto result = parser.parse(buf, len);
     *
     * // Compact for O(1) random access
     * result.compact();
     *
     * // Now LazyColumn access is O(1) per field
     * auto lazy_col = result.get_lazy_column(0);
     * auto value = lazy_col[1000];  // O(1) instead of O(n_threads)
     * @endcode
     *
     * @see is_flat() to check if the index is already compacted
     */
    void compact() { idx.compact(); }

    /**
     * @brief Check if the index has been compacted for O(1) access.
     *
     * Returns true if the index is in flat format (either from calling
     * compact() or from loading a cached index).
     *
     * @return true if the index has O(1) field access, false otherwise.
     *
     * @see compact() to convert an index to flat format
     */
    bool is_flat() const { return idx.is_flat(); }

    // =====================================================================
    // Row/Column Iteration API
    // =====================================================================

    /**
     * @brief Get the effective number of data rows after applying row filtering.
     *
     * The returned count reflects:
     * - Rows after skipping `skip_` initial rows
     * - Limited by `n_max_` if set
     * - Empty rows excluded if `skip_empty_rows_` is true
     *
     * @note When skip_empty_rows_ is true, this requires scanning all rows
     *       to count non-empty ones, which has O(n) cost.
     *
     * @return Number of data rows (excluding header and filtered rows).
     */
    size_t num_rows() const {
      ensure_extractor();
      if (!extractor_)
        return 0;

      size_t total = extractor_->num_rows();

      // If no filtering, return total
      if (skip_ == 0 && n_max_ == 0 && !skip_empty_rows_) {
        return total;
      }

      // Apply skip
      if (skip_ >= total) {
        return 0;
      }
      size_t available = total - skip_;

      // If we don't need to skip empty rows, just apply n_max
      if (!skip_empty_rows_) {
        if (n_max_ > 0 && n_max_ < available) {
          return n_max_;
        }
        return available;
      }

      // With skip_empty_rows, we need to count non-empty rows
      size_t count = 0;
      size_t max_to_check = (n_max_ > 0) ? n_max_ : available;
      for (size_t i = 0; i < available && count < max_to_check; ++i) {
        size_t actual_idx = skip_ + i;
        if (!is_row_empty(actual_idx)) {
          ++count;
        }
      }
      return count;
    }

    /**
     * @brief Get the total number of rows before filtering.
     *
     * This returns the raw row count from parsing, before applying
     * skip/n_max/skip_empty_rows filters.
     *
     * @return Total number of data rows in the parsed index.
     */
    size_t total_rows() const {
      ensure_extractor();
      return extractor_ ? extractor_->num_rows() : 0;
    }

    /**
     * @brief Get an iterable view over all data rows (respects skip/n_max/skip_empty_rows).
     *
     * This enables range-based for loop iteration over the parsed CSV.
     * When row filtering options are set, this returns a filtered view.
     *
     * @return FilteredRowView for iteration (applies skip/n_max/skip_empty_rows).
     *
     * @example
     * @code
     * for (auto row : result.rows()) {
     *     std::cout << row.get_string(0) << "\n";
     * }
     * @endcode
     */
    FilteredRowView rows() const {
      ensure_extractor();
      ensure_column_map();
      return FilteredRowView(extractor_.get(), &column_map_, skip_, n_max_, skip_empty_rows_);
    }

    /**
     * @brief Get an unfiltered iterable view over all data rows.
     *
     * This returns a view that ignores skip/n_max/skip_empty_rows settings,
     * iterating over all parsed rows. Useful when you need to access the
     * raw parsed data.
     *
     * @return RowView for iteration (no filtering applied).
     */
    RowView all_rows() const {
      ensure_extractor();
      ensure_column_map();
      return RowView(extractor_.get(), &column_map_);
    }

    /**
     * @brief Get a specific row by index (respects skip/n_max/skip_empty_rows).
     *
     * @param row_index 0-based row index (excluding header and filtered rows).
     * @return Row object for accessing fields.
     * @throws std::out_of_range if row_index >= num_rows().
     */
    Row row(size_t row_index) const {
      ensure_extractor();
      ensure_column_map();
      size_t actual_idx = translate_row_index(row_index);
      if (actual_idx == SIZE_MAX) {
        throw std::out_of_range("Row index out of range");
      }
      return Row(extractor_.get(), actual_idx, &column_map_);
    }

    /**
     * @brief Extract an entire column as a vector of optional values.
     *
     * @tparam T The type to convert values to (int32_t, int64_t, double, bool).
     * @param col Column index (0-based).
     * @return Vector of optional values (nullopt for NA/missing values).
     *
     * @example
     * @code
     * auto ages = result.column<int64_t>(1);
     * for (const auto& age : ages) {
     *     if (age) {
     *         std::cout << *age << "\n";
     *     }
     * }
     * @endcode
     */
    template <typename T> std::vector<std::optional<T>> column(size_t col) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column<T>(col) : std::vector<std::optional<T>>{};
    }

    /**
     * @brief Extract an entire column by name as a vector of optional values.
     *
     * @tparam T The type to convert values to.
     * @param name Column name (must match header exactly).
     * @return Vector of optional values.
     * @throws std::out_of_range if column name is not found.
     *
     * @example
     * @code
     * auto names = result.column<std::string>("name");
     * auto ages = result.column<int64_t>("age");
     * @endcode
     */
    template <typename T> std::vector<std::optional<T>> column(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column<T>(it->second);
    }

    /**
     * @brief Extract a column with a default value for NA/missing entries.
     *
     * @tparam T The type to convert values to.
     * @param col Column index (0-based).
     * @param default_value Value to use for NA/missing entries.
     * @return Vector of values with default substituted for NA.
     *
     * @example
     * @code
     * auto ages = result.column_or<int64_t>(1, -1);  // -1 for missing
     * @endcode
     */
    template <typename T> std::vector<T> column_or(size_t col, T default_value) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column_or<T>(col, default_value) : std::vector<T>{};
    }

    /**
     * @brief Extract a column by name with a default value for NA/missing
     * entries.
     *
     * @tparam T The type to convert values to.
     * @param name Column name.
     * @param default_value Value to use for NA/missing entries.
     * @return Vector of values with default substituted for NA.
     * @throws std::out_of_range if column name is not found.
     */
    template <typename T> std::vector<T> column_or(const std::string& name, T default_value) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column_or<T>(it->second, default_value);
    }

    /**
     * @brief Extract a string column as string_views (zero-copy).
     *
     * @param col Column index (0-based).
     * @return Vector of string_views into the original buffer.
     * @note Views are valid only as long as the original buffer exists.
     */
    std::vector<std::string_view> column_string_view(size_t col) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column_string_view(col)
                        : std::vector<std::string_view>{};
    }

    /**
     * @brief Extract a string column by name as string_views (zero-copy).
     *
     * @param name Column name.
     * @return Vector of string_views into the original buffer.
     * @throws std::out_of_range if column name is not found.
     */
    std::vector<std::string_view> column_string_view(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column_string_view(it->second);
    }

    /**
     * @brief Extract a string column as strings (with proper unescaping).
     *
     * @param col Column index (0-based).
     * @return Vector of strings with quotes and escapes processed.
     */
    std::vector<std::string> column_string(size_t col) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column_string(col) : std::vector<std::string>{};
    }

    /**
     * @brief Extract a string column by name as strings.
     *
     * @param name Column name.
     * @return Vector of strings with quotes and escapes processed.
     * @throws std::out_of_range if column name is not found.
     */
    std::vector<std::string> column_string(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column_string(it->second);
    }

    /**
     * @brief Get a lazy column accessor for ALTREP-style deferred field access.
     *
     * Creates a LazyColumn that provides per-row access to a column without
     * parsing the entire column upfront. Ideal for R's ALTREP pattern where
     * columns are only parsed when accessed.
     *
     * @param col 0-based column index.
     * @return LazyColumn accessor for the specified column.
     *
     * @example
     * @code
     * auto lazy_col = result.get_lazy_column(0);
     * for (size_t i = 0; i < lazy_col.size(); ++i) {
     *     // Access individual values on demand
     *     std::string_view value = lazy_col[i];
     * }
     * @endcode
     */
    LazyColumn get_lazy_column(size_t col) const {
      ensure_extractor();
      if (!extractor_) {
        throw std::runtime_error("Extractor not initialized");
      }
      return extractor_->get_lazy_column(col);
    }

    /**
     * @brief Get a lazy column accessor by column name.
     *
     * @param name Column name (must match header exactly).
     * @return LazyColumn accessor for the specified column.
     * @throws std::out_of_range if column name is not found.
     */
    LazyColumn get_lazy_column(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return get_lazy_column(it->second);
    }

    /**
     * @brief Get the column headers.
     *
     * @return Vector of column names from the header row.
     * @throws std::runtime_error if the CSV has no header row.
     */
    std::vector<std::string> header() const {
      ensure_extractor();
      return extractor_ ? extractor_->get_header() : std::vector<std::string>{};
    }

    /**
     * @brief Check if the CSV has a header row.
     * @return true if a header row is present.
     */
    bool has_header() const {
      ensure_extractor();
      return extractor_ ? extractor_->has_header() : true;
    }

    /**
     * @brief Set whether the CSV has a header row.
     *
     * @param has_header true if first row should be treated as header.
     */
    void set_has_header(bool has_header) {
      ensure_extractor();
      if (extractor_) {
        extractor_->set_has_header(has_header);
        // Reset column map since header status changed
        column_map_.clear();
        column_map_initialized_ = false;
      }
    }

    /**
     * @brief Get the column index for a column name.
     *
     * @param name Column name.
     * @return Column index, or std::nullopt if not found.
     */
    std::optional<size_t> column_index(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        return std::nullopt;
      }
      return it->second;
    }

    // =====================================================================
    // Error Handling API (Unified)
    // =====================================================================

    /**
     * @brief Check if any errors were recorded during parsing.
     *
     * This method provides a unified way to check for errors without
     * needing to pass an external ErrorCollector.
     *
     * @return true if at least one error was recorded.
     *
     * @example
     * @code
     * auto result = parser.parse(buffer.data(), buffer.size());
     * if (result.has_errors()) {
     *     std::cerr << "Parsing encountered errors\n";
     * }
     * @endcode
     */
    bool has_errors() const { return error_collector_.has_errors(); }

    /**
     * @brief Check if any fatal errors were recorded during parsing.
     *
     * Fatal errors indicate unrecoverable parsing failures, such as
     * unclosed quotes at end of file.
     *
     * @return true if at least one FATAL error was recorded.
     */
    bool has_fatal_errors() const { return error_collector_.has_fatal_errors(); }

    /**
     * @brief Get the number of errors recorded during parsing.
     *
     * @return Number of errors in the internal error collector.
     */
    size_t error_count() const { return error_collector_.error_count(); }

    /**
     * @brief Get read-only access to all recorded errors.
     *
     * @return Const reference to the vector of ParseError objects.
     *
     * @example
     * @code
     * auto result = parser.parse(buffer.data(), buffer.size());
     * for (const auto& err : result.errors()) {
     *     std::cerr << err.to_string() << std::endl;
     * }
     * @endcode
     */
    const std::vector<ParseError>& errors() const { return error_collector_.errors(); }

    /**
     * @brief Get a summary string of all errors.
     *
     * @return Human-readable summary of error counts by type.
     *
     * @example
     * @code
     * auto result = parser.parse(buffer.data(), buffer.size());
     * if (result.has_errors()) {
     *     std::cerr << result.error_summary() << std::endl;
     * }
     * @endcode
     */
    std::string error_summary() const { return error_collector_.summary(); }

    /**
     * @brief Get the error handling mode used during parsing.
     *
     * @return The ErrorMode of the internal error collector.
     */
    ErrorMode error_mode() const { return error_collector_.mode(); }

    /**
     * @brief Get mutable access to the internal error collector.
     *
     * This method is primarily for internal use by Parser::parse() to
     * populate errors during parsing. Users should prefer the convenience
     * methods has_errors(), errors(), etc.
     *
     * @return Reference to the internal ErrorCollector.
     */
    ErrorCollector& error_collector() { return error_collector_; }

    /**
     * @brief Get read-only access to the internal error collector.
     *
     * @return Const reference to the internal ErrorCollector.
     */
    const ErrorCollector& error_collector() const { return error_collector_; }

    // =====================================================================
    // Byte Offset Lookup API
    // =====================================================================

    /**
     * @brief Result of a byte offset to (row, column) lookup.
     *
     * Location represents the result of finding which CSV cell contains
     * a given byte offset. This enables efficient error reporting by
     * converting internal byte positions to human-readable row/column
     * coordinates.
     */
    struct Location {
      size_t row;    ///< 0-based row index (row 0 = header if present, else first data row)
      size_t column; ///< 0-based column index
      bool found;    ///< true if byte offset is within valid CSV data

      /// @return true if the location is valid (found == true)
      explicit operator bool() const { return found; }
    };

    /**
     * @brief Convert a byte offset to (row, column) coordinates.
     *
     * Uses binary search on the internal index for O(log n) lookup instead of
     * O(n) linear scan. This is useful for error reporting when you have a
     * byte offset from parsing and need to display the location to users.
     *
     * The row number returned is 0-based and includes the header row (if present).
     * So row 0 is the header (if has_header() is true), row 1 is the first data row.
     * If has_header() is false, row 0 is the first data row.
     *
     * @param byte_offset Byte offset into the CSV buffer
     * @return Location with row/column if found, or {0, 0, false} if offset is
     *         out of range or no data exists
     *
     * @note Complexity: O(log n) where n is the number of fields in the CSV
     *
     * @example
     * @code
     * auto result = parser.parse(buf, len);
     *
     * // Convert byte offset 150 to row/column
     * auto loc = result.byte_offset_to_location(150);
     * if (loc) {
     *     std::cout << "Row " << loc.row << ", Column " << loc.column << std::endl;
     * }
     * @endcode
     */
    Location byte_offset_to_location(size_t byte_offset) const {
      ensure_extractor();
      if (!extractor_) {
        return {0, 0, false};
      }
      auto ve_loc = extractor_->byte_offset_to_location(byte_offset);
      return {ve_loc.row, ve_loc.column, ve_loc.found};
    }
  };

  /**
   * @brief Construct a Parser with the specified number of threads.
   * @param num_threads Number of threads to use for parsing (default: 1).
   *                    Use std::thread::hardware_concurrency() for CPU count.
   */
  explicit Parser(size_t num_threads = 1) : num_threads_(num_threads > 0 ? num_threads : 1) {}

  /**
   * @brief Unified parse method with configurable options.
   *
   * This is the primary parsing method that handles all use cases through
   * the ParseOptions structure. It unifies the previous parse(),
   * parse_with_errors(), and parse_auto() methods into a single entry point.
   *
   * **Key Design Principle**: This method never throws exceptions for parse
   * errors. Parse errors are always returned via the Result object's
   * error_collector(). Exceptions are reserved for truly exceptional conditions
   * (e.g., memory allocation failures at the system level).
   *
   * Behavior based on options:
   * - **dialect = nullopt** (default): Auto-detect dialect from data
   * - **dialect = Dialect::xxx()**: Use the specified dialect
   * - **errors = nullptr** (default): Errors collected in result.errors()
   * - **errors = &collector**: Errors go to both external and result.errors()
   *
   * @param buf Pointer to the CSV data buffer. Must remain valid during
   * parsing. Should have at least 64 bytes of padding beyond len for SIMD
   * safety.
   * @param len Length of the CSV data in bytes (excluding any padding).
   * @param options Configuration options for parsing (default: auto-detect).
   *
   * @return Result containing the parsed index, dialect used, detection info,
   *         and any errors via result.errors().
   *
   * @note This method does NOT throw for parse errors. Check result.success()
   *       and result.has_errors() to detect parsing issues.
   *
   * @example
   * @code
   * Parser parser;
   *
   * // Simple usage - errors accessible via result
   * auto result = parser.parse(buf, len);
   * if (!result.success() || result.has_errors()) {
   *     for (const auto& err : result.errors()) {
   *         std::cerr << err.to_string() << std::endl;
   *     }
   * }
   *
   * // Explicit dialect
   * auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});
   *
   * // With external error collector (backward compatibility)
   * ErrorCollector errors(ErrorMode::PERMISSIVE);
   * auto result = parser.parse(buf, len, {.errors = &errors});
   * @endcode
   *
   * @see ParseOptions for configuration details
   * @see Result::errors() for accessing parse errors
   */
  Result parse(const uint8_t* buf, size_t len, const ParseOptions& options = ParseOptions{}) {
    Result result;

    // Configure the internal error collector with the max_errors limit
    result.error_collector().set_max_errors(options.max_errors);

    // Determine which error collector to use:
    // - If external collector provided, use it and copy errors to internal
    // collector
    // - Otherwise, use the internal collector directly (never throw for parse
    // errors)
    ErrorCollector* collector =
        options.errors != nullptr ? options.errors : &result.error_collector();

    // SECURITY: Validate file size limits before any allocation
    if (options.limits.max_file_size > 0 && len > options.limits.max_file_size) {
      collector->add_error(ErrorCode::FILE_TOO_LARGE, ErrorSeverity::FATAL, 1, 1, 0,
                           "File size " + std::to_string(len) + " bytes exceeds maximum " +
                               std::to_string(options.limits.max_file_size) + " bytes");
      if (options.errors != nullptr) {
        result.error_collector().merge_from(*options.errors);
      }
      result.successful = false;
      return result;
    }

    // UTF-8 validation (optional, enabled via SizeLimits::validate_utf8)
    if (options.limits.validate_utf8) {
      validate_utf8_internal(buf, len, *collector);
      if (collector->should_stop()) {
        if (options.errors != nullptr) {
          result.error_collector().merge_from(*options.errors);
        }
        result.successful = false;
        return result;
      }
    }

    // =======================================================================
    // Index Caching Logic
    // =======================================================================
    // Caching is only supported when:
    // 1. CacheConfig is provided (cache.has_value())
    // 2. A source file path is provided (!source_path.empty())
    bool can_use_cache = options.cache.has_value() && !options.source_path.empty();

    // Helper to emit cache warnings (captures warning callback from config)
    auto cache_warn = [&options]([[maybe_unused]] const std::string& message) {
      if (options.cache.has_value() && options.cache->warning_callback) {
        options.cache->warning_callback(message);
      }
    };

    if (can_use_cache) {
      const CacheConfig& cache_config = *options.cache;
      auto [cache_path, writable] =
          IndexCache::try_compute_writable_path(options.source_path, cache_config);
      result.cache_path = cache_path;

      // Try to load from cache (unless force_cache_refresh is set)
      if (!options.force_cache_refresh && !cache_path.empty()) {
        // Use IndexCache::load() for automatic corruption detection and cleanup
        auto load_result = IndexCache::load(cache_path, options.source_path);

        if (load_result.success()) {
          // Cache hit! Use the cached index
          result.idx = std::move(load_result.index);
          result.used_cache = true;
          result.successful = true;

          // Determine dialect for cached result
          if (options.dialect.has_value()) {
            result.dialect = options.dialect.value();
          } else {
            // Auto-detect dialect even for cached indexes
            DialectDetector detector(options.detection_options);
            result.detection = detector.detect(buf, len);
            result.dialect = result.detection.success() ? result.detection.dialect : Dialect::csv();
          }

          // Store buffer reference and row filtering options to enable row/column iteration
          result.set_buffer(buf, len, options.skip, options.n_max, options.skip_empty_rows);
          // Pass extraction options from ParseOptions to Result
          result.set_extraction_options(options.extraction_config, options.column_configs);

          return result;
        }

        // Cache miss or corruption
        // Log corruption as a warning (non-fatal) so callers are aware
        if (load_result.was_corrupted) {
          cache_warn("Cache corruption detected and file deleted: " + load_result.error_message);
        }
      }

      // Cache miss - continue with normal parsing, then write cache
    }

    // =======================================================================
    // Normal Parsing Path
    // =======================================================================

    // Determine dialect (explicit or auto-detect)
    if (options.dialect.has_value()) {
      result.dialect = options.dialect.value();
    } else {
      // Auto-detect dialect
      DialectDetector detector(options.detection_options);
      result.detection = detector.detect(buf, len);
      result.dialect = result.detection.success() ? result.detection.dialect : Dialect::csv();
    }

    // Apply comment character from ParseOptions if specified
    // This overrides any comment_char in the dialect
    if (options.comment != '\0') {
      result.dialect.comment_char = options.comment;
    }

    // =======================================================================
    // Progress Tracking Setup
    // =======================================================================
    // Create progress tracker if callback is provided
    // First pass (separator counting) is ~10% of work, second pass ~90%
    ProgressTracker progress_tracker(options.progress_callback, len, 0.1);

    // Report start of parsing (0%)
    if (progress_tracker.has_callback()) {
      if (!options.progress_callback(0, len)) {
        result.successful = false;
        return result;
      }
    }

    // Create second-pass progress callback that wraps the progress tracker
    SecondPassProgressCallback second_pass_progress = nullptr;
    if (progress_tracker.has_callback()) {
      second_pass_progress = [&progress_tracker](size_t bytes_processed) {
        return progress_tracker.add_second_pass_progress(bytes_processed);
      };
    }

    // =======================================================================
    // Fast Path Detection
    // =======================================================================
    // Performance optimization (issue #443, #591): Use fast path when:
    // 1. Explicit dialect is provided (skips detection overhead)
    // 2. No external error collector requested (options.errors == nullptr)
    //
    // For multi-threaded fast path (issue #591), use optimized per-thread
    // allocation that dramatically reduces memory usage and improves scaling.
    bool use_fast_path = options.errors == nullptr && options.dialect.has_value() &&
                         (options.algorithm == ParseAlgorithm::AUTO ||
                          options.algorithm == ParseAlgorithm::SPECULATIVE);

    // =======================================================================
    // Multi-threaded Fast Path (Issue #591 optimization)
    // =======================================================================
    // For multi-threaded parsing with explicit dialect and no error collection,
    // use the optimized parse_optimized() method which:
    // 1. Finds chunk boundaries in parallel
    // 2. Counts separators per-chunk (not total file)
    // 3. Allocates right-sized per-thread regions (~N total vs T*N)
    // 4. Parses chunks in parallel
    //
    // This avoids the redundant first pass and dramatically reduces memory.
    if (use_fast_path && num_threads_ > 1) {
      try {
        result.idx =
            parser_.parse_optimized(buf, len, num_threads_, result.dialect, second_pass_progress);
        result.successful = result.idx.indexes != nullptr;
      } catch (const std::exception& e) {
        collector->add_error(ErrorCode::INTERNAL_ERROR, ErrorSeverity::FATAL, 0, 0, 0, e.what());
        result.successful = false;
      }

      // Skip the normal allocation and parsing paths below
      if (result.successful || !result.idx.indexes) {
        // Store buffer reference and row filtering options
        result.set_buffer(buf, len, options.skip, options.n_max, options.skip_empty_rows);
        result.set_extraction_options(options.extraction_config, options.column_configs);

        // Set column count in index
        if (result.successful && result.idx.columns == 0) {
          result.idx.columns = result.num_columns();
        }

        // Compact index for O(1) field access
        if (result.successful) {
          result.idx.compact();
        }

        // Report completion
        if (options.progress_callback && result.successful) {
          options.progress_callback(len, len);
        }

        // Handle caching for optimized path
        if (can_use_cache && result.successful && !result.cache_path.empty()) {
          IndexCache::write_atomic(result.cache_path, result.idx, options.source_path);
          // Note: Cache write failures are silently ignored in optimized path
          // (consistent with how they're handled elsewhere)
        }

        return result;
      }
    }

    // =======================================================================
    // First Pass: Count separators with granular progress
    // =======================================================================
    // For granular progress during first pass, split into chunks and report
    // progress after each chunk completes.
    TwoPass::Stats count_stats;
    const size_t min_chunk_size = 1024 * 1024; // 1MB chunks for progress granularity

    if (progress_tracker.has_callback() && len > min_chunk_size * 2) {
      // Split first pass into chunks for progress reporting
      size_t n_chunks = std::min(static_cast<size_t>(100), len / min_chunk_size);
      n_chunks = std::max(n_chunks, static_cast<size_t>(1));
      size_t chunk_size = len / n_chunks;

      count_stats = {0, null_pos, null_pos, 0};

      for (size_t i = 0; i < n_chunks && !progress_tracker.is_cancelled(); ++i) {
        size_t start = i * chunk_size;
        size_t end = (i == n_chunks - 1) ? len : (i + 1) * chunk_size;

        auto chunk_stats = TwoPass::first_pass_simd(buf, start, end, result.dialect.quote_char,
                                                    result.dialect.delimiter);
        count_stats.n_separators += chunk_stats.n_separators;
        count_stats.n_quotes += chunk_stats.n_quotes;
        // Track first even/odd newlines from first chunk only
        if (i == 0) {
          count_stats.first_even_nl = chunk_stats.first_even_nl;
          count_stats.first_odd_nl = chunk_stats.first_odd_nl;
        }

        // Report progress for this chunk
        progress_tracker.add_first_pass_progress(end - start);
      }

      if (progress_tracker.is_cancelled()) {
        result.successful = false;
        return result;
      }
    } else {
      // Single-pass for small files or no progress callback
      count_stats = TwoPass::first_pass_simd(buf, 0, len, result.dialect.quote_char,
                                             result.dialect.delimiter);

      // Report first pass complete
      if (progress_tracker.has_callback()) {
        progress_tracker.add_first_pass_progress(len);
      }
    }

    result.idx = parser_.init_counted_safe(count_stats.n_separators, num_threads_, collector,
                                           count_stats.n_quotes, len);
    if (result.idx.indexes == nullptr) {
      // Allocation failed or would overflow
      if (options.errors != nullptr) {
        result.error_collector().merge_from(*options.errors);
      }
      result.successful = false;
      return result;
    }

    // =======================================================================
    // Parse with the appropriate algorithm
    // =======================================================================
    // Design principle: The error-collecting variants (_with_errors) do
    // comprehensive validation (field counts, quote checking, etc.), while
    // the fast-path variants only check for fatal quote errors.
    //
    // When an external error collector is explicitly provided, we use the
    // comprehensive validation path to detect issues like inconsistent field
    // counts and duplicate column names.

    try {
      if (!options.dialect.has_value()) {
        // Auto-detect path - always uses error collection
        // Note: parse_auto doesn't support progress callback yet
        result.successful = parser_.parse_auto(buf, result.idx, len, *collector, &result.detection,
                                               options.detection_options);
        result.dialect = result.detection.dialect;
      } else if (use_fast_path) {
        // Single-threaded fast path: speculative parsing without comprehensive validation
        // This path achieves ~370 MB/s vs ~205 MB/s with validation (issue #443)
        // Note: This path does NOT detect inconsistent field counts or
        // duplicate column names - use an explicit error collector if needed
        result.successful =
            parser_.parse_speculate(buf, result.idx, len, result.dialect, second_pass_progress);
      } else if (options.algorithm == ParseAlgorithm::BRANCHLESS) {
        // Branchless with comprehensive error collection
        result.successful =
            parser_.parse_branchless_with_errors(buf, result.idx, len, *collector, result.dialect);
      } else if (options.algorithm == ParseAlgorithm::TWO_PASS) {
        // Two-pass with comprehensive error collection
        result.successful =
            parser_.parse_two_pass_with_errors(buf, result.idx, len, *collector, result.dialect);
      } else if (options.algorithm == ParseAlgorithm::SPECULATIVE) {
        // Speculative with external error collector - use validation
        result.successful =
            parser_.parse_with_errors(buf, result.idx, len, *collector, result.dialect);
      } else {
        // AUTO with external error collector: Use comprehensive validation
        result.successful =
            parser_.parse_with_errors(buf, result.idx, len, *collector, result.dialect);
      }
    } catch (const ParseException& e) {
      // Convert exception to error collector entries
      for (const auto& err : e.errors()) {
        collector->add_error(err);
      }
      result.successful = false;
    } catch (const std::runtime_error& e) {
      // Convert runtime_error to a generic parse error
      collector->add_error(ErrorCode::INTERNAL_ERROR, ErrorSeverity::FATAL, 0, 0, 0, e.what());
      result.successful = false;
    }

    // If external collector was used, copy errors to internal collector
    if (options.errors != nullptr) {
      result.error_collector().merge_from(*options.errors);
    }

    // =======================================================================
    // Progress Callback: Report parsing complete (100%)
    // =======================================================================
    if (options.progress_callback && result.successful) {
      // Report full completion; ignore return value since we're done parsing
      options.progress_callback(len, len);
    }

    // Store buffer reference and row filtering options to enable row/column iteration
    // (must be done before num_columns() which needs the buffer for ValueExtractor)
    result.set_buffer(buf, len, options.skip, options.n_max, options.skip_empty_rows);
    // Pass extraction options from ParseOptions to Result
    result.set_extraction_options(options.extraction_config, options.column_configs);

    // =======================================================================
    // Set Column Count in Index (needed for ParseIndex::get_field_span)
    // =======================================================================
    if (result.successful && result.idx.columns == 0) {
      result.idx.columns = result.num_columns();
    }

    // =======================================================================
    // Compact Index for O(1) Field Access
    // =======================================================================
    if (result.successful) {
      result.idx.compact();
    }

    // =======================================================================
    // Write Cache on Miss (if caching enabled and parse successful)
    // =======================================================================
    if (can_use_cache && result.successful && !result.cache_path.empty()) {
      // Write cache file - emit warning on failure
      bool write_success =
          IndexCache::write_atomic(result.cache_path, result.idx, options.source_path);
      if (!write_success) {
        cache_warn("Failed to write cache file '" + result.cache_path +
                   "'; caching disabled for this parse");
      }
    }

    return result;
  }

  /**
   * @brief Set the number of threads for parsing.
   * @param num_threads Number of threads (minimum 1).
   */
  void set_num_threads(size_t num_threads) { num_threads_ = num_threads > 0 ? num_threads : 1; }

  /// @return Current number of threads configured for parsing.
  size_t num_threads() const { return num_threads_; }

private:
  TwoPass parser_;
  size_t num_threads_;
};

/**
 * @brief Detect CSV dialect from a memory buffer.
 *
 * Convenience function that creates a DialectDetector and detects the
 * dialect from the provided data.
 *
 * @param buf Pointer to CSV data.
 * @param len Length of the data in bytes.
 * @param options Detection options (sample size, candidates, etc.).
 * @return DetectionResult with detected dialect and confidence.
 *
 * @example
 * @code
 * auto result = libvroom::detect_dialect(buffer.data(), buffer.size());
 * if (result.success()) {
 *     std::cout << "Delimiter: '" << result.dialect.delimiter << "'\n";
 *     std::cout << "Confidence: " << result.confidence << "\n";
 * }
 * @endcode
 *
 * @see DialectDetector For more control over detection.
 * @see detect_dialect_file() For detecting from a file path.
 */
inline DetectionResult detect_dialect(const uint8_t* buf, size_t len,
                                      const DetectionOptions& options = DetectionOptions()) {
  DialectDetector detector(options);
  return detector.detect(buf, len);
}

/**
 * @brief Detect CSV dialect from a file.
 *
 * Convenience function that loads a file and detects its dialect.
 * Only samples the beginning of the file for efficiency.
 *
 * @param filename Path to the CSV file.
 * @param options Detection options (sample size, candidates, etc.).
 * @return DetectionResult with detected dialect and confidence.
 *
 * @example
 * @code
 * auto result = libvroom::detect_dialect_file("data.csv");
 * if (result.success()) {
 *     std::cout << "Detected " << result.detected_columns << " columns\n";
 *     std::cout << "Format: " << result.dialect.to_string() << "\n";
 * }
 * @endcode
 *
 * @see DialectDetector For more control over detection.
 * @see detect_dialect() For detecting from an in-memory buffer.
 */
inline DetectionResult detect_dialect_file(const std::string& filename,
                                           const DetectionOptions& options = DetectionOptions()) {
  DialectDetector detector(options);
  return detector.detect_file(filename);
}

} // namespace libvroom

#endif // LIBVROOM_H
