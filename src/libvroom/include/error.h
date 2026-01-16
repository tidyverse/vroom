#ifndef LIBVROOM_ERROR_H
#define LIBVROOM_ERROR_H

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

/**
 * @file error.h
 * @brief Error handling framework for the libvroom CSV parser.
 *
 * This header defines the error types, severity levels, and error collection
 * mechanisms used throughout the libvroom library. The framework supports three
 * error handling modes (STRICT, PERMISSIVE, BEST_EFFORT) to accommodate different
 * use cases from strict validation to best-effort parsing.
 *
 * @see ErrorCollector for collecting and managing parse errors
 * @see ErrorMode for configuring error handling behavior
 */

namespace libvroom {

/**
 * @brief Error codes representing different types of CSV parsing errors.
 *
 * Error codes are grouped by category:
 * - Quote-related errors (UNCLOSED_QUOTE, INVALID_QUOTE_ESCAPE, QUOTE_IN_UNQUOTED_FIELD)
 * - Field structure errors (INCONSISTENT_FIELD_COUNT, FIELD_TOO_LARGE)
 * - Line ending errors (MIXED_LINE_ENDINGS)
 * - Character encoding errors (INVALID_UTF8, NULL_BYTE)
 * - Structure errors (EMPTY_HEADER, DUPLICATE_COLUMN_NAMES)
 * - Separator errors (AMBIGUOUS_SEPARATOR)
 * - General errors (FILE_TOO_LARGE, IO_ERROR, INTERNAL_ERROR)
 */
enum class ErrorCode {
  NONE = 0, ///< No error

  // Quote-related errors (all implemented)
  UNCLOSED_QUOTE,          ///< Quoted field not closed before EOF
  INVALID_QUOTE_ESCAPE,    ///< Invalid quote escape sequence (e.g., "abc"def)
  QUOTE_IN_UNQUOTED_FIELD, ///< Quote appears in middle of unquoted field

  // Field structure errors
  INCONSISTENT_FIELD_COUNT, ///< Row has different number of fields than header
  FIELD_TOO_LARGE,          ///< Field exceeds maximum size limit

  // Line ending errors
  MIXED_LINE_ENDINGS, ///< File uses inconsistent line endings (warning)

  // Character encoding errors
  INVALID_UTF8, ///< Invalid UTF-8 byte sequence detected
  NULL_BYTE,    ///< Unexpected null byte in data

  // Structure errors (all implemented)
  EMPTY_HEADER,           ///< Header row is empty
  DUPLICATE_COLUMN_NAMES, ///< Header contains duplicate column names

  // Separator errors
  AMBIGUOUS_SEPARATOR, ///< Cannot determine separator reliably (used in dialect detection)

  // General errors
  FILE_TOO_LARGE,            ///< File exceeds maximum size limit
  INDEX_ALLOCATION_OVERFLOW, ///< Index allocation would overflow
  IO_ERROR,                  ///< File I/O error (e.g., read failure)
  INTERNAL_ERROR             ///< Internal parser error
};

/**
 * @brief Default limit for individual field size (16 MB).
 *
 * Fields larger than this are flagged with FIELD_TOO_LARGE to prevent
 * denial-of-service attacks via maliciously crafted CSV files with
 * extremely large fields.
 */
constexpr size_t DEFAULT_MAX_FIELD_SIZE = 16 * 1024 * 1024; // 16 MB

/**
 * @brief Default limit for total file size (4 GB).
 *
 * Files larger than this are flagged with FILE_TOO_LARGE. This limit
 * prevents out-of-memory conditions when allocating index buffers.
 * For larger files, consider using the streaming API.
 */
constexpr size_t DEFAULT_MAX_FILE_SIZE = 4ULL * 1024 * 1024 * 1024; // 4 GB

/**
 * @brief Severity levels for parse errors.
 *
 * Severity levels indicate how serious an error is and whether the parser
 * can continue after encountering it.
 *
 * @note The enum values use a naming pattern that avoids conflicts with
 * Windows macros (e.g., ERROR is defined in WinGDI.h).
 */
enum class ErrorSeverity {
  WARNING,     ///< Non-fatal issue, parser continues (e.g., mixed line endings)
  RECOVERABLE, ///< Recoverable error, can skip affected row (e.g., inconsistent field count)
  FATAL        ///< Unrecoverable error, parsing must stop (e.g., unclosed quote at EOF)
};

/**
 * @brief Detailed information about a single parse error.
 *
 * Contains the error type, severity, location (line, column, byte offset),
 * and contextual information to help users identify and fix the issue.
 *
 * @example
 * @code
 * ParseError error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL,
 *                  10, 5, 1024, "Unclosed quote at end of file", "...content...");
 * std::cout << error.to_string() << std::endl;
 * // Output: FATAL [line 10, col 5]: Unclosed quote at end of file
 * @endcode
 */
struct ParseError {
  ErrorCode code;         ///< The type of error that occurred
  ErrorSeverity severity; ///< Severity level of the error

  // Location information
  size_t line;        ///< Line number where error occurred (1-indexed)
  size_t column;      ///< Column number where error occurred (1-indexed)
  size_t byte_offset; ///< Byte offset from start of file

  // Context
  std::string message; ///< Human-readable error description
  std::string context; ///< Snippet of data around the error location

  /**
   * @brief Construct a ParseError with full details.
   *
   * @param c Error code identifying the type of error
   * @param s Severity level of the error
   * @param l Line number (1-indexed) where the error occurred
   * @param col Column number (1-indexed) where the error occurred
   * @param offset Byte offset from the start of the file
   * @param msg Human-readable error message
   * @param ctx Optional context snippet showing data around the error
   */
  ParseError(ErrorCode c, ErrorSeverity s, size_t l, size_t col, size_t offset,
             const std::string& msg, const std::string& ctx = "")
      : code(c), severity(s), line(l), column(col), byte_offset(offset), message(msg),
        context(ctx) {}

  /**
   * @brief Convert the error to a human-readable string.
   *
   * @return Formatted string with severity, location, and message.
   */
  std::string to_string() const;
};

/**
 * @brief Error handling modes that control parser behavior on errors.
 *
 * Choose the appropriate mode based on your use case:
 * - STRICT: Best for data validation, stops immediately on any error
 * - PERMISSIVE: Best for data exploration, collects all errors while parsing
 * - BEST_EFFORT: Best for importing imperfect data, ignores errors
 *
 * @example
 * @code
 * // For strict validation
 * ErrorCollector errors(ErrorMode::FAIL_FAST);
 *
 * // For collecting all issues in a file
 * ErrorCollector errors(ErrorMode::PERMISSIVE);
 *
 * // For best-effort import of messy data
 * ErrorCollector errors(ErrorMode::BEST_EFFORT);
 * @endcode
 */
enum class ErrorMode {
  FAIL_FAST,  ///< Stop parsing on first error encountered (renamed from STRICT
              ///< to avoid Windows macro conflict)
  PERMISSIVE, ///< Try to recover from errors, collect and report all
  BEST_EFFORT ///< Ignore errors completely, parse what we can
};

/**
 * @brief Collects and manages parse errors during CSV parsing.
 *
 * ErrorCollector accumulates errors encountered during parsing and provides
 * methods to query, filter, and manage them. It supports different error
 * handling modes and includes a maximum error limit to prevent out-of-memory
 * conditions when parsing malicious or severely malformed inputs.
 *
 * @note Thread Safety: ErrorCollector is NOT thread-safe. When using multi-threaded
 *       parsing, each thread should use its own collector, then merge results
 *       using merge_sorted() after parsing completes.
 *
 * @example
 * @code
 * // Create collector in permissive mode
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 *
 * // Parse CSV with error collection
 * libvroom::TwoPass parser;
 * auto idx = parser.init(data_len, 1);
 * parser.parse_with_errors(data, idx, data_len, errors);
 *
 * // Check for errors
 * if (errors.has_errors()) {
 *     std::cout << errors.summary() << std::endl;
 *     for (const auto& err : errors.errors()) {
 *         std::cout << err.to_string() << std::endl;
 *     }
 * }
 * @endcode
 */
class ErrorCollector {
public:
  /** @brief Default maximum number of errors to collect (prevents OOM attacks) */
  static constexpr size_t DEFAULT_MAX_ERRORS = 10000;

  /**
   * @brief Construct an ErrorCollector with specified mode and limits.
   *
   * @param mode Error handling mode (STRICT, PERMISSIVE, or BEST_EFFORT)
   * @param max_errors Maximum errors to collect (default: 10000)
   */
  explicit ErrorCollector(ErrorMode mode = ErrorMode::FAIL_FAST,
                          size_t max_errors = DEFAULT_MAX_ERRORS)
      : mode_(mode), max_errors_(max_errors), has_fatal_(false), suppressed_count_(0) {}

  /**
   * @brief Add an error to the collection.
   *
   * Errors are only added if the collection has not reached max_errors_.
   * If the limit is reached, the error is not stored but suppressed_count_
   * is incremented to track how many errors were dropped.
   * If a FATAL error is encountered, has_fatal_ is set to true regardless
   * of whether the error is stored or suppressed (to ensure should_stop()
   * works correctly even when fatal errors are suppressed).
   *
   * @param error The ParseError to add
   */
  void add_error(const ParseError& error) {
    // Always track fatal errors, even if suppressed, so should_stop() works correctly
    if (error.severity == ErrorSeverity::FATAL) {
      has_fatal_ = true;
    }
    if (errors_.size() >= max_errors_) {
      ++suppressed_count_;
      return;
    }
    errors_.push_back(error);
  }

  /**
   * @brief Check if the error limit has been reached.
   *
   * @return true if max_errors_ limit has been reached
   */
  bool at_error_limit() const { return errors_.size() >= max_errors_; }

  /**
   * @brief Add an error with individual parameters (convenience overload).
   *
   * @param code Error code identifying the type of error
   * @param severity Severity level of the error
   * @param line Line number (1-indexed) where the error occurred
   * @param column Column number (1-indexed) where the error occurred
   * @param offset Byte offset from the start of the file
   * @param message Human-readable error message
   * @param context Optional context snippet showing data around the error
   */
  void add_error(ErrorCode code, ErrorSeverity severity, size_t line, size_t column, size_t offset,
                 const std::string& message, const std::string& context = "") {
    add_error(ParseError(code, severity, line, column, offset, message, context));
  }

  /**
   * @brief Check if parsing should stop based on current errors and mode.
   *
   * Returns true in the following cases:
   * - STRICT mode and any error has been recorded
   * - Any FATAL error has been recorded (regardless of mode)
   *
   * @return true if parsing should stop
   */
  bool should_stop() const {
    if (mode_ == ErrorMode::FAIL_FAST && !errors_.empty())
      return true;
    if (has_fatal_)
      return true;
    return false;
  }

  /**
   * @brief Check if any errors have been recorded.
   * @return true if at least one error has been recorded
   */
  bool has_errors() const { return !errors_.empty(); }

  /**
   * @brief Check if any fatal errors have been recorded.
   * @return true if at least one FATAL error has been recorded
   */
  bool has_fatal_errors() const { return has_fatal_; }

  /**
   * @brief Get the number of recorded errors.
   * @return Number of errors in the collection
   */
  size_t error_count() const { return errors_.size(); }

  /**
   * @brief Get read-only access to all recorded errors.
   * @return Const reference to the vector of ParseError objects
   */
  const std::vector<ParseError>& errors() const { return errors_; }

  /**
   * @brief Get a summary string of all errors.
   * @return Human-readable summary of error counts by type
   */
  std::string summary() const;

  /**
   * @brief Clear all recorded errors and reset flags.
   */
  void clear() {
    errors_.clear();
    has_fatal_ = false;
    suppressed_count_ = 0;
  }

  /**
   * @brief Get the current error handling mode.
   * @return Current ErrorMode
   */
  ErrorMode mode() const { return mode_; }

  /**
   * @brief Change the error handling mode.
   * @param mode New ErrorMode to use
   */
  void set_mode(ErrorMode mode) { mode_ = mode; }

  /**
   * @brief Change the maximum number of errors to collect.
   *
   * This can be called before parsing begins to configure the error limit.
   * If called after errors have been collected, the new limit takes effect
   * for subsequent add_error() calls.
   *
   * @param max_errors New maximum errors limit
   */
  void set_max_errors(size_t max_errors) { max_errors_ = max_errors; }

  /**
   * @brief Merge errors from another collector.
   *
   * Used for multi-threaded parsing where each thread has its own collector.
   * Respects max_errors_ limit when merging. Suppressed counts from both
   * collectors are combined, plus any errors that couldn't be copied due
   * to the limit.
   *
   * @param other The ErrorCollector to merge from
   */
  void merge_from(const ErrorCollector& other) {
    // Always merge suppressed counts first
    suppressed_count_ += other.suppressed_count_;

    if (other.errors_.empty()) {
      if (other.has_fatal_) {
        has_fatal_ = true;
      }
      return;
    }

    // Respect max_errors_ limit when merging
    size_t available = max_errors_ > errors_.size() ? max_errors_ - errors_.size() : 0;
    size_t to_copy = std::min(available, other.errors_.size());

    // Track errors we couldn't copy as suppressed
    if (to_copy < other.errors_.size()) {
      suppressed_count_ += other.errors_.size() - to_copy;
    }

    errors_.reserve(errors_.size() + to_copy);
    for (size_t i = 0; i < to_copy; ++i) {
      errors_.push_back(other.errors_[i]);
    }
    if (other.has_fatal_) {
      has_fatal_ = true;
    }
  }

  /**
   * @brief Sort errors by byte offset.
   *
   * Call this after merging errors from multiple threads to ensure
   * errors are in logical file order.
   */
  void sort_by_offset() {
    std::sort(errors_.begin(), errors_.end(), [](const ParseError& a, const ParseError& b) {
      return a.byte_offset < b.byte_offset;
    });
  }

  /**
   * @brief Merge multiple collectors and sort by byte offset.
   *
   * Convenience method for multi-threaded parsing that merges all
   * thread-local collectors and sorts the result.
   *
   * @param collectors Vector of ErrorCollectors to merge from
   */
  void merge_sorted(std::vector<ErrorCollector>& collectors) {
    for (auto& collector : collectors) {
      merge_from(collector);
    }
    sort_by_offset();
  }

  /**
   * @brief Get the number of suppressed errors.
   *
   * Returns the count of errors that were dropped after the max_errors
   * limit was reached. This helps users understand the full scope of
   * issues even when only a subset is collected.
   *
   * @return Number of errors suppressed due to hitting the limit
   */
  size_t suppressed_count() const { return suppressed_count_; }

  /**
   * @brief Get the configured maximum number of errors.
   * @return The max_errors limit
   */
  size_t max_errors() const { return max_errors_; }

private:
  ErrorMode mode_;
  size_t max_errors_;
  std::vector<ParseError> errors_;
  bool has_fatal_;
  size_t suppressed_count_;
};

/**
 * @brief Exception thrown for fatal parse errors when using exception-based error handling.
 *
 * This exception is thrown by the parser when a fatal error occurs and the
 * caller prefers exception-based error handling over ErrorCollector.
 *
 * @example
 * @code
 * try {
 *     parser.parse(data, idx, len);
 * } catch (const libvroom::ParseException& e) {
 *     std::cerr << "Parse failed: " << e.what() << std::endl;
 *     for (const auto& err : e.errors()) {
 *         std::cerr << "  " << err.to_string() << std::endl;
 *     }
 * }
 * @endcode
 */
class ParseException : public std::runtime_error {
public:
  /**
   * @brief Construct exception from a single error.
   * @param error The ParseError that caused the exception
   */
  explicit ParseException(const ParseError& error)
      : std::runtime_error(error.message), errors_{error} {}

  /**
   * @brief Construct exception from multiple errors.
   * @param errors Vector of ParseError objects
   */
  explicit ParseException(const std::vector<ParseError>& errors)
      : std::runtime_error(format_errors(errors)), errors_(errors) {}

  /**
   * @brief Get the first (primary) error.
   * @return Reference to the first ParseError
   * @throws std::logic_error if no errors are present
   */
  const ParseError& error() const {
    if (errors_.empty())
      throw std::logic_error("No errors in ParseException");
    return errors_[0];
  }

  /**
   * @brief Get all errors that contributed to this exception.
   * @return Const reference to the vector of ParseError objects
   */
  const std::vector<ParseError>& errors() const { return errors_; }

private:
  std::vector<ParseError> errors_;

  static std::string format_errors(const std::vector<ParseError>& errors);
};

/**
 * @brief Convert an ErrorCode to its string representation.
 *
 * @param code The ErrorCode to convert
 * @return C-string name of the error code (e.g., "UNCLOSED_QUOTE")
 */
const char* error_code_to_string(ErrorCode code);

/**
 * @brief Convert an ErrorSeverity to its string representation.
 *
 * @param severity The ErrorSeverity to convert
 * @return C-string name of the severity (e.g., "FATAL", "ERROR", "WARNING")
 */
const char* error_severity_to_string(ErrorSeverity severity);

} // namespace libvroom

#endif // LIBVROOM_ERROR_H
