/**
 * @file libvroom_c.h
 * @brief C API wrapper for the libvroom high-performance CSV parser.
 *
 * This header provides a C-compatible interface to libvroom, enabling use from
 * C programs and FFI bindings in languages like Python, R, Ruby, and Rust.
 *
 * @section memory_management Memory Management
 *
 * libvroom uses an opaque handle pattern for resource management. Functions that
 * create resources return pointers to opaque structs, which must be freed using
 * the corresponding destroy function:
 *
 * | Create Function                         | Destroy Function                      |
 * |-----------------------------------------|---------------------------------------|
 * | libvroom_buffer_load_file()             | libvroom_buffer_destroy()             |
 * | libvroom_buffer_create()                | libvroom_buffer_destroy()             |
 * | libvroom_dialect_create()               | libvroom_dialect_destroy()            |
 * | libvroom_error_collector_create()       | libvroom_error_collector_destroy()    |
 * | libvroom_index_create()                 | libvroom_index_destroy()              |
 * | libvroom_parser_create()                | libvroom_parser_destroy()             |
 * | libvroom_detect_dialect()               | libvroom_detection_result_destroy()   |
 * | libvroom_load_file_with_encoding()      | libvroom_load_result_destroy()        |
 * | libvroom_detection_result_dialect()     | libvroom_dialect_destroy()            |
 * | libvroom_load_result_to_buffer()        | libvroom_buffer_destroy()             |
 *
 * @note All destroy functions accept NULL safely (no-op).
 *
 * @section thread_safety Thread Safety
 *
 * - **Parser objects** (`libvroom_parser_t`): NOT thread-safe. Each thread should
 *   create its own parser instance.
 * - **Buffer objects** (`libvroom_buffer_t`): Thread-safe for read-only access.
 *   Multiple threads may read from the same buffer simultaneously.
 * - **Index objects** (`libvroom_index_t`): Thread-safe for read-only access after
 *   parsing completes. The index stores per-thread data accessed via thread_id.
 * - **Error collectors** (`libvroom_error_collector_t`): NOT thread-safe.
 *   Each thread should use its own collector, or synchronize access externally.
 * - **Dialect objects** (`libvroom_dialect_t`): Immutable after creation, thread-safe.
 * - **Detection results** (`libvroom_detection_result_t`): Thread-safe for read access.
 *
 * @section usage_example Usage Example
 *
 * @code{.c}
 * #include <libvroom_c.h>
 * #include <stdio.h>
 * #include <stdlib.h>
 *
 * int main(void) {
 *     // Load CSV file into buffer
 *     libvroom_buffer_t* buffer = libvroom_buffer_load_file("data.csv");
 *     if (!buffer) {
 *         fprintf(stderr, "Failed to load file\n");
 *         return 1;
 *     }
 *
 *     // Create parser and supporting objects
 *     libvroom_parser_t* parser = libvroom_parser_create();
 *     size_t num_threads = libvroom_recommended_threads();
 *     libvroom_index_t* index = libvroom_index_create(
 *         libvroom_buffer_length(buffer), num_threads);
 *     libvroom_error_collector_t* errors = libvroom_error_collector_create(
 *         LIBVROOM_MODE_PERMISSIVE, 100);
 *
 *     // Auto-detect dialect and parse
 *     libvroom_detection_result_t* detected = NULL;
 *     libvroom_error_t err = libvroom_parse_auto(parser, buffer, index, errors, &detected);
 *
 *     if (err != LIBVROOM_OK) {
 *         fprintf(stderr, "Parse error: %s\n", libvroom_error_string(err));
 *     } else {
 *         printf("Parsed %llu fields in %zu columns\n",
 *                (unsigned long long)libvroom_index_total_count(index),
 *                libvroom_index_columns(index));
 *     }
 *
 *     // Check for parse warnings/errors
 *     if (libvroom_error_collector_has_errors(errors)) {
 *         size_t count = libvroom_error_collector_count(errors);
 *         for (size_t i = 0; i < count; i++) {
 *             libvroom_parse_error_t parse_err;
 *             libvroom_error_collector_get(errors, i, &parse_err);
 *             fprintf(stderr, "Line %zu: %s\n", parse_err.line, parse_err.message);
 *         }
 *     }
 *
 *     // Cleanup - order doesn't matter, but good practice to reverse creation order
 *     libvroom_detection_result_destroy(detected);
 *     libvroom_error_collector_destroy(errors);
 *     libvroom_index_destroy(index);
 *     libvroom_parser_destroy(parser);
 *     libvroom_buffer_destroy(buffer);
 *
 *     return 0;
 * }
 * @endcode
 *
 * @section error_handling Error Handling
 *
 * The library provides three error handling modes via libvroom_error_mode_t:
 *
 * - **LIBVROOM_MODE_STRICT**: Stop on first error, no recovery attempted
 * - **LIBVROOM_MODE_PERMISSIVE**: Collect errors but continue parsing where possible
 * - **LIBVROOM_MODE_BEST_EFFORT**: Maximum recovery, may produce partial/invalid data
 *
 * Functions return `libvroom_error_t` codes. Use libvroom_error_string() to get
 * human-readable error messages.
 *
 * @version 0.1.0
 * @author Jim Hester
 * @see https://github.com/jimhester/libvroom
 */

#ifndef LIBVROOM_C_H
#define LIBVROOM_C_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @defgroup version Version Information
 * @brief Library version constants and query function.
 * @{
 */

/** @brief Major version number (breaking changes). */
#define LIBVROOM_VERSION_MAJOR 0

/** @brief Minor version number (new features, backwards compatible). */
#define LIBVROOM_VERSION_MINOR 1

/** @brief Patch version number (bug fixes). */
#define LIBVROOM_VERSION_PATCH 0

/**
 * @brief Get the library version as a string.
 *
 * @return Version string in "MAJOR.MINOR.PATCH" format (e.g., "0.1.0").
 *         The returned string is statically allocated and should not be freed.
 */
const char* libvroom_version(void);

/** @} */ /* end of version group */

/**
 * @defgroup errors Error Handling
 * @brief Error codes, severity levels, and error handling modes.
 * @{
 */

/**
 * @brief Error codes returned by libvroom functions.
 *
 * Error codes are divided into ranges:
 * - 0: Success
 * - 1-99: Parse errors (problems with CSV content)
 * - 100-199: API errors (invalid usage of the library)
 */
typedef enum libvroom_error {
  /** @brief Success, no error. */
  LIBVROOM_OK = 0,

  /* Parse errors (1-99) */

  /** @brief Quoted field was not properly closed before end of input. */
  LIBVROOM_ERROR_UNCLOSED_QUOTE = 1,

  /** @brief Invalid escape sequence inside quoted field. */
  LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE = 2,

  /** @brief Quote character found in unquoted field (RFC 4180 violation). */
  LIBVROOM_ERROR_QUOTE_IN_UNQUOTED = 3,

  /** @brief Row has different number of fields than header/first row. */
  LIBVROOM_ERROR_INCONSISTENT_FIELDS = 4,

  /** @brief Single field exceeds maximum allowed size. */
  LIBVROOM_ERROR_FIELD_TOO_LARGE = 5,

  /** @brief File contains mixed line ending styles (e.g., both CRLF and LF). */
  LIBVROOM_ERROR_MIXED_LINE_ENDINGS = 6,

  /* Note: value 7 was previously INVALID_LINE_ENDING (removed in v0.2.0) */

  /** @brief Invalid UTF-8 byte sequence detected. */
  LIBVROOM_ERROR_INVALID_UTF8 = 8,

  /** @brief Null byte (0x00) found in input data. */
  LIBVROOM_ERROR_NULL_BYTE = 9,

  /** @brief Header row contains empty column name. */
  LIBVROOM_ERROR_EMPTY_HEADER = 10,

  /** @brief Header row contains duplicate column names. */
  LIBVROOM_ERROR_DUPLICATE_COLUMNS = 11,

  /** @brief Could not determine field separator (multiple candidates). */
  LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR = 12,

  /** @brief Input file exceeds maximum supported size. */
  LIBVROOM_ERROR_FILE_TOO_LARGE = 13,

  /** @brief I/O error reading file (check errno for details). */
  LIBVROOM_ERROR_IO = 14,

  /** @brief Internal error (bug in library, please report). */
  LIBVROOM_ERROR_INTERNAL = 15,

  /* API errors (100-199) */

  /** @brief NULL pointer passed where non-NULL was required. */
  LIBVROOM_ERROR_NULL_POINTER = 100,

  /** @brief Invalid argument value (e.g., invalid enum value). */
  LIBVROOM_ERROR_INVALID_ARGUMENT = 101,

  /** @brief Memory allocation failed. */
  LIBVROOM_ERROR_OUT_OF_MEMORY = 102,

  /** @brief Invalid or already-destroyed handle passed to function. */
  LIBVROOM_ERROR_INVALID_HANDLE = 103,

  /** @brief Operation was cancelled by user (e.g., progress callback returned false). */
  LIBVROOM_ERROR_CANCELLED = 104
} libvroom_error_t;

/**
 * @brief Get a human-readable string for an error code.
 *
 * @param error The error code to describe.
 * @return Static string describing the error. Never returns NULL.
 *         The returned string should not be freed.
 */
const char* libvroom_error_string(libvroom_error_t error);

/**
 * @brief Severity levels for parse errors.
 *
 * Used to classify errors by their impact on parsing:
 */
typedef enum libvroom_severity {
  /** @brief Warning: parsing continued, data may be slightly off. */
  LIBVROOM_SEVERITY_WARNING = 0,

  /** @brief Error: parsing continued but data quality is affected. */
  LIBVROOM_SEVERITY_ERROR = 1,

  /** @brief Fatal: parsing cannot continue from this point. */
  LIBVROOM_SEVERITY_FATAL = 2
} libvroom_severity_t;

/**
 * @brief Error handling modes for the parser.
 *
 * Controls how the parser responds to errors:
 *
 * | Mode              | On Error                  | Data Quality    |
 * |-------------------|---------------------------|-----------------|
 * | STRICT            | Stop immediately          | Guaranteed      |
 * | PERMISSIVE        | Log and continue          | Usually correct |
 * | BEST_EFFORT       | Recover aggressively      | May be partial  |
 */
typedef enum libvroom_error_mode {
  /** @brief Stop on first error. Use when data quality is critical. */
  LIBVROOM_MODE_STRICT = 0,

  /** @brief Collect errors but continue parsing. Default for most uses. */
  LIBVROOM_MODE_PERMISSIVE = 1,

  /** @brief Maximum error recovery. Use for exploring malformed data. */
  LIBVROOM_MODE_BEST_EFFORT = 2
} libvroom_error_mode_t;

/** @} */ /* end of errors group */

/**
 * @defgroup handles Opaque Handle Types
 * @brief Opaque pointer types for library objects.
 *
 * All handles are pointers to opaque structs. They must be created using
 * the appropriate create function and destroyed using the matching destroy
 * function. Handles should not be dereferenced directly.
 * @{
 */

/**
 * @brief Opaque handle to a CSV parser instance.
 *
 * Created with libvroom_parser_create(), destroyed with libvroom_parser_destroy().
 * Each parser instance maintains internal state and should only be used by one
 * thread at a time.
 */
typedef struct libvroom_parser libvroom_parser_t;

/**
 * @brief Opaque handle to a parsed CSV index.
 *
 * Created with libvroom_index_create(), destroyed with libvroom_index_destroy().
 * The index stores field positions after parsing, enabling random access to
 * CSV fields without re-parsing.
 */
typedef struct libvroom_index libvroom_index_t;

/**
 * @brief Opaque handle to an input buffer.
 *
 * Created with libvroom_buffer_load_file() or libvroom_buffer_create(),
 * destroyed with libvroom_buffer_destroy(). Buffers are SIMD-aligned for
 * optimal parsing performance.
 */
typedef struct libvroom_buffer libvroom_buffer_t;

/**
 * @brief Opaque handle to a CSV dialect configuration.
 *
 * Created with libvroom_dialect_create() or obtained from detection results,
 * destroyed with libvroom_dialect_destroy(). Dialects are immutable after
 * creation and describe the format of a CSV file (delimiter, quoting, etc.).
 */
typedef struct libvroom_dialect libvroom_dialect_t;

/**
 * @brief Opaque handle to an error collector.
 *
 * Created with libvroom_error_collector_create(), destroyed with
 * libvroom_error_collector_destroy(). Collects parse errors according to
 * the configured error mode.
 */
typedef struct libvroom_error_collector libvroom_error_collector_t;

/**
 * @brief Opaque handle to dialect detection results.
 *
 * Created by libvroom_detect_dialect() or libvroom_parse_auto(), destroyed
 * with libvroom_detection_result_destroy(). Contains detected CSV format
 * and confidence metrics.
 */
typedef struct libvroom_detection_result libvroom_detection_result_t;

/**
 * @brief Opaque handle to a column configuration map.
 *
 * Created with libvroom_column_config_create(), destroyed with
 * libvroom_column_config_destroy(). Stores per-column extraction settings.
 */
typedef struct libvroom_column_config libvroom_column_config_t;

/**
 * @brief Opaque handle to a lazy column accessor.
 *
 * Created with libvroom_lazy_column_create(), destroyed with
 * libvroom_lazy_column_destroy(). Provides ALTREP-style lazy access to a
 * single column without parsing the entire file upfront.
 */
typedef struct libvroom_lazy_column libvroom_lazy_column_t;

/** @} */ /* end of handles group */

/**
 * @defgroup structs Data Structures
 * @brief Non-opaque structures for data exchange.
 * @{
 */

/**
 * @brief Parse error information returned from error collector.
 *
 * This structure contains details about a single parse error, including its
 * location and a descriptive message.
 *
 * @par Ownership Semantics
 * This is a value type - the structure itself is owned by the caller.
 * However, the `message` and `context` string pointers point to internal
 * storage owned by the error collector.
 *
 * @par Lifetime Requirements
 * The `message` and `context` pointers are only valid as long as:
 * 1. The error collector has not been destroyed
 * 2. The error collector has not been cleared (libvroom_error_collector_clear)
 * 3. No new errors have been added to the collector
 *
 * If you need to persist error information, copy the strings before any of
 * these events occur.
 *
 * @par Example
 * @code{.c}
 * libvroom_parse_error_t err;
 * if (libvroom_error_collector_get(collector, 0, &err) == LIBVROOM_OK) {
 *     // Safe to use err.message here
 *     printf("Error at line %zu: %s\n", err.line, err.message);
 *
 *     // To persist, copy the string:
 *     char* saved_msg = strdup(err.message);
 * }
 * @endcode
 */
typedef struct libvroom_parse_error {
  /** @brief Error code identifying the type of error. */
  libvroom_error_t code;

  /** @brief Severity level of this error. */
  libvroom_severity_t severity;

  /** @brief 1-based line number where error occurred. */
  size_t line;

  /** @brief 1-based column number where error occurred. */
  size_t column;

  /** @brief Byte offset from start of input where error occurred. */
  size_t byte_offset;

  /**
   * @brief Human-readable error message.
   * @warning Borrowed pointer - see struct documentation for lifetime rules.
   */
  const char* message;

  /**
   * @brief Context around the error (snippet of surrounding text).
   * @warning Borrowed pointer - see struct documentation for lifetime rules.
   *          May be NULL if context is not available.
   */
  const char* context;
} libvroom_parse_error_t;

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
 * @par Example
 * @code
 * // For CSV: "hello,world\n"
 * //          ^     ^
 * //          0     6
 * // Field 0: {0, 5}   -> "hello"
 * // Field 1: {6, 11}  -> "world"
 * @endcode
 */
typedef struct libvroom_field_span {
  /** @brief Byte offset of field start (inclusive). */
  uint64_t start;

  /** @brief Byte offset of field end (exclusive, at delimiter/newline). */
  uint64_t end;
} libvroom_field_span_t;

/**
 * @brief Result of byte offset to location conversion.
 *
 * Contains the row and column indices corresponding to a byte offset,
 * and a flag indicating whether the location was found.
 */
typedef struct libvroom_location {
  /** @brief 0-based row index. */
  size_t row;

  /** @brief 0-based column index. */
  size_t column;

  /** @brief true if the byte offset was found within a valid field. */
  bool found;
} libvroom_location_t;

/** @} */ /* end of structs group */

/**
 * @defgroup buffer Buffer Management
 * @brief Functions for loading and managing input data buffers.
 *
 * Buffers hold CSV data in memory with proper SIMD alignment for optimal
 * parsing performance. Use libvroom_buffer_load_file() for files or
 * libvroom_buffer_create() for in-memory data.
 * @{
 */

/**
 * @brief Load a file into a buffer for parsing.
 *
 * Reads the entire file into memory with proper SIMD alignment. The file
 * is memory-mapped when possible for efficiency.
 *
 * @param filename Path to the file to load. Must be a valid, readable file.
 * @return New buffer handle on success, NULL on failure.
 *         The caller owns the returned buffer and must call
 *         libvroom_buffer_destroy() to free it.
 *
 * @note For files with non-UTF-8 encoding, use libvroom_load_file_with_encoding()
 *       instead, which will auto-detect and transcode the encoding.
 *
 * @see libvroom_buffer_create() for creating buffers from in-memory data.
 * @see libvroom_load_file_with_encoding() for encoding-aware loading.
 */
libvroom_buffer_t* libvroom_buffer_load_file(const char* filename);

/**
 * @brief Create a buffer from in-memory data.
 *
 * Copies the provided data into a new SIMD-aligned buffer. Use this when
 * you already have CSV data in memory (e.g., from a network socket or
 * decompression).
 *
 * @param data Pointer to the CSV data to copy. Must not be NULL if length > 0.
 * @param length Number of bytes to copy from data.
 * @return New buffer handle on success, NULL on failure (e.g., out of memory).
 *         The caller owns the returned buffer and must call
 *         libvroom_buffer_destroy() to free it.
 *
 * @note The data is copied, so the caller may free the original data after
 *       this function returns.
 *
 * @see libvroom_buffer_load_file() for loading from files.
 */
libvroom_buffer_t* libvroom_buffer_create(const uint8_t* data, size_t length);

/**
 * @brief Get a pointer to the buffer's data.
 *
 * @param buffer The buffer to access. Must not be NULL.
 * @return Pointer to the raw CSV data, or NULL if buffer is invalid.
 *         The returned pointer is valid until libvroom_buffer_destroy()
 *         is called on the buffer.
 *
 * @warning The returned pointer is borrowed - do not free it directly.
 */
const uint8_t* libvroom_buffer_data(const libvroom_buffer_t* buffer);

/**
 * @brief Get the length of the buffer's data in bytes.
 *
 * @param buffer The buffer to query. Must not be NULL.
 * @return Number of bytes in the buffer, or 0 if buffer is invalid.
 */
size_t libvroom_buffer_length(const libvroom_buffer_t* buffer);

/**
 * @brief Destroy a buffer and free its resources.
 *
 * @param buffer The buffer to destroy. May be NULL (no-op).
 *
 * @warning After calling this function, the buffer handle and any pointers
 *          obtained from libvroom_buffer_data() are invalid.
 */
void libvroom_buffer_destroy(libvroom_buffer_t* buffer);

/** @} */ /* end of buffer group */

/**
 * @defgroup dialect Dialect Configuration
 * @brief Functions for creating and querying CSV dialect settings.
 *
 * A dialect describes the format of a CSV file: field delimiter, quote
 * character, escape handling, etc. Dialects are immutable after creation.
 * @{
 */

/**
 * @brief Create a new dialect configuration.
 *
 * @param delimiter Field separator character (e.g., ',' for CSV, '\\t' for TSV).
 * @param quote_char Quote character for escaping fields (typically '"').
 * @param escape_char Escape character (typically '\\' or same as quote_char).
 * @param double_quote If true, quotes inside quoted fields are escaped by
 *                     doubling (RFC 4180 style: ""). If false, escape_char
 *                     is used instead.
 * @return New dialect handle on success, NULL on failure.
 *         The caller owns the returned dialect and must call
 *         libvroom_dialect_destroy() to free it.
 *
 * @par Common Dialects
 * - CSV (RFC 4180): delimiter=',', quote='"', escape='"', double_quote=true
 * - TSV: delimiter='\\t', quote='"', escape='\\', double_quote=false
 *
 * @see libvroom_detect_dialect() for automatic dialect detection.
 */
libvroom_dialect_t* libvroom_dialect_create(char delimiter, char quote_char, char escape_char,
                                            bool double_quote);

/**
 * @brief Get the field delimiter character from a dialect.
 *
 * @param dialect The dialect to query. Must not be NULL.
 * @return The delimiter character (e.g., ',' or '\\t'), or '\\0' if invalid.
 */
char libvroom_dialect_delimiter(const libvroom_dialect_t* dialect);

/**
 * @brief Get the quote character from a dialect.
 *
 * @param dialect The dialect to query. Must not be NULL.
 * @return The quote character (typically '"'), or '\\0' if invalid.
 */
char libvroom_dialect_quote_char(const libvroom_dialect_t* dialect);

/**
 * @brief Get the escape character from a dialect.
 *
 * @param dialect The dialect to query. Must not be NULL.
 * @return The escape character, or '\\0' if invalid.
 */
char libvroom_dialect_escape_char(const libvroom_dialect_t* dialect);

/**
 * @brief Check if the dialect uses doubled quotes for escaping.
 *
 * @param dialect The dialect to query. Must not be NULL.
 * @return true if quotes are escaped by doubling (""), false if escape_char is used.
 */
bool libvroom_dialect_double_quote(const libvroom_dialect_t* dialect);

/**
 * @brief Destroy a dialect and free its resources.
 *
 * @param dialect The dialect to destroy. May be NULL (no-op).
 */
void libvroom_dialect_destroy(libvroom_dialect_t* dialect);

/** @} */ /* end of dialect group */

/**
 * @defgroup error_collector Error Collector
 * @brief Functions for collecting and querying parse errors.
 *
 * The error collector accumulates parse errors according to its error mode.
 * After parsing, you can query the collector to see what errors occurred.
 * @{
 */

/**
 * @brief Create a new error collector.
 *
 * @param mode Error handling mode determining how errors are processed.
 * @param max_errors Maximum number of errors to collect before stopping.
 *                   Pass 0 for unlimited collection.
 * @return New error collector handle on success, NULL on failure.
 *         The caller owns the returned collector and must call
 *         libvroom_error_collector_destroy() to free it.
 *
 * @see libvroom_error_mode_t for available modes.
 */
libvroom_error_collector_t* libvroom_error_collector_create(libvroom_error_mode_t mode,
                                                            size_t max_errors);

/**
 * @brief Get the error mode of a collector.
 *
 * @param collector The collector to query. Must not be NULL.
 * @return The error mode, or LIBVROOM_MODE_STRICT if collector is invalid.
 */
libvroom_error_mode_t libvroom_error_collector_mode(const libvroom_error_collector_t* collector);

/**
 * @brief Check if any errors have been collected.
 *
 * @param collector The collector to query. Must not be NULL.
 * @return true if at least one error has been collected, false otherwise.
 *
 * @note This returns true for any severity level (warning, error, or fatal).
 */
bool libvroom_error_collector_has_errors(const libvroom_error_collector_t* collector);

/**
 * @brief Check if a fatal error has been collected.
 *
 * @param collector The collector to query. Must not be NULL.
 * @return true if at least one fatal error has been collected.
 *
 * @note A fatal error means parsing could not continue from that point.
 *       The parsed data may be incomplete.
 */
bool libvroom_error_collector_has_fatal(const libvroom_error_collector_t* collector);

/**
 * @brief Get the number of errors collected.
 *
 * @param collector The collector to query. Must not be NULL.
 * @return Number of errors collected, or 0 if collector is invalid.
 */
size_t libvroom_error_collector_count(const libvroom_error_collector_t* collector);

/**
 * @brief Get a specific error by index.
 *
 * Retrieves error information into the provided structure. The error's
 * message and context strings are borrowed from the collector.
 *
 * @param collector The collector to query. Must not be NULL.
 * @param index Zero-based index of the error (0 to count-1).
 * @param[out] error Pointer to structure to fill with error information.
 *                   Must not be NULL.
 * @return LIBVROOM_OK on success, LIBVROOM_ERROR_INVALID_ARGUMENT if
 *         index is out of range.
 *
 * @warning The message and context pointers in the returned error are
 *          only valid until the collector is modified or destroyed.
 *          Copy strings if you need to persist them.
 *
 * @see libvroom_parse_error_t for lifetime requirements.
 */
libvroom_error_t libvroom_error_collector_get(const libvroom_error_collector_t* collector,
                                              size_t index, libvroom_parse_error_t* error);

/**
 * @brief Clear all collected errors.
 *
 * Removes all errors from the collector, allowing it to be reused for
 * another parse operation.
 *
 * @param collector The collector to clear. Must not be NULL.
 *
 * @warning After calling this function, any error message/context pointers
 *          previously obtained from libvroom_error_collector_get() are invalid.
 */
void libvroom_error_collector_clear(libvroom_error_collector_t* collector);

/**
 * @brief Generate a human-readable summary of all collected parse errors.
 *
 * Creates a formatted string containing:
 * - Total error count with breakdown by severity (warnings, errors, fatal)
 * - Detailed listing of each error with location and message
 *
 * @param collector The error collector to summarize.
 * @return Newly allocated string containing the summary. The caller is responsible
 *         for freeing this string using free(). Returns NULL if collector is NULL
 *         or if memory allocation fails.
 *
 * @example
 * @code
 * libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE,
 * 100);
 * // ... parse with errors ...
 * char* summary = libvroom_error_collector_summary(errors);
 * if (summary) {
 *     printf("%s\n", summary);
 *     free(summary);
 * }
 * libvroom_error_collector_destroy(errors);
 * @endcode
 */
char* libvroom_error_collector_summary(const libvroom_error_collector_t* collector);

/**
 * @brief Destroy an error collector and free its resources.
 *
 * @param collector The collector to destroy. May be NULL (no-op).
 *
 * @warning After calling this function, any error message/context pointers
 *          previously obtained from this collector are invalid.
 */
void libvroom_error_collector_destroy(libvroom_error_collector_t* collector);

/** @} */ /* end of error_collector group */

/**
 * @defgroup index Index Structure
 * @brief Functions for creating and querying the parsed field index.
 *
 * The index stores field positions after parsing, enabling random access
 * to CSV fields without re-parsing the file.
 * @{
 */

/**
 * @brief Create a new index for storing parsed field positions.
 *
 * The index must be created before parsing and sized appropriately for
 * the input buffer.
 *
 * @param buffer_length Length of the buffer to be parsed (in bytes).
 *                      Used to estimate initial capacity.
 * @param num_threads Number of threads that will be used for parsing.
 *                    Use libvroom_recommended_threads() if unsure.
 * @return New index handle on success, NULL on failure.
 *         The caller owns the returned index and must call
 *         libvroom_index_destroy() to free it.
 *
 * @see libvroom_recommended_threads() for getting optimal thread count.
 */
libvroom_index_t* libvroom_index_create(size_t buffer_length, size_t num_threads);

/**
 * @brief Get the number of threads the index was created for.
 *
 * @param index The index to query. Must not be NULL.
 * @return Number of threads, or 0 if index is invalid.
 */
size_t libvroom_index_num_threads(const libvroom_index_t* index);

/**
 * @brief Get the number of columns detected in the CSV.
 *
 * @param index The index to query. Must not be NULL.
 * @return Number of columns, or 0 if index is invalid or not yet parsed.
 *
 * @note This is only valid after a successful parse operation.
 */
size_t libvroom_index_columns(const libvroom_index_t* index);

/**
 * @brief Get the number of fields indexed by a specific thread.
 *
 * In multi-threaded parsing, each thread indexes a portion of the file.
 * This returns the count for one thread.
 *
 * @param index The index to query. Must not be NULL.
 * @param thread_id Zero-based thread ID (0 to num_threads-1).
 * @return Number of fields indexed by this thread, or 0 if invalid.
 */
uint64_t libvroom_index_count(const libvroom_index_t* index, size_t thread_id);

/**
 * @brief Get the total number of fields indexed across all threads.
 *
 * @param index The index to query. Must not be NULL.
 * @return Total field count, or 0 if invalid.
 *
 * @note This equals the sum of libvroom_index_count() for all thread IDs.
 */
uint64_t libvroom_index_total_count(const libvroom_index_t* index);

/**
 * @brief Get a pointer to the raw field position array.
 *
 * Returns direct access to the internal position array. Each position is
 * a byte offset into the original buffer where a field starts.
 *
 * @param index The index to query. Must not be NULL.
 * @return Pointer to position array, or NULL if invalid.
 *         The returned pointer is valid until libvroom_index_destroy()
 *         is called.
 *
 * @warning This is a low-level function for advanced use. The pointer
 *          is borrowed and must not be freed.
 */
const uint64_t* libvroom_index_positions(const libvroom_index_t* index);

/**
 * @brief Destroy an index and free its resources.
 *
 * @param index The index to destroy. May be NULL (no-op).
 */
void libvroom_index_destroy(libvroom_index_t* index);

/**
 * @brief Compact the index for O(1) field access.
 *
 * After parsing, field separators are stored in per-thread regions which
 * require O(n_threads) iteration to find a specific field. This function
 * consolidates all separators into a single flat array sorted by file order,
 * enabling O(1) random access.
 *
 * This is particularly beneficial for ALTREP-style lazy column access where
 * fields are accessed randomly and individually.
 *
 * Memory usage: No additional memory allocation - reorganizes existing data.
 *
 * @param index The index to compact. Must be non-null.
 *
 * @note This function is idempotent - calling it multiple times has no effect
 *       after the first successful call.
 *
 * @note Indexes loaded from cache files (via libvroom_index_read()) are
 *       automatically in flat format and don't need explicit compaction.
 *
 * @example
 * @code
 * // Parse a CSV file
 * libvroom_index_t* index = libvroom_index_create(len, 4);
 * libvroom_parse_auto(parser, buf, len, index, errors, NULL, NULL);
 *
 * // Before compact: O(n_threads) per field access
 * // After compact: O(1) per field access
 * libvroom_index_compact(index);
 *
 * // Now field access is O(1)
 * libvroom_field_span_t span = libvroom_index_get_field_span_rc(index, 1000, 0);
 * @endcode
 *
 * @see libvroom_index_is_flat() to check if the index is already compacted
 */
void libvroom_index_compact(libvroom_index_t* index);

/**
 * @brief Check if the index has been compacted for O(1) access.
 *
 * Returns true if the index is in flat format (either from calling
 * libvroom_index_compact() or from loading a cached index).
 *
 * @param index The index to check. Must be non-null.
 * @return true if the index has O(1) field access, false otherwise.
 *
 * @see libvroom_index_compact() to convert an index to flat format
 */
bool libvroom_index_is_flat(const libvroom_index_t* index);

/**
 * @brief Serialize an index to a binary file.
 *
 * Writes the index structure to disk for later retrieval, avoiding the need
 * to re-parse large CSV files. This enables caching parsed indexes for
 * faster subsequent loads.
 *
 * Use cases:
 * - Caching: Parse a large CSV once, save the index, reload on subsequent runs
 * - Sharing: Distribute pre-computed indexes across multiple processes
 * - Testing: Establish reproducible index states for unit testing
 *
 * @param index The index to serialize. Must be non-null and have been populated
 *              by a successful parse operation.
 * @param filename Path to the output file. Will be created or overwritten.
 * @return LIBVROOM_OK on success, or an error code:
 *         - LIBVROOM_ERROR_NULL_POINTER if index or filename is NULL
 *         - LIBVROOM_ERROR_IO if the file cannot be opened or written
 *         - LIBVROOM_ERROR_INVALID_HANDLE if the index has not been populated
 *
 * @see libvroom_index_read() to load a previously saved index
 */
libvroom_error_t libvroom_index_write(const libvroom_index_t* index, const char* filename);

/**
 * @brief Deserialize an index from a binary file.
 *
 * Reads a previously saved index structure from disk. The returned index
 * is fully populated and ready for use with value extraction functions.
 *
 * @param filename Path to the input file created by libvroom_index_write().
 * @return Pointer to the loaded index, or NULL on error. The caller is
 *         responsible for calling libvroom_index_destroy() to free the index.
 *
 * @note Errors include: file not found, corrupted file format, or memory
 *       allocation failure. The function returns NULL in all error cases.
 *
 * @see libvroom_index_write() to save an index
 * @see libvroom_index_destroy() to free the returned index
 */
libvroom_index_t* libvroom_index_read(const char* filename);

/**
 * @brief Sentinel value for invalid field span positions.
 *
 * Used in libvroom_field_span_t to indicate an invalid or unset position.
 * Check with libvroom_field_span_is_valid() or compare start/end to this value.
 */
#define LIBVROOM_FIELD_SPAN_INVALID UINT64_MAX

/**
 * @brief Check if a field span is valid.
 *
 * A field span is valid if both start and end are not LIBVROOM_FIELD_SPAN_INVALID.
 *
 * @param span The field span to check.
 * @return true if the span is valid, false otherwise.
 */
static inline bool libvroom_field_span_is_valid(libvroom_field_span_t span) {
  return span.start != LIBVROOM_FIELD_SPAN_INVALID && span.end != LIBVROOM_FIELD_SPAN_INVALID;
}

/**
 * @brief Get the length of a field span in bytes.
 *
 * @param span The field span to measure.
 * @return Length in bytes, or 0 if the span is invalid.
 */
static inline uint64_t libvroom_field_span_length(libvroom_field_span_t span) {
  return libvroom_field_span_is_valid(span) ? span.end - span.start : 0;
}

/**
 * @brief Get field span by global field index.
 *
 * Iterates through threads in file order to find the field at the given
 * global index. This is O(n_threads) in the worst case but avoids sorting.
 *
 * @param index The index to query. Must not be NULL.
 * @param global_field_idx Zero-based index of the field across the entire file.
 * @return FieldSpan with byte boundaries, or invalid span (LIBVROOM_FIELD_SPAN_INVALID)
 *         if out of bounds or index is NULL.
 *
 * @note For the first field (global_field_idx == 0), the start position is
 *       always 0 (beginning of file).
 *
 * @example
 * @code
 * // Get the 100th field in the file
 * libvroom_field_span_t span = libvroom_index_get_field_span(index, 100);
 * if (libvroom_field_span_is_valid(span)) {
 *     const uint8_t* field_data = libvroom_buffer_data(buffer) + span.start;
 *     size_t field_len = libvroom_field_span_length(span);
 *     // Process field_data[0..field_len]
 * }
 * @endcode
 */
libvroom_field_span_t libvroom_index_get_field_span(const libvroom_index_t* index,
                                                    uint64_t global_field_idx);

/**
 * @brief Get field span by row and column.
 *
 * Converts (row, col) to a global field index and retrieves the byte
 * boundaries. The number of columns must have been set during parsing.
 *
 * @param index The index to query. Must not be NULL.
 * @param row Zero-based row index.
 * @param col Zero-based column index.
 * @return FieldSpan with byte boundaries, or invalid span (LIBVROOM_FIELD_SPAN_INVALID)
 *         if out of bounds or index is NULL.
 *
 * @note Row 0 is the first row (header if has_header is true during parsing).
 *
 * @example
 * @code
 * // Get the field at row 5, column 2
 * libvroom_field_span_t span = libvroom_index_get_field_span_rc(index, 5, 2);
 * if (libvroom_field_span_is_valid(span)) {
 *     const uint8_t* field_data = libvroom_buffer_data(buffer) + span.start;
 *     size_t field_len = libvroom_field_span_length(span);
 *     // Process field_data[0..field_len]
 * }
 * @endcode
 */
libvroom_field_span_t libvroom_index_get_field_span_rc(const libvroom_index_t* index, uint64_t row,
                                                       uint64_t col);

/**
 * @brief Convert a byte offset to row/column location.
 *
 * Given a byte offset into the CSV data, finds which row and column
 * contain that byte offset. This is useful for error reporting, where
 * you have a byte position and need to report the logical location.
 *
 * @param index The index to query. Must not be NULL and must be populated.
 * @param byte_offset Byte offset into the CSV data.
 * @return libvroom_location_t with row, column, and found flag.
 *         If the byte offset is beyond all indexed fields, found will be false.
 *
 * @note Complexity: O(n) where n is the total number of fields. This is
 *       designed for occasional use (e.g., error reporting), not for
 *       high-frequency access patterns.
 *
 * @example
 * @code
 * // Find which row/column contains byte offset 150
 * libvroom_location_t loc = libvroom_index_byte_offset_to_location(index, 150);
 * if (loc.found) {
 *     printf("Byte 150 is at row %zu, column %zu\n", loc.row, loc.column);
 * } else {
 *     printf("Byte 150 is beyond the indexed data\n");
 * }
 * @endcode
 */
libvroom_location_t libvroom_index_byte_offset_to_location(const libvroom_index_t* index,
                                                            size_t byte_offset);

/** @} */ /* end of index group */

/**
 * @defgroup progress Progress Reporting
 * @brief Functions for monitoring parsing progress.
 *
 * Progress callbacks allow monitoring of long-running parse operations.
 * They can be used to display progress bars, update UIs, or implement
 * cancellation.
 * @{
 */

/**
 * @brief Callback signature for progress reporting during parsing.
 *
 * This callback is invoked periodically during parsing to report progress.
 * It can be used to display a progress bar, update a UI, or implement
 * cancellation logic.
 *
 * @param bytes_processed Number of bytes processed so far.
 * @param total_bytes Total number of bytes to process.
 * @param user_data User-provided context pointer (passed to parse functions).
 * @return true to continue parsing, false to abort.
 *
 * @note The callback should return quickly to avoid slowing down parsing.
 *
 * @example
 * @code
 * bool my_progress(size_t processed, size_t total, void* user_data) {
 *     int* percent_ptr = (int*)user_data;
 *     *percent_ptr = total > 0 ? (processed * 100 / total) : 0;
 *     printf("\rProgress: %d%%", *percent_ptr);
 *     fflush(stdout);
 *     return true;  // continue parsing
 * }
 * @endcode
 */
typedef bool (*libvroom_progress_callback_t)(size_t bytes_processed, size_t total_bytes,
                                             void* user_data);

/** @} */ /* end of progress group */

/**
 * @defgroup parser Parser
 * @brief Core parsing functions.
 * @{
 */

/**
 * @brief Create a new parser instance.
 *
 * @return New parser handle on success, NULL on failure.
 *         The caller owns the returned parser and must call
 *         libvroom_parser_destroy() to free it.
 *
 * @note Parser instances maintain internal state and are NOT thread-safe.
 *       Create one parser per thread if parsing concurrently.
 */
libvroom_parser_t* libvroom_parser_create(void);

/**
 * @brief Parse a CSV buffer using the specified dialect.
 *
 * Parses the buffer content and populates the index with field positions.
 * Errors are collected in the error collector according to its mode.
 *
 * @param parser The parser to use. Must not be NULL.
 * @param buffer The buffer containing CSV data. Must not be NULL.
 * @param[in,out] index Index to store field positions. Must not be NULL.
 *                      Will be populated during parsing.
 * @param[in,out] errors Error collector for parse errors. Must not be NULL.
 * @param dialect CSV dialect to use for parsing. Must not be NULL.
 * @return LIBVROOM_OK on success (or if errors were collected but parsing
 *         continued), or an error code if parsing failed completely.
 *
 * @note After a successful parse, use libvroom_index_columns() and related
 *       functions to access the parsed data.
 *
 * @see libvroom_parse_auto() for automatic dialect detection.
 */
libvroom_error_t libvroom_parse(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                libvroom_index_t* index, libvroom_error_collector_t* errors,
                                const libvroom_dialect_t* dialect);

/**
 * @brief Parse a CSV buffer with progress reporting.
 *
 * Same as libvroom_parse(), but additionally calls a progress callback
 * periodically to report parsing progress. This can be used to display
 * progress bars or implement cancellation.
 *
 * @param parser The parser to use. Must not be NULL.
 * @param buffer The buffer containing CSV data. Must not be NULL.
 * @param[in,out] index Index to store field positions. Must not be NULL.
 * @param[in,out] errors Error collector for parse errors. Must not be NULL.
 * @param dialect CSV dialect to use for parsing. Must not be NULL.
 * @param progress Progress callback function. May be NULL (no progress reporting).
 * @param user_data User data pointer passed to the progress callback.
 * @return LIBVROOM_OK on success, or an error code if parsing failed.
 *
 * @note If the progress callback returns false, parsing is aborted and
 *       LIBVROOM_ERROR_CANCELLED is returned.
 *
 * @example
 * @code
 * bool show_progress(size_t processed, size_t total, void* user_data) {
 *     printf("\rProgress: %zu%%", total > 0 ? processed * 100 / total : 0);
 *     fflush(stdout);
 *     return true;
 * }
 *
 * libvroom_error_t err = libvroom_parse_with_progress(
 *     parser, buffer, index, errors, dialect, show_progress, NULL);
 * printf("\n");
 * @endcode
 *
 * @see libvroom_parse() for parsing without progress reporting.
 */
libvroom_error_t
libvroom_parse_with_progress(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                             libvroom_index_t* index, libvroom_error_collector_t* errors,
                             const libvroom_dialect_t* dialect,
                             libvroom_progress_callback_t progress, void* user_data);

/**
 * @brief Destroy a parser and free its resources.
 *
 * @param parser The parser to destroy. May be NULL (no-op).
 */
void libvroom_parser_destroy(libvroom_parser_t* parser);

/**
 * @brief Set per-column configuration for subsequent parse operations.
 *
 * The column configuration is stored on the parser and applied to all
 * subsequent parse operations until cleared or overwritten. The configuration
 * affects value extraction, not the parsing itself.
 *
 * @param parser The parser to configure. Must not be NULL.
 * @param config The column configuration to use. May be NULL to clear.
 * @return LIBVROOM_OK on success, error code on failure.
 *
 * @note The parser takes a copy of the configuration, so the original
 *       can be modified or destroyed after this call.
 *
 * @example
 * @code
 * // Create column config
 * libvroom_column_config_t* config = libvroom_column_config_create();
 * libvroom_column_config_set_type_by_name(config, "id", LIBVROOM_TYPE_INTEGER);
 * libvroom_column_config_set_type_by_name(config, "price", LIBVROOM_TYPE_DOUBLE);
 *
 * // Set on parser
 * libvroom_parser_set_column_config(parser, config);
 *
 * // Parse - column configs are automatically applied
 * libvroom_parse(parser, buffer, index, errors, dialect);
 *
 * // Cleanup
 * libvroom_column_config_destroy(config);
 * @endcode
 *
 * @see libvroom_column_config_create() for creating column configurations.
 */
libvroom_error_t libvroom_parser_set_column_config(libvroom_parser_t* parser,
                                                   const libvroom_column_config_t* config);

/**
 * @brief Get the current per-column configuration from a parser.
 *
 * @param parser The parser to query. Must not be NULL.
 * @return Pointer to the internal column config, or NULL if none is set.
 *         The returned pointer is owned by the parser and must not be freed.
 */
const libvroom_column_config_t* libvroom_parser_get_column_config(const libvroom_parser_t* parser);

/**
 * @brief Clear the per-column configuration from a parser.
 *
 * Equivalent to calling libvroom_parser_set_column_config(parser, NULL).
 *
 * @param parser The parser to clear. Must not be NULL.
 * @return LIBVROOM_OK on success, error code on failure.
 */
libvroom_error_t libvroom_parser_clear_column_config(libvroom_parser_t* parser);

/**
 * @brief Row filtering options for parsing.
 *
 * These options control which rows are included in the parsed result:
 * - skip: Number of data rows to skip at the beginning
 * - n_max: Maximum number of data rows to read (0 = unlimited)
 * - comment: Comment character ('\0' = no comment handling)
 * - skip_empty_rows: Whether to skip rows containing only whitespace
 *
 * @example
 * @code
 * libvroom_row_filter_options_t opts = {
 *     .skip = 10,           // Skip first 10 rows
 *     .n_max = 100,         // Read at most 100 rows
 *     .comment = '#',       // Skip lines starting with #
 *     .skip_empty_rows = true
 * };
 * @endcode
 */
typedef struct libvroom_row_filter_options {
  /** @brief Number of data rows to skip at the beginning (default: 0) */
  size_t skip;

  /** @brief Maximum number of data rows to read, 0 = unlimited (default: 0) */
  size_t n_max;

  /** @brief Comment character, '\0' = no comment handling (default: '\0') */
  char comment;

  /** @brief Whether to skip rows containing only whitespace (default: false) */
  bool skip_empty_rows;
} libvroom_row_filter_options_t;

/**
 * @brief Parse a CSV buffer with row filtering options.
 *
 * Similar to libvroom_parse(), but also applies row filtering based on
 * the provided options. Filtering is applied during iteration/access.
 *
 * @param parser The parser to use. Must not be NULL.
 * @param buffer The buffer containing CSV data. Must not be NULL.
 * @param[in,out] index Index to store field positions. Must not be NULL.
 * @param[in,out] errors Error collector for parse errors. Must not be NULL.
 * @param dialect CSV dialect to use for parsing. Must not be NULL.
 * @param filter Row filtering options. May be NULL for no filtering.
 * @return LIBVROOM_OK on success, or an error code if parsing failed.
 *
 * @see libvroom_parse() for parsing without filtering.
 */
libvroom_error_t libvroom_parse_filtered(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                         libvroom_index_t* index,
                                         libvroom_error_collector_t* errors,
                                         const libvroom_dialect_t* dialect,
                                         const libvroom_row_filter_options_t* filter);

/** @} */ /* end of parser group */

/**
 * @defgroup detection Dialect Detection
 * @brief Functions for automatic CSV format detection.
 *
 * These functions analyze CSV data to automatically detect the delimiter,
 * quote character, and other format settings.
 * @{
 */

/**
 * @brief Detect the CSV dialect of a buffer.
 *
 * Analyzes the buffer content to determine the field delimiter, quote
 * character, and other format settings.
 *
 * @param buffer The buffer to analyze. Must not be NULL.
 * @return Detection result handle on success, NULL on failure.
 *         The caller owns the returned result and must call
 *         libvroom_detection_result_destroy() to free it.
 *
 * @note Detection analyzes a sample of rows, not the entire file.
 *       Use libvroom_detection_result_confidence() to check reliability.
 *
 * @see libvroom_parse_auto() to detect and parse in one step.
 */
libvroom_detection_result_t* libvroom_detect_dialect(const libvroom_buffer_t* buffer);

/**
 * @brief Detect the CSV dialect directly from a file.
 *
 * Convenience function that combines file loading and dialect detection into
 * a single operation. Internally loads a sample of the file and detects the
 * dialect without requiring manual buffer management.
 *
 * @param filename Path to the CSV file to analyze.
 * @return Detection result handle, or NULL on failure.
 *         The caller must call libvroom_detection_result_destroy() to free the result.
 *
 * @example
 * @code
 * libvroom_detection_result_t* result = libvroom_detect_dialect_file("data.csv");
 * if (result && libvroom_detection_result_success(result)) {
 *     libvroom_dialect_t* dialect = libvroom_detection_result_dialect(result);
 *     printf("Delimiter: %c\n", libvroom_dialect_delimiter(dialect));
 *     libvroom_dialect_destroy(dialect);
 * }
 * libvroom_detection_result_destroy(result);
 * @endcode
 */
libvroom_detection_result_t* libvroom_detect_dialect_file(const char* filename);

/**
 * @brief Check if dialect detection was successful.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return true if detection succeeded, false otherwise.
 */
bool libvroom_detection_result_success(const libvroom_detection_result_t* result);

/**
 * @brief Get the confidence level of the detection.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return Confidence value from 0.0 (no confidence) to 1.0 (certain).
 *         Generally, values above 0.8 indicate reliable detection.
 */
double libvroom_detection_result_confidence(const libvroom_detection_result_t* result);

/**
 * @brief Get the detected dialect from a detection result.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return New dialect handle, or NULL if detection failed.
 *         The caller owns the returned dialect and must call
 *         libvroom_dialect_destroy() to free it.
 *
 * @note This creates a new dialect object. The caller must destroy it
 *       separately from the detection result.
 */
libvroom_dialect_t* libvroom_detection_result_dialect(const libvroom_detection_result_t* result);

/**
 * @brief Get the number of columns detected.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return Number of columns detected, or 0 if detection failed.
 */
size_t libvroom_detection_result_columns(const libvroom_detection_result_t* result);

/**
 * @brief Get the number of rows analyzed during detection.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return Number of sample rows analyzed, or 0 if detection failed.
 */
size_t libvroom_detection_result_rows_analyzed(const libvroom_detection_result_t* result);

/**
 * @brief Check if the file appears to have a header row.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return true if a header row was detected, false otherwise.
 */
bool libvroom_detection_result_has_header(const libvroom_detection_result_t* result);

/**
 * @brief Get any warning message from detection.
 *
 * @param result The detection result to query. Must not be NULL.
 * @return Warning message string, or NULL if no warning.
 *         The returned pointer is borrowed and valid until the
 *         detection result is destroyed.
 */
const char* libvroom_detection_result_warning(const libvroom_detection_result_t* result);

/**
 * @brief Destroy a detection result and free its resources.
 *
 * @param result The detection result to destroy. May be NULL (no-op).
 */
void libvroom_detection_result_destroy(libvroom_detection_result_t* result);

/**
 * @brief Detect dialect and parse a buffer in one step.
 *
 * Convenience function that combines dialect detection with parsing.
 * Equivalent to calling libvroom_detect_dialect() followed by libvroom_parse(),
 * but more efficient.
 *
 * @param parser The parser to use. Must not be NULL.
 * @param buffer The buffer containing CSV data. Must not be NULL.
 * @param[in,out] index Index to store field positions. Must not be NULL.
 * @param[in,out] errors Error collector for parse errors. Must not be NULL.
 * @param[out] detected Optional pointer to receive detection result.
 *                      If not NULL, the caller must destroy the result.
 *                      Pass NULL if you don't need the detection details.
 * @return LIBVROOM_OK on success, or an error code on failure.
 *
 * @par Example
 * @code{.c}
 * libvroom_detection_result_t* detected = NULL;
 * libvroom_error_t err = libvroom_parse_auto(parser, buffer, index, errors, &detected);
 *
 * if (err == LIBVROOM_OK && detected) {
 *     printf("Detected %zu columns with %.0f%% confidence\n",
 *            libvroom_detection_result_columns(detected),
 *            libvroom_detection_result_confidence(detected) * 100);
 *     libvroom_detection_result_destroy(detected);
 * }
 * @endcode
 */
libvroom_error_t libvroom_parse_auto(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                     libvroom_index_t* index, libvroom_error_collector_t* errors,
                                     libvroom_detection_result_t** detected);

/** @} */ /* end of detection group */

/**
 * @defgroup utility Utility Functions
 * @brief Helper functions for configuration and system information.
 * @{
 */

/**
 * @brief Get the recommended number of threads for parsing.
 *
 * Returns an optimal thread count based on the system's CPU configuration.
 * Uses hardware concurrency detection.
 *
 * @return Recommended number of threads (always >= 1).
 */
size_t libvroom_recommended_threads(void);

/**
 * @brief Get the required SIMD padding size in bytes.
 *
 * Buffers should include this much padding beyond their actual data length
 * to allow SIMD operations to safely read past the end.
 *
 * @return Padding size in bytes (typically 64 for AVX-512).
 *
 * @note This is handled automatically by libvroom_buffer_create() and
 *       libvroom_buffer_load_file(). Only needed for custom buffer management.
 */
size_t libvroom_simd_padding(void);

/** @} */ /* end of utility group */

/**
 * @defgroup encoding Encoding Detection and Transcoding
 * @brief Functions for detecting file encodings and transcoding to UTF-8.
 *
 * These functions handle files with various character encodings (UTF-8,
 * UTF-16, UTF-32, Latin-1) and automatically transcode to UTF-8 for parsing.
 * @{
 */

/**
 * @brief Character encodings supported by the parser.
 */
typedef enum libvroom_encoding {
  LIBVROOM_ENCODING_UTF8 = 0,     /**< UTF-8 (default) */
  LIBVROOM_ENCODING_UTF8_BOM = 1, /**< UTF-8 with BOM (EF BB BF) */
  LIBVROOM_ENCODING_UTF16_LE = 2, /**< UTF-16 Little Endian */
  LIBVROOM_ENCODING_UTF16_BE = 3, /**< UTF-16 Big Endian */
  LIBVROOM_ENCODING_UTF32_LE = 4, /**< UTF-32 Little Endian */
  LIBVROOM_ENCODING_UTF32_BE = 5, /**< UTF-32 Big Endian */
  LIBVROOM_ENCODING_LATIN1 = 6,   /**< Latin-1 (ISO-8859-1) */
  LIBVROOM_ENCODING_UNKNOWN = 7   /**< Unknown encoding */
} libvroom_encoding_t;

/**
 * @brief Result of encoding detection.
 *
 * This structure is filled by libvroom_detect_encoding() with information
 * about the detected encoding.
 */
typedef struct libvroom_encoding_result {
  libvroom_encoding_t encoding; /**< Detected encoding */
  size_t bom_length;            /**< Length of BOM in bytes (0 if no BOM) */
  double confidence;            /**< Detection confidence [0.0, 1.0] */
  bool needs_transcoding;       /**< True if transcoding to UTF-8 is needed */
} libvroom_encoding_result_t;

/**
 * @brief Opaque handle to a load result (buffer + encoding info).
 *
 * Created by libvroom_load_file_with_encoding(), destroyed with
 * libvroom_load_result_destroy(). Contains both the (possibly transcoded)
 * data and information about the original encoding.
 */
typedef struct libvroom_load_result libvroom_load_result_t;

/**
 * @brief Convert encoding enum to human-readable string.
 *
 * @param encoding The encoding to convert.
 * @return C-string name of the encoding (e.g., "UTF-16LE", "UTF-8").
 *         The returned string is statically allocated and should not be freed.
 */
const char* libvroom_encoding_string(libvroom_encoding_t encoding);

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
 * @param data Pointer to the byte buffer. Must not be NULL if length > 0.
 * @param length Length of the buffer in bytes.
 * @param[out] result Pointer to store the detection result. Must not be NULL.
 * @return LIBVROOM_OK on success, error code on failure.
 */
libvroom_error_t libvroom_detect_encoding(const uint8_t* data, size_t length,
                                          libvroom_encoding_result_t* result);

/**
 * @brief Load a file with automatic encoding detection and transcoding.
 *
 * This function detects the encoding of a file (via BOM or heuristics),
 * and automatically transcodes UTF-16 and UTF-32 files to UTF-8. The
 * returned data is always UTF-8 (or ASCII-compatible) for parsing.
 *
 * @param filename Path to the file to load.
 * @return Handle to the load result, or NULL on failure.
 *         The caller must call libvroom_load_result_destroy() to free the result.
 *
 * @see libvroom_buffer_load_file() for simple loading without transcoding.
 */
libvroom_load_result_t* libvroom_load_file_with_encoding(const char* filename);

/**
 * @brief Get the buffer from a load result.
 *
 * @param result The load result handle. Must not be NULL.
 * @return Pointer to the loaded data (UTF-8 encoded), or NULL if invalid.
 *         The pointer is valid until libvroom_load_result_destroy() is called.
 *
 * @warning The returned pointer is borrowed - do not free it directly.
 */
const uint8_t* libvroom_load_result_data(const libvroom_load_result_t* result);

/**
 * @brief Get the length of the loaded data.
 *
 * @param result The load result handle. Must not be NULL.
 * @return Length of the data in bytes, or 0 if invalid.
 */
size_t libvroom_load_result_length(const libvroom_load_result_t* result);

/**
 * @brief Get the detected encoding from a load result.
 *
 * @param result The load result handle. Must not be NULL.
 * @return The detected encoding, or LIBVROOM_ENCODING_UNKNOWN if invalid.
 */
libvroom_encoding_t libvroom_load_result_encoding(const libvroom_load_result_t* result);

/**
 * @brief Get the BOM length from a load result.
 *
 * @param result The load result handle. Must not be NULL.
 * @return Length of the BOM in bytes (0 if no BOM), or 0 if invalid.
 */
size_t libvroom_load_result_bom_length(const libvroom_load_result_t* result);

/**
 * @brief Get the detection confidence from a load result.
 *
 * @param result The load result handle. Must not be NULL.
 * @return Detection confidence [0.0, 1.0], or 0.0 if invalid.
 */
double libvroom_load_result_confidence(const libvroom_load_result_t* result);

/**
 * @brief Check if the loaded data was modified from the original file.
 *
 * Returns true if any transformation was applied to the data, including:
 * - Transcoding from UTF-16 or UTF-32 to UTF-8
 * - Stripping a BOM (Byte Order Mark), including UTF-8 BOM
 *
 * @param result The load result handle. Must not be NULL.
 * @return true if the data was transformed, false if unchanged.
 */
bool libvroom_load_result_was_transcoded(const libvroom_load_result_t* result);

/**
 * @brief Create a buffer from the load result for parsing.
 *
 * Creates a new buffer suitable for use with libvroom_parse() and
 * related functions. The buffer includes the necessary SIMD padding.
 *
 * @param result The load result handle. Must not be NULL.
 * @return A new buffer handle, or NULL on failure.
 *         The caller must call libvroom_buffer_destroy() to free the buffer.
 *
 * @note This creates a copy of the data. For large files, consider using
 *       libvroom_load_result_data() directly if you need read-only access.
 */
libvroom_buffer_t* libvroom_load_result_to_buffer(const libvroom_load_result_t* result);

/**
 * @brief Destroy a load result and free its resources.
 *
 * @param result The load result handle to destroy. May be NULL (no-op).
 */
void libvroom_load_result_destroy(libvroom_load_result_t* result);

/** @} */ /* end of encoding group */

/**
 * @defgroup column_config Per-Column Configuration
 * @brief Functions for configuring per-column extraction settings.
 *
 * These functions allow specifying different extraction settings for
 * individual columns, such as type hints, custom NA values, and boolean
 * parsing rules. This is useful for CSVs with mixed formats or when
 * automatic type detection needs to be overridden.
 * @{
 */

/**
 * @brief Type hints for per-column configuration.
 *
 * Used to override automatic type detection for specific columns.
 */
typedef enum libvroom_type_hint {
  LIBVROOM_TYPE_AUTO = 0,     /**< Automatic type detection (default) */
  LIBVROOM_TYPE_BOOLEAN = 1,  /**< Force interpretation as boolean */
  LIBVROOM_TYPE_INTEGER = 2,  /**< Force interpretation as integer */
  LIBVROOM_TYPE_DOUBLE = 3,   /**< Force interpretation as double/float */
  LIBVROOM_TYPE_STRING = 4,   /**< Force interpretation as string (no conversion) */
  LIBVROOM_TYPE_DATE = 5,     /**< Force interpretation as date */
  LIBVROOM_TYPE_DATETIME = 6, /**< Force interpretation as datetime/timestamp */
  LIBVROOM_TYPE_SKIP = 7      /**< Skip this column during extraction */
} libvroom_type_hint_t;

/**
 * @brief Create a new empty column configuration map.
 *
 * @return New column config handle on success, NULL on failure.
 *         The caller owns the returned handle and must call
 *         libvroom_column_config_destroy() to free it.
 */
libvroom_column_config_t* libvroom_column_config_create(void);

/**
 * @brief Set a type hint for a column by index.
 *
 * @param config The column config handle. Must not be NULL.
 * @param col_index 0-based column index.
 * @param type_hint The type hint to set for this column.
 * @return LIBVROOM_OK on success, error code on failure.
 */
libvroom_error_t libvroom_column_config_set_type_by_index(libvroom_column_config_t* config,
                                                          size_t col_index,
                                                          libvroom_type_hint_t type_hint);

/**
 * @brief Set a type hint for a column by name.
 *
 * @param config The column config handle. Must not be NULL.
 * @param col_name Column name (case-sensitive). Must not be NULL.
 * @param type_hint The type hint to set for this column.
 * @return LIBVROOM_OK on success, error code on failure.
 */
libvroom_error_t libvroom_column_config_set_type_by_name(libvroom_column_config_t* config,
                                                         const char* col_name,
                                                         libvroom_type_hint_t type_hint);

/**
 * @brief Set custom NA values for a column by index.
 *
 * @param config The column config handle. Must not be NULL.
 * @param col_index 0-based column index.
 * @param na_values Array of null-terminated strings to treat as NA.
 * @param num_values Number of strings in the na_values array.
 * @return LIBVROOM_OK on success, error code on failure.
 *
 * @note The strings are copied, so the caller may free them after this call.
 */
libvroom_error_t libvroom_column_config_set_na_values_by_index(libvroom_column_config_t* config,
                                                               size_t col_index,
                                                               const char** na_values,
                                                               size_t num_values);

/**
 * @brief Set custom NA values for a column by name.
 *
 * @param config The column config handle. Must not be NULL.
 * @param col_name Column name (case-sensitive). Must not be NULL.
 * @param na_values Array of null-terminated strings to treat as NA.
 * @param num_values Number of strings in the na_values array.
 * @return LIBVROOM_OK on success, error code on failure.
 *
 * @note The strings are copied, so the caller may free them after this call.
 */
libvroom_error_t libvroom_column_config_set_na_values_by_name(libvroom_column_config_t* config,
                                                              const char* col_name,
                                                              const char** na_values,
                                                              size_t num_values);

/**
 * @brief Get the type hint for a column by index.
 *
 * @param config The column config handle. Must not be NULL.
 * @param col_index 0-based column index.
 * @return The type hint for the column, or LIBVROOM_TYPE_AUTO if not set.
 */
libvroom_type_hint_t
libvroom_column_config_get_type_by_index(const libvroom_column_config_t* config, size_t col_index);

/**
 * @brief Check if any column configurations are set.
 *
 * @param config The column config handle. Must not be NULL.
 * @return true if no configurations are set, false otherwise.
 */
bool libvroom_column_config_empty(const libvroom_column_config_t* config);

/**
 * @brief Clear all column configurations.
 *
 * @param config The column config handle. Must not be NULL.
 */
void libvroom_column_config_clear(libvroom_column_config_t* config);

/**
 * @brief Destroy a column configuration map and free its resources.
 *
 * @param config The column config handle to destroy. May be NULL (no-op).
 */
void libvroom_column_config_destroy(libvroom_column_config_t* config);

/**
 * @brief Convert a type hint enum to its string representation.
 *
 * @param type_hint The type hint to convert.
 * @return Static string name of the type hint (e.g., "integer", "double").
 *         The returned string is statically allocated and should not be freed.
 */
const char* libvroom_type_hint_string(libvroom_type_hint_t type_hint);

/** @} */ /* end of column_config group */

/**
 * @defgroup lazy_column Lazy Column Access
 * @brief Functions for ALTREP-style lazy column access.
 *
 * The LazyColumn API provides per-column lazy access to CSV data without
 * parsing the entire file upfront. This enables R's ALTREP pattern where
 * columns are only parsed when accessed.
 *
 * Key features:
 * - **Random access**: O(n_threads) access to any row
 * - **Byte range access**: Get raw byte ranges for deferred parsing
 * - **Zero-copy views**: Returns views into the original buffer
 *
 * @{
 */

/**
 * @brief Create a lazy column accessor.
 *
 * Creates a lazy accessor for a specific column in a parsed CSV file.
 * The accessor provides random access to field values without requiring
 * a full sort of the index.
 *
 * @param buffer The buffer containing CSV data. Must remain valid for the
 *               lifetime of the lazy column.
 * @param index The parsed index. Must remain valid for the lifetime of
 *              the lazy column.
 * @param col Column index (0-based).
 * @param has_header Whether the CSV has a header row (affects row indexing).
 * @param dialect CSV dialect for quote handling. May be NULL for default CSV.
 * @return New lazy column handle on success, NULL on failure.
 *         The caller owns the returned handle and must call
 *         libvroom_lazy_column_destroy() to free it.
 *
 * @warning The buffer and index must remain valid for the lifetime of the
 *          lazy column. Destroying either while the lazy column is in use
 *          results in undefined behavior.
 *
 * @example
 * @code
 * // Parse CSV
 * libvroom_error_t err = libvroom_parse(parser, buffer, index, errors, dialect);
 *
 * // Create lazy accessor for column 0
 * libvroom_lazy_column_t* col = libvroom_lazy_column_create(
 *     buffer, index, 0, true, dialect);
 *
 * // Access values lazily
 * for (size_t i = 0; i < libvroom_lazy_column_size(col); ++i) {
 *     libvroom_field_span_t span = libvroom_lazy_column_get_bounds(col, i);
 *     // Process span...
 * }
 *
 * libvroom_lazy_column_destroy(col);
 * @endcode
 */
libvroom_lazy_column_t* libvroom_lazy_column_create(const libvroom_buffer_t* buffer,
                                                    const libvroom_index_t* index, size_t col,
                                                    bool has_header,
                                                    const libvroom_dialect_t* dialect);

/**
 * @brief Get the number of data rows in the lazy column.
 *
 * @param column The lazy column to query. Must not be NULL.
 * @return Number of rows (excludes header if has_header was true), or 0 if invalid.
 */
size_t libvroom_lazy_column_size(const libvroom_lazy_column_t* column);

/**
 * @brief Check if the lazy column is empty.
 *
 * @param column The lazy column to query. Must not be NULL.
 * @return true if empty or invalid, false otherwise.
 */
bool libvroom_lazy_column_empty(const libvroom_lazy_column_t* column);

/**
 * @brief Get the column index.
 *
 * @param column The lazy column to query. Must not be NULL.
 * @return 0-based column index, or 0 if invalid.
 */
size_t libvroom_lazy_column_index(const libvroom_lazy_column_t* column);

/**
 * @brief Get raw byte boundaries for a row.
 *
 * Returns the byte range in the source buffer for deferred parsing.
 * This enables the ALTREP pattern where parsing happens only when
 * the value is actually needed.
 *
 * @param column The lazy column to access. Must not be NULL.
 * @param row Row index (0-based, excludes header).
 * @return FieldSpan with start/end byte positions, or invalid span if out of bounds.
 *
 * @note Complexity: O(n_threads) due to ParseIndex::get_field_span()
 *
 * @example
 * @code
 * libvroom_field_span_t span = libvroom_lazy_column_get_bounds(col, row);
 * if (libvroom_field_span_is_valid(span)) {
 *     const uint8_t* data = libvroom_buffer_data(buffer) + span.start;
 *     size_t len = libvroom_field_span_length(span);
 *     // Parse data[0..len] on demand
 * }
 * @endcode
 */
libvroom_field_span_t libvroom_lazy_column_get_bounds(const libvroom_lazy_column_t* column,
                                                      size_t row);

/**
 * @brief Get string value for a row.
 *
 * Returns a pointer to the field content. The returned pointer points into
 * the original buffer and is only valid as long as the buffer remains valid.
 * For quoted fields, the outer quotes are stripped.
 *
 * @param column The lazy column to access. Must not be NULL.
 * @param row Row index (0-based, excludes header).
 * @param[out] length Pointer to receive the string length. May be NULL.
 * @return Pointer to field content, or NULL if row is out of bounds.
 *         The pointer is valid until the buffer is destroyed.
 *
 * @warning The returned pointer is borrowed from the buffer. Do not free it.
 */
const char* libvroom_lazy_column_get_string(const libvroom_lazy_column_t* column, size_t row,
                                            size_t* length);

/**
 * @brief Destroy a lazy column and free its resources.
 *
 * @param column The lazy column to destroy. May be NULL (no-op).
 *
 * @note This does not affect the underlying buffer or index.
 */
void libvroom_lazy_column_destroy(libvroom_lazy_column_t* column);

/** @} */ /* end of lazy_column group */

#ifdef __cplusplus
}
#endif

#endif /* LIBVROOM_C_H */
