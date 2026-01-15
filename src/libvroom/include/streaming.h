/**
 * @file streaming.h
 * @brief Streaming API for memory-efficient CSV parsing.
 *
 * This header provides a streaming interface for CSV parsing that processes
 * data row-by-row without building a complete in-memory index. This is ideal
 * for memory-constrained environments or when processing large files.
 *
 * Two parsing models are supported:
 * - **Push Model**: Feed data chunks to the parser, callbacks are invoked for each row
 * - **Pull Model**: Request rows one at a time via next_row() or iterate with range-for
 *
 * @example Push Model
 * @code
 * libvroom::StreamParser parser;
 * parser.set_row_handler([](const libvroom::Row& row) {
 *     std::cout << row[0].data << "\n";
 *     return true;  // continue parsing
 * });
 *
 * std::ifstream file("large.csv", std::ios::binary);
 * char buffer[65536];
 * while (file.read(buffer, sizeof(buffer)) || file.gcount() > 0) {
 *     parser.parse_chunk(buffer, file.gcount());
 * }
 * parser.finish();
 * @endcode
 *
 * @example Pull Model
 * @code
 * libvroom::StreamReader reader("large.csv");
 * for (const auto& row : reader) {
 *     std::cout << row[0].data << "\n";
 * }
 * @endcode
 *
 * @see two_pass.h for batch parsing with full indexing
 * @see dialect.h for dialect configuration
 * @see error.h for error handling
 */

#ifndef LIBVROOM_STREAMING_H
#define LIBVROOM_STREAMING_H

#include "dialect.h"
#include "error.h"

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace libvroom {

/**
 * @brief Status codes returned by streaming operations.
 */
enum class StreamStatus {
  OK,             ///< Operation succeeded
  ROW_READY,      ///< A complete row is available (pull model)
  END_OF_DATA,    ///< No more data to process
  NEED_MORE_DATA, ///< Parser needs more input data
  STREAM_ERROR    ///< Parse error occurred (renamed from ERROR to avoid Windows
                  ///< macro conflict)
};

/**
 * @brief Represents a single field within a row.
 *
 * Provides zero-copy access to field data via string_view.
 * The underlying data is valid only until the next row is fetched
 * or the parser is reset.
 */
struct Field {
  std::string_view data; ///< View into the buffer (zero-copy)
  bool is_quoted;        ///< Whether the field was quoted in the source
  size_t field_index;    ///< Column index (0-based)

  /// Returns true if the field is empty
  bool empty() const { return data.empty(); }

  /// Returns the field content as a string (allocates memory)
  std::string str() const { return std::string(data); }

  /**
   * @brief Returns the field with quotes and escapes removed.
   *
   * For quoted fields, removes surrounding quotes and handles escape sequences
   * (e.g., "" becomes "). For unquoted fields, returns the data as-is.
   *
   * @param quote_char The quote character used (default: ")
   * @return Unescaped field content (allocates memory)
   */
  std::string unescaped(char quote_char = '"') const;
};

/**
 * @brief Represents a complete row from the CSV.
 *
 * Provides access to all fields in the current row. The data is valid
 * only until StreamParser::next_row() is called again or the callback returns.
 */
class Row {
public:
  /// Default constructor (empty row)
  Row() = default;

  /// Number of fields in this row
  size_t field_count() const { return fields_.size(); }

  /// Check if row is empty
  bool empty() const { return fields_.empty(); }

  /// Access field by index (0-based), no bounds checking
  const Field& operator[](size_t index) const { return fields_[index]; }

  /// Access field by index with bounds checking
  const Field& at(size_t index) const;

  /// Access field by column name (requires header parsing)
  /// @throws std::out_of_range if column name not found
  const Field& operator[](const std::string& name) const;

  /// Current row number (1-based, counts from start of file/stream)
  size_t row_number() const { return row_number_; }

  /// Byte offset where this row starts in the source
  size_t byte_offset() const { return byte_offset_; }

  /// Iterator support for range-based for
  using iterator = std::vector<Field>::const_iterator;
  iterator begin() const { return fields_.begin(); }
  iterator end() const { return fields_.end(); }

private:
  friend class StreamParser;
  friend class StreamReader;

  std::vector<Field> fields_;
  std::vector<std::string> field_storage_; // Owns the field string data
  size_t row_number_ = 0;
  size_t byte_offset_ = 0;

  // Column name lookup (set by StreamParser if header parsing enabled)
  const std::unordered_map<std::string, size_t>* column_map_ = nullptr;

  void clear() {
    fields_.clear();
    field_storage_.clear();
    row_number_ = 0;
    byte_offset_ = 0;
  }
};

/**
 * @brief Configuration for the streaming parser.
 */
struct StreamConfig {
  Dialect dialect = Dialect::csv();             ///< CSV dialect settings
  ErrorMode error_mode = ErrorMode::PERMISSIVE; ///< Error handling mode

  size_t chunk_size = 64 * 1024;            ///< Default chunk size for file reading (64KB)
  size_t max_field_size = 16 * 1024 * 1024; ///< Maximum field size (16MB, for safety)
  size_t initial_field_capacity = 64;       ///< Initial capacity for fields vector

  bool parse_header = true;     ///< Parse first row as header
  bool skip_empty_rows = false; ///< Skip rows with no fields
};

/**
 * @brief Callback signature for row processing (push model).
 *
 * @param row The current row being processed
 * @return true to continue parsing, false to stop
 */
using RowCallback = std::function<bool(const Row& row)>;

/**
 * @brief Callback signature for error handling.
 *
 * @param error The parse error that occurred
 * @return true to continue parsing, false to stop
 */
using ErrorCallback = std::function<bool(const ParseError& error)>;

/**
 * @brief Streaming CSV parser supporting both push and pull models.
 *
 * This class provides memory-efficient CSV parsing by processing data
 * incrementally without building a complete index of field positions.
 *
 * ## Push Model (Callback-based)
 *
 * Set a row handler callback and feed data chunks:
 * @code
 * libvroom::StreamParser parser;
 * parser.set_row_handler([](const libvroom::Row& row) {
 *     process(row);
 *     return true;
 * });
 *
 * while (has_data()) {
 *     parser.parse_chunk(data, size);
 * }
 * parser.finish();
 * @endcode
 *
 * ## Pull Model (Iterator-based)
 *
 * Call next_row() and access current_row():
 * @code
 * libvroom::StreamParser parser;
 * parser.parse_chunk(data, size);
 *
 * while (parser.next_row() == libvroom::StreamStatus::ROW_READY) {
 *     const auto& row = parser.current_row();
 *     process(row);
 * }
 * @endcode
 */
class StreamParser {
public:
  /**
   * @brief Construct a streaming parser with the given configuration.
   * @param config Parser configuration
   */
  explicit StreamParser(const StreamConfig& config = StreamConfig());

  /// Destructor
  ~StreamParser();

  // Non-copyable, moveable
  StreamParser(const StreamParser&) = delete;
  StreamParser& operator=(const StreamParser&) = delete;
  StreamParser(StreamParser&&) noexcept;
  StreamParser& operator=(StreamParser&&) noexcept;

  //--- Configuration ---//

  /// Get the current configuration (read-only)
  const StreamConfig& config() const;

  //--- Push Model Operations ---//

  /**
   * @brief Set the row callback handler (push model).
   *
   * The callback is invoked for each complete row found during parsing.
   * Return true from the callback to continue parsing, false to stop.
   *
   * @param callback Function to call for each row
   */
  void set_row_handler(RowCallback callback);

  /**
   * @brief Set the error callback handler.
   *
   * The callback is invoked when parse errors occur. Return true to
   * continue parsing, false to stop.
   *
   * @param callback Function to call for each error
   */
  void set_error_handler(ErrorCallback callback);

  /**
   * @brief Feed a chunk of data to the parser (push model).
   *
   * The parser will invoke the row callback for each complete row found.
   * Partial rows at chunk boundaries are buffered internally.
   *
   * @param data Pointer to chunk data
   * @param size Size of chunk in bytes
   * @return StreamStatus::OK if processed successfully
   */
  StreamStatus parse_chunk(const uint8_t* data, size_t size);

  /// Convenience overload for char* data
  StreamStatus parse_chunk(const char* data, size_t size) {
    return parse_chunk(reinterpret_cast<const uint8_t*>(data), size);
  }

  /// Convenience overload for string_view
  StreamStatus parse_chunk(std::string_view data) {
    return parse_chunk(reinterpret_cast<const uint8_t*>(data.data()), data.size());
  }

  /**
   * @brief Signal end of input and process any remaining data.
   *
   * Must be called after all chunks have been fed to process
   * any partial row at the end of the file.
   *
   * @return StreamStatus::END_OF_DATA on success, StreamStatus::STREAM_ERROR if errors occurred
   */
  StreamStatus finish();

  /**
   * @brief Reset parser state for reuse with new input.
   *
   * Clears all internal buffers and state, allowing the parser
   * to be reused for a new file or stream.
   */
  void reset();

  //--- Pull Model Operations ---//

  /**
   * @brief Attempt to parse and return the next row (pull model).
   *
   * This method extracts the next complete row from the buffered data.
   * If no complete row is available, returns NEED_MORE_DATA.
   *
   * @return StreamStatus::ROW_READY if a row is available,
   *         StreamStatus::NEED_MORE_DATA if more input needed,
   *         StreamStatus::END_OF_DATA if finish() was called and no more rows,
   *         StreamStatus::STREAM_ERROR if a parse error occurred
   */
  StreamStatus next_row();

  /**
   * @brief Get the current row (valid after next_row() returns ROW_READY).
   *
   * @return Reference to the current row
   * @note The returned reference is invalidated by the next call to next_row()
   */
  const Row& current_row() const;

  //--- State Queries ---//

  /// Get header column names (if parse_header enabled)
  const std::vector<std::string>& header() const;

  /// Get column index by name (-1 if not found)
  int column_index(const std::string& name) const;

  /// Number of rows processed so far (excluding header if parse_header enabled)
  size_t rows_processed() const;

  /// Total bytes processed so far
  size_t bytes_processed() const;

  /// Get error collector for inspecting accumulated errors
  const ErrorCollector& error_collector() const;

  /// Check if the parser has finished (finish() was called)
  bool is_finished() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/**
 * @brief Iterator for range-based for loops over CSV rows from StreamReader.
 *
 * This is an input iterator that reads rows from a StreamReader.
 * Named StreamRowIterator to avoid conflict with value_extraction.h::RowIterator.
 */
class StreamRowIterator {
public:
  using iterator_category = std::input_iterator_tag;
  using value_type = Row;
  using difference_type = std::ptrdiff_t;
  using pointer = const Row*;
  using reference = const Row&;

  /// Create end iterator
  StreamRowIterator();

  /// Create iterator from StreamReader
  explicit StreamRowIterator(class StreamReader* reader);

  reference operator*() const;
  pointer operator->() const;

  StreamRowIterator& operator++();
  StreamRowIterator operator++(int);

  bool operator==(const StreamRowIterator& other) const;
  bool operator!=(const StreamRowIterator& other) const;

private:
  class StreamReader* reader_ = nullptr;
  bool at_end_ = true;
};

/**
 * @brief High-level file reader with pull-model iteration.
 *
 * StreamReader combines StreamParser with file I/O, automatically
 * reading and parsing data in chunks.
 *
 * @example
 * @code
 * libvroom::StreamReader reader("data.csv");
 *
 * // Method 1: Range-based for loop
 * for (const auto& row : reader) {
 *     std::cout << row[0].data << "\n";
 * }
 *
 * // Method 2: Explicit iteration
 * libvroom::StreamReader reader2("data.csv");
 * while (reader2.next_row()) {
 *     const auto& row = reader2.row();
 *     std::cout << row[0].data << "\n";
 * }
 * @endcode
 */
class StreamReader {
public:
  /**
   * @brief Construct a reader for the given file.
   *
   * @param filename Path to the CSV file
   * @param config Parser configuration
   * @throws std::runtime_error if file cannot be opened
   */
  explicit StreamReader(const std::string& filename, const StreamConfig& config = StreamConfig());

  /**
   * @brief Construct a reader from an input stream.
   *
   * @param input Input stream to read from
   * @param config Parser configuration
   */
  explicit StreamReader(std::istream& input, const StreamConfig& config = StreamConfig());

  /// Destructor
  ~StreamReader();

  // Non-copyable
  StreamReader(const StreamReader&) = delete;
  StreamReader& operator=(const StreamReader&) = delete;

  // Moveable
  StreamReader(StreamReader&&) noexcept;
  StreamReader& operator=(StreamReader&&) noexcept;

  /// Access configuration (read-only after construction)
  const StreamConfig& config() const;

  /**
   * @brief Read next row from the file.
   *
   * @return true if a row was read, false at end of file or on error
   */
  bool next_row();

  /**
   * @brief Get current row (valid after next_row() returns true).
   *
   * @return Reference to the current row
   */
  const Row& row() const;

  /// Get header column names (if parse_header enabled)
  const std::vector<std::string>& header() const;

  /// Get column index by name (-1 if not found)
  int column_index(const std::string& name) const;

  /// Get error collector for inspecting errors
  const ErrorCollector& error_collector() const;

  /// Number of rows read (excluding header)
  size_t rows_read() const;

  /// Total bytes read from file
  size_t bytes_read() const;

  /// Check if end of file has been reached
  bool eof() const;

  /// Iterator support for range-based for
  StreamRowIterator begin();
  StreamRowIterator end();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class StreamRowIterator;
};

} // namespace libvroom

#endif // LIBVROOM_STREAMING_H
