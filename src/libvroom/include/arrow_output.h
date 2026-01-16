/**
 * @file arrow_output.h
 * @brief Apache Arrow output integration for libvroom.
 *
 * This header provides functionality to convert parsed CSV data into Apache Arrow
 * format (Arrays and Tables). Arrow integration is optional and requires building
 * with -DLIBVROOM_ENABLE_ARROW=ON.
 *
 * @note This header is only available when compiled with LIBVROOM_ENABLE_ARROW=ON
 */

#ifndef LIBVROOM_ARROW_OUTPUT_H
#define LIBVROOM_ARROW_OUTPUT_H

#ifdef LIBVROOM_ENABLE_ARROW

#include "dialect.h"
#include "two_pass.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace libvroom {

enum class ColumnType { STRING, INT64, DOUBLE, BOOLEAN, DATE, TIMESTAMP, NULL_TYPE, AUTO };

std::shared_ptr<arrow::DataType> column_type_to_arrow(ColumnType type);
const char* column_type_to_string(ColumnType type);

struct ColumnSpec {
  std::string name;
  ColumnType type = ColumnType::AUTO;
  std::shared_ptr<arrow::DataType> arrow_type = nullptr;
  bool nullable = true;

  ColumnSpec() = default;
  ColumnSpec(std::string name_, ColumnType type_ = ColumnType::AUTO)
      : name(std::move(name_)), type(type_) {}
};

struct ArrowConvertOptions {
  bool infer_types = true;
  // Number of rows to sample for type inference (0 = all rows).
  // Maximum allowed value is MAX_TYPE_INFERENCE_ROWS; exceeding it throws std::invalid_argument.
  size_t type_inference_rows = 1000;
  bool empty_is_null = false;
  std::vector<std::string> null_values = {"", "NA", "N/A", "null", "NULL", "None", "NaN"};
  std::vector<std::string> true_values = {"true", "True", "TRUE", "1", "yes", "Yes", "YES"};
  std::vector<std::string> false_values = {"false", "False", "FALSE", "0", "no", "No", "NO"};
  arrow::MemoryPool* memory_pool = nullptr;

  // Security limits to prevent resource exhaustion from malformed/malicious CSV files.
  // A value of 0 means no limit (unlimited).
  size_t max_columns =
      10000; // Maximum number of columns allowed (e.g., 5000 rejects CSVs with > 5000 columns)
  size_t max_rows = 0;                // Maximum number of rows allowed (0 = unlimited)
  size_t max_total_cells = 100000000; // Maximum total cells (rows * columns) allowed (100M default)
  static constexpr size_t MAX_TYPE_INFERENCE_ROWS = 100000; // Upper bound for type_inference_rows
};

struct ArrowConvertResult {
  std::shared_ptr<arrow::Table> table;
  std::string error_message;
  int64_t num_rows = 0;
  int64_t num_columns = 0;
  std::shared_ptr<arrow::Schema> schema;
  bool ok() const { return table != nullptr; }
};

class ArrowConverter {
public:
  ArrowConverter();
  explicit ArrowConverter(const ArrowConvertOptions& options);
  ArrowConverter(const std::vector<ColumnSpec>& columns,
                 const ArrowConvertOptions& options = ArrowConvertOptions());

  ArrowConvertResult convert(const uint8_t* buf, size_t len, const ParseIndex& idx,
                             const Dialect& dialect = Dialect::csv());

  std::vector<ColumnType> infer_types(const uint8_t* buf, size_t len, const ParseIndex& idx,
                                      const Dialect& dialect = Dialect::csv());

  std::shared_ptr<arrow::Schema> build_schema(const std::vector<std::string>& column_names,
                                              const std::vector<ColumnType>& column_types);

private:
  ArrowConvertOptions options_;
  std::vector<ColumnSpec> columns_;
  bool has_user_schema_ = false;

  struct FieldRange {
    size_t start;
    size_t end;
  };

  /**
   * @brief Result of field extraction containing both column data and headers.
   *
   * This struct enables single-pass extraction of all field information needed
   * for Arrow conversion, avoiding redundant sorting and traversal operations.
   */
  struct FieldExtractionResult {
    std::vector<std::vector<FieldRange>> columns;
    std::vector<std::string> header_names;
  };

  FieldExtractionResult extract_field_ranges_with_headers(const uint8_t* buf, size_t len,
                                                          const ParseIndex& idx,
                                                          const Dialect& dialect);

  std::vector<ColumnType>
  infer_types_from_ranges(const uint8_t* buf,
                          const std::vector<std::vector<FieldRange>>& field_ranges,
                          const Dialect& dialect);

  /**
   * @brief Extract a field from the buffer as a string_view.
   * @param buf Pointer to the CSV buffer
   * @param start Starting byte offset of the field (inclusive)
   * @param end Ending byte offset of the field (exclusive)
   * @param dialect CSV dialect settings
   * @return A string_view of the field contents, with quotes stripped if present.
   *         Returns empty string_view with valid data pointer (buf+start) if start >= end.
   * @pre end >= start (asserted in debug builds to catch corrupted index data)
   * @note The returned string_view always has a valid (non-null) data pointer,
   *       even when empty. This avoids undefined behavior when converting to std::string.
   */
  std::string_view extract_field(const uint8_t* buf, size_t start, size_t end,
                                 const Dialect& dialect);
  ColumnType infer_cell_type(std::string_view cell);
  bool is_null_value(std::string_view value);
  std::optional<bool> parse_boolean(std::string_view value);
  std::optional<int64_t> parse_int64(std::string_view value);
  std::optional<double> parse_double(std::string_view value);

  arrow::Result<std::shared_ptr<arrow::Array>> build_column(const uint8_t* buf,
                                                            const std::vector<FieldRange>& ranges,
                                                            ColumnType type,
                                                            const Dialect& dialect);
  arrow::Result<std::shared_ptr<arrow::Array>>
  build_string_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                      const Dialect& dialect);
  arrow::Result<std::shared_ptr<arrow::Array>>
  build_int64_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                     const Dialect& dialect);
  arrow::Result<std::shared_ptr<arrow::Array>>
  build_double_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                      const Dialect& dialect);
  arrow::Result<std::shared_ptr<arrow::Array>>
  build_boolean_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                       const Dialect& dialect);
};

ArrowConvertResult csv_to_arrow(const std::string& filename,
                                const ArrowConvertOptions& options = ArrowConvertOptions(),
                                const Dialect& dialect = Dialect::csv());

ArrowConvertResult
csv_to_arrow_from_memory(const uint8_t* data, size_t len,
                         const ArrowConvertOptions& options = ArrowConvertOptions(),
                         const Dialect& dialect = Dialect::csv());

// =============================================================================
// Columnar Format Export (Parquet/Feather)
// =============================================================================

/// Output format for columnar file export
enum class ColumnarFormat {
  PARQUET, ///< Apache Parquet format (compressed columnar)
  FEATHER, ///< Arrow IPC/Feather format (fast serialization)
  AUTO     ///< Auto-detect from file extension (.parquet, .feather, .arrow)
};

/// Options for Parquet export
struct ParquetWriteOptions {
  /// Compression codec for Parquet files
  /// Default is SNAPPY for good balance of speed and compression
  enum class Compression {
    UNCOMPRESSED,
    SNAPPY, ///< Default - fast with moderate compression
    GZIP,   ///< Better compression, slower
    ZSTD,   ///< Best compression/speed tradeoff
    LZ4     ///< Fastest compression
  };
  Compression compression = Compression::SNAPPY;

  /// Row group size (number of rows per row group)
  /// Smaller values use less memory during write, larger values may compress better
  int64_t row_group_size = 1024 * 1024; // 1M rows default
};

/// Result of a columnar file write operation
struct WriteResult {
  bool success = false;
  std::string error_message;
  int64_t bytes_written = 0;

  bool ok() const { return success; }
};

/**
 * @brief Write an Arrow table to a Parquet file.
 *
 * @param table The Arrow table to write
 * @param output_path Path to the output Parquet file
 * @param options Write options (compression, row group size)
 * @return WriteResult indicating success or failure
 */
WriteResult write_parquet(const std::shared_ptr<arrow::Table>& table,
                          const std::string& output_path,
                          const ParquetWriteOptions& options = ParquetWriteOptions());

/**
 * @brief Write an Arrow table to a Feather (Arrow IPC) file.
 *
 * Feather is a fast binary columnar format optimized for reading/writing
 * rather than storage efficiency. It's ideal for temporary files or
 * inter-process communication.
 *
 * @param table The Arrow table to write
 * @param output_path Path to the output Feather file
 * @return WriteResult indicating success or failure
 */
WriteResult write_feather(const std::shared_ptr<arrow::Table>& table,
                          const std::string& output_path);

/**
 * @brief Detect output format from file extension.
 *
 * @param path File path to examine
 * @return Detected format, or AUTO if extension not recognized
 */
ColumnarFormat detect_format_from_extension(const std::string& path);

/**
 * @brief Write an Arrow table to a columnar file with format auto-detection.
 *
 * @param table The Arrow table to write
 * @param output_path Path to the output file (extension determines format)
 * @param format Explicit format, or AUTO to detect from extension
 * @param parquet_options Options for Parquet output (ignored for Feather)
 * @return WriteResult indicating success or failure
 */
WriteResult write_columnar(const std::shared_ptr<arrow::Table>& table,
                           const std::string& output_path,
                           ColumnarFormat format = ColumnarFormat::AUTO,
                           const ParquetWriteOptions& parquet_options = ParquetWriteOptions());

/**
 * @brief Convert a CSV file directly to Parquet format.
 *
 * Convenience function that combines csv_to_arrow() and write_parquet().
 *
 * @param csv_path Path to input CSV file
 * @param parquet_path Path to output Parquet file
 * @param arrow_options Options for CSV to Arrow conversion
 * @param parquet_options Options for Parquet output
 * @param dialect CSV dialect (default: auto-detect)
 * @return WriteResult indicating success or failure
 */
WriteResult csv_to_parquet(const std::string& csv_path, const std::string& parquet_path,
                           const ArrowConvertOptions& arrow_options = ArrowConvertOptions(),
                           const ParquetWriteOptions& parquet_options = ParquetWriteOptions(),
                           const Dialect& dialect = Dialect::csv());

/**
 * @brief Convert a CSV file directly to Feather format.
 *
 * Convenience function that combines csv_to_arrow() and write_feather().
 *
 * @param csv_path Path to input CSV file
 * @param feather_path Path to output Feather file
 * @param arrow_options Options for CSV to Arrow conversion
 * @param dialect CSV dialect (default: auto-detect)
 * @return WriteResult indicating success or failure
 */
WriteResult csv_to_feather(const std::string& csv_path, const std::string& feather_path,
                           const ArrowConvertOptions& arrow_options = ArrowConvertOptions(),
                           const Dialect& dialect = Dialect::csv());

} // namespace libvroom

#endif // LIBVROOM_ENABLE_ARROW
#endif // LIBVROOM_ARROW_OUTPUT_H
