#ifdef LIBVROOM_ENABLE_ARROW

#include "arrow_output.h"

#include "io_util.h"
#include "mem_util.h"

#include <algorithm>
#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/table.h>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstring>
#include <memory>
#include <stdexcept>

#ifdef LIBVROOM_ENABLE_PARQUET
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#endif

namespace libvroom {

// RAII wrapper for aligned memory to ensure proper cleanup on all code paths
// This prevents memory leaks when exceptions are thrown during parsing or
// conversion
struct AlignedDeleter {
  void operator()(uint8_t* ptr) const noexcept {
    if (ptr)
      aligned_free(ptr);
  }
};
using AlignedBufferPtr = std::unique_ptr<uint8_t, AlignedDeleter>;

std::shared_ptr<arrow::DataType> column_type_to_arrow(ColumnType type) {
  switch (type) {
  case ColumnType::STRING:
    return arrow::utf8();
  case ColumnType::INT64:
    return arrow::int64();
  case ColumnType::DOUBLE:
    return arrow::float64();
  case ColumnType::BOOLEAN:
    return arrow::boolean();
  case ColumnType::DATE:
    return arrow::date32();
  case ColumnType::TIMESTAMP:
    return arrow::timestamp(arrow::TimeUnit::MICRO);
  case ColumnType::NULL_TYPE:
    return arrow::null();
  default:
    return arrow::utf8();
  }
}

const char* column_type_to_string(ColumnType type) {
  switch (type) {
  case ColumnType::STRING:
    return "STRING";
  case ColumnType::INT64:
    return "INT64";
  case ColumnType::DOUBLE:
    return "DOUBLE";
  case ColumnType::BOOLEAN:
    return "BOOLEAN";
  case ColumnType::DATE:
    return "DATE";
  case ColumnType::TIMESTAMP:
    return "TIMESTAMP";
  case ColumnType::NULL_TYPE:
    return "NULL";
  case ColumnType::AUTO:
    return "AUTO";
  default:
    return "UNKNOWN";
  }
}

namespace {
// Case-insensitive string comparison for ASCII
bool iequals(std::string_view a, std::string_view b) {
  if (a.size() != b.size())
    return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i])))
      return false;
  }
  return true;
}
} // anonymous namespace

ArrowConverter::ArrowConverter() : options_(), has_user_schema_(false) {}
ArrowConverter::ArrowConverter(const ArrowConvertOptions& options)
    : options_(options), has_user_schema_(false) {
  // Validate type_inference_rows does not exceed the maximum allowed value
  if (options_.type_inference_rows > ArrowConvertOptions::MAX_TYPE_INFERENCE_ROWS) {
    throw std::invalid_argument("type_inference_rows (" +
                                std::to_string(options_.type_inference_rows) +
                                ") exceeds maximum allowed (" +
                                std::to_string(ArrowConvertOptions::MAX_TYPE_INFERENCE_ROWS) + ")");
  }
}
ArrowConverter::ArrowConverter(const std::vector<ColumnSpec>& columns,
                               const ArrowConvertOptions& options)
    : options_(options), columns_(columns), has_user_schema_(true) {
  // Validate type_inference_rows does not exceed the maximum allowed value
  if (options_.type_inference_rows > ArrowConvertOptions::MAX_TYPE_INFERENCE_ROWS) {
    throw std::invalid_argument("type_inference_rows (" +
                                std::to_string(options_.type_inference_rows) +
                                ") exceeds maximum allowed (" +
                                std::to_string(ArrowConvertOptions::MAX_TYPE_INFERENCE_ROWS) + ")");
  }
}

bool ArrowConverter::is_null_value(std::string_view value) {
  for (const auto& null_str : options_.null_values) {
    if (value == null_str)
      return true;
  }
  return false;
}

std::optional<bool> ArrowConverter::parse_boolean(std::string_view value) {
  // Use case-insensitive comparison without allocating temporary strings
  for (const auto& v : options_.true_values) {
    if (iequals(value, v))
      return true;
  }
  for (const auto& v : options_.false_values) {
    if (iequals(value, v))
      return false;
  }
  return std::nullopt;
}

std::optional<int64_t> ArrowConverter::parse_int64(std::string_view value) {
  if (value.empty())
    return std::nullopt;
  size_t start = 0, end = value.size();
  while (start < end && std::isspace(static_cast<unsigned char>(value[start])))
    ++start;
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])))
    --end;
  if (start >= end)
    return std::nullopt;
  int64_t result;
  auto [ptr, ec] = std::from_chars(value.data() + start, value.data() + end, result);
  if (ec == std::errc() && ptr == value.data() + end)
    return result;
  return std::nullopt;
}

std::optional<double> ArrowConverter::parse_double(std::string_view value) {
  if (value.empty())
    return std::nullopt;
  size_t start = 0, end = value.size();
  while (start < end && std::isspace(static_cast<unsigned char>(value[start])))
    ++start;
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])))
    --end;
  if (start >= end)
    return std::nullopt;
  std::string_view trimmed = value.substr(start, end - start);
  if (trimmed == "inf" || trimmed == "Inf")
    return std::numeric_limits<double>::infinity();
  if (trimmed == "-inf" || trimmed == "-Inf")
    return -std::numeric_limits<double>::infinity();
  if (trimmed == "nan" || trimmed == "NaN")
    return std::numeric_limits<double>::quiet_NaN();
  char* endptr;
  std::string temp(trimmed);
  errno = 0; // Reset errno before call
  double result = std::strtod(temp.c_str(), &endptr);
  // Check for parsing success and overflow/underflow (ERANGE)
  if (endptr == temp.c_str() + temp.size() && errno != ERANGE)
    return result;
  return std::nullopt;
}

ColumnType ArrowConverter::infer_cell_type(std::string_view cell) {
  if (cell.empty() || is_null_value(cell))
    return ColumnType::NULL_TYPE;
  if (parse_boolean(cell).has_value())
    return ColumnType::BOOLEAN;
  if (parse_int64(cell).has_value())
    return ColumnType::INT64;
  if (parse_double(cell).has_value())
    return ColumnType::DOUBLE;
  return ColumnType::STRING;
}

std::string_view ArrowConverter::extract_field(const uint8_t* buf, size_t start, size_t end,
                                               const Dialect& dialect) {
  // Validate bounds to catch corrupted index data early in debug builds.
  // If end < start with unsigned types, subtraction would underflow causing
  // buffer over-reads and undefined behavior.
  assert(end >= start && "Invalid field range: end must be >= start");
  // Return an empty string_view with a valid data pointer (not nullptr).
  // Using std::string_view() returns a view with data()==nullptr, which causes
  // undefined behavior when constructing std::string from it on some platforms
  // (notably macOS). By using buf+start, we ensure data() is always valid.
  if (start >= end)
    return std::string_view(reinterpret_cast<const char*>(buf + start), 0);
  const char* field_start = reinterpret_cast<const char*>(buf + start);
  size_t len = end - start;
  if (len >= 2 && field_start[0] == dialect.quote_char &&
      field_start[len - 1] == dialect.quote_char) {
    field_start++;
    len -= 2;
  }
  return std::string_view(field_start, len);
}

ArrowConverter::FieldExtractionResult
ArrowConverter::extract_field_ranges_with_headers(const uint8_t* buf, size_t len,
                                                  const ParseIndex& idx, const Dialect& dialect) {
  FieldExtractionResult result;
  if (idx.n_threads == 0)
    return result;
  size_t total_seps = 0;
  for (uint16_t t = 0; t < idx.n_threads; ++t)
    total_seps += idx.n_indexes[t];
  if (total_seps == 0)
    return result;
  std::vector<uint64_t> all_positions;
  all_positions.reserve(total_seps);
  for (uint16_t t = 0; t < idx.n_threads; ++t)
    for (size_t i = 0; i < idx.n_indexes[t]; ++i) {
      uint64_t pos = idx.indexes[t + i * idx.n_threads];
      if (pos < len)
        all_positions.push_back(pos); // Bounds check to prevent buffer overrun
    }
  std::sort(all_positions.begin(), all_positions.end());
  if (all_positions.empty())
    return result; // No valid positions after bounds filtering
  size_t num_columns = 0;
  for (size_t i = 0; i < all_positions.size(); ++i) {
    num_columns++;
    if (buf[all_positions[i]] == '\n')
      break;
  }
  if (num_columns == 0)
    return result;
  result.columns.resize(num_columns);
  result.header_names.reserve(num_columns);

  size_t field_start = 0, current_col = 0;
  bool in_header = true;
  for (size_t i = 0; i < all_positions.size(); ++i) {
    size_t field_end = all_positions[i];
    char sep_char = static_cast<char>(buf[field_end]);
    if (in_header) {
      // Extract header name during first row
      result.header_names.push_back(
          std::string(extract_field(buf, field_start, field_end, dialect)));
    } else if (current_col < num_columns) {
      result.columns[current_col].push_back({field_start, field_end});
    }
    if (sep_char == '\n') {
      if (in_header)
        in_header = false;
      current_col = 0;
    } else {
      current_col++;
    }
    field_start = field_end + 1;
  }

  // Auto-generate names for any missing columns
  while (result.header_names.size() < num_columns)
    result.header_names.push_back("column_" + std::to_string(result.header_names.size()));

  return result;
}

std::vector<ColumnType>
ArrowConverter::infer_types_from_ranges(const uint8_t* buf,
                                        const std::vector<std::vector<FieldRange>>& field_ranges,
                                        const Dialect& dialect) {
  std::vector<ColumnType> types(field_ranges.size(), ColumnType::NULL_TYPE);
  for (size_t col = 0; col < field_ranges.size(); ++col) {
    const auto& ranges = field_ranges[col];
    size_t samples = options_.type_inference_rows > 0
                         ? std::min(options_.type_inference_rows, ranges.size())
                         : ranges.size();
    ColumnType strongest = ColumnType::NULL_TYPE;
    for (size_t row = 0; row < samples; ++row) {
      ColumnType ct =
          infer_cell_type(extract_field(buf, ranges[row].start, ranges[row].end, dialect));
      if (ct == ColumnType::NULL_TYPE)
        continue;
      if (strongest == ColumnType::NULL_TYPE)
        strongest = ct;
      else if (strongest != ct) {
        // Type promotion rules:
        // - INT64 + DOUBLE -> DOUBLE (standard numeric promotion)
        // - BOOLEAN + INT64 -> INT64 (boolean values 0/1 are valid ints)
        // - BOOLEAN + DOUBLE -> DOUBLE (boolean values 0/1 are valid doubles)
        // - Any other mismatch -> STRING
        if ((strongest == ColumnType::INT64 && ct == ColumnType::DOUBLE) ||
            (strongest == ColumnType::DOUBLE && ct == ColumnType::INT64)) {
          strongest = ColumnType::DOUBLE;
        } else if ((strongest == ColumnType::BOOLEAN && ct == ColumnType::INT64) ||
                   (strongest == ColumnType::INT64 && ct == ColumnType::BOOLEAN)) {
          strongest = ColumnType::INT64;
        } else if ((strongest == ColumnType::BOOLEAN && ct == ColumnType::DOUBLE) ||
                   (strongest == ColumnType::DOUBLE && ct == ColumnType::BOOLEAN)) {
          strongest = ColumnType::DOUBLE;
        } else {
          strongest = ColumnType::STRING;
          break;
        }
      }
    }
    types[col] = (strongest == ColumnType::NULL_TYPE) ? ColumnType::STRING : strongest;
  }
  return types;
}

std::vector<ColumnType> ArrowConverter::infer_types(const uint8_t* buf, size_t len,
                                                    const ParseIndex& idx, const Dialect& dialect) {
  auto extraction = extract_field_ranges_with_headers(buf, len, idx, dialect);
  return infer_types_from_ranges(buf, extraction.columns, dialect);
}

std::shared_ptr<arrow::Schema> ArrowConverter::build_schema(const std::vector<std::string>& names,
                                                            const std::vector<ColumnType>& types) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (size_t i = 0; i < names.size(); ++i) {
    auto arrow_type = (has_user_schema_ && i < columns_.size() && columns_[i].arrow_type)
                          ? columns_[i].arrow_type
                          : (i < types.size() ? column_type_to_arrow(types[i]) : arrow::utf8());
    bool nullable = has_user_schema_ && i < columns_.size() ? columns_[i].nullable : true;
    fields.push_back(arrow::field(names[i], arrow_type, nullable));
  }
  return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::Array>>
ArrowConverter::build_string_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                                    const Dialect& dialect) {
  arrow::StringBuilder builder(options_.memory_pool);
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
  for (const auto& range : ranges) {
    auto cell = extract_field(buf, range.start, range.end, dialect);
    if (is_null_value(cell))
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    else
      ARROW_RETURN_NOT_OK(builder.Append(std::string(cell)));
  }
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
ArrowConverter::build_int64_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                                   const Dialect& dialect) {
  arrow::Int64Builder builder(options_.memory_pool);
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
  for (const auto& range : ranges) {
    auto cell = extract_field(buf, range.start, range.end, dialect);
    if (is_null_value(cell))
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    else if (auto v = parse_int64(cell))
      ARROW_RETURN_NOT_OK(builder.Append(*v));
    else
      ARROW_RETURN_NOT_OK(builder.AppendNull());
  }
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
ArrowConverter::build_double_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                                    const Dialect& dialect) {
  arrow::DoubleBuilder builder(options_.memory_pool);
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
  for (const auto& range : ranges) {
    auto cell = extract_field(buf, range.start, range.end, dialect);
    if (is_null_value(cell))
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    else if (auto v = parse_double(cell))
      ARROW_RETURN_NOT_OK(builder.Append(*v));
    else
      ARROW_RETURN_NOT_OK(builder.AppendNull());
  }
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
ArrowConverter::build_boolean_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                                     const Dialect& dialect) {
  arrow::BooleanBuilder builder(options_.memory_pool);
  ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
  for (const auto& range : ranges) {
    auto cell = extract_field(buf, range.start, range.end, dialect);
    if (is_null_value(cell))
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    else if (auto v = parse_boolean(cell))
      ARROW_RETURN_NOT_OK(builder.Append(*v));
    else
      ARROW_RETURN_NOT_OK(builder.AppendNull());
  }
  return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>>
ArrowConverter::build_column(const uint8_t* buf, const std::vector<FieldRange>& ranges,
                             ColumnType type, const Dialect& dialect) {
  switch (type) {
  case ColumnType::INT64:
    return build_int64_column(buf, ranges, dialect);
  case ColumnType::DOUBLE:
    return build_double_column(buf, ranges, dialect);
  case ColumnType::BOOLEAN:
    return build_boolean_column(buf, ranges, dialect);
  default:
    return build_string_column(buf, ranges, dialect);
  }
}

ArrowConvertResult ArrowConverter::convert(const uint8_t* buf, size_t len, const ParseIndex& idx,
                                           const Dialect& dialect) {
  ArrowConvertResult result;

  // Single extraction of field ranges and header names to avoid redundant parsing
  auto extraction = extract_field_ranges_with_headers(buf, len, idx, dialect);
  if (extraction.columns.empty()) {
    result.error_message = "No data";
    return result;
  }
  size_t num_columns = extraction.columns.size();
  size_t num_rows = extraction.columns[0].size();

  // Validate column count against security limit
  if (options_.max_columns > 0 && num_columns > options_.max_columns) {
    result.error_message = "Column count " + std::to_string(num_columns) +
                           " exceeds maximum allowed " + std::to_string(options_.max_columns);
    return result;
  }

  // Validate row count against security limit
  if (options_.max_rows > 0 && num_rows > options_.max_rows) {
    result.error_message = "Row count " + std::to_string(num_rows) + " exceeds maximum allowed " +
                           std::to_string(options_.max_rows);
    return result;
  }

  // Validate total cell count against security limit (with overflow protection)
  // CSVs with individually acceptable dimensions can still exhaust memory
  // through their multiplicative effect (e.g., 9999 columns × 1M rows = ~10B
  // cells)
  if (options_.max_total_cells > 0 && num_columns > 0) {
    // Use division-based check to prevent integer overflow in multiplication.
    // If num_rows > max_total_cells / num_columns, then num_rows * num_columns
    // > max_total_cells. This is guaranteed by integer division properties and
    // avoids computing the potentially overflowing product. The num_columns > 0
    // check prevents division by zero.
    if (num_rows > options_.max_total_cells / num_columns) {
      result.error_message = "Total cell count (" + std::to_string(num_columns) + " columns × " +
                             std::to_string(num_rows) + " rows) exceeds maximum allowed " +
                             std::to_string(options_.max_total_cells);
      return result;
    }
  }

  // Get column types using pre-extracted field ranges (avoids redundant extraction)
  auto column_types = options_.infer_types
                          ? infer_types_from_ranges(buf, extraction.columns, dialect)
                          : std::vector<ColumnType>(num_columns, ColumnType::STRING);
  result.schema = build_schema(extraction.header_names, column_types);

  // Build columns
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (size_t col = 0; col < num_columns; ++col) {
    auto arr = build_column(buf, extraction.columns[col], column_types[col], dialect);
    if (!arr.ok()) {
      result.error_message = arr.status().ToString();
      return result;
    }
    arrays.push_back(*arr);
  }
  result.table = arrow::Table::Make(result.schema, arrays);
  result.num_rows = static_cast<int64_t>(num_rows);
  result.num_columns = static_cast<int64_t>(num_columns);
  return result;
}

ArrowConvertResult csv_to_arrow(const std::string& filename, const ArrowConvertOptions& options,
                                const Dialect& dialect) {
  ArrowConvertResult result;
  try {
    auto [buffer, size] = read_file(filename, 64);

    two_pass parser;
    index idx = parser.init(size, 1);
    parser.parse(buffer.get(), idx, size, dialect);
    ArrowConverter converter(options);
    result = converter.convert(buffer.get(), size, idx, dialect);
    // buffer automatically frees memory when it goes out of scope
  } catch (const std::exception& e) {
    result.error_message = e.what();
  }
  return result;
}

ArrowConvertResult csv_to_arrow_from_memory(const uint8_t* data, size_t len,
                                            const ArrowConvertOptions& options,
                                            const Dialect& dialect) {
  ArrowConvertResult result;
  uint8_t* buf = allocate_padded_buffer(len, 64);
  if (!buf) {
    result.error_message = "Allocation failed";
    return result;
  }
  // Use RAII to ensure memory is freed even if an exception is thrown
  AlignedBufferPtr buffer_guard(buf);

  try {
    std::memcpy(buf, data, len);
    two_pass parser;
    index idx = parser.init(len, 1);
    parser.parse(buf, idx, len, dialect);
    ArrowConverter converter(options);
    result = converter.convert(buf, len, idx, dialect);
    // buffer_guard automatically frees memory when it goes out of scope
  } catch (const std::exception& e) {
    result.error_message = e.what();
  }
  return result;
}

// =============================================================================
// Columnar Format Export Implementation
// =============================================================================

ColumnarFormat detect_format_from_extension(const std::string& path) {
  // Find the last dot to extract extension
  size_t dot_pos = path.rfind('.');
  if (dot_pos == std::string::npos || dot_pos == path.length() - 1) {
    return ColumnarFormat::AUTO; // No extension found
  }

  std::string ext = path.substr(dot_pos + 1);
  // Convert to lowercase for comparison
  std::transform(ext.begin(), ext.end(), ext.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (ext == "parquet" || ext == "pq") {
    return ColumnarFormat::PARQUET;
  } else if (ext == "feather" || ext == "arrow" || ext == "ipc") {
    return ColumnarFormat::FEATHER;
  }

  return ColumnarFormat::AUTO;
}

WriteResult write_feather(const std::shared_ptr<arrow::Table>& table,
                          const std::string& output_path) {
  WriteResult result;

  if (!table) {
    result.error_message = "Table is null";
    return result;
  }

  // Open output file
  auto file_result = arrow::io::FileOutputStream::Open(output_path);
  if (!file_result.ok()) {
    result.error_message = "Failed to open output file: " + file_result.status().ToString();
    return result;
  }
  auto output_file = *file_result;

  // Write table as Arrow IPC stream (Feather v2 format)
  auto writer_result = arrow::ipc::MakeFileWriter(output_file, table->schema());
  if (!writer_result.ok()) {
    result.error_message = "Failed to create IPC writer: " + writer_result.status().ToString();
    return result;
  }
  auto writer = *writer_result;

  // Write table batches
  auto batches_result = table->CombineChunksToBatch();
  if (!batches_result.ok()) {
    result.error_message = "Failed to combine table chunks: " + batches_result.status().ToString();
    return result;
  }
  auto batch = *batches_result;

  auto write_status = writer->WriteRecordBatch(*batch);
  if (!write_status.ok()) {
    result.error_message = "Failed to write record batch: " + write_status.ToString();
    return result;
  }

  auto close_status = writer->Close();
  if (!close_status.ok()) {
    result.error_message = "Failed to close writer: " + close_status.ToString();
    return result;
  }

  // Get bytes written
  auto pos_result = output_file->Tell();
  if (pos_result.ok()) {
    result.bytes_written = *pos_result;
  }

  auto file_close_status = output_file->Close();
  if (!file_close_status.ok()) {
    result.error_message = "Failed to close file: " + file_close_status.ToString();
    return result;
  }

  result.success = true;
  return result;
}

#ifdef LIBVROOM_ENABLE_PARQUET

WriteResult write_parquet(const std::shared_ptr<arrow::Table>& table,
                          const std::string& output_path, const ParquetWriteOptions& options) {
  WriteResult result;

  if (!table) {
    result.error_message = "Table is null";
    return result;
  }

  // Open output file
  auto file_result = arrow::io::FileOutputStream::Open(output_path);
  if (!file_result.ok()) {
    result.error_message = "Failed to open output file: " + file_result.status().ToString();
    return result;
  }
  auto output_file = *file_result;

  // Configure Parquet writer properties
  auto builder = parquet::WriterProperties::Builder();

  // Set compression codec
  parquet::Compression::type compression;
  switch (options.compression) {
  case ParquetWriteOptions::Compression::UNCOMPRESSED:
    compression = parquet::Compression::UNCOMPRESSED;
    break;
  case ParquetWriteOptions::Compression::SNAPPY:
    compression = parquet::Compression::SNAPPY;
    break;
  case ParquetWriteOptions::Compression::GZIP:
    compression = parquet::Compression::GZIP;
    break;
  case ParquetWriteOptions::Compression::ZSTD:
    compression = parquet::Compression::ZSTD;
    break;
  case ParquetWriteOptions::Compression::LZ4:
    compression = parquet::Compression::LZ4;
    break;
  default:
    compression = parquet::Compression::SNAPPY;
    break;
  }
  builder.compression(compression);

  auto writer_properties = builder.build();

  // Configure Arrow writer properties
  auto arrow_properties = parquet::ArrowWriterProperties::Builder().store_schema()->build();

  // Write the table
  auto status =
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output_file,
                                 options.row_group_size, writer_properties, arrow_properties);

  if (!status.ok()) {
    result.error_message = "Failed to write Parquet file: " + status.ToString();
    return result;
  }

  // Get bytes written
  auto pos_result = output_file->Tell();
  if (pos_result.ok()) {
    result.bytes_written = *pos_result;
  }

  auto close_status = output_file->Close();
  if (!close_status.ok()) {
    result.error_message = "Failed to close file: " + close_status.ToString();
    return result;
  }

  result.success = true;
  return result;
}

#else // !LIBVROOM_ENABLE_PARQUET

WriteResult write_parquet(const std::shared_ptr<arrow::Table>& /*table*/,
                          const std::string& /*output_path*/,
                          const ParquetWriteOptions& /*options*/) {
  WriteResult result;
  result.error_message =
      "Parquet support not available. This build was compiled without Parquet support.";
  return result;
}

#endif // LIBVROOM_ENABLE_PARQUET

WriteResult write_columnar(const std::shared_ptr<arrow::Table>& table,
                           const std::string& output_path, ColumnarFormat format,
                           const ParquetWriteOptions& parquet_options) {
  // Auto-detect format from extension if needed
  if (format == ColumnarFormat::AUTO) {
    format = detect_format_from_extension(output_path);
    if (format == ColumnarFormat::AUTO) {
      // Default to Parquet if extension not recognized
      format = ColumnarFormat::PARQUET;
    }
  }

  switch (format) {
  case ColumnarFormat::PARQUET:
    return write_parquet(table, output_path, parquet_options);
  case ColumnarFormat::FEATHER:
    return write_feather(table, output_path);
  default:
    WriteResult result;
    result.error_message = "Unknown output format";
    return result;
  }
}

WriteResult csv_to_parquet(const std::string& csv_path, const std::string& parquet_path,
                           const ArrowConvertOptions& arrow_options,
                           const ParquetWriteOptions& parquet_options, const Dialect& dialect) {
  // First convert CSV to Arrow table
  auto arrow_result = csv_to_arrow(csv_path, arrow_options, dialect);
  if (!arrow_result.ok()) {
    WriteResult result;
    result.error_message = "CSV to Arrow conversion failed: " + arrow_result.error_message;
    return result;
  }

  // Then write to Parquet
  return write_parquet(arrow_result.table, parquet_path, parquet_options);
}

WriteResult csv_to_feather(const std::string& csv_path, const std::string& feather_path,
                           const ArrowConvertOptions& arrow_options, const Dialect& dialect) {
  // First convert CSV to Arrow table
  auto arrow_result = csv_to_arrow(csv_path, arrow_options, dialect);
  if (!arrow_result.ok()) {
    WriteResult result;
    result.error_message = "CSV to Arrow conversion failed: " + arrow_result.error_message;
    return result;
  }

  // Then write to Feather
  return write_feather(arrow_result.table, feather_path);
}

} // namespace libvroom

#endif // LIBVROOM_ENABLE_ARROW
