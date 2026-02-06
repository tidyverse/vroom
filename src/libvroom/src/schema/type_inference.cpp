#include "libvroom/vroom.h"

#include <cctype>
#include <cstring>
#include <fast_float/fast_float.h>

namespace libvroom {

TypeInference::TypeInference(const CsvOptions& options) : options_(options) {}

DataType TypeInference::infer_field(std::string_view value) {
  // Empty or null values don't help inference
  if (value.empty()) {
    return DataType::NA;
  }

  // Check for configured null values
  std::string_view null_values = options_.null_values;
  size_t start = 0;
  while (start < null_values.size()) {
    size_t end = null_values.find(',', start);
    if (end == std::string_view::npos)
      end = null_values.size();
    if (value == null_values.substr(start, end - start)) {
      return DataType::NA;
    }
    start = end + 1;
  }

  // Check for boolean values
  std::string_view true_vals = options_.true_values;
  start = 0;
  while (start < true_vals.size()) {
    size_t end = true_vals.find(',', start);
    if (end == std::string_view::npos)
      end = true_vals.size();
    if (value == true_vals.substr(start, end - start)) {
      return DataType::BOOL;
    }
    start = end + 1;
  }

  std::string_view false_vals = options_.false_values;
  start = 0;
  while (start < false_vals.size()) {
    size_t end = false_vals.find(',', start);
    if (end == std::string_view::npos)
      end = false_vals.size();
    if (value == false_vals.substr(start, end - start)) {
      return DataType::BOOL;
    }
    start = end + 1;
  }

  // Try to parse as integer
  bool is_negative = false;
  size_t i = 0;
  if (!value.empty() && (value[0] == '-' || value[0] == '+')) {
    is_negative = (value[0] == '-');
    i = 1;
  }

  bool all_digits = true;
  bool has_digit = false;
  for (; i < value.size(); ++i) {
    if (value[i] >= '0' && value[i] <= '9') {
      has_digit = true;
    } else {
      all_digits = false;
      break;
    }
  }

  if (all_digits && has_digit) {
    // When guess_integer is false (default, R parity), treat integer-like values as double
    if (!options_.guess_integer) {
      return DataType::FLOAT64;
    }

    // Check if it fits in int32
    if (value.size() <= 10) { // Max int32 is 10 digits
      // Try to parse as int32
      int64_t val = 0;
      bool overflow = false;
      size_t start_idx = (value[0] == '-' || value[0] == '+') ? 1 : 0;
      for (size_t j = start_idx; j < value.size(); ++j) {
        val = val * 10 + (value[j] - '0');
        if (val > 2147483647LL) {
          overflow = true;
          break;
        }
      }
      if (!overflow && (!is_negative || val <= 2147483648LL)) {
        return DataType::INT32;
      }
    }
    // Fits in int64
    return DataType::INT64;
  }

  // Try to parse as float
  double result;
  fast_float::parse_options float_opts{fast_float::chars_format::general, options_.decimal_mark};
  auto [ptr, ec] = fast_float::from_chars_advanced(value.data(), value.data() + value.size(), result, float_opts);
  if (ec == std::errc() && ptr == value.data() + value.size()) {
    return DataType::FLOAT64;
  }

  // fast_float doesn't handle leading '+' — strip it and retry
  if (!value.empty() && value[0] == '+') {
    auto rest = std::string_view(value.data() + 1, value.size() - 1);
    auto [ptr2, ec2] = fast_float::from_chars(rest.data(), rest.data() + rest.size(), result);
    if (ec2 == std::errc() && ptr2 == rest.data() + rest.size()) {
      return DataType::FLOAT64;
    }
  }

  // Check for ISO8601 date format (YYYY-MM-DD or YYYY/MM/DD)
  if (value.size() == 10 && (value[4] == '-' || value[4] == '/') &&
      value[7] == value[4] && // Separators must match
      std::isdigit(static_cast<unsigned char>(value[0])) &&
      std::isdigit(static_cast<unsigned char>(value[1])) &&
      std::isdigit(static_cast<unsigned char>(value[2])) &&
      std::isdigit(static_cast<unsigned char>(value[3])) &&
      std::isdigit(static_cast<unsigned char>(value[5])) &&
      std::isdigit(static_cast<unsigned char>(value[6])) &&
      std::isdigit(static_cast<unsigned char>(value[8])) &&
      std::isdigit(static_cast<unsigned char>(value[9]))) {
    return DataType::DATE;
  }

  // Check for ISO8601 timestamp format
  // Formats: YYYY-MM-DDTHH:MM:SS, YYYY-MM-DD HH:MM:SS
  // Optional: fractional seconds (.ffffff), timezone (Z, +HH:MM, -HH:MM)
  if (value.size() >= 19 && (value[4] == '-' || value[4] == '/') &&
      value[7] == value[4] && // Separators must match
      (value[10] == 'T' || value[10] == ' ') && value[13] == ':' && value[16] == ':') {
    // Validate hour, minute, second digits
    bool valid_time = true;
    for (int i : {11, 12, 14, 15, 17, 18}) {
      if (!std::isdigit(static_cast<unsigned char>(value[i]))) {
        valid_time = false;
        break;
      }
    }
    if (valid_time) {
      return DataType::TIMESTAMP;
    }
  }

  // Default to string
  return DataType::STRING;
}

std::vector<DataType> TypeInference::infer_from_sample(const char* data, size_t size,
                                                       size_t n_columns, size_t max_rows) {
  std::vector<DataType> types(n_columns, DataType::UNKNOWN);

  if (size == 0 || n_columns == 0) {
    return types;
  }

  LineParser parser(options_);
  ChunkFinder finder(options_.separator, options_.quote, options_.escape_backslash);

  size_t offset = 0;
  size_t rows_sampled = 0;

  // Note: callers already pass data + header_end_offset, so no header skip needed here.

  // Sample rows
  while (offset < size && rows_sampled < max_rows) {
    size_t row_end = finder.find_row_end(data, size, offset);
    size_t row_size = row_end - offset;

    if (row_size == 0) {
      ++offset;
      continue;
    }

    // Parse this row's fields
    const auto& sep = options_.separator;
    auto matches_sep = [&](size_t pos) -> bool {
      if (pos + sep.size() > row_end) return false;
      return std::memcmp(data + pos, sep.data(), sep.size()) == 0;
    };
    std::vector<std::string> fields;
    bool in_quote = false;
    std::string current_field;

    for (size_t i = offset; i < row_end; ++i) {
      char c = data[i];

      if (c == '\n' || c == '\r') {
        // End of line
        if (options_.trim_ws) {
          while (!current_field.empty() &&
                 (current_field.back() == ' ' || current_field.back() == '\t')) {
            current_field.pop_back();
          }
        }
        fields.push_back(std::move(current_field));
        break;
      }

      if (options_.escape_backslash && c == '\\' && i + 1 < row_end) {
        current_field += data[++i];
      } else if (c == options_.quote) {
        if (in_quote && i + 1 < row_end && data[i + 1] == options_.quote) {
          current_field += options_.quote;
          ++i;
        } else {
          in_quote = !in_quote;
        }
      } else if (!in_quote && matches_sep(i)) {
        if (options_.trim_ws) {
          while (!current_field.empty() &&
                 (current_field.back() == ' ' || current_field.back() == '\t')) {
            current_field.pop_back();
          }
        }
        fields.push_back(std::move(current_field));
        current_field.clear();
        i += sep.size() - 1; // Loop will ++i
      } else {
        if (options_.trim_ws && current_field.empty() && !in_quote && (c == ' ' || c == '\t')) {
          continue;
        }
        current_field += c;
      }
    }

    // If the line ended without a newline
    if (!current_field.empty()) {
      if (options_.trim_ws) {
        while (!current_field.empty() &&
               (current_field.back() == ' ' || current_field.back() == '\t')) {
          current_field.pop_back();
        }
      }
      fields.push_back(std::move(current_field));
    }

    // Update type inference for each column
    for (size_t col = 0; col < n_columns && col < fields.size(); ++col) {
      DataType field_type = infer_field(fields[col]);
      types[col] = wider_type(types[col], field_type);
    }

    offset = row_end;
    ++rows_sampled;
  }

  // Convert UNKNOWN to STRING
  for (auto& t : types) {
    if (t == DataType::UNKNOWN) {
      t = DataType::STRING;
    }
  }

  return types;
}

} // namespace libvroom
