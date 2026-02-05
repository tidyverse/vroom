#pragma once

#include "options.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace libvroom {

// Helper function to unescape doubled quotes in a quoted field
// Converts "" to " as per CSV specification (RFC 4180)
// If has_invalid_escape is non-null, sets it to true when a lone quote is found
inline std::string unescape_quotes(std::string_view value, char quote,
                                   bool* has_invalid_escape = nullptr) {
  // Fast path: no escaped quotes
  if (value.find(quote) == std::string_view::npos) {
    return std::string(value);
  }

  std::string result;
  result.reserve(value.size());

  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == quote && i + 1 < value.size() && value[i + 1] == quote) {
      // Escaped quote (doubled) - output single quote
      result += quote;
      ++i; // Skip next quote
    } else if (value[i] == quote) {
      // Lone quote - invalid escape
      if (has_invalid_escape) {
        *has_invalid_escape = true;
      }
      result += value[i];
    } else {
      result += value[i];
    }
  }

  return result;
}

// Helper class for fast null value checking
// Pre-parses the null values string once. Uses simple linear search since
// the number of null values is typically very small (3-5 items).
class NullChecker {
public:
  explicit NullChecker(const CsvOptions& options) {
    // Parse comma-separated null values into a vector
    std::string_view null_values = options.null_values;
    size_t start = 0;

    while (start < null_values.size()) {
      size_t end = null_values.find(',', start);
      if (end == std::string_view::npos) {
        end = null_values.size();
      }

      std::string_view null_val = null_values.substr(start, end - start);
      if (!null_val.empty()) {
        null_values_.emplace_back(null_val);
        max_null_length_ = std::max(max_null_length_, null_val.size());
      } else {
        empty_is_null_ = true;
      }

      start = end + 1;
    }
  }

  bool is_null(std::string_view value) const {
    // Fast path: empty string check
    if (value.empty()) {
      return empty_is_null_;
    }

    // Fast path: length check (most null values are short: NA, null, NULL, etc.)
    if (value.size() > max_null_length_) {
      return false;
    }

    // Simple linear search - faster than hash for small N (typically 3-5 items)
    for (const auto& nv : null_values_) {
      if (nv == value) {
        return true;
      }
    }
    return false;
  }

private:
  std::vector<std::string> null_values_;
  size_t max_null_length_ = 0;
  bool empty_is_null_ = true; // Default: empty strings are null
};

} // namespace libvroom
