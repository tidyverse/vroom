#pragma once

#include "options.h"
#include "types.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace libvroom {

// Forward declarations
class Table;

// Progress callback: (bytes_processed, total_bytes) -> should_continue
using ProgressCallback = std::function<bool(size_t, size_t)>;

// Conversion result with stats (avoids re-reading file for summary)
struct ConversionResult {
  std::string error; // Empty on success (simple error message)
  size_t rows = 0;
  size_t cols = 0;

  // Rich error information (only populated when error_mode != DISABLED)
  std::vector<ParseError> parse_errors;

  // Check if conversion succeeded (no fatal errors)
  bool ok() const { return error.empty(); }

  // Check if any errors were collected
  bool has_errors() const { return !parse_errors.empty(); }

  // Check if any warnings were collected
  bool has_warnings() const {
    for (const auto& e : parse_errors) {
      if (e.severity == ErrorSeverity::WARNING)
        return true;
    }
    return false;
  }

  // Check if any fatal errors were collected
  bool has_fatal() const {
    for (const auto& e : parse_errors) {
      if (e.severity == ErrorSeverity::FATAL)
        return true;
    }
    return false;
  }

  // Get error count
  size_t error_count() const { return parse_errors.size(); }

  // Get summary string (e.g., "3 errors, 2 warnings")
  std::string error_summary() const {
    if (parse_errors.empty())
      return "No errors";
    size_t warnings = 0, errors = 0, fatal = 0;
    for (const auto& e : parse_errors) {
      switch (e.severity) {
      case ErrorSeverity::WARNING:
        warnings++;
        break;
      case ErrorSeverity::RECOVERABLE:
        errors++;
        break;
      case ErrorSeverity::FATAL:
        fatal++;
        break;
      }
    }
    std::string result;
    if (fatal > 0)
      result += std::to_string(fatal) + " fatal";
    if (errors > 0) {
      if (!result.empty())
        result += ", ";
      result += std::to_string(errors) + " errors";
    }
    if (warnings > 0) {
      if (!result.empty())
        result += ", ";
      result += std::to_string(warnings) + " warnings";
    }
    return result;
  }
};

// Main conversion function
// Returns ConversionResult with error message on failure, or stats on success
ConversionResult convert_csv_to_parquet(const VroomOptions& options,
                                        ProgressCallback progress = nullptr);

/// Read a CSV file and return a Table (Arrow-exportable).
/// Convenience wrapper that throws std::runtime_error on failure.
std::shared_ptr<Table> read_csv_to_table(const std::string& path,
                                         const CsvOptions& opts = CsvOptions{});

} // namespace libvroom
