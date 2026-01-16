#ifndef LIBVROOM_EXTRACTION_CONFIG_H
#define LIBVROOM_EXTRACTION_CONFIG_H

#include "common_defs.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace libvroom {

/**
 * Result structure for value extraction operations.
 * Contains either a successfully parsed value or an error indicator.
 */
template <typename T> struct ExtractResult {
  std::optional<T> value;
  const char* error = nullptr;

  bool ok() const { return value.has_value(); }
  bool is_na() const { return !value.has_value() && error == nullptr; }

  T get() const {
    if (!value.has_value()) {
      throw std::runtime_error(error ? error : "Value is NA");
    }
    return *value;
  }

  T get_or(T default_value) const { return value.value_or(default_value); }
};

/**
 * Configuration for value extraction behavior.
 * Controls NA detection, boolean parsing, and whitespace handling.
 *
 * @note **Field Usage by Parser Type**:
 *
 * | Field               | Integer Parsers | Double Parsers | Boolean Parser |
 * |---------------------|-----------------|----------------|----------------|
 * | na_values           | Yes             | No*            | Yes            |
 * | true_values         | No              | No             | Yes            |
 * | false_values        | No              | No             | Yes            |
 * | trim_whitespace     | Yes             | Yes            | Yes            |
 * | allow_leading_zeros | Yes             | N/A            | No             |
 * | max_integer_digits  | Yes             | No             | No             |
 *
 * *Double parsers (parse_double_simd, SIMDDoubleParser) do NOT check na_values
 * because floating-point has valid special values (NaN, Inf) that overlap with
 * common NA strings. Use is_na() before parsing doubles if you need NA detection.
 *
 * @see parse_integer_simd() - uses na_values, trim_whitespace, max_integer_digits
 * @see parse_double_simd() - uses only trim_whitespace (see its docs for rationale)
 * @see parse_bool() - uses na_values, true_values, false_values, trim_whitespace
 * @see is_na() - uses na_values, trim_whitespace
 */
struct ExtractionConfig {
  /// Strings recognized as NA/missing values. Used by integer and boolean
  /// parsers. NOT used by double parsers (see class documentation).
  std::vector<std::string_view> na_values = {"", "NA", "N/A", "NaN", "null", "NULL", "None"};
  /// Strings recognized as boolean true. Used only by parse_bool().
  std::vector<std::string_view> true_values = {"true", "True", "TRUE", "1",
                                               "yes",  "Yes",  "YES",  "T"};
  /// Strings recognized as boolean false. Used only by parse_bool().
  std::vector<std::string_view> false_values = {"false", "False", "FALSE", "0",
                                                "no",    "No",    "NO",    "F"};
  /// Whether to trim leading/trailing whitespace. Used by all parsers.
  bool trim_whitespace = true;
  /// Whether to allow leading zeros in integers. Used by integer parsers.
  bool allow_leading_zeros = true;
  /// Maximum digits allowed in an integer. Used by integer parsers.
  size_t max_integer_digits = 20;

  static ExtractionConfig defaults() { return ExtractionConfig{}; }
};

/**
 * Data type hints for per-column type configuration.
 *
 * When set, the parser will attempt to interpret the column as this type,
 * bypassing automatic type inference. Use TypeHint::AUTO to allow
 * automatic type detection for a column.
 */
enum class TypeHint : uint8_t {
  AUTO = 0, ///< Automatic type detection (default)
  BOOLEAN,  ///< Force interpretation as boolean
  INTEGER,  ///< Force interpretation as integer
  DOUBLE,   ///< Force interpretation as double/float
  STRING,   ///< Force interpretation as string (no conversion)
  DATE,     ///< Force interpretation as date
  DATETIME, ///< Force interpretation as datetime/timestamp
  SKIP      ///< Skip this column during extraction
};

/**
 * Convert a TypeHint enum to its string representation.
 */
inline const char* type_hint_to_string(TypeHint hint) {
  switch (hint) {
  case TypeHint::AUTO:
    return "auto";
  case TypeHint::BOOLEAN:
    return "boolean";
  case TypeHint::INTEGER:
    return "integer";
  case TypeHint::DOUBLE:
    return "double";
  case TypeHint::STRING:
    return "string";
  case TypeHint::DATE:
    return "date";
  case TypeHint::DATETIME:
    return "datetime";
  case TypeHint::SKIP:
    return "skip";
  }
  return "unknown";
}

/**
 * Per-column configuration for value extraction.
 *
 * ColumnConfig allows specifying extraction settings on a per-column basis,
 * overriding the global ExtractionConfig for specific columns. This is useful
 * for CSVs with mixed formats, such as:
 * - Different decimal separators in different columns
 * - Column-specific NA value definitions
 * - Forcing specific type interpretations
 *
 * Fields use std::optional to enable selective overrides: when a field is
 * nullopt, the global ExtractionConfig value is used instead.
 *
 * @example
 * @code
 * // Create per-column config for a price column with European decimal format
 * ColumnConfig price_config;
 * price_config.type_hint = TypeHint::DOUBLE;
 * price_config.na_values = {"", "N/A", "-"};
 *
 * // Apply to specific column by index
 * ColumnConfigMap configs;
 * configs.set(2, price_config);  // Column index 2
 *
 * // Or by column name (requires header)
 * configs.set("price", price_config);
 * @endcode
 *
 * @see ExtractionConfig for global configuration options
 * @see ColumnConfigMap for managing per-column configurations
 */
struct ColumnConfig {
  /// Type hint to override automatic type detection.
  /// When set, the extractor will attempt to interpret values as this type.
  std::optional<TypeHint> type_hint = std::nullopt;

  /// Column-specific NA values (overrides ExtractionConfig::na_values).
  /// When set, only these values are recognized as NA for this column.
  std::optional<std::vector<std::string_view>> na_values = std::nullopt;

  /// Column-specific true values for boolean parsing.
  std::optional<std::vector<std::string_view>> true_values = std::nullopt;

  /// Column-specific false values for boolean parsing.
  std::optional<std::vector<std::string_view>> false_values = std::nullopt;

  /// Column-specific whitespace trimming behavior.
  std::optional<bool> trim_whitespace = std::nullopt;

  /// Column-specific leading zeros handling for integers.
  std::optional<bool> allow_leading_zeros = std::nullopt;

  /// Column-specific maximum integer digits.
  std::optional<size_t> max_integer_digits = std::nullopt;

  /**
   * Merge this column config with a global ExtractionConfig.
   * Returns an ExtractionConfig with this column's overrides applied.
   */
  ExtractionConfig merge_with(const ExtractionConfig& global) const {
    ExtractionConfig result = global;
    if (na_values.has_value()) {
      result.na_values = *na_values;
    }
    if (true_values.has_value()) {
      result.true_values = *true_values;
    }
    if (false_values.has_value()) {
      result.false_values = *false_values;
    }
    if (trim_whitespace.has_value()) {
      result.trim_whitespace = *trim_whitespace;
    }
    if (allow_leading_zeros.has_value()) {
      result.allow_leading_zeros = *allow_leading_zeros;
    }
    if (max_integer_digits.has_value()) {
      result.max_integer_digits = *max_integer_digits;
    }
    return result;
  }

  /**
   * Check if this config has any overrides set.
   */
  bool has_overrides() const {
    return type_hint.has_value() || na_values.has_value() || true_values.has_value() ||
           false_values.has_value() || trim_whitespace.has_value() ||
           allow_leading_zeros.has_value() || max_integer_digits.has_value();
  }

  /// Factory for default config (no overrides).
  static ColumnConfig defaults() { return ColumnConfig{}; }

  /// Factory for string-only column (skip type conversion).
  static ColumnConfig as_string() {
    ColumnConfig config;
    config.type_hint = TypeHint::STRING;
    return config;
  }

  /// Factory for integer column.
  static ColumnConfig as_integer() {
    ColumnConfig config;
    config.type_hint = TypeHint::INTEGER;
    return config;
  }

  /// Factory for double/float column.
  static ColumnConfig as_double() {
    ColumnConfig config;
    config.type_hint = TypeHint::DOUBLE;
    return config;
  }

  /// Factory for boolean column.
  static ColumnConfig as_boolean() {
    ColumnConfig config;
    config.type_hint = TypeHint::BOOLEAN;
    return config;
  }

  /// Factory for skipped column.
  static ColumnConfig skip() {
    ColumnConfig config;
    config.type_hint = TypeHint::SKIP;
    return config;
  }
};

/**
 * Container for managing per-column configurations.
 *
 * ColumnConfigMap allows setting configuration overrides for specific columns,
 * either by index (0-based) or by column name (requires header row). Columns
 * without explicit configuration use the global ExtractionConfig.
 *
 * @example
 * @code
 * ColumnConfigMap configs;
 *
 * // Set by index (works without header)
 * configs.set(0, ColumnConfig::as_string());   // First column as string
 * configs.set(2, ColumnConfig::as_double());   // Third column as double
 *
 * // Set by name (resolved at extraction time, requires header)
 * configs.set("id", ColumnConfig::as_integer());
 * configs.set("price", ColumnConfig::as_double());
 *
 * // Get config for a column (returns nullptr if no override)
 * const ColumnConfig* config = configs.get(2);
 * if (config && config->type_hint == TypeHint::DOUBLE) {
 *     // Parse as double
 * }
 * @endcode
 *
 * @see ColumnConfig for available configuration options
 * @see ValueExtractor for using column configs during extraction
 */
class ColumnConfigMap {
public:
  ColumnConfigMap() = default;

  /**
   * Set configuration for a column by index.
   * @param col_index 0-based column index
   * @param config Configuration to apply to this column
   */
  void set(size_t col_index, const ColumnConfig& config) { by_index_[col_index] = config; }

  /**
   * Set configuration for a column by name.
   * Column names are resolved when the ValueExtractor is initialized with headers.
   * @param col_name Column name (case-sensitive, must match header exactly)
   * @param config Configuration to apply to this column
   */
  void set(const std::string& col_name, const ColumnConfig& config) { by_name_[col_name] = config; }

  /**
   * Get configuration for a column by index.
   * @param col_index 0-based column index
   * @return Pointer to column config, or nullptr if no override is set
   */
  const ColumnConfig* get(size_t col_index) const {
    auto it = by_index_.find(col_index);
    return it != by_index_.end() ? &it->second : nullptr;
  }

  /**
   * Get configuration for a column by name.
   * @param col_name Column name
   * @return Pointer to column config, or nullptr if no override is set
   */
  const ColumnConfig* get(const std::string& col_name) const {
    auto it = by_name_.find(col_name);
    return it != by_name_.end() ? &it->second : nullptr;
  }

  /**
   * Check if any column configurations are set.
   */
  bool empty() const { return by_index_.empty() && by_name_.empty(); }

  /**
   * Clear all column configurations.
   */
  void clear() {
    by_index_.clear();
    by_name_.clear();
  }

  /**
   * Get all configurations by index.
   */
  const std::unordered_map<size_t, ColumnConfig>& by_index() const { return by_index_; }

  /**
   * Get all configurations by name.
   */
  const std::unordered_map<std::string, ColumnConfig>& by_name() const { return by_name_; }

  /**
   * Resolve name-based configurations to indices using a column name map.
   * After calling this, all by-name configs are converted to by-index configs.
   * @param name_to_index Map from column name to index
   */
  void resolve_names(const std::unordered_map<std::string, size_t>& name_to_index) {
    for (const auto& [name, config] : by_name_) {
      auto it = name_to_index.find(name);
      if (it != name_to_index.end()) {
        // Name-based config takes precedence if there's a conflict
        by_index_[it->second] = config;
      }
    }
    // Keep by_name_ around for reference, but by_index_ now has all resolved configs
  }

private:
  std::unordered_map<size_t, ColumnConfig> by_index_;
  std::unordered_map<std::string, ColumnConfig> by_name_;
};

/**
 * Parse a boolean value from a string.
 * Checks against configurable true/false/NA values.
 */
really_inline ExtractResult<bool>
parse_bool(const char* str, size_t len,
           const ExtractionConfig& config = ExtractionConfig::defaults()) {
  if (len == 0)
    return {std::nullopt, nullptr};

  const char* ptr = str;
  const char* end = str + len;

  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return {std::nullopt, nullptr};
  }

  std::string_view sv(ptr, end - ptr);
  for (const auto& tv : config.true_values)
    if (sv == tv)
      return {true, nullptr};
  for (const auto& fv : config.false_values)
    if (sv == fv)
      return {false, nullptr};
  for (const auto& na : config.na_values)
    if (sv == na)
      return {std::nullopt, nullptr};
  return {std::nullopt, "Invalid boolean value"};
}

/**
 * Check if a string value represents NA/missing.
 */
really_inline bool is_na(const char* str, size_t len,
                         const ExtractionConfig& config = ExtractionConfig::defaults()) {
  if (len == 0)
    return true;
  const char* ptr = str;
  const char* end = str + len;
  if (config.trim_whitespace) {
    while (ptr < end && (*ptr == ' ' || *ptr == '\t'))
      ++ptr;
    while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t'))
      --end;
    if (ptr == end)
      return true;
  }
  std::string_view sv(ptr, end - ptr);
  for (const auto& na : config.na_values)
    if (sv == na)
      return true;
  return false;
}

} // namespace libvroom

#endif // LIBVROOM_EXTRACTION_CONFIG_H
