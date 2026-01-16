/**
 * @file dialect.cpp
 * @brief Implementation of CSV dialect detection algorithm.
 */

#include "dialect.h"

#include "io_util.h"
#include "simd_number_parsing.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstring>
#include <fstream>
#include <sstream>
#include <unordered_map>

namespace libvroom {

// ============================================================================
// Constants for dialect scoring
// ============================================================================

/// Score boost for dialects matching detected escape patterns (e.g., \" or "")
/// Applied when there's a clear escape pattern signal in the data
constexpr double ESCAPE_PATTERN_MATCH_BOOST = 1.2;

/// Smaller boost for double-quote escaping when explicitly detected
/// Used to slightly prefer RFC 4180 style when evidence is present
constexpr double DOUBLE_QUOTE_ESCAPE_BOOST = 1.1;

// ============================================================================
// Dialect
// ============================================================================

std::string Dialect::to_string() const {
  std::ostringstream ss;
  ss << "Dialect{delimiter=";

  // Format special characters nicely
  // LCOV_EXCL_BR_START - switch branches; common delimiters tested
  switch (delimiter) {
  case ',':
    ss << "','";
    break;
  case '\t':
    ss << "'\\t'";
    break;
  case ';':
    ss << "';'";
    break;
  case '|':
    ss << "'|'";
    break;
  case ':':
    ss << "':'";
    break;
  default:
    ss << "'" << delimiter << "'";
    break;
  }
  // LCOV_EXCL_BR_STOP

  ss << ", quote=";
  // LCOV_EXCL_BR_START - formatting branches; common cases tested
  if (quote_char == '"') {
    ss << "'\"'";
  } else if (quote_char == '\'') {
    ss << "\"'\"";
  } else if (quote_char == '\0') {
    ss << "none";
  } else {
    ss << "'" << quote_char << "'";
  }

  ss << ", escape=";
  if (double_quote) {
    ss << "double";
  } else if (escape_char == '\\') {
    ss << "backslash";
  } else {
    ss << "'" << escape_char << "'";
  }
  // LCOV_EXCL_BR_STOP

  if (comment_char != '\0') {
    ss << ", comment='" << comment_char << "'";
  }

  ss << "}";
  return ss.str();
}

// ============================================================================
// DialectDetector
// ============================================================================

DialectDetector::DialectDetector(const DetectionOptions& options) : options_(options) {}

DetectionResult DialectDetector::detect(const uint8_t* buf, size_t len) const {
  DetectionResult result;

  if (buf == nullptr || len == 0) {
    result.warning = "Empty or null input";
    return result;
  }

  // Skip leading comment lines before dialect detection
  char comment_char = '\0';
  size_t comment_lines_skipped = 0;
  size_t comment_offset = skip_comment_lines(buf, len, comment_char, comment_lines_skipped);

  // Record detected comment info
  result.comment_char = comment_char;
  result.comment_lines_skipped = comment_lines_skipped;

  // Adjust buffer to skip comment lines
  const uint8_t* data_buf = buf + comment_offset;
  size_t data_len = len - comment_offset;

  if (data_len == 0) {
    result.warning = "File contains only comment lines";
    return result;
  }

  // Calculate adaptive sample size based on first row length.
  // For wide CSV files (many columns), rows can be very long.
  // We need at least min_rows complete rows for pattern detection.
  size_t effective_sample_size = options_.sample_size;

  // Find the first newline to estimate row length
  size_t first_newline = 0;
  for (size_t i = 0; i < std::min(data_len, options_.sample_size); ++i) {
    if (data_buf[i] == '\n') {
      first_newline = i + 1; // Include the newline
      break;
    }
  }

  // If we found a newline and the row is long, increase sample size
  // to ensure we can get at least min_rows complete rows
  if (first_newline > 0) {
    // Estimate bytes needed: first_row_len * (min_rows + 1) to be safe
    // The +1 accounts for potential variation in row lengths
    size_t estimated_needed = first_newline * (options_.min_rows + 1);
    if (estimated_needed > effective_sample_size) {
      // Cap at a reasonable maximum (1MB) to avoid excessive memory use
      constexpr size_t MAX_ADAPTIVE_SAMPLE = 1024 * 1024;
      effective_sample_size = std::min(estimated_needed, MAX_ADAPTIVE_SAMPLE);
    }
  } else if (data_len > options_.sample_size) {
    // No newline found in initial sample - this means rows are very long
    // Expand sample to try to capture at least one complete row
    // Use 4x the default sample size as a heuristic
    constexpr size_t MAX_ADAPTIVE_SAMPLE = 1024 * 1024;
    effective_sample_size = std::min(options_.sample_size * 4, MAX_ADAPTIVE_SAMPLE);
  }

  // Limit to actual data size and effective sample size
  size_t sample_len = std::min(data_len, effective_sample_size);

  // Detect line ending style
  result.dialect.line_ending = detect_line_ending(data_buf, sample_len);

  // Generate all candidate dialects
  auto candidates = generate_candidates();

  // Score each candidate
  for (const auto& dialect : candidates) {
    auto candidate = score_dialect(dialect, data_buf, sample_len);
    result.candidates.push_back(candidate);
  }

  // Sort by consistency score (best first)
  std::sort(result.candidates.begin(), result.candidates.end());

  // Select best candidate
  if (!result.candidates.empty() && result.candidates[0].consistency_score > 0) {
    const auto& best = result.candidates[0];
    result.dialect = best.dialect;
    result.dialect.line_ending = detect_line_ending(data_buf, sample_len);
    result.dialect.comment_char = comment_char; // Propagate detected comment char
    result.confidence = best.consistency_score;
    result.detected_columns = best.num_columns;

    // Detect header
    result.has_header = detect_header(result.dialect, data_buf, sample_len);

    // Count rows analyzed
    auto rows = find_rows(result.dialect, data_buf, sample_len);
    result.rows_analyzed = rows.size();

    // Check for ambiguous cases (multiple candidates with similar scores)
    if (result.candidates.size() > 1) {
      double second_score = result.candidates[1].consistency_score;
      if (second_score > 0.9 * best.consistency_score) {
        result.warning = "Multiple dialects have similar scores; detection may be ambiguous";
      }
    }
  } else {
    result.warning = "Could not detect a valid CSV dialect";
  }

  return result;
}

DetectionResult DialectDetector::detect_file(const std::string& filename) const {
  // Read sample from file
  std::ifstream file(filename, std::ios::binary);
  if (!file) {
    DetectionResult result;
    result.warning = "Could not open file: " + filename;
    return result;
  }

  std::vector<uint8_t> buffer(options_.sample_size);
  file.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
  size_t bytes_read = static_cast<size_t>(file.gcount());

  return detect(buffer.data(), bytes_read);
}

std::vector<Dialect> DialectDetector::generate_candidates() const {
  std::vector<Dialect> candidates;

  // Generate all combinations of delimiter, quote char, and escape style
  for (char delim : options_.delimiters) {
    for (char quote : options_.quote_chars) {
      // Test double-quote escaping (RFC 4180 style: "" -> ")
      {
        Dialect d;
        d.delimiter = delim;
        d.quote_char = quote;
        d.escape_char = quote;
        d.double_quote = true;
        candidates.push_back(d);
      }

      // Test each escape character (e.g., backslash: \" -> ")
      for (char esc : options_.escape_chars) {
        if (esc != quote) { // Skip if same as quote (handled above)
          Dialect d;
          d.delimiter = delim;
          d.quote_char = quote;
          d.escape_char = esc;
          d.double_quote = false;
          candidates.push_back(d);
        }
      }
    }
    // Also test without quotes
    Dialect d;
    d.delimiter = delim;
    d.quote_char = '\0';
    d.escape_char = '\0';
    d.double_quote = false;
    candidates.push_back(d);
  }

  return candidates;
}

// Helper: detect escape pattern usage in data
// Returns: negative for double-quote pattern (RFC 4180), positive for escape-char preference
static int detect_escape_pattern(const uint8_t* buf, size_t len, char quote_char,
                                 char escape_char) {
  int escape_char_count = 0;
  int double_quote_count = 0;

  for (size_t i = 0; i + 1 < len; ++i) {
    // Check for escape_char followed by quote_char (e.g., \")
    // Only count if escape_char is different from quote_char
    if (escape_char != quote_char && buf[i] == static_cast<uint8_t>(escape_char) &&
        buf[i + 1] == static_cast<uint8_t>(quote_char)) {
      escape_char_count++;
    }
    // Check for double-quote pattern (e.g., "")
    if (buf[i] == static_cast<uint8_t>(quote_char) &&
        buf[i + 1] == static_cast<uint8_t>(quote_char)) {
      double_quote_count++;
    }
  }

  // Return negative for double-quote preference, positive for escape-char preference
  if (escape_char_count > 0 && double_quote_count == 0) {
    return escape_char_count; // Escape char pattern detected (e.g., \")
  } else if (double_quote_count > 0 && escape_char_count == 0) {
    return -double_quote_count; // Double-quote pattern detected (e.g., "")
  }
  return 0; // Ambiguous or no escapes
}

DialectCandidate DialectDetector::score_dialect(const Dialect& dialect, const uint8_t* buf,
                                                size_t len) const {

  DialectCandidate candidate;
  candidate.dialect = dialect;

  std::vector<size_t> row_field_counts;
  candidate.pattern_score = compute_pattern_score(dialect, buf, len, row_field_counts);

  if (row_field_counts.empty()) {
    return candidate; // No rows found
  }

  // Find modal column count
  std::unordered_map<size_t, size_t> count_freq;
  for (size_t c : row_field_counts) {
    count_freq[c]++;
  }
  size_t modal_count = 0;
  size_t modal_freq = 0;
  for (const auto& [count, freq] : count_freq) {
    if (freq > modal_freq) {
      modal_freq = freq;
      modal_count = count;
    }
  }
  candidate.num_columns = modal_count;

  // Compute type score
  candidate.type_score = compute_type_score(dialect, buf, len);

  // Combined consistency score
  // Use pattern_score as primary signal, with type_score as a bonus
  // This handles string-heavy files that would otherwise get penalized
  if (candidate.pattern_score > 0.9 && candidate.num_columns > 1) {
    // For highly consistent row patterns with multiple columns,
    // give a strong baseline score even if type_score is low.
    // Files with all strings are valid CSVs and should be detected.
    candidate.consistency_score =
        candidate.pattern_score * std::max(0.6, std::sqrt(std::max(0.1, candidate.type_score)));
  } else if (candidate.pattern_score > 0.8 && candidate.num_columns > 1) {
    candidate.consistency_score =
        candidate.pattern_score * std::sqrt(std::max(0.1, candidate.type_score));
  } else {
    candidate.consistency_score = candidate.pattern_score * candidate.type_score;
  }

  // Boost score based on escape pattern match
  // This helps distinguish dialects that produce similar field counts
  // but use different escape mechanisms.
  // Note: When both \" and "" patterns are present, returns 0 (ambiguous),
  // and no boost is applied - the tie-breakers will decide.
  if (dialect.quote_char != '\0') {
    char esc_to_check = dialect.double_quote ? '\0' : dialect.escape_char;
    if (esc_to_check != '\0') {
      int escape_signal = detect_escape_pattern(buf, len, dialect.quote_char, esc_to_check);
      if (escape_signal > 0 && !dialect.double_quote) {
        // Backslash escapes detected and this dialect uses backslash escaping
        candidate.consistency_score *= ESCAPE_PATTERN_MATCH_BOOST;
      } else if (escape_signal < 0 && dialect.double_quote) {
        // Double-quote escapes detected and this dialect uses double-quote
        candidate.consistency_score *= ESCAPE_PATTERN_MATCH_BOOST;
      }
    } else if (dialect.double_quote) {
      // Check if double-quote escapes are present
      int escape_signal = detect_escape_pattern(buf, len, dialect.quote_char, dialect.quote_char);
      if (escape_signal < 0) {
        // Double-quote escapes detected
        candidate.consistency_score *= DOUBLE_QUOTE_ESCAPE_BOOST;
      }
    }
  }

  return candidate;
}

double DialectDetector::compute_pattern_score(const Dialect& dialect, const uint8_t* buf,
                                              size_t len,
                                              std::vector<size_t>& row_field_counts) const {

  row_field_counts.clear();

  auto rows = find_rows(dialect, buf, len);
  if (rows.size() < options_.min_rows) {
    return 0.0;
  }

  // Count fields in each row
  for (const auto& [start, end] : rows) {
    assert(end >= start && "Invalid row range: end must be >= start");
    auto fields = extract_fields(dialect, buf + start, end - start);
    row_field_counts.push_back(fields.size());
  }

  if (row_field_counts.empty()) {
    return 0.0;
  }

  // Calculate consistency: fraction of rows matching modal field count
  std::unordered_map<size_t, size_t> count_freq;
  for (size_t c : row_field_counts) {
    count_freq[c]++;
  }

  size_t modal_freq = 0;
  for (const auto& [count, freq] : count_freq) {
    modal_freq = std::max(modal_freq, freq);
  }

  return static_cast<double>(modal_freq) / row_field_counts.size();
}

double DialectDetector::compute_type_score(const Dialect& dialect, const uint8_t* buf,
                                           size_t len) const {

  auto rows = find_rows(dialect, buf, len);
  if (rows.empty()) {
    return 0.0;
  }

  size_t typed_cells = 0;
  size_t total_cells = 0;

  // Skip first row if it might be a header
  size_t start_row = (rows.size() > 1) ? 1 : 0;

  // Collect all fields for batch processing.
  // Note: field_ptrs point into the input buffer `buf` (via extract_fields),
  // so they remain valid throughout this function's scope.
  std::vector<const uint8_t*> field_ptrs;
  std::vector<size_t> field_lengths;

  // Pre-allocate based on estimated fields
  size_t estimated_fields = (rows.size() - start_row) * 10;
  field_ptrs.reserve(estimated_fields);
  field_lengths.reserve(estimated_fields);

  for (size_t i = start_row; i < rows.size(); ++i) {
    const auto& [start, end] = rows[i];
    assert(end >= start && "Invalid row range: end must be >= start");
    auto fields = extract_fields(dialect, buf + start, end - start);

    for (const auto& field : fields) {
      field_ptrs.push_back(reinterpret_cast<const uint8_t*>(field.data()));
      field_lengths.push_back(field.size());
      total_cells++;
    }
  }

  if (total_cells == 0) {
    return 0.0;
  }

  // Use SIMD batch validation for integer/float detection.
  // This efficiently identifies numeric fields without calling infer_cell_type().
  size_t integer_count = 0;
  size_t float_count = 0;
  size_t other_count = 0;

  SIMDTypeValidator::validate_batch(field_ptrs.data(), field_lengths.data(), total_cells,
                                    integer_count, float_count, other_count);

  // Integer and float cells are definitely typed
  typed_cells = integer_count + float_count;

  // For non-numeric fields, check if they're other typed values using
  // infer_cell_type(). We only call this for fields in the "other" category.
  //
  // Trade-off: For numeric-heavy CSVs, we avoid calling infer_cell_type()
  // for most fields. For mixed-type CSVs, performance is similar to scalar.
  //
  // Note: We re-call could_be_integer/could_be_float to identify which
  // specific fields to skip. This is a trade-off: avoiding storing a
  // per-field type marker array vs. re-validating numeric fields.
  // For most CSV files, numeric fields are quick to validate.
  if (other_count > 0) {
    for (size_t i = 0; i < total_cells; ++i) {
      const uint8_t* data = field_ptrs[i];
      size_t field_len = field_lengths[i];

      // Skip cells already counted as integer or float by SIMD
      if (SIMDTypeValidator::could_be_integer(data, field_len) ||
          SIMDTypeValidator::could_be_float(data, field_len)) {
        continue;
      }

      // Use infer_cell_type for non-numeric fields to detect:
      // empty, boolean, date, time, datetime
      std::string_view sv(reinterpret_cast<const char*>(data), field_len);
      CellType type = infer_cell_type(sv);
      if (type != CellType::STRING) {
        typed_cells++;
      }
    }
  }

  // Add small epsilon to avoid zero scores
  constexpr double eps = 1e-10;
  return std::max(eps, static_cast<double>(typed_cells) / total_cells);
}

Dialect::LineEnding DialectDetector::detect_line_ending(const uint8_t* buf, size_t len) {

  bool has_crlf = false;
  bool has_lf = false;
  bool has_cr = false;

  for (size_t i = 0; i < len; ++i) {
    if (buf[i] == '\r') {
      if (i + 1 < len && buf[i + 1] == '\n') {
        has_crlf = true;
        ++i; // Skip the \n
      } else {
        has_cr = true;
      }
    } else if (buf[i] == '\n') {
      has_lf = true;
    }
  }

  int count = (has_crlf ? 1 : 0) + (has_lf ? 1 : 0) + (has_cr ? 1 : 0);
  if (count > 1) {
    return Dialect::LineEnding::MIXED;
  }
  if (has_crlf)
    return Dialect::LineEnding::CRLF;
  if (has_lf)
    return Dialect::LineEnding::LF;
  if (has_cr)
    return Dialect::LineEnding::CR;
  return Dialect::LineEnding::UNKNOWN;
}

bool DialectDetector::detect_header(const Dialect& dialect, const uint8_t* buf, size_t len) const {

  auto rows = find_rows(dialect, buf, len);
  if (rows.size() < 2) {
    return false;
  }

  // Extract first two rows
  auto header_fields = extract_fields(dialect, buf + rows[0].first, rows[0].second - rows[0].first);
  auto data_fields = extract_fields(dialect, buf + rows[1].first, rows[1].second - rows[1].first);

  if (header_fields.empty() || data_fields.empty()) {
    return false;
  }

  // Heuristics for header detection:
  // 1. Header cells are typically all strings (non-empty)
  // 2. Data row has different types than header

  size_t header_strings = 0;
  size_t header_non_empty = 0;

  for (const auto& field : header_fields) {
    CellType type = infer_cell_type(field);
    if (type == CellType::STRING && !field.empty()) {
      header_strings++;
    }
    if (!field.empty()) {
      header_non_empty++;
    }
  }

  size_t data_non_strings = 0;
  for (const auto& field : data_fields) {
    CellType type = infer_cell_type(field);
    if (type != CellType::STRING && type != CellType::EMPTY) {
      data_non_strings++;
    }
  }

  // Header likely if:
  // - Most header cells are non-empty strings
  // - Data row has some typed (non-string) values, OR all header cells are strings
  double string_ratio =
      header_non_empty > 0 ? static_cast<double>(header_strings) / header_non_empty : 0.0;

  return (string_ratio > 0.5) && (data_non_strings > 0 || header_strings == header_fields.size());
}

/// Check if a row starts with a comment character (after optional leading whitespace)
bool DialectDetector::is_comment_line(const uint8_t* row_start, size_t row_len) const {
  if (options_.comment_chars.empty() || row_len == 0) {
    return false;
  }

  // Skip leading whitespace
  size_t i = 0;
  while (i < row_len && (row_start[i] == ' ' || row_start[i] == '\t')) {
    i++;
  }

  if (i >= row_len) {
    return false; // Empty line (all whitespace)
  }

  // Check if first non-whitespace character is a comment character
  char first_char = static_cast<char>(row_start[i]);
  for (char c : options_.comment_chars) {
    if (first_char == c) {
      return true;
    }
  }

  return false;
}

std::vector<std::pair<size_t, size_t>>
DialectDetector::find_rows(const Dialect& dialect, const uint8_t* buf, size_t len) const {

  std::vector<std::pair<size_t, size_t>> rows;
  if (len == 0)
    return rows;

  bool in_quote = false;
  size_t row_start = 0;

  for (size_t i = 0; i < len; ++i) {
    uint8_t c = buf[i];

    // Handle escape character (backslash or other)
    // When we see an escape char, we skip both it and the next character.
    // Note: ++i here plus the for-loop's ++i after continue = skip 2 chars total
    if (!dialect.double_quote && dialect.escape_char != '\0' &&
        c == static_cast<uint8_t>(dialect.escape_char) && i + 1 < len) {
      ++i; // Move to escaped char; for-loop ++i will move past it
      continue;
    }

    if (dialect.quote_char != '\0' && c == static_cast<uint8_t>(dialect.quote_char)) {
      // Handle double-quote escaping (RFC 4180 style)
      if (dialect.double_quote && i + 1 < len &&
          buf[i + 1] == static_cast<uint8_t>(dialect.quote_char)) {
        ++i; // Skip escaped quote
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote) {
      if (c == '\n') {
        // Handle CRLF
        size_t row_end = i;
        if (row_end > row_start && buf[row_end - 1] == '\r') {
          row_end--;
        }
        if (row_end > row_start) { // Non-empty row
          // Skip comment lines
          if (!is_comment_line(buf + row_start, row_end - row_start)) {
            rows.emplace_back(row_start, row_end);
          }
        }
        row_start = i + 1;

        if (rows.size() >= options_.max_rows) {
          break;
        }
      } else if (c == '\r' && (i + 1 >= len || buf[i + 1] != '\n')) {
        // CR not followed by LF (old Mac style)
        if (i > row_start) {
          // Skip comment lines
          if (!is_comment_line(buf + row_start, i - row_start)) {
            rows.emplace_back(row_start, i);
          }
        }
        row_start = i + 1;

        if (rows.size() >= options_.max_rows) {
          break;
        }
      }
    }
  }

  // Handle last row without trailing newline
  if (row_start < len && rows.size() < options_.max_rows) {
    // Skip comment lines
    if (!is_comment_line(buf + row_start, len - row_start)) {
      rows.emplace_back(row_start, len);
    }
  }

  return rows;
}

std::vector<std::string_view> DialectDetector::extract_fields(const Dialect& dialect,
                                                              const uint8_t* row_start,
                                                              size_t row_len) const {

  std::vector<std::string_view> fields;
  if (row_len == 0)
    return fields;

  const char* data = reinterpret_cast<const char*>(row_start);
  bool in_quote = false;
  size_t field_start = 0;

  for (size_t i = 0; i < row_len; ++i) {
    char c = data[i];

    // Handle escape character (backslash or other)
    // When we see an escape char, we skip both it and the next character.
    // Note: ++i here plus the for-loop's ++i after continue = skip 2 chars total
    if (!dialect.double_quote && dialect.escape_char != '\0' && c == dialect.escape_char &&
        i + 1 < row_len) {
      ++i; // Move to escaped char; for-loop ++i will move past it
      continue;
    }

    if (dialect.quote_char != '\0' && c == dialect.quote_char) {
      if (dialect.double_quote && i + 1 < row_len && data[i + 1] == dialect.quote_char) {
        ++i; // Skip escaped quote
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote && c == dialect.delimiter) {
      // End of field
      fields.emplace_back(data + field_start, i - field_start);
      field_start = i + 1;
    }
  }

  // Add last field
  fields.emplace_back(data + field_start, row_len - field_start);

  // Remove quotes from quoted fields
  for (auto& field : fields) {
    if (field.size() >= 2 && dialect.quote_char != '\0' && field.front() == dialect.quote_char &&
        field.back() == dialect.quote_char) {
      field = field.substr(1, field.size() - 2);
    }
  }

  return fields;
}

DialectDetector::CellType DialectDetector::infer_cell_type(std::string_view cell) {
  // Trim whitespace
  while (!cell.empty() && std::isspace(static_cast<unsigned char>(cell.front()))) {
    cell.remove_prefix(1);
  }
  while (!cell.empty() && std::isspace(static_cast<unsigned char>(cell.back()))) {
    cell.remove_suffix(1);
  }

  if (cell.empty()) {
    return CellType::EMPTY;
  }

  // Boolean check
  if (cell == "true" || cell == "false" || cell == "TRUE" || cell == "FALSE" || cell == "True" ||
      cell == "False") {
    return CellType::BOOLEAN;
  }

  // Try parsing as integer
  {
    size_t i = 0;
    if (cell[0] == '+' || cell[0] == '-')
      i++;
    if (i < cell.size() && std::isdigit(static_cast<unsigned char>(cell[i]))) {
      bool all_digits = true;
      for (; i < cell.size() && all_digits; ++i) {
        if (!std::isdigit(static_cast<unsigned char>(cell[i]))) {
          all_digits = false;
        }
      }
      if (all_digits && i == cell.size()) {
        return CellType::INTEGER;
      }
    }
  }

  // Try parsing as float
  {
    size_t i = 0;
    if (cell[0] == '+' || cell[0] == '-')
      i++;

    bool has_digits = false;
    bool has_dot = false;
    bool has_exp = false;
    bool valid = true;

    // Integer part
    while (i < cell.size() && std::isdigit(static_cast<unsigned char>(cell[i]))) {
      has_digits = true;
      i++;
    }

    // Decimal part
    if (i < cell.size() && cell[i] == '.') {
      has_dot = true;
      i++;
      while (i < cell.size() && std::isdigit(static_cast<unsigned char>(cell[i]))) {
        has_digits = true;
        i++;
      }
    }

    // Exponent part
    if (i < cell.size() && (cell[i] == 'e' || cell[i] == 'E')) {
      has_exp = true;
      i++;
      if (i < cell.size() && (cell[i] == '+' || cell[i] == '-'))
        i++;
      bool exp_digits = false;
      while (i < cell.size() && std::isdigit(static_cast<unsigned char>(cell[i]))) {
        exp_digits = true;
        i++;
      }
      if (!exp_digits)
        valid = false;
    }

    if (valid && has_digits && (has_dot || has_exp) && i == cell.size()) {
      return CellType::FLOAT;
    }
  }

  // Date patterns: YYYY-MM-DD, YYYY/MM/DD, DD-MM-YYYY, DD/MM/YYYY
  if (cell.size() >= 8 && cell.size() <= 10) {
    bool might_be_date = false;

    // YYYY-MM-DD or YYYY/MM/DD
    if (cell.size() == 10 && std::isdigit(static_cast<unsigned char>(cell[0])) &&
        std::isdigit(static_cast<unsigned char>(cell[1])) &&
        std::isdigit(static_cast<unsigned char>(cell[2])) &&
        std::isdigit(static_cast<unsigned char>(cell[3])) && (cell[4] == '-' || cell[4] == '/') &&
        std::isdigit(static_cast<unsigned char>(cell[5])) &&
        std::isdigit(static_cast<unsigned char>(cell[6])) && cell[7] == cell[4] &&
        std::isdigit(static_cast<unsigned char>(cell[8])) &&
        std::isdigit(static_cast<unsigned char>(cell[9]))) {
      might_be_date = true;
    }

    // DD-MM-YYYY or DD/MM/YYYY
    if (cell.size() == 10 && std::isdigit(static_cast<unsigned char>(cell[0])) &&
        std::isdigit(static_cast<unsigned char>(cell[1])) && (cell[2] == '-' || cell[2] == '/') &&
        std::isdigit(static_cast<unsigned char>(cell[3])) &&
        std::isdigit(static_cast<unsigned char>(cell[4])) && cell[5] == cell[2] &&
        std::isdigit(static_cast<unsigned char>(cell[6])) &&
        std::isdigit(static_cast<unsigned char>(cell[7])) &&
        std::isdigit(static_cast<unsigned char>(cell[8])) &&
        std::isdigit(static_cast<unsigned char>(cell[9]))) {
      might_be_date = true;
    }

    if (might_be_date) {
      return CellType::DATE;
    }
  }

  // Time pattern: HH:MM or HH:MM:SS
  if ((cell.size() == 5 || cell.size() == 8) && std::isdigit(static_cast<unsigned char>(cell[0])) &&
      std::isdigit(static_cast<unsigned char>(cell[1])) && cell[2] == ':' &&
      std::isdigit(static_cast<unsigned char>(cell[3])) &&
      std::isdigit(static_cast<unsigned char>(cell[4]))) {
    if (cell.size() == 5) {
      return CellType::TIME;
    }
    if (cell[5] == ':' && std::isdigit(static_cast<unsigned char>(cell[6])) &&
        std::isdigit(static_cast<unsigned char>(cell[7]))) {
      return CellType::TIME;
    }
  }

  // Datetime: date + T/space + time
  if (cell.size() >= 16) {
    size_t sep_pos = cell.find('T');
    if (sep_pos == std::string_view::npos) {
      sep_pos = cell.find(' ');
    }
    if (sep_pos != std::string_view::npos && sep_pos >= 8) {
      auto date_part = cell.substr(0, sep_pos);
      auto time_part = cell.substr(sep_pos + 1);

      // Remove timezone suffix if present
      if (!time_part.empty() && time_part.back() == 'Z') {
        time_part.remove_suffix(1);
      }
      // Handle +HH:MM timezone
      auto plus_pos = time_part.find('+');
      auto minus_pos = time_part.find('-');
      if (plus_pos != std::string_view::npos && plus_pos > 0) {
        time_part = time_part.substr(0, plus_pos);
      } else if (minus_pos != std::string_view::npos && minus_pos > 5) {
        time_part = time_part.substr(0, minus_pos);
      }

      if (infer_cell_type(date_part) == CellType::DATE &&
          (infer_cell_type(time_part) == CellType::TIME || time_part.size() >= 5)) {
        return CellType::DATETIME;
      }
    }
  }

  return CellType::STRING;
}

const char* DialectDetector::cell_type_to_string(CellType type) {
  // LCOV_EXCL_BR_START - exhaustive switch; all types tested elsewhere
  switch (type) {
  case CellType::EMPTY:
    return "EMPTY";
  case CellType::INTEGER:
    return "INTEGER";
  case CellType::FLOAT:
    return "FLOAT";
  case CellType::DATE:
    return "DATE";
  case CellType::DATETIME:
    return "DATETIME";
  case CellType::TIME:
    return "TIME";
  case CellType::BOOLEAN:
    return "BOOLEAN";
  case CellType::STRING:
    return "STRING";
  default:
    return "UNKNOWN";
  }
  // LCOV_EXCL_BR_STOP
}

size_t DialectDetector::skip_comment_lines(const uint8_t* buf, size_t len, char& comment_char,
                                           size_t& lines_skipped) const {
  comment_char = '\0';
  lines_skipped = 0;

  if (buf == nullptr || len == 0 || options_.comment_chars.empty()) {
    return 0;
  }

  size_t offset = 0;

  while (offset < len) {
    // Skip leading whitespace on the line (spaces and tabs only)
    size_t line_start = offset;
    while (offset < len && (buf[offset] == ' ' || buf[offset] == '\t')) {
      offset++;
    }

    if (offset >= len) {
      break;
    }

    // Check if this line starts with a comment character
    char current_char = static_cast<char>(buf[offset]);
    bool is_comment = false;

    for (char c : options_.comment_chars) {
      if (current_char == c) {
        is_comment = true;
        // Record the comment character (first one found wins)
        if (comment_char == '\0') {
          comment_char = c;
        }
        break;
      }
    }

    if (!is_comment) {
      // Not a comment line; return the start of this line (before whitespace)
      return line_start;
    }

    // This is a comment line; skip to end of line
    lines_skipped++;
    while (offset < len && buf[offset] != '\n' && buf[offset] != '\r') {
      offset++;
    }

    // Skip line ending (LF, CR, or CRLF)
    if (offset < len) {
      if (buf[offset] == '\r') {
        offset++;
        if (offset < len && buf[offset] == '\n') {
          offset++;
        }
      } else if (buf[offset] == '\n') {
        offset++;
      }
    }
  }

  // All lines were comments; return end of buffer
  return offset;
}

} // namespace libvroom
