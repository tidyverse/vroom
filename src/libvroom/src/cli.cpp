/**
 * vroom - Command-line utility for CSV processing using libvroom
 * Inspired by zsv (https://github.com/liquidaty/zsv)
 */

#include "libvroom.h"
#include "libvroom_types.h"

#include "common_defs.h"
#include "encoding.h"
#include "io_util.h"
#include "mem_util.h"
#include "simd_highway.h"
#include "streaming.h"
#include "utf8.h"

#ifdef LIBVROOM_ENABLE_ARROW
#include "arrow_output.h"
#endif

#include <algorithm>
#include <array>
#include <cfloat>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <vector>

using namespace std;

// Constants
// MAX_THREADS raised to 1024 with uint16_t n_threads in index struct
constexpr int MAX_THREADS = 1024;
constexpr int MIN_THREADS = 1;
constexpr size_t MAX_COLUMN_WIDTH = 40;
constexpr size_t DEFAULT_NUM_ROWS = 10;
constexpr const char* VERSION = "0.1.0";

// =============================================================================
// Progress Bar Support
// =============================================================================

/**
 * @brief Simple text-based progress bar for terminal output.
 *
 * Displays a progress bar like: [====================] 100%
 * Only outputs to stderr when connected to a TTY.
 */
class ProgressBar {
public:
  /**
   * @brief Create a progress bar.
   *
   * @param enabled Whether to actually display progress (typically based on isatty)
   * @param width Width of the progress bar in characters (default: 40)
   */
  explicit ProgressBar(bool enabled, size_t width = 40) : enabled_(enabled), width_(width) {}

  /**
   * @brief Update the progress bar display.
   *
   * @param bytes_processed Bytes processed so far
   * @param total_bytes Total bytes to process
   * @return true (always continues - for use as progress callback)
   */
  bool update(size_t bytes_processed, size_t total_bytes) {
    if (!enabled_ || total_bytes == 0)
      return true;

    // Calculate percentage
    int percent = static_cast<int>((bytes_processed * 100) / total_bytes);

    // Only update if percentage changed (reduces flicker)
    if (percent == last_percent_)
      return true;
    last_percent_ = percent;

    // Calculate filled portion
    size_t filled = (percent * width_) / 100;

    // Build progress bar string
    std::string bar(width_, ' ');
    for (size_t i = 0; i < filled && i < width_; ++i) {
      bar[i] = '=';
    }
    if (filled < width_) {
      bar[filled] = '>';
    }

    // Output to stderr with carriage return (overwrites previous line)
    std::cerr << "\r[" << bar << "] " << std::setw(3) << percent << "%" << std::flush;

    return true;
  }

  /**
   * @brief Complete the progress bar and move to next line.
   */
  void finish() {
    if (enabled_) {
      // Show 100% and newline
      std::string bar(width_, '=');
      std::cerr << "\r[" << bar << "] 100%" << std::endl;
    }
  }

  /**
   * @brief Clear the progress bar (e.g., on error or cancellation).
   */
  void clear() {
    if (enabled_) {
      // Overwrite with spaces and return to start of line
      std::cerr << "\r" << std::string(width_ + 7, ' ') << "\r" << std::flush;
    }
  }

  /**
   * @brief Create a progress callback function for use with libvroom::Parser.
   *
   * @return A std::function suitable for ParseOptions::progress_callback
   */
  libvroom::ProgressCallback callback() {
    return [this](size_t processed, size_t total) { return this->update(processed, total); };
  }

private:
  bool enabled_;
  size_t width_;
  int last_percent_ = -1;
};

// Performance tuning constants
constexpr size_t QUOTE_LOOKBACK_LIMIT = 64 * 1024; // 64KB lookback for quote state
constexpr size_t MAX_BOUNDARY_SEARCH = 8192;       // Max search for row boundary
constexpr size_t MIN_PARALLEL_SIZE = 1024 * 1024;  // Minimum size for parallel processing

/**
 * CSV Iterator - Helper class to iterate over parsed CSV data
 */
class CsvIterator {
public:
  CsvIterator(const uint8_t* buf, const libvroom::ParseIndex& idx) : buf_(buf), idx_(idx) {
    // Merge indexes from all threads into sorted order
    mergeIndexes();
  }

  size_t numFields() const { return merged_indexes_.size(); }

  // Get the content of field at position i (0-indexed)
  std::string getField(size_t i) const {
    if (i >= merged_indexes_.size())
      return "";

    size_t start = (i == 0) ? 0 : merged_indexes_[i - 1] + 1;
    size_t end = merged_indexes_[i];

    // Bounds check: ensure start <= end
    if (start > end)
      return "";

    // Handle quoted fields
    std::string field;
    bool in_quote = false;
    for (size_t j = start; j < end; ++j) {
      char c = static_cast<char>(buf_[j]);
      if (c == '"') {
        if (in_quote && j + 1 < end && buf_[j + 1] == '"') {
          field += '"';
          ++j; // Skip escaped quote
        } else {
          in_quote = !in_quote;
        }
      } else {
        field += c;
      }
    }
    return field;
  }

  // Check if a field ends with newline (marks end of row)
  // Supports LF (\n) and CR (\r) line endings
  bool isRowEnd(size_t i) const {
    if (i >= merged_indexes_.size())
      return true;
    size_t pos = merged_indexes_[i];
    return buf_[pos] == '\n' || buf_[pos] == '\r';
  }

  // Get all rows as vector of vectors of strings
  std::vector<std::vector<std::string>> getRows(size_t max_rows = SIZE_MAX) const {
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> current_row;

    for (size_t i = 0; i < merged_indexes_.size() && rows.size() < max_rows; ++i) {
      current_row.push_back(getField(i));
      if (isRowEnd(i)) {
        rows.push_back(std::move(current_row));
        current_row.clear();
      }
    }
    if (!current_row.empty() && rows.size() < max_rows) {
      rows.push_back(std::move(current_row));
    }
    return rows;
  }

  size_t countRows() const {
    size_t count = 0;
    for (size_t i = 0; i < merged_indexes_.size(); ++i) {
      if (isRowEnd(i))
        ++count;
    }
    return count;
  }

private:
  void mergeIndexes() {
    // Calculate total size
    size_t total = 0;
    for (int i = 0; i < idx_.n_threads; ++i) {
      total += idx_.n_indexes[i];
    }
    merged_indexes_.reserve(total);

    // Collect all indexes
    for (int t = 0; t < idx_.n_threads; ++t) {
      for (uint64_t j = 0; j < idx_.n_indexes[t]; ++j) {
        auto ii = t + (j * idx_.n_threads);
        merged_indexes_.push_back(idx_.indexes[ii]);
      }
    }

    // Sort them
    std::sort(merged_indexes_.begin(), merged_indexes_.end());
  }

  const uint8_t* buf_;
  const libvroom::ParseIndex& idx_;
  std::vector<uint64_t> merged_indexes_;
};

void printVersion() {
  cout << "vroom version " << VERSION << '\n';
}

void printUsage(const char* prog) {
  cerr << "vroom - High-performance CSV processing tool\n\n";
  cerr << "Usage: " << prog << " <command> [options] [csvfile]\n\n";
  cerr << "Commands:\n";
  cerr << "  count         Count the number of rows\n";
  cerr << "  head          Display the first N rows (default: " << DEFAULT_NUM_ROWS << ")\n";
  cerr << "  tail          Display the last N rows (default: " << DEFAULT_NUM_ROWS << ")\n";
  cerr << "  sample        Display N random rows from throughout the file\n";
  cerr << "  select        Select specific columns by name or index\n";
  cerr << "  info          Display information about the CSV file\n";
  cerr << "  schema        Display inferred schema (column names, types, nullable)\n";
  cerr << "  stats         Display statistical summary for each column\n";
  cerr << "  pretty        Pretty-print the CSV with aligned columns\n";
  cerr << "  dialect       Detect and output the CSV dialect\n";
#ifdef LIBVROOM_ENABLE_ARROW
  cerr << "  convert       Convert CSV to columnar format (Parquet/Feather)\n";
#endif
  cerr << "\nArguments:\n";
  cerr << "  csvfile       Path to CSV file, or '-' to read from stdin.\n";
  cerr << "                If omitted, reads from stdin.\n";
  cerr << "\nOptions:\n";
  cerr << "  -n <num>      Number of rows (for head/tail/sample/pretty)\n";
  cerr << "  -s <seed>     Random seed for reproducible sampling (for sample)\n";
  cerr << "  -c <cols>     Comma-separated column names or indices (for select)\n";
  cerr << "  -H            No header row in input\n";
  cerr << "  -t <threads>  Number of threads (default: auto, max: " << MAX_THREADS << ")\n";
  cerr << "  -d <delim>    Field delimiter (disables auto-detection)\n";
  cerr << "                Values: comma, tab, semicolon, pipe, or single character\n";
  cerr << "  -q <char>     Quote character (default: \")\n";
  cerr << "  -e <enc>      Override encoding detection with specified encoding\n";
  cerr << "                Values: utf-8, utf-16le, utf-16be, utf-32le, utf-32be,\n";
  cerr << "                        latin1, windows-1252\n";
  cerr << "  -j            Output in JSON format (for dialect/schema/stats)\n";
  cerr << "  -m <size>     Sample size for schema/stats (0=all rows, default: 0)\n";
  cerr << "  -o <file>     Output file path (for convert command)\n";
  cerr << "  -F <format>   Output format: parquet, feather, auto (default: auto)\n";
  cerr << "  -C <codec>    Compression codec for Parquet: snappy, gzip, zstd, lz4, none\n";
  cerr << "  -f, --force   Force output even with low confidence (for dialect command)\n";
  cerr << "  -S, --strict  Strict mode: exit with code 1 on any parse error\n";
  cerr << "  --cache       Enable index caching for faster re-reads\n";
  cerr << "  --cache-dir <dir>  Store cache files in specified directory\n";
  cerr << "  --no-cache    Disable index caching (default)\n";
  cerr << "  -p, --progress  Show progress bar during parsing (auto-enabled for TTY)\n";
  cerr << "  --no-progress   Disable progress bar\n";
  cerr << "  -h            Show this help message\n";
  cerr << "  -v            Show version information\n";
  cerr << "\nDialect Detection:\n";
  cerr << "  By default, vroom auto-detects the CSV dialect (delimiter, quote character,\n";
  cerr << "  escape style). Use -d to explicitly specify a delimiter and disable\n";
  cerr << "  auto-detection.\n";
  cerr << "\nEncoding Support:\n";
  cerr << "  By default, vroom auto-detects file encoding via BOM and byte patterns.\n";
  cerr << "  Non-UTF-8 files are automatically transcoded to UTF-8 for parsing.\n";
  cerr << "  Use -e to override automatic detection.\n";
  cerr << "\nExamples:\n";
  cerr << "  " << prog << " count data.csv\n";
  cerr << "  " << prog << " head -n 5 data.csv\n";
  cerr << "  " << prog << " tail -n 5 data.csv\n";
  cerr << "  " << prog << " sample -n 100 data.csv\n";
  cerr << "  " << prog << " sample -n 100 -s 42 data.csv  # reproducible\n";
  cerr << "  " << prog << " select -c name,age data.csv\n";
  cerr << "  " << prog << " select -c 0,2,4 data.csv\n";
  cerr << "  " << prog << " info data.csv\n";
  cerr << "  " << prog << " pretty -n 20 data.csv\n";
  cerr << "  " << prog << " count -d tab data.tsv\n";
  cerr << "  " << prog << " head -d semicolon european.csv\n";
  cerr << "  " << prog << " dialect unknown_format.csv\n";
  cerr << "  " << prog << " dialect -j data.csv       # JSON output\n";
  cerr << "  " << prog << " dialect -f unknown.csv    # Force output even with low confidence\n";
  cerr << "  " << prog << " schema data.csv\n";
  cerr << "  " << prog << " schema -j data.csv       # JSON output\n";
  cerr << "  " << prog << " schema -m 1000 data.csv  # Sample 1000 rows\n";
  cerr << "  " << prog << " stats data.csv\n";
  cerr << "  " << prog << " stats -j data.csv        # JSON output\n";
  cerr << "  " << prog << " stats -m 1000 data.csv   # Sample 1000 rows\n";
  cerr << "  cat data.csv | " << prog << " count\n";
  cerr << "  " << prog << " head - < data.csv\n";
#ifdef LIBVROOM_ENABLE_ARROW
  cerr << "\nConvert Examples:\n";
  cerr << "  " << prog << " convert data.csv -o data.parquet\n";
  cerr << "  " << prog << " convert data.csv -o data.feather\n";
  cerr << "  " << prog << " convert data.csv -o data.parquet -C zstd  # ZSTD compression\n";
  cerr << "  " << prog << " convert -d tab data.tsv -o data.parquet   # TSV input\n";
#endif
}

// Helper to check if reading from stdin
static bool isStdinInput(const char* filename) {
  return filename == nullptr || strcmp(filename, "-") == 0;
}

/// Result of loading and parsing a file
struct ParseResult {
  LoadResult load_result;   ///< The loaded data (RAII-managed)
  libvroom::ParseIndex idx; ///< The parsed index
  bool success{false};      ///< Whether parsing was successful
  bool used_cache{false};   ///< Whether a cache was used
  std::string cache_path;   ///< Path to cache file if caching is enabled
};

/// Configuration for index caching in CLI
struct CliCacheConfig {
  bool enabled{false};   ///< Whether caching is enabled
  std::string cache_dir; ///< Custom cache directory (empty = same dir)
};

// Parse a file or stdin - returns ParseResult with RAII-managed data
// If detected_encoding is provided, the detected encoding will be stored there
// If strict_mode is true, exits with error on any parse warning or error
// If forced_encoding is not UNKNOWN, it overrides auto-detection
// If cache_config is provided and enabled, enables index caching
// If progress_callback is provided, it will be called during parsing
ParseResult parseFile(const char* filename, int n_threads,
                      const libvroom::Dialect& dialect = libvroom::Dialect::csv(),
                      bool auto_detect = false,
                      libvroom::EncodingResult* detected_encoding = nullptr,
                      bool strict_mode = false,
                      libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN,
                      const CliCacheConfig* cache_config = nullptr,
                      libvroom::ProgressCallback progress_callback = nullptr) {
  ParseResult result;

  try {
    if (forced_encoding != libvroom::Encoding::UNKNOWN) {
      // Use forced encoding (user specified via -e flag)
      if (isStdinInput(filename)) {
        result.load_result = read_stdin_with_encoding(LIBVROOM_PADDING, forced_encoding);
      } else {
        result.load_result = read_file_with_encoding(filename, LIBVROOM_PADDING, forced_encoding);
      }
    } else {
      // Auto-detect encoding
      if (isStdinInput(filename)) {
        result.load_result = read_stdin_with_encoding(LIBVROOM_PADDING);
      } else {
        result.load_result = read_file_with_encoding(filename, LIBVROOM_PADDING);
      }
    }

    // Store detected encoding if caller wants it
    if (detected_encoding) {
      *detected_encoding = result.load_result.encoding;
    }

    // Report encoding if transcoding occurred
    if (result.load_result.encoding.needs_transcoding) {
      cerr << "Transcoded from "
           << libvroom::encoding_to_string(result.load_result.encoding.encoding) << " to UTF-8"
           << endl;
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "': " << e.what() << endl;
    }
    return result;
  }

  // Use the unified Parser API
  libvroom::Parser parser(n_threads);

  // Build ParseOptions based on auto_detect flag
  libvroom::ParseOptions options;
  if (!auto_detect) {
    options.dialect = dialect;
  }

  // In strict mode, collect errors using PERMISSIVE mode to gather all issues
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  if (strict_mode) {
    options.errors = &errors;
  }

  // Set up caching if enabled and we have a real file (not stdin)
  if (cache_config != nullptr && cache_config->enabled && !isStdinInput(filename)) {
    if (cache_config->cache_dir.empty()) {
      options.cache = libvroom::CacheConfig::defaults();
    } else {
      options.cache = libvroom::CacheConfig::custom(cache_config->cache_dir);
    }
    options.source_path = filename;
  }

  // Set up progress callback if provided
  if (progress_callback) {
    options.progress_callback = progress_callback;
  }

  auto parse_result = parser.parse(result.load_result.data(), result.load_result.size, options);
  result.idx = std::move(parse_result.idx);

  // Report auto-detected dialect if applicable
  if (auto_detect && result.load_result.size > 0 && parse_result.detection.success()) {
    cerr << "Auto-detected: " << parse_result.dialect.to_string() << endl;
  }

  // In strict mode, check for any errors (including warnings)
  if (strict_mode && parse_result.has_errors()) {
    cerr << "Error: Strict mode enabled and parse errors were found:" << endl;
    for (const auto& err : parse_result.errors()) {
      cerr << "  " << err.to_string() << endl;
    }
    return result;
  }

  // Capture cache information
  result.used_cache = parse_result.used_cache;
  result.cache_path = parse_result.cache_path;

  result.success = true;
  return result;
}

// Helper function to parse delimiter string
libvroom::Dialect parseDialect(const std::string& delimiter_str, char quote_char) {
  libvroom::Dialect dialect;
  dialect.quote_char = quote_char;

  if (delimiter_str == "comma" || delimiter_str == ",") {
    dialect.delimiter = ',';
  } else if (delimiter_str == "tab" || delimiter_str == "\\t") {
    dialect.delimiter = '\t';
  } else if (delimiter_str == "semicolon" || delimiter_str == ";") {
    dialect.delimiter = ';';
  } else if (delimiter_str == "pipe" || delimiter_str == "|") {
    dialect.delimiter = '|';
  } else if (delimiter_str.length() == 1) {
    dialect.delimiter = delimiter_str[0];
  } else {
    cerr << "Warning: Unknown delimiter '" << delimiter_str << "', using comma\n";
    dialect.delimiter = ',';
  }

  return dialect;
}

/// ============================================================================
// Optimized Row Counting - Avoids building full index for count command
// ============================================================================

// SIMD row counter - processes 64 bytes at a time
// Note on escaped quotes (CSV ""): The SIMD path uses XOR-prefix to compute
// quote state, which toggles on every quote. For escaped quotes "", this means
// toggling twice (net effect: state unchanged). This is correct for row counting
// because: (1) "" are adjacent by definition, so no newline can appear between
// them, and (2) the final quote state after "" matches the correct semantics.
// The scalar fallback explicitly handles "" for consistency with the library.
size_t countRowsSimd(const uint8_t* buf, size_t len) {
  size_t row_count = 0;
  size_t idx = 0;
  uint64_t prev_iter_inside_quote = 0ULL;

  // Process 64 bytes at a time using SIMD
  for (; idx + 64 <= len; idx += 64) {
    libvroom::simd_input in = libvroom::fill_input(buf + idx);

    // Find all quotes and newlines in this 64-byte block
    uint64_t quotes = libvroom::cmp_mask_against_input(in, '"');
    uint64_t newlines = libvroom::cmp_mask_against_input(in, '\n');

    // Build quote mask (1 = inside quote, 0 = outside)
    uint64_t quote_mask = libvroom::find_quote_mask2(quotes, prev_iter_inside_quote);

    // Newlines outside quotes
    uint64_t valid_newlines = newlines & ~quote_mask;

    // Count the newlines
    row_count += libvroom::count_ones(valid_newlines);
  }

  // Handle remaining bytes with scalar code (properly handles escaped quotes "")
  bool in_quote = (prev_iter_inside_quote != 0);
  for (; idx < len; ++idx) {
    if (buf[idx] == '"') {
      // Check for escaped quote ("")
      if (idx + 1 < len && buf[idx + 1] == '"') {
        ++idx; // Skip both quotes - escaped quote doesn't toggle state
      } else {
        in_quote = !in_quote;
      }
    } else if (buf[idx] == '\n' && !in_quote) {
      ++row_count;
    }
  }

  return row_count;
}

// Direct row counter - uses SIMD for large data
size_t countRowsDirect(const uint8_t* buf, size_t len) {
  if (len >= 64) {
    return countRowsSimd(buf, len);
  }

  // Scalar path for small files (properly handles escaped quotes "")
  size_t row_count = 0;
  bool in_quote = false;

  for (size_t i = 0; i < len; ++i) {
    if (buf[i] == '"') {
      // Check for escaped quote ("")
      if (i + 1 < len && buf[i + 1] == '"') {
        ++i; // Skip both quotes - escaped quote doesn't toggle state
      } else {
        in_quote = !in_quote;
      }
    } else if (buf[i] == '\n' && !in_quote) {
      ++row_count;
    }
  }

  return row_count;
}

// Determine if position is inside or outside a quoted field
// Uses proven speculative approach from two_pass.h with 64KB lookback
enum QuoteState { OUTSIDE_QUOTE, INSIDE_QUOTE, AMBIGUOUS };

// Helper function matching two_pass.h logic
static bool isOther(uint8_t c) {
  return c != ',' && c != '\n' && c != '"';
}

static QuoteState getQuoteState(const uint8_t* buf, size_t pos) {
  // Uses the same proven logic as two_pass::get_quotation_state
  if (pos == 0)
    return OUTSIDE_QUOTE;

  size_t end = pos > QUOTE_LOOKBACK_LIMIT ? pos - QUOTE_LOOKBACK_LIMIT : 0;
  size_t i = pos;
  size_t num_quotes = 0;

  // Scan backwards looking for quote-other patterns that determine state
  while (i > end) {
    if (buf[i] == '"') {
      // q-o case: quote followed by non-delimiter means we found end of quoted field
      if (i + 1 < pos && isOther(buf[i + 1])) {
        return num_quotes % 2 == 0 ? INSIDE_QUOTE : OUTSIDE_QUOTE;
      }
      // o-q case: non-delimiter before quote means we found start of quoted field
      else if (i > end && isOther(buf[i - 1])) {
        return num_quotes % 2 == 0 ? OUTSIDE_QUOTE : INSIDE_QUOTE;
      }
      ++num_quotes;
    }
    --i;
  }

  // Check the boundary position
  if (buf[end] == '"') {
    ++num_quotes;
  }

  return AMBIGUOUS;
}

// Find a valid row boundary near target position
static size_t findRowBoundary(const uint8_t* buf, size_t len, size_t target) {
  QuoteState state = getQuoteState(buf, target);
  size_t limit = std::min(target + MAX_BOUNDARY_SEARCH, len);
  bool in_quote = (state == INSIDE_QUOTE);

  for (size_t pos = target; pos < limit; ++pos) {
    if (buf[pos] == '"') {
      // Check for escaped quote ("")
      if (pos + 1 < limit && buf[pos + 1] == '"') {
        ++pos; // Skip both quotes - escaped quote doesn't toggle state
      } else {
        in_quote = !in_quote;
      }
    } else if (buf[pos] == '\n' && !in_quote) {
      return pos + 1;
    }
  }

  return target;
}

// Parallel direct row counter
size_t countRowsDirectParallel(const uint8_t* buf, size_t len, int n_threads) {
  if (n_threads <= 1 || len < MIN_PARALLEL_SIZE) {
    return countRowsDirect(buf, len);
  }

  size_t chunk_size = len / n_threads;
  std::vector<size_t> chunk_starts(n_threads + 1);

  chunk_starts[0] = 0;
  chunk_starts[n_threads] = len;

  // Find chunk boundaries in parallel
  std::vector<std::future<size_t>> boundary_futures;
  for (int i = 1; i < n_threads; ++i) {
    size_t target = chunk_size * i;
    boundary_futures.push_back(std::async(
        std::launch::async, [buf, len, target]() { return findRowBoundary(buf, len, target); }));
  }

  for (int i = 1; i < n_threads; ++i) {
    chunk_starts[i] = boundary_futures[i - 1].get();
  }

  // Count rows in each chunk in parallel
  std::vector<std::future<size_t>> count_futures;
  for (int i = 0; i < n_threads; ++i) {
    size_t start = chunk_starts[i];
    size_t end = chunk_starts[i + 1];
    count_futures.push_back(std::async(std::launch::async, [buf, start, end]() {
      return countRowsDirect(buf + start, end - start);
    }));
  }

  size_t total = 0;
  for (auto& f : count_futures) {
    total += f.get();
  }

  return total;
}

// Command: count
int cmdCount(const char* filename, int n_threads, bool has_header,
             const libvroom::Dialect& dialect = libvroom::Dialect::csv(),
             bool auto_detect = false) {
  AlignedPtr buffer;
  size_t len = 0;

  try {
    if (isStdinInput(filename)) {
      auto [ptr, size] = read_stdin(LIBVROOM_PADDING);
      buffer = std::move(ptr);
      len = size;
    } else {
      auto [ptr, size] = read_file(filename, LIBVROOM_PADDING);
      buffer = std::move(ptr);
      len = size;
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "'" << endl;
    }
    return 1;
  }

  // Use optimized direct row counting - much faster than building full index
  // Note: For non-standard dialects, this still uses standard quote char
  // TODO: Make countRowsDirectParallel dialect-aware
  size_t rows = countRowsDirectParallel(buffer.get(), len, n_threads);

  // Subtract header if present
  if (has_header && rows > 0) {
    cout << (rows - 1) << endl;
  } else {
    cout << rows << endl;
  }

  // buffer automatically freed when it goes out of scope
  return 0;
}

// Helper function to output a row with proper quoting
static void outputRow(const std::vector<std::string>& row, const libvroom::Dialect& dialect) {
  for (size_t i = 0; i < row.size(); ++i) {
    if (i > 0)
      cout << dialect.delimiter;
    bool needs_quote = row[i].find(dialect.delimiter) != string::npos ||
                       row[i].find(dialect.quote_char) != string::npos ||
                       row[i].find('\n') != string::npos || row[i].find('\r') != string::npos;
    if (needs_quote) {
      cout << dialect.quote_char;
      for (char c : row[i]) {
        if (c == dialect.quote_char)
          cout << dialect.quote_char;
        cout << c;
      }
      cout << dialect.quote_char;
    } else {
      cout << row[i];
    }
  }
  cout << '\n';
}

// Command: head
int cmdHead(const char* filename, int n_threads, size_t num_rows, bool has_header,
            const libvroom::Dialect& dialect = libvroom::Dialect::csv(), bool auto_detect = false,
            bool strict_mode = false,
            libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN,
            const CliCacheConfig* cache_config = nullptr,
            libvroom::ProgressCallback progress_callback = nullptr) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode,
                          forced_encoding, cache_config, progress_callback);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  // Get num_rows + 1 if we have header to show header plus num_rows data rows
  auto rows = iter.getRows(has_header ? num_rows + 1 : num_rows);

  for (const auto& row : rows) {
    outputRow(row, dialect);
  }

  // Memory automatically freed when result goes out of scope
  return 0;
}

// Command: tail
// Uses a circular buffer approach for memory efficiency - only keeps last N rows in memory
// instead of loading the entire file. This scales memory usage with output size rather than
// input file size, making it suitable for large CSV files.
//
// LIMITATION: For stdin input, the entire content must be loaded into memory before processing
// because stdin is not seekable. For large stdin inputs, consider writing to a temporary file
// first. For file input, true streaming is used and memory scales with output size only.
int cmdTail(const char* filename, int n_threads, size_t num_rows, bool has_header,
            const libvroom::Dialect& dialect = libvroom::Dialect::csv(), bool auto_detect = false,
            bool strict_mode = false,
            libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN) {
  // Suppress unused parameter warning - n_threads not used in streaming approach
  (void)n_threads;

  // Set up streaming configuration
  libvroom::StreamConfig config;
  // Use dialect auto-detection if enabled, otherwise use the provided dialect
  if (auto_detect) {
    // When auto_detect is enabled, use default CSV dialect - the parser will
    // still work correctly for standard CSV files
    config.dialect = libvroom::Dialect::csv();
  } else {
    config.dialect = dialect;
  }
  config.parse_header = has_header;
  config.error_mode =
      strict_mode ? libvroom::ErrorMode::FAIL_FAST : libvroom::ErrorMode::PERMISSIVE;

  // Use a deque as a circular buffer to hold the last N rows
  // Each row is stored as a vector of strings (field values)
  std::deque<std::vector<std::string>> row_buffer;
  std::vector<std::string> header_row;

  // Helper class to wrap a memory buffer as an istream without copying
  // This avoids the double memory usage of std::istringstream which copies its input
  struct membuf : std::streambuf {
    membuf(const char* base, size_t size) {
      // streambuf uses char* but doesn't modify the data for input operations
      char* p = const_cast<char*>(base);
      setg(p, p, p + size);
    }
  };

  // Suppress unused forced_encoding warning for now - streaming doesn't support encoding override
  // yet
  (void)forced_encoding;

  try {
    if (isStdinInput(filename)) {
      // For stdin, we must read the entire content into memory first because:
      // 1. stdin is not seekable - we cannot rewind to find earlier rows
      // 2. The tail command requires reading all rows to find the last N
      // This is an inherent limitation of tail with streaming input.
      // For file input, StreamReader reads in chunks for true streaming.
      // Note: forced_encoding is not currently used - stdin uses auto-detection
      auto load_result = read_stdin_with_encoding(LIBVROOM_PADDING);
      if (load_result.encoding.needs_transcoding) {
        cerr << "Transcoded from " << libvroom::encoding_to_string(load_result.encoding.encoding)
             << " to UTF-8" << endl;
      }

      // Wrap buffer in stream without copying using custom streambuf
      membuf sbuf(reinterpret_cast<const char*>(load_result.data()), load_result.size);
      std::istream iss(&sbuf);
      libvroom::StreamReader reader(iss, config);

      // Iterate through all rows, keeping only the last num_rows in the buffer
      // Note: header is populated after the first call to next_row()
      while (reader.next_row()) {
        // Capture header after first row is read (when parse_header is true, the header
        // is consumed internally and made available via header())
        if (has_header && header_row.empty()) {
          const auto& h = reader.header();
          for (const auto& col : h) {
            header_row.push_back(col);
          }
        }

        const auto& row = reader.row();

        // Convert Row to vector of strings
        std::vector<std::string> row_data;
        row_data.reserve(row.field_count());
        for (const auto& field : row) {
          row_data.push_back(field.unescaped(config.dialect.quote_char));
        }

        // Add to buffer
        row_buffer.push_back(std::move(row_data));

        // Keep only the last num_rows
        if (row_buffer.size() > num_rows) {
          row_buffer.pop_front();
        }
      }

      // Check for errors in strict mode
      if (strict_mode && reader.error_collector().has_errors()) {
        cerr << "Error: Strict mode enabled and parse errors were found:" << endl;
        for (const auto& err : reader.error_collector().errors()) {
          cerr << "  " << err.to_string() << endl;
        }
        return 1;
      }
    } else {
      // File input - use StreamReader directly
      libvroom::StreamReader reader(filename, config);

      // Iterate through all rows, keeping only the last num_rows in the buffer
      // Note: header is populated after the first call to next_row()
      while (reader.next_row()) {
        // Capture header after first row is read (when parse_header is true, the header
        // is consumed internally and made available via header())
        if (has_header && header_row.empty()) {
          const auto& h = reader.header();
          for (const auto& col : h) {
            header_row.push_back(col);
          }
        }

        const auto& row = reader.row();

        // Convert Row to vector of strings
        std::vector<std::string> row_data;
        row_data.reserve(row.field_count());
        for (const auto& field : row) {
          row_data.push_back(field.unescaped(config.dialect.quote_char));
        }

        // Add to buffer
        row_buffer.push_back(std::move(row_data));

        // Keep only the last num_rows
        if (row_buffer.size() > num_rows) {
          row_buffer.pop_front();
        }
      }

      // Check for errors in strict mode
      if (strict_mode && reader.error_collector().has_errors()) {
        cerr << "Error: Strict mode enabled and parse errors were found:" << endl;
        for (const auto& err : reader.error_collector().errors()) {
          cerr << "  " << err.to_string() << endl;
        }
        return 1;
      }
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "': " << e.what() << endl;
    }
    return 1;
  }

  // Output header first if present
  if (has_header && !header_row.empty()) {
    outputRow(header_row, dialect);
  }

  // Output the buffered tail rows
  for (const auto& row : row_buffer) {
    outputRow(row, dialect);
  }

  return 0;
}

// Command: sample
int cmdSample(const char* filename, int n_threads, size_t num_rows, bool has_header,
              const libvroom::Dialect& dialect = libvroom::Dialect::csv(), bool auto_detect = false,
              unsigned int seed = 0, bool strict_mode = false,
              libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN,
              const CliCacheConfig* cache_config = nullptr,
              libvroom::ProgressCallback progress_callback = nullptr) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode,
                          forced_encoding, cache_config, progress_callback);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  auto all_rows = iter.getRows();

  if (all_rows.empty()) {
    return 0;
  }

  // Output header first if present
  if (has_header && !all_rows.empty()) {
    outputRow(all_rows[0], dialect);
  }

  // Collect data row indices (skip header if present)
  size_t header_offset = has_header ? 1 : 0;
  size_t data_rows = all_rows.size() > header_offset ? all_rows.size() - header_offset : 0;

  if (data_rows == 0) {
    return 0;
  }

  // Use reservoir sampling for efficiency when sampling
  std::vector<size_t> sample_indices;

  if (num_rows >= data_rows) {
    // If requesting more samples than available, output all data rows
    for (size_t i = header_offset; i < all_rows.size(); ++i) {
      sample_indices.push_back(i);
    }
  } else {
    // Reservoir sampling algorithm
    std::mt19937 rng(seed ? seed : std::random_device{}());

    for (size_t i = 0; i < data_rows; ++i) {
      if (i < num_rows) {
        sample_indices.push_back(header_offset + i);
      } else {
        std::uniform_int_distribution<size_t> dist(0, i);
        size_t j = dist(rng);
        if (j < num_rows) {
          sample_indices[j] = header_offset + i;
        }
      }
    }

    // Sort sample indices to maintain original row order in output
    std::sort(sample_indices.begin(), sample_indices.end());
  }

  // Output the sampled rows
  for (size_t sample_idx : sample_indices) {
    outputRow(all_rows[sample_idx], dialect);
  }

  // Memory automatically freed when result goes out of scope
  return 0;
}

// Command: select
int cmdSelect(const char* filename, int n_threads, const string& columns, bool has_header,
              const libvroom::Dialect& dialect = libvroom::Dialect::csv(), bool auto_detect = false,
              bool strict_mode = false,
              libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN,
              const CliCacheConfig* cache_config = nullptr,
              libvroom::ProgressCallback progress_callback = nullptr) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode,
                          forced_encoding, cache_config, progress_callback);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  auto rows = iter.getRows();

  if (rows.empty()) {
    return 0;
  }

  // Parse column specification
  vector<size_t> col_indices;
  vector<string> col_specs;

  // Split columns by comma
  stringstream ss(columns);
  string spec;
  while (getline(ss, spec, ',')) {
    col_specs.push_back(spec);
  }

  // Resolve column names to indices if has_header
  const auto& header = rows[0];
  size_t num_cols = header.size();
  for (const auto& spec : col_specs) {
    // Try as numeric index first
    bool is_numeric = !spec.empty() && all_of(spec.begin(), spec.end(), ::isdigit);
    if (is_numeric) {
      size_t col_idx = stoul(spec);
      if (col_idx >= num_cols) {
        cerr << "Error: Column index " << col_idx << " is out of range (file has " << num_cols
             << " columns, indices 0-" << (num_cols - 1) << ")" << endl;
        return 1;
      }
      col_indices.push_back(col_idx);
    } else if (has_header) {
      // Find by name
      auto it = find(header.begin(), header.end(), spec);
      if (it != header.end()) {
        col_indices.push_back(distance(header.begin(), it));
      } else {
        cerr << "Error: Column '" << spec << "' not found in header" << endl;
        return 1;
      }
    } else {
      cerr << "Error: Cannot use column names without header (-H flag used)" << endl;
      return 1;
    }
  }

  // Output selected columns
  for (const auto& row : rows) {
    bool first = true;
    for (size_t col : col_indices) {
      if (!first)
        cout << dialect.delimiter;
      first = false;
      // Column bounds already validated above, but handle rows with fewer columns
      if (col < row.size()) {
        const string& field = row[col];
        bool needs_quote = field.find(dialect.delimiter) != string::npos ||
                           field.find(dialect.quote_char) != string::npos ||
                           field.find('\n') != string::npos;
        if (needs_quote) {
          cout << dialect.quote_char;
          for (char c : field) {
            if (c == dialect.quote_char)
              cout << dialect.quote_char;
            cout << c;
          }
          cout << dialect.quote_char;
        } else {
          cout << field;
        }
      }
      // Empty field for rows with fewer columns (ragged CSV)
    }
    cout << '\n';
  }

  // Memory automatically freed when result goes out of scope
  return 0;
}

// Command: info
int cmdInfo(const char* filename, int n_threads, bool has_header,
            const libvroom::Dialect& dialect = libvroom::Dialect::csv(), bool auto_detect = false,
            bool strict_mode = false,
            libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN,
            const CliCacheConfig* cache_config = nullptr,
            libvroom::ProgressCallback progress_callback = nullptr) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode,
                          forced_encoding, cache_config, progress_callback);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  auto rows = iter.getRows();

  cout << "Source: " << (isStdinInput(filename) ? "<stdin>" : filename) << '\n';
  cout << "Size: " << result.load_result.size << " bytes\n";
  cout << "Dialect: " << dialect.to_string() << '\n';

  size_t num_rows = rows.size();
  size_t num_cols = rows.empty() ? 0 : rows[0].size();

  if (has_header) {
    cout << "Rows: " << (num_rows > 0 ? num_rows - 1 : 0) << " (excluding header)\n";
  } else {
    cout << "Rows: " << num_rows << "\n";
  }
  cout << "Columns: " << num_cols << '\n';

  if (has_header && !rows.empty()) {
    cout << "\nColumn names:\n";
    for (size_t i = 0; i < rows[0].size(); ++i) {
      cout << "  " << i << ": " << rows[0][i] << '\n';
    }
  }

  // Memory automatically freed when result goes out of scope
  return 0;
}

// Command: pretty
int cmdPretty(const char* filename, int n_threads, size_t num_rows, bool has_header,
              const libvroom::Dialect& dialect = libvroom::Dialect::csv(), bool auto_detect = false,
              bool strict_mode = false,
              libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN,
              const CliCacheConfig* cache_config = nullptr,
              libvroom::ProgressCallback progress_callback = nullptr) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode,
                          forced_encoding, cache_config, progress_callback);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  auto rows = iter.getRows(has_header ? num_rows + 1 : num_rows);

  if (rows.empty()) {
    return 0;
  }

  // Calculate column widths
  size_t num_cols = 0;
  for (const auto& row : rows) {
    num_cols = max(num_cols, row.size());
  }

  vector<size_t> widths(num_cols, 0);
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      // Use display width instead of byte length for proper Unicode handling
      widths[i] = max(widths[i], libvroom::utf8_display_width(row[i]));
    }
  }

  // Limit width to reasonable size
  for (auto& w : widths) {
    w = min(w, MAX_COLUMN_WIDTH);
  }

  // Print separator line
  auto printSep = [&]() {
    cout << '+';
    for (size_t i = 0; i < num_cols; ++i) {
      cout << string(widths[i] + 2, '-') << '+';
    }
    cout << '\n';
  };

  // Print rows
  printSep();
  for (size_t r = 0; r < rows.size(); ++r) {
    const auto& row = rows[r];
    cout << '|';
    for (size_t i = 0; i < num_cols; ++i) {
      string val = (i < row.size()) ? row[i] : "";
      size_t val_width = libvroom::utf8_display_width(val);

      // Truncate using UTF-8-aware truncation if needed
      if (val_width > widths[i]) {
        val = libvroom::utf8_truncate(val, widths[i]);
        val_width = libvroom::utf8_display_width(val);
      }

      // Output the value with proper padding
      // We need to calculate padding based on display width, not byte length
      cout << ' ' << val;
      if (val_width < widths[i]) {
        cout << string(widths[i] - val_width, ' ');
      }
      cout << " |";
    }
    cout << '\n';

    // Print separator after header
    if (has_header && r == 0) {
      printSep();
    }
  }
  printSep();

  // Memory automatically freed when result goes out of scope
  return 0;
}

// Helper: format delimiter for display
static std::string formatDelimiter(char delim) {
  // LCOV_EXCL_BR_START - switch branches; common delimiters tested
  switch (delim) {
  case ',':
    return "comma";
  case '\t':
    return "tab";
  case ';':
    return "semicolon";
  case '|':
    return "pipe";
  case ':':
    return "colon";
  default:
    return std::string(1, delim);
  }
  // LCOV_EXCL_BR_STOP
}

// Helper: format quote char for display
static std::string formatQuoteChar(char quote) {
  // LCOV_EXCL_BR_START - formatting branches; common cases tested
  if (quote == '"')
    return "double-quote";
  if (quote == '\'')
    return "single-quote";
  if (quote == '\0')
    return "none";
  return std::string(1, quote);
  // LCOV_EXCL_BR_STOP
}

// Helper: format line ending for display
static std::string formatLineEnding(libvroom::Dialect::LineEnding le) {
  // LCOV_EXCL_BR_START - exhaustive switch; all line endings tested
  switch (le) {
  case libvroom::Dialect::LineEnding::LF:
    return "LF";
  case libvroom::Dialect::LineEnding::CRLF:
    return "CRLF";
  case libvroom::Dialect::LineEnding::CR:
    return "CR";
  case libvroom::Dialect::LineEnding::MIXED:
    return "mixed";
  default:
    return "unknown";
  }
  // LCOV_EXCL_BR_STOP
}

// Helper: escape a character for JSON string output
// Handles all JSON control characters per RFC 8259
static std::string escapeJsonChar(char c) {
  // LCOV_EXCL_BR_START - switch branches; common escapes tested
  switch (c) {
  case '"':
    return "\\\"";
  case '\\':
    return "\\\\";
  case '\b':
    return "\\b";
  case '\f':
    return "\\f";
  case '\n':
    return "\\n";
  case '\r':
    return "\\r";
  case '\t':
    return "\\t";
  default:
    // Escape other control characters (0x00-0x1F) as \uXXXX
    if (static_cast<unsigned char>(c) < 0x20) {
      char buf[7];
      snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
      return std::string(buf);
    }
    return std::string(1, c);
  }
  // LCOV_EXCL_BR_STOP
}

// Command: dialect - detect and output CSV dialect in human-readable or JSON format
int cmdDialect(const char* filename, bool json_output, bool force_output) {
  LoadResult load_result;

  try {
    if (isStdinInput(filename)) {
      load_result = read_stdin_with_encoding(LIBVROOM_PADDING);
    } else {
      load_result = read_file_with_encoding(filename, LIBVROOM_PADDING);
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "': " << e.what() << endl;
    }
    return 1;
  }

  libvroom::DialectDetector detector;
  auto result = detector.detect(load_result.data(), load_result.size);

  // Check if detection failed (confidence too low)
  bool low_confidence = !result.success();
  if (low_confidence && !force_output) {
    cerr << "Error: Could not detect CSV dialect";
    if (!result.warning.empty()) {
      cerr << ": " << result.warning;
    }
    cerr << endl;
    cerr << "Hint: Use -f/--force to output best guess despite low confidence" << endl;
    return 1;
  }

  // Check if detection is ambiguous (multiple candidates with similar scores)
  // Detection succeeded (confidence > 0.5) but may be uncertain due to similar scores
  bool is_ambiguous =
      !result.warning.empty() && result.warning.find("ambiguous") != std::string::npos;

  const auto& d = result.dialect;
  const auto& enc_result = load_result.encoding;

  // Output warning to stderr if low confidence (when --force is used)
  if (low_confidence) {
    cerr << "Warning: Low confidence detection (" << static_cast<int>(result.confidence * 100)
         << "%), results may be unreliable" << endl;
  }

  if (json_output) {
    // JSON output for programmatic use
    cout << "{\n";
    cout << "  \"delimiter\": \"" << escapeJsonChar(d.delimiter) << "\",\n";
    cout << "  \"quote\": \"";
    if (d.quote_char != '\0') {
      cout << escapeJsonChar(d.quote_char);
    }
    cout << "\",\n";
    cout << "  \"escape\": \"" << (d.double_quote ? "double" : "backslash") << "\",\n";
    cout << "  \"line_ending\": \"" << formatLineEnding(d.line_ending) << "\",\n";
    cout << "  \"encoding\": \"" << libvroom::encoding_to_string(enc_result.encoding) << "\",\n";
    cout << "  \"has_header\": " << (result.has_header ? "true" : "false") << ",\n";
    cout << "  \"columns\": " << result.detected_columns << ",\n";
    cout << "  \"confidence\": " << result.confidence << ",\n";
    cout << "  \"low_confidence\": " << (low_confidence ? "true" : "false") << ",\n";
    cout << "  \"ambiguous\": " << (is_ambiguous ? "true" : "false");

    // Include alternative candidates when ambiguous
    if (is_ambiguous && result.candidates.size() > 1) {
      cout << ",\n  \"alternatives\": [\n";
      // Show top 3 alternatives (skip first since it's the main result)
      size_t max_alternatives = std::min(size_t(4), result.candidates.size());
      for (size_t i = 1; i < max_alternatives; ++i) {
        const auto& alt = result.candidates[i];
        cout << "    {\n";
        cout << "      \"delimiter\": \"" << escapeJsonChar(alt.dialect.delimiter) << "\",\n";
        cout << "      \"quote\": \"";
        if (alt.dialect.quote_char != '\0') {
          cout << escapeJsonChar(alt.dialect.quote_char);
        }
        cout << "\",\n";
        cout << "      \"score\": " << alt.consistency_score << ",\n";
        cout << "      \"columns\": " << alt.num_columns << "\n";
        cout << "    }";
        if (i + 1 < max_alternatives) {
          cout << ",";
        }
        cout << "\n";
      }
      cout << "  ]";
    }
    cout << "\n}\n";
  } else {
    // Human-readable output with CLI flags
    cout << "Detected dialect:\n";
    cout << "  Delimiter:    " << formatDelimiter(d.delimiter) << "\n";
    cout << "  Quote:        " << formatQuoteChar(d.quote_char) << "\n";
    cout << "  Escape:       " << (d.double_quote ? "double-quote (\"\")" : "backslash (\\)")
         << "\n";
    cout << "  Line ending:  " << formatLineEnding(d.line_ending) << "\n";
    cout << "  Encoding:     " << libvroom::encoding_to_string(enc_result.encoding) << "\n";
    cout << "  Has header:   " << (result.has_header ? "yes" : "no") << "\n";
    cout << "  Columns:      " << result.detected_columns << "\n";
    cout << "  Confidence:   " << static_cast<int>(result.confidence * 100) << "%\n";
    if (low_confidence) {
      cout << "  Status:       LOW CONFIDENCE (best guess)\n";
    }

    // Show ambiguity warning and alternatives
    if (is_ambiguous) {
      cout << "\n";
      cerr << "Warning: Detection is ambiguous. Multiple dialects have similar scores.\n";
      if (result.candidates.size() > 1) {
        cerr << "Alternative candidates:\n";
        size_t max_alternatives = std::min(size_t(4), result.candidates.size());
        for (size_t i = 1; i < max_alternatives; ++i) {
          const auto& alt = result.candidates[i];
          cerr << "  - delimiter=" << formatDelimiter(alt.dialect.delimiter)
               << ", quote=" << formatQuoteChar(alt.dialect.quote_char)
               << ", score=" << static_cast<int>(alt.consistency_score * 100) << "%"
               << ", columns=" << alt.num_columns << "\n";
        }
      }
    }

    cout << "\n";

    // Output CLI flags that can be reused
    cout << "CLI flags: -d " << formatDelimiter(d.delimiter);
    if (d.quote_char != '"') {
      cout << " -q " << d.quote_char;
    }
    if (!result.has_header) {
      cout << " -H";
    }
    cout << "\n";
  }

  // Memory automatically freed when load_result goes out of scope
  return 0;
}

// Helper: escape a string for JSON output
static std::string escapeJsonString(const std::string& s) {
  std::string result;
  result.reserve(s.size());
  for (char c : s) {
    result += escapeJsonChar(c);
  }
  return result;
}

// Structure to hold column statistics for the stats command
struct ColumnStats {
  std::string name;
  libvroom::FieldType type = libvroom::FieldType::EMPTY;
  size_t count = 0;
  size_t null_count = 0;
  bool has_numeric = false;
  double min_value = DBL_MAX;
  double max_value = -DBL_MAX;
  double sum = 0.0;
  size_t numeric_count = 0;

  // For Welford's online algorithm (numerically stable variance calculation)
  double mean_accum = 0.0;
  double m2_accum = 0.0; // Sum of squares of differences from current mean

  // Store numeric values for percentile calculation
  std::vector<double> numeric_values;

  // String statistics
  size_t min_str_length = SIZE_MAX;
  size_t max_str_length = 0;
  std::unordered_set<std::string> unique_values;
  bool has_string = false;

  double mean() const { return numeric_count > 0 ? mean_accum : 0.0; }

  double variance() const {
    if (numeric_count < 2)
      return 0.0;
    return m2_accum / (numeric_count - 1); // Sample variance (Bessel's correction)
  }

  double std_dev() const { return std::sqrt(variance()); }

  // Update running mean and variance using Welford's algorithm
  void add_numeric_value(double val) {
    numeric_count++;
    double delta = val - mean_accum;
    mean_accum += delta / numeric_count;
    double delta2 = val - mean_accum;
    m2_accum += delta * delta2;

    // Update sum for backwards compatibility
    sum += val;

    // Track min/max
    if (val < min_value)
      min_value = val;
    if (val > max_value)
      max_value = val;

    // Store value for percentile calculation
    numeric_values.push_back(val);
    has_numeric = true;
  }

  // Update string statistics
  void add_string_value(const std::string& val) {
    size_t len = val.length();
    if (len < min_str_length)
      min_str_length = len;
    if (len > max_str_length)
      max_str_length = len;
    unique_values.insert(val);
    has_string = true;
  }

  // Calculate percentile (0-100)
  double percentile(double p) const {
    if (numeric_values.empty())
      return 0.0;

    // Make a sorted copy
    std::vector<double> sorted_values = numeric_values;
    std::sort(sorted_values.begin(), sorted_values.end());

    if (p <= 0)
      return sorted_values.front();
    if (p >= 100)
      return sorted_values.back();

    // Linear interpolation for percentile
    double idx = (p / 100.0) * (sorted_values.size() - 1);
    size_t lower_idx = static_cast<size_t>(std::floor(idx));
    size_t upper_idx = static_cast<size_t>(std::ceil(idx));

    if (lower_idx == upper_idx)
      return sorted_values[lower_idx];

    double fraction = idx - lower_idx;
    return sorted_values[lower_idx] * (1 - fraction) + sorted_values[upper_idx] * fraction;
  }

  // Generate a text histogram using Unicode block characters
  // Returns an 8-character histogram showing distribution
  std::string histogram() const {
    if (numeric_values.empty())
      return "";

    constexpr size_t NUM_BINS = 8;
    std::array<size_t, NUM_BINS> bins = {0};

    double range = max_value - min_value;
    if (range == 0) {
      // All values are the same - fill middle bins
      bins[NUM_BINS / 2] = numeric_values.size();
    } else {
      double bin_width = range / NUM_BINS;
      for (double val : numeric_values) {
        size_t bin = static_cast<size_t>((val - min_value) / bin_width);
        if (bin >= NUM_BINS)
          bin = NUM_BINS - 1;
        bins[bin]++;
      }
    }

    // Find max bin count for normalization
    size_t max_count = *std::max_element(bins.begin(), bins.end());
    if (max_count == 0)
      return std::string(NUM_BINS, ' ');

    // Unicode block characters: empty to full block (8 levels)
    // Using simple ASCII for maximum compatibility
    static const char* block_chars[] = {" ",
                                        "\xe2\x96\x81",
                                        "\xe2\x96\x82",
                                        "\xe2\x96\x83",
                                        "\xe2\x96\x84",
                                        "\xe2\x96\x85",
                                        "\xe2\x96\x86",
                                        "\xe2\x96\x87",
                                        "\xe2\x96\x88"};

    std::string result;
    for (size_t i = 0; i < NUM_BINS; ++i) {
      // Map count to 0-8 range
      size_t level = (bins[i] * 8) / max_count;
      result += block_chars[level];
    }
    return result;
  }
};

// Command: schema - display inferred schema with column names, types, and nullable
int cmdSchema(const char* filename, int n_threads, bool has_header,
              const libvroom::Dialect& dialect, bool auto_detect, bool json_output,
              bool strict_mode, size_t sample_size = 0) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  auto rows = iter.getRows();

  if (rows.empty()) {
    if (json_output) {
      cout << "{\"columns\": []}\n";
    } else {
      cout << "Empty file - no schema available\n";
    }
    return 0;
  }

  // Get column headers
  vector<string> headers;
  size_t num_cols = rows[0].size();
  if (has_header && !rows.empty()) {
    headers = rows[0];
  } else {
    // Generate default column names
    for (size_t i = 0; i < num_cols; ++i) {
      headers.push_back("column_" + std::to_string(i));
    }
  }

  // Initialize type inference
  libvroom::ColumnTypeInference type_inference(num_cols);

  // Process data rows (skip header if present)
  // With sampling and early termination optimization
  size_t start_row = has_header ? 1 : 0;
  size_t total_data_rows = rows.size() > start_row ? rows.size() - start_row : 0;
  size_t max_rows_to_process =
      (sample_size > 0) ? std::min(sample_size, total_data_rows) : total_data_rows;
  size_t rows_processed = 0;
  constexpr size_t EARLY_TERMINATION_CHECK_INTERVAL = 1000;
  constexpr size_t EARLY_TERMINATION_MIN_SAMPLES = 100;

  for (size_t r = start_row; r < rows.size() && rows_processed < max_rows_to_process; ++r) {
    const auto& row = rows[r];
    for (size_t c = 0; c < std::min(row.size(), num_cols); ++c) {
      const std::string& field = row[c];
      type_inference.add_field(c, reinterpret_cast<const uint8_t*>(field.data()), field.size());
    }
    ++rows_processed;

    // Check for early termination every N rows (only if not sampling)
    if (sample_size == 0 && rows_processed % EARLY_TERMINATION_CHECK_INTERVAL == 0) {
      if (type_inference.all_types_confirmed(EARLY_TERMINATION_MIN_SAMPLES)) {
        break;
      }
    }
  }

  // Infer types and check nullability
  auto types = type_inference.infer_types();

  if (json_output) {
    cout << "{\n";
    cout << "  \"columns\": [\n";
    for (size_t i = 0; i < num_cols; ++i) {
      const auto& stats = type_inference.column_stats(i);
      bool nullable = stats.empty_count > 0;

      cout << "    {\n";
      cout << "      \"name\": \"" << escapeJsonString(headers[i]) << "\",\n";
      cout << "      \"type\": \"" << libvroom::field_type_to_string(types[i]) << "\",\n";
      cout << "      \"nullable\": " << (nullable ? "true" : "false") << "\n";
      cout << "    }";
      if (i < num_cols - 1)
        cout << ",";
      cout << "\n";
    }
    cout << "  ]\n";
    cout << "}\n";
  } else {
    // Human-readable output
    cout << "Schema:\n";
    cout << left << setw(4) << "#" << setw(30) << "Column" << setw(15) << "Type" << "Nullable\n";
    cout << string(60, '-') << "\n";
    for (size_t i = 0; i < num_cols; ++i) {
      const auto& stats = type_inference.column_stats(i);
      bool nullable = stats.empty_count > 0;

      cout << left << setw(4) << i << setw(30)
           << (headers[i].length() > 28 ? headers[i].substr(0, 27) + "..." : headers[i]) << setw(15)
           << libvroom::field_type_to_string(types[i]) << (nullable ? "Yes" : "No") << "\n";
    }
  }

  return 0;
}

#ifdef LIBVROOM_ENABLE_ARROW
// Command: convert - convert CSV to Parquet or Feather format
int cmdConvert(const char* filename, const std::string& output_path, int n_threads,
               const libvroom::Dialect& dialect, bool auto_detect, const std::string& format_str,
               const std::string& compression_str, libvroom::Encoding forced_encoding,
               libvroom::ProgressCallback progress_callback) {
  // Validate output path
  if (output_path.empty()) {
    cerr << "Error: Output path required (-o <file>)" << endl;
    return 1;
  }

  // Cannot read from stdin for convert (need seekable file for Arrow)
  if (isStdinInput(filename)) {
    cerr << "Error: Cannot convert from stdin. Please specify an input file." << endl;
    return 1;
  }

  // Determine output format
  libvroom::ColumnarFormat format = libvroom::ColumnarFormat::AUTO;
  if (!format_str.empty()) {
    std::string fmt_lower = format_str;
    std::transform(fmt_lower.begin(), fmt_lower.end(), fmt_lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (fmt_lower == "parquet" || fmt_lower == "pq") {
      format = libvroom::ColumnarFormat::PARQUET;
    } else if (fmt_lower == "feather" || fmt_lower == "arrow" || fmt_lower == "ipc") {
      format = libvroom::ColumnarFormat::FEATHER;
    } else if (fmt_lower != "auto") {
      cerr << "Error: Unknown output format '" << format_str << "'" << endl;
      cerr << "Valid formats: parquet, feather, auto" << endl;
      return 1;
    }
  }

  // If still auto, detect from extension
  if (format == libvroom::ColumnarFormat::AUTO) {
    format = libvroom::detect_format_from_extension(output_path);
    if (format == libvroom::ColumnarFormat::AUTO) {
      cerr << "Error: Cannot determine output format from extension." << endl;
      cerr << "Use -F to specify format, or use .parquet/.feather extension." << endl;
      return 1;
    }
  }

  // Set up Parquet options
  libvroom::ParquetWriteOptions parquet_opts;
  if (!compression_str.empty()) {
    std::string comp_lower = compression_str;
    std::transform(comp_lower.begin(), comp_lower.end(), comp_lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (comp_lower == "snappy") {
      parquet_opts.compression = libvroom::ParquetWriteOptions::Compression::SNAPPY;
    } else if (comp_lower == "gzip" || comp_lower == "gz") {
      parquet_opts.compression = libvroom::ParquetWriteOptions::Compression::GZIP;
    } else if (comp_lower == "zstd") {
      parquet_opts.compression = libvroom::ParquetWriteOptions::Compression::ZSTD;
    } else if (comp_lower == "lz4") {
      parquet_opts.compression = libvroom::ParquetWriteOptions::Compression::LZ4;
    } else if (comp_lower == "none" || comp_lower == "uncompressed") {
      parquet_opts.compression = libvroom::ParquetWriteOptions::Compression::UNCOMPRESSED;
    } else {
      cerr << "Error: Unknown compression codec '" << compression_str << "'" << endl;
      cerr << "Valid codecs: snappy (default), gzip, zstd, lz4, none" << endl;
      return 1;
    }
  }

  // Load file and convert to Arrow
  cerr << "Reading CSV file: " << filename << endl;

  libvroom::ArrowConvertOptions arrow_opts;
  libvroom::Dialect effective_dialect = dialect;

  // If auto-detect is enabled, detect dialect first
  if (auto_detect) {
    LoadResult load_result;
    try {
      if (forced_encoding != libvroom::Encoding::UNKNOWN) {
        load_result = read_file_with_encoding(filename, LIBVROOM_PADDING, forced_encoding);
      } else {
        load_result = read_file_with_encoding(filename, LIBVROOM_PADDING);
      }
    } catch (const std::exception& e) {
      cerr << "Error: Could not load file '" << filename << "': " << e.what() << endl;
      return 1;
    }

    libvroom::DialectDetector detector;
    auto detection = detector.detect(load_result.data(), load_result.size);
    if (detection.success()) {
      effective_dialect = detection.dialect;
      cerr << "Auto-detected dialect: " << effective_dialect.to_string() << endl;
    }
  }

  auto arrow_result = libvroom::csv_to_arrow(filename, arrow_opts, effective_dialect);
  if (!arrow_result.ok()) {
    cerr << "Error: Failed to convert CSV to Arrow: " << arrow_result.error_message << endl;
    return 1;
  }

  cerr << "Converted " << arrow_result.num_rows << " rows x " << arrow_result.num_columns
       << " columns" << endl;

  // Write to output format
  const char* format_name = (format == libvroom::ColumnarFormat::PARQUET) ? "Parquet" : "Feather";
  cerr << "Writing " << format_name << " file: " << output_path << endl;

  libvroom::WriteResult write_result;
  if (format == libvroom::ColumnarFormat::PARQUET) {
    write_result = libvroom::write_parquet(arrow_result.table, output_path, parquet_opts);
  } else {
    write_result = libvroom::write_feather(arrow_result.table, output_path);
  }

  if (!write_result.ok()) {
    cerr << "Error: Failed to write output file: " << write_result.error_message << endl;
    return 1;
  }

  cerr << "Successfully wrote " << write_result.bytes_written << " bytes to " << output_path
       << endl;
  return 0;
}
#endif // LIBVROOM_ENABLE_ARROW

// Command: stats - display statistical summary for each column
int cmdStats(const char* filename, int n_threads, bool has_header, const libvroom::Dialect& dialect,
             bool auto_detect, bool json_output, bool strict_mode, size_t sample_size = 0) {
  auto result = parseFile(filename, n_threads, dialect, auto_detect, nullptr, strict_mode);
  if (!result.success)
    return 1;

  CsvIterator iter(result.load_result.data(), result.idx);
  auto rows = iter.getRows();

  if (rows.empty()) {
    if (json_output) {
      cout << "{\"columns\": []}\n";
    } else {
      cout << "Empty file - no stats available\n";
    }
    return 0;
  }

  // Get column headers
  vector<string> headers;
  size_t num_cols = rows[0].size();
  if (has_header && !rows.empty()) {
    headers = rows[0];
  } else {
    // Generate default column names
    for (size_t i = 0; i < num_cols; ++i) {
      headers.push_back("column_" + std::to_string(i));
    }
  }

  // Initialize statistics
  vector<ColumnStats> stats(num_cols);
  for (size_t i = 0; i < num_cols; ++i) {
    stats[i].name = headers[i];
  }

  // Initialize type inference for type detection
  libvroom::ColumnTypeInference type_inference(num_cols);

  // Process data rows (skip header if present)
  // With sampling optimization
  size_t start_row = has_header ? 1 : 0;
  size_t total_data_rows = rows.size() > start_row ? rows.size() - start_row : 0;
  size_t max_rows_to_process =
      (sample_size > 0) ? std::min(sample_size, total_data_rows) : total_data_rows;
  size_t rows_processed = 0;

  for (size_t r = start_row; r < rows.size() && rows_processed < max_rows_to_process; ++r) {
    const auto& row = rows[r];
    for (size_t c = 0; c < std::min(row.size(), num_cols); ++c) {
      const std::string& field = row[c];
      stats[c].count++;

      // Add to type inference
      type_inference.add_field(c, reinterpret_cast<const uint8_t*>(field.data()), field.size());

      // Check for null/empty
      if (field.empty()) {
        stats[c].null_count++;
        continue;
      }

      // Try to parse as numeric for extended statistics
      auto numeric_result = libvroom::parse_double(field.c_str(), field.size(),
                                                   libvroom::ExtractionConfig::defaults());
      if (numeric_result.ok()) {
        double val = numeric_result.get();
        if (!std::isnan(val) && !std::isinf(val)) {
          // Use Welford's algorithm for numerically stable mean/variance
          stats[c].add_numeric_value(val);
        }
      }

      // Track string statistics for non-empty fields
      stats[c].add_string_value(field);
    }
    ++rows_processed;
  }

  // Calculate data_row_count based on what we actually processed
  size_t data_row_count = rows_processed;

  // Get inferred types
  auto types = type_inference.infer_types();
  for (size_t i = 0; i < num_cols; ++i) {
    stats[i].type = types[i];
  }

  if (json_output) {
    cout << "{\n";
    cout << "  \"rows\": " << data_row_count << ",\n";
    cout << "  \"columns\": [\n";
    for (size_t i = 0; i < num_cols; ++i) {
      const auto& s = stats[i];
      cout << "    {\n";
      cout << "      \"name\": \"" << escapeJsonString(s.name) << "\",\n";
      cout << "      \"type\": \"" << libvroom::field_type_to_string(s.type) << "\",\n";
      cout << "      \"count\": " << s.count << ",\n";
      cout << "      \"nulls\": " << s.null_count << ",\n";
      cout << "      \"non_null_count\": " << (s.count - s.null_count) << ",\n";
      cout << "      \"complete_rate\": "
           << (s.count > 0 ? static_cast<double>(s.count - s.null_count) / s.count : 0.0) << ",\n";

      if (s.has_numeric) {
        // Use fixed precision for JSON numbers
        cout << fixed << setprecision(6);
        cout << "      \"min\": " << s.min_value << ",\n";
        cout << "      \"max\": " << s.max_value << ",\n";
        cout << "      \"mean\": " << s.mean() << ",\n";
        cout << "      \"sd\": " << s.std_dev() << ",\n";
        cout << "      \"p0\": " << s.percentile(0) << ",\n";
        cout << "      \"p25\": " << s.percentile(25) << ",\n";
        cout << "      \"p50\": " << s.percentile(50) << ",\n";
        cout << "      \"p75\": " << s.percentile(75) << ",\n";
        cout << "      \"p100\": " << s.percentile(100) << ",\n";
        cout << defaultfloat;
        cout << "      \"hist\": \"" << s.histogram() << "\"";
      } else {
        cout << "      \"min\": null,\n";
        cout << "      \"max\": null,\n";
        cout << "      \"mean\": null,\n";
        cout << "      \"sd\": null,\n";
        cout << "      \"p0\": null,\n";
        cout << "      \"p25\": null,\n";
        cout << "      \"p50\": null,\n";
        cout << "      \"p75\": null,\n";
        cout << "      \"p100\": null,\n";
        cout << "      \"hist\": null";
      }

      // String statistics (for all columns)
      // Note: has_string implies min_str_length != SIZE_MAX because both are set
      // together in add_string_value(). We use has_string as the primary check.
      if (s.has_string) {
        cout << ",\n";
        cout << "      \"n_unique\": " << s.unique_values.size() << ",\n";
        cout << "      \"min_length\": " << s.min_str_length << ",\n";
        cout << "      \"max_length\": " << s.max_str_length << "\n";
      } else {
        // No non-empty string values
        cout << ",\n";
        cout << "      \"n_unique\": 0,\n";
        cout << "      \"min_length\": null,\n";
        cout << "      \"max_length\": null\n";
      }

      cout << "    }";
      if (i < num_cols - 1)
        cout << ",";
      cout << "\n";
    }
    cout << "  ]\n";
    cout << "}\n";
  } else {
    // Human-readable output
    cout << "Statistics (" << data_row_count << " rows):\n\n";

    for (size_t i = 0; i < num_cols; ++i) {
      const auto& s = stats[i];
      double complete_rate =
          s.count > 0 ? static_cast<double>(s.count - s.null_count) / s.count : 0.0;

      cout << "Column " << i << ": " << s.name << "\n";
      cout << "  Type:          " << libvroom::field_type_to_string(s.type) << "\n";
      cout << "  Count:         " << s.count << "\n";
      cout << "  Nulls:         " << s.null_count << " ("
           << (s.count > 0 ? static_cast<int>(100.0 * s.null_count / s.count) : 0) << "%)\n";
      cout << "  Complete rate: " << fixed << setprecision(2) << (complete_rate * 100) << "%\n";
      cout << defaultfloat;

      if (s.has_numeric) {
        cout << fixed << setprecision(2);
        cout << "  Min:           " << s.min_value << "\n";
        cout << "  Max:           " << s.max_value << "\n";
        cout << "  Mean:          " << s.mean() << "\n";
        cout << "  Std Dev:       " << s.std_dev() << "\n";
        cout << "  Percentiles:   p0=" << s.percentile(0) << ", p25=" << s.percentile(25)
             << ", p50=" << s.percentile(50) << ", p75=" << s.percentile(75)
             << ", p100=" << s.percentile(100) << "\n";
        cout << defaultfloat;
        cout << "  Histogram:     " << s.histogram() << "\n";
      }

      // String statistics for non-numeric columns or columns with string values
      // Note: has_string is only true if add_string_value was called, which requires
      // non-empty fields. So if has_string is true, min_str_length is always valid.
      if (s.has_string && !s.has_numeric) {
        cout << "  Unique values: " << s.unique_values.size() << "\n";
        cout << "  Min length:    " << s.min_str_length << "\n";
        cout << "  Max length:    " << s.max_str_length << "\n";
      } else if (s.has_string && s.has_numeric) {
        // For numeric columns, still show unique count
        cout << "  Unique values: " << s.unique_values.size() << "\n";
      }
      cout << "\n";
    }
  }

  return 0;
}

int main(int argc, char* argv[]) {
  // Disable buffering for stdout to ensure output is written immediately.
  // This fixes flaky test failures on macOS where popen() may not capture
  // all output if the process exits before buffers are flushed.
  setvbuf(stdout, nullptr, _IONBF, 0);

  if (argc < 2) {
    printUsage(argv[0]);
    return 1;
  }

  // Check for help or version
  if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
    printUsage(argv[0]);
    return 0;
  }
  if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0) {
    printVersion();
    return 0;
  }

  // Parse command
  string command = argv[1];

  // Skip command for option parsing
  optind = 2;

  // Auto-detect number of threads based on hardware concurrency
  unsigned int hw_threads = std::thread::hardware_concurrency();
  int n_threads =
      (hw_threads > 0)
          ? static_cast<int>(std::min(hw_threads, static_cast<unsigned int>(MAX_THREADS)))
          : 1;
  size_t num_rows = DEFAULT_NUM_ROWS;
  bool has_header = true;
  bool auto_detect = true;          // Auto-detect by default
  bool delimiter_specified = false; // Track if user specified delimiter
  bool json_output = false;         // JSON output for dialect command
  bool force_output = false;        // Force output even with low confidence (dialect command)
  bool strict_mode = false;         // Strict mode: exit with code 1 on any parse error
  unsigned int random_seed = 0;     // Random seed for sample command (0 = use random_device)
  libvroom::Encoding forced_encoding = libvroom::Encoding::UNKNOWN; // User-specified encoding
  size_t sample_size = 0;      // Sample size for schema/stats (0 = all rows)
  CliCacheConfig cache_config; // Index caching configuration
  // Progress bar: auto-enabled for TTY by default, can be overridden by --progress/--no-progress
  bool progress_auto = true;     // Use automatic TTY detection
  bool progress_enabled = false; // Explicit override value
  string columns;
  string delimiter_str = "comma";
  string encoding_str; // User-specified encoding string
  char quote_char = '"';
  string output_path;     // Output file for convert command
  string output_format;   // Output format for convert command (parquet, feather, auto)
  string compression_str; // Compression codec for Parquet (snappy, gzip, zstd, lz4, none)

  // Pre-scan for long options (since we're not using getopt_long)
  for (int i = 2; i < argc; ++i) {
    if (strcmp(argv[i], "--strict") == 0) {
      strict_mode = true;
      // Remove --strict from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    } else if (strcmp(argv[i], "--force") == 0) {
      force_output = true;
      // Remove --force from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    } else if (strcmp(argv[i], "--cache") == 0) {
      cache_config.enabled = true;
      // Remove --cache from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    } else if (strcmp(argv[i], "--no-cache") == 0) {
      cache_config.enabled = false;
      cache_config.cache_dir.clear();
      // Remove --no-cache from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    } else if (strncmp(argv[i], "--cache-dir=", 12) == 0) {
      cache_config.enabled = true;
      cache_config.cache_dir = argv[i] + 12;
      // Remove --cache-dir=... from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    } else if (strcmp(argv[i], "--cache-dir") == 0 && i + 1 < argc) {
      cache_config.enabled = true;
      cache_config.cache_dir = argv[i + 1];
      // Remove both --cache-dir and its argument from argv
      for (int j = i; j < argc - 2; ++j) {
        argv[j] = argv[j + 2];
      }
      argc -= 2;
      --i; // Recheck this position
    } else if (strcmp(argv[i], "--progress") == 0) {
      progress_auto = false;
      progress_enabled = true;
      // Remove --progress from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    } else if (strcmp(argv[i], "--no-progress") == 0) {
      progress_auto = false;
      progress_enabled = false;
      // Remove --no-progress from argv by shifting remaining args
      for (int j = i; j < argc - 1; ++j) {
        argv[j] = argv[j + 1];
      }
      --argc;
      --i; // Recheck this position
    }
  }

  int c;
  while ((c = getopt(argc, argv, "n:c:Ht:d:q:e:s:m:o:F:C:jfpShv")) != -1) {
    switch (c) {
    case 'n': {
      char* endptr;
      long val = strtol(optarg, &endptr, 10);
      if (*endptr != '\0' || val < 0) {
        cerr << "Error: Invalid row count '" << optarg << "'\n";
        return 1;
      }
      num_rows = static_cast<size_t>(val);
      break;
    }
    case 'c':
      columns = optarg;
      break;
    case 'H':
      has_header = false;
      break;
    case 't': {
      char* endptr;
      long val = strtol(optarg, &endptr, 10);
      if (*endptr != '\0' || val < MIN_THREADS || val > MAX_THREADS) {
        cerr << "Error: Thread count must be between " << MIN_THREADS << " and " << MAX_THREADS
             << "\n";
        return 1;
      }
      n_threads = static_cast<int>(val);
      break;
    }
    case 'd':
      delimiter_str = optarg;
      delimiter_specified = true;
      auto_detect = false; // Disable auto-detect when delimiter is specified
      break;
    case 'q':
      if (strlen(optarg) == 1) {
        quote_char = optarg[0];
      } else {
        cerr << "Error: Quote character must be a single character\n";
        return 1;
      }
      break;
    case 'e':
      encoding_str = optarg;
      forced_encoding = libvroom::parse_encoding_name(encoding_str);
      if (forced_encoding == libvroom::Encoding::UNKNOWN) {
        cerr << "Error: Unknown encoding '" << encoding_str << "'\n";
        cerr << "Supported encodings: utf-8, utf-16le, utf-16be, utf-32le, utf-32be, "
             << "latin1, windows-1252\n";
        return 1;
      }
      break;
    case 's': {
      char* endptr;
      long val = strtol(optarg, &endptr, 10);
      if (*endptr != '\0' || val < 0) {
        cerr << "Error: Invalid seed value '" << optarg << "'\n";
        return 1;
      }
      random_seed = static_cast<unsigned int>(val);
      break;
    }
    case 'j':
      json_output = true;
      break;
    case 'f':
      force_output = true;
      break;
    case 'p':
      progress_auto = false;
      progress_enabled = true;
      break;
    case 'm': {
      char* endptr;
      long val = strtol(optarg, &endptr, 10);
      if (*endptr != '\0' || val < 0) {
        cerr << "Error: Invalid sample size '" << optarg << "'\n";
        return 1;
      }
      sample_size = static_cast<size_t>(val);
      break;
    }
    case 'S':
      strict_mode = true;
      break;
    case 'o':
      output_path = optarg;
      break;
    case 'F':
      output_format = optarg;
      break;
    case 'C':
      compression_str = optarg;
      break;
    case 'h':
      printUsage(argv[0]);
      return 0;
    case 'v':
      printVersion();
      return 0;
    default:
      printUsage(argv[0]);
      return 1;
    }
  }

  // Allow reading from stdin if no filename is specified
  const char* filename = nullptr;
  if (optind < argc) {
    filename = argv[optind];
  }
  libvroom::Dialect dialect = parseDialect(delimiter_str, quote_char);

  // Set up progress bar based on flags and TTY detection
  // Progress is enabled if:
  // 1. Explicitly requested via -p/--progress, OR
  // 2. Auto mode AND stderr is a TTY AND not reading from stdin
  bool show_progress =
      progress_auto ? (isatty(STDERR_FILENO) && !isStdinInput(filename)) : progress_enabled;
  ProgressBar progress_bar(show_progress);

  // Create progress callback that updates progress bar
  auto progress_cb = progress_bar.callback();

  // Dispatch to command handlers
  int result = 0;
  if (command == "count") {
    // Note: count uses optimized row counting that doesn't do full parse validation,
    // so strict_mode, forced_encoding, caching, and progress are not applicable
    result = cmdCount(filename, n_threads, has_header, dialect, auto_detect);
  } else if (command == "head") {
    result = cmdHead(filename, n_threads, num_rows, has_header, dialect, auto_detect, strict_mode,
                     forced_encoding, &cache_config, progress_cb);
    progress_bar.finish();
  } else if (command == "tail") {
    // Note: tail uses streaming API which doesn't support caching or progress yet
    result = cmdTail(filename, n_threads, num_rows, has_header, dialect, auto_detect, strict_mode,
                     forced_encoding);
  } else if (command == "sample") {
    result = cmdSample(filename, n_threads, num_rows, has_header, dialect, auto_detect, random_seed,
                       strict_mode, forced_encoding, &cache_config, progress_cb);
    progress_bar.finish();
  } else if (command == "select") {
    if (columns.empty()) {
      cerr << "Error: -c option required for select command\n";
      return 1;
    }
    result = cmdSelect(filename, n_threads, columns, has_header, dialect, auto_detect, strict_mode,
                       forced_encoding, &cache_config, progress_cb);
    progress_bar.finish();
  } else if (command == "info") {
    result = cmdInfo(filename, n_threads, has_header, dialect, auto_detect, strict_mode,
                     forced_encoding, &cache_config, progress_cb);
    progress_bar.finish();
  } else if (command == "pretty") {
    result = cmdPretty(filename, n_threads, num_rows, has_header, dialect, auto_detect, strict_mode,
                       forced_encoding, &cache_config, progress_cb);
    progress_bar.finish();
  } else if (command == "dialect") {
    // Note: dialect command ignores -d, --strict, and -e flags since it's for detection
    (void)delimiter_specified; // Suppress unused warning
    result = cmdDialect(filename, json_output, force_output);
  } else if (command == "schema") {
    result = cmdSchema(filename, n_threads, has_header, dialect, auto_detect, json_output,
                       strict_mode, sample_size);
  } else if (command == "stats") {
    result = cmdStats(filename, n_threads, has_header, dialect, auto_detect, json_output,
                      strict_mode, sample_size);
#ifdef LIBVROOM_ENABLE_ARROW
  } else if (command == "convert") {
    result = cmdConvert(filename, output_path, n_threads, dialect, auto_detect, output_format,
                        compression_str, forced_encoding, progress_cb);
#endif
  } else {
    cerr << "Error: Unknown command '" << command << "'\n";
    printUsage(argv[0]);
    return 1;
  }

  // Ensure all output is flushed before exit.
  // This fixes flaky test failures on macOS where popen() may not capture
  // all output if the process exits before buffers are flushed.
  // Use both C++ stream flush and C stdio flush to handle all buffering layers.
  std::cout.flush();
  std::cerr.flush();
  fflush(stdout);
  fflush(stderr);
  return result;
}
