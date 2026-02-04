/**
 * vroom - Command-line utility for CSV processing
 *
 * Commands:
 * - convert: Convert CSV to Parquet format
 * - count: Count rows in CSV file
 * - head: Show first N rows
 * - info: Show file information
 * - select: Select specific columns
 * - pretty: Pretty-print CSV in table format
 */

#include "libvroom.h"
#include "libvroom/io_util.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

using namespace std;

// Constants
constexpr const char* VERSION = "2.0.0";
constexpr size_t DEFAULT_NUM_ROWS = 10;
constexpr size_t MAX_COLUMN_WIDTH = 40;

// =============================================================================
// Progress Bar Support
// =============================================================================

class ProgressBar {
public:
  explicit ProgressBar(bool enabled, size_t width = 40) : enabled_(enabled), width_(width) {}

  bool update(size_t bytes_processed, size_t total_bytes) {
    if (!enabled_ || total_bytes == 0)
      return true;

    int percent = static_cast<int>((bytes_processed * 100) / total_bytes);

    if (percent == last_percent_)
      return true;
    last_percent_ = percent;

    size_t filled = (percent * width_) / 100;

    std::string bar(width_, ' ');
    for (size_t i = 0; i < filled && i < width_; ++i) {
      bar[i] = '=';
    }
    if (filled < width_) {
      bar[filled] = '>';
    }

    std::cerr << "\r[" << bar << "] " << std::setw(3) << percent << "%" << std::flush;

    return true;
  }

  void finish() {
    if (enabled_) {
      std::string bar(width_, '=');
      std::cerr << "\r[" << bar << "] 100%" << std::endl;
    }
  }

  void clear() {
    if (enabled_) {
      std::cerr << "\r" << std::string(width_ + 7, ' ') << "\r" << std::flush;
    }
  }

  libvroom::ProgressCallback callback() {
    return [this](size_t processed, size_t total) { return this->update(processed, total); };
  }

private:
  bool enabled_;
  size_t width_;
  int last_percent_ = -1;
};

// =============================================================================
// Helper Functions
// =============================================================================

// Check if input is stdin
static bool isStdinInput(const char* filename) {
  return filename == nullptr || strcmp(filename, "-") == 0 || strcmp(filename, "/dev/stdin") == 0;
}

// Get string value from column at row index
static std::string getColumnValueAsString(const libvroom::ArrowColumnBuilder* col, size_t row_idx) {
  if (!col || row_idx >= col->size())
    return "";

  // Check for null
  const auto& nulls = col->null_bitmap();
  if (nulls.has_nulls() && nulls.is_null(row_idx)) {
    return "";
  }

  switch (col->type()) {
  case libvroom::DataType::INT32: {
    auto* typed = static_cast<const libvroom::ArrowInt32ColumnBuilder*>(col);
    return std::to_string(typed->values().get(row_idx));
  }
  case libvroom::DataType::INT64: {
    auto* typed = static_cast<const libvroom::ArrowInt64ColumnBuilder*>(col);
    return std::to_string(typed->values().get(row_idx));
  }
  case libvroom::DataType::FLOAT64: {
    auto* typed = static_cast<const libvroom::ArrowFloat64ColumnBuilder*>(col);
    std::ostringstream oss;
    oss << typed->values().get(row_idx);
    return oss.str();
  }
  case libvroom::DataType::BOOL: {
    auto* typed = static_cast<const libvroom::ArrowBoolColumnBuilder*>(col);
    return typed->values().get(row_idx) ? "true" : "false";
  }
  case libvroom::DataType::STRING: {
    auto* typed = static_cast<const libvroom::ArrowStringColumnBuilder*>(col);
    return std::string(typed->values().get(row_idx));
  }
  case libvroom::DataType::DATE: {
    auto* typed = static_cast<const libvroom::ArrowDateColumnBuilder*>(col);
    // Convert days since epoch to YYYY-MM-DD
    int32_t days = typed->values().get(row_idx);
    // Simple conversion: 1970-01-01 + days
    time_t t = static_cast<time_t>(days) * 86400;
    struct tm* tm_info = gmtime(&t);
    char buf[16];
    strftime(buf, sizeof(buf), "%Y-%m-%d", tm_info);
    return buf;
  }
  case libvroom::DataType::TIMESTAMP: {
    auto* typed = static_cast<const libvroom::ArrowTimestampColumnBuilder*>(col);
    // Convert microseconds since epoch to ISO format
    int64_t us = typed->values().get(row_idx);
    time_t t = static_cast<time_t>(us / 1000000);
    struct tm* tm_info = gmtime(&t);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", tm_info);
    return buf;
  }
  default:
    return "<unknown>";
  }
}

// Output a row with proper CSV quoting
static void outputRow(const std::vector<std::string>& row, char delimiter, char quote_char) {
  for (size_t i = 0; i < row.size(); ++i) {
    if (i > 0)
      cout << delimiter;
    bool needs_quote = row[i].find(delimiter) != string::npos ||
                       row[i].find(quote_char) != string::npos ||
                       row[i].find('\n') != string::npos || row[i].find('\r') != string::npos;
    if (needs_quote) {
      cout << quote_char;
      for (char c : row[i]) {
        if (c == quote_char)
          cout << quote_char;
        cout << c;
      }
      cout << quote_char;
    } else {
      cout << row[i];
    }
  }
  cout << '\n';
}

// Format delimiter for display
static std::string formatDelimiter(char delim) {
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
}

// =============================================================================
// Help and Usage
// =============================================================================

void print_version() {
  cout << "vroom " << VERSION << endl;
}

void print_usage() {
  cout << R"(vroom - High-performance CSV processor

USAGE:
    vroom <COMMAND> [OPTIONS] <INPUT>

COMMANDS:
    convert     Convert CSV to Parquet format
    count       Count rows in CSV file
    head        Show first N rows of CSV file
    info        Show information about CSV file
    select      Select specific columns
    pretty      Pretty-print CSV in table format
    help        Show this help message
    version     Show version information

CONVERT OPTIONS:
    -o, --output <FILE>      Output Parquet file path (required)
    -c, --compression <TYPE> Compression: zstd, snappy, lz4, gzip, none (default: zstd)
    -r, --row-group <SIZE>   Rows per row group (default: 1000000)

COMMON OPTIONS:
    -n, --rows <N>           Number of rows for head/pretty (default: 10)
    -j, --threads <N>        Number of threads (default: auto)
    -d, --delimiter <CHAR>   Field delimiter (default: ,)
    -q, --quote <CHAR>       Quote character (default: ")
    --no-header              CSV has no header row
    -p, --progress           Show progress bar
    -v, --verbose            Verbose output
    -h, --help               Show this help message

SELECT OPTIONS:
    -c, --columns <COLS>     Comma-separated column names or indices

INDEX CACHING:
    --cache                  Enable index caching (stores .vidx next to source)
    --cache-dir <PATH>       Store cache files in specified directory
    --no-cache               Disable caching (default behavior)

ERROR HANDLING:
    --strict                 Stop on first error
    --permissive             Collect all errors, continue parsing
    --max-errors <N>         Maximum errors to collect (default: 10000)

EXAMPLES:
    vroom convert data.csv -o data.parquet
    vroom count data.csv
    vroom head data.csv -n 20
    vroom info data.csv
    vroom select data.csv -c name,age,city
    vroom pretty data.csv -n 5

For more information, visit: https://github.com/jimhester/libvroom
)";
}

// =============================================================================
// Common argument parsing
// =============================================================================

struct CommonOptions {
  string input_path;
  char delimiter = ',';
  char quote = '"';
  bool has_header = true;
  size_t num_threads = 0;
  size_t num_rows = DEFAULT_NUM_ROWS;
  bool show_progress = false;
  bool verbose = false;
  libvroom::ErrorMode error_mode = libvroom::ErrorMode::DISABLED;
  size_t max_errors = libvroom::ErrorCollector::DEFAULT_MAX_ERRORS;
  string columns; // For select command

  // Index caching
  bool enable_cache = false;
  string cache_dir;      // Non-empty = CUSTOM mode
  bool no_cache = false; // Explicitly disable caching
};

// Apply cache configuration from CommonOptions to CsvOptions
static void applyCacheConfig(libvroom::CsvOptions& csv_opts, const CommonOptions& opts) {
  if (opts.no_cache || !opts.enable_cache)
    return;

  if (!opts.cache_dir.empty()) {
    csv_opts.cache = libvroom::CacheConfig::custom(opts.cache_dir);
  } else {
    csv_opts.cache = libvroom::CacheConfig::defaults();
  }
}

// Parse common options, returns index of first unparsed argument
static int parseCommonOptions(int argc, char* argv[], CommonOptions& opts, int start_idx = 1) {
  for (int i = start_idx; i < argc; ++i) {
    string arg = argv[i];

    if (arg == "-n" || arg == "--rows") {
      if (++i >= argc) {
        cerr << "Error: --rows requires a number" << endl;
        return -1;
      }
      opts.num_rows = stoul(argv[i]);
    } else if (arg == "-j" || arg == "-t" || arg == "--threads") {
      if (++i >= argc) {
        cerr << "Error: --threads requires a number" << endl;
        return -1;
      }
      opts.num_threads = stoul(argv[i]);
    } else if (arg == "-d" || arg == "--delimiter") {
      if (++i >= argc) {
        cerr << "Error: --delimiter requires a character" << endl;
        return -1;
      }
      string delim_str = argv[i];
      if (delim_str == "\\t" || delim_str == "tab") {
        opts.delimiter = '\t';
      } else if (delim_str == "comma") {
        opts.delimiter = ',';
      } else if (delim_str == "semicolon") {
        opts.delimiter = ';';
      } else if (delim_str == "pipe") {
        opts.delimiter = '|';
      } else if (delim_str == "colon") {
        opts.delimiter = ':';
      } else if (delim_str.length() == 1) {
        opts.delimiter = delim_str[0];
      } else {
        cerr << "Error: --delimiter must be a single character or name (comma, tab, semicolon, "
                "pipe, colon)"
             << endl;
        return -1;
      }
    } else if (arg == "-q" || arg == "--quote") {
      if (++i >= argc) {
        cerr << "Error: --quote requires a character" << endl;
        return -1;
      }
      opts.quote = argv[i][0];
    } else if (arg == "--no-header" || arg == "-H") {
      opts.has_header = false;
    } else if (arg == "-p" || arg == "--progress") {
      opts.show_progress = true;
    } else if (arg == "-v" || arg == "--verbose") {
      opts.verbose = true;
    } else if (arg == "--strict") {
      opts.error_mode = libvroom::ErrorMode::FAIL_FAST;
    } else if (arg == "--permissive") {
      opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    } else if (arg == "--max-errors") {
      if (++i >= argc) {
        cerr << "Error: --max-errors requires a number" << endl;
        return -1;
      }
      opts.max_errors = stoul(argv[i]);
      if (opts.error_mode == libvroom::ErrorMode::DISABLED) {
        opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
      }
    } else if ((arg == "-c" || arg == "--columns") && opts.columns.empty()) {
      if (++i >= argc) {
        cerr << "Error: --columns requires column specification" << endl;
        return -1;
      }
      opts.columns = argv[i];
    } else if (arg == "--cache") {
      opts.enable_cache = true;
    } else if (arg == "--cache-dir") {
      if (++i >= argc) {
        cerr << "Error: --cache-dir requires a path" << endl;
        return -1;
      }
      opts.cache_dir = argv[i];
      opts.enable_cache = true;
    } else if (arg == "--no-cache") {
      opts.no_cache = true;
      opts.enable_cache = false;
    } else if (arg == "-h" || arg == "--help") {
      print_usage();
      exit(0);
    } else if (arg[0] != '-' && opts.input_path.empty()) {
      opts.input_path = arg;
    } else if (arg[0] == '-') {
      // Unknown option - might be command-specific
      return i;
    }
  }
  return argc;
}

// =============================================================================
// Command: convert
// =============================================================================

int cmd_convert(int argc, char* argv[]) {
  CommonOptions common;
  string output_path;
  string compression = "zstd";
  size_t row_group_size = 1'000'000;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    string arg = argv[i];

    if (arg == "-o" || arg == "--output") {
      if (++i >= argc) {
        cerr << "Error: --output requires a file path" << endl;
        return 1;
      }
      output_path = argv[i];
    } else if (arg == "-c" || arg == "--compression") {
      if (++i >= argc) {
        cerr << "Error: --compression requires a type" << endl;
        return 1;
      }
      compression = argv[i];
    } else if (arg == "-r" || arg == "--row-group") {
      if (++i >= argc) {
        cerr << "Error: --row-group requires a size" << endl;
        return 1;
      }
      row_group_size = stoul(argv[i]);
    } else if (arg == "-j" || arg == "--threads") {
      if (++i >= argc) {
        cerr << "Error: --threads requires a number" << endl;
        return 1;
      }
      common.num_threads = stoul(argv[i]);
    } else if (arg == "-d" || arg == "--delimiter") {
      if (++i >= argc) {
        cerr << "Error: --delimiter requires a character" << endl;
        return 1;
      }
      common.delimiter = argv[i][0];
    } else if (arg == "-q" || arg == "--quote") {
      if (++i >= argc) {
        cerr << "Error: --quote requires a character" << endl;
        return 1;
      }
      common.quote = argv[i][0];
    } else if (arg == "--no-header") {
      common.has_header = false;
    } else if (arg == "--strict") {
      common.error_mode = libvroom::ErrorMode::FAIL_FAST;
    } else if (arg == "--permissive") {
      common.error_mode = libvroom::ErrorMode::PERMISSIVE;
    } else if (arg == "--max-errors") {
      if (++i >= argc) {
        cerr << "Error: --max-errors requires a number" << endl;
        return 1;
      }
      common.max_errors = stoul(argv[i]);
      if (common.error_mode == libvroom::ErrorMode::DISABLED) {
        common.error_mode = libvroom::ErrorMode::PERMISSIVE;
      }
    } else if (arg == "-p" || arg == "--progress") {
      common.show_progress = true;
    } else if (arg == "-v" || arg == "--verbose") {
      common.verbose = true;
    } else if (arg == "--cache") {
      common.enable_cache = true;
    } else if (arg == "--cache-dir") {
      if (++i >= argc) {
        cerr << "Error: --cache-dir requires a path" << endl;
        return 1;
      }
      common.cache_dir = argv[i];
      common.enable_cache = true;
    } else if (arg == "--no-cache") {
      common.no_cache = true;
      common.enable_cache = false;
    } else if (arg == "-h" || arg == "--help") {
      print_usage();
      return 0;
    } else if (arg == "convert") {
      // Skip command name
    } else if (arg[0] != '-' && common.input_path.empty()) {
      common.input_path = arg;
    } else {
      cerr << "Error: Unknown option: " << arg << endl;
      return 1;
    }
  }

  // Validate arguments
  if (common.input_path.empty()) {
    cerr << "Error: Input file required" << endl;
    print_usage();
    return 1;
  }

  if (output_path.empty()) {
    cerr << "Error: Output file required (use -o or --output)" << endl;
    return 1;
  }

  // Set up options
  libvroom::VroomOptions opts;
  opts.input_path = common.input_path;
  opts.output_path = output_path;
  opts.verbose = common.verbose;
  opts.progress = common.show_progress;

  // CSV options
  opts.csv.separator = common.delimiter;
  opts.csv.quote = common.quote;
  opts.csv.has_header = common.has_header;
  opts.csv.error_mode = common.error_mode;
  opts.csv.max_errors = common.max_errors;
  if (common.num_threads > 0) {
    opts.csv.num_threads = common.num_threads;
    opts.threads.num_threads = common.num_threads;
  }
  applyCacheConfig(opts.csv, common);

  // Parquet options
  opts.parquet.row_group_size = row_group_size;

  // Set compression
  if (compression == "zstd") {
    opts.parquet.compression = libvroom::Compression::ZSTD;
  } else if (compression == "snappy") {
    opts.parquet.compression = libvroom::Compression::SNAPPY;
  } else if (compression == "lz4") {
    opts.parquet.compression = libvroom::Compression::LZ4;
  } else if (compression == "gzip") {
    opts.parquet.compression = libvroom::Compression::GZIP;
  } else if (compression == "none") {
    opts.parquet.compression = libvroom::Compression::NONE;
  } else {
    cerr << "Error: Unknown compression type: " << compression << endl;
    return 1;
  }

  // Set up progress callback
  ProgressBar progress(common.show_progress && isatty(STDERR_FILENO));
  libvroom::ProgressCallback progress_cb = nullptr;
  if (common.show_progress) {
    progress_cb = progress.callback();
  }

  // Run conversion
  if (common.verbose) {
    cerr << "Converting " << common.input_path << " to " << output_path << endl;
    cerr << "Compression: " << compression << endl;
    cerr << "Row group size: " << row_group_size << endl;
  }

  auto result = libvroom::convert_csv_to_parquet(opts, progress_cb);

  if (common.show_progress) {
    progress.finish();
  }

  if (!result.ok()) {
    cerr << "Error: " << result.error << endl;
    if (result.has_errors()) {
      cerr << "\nParse errors (" << result.error_summary() << "):" << endl;
      for (const auto& err : result.parse_errors) {
        cerr << "  " << err.to_string() << endl;
      }
    }
    return 1;
  }

  // Display warnings/errors even on success
  if (result.has_errors()) {
    if (common.verbose || common.error_mode != libvroom::ErrorMode::DISABLED) {
      cerr << "\n" << result.error_summary() << ":" << endl;
      for (const auto& err : result.parse_errors) {
        cerr << "  " << err.to_string() << endl;
      }
    }
    if (common.error_mode == libvroom::ErrorMode::FAIL_FAST) {
      return 1;
    }
  }

  if (common.verbose) {
    cerr << "Converted " << result.rows << " rows, " << result.cols << " columns" << endl;
  }

  return 0;
}

// =============================================================================
// Command: count
// =============================================================================

int cmd_count(int argc, char* argv[]) {
  CommonOptions opts;

  // Skip "count" command
  int start = 1;
  if (argc > 1 && string(argv[1]) == "count") {
    start = 2;
  }

  if (parseCommonOptions(argc, argv, opts, start) < 0) {
    return 1;
  }

  // Set up CsvReader
  libvroom::CsvOptions csv_opts;
  csv_opts.separator = opts.delimiter;
  csv_opts.quote = opts.quote;
  csv_opts.has_header = opts.has_header;
  csv_opts.error_mode = opts.error_mode;
  csv_opts.max_errors = opts.max_errors;
  if (opts.num_threads > 0) {
    csv_opts.num_threads = opts.num_threads;
  }
  applyCacheConfig(csv_opts, opts);

  libvroom::CsvReader reader(csv_opts);
  libvroom::Result<bool> open_result;

  // Check for stdin input
  if (opts.input_path.empty() || isStdinInput(opts.input_path.c_str())) {
    auto buffer = libvroom::read_stdin_to_ptr();
    if (buffer.size() == 0) {
      // Empty stdin - output 0
      cout << 0 << endl;
      return 0;
    }
    open_result = reader.open_from_buffer(std::move(buffer));
  } else {
    open_result = reader.open(opts.input_path);
  }

  if (!open_result) {
    // Handle empty file gracefully - output 0
    if (open_result.error.find("Empty file") != string::npos) {
      cout << 0 << endl;
      return 0;
    }
    cerr << "Error: " << open_result.error << endl;
    return 1;
  }

  auto read_result = reader.read_all();
  if (!read_result) {
    // Handle empty file gracefully - output 0
    if (read_result.error.find("Empty file") != string::npos) {
      cout << 0 << endl;
      return 0;
    }
    cerr << "Error: " << read_result.error << endl;
    return 1;
  }

  size_t row_count = read_result.value.total_rows;
  cout << row_count << endl;

  return 0;
}

// =============================================================================
// Command: head
// =============================================================================

int cmd_head(int argc, char* argv[]) {
  CommonOptions opts;

  // Skip "head" command
  int start = 1;
  if (argc > 1 && string(argv[1]) == "head") {
    start = 2;
  }

  if (parseCommonOptions(argc, argv, opts, start) < 0) {
    return 1;
  }

  // Set up CsvReader
  libvroom::CsvOptions csv_opts;
  csv_opts.separator = opts.delimiter;
  csv_opts.quote = opts.quote;
  csv_opts.has_header = opts.has_header;
  csv_opts.error_mode = opts.error_mode;
  csv_opts.max_errors = opts.max_errors;
  if (opts.num_threads > 0) {
    csv_opts.num_threads = opts.num_threads;
  }
  applyCacheConfig(csv_opts, opts);

  libvroom::CsvReader reader(csv_opts);
  libvroom::Result<bool> open_result;

  // Check for stdin input
  if (opts.input_path.empty() || isStdinInput(opts.input_path.c_str())) {
    auto buffer = libvroom::read_stdin_to_ptr();
    if (buffer.size() == 0) {
      // Empty stdin - just exit with success
      return 0;
    }
    open_result = reader.open_from_buffer(std::move(buffer));
  } else {
    open_result = reader.open(opts.input_path);
  }

  if (!open_result) {
    // Handle empty file gracefully - just exit with success
    if (open_result.error.find("Empty file") != string::npos) {
      return 0;
    }
    cerr << "Error: " << open_result.error << endl;
    return 1;
  }

  const auto& schema = reader.schema();

  auto read_result = reader.read_all();
  if (!read_result) {
    // Handle empty file gracefully
    if (read_result.error.find("Empty file") != string::npos) {
      return 0;
    }
    cerr << "Error: " << read_result.error << endl;
    return 1;
  }

  const auto& chunks = read_result.value.chunks;
  if (chunks.empty()) {
    return 0;
  }

  // Output header if present
  if (opts.has_header) {
    vector<string> header_row;
    for (const auto& col_schema : schema) {
      header_row.push_back(col_schema.name);
    }
    outputRow(header_row, opts.delimiter, opts.quote);
  }

  // Output data rows (limited to num_rows)
  size_t rows_output = 0;
  for (const auto& chunk : chunks) {
    if (chunk.empty())
      continue;
    size_t chunk_rows = chunk[0]->size();
    size_t num_cols = chunk.size();

    for (size_t row = 0; row < chunk_rows && rows_output < opts.num_rows; ++row) {
      vector<string> row_data;
      for (size_t col = 0; col < num_cols; ++col) {
        row_data.push_back(getColumnValueAsString(chunk[col].get(), row));
      }
      outputRow(row_data, opts.delimiter, opts.quote);
      ++rows_output;
    }
    if (rows_output >= opts.num_rows)
      break;
  }

  return 0;
}

// =============================================================================
// Command: info
// =============================================================================

int cmd_info(int argc, char* argv[]) {
  CommonOptions opts;

  // Skip "info" command
  int start = 1;
  if (argc > 1 && string(argv[1]) == "info") {
    start = 2;
  }

  if (parseCommonOptions(argc, argv, opts, start) < 0) {
    return 1;
  }

  // Set up CsvReader
  libvroom::CsvOptions csv_opts;
  csv_opts.separator = opts.delimiter;
  csv_opts.quote = opts.quote;
  csv_opts.has_header = opts.has_header;
  csv_opts.error_mode = opts.error_mode;
  csv_opts.max_errors = opts.max_errors;
  if (opts.num_threads > 0) {
    csv_opts.num_threads = opts.num_threads;
  }
  applyCacheConfig(csv_opts, opts);

  libvroom::CsvReader reader(csv_opts);
  libvroom::Result<bool> open_result;
  bool is_stdin = opts.input_path.empty() || isStdinInput(opts.input_path.c_str());
  size_t buffer_size = 0;

  // Check for stdin input
  if (is_stdin) {
    auto buffer = libvroom::read_stdin_to_ptr();
    buffer_size = buffer.size();
    if (buffer_size == 0) {
      // Empty stdin - show basic info
      cout << "Source: <stdin>\n";
      cout << "Size: 0 bytes\n";
      cout << "Dialect: delimiter=" << formatDelimiter(opts.delimiter)
           << ", quote=" << (opts.quote == '"' ? "double-quote" : string(1, opts.quote)) << '\n';
      cout << "Rows: 0\n";
      cout << "Columns: 0\n";
      return 0;
    }
    open_result = reader.open_from_buffer(std::move(buffer));
  } else {
    open_result = reader.open(opts.input_path);
  }

  // Handle empty file - still show basic info
  if (!open_result && open_result.error.find("Empty file") != string::npos) {
    cout << "Source: " << (is_stdin ? "<stdin>" : opts.input_path) << '\n';
    cout << "Size: 0 bytes\n";
    cout << "Dialect: delimiter=" << formatDelimiter(opts.delimiter)
         << ", quote=" << (opts.quote == '"' ? "double-quote" : string(1, opts.quote)) << '\n';
    cout << "Rows: 0\n";
    cout << "Columns: 0\n";
    return 0;
  }

  if (!open_result) {
    cerr << "Error: " << open_result.error << endl;
    return 1;
  }

  const auto& schema = reader.schema();

  auto read_result = reader.read_all();
  if (!read_result) {
    // Handle empty file
    if (read_result.error.find("Empty file") != string::npos) {
      cout << "Source: " << (is_stdin ? "<stdin>" : opts.input_path) << '\n';
      cout << "Size: 0 bytes\n";
      cout << "Dialect: delimiter=" << formatDelimiter(opts.delimiter)
           << ", quote=" << (opts.quote == '"' ? "double-quote" : string(1, opts.quote)) << '\n';
      cout << "Rows: 0\n";
      cout << "Columns: 0\n";
      return 0;
    }
    cerr << "Error: " << read_result.error << endl;
    return 1;
  }

  size_t row_count = read_result.value.total_rows;
  size_t col_count = schema.size();

  cout << "Source: " << (is_stdin ? "<stdin>" : opts.input_path) << '\n';

  // Try to get file size (only for non-stdin)
  if (!is_stdin) {
    libvroom::MmapSource source;
    if (source.open(opts.input_path)) {
      cout << "Size: " << source.size() << " bytes\n";
    }
  } else {
    cout << "Size: " << buffer_size << " bytes\n";
  }

  cout << "Dialect: delimiter=" << formatDelimiter(opts.delimiter)
       << ", quote=" << (opts.quote == '"' ? "double-quote" : string(1, opts.quote)) << '\n';
  cout << "Rows: " << row_count << '\n';
  cout << "Columns: " << col_count << '\n';

  if (opts.has_header && !schema.empty()) {
    cout << "\nColumn names:\n";
    for (size_t i = 0; i < schema.size(); ++i) {
      cout << "  " << i << ": " << schema[i].name << " (" << libvroom::type_name(schema[i].type)
           << ")\n";
    }
  }

  return 0;
}

// =============================================================================
// Command: select
// =============================================================================

int cmd_select(int argc, char* argv[]) {
  CommonOptions opts;

  // Skip "select" command
  int start = 1;
  if (argc > 1 && string(argv[1]) == "select") {
    start = 2;
  }

  if (parseCommonOptions(argc, argv, opts, start) < 0) {
    return 1;
  }

  if (opts.columns.empty()) {
    cerr << "Error: -c option required for select command" << endl;
    return 1;
  }

  // Set up CsvReader
  libvroom::CsvOptions csv_opts;
  csv_opts.separator = opts.delimiter;
  csv_opts.quote = opts.quote;
  csv_opts.has_header = opts.has_header;
  csv_opts.error_mode = opts.error_mode;
  csv_opts.max_errors = opts.max_errors;
  if (opts.num_threads > 0) {
    csv_opts.num_threads = opts.num_threads;
  }
  applyCacheConfig(csv_opts, opts);

  libvroom::CsvReader reader(csv_opts);
  libvroom::Result<bool> open_result;

  // Check for stdin input
  if (opts.input_path.empty() || isStdinInput(opts.input_path.c_str())) {
    auto buffer = libvroom::read_stdin_to_ptr();
    if (buffer.size() == 0) {
      // Empty stdin - just exit with success
      return 0;
    }
    open_result = reader.open_from_buffer(std::move(buffer));
  } else {
    open_result = reader.open(opts.input_path);
  }

  if (!open_result) {
    // Handle empty file gracefully
    if (open_result.error.find("Empty file") != string::npos) {
      return 0;
    }
    cerr << "Error: " << open_result.error << endl;
    return 1;
  }

  const auto& schema = reader.schema();

  // Parse column specification
  vector<size_t> col_indices;
  stringstream ss(opts.columns);
  string spec;
  while (getline(ss, spec, ',')) {
    // Trim whitespace
    size_t start_pos = spec.find_first_not_of(" \t");
    size_t end_pos = spec.find_last_not_of(" \t");
    if (start_pos == string::npos)
      continue;
    spec = spec.substr(start_pos, end_pos - start_pos + 1);

    // Try as numeric index first
    bool is_numeric = !spec.empty() && all_of(spec.begin(), spec.end(), ::isdigit);
    if (is_numeric) {
      size_t col_idx = stoul(spec);
      if (col_idx >= schema.size()) {
        cerr << "Error: Column index " << col_idx << " is out of range (file has " << schema.size()
             << " columns, indices 0-" << (schema.size() - 1) << ")" << endl;
        return 1;
      }
      col_indices.push_back(col_idx);
    } else if (opts.has_header) {
      // Find by name
      bool found = false;
      for (size_t i = 0; i < schema.size(); ++i) {
        if (schema[i].name == spec) {
          col_indices.push_back(i);
          found = true;
          break;
        }
      }
      if (!found) {
        cerr << "Error: Column '" << spec << "' not found in header" << endl;
        return 1;
      }
    } else {
      cerr << "Error: Cannot use column names without header (--no-header used)" << endl;
      return 1;
    }
  }

  if (col_indices.empty()) {
    cerr << "Error: No columns specified" << endl;
    return 1;
  }

  auto read_result = reader.read_all();
  if (!read_result) {
    cerr << "Error: " << read_result.error << endl;
    return 1;
  }

  const auto& chunks = read_result.value.chunks;

  // Output header if present
  if (opts.has_header) {
    vector<string> header_row;
    for (size_t col : col_indices) {
      header_row.push_back(schema[col].name);
    }
    outputRow(header_row, opts.delimiter, opts.quote);
  }

  // Output selected columns for each row
  for (const auto& chunk : chunks) {
    if (chunk.empty())
      continue;
    size_t chunk_rows = chunk[0]->size();

    for (size_t row = 0; row < chunk_rows; ++row) {
      vector<string> row_data;
      for (size_t col : col_indices) {
        row_data.push_back(getColumnValueAsString(chunk[col].get(), row));
      }
      outputRow(row_data, opts.delimiter, opts.quote);
    }
  }

  return 0;
}

// =============================================================================
// Command: pretty
// =============================================================================

int cmd_pretty(int argc, char* argv[]) {
  CommonOptions opts;

  // Skip "pretty" command
  int start = 1;
  if (argc > 1 && string(argv[1]) == "pretty") {
    start = 2;
  }

  if (parseCommonOptions(argc, argv, opts, start) < 0) {
    return 1;
  }

  // Set up CsvReader
  libvroom::CsvOptions csv_opts;
  csv_opts.separator = opts.delimiter;
  csv_opts.quote = opts.quote;
  csv_opts.has_header = opts.has_header;
  csv_opts.error_mode = opts.error_mode;
  csv_opts.max_errors = opts.max_errors;
  if (opts.num_threads > 0) {
    csv_opts.num_threads = opts.num_threads;
  }
  applyCacheConfig(csv_opts, opts);

  libvroom::CsvReader reader(csv_opts);
  libvroom::Result<bool> open_result;

  // Check for stdin input
  if (opts.input_path.empty() || isStdinInput(opts.input_path.c_str())) {
    auto buffer = libvroom::read_stdin_to_ptr();
    if (buffer.size() == 0) {
      // Empty stdin - just exit with success
      return 0;
    }
    open_result = reader.open_from_buffer(std::move(buffer));
  } else {
    open_result = reader.open(opts.input_path);
  }

  if (!open_result) {
    // Handle empty file gracefully
    if (open_result.error.find("Empty file") != string::npos) {
      return 0;
    }
    cerr << "Error: " << open_result.error << endl;
    return 1;
  }

  const auto& schema = reader.schema();

  auto read_result = reader.read_all();
  if (!read_result) {
    // Handle empty file gracefully
    if (read_result.error.find("Empty file") != string::npos) {
      return 0;
    }
    cerr << "Error: " << read_result.error << endl;
    return 1;
  }

  const auto& chunks = read_result.value.chunks;
  if (chunks.empty() && !opts.has_header) {
    return 0;
  }

  // Collect rows to display (header + num_rows data rows)
  vector<vector<string>> rows;

  // Add header row
  if (opts.has_header) {
    vector<string> header_row;
    for (const auto& col_schema : schema) {
      header_row.push_back(col_schema.name);
    }
    rows.push_back(header_row);
  }

  // Add data rows
  size_t rows_collected = 0;
  for (const auto& chunk : chunks) {
    if (chunk.empty())
      continue;
    size_t chunk_rows = chunk[0]->size();
    size_t num_cols = chunk.size();

    for (size_t row = 0; row < chunk_rows && rows_collected < opts.num_rows; ++row) {
      vector<string> row_data;
      for (size_t col = 0; col < num_cols; ++col) {
        row_data.push_back(getColumnValueAsString(chunk[col].get(), row));
      }
      rows.push_back(row_data);
      ++rows_collected;
    }
    if (rows_collected >= opts.num_rows)
      break;
  }

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
      widths[i] = max(widths[i], row[i].length());
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

      // Truncate if needed, adding "..." suffix
      if (val.length() > widths[i]) {
        if (widths[i] > 3) {
          val = val.substr(0, widths[i] - 3) + "...";
        } else {
          val = val.substr(0, widths[i]);
        }
      }

      cout << ' ' << val;
      if (val.length() < widths[i]) {
        cout << string(widths[i] - val.length(), ' ');
      }
      cout << " |";
    }
    cout << '\n';

    // Print separator after header
    if (opts.has_header && r == 0) {
      printSep();
    }
  }
  printSep();

  return 0;
}

// =============================================================================
// Stub Commands (not yet implemented)
// =============================================================================

int cmd_tail([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  cerr << "Error: 'tail' command not yet implemented" << endl;
  cerr << "Use 'head' with the file reversed, or use another tool for now." << endl;
  return 1;
}

int cmd_sample([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  cerr << "Error: 'sample' command not yet implemented" << endl;
  return 1;
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char* argv[]) {
  if (argc < 2) {
    print_usage();
    return 1;
  }

  string cmd = argv[1];

  if (cmd == "help" || cmd == "--help" || cmd == "-h") {
    print_usage();
    return 0;
  }

  if (cmd == "version" || cmd == "--version" || cmd == "-V") {
    print_version();
    return 0;
  }

  if (cmd == "convert") {
    return cmd_convert(argc, argv);
  }

  if (cmd == "count") {
    return cmd_count(argc, argv);
  }

  if (cmd == "head") {
    return cmd_head(argc, argv);
  }

  if (cmd == "info") {
    return cmd_info(argc, argv);
  }

  if (cmd == "select") {
    return cmd_select(argc, argv);
  }

  if (cmd == "pretty") {
    return cmd_pretty(argc, argv);
  }

  if (cmd == "tail") {
    return cmd_tail(argc, argv);
  }

  if (cmd == "sample") {
    return cmd_sample(argc, argv);
  }

  // Default: treat as convert command with file argument
  if (cmd[0] != '-' && cmd.find('.') != string::npos) {
    // Looks like a filename, run convert
    return cmd_convert(argc, argv);
  }

  cerr << "Error: Unknown command: " << cmd << endl;
  print_usage();
  return 1;
}
