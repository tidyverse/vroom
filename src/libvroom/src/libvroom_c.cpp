/**
 * @file libvroom_c.cpp
 * @brief C API wrapper implementation for the libvroom library.
 */

#include "libvroom_c.h"

#include "libvroom.h"

#include "common_defs.h"
#include "dialect.h"
#include "encoding.h"
#include "error.h"
#include "io_util.h"
#include "mem_util.h"
#include "value_extraction.h"

#include <cstring>
#include <limits>
#include <list>
#include <new>
#include <string>
#include <thread>
#include <vector>

// Internal structure for per-column configuration (needs to be defined before libvroom_parser)
struct libvroom_column_config {
  libvroom::ColumnConfigMap configs;
  // Store owned strings for NA values since ColumnConfig uses string_view.
  // Using std::list instead of std::vector to ensure stability of references
  // when new entries are added (vector reallocation would invalidate string_view pointers).
  std::list<std::vector<std::string>> owned_na_values;
};

// Internal structures wrapping C++ objects
struct libvroom_parser {
  libvroom::Parser parser;
  std::unique_ptr<libvroom_column_config> column_config;

  libvroom_parser(size_t num_threads = 1) : parser(num_threads) {}
};

struct libvroom_index {
  libvroom::ParseIndex idx;
  size_t num_threads;

  // Memory management for indexes loaded from file (not owned by ParseIndex's unique_ptrs)
  // These are only used when the index is loaded via libvroom_index_read()
  std::unique_ptr<uint64_t[]> external_n_indexes;
  std::unique_ptr<uint64_t[]> external_indexes;

  // Default constructor - index will be populated by Parser::parse()
  libvroom_index(size_t threads) : idx(), num_threads(threads) {}

  // Note: For indexes populated by Parser::parse(), ParseIndex's destructor handles cleanup.
  // For indexes loaded via libvroom_index_read(), the external_* unique_ptrs handle cleanup.
};

struct libvroom_buffer {
  std::vector<uint8_t> data;
  size_t original_length; // Length of the original data (without padding)

  libvroom_buffer(const uint8_t* ptr, size_t len) : original_length(len) {
    // Allocate space for data + SIMD padding to allow safe 64-byte reads
    data.resize(len + LIBVROOM_PADDING);
    std::memcpy(data.data(), ptr, len);
    // Zero the padding bytes to avoid undefined behavior in SIMD comparisons
    std::memset(data.data() + len, 0, LIBVROOM_PADDING);
  }
  libvroom_buffer() : original_length(0) {}
};

struct libvroom_dialect {
  libvroom::Dialect dialect;

  libvroom_dialect() = default;
  libvroom_dialect(const libvroom::Dialect& d) : dialect(d) {}
};

struct libvroom_error_collector {
  libvroom::ErrorCollector collector;

  libvroom_error_collector(libvroom::ErrorMode mode, size_t max_errors)
      : collector(mode, max_errors) {}
};

struct libvroom_detection_result {
  libvroom::DetectionResult result;
  std::string warning_str;

  libvroom_detection_result(const libvroom::DetectionResult& r) : result(r) {
    if (!r.warning.empty()) {
      warning_str = r.warning;
    }
  }
};

struct libvroom_load_result {
  LoadResult cpp_result;

  libvroom_load_result(LoadResult&& r) : cpp_result(std::move(r)) {}
};

struct libvroom_lazy_column {
  // Store references to the underlying data
  const uint8_t* buf;
  size_t buf_len;
  const libvroom::ParseIndex* idx;
  size_t col;
  bool has_header;
  libvroom::Dialect dialect;

  // Cached row count
  size_t num_rows;

  libvroom_lazy_column(const uint8_t* buffer, size_t length, const libvroom::ParseIndex* index,
                       size_t column, bool header, const libvroom::Dialect& d)
      : buf(buffer), buf_len(length), idx(index), col(column), has_header(header), dialect(d),
        num_rows(0) {
    // Compute number of rows
    if (idx->columns > 0) {
      uint64_t total_fields = idx->total_indexes();
      uint64_t total_rows = total_fields / idx->columns;
      num_rows = has_header ? (total_rows > 0 ? total_rows - 1 : 0) : total_rows;
    }
  }
};

// Helper functions to convert between C and C++ types
static libvroom::ErrorMode to_cpp_mode(libvroom_error_mode_t mode) {
  switch (mode) {
  case LIBVROOM_MODE_STRICT:
    return libvroom::ErrorMode::FAIL_FAST;
  case LIBVROOM_MODE_PERMISSIVE:
    return libvroom::ErrorMode::PERMISSIVE;
  case LIBVROOM_MODE_BEST_EFFORT:
    return libvroom::ErrorMode::BEST_EFFORT;
  default:
    return libvroom::ErrorMode::FAIL_FAST;
  }
}

static libvroom_error_mode_t to_c_mode(libvroom::ErrorMode mode) {
  switch (mode) {
  case libvroom::ErrorMode::FAIL_FAST:
    return LIBVROOM_MODE_STRICT;
  case libvroom::ErrorMode::PERMISSIVE:
    return LIBVROOM_MODE_PERMISSIVE;
  case libvroom::ErrorMode::BEST_EFFORT:
    return LIBVROOM_MODE_BEST_EFFORT;
  default:
    return LIBVROOM_MODE_STRICT;
  }
}

static libvroom_error_t to_c_error(libvroom::ErrorCode code) {
  switch (code) {
  case libvroom::ErrorCode::NONE:
    return LIBVROOM_OK;
  case libvroom::ErrorCode::UNCLOSED_QUOTE:
    return LIBVROOM_ERROR_UNCLOSED_QUOTE;
  case libvroom::ErrorCode::INVALID_QUOTE_ESCAPE:
    return LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE;
  case libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD:
    return LIBVROOM_ERROR_QUOTE_IN_UNQUOTED;
  case libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT:
    return LIBVROOM_ERROR_INCONSISTENT_FIELDS;
  case libvroom::ErrorCode::FIELD_TOO_LARGE:
    return LIBVROOM_ERROR_FIELD_TOO_LARGE;
  case libvroom::ErrorCode::MIXED_LINE_ENDINGS:
    return LIBVROOM_ERROR_MIXED_LINE_ENDINGS;
  case libvroom::ErrorCode::INVALID_UTF8:
    return LIBVROOM_ERROR_INVALID_UTF8;
  case libvroom::ErrorCode::NULL_BYTE:
    return LIBVROOM_ERROR_NULL_BYTE;
  case libvroom::ErrorCode::EMPTY_HEADER:
    return LIBVROOM_ERROR_EMPTY_HEADER;
  case libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES:
    return LIBVROOM_ERROR_DUPLICATE_COLUMNS;
  case libvroom::ErrorCode::AMBIGUOUS_SEPARATOR:
    return LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR;
  case libvroom::ErrorCode::FILE_TOO_LARGE:
    return LIBVROOM_ERROR_FILE_TOO_LARGE;
  case libvroom::ErrorCode::IO_ERROR:
    return LIBVROOM_ERROR_IO;
  case libvroom::ErrorCode::INTERNAL_ERROR:
    return LIBVROOM_ERROR_INTERNAL;
  default:
    return LIBVROOM_ERROR_INTERNAL;
  }
}

static libvroom_severity_t to_c_severity(libvroom::ErrorSeverity severity) {
  switch (severity) {
  case libvroom::ErrorSeverity::WARNING:
    return LIBVROOM_SEVERITY_WARNING;
  case libvroom::ErrorSeverity::RECOVERABLE:
    return LIBVROOM_SEVERITY_ERROR;
  case libvroom::ErrorSeverity::FATAL:
    return LIBVROOM_SEVERITY_FATAL;
  default:
    return LIBVROOM_SEVERITY_ERROR;
  }
}

// Version
const char* libvroom_version(void) {
  static const char version[] = "0.1.0";
  return version;
}

// Error strings
const char* libvroom_error_string(libvroom_error_t error) {
  switch (error) {
  case LIBVROOM_OK:
    return "No error";
  case LIBVROOM_ERROR_UNCLOSED_QUOTE:
    return "Unclosed quote";
  case LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE:
    return "Invalid quote escape";
  case LIBVROOM_ERROR_QUOTE_IN_UNQUOTED:
    return "Quote in unquoted field";
  case LIBVROOM_ERROR_INCONSISTENT_FIELDS:
    return "Inconsistent field count";
  case LIBVROOM_ERROR_FIELD_TOO_LARGE:
    return "Field too large";
  case LIBVROOM_ERROR_MIXED_LINE_ENDINGS:
    return "Mixed line endings";
  case LIBVROOM_ERROR_INVALID_UTF8:
    return "Invalid UTF-8";
  case LIBVROOM_ERROR_NULL_BYTE:
    return "Null byte in data";
  case LIBVROOM_ERROR_EMPTY_HEADER:
    return "Empty header";
  case LIBVROOM_ERROR_DUPLICATE_COLUMNS:
    return "Duplicate columns";
  case LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR:
    return "Ambiguous separator";
  case LIBVROOM_ERROR_FILE_TOO_LARGE:
    return "File too large";
  case LIBVROOM_ERROR_IO:
    return "I/O error";
  case LIBVROOM_ERROR_INTERNAL:
    return "Internal error";
  case LIBVROOM_ERROR_NULL_POINTER:
    return "Null pointer";
  case LIBVROOM_ERROR_INVALID_ARGUMENT:
    return "Invalid argument";
  case LIBVROOM_ERROR_OUT_OF_MEMORY:
    return "Out of memory";
  case LIBVROOM_ERROR_INVALID_HANDLE:
    return "Invalid handle";
  case LIBVROOM_ERROR_CANCELLED:
    return "Operation cancelled";
  default:
    return "Unknown error";
  }
}

// Buffer Management
libvroom_buffer_t* libvroom_buffer_load_file(const char* filename) {
  if (!filename)
    return nullptr;

  try {
    auto [ptr, size] = read_file(filename, LIBVROOM_PADDING);
    if (size == 0)
      return nullptr;

    auto* buffer = new (std::nothrow) libvroom_buffer();
    if (!buffer) {
      return nullptr;
    }

    // Store the original length (data size without padding)
    buffer->original_length = size;
    // Copy from buffer into vector
    buffer->data.assign(ptr.get(), ptr.get() + size);
    // Memory automatically freed when ptr goes out of scope
    return buffer;
  } catch (...) {
    return nullptr;
  }
}

libvroom_buffer_t* libvroom_buffer_create(const uint8_t* data, size_t length) {
  if (!data || length == 0)
    return nullptr;

  try {
    return new (std::nothrow) libvroom_buffer(data, length);
  } catch (...) {
    return nullptr;
  }
}

const uint8_t* libvroom_buffer_data(const libvroom_buffer_t* buffer) {
  if (!buffer)
    return nullptr;
  return buffer->data.data();
}

size_t libvroom_buffer_length(const libvroom_buffer_t* buffer) {
  if (!buffer)
    return 0;
  return buffer->original_length; // Return original length, not padded size
}

void libvroom_buffer_destroy(libvroom_buffer_t* buffer) {
  delete buffer;
}

// Dialect Configuration
libvroom_dialect_t* libvroom_dialect_create(char delimiter, char quote_char, char escape_char,
                                            bool double_quote) {
  try {
    auto* d = new (std::nothrow) libvroom_dialect();
    if (d) {
      d->dialect.delimiter = delimiter;
      d->dialect.quote_char = quote_char;
      d->dialect.escape_char = escape_char;
      d->dialect.double_quote = double_quote;
    }
    return d;
  } catch (...) {
    return nullptr;
  }
}

char libvroom_dialect_delimiter(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return '\0';
  return dialect->dialect.delimiter;
}

char libvroom_dialect_quote_char(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return '\0';
  return dialect->dialect.quote_char;
}

char libvroom_dialect_escape_char(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return '\0';
  return dialect->dialect.escape_char;
}

bool libvroom_dialect_double_quote(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return false;
  return dialect->dialect.double_quote;
}

void libvroom_dialect_destroy(libvroom_dialect_t* dialect) {
  delete dialect;
}

// Error Collector
libvroom_error_collector_t* libvroom_error_collector_create(libvroom_error_mode_t mode,
                                                            size_t max_errors) {
  try {
    return new (std::nothrow) libvroom_error_collector(to_cpp_mode(mode), max_errors);
  } catch (...) {
    return nullptr;
  }
}

libvroom_error_mode_t libvroom_error_collector_mode(const libvroom_error_collector_t* collector) {
  if (!collector)
    return LIBVROOM_MODE_STRICT;
  return to_c_mode(collector->collector.mode());
}

bool libvroom_error_collector_has_errors(const libvroom_error_collector_t* collector) {
  if (!collector)
    return false;
  return collector->collector.has_errors();
}

bool libvroom_error_collector_has_fatal(const libvroom_error_collector_t* collector) {
  if (!collector)
    return false;
  return collector->collector.has_fatal_errors();
}

size_t libvroom_error_collector_count(const libvroom_error_collector_t* collector) {
  if (!collector)
    return 0;
  return collector->collector.errors().size();
}

libvroom_error_t libvroom_error_collector_get(const libvroom_error_collector_t* collector,
                                              size_t index, libvroom_parse_error_t* error) {
  if (!collector)
    return LIBVROOM_ERROR_NULL_POINTER;
  if (!error)
    return LIBVROOM_ERROR_NULL_POINTER;

  const auto& errors = collector->collector.errors();
  if (index >= errors.size())
    return LIBVROOM_ERROR_INVALID_ARGUMENT;

  const auto& e = errors[index];
  error->code = to_c_error(e.code);
  error->severity = to_c_severity(e.severity);
  error->line = e.line;
  error->column = e.column;
  error->byte_offset = e.byte_offset;
  error->message = e.message.c_str();
  error->context = e.context.c_str();

  return LIBVROOM_OK;
}

void libvroom_error_collector_clear(libvroom_error_collector_t* collector) {
  if (!collector)
    return;
  collector->collector.clear();
}

char* libvroom_error_collector_summary(const libvroom_error_collector_t* collector) {
  if (!collector)
    return nullptr;

  try {
    std::string summary = collector->collector.summary();

    // Allocate a copy that the caller can free()
    char* result = static_cast<char*>(std::malloc(summary.size() + 1));
    if (!result)
      return nullptr;

    std::memcpy(result, summary.c_str(), summary.size() + 1);
    return result;
  } catch (...) {
    return nullptr;
  }
}

void libvroom_error_collector_destroy(libvroom_error_collector_t* collector) {
  delete collector;
}

// Index Structure
libvroom_index_t* libvroom_index_create(size_t buffer_length, size_t num_threads) {
  // Note: buffer_length is now ignored since Parser allocates the index internally
  (void)buffer_length; // Suppress unused parameter warning
  if (num_threads == 0)
    return nullptr;

  try {
    return new (std::nothrow) libvroom_index(num_threads);
  } catch (...) {
    return nullptr;
  }
}

size_t libvroom_index_num_threads(const libvroom_index_t* index) {
  if (!index)
    return 0;
  return index->num_threads;
}

size_t libvroom_index_columns(const libvroom_index_t* index) {
  if (!index)
    return 0;
  return index->idx.columns;
}

uint64_t libvroom_index_count(const libvroom_index_t* index, size_t thread_id) {
  if (!index)
    return 0;
  if (thread_id >= index->num_threads)
    return 0;
  if (!index->idx.n_indexes)
    return 0;
  return index->idx.n_indexes[thread_id];
}

uint64_t libvroom_index_total_count(const libvroom_index_t* index) {
  if (!index)
    return 0;
  if (!index->idx.n_indexes)
    return 0;
  uint64_t total = 0;
  for (size_t i = 0; i < index->num_threads; ++i) {
    total += index->idx.n_indexes[i];
  }
  return total;
}

const uint64_t* libvroom_index_positions(const libvroom_index_t* index) {
  if (!index)
    return nullptr;
  return index->idx.indexes;
}

void libvroom_index_destroy(libvroom_index_t* index) {
  delete index;
}

void libvroom_index_compact(libvroom_index_t* index) {
  if (index) {
    index->idx.compact();
  }
}

bool libvroom_index_is_flat(const libvroom_index_t* index) {
  if (!index)
    return false;
  return index->idx.is_flat();
}

libvroom_error_t libvroom_index_write(const libvroom_index_t* index, const char* filename) {
  if (!index || !filename)
    return LIBVROOM_ERROR_NULL_POINTER;

  // Check if the index has been populated (has valid data)
  if (!index->idx.indexes || !index->idx.n_indexes)
    return LIBVROOM_ERROR_INVALID_HANDLE;

  try {
    // Use a const_cast here because ParseIndex::write() is not const-qualified,
    // but it doesn't actually modify the index. We need the cast because we're
    // implementing a const C API function.
    const_cast<libvroom::ParseIndex&>(index->idx).write(filename);
    return LIBVROOM_OK;
  } catch (const std::runtime_error&) {
    return LIBVROOM_ERROR_IO;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_index_t* libvroom_index_read(const char* filename) {
  if (!filename)
    return nullptr;

  std::FILE* fp = std::fopen(filename, "rb");
  if (!fp)
    return nullptr;

  try {
    // Index file format version for backward compatibility
    // Version 1 (legacy): columns (uint64_t), n_threads (uint8_t), n_indexes, indexes
    // Version 2: version (uint8_t=2), columns (uint64_t), n_threads (uint16_t), n_indexes, indexes
    static constexpr uint8_t INDEX_FORMAT_VERSION = 2;

    uint64_t columns = 0;
    uint16_t n_threads = 0;

    // Read first byte to detect version
    uint8_t first_byte;
    if (std::fread(&first_byte, sizeof(uint8_t), 1, fp) != 1) {
      std::fclose(fp);
      return nullptr;
    }

    if (first_byte == INDEX_FORMAT_VERSION) {
      // Version 2 format: read columns, n_threads (16-bit)
      if (std::fread(&columns, sizeof(uint64_t), 1, fp) != 1 ||
          std::fread(&n_threads, sizeof(uint16_t), 1, fp) != 1) {
        std::fclose(fp);
        return nullptr;
      }
    } else {
      // Version 1 (legacy) format: first_byte is part of columns
      uint8_t columns_rest[7];
      if (std::fread(columns_rest, 1, 7, fp) != 7) {
        std::fclose(fp);
        return nullptr;
      }
      // Reconstruct columns from first_byte + columns_rest (little-endian)
      columns = first_byte;
      for (int i = 0; i < 7; ++i) {
        columns |= static_cast<uint64_t>(columns_rest[i]) << (8 * (i + 1));
      }
      // Read n_threads as uint8_t
      uint8_t n_threads_v1;
      if (std::fread(&n_threads_v1, sizeof(uint8_t), 1, fp) != 1) {
        std::fclose(fp);
        return nullptr;
      }
      n_threads = n_threads_v1;
    }

    // Validate n_threads
    if (n_threads == 0) {
      std::fclose(fp);
      return nullptr;
    }

    // Create the index wrapper using unique_ptr for RAII-safe cleanup
    std::unique_ptr<libvroom_index> index;
    try {
      index = std::make_unique<libvroom_index>(n_threads);
    } catch (...) {
      std::fclose(fp);
      return nullptr;
    }

    // Allocate and populate the ParseIndex
    index->idx.columns = columns;
    index->idx.n_threads = n_threads;

    // Allocate n_indexes array
    auto n_indexes_ptr = std::make_unique<uint64_t[]>(n_threads);
    if (std::fread(n_indexes_ptr.get(), sizeof(uint64_t), n_threads, fp) != n_threads) {
      std::fclose(fp);
      return nullptr;
    }

    // Calculate total indexes size with overflow checking
    size_t total_size = 0;
    for (uint16_t i = 0; i < n_threads; ++i) {
      // Check for overflow before adding
      if (n_indexes_ptr[i] > std::numeric_limits<size_t>::max() - total_size) {
        std::fclose(fp);
        return nullptr;
      }
      total_size += n_indexes_ptr[i];
    }

    // Validate that total_size won't cause issues with memory allocation
    // total_size * sizeof(uint64_t) must not overflow
    if (total_size > std::numeric_limits<size_t>::max() / sizeof(uint64_t)) {
      std::fclose(fp);
      return nullptr;
    }

    // Allocate indexes array
    auto indexes_ptr = std::make_unique<uint64_t[]>(total_size);
    if (std::fread(indexes_ptr.get(), sizeof(uint64_t), total_size, fp) != total_size) {
      std::fclose(fp);
      return nullptr;
    }

    std::fclose(fp);

    // Transfer ownership to the external memory managers in libvroom_index
    // This ensures proper cleanup when the index is destroyed
    index->external_n_indexes = std::move(n_indexes_ptr);
    index->external_indexes = std::move(indexes_ptr);

    // Set the raw pointers in ParseIndex to point to the externally-managed memory
    index->idx.n_indexes = index->external_n_indexes.get();
    index->idx.indexes = index->external_indexes.get();

    return index.release();
  } catch (...) {
    std::fclose(fp);
    return nullptr;
  }
}

// Parser
libvroom_parser_t* libvroom_parser_create(void) {
  try {
    return new (std::nothrow) libvroom_parser(1);
  } catch (...) {
    return nullptr;
  }
}

libvroom_error_t libvroom_parse(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                libvroom_index_t* index, libvroom_error_collector_t* errors,
                                const libvroom_dialect_t* dialect) {
  if (!parser || !buffer || !index)
    return LIBVROOM_ERROR_NULL_POINTER;

  try {
    libvroom::Dialect d = dialect ? dialect->dialect : libvroom::Dialect::csv();

    // Configure parser with the number of threads from the index
    parser->parser.set_num_threads(index->num_threads);

    // Build parse options
    libvroom::ParseOptions options;
    options.dialect = d;
    if (errors) {
      options.errors = &errors->collector;
    }

    // Parse using the unified Parser API
    // Use original_length (not padded data.size()) for correct parsing
    auto result = parser->parser.parse(buffer->data.data(), buffer->original_length, options);

    // Move the index from the result
    index->idx = std::move(result.idx);

    if (errors && errors->collector.has_fatal_errors()) {
      const auto& errs = errors->collector.errors();
      for (const auto& e : errs) {
        if (e.severity == libvroom::ErrorSeverity::FATAL) {
          return to_c_error(e.code);
        }
      }
    }

    return result.success() ? LIBVROOM_OK : LIBVROOM_ERROR_INTERNAL;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_error_t
libvroom_parse_with_progress(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                             libvroom_index_t* index, libvroom_error_collector_t* errors,
                             const libvroom_dialect_t* dialect,
                             libvroom_progress_callback_t progress, void* user_data) {
  if (!parser || !buffer || !index)
    return LIBVROOM_ERROR_NULL_POINTER;

  try {
    libvroom::Dialect d = dialect ? dialect->dialect : libvroom::Dialect::csv();

    // Configure parser with the number of threads from the index
    parser->parser.set_num_threads(index->num_threads);

    // Build parse options
    libvroom::ParseOptions options;
    options.dialect = d;
    if (errors) {
      options.errors = &errors->collector;
    }

    // Set up progress callback wrapper if provided
    if (progress) {
      options.progress_callback = [progress, user_data](size_t bytes_processed,
                                                        size_t total_bytes) {
        return progress(bytes_processed, total_bytes, user_data);
      };
    }

    // Parse using the unified Parser API
    // Use original_length (not padded data.size()) for correct parsing
    auto result = parser->parser.parse(buffer->data.data(), buffer->original_length, options);

    // Move the index from the result
    index->idx = std::move(result.idx);

    // Check if parsing was cancelled (result not successful but no fatal errors)
    if (!result.success() && (!errors || !errors->collector.has_fatal_errors())) {
      // Likely cancelled by progress callback
      return LIBVROOM_ERROR_CANCELLED;
    }

    if (errors && errors->collector.has_fatal_errors()) {
      const auto& errs = errors->collector.errors();
      for (const auto& e : errs) {
        if (e.severity == libvroom::ErrorSeverity::FATAL) {
          return to_c_error(e.code);
        }
      }
    }

    return result.success() ? LIBVROOM_OK : LIBVROOM_ERROR_INTERNAL;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

void libvroom_parser_destroy(libvroom_parser_t* parser) {
  delete parser;
}

libvroom_error_t libvroom_parser_set_column_config(libvroom_parser_t* parser,
                                                   const libvroom_column_config_t* config) {
  if (!parser) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }

  try {
    if (config) {
      // Copy the config
      parser->column_config = std::make_unique<libvroom_column_config>();
      parser->column_config->configs = config->configs;
      parser->column_config->owned_na_values = config->owned_na_values;
    } else {
      parser->column_config.reset();
    }
    return LIBVROOM_OK;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_error_t libvroom_parse_filtered(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                         libvroom_index_t* index,
                                         libvroom_error_collector_t* errors,
                                         const libvroom_dialect_t* dialect,
                                         const libvroom_row_filter_options_t* filter) {
  if (!parser || !buffer || !index)
    return LIBVROOM_ERROR_NULL_POINTER;

  try {
    libvroom::Dialect d = dialect ? dialect->dialect : libvroom::Dialect::csv();

    // Configure parser with the number of threads from the index
    parser->parser.set_num_threads(index->num_threads);

    // Build parse options
    libvroom::ParseOptions options;
    options.dialect = d;
    if (errors) {
      options.errors = &errors->collector;
    }

    // Apply row filtering options if provided
    if (filter) {
      options.skip = filter->skip;
      options.n_max = filter->n_max;
      options.comment = filter->comment;
      options.skip_empty_rows = filter->skip_empty_rows;
    }

    // Parse using the unified Parser API
    // Use original_length (not padded data.size()) for correct parsing
    auto result = parser->parser.parse(buffer->data.data(), buffer->original_length, options);

    // Move the index from the result
    index->idx = std::move(result.idx);

    if (errors && errors->collector.has_fatal_errors()) {
      const auto& errs = errors->collector.errors();
      for (const auto& e : errs) {
        if (e.severity == libvroom::ErrorSeverity::FATAL) {
          return to_c_error(e.code);
        }
      }
    }

    return result.success() ? LIBVROOM_OK : LIBVROOM_ERROR_INTERNAL;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

const libvroom_column_config_t* libvroom_parser_get_column_config(const libvroom_parser_t* parser) {
  if (!parser) {
    return nullptr;
  }
  return parser->column_config.get();
}

libvroom_error_t libvroom_parser_clear_column_config(libvroom_parser_t* parser) {
  return libvroom_parser_set_column_config(parser,
                                           static_cast<const libvroom_column_config_t*>(nullptr));
}

// Dialect Detection
libvroom_detection_result_t* libvroom_detect_dialect(const libvroom_buffer_t* buffer) {
  if (!buffer)
    return nullptr;

  try {
    libvroom::DialectDetector detector;
    auto result = detector.detect(buffer->data.data(), buffer->original_length);
    return new (std::nothrow) libvroom_detection_result(result);
  } catch (...) {
    return nullptr;
  }
}

libvroom_detection_result_t* libvroom_detect_dialect_file(const char* filename) {
  if (!filename)
    return nullptr;

  try {
    libvroom::DialectDetector detector;
    auto result = detector.detect_file(filename);
    return new (std::nothrow) libvroom_detection_result(result);
  } catch (...) {
    return nullptr;
  }
}

bool libvroom_detection_result_success(const libvroom_detection_result_t* result) {
  if (!result)
    return false;
  return result->result.success();
}

double libvroom_detection_result_confidence(const libvroom_detection_result_t* result) {
  if (!result)
    return 0.0;
  return result->result.confidence;
}

libvroom_dialect_t* libvroom_detection_result_dialect(const libvroom_detection_result_t* result) {
  if (!result)
    return nullptr;

  try {
    auto* d = new (std::nothrow) libvroom_dialect(result->result.dialect);
    return d;
  } catch (...) {
    return nullptr;
  }
}

size_t libvroom_detection_result_columns(const libvroom_detection_result_t* result) {
  if (!result)
    return 0;
  return result->result.detected_columns;
}

size_t libvroom_detection_result_rows_analyzed(const libvroom_detection_result_t* result) {
  if (!result)
    return 0;
  return result->result.rows_analyzed;
}

bool libvroom_detection_result_has_header(const libvroom_detection_result_t* result) {
  if (!result)
    return false;
  return result->result.has_header;
}

const char* libvroom_detection_result_warning(const libvroom_detection_result_t* result) {
  if (!result)
    return nullptr;
  if (result->warning_str.empty())
    return nullptr;
  return result->warning_str.c_str();
}

void libvroom_detection_result_destroy(libvroom_detection_result_t* result) {
  delete result;
}

libvroom_error_t libvroom_parse_auto(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                     libvroom_index_t* index, libvroom_error_collector_t* errors,
                                     libvroom_detection_result_t** detected) {
  if (!parser || !buffer || !index)
    return LIBVROOM_ERROR_NULL_POINTER;

  try {
    // Configure parser with the number of threads from the index
    parser->parser.set_num_threads(index->num_threads);

    // Build parse options for auto-detection (dialect = nullopt)
    libvroom::ParseOptions options;
    // Leave dialect as nullopt for auto-detection
    if (errors) {
      options.errors = &errors->collector;
    }

    // Parse using the unified Parser API with auto-detection
    // Use original_length (not padded data.size()) for correct parsing
    auto result = parser->parser.parse(buffer->data.data(), buffer->original_length, options);

    // Store detection result if requested
    if (detected) {
      *detected = new (std::nothrow) libvroom_detection_result(result.detection);
    }

    // Check if detection succeeded
    if (!result.detection.success()) {
      return LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR;
    }

    // Move the index from the result
    index->idx = std::move(result.idx);

    return result.success() ? LIBVROOM_OK : LIBVROOM_ERROR_INTERNAL;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

// Utility Functions
size_t libvroom_recommended_threads(void) {
  return std::thread::hardware_concurrency();
}

size_t libvroom_simd_padding(void) {
  return LIBVROOM_PADDING;
}

// Encoding helper functions
static libvroom_encoding_t to_c_encoding(libvroom::Encoding enc) {
  switch (enc) {
  case libvroom::Encoding::UTF8:
    return LIBVROOM_ENCODING_UTF8;
  case libvroom::Encoding::UTF8_BOM:
    return LIBVROOM_ENCODING_UTF8_BOM;
  case libvroom::Encoding::UTF16_LE:
    return LIBVROOM_ENCODING_UTF16_LE;
  case libvroom::Encoding::UTF16_BE:
    return LIBVROOM_ENCODING_UTF16_BE;
  case libvroom::Encoding::UTF32_LE:
    return LIBVROOM_ENCODING_UTF32_LE;
  case libvroom::Encoding::UTF32_BE:
    return LIBVROOM_ENCODING_UTF32_BE;
  case libvroom::Encoding::LATIN1:
    return LIBVROOM_ENCODING_LATIN1;
  case libvroom::Encoding::UNKNOWN:
  default:
    return LIBVROOM_ENCODING_UNKNOWN;
  }
}

// Encoding Detection and Transcoding
const char* libvroom_encoding_string(libvroom_encoding_t encoding) {
  switch (encoding) {
  case LIBVROOM_ENCODING_UTF8:
    return "UTF-8";
  case LIBVROOM_ENCODING_UTF8_BOM:
    return "UTF-8 (BOM)";
  case LIBVROOM_ENCODING_UTF16_LE:
    return "UTF-16LE";
  case LIBVROOM_ENCODING_UTF16_BE:
    return "UTF-16BE";
  case LIBVROOM_ENCODING_UTF32_LE:
    return "UTF-32LE";
  case LIBVROOM_ENCODING_UTF32_BE:
    return "UTF-32BE";
  case LIBVROOM_ENCODING_LATIN1:
    return "Latin-1";
  case LIBVROOM_ENCODING_UNKNOWN:
  default:
    return "Unknown";
  }
}

libvroom_error_t libvroom_detect_encoding(const uint8_t* data, size_t length,
                                          libvroom_encoding_result_t* result) {
  if (!result)
    return LIBVROOM_ERROR_NULL_POINTER;

  // detect_encoding handles null data gracefully
  auto cpp_result = libvroom::detect_encoding(data, length);

  result->encoding = to_c_encoding(cpp_result.encoding);
  result->bom_length = cpp_result.bom_length;
  result->confidence = cpp_result.confidence;
  result->needs_transcoding = cpp_result.needs_transcoding;

  return LIBVROOM_OK;
}

libvroom_load_result_t* libvroom_load_file_with_encoding(const char* filename) {
  if (!filename)
    return nullptr;

  try {
    auto result = read_file_with_encoding(filename, LIBVROOM_PADDING);
    if (!result.valid()) {
      return nullptr;
    }

    return new (std::nothrow) libvroom_load_result(std::move(result));
  } catch (...) {
    return nullptr;
  }
}

const uint8_t* libvroom_load_result_data(const libvroom_load_result_t* result) {
  if (!result)
    return nullptr;
  return result->cpp_result.data();
}

size_t libvroom_load_result_length(const libvroom_load_result_t* result) {
  if (!result)
    return 0;
  return result->cpp_result.size;
}

libvroom_encoding_t libvroom_load_result_encoding(const libvroom_load_result_t* result) {
  if (!result)
    return LIBVROOM_ENCODING_UNKNOWN;
  return to_c_encoding(result->cpp_result.encoding.encoding);
}

size_t libvroom_load_result_bom_length(const libvroom_load_result_t* result) {
  if (!result)
    return 0;
  return result->cpp_result.encoding.bom_length;
}

double libvroom_load_result_confidence(const libvroom_load_result_t* result) {
  if (!result)
    return 0.0;
  return result->cpp_result.encoding.confidence;
}

bool libvroom_load_result_was_transcoded(const libvroom_load_result_t* result) {
  if (!result)
    return false;
  // Data was transformed if either:
  // 1. needs_transcoding is true (UTF-16/UTF-32 -> UTF-8)
  // 2. BOM was present and stripped (includes UTF-8 BOM)
  return result->cpp_result.encoding.needs_transcoding ||
         result->cpp_result.encoding.bom_length > 0;
}

libvroom_buffer_t* libvroom_load_result_to_buffer(const libvroom_load_result_t* result) {
  if (!result || !result->cpp_result.valid())
    return nullptr;

  try {
    return new (std::nothrow) libvroom_buffer(result->cpp_result.data(), result->cpp_result.size);
  } catch (...) {
    return nullptr;
  }
}

void libvroom_load_result_destroy(libvroom_load_result_t* result) {
  delete result;
}

// =============================================================================
// Per-Column Configuration API
// =============================================================================

// Note: libvroom_column_config struct is defined at the top of the file

// Helper to convert C type hint to C++ TypeHint
static libvroom::TypeHint c_type_hint_to_cpp(libvroom_type_hint_t hint) {
  switch (hint) {
  case LIBVROOM_TYPE_AUTO:
    return libvroom::TypeHint::AUTO;
  case LIBVROOM_TYPE_BOOLEAN:
    return libvroom::TypeHint::BOOLEAN;
  case LIBVROOM_TYPE_INTEGER:
    return libvroom::TypeHint::INTEGER;
  case LIBVROOM_TYPE_DOUBLE:
    return libvroom::TypeHint::DOUBLE;
  case LIBVROOM_TYPE_STRING:
    return libvroom::TypeHint::STRING;
  case LIBVROOM_TYPE_DATE:
    return libvroom::TypeHint::DATE;
  case LIBVROOM_TYPE_DATETIME:
    return libvroom::TypeHint::DATETIME;
  case LIBVROOM_TYPE_SKIP:
    return libvroom::TypeHint::SKIP;
  default:
    return libvroom::TypeHint::AUTO;
  }
}

// Helper to convert C++ TypeHint to C type hint
static libvroom_type_hint_t cpp_type_hint_to_c(libvroom::TypeHint hint) {
  switch (hint) {
  case libvroom::TypeHint::AUTO:
    return LIBVROOM_TYPE_AUTO;
  case libvroom::TypeHint::BOOLEAN:
    return LIBVROOM_TYPE_BOOLEAN;
  case libvroom::TypeHint::INTEGER:
    return LIBVROOM_TYPE_INTEGER;
  case libvroom::TypeHint::DOUBLE:
    return LIBVROOM_TYPE_DOUBLE;
  case libvroom::TypeHint::STRING:
    return LIBVROOM_TYPE_STRING;
  case libvroom::TypeHint::DATE:
    return LIBVROOM_TYPE_DATE;
  case libvroom::TypeHint::DATETIME:
    return LIBVROOM_TYPE_DATETIME;
  case libvroom::TypeHint::SKIP:
    return LIBVROOM_TYPE_SKIP;
  default:
    return LIBVROOM_TYPE_AUTO;
  }
}

libvroom_column_config_t* libvroom_column_config_create(void) {
  try {
    return new (std::nothrow) libvroom_column_config{};
  } catch (...) {
    return nullptr;
  }
}

libvroom_error_t libvroom_column_config_set_type_by_index(libvroom_column_config_t* config,
                                                          size_t col_index,
                                                          libvroom_type_hint_t type_hint) {
  if (!config) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }

  try {
    libvroom::ColumnConfig col_config;
    col_config.type_hint = c_type_hint_to_cpp(type_hint);
    config->configs.set(col_index, col_config);
    return LIBVROOM_OK;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_error_t libvroom_column_config_set_type_by_name(libvroom_column_config_t* config,
                                                         const char* col_name,
                                                         libvroom_type_hint_t type_hint) {
  if (!config || !col_name) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }

  try {
    libvroom::ColumnConfig col_config;
    col_config.type_hint = c_type_hint_to_cpp(type_hint);
    config->configs.set(std::string(col_name), col_config);
    return LIBVROOM_OK;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_error_t libvroom_column_config_set_na_values_by_index(libvroom_column_config_t* config,
                                                               size_t col_index,
                                                               const char** na_values,
                                                               size_t num_values) {
  if (!config) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }
  if (num_values > 0 && !na_values) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }

  try {
    // Store owned strings
    std::vector<std::string> owned;
    owned.reserve(num_values);
    for (size_t i = 0; i < num_values; ++i) {
      if (na_values[i]) {
        owned.push_back(na_values[i]);
      }
    }
    config->owned_na_values.push_back(std::move(owned));

    // Create string_views pointing to owned storage
    std::vector<std::string_view> views;
    const auto& stored = config->owned_na_values.back();
    views.reserve(stored.size());
    for (const auto& s : stored) {
      views.emplace_back(s);
    }

    // Get or create column config
    libvroom::ColumnConfig col_config;
    const auto* existing = config->configs.get(col_index);
    if (existing) {
      col_config = *existing;
    }
    col_config.na_values = views;
    config->configs.set(col_index, col_config);

    return LIBVROOM_OK;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_error_t libvroom_column_config_set_na_values_by_name(libvroom_column_config_t* config,
                                                              const char* col_name,
                                                              const char** na_values,
                                                              size_t num_values) {
  if (!config || !col_name) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }
  if (num_values > 0 && !na_values) {
    return LIBVROOM_ERROR_NULL_POINTER;
  }

  try {
    // Store owned strings
    std::vector<std::string> owned;
    owned.reserve(num_values);
    for (size_t i = 0; i < num_values; ++i) {
      if (na_values[i]) {
        owned.push_back(na_values[i]);
      }
    }
    config->owned_na_values.push_back(std::move(owned));

    // Create string_views pointing to owned storage
    std::vector<std::string_view> views;
    const auto& stored = config->owned_na_values.back();
    views.reserve(stored.size());
    for (const auto& s : stored) {
      views.emplace_back(s);
    }

    // Get or create column config
    libvroom::ColumnConfig col_config;
    const auto* existing = config->configs.get(std::string(col_name));
    if (existing) {
      col_config = *existing;
    }
    col_config.na_values = views;
    config->configs.set(std::string(col_name), col_config);

    return LIBVROOM_OK;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

libvroom_type_hint_t
libvroom_column_config_get_type_by_index(const libvroom_column_config_t* config, size_t col_index) {
  if (!config) {
    return LIBVROOM_TYPE_AUTO;
  }

  const auto* col_config = config->configs.get(col_index);
  if (col_config && col_config->type_hint.has_value()) {
    return cpp_type_hint_to_c(*col_config->type_hint);
  }
  return LIBVROOM_TYPE_AUTO;
}

bool libvroom_column_config_empty(const libvroom_column_config_t* config) {
  if (!config) {
    return true;
  }
  return config->configs.empty();
}

void libvroom_column_config_clear(libvroom_column_config_t* config) {
  if (config) {
    config->configs.clear();
    config->owned_na_values.clear();
  }
}

void libvroom_column_config_destroy(libvroom_column_config_t* config) {
  delete config;
}

const char* libvroom_type_hint_string(libvroom_type_hint_t type_hint) {
  switch (type_hint) {
  case LIBVROOM_TYPE_AUTO:
    return "auto";
  case LIBVROOM_TYPE_BOOLEAN:
    return "boolean";
  case LIBVROOM_TYPE_INTEGER:
    return "integer";
  case LIBVROOM_TYPE_DOUBLE:
    return "double";
  case LIBVROOM_TYPE_STRING:
    return "string";
  case LIBVROOM_TYPE_DATE:
    return "date";
  case LIBVROOM_TYPE_DATETIME:
    return "datetime";
  case LIBVROOM_TYPE_SKIP:
    return "skip";
  default:
    return "unknown";
  }
}

// ============================================================================
// FieldSpan Functions
// ============================================================================

libvroom_field_span_t libvroom_index_get_field_span(const libvroom_index_t* index,
                                                    uint64_t global_field_idx) {
  libvroom_field_span_t invalid_span = {LIBVROOM_FIELD_SPAN_INVALID, LIBVROOM_FIELD_SPAN_INVALID};

  if (!index) {
    return invalid_span;
  }

  libvroom::FieldSpan cpp_span = index->idx.get_field_span(global_field_idx);

  if (!cpp_span.is_valid()) {
    return invalid_span;
  }

  libvroom_field_span_t result;
  result.start = cpp_span.start;
  result.end = cpp_span.end;
  return result;
}

libvroom_field_span_t libvroom_index_get_field_span_rc(const libvroom_index_t* index, uint64_t row,
                                                       uint64_t col) {
  libvroom_field_span_t invalid_span = {LIBVROOM_FIELD_SPAN_INVALID, LIBVROOM_FIELD_SPAN_INVALID};

  if (!index) {
    return invalid_span;
  }

  libvroom::FieldSpan cpp_span = index->idx.get_field_span(row, col);

  if (!cpp_span.is_valid()) {
    return invalid_span;
  }

  libvroom_field_span_t result;
  result.start = cpp_span.start;
  result.end = cpp_span.end;
  return result;
}

libvroom_location_t libvroom_index_byte_offset_to_location(const libvroom_index_t* index,
                                                            size_t byte_offset) {
  libvroom_location_t not_found = {0, 0, false};

  if (!index) {
    return not_found;
  }

  // Use the ValueExtractor's byte_offset_to_location logic directly
  // Since we don't have a ValueExtractor here, implement the logic inline
  uint64_t total_indexes = index->idx.total_indexes();
  size_t num_columns = index->idx.columns;

  if (total_indexes == 0 || num_columns == 0) {
    return not_found;
  }

  // Linear search through fields to find which one contains the byte offset
  for (uint64_t i = 0; i < total_indexes; ++i) {
    libvroom::FieldSpan span = index->idx.get_field_span(i);
    if (!span.is_valid())
      continue;

    // Check if byte_offset falls within this field's bounds
    if (byte_offset <= span.end) {
      // Found the field containing this byte offset
      libvroom_location_t result;
      result.row = i / num_columns;
      result.column = i % num_columns;
      result.found = true;
      return result;
    }
  }

  // Byte offset is beyond the last field
  return not_found;
}

// ============================================================================
// Lazy Column Functions
// ============================================================================

libvroom_lazy_column_t* libvroom_lazy_column_create(const libvroom_buffer_t* buffer,
                                                    const libvroom_index_t* index, size_t col,
                                                    bool has_header,
                                                    const libvroom_dialect_t* dialect) {
  if (!buffer || !index) {
    return nullptr;
  }

  // Check column index is valid
  if (index->idx.columns > 0 && col >= index->idx.columns) {
    return nullptr;
  }

  libvroom::Dialect cpp_dialect = dialect ? dialect->dialect : libvroom::Dialect::csv();

  try {
    return new libvroom_lazy_column(buffer->data.data(), buffer->original_length, &index->idx, col,
                                    has_header, cpp_dialect);
  } catch (...) {
    return nullptr;
  }
}

size_t libvroom_lazy_column_size(const libvroom_lazy_column_t* column) {
  if (!column) {
    return 0;
  }
  return column->num_rows;
}

bool libvroom_lazy_column_empty(const libvroom_lazy_column_t* column) {
  if (!column) {
    return true;
  }
  return column->num_rows == 0;
}

size_t libvroom_lazy_column_index(const libvroom_lazy_column_t* column) {
  if (!column) {
    return 0;
  }
  return column->col;
}

libvroom_field_span_t libvroom_lazy_column_get_bounds(const libvroom_lazy_column_t* column,
                                                      size_t row) {
  libvroom_field_span_t invalid_span = {LIBVROOM_FIELD_SPAN_INVALID, LIBVROOM_FIELD_SPAN_INVALID};

  if (!column || row >= column->num_rows) {
    return invalid_span;
  }

  // Adjust row for header
  size_t actual_row = column->has_header ? row + 1 : row;
  libvroom::FieldSpan cpp_span = column->idx->get_field_span(actual_row, column->col);

  if (!cpp_span.is_valid()) {
    return invalid_span;
  }

  libvroom_field_span_t result;
  result.start = cpp_span.start;
  result.end = cpp_span.end;
  return result;
}

const char* libvroom_lazy_column_get_string(const libvroom_lazy_column_t* column, size_t row,
                                            size_t* length) {
  if (!column || row >= column->num_rows) {
    if (length) {
      *length = 0;
    }
    return nullptr;
  }

  // Get field span
  size_t actual_row = column->has_header ? row + 1 : row;
  libvroom::FieldSpan span = column->idx->get_field_span(actual_row, column->col);

  if (!span.is_valid() || span.start >= column->buf_len) {
    if (length) {
      *length = 0;
    }
    return nullptr;
  }

  uint64_t start = span.start;
  uint64_t end = std::min(span.end, static_cast<uint64_t>(column->buf_len));

  // Handle CR in CRLF endings
  if (end > start && column->buf[end - 1] == '\r') {
    --end;
  }

  // Handle quoted fields - strip outer quotes
  if (end > start && column->buf[start] == static_cast<uint8_t>(column->dialect.quote_char)) {
    if (column->buf[end - 1] == static_cast<uint8_t>(column->dialect.quote_char)) {
      ++start;
      --end;
    }
  }

  if (end < start) {
    end = start;
  }

  if (length) {
    *length = static_cast<size_t>(end - start);
  }

  return reinterpret_cast<const char*>(column->buf + start);
}

void libvroom_lazy_column_destroy(libvroom_lazy_column_t* column) {
  delete column;
}
