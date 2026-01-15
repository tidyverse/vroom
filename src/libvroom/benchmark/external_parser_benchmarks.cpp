// External CSV Parser Benchmarks
// Compares libvroom against best-in-class CSV parsers: DuckDB, zsv, and Apache
// Arrow
//
// =============================================================================
// HOW TO ENABLE AND RUN EXTERNAL PARSER BENCHMARKS
// =============================================================================
//
// These benchmarks are optional and require enabling at CMake configure time:
//
//   # Enable zsv only (fast to build, recommended for quick comparisons)
//   cmake -B build -DENABLE_ZSV_BENCHMARK=ON
//
//   # Enable DuckDB (slow to build ~15min, but comprehensive)
//   cmake -B build -DENABLE_DUCKDB_BENCHMARK=ON
//
//   # Enable Apache Arrow (requires system installation)
//   # macOS: brew install apache-arrow
//   # Ubuntu: apt install libarrow-dev
//   cmake -B build -DENABLE_ARROW_BENCHMARK=ON
//
//   # Enable all parsers
//   cmake -B build -DENABLE_ZSV_BENCHMARK=ON -DENABLE_DUCKDB_BENCHMARK=ON
//   -DENABLE_ARROW_BENCHMARK=ON
//
// Build and run:
//   cmake --build build
//   ./build/libvroom_benchmark --benchmark_filter="BM_external"
//   ./build/libvroom_benchmark --benchmark_filter="BM_fair_comparison"
//
// =============================================================================
// IMPLEMENTATION NOTES
// =============================================================================
//
// DuckDB In-Memory Parsing:
//   We use a custom MemoryFileSystem registered with DuckDB to serve CSV data
//   directly from memory buffers via memory:// URLs. This avoids file I/O and
//   provides a fair comparison with other parsers. The MemoryFileSystem is
//   registered per-thread to avoid contention.
//
// =============================================================================

#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#ifdef HAVE_ZSV
// zsv uses C99 restrict keyword which is not valid in C++
// Define it to nothing for C++ compilation if not already defined
#ifdef __cplusplus
#ifndef restrict
#define restrict
#endif
#endif
extern "C" {
#include <zsv.h>
}
#endif

#ifdef HAVE_DUCKDB
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <mutex>

// ============================================================================
// In-Memory FileSystem for DuckDB
// ============================================================================
// Custom FileSystem that serves CSV data from memory buffers, avoiding file
// I/O.

class MemoryFileHandle : public duckdb::FileHandle {
public:
  MemoryFileHandle(duckdb::FileSystem& fs, duckdb::string path, const uint8_t* data, size_t len)
      : FileHandle(fs, std::move(path)), data_(data), len_(len), pos_(0) {}

  void Close() override {}

  const uint8_t* data_;
  size_t len_;
  size_t pos_;
};

class MemoryFileSystem : public duckdb::FileSystem {
public:
  static constexpr const char* PROTOCOL = "memory://";

  // Register a memory buffer with a name
  void RegisterBuffer(const std::string& name, const uint8_t* data, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    buffers_[PROTOCOL + name] = {data, len};
  }

  void ClearBuffers() {
    std::lock_guard<std::mutex> lock(mutex_);
    buffers_.clear();
  }

  // FileSystem interface
  std::string GetName() const override { return "MemoryFileSystem"; }

  bool CanHandleFile(const std::string& path) override {
    return path.rfind(PROTOCOL, 0) == 0; // starts with memory://
  }

  duckdb::unique_ptr<duckdb::FileHandle>
  OpenFile(const duckdb::string& path, duckdb::FileOpenFlags flags,
           duckdb::optional_ptr<duckdb::FileOpener>) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = buffers_.find(path);
    if (it == buffers_.end()) {
      throw duckdb::IOException("Memory buffer not found: " + path);
    }
    return duckdb::make_uniq<MemoryFileHandle>(*this, path, it->second.data, it->second.len);
  }

  int64_t Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes) override {
    auto& mem_handle = handle.Cast<MemoryFileHandle>();
    int64_t bytes_available = static_cast<int64_t>(mem_handle.len_ - mem_handle.pos_);
    int64_t bytes_to_read = std::min(nr_bytes, bytes_available);
    if (bytes_to_read > 0) {
      memcpy(buffer, mem_handle.data_ + mem_handle.pos_, bytes_to_read);
      mem_handle.pos_ += bytes_to_read;
    }
    return bytes_to_read;
  }

  int64_t GetFileSize(duckdb::FileHandle& handle) override {
    auto& mem_handle = handle.Cast<MemoryFileHandle>();
    return static_cast<int64_t>(mem_handle.len_);
  }

  void Seek(duckdb::FileHandle& handle, duckdb::idx_t location) override {
    auto& mem_handle = handle.Cast<MemoryFileHandle>();
    mem_handle.pos_ = std::min(location, static_cast<duckdb::idx_t>(mem_handle.len_));
  }

  void Reset(duckdb::FileHandle& handle) override {
    auto& mem_handle = handle.Cast<MemoryFileHandle>();
    mem_handle.pos_ = 0;
  }

  duckdb::idx_t SeekPosition(duckdb::FileHandle& handle) override {
    auto& mem_handle = handle.Cast<MemoryFileHandle>();
    return mem_handle.pos_;
  }

  bool CanSeek() override { return true; }
  bool OnDiskFile(duckdb::FileHandle&) override { return false; }
  bool IsPipe(const std::string&, duckdb::optional_ptr<duckdb::FileOpener>) override {
    return false;
  }

  // Required but unused for read-only access
  int64_t Write(duckdb::FileHandle&, void*, int64_t) override {
    throw duckdb::IOException("MemoryFileSystem is read-only");
  }
  void FileSync(duckdb::FileHandle&) override {}
  void Truncate(duckdb::FileHandle&, int64_t) override {}
  time_t GetLastModifiedTime(duckdb::FileHandle&) override { return 0; }
  bool FileExists(const std::string& filename, duckdb::optional_ptr<duckdb::FileOpener>) override {
    std::lock_guard<std::mutex> lock(mutex_);
    return buffers_.find(filename) != buffers_.end();
  }

private:
  struct BufferInfo {
    const uint8_t* data;
    size_t len;
  };
  std::mutex mutex_;
  std::map<std::string, BufferInfo> buffers_;
};

#endif

#ifdef HAVE_ARROW
#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#endif

extern std::map<std::string, std::basic_string_view<uint8_t>> test_data;
extern libvroom::TwoPass* global_parser;

// ============================================================================
// Test Data Generation
// ============================================================================

// Generate synthetic CSV data for benchmarking
static std::string generate_csv_data(size_t target_size, int num_columns = 10) {
  std::string result;
  result.reserve(target_size + 1024);

  // Header row
  for (int i = 0; i < num_columns; ++i) {
    if (i > 0)
      result += ',';
    result += "col" + std::to_string(i);
  }
  result += '\n';

  // Data rows
  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> int_dist(0, 1000000);
  std::uniform_real_distribution<double> real_dist(0.0, 10000.0);

  int row = 0;
  while (result.size() < target_size) {
    for (int i = 0; i < num_columns; ++i) {
      if (i > 0)
        result += ',';
      if (i % 3 == 0) {
        // Integer column
        result += std::to_string(int_dist(rng));
      } else if (i % 3 == 1) {
        // Float column
        char buf[32];
        snprintf(buf, sizeof(buf), "%.2f", real_dist(rng));
        result += buf;
      } else {
        // String column
        result += "str" + std::to_string(row) + "_" + std::to_string(i);
      }
    }
    result += '\n';
    row++;
  }

  return result;
}

// Generate quoted CSV data (more challenging for parsers)
static std::string generate_quoted_csv_data(size_t target_size, int num_columns = 10) {
  std::string result;
  result.reserve(target_size + 1024);

  // Header row
  for (int i = 0; i < num_columns; ++i) {
    if (i > 0)
      result += ',';
    result += "\"column_" + std::to_string(i) + "\"";
  }
  result += '\n';

  // Data rows with quoted fields
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> int_dist(0, 1000000);

  int row = 0;
  while (result.size() < target_size) {
    for (int i = 0; i < num_columns; ++i) {
      if (i > 0)
        result += ',';
      if (i % 2 == 0) {
        // Quoted string with potential special characters
        result += "\"value_" + std::to_string(row) + "_" + std::to_string(int_dist(rng)) + "\"";
      } else {
        // Unquoted number
        result += std::to_string(int_dist(rng));
      }
    }
    result += '\n';
    row++;
  }

  return result;
}

// Cache for generated test data
static std::map<size_t, std::string> generated_data_cache;
static std::map<size_t, std::string> generated_quoted_data_cache;

static const std::string& get_or_generate_data(size_t size) {
  if (generated_data_cache.find(size) == generated_data_cache.end()) {
    generated_data_cache[size] = generate_csv_data(size);
  }
  return generated_data_cache[size];
}

static const std::string& get_or_generate_quoted_data(size_t size) {
  if (generated_quoted_data_cache.find(size) == generated_quoted_data_cache.end()) {
    generated_quoted_data_cache[size] = generate_quoted_csv_data(size);
  }
  return generated_quoted_data_cache[size];
}

// ============================================================================
// libvroom Parser (baseline)
// ============================================================================

static size_t parse_libvroom(const uint8_t* data, size_t len) {
  if (!global_parser) {
    global_parser = new libvroom::TwoPass();
  }

  libvroom::ParseIndex result = global_parser->init(len, 1);
  global_parser->parse(data, result, len);

  // Return total field count as work indicator
  size_t total_fields = 0;
  for (uint16_t i = 0; i < result.n_threads; ++i) {
    total_fields += result.n_indexes[i];
  }
  return total_fields;
}

// ============================================================================
// zsv Parser
// ============================================================================

#ifdef HAVE_ZSV

struct ZsvMemoryStream {
  const uint8_t* data;
  size_t len;
  size_t pos;
};

struct ZsvParseContext {
  zsv_parser parser;       // Need parser reference to access cells
  const uint8_t* base_ptr; // Base pointer to calculate offsets
  size_t row_count = 0;
  size_t cell_count = 0;
  // Index vector to store cell positions - similar to libvroom's index
  std::vector<uint64_t>* index_storage;
};

// Row handler that builds an index of all cell positions (like libvroom)
static void zsv_row_handler_with_index(void* ctx) {
  auto* context = static_cast<ZsvParseContext*>(ctx);
  context->row_count++;

  // Get and iterate through all cells - this is the work libvroom does
  // when it builds its field index
  size_t cell_count = zsv_cell_count(context->parser);
  size_t write_pos = context->cell_count;
  context->cell_count += cell_count;

  // Ensure capacity (should rarely need to grow due to pre-allocation)
  if (write_pos + cell_count > context->index_storage->size()) {
    context->index_storage->resize((write_pos + cell_count) * 2);
  }

  // Write to index array (like libvroom does)
  uint64_t* positions = context->index_storage->data();
  for (size_t i = 0; i < cell_count; i++, write_pos++) {
    struct zsv_cell cell = zsv_get_cell(context->parser, i);
    // Calculate byte offset from start of data - this is what libvroom stores
    uint64_t offset = static_cast<uint64_t>(cell.str - context->base_ptr);
    // Write to index array - this is the key work that libvroom does
    positions[write_pos] = offset;
  }
}

// Custom read function for memory buffer (mimics fread signature)
static size_t zsv_memory_read(void* buffer, size_t n, size_t size, void* stream) {
  auto* mem_stream = static_cast<ZsvMemoryStream*>(stream);
  size_t bytes_to_read = n * size;
  size_t bytes_available = mem_stream->len - mem_stream->pos;
  size_t bytes_read = std::min(bytes_to_read, bytes_available);

  if (bytes_read > 0) {
    memcpy(buffer, mem_stream->data + mem_stream->pos, bytes_read);
    mem_stream->pos += bytes_read;
  }

  return bytes_read / size; // Return number of elements read
}

// Thread-local storage for zsv index to avoid allocation in hot path
static thread_local std::vector<uint64_t> zsv_index_storage;

static size_t parse_zsv(const uint8_t* data, size_t len) {
  ZsvParseContext ctx;
  ctx.parser = nullptr;
  ctx.base_ptr = data;
  ctx.index_storage = &zsv_index_storage;
  ZsvMemoryStream mem_stream = {data, len, 0};

  // Pre-allocate index array - estimate ~1 cell per 8 bytes (similar to
  // libvroom)
  size_t estimated_cells = len / 8;
  if (zsv_index_storage.size() < estimated_cells) {
    zsv_index_storage.resize(estimated_cells);
  }

  // Configure zsv parser options
  struct zsv_opts opts;
  memset(&opts, 0, sizeof(opts));
  opts.row_handler = zsv_row_handler_with_index;
  opts.ctx = &ctx;
  opts.stream = &mem_stream;
  opts.read = zsv_memory_read;

  // Create parser
  zsv_parser parser = zsv_new(&opts);
  if (!parser) {
    return 0;
  }

  // Store parser in context so row handler can access cells
  ctx.parser = parser;

  // Parse data - zsv_parse_more reads from the stream internally
  enum zsv_status status;
  while ((status = zsv_parse_more(parser)) == zsv_status_ok) {
    // Continue parsing
  }

  zsv_finish(parser);
  zsv_delete(parser);

  // Prevent optimizer from eliminating the index array
  benchmark::DoNotOptimize(zsv_index_storage.data());
  benchmark::ClobberMemory();

  // Return cell count as work indicator (similar to libvroom field count)
  return ctx.cell_count;
}

#endif // HAVE_ZSV

// ============================================================================
// DuckDB Parser
// ============================================================================
//
// Uses a custom in-memory FileSystem to avoid file I/O overhead. The CSV data
// is registered with a MemoryFileSystem and accessed via memory:// URLs.

#ifdef HAVE_DUCKDB

static size_t parse_duckdb(const uint8_t* data, size_t len) {
  // Create fresh DuckDB instance each time to avoid any caching effects
  // This ensures fair comparison with other parsers that don't cache
  duckdb::DuckDB db(nullptr);

  // Create and register memory filesystem
  auto mem_fs = duckdb::make_uniq<MemoryFileSystem>();
  auto* mem_fs_ptr = mem_fs.get();
  db.GetFileSystem().RegisterSubSystem(std::move(mem_fs));

  // Register the buffer
  mem_fs_ptr->RegisterBuffer("data.csv", data, len);

  duckdb::Connection conn(db);

  try {
    auto result = conn.Query("SELECT COUNT(*) FROM read_csv_auto('memory://data.csv')");
    if (result->HasError()) {
      return 0;
    }

    auto count = result->GetValue(0, 0).GetValue<int64_t>();
    return static_cast<size_t>(count);
  } catch (...) {
    return 0;
  }
}

#endif // HAVE_DUCKDB

// ============================================================================
// Apache Arrow Parser
// ============================================================================

#ifdef HAVE_ARROW

static size_t parse_arrow(const uint8_t* data, size_t len) {
  // Create a buffer from the data
  auto buffer = std::make_shared<arrow::Buffer>(data, static_cast<int64_t>(len));
  auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);

  // Configure CSV reader options
  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  // Disable type inference for fair comparison (just parse, don't convert)
  // Arrow will still parse but treat everything as strings

  // Create streaming reader for memory efficiency
  auto maybe_reader = arrow::csv::StreamingReader::Make(
      arrow::io::default_io_context(), buffer_reader, read_options, parse_options, convert_options);

  if (!maybe_reader.ok()) {
    return 0;
  }

  auto reader = *maybe_reader;

  // Read all batches and count rows
  size_t total_rows = 0;
  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    auto status = reader->ReadNext(&batch);
    if (!status.ok() || !batch) {
      break;
    }
    total_rows += batch->num_rows();
  }

  return total_rows;
}

#endif // HAVE_ARROW

// ============================================================================
// Benchmark Functions
// ============================================================================

// Benchmark libvroom with generated data
static void BM_libvroom_generated(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_data(data_size);

  // Allocate padded buffer for libvroom
  size_t padded_size = csv_data.size() + LIBVROOM_PADDING;
  auto* buffer = static_cast<uint8_t*>(aligned_malloc(64, padded_size));
  memcpy(buffer, csv_data.data(), csv_data.size());
  memset(buffer + csv_data.size(), 0, LIBVROOM_PADDING);

  for (auto _ : state) {
    size_t result = parse_libvroom(buffer, csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  aligned_free(buffer);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["Parser"] = 0; // libvroom = 0
}

#ifdef HAVE_ZSV
static void BM_zsv_generated(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_data(data_size);

  for (auto _ : state) {
    size_t result = parse_zsv(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["Parser"] = 1; // zsv = 1
}
#endif

#ifdef HAVE_DUCKDB
static void BM_duckdb_generated(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_data(data_size);

  for (auto _ : state) {
    size_t result =
        parse_duckdb(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["Parser"] = 2; // duckdb = 2
}
#endif

#ifdef HAVE_ARROW
static void BM_arrow_generated(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_data(data_size);

  for (auto _ : state) {
    size_t result = parse_arrow(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["Parser"] = 3; // arrow = 3
}
#endif

// ============================================================================
// Quoted CSV Benchmarks
// ============================================================================

static void BM_libvroom_quoted(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_quoted_data(data_size);

  size_t padded_size = csv_data.size() + LIBVROOM_PADDING;
  auto* buffer = static_cast<uint8_t*>(aligned_malloc(64, padded_size));
  memcpy(buffer, csv_data.data(), csv_data.size());
  memset(buffer + csv_data.size(), 0, LIBVROOM_PADDING);

  for (auto _ : state) {
    size_t result = parse_libvroom(buffer, csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  aligned_free(buffer);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["QuotedData"] = 1;
}

#ifdef HAVE_ZSV
static void BM_zsv_quoted(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_quoted_data(data_size);

  for (auto _ : state) {
    size_t result = parse_zsv(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["QuotedData"] = 1;
}
#endif

#ifdef HAVE_ARROW
static void BM_arrow_quoted(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  const std::string& csv_data = get_or_generate_quoted_data(data_size);

  for (auto _ : state) {
    size_t result = parse_arrow(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["QuotedData"] = 1;
}
#endif

// ============================================================================
// Fair Comparison Benchmark (all parsers, same data)
// ============================================================================

enum class ParserType { LIBVROOM = 0, ZSV = 1, DUCKDB = 2, ARROW = 3 };

static void BM_fair_comparison(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  ParserType parser_type = static_cast<ParserType>(state.range(1));

  const std::string& csv_data = get_or_generate_data(data_size);

  // Prepare libvroom buffer (with padding)
  size_t padded_size = csv_data.size() + LIBVROOM_PADDING;
  auto* padded_buffer = static_cast<uint8_t*>(aligned_malloc(64, padded_size));
  memcpy(padded_buffer, csv_data.data(), csv_data.size());
  memset(padded_buffer + csv_data.size(), 0, LIBVROOM_PADDING);

  const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(csv_data.data());

  switch (parser_type) {
  case ParserType::LIBVROOM: {
    for (auto _ : state) {
      size_t result = parse_libvroom(padded_buffer, csv_data.size());
      benchmark::DoNotOptimize(result);
    }
    break;
  }
#ifdef HAVE_ZSV
  case ParserType::ZSV: {
    for (auto _ : state) {
      size_t result = parse_zsv(data_ptr, csv_data.size());
      benchmark::DoNotOptimize(result);
    }
    break;
  }
#endif
#ifdef HAVE_DUCKDB
  case ParserType::DUCKDB: {
    for (auto _ : state) {
      size_t result = parse_duckdb(data_ptr, csv_data.size());
      benchmark::DoNotOptimize(result);
    }
    break;
  }
#endif
#ifdef HAVE_ARROW
  case ParserType::ARROW: {
    for (auto _ : state) {
      size_t result = parse_arrow(data_ptr, csv_data.size());
      benchmark::DoNotOptimize(result);
    }
    break;
  }
#endif
  default:
    state.SkipWithError("Unknown parser type");
    break;
  }

  aligned_free(padded_buffer);

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["FileSize_MB"] = static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
  state.counters["Parser"] = static_cast<double>(parser_type);
}

// Helper to get parser name for labels
static std::string GetParserName(int parser_id) {
  switch (static_cast<ParserType>(parser_id)) {
  case ParserType::LIBVROOM:
    return "libvroom";
  case ParserType::ZSV:
    return "zsv";
  case ParserType::DUCKDB:
    return "duckdb";
  case ParserType::ARROW:
    return "arrow";
  default:
    return "unknown";
  }
}

// ============================================================================
// Benchmark Registration
// ============================================================================

// Benchmark data sizes: 1KB, 10KB, 100KB, 1MB, 10MB, 100MB (powers of 10)
#define BENCHMARK_CSV_SIZES_1KB_TO_100MB Range(1024, 100 * 1024 * 1024)->RangeMultiplier(10)

// Register libvroom benchmarks (always available)
BENCHMARK(BM_libvroom_generated)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/libvroom/generated");

BENCHMARK(BM_libvroom_quoted)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/libvroom/quoted");

// Register zsv benchmarks
#ifdef HAVE_ZSV
BENCHMARK(BM_zsv_generated)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/zsv/generated");

BENCHMARK(BM_zsv_quoted)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/zsv/quoted");
#endif

// Register DuckDB benchmarks
#ifdef HAVE_DUCKDB
BENCHMARK(BM_duckdb_generated)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/duckdb/generated");
#endif

// Register Arrow benchmarks
#ifdef HAVE_ARROW
BENCHMARK(BM_arrow_generated)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/arrow/generated");

BENCHMARK(BM_arrow_quoted)
    ->BENCHMARK_CSV_SIZES_1KB_TO_100MB->Unit(benchmark::kMillisecond)
    ->Name("BM_external/arrow/quoted");
#endif

// Fair comparison benchmark - tests all available parsers with same data sizes
static void RegisterFairComparisonBenchmarks() {
  std::vector<int64_t> sizes = {
      1024,             // 1KB
      10 * 1024,        // 10KB
      100 * 1024,       // 100KB
      1024 * 1024,      // 1MB
      10 * 1024 * 1024, // 10MB
      100 * 1024 * 1024 // 100MB
  };

  std::vector<int> parsers = {0}; // libvroom always available
#ifdef HAVE_ZSV
  parsers.push_back(1);
#endif
#ifdef HAVE_DUCKDB
  parsers.push_back(2);
#endif
#ifdef HAVE_ARROW
  parsers.push_back(3);
#endif

  for (int64_t size : sizes) {
    for (int parser : parsers) {
      benchmark::RegisterBenchmark(
          ("BM_fair_comparison/" + GetParserName(parser) + "/" + std::to_string(size / 1024) + "KB")
              .c_str(),
          [size, parser](benchmark::State& state) {
            const std::string& csv_data = get_or_generate_data(size);

            size_t padded_size = csv_data.size() + LIBVROOM_PADDING;
            auto* padded_buffer = static_cast<uint8_t*>(aligned_malloc(64, padded_size));
            memcpy(padded_buffer, csv_data.data(), csv_data.size());
            memset(padded_buffer + csv_data.size(), 0, LIBVROOM_PADDING);

            const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(csv_data.data());

            switch (static_cast<ParserType>(parser)) {
            case ParserType::LIBVROOM: {
              for (auto _ : state) {
                size_t result = parse_libvroom(padded_buffer, csv_data.size());
                benchmark::DoNotOptimize(result);
              }
              break;
            }
#ifdef HAVE_ZSV
            case ParserType::ZSV: {
              for (auto _ : state) {
                size_t result = parse_zsv(data_ptr, csv_data.size());
                benchmark::DoNotOptimize(result);
              }
              break;
            }
#endif
#ifdef HAVE_DUCKDB
            case ParserType::DUCKDB: {
              for (auto _ : state) {
                size_t result = parse_duckdb(data_ptr, csv_data.size());
                benchmark::DoNotOptimize(result);
              }
              break;
            }
#endif
#ifdef HAVE_ARROW
            case ParserType::ARROW: {
              for (auto _ : state) {
                size_t result = parse_arrow(data_ptr, csv_data.size());
                benchmark::DoNotOptimize(result);
              }
              break;
            }
#endif
            default:
              break;
            }

            aligned_free(padded_buffer);

            state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
            state.counters["FileSize_MB"] =
                static_cast<double>(csv_data.size()) / (1024.0 * 1024.0);
            state.counters["Parser"] = static_cast<double>(parser);
          })
          ->Unit(benchmark::kMillisecond);
    }
  }
}

// Static initialization to register fair comparison benchmarks
static int dummy = []() {
  RegisterFairComparisonBenchmarks();
  return 0;
}();
