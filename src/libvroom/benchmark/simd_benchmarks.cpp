#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>

// SIMD instruction level benchmarks and comparisons

// Helper to create temporary CSV file
class TempCSVFile {
private:
  std::string filename_;

public:
  explicit TempCSVFile(const std::string& content)
      : filename_("/tmp/libvroom_simd_" + std::to_string(std::random_device{}()) + ".csv") {
    std::ofstream file(filename_);
    file << content;
    file.close();
  }

  ~TempCSVFile() { std::remove(filename_.c_str()); }

  const std::string& path() const { return filename_; }
};

// Generate test CSV data for SIMD benchmarking
std::string generate_simd_test_data(size_t rows, size_t cols,
                                    const std::string& pattern = "mixed") {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::ostringstream ss;

  // Header
  for (size_t col = 0; col < cols; ++col) {
    if (col > 0)
      ss << ",";
    ss << "col_" << col;
  }
  ss << "\n";

  // Generate data based on pattern
  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      if (col > 0)
        ss << ",";

      if (pattern == "quote_heavy") {
        // Lots of quoted fields (tests quote detection SIMD)
        if (col % 2 == 0) {
          ss << "\"quoted field " << row << "_" << col << " with, comma\"";
        } else {
          ss << "unquoted_" << row;
        }
      } else if (pattern == "long_fields") {
        // Long fields (tests memory bandwidth)
        ss << "\"very_long_field_name_that_spans_multiple_cache_lines_"
           << std::string(50, 'a' + (row % 26)) << "_" << row << "_" << col << "\"";
      } else if (pattern == "many_commas") {
        // Many separators (tests separator detection)
        ss << "field" << row << col;
      } else if (pattern == "newlines_in_quotes") {
        // Newlines in quoted fields (tests quote state tracking)
        if (col % 3 == 0) {
          ss << "\"field with\nnewline " << row << "_" << col << "\"";
        } else {
          ss << "normal_field_" << row;
        }
      } else {
        // Mixed pattern
        switch ((row + col) % 4) {
        case 0:
          ss << row * col;
          break;
        case 1:
          ss << std::fixed << std::setprecision(2) << (row + col) * 0.1;
          break;
        case 2:
          ss << "\"text_" << row << "\"";
          break;
        case 3:
          ss << "simple" << row;
          break;
        }
      }
    }
    ss << "\n";
  }

  return ss.str();
}

// Benchmark CsvReader with different thread counts for various CSV patterns
static void BM_SIMD_vs_Scalar(benchmark::State& state) {
  int pattern_type = static_cast<int>(state.range(0));
  int n_threads = static_cast<int>(state.range(1)); // 1 = single-threaded, 4 = multi-threaded

  std::string pattern;
  switch (pattern_type) {
  case 0:
    pattern = "mixed";
    break;
  case 1:
    pattern = "quote_heavy";
    break;
  case 2:
    pattern = "long_fields";
    break;
  case 3:
    pattern = "many_commas";
    break;
  case 4:
    pattern = "newlines_in_quotes";
    break;
  default:
    pattern = "mixed";
    break;
  }

  size_t rows = 5000;
  size_t cols = 10;
  auto csv_data = generate_simd_test_data(rows, cols, pattern);
  TempCSVFile temp_file(csv_data);

  try {
    libvroom::CsvOptions opts;
    opts.num_threads = static_cast<size_t>(n_threads);

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
    state.counters["PatternType"] = static_cast<double>(pattern_type);
    state.counters["FileSize"] = static_cast<double>(csv_data.size());
    state.counters["Threads"] = static_cast<double>(n_threads);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_SIMD_vs_Scalar)
    ->ArgsProduct({{0, 1, 2, 3, 4}, {1, 4}}) // pattern_type: 0-4, n_threads: 1 or 4
    ->Unit(benchmark::kMillisecond);

// Vector width effectiveness benchmark
static void BM_VectorWidth_Effectiveness(benchmark::State& state) {
  size_t chunk_size = static_cast<size_t>(state.range(0));

  // Generate data with specific characteristics for vector processing
  size_t data_size = 64 * 1024; // 64KB

  // Build CSV content with a header and structured rows
  std::ostringstream ss;
  ss << "col_0";
  ss << "\n";

  // Pattern optimized for different vector widths
  std::string row;
  row.reserve(chunk_size + 1);
  for (size_t i = 0; i < data_size; ++i) {
    if (i % chunk_size == 0) {
      if (!row.empty()) {
        ss << row << "\n";
        row.clear();
      }
    } else if ((i % chunk_size) % 8 == 0) {
      // Insert comma as field separator within row
      row += ',';
    } else {
      row += static_cast<char>('a' + (i % 26));
    }
  }
  if (!row.empty()) {
    ss << row << "\n";
  }

  std::string csv_data = ss.str();
  std::string temp_path = "/tmp/libvroom_bench_vecwidth_" + std::to_string(chunk_size) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["ChunkSize"] = static_cast<double>(chunk_size);
  state.counters["DataSize"] = static_cast<double>(csv_data.size());

  // Estimate vector width efficiency
  if (chunk_size <= 16) {
    state.counters["VectorWidth"] = 128.0; // SSE/NEON
  } else if (chunk_size <= 32) {
    state.counters["VectorWidth"] = 256.0; // AVX2
  } else {
    state.counters["VectorWidth"] = 512.0; // AVX-512
  }

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_VectorWidth_Effectiveness)
    ->Arg(16) // Optimized for 128-bit (SSE/NEON)
    ->Arg(32) // Optimized for 256-bit (AVX2)
    ->Arg(64) // Optimized for 512-bit (AVX-512)
    ->Unit(benchmark::kMicrosecond);

// Quote detection SIMD effectiveness
static void BM_QuoteDetection_SIMD(benchmark::State& state) {
  double quote_density = static_cast<double>(state.range(0)) / 100.0; // Percentage as decimal

  size_t data_size = 1024 * 1024; // 1MB

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dist(0.0, 1.0);

  // Build CSV content with specific quote density
  std::ostringstream ss;
  ss << "col_0\n"; // header

  std::string row;
  bool in_quote = false;
  for (size_t i = 0; i < data_size; ++i) {
    if (dist(gen) < quote_density) {
      row += '"';
      in_quote = !in_quote;
    } else if (!in_quote && dist(gen) < 0.1) {
      row += ',';
    } else if (!in_quote && dist(gen) < 0.02) {
      // End of row
      if (in_quote) {
        row += '"';
        in_quote = false;
      }
      ss << row << "\n";
      row.clear();
    } else {
      row += static_cast<char>('a' + (i % 26));
    }
  }
  // Ensure we end not in quote state
  if (in_quote) {
    row += '"';
  }
  if (!row.empty()) {
    ss << row << "\n";
  }

  std::string csv_data = ss.str();
  std::string temp_path = "/tmp/libvroom_bench_quotes_" + std::to_string(state.range(0)) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["QuoteDensity%"] = quote_density * 100.0;
  state.counters["DataSize"] = static_cast<double>(csv_data.size());

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_QuoteDetection_SIMD)
    ->Arg(0)  // No quotes
    ->Arg(1)  // 1% quotes
    ->Arg(5)  // 5% quotes
    ->Arg(10) // 10% quotes
    ->Arg(20) // 20% quotes (heavy)
    ->Unit(benchmark::kMillisecond);

// Separator detection SIMD effectiveness
static void BM_SeparatorDetection_SIMD(benchmark::State& state) {
  char separator = static_cast<char>(state.range(0));

  size_t data_size = 1024 * 1024; // 1MB

  // Generate CSV data with specific separator
  std::ostringstream ss;
  ss << "col_0" << separator << "col_1\n"; // header with the target separator

  std::string row;
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 50 == 0) {
      // End of row
      ss << row << "\n";
      row.clear();
    } else if (i % 8 == 0) {
      row += separator;
    } else {
      row += static_cast<char>('a' + (i % 26));
    }
  }
  if (!row.empty()) {
    ss << row << "\n";
  }

  std::string csv_data = ss.str();
  std::string temp_path =
      "/tmp/libvroom_bench_sep_" + std::to_string(static_cast<int>(separator)) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.separator = separator;
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["SeparatorASCII"] = static_cast<double>(separator);
  state.counters["DataSize"] = static_cast<double>(csv_data.size());

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_SeparatorDetection_SIMD)
    ->Arg(',')  // Comma (most common)
    ->Arg('\t') // Tab
    ->Arg(';')  // Semicolon
    ->Arg('|')  // Pipe
    ->Unit(benchmark::kMillisecond);

// Memory access pattern benchmark for SIMD
static void BM_MemoryAccess_SIMD(benchmark::State& state) {
  int access_pattern = static_cast<int>(state.range(0));

  size_t data_size = 2 * 1024 * 1024; // 2MB

  // Build CSV data with different memory access patterns
  std::vector<uint8_t> raw(data_size);

  switch (access_pattern) {
  case 0: // Sequential (SIMD-friendly)
    for (size_t i = 0; i < data_size; ++i) {
      raw[i] = static_cast<uint8_t>(i % 256);
    }
    break;
  case 1: // Strided (less SIMD-friendly)
    for (size_t i = 0; i < data_size; i += 2) {
      raw[i] = static_cast<uint8_t>(i % 256);
      if (i + 1 < data_size)
        raw[i + 1] = 0;
    }
    break;
  case 2: // Random (SIMD-unfriendly)
  {
    std::mt19937 gen(12345);
    for (size_t i = 0; i < data_size; ++i) {
      raw[i] = static_cast<uint8_t>(gen() % 256);
    }
  } break;
  }

  // Add CSV structure: newlines and commas at regular intervals
  for (size_t i = 0; i < data_size; i += 100) {
    raw[i] = '\n';
  }
  for (size_t i = 10; i < data_size; i += 20) {
    raw[i] = ',';
  }

  // Write to temp file with a header
  std::string temp_path =
      "/tmp/libvroom_bench_memaccess_" + std::to_string(access_pattern) + ".csv";
  {
    std::ofstream out(temp_path, std::ios::binary);
    // Write a simple header first
    std::string header = "col_0,col_1\n";
    out.write(header.data(), static_cast<std::streamsize>(header.size()));
    out.write(reinterpret_cast<const char*>(raw.data()), static_cast<std::streamsize>(data_size));
  }

  size_t total_size = data_size + 12; // header + data

  libvroom::CsvOptions opts;
  opts.num_threads = 1;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(total_size * state.iterations()));
  state.counters["AccessPattern"] = static_cast<double>(access_pattern);
  state.counters["DataSize"] = static_cast<double>(total_size);

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_MemoryAccess_SIMD)
    ->Arg(0) // Sequential
    ->Arg(1) // Strided
    ->Arg(2) // Random
    ->Unit(benchmark::kMillisecond);
