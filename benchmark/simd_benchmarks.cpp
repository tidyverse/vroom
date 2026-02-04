#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

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

// Benchmark SIMD vs scalar for different CSV patterns
static void BM_SIMD_vs_Scalar(benchmark::State& state) {
  int pattern_type = static_cast<int>(state.range(0));
  int use_simd = static_cast<int>(state.range(1)); // 0 = scalar simulation, 1 = SIMD

  std::string pattern_name;
  std::string pattern;
  switch (pattern_type) {
  case 0:
    pattern_name = "mixed";
    pattern = "mixed";
    break;
  case 1:
    pattern_name = "quote_heavy";
    pattern = "quote_heavy";
    break;
  case 2:
    pattern_name = "long_fields";
    pattern = "long_fields";
    break;
  case 3:
    pattern_name = "many_commas";
    pattern = "many_commas";
    break;
  case 4:
    pattern_name = "newlines_in_quotes";
    pattern = "newlines_in_quotes";
    break;
  default:
    pattern_name = "mixed";
    pattern = "mixed";
    break;
  }

  size_t rows = 5000;
  size_t cols = 10;
  auto csv_data = generate_simd_test_data(rows, cols, pattern);
  TempCSVFile temp_file(csv_data);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    // For this benchmark, we're testing the current SIMD implementation
    // vs a conceptual scalar implementation (using single thread to simulate)
    int n_threads = use_simd ? 4 : 1; // More threads generally means more SIMD usage
    libvroom::Parser parser(n_threads);

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["PatternType"] = static_cast<double>(pattern_type);
    state.counters["UseSIMD"] = static_cast<double>(use_simd);
    state.counters["FileSize"] = static_cast<double>(buffer.size);
    state.counters["Threads"] = static_cast<double>(n_threads);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_SIMD_vs_Scalar)
    ->Ranges({{0, 4}, {0, 1}}) // pattern_type: 0-4, use_simd: 0-1
    ->Unit(benchmark::kMillisecond);

// Vector width effectiveness benchmark
static void BM_VectorWidth_Effectiveness(benchmark::State& state) {
  size_t chunk_size = static_cast<size_t>(state.range(0));

  // Generate data with specific characteristics for vector processing
  size_t data_size = 64 * 1024; // 64KB
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  // Pattern optimized for different vector widths
  for (size_t i = 0; i < data_size; ++i) {
    // Create pattern that benefits from wide vectors
    if (i % chunk_size == 0)
      data[i] = '\n';
    else if ((i % chunk_size) % 8 == 0)
      data[i] = ',';
    else if ((i % chunk_size) % 16 == 0)
      data[i] = '"';
    else
      data[i] = 'a' + (i % 26);
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["ChunkSize"] = static_cast<double>(chunk_size);
  state.counters["DataSize"] = static_cast<double>(data_size);

  // Estimate vector width efficiency
  if (chunk_size <= 16) {
    state.counters["VectorWidth"] = 128.0; // SSE/NEON
  } else if (chunk_size <= 32) {
    state.counters["VectorWidth"] = 256.0; // AVX2
  } else {
    state.counters["VectorWidth"] = 512.0; // AVX-512
  }

  aligned_free(data);
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
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dist(0.0, 1.0);

  // Generate data with specific quote density
  bool in_quote = false;
  for (size_t i = 0; i < data_size; ++i) {
    if (dist(gen) < quote_density) {
      data[i] = '"';
      in_quote = !in_quote;
    } else if (!in_quote && dist(gen) < 0.1) {
      data[i] = ',';
    } else if (!in_quote && dist(gen) < 0.02) {
      data[i] = '\n';
    } else {
      data[i] = 'a' + (i % 26);
    }
  }

  // Ensure we end not in quote state
  if (in_quote) {
    for (int i = data_size - 1; i >= 0; --i) {
      if (data[i] == '"') {
        break;
      }
      if (i == data_size - 100) { // Add quote if needed
        data[i] = '"';
        break;
      }
    }
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["QuoteDensity%"] = quote_density * 100.0;
  state.counters["DataSize"] = static_cast<double>(data_size);

  aligned_free(data);
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
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  // Generate data with specific separator
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 50 == 0) {
      data[i] = '\n';
    } else if (i % 8 == 0) {
      data[i] = separator;
    } else {
      data[i] = 'a' + (i % 26);
    }
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["SeparatorASCII"] = static_cast<double>(separator);
  state.counters["DataSize"] = static_cast<double>(data_size);

  aligned_free(data);
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
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  // Different memory access patterns
  switch (access_pattern) {
  case 0: // Sequential (SIMD-friendly)
    for (size_t i = 0; i < data_size; ++i) {
      data[i] = static_cast<uint8_t>(i % 256);
    }
    break;
  case 1: // Strided (less SIMD-friendly)
    for (size_t i = 0; i < data_size; i += 2) {
      data[i] = static_cast<uint8_t>(i % 256);
      if (i + 1 < data_size)
        data[i + 1] = 0;
    }
    break;
  case 2: // Random (SIMD-unfriendly)
  {
    std::mt19937 gen(12345);
    for (size_t i = 0; i < data_size; ++i) {
      data[i] = static_cast<uint8_t>(gen() % 256);
    }
  } break;
  }

  // Add CSV structure
  for (size_t i = 0; i < data_size; i += 100) {
    data[i] = '\n';
  }
  for (size_t i = 10; i < data_size; i += 20) {
    data[i] = ',';
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(data, data_size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["AccessPattern"] = static_cast<double>(access_pattern);
  state.counters["DataSize"] = static_cast<double>(data_size);

  aligned_free(data);
}

BENCHMARK(BM_MemoryAccess_SIMD)
    ->Arg(0) // Sequential
    ->Arg(1) // Strided
    ->Arg(2) // Random
    ->Unit(benchmark::kMillisecond);

// ============================================================================
// BRANCHLESS STATE MACHINE BENCHMARKS
// ============================================================================

// Compare branchless vs standard state machine for different data patterns
static void BM_Branchless_vs_Standard(benchmark::State& state) {
  int use_branchless = static_cast<int>(state.range(0));
  int pattern_type = static_cast<int>(state.range(1));

  std::string pattern;
  switch (pattern_type) {
  case 0:
    pattern = "mixed";
    break;
  case 1:
    pattern = "quote_heavy";
    break;
  case 2:
    pattern = "many_commas";
    break;
  default:
    pattern = "mixed";
    break;
  }

  size_t rows = 10000;
  size_t cols = 10;
  auto csv_data = generate_simd_test_data(rows, cols, pattern);
  TempCSVFile temp_file(csv_data);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(1);
    libvroom::ParseOptions options;
    if (use_branchless) {
      options.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;
    }

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size, options);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Branchless"] = static_cast<double>(use_branchless);
    state.counters["PatternType"] = static_cast<double>(pattern_type);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_Branchless_vs_Standard)
    ->ArgsProduct({{0, 1}, {0, 1, 2}}) // use_branchless: 0-1, pattern_type: 0-2
    ->Unit(benchmark::kMillisecond);

// Branchless state machine with varying file sizes
static void BM_Branchless_Scalability(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  int use_branchless = static_cast<int>(state.range(1));

  auto csv_data = generate_simd_test_data(rows, 10, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(1);
    libvroom::ParseOptions options;
    if (use_branchless) {
      options.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;
    }

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size, options);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Rows"] = static_cast<double>(rows);
    state.counters["Branchless"] = static_cast<double>(use_branchless);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

    // Calculate throughput in MB/s
    double bytes = static_cast<double>(buffer.size);
    state.counters["MB"] = bytes / (1024.0 * 1024.0);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_Branchless_Scalability)
    ->ArgsProduct({{1000, 5000, 10000, 50000}, {0, 1}}) // rows, use_branchless
    ->Unit(benchmark::kMillisecond);

// Branchless multithreaded performance comparison
static void BM_Branchless_Multithreaded(benchmark::State& state) {
  int n_threads = static_cast<int>(state.range(0));
  int use_branchless = static_cast<int>(state.range(1));

  size_t rows = 50000;
  size_t cols = 10;
  auto csv_data = generate_simd_test_data(rows, cols, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(n_threads);
    libvroom::ParseOptions options;
    if (use_branchless) {
      options.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;
    }

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size, options);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Threads"] = static_cast<double>(n_threads);
    state.counters["Branchless"] = static_cast<double>(use_branchless);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_Branchless_Multithreaded)
    ->ArgsProduct({{1, 2, 4, 8}, {0, 1}}) // n_threads, use_branchless
    ->Unit(benchmark::kMillisecond);

// Branch misprediction sensitive patterns
static void BM_Branchless_BranchSensitive(benchmark::State& state) {
  int use_branchless = static_cast<int>(state.range(0));
  int pattern_type = static_cast<int>(state.range(1));

  size_t data_size = 1024 * 1024; // 1MB
  auto data = static_cast<uint8_t*>(aligned_malloc(64, data_size + LIBVROOM_PADDING));

  std::random_device rd;
  std::mt19937 gen(rd());

  // Generate patterns that are particularly sensitive to branch prediction
  switch (pattern_type) {
  case 0: // Highly predictable: regular pattern
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 10 == 0)
        data[i] = ',';
      else if (i % 100 == 0)
        data[i] = '\n';
      else
        data[i] = 'a' + (i % 26);
    }
    break;
  case 1: // Unpredictable: random quotes
  {
    std::uniform_int_distribution<> dist(0, 99);
    bool in_quote = false;
    for (size_t i = 0; i < data_size; ++i) {
      if (!in_quote && dist(gen) < 5) {
        data[i] = '"';
        in_quote = true;
      } else if (in_quote && dist(gen) < 10) {
        data[i] = '"';
        in_quote = false;
      } else if (!in_quote && i % 10 == 0) {
        data[i] = ',';
      } else if (!in_quote && i % 100 == 0) {
        data[i] = '\n';
      } else {
        data[i] = 'a' + (i % 26);
      }
    }
    // Close any open quote
    if (in_quote)
      data[data_size - 1] = '"';
  } break;
  case 2: // Alternating: quote every other field
  {
    bool use_quote = false;
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 10 == 0) {
        data[i] = ',';
        use_quote = !use_quote;
      } else if (i % 100 == 0) {
        data[i] = '\n';
        use_quote = false;
      } else if (i % 10 == 1 && use_quote) {
        data[i] = '"';
      } else if (i % 10 == 9 && use_quote) {
        data[i] = '"';
      } else {
        data[i] = 'a' + (i % 26);
      }
    }
  } break;
  }

  // Add padding
  for (size_t i = data_size; i < data_size + LIBVROOM_PADDING; ++i) {
    data[i] = '\0';
  }

  libvroom::Parser parser(1);
  libvroom::ParseOptions options;
  if (use_branchless) {
    options.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;
  }

  for (auto _ : state) {
    auto result = parser.parse(data, data_size, options);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["Branchless"] = static_cast<double>(use_branchless);
  state.counters["PatternType"] = static_cast<double>(pattern_type);
  state.counters["DataSize"] = static_cast<double>(data_size);

  // Pattern descriptions for reporting
  const char* pattern_names[] = {"predictable", "random_quotes", "alternating"};
  state.SetLabel(pattern_names[pattern_type]);

  aligned_free(data);
}

BENCHMARK(BM_Branchless_BranchSensitive)
    ->ArgsProduct({{0, 1}, {0, 1, 2}}) // use_branchless, pattern_type
    ->Unit(benchmark::kMillisecond);