#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Synthetic data generator for dimension testing
class DimensionDataGenerator {
public:
  // Generate CSV with specific dimensions
  static std::string generate_csv(size_t num_rows, size_t num_cols,
                                  const std::string& data_type = "mixed") {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> float_dist(0.0, 1000.0);
    std::uniform_int_distribution<> int_dist(1, 100000);
    std::uniform_int_distribution<> string_len_dist(5, 50);

    std::ostringstream ss;

    // Header
    for (size_t col = 0; col < num_cols; ++col) {
      if (col > 0)
        ss << ",";
      ss << "col_" << col;
    }
    ss << "\n";

    // Data rows
    for (size_t row = 0; row < num_rows; ++row) {
      for (size_t col = 0; col < num_cols; ++col) {
        if (col > 0)
          ss << ",";

        if (data_type == "integers") {
          ss << int_dist(gen);
        } else if (data_type == "floats") {
          ss << std::fixed << std::setprecision(3) << float_dist(gen);
        } else if (data_type == "strings") {
          ss << "\"string_" << row << "_" << col << "\"";
        } else { // mixed
          switch (col % 3) {
          case 0:
            ss << int_dist(gen);
            break;
          case 1:
            ss << std::fixed << std::setprecision(2) << float_dist(gen);
            break;
          case 2:
            ss << "\"text_" << row << "_" << col << "\"";
            break;
          }
        }
      }
      ss << "\n";
    }

    return ss.str();
  }
};

// Helper for temporary files
class TempCSVFile {
private:
  std::string filename_;

public:
  explicit TempCSVFile(const std::string& content)
      : filename_("/tmp/libvroom_dim_" + std::to_string(std::random_device{}()) + ".csv") {
    std::ofstream file(filename_);
    file << content;
    file.close();
  }

  ~TempCSVFile() { std::remove(filename_.c_str()); }

  const std::string& path() const { return filename_; }
};

// Benchmark different file sizes (1KB to 100MB)
static void BM_FileSizes(benchmark::State& state) {
  size_t target_size = static_cast<size_t>(state.range(0));

  // Estimate rows needed for target size (rough calculation)
  size_t cols = 10;
  size_t avg_field_size = 8; // average characters per field
  size_t estimated_rows = target_size / (cols * avg_field_size);
  estimated_rows = std::max(estimated_rows, size_t(10)); // minimum 10 rows

  auto csv_data = DimensionDataGenerator::generate_csv(estimated_rows, cols, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    size_t file_size = csv_data.size();

    libvroom::CsvOptions opts;
    opts.num_threads = 4;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    // Performance metrics
    state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
    state.counters["ActualSize"] = static_cast<double>(file_size);
    state.counters["TargetSize"] = static_cast<double>(target_size);
    state.counters["Rows"] = static_cast<double>(estimated_rows);
    state.counters["Cols"] = static_cast<double>(cols);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_FileSizes)
    ->RangeMultiplier(10)
    ->Range(1024, 100 * 1024 * 1024) // 1KB to 100MB
    ->Unit(benchmark::kMillisecond);

// Benchmark different column counts
static void BM_ColumnCounts(benchmark::State& state) {
  size_t num_cols = static_cast<size_t>(state.range(0));
  size_t num_rows = 1000; // Fixed row count

  auto csv_data = DimensionDataGenerator::generate_csv(num_rows, num_cols, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    size_t file_size = csv_data.size();

    libvroom::CsvOptions opts;
    opts.num_threads = 4;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    // Performance metrics
    state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
    state.counters["FileSize"] = static_cast<double>(file_size);
    state.counters["Columns"] = static_cast<double>(num_cols);
    state.counters["Rows"] = static_cast<double>(num_rows);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_ColumnCounts)
    ->DenseRange(2, 20, 2) // 2, 4, 6, ..., 20 columns
    ->Arg(50)              // 50 columns
    ->Arg(100)             // 100 columns
    ->Arg(500)             // 500 columns
    ->Unit(benchmark::kMillisecond);

// Benchmark different data types
static void BM_DataTypes(benchmark::State& state) {
  int data_type = static_cast<int>(state.range(0));
  size_t num_rows = 5000;
  size_t num_cols = 10;

  std::string type_name;
  switch (data_type) {
  case 0:
    type_name = "integers";
    break;
  case 1:
    type_name = "floats";
    break;
  case 2:
    type_name = "strings";
    break;
  case 3:
    type_name = "mixed";
    break;
  default:
    type_name = "mixed";
    break;
  }

  auto csv_data = DimensionDataGenerator::generate_csv(num_rows, num_cols, type_name);
  TempCSVFile temp_file(csv_data);

  try {
    size_t file_size = csv_data.size();

    libvroom::CsvOptions opts;
    opts.num_threads = 4;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    // Performance metrics
    state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
    state.counters["FileSize"] = static_cast<double>(file_size);
    state.counters["DataType"] = static_cast<double>(data_type);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_DataTypes)
    ->DenseRange(0, 3, 1) // 0=integers, 1=floats, 2=strings, 3=mixed
    ->Unit(benchmark::kMillisecond);

// Benchmark thread scaling
static void BM_ThreadScaling(benchmark::State& state) {
  int n_threads = static_cast<int>(state.range(0));
  size_t num_rows = 10000;
  size_t num_cols = 20;

  auto csv_data = DimensionDataGenerator::generate_csv(num_rows, num_cols, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    size_t file_size = csv_data.size();

    libvroom::CsvOptions opts;
    opts.num_threads = static_cast<size_t>(n_threads);

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    // Performance metrics
    state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
    state.counters["FileSize"] = static_cast<double>(file_size);
    state.counters["Threads"] = static_cast<double>(n_threads);

    // Calculate efficiency metrics
    if (n_threads > 1) {
      // This is approximate - would need baseline single-thread measurement
      // for accurate efficiency calculation
    }

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_ThreadScaling)
    ->DenseRange(1, 16, 1) // 1 to 16 threads
    ->Unit(benchmark::kMillisecond);

// Benchmark row count scaling
static void BM_RowScaling(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = 10;

  auto csv_data = DimensionDataGenerator::generate_csv(num_rows, num_cols, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    size_t file_size = csv_data.size();

    libvroom::CsvOptions opts;
    opts.num_threads = 4;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    // Performance metrics
    state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
    state.counters["FileSize"] = static_cast<double>(file_size);
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["BytesPerRecord"] = static_cast<double>(file_size) / num_rows;

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_RowScaling)
    ->RangeMultiplier(10)
    ->Range(100, 1000000) // 100 to 1M rows
    ->Unit(benchmark::kMillisecond);

// Comprehensive dimensions benchmark (rows x columns matrix)
static void BM_RowColumnMatrix(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));

  auto csv_data = DimensionDataGenerator::generate_csv(num_rows, num_cols, "mixed");
  TempCSVFile temp_file(csv_data);

  try {
    size_t file_size = csv_data.size();

    libvroom::CsvOptions opts;
    opts.num_threads = 4;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_file.path());
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    // Comprehensive performance metrics
    state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
    state.counters["FileSize"] = static_cast<double>(file_size);
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["Cols"] = static_cast<double>(num_cols);
    state.counters["TotalFields"] = static_cast<double>(num_rows * num_cols);
    state.counters["BytesPerField"] = static_cast<double>(file_size) / (num_rows * num_cols);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}

BENCHMARK(BM_RowColumnMatrix)
    ->Ranges({{100, 10000}, {5, 100}}) // rows: 100-10k, columns: 5-100
    ->Unit(benchmark::kMillisecond);
