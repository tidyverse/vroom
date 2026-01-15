#include "libvroom.h"

#include "common_defs.h"
#include "mem_util.h"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Generate synthetic CSV data for real-world scenarios
class CSVDataGenerator {
public:
  // Generate NYC Taxi Trip data (similar to real NYC taxi dataset)
  static std::string generate_nyc_taxi_data(size_t num_rows) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> lat_dist(40.7, 40.8);   // Manhattan lat range
    std::uniform_real_distribution<> lon_dist(-74.0, -73.9); // Manhattan lon range
    std::uniform_real_distribution<> fare_dist(5.0, 50.0);
    std::uniform_real_distribution<> distance_dist(0.5, 20.0);
    std::uniform_int_distribution<> passenger_dist(1, 6);
    std::uniform_int_distribution<> payment_dist(1, 4);
    std::uniform_int_distribution<> vendor_dist(1, 2);

    std::ostringstream ss;

    // NYC Taxi dataset header (19 columns like real dataset)
    ss << "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,"
       << "pickup_longitude,pickup_latitude,RatecodeID,store_and_fwd_flag,"
       << "dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,"
       << "tip_amount,tolls_amount,improvement_surcharge,total_amount\n";

    for (size_t i = 0; i < num_rows; ++i) {
      // Generate realistic taxi trip data
      double trip_distance = distance_dist(gen);
      double fare = fare_dist(gen) + trip_distance * 2.5;        // Base fare + distance
      double tip = (payment_dist(gen) == 1) ? fare * 0.15 : 0.0; // Tips for credit cards

      ss << vendor_dist(gen) << ",";
      ss << "2024-01-" << std::setfill('0') << std::setw(2) << ((i % 30) + 1) << " " << std::setw(2)
         << (i % 24) << ":" << std::setw(2) << (i % 60) << ":00,";
      ss << "2024-01-" << std::setfill('0') << std::setw(2) << ((i % 30) + 1) << " " << std::setw(2)
         << ((i + 1) % 24) << ":" << std::setw(2) << ((i + 30) % 60) << ":00,";
      ss << passenger_dist(gen) << ",";
      ss << std::fixed << std::setprecision(2) << trip_distance << ",";
      ss << std::setprecision(6) << lon_dist(gen) << ",";
      ss << lat_dist(gen) << ",";
      ss << "1,"; // RatecodeID
      ss << "N,"; // store_and_fwd_flag
      ss << lon_dist(gen) << ",";
      ss << lat_dist(gen) << ",";
      ss << payment_dist(gen) << ",";
      ss << std::setprecision(2) << fare << ",";
      ss << "0.50,"; // extra
      ss << "0.50,"; // mta_tax
      ss << tip << ",";
      ss << "0.00,";                     // tolls_amount
      ss << "0.30,";                     // improvement_surcharge
      ss << (fare + tip + 1.30) << "\n"; // total_amount
    }

    return ss.str();
  }

  // Generate financial data (timestamp, symbol, price, volume, etc.)
  static std::string generate_financial_data(size_t num_rows, size_t num_cols = 8) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> price_dist(10.0, 1000.0);
    std::uniform_int_distribution<> volume_dist(100, 100000);

    std::ostringstream ss;

    // Header
    ss << "timestamp,symbol,open,high,low,close,volume,adj_close\n";

    for (size_t i = 0; i < num_rows; ++i) {
      ss << "2024-01-01T" << std::setfill('0') << std::setw(2) << (i % 24) << ":" << std::setw(2)
         << (i % 60) << ":00,";
      ss << "STOCK" << (i % 100) << ",";

      double base_price = price_dist(gen);
      ss << std::fixed << std::setprecision(2) << base_price << ",";
      ss << base_price * 1.02 << ",";
      ss << base_price * 0.98 << ",";
      ss << base_price * 1.01 << ",";
      ss << volume_dist(gen) << ",";
      ss << base_price * 1.01 << "\n";
    }

    return ss.str();
  }

  // Generate genomics data (sequence information)
  static std::string generate_genomics_data(size_t num_rows, size_t num_cols = 10) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> base_dist(0, 3);
    std::uniform_real_distribution<> quality_dist(0.0, 40.0);

    const char bases[] = {'A', 'C', 'G', 'T'};
    std::ostringstream ss;

    // Header
    ss << "seq_id,chromosome,position,ref_base,alt_base,quality_score,coverage,frequency,"
          "annotation,sample_id\n";

    for (size_t i = 0; i < num_rows; ++i) {
      ss << "seq_" << i << ",";
      ss << "chr" << (i % 22 + 1) << ",";
      ss << (i * 1000) + (i % 1000) << ",";
      ss << bases[base_dist(gen)] << ",";
      ss << bases[base_dist(gen)] << ",";
      ss << std::fixed << std::setprecision(1) << quality_dist(gen) << ",";
      ss << (50 + (i % 200)) << ",";
      ss << std::setprecision(3) << (0.1 + (i % 100) / 1000.0) << ",";
      ss << "annotation_" << (i % 10) << ",";
      ss << "sample_" << (i % 20) << "\n";
    }

    return ss.str();
  }

  // Generate log file data (timestamp, level, message, etc.)
  static std::string generate_log_data(size_t num_rows, size_t num_cols = 6) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> level_dist(0, 4);
    std::uniform_int_distribution<> msg_dist(0, 9);

    const std::vector<std::string> levels = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
    const std::vector<std::string> messages = {"Connection established successfully",
                                               "Processing request from client",
                                               "Database query completed",
                                               "Cache miss for key",
                                               "Memory allocation failed",
                                               "Network timeout occurred",
                                               "Authentication successful",
                                               "Invalid input parameter",
                                               "Service started successfully",
                                               "Cleanup process initiated"};

    std::ostringstream ss;

    // Header
    ss << "timestamp,level,thread_id,component,message,duration_ms\n";

    for (size_t i = 0; i < num_rows; ++i) {
      ss << "2024-01-01T" << std::setfill('0') << std::setw(2) << (i % 24) << ":" << std::setw(2)
         << (i % 60) << ":" << std::setw(2) << (i % 60) << ",";
      ss << levels[level_dist(gen)] << ",";
      ss << "thread-" << (i % 10) << ",";
      ss << "component-" << (i % 5) << ",";
      ss << "\"" << messages[msg_dist(gen)] << " (ID: " << i << ")\",";
      ss << (i % 1000) << "\n";
    }

    return ss.str();
  }

  // Generate wide table (many columns)
  static std::string generate_wide_table(size_t num_rows, size_t num_cols = 100) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> val_dist(0.0, 100.0);

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
        if (col % 3 == 0) {
          ss << static_cast<int>(val_dist(gen));
        } else if (col % 3 == 1) {
          ss << std::fixed << std::setprecision(2) << val_dist(gen);
        } else {
          ss << "\"text_" << row << "_" << col << "\"";
        }
      }
      ss << "\n";
    }

    return ss.str();
  }
};

// Helper to create temporary file for benchmarking
class TempFile {
private:
  std::string filename_;

public:
  explicit TempFile(const std::string& content)
      : filename_("/tmp/libvroom_benchmark_" + std::to_string(std::random_device{}())) {
    std::ofstream file(filename_);
    file << content;
    file.close();
  }

  ~TempFile() { std::remove(filename_.c_str()); }

  const std::string& path() const { return filename_; }
};

// Benchmark real-world data types
static void BM_financial_data(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  auto data_str = CSVDataGenerator::generate_financial_data(num_rows);
  TempFile temp_file(data_str);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(4);

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}
BENCHMARK(BM_financial_data)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kMillisecond);

// NYC Taxi data benchmark (realistic large dataset)
static void BM_nyc_taxi_data(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  auto data_str = CSVDataGenerator::generate_nyc_taxi_data(num_rows);
  TempFile temp_file(data_str);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(4);

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["FileSize"] = static_cast<double>(buffer.size);
    state.counters["Columns"] = 19.0; // NYC taxi has 19 columns

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}
BENCHMARK(BM_nyc_taxi_data)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000) // 1K to 1M rows (like real taxi data)
    ->Unit(benchmark::kMillisecond);

static void BM_genomics_data(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  auto data_str = CSVDataGenerator::generate_genomics_data(num_rows);
  TempFile temp_file(data_str);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(4);

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}
BENCHMARK(BM_genomics_data)
    ->RangeMultiplier(10)
    ->Range(1000, 100000)
    ->Unit(benchmark::kMillisecond);

static void BM_log_data(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  auto data_str = CSVDataGenerator::generate_log_data(num_rows);
  TempFile temp_file(data_str);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(4);

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}
BENCHMARK(BM_log_data)->RangeMultiplier(10)->Range(1000, 500000)->Unit(benchmark::kMillisecond);

static void BM_wide_table(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));
  auto data_str = CSVDataGenerator::generate_wide_table(num_rows, num_cols);
  TempFile temp_file(data_str);

  try {
    auto buffer = libvroom::load_file_to_ptr(temp_file.path(), LIBVROOM_PADDING);
    if (!buffer) {
      state.SkipWithError("Failed to load temp file");
      return;
    }

    libvroom::Parser parser(4);

    for (auto _ : state) {
      auto result = parser.parse(buffer.data(), buffer.size);
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
    state.counters["Rows"] = static_cast<double>(num_rows);
    state.counters["Cols"] = static_cast<double>(num_cols);
    state.counters["FileSize"] = static_cast<double>(buffer.size);

  } catch (const std::exception& e) {
    state.SkipWithError(e.what());
  }
}
BENCHMARK(BM_wide_table)
    ->Ranges({{100, 10000}, {10, 1000}}) // rows x columns
    ->Unit(benchmark::kMillisecond);

// SIMD instruction level benchmarks
static void BM_simd_levels(benchmark::State& state) {
  // This would require modifying the parser to expose SIMD level selection
  // For now, we'll benchmark the default parser
  std::string filename = "test/data/basic/many_rows.csv";

  if (test_data.find(filename) == test_data.end()) {
    try {
      auto buffer = libvroom::load_file_to_ptr(filename, LIBVROOM_PADDING);
      if (!buffer) {
        state.SkipWithError(("Failed to load " + filename).c_str());
        return;
      }
      test_data.emplace(filename, std::move(buffer));
    } catch (const std::exception& e) {
      state.SkipWithError(("Failed to load " + filename + ": " + e.what()).c_str());
      return;
    }
  }

  const auto& buffer = test_data.at(filename);
  libvroom::Parser parser(1);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data(), buffer.size);
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size * state.iterations()));
  state.counters["SIMD"] = 1.0; // Default SIMD level
}
BENCHMARK(BM_simd_levels)->Unit(benchmark::kMillisecond);