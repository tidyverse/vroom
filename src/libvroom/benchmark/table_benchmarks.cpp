/**
 * @file table_benchmarks.cpp
 * @brief Benchmarks for Table construction and Arrow stream export.
 *
 * Validates Issue #632: Table::from_parsed_chunks() is O(1) (moves vectors,
 * no data copy), and multi-batch stream export avoids merge overhead.
 */

#include "libvroom.h"
#include "libvroom/table.h"

#include <benchmark/benchmark.h>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>

namespace {

// Generate CSV data with mixed types for benchmarking
std::string generate_csv(size_t num_rows, size_t num_cols) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> int_dist(0, 999999);
  std::uniform_real_distribution<double> dbl_dist(0.0, 1000.0);

  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < num_cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  // Data rows
  for (size_t r = 0; r < num_rows; ++r) {
    for (size_t c = 0; c < num_cols; ++c) {
      if (c > 0)
        oss << ',';
      switch (c % 3) {
      case 0:
        oss << int_dist(rng);
        break;
      case 1:
        oss << std::fixed << std::setprecision(2) << dbl_dist(rng);
        break;
      case 2:
        oss << "str_" << r << "_" << c;
        break;
      }
    }
    oss << '\n';
  }

  return oss.str();
}

// Write CSV to a temp file and return path
std::string write_temp_csv(const std::string& csv_data) {
  std::string path = "/tmp/table_benchmark.csv";
  std::ofstream out(path);
  out << csv_data;
  return path;
}

} // namespace

// =============================================================================
// BM_TableFromParsedChunks - Measure Table construction time (should be O(1))
// =============================================================================

static void BM_TableFromParsedChunks(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));
  size_t num_threads = static_cast<size_t>(state.range(2));

  std::string csv_data = generate_csv(num_rows, num_cols);
  std::string path = write_temp_csv(csv_data);

  libvroom::CsvOptions opts;
  opts.num_threads = num_threads;

  // Pre-parse to get schema
  libvroom::CsvReader reader(opts);
  reader.open(path);
  auto result = reader.read_all();
  if (!result.ok) {
    state.SkipWithError(result.error.c_str());
    return;
  }
  auto schema = reader.schema();

  for (auto _ : state) {
    // Re-parse each iteration to get fresh ParsedChunks
    state.PauseTiming();
    libvroom::CsvReader r(opts);
    r.open(path);
    auto res = r.read_all();
    if (!res.ok) {
      state.SkipWithError(res.error.c_str());
      return;
    }
    state.ResumeTiming();

    // This is what we're benchmarking: O(1) table construction
    auto table = libvroom::Table::from_parsed_chunks(schema, std::move(res.value));
    benchmark::DoNotOptimize(table);
  }

  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
  state.counters["Threads"] = static_cast<double>(num_threads);
}

BENCHMARK(BM_TableFromParsedChunks)
    ->Args({10000, 10, 1})
    ->Args({10000, 10, 4})
    ->Args({100000, 10, 1})
    ->Args({100000, 10, 4})
    ->Args({1000000, 10, 1})
    ->Args({1000000, 10, 4})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

// =============================================================================
// BM_TableStreamExport - Measure Arrow stream setup + consumption
// =============================================================================

static void BM_TableStreamExport(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));
  size_t num_threads = static_cast<size_t>(state.range(2));

  std::string csv_data = generate_csv(num_rows, num_cols);
  std::string path = write_temp_csv(csv_data);

  libvroom::CsvOptions opts;
  opts.num_threads = num_threads;

  // Parse once and create table
  libvroom::CsvReader reader(opts);
  reader.open(path);
  auto result = reader.read_all();
  if (!result.ok) {
    state.SkipWithError(result.error.c_str());
    return;
  }
  auto table = libvroom::Table::from_parsed_chunks(reader.schema(), std::move(result.value));

  size_t batch_count = 0;
  size_t total_rows_exported = 0;

  for (auto _ : state) {
    libvroom::ArrowArrayStream stream;
    table->export_to_stream(&stream);

    // Consume all batches
    batch_count = 0;
    total_rows_exported = 0;
    while (true) {
      libvroom::ArrowArray batch;
      stream.get_next(&stream, &batch);
      if (batch.release == nullptr)
        break;
      total_rows_exported += static_cast<size_t>(batch.length);
      batch_count++;
      batch.release(&batch);
    }

    stream.release(&stream);
  }

  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
  state.counters["Threads"] = static_cast<double>(num_threads);
  state.counters["Batches"] = static_cast<double>(batch_count);
  state.counters["ExportedRows"] = static_cast<double>(total_rows_exported);
}

BENCHMARK(BM_TableStreamExport)
    ->Args({10000, 10, 1})
    ->Args({10000, 10, 4})
    ->Args({100000, 10, 1})
    ->Args({100000, 10, 4})
    ->Args({1000000, 10, 1})
    ->Args({1000000, 10, 4})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

// =============================================================================
// BM_EndToEndReadToStream - Full pipeline: read CSV -> Table -> stream
// =============================================================================

static void BM_EndToEndReadToStream(benchmark::State& state) {
  size_t num_rows = static_cast<size_t>(state.range(0));
  size_t num_cols = static_cast<size_t>(state.range(1));
  size_t num_threads = static_cast<size_t>(state.range(2));

  std::string csv_data = generate_csv(num_rows, num_cols);
  std::string path = write_temp_csv(csv_data);

  libvroom::CsvOptions opts;
  opts.num_threads = num_threads;

  size_t batch_count = 0;

  for (auto _ : state) {
    // Full pipeline
    libvroom::CsvReader reader(opts);
    reader.open(path);
    auto result = reader.read_all();
    if (!result.ok) {
      state.SkipWithError(result.error.c_str());
      return;
    }
    auto table = libvroom::Table::from_parsed_chunks(reader.schema(), std::move(result.value));

    // Export and consume stream
    libvroom::ArrowArrayStream stream;
    table->export_to_stream(&stream);

    batch_count = 0;
    while (true) {
      libvroom::ArrowArray batch;
      stream.get_next(&stream, &batch);
      if (batch.release == nullptr)
        break;
      batch_count++;
      batch.release(&batch);
    }
    stream.release(&stream);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(num_rows);
  state.counters["Cols"] = static_cast<double>(num_cols);
  state.counters["Threads"] = static_cast<double>(num_threads);
  state.counters["Batches"] = static_cast<double>(batch_count);
}

BENCHMARK(BM_EndToEndReadToStream)
    ->Args({10000, 10, 1})
    ->Args({10000, 10, 4})
    ->Args({100000, 10, 1})
    ->Args({100000, 10, 4})
    ->Args({1000000, 10, 1})
    ->Args({1000000, 10, 4})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
