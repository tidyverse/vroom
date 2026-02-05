#include "libvroom.h"

#include <benchmark/benchmark.h>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <thread>

#ifdef __linux__
#include <cstdint>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

extern std::map<std::string, libvroom::AlignedBuffer> test_data;

// Energy efficiency benchmarks (Linux RAPL counters when available)

#ifdef __linux__
// Linux RAPL (Running Average Power Limit) energy measurement
class RAPLEnergyMonitor {
private:
  std::vector<std::pair<std::string, int>> rapl_fds;
  std::vector<std::pair<std::string, double>> rapl_scales;

public:
  RAPLEnergyMonitor() {
    // Try to open RAPL energy measurement files
    std::vector<std::string> rapl_paths = {
        "/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj",   // Package
        "/sys/class/powercap/intel-rapl/intel-rapl:0:0/energy_uj", // Core
        "/sys/class/powercap/intel-rapl/intel-rapl:0:1/energy_uj", // Uncore
        "/sys/class/powercap/intel-rapl/intel-rapl:0:2/energy_uj"  // DRAM
    };

    std::vector<std::string> rapl_names = {"Package", "Core", "Uncore", "DRAM"};

    for (size_t i = 0; i < rapl_paths.size() && i < rapl_names.size(); ++i) {
      int fd = open(rapl_paths[i].c_str(), O_RDONLY);
      if (fd >= 0) {
        rapl_fds.push_back({rapl_names[i], fd});
        rapl_scales.push_back({rapl_names[i], 1.0e-6}); // microjoules to joules
      }
    }
  }

  ~RAPLEnergyMonitor() {
    for (auto& fd_pair : rapl_fds) {
      close(fd_pair.second);
    }
  }

  bool available() const { return !rapl_fds.empty(); }

  std::vector<std::pair<std::string, double>> read_energy() {
    std::vector<std::pair<std::string, double>> energies;

    for (size_t i = 0; i < rapl_fds.size(); ++i) {
      lseek(rapl_fds[i].second, 0, SEEK_SET);
      char buffer[64];
      ssize_t bytes_read = read(rapl_fds[i].second, buffer, sizeof(buffer) - 1);

      if (bytes_read > 0) {
        buffer[bytes_read] = '\0';
        uint64_t energy_uj = std::stoull(buffer);
        double energy_j = energy_uj * rapl_scales[i].second;
        energies.push_back({rapl_fds[i].first, energy_j});
      }
    }

    return energies;
  }
};
#else
// Stub for non-Linux systems
class RAPLEnergyMonitor {
public:
  bool available() const { return false; }
  std::vector<std::pair<std::string, double>> read_energy() { return {}; }
};
#endif

// Energy per byte benchmark
static void BM_EnergyPerByte(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));

  // Create test data as a CSV temp file
  std::string csv_data;
  csv_data.reserve(data_size + LIBVROOM_PADDING);

  // Fill with CSV-like pattern
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      csv_data += '\n';
    else if (i % 10 == 0)
      csv_data += ',';
    else
      csv_data += static_cast<char>('a' + (i % 26));
  }

  // Write to temp file
  std::string temp_path = "/tmp/libvroom_energy_" + std::to_string(data_size) + ".csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 4;

  RAPLEnergyMonitor energy_monitor;

  // Measure energy if available
  auto start_energy = energy_monitor.read_energy();
  auto start_time = std::chrono::high_resolution_clock::now();

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(temp_path);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto end_energy = energy_monitor.read_energy();

  // Silence unused variable warning
  (void)start_time;
  (void)end_time;

  state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
  state.counters["DataSize"] = static_cast<double>(data_size);

  // Calculate energy efficiency if RAPL is available
  if (energy_monitor.available() && start_energy.size() == end_energy.size()) {
    for (size_t i = 0; i < start_energy.size(); ++i) {
      double energy_consumed = end_energy[i].second - start_energy[i].second;

      state.counters[start_energy[i].first + "_Energy_J"] = energy_consumed;
    }
  }

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_EnergyPerByte)
    ->RangeMultiplier(4)
    ->Range(1024, 16 * 1024 * 1024) // 1KB to 16MB
    ->Unit(benchmark::kMillisecond);

// Energy efficiency vs thread count
static void BM_EnergyEfficiency_ThreadCount(benchmark::State& state) {
  int n_threads = static_cast<int>(state.range(0));

  // Use a medium-sized file
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
  libvroom::CsvOptions opts;
  opts.num_threads = static_cast<size_t>(n_threads);

  RAPLEnergyMonitor energy_monitor;

  // Measure energy and performance
  auto start_energy = energy_monitor.read_energy();
  auto start_time = std::chrono::high_resolution_clock::now();

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    reader.open(filename);
    auto result = reader.read_all();
    benchmark::DoNotOptimize(result);
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto end_energy = energy_monitor.read_energy();

  // Silence unused variable warning
  (void)start_time;
  (void)end_time;

  state.SetBytesProcessed(static_cast<int64_t>(buffer.size() * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["FileSize"] = static_cast<double>(buffer.size());

  // Energy efficiency metrics
  if (energy_monitor.available() && start_energy.size() == end_energy.size()) {
    for (size_t i = 0; i < start_energy.size(); ++i) {
      double energy_consumed = end_energy[i].second - start_energy[i].second;

      state.counters[start_energy[i].first + "_Energy_J"] = energy_consumed;
    }
  }
}

BENCHMARK(BM_EnergyEfficiency_ThreadCount)
    ->DenseRange(1, 8, 1) // 1 to 8 threads
    ->Unit(benchmark::kMillisecond);

// Power consumption estimate (without RAPL)
static void BM_PowerConsumption_Estimate(benchmark::State& state) {
  size_t workload_size = static_cast<size_t>(state.range(0));

  // Create synthetic workload as CSV temp file
  size_t data_size = 1024 * 1024; // 1MB base
  std::string csv_data;
  csv_data.reserve(data_size);

  // Fill with data
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 100 == 0)
      csv_data += '\n';
    else if (i % 10 == 0)
      csv_data += ',';
    else
      csv_data += static_cast<char>('a' + (i % 26));
  }

  std::string temp_path = "/tmp/libvroom_power_estimate.csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 4;

  // Measure CPU usage time as proxy for power consumption
  auto start_time = std::chrono::high_resolution_clock::now();

  for (auto _ : state) {
    // Repeat parsing based on workload size
    for (size_t i = 0; i < workload_size; ++i) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_path);
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();

  double duration = std::chrono::duration<double>(end_time - start_time).count();
  double total_bytes = static_cast<double>(data_size * workload_size * state.iterations());

  state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
  state.counters["WorkloadSize"] = static_cast<double>(workload_size);
  state.counters["CPU_Time_s"] = duration;

  // Estimate power consumption based on CPU usage
  // Assume typical CPU power consumption: 15-65W for mobile, 65-125W for desktop
  double estimated_cpu_power = 45.0;                        // Watts (conservative estimate)
  double estimated_energy = estimated_cpu_power * duration; // Joules

  state.counters["Est_CPU_Power_W"] = estimated_cpu_power;
  state.counters["Est_Energy_J"] = estimated_energy;

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_PowerConsumption_Estimate)
    ->Arg(1)  // Light workload
    ->Arg(5)  // Medium workload
    ->Arg(10) // Heavy workload
    ->Unit(benchmark::kMillisecond);

// Idle vs active power measurement
static void BM_IdleVsActive_Power(benchmark::State& state) {
  int active_mode = static_cast<int>(state.range(0)); // 0 = idle, 1 = active

  if (active_mode == 0) {
    // Idle measurement - just sleep
    for (auto _ : state) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      benchmark::DoNotOptimize(active_mode);
    }

    state.counters["Mode"] = 0.0; // Idle
  } else {
    // Active measurement - do parsing work
    size_t data_size = 512 * 1024; // 512KB
    std::string csv_data;
    csv_data.reserve(data_size);

    // Fill with CSV pattern
    for (size_t i = 0; i < data_size; ++i) {
      if (i % 100 == 0)
        csv_data += '\n';
      else if (i % 10 == 0)
        csv_data += ',';
      else
        csv_data += static_cast<char>('a' + (i % 26));
    }

    std::string temp_path = "/tmp/libvroom_idle_active.csv";
    {
      std::ofstream out(temp_path);
      out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
    }

    libvroom::CsvOptions opts;
    opts.num_threads = 4;

    for (auto _ : state) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_path);
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(static_cast<int64_t>(data_size * state.iterations()));
    state.counters["Mode"] = 1.0; // Active
    state.counters["DataSize"] = static_cast<double>(data_size);

    std::remove(temp_path.c_str());
  }
}

BENCHMARK(BM_IdleVsActive_Power)
    ->Arg(0) // Idle
    ->Arg(1) // Active
    ->Unit(benchmark::kMicrosecond);

// Temperature impact benchmark (proxy)
static void BM_ThermalThrottling_Impact(benchmark::State& state) {
  size_t duration_ms = static_cast<size_t>(state.range(0));

  // Sustained workload to potentially trigger thermal throttling
  size_t data_size = 2 * 1024 * 1024; // 2MB
  std::string csv_data;
  csv_data.reserve(data_size);

  // Fill with intensive pattern
  for (size_t i = 0; i < data_size; ++i) {
    if (i % 5 == 0)
      csv_data += '"'; // Quote heavy for complex processing
    else if (i % 50 == 0)
      csv_data += '\n';
    else if (i % 8 == 0)
      csv_data += ',';
    else
      csv_data += static_cast<char>('a' + (i % 26));
  }

  std::string temp_path = "/tmp/libvroom_thermal.csv";
  {
    std::ofstream out(temp_path);
    out.write(csv_data.data(), static_cast<std::streamsize>(csv_data.size()));
  }

  libvroom::CsvOptions opts;
  opts.num_threads = 4;

  auto start_time = std::chrono::high_resolution_clock::now();
  auto target_duration = std::chrono::milliseconds(duration_ms);

  size_t iterations = 0;
  for (auto _ : state) {
    auto current_time = std::chrono::high_resolution_clock::now();

    // Run for specified duration
    while (current_time - start_time < target_duration) {
      libvroom::CsvReader reader(opts);
      reader.open(temp_path);
      auto result = reader.read_all();
      benchmark::DoNotOptimize(result);
      iterations++;
      current_time = std::chrono::high_resolution_clock::now();
    }

    break; // Only do one iteration for this benchmark
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  double actual_duration = std::chrono::duration<double>(end_time - start_time).count();
  double total_bytes = static_cast<double>(data_size * iterations);

  state.SetBytesProcessed(static_cast<int64_t>(total_bytes));
  state.counters["Duration_ms"] = actual_duration * 1000.0;
  state.counters["Iterations"] = static_cast<double>(iterations);

  std::remove(temp_path.c_str());
}

BENCHMARK(BM_ThermalThrottling_Impact)
    ->Arg(1000)  // 1 second sustained
    ->Arg(5000)  // 5 seconds sustained
    ->Arg(10000) // 10 seconds sustained
    ->Unit(benchmark::kSecond);
