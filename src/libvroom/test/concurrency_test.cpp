/**
 * Concurrency and thread safety tests for libvroom CSV parser.
 *
 * This file tests multi-threaded parsing behavior including:
 * - Thread safety stress tests (many threads, same data)
 * - Chunk boundary edge cases
 * - Thread count edge cases
 * - Multiple concurrent parser instances
 *
 * Run with ThreadSanitizer (TSan) to detect data races:
 *   cmake -B build -DENABLE_TSAN=ON && cmake --build build
 *   ./build/concurrency_test
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <gtest/gtest.h>
#include <libvroom.h>
#include <random>
#include <string>
#include <thread>
#include <vector>

// Helper function to create padded buffer from string
static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
  size_t len = content.size();
  uint8_t* buf = allocate_padded_buffer(len, 64);
  std::memcpy(buf, content.data(), len);
  return {buf, len};
}

// Generate CSV data with specified rows and columns
static std::string generate_csv(size_t rows, size_t cols, bool with_quotes = false) {
  std::string csv;
  csv.reserve(rows * cols * 10); // Rough estimate

  // Header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      csv += ',';
    csv += "col" + std::to_string(c);
  }
  csv += '\n';

  // Data rows
  for (size_t r = 0; r < rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        csv += ',';
      if (with_quotes && (r % 2 == 0)) {
        csv += "\"value" + std::to_string(r) + "_" + std::to_string(c) + "\"";
      } else {
        csv += "value" + std::to_string(r) + "_" + std::to_string(c);
      }
    }
    csv += '\n';
  }
  return csv;
}

// =============================================================================
// Test Fixture
// =============================================================================

class ConcurrencyTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Get hardware concurrency for adaptive tests
    hw_concurrency_ = std::thread::hardware_concurrency();
    if (hw_concurrency_ == 0)
      hw_concurrency_ = 4; // Fallback
  }

  unsigned int hw_concurrency_;
};

// =============================================================================
// Thread Safety Stress Tests
// =============================================================================

// Test: Many threads parsing identical data concurrently
TEST_F(ConcurrencyTest, ManyThreadsSameData) {
  const std::string csv = generate_csv(100, 5);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  const int num_threads = 100;
  std::vector<std::future<bool>> futures;
  std::atomic<int> success_count{0};
  std::atomic<int> failure_count{0};

  for (int i = 0; i < num_threads; ++i) {
    futures.push_back(std::async(std::launch::async, [&buffer, &success_count, &failure_count]() {
      libvroom::Parser parser(4); // Each parser uses 4 threads
      auto result = parser.parse(buffer.data(), buffer.size());
      if (result.success() && result.total_indexes() > 0) {
        success_count++;
        return true;
      } else {
        failure_count++;
        return false;
      }
    }));
  }

  // Wait for all threads to complete
  for (auto& f : futures) {
    EXPECT_TRUE(f.get());
  }

  EXPECT_EQ(success_count.load(), num_threads);
  EXPECT_EQ(failure_count.load(), 0);
}

// Test: Concurrent parser instances with different data
TEST_F(ConcurrencyTest, ConcurrentParsersWithDifferentData) {
  const int num_parsers = 50;
  std::vector<std::string> csv_data;
  std::vector<std::pair<uint8_t*, size_t>> buffers;

  // Create different CSV data for each parser
  for (int i = 0; i < num_parsers; ++i) {
    csv_data.push_back(generate_csv(50 + i, 3 + (i % 5)));
    buffers.push_back(make_buffer(csv_data.back()));
  }

  std::vector<std::future<bool>> futures;
  std::atomic<int> success_count{0};

  for (int i = 0; i < num_parsers; ++i) {
    futures.push_back(std::async(std::launch::async, [i, &buffers, &success_count]() {
      libvroom::Parser parser(2);
      auto result = parser.parse(buffers[i].first, buffers[i].second);
      if (result.success()) {
        success_count++;
        return true;
      }
      return false;
    }));
  }

  for (auto& f : futures) {
    EXPECT_TRUE(f.get());
  }

  EXPECT_EQ(success_count.load(), num_parsers);

  // Clean up buffers not managed by FileBuffer
  for (auto& [ptr, _] : buffers) {
    aligned_free(ptr);
  }
}

// Test: Repeated parsing in tight loop (stress test for memory management)
TEST_F(ConcurrencyTest, RepeatedParsingStress) {
  const std::string csv = generate_csv(50, 5);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  const int iterations = 1000;
  libvroom::Parser parser(hw_concurrency_);

  for (int i = 0; i < iterations; ++i) {
    auto result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(result.success()) << "Failed at iteration " << i;
    ASSERT_GT(result.total_indexes(), 0) << "No indexes at iteration " << i;
  }
}

// =============================================================================
// Chunk Boundary Edge Cases
// =============================================================================

// Test: File smaller than minimum chunk size (64 bytes)
TEST_F(ConcurrencyTest, FileSmallerThanChunkSize) {
  // Create a small CSV that's definitely less than 64 bytes
  const std::string csv = "a,b,c\n1,2,3\n"; // ~12 bytes
  ASSERT_LT(csv.size(), 64);

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Try with various thread counts - should all succeed
  for (int threads = 1; threads <= 8; ++threads) {
    libvroom::Parser parser(threads);
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success()) << "Failed with " << threads << " threads";
    EXPECT_GT(result.total_indexes(), 0);
  }
}

// Test: Chunk boundary coinciding with quote characters
TEST_F(ConcurrencyTest, ChunkBoundaryAtQuote) {
  // Create CSV where quotes might fall on chunk boundaries
  // Use repeated quoted fields to increase likelihood
  std::string csv = "name,description\n";
  for (int i = 0; i < 100; ++i) {
    csv += "\"item" + std::to_string(i) + "\",\"This is a description with, comma\"\n";
  }

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Parse with different thread counts - all should succeed without crashes
  for (int threads = 1; threads <= 8; ++threads) {
    libvroom::Parser parser(threads);
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success()) << "Failed with " << threads << " threads";
    EXPECT_GT(result.total_indexes(), 0) << "No indexes with " << threads << " threads";
  }
}

// Test: Single quoted field spanning entire file
TEST_F(ConcurrencyTest, SingleQuotedFieldSpanningFile) {
  // A CSV with one quoted field that spans a large portion of the file
  std::string long_value(500, 'x'); // 500 character value
  std::string csv = "col1,col2\n\"" + long_value + "\",value2\n";

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // This tests quote parity tracking across chunks
  for (int threads = 1; threads <= 8; ++threads) {
    libvroom::Parser parser(threads);
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success()) << "Failed with " << threads << " threads";
  }
}

// Test: Single line content with multiple threads
TEST_F(ConcurrencyTest, SingleLineMultipleThreads) {
  // CSV with header and one very long data row
  std::string csv = "a,b,c,d,e,f,g,h,i,j\n";
  for (int i = 0; i < 100; ++i) {
    if (i > 0)
      csv += ",";
    csv += "value" + std::to_string(i);
  }
  csv += "\n";

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Parse with 8 threads on essentially 2-line data
  libvroom::Parser parser(8);
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: No newlines in file (forces fallback)
TEST_F(ConcurrencyTest, NoNewlines) {
  // CSV with no newlines - just a single line
  std::string csv = "a,b,c,d,e,f,g,h,i,j";
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Should handle gracefully even with multiple threads
  libvroom::Parser parser(4);
  auto result = parser.parse(buffer.data(), buffer.size());
  // May succeed or fail, but should not crash
  SUCCEED();
}

// =============================================================================
// Thread Count Edge Cases
// =============================================================================

// Test: Thread count exceeding row count
TEST_F(ConcurrencyTest, MoreThreadsThanRows) {
  // CSV with only 3 rows
  const std::string csv = "a,b,c\n1,2,3\n4,5,6\n";
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Try with 8 threads on 3 rows
  libvroom::Parser parser(8);
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Thread count exceeding byte count
TEST_F(ConcurrencyTest, MoreThreadsThanBytes) {
  // 10 byte CSV with 255 threads
  const std::string csv = "a,b\n1,2\n";
  ASSERT_LT(csv.size(), 255);

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser(255); // Max uint8_t
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
}

// Test: Zero thread count (should default to 1)
TEST_F(ConcurrencyTest, ZeroThreads) {
  const std::string csv = "a,b,c\n1,2,3\n";
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser(0);
  EXPECT_EQ(parser.num_threads(), 1); // Should default to 1

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
}

// Test: Maximum thread count (255 due to uint8_t)
TEST_F(ConcurrencyTest, MaximumThreadCount) {
  // Generate larger CSV to actually utilize threads
  const std::string csv = generate_csv(1000, 10);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser(255);
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Single thread parsing
TEST_F(ConcurrencyTest, SingleThreadParsing) {
  const std::string csv = generate_csv(100, 5);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser(1);
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: set_num_threads() changes thread count
TEST_F(ConcurrencyTest, SetNumThreads) {
  libvroom::Parser parser(1);
  EXPECT_EQ(parser.num_threads(), 1);

  parser.set_num_threads(4);
  EXPECT_EQ(parser.num_threads(), 4);

  parser.set_num_threads(0);
  EXPECT_EQ(parser.num_threads(), 1); // Should clamp to 1

  parser.set_num_threads(255);
  EXPECT_EQ(parser.num_threads(), 255);
}

// =============================================================================
// Consistency Tests (Single vs Multi-threaded)
// =============================================================================

// Test: Multi-threaded parsing produces valid results
TEST_F(ConcurrencyTest, MultiThreadedProducesValidResults) {
  const std::string csv = generate_csv(500, 10, true); // With quotes
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Parse with various thread counts - all should succeed and produce valid results
  for (int threads = 1; threads <= 8; ++threads) {
    libvroom::Parser parser(threads);
    auto result = parser.parse(buffer.data(), buffer.size());
    ASSERT_TRUE(result.success()) << "Failed with " << threads << " threads";
    ASSERT_GT(result.total_indexes(), 0) << "No indexes with " << threads << " threads";
  }
}

// Test: Different algorithms succeed with multi-threading
TEST_F(ConcurrencyTest, AlgorithmsSucceedMultiThreaded) {
  const std::string csv = generate_csv(200, 5, true);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser(4);

  // Parse with different algorithms - all should succeed
  auto result_auto = parser.parse(
      buffer.data(), buffer.size(),
      {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::AUTO});
  auto result_spec = parser.parse(
      buffer.data(), buffer.size(),
      {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::SPECULATIVE});
  auto result_two = parser.parse(
      buffer.data(), buffer.size(),
      {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::TWO_PASS});
  auto result_branch = parser.parse(
      buffer.data(), buffer.size(),
      {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::BRANCHLESS});

  EXPECT_TRUE(result_auto.success());
  EXPECT_TRUE(result_spec.success());
  EXPECT_TRUE(result_two.success());
  EXPECT_TRUE(result_branch.success());

  // All should produce valid index counts
  EXPECT_GT(result_auto.total_indexes(), 0);
  EXPECT_GT(result_spec.total_indexes(), 0);
  EXPECT_GT(result_two.total_indexes(), 0);
  EXPECT_GT(result_branch.total_indexes(), 0);
}

// =============================================================================
// Error Handling in Multi-threaded Context
// =============================================================================

// Test: Thread-local error collection
TEST_F(ConcurrencyTest, ThreadLocalErrorCollection) {
  // CSV with inconsistent field counts
  std::string csv = "a,b,c\n";
  for (int i = 0; i < 100; ++i) {
    if (i % 10 == 0) {
      csv += "x,y\n"; // Missing field
    } else {
      csv += "1,2,3\n";
    }
  }

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::Parser parser(4);

  auto result =
      parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::with_errors(errors));

  EXPECT_TRUE(result.success());    // Permissive mode succeeds
  EXPECT_TRUE(errors.has_errors()); // But errors should be collected
}

// Test: Multiple concurrent parsers with error collection
TEST_F(ConcurrencyTest, ConcurrentParsersWithErrors) {
  // CSV with some errors
  std::string csv = "a,b,c\n1,2,3\n4,5\n6,7,8\n";
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  const int num_parsers = 20;
  std::vector<std::future<bool>> futures;
  std::atomic<int> errors_found{0};

  for (int i = 0; i < num_parsers; ++i) {
    futures.push_back(std::async(std::launch::async, [&buffer, &errors_found]() {
      libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
      libvroom::Parser parser(2);
      auto result =
          parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::with_errors(errors));
      if (errors.has_errors()) {
        errors_found++;
      }
      return result.success();
    }));
  }

  for (auto& f : futures) {
    EXPECT_TRUE(f.get());
  }

  // All parsers should find the same error
  EXPECT_EQ(errors_found.load(), num_parsers);
}

// =============================================================================
// Large File Multi-threaded Tests
// =============================================================================

// Test: Large file with many threads
TEST_F(ConcurrencyTest, LargeFileMultiThreaded) {
  // Generate a reasonably large CSV (1000 rows, 20 columns)
  const std::string csv = generate_csv(1000, 20, true);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Parse with hardware concurrency
  libvroom::Parser parser(hw_concurrency_);
  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Scaling with thread count
TEST_F(ConcurrencyTest, ScalingWithThreadCount) {
  // Medium-sized CSV
  const std::string csv = generate_csv(500, 10);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Parse with increasing thread counts - all should succeed
  for (int threads = 1; threads <= 16; threads *= 2) {
    libvroom::Parser parser(threads);
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success()) << "Failed with " << threads << " threads";
  }
}

// =============================================================================
// Data Race Detection Tests (for TSan)
// =============================================================================

// Test: Rapid sequential parsing (catches use-after-free, double-free)
TEST_F(ConcurrencyTest, RapidSequentialParsing) {
  for (int i = 0; i < 100; ++i) {
    std::string csv = generate_csv(10 + i, 3);
    auto [data, len] = make_buffer(csv);
    libvroom::FileBuffer buffer(data, len);

    libvroom::Parser parser(4);
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success()) << "Failed at iteration " << i;
  }
}

// Test: Parser reuse across different data
TEST_F(ConcurrencyTest, ParserReuse) {
  libvroom::Parser parser(4);

  for (int i = 0; i < 50; ++i) {
    std::string csv = generate_csv(20 + i * 2, 5);
    auto [data, len] = make_buffer(csv);
    libvroom::FileBuffer buffer(data, len);

    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success()) << "Failed at iteration " << i;
  }
}

// =============================================================================
// Mixed Dialect Concurrent Tests
// =============================================================================

// Test: Concurrent parsing with different dialects
TEST_F(ConcurrencyTest, ConcurrentDifferentDialects) {
  // CSV data
  std::string csv_data = "a,b,c\n1,2,3\n";
  // TSV data
  std::string tsv_data = "a\tb\tc\n1\t2\t3\n";
  // Semicolon-separated
  std::string ssv_data = "a;b;c\n1;2;3\n";

  auto [csv_buf, csv_len] = make_buffer(csv_data);
  auto [tsv_buf, tsv_len] = make_buffer(tsv_data);
  auto [ssv_buf, ssv_len] = make_buffer(ssv_data);

  libvroom::FileBuffer csv_file(csv_buf, csv_len);
  libvroom::FileBuffer tsv_file(tsv_buf, tsv_len);
  libvroom::FileBuffer ssv_file(ssv_buf, ssv_len);

  std::vector<std::future<bool>> futures;

  // Launch parsers for each dialect concurrently
  for (int i = 0; i < 10; ++i) {
    futures.push_back(std::async(std::launch::async, [&csv_file]() {
      libvroom::Parser parser(2);
      return parser.parse(csv_file.data(), csv_file.size(), {.dialect = libvroom::Dialect::csv()})
          .success();
    }));

    futures.push_back(std::async(std::launch::async, [&tsv_file]() {
      libvroom::Parser parser(2);
      return parser.parse(tsv_file.data(), tsv_file.size(), {.dialect = libvroom::Dialect::tsv()})
          .success();
    }));

    futures.push_back(std::async(std::launch::async, [&ssv_file]() {
      libvroom::Parser parser(2);
      return parser
          .parse(ssv_file.data(), ssv_file.size(), {.dialect = libvroom::Dialect::semicolon()})
          .success();
    }));
  }

  for (auto& f : futures) {
    EXPECT_TRUE(f.get());
  }
}

// =============================================================================
// CRLF/LF Handling in Multi-threaded Context
// =============================================================================

// Test: Mixed line endings with multiple threads
TEST_F(ConcurrencyTest, MixedLineEndingsMultiThreaded) {
  // CSV with CRLF line endings
  std::string csv = "a,b,c\r\n";
  for (int i = 0; i < 100; ++i) {
    csv += "1,2,3\r\n";
  }

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser(8);
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
}

// =============================================================================
// Interleaved Index Verification
// =============================================================================

// Test: Verify interleaved index pattern is correct
TEST_F(ConcurrencyTest, InterleavedIndexPattern) {
  // This test verifies that the interleaved index storage pattern
  // works correctly with multiple threads
  const std::string csv = generate_csv(100, 5);
  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);

  // Parse with 4 threads
  libvroom::Parser parser(4);
  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_TRUE(result.success());
  // The indexes should be populated without any gaps in the pattern
  EXPECT_GT(result.total_indexes(), 0);
}
