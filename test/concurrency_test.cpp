/**
 * @file concurrency_test.cpp
 * @brief Multi-threaded CSV parsing safety and correctness tests.
 *
 * Rewritten from old concurrency_test.cpp to use the libvroom2 CsvReader API.
 * Tests thread safety, deterministic results, and concurrent reader instances.
 *
 * @see GitHub issue #626
 */

#include "libvroom.h"

#include "test_util.h"

#include <cstdio>
#include <fstream>
#include <future>
#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// CSV data generators
// ---------------------------------------------------------------------------

static std::string generate_csv(size_t rows, size_t cols, bool with_quotes = false) {
  std::ostringstream oss;
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  for (size_t r = 0; r < rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';
      if (with_quotes && (r % 2 == 0)) {
        oss << "\"value" << r << "_" << c << "\"";
      } else {
        oss << "value" << r << "_" << c;
      }
    }
    oss << '\n';
  }
  return oss.str();
}

static std::string generate_numeric_csv(size_t rows, size_t cols) {
  std::ostringstream oss;
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0)
      oss << ',';
    oss << "col" << c;
  }
  oss << '\n';

  for (size_t r = 0; r < rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0)
        oss << ',';
      oss << (r * cols + c);
    }
    oss << '\n';
  }
  return oss.str();
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class ConcurrencyTest : public ::testing::Test {
protected:
  void SetUp() override {
    hw_concurrency_ = std::thread::hardware_concurrency();
    if (hw_concurrency_ == 0)
      hw_concurrency_ = 4;
  }

  // Helper: parse a file and return total_rows, schema size, and success flag.
  struct ParseResult {
    size_t total_rows = 0;
    size_t num_columns = 0;
    bool ok = false;
  };

  ParseResult parse_file(const std::string& path, libvroom::CsvOptions opts = {}) {
    libvroom::CsvReader reader(opts);
    auto open_result = reader.open(path);
    if (!open_result.ok)
      return {};
    auto read_result = reader.read_all();
    if (!read_result.ok)
      return {};
    return {read_result.value.total_rows, reader.schema().size(), true};
  }

  unsigned int hw_concurrency_;
};

// =============================================================================
// 1. Thread Count Edge Cases
// =============================================================================

TEST_F(ConcurrencyTest, MoreThreadsThanRows) {
  // 8 threads parsing CSV with only 3 data rows
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = 8;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 3u);
  EXPECT_EQ(result.num_columns, 3u);
}

TEST_F(ConcurrencyTest, MoreThreadsThanBytes) {
  // 8 threads on a tiny CSV (< 20 bytes)
  std::string csv = "a,b\n1,2\n";
  ASSERT_LT(csv.size(), 20u);
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = 8;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1u);
  EXPECT_EQ(result.num_columns, 2u);
}

TEST_F(ConcurrencyTest, SingleThreadOnLargeCSV) {
  std::string csv = generate_numeric_csv(5000, 10);
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = 1;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 5000u);
  EXPECT_EQ(result.num_columns, 10u);
}

TEST_F(ConcurrencyTest, ManyThreadsOnLargeCSV) {
  std::string csv = generate_numeric_csv(5000, 10);
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = 16;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 5000u);
  EXPECT_EQ(result.num_columns, 10u);
}

TEST_F(ConcurrencyTest, DefaultThreadCountAutoDetects) {
  // num_threads=0 should auto-detect via hardware_concurrency
  std::string csv = generate_numeric_csv(1000, 5);
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = 0;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1000u);
  EXPECT_EQ(result.num_columns, 5u);
}

// =============================================================================
// 2. Deterministic Results Across Thread Counts
// =============================================================================

TEST_F(ConcurrencyTest, SameRowCountWithDifferentThreadCounts) {
  std::string csv = generate_numeric_csv(2000, 8);
  test_util::TempCsvFile tmp(csv);

  size_t expected_rows = 0;
  for (size_t threads : {1u, 2u, 4u, 8u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto result = parse_file(tmp.path(), opts);

    ASSERT_TRUE(result.ok) << "Failed with " << threads << " threads";
    if (threads == 1) {
      expected_rows = result.total_rows;
    }
    EXPECT_EQ(result.total_rows, expected_rows)
        << "Row count differs with " << threads << " threads (expected " << expected_rows << ")";
  }
}

TEST_F(ConcurrencyTest, SameColumnCountAcrossThreadCounts) {
  std::string csv = generate_numeric_csv(2000, 12);
  test_util::TempCsvFile tmp(csv);

  for (size_t threads : {1u, 2u, 4u, 8u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto result = parse_file(tmp.path(), opts);

    ASSERT_TRUE(result.ok) << "Failed with " << threads << " threads";
    EXPECT_EQ(result.num_columns, 12u) << "Column count wrong with " << threads << " threads";
  }
}

TEST_F(ConcurrencyTest, QuotedFieldsSameResultsSingleVsMulti) {
  std::ostringstream oss;
  oss << "A,B,C\n";
  for (int i = 0; i < 3000; ++i) {
    oss << i << ",\"quoted value " << i << "\",end\n";
  }
  std::string csv = oss.str();
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts1;
  opts1.num_threads = 1;
  auto r1 = parse_file(tmp.path(), opts1);

  libvroom::CsvOptions opts4;
  opts4.num_threads = 4;
  auto r4 = parse_file(tmp.path(), opts4);

  ASSERT_TRUE(r1.ok);
  ASSERT_TRUE(r4.ok);
  EXPECT_EQ(r1.total_rows, r4.total_rows);
  EXPECT_EQ(r1.num_columns, r4.num_columns);
}

// =============================================================================
// 3. Concurrent CsvReader Instances
// =============================================================================

TEST_F(ConcurrencyTest, MultipleReadersSameFileSimultaneously) {
  std::string csv = generate_numeric_csv(1000, 5);
  test_util::TempCsvFile tmp(csv);

  const int num_readers = 10;
  std::vector<std::future<bool>> futures;
  std::atomic<int> success_count{0};

  for (int i = 0; i < num_readers; ++i) {
    futures.push_back(std::async(std::launch::async, [&tmp, &success_count]() {
      libvroom::CsvOptions opts;
      opts.num_threads = 2;
      libvroom::CsvReader reader(opts);
      auto open_result = reader.open(tmp.path());
      if (!open_result.ok)
        return false;
      auto read_result = reader.read_all();
      if (!read_result.ok)
        return false;
      if (read_result.value.total_rows == 1000) {
        success_count.fetch_add(1);
        return true;
      }
      return false;
    }));
  }

  for (auto& f : futures) {
    EXPECT_TRUE(f.get());
  }
  EXPECT_EQ(success_count.load(), num_readers);
}

TEST_F(ConcurrencyTest, MultipleReadersDifferentFilesSimultaneously) {
  // Create different CSV files
  std::vector<std::unique_ptr<test_util::TempCsvFile>> files;
  std::vector<size_t> expected_rows;
  const int num_files = 8;

  for (int i = 0; i < num_files; ++i) {
    size_t rows = 500 + static_cast<size_t>(i) * 100;
    size_t cols = 3 + static_cast<size_t>(i % 4);
    files.push_back(std::make_unique<test_util::TempCsvFile>(generate_numeric_csv(rows, cols)));
    expected_rows.push_back(rows);
  }

  std::vector<std::future<size_t>> futures;
  for (int i = 0; i < num_files; ++i) {
    futures.push_back(std::async(std::launch::async, [&files, i]() -> size_t {
      libvroom::CsvOptions opts;
      opts.num_threads = 2;
      libvroom::CsvReader reader(opts);
      auto open_result = reader.open(files[i]->path());
      if (!open_result.ok)
        return 0;
      auto read_result = reader.read_all();
      if (!read_result.ok)
        return 0;
      return read_result.value.total_rows;
    }));
  }

  for (int i = 0; i < num_files; ++i) {
    size_t rows = futures[i].get();
    EXPECT_EQ(rows, expected_rows[i]) << "File " << i << " had wrong row count";
  }
}

TEST_F(ConcurrencyTest, MultipleReadersWithDifferentOptionsSimultaneously) {
  // Same data, different separator configs parsed concurrently
  std::string csv_comma = "a,b,c\n1,2,3\n4,5,6\n";
  std::string csv_semi = "a;b;c\n1;2;3\n4;5;6\n";
  std::string csv_tab = "a\tb\tc\n1\t2\t3\n4\t5\t6\n";

  test_util::TempCsvFile tmp_comma(csv_comma);
  test_util::TempCsvFile tmp_semi(csv_semi);
  test_util::TempCsvFile tmp_tab(csv_tab);

  auto parse_with_sep = [](const std::string& path, char sep) -> size_t {
    libvroom::CsvOptions opts;
    opts.separator = sep;
    opts.num_threads = 2;
    libvroom::CsvReader reader(opts);
    auto open_result = reader.open(path);
    if (!open_result.ok)
      return 0;
    auto read_result = reader.read_all();
    if (!read_result.ok)
      return 0;
    return read_result.value.total_rows;
  };

  auto f1 = std::async(std::launch::async, parse_with_sep, tmp_comma.path(), ',');
  auto f2 = std::async(std::launch::async, parse_with_sep, tmp_semi.path(), ';');
  auto f3 = std::async(std::launch::async, parse_with_sep, tmp_tab.path(), '\t');

  EXPECT_EQ(f1.get(), 2u);
  EXPECT_EQ(f2.get(), 2u);
  EXPECT_EQ(f3.get(), 2u);
}

// =============================================================================
// 4. Chunk Boundary Edge Cases
// =============================================================================

TEST_F(ConcurrencyTest, FileSmallerThanChunkSize) {
  // A tiny CSV with multiple threads -- forces at most one chunk
  std::string csv = "a,b,c\n1,2,3\n";
  ASSERT_LT(csv.size(), 64u);
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = 4;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1u);
}

TEST_F(ConcurrencyTest, SingleVeryLongRowMultiThreaded) {
  // CSV with header and one very long data row (1000 columns)
  std::ostringstream oss;
  for (int c = 0; c < 1000; ++c) {
    if (c > 0)
      oss << ',';
    oss << "c" << c;
  }
  oss << '\n';
  for (int c = 0; c < 1000; ++c) {
    if (c > 0)
      oss << ',';
    oss << "val" << c;
  }
  oss << '\n';

  test_util::TempCsvFile tmp(oss.str());

  libvroom::CsvOptions opts;
  opts.num_threads = 8;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1u);
  EXPECT_EQ(result.num_columns, 1000u);
}

TEST_F(ConcurrencyTest, RepeatedQuotedFieldsAcrossChunks) {
  // Many quoted fields -- chunk boundaries may fall inside quotes
  std::ostringstream oss;
  oss << "name,description\n";
  for (int i = 0; i < 500; ++i) {
    oss << "\"item" << i << "\",\"This is a description with, comma\"\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  for (size_t threads : {1u, 2u, 4u, 8u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto result = parse_file(tmp.path(), opts);
    ASSERT_TRUE(result.ok) << "Failed with " << threads << " threads";
    EXPECT_EQ(result.total_rows, 500u) << "Wrong row count with " << threads << " threads";
    EXPECT_EQ(result.num_columns, 2u);
  }
}

TEST_F(ConcurrencyTest, LargeFileManyShortRows) {
  // Many short rows -- many row boundaries per chunk
  std::ostringstream oss;
  oss << "a,b\n";
  for (int i = 0; i < 10000; ++i) {
    oss << i << "," << (i + 1) << "\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  libvroom::CsvOptions opts;
  opts.num_threads = 4;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 10000u);
}

// =============================================================================
// 5. Error Handling with Threads
// =============================================================================

TEST_F(ConcurrencyTest, InconsistentColumnsPermissiveMode) {
  // CSV where some rows have wrong field count
  std::ostringstream oss;
  oss << "a,b,c\n";
  for (int i = 0; i < 500; ++i) {
    oss << "1,2,3\n";
  }
  oss << "x,y\n"; // Missing field
  for (int i = 0; i < 500; ++i) {
    oss << "4,5,6\n";
  }
  oss << "7,8,9,10\n"; // Extra field
  for (int i = 0; i < 500; ++i) {
    oss << "a,b,c\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  libvroom::CsvOptions opts;
  opts.num_threads = 4;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp.path());
  ASSERT_TRUE(open_result.ok);
  auto read_result = reader.read_all();
  // In permissive mode the parse may succeed or report errors but should not crash
  EXPECT_TRUE(reader.has_errors()) << "Should detect field count inconsistencies";
}

TEST_F(ConcurrencyTest, MultiThreadedErrorsSorted) {
  // Errors from multi-threaded parsing should be sorted by byte offset
  std::ostringstream oss;
  oss << "a,b,c\n";
  for (int i = 0; i < 400; ++i) {
    oss << "1,2,3\n";
  }
  oss << "error_row_1\n"; // First error
  for (int i = 0; i < 400; ++i) {
    oss << "4,5,6\n";
  }
  oss << "error_row_2,extra\n"; // Second error
  for (int i = 0; i < 400; ++i) {
    oss << "7,8,9\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  libvroom::CsvOptions opts;
  opts.num_threads = 4;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp.path());
  ASSERT_TRUE(open_result.ok);
  reader.read_all();

  const auto& errors = reader.errors();
  // Verify errors are sorted by byte offset (if multiple errors detected)
  for (size_t i = 1; i < errors.size(); ++i) {
    EXPECT_LE(errors[i - 1].byte_offset, errors[i].byte_offset)
        << "Errors should be sorted by byte offset";
  }
}

TEST_F(ConcurrencyTest, SingleVsMultiThreadErrorCountConsistency) {
  // Same malformed CSV should produce the same error count regardless of threads
  std::ostringstream oss;
  oss << "a,b,c\n";
  oss << "1,2,3\n";
  oss << "bad\n"; // Missing fields
  oss << "4,5,6\n";
  oss << "7,8\n"; // Missing field
  oss << "9,10,11\n";

  std::string csv = oss.str();
  test_util::TempCsvFile tmp(csv);

  auto count_errors = [&](size_t threads) -> size_t {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    libvroom::CsvReader reader(opts);
    auto open_result = reader.open(tmp.path());
    if (!open_result.ok)
      return 0;
    reader.read_all();
    return reader.errors().size();
  };

  size_t errors_1 = count_errors(1);
  size_t errors_2 = count_errors(2);

  EXPECT_EQ(errors_1, errors_2) << "Single and multi-threaded should detect same number of errors";
}

// =============================================================================
// 6. Stress Tests
// =============================================================================

TEST_F(ConcurrencyTest, RapidSequentialParsing) {
  // Parse 100 different CSVs in rapid succession to detect leaks/use-after-free
  for (int i = 0; i < 100; ++i) {
    std::string csv = generate_numeric_csv(10 + static_cast<size_t>(i), 3);
    test_util::TempCsvFile tmp(csv);

    libvroom::CsvOptions opts;
    opts.num_threads = 2;
    auto result = parse_file(tmp.path(), opts);
    ASSERT_TRUE(result.ok) << "Failed at iteration " << i;
    EXPECT_EQ(result.total_rows, 10u + static_cast<size_t>(i));
  }
}

TEST_F(ConcurrencyTest, LargeFileStress) {
  // 1000 rows x 20 columns with hardware_concurrency threads
  std::string csv = generate_csv(1000, 20, true);
  test_util::TempCsvFile tmp(csv);

  libvroom::CsvOptions opts;
  opts.num_threads = hw_concurrency_;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 1000u);
  EXPECT_EQ(result.num_columns, 20u);
}

TEST_F(ConcurrencyTest, ScalingThreadCounts) {
  // Parse the same data with 1, 2, 4, 8 threads and verify all succeed
  std::string csv = generate_numeric_csv(3000, 8);
  test_util::TempCsvFile tmp(csv);

  for (size_t threads : {1u, 2u, 4u, 8u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto result = parse_file(tmp.path(), opts);
    ASSERT_TRUE(result.ok) << "Failed with " << threads << " threads";
    EXPECT_EQ(result.total_rows, 3000u) << "Wrong row count with " << threads << " threads";
  }
}

TEST_F(ConcurrencyTest, ManyConcurrentReaders) {
  // 20 readers parsing from separate threads
  std::string csv = generate_numeric_csv(500, 5);
  test_util::TempCsvFile tmp(csv);

  const int num_readers = 20;
  std::vector<std::future<bool>> futures;
  std::atomic<int> success_count{0};

  for (int i = 0; i < num_readers; ++i) {
    futures.push_back(std::async(std::launch::async, [&tmp, &success_count]() {
      libvroom::CsvOptions opts;
      opts.num_threads = 2;
      libvroom::CsvReader reader(opts);
      auto open_result = reader.open(tmp.path());
      if (!open_result.ok)
        return false;
      auto read_result = reader.read_all();
      if (!read_result.ok)
        return false;
      if (read_result.value.total_rows == 500) {
        success_count.fetch_add(1);
        return true;
      }
      return false;
    }));
  }

  for (auto& f : futures) {
    EXPECT_TRUE(f.get());
  }
  EXPECT_EQ(success_count.load(), num_readers);
}

// =============================================================================
// 7. Mixed Data Patterns
// =============================================================================

TEST_F(ConcurrencyTest, CRLFLineEndingsMultiThreaded) {
  // CSV with CRLF line endings
  std::ostringstream oss;
  oss << "a,b,c\r\n";
  for (int i = 0; i < 2000; ++i) {
    oss << i << "," << (i + 1) << "," << (i + 2) << "\r\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  libvroom::CsvOptions opts;
  opts.num_threads = 4;
  auto result = parse_file(tmp.path(), opts);

  ASSERT_TRUE(result.ok);
  EXPECT_EQ(result.total_rows, 2000u);
  EXPECT_EQ(result.num_columns, 3u);
}

TEST_F(ConcurrencyTest, AllQuotedFieldsMultiThreaded) {
  // Every field is quoted
  std::ostringstream oss;
  oss << "\"A\",\"B\",\"C\"\n";
  for (int i = 0; i < 2000; ++i) {
    oss << "\"" << i << "\",\"" << (i * 2) << "\",\"" << (i * 3) << "\"\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  // Verify same results for single and multi-threaded
  libvroom::CsvOptions opts1;
  opts1.num_threads = 1;
  auto r1 = parse_file(tmp.path(), opts1);

  libvroom::CsvOptions opts4;
  opts4.num_threads = 4;
  auto r4 = parse_file(tmp.path(), opts4);

  ASSERT_TRUE(r1.ok);
  ASSERT_TRUE(r4.ok);
  EXPECT_EQ(r1.total_rows, r4.total_rows);
  EXPECT_EQ(r1.total_rows, 2000u);
}

TEST_F(ConcurrencyTest, MixedQuotedUnquotedMultiThreaded) {
  // Alternating quoted and unquoted fields with different lengths
  std::ostringstream oss;
  oss << "A,B,C,D\n";
  for (int i = 0; i < 2000; ++i) {
    oss << "plain" << i << "," << "\"quoted " << i << "\"," << (i * 10) << ","
        << "\"has, comma\"\n";
  }

  test_util::TempCsvFile tmp(oss.str());

  for (size_t threads : {1u, 2u, 4u, 8u}) {
    libvroom::CsvOptions opts;
    opts.num_threads = threads;
    auto result = parse_file(tmp.path(), opts);
    ASSERT_TRUE(result.ok) << "Failed with " << threads << " threads";
    EXPECT_EQ(result.total_rows, 2000u) << "Wrong row count with " << threads << " threads";
    EXPECT_EQ(result.num_columns, 4u) << "Wrong column count with " << threads << " threads";
  }
}

} // anonymous namespace
