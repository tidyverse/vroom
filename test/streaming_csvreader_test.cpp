#include "libvroom.h"

#include "test_util.h"

#include <cstring>
#include <gtest/gtest.h>
#include <sstream>
#include <string>

class StreamingCsvReaderTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }
};

TEST_F(StreamingCsvReaderTest, BasicStreaming) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(f.path());
  ASSERT_TRUE(open_result.ok);

  auto start_result = reader.start_streaming();
  ASSERT_TRUE(start_result.ok);

  size_t total_rows = 0;
  size_t chunk_count = 0;
  while (auto chunk = reader.next_chunk()) {
    chunk_count++;
    if (!chunk->empty()) {
      total_rows += (*chunk)[0]->size();
    }
  }
  EXPECT_EQ(total_rows, 3u);
  EXPECT_GE(chunk_count, 1u);
}

TEST_F(StreamingCsvReaderTest, StreamingMatchesReadAll) {
  std::string csv = "x,y\n1,hello\n2,world\n3,foo\n4,bar\n5,baz\n";
  test_util::TempCsvFile f(csv);

  // read_all path
  libvroom::CsvReader reader1(libvroom::CsvOptions{});
  reader1.open(f.path());
  auto all = reader1.read_all();
  ASSERT_TRUE(all.ok);

  // streaming path
  libvroom::CsvReader reader2(libvroom::CsvOptions{});
  reader2.open(f.path());
  auto start = reader2.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t streaming_rows = 0;
  while (auto chunk = reader2.next_chunk()) {
    if (!chunk->empty()) {
      streaming_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(streaming_rows, all.value.total_rows);
}

TEST_F(StreamingCsvReaderTest, StartStreamingBeforeOpen) {
  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto result = reader.start_streaming();
  EXPECT_FALSE(result.ok);
}

TEST_F(StreamingCsvReaderTest, DoubleStartStreaming) {
  std::string csv = "a\n1\n2\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());
  auto start1 = reader.start_streaming();
  ASSERT_TRUE(start1.ok);

  auto start2 = reader.start_streaming();
  EXPECT_FALSE(start2.ok);
}

TEST_F(StreamingCsvReaderTest, NextChunkWithoutStartStreaming) {
  std::string csv = "a\n1\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());

  auto chunk = reader.next_chunk();
  EXPECT_FALSE(chunk.has_value());
}

TEST_F(StreamingCsvReaderTest, StreamingFromBuffer) {
  std::string csv = "x,y\n1,2\n3,4\n";
  auto buf = libvroom::AlignedBuffer::allocate(csv.size());
  std::memcpy(buf.data(), csv.data(), csv.size());

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open_from_buffer(std::move(buf));
  auto start = reader.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t total = 0;
  while (auto chunk = reader.next_chunk()) {
    if (!chunk->empty())
      total += (*chunk)[0]->size();
  }
  EXPECT_EQ(total, 2u);
}

// ---------------------------------------------------------------------------
// Large file tests that exercise multi-threaded / multi-chunk code paths
// ---------------------------------------------------------------------------

// Helper: generate a large CSV string with N rows and 3 columns (id, name, value)
static std::string generate_large_csv(size_t n_rows) {
  std::ostringstream oss;
  oss << "id,name,value\n";
  for (size_t i = 0; i < n_rows; ++i) {
    oss << i << ",name_" << i << "," << (i * 1.5) << "\n";
  }
  return oss.str();
}

TEST_F(StreamingCsvReaderTest, LargeFileMultipleChunks) {
  constexpr size_t N = 50000;
  std::string csv = generate_large_csv(N);
  // Sanity-check that our generated file is indeed >1 MB
  ASSERT_GT(csv.size(), 1'000'000u);

  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(f.path());
  ASSERT_TRUE(open_result.ok);

  auto start_result = reader.start_streaming();
  ASSERT_TRUE(start_result.ok);

  size_t total_rows = 0;
  size_t chunk_count = 0;
  while (auto chunk = reader.next_chunk()) {
    chunk_count++;
    if (!chunk->empty()) {
      total_rows += (*chunk)[0]->size();
    }
  }
  EXPECT_EQ(total_rows, N);
  EXPECT_GE(chunk_count, 1u);
}

TEST_F(StreamingCsvReaderTest, StreamingMatchesReadAllLargeFile) {
  constexpr size_t N = 50000;
  std::string csv = generate_large_csv(N);
  test_util::TempCsvFile f(csv);

  // read_all path
  libvroom::CsvReader reader1(libvroom::CsvOptions{});
  reader1.open(f.path());
  auto all = reader1.read_all();
  ASSERT_TRUE(all.ok);

  // streaming path
  libvroom::CsvReader reader2(libvroom::CsvOptions{});
  reader2.open(f.path());
  auto start = reader2.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t streaming_rows = 0;
  while (auto chunk = reader2.next_chunk()) {
    if (!chunk->empty()) {
      streaming_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(streaming_rows, all.value.total_rows);
  EXPECT_EQ(streaming_rows, N);
}

// Helper: generate a large CSV with quoted fields (including escaped quotes)
static std::string generate_quoted_csv(size_t n_rows) {
  std::ostringstream oss;
  oss << "id,description,amount\n";
  for (size_t i = 0; i < n_rows; ++i) {
    // Every 5th row gets a field with an escaped quote inside
    if (i % 5 == 0) {
      oss << i << ",\"He said \"\"hello\"\" today\"," << (i * 2.0) << "\n";
    } else {
      oss << i << ",\"simple quoted field\"," << (i * 2.0) << "\n";
    }
  }
  return oss.str();
}

TEST_F(StreamingCsvReaderTest, StreamingWithQuotedFields) {
  constexpr size_t N = 50000;
  std::string csv = generate_quoted_csv(N);
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(f.path());
  ASSERT_TRUE(open_result.ok);

  auto start_result = reader.start_streaming();
  ASSERT_TRUE(start_result.ok);

  size_t total_rows = 0;
  size_t chunk_count = 0;
  while (auto chunk = reader.next_chunk()) {
    chunk_count++;
    if (!chunk->empty()) {
      // All chunks should have exactly 3 columns
      EXPECT_EQ(chunk->size(), 3u);
      total_rows += (*chunk)[0]->size();
    }
  }
  EXPECT_EQ(total_rows, N);
  EXPECT_GE(chunk_count, 1u);
}

TEST_F(StreamingCsvReaderTest, StreamingValuesMatchReadAll) {
  // Use a small file so it fits in a single chunk, making value comparison straightforward
  std::string csv = "x,y,z\n1,hello,3.14\n2,world,2.72\n3,foo,1.41\n4,bar,0.57\n5,baz,9.81\n";
  test_util::TempCsvFile f(csv);

  // read_all path
  libvroom::CsvReader reader1(libvroom::CsvOptions{});
  reader1.open(f.path());
  auto all = reader1.read_all();
  ASSERT_TRUE(all.ok);

  // streaming path
  libvroom::CsvReader reader2(libvroom::CsvOptions{});
  reader2.open(f.path());
  auto start = reader2.start_streaming();
  ASSERT_TRUE(start.ok);

  // Collect all streaming chunks
  std::vector<std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>> streaming_chunks;
  while (auto chunk = reader2.next_chunk()) {
    streaming_chunks.push_back(std::move(*chunk));
  }
  ASSERT_FALSE(streaming_chunks.empty());

  // Compare values from first streaming chunk against read_all
  // (small file should produce exactly one chunk in each path)
  size_t n_cols = all.value.chunks[0].size();
  ASSERT_GE(streaming_chunks[0].size(), n_cols);

  size_t rows_in_first_chunk = streaming_chunks[0][0]->size();
  for (size_t col = 0; col < n_cols; ++col) {
    for (size_t row = 0; row < rows_in_first_chunk; ++row) {
      std::string streaming_val = test_util::getValue(streaming_chunks[0][col].get(), row);
      std::string readall_val = test_util::getStringValue(all.value, col, row);
      EXPECT_EQ(streaming_val, readall_val) << "Mismatch at col=" << col << " row=" << row;
    }
  }
}

TEST_F(StreamingCsvReaderTest, StreamingMultipleTypes) {
  // CSV with multiple column types: int, float, string, bool
  std::string csv = "id,ratio,label,flag\n"
                    "1,3.14,hello,true\n"
                    "2,2.72,world,false\n"
                    "3,1.41,foo,true\n"
                    "4,0.57,bar,false\n"
                    "5,9.81,baz,true\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(f.path());
  ASSERT_TRUE(open_result.ok);

  // Verify detected schema covers expected types
  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 4u);
  // id should be inferred as INT32 (or INT64)
  EXPECT_TRUE(schema[0].type == libvroom::DataType::INT32 ||
              schema[0].type == libvroom::DataType::INT64)
      << "id column type: " << libvroom::type_name(schema[0].type);
  // ratio should be FLOAT64
  EXPECT_EQ(schema[1].type, libvroom::DataType::FLOAT64);
  // label should be STRING
  EXPECT_EQ(schema[2].type, libvroom::DataType::STRING);
  // flag should be BOOL
  EXPECT_EQ(schema[3].type, libvroom::DataType::BOOL);

  auto start_result = reader.start_streaming();
  ASSERT_TRUE(start_result.ok);

  size_t total_rows = 0;
  while (auto chunk = reader.next_chunk()) {
    if (!chunk->empty()) {
      EXPECT_EQ(chunk->size(), 4u);
      // Each column in the chunk should have the same number of rows
      size_t chunk_rows = (*chunk)[0]->size();
      for (size_t c = 1; c < chunk->size(); ++c) {
        EXPECT_EQ((*chunk)[c]->size(), chunk_rows) << "Column " << c << " row count mismatch";
      }
      total_rows += chunk_rows;
    }
  }
  EXPECT_EQ(total_rows, 5u);
}

TEST_F(StreamingCsvReaderTest, EarlyAbandonmentNoDeadlock) {
  // Verify that destroying CsvReader without consuming all chunks doesn't deadlock.
  // This exercises the Impl destructor's queue close + pool drain logic.
  std::string csv = "id,name,value\n";
  for (int i = 0; i < 50000; ++i) {
    csv += std::to_string(i) + ",name_" + std::to_string(i) + "," + std::to_string(i * 1.5) + "\n";
  }
  test_util::TempCsvFile f(csv);

  {
    libvroom::CsvReader reader(libvroom::CsvOptions{});
    reader.open(f.path());
    auto start = reader.start_streaming();
    ASSERT_TRUE(start.ok);

    // Consume only the first chunk, then let reader go out of scope
    auto chunk = reader.next_chunk();
    EXPECT_TRUE(chunk.has_value());
    // Deliberately do NOT consume remaining chunks — destructor must handle cleanup
  }
  // If we reach here without deadlocking, the test passes
  SUCCEED();
}
