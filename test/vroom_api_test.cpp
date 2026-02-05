/**
 * @file vroom_api_test.cpp
 * @brief Tests for the new libvroom2 API
 *
 * Tests the core functionality:
 * - CsvReader: opening files, reading schema, reading data
 * - ParquetWriter: writing Parquet files
 * - convert_csv_to_parquet: end-to-end conversion
 */

#include "libvroom.h"

#include <atomic>
#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>

// Counter to ensure unique file names across all tests
static std::atomic<uint64_t> g_temp_file_counter{0};

// Helper to create a temporary CSV file
class TempCsvFile {
public:
  explicit TempCsvFile(const std::string& content) {
    uint64_t id = g_temp_file_counter.fetch_add(1);
    path_ = "/tmp/vroom_test_" + std::to_string(getpid()) + "_" + std::to_string(id) + ".csv";
    std::ofstream f(path_);
    f << content;
    f.close();
  }

  ~TempCsvFile() { std::remove(path_.c_str()); }

  const std::string& path() const { return path_; }

private:
  std::string path_;
};

// Helper to create a temporary output path
class TempOutputFile {
public:
  TempOutputFile() {
    uint64_t id = g_temp_file_counter.fetch_add(1);
    path_ = "/tmp/vroom_test_" + std::to_string(getpid()) + "_" + std::to_string(id) + ".parquet";
  }

  ~TempOutputFile() { std::remove(path_.c_str()); }

  const std::string& path() const { return path_; }

private:
  std::string path_;
};

// =============================================================================
// CsvReader Tests
// =============================================================================

TEST(CsvReaderTest, OpenValidFile) {
  TempCsvFile csv("a,b,c\n1,2,3\n4,5,6\n");

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);

  auto result = reader.open(csv.path());
  ASSERT_TRUE(result.ok) << result.error;
}

TEST(CsvReaderTest, OpenNonExistentFile) {
  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);

  auto result = reader.open("/nonexistent/path/to/file.csv");
  ASSERT_FALSE(result.ok);
  EXPECT_FALSE(result.error.empty());
}

TEST(CsvReaderTest, ReadSchema) {
  TempCsvFile csv("name,age,city\nAlice,30,NYC\nBob,25,LA\n");

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 3);
  EXPECT_EQ(schema[0].name, "name");
  EXPECT_EQ(schema[1].name, "age");
  EXPECT_EQ(schema[2].name, "city");
}

TEST(CsvReaderTest, ReadAllData) {
  TempCsvFile csv("x,y\n1,2\n3,4\n5,6\n");

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  EXPECT_EQ(read_result.value.total_rows, 3);
  EXPECT_FALSE(read_result.value.chunks.empty());
}

TEST(CsvReaderTest, TypeInference) {
  TempCsvFile csv("int_col,float_col,str_col\n1,1.5,hello\n2,2.5,world\n");

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 3);

  // Check that types were inferred (exact types may vary based on implementation)
  // At minimum, the schema should have valid types
  for (const auto& col : schema) {
    EXPECT_NE(col.type, libvroom::DataType::UNKNOWN);
  }
}

TEST(CsvReaderTest, CustomDelimiter) {
  TempCsvFile csv("a;b;c\n1;2;3\n");

  libvroom::CsvOptions opts;
  opts.separator = ';';
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 3);
  EXPECT_EQ(schema[0].name, "a");
  EXPECT_EQ(schema[1].name, "b");
  EXPECT_EQ(schema[2].name, "c");
}

TEST(CsvReaderTest, QuotedFields) {
  TempCsvFile csv("name,description\n\"John\",\"Hello, World\"\n\"Jane\",\"Line1\nLine2\"\n");

  libvroom::CsvOptions opts;
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  EXPECT_EQ(read_result.value.total_rows, 2);
}

TEST(CsvReaderTest, NoHeader) {
  TempCsvFile csv("1,2,3\n4,5,6\n");

  libvroom::CsvOptions opts;
  opts.has_header = false;
  libvroom::CsvReader reader(opts);

  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  // With no header, both rows should be data
  EXPECT_EQ(read_result.value.total_rows, 2);
}

// =============================================================================
// convert_csv_to_parquet Tests
// =============================================================================

TEST(ConversionTest, BasicConversion) {
  TempCsvFile csv("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();
  opts.parquet.compression = libvroom::Compression::NONE;

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;

  EXPECT_EQ(result.rows, 3);
  EXPECT_EQ(result.cols, 3);

  // Verify file was created
  std::ifstream f(parquet.path());
  EXPECT_TRUE(f.good());
}

#ifdef VROOM_HAVE_ZSTD
TEST(ConversionTest, WithZstdCompression) {
  TempCsvFile csv("x,y\n1,2\n3,4\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();
  opts.parquet.compression = libvroom::Compression::ZSTD;

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;

  EXPECT_EQ(result.rows, 2);
}
#endif

TEST(ConversionTest, EmptyFile) {
  TempCsvFile csv("a,b,c\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;

  EXPECT_EQ(result.rows, 0);
  EXPECT_EQ(result.cols, 3);
}

TEST(ConversionTest, LargerFile) {
  // Create a CSV with 1000 rows
  std::string content = "id,value,name\n";
  for (int i = 0; i < 1000; ++i) {
    content +=
        std::to_string(i) + "," + std::to_string(i * 1.5) + ",name" + std::to_string(i) + "\n";
  }

  TempCsvFile csv(content);
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;

  EXPECT_EQ(result.rows, 1000);
  EXPECT_EQ(result.cols, 3);
}

TEST(ConversionTest, InvalidInputPath) {
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = "/nonexistent/file.csv";
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(result.error.empty());
}

// =============================================================================
// Type-specific Tests
// =============================================================================

TEST(TypeTest, IntegerColumn) {
  TempCsvFile csv("numbers\n1\n2\n3\n100\n-50\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.rows, 5);
}

TEST(TypeTest, FloatColumn) {
  TempCsvFile csv("values\n1.5\n2.7\n3.14159\n-0.5\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.rows, 4);
}

TEST(TypeTest, StringColumn) {
  TempCsvFile csv("names\nhello\nworld\n\"with spaces\"\n\"with,comma\"\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.rows, 4);
}

TEST(TypeTest, MixedTypes) {
  TempCsvFile csv("int_col,float_col,str_col,bool_col\n1,1.5,hello,true\n2,2.5,world,false\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.rows, 2);
  EXPECT_EQ(result.cols, 4);
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST(EdgeCaseTest, SingleColumn) {
  TempCsvFile csv("value\n1\n2\n3\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.cols, 1);
}

TEST(EdgeCaseTest, SingleRow) {
  TempCsvFile csv("a,b,c\n1,2,3\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.rows, 1);
}

TEST(EdgeCaseTest, ManyColumns) {
  // Create CSV with 100 columns
  std::string header;
  std::string row;
  for (int i = 0; i < 100; ++i) {
    if (i > 0) {
      header += ",";
      row += ",";
    }
    header += "col" + std::to_string(i);
    row += std::to_string(i);
  }

  TempCsvFile csv(header + "\n" + row + "\n");
  TempOutputFile parquet;

  libvroom::VroomOptions opts;
  opts.input_path = csv.path();
  opts.output_path = parquet.path();

  auto result = libvroom::convert_csv_to_parquet(opts);
  ASSERT_TRUE(result.ok()) << result.error;
  EXPECT_EQ(result.cols, 100);
}
