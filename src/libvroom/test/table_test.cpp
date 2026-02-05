/**
 * @file table_test.cpp
 * @brief Tests for multi-batch Arrow stream Table export (Issue #632).
 *
 * Verifies that Table stores parsed chunks separately and exports them
 * as individual RecordBatches via ArrowArrayStream, eliminating the
 * O(n) merge overhead.
 */

#include "libvroom.h"
#include "libvroom/table.h"

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>

// Helper to create temp CSV files
class TempFile {
public:
  explicit TempFile(const std::string& content, const std::string& ext = ".csv") {
    static int counter = 0;
    path_ = "/tmp/table_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) + ext;
    std::ofstream f(path_);
    f << content;
  }
  ~TempFile() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }

private:
  std::string path_;
};

// Helper: parse a CSV file and return a Table
std::shared_ptr<libvroom::Table> parse_to_table(const std::string& path, size_t num_threads = 0) {
  libvroom::CsvOptions opts;
  if (num_threads > 0)
    opts.num_threads = num_threads;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(path);
  if (!open_result.ok)
    return nullptr;
  auto read_result = reader.read_all();
  if (!read_result.ok)
    return nullptr;
  return libvroom::Table::from_parsed_chunks(reader.schema(), std::move(read_result.value));
}

// =============================================================================
// Table Construction Tests
// =============================================================================

TEST(TableTest, SingleChunkConstruction) {
  TempFile csv("a,b\n1,2\n3,4\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  EXPECT_EQ(table->num_rows(), 2);
  EXPECT_EQ(table->num_columns(), 2);
  EXPECT_GE(table->num_chunks(), 1);
}

TEST(TableTest, MultiChunkConstruction) {
  // Create a large-enough CSV to trigger multiple chunks with parallel parsing
  std::string content = "x,y,z\n";
  for (int i = 0; i < 10000; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 2) + "," + std::to_string(i * 3) + "\n";
  }
  TempFile csv(content);
  auto table = parse_to_table(csv.path(), /*num_threads=*/4);
  ASSERT_NE(table, nullptr);

  EXPECT_EQ(table->num_rows(), 10000);
  EXPECT_EQ(table->num_columns(), 3);
}

TEST(TableTest, EmptyTable) {
  TempFile csv("a,b,c\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  EXPECT_EQ(table->num_rows(), 0);
  EXPECT_EQ(table->num_columns(), 3);
  EXPECT_EQ(table->num_chunks(), 0);
}

TEST(TableTest, SchemaPreserved) {
  TempFile csv("name,age,score\nAlice,30,95.5\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  const auto& schema = table->schema();
  ASSERT_EQ(schema.size(), 3);
  EXPECT_EQ(schema[0].name, "name");
  EXPECT_EQ(schema[1].name, "age");
  EXPECT_EQ(schema[2].name, "score");
}

// =============================================================================
// Arrow Stream Tests
// =============================================================================

TEST(TableStreamTest, StreamSchemaCorrect) {
  TempFile csv("a,b\n1,hello\n2,world\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Get schema
  libvroom::ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);

  // Verify struct schema
  EXPECT_STREQ(schema.format, "+s");
  EXPECT_EQ(schema.n_children, 2);
  EXPECT_STREQ(schema.children[0]->name, "a");
  EXPECT_STREQ(schema.children[1]->name, "b");

  // Clean up
  schema.release(&schema);
  stream.release(&stream);
}

TEST(TableStreamTest, SingleChunkStream) {
  TempFile csv("x,y\n1,2\n3,4\n5,6\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Count batches and total rows
  size_t total_rows = 0;
  size_t num_batches = 0;

  while (true) {
    libvroom::ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break; // End of stream

    EXPECT_GT(batch.length, 0);
    EXPECT_EQ(batch.n_children, 2);
    total_rows += static_cast<size_t>(batch.length);
    num_batches++;

    batch.release(&batch);
  }

  EXPECT_EQ(total_rows, 3);
  EXPECT_GE(num_batches, 1);

  stream.release(&stream);
}

TEST(TableStreamTest, MultiBatchStream) {
  // Create a large CSV that will produce multiple chunks
  std::string content = "a,b,c\n";
  for (int i = 0; i < 10000; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 2) + ",str" + std::to_string(i) + "\n";
  }
  TempFile csv(content);
  auto table = parse_to_table(csv.path(), /*num_threads=*/4);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Consume all batches
  size_t total_rows = 0;
  size_t num_batches = 0;

  while (true) {
    libvroom::ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break;

    EXPECT_GT(batch.length, 0);
    EXPECT_EQ(batch.n_children, 3);
    total_rows += static_cast<size_t>(batch.length);
    num_batches++;

    batch.release(&batch);
  }

  EXPECT_EQ(total_rows, 10000);
  // Verify we got the same number of batches as chunks
  EXPECT_EQ(num_batches, table->num_chunks());

  stream.release(&stream);
}

TEST(TableStreamTest, EmptyStream) {
  TempFile csv("a,b\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Schema should still be available
  libvroom::ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  EXPECT_EQ(schema.n_children, 2);
  schema.release(&schema);

  // First get_next should signal end of stream
  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  EXPECT_EQ(batch.release, nullptr); // End of stream

  stream.release(&stream);
}

TEST(TableStreamTest, ChunkRowCountsMatchTotal) {
  std::string content = "id,val\n";
  for (int i = 0; i < 5000; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 10) + "\n";
  }
  TempFile csv(content);
  auto table = parse_to_table(csv.path(), /*num_threads=*/4);
  ASSERT_NE(table, nullptr);

  // Verify chunk_rows accessor matches total
  size_t sum_from_accessor = 0;
  for (size_t i = 0; i < table->num_chunks(); ++i) {
    sum_from_accessor += table->chunk_rows(i);
  }
  EXPECT_EQ(sum_from_accessor, table->num_rows());

  // Verify stream output matches
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  size_t sum_from_stream = 0;
  while (true) {
    libvroom::ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break;
    sum_from_stream += static_cast<size_t>(batch.length);
    batch.release(&batch);
  }
  EXPECT_EQ(sum_from_stream, table->num_rows());

  stream.release(&stream);
}

TEST(TableStreamTest, StreamCanBeConsumedMultipleTimes) {
  TempFile csv("a\n1\n2\n");
  auto table = parse_to_table(csv.path(), /*num_threads=*/1);
  ASSERT_NE(table, nullptr);

  // First stream consumption
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  batch.release(&batch);

  // Exhaust stream
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  EXPECT_EQ(batch.release, nullptr);

  stream.release(&stream);

  // Can get a new stream from same table
  libvroom::ArrowArrayStream stream2;
  table->export_to_stream(&stream2);

  // Should be able to consume again
  ASSERT_EQ(stream2.get_next(&stream2, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  batch.release(&batch);

  stream2.release(&stream2);
}

// =============================================================================
// read_csv_to_table() convenience function tests
// =============================================================================

TEST(ReadCsvToTable, HappyPath) {
  TempFile csv("name,age,score\nAlice,30,95.5\nBob,25,87.3\nCharlie,35,91.0\n");

  auto table = libvroom::read_csv_to_table(csv.path());

  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 3);
  EXPECT_EQ(table->num_columns(), 3);

  auto names = table->column_names();
  EXPECT_EQ(names[0], "name");
  EXPECT_EQ(names[1], "age");
  EXPECT_EQ(names[2], "score");

  // Verify data is accessible via Arrow stream
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  EXPECT_EQ(batch.length, 3);
  batch.release(&batch);

  stream.release(&stream);
}

TEST(ReadCsvToTable, FileNotFound) {
  EXPECT_THROW(libvroom::read_csv_to_table("/nonexistent/path/file.csv"), std::runtime_error);
}

TEST(ReadCsvToTable, DefaultOptions) {
  TempFile csv("x,y\n1,2\n3,4\n");

  // Default CsvOptions{} should auto-detect delimiter and types
  auto table = libvroom::read_csv_to_table(csv.path(), libvroom::CsvOptions{});

  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 2);
  EXPECT_EQ(table->num_columns(), 2);
}
