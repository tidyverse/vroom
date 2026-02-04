/**
 * @file streaming_test.cpp
 * @brief Tests for the StreamingParser API (Issue #633).
 *
 * Verifies that StreamingParser correctly accepts chunked CSV input
 * and produces columnar ArrowColumnBuilder batches incrementally.
 */

#include "libvroom.h"
#include "libvroom/streaming.h"
#include "libvroom/table.h"

#include <gtest/gtest.h>
#include <sstream>
#include <string>

using namespace libvroom;

// =============================================================================
// Basic Functionality Tests
// =============================================================================

TEST(StreamingParserTest, FeedCompleteCSV) {
  StreamingOptions opts;
  opts.batch_size = 8192;
  StreamingParser parser(opts);

  std::string csv = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  auto result = parser.feed(csv.data(), csv.size());
  ASSERT_TRUE(result.ok);

  auto finish_result = parser.finish();
  ASSERT_TRUE(finish_result.ok);

  // Collect all batches
  size_t total_rows = 0;
  std::vector<StreamBatch> batches;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
    batches.push_back(std::move(*batch));
  }

  EXPECT_EQ(total_rows, 3);
  ASSERT_FALSE(batches.empty());

  // Verify column count
  EXPECT_EQ(batches[0].columns.size(), 3);
}

TEST(StreamingParserTest, ColumnNamesFromHeader) {
  StreamingParser parser;

  std::string csv = "name,age,score\nAlice,30,95.5\n";
  auto result = parser.feed(csv.data(), csv.size());
  ASSERT_TRUE(result.ok);

  ASSERT_TRUE(parser.schema_ready());
  const auto& schema = parser.schema();
  ASSERT_EQ(schema.size(), 3);
  EXPECT_EQ(schema[0].name, "name");
  EXPECT_EQ(schema[1].name, "age");
  EXPECT_EQ(schema[2].name, "score");
}

TEST(StreamingParserTest, TypedColumns) {
  StreamingParser parser;

  std::string csv = "name,age,score\nAlice,30,95.5\nBob,25,87.3\n";
  auto result = parser.feed(csv.data(), csv.size());
  ASSERT_TRUE(result.ok);
  parser.finish();

  auto batch = parser.next_batch();
  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->num_rows, 2);

  // Verify types were inferred
  const auto& schema = parser.schema();
  EXPECT_EQ(schema[0].type, DataType::STRING);
  EXPECT_EQ(schema[1].type, DataType::INT32);
  EXPECT_EQ(schema[2].type, DataType::FLOAT64);
}

TEST(StreamingParserTest, RowCountAndStatistics) {
  StreamingParser parser;

  std::string csv = "x,y\n1,2\n3,4\n5,6\n7,8\n9,10\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
    // Each batch should have exactly 2 columns
    EXPECT_EQ(batch->columns.size(), 2);
    // Each column should have the same number of rows as the batch
    for (const auto& col : batch->columns) {
      EXPECT_EQ(col->size(), batch->num_rows);
    }
  }
  EXPECT_EQ(total_rows, 5);
}

TEST(StreamingParserTest, NullHandling) {
  StreamingParser parser;

  std::string csv = "a,b\n1,NA\n,3\nNULL,null\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  auto batch = parser.next_batch();
  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->num_rows, 3);

  // Both columns should have some nulls
  EXPECT_GT(batch->columns[0]->null_count(), 0);
  EXPECT_GT(batch->columns[1]->null_count(), 0);
}

TEST(StreamingParserTest, QuotedFields) {
  StreamingParser parser;

  std::string csv = "a,b\n\"hello, world\",1\n\"with \"\"quotes\"\"\",2\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  auto batch = parser.next_batch();
  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->num_rows, 2);
}

TEST(StreamingParserTest, EmptyInput) {
  StreamingParser parser;

  parser.finish();
  auto batch = parser.next_batch();
  // Should get no batch from empty input
  EXPECT_FALSE(batch.has_value());
}

TEST(StreamingParserTest, HeaderOnly) {
  StreamingParser parser;

  std::string csv = "a,b,c\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  ASSERT_TRUE(parser.schema_ready());
  EXPECT_EQ(parser.schema().size(), 3);

  // Should get no data batch (or a batch with 0 rows)
  bool got_data = false;
  while (auto batch = parser.next_batch()) {
    if (batch->num_rows > 0)
      got_data = true;
  }
  EXPECT_FALSE(got_data);
}

// =============================================================================
// Chunk Boundary Tests
// =============================================================================

TEST(StreamingParserTest, FeedByteByByte) {
  StreamingParser parser;

  std::string csv = "a,b\n1,2\n3,4\n";
  for (size_t i = 0; i < csv.size(); ++i) {
    auto result = parser.feed(csv.data() + i, 1);
    ASSERT_TRUE(result.ok) << "Failed at byte " << i;
  }
  parser.finish();

  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
  }
  EXPECT_EQ(total_rows, 2);
}

TEST(StreamingParserTest, SplitAtEveryPosition) {
  std::string csv = "a,b\n1,2\n3,4\n";

  for (size_t split = 0; split <= csv.size(); ++split) {
    StreamingParser parser;

    if (split > 0) {
      auto r1 = parser.feed(csv.data(), split);
      ASSERT_TRUE(r1.ok) << "Failed at split " << split;
    }
    if (split < csv.size()) {
      auto r2 = parser.feed(csv.data() + split, csv.size() - split);
      ASSERT_TRUE(r2.ok) << "Failed at split " << split;
    }
    parser.finish();

    size_t total_rows = 0;
    while (auto batch = parser.next_batch()) {
      total_rows += batch->num_rows;
    }
    EXPECT_EQ(total_rows, 2) << "Wrong row count at split position " << split;
  }
}

TEST(StreamingParserTest, SplitCRLFAcrossChunks) {
  StreamingParser parser;

  // "a,b\r\n1,2\r\n" with split between \r and \n
  std::string part1 = "a,b\r";
  std::string part2 = "\n1,2\r";
  std::string part3 = "\n";

  parser.feed(part1.data(), part1.size());
  parser.feed(part2.data(), part2.size());
  parser.feed(part3.data(), part3.size());
  parser.finish();

  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
  }
  EXPECT_EQ(total_rows, 1);
}

TEST(StreamingParserTest, SplitInsideQuotedField) {
  std::string csv = "a,b\n\"hello, world\",1\n";

  // Split right in the middle of the quoted field
  for (size_t split = 4; split < csv.size(); ++split) {
    StreamingParser parser;

    parser.feed(csv.data(), split);
    parser.feed(csv.data() + split, csv.size() - split);
    parser.finish();

    size_t total_rows = 0;
    while (auto batch = parser.next_batch()) {
      total_rows += batch->num_rows;
    }
    EXPECT_EQ(total_rows, 1) << "Wrong row count at split position " << split;
  }
}

TEST(StreamingParserTest, SplitInsideDoubleQuoteEscape) {
  std::string csv = "a\n\"ab\"\"cd\"\n";

  for (size_t split = 0; split <= csv.size(); ++split) {
    StreamingParser parser;

    if (split > 0) {
      parser.feed(csv.data(), split);
    }
    if (split < csv.size()) {
      parser.feed(csv.data() + split, csv.size() - split);
    }
    parser.finish();

    size_t total_rows = 0;
    while (auto batch = parser.next_batch()) {
      total_rows += batch->num_rows;
    }
    EXPECT_EQ(total_rows, 1) << "Wrong row count at split position " << split;
  }
}

TEST(StreamingParserTest, SplitHeaderAcrossChunks) {
  StreamingParser parser;

  // Split header "name,a" + "ge\n..."
  std::string part1 = "name,a";
  std::string part2 = "ge\n30\n";

  // After first feed, schema should not be ready yet (no newline)
  // Actually it depends on implementation - header needs a complete line
  parser.feed(part1.data(), part1.size());
  parser.feed(part2.data(), part2.size());
  parser.finish();

  ASSERT_TRUE(parser.schema_ready());
  const auto& schema = parser.schema();
  ASSERT_EQ(schema.size(), 2);
  EXPECT_EQ(schema[0].name, "name");
  EXPECT_EQ(schema[1].name, "age");

  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
  }
  EXPECT_EQ(total_rows, 1);
}

TEST(StreamingParserTest, FeedWithNoCompleteRows) {
  StreamingParser parser;

  // Feed header but no data rows
  std::string header = "a,b\n";
  parser.feed(header.data(), header.size());

  // Feed partial row (no newline)
  std::string partial = "1,2";
  parser.feed(partial.data(), partial.size());

  // No batch should be ready yet (no complete data row terminated)
  // Actually with batch_size=8192 default, it would only yield on finish or batch full
  // But the partial row should be buffered

  parser.finish();

  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
  }
  EXPECT_EQ(total_rows, 1);
}

// =============================================================================
// Batch Size Control Tests
// =============================================================================

TEST(StreamingParserTest, BatchSizeOne) {
  StreamingOptions opts;
  opts.batch_size = 1;
  StreamingParser parser(opts);

  std::string csv = "a\n1\n2\n3\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  size_t batch_count = 0;
  while (auto batch = parser.next_batch()) {
    EXPECT_EQ(batch->num_rows, 1) << "Batch " << batch_count << " has wrong size";
    batch_count++;
  }
  EXPECT_EQ(batch_count, 3);
}

TEST(StreamingParserTest, BatchSizeZero) {
  StreamingOptions opts;
  opts.batch_size = 0; // All available rows per call
  StreamingParser parser(opts);

  std::string csv = "a\n1\n2\n3\n4\n5\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  auto batch = parser.next_batch();
  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->num_rows, 5);

  // No more batches
  EXPECT_FALSE(parser.next_batch().has_value());
}

TEST(StreamingParserTest, BatchSizeWithRemainder) {
  StreamingOptions opts;
  opts.batch_size = 100;
  StreamingParser parser(opts);

  // Generate 250 rows
  std::string csv = "x\n";
  for (int i = 0; i < 250; ++i) {
    csv += std::to_string(i) + "\n";
  }

  parser.feed(csv.data(), csv.size());
  parser.finish();

  std::vector<size_t> batch_sizes;
  while (auto batch = parser.next_batch()) {
    batch_sizes.push_back(batch->num_rows);
  }

  ASSERT_EQ(batch_sizes.size(), 3);
  EXPECT_EQ(batch_sizes[0], 100);
  EXPECT_EQ(batch_sizes[1], 100);
  EXPECT_EQ(batch_sizes[2], 50);
}

// =============================================================================
// Schema Handling Tests
// =============================================================================

TEST(StreamingParserTest, ExplicitSchema) {
  StreamingParser parser;

  // Set schema explicitly before feeding data
  std::vector<ColumnSchema> schema;
  schema.push_back({"name", DataType::STRING, true, 0});
  schema.push_back({"value", DataType::INT32, true, 1});
  parser.set_schema(schema);

  ASSERT_TRUE(parser.schema_ready());

  std::string csv = "name,value\nfoo,42\nbar,99\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  auto batch = parser.next_batch();
  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->num_rows, 2);
  EXPECT_EQ(batch->columns.size(), 2);

  // Types should match the explicit schema
  EXPECT_EQ(batch->columns[0]->type(), DataType::STRING);
  EXPECT_EQ(batch->columns[1]->type(), DataType::INT32);
}

TEST(StreamingParserTest, NoHeader) {
  StreamingOptions opts;
  opts.csv.has_header = false;
  StreamingParser parser(opts);

  std::string csv = "1,2,3\n4,5,6\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  ASSERT_TRUE(parser.schema_ready());
  const auto& schema = parser.schema();
  ASSERT_EQ(schema.size(), 3);
  // Without header, columns get auto-generated names
  EXPECT_EQ(schema[0].name, "V1");
  EXPECT_EQ(schema[1].name, "V2");
  EXPECT_EQ(schema[2].name, "V3");

  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
  }
  EXPECT_EQ(total_rows, 2);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

TEST(StreamingParserTest, FailFastMode) {
  StreamingOptions opts;
  opts.csv.error_mode = ErrorMode::FAIL_FAST;
  StreamingParser parser(opts);

  // Inconsistent field count should trigger an error
  std::string csv = "a,b\n1,2\n3\n4,5\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  EXPECT_TRUE(parser.has_errors());
}

TEST(StreamingParserTest, PermissiveMode) {
  StreamingOptions opts;
  opts.csv.error_mode = ErrorMode::PERMISSIVE;
  StreamingParser parser(opts);

  // Inconsistent field count in permissive mode
  std::string csv = "a,b\n1,2\n3\n4,5\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  // Errors should be collected
  EXPECT_TRUE(parser.has_errors());

  // But we should still get data
  size_t total_rows = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
  }
  EXPECT_EQ(total_rows, 3); // All rows parsed despite error
}

TEST(StreamingParserTest, ErrorCollectorAccess) {
  StreamingOptions opts;
  opts.csv.error_mode = ErrorMode::PERMISSIVE;
  StreamingParser parser(opts);

  std::string csv = "a,b\n1,2\n3\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  const auto& collector = parser.error_collector();
  EXPECT_TRUE(collector.has_errors());
  EXPECT_GE(collector.error_count(), 1);
}

// =============================================================================
// Integration Tests
// =============================================================================

TEST(StreamingParserTest, ReadCsvStream) {
  std::istringstream input("a,b\n1,2\n3,4\n5,6\n");

  auto table = read_csv_stream(input);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 3);
  EXPECT_EQ(table->num_columns(), 2);

  auto names = table->column_names();
  EXPECT_EQ(names[0], "a");
  EXPECT_EQ(names[1], "b");
}

TEST(StreamingParserTest, ReadCsvStreamToArrow) {
  std::istringstream input("x,y,z\n1,2,3\n4,5,6\n");

  auto table = read_csv_stream(input);
  ASSERT_NE(table, nullptr);

  // Export to Arrow stream and verify
  ArrowArrayStream stream;
  table->export_to_stream(&stream);

  ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  EXPECT_EQ(schema.n_children, 3);
  schema.release(&schema);

  size_t total_rows = 0;
  while (true) {
    ArrowArray batch;
    ASSERT_EQ(stream.get_next(&stream, &batch), 0);
    if (batch.release == nullptr)
      break;
    total_rows += static_cast<size_t>(batch.length);
    batch.release(&batch);
  }
  EXPECT_EQ(total_rows, 2);

  stream.release(&stream);
}

TEST(StreamingParserTest, ReadCsvStreamEmpty) {
  std::istringstream input("");
  auto table = read_csv_stream(input);
  // Empty input should return nullptr or empty table
  // Implementation can choose either, but should not crash
  if (table) {
    EXPECT_EQ(table->num_rows(), 0);
  }
}

TEST(StreamingParserTest, ReadCsvStreamHeaderOnly) {
  std::istringstream input("a,b,c\n");
  auto table = read_csv_stream(input);
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 0);
  EXPECT_EQ(table->num_columns(), 3);
}

TEST(StreamingParserTest, MultipleFeedsThenBatches) {
  StreamingOptions opts;
  opts.batch_size = 2;
  StreamingParser parser(opts);

  // Feed header + first row
  std::string part1 = "a,b\n1,2\n";
  parser.feed(part1.data(), part1.size());

  // Should have 0 or 1 batch ready depending on batch threshold
  // Feed more data
  std::string part2 = "3,4\n5,6\n";
  parser.feed(part2.data(), part2.size());

  // Feed final data
  std::string part3 = "7,8\n";
  parser.feed(part3.data(), part3.size());
  parser.finish();

  size_t total_rows = 0;
  size_t batch_count = 0;
  while (auto batch = parser.next_batch()) {
    total_rows += batch->num_rows;
    batch_count++;
  }
  EXPECT_EQ(total_rows, 4);
  // With batch_size=2 and 4 rows, we should get 2 batches
  EXPECT_EQ(batch_count, 2);
}

TEST(StreamingParserTest, IsLastFlag) {
  StreamingParser parser;

  std::string csv = "a\n1\n2\n";
  parser.feed(csv.data(), csv.size());
  parser.finish();

  std::vector<StreamBatch> batches;
  while (auto batch = parser.next_batch()) {
    batches.push_back(std::move(*batch));
  }

  ASSERT_FALSE(batches.empty());
  // Last batch should have is_last set
  EXPECT_TRUE(batches.back().is_last);
}
