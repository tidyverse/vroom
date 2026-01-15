#include <cstring>
#include <gtest/gtest.h>
#include <libvroom.h>
#include <string>

class SimplifiedAPITest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

TEST_F(SimplifiedAPITest, FileBufferBasics) {
  libvroom::FileBuffer empty;
  EXPECT_FALSE(empty.valid());
  EXPECT_TRUE(empty.empty());

  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  EXPECT_TRUE(buffer.valid());
  EXPECT_FALSE(buffer.empty());
}

TEST_F(SimplifiedAPITest, FileBufferMove) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer1(data, len);
  libvroom::FileBuffer buffer2(std::move(buffer1));
  EXPECT_FALSE(buffer1.valid());
  EXPECT_TRUE(buffer2.valid());
}

TEST_F(SimplifiedAPITest, FileBufferRelease) {
  auto [data, len] = make_buffer("a,b,c\n");
  libvroom::FileBuffer buffer(data, len);
  uint8_t* released = buffer.release();
  EXPECT_FALSE(buffer.valid());
  aligned_free(released);
}

TEST_F(SimplifiedAPITest, ParserBasicParsing) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

TEST_F(SimplifiedAPITest, ParserWithErrors) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(),
                             {.dialect = libvroom::Dialect::csv(), .errors = &errors});
  EXPECT_TRUE(result.success());
  EXPECT_TRUE(errors.has_errors());
}

TEST_F(SimplifiedAPITest, ParserDialects) {
  {
    auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;
    auto result = parser.parse(buffer.data(), buffer.size(), {.dialect = libvroom::Dialect::tsv()});
    EXPECT_TRUE(result.success());
  }
  {
    auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;
    auto result =
        parser.parse(buffer.data(), buffer.size(), {.dialect = libvroom::Dialect::semicolon()});
    EXPECT_TRUE(result.success());
  }
}

TEST_F(SimplifiedAPITest, DetectDialect) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len); // RAII wrapper handles cleanup
  auto detection = libvroom::detect_dialect(buffer.data(), buffer.size());
  EXPECT_TRUE(detection.success());
  EXPECT_EQ(detection.dialect.delimiter, ',');
}

TEST_F(SimplifiedAPITest, ParserAutoDetection) {
  auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
}

TEST_F(SimplifiedAPITest, ParserThreadCount) {
  libvroom::Parser parser1(1);
  EXPECT_EQ(parser1.num_threads(), 1);
  libvroom::Parser parser4(4);
  EXPECT_EQ(parser4.num_threads(), 4);
  parser4.set_num_threads(0);
  EXPECT_EQ(parser4.num_threads(), 1);
}

TEST_F(SimplifiedAPITest, CustomDialect) {
  auto [data, len] = make_buffer("a:b:c\n'hello':'world':'!'\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Dialect custom;
  custom.delimiter = ':';
  custom.quote_char = '\'';
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.dialect = custom});
  EXPECT_TRUE(result.success());
}

// ============================================================================
// Tests for the unified ParseOptions API
// ============================================================================

class UnifiedAPITest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: Default options (auto-detect dialect, fast path)
TEST_F(UnifiedAPITest, DefaultOptions) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  // Default: auto-detect dialect, throw on errors
  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Auto-detect semicolon-separated data
TEST_F(UnifiedAPITest, AutoDetectSemicolon) {
  auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: Auto-detect tab-separated data
TEST_F(UnifiedAPITest, AutoDetectTSV) {
  auto [data, len] = make_buffer("name\tage\tcity\nJohn\t25\tNYC\nJane\t30\tLA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, '\t');
}

// Test: Explicit dialect via ParseOptions
TEST_F(UnifiedAPITest, ExplicitDialect) {
  auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::semicolon();

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: Explicit dialect using factory method
TEST_F(UnifiedAPITest, ExplicitDialectFactory) {
  auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(),
                             libvroom::ParseOptions::with_dialect(libvroom::Dialect::tsv()));
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, '\t');
}

// Test: Error collection via ParseOptions
TEST_F(UnifiedAPITest, ErrorCollection) {
  // CSV with inconsistent field count (row 3 has only 2 fields)
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::ParseOptions opts;
  opts.errors = &errors;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success()); // Parsing succeeds in permissive mode
  EXPECT_TRUE(errors.has_errors());
}

// Test: Error collection using factory method
TEST_F(UnifiedAPITest, ErrorCollectionFactory) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  auto result =
      parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::with_errors(errors));
  EXPECT_TRUE(result.success());
  EXPECT_TRUE(errors.has_errors());
}

// Test: Explicit dialect + error collection
TEST_F(UnifiedAPITest, ExplicitDialectWithErrors) {
  auto [data, len] = make_buffer("a;b;c\n1;2;3\n4;5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::semicolon();
  opts.errors = &errors;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
  EXPECT_TRUE(errors.has_errors());
}

// Test: Explicit dialect + error collection using factory
TEST_F(UnifiedAPITest, ExplicitDialectWithErrorsFactory) {
  auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n4\t5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  auto result = parser.parse(
      buffer.data(), buffer.size(),
      libvroom::ParseOptions::with_dialect_and_errors(libvroom::Dialect::tsv(), errors));
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, '\t');
  EXPECT_TRUE(errors.has_errors());
}

// Test: Detection result is populated
TEST_F(UnifiedAPITest, DetectionResultPopulated) {
  auto [data, len] = make_buffer("name|age|city\nJohn|25|NYC\nJane|30|LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, '|');
  // Detection result should be populated when auto-detecting
  EXPECT_TRUE(result.detection.success());
  EXPECT_EQ(result.detection.dialect.delimiter, '|');
}

// Test: Legacy parse(buf, len, dialect) still works
TEST_F(UnifiedAPITest, LegacyParseWithDialect) {
  auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result =
      parser.parse(buffer.data(), buffer.size(), {.dialect = libvroom::Dialect::semicolon()});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: Legacy parse_with_errors still works
TEST_F(UnifiedAPITest, LegacyParseWithErrors) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  auto result = parser.parse(buffer.data(), buffer.size(),
                             {.dialect = libvroom::Dialect::csv(), .errors = &errors});
  EXPECT_TRUE(result.success());
  EXPECT_TRUE(errors.has_errors());
}

// Test: Legacy parse_auto still works
TEST_F(UnifiedAPITest, LegacyParseAuto) {
  auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: ParseOptions defaults factory
TEST_F(UnifiedAPITest, ParseOptionsDefaults) {
  auto opts = libvroom::ParseOptions::defaults();
  EXPECT_FALSE(opts.dialect.has_value());
  EXPECT_EQ(opts.errors, nullptr);
}

// Test: ParseOptions::auto_detect() factory method
TEST_F(UnifiedAPITest, ParseOptionsAutoDetect) {
  // auto_detect() should return default options with no dialect set (for auto-detection)
  auto opts = libvroom::ParseOptions::auto_detect();
  EXPECT_FALSE(opts.dialect.has_value());
  EXPECT_EQ(opts.errors, nullptr);
  EXPECT_EQ(opts.algorithm, libvroom::ParseAlgorithm::AUTO);
}

// Test: ParseOptions::auto_detect() actually performs auto-detection
TEST_F(UnifiedAPITest, ParseOptionsAutoDetectWithParsing) {
  auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::auto_detect());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';'); // Should auto-detect semicolon
  EXPECT_TRUE(result.detection.success());
}

// Test: ParseOptions::auto_detect_with_errors() factory method
TEST_F(UnifiedAPITest, ParseOptionsAutoDetectWithErrors) {
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  auto opts = libvroom::ParseOptions::auto_detect_with_errors(errors);
  EXPECT_FALSE(opts.dialect.has_value());
  EXPECT_EQ(opts.errors, &errors);
}

// Test: ParseOptions::auto_detect_with_errors() performs auto-detection with error collection
TEST_F(UnifiedAPITest, ParseOptionsAutoDetectWithErrorsAndParsing) {
  // CSV with inconsistent field count - should auto-detect delimiter and collect errors
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  auto result = parser.parse(buffer.data(), buffer.size(),
                             libvroom::ParseOptions::auto_detect_with_errors(errors));

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ','); // Should auto-detect comma
  EXPECT_TRUE(errors.has_errors());         // Should collect field count error
}

// Test: Custom detection options
TEST_F(UnifiedAPITest, CustomDetectionOptions) {
  auto [data, len] = make_buffer("a:b:c\n1:2:3\n4:5:6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ParseOptions opts;
  opts.detection_options.delimiters = {':', ','}; // Only check colon and comma

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ':');
}

// Test: Custom detection options with error collection
TEST_F(UnifiedAPITest, CustomDetectionOptionsWithErrors) {
  auto [data, len] = make_buffer("a:b:c\n1:2:3\n4:5\n"); // Inconsistent field count
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::ParseOptions opts;
  opts.detection_options.delimiters = {':', ','}; // Only check colon and comma
  opts.errors = &errors;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ':');
  EXPECT_TRUE(errors.has_errors()); // Should detect field count mismatch
}

// Test: Explicit dialect skips detection (performance optimization)
TEST_F(UnifiedAPITest, ExplicitDialectSkipsDetection) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(), {.dialect = libvroom::Dialect::csv()});
  EXPECT_TRUE(result.success());
  // Detection should not run when dialect is explicit
  EXPECT_EQ(result.detection.confidence, 0.0);
  EXPECT_EQ(result.detection.rows_analyzed, 0);
}

// ============================================================================
// Tests for ParseAlgorithm selection
// ============================================================================

class AlgorithmSelectionTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: ParseAlgorithm::AUTO (default)
TEST_F(AlgorithmSelectionTest, AutoAlgorithm) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result =
      parser.parse(buffer.data(), buffer.size(),
                   libvroom::ParseOptions::with_algorithm(libvroom::ParseAlgorithm::AUTO));
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseAlgorithm::SPECULATIVE
TEST_F(AlgorithmSelectionTest, SpeculativeAlgorithm) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::csv();
  opts.algorithm = libvroom::ParseAlgorithm::SPECULATIVE;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseAlgorithm::TWO_PASS
TEST_F(AlgorithmSelectionTest, TwoPassAlgorithm) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::csv();
  opts.algorithm = libvroom::ParseAlgorithm::TWO_PASS;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseAlgorithm::BRANCHLESS
TEST_F(AlgorithmSelectionTest, BranchlessAlgorithm) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::csv();
  opts.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseOptions::branchless() factory
TEST_F(AlgorithmSelectionTest, BranchlessFactory) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::branchless());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Branchless with custom dialect
TEST_F(AlgorithmSelectionTest, BranchlessWithDialect) {
  auto [data, len] = make_buffer("a;b;c\n1;2;3\n4;5;6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(),
                             libvroom::ParseOptions::branchless(libvroom::Dialect::semicolon()));
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Algorithm with multi-threading
TEST_F(AlgorithmSelectionTest, BranchlessMultiThreaded) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser(4); // 4 threads

  auto result = parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::branchless());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Different algorithms produce same results
TEST_F(AlgorithmSelectionTest, AlgorithmsProduceSameResults) {
  auto [data, len] = make_buffer("name,age,city\nAlice,30,NYC\nBob,25,LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  // Parse with each algorithm
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

  // All should succeed and produce same number of indexes
  EXPECT_TRUE(result_auto.success());
  EXPECT_TRUE(result_spec.success());
  EXPECT_TRUE(result_two.success());
  EXPECT_TRUE(result_branch.success());

  EXPECT_EQ(result_auto.total_indexes(), result_spec.total_indexes());
  EXPECT_EQ(result_auto.total_indexes(), result_two.total_indexes());
  EXPECT_EQ(result_auto.total_indexes(), result_branch.total_indexes());
}

// Test: Algorithm selection with quoted fields
TEST_F(AlgorithmSelectionTest, BranchlessWithQuotedFields) {
  auto [data, len] =
      make_buffer("name,description\n\"Alice\",\"Hello, World\"\n\"Bob\",\"Line1\\nLine2\"\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(), libvroom::ParseOptions::branchless());
  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// ============================================================================
// Tests for Row/Column Iteration API (Parser::Result)
// ============================================================================

class RowColumnIterationTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// --- Basic Iteration Tests ---

TEST_F(RowColumnIterationTest, NumRowsWithHeader) {
  auto [data, len] = make_buffer("name,age,city\nAlice,30,NYC\nBob,25,LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2); // Header is excluded
  EXPECT_EQ(result.num_columns(), 3);
}

TEST_F(RowColumnIterationTest, RangeBasedForLoop) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\nCharlie,35\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());

  std::vector<std::string> names;
  for (auto row : result.rows()) {
    names.push_back(std::string(row.get_string_view(0)));
  }

  EXPECT_EQ(names.size(), 3);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
  EXPECT_EQ(names[2], "Charlie");
}

TEST_F(RowColumnIterationTest, RowViewSize) {
  auto [data, len] = make_buffer("a,b\n1,2\n3,4\n5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto rows = result.rows();

  EXPECT_EQ(rows.size(), 3);
  EXPECT_FALSE(rows.empty());
}

TEST_F(RowColumnIterationTest, RowViewEmpty) {
  auto [data, len] = make_buffer("a,b\n"); // Header only, no data rows
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto rows = result.rows();

  EXPECT_EQ(rows.size(), 0);
  EXPECT_TRUE(rows.empty());
}

TEST_F(RowColumnIterationTest, RowByIndex) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\nCharlie,35\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());

  auto row0 = result.row(0);
  auto row1 = result.row(1);
  auto row2 = result.row(2);

  EXPECT_EQ(row0.get_string_view(0), "Alice");
  EXPECT_EQ(row1.get_string_view(0), "Bob");
  EXPECT_EQ(row2.get_string_view(0), "Charlie");
}

TEST_F(RowColumnIterationTest, RowByIndexOutOfRange) {
  auto [data, len] = make_buffer("a,b\n1,2\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_THROW(result.row(99), std::out_of_range);
}

// --- Typed Value Access Tests ---

TEST_F(RowColumnIterationTest, GetByColumnIndex) {
  auto [data, len] = make_buffer("name,age,score\nAlice,30,95.5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto row = result.row(0);

  EXPECT_EQ(row.get_string_view(0), "Alice");
  EXPECT_EQ(row.get<int64_t>(1).get(), 30);
  EXPECT_NEAR(row.get<double>(2).get(), 95.5, 0.01);
}

TEST_F(RowColumnIterationTest, GetByColumnName) {
  auto [data, len] = make_buffer("name,age,score\nAlice,30,95.5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto row = result.row(0);

  EXPECT_EQ(row.get_string_view("name"), "Alice");
  EXPECT_EQ(row.get<int64_t>("age").get(), 30);
  EXPECT_NEAR(row.get<double>("score").get(), 95.5, 0.01);
}

TEST_F(RowColumnIterationTest, GetByColumnNameNotFound) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto row = result.row(0);

  EXPECT_THROW(row.get<int64_t>("nonexistent"), std::out_of_range);
  EXPECT_THROW(row.get_string_view("nonexistent"), std::out_of_range);
  EXPECT_THROW(row.get_string("nonexistent"), std::out_of_range);
}

TEST_F(RowColumnIterationTest, GetStringWithEscaping) {
  auto [data, len] = make_buffer("name,desc\nAlice,\"Hello, \"\"World\"\"\"\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto row = result.row(0);

  // get_string() should unescape the quoted field
  EXPECT_EQ(row.get_string(1), "Hello, \"World\"");
}

TEST_F(RowColumnIterationTest, RowNumColumns) {
  auto [data, len] = make_buffer("a,b,c,d,e\n1,2,3,4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto row = result.row(0);

  EXPECT_EQ(row.num_columns(), 5);
}

TEST_F(RowColumnIterationTest, RowIndex) {
  auto [data, len] = make_buffer("a\n1\n2\n3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  size_t i = 0;
  for (auto row : result.rows()) {
    EXPECT_EQ(row.row_index(), i);
    ++i;
  }
}

// --- Column Extraction Tests ---

TEST_F(RowColumnIterationTest, ColumnExtractionByIndex) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\nCharlie,35\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto ages = result.column<int64_t>(1);

  EXPECT_EQ(ages.size(), 3);
  EXPECT_EQ(*ages[0], 30);
  EXPECT_EQ(*ages[1], 25);
  EXPECT_EQ(*ages[2], 35);
}

TEST_F(RowColumnIterationTest, ColumnExtractionByName) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\nCharlie,35\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto ages = result.column<int64_t>("age");

  EXPECT_EQ(ages.size(), 3);
  EXPECT_EQ(*ages[0], 30);
  EXPECT_EQ(*ages[1], 25);
  EXPECT_EQ(*ages[2], 35);
}

TEST_F(RowColumnIterationTest, ColumnExtractionByNameNotFound) {
  auto [data, len] = make_buffer("a,b\n1,2\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_THROW(result.column<int64_t>("nonexistent"), std::out_of_range);
}

TEST_F(RowColumnIterationTest, ColumnWithNAValues) {
  auto [data, len] = make_buffer("val\n1\nNA\n3\n\n5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto vals = result.column<int64_t>(0);

  EXPECT_EQ(vals.size(), 5);
  EXPECT_TRUE(vals[0].has_value());
  EXPECT_FALSE(vals[1].has_value()); // NA
  EXPECT_TRUE(vals[2].has_value());
  EXPECT_FALSE(vals[3].has_value()); // empty
  EXPECT_TRUE(vals[4].has_value());
}

TEST_F(RowColumnIterationTest, ColumnOrWithDefault) {
  auto [data, len] = make_buffer("val\n1\nNA\n3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto vals = result.column_or<int64_t>(0, -999);

  EXPECT_EQ(vals.size(), 3);
  EXPECT_EQ(vals[0], 1);
  EXPECT_EQ(vals[1], -999); // NA replaced with default
  EXPECT_EQ(vals[2], 3);
}

TEST_F(RowColumnIterationTest, ColumnOrByName) {
  auto [data, len] = make_buffer("score\n90.5\nNA\n75.0\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto scores = result.column_or<double>("score", 0.0);

  EXPECT_EQ(scores.size(), 3);
  EXPECT_NEAR(scores[0], 90.5, 0.01);
  EXPECT_NEAR(scores[1], 0.0, 0.01); // NA replaced with default
  EXPECT_NEAR(scores[2], 75.0, 0.01);
}

TEST_F(RowColumnIterationTest, ColumnOrByNameNotFound) {
  auto [data, len] = make_buffer("a\n1\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_THROW(result.column_or<int64_t>("nonexistent", 0), std::out_of_range);
}

TEST_F(RowColumnIterationTest, ColumnStringView) {
  auto [data, len] = make_buffer("name\nAlice\nBob\nCharlie\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto names = result.column_string_view(0);

  EXPECT_EQ(names.size(), 3);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
  EXPECT_EQ(names[2], "Charlie");
}

TEST_F(RowColumnIterationTest, ColumnStringViewByName) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto names = result.column_string_view("name");

  EXPECT_EQ(names.size(), 2);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
}

TEST_F(RowColumnIterationTest, ColumnStringViewByNameNotFound) {
  auto [data, len] = make_buffer("a\n1\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_THROW(result.column_string_view("nonexistent"), std::out_of_range);
}

TEST_F(RowColumnIterationTest, ColumnString) {
  auto [data, len] = make_buffer("name\n\"Alice\"\n\"Bob\"\n\"Charlie\"\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto names = result.column_string(0);

  EXPECT_EQ(names.size(), 3);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
  EXPECT_EQ(names[2], "Charlie");
}

TEST_F(RowColumnIterationTest, ColumnStringByName) {
  auto [data, len] = make_buffer("desc\n\"Hello, \"\"World\"\"\"\n\"Simple\"\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto descs = result.column_string("desc");

  EXPECT_EQ(descs.size(), 2);
  EXPECT_EQ(descs[0], "Hello, \"World\"");
  EXPECT_EQ(descs[1], "Simple");
}

TEST_F(RowColumnIterationTest, ColumnStringByNameNotFound) {
  auto [data, len] = make_buffer("a\n1\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_THROW(result.column_string("nonexistent"), std::out_of_range);
}

// --- Header Tests ---

TEST_F(RowColumnIterationTest, Header) {
  auto [data, len] = make_buffer("name,age,city\nAlice,30,NYC\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto headers = result.header();

  EXPECT_EQ(headers.size(), 3);
  EXPECT_EQ(headers[0], "name");
  EXPECT_EQ(headers[1], "age");
  EXPECT_EQ(headers[2], "city");
}

TEST_F(RowColumnIterationTest, HasHeader) {
  auto [data, len] = make_buffer("a,b\n1,2\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.has_header());
}

TEST_F(RowColumnIterationTest, SetHasHeader) {
  auto [data, len] = make_buffer("1,2\n3,4\n5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  // Default: has header
  EXPECT_TRUE(result.has_header());
  EXPECT_EQ(result.num_rows(), 2);

  // Disable header
  result.set_has_header(false);
  EXPECT_FALSE(result.has_header());
  EXPECT_EQ(result.num_rows(), 3);
}

TEST_F(RowColumnIterationTest, ColumnIndex) {
  auto [data, len] = make_buffer("name,age,city\nAlice,30,NYC\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  auto name_idx = result.column_index("name");
  auto age_idx = result.column_index("age");
  auto city_idx = result.column_index("city");
  auto missing_idx = result.column_index("nonexistent");

  EXPECT_TRUE(name_idx.has_value());
  EXPECT_EQ(*name_idx, 0);
  EXPECT_TRUE(age_idx.has_value());
  EXPECT_EQ(*age_idx, 1);
  EXPECT_TRUE(city_idx.has_value());
  EXPECT_EQ(*city_idx, 2);
  EXPECT_FALSE(missing_idx.has_value());
}

// --- Iterator Tests ---

TEST_F(RowColumnIterationTest, IteratorIncrement) {
  auto [data, len] = make_buffer("a\n1\n2\n3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto rows = result.rows();
  auto it = rows.begin();

  EXPECT_EQ((*it).get_string_view(0), "1");
  ++it;
  EXPECT_EQ((*it).get_string_view(0), "2");
  it++; // post-increment
  EXPECT_EQ((*it).get_string_view(0), "3");
  ++it;
  EXPECT_EQ(it, rows.end());
}

TEST_F(RowColumnIterationTest, IteratorEquality) {
  auto [data, len] = make_buffer("a\n1\n2\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto rows = result.rows();

  auto it1 = rows.begin();
  auto it2 = rows.begin();

  EXPECT_TRUE(it1 == it2);
  EXPECT_FALSE(it1 != it2);

  ++it1;
  EXPECT_FALSE(it1 == it2);
  EXPECT_TRUE(it1 != it2);
}

// --- Type Conversion Tests ---

TEST_F(RowColumnIterationTest, TypeConversionInt32) {
  auto [data, len] = make_buffer("val\n42\n-17\n0\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_EQ(result.row(0).get<int32_t>(0).get(), 42);
  EXPECT_EQ(result.row(1).get<int32_t>(0).get(), -17);
  EXPECT_EQ(result.row(2).get<int32_t>(0).get(), 0);
}

TEST_F(RowColumnIterationTest, TypeConversionInt64) {
  auto [data, len] = make_buffer("val\n9223372036854775807\n-9223372036854775808\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_EQ(result.row(0).get<int64_t>(0).get(), INT64_MAX);
  EXPECT_EQ(result.row(1).get<int64_t>(0).get(), INT64_MIN);
}

TEST_F(RowColumnIterationTest, TypeConversionDouble) {
  auto [data, len] = make_buffer("val\n3.14159\n-2.5e10\n0.0\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_NEAR(result.row(0).get<double>(0).get(), 3.14159, 0.0001);
  EXPECT_NEAR(result.row(1).get<double>(0).get(), -2.5e10, 1e5);
  EXPECT_NEAR(result.row(2).get<double>(0).get(), 0.0, 0.0001);
}

TEST_F(RowColumnIterationTest, TypeConversionBool) {
  auto [data, len] = make_buffer("val\ntrue\nfalse\nTRUE\nFALSE\n1\n0\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_TRUE(result.row(0).get<bool>(0).get());
  EXPECT_FALSE(result.row(1).get<bool>(0).get());
  EXPECT_TRUE(result.row(2).get<bool>(0).get());
  EXPECT_FALSE(result.row(3).get<bool>(0).get());
  EXPECT_TRUE(result.row(4).get<bool>(0).get());
  EXPECT_FALSE(result.row(5).get<bool>(0).get());
}

// --- Multi-threaded Parsing Tests ---

TEST_F(RowColumnIterationTest, MultiThreadedParsing) {
  // Create a larger CSV to benefit from multi-threading
  std::string csv = "name,age,score\n";
  for (int i = 0; i < 100; ++i) {
    csv += "Person" + std::to_string(i) + "," + std::to_string(20 + i % 50) + "," +
           std::to_string(50 + i) + "\n";
  }

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);
  // Use single-threaded parsing for now - multi-threaded parsing
  // with the iteration API is tested separately in other test files
  libvroom::Parser parser(1);

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 100);

  // Verify data integrity
  EXPECT_EQ(result.row(0).get_string_view("name"), "Person0");
  EXPECT_EQ(result.row(99).get_string_view("name"), "Person99");
  EXPECT_EQ(result.row(50).get<int64_t>("age").get(), 20);
}

// --- Edge Cases ---

TEST_F(RowColumnIterationTest, SingleColumn) {
  auto [data, len] = make_buffer("value\n1\n2\n3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_EQ(result.num_columns(), 1);
  EXPECT_EQ(result.num_rows(), 3);

  auto vals = result.column<int64_t>(0);
  EXPECT_EQ(vals.size(), 3);
}

TEST_F(RowColumnIterationTest, SingleRow) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_EQ(result.num_rows(), 1);

  int count = 0;
  for (auto row : result.rows()) {
    EXPECT_EQ(row.get<int64_t>(0).get(), 1);
    EXPECT_EQ(row.get<int64_t>(1).get(), 2);
    EXPECT_EQ(row.get<int64_t>(2).get(), 3);
    ++count;
  }
  EXPECT_EQ(count, 1);
}

TEST_F(RowColumnIterationTest, EmptyFields) {
  auto [data, len] = make_buffer("a,b,c\n,,\n1,,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  // First row: all empty
  EXPECT_EQ(result.row(0).get_string_view(0), "");
  EXPECT_EQ(result.row(0).get_string_view(1), "");
  EXPECT_EQ(result.row(0).get_string_view(2), "");
  EXPECT_TRUE(result.row(0).get<int64_t>(0).is_na());

  // Second row: middle empty
  EXPECT_EQ(result.row(1).get<int64_t>(0).get(), 1);
  EXPECT_TRUE(result.row(1).get<int64_t>(1).is_na());
  EXPECT_EQ(result.row(1).get<int64_t>(2).get(), 3);
}

TEST_F(RowColumnIterationTest, QuotedFieldsWithDelimiters) {
  auto [data, len] = make_buffer("name,desc\nAlice,\"Hello, World\"\nBob,\"Line1\nLine2\"\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_EQ(result.num_rows(), 2);

  // Quoted field containing delimiter
  EXPECT_EQ(result.row(0).get_string(1), "Hello, World");
}

TEST_F(RowColumnIterationTest, CRLFLineEndings) {
  auto [data, len] = make_buffer("a,b\r\n1,2\r\n3,4\r\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  EXPECT_EQ(result.num_rows(), 2);
  EXPECT_EQ(result.row(0).get<int64_t>(0).get(), 1);
  EXPECT_EQ(result.row(1).get<int64_t>(0).get(), 3);
}

TEST_F(RowColumnIterationTest, WhitespaceInFields) {
  auto [data, len] = make_buffer("a,b\n  1  ,  2  \n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  // get_string_view preserves whitespace
  EXPECT_EQ(result.row(0).get_string_view(0), "  1  ");

  // get<int64_t> should trim whitespace during parsing
  EXPECT_EQ(result.row(0).get<int64_t>(0).get(), 1);
  EXPECT_EQ(result.row(0).get<int64_t>(1).get(), 2);
}

TEST_F(RowColumnIterationTest, ColumnDoubleType) {
  auto [data, len] = make_buffer("score\n1.5\n2.5\n3.5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto scores = result.column<double>(0);

  EXPECT_EQ(scores.size(), 3);
  EXPECT_NEAR(*scores[0], 1.5, 0.01);
  EXPECT_NEAR(*scores[1], 2.5, 0.01);
  EXPECT_NEAR(*scores[2], 3.5, 0.01);
}

TEST_F(RowColumnIterationTest, ColumnBoolType) {
  auto [data, len] = make_buffer("flag\ntrue\nfalse\ntrue\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());
  auto flags = result.column<bool>(0);

  EXPECT_EQ(flags.size(), 3);
  EXPECT_TRUE(*flags[0]);
  EXPECT_FALSE(*flags[1]);
  EXPECT_TRUE(*flags[2]);
}

// --- Different Dialects ---

TEST_F(RowColumnIterationTest, TSVIteration) {
  auto [data, len] = make_buffer("name\tage\nAlice\t30\nBob\t25\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size(), {.dialect = libvroom::Dialect::tsv()});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2);

  EXPECT_EQ(result.row(0).get_string_view("name"), "Alice");
  EXPECT_EQ(result.row(0).get<int64_t>("age").get(), 30);
}

TEST_F(RowColumnIterationTest, SemicolonIteration) {
  auto [data, len] = make_buffer("name;age\nAlice;30\nBob;25\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result =
      parser.parse(buffer.data(), buffer.size(), {.dialect = libvroom::Dialect::semicolon()});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2);

  auto names = result.column_string_view("name");
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
}

// ============================================================================
// Tests for UTF-8 Validation
// ============================================================================

class UTF8ValidationTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: SizeLimits::validate_utf8 default is false
TEST_F(UTF8ValidationTest, ValidationDisabledByDefault) {
  auto limits = libvroom::SizeLimits::defaults();
  EXPECT_FALSE(limits.validate_utf8);
}

// Test: SizeLimits::strict() enables UTF-8 validation
TEST_F(UTF8ValidationTest, StrictEnablesValidation) {
  auto limits = libvroom::SizeLimits::strict();
  EXPECT_TRUE(limits.validate_utf8);
}

// Test: Invalid UTF-8 detection (0xFF is never valid)
TEST_F(UTF8ValidationTest, InvalidByteDetected) {
  std::string content = "a,b,c\n1,\xFF,3\n";
  auto [data, len] = make_buffer(content);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::SizeLimits limits;
  limits.validate_utf8 = true;

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors, .limits = limits});
  EXPECT_TRUE(errors.has_errors());
  bool found_utf8_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == libvroom::ErrorCode::INVALID_UTF8) {
      found_utf8_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_utf8_error);
}

// Test: Valid UTF-8 with multi-byte characters passes
TEST_F(UTF8ValidationTest, ValidMultiByteCharacters) {
  // Valid UTF-8: Zürich (ü = 2 bytes), 日本 (each = 3 bytes)
  std::string content = "name,city\nAlice,Zürich\nBob,日本\n";
  auto [data, len] = make_buffer(content);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::SizeLimits limits;
  limits.validate_utf8 = true;

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors, .limits = limits});
  EXPECT_TRUE(result.success());
  // No UTF-8 errors
  for (const auto& err : errors.errors()) {
    EXPECT_NE(err.code, libvroom::ErrorCode::INVALID_UTF8);
  }
}

// Test: Invalid UTF-8 not detected when validation disabled
TEST_F(UTF8ValidationTest, NoValidationWhenDisabled) {
  std::string content = "a,b,c\n1,\xFF,3\n"; // Invalid UTF-8
  auto [data, len] = make_buffer(content);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::SizeLimits limits = libvroom::SizeLimits::defaults();
  // validate_utf8 is false by default

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors, .limits = limits});
  // No UTF-8 error because validation is disabled
  for (const auto& err : errors.errors()) {
    EXPECT_NE(err.code, libvroom::ErrorCode::INVALID_UTF8);
  }
}

// Test: Truncated UTF-8 sequence detected
TEST_F(UTF8ValidationTest, TruncatedSequenceDetected) {
  // Truncated 2-byte sequence (starts with 110xxxxx but no continuation byte)
  std::string content = "a,b\n1,\xC0\n";
  auto [data, len] = make_buffer(content);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::SizeLimits limits;
  limits.validate_utf8 = true;

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors, .limits = limits});
  EXPECT_TRUE(errors.has_errors());
  bool found_utf8_error = false;
  for (const auto& err : errors.errors()) {
    if (err.code == libvroom::ErrorCode::INVALID_UTF8) {
      found_utf8_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_utf8_error);
}

// Test: Overlong UTF-8 encoding detected
TEST_F(UTF8ValidationTest, OverlongEncodingDetected) {
  // 0xC0 0x80 encodes NUL as 2 bytes (overlong)
  std::string content = "a,b\n1,\xC0\x80\n";
  auto [data, len] = make_buffer(content);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::SizeLimits limits;
  limits.validate_utf8 = true;

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors, .limits = limits});
  EXPECT_TRUE(errors.has_errors());
}

// ============================================================================
// Tests for AlignedBuffer and RAII memory management utilities
// ============================================================================

class AlignedBufferTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: AlignedBuffer basic construction and usage
TEST_F(AlignedBufferTest, BasicConstruction) {
  libvroom::AlignedBuffer empty;
  EXPECT_FALSE(empty);
  EXPECT_FALSE(empty.valid());
  EXPECT_EQ(empty.data(), nullptr);
  EXPECT_EQ(empty.size, 0);
}

// Test: AlignedBuffer with data
TEST_F(AlignedBufferTest, WithData) {
  AlignedPtr ptr = make_aligned_ptr(100, 64);
  ASSERT_NE(ptr.get(), nullptr);
  ptr[0] = 'X';
  ptr[99] = 'Y';

  uint8_t* raw = ptr.get();
  libvroom::AlignedBuffer buffer(std::move(ptr), 100);

  EXPECT_TRUE(buffer);
  EXPECT_TRUE(buffer.valid());
  EXPECT_EQ(buffer.data(), raw);
  EXPECT_EQ(buffer.size, 100);
  EXPECT_EQ(buffer.data()[0], 'X');
  EXPECT_EQ(buffer.data()[99], 'Y');
}

// Test: AlignedBuffer move semantics
TEST_F(AlignedBufferTest, MoveSemantics) {
  AlignedPtr ptr = make_aligned_ptr(100, 64);
  ptr[0] = 'A';
  uint8_t* raw = ptr.get();

  libvroom::AlignedBuffer buffer1(std::move(ptr), 100);
  libvroom::AlignedBuffer buffer2(std::move(buffer1));

  EXPECT_FALSE(buffer1.valid());
  EXPECT_TRUE(buffer2.valid());
  EXPECT_EQ(buffer2.data(), raw);
  EXPECT_EQ(buffer2.data()[0], 'A');
}

// Test: AlignedBuffer move assignment
TEST_F(AlignedBufferTest, MoveAssignment) {
  AlignedPtr ptr1 = make_aligned_ptr(100, 64);
  ptr1[0] = 'B';
  uint8_t* raw1 = ptr1.get();

  AlignedPtr ptr2 = make_aligned_ptr(200, 64);
  ptr2[0] = 'C';

  libvroom::AlignedBuffer buffer1(std::move(ptr1), 100);
  libvroom::AlignedBuffer buffer2(std::move(ptr2), 200);

  buffer2 = std::move(buffer1);

  EXPECT_FALSE(buffer1.valid());
  EXPECT_TRUE(buffer2.valid());
  EXPECT_EQ(buffer2.data(), raw1);
  EXPECT_EQ(buffer2.size, 100);
  EXPECT_EQ(buffer2.data()[0], 'B');
}

// Test: AlignedBuffer release
TEST_F(AlignedBufferTest, Release) {
  AlignedPtr ptr = make_aligned_ptr(100, 64);
  ptr[0] = 'D';
  uint8_t* raw = ptr.get();

  libvroom::AlignedBuffer buffer(std::move(ptr), 100);
  uint8_t* released = buffer.release();

  EXPECT_FALSE(buffer.valid());
  EXPECT_EQ(buffer.size, 0);
  EXPECT_EQ(released, raw);
  EXPECT_EQ(released[0], 'D');

  // Must manually free released pointer
  aligned_free(released);
}

// Test: AlignedBuffer empty() method
TEST_F(AlignedBufferTest, EmptyMethod) {
  libvroom::AlignedBuffer empty;
  EXPECT_TRUE(empty.empty());

  AlignedPtr ptr = make_aligned_ptr(0, 64);
  libvroom::AlignedBuffer zero_size(std::move(ptr), 0);
  EXPECT_TRUE(zero_size.empty());
  EXPECT_TRUE(zero_size.valid()); // Valid pointer but empty data
}

// Test: AlignedBuffer with Parser
TEST_F(AlignedBufferTest, WithParser) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\n");
  AlignedPtr ptr(data);
  libvroom::AlignedBuffer buffer(std::move(ptr), len);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size);

  EXPECT_TRUE(result.success());
  EXPECT_GT(result.total_indexes(), 0);
}

// Test: Multiple AlignedBuffers (memory sanitizers will catch leaks)
TEST_F(AlignedBufferTest, MultipleBuffers) {
  std::vector<libvroom::AlignedBuffer> buffers;
  for (int i = 0; i < 10; ++i) {
    AlignedPtr ptr = make_aligned_ptr(1024, 64);
    buffers.emplace_back(std::move(ptr), 1024);
    EXPECT_TRUE(buffers.back().valid());
  }
  // All automatically freed when vector goes out of scope
}

// ============================================================================
// Tests for ParseIndex class RAII memory management
// ============================================================================

class IndexMemoryTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: ParseIndex default construction creates empty, uninitialized index
TEST_F(IndexMemoryTest, DefaultConstruction) {
  libvroom::ParseIndex idx;
  EXPECT_EQ(idx.columns, 0);
  EXPECT_EQ(idx.n_threads, 0);
  EXPECT_EQ(idx.n_indexes, nullptr);
  EXPECT_EQ(idx.indexes, nullptr);
}

// Test: ParseIndex initialization via TwoPass::init() allocates memory
TEST_F(IndexMemoryTest, Initialization) {
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(1024, 4);

  EXPECT_EQ(idx.n_threads, 4);
  EXPECT_NE(idx.n_indexes, nullptr);
  EXPECT_NE(idx.indexes, nullptr);
  // Memory automatically freed when idx goes out of scope
}

// Test: ParseIndex move construction transfers ownership
TEST_F(IndexMemoryTest, MoveConstruction) {
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx1 = parser.init(1024, 2);

  uint64_t* original_n_indexes = idx1.n_indexes;
  uint64_t* original_indexes = idx1.indexes;

  libvroom::ParseIndex idx2(std::move(idx1));

  // Original should be nulled out
  EXPECT_EQ(idx1.n_indexes, nullptr);
  EXPECT_EQ(idx1.indexes, nullptr);

  // New index should have the pointers
  EXPECT_EQ(idx2.n_indexes, original_n_indexes);
  EXPECT_EQ(idx2.indexes, original_indexes);
  EXPECT_EQ(idx2.n_threads, 2);
}

// Test: ParseIndex move assignment transfers ownership
TEST_F(IndexMemoryTest, MoveAssignment) {
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx1 = parser.init(1024, 2);
  libvroom::ParseIndex idx2 = parser.init(2048, 4);

  uint64_t* idx1_n_indexes = idx1.n_indexes;
  uint64_t* idx1_indexes = idx1.indexes;

  idx2 = std::move(idx1);

  // Original should be nulled out
  EXPECT_EQ(idx1.n_indexes, nullptr);
  EXPECT_EQ(idx1.indexes, nullptr);

  // idx2 should now have idx1's pointers (old idx2 memory was freed)
  EXPECT_EQ(idx2.n_indexes, idx1_n_indexes);
  EXPECT_EQ(idx2.indexes, idx1_indexes);
  EXPECT_EQ(idx2.n_threads, 2);
}

// Test: ParseIndex self-assignment is safe
TEST_F(IndexMemoryTest, SelfAssignment) {
  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(1024, 2);

  uint64_t* original_n_indexes = idx.n_indexes;
  uint64_t* original_indexes = idx.indexes;

  idx = std::move(idx); // Self-assignment

  // Should still have valid pointers
  EXPECT_EQ(idx.n_indexes, original_n_indexes);
  EXPECT_EQ(idx.indexes, original_indexes);
}

// Test: Multiple ParseIndex allocations (memory sanitizers will catch leaks)
TEST_F(IndexMemoryTest, MultipleAllocations) {
  libvroom::TwoPass parser;
  std::vector<libvroom::ParseIndex> indexes;

  for (int i = 0; i < 10; ++i) {
    indexes.push_back(parser.init(1024, 4));
    EXPECT_NE(indexes.back().n_indexes, nullptr);
    EXPECT_NE(indexes.back().indexes, nullptr);
  }
  // All automatically freed when vector goes out of scope
}

// Test: ParseIndex with parsing (integration test)
TEST_F(IndexMemoryTest, WithParsing) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size(), 1);

  bool success = parser.parse(buffer.data(), idx, buffer.size());

  EXPECT_TRUE(success);
  EXPECT_GT(idx.n_indexes[0], 0);
  // Memory automatically freed when idx and buffer go out of scope
}

// Test: ParseIndex with multi-threaded parsing
TEST_F(IndexMemoryTest, WithMultiThreadedParsing) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n");
  libvroom::FileBuffer buffer(data, len);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size(), 4);

  bool success = parser.parse(buffer.data(), idx, buffer.size());

  EXPECT_TRUE(success);
  // Memory automatically freed when idx and buffer go out of scope
}

// Test: Parser::Result (contains ParseIndex) memory management
TEST_F(IndexMemoryTest, ParserResultMemory) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size());

  EXPECT_TRUE(result.success());
  EXPECT_NE(result.idx.n_indexes, nullptr);
  EXPECT_NE(result.idx.indexes, nullptr);
  // Memory automatically freed when result goes out of scope
}

// Test: Parser::Result move semantics
TEST_F(IndexMemoryTest, ParserResultMove) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);

  libvroom::Parser parser;
  auto result1 = parser.parse(buffer.data(), buffer.size());

  uint64_t* original_n_indexes = result1.idx.n_indexes;
  uint64_t* original_indexes = result1.idx.indexes;

  auto result2 = std::move(result1);

  // Original should be nulled out
  EXPECT_EQ(result1.idx.n_indexes, nullptr);
  EXPECT_EQ(result1.idx.indexes, nullptr);

  // New result should have the pointers
  EXPECT_EQ(result2.idx.n_indexes, original_n_indexes);
  EXPECT_EQ(result2.idx.indexes, original_indexes);
}

// ============================================================================
// Tests for Unified Error Handling API (Result.errors())
// ============================================================================

class UnifiedErrorHandlingTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: No errors on well-formed CSV
TEST_F(UnifiedErrorHandlingTest, NoErrorsOnWellFormedCSV) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.has_errors());
  EXPECT_FALSE(result.has_fatal_errors());
  EXPECT_EQ(result.error_count(), 0);
  EXPECT_TRUE(result.errors().empty());
}

// Test: Errors collected on malformed CSV via result.errors()
TEST_F(UnifiedErrorHandlingTest, ErrorsCollectedInResult) {
  // CSV with inconsistent field count
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  EXPECT_TRUE(result.success()); // Parsing continues despite errors
  EXPECT_TRUE(result.has_errors());
  EXPECT_GT(result.error_count(), 0);
  EXPECT_FALSE(result.errors().empty());

  // Check that error is INCONSISTENT_FIELD_COUNT
  bool found_field_count_error = false;
  for (const auto& err : result.errors()) {
    if (err.code == libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT) {
      found_field_count_error = true;
      break;
    }
  }
  EXPECT_TRUE(found_field_count_error);
}

// Test: error_summary() returns non-empty string
TEST_F(UnifiedErrorHandlingTest, ErrorSummaryWorks) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  std::string summary = result.error_summary();
  EXPECT_FALSE(summary.empty());
}

// Test: error_mode() returns PERMISSIVE by default for internal collector
TEST_F(UnifiedErrorHandlingTest, DefaultErrorMode) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  auto result = parser.parse(buffer.data(), buffer.size());

  // Internal collector uses PERMISSIVE mode by default
  EXPECT_EQ(result.error_mode(), libvroom::ErrorMode::PERMISSIVE);
}

// Test: Backward compatibility - external ErrorCollector still works
TEST_F(UnifiedErrorHandlingTest, BackwardCompatibilityWithExternalCollector) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);
  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  // Both external and internal collectors should have errors
  EXPECT_TRUE(external_errors.has_errors());
  EXPECT_TRUE(result.has_errors());
  EXPECT_EQ(external_errors.error_count(), result.error_count());
}

// Test: Internal error collector uses PERMISSIVE mode even with external collector
TEST_F(UnifiedErrorHandlingTest, InternalCollectorAlwaysPermissive) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  // Even if external collector has BEST_EFFORT mode, internal stays PERMISSIVE
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::BEST_EFFORT);
  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  // Internal collector always uses PERMISSIVE to collect all errors
  EXPECT_EQ(result.error_mode(), libvroom::ErrorMode::PERMISSIVE);
}

// Test: Access to internal error_collector()
TEST_F(UnifiedErrorHandlingTest, AccessToInternalErrorCollector) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  // Access const version
  const auto& collector = result.error_collector();
  EXPECT_TRUE(collector.has_errors());
}

// Test: Multiple errors collected
TEST_F(UnifiedErrorHandlingTest, MultipleErrorsCollected) {
  // CSV with multiple issues: inconsistent field count on multiple rows
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  EXPECT_TRUE(result.has_errors());
  EXPECT_GE(result.error_count(), 1); // At least one error
}

// Test: Errors accessible via iteration
TEST_F(UnifiedErrorHandlingTest, ErrorsAccessibleViaIteration) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  // Iterate through errors
  size_t count = 0;
  for (const auto& err : result.errors()) {
    EXPECT_NE(err.code, libvroom::ErrorCode::NONE);
    ++count;
  }
  EXPECT_EQ(count, result.error_count());
}

// Test: Result without parsing has no errors
TEST_F(UnifiedErrorHandlingTest, EmptyResultHasNoErrors) {
  libvroom::Parser::Result result;

  EXPECT_FALSE(result.has_errors());
  EXPECT_FALSE(result.has_fatal_errors());
  EXPECT_EQ(result.error_count(), 0);
  EXPECT_TRUE(result.errors().empty());
}

// Test: Parse with valid data and iterate using new API
TEST_F(UnifiedErrorHandlingTest, ParseAndIterateWithErrorCheck) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob,25\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  // Check no errors
  EXPECT_FALSE(result.has_errors());

  // Can still iterate over rows
  int count = 0;
  for (auto row : result.rows()) {
    auto name = row.get_string_view("name");
    auto age = row.get<int64_t>("age");
    EXPECT_TRUE(age.ok());
    ++count;
  }
  EXPECT_EQ(count, 2);
}

// Test: Parse malformed data and still iterate
TEST_F(UnifiedErrorHandlingTest, ParseMalformedAndIterate) {
  auto [data, len] = make_buffer("name,age\nAlice,30\nBob\n"); // Bob row is missing age
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  libvroom::ErrorCollector external_errors(libvroom::ErrorMode::PERMISSIVE);

  auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &external_errors});

  // Should have errors
  EXPECT_TRUE(result.has_errors());

  // But we can still check the error details
  bool found_error = false;
  for (const auto& err : result.errors()) {
    if (err.code == libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT) {
      found_error = true;
      EXPECT_EQ(err.line, 3); // Error on line 3
    }
  }
  EXPECT_TRUE(found_error);
}

// ============================================================================
// Tests for Progress Callback API
// ============================================================================

class ProgressCallbackTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// Test: Progress callback is called during parsing
TEST_F(ProgressCallbackTest, CallbackIsCalled) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  int call_count = 0;
  size_t last_processed = 0;
  size_t reported_total = 0;

  libvroom::ParseOptions opts;
  opts.progress_callback = [&](size_t processed, size_t total) {
    ++call_count;
    last_processed = processed;
    reported_total = total;
    return true; // continue parsing
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_GT(call_count, 0);       // Callback was called at least once
  EXPECT_EQ(reported_total, len); // Total matches buffer size
  EXPECT_EQ(last_processed, len); // Final call reports 100%
}

// Test: Progress callback receives correct total size
TEST_F(ProgressCallbackTest, CorrectTotalSize) {
  std::string csv = "name,age\n";
  for (int i = 0; i < 100; ++i) {
    csv += "Person" + std::to_string(i) + "," + std::to_string(20 + i) + "\n";
  }

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  size_t reported_total = 0;

  libvroom::ParseOptions opts;
  opts.progress_callback = [&](size_t, size_t total) {
    reported_total = total;
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_EQ(reported_total, len);
}

// Test: Progress callback can cancel parsing
TEST_F(ProgressCallbackTest, CancellationSupport) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  int call_count = 0;

  libvroom::ParseOptions opts;
  opts.progress_callback = [&](size_t, size_t) {
    ++call_count;
    return false; // Cancel after first callback
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_FALSE(result.success()); // Parsing was cancelled
  EXPECT_EQ(call_count, 1);       // Callback was called once before cancellation
}

// Test: Progress callback works with different dialects
TEST_F(ProgressCallbackTest, WorksWithDifferentDialects) {
  auto [data, len] = make_buffer("a;b;c\n1;2;3\n4;5;6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  bool callback_called = false;

  libvroom::ParseOptions opts;
  opts.dialect = libvroom::Dialect::semicolon();
  opts.progress_callback = [&](size_t, size_t) {
    callback_called = true;
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(callback_called);
}

// Test: Progress callback works with auto-detection
TEST_F(ProgressCallbackTest, WorksWithAutoDetection) {
  auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  bool callback_called = false;

  libvroom::ParseOptions opts;
  // No dialect set - auto-detection will be used
  opts.progress_callback = [&](size_t, size_t) {
    callback_called = true;
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(result.dialect.delimiter, ';'); // Should auto-detect semicolon
}

// Test: Progress callback works with error collection
TEST_F(ProgressCallbackTest, WorksWithErrorCollection) {
  // CSV with inconsistent field count
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  bool callback_called = false;
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);

  libvroom::ParseOptions opts;
  opts.errors = &errors;
  opts.progress_callback = [&](size_t, size_t) {
    callback_called = true;
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(callback_called);
  EXPECT_TRUE(errors.has_errors());
}

// Test: Progress callback with null callback is ignored
TEST_F(ProgressCallbackTest, NullCallbackIsIgnored) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  libvroom::ParseOptions opts;
  opts.progress_callback = nullptr;

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
}

// Test: ParseOptions::with_progress() factory
TEST_F(ProgressCallbackTest, WithProgressFactory) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  bool callback_called = false;

  auto result = parser.parse(buffer.data(), buffer.size(),
                             libvroom::ParseOptions::with_progress([&](size_t, size_t) {
                               callback_called = true;
                               return true;
                             }));

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(callback_called);
}

// Test: Progress reports monotonically increasing values
TEST_F(ProgressCallbackTest, MonotonicallyIncreasing) {
  std::string csv = "name,age\n";
  for (int i = 0; i < 50; ++i) {
    csv += "Person" + std::to_string(i) + "," + std::to_string(20 + i) + "\n";
  }

  auto [data, len] = make_buffer(csv);
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  std::vector<size_t> progress_values;

  libvroom::ParseOptions opts;
  opts.progress_callback = [&](size_t processed, size_t) {
    progress_values.push_back(processed);
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_GT(progress_values.size(), 0);

  // Values should be monotonically non-decreasing
  for (size_t i = 1; i < progress_values.size(); ++i) {
    EXPECT_GE(progress_values[i], progress_values[i - 1]);
  }
}

// Test: Progress callback with single-threaded parser
TEST_F(ProgressCallbackTest, SingleThreaded) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser(1); // Single thread

  int call_count = 0;

  libvroom::ParseOptions opts;
  opts.progress_callback = [&](size_t, size_t) {
    ++call_count;
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_GT(call_count, 0);
}

// Test: Progress callback with multi-threaded parser
TEST_F(ProgressCallbackTest, MultiThreaded) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser(4); // Multiple threads

  int call_count = 0;

  libvroom::ParseOptions opts;
  opts.progress_callback = [&](size_t, size_t) {
    ++call_count;
    return true;
  };

  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_GT(call_count, 0);
}
