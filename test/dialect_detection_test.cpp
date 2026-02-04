#include "libvroom.h"
#include "libvroom/common_defs.h"
#include "libvroom/dialect.h"
#include "libvroom/io_util.h"

// Note: two_pass.h is not available in the v2 migration
// TwoPass-based tests are disabled below with #if 0 blocks

#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

// ============================================================================
// DIALECT DETECTION TESTS
// ============================================================================
//
// Branch Coverage Strategy
// ------------------------
// These tests provide comprehensive coverage of the dialect detection system,
// organized into logical sections that exercise all major code paths:
//
// 1. DELIMITER DETECTION (5 tests)
//    Tests detection of comma, semicolon, tab, pipe delimiters.
//    Exercises: generate_candidates(), score_dialect(), compute_pattern_score()
//
// 2. EMBEDDED SEPARATOR HANDLING (1 test)
//    Ensures quoted delimiters don't fool detection.
//    Exercises: find_rows(), extract_fields() quote-aware parsing
//
// 3. QUOTE CHARACTER DETECTION (1 test)
//    Tests double-quote recognition.
//    Exercises: quote_char candidate scoring in score_dialect()
//
// 4. HEADER DETECTION (2 tests)
//    Tests header presence/absence detection.
//    Exercises: detect_header() string vs typed data heuristics
//
// 5. LINE ENDING DETECTION (3 tests)
//    Tests LF, CRLF, CR recognition.
//    Exercises: detect_line_ending() for all three styles
//
// 6. CELL TYPE INFERENCE (8 tests)
//    Tests type recognition: integer, float, boolean, date, time, datetime,
//    empty, string.
//    Exercises: infer_cell_type() for each CellType enum value
//
// 7. TYPE SCORE VALIDATION (6 tests)
//    Tests type_score numerical thresholds for typed data.
//    Exercises: compute_type_score() with homogeneous type data
//
// 8. DIALECT FACTORIES & UTILITIES (3 tests)
//    Tests factory methods, equality, to_string().
//    Exercises: Dialect struct methods
//
// 9. EDGE CASES (5 tests)
//    Tests empty file, single cell, non-existent file, null buffer, zero length.
//    Exercises: error handling and boundary conditions
//
// 10. BUFFER DETECTION (2 tests)
//     Tests in-memory detection (vs file-based).
//     Exercises: detect() buffer overloads
//
// 11. CUSTOM DETECTION OPTIONS (1 test)
//     Tests custom delimiter sets.
//     Exercises: DetectionOptions handling
//
// 12. REAL-WORLD FILES (2 tests)
//     Tests detection on realistic data (financial, contacts).
//     Exercises: end-to-end detection quality
//
// 13. PARSER INTEGRATION (8 tests)
//     Tests parse_auto, parse with explicit dialect, parse_two_pass variants.
//     Exercises: two_pass parser dialect integration
//
// 14. DIALECT VALIDATION (4 tests)
//     Tests valid/invalid dialect configurations.
//     Exercises: is_valid(), validate() boundary checks
//
// 15. ESCAPE SEQUENCE DETECTION (8 tests)
//     Tests backslash vs double-quote escape detection.
//     Exercises: escape_char scoring and double_quote inference
//
// Total: 128 tests covering delimiter, quote, escape, header, line ending,
// type inference, scoring, validation, and parser integration paths.
// ============================================================================

class DialectDetectionTest : public ::testing::Test {
protected:
  std::string getTestDataPath(const std::string& category, const std::string& filename) {
    return "test/data/" + category + "/" + filename;
  }

  // Create a padded buffer from a string for SIMD-safe parsing
  std::vector<uint8_t> makeBuffer(const std::string& content) {
    std::vector<uint8_t> buf(content.size() + LIBVROOM_PADDING);
    std::memcpy(buf.data(), content.data(), content.size());
    std::memset(buf.data() + content.size(), 0, LIBVROOM_PADDING);
    return buf;
  }

  libvroom::DialectDetector detector;
};

// ============================================================================
// Delimiter Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectCommaDelimiter) {
  std::string path = getTestDataPath("basic", "simple.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for simple.csv";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
  EXPECT_EQ(result.detected_columns, 3) << "simple.csv has 3 columns";
}

TEST_F(DialectDetectionTest, DetectSemicolonDelimiter) {
  std::string path = getTestDataPath("separators", "semicolon.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for semicolon.csv";
  EXPECT_EQ(result.dialect.delimiter, ';') << "Should detect semicolon delimiter";
  EXPECT_EQ(result.detected_columns, 3) << "semicolon.csv has 3 columns";
}

TEST_F(DialectDetectionTest, DetectTabDelimiter) {
  std::string path = getTestDataPath("separators", "tab.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for tab.csv";
  EXPECT_EQ(result.dialect.delimiter, '\t') << "Should detect tab delimiter";
  EXPECT_EQ(result.detected_columns, 3) << "tab.csv has 3 columns";
}

TEST_F(DialectDetectionTest, DetectPipeDelimiter) {
  std::string path = getTestDataPath("separators", "pipe.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for pipe.csv";
  EXPECT_EQ(result.dialect.delimiter, '|') << "Should detect pipe delimiter";
  EXPECT_EQ(result.detected_columns, 3) << "pipe.csv has 3 columns";
}

// ============================================================================
// Embedded Separator Tests (should not be fooled by quoted delimiters)
// ============================================================================

TEST_F(DialectDetectionTest, NotFooledByQuotedCommas) {
  std::string path = getTestDataPath("quoted", "embedded_separators.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for embedded_separators.csv";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma, not be fooled by quoted commas";
  EXPECT_EQ(result.detected_columns, 3) << "embedded_separators.csv has 3 columns";
}

// ============================================================================
// Quote Character Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectDoubleQuote) {
  std::string path = getTestDataPath("quoted", "quoted_fields.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for quoted_fields.csv";
  EXPECT_EQ(result.dialect.quote_char, '"') << "Should detect double-quote character";
}

// ============================================================================
// Header Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectsHeaderInSimpleCSV) {
  std::string path = getTestDataPath("basic", "simple.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed";
  EXPECT_TRUE(result.has_header) << "simple.csv has a header row (A,B,C)";
}

TEST_F(DialectDetectionTest, DetectsNoHeaderWhenExplicitlyNone) {
  std::string path = getTestDataPath("basic", "simple_no_header.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed";
  // File contains only numeric data rows, so should not detect header
  EXPECT_FALSE(result.has_header) << "simple_no_header.csv has no header";
}

// ============================================================================
// Line Ending Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectLFLineEnding) {
  std::string path = getTestDataPath("line_endings", "lf.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for lf.csv";
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::LF);
}

TEST_F(DialectDetectionTest, DetectCRLFLineEnding) {
  std::string path = getTestDataPath("line_endings", "crlf.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for crlf.csv";
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::CRLF);
}

TEST_F(DialectDetectionTest, DetectCRLineEnding) {
  std::string path = getTestDataPath("line_endings", "cr.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for cr.csv";
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::CR);
}

// ============================================================================
// Cell Type Inference Tests
// ============================================================================

TEST_F(DialectDetectionTest, InferIntegerType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("123"), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("-456"), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("+789"), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("0"), CellType::INTEGER);
}

TEST_F(DialectDetectionTest, InferFloatType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("3.14"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("-2.718"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1e10"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1.5E-3"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type(".5"), CellType::FLOAT);
}

TEST_F(DialectDetectionTest, InferBooleanType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("true"), CellType::BOOLEAN);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("false"), CellType::BOOLEAN);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("TRUE"), CellType::BOOLEAN);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("FALSE"), CellType::BOOLEAN);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("True"), CellType::BOOLEAN);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("False"), CellType::BOOLEAN);
}

TEST_F(DialectDetectionTest, InferDateType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15"), CellType::DATE);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024/01/15"), CellType::DATE);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("15-01-2024"), CellType::DATE);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("15/01/2024"), CellType::DATE);
}

TEST_F(DialectDetectionTest, InferTimeType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("14:30"), CellType::TIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("14:30:59"), CellType::TIME);
}

TEST_F(DialectDetectionTest, InferDateTimeType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15T14:30:00"), CellType::DATETIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15 14:30:00"), CellType::DATETIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15T14:30:00Z"), CellType::DATETIME);
}

TEST_F(DialectDetectionTest, InferEmptyType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type(""), CellType::EMPTY);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("   "), CellType::EMPTY);
}

TEST_F(DialectDetectionTest, InferStringType) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("hello"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("John Doe"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("123abc"), CellType::STRING);
}

// ============================================================================
// Dialect Factory Tests
// ============================================================================

TEST_F(DialectDetectionTest, DialectFactories) {
  auto csv = libvroom::Dialect::csv();
  EXPECT_EQ(csv.delimiter, ',');
  EXPECT_EQ(csv.quote_char, '"');
  EXPECT_TRUE(csv.double_quote);

  auto tsv = libvroom::Dialect::tsv();
  EXPECT_EQ(tsv.delimiter, '\t');
  EXPECT_EQ(tsv.quote_char, '"');

  auto semicolon = libvroom::Dialect::semicolon();
  EXPECT_EQ(semicolon.delimiter, ';');

  auto pipe = libvroom::Dialect::pipe();
  EXPECT_EQ(pipe.delimiter, '|');
}

TEST_F(DialectDetectionTest, DialectEquality) {
  auto d1 = libvroom::Dialect::csv();
  auto d2 = libvroom::Dialect::csv();
  auto d3 = libvroom::Dialect::tsv();

  EXPECT_EQ(d1, d2);
  EXPECT_NE(d1, d3);
}

TEST_F(DialectDetectionTest, DialectToString) {
  auto csv = libvroom::Dialect::csv();
  std::string str = csv.to_string();

  EXPECT_NE(str.find("','"), std::string::npos) << "Should contain comma repr";
  EXPECT_NE(str.find("Dialect"), std::string::npos) << "Should contain 'Dialect'";
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, EmptyFile) {
  std::string path = getTestDataPath("edge_cases", "empty_file.csv");
  auto result = detector.detect_file(path);

  EXPECT_FALSE(result.success()) << "Detection should fail for empty file";
  EXPECT_FALSE(result.warning.empty()) << "Should have a warning";
}

TEST_F(DialectDetectionTest, SingleCell) {
  std::string path = getTestDataPath("edge_cases", "single_cell.csv");
  auto result = detector.detect_file(path);

  // Single cell doesn't meet min_rows requirement by default
  // Detection may or may not succeed depending on content
  // Just verify it doesn't crash
  EXPECT_NO_FATAL_FAILURE(detector.detect_file(path));
}

TEST_F(DialectDetectionTest, NonExistentFile) {
  auto result = detector.detect_file("nonexistent.csv");

  EXPECT_FALSE(result.success()) << "Detection should fail for non-existent file";
  EXPECT_NE(result.warning.find("Could not open"), std::string::npos)
      << "Should warn about file not found";
}

// ============================================================================
// Detection from Memory Buffer
// ============================================================================

TEST_F(DialectDetectionTest, DetectFromBuffer) {
  const std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Detection should succeed for in-memory CSV";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, DetectSemicolonFromBuffer) {
  const std::string csv_data = "a;b;c\n1;2;3\n4;5;6\n7;8;9\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Detection should succeed for semicolon-separated data";
  EXPECT_EQ(result.dialect.delimiter, ';');
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, NullBuffer) {
  auto result = detector.detect(nullptr, 100);

  EXPECT_FALSE(result.success()) << "Detection should fail for null buffer";
  EXPECT_FALSE(result.warning.empty());
}

TEST_F(DialectDetectionTest, ZeroLength) {
  uint8_t buf[1] = {0};
  auto result = detector.detect(buf, 0);

  EXPECT_FALSE(result.success()) << "Detection should fail for zero-length buffer";
}

// ============================================================================
// Custom Detection Options
// ============================================================================

TEST_F(DialectDetectionTest, CustomDelimiters) {
  libvroom::DetectionOptions opts;
  opts.delimiters = {'#'}; // Only test hash

  libvroom::DialectDetector custom_detector(opts);

  const std::string csv_data = "a#b#c\n1#2#3\n4#5#6\n7#8#9\n";
  auto result =
      custom_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, '#');
}

// ============================================================================
// Real-World File Tests
// ============================================================================

TEST_F(DialectDetectionTest, RealWorldFinancial) {
  std::string path = getTestDataPath("real_world", "financial.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for financial.csv";
  EXPECT_EQ(result.dialect.delimiter, ',');
}

TEST_F(DialectDetectionTest, RealWorldContacts) {
  std::string path = getTestDataPath("real_world", "contacts.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for contacts.csv";
  EXPECT_EQ(result.dialect.delimiter, ',');
}

// ============================================================================
// Parser Integration Tests
// ============================================================================
// NOTE: These tests are disabled because they depend on TwoPass API which is
// not yet ported to the v2 architecture. Enable these when TwoPass is available.

#if 0  // TwoPass-dependent tests disabled
TEST_F(DialectDetectionTest, ParseAutoWithCommaCSV) {
  std::string path = getTestDataPath("basic", "simple.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::DetectionResult detected;

  bool success = parser.parse_auto(buffer.data(), idx, buffer.size, errors, &detected);

  EXPECT_TRUE(success) << "parse_auto should succeed for simple.csv";
  EXPECT_TRUE(detected.success()) << "Detection should succeed";
  EXPECT_EQ(detected.dialect.delimiter, ',');
  EXPECT_EQ(detected.detected_columns, 3);
  EXPECT_EQ(errors.error_count(), 0) << "Should have no errors for valid CSV";
}

TEST_F(DialectDetectionTest, ParseAutoWithSemicolonCSV) {
  std::string path = getTestDataPath("separators", "semicolon.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::DetectionResult detected;

  bool success = parser.parse_auto(buffer.data(), idx, buffer.size, errors, &detected);

  EXPECT_TRUE(success) << "parse_auto should succeed";
  EXPECT_TRUE(detected.success()) << "Detection should succeed";
  EXPECT_EQ(detected.dialect.delimiter, ';') << "Should detect semicolon";

  // Parser now uses detected dialect - verify it parsed correctly
  // by checking the number of fields found (should match detected_columns)
  size_t total_fields = 0;
  for (int t = 0; t < idx.n_threads; ++t) {
    total_fields += idx.n_indexes[t];
  }
  // Should have found field separators with the semicolon delimiter
  EXPECT_GT(total_fields, 0) << "Should find field separators with detected dialect";
  EXPECT_EQ(detected.detected_columns, 3) << "Should detect 3 columns";
}

TEST_F(DialectDetectionTest, DetectDialectStatic) {
  const std::string csv_data = "a;b;c\n1;2;3\n4;5;6\n7;8;9\n";
  auto result = libvroom::TwoPass::detect_dialect(reinterpret_cast<const uint8_t*>(csv_data.data()),
                                                  csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ';');
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, DetectDialectWithOptions) {
  const std::string csv_data = "a#b#c\n1#2#3\n4#5#6\n7#8#9\n";

  libvroom::DetectionOptions opts;
  opts.delimiters = {'#'};

  auto result = libvroom::TwoPass::detect_dialect(reinterpret_cast<const uint8_t*>(csv_data.data()),
                                                  csv_data.size(), opts);

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, '#');
}

// ============================================================================
// Dialect-Aware Parsing Tests
// ============================================================================

TEST_F(DialectDetectionTest, ParseWithTSVDialect) {
  std::string path = getTestDataPath("separators", "tab.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::Dialect tsv = libvroom::Dialect::tsv();

  bool success = parser.parse(buffer.data(), idx, buffer.size, tsv);

  EXPECT_TRUE(success) << "Should parse TSV successfully";
  EXPECT_GT(idx.n_indexes[0], 0) << "Should find tab separators";
}

TEST_F(DialectDetectionTest, ParseWithSemicolonDialect) {
  std::string path = getTestDataPath("separators", "semicolon.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::Dialect semicolon = libvroom::Dialect::semicolon();

  bool success = parser.parse(buffer.data(), idx, buffer.size, semicolon);

  EXPECT_TRUE(success) << "Should parse semicolon-separated successfully";
  EXPECT_GT(idx.n_indexes[0], 0) << "Should find semicolon separators";
}

TEST_F(DialectDetectionTest, ParseWithPipeDialect) {
  std::string path = getTestDataPath("separators", "pipe.csv");
  auto buffer = libvroom::load_file_to_ptr(path, LIBVROOM_PADDING);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(buffer.size, 1);
  libvroom::Dialect pipe = libvroom::Dialect::pipe();

  bool success = parser.parse(buffer.data(), idx, buffer.size, pipe);

  EXPECT_TRUE(success) << "Should parse pipe-separated successfully";
  EXPECT_GT(idx.n_indexes[0], 0) << "Should find pipe separators";
}

TEST_F(DialectDetectionTest, ParseWithErrorsDialect) {
  // Test parse_with_errors with semicolon dialect
  const std::string csv_data = "name;age;city\nAlice;30;Paris\nBob;25;London\n";
  auto buf = makeBuffer(csv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(csv_data.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::Dialect semicolon = libvroom::Dialect::semicolon();

  bool success = parser.parse_with_errors(buf.data(), idx, csv_data.size(), errors, semicolon);

  EXPECT_TRUE(success) << "Should parse successfully";
  EXPECT_EQ(errors.error_count(), 0) << "Should have no errors";
}

TEST_F(DialectDetectionTest, ParseValidateDialect) {
  // Test parse_validate with tab dialect
  const std::string tsv_data = "name\tage\tcity\nAlice\t30\tParis\nBob\t25\tLondon\n";
  auto buf = makeBuffer(tsv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(tsv_data.size(), 1);
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::Dialect tsv = libvroom::Dialect::tsv();

  bool success = parser.parse_validate(buf.data(), idx, tsv_data.size(), errors, tsv);

  EXPECT_TRUE(success) << "Validation should pass";
  EXPECT_EQ(errors.error_count(), 0) << "Should have no validation errors";
}

TEST_F(DialectDetectionTest, ParseWithSingleQuote) {
  // Test parsing with single-quote as quote character
  const std::string csv_data = "name,description\nAlice,'Hello, World'\nBob,'Test \"quote\"'\n";
  auto buf = makeBuffer(csv_data);

  libvroom::Dialect single_quote;
  single_quote.delimiter = ',';
  single_quote.quote_char = '\'';

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(csv_data.size(), 1);

  bool success = parser.parse(buf.data(), idx, csv_data.size(), single_quote);

  EXPECT_TRUE(success) << "Should parse successfully with single-quote";
}

TEST_F(DialectDetectionTest, ParseTwoPassWithErrorsDialect) {
  // Test parse_two_pass_with_errors with semicolon dialect
  const std::string csv_data = "name;age;city\nAlice;30;Paris\nBob;25;London\nCharlie;35;Berlin\n";
  auto buf = makeBuffer(csv_data);

  libvroom::TwoPass parser;
  libvroom::ParseIndex idx = parser.init(csv_data.size(), 2); // 2 threads
  libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
  libvroom::Dialect semicolon = libvroom::Dialect::semicolon();

  bool success =
      parser.parse_two_pass_with_errors(buf.data(), idx, csv_data.size(), errors, semicolon);

  EXPECT_TRUE(success) << "Should parse successfully with multi-threading";
  EXPECT_EQ(errors.error_count(), 0) << "Should have no errors";
}
#endif // TwoPass-dependent tests disabled

// ============================================================================
// Dialect Validation Tests
// ============================================================================

TEST_F(DialectDetectionTest, DialectValidation_Valid) {
  libvroom::Dialect csv = libvroom::Dialect::csv();
  EXPECT_TRUE(csv.is_valid()) << "Standard CSV should be valid";

  libvroom::Dialect tsv = libvroom::Dialect::tsv();
  EXPECT_TRUE(tsv.is_valid()) << "TSV should be valid";

  libvroom::Dialect semicolon = libvroom::Dialect::semicolon();
  EXPECT_TRUE(semicolon.is_valid()) << "Semicolon-separated should be valid";

  libvroom::Dialect pipe = libvroom::Dialect::pipe();
  EXPECT_TRUE(pipe.is_valid()) << "Pipe-separated should be valid";
}

TEST_F(DialectDetectionTest, DialectValidation_SameDelimiterAndQuote) {
  libvroom::Dialect invalid;
  invalid.delimiter = '"';
  invalid.quote_char = '"';
  EXPECT_FALSE(invalid.is_valid()) << "Same delimiter and quote should be invalid";

  EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

TEST_F(DialectDetectionTest, DialectValidation_NewlineDelimiter) {
  libvroom::Dialect invalid;
  invalid.delimiter = '\n';
  invalid.quote_char = '"';
  EXPECT_FALSE(invalid.is_valid()) << "Newline delimiter should be invalid";

  EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

TEST_F(DialectDetectionTest, DialectValidation_NewlineQuote) {
  libvroom::Dialect invalid;
  invalid.delimiter = ',';
  invalid.quote_char = '\n';
  EXPECT_FALSE(invalid.is_valid()) << "Newline quote should be invalid";

  EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

// ============================================================================
// Escape Sequence Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectBackslashEscape) {
  // CSV with backslash-escaped quotes: \"
  const std::string csv_data = "Name,Value\n"
                               "\"John \\\"Boss\\\" Smith\",100\n"
                               "\"Jane Doe\",200\n"
                               "\"Bob\",300\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Detection should succeed for backslash-escaped CSV";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.dialect.quote_char, '"');
  // Should detect backslash escape, not double-quote
  EXPECT_EQ(result.dialect.escape_char, '\\');
  EXPECT_FALSE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, DetectDoubleQuoteEscape) {
  // Standard RFC 4180 CSV with "" escaping
  const std::string csv_data = "Name,Value\n"
                               "\"John \"\"Boss\"\" Smith\",100\n"
                               "\"Jane Doe\",200\n"
                               "\"Bob\",300\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Detection should succeed for double-quote escaped CSV";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.dialect.quote_char, '"');
  EXPECT_TRUE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, BackslashEscapedDelimiter) {
  // CSV with backslash-escaped delimiter
  const std::string csv_data = "Name,Description\n"
                               "\"Item A\",\"Has \\, comma\"\n"
                               "\"Item B\",\"Normal text\"\n"
                               "\"Item C\",\"More text\"\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 2);
}

TEST_F(DialectDetectionTest, EscapeCharOptions) {
  // Test with custom escape character options
  libvroom::DetectionOptions opts;
  opts.escape_chars = {'\\', '~'}; // Test backslash and tilde

  libvroom::DialectDetector custom_detector(opts);

  const std::string csv_data = "A,B\n"
                               "\"X \\\" Y\",1\n"
                               "\"Z\",2\n"
                               "\"W\",3\n";

  auto result =
      custom_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.escape_char, '\\');
}

TEST_F(DialectDetectionTest, NoEscapeNeeded) {
  // Simple CSV without any escape sequences
  const std::string csv_data = "Name,Value\n"
                               "John,100\n"
                               "Jane,200\n"
                               "Bob,300\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  // Should default to double-quote style when no escapes are present
  EXPECT_TRUE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, MixedEscapeStyles) {
  // CSV with both \" and "" patterns - should be ambiguous
  // The tie-breaker should prefer RFC 4180 (double_quote = true)
  const std::string csv_data = "Name,Value\n"
                               "\"John \\\"Boss\\\" Smith\",100\n"
                               "\"Jane \"\"Doe\"\" Jones\",200\n"
                               "\"Bob\",300\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  // When mixed, tie-breakers prefer RFC 4180
  EXPECT_TRUE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, EscapeInMiddleOfField) {
  // Test escape character appearing in the middle of field content
  const std::string csv_data = "Name,Description\n"
                               "\"Test\",\"Hello \\\"World\\\" Here\"\n"
                               "\"Item\",\"Normal\"\n"
                               "\"Other\",\"Text\"\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.dialect.escape_char, '\\');
  EXPECT_FALSE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, ConsecutiveEscapes) {
  // Test multiple consecutive escape sequences
  // Each row has backslash-escaped quotes to ensure clear signal
  const std::string csv_data = "A,B\n"
                               "\"First \\\"One\\\" here\",1\n"
                               "\"Second \\\"Two\\\" here\",2\n"
                               "\"Third \\\"Three\\\" here\",3\n"
                               "\"Fourth \\\"Four\\\" here\",4\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.escape_char, '\\');
  EXPECT_FALSE(result.dialect.double_quote);
}

// ============================================================================
// Additional Branch Coverage Tests - Delimiter Detection
// ============================================================================

TEST_F(DialectDetectionTest, DetectColonDelimiter) {
  // Test colon delimiter detection
  const std::string csv_data = "a:b:c\n1:2:3\n4:5:6\n7:8:9\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ':');
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, AmbiguousDelimiterSimilarScores) {
  // Create data where multiple delimiters could work, testing the ambiguity warning
  // Use data that scores similarly for multiple delimiters
  const std::string csv_data = "a,b;c\n1,2;3\n4,5;6\n7,8;9\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Detection should succeed - the tie-breaking rules will choose one delimiter
  // The data is ambiguous (both comma and semicolon give consistent 2-column results)
  // so a warning may be present. Either way, detection should work.
  EXPECT_TRUE(result.success());
}

TEST_F(DialectDetectionTest, SingleColumnData) {
  // Single column CSV - each delimiter gives 1 column
  const std::string csv_data = "value\n100\n200\n300\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should still detect something, likely comma with 1 column
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.detected_columns, 1);
}

// ============================================================================
// Additional Branch Coverage Tests - Quote Character Detection
// ============================================================================

TEST_F(DialectDetectionTest, DetectSingleQuoteCharacter) {
  // CSV with single quotes containing embedded commas
  // The embedded delimiters force single quote detection since double quotes
  // would produce inconsistent column counts
  const std::string csv_data = "name,value\n"
                               "'Alice, Jr.',100\n"
                               "'Bob, Sr.',200\n"
                               "'Charlie, III',300\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.dialect.quote_char, '\'');
}

TEST_F(DialectDetectionTest, SingleQuoteWithEmbeddedComma) {
  // Single quotes with embedded delimiter
  const std::string csv_data = "name,description\n"
                               "'Alice','Hello, World'\n"
                               "'Bob','Test, data'\n"
                               "'Charlie','More, commas'\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.dialect.quote_char, '\'');
  EXPECT_EQ(result.detected_columns, 2);
}

TEST_F(DialectDetectionTest, NoQuoteCharacter) {
  // Simple data without any quotes - tests that detection works without quote evidence
  const std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 3);
  // Quote char defaults to double quote per RFC 4180 preference, even without evidence
  EXPECT_EQ(result.dialect.quote_char, '"');
}

// ============================================================================
// Additional Branch Coverage Tests - Line Ending Detection
// ============================================================================

TEST_F(DialectDetectionTest, DetectMixedLineEndings) {
  // Create data with mixed line endings (LF and CRLF)
  const std::string csv_data = "a,b,c\n1,2,3\r\n4,5,6\n7,8,9\r\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::MIXED);
}

TEST_F(DialectDetectionTest, DetectMixedLineEndingsWithCR) {
  // Mixed with CR (old Mac) and LF
  const std::string csv_data = "a,b,c\r1,2,3\n4,5,6\r7,8,9\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::MIXED);
}

TEST_F(DialectDetectionTest, DetectUnknownLineEnding) {
  // Data with no newlines at all
  const std::string csv_data = "a,b,c";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // May not have enough rows, but should detect UNKNOWN line ending
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::UNKNOWN);
}

// ============================================================================
// Additional Branch Coverage Tests - Header Detection
// ============================================================================

TEST_F(DialectDetectionTest, HeaderDetectionAllStrings) {
  // Both header and data are all strings
  const std::string csv_data = "name,city,country\n"
                               "Alice,Paris,France\n"
                               "Bob,London,UK\n"
                               "Charlie,Berlin,Germany\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  // All strings in both header and data - header detection uses special logic
  EXPECT_TRUE(result.has_header);
}

TEST_F(DialectDetectionTest, HeaderDetectionNumericData) {
  // String header with numeric data
  const std::string csv_data = "id,value,count\n"
                               "1,100,10\n"
                               "2,200,20\n"
                               "3,300,30\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(result.has_header);
}

TEST_F(DialectDetectionTest, HeaderDetectionNumericHeader) {
  // Numeric header and numeric data - should not detect header
  const std::string csv_data = "1,2,3\n"
                               "4,5,6\n"
                               "7,8,9\n"
                               "10,11,12\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.has_header);
}

TEST_F(DialectDetectionTest, HeaderDetectionEmptyFirstRow) {
  // Empty first row should not crash
  const std::string csv_data = ",,\n"
                               "1,2,3\n"
                               "4,5,6\n"
                               "7,8,9\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
}

TEST_F(DialectDetectionTest, HeaderDetectionSingleRow) {
  // Only one row - can't detect header
  const std::string csv_data = "name,value,count\n";

  libvroom::DetectionOptions opts;
  opts.min_rows = 1; // Allow single row
  libvroom::DialectDetector single_row_detector(opts);

  auto result = single_row_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()),
                                           csv_data.size());

  // With only one row, header detection returns false (needs at least 2 rows)
  EXPECT_FALSE(result.has_header);
}

// ============================================================================
// Additional Branch Coverage Tests - Field Consistency / Ragged Rows
// ============================================================================

TEST_F(DialectDetectionTest, RaggedRowsDifferentFieldCounts) {
  // Rows with inconsistent field counts
  const std::string csv_data = "a,b,c\n"
                               "1,2,3\n"
                               "4,5\n"
                               "6,7,8,9\n"
                               "10,11,12\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should still detect, using modal field count
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  // Modal count is 3 (appears 3 times: rows 1, 2, 5)
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, AllDifferentFieldCounts) {
  // Every row has different field count - tests handling of highly inconsistent data
  const std::string csv_data = "a\n"
                               "b,c\n"
                               "d,e,f\n"
                               "g,h,i,j\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Detection may or may not succeed with highly inconsistent data
  // The pattern score will be 0.25 (1/4 rows match modal count)
  // Verify delimiter is detected as comma regardless of success
  EXPECT_EQ(result.dialect.delimiter, ',');
}

// ============================================================================
// Additional Branch Coverage Tests - Dialect::to_string()
// ============================================================================

TEST_F(DialectDetectionTest, DialectToStringTab) {
  libvroom::Dialect tsv = libvroom::Dialect::tsv();
  std::string str = tsv.to_string();

  EXPECT_NE(str.find("'\\t'"), std::string::npos) << "Should contain tab representation";
}

TEST_F(DialectDetectionTest, DialectToStringSemicolon) {
  libvroom::Dialect semi = libvroom::Dialect::semicolon();
  std::string str = semi.to_string();

  EXPECT_NE(str.find("';'"), std::string::npos) << "Should contain semicolon";
}

TEST_F(DialectDetectionTest, DialectToStringPipe) {
  libvroom::Dialect pipe = libvroom::Dialect::pipe();
  std::string str = pipe.to_string();

  EXPECT_NE(str.find("'|'"), std::string::npos) << "Should contain pipe";
}

TEST_F(DialectDetectionTest, DialectToStringColon) {
  libvroom::Dialect colon;
  colon.delimiter = ':';
  colon.quote_char = '"';
  std::string str = colon.to_string();

  EXPECT_NE(str.find("':'"), std::string::npos) << "Should contain colon";
}

TEST_F(DialectDetectionTest, DialectToStringSingleQuote) {
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '\'';
  std::string str = d.to_string();

  EXPECT_NE(str.find("\"'\""), std::string::npos) << "Should contain single quote repr";
}

TEST_F(DialectDetectionTest, DialectToStringNoQuote) {
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '\0';
  std::string str = d.to_string();

  EXPECT_NE(str.find("none"), std::string::npos) << "Should contain 'none' for no quote";
}

TEST_F(DialectDetectionTest, DialectToStringBackslashEscape) {
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '"';
  d.escape_char = '\\';
  d.double_quote = false;
  std::string str = d.to_string();

  EXPECT_NE(str.find("backslash"), std::string::npos) << "Should contain 'backslash'";
}

TEST_F(DialectDetectionTest, DialectToStringDoubleQuoteEscape) {
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '"';
  d.double_quote = true;
  std::string str = d.to_string();

  EXPECT_NE(str.find("double"), std::string::npos) << "Should contain 'double'";
}

TEST_F(DialectDetectionTest, DialectToStringOtherEscape) {
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '"';
  d.escape_char = '~';
  d.double_quote = false;
  std::string str = d.to_string();

  EXPECT_NE(str.find("'~'"), std::string::npos) << "Should contain escape char";
}

TEST_F(DialectDetectionTest, DialectToStringOtherDelimiter) {
  // Test an unusual delimiter character
  libvroom::Dialect d;
  d.delimiter = '#';
  d.quote_char = '"';
  std::string str = d.to_string();

  EXPECT_NE(str.find("'#'"), std::string::npos) << "Should contain hash";
}

TEST_F(DialectDetectionTest, DialectToStringOtherQuote) {
  // Test an unusual quote character
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '`';
  std::string str = d.to_string();

  EXPECT_NE(str.find("'`'"), std::string::npos) << "Should contain backtick";
}

// ============================================================================
// Additional Branch Coverage Tests - Detection Warnings
// ============================================================================

TEST_F(DialectDetectionTest, WarningForAmbiguousDialect) {
  // Create data that produces similar scores for multiple dialects
  // Multiple quote/escape combinations will score similarly
  const std::string csv_data = "a,b\n"
                               "1,2\n"
                               "3,4\n"
                               "5,6\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Detection should succeed with this basic CSV
  EXPECT_TRUE(result.success());

  // Verify that candidates were generated and scored
  // The exact warning depends on score distributions, but we verify:
  // 1. Multiple candidates exist (different quote/escape combinations)
  // 2. The best candidate has a reasonable score
  EXPECT_GT(result.candidates.size(), 1u);
  EXPECT_GT(result.candidates[0].consistency_score, 0.5);
}

TEST_F(DialectDetectionTest, NoValidDialectWarning) {
  // Data that doesn't form valid CSV structure
  const std::string csv_data = "x\ny\n"; // Only 2 rows, may not meet min_rows

  libvroom::DetectionOptions opts;
  opts.min_rows = 5; // Require more rows than we have
  libvroom::DialectDetector strict_detector(opts);

  auto result =
      strict_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_FALSE(result.success());
  EXPECT_NE(result.warning.find("Could not detect"), std::string::npos);
}

// ============================================================================
// Additional Branch Coverage Tests - Type Score Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, TypeScoreAllEmpty) {
  // Data with all empty cells
  const std::string csv_data = "a,b,c\n"
                               ",,\n"
                               ",,\n"
                               ",,\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should still detect delimiter
  EXPECT_EQ(result.dialect.delimiter, ',');
}

TEST_F(DialectDetectionTest, TypeScoreAllDates) {
  // Data with date values
  const std::string csv_data = "date1,date2,date3\n"
                               "2024-01-15,2024-02-20,2024-03-25\n"
                               "2024-04-10,2024-05-15,2024-06-20\n"
                               "2024-07-05,2024-08-10,2024-09-15\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(result.has_header);
  ASSERT_FALSE(result.candidates.empty());
  EXPECT_GT(result.candidates[0].type_score, 0.8)
      << "type_score should be high (>0.8) for all-date data";
}

TEST_F(DialectDetectionTest, TypeScoreAllTimes) {
  // Data with time values
  const std::string csv_data = "time1,time2,time3\n"
                               "10:30,11:45,12:00\n"
                               "14:30:00,15:45:30,16:00:00\n"
                               "20:00,21:30,22:45\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  ASSERT_FALSE(result.candidates.empty());
  EXPECT_GT(result.candidates[0].type_score, 0.8)
      << "type_score should be high (>0.8) for all-time data";
}

TEST_F(DialectDetectionTest, TypeScoreDateTimes) {
  // Data with datetime values
  const std::string csv_data = "created,updated\n"
                               "2024-01-15T10:30:00,2024-01-16T11:45:00\n"
                               "2024-02-20T14:30:00Z,2024-02-21T15:45:00Z\n"
                               "2024-03-25 20:00:00,2024-03-26 21:30:00\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(result.has_header);
  ASSERT_FALSE(result.candidates.empty());
  EXPECT_GT(result.candidates[0].type_score, 0.8)
      << "type_score should be high (>0.8) for all-datetime data";
}

TEST_F(DialectDetectionTest, TypeScoreBooleansAndIntegers) {
  // Mixed booleans and integers
  const std::string csv_data = "id,active,count\n"
                               "1,true,100\n"
                               "2,false,200\n"
                               "3,TRUE,300\n"
                               "4,FALSE,400\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(result.has_header);
  ASSERT_FALSE(result.candidates.empty());
  EXPECT_GT(result.candidates[0].type_score, 0.8)
      << "type_score should be high (>0.8) for boolean+integer data";
}

TEST_F(DialectDetectionTest, TypeScoreFloatsWithExponents) {
  // Floats with scientific notation
  const std::string csv_data = "value1,value2,value3\n"
                               "1.5e10,2.5E-5,3.14\n"
                               "-1.23e4,+4.56E7,0.001\n"
                               "1e10,2E20,.5\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  ASSERT_FALSE(result.candidates.empty());
  EXPECT_GT(result.candidates[0].type_score, 0.8)
      << "type_score should be high (>0.8) for all-float data";
}

TEST_F(DialectDetectionTest, TypeScoreMixedTypes) {
  // Mixed string, integer, float, boolean, date
  const std::string csv_data = "name,age,score,active,birthdate\n"
                               "Alice,30,95.5,true,1994-05-15\n"
                               "Bob,25,88.0,false,1999-08-20\n"
                               "Charlie,35,92.3,True,1989-12-10\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.detected_columns, 5);
  ASSERT_FALSE(result.candidates.empty());
  EXPECT_GE(result.candidates[0].type_score, 0.8)
      << "type_score should be high (>=0.8) for mixed typed data";
}

// ============================================================================
// Additional Branch Coverage Tests - infer_cell_type Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, InferCellTypeWhitespace) {
  using CellType = libvroom::DialectDetector::CellType;

  // Whitespace-padded values
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("  123  "), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("\t3.14\t"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("  true  "), CellType::BOOLEAN);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("\n"), CellType::EMPTY);
}

TEST_F(DialectDetectionTest, InferCellTypeDateFormats) {
  using CellType = libvroom::DialectDetector::CellType;

  // Various date formats
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-12-31"), CellType::DATE);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024/12/31"), CellType::DATE);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("31-12-2024"), CellType::DATE);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("31/12/2024"), CellType::DATE);

  // Invalid date-like strings
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-1-5"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("24-12-31"), CellType::STRING);
}

TEST_F(DialectDetectionTest, InferCellTypeTimeFormats) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("00:00"), CellType::TIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("23:59"), CellType::TIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("00:00:00"), CellType::TIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("23:59:59"), CellType::TIME);

  // Invalid time formats
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1:30"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("12:3"), CellType::STRING);
}

TEST_F(DialectDetectionTest, InferCellTypeDateTimeFormats) {
  using CellType = libvroom::DialectDetector::CellType;

  // ISO 8601 datetime
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15T00:00:00"), CellType::DATETIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15T23:59:59"), CellType::DATETIME);

  // With timezone
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15T10:30:00+05:00"),
            CellType::DATETIME);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15T10:30:00-08:00"),
            CellType::DATETIME);

  // Space separator
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("2024-01-15 10:30:00"), CellType::DATETIME);
}

TEST_F(DialectDetectionTest, InferCellTypeIntegerEdgeCases) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("+0"), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("-0"), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("0000"), CellType::INTEGER);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("999999999"), CellType::INTEGER);

  // Not integers
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("+"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("-"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("+-1"), CellType::STRING);
}

TEST_F(DialectDetectionTest, InferCellTypeFloatEdgeCases) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("0.0"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type(".0"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("0."), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("+.5"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("-.5"), CellType::FLOAT);

  // Exponent edge cases
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1e0"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1E+0"), CellType::FLOAT);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1E-0"), CellType::FLOAT);

  // Invalid floats
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1e"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("1E+"), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("."), CellType::STRING);
  EXPECT_EQ(libvroom::DialectDetector::infer_cell_type("..5"), CellType::STRING);
}

// ============================================================================
// Additional Branch Coverage Tests - cell_type_to_string
// ============================================================================

TEST_F(DialectDetectionTest, CellTypeToString) {
  using CellType = libvroom::DialectDetector::CellType;

  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::EMPTY), "EMPTY");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::INTEGER), "INTEGER");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::FLOAT), "FLOAT");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::DATE), "DATE");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::DATETIME), "DATETIME");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::TIME), "TIME");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::BOOLEAN), "BOOLEAN");
  EXPECT_STREQ(libvroom::DialectDetector::cell_type_to_string(CellType::STRING), "STRING");
}

// ============================================================================
// Additional Branch Coverage Tests - Dialect Validation Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, DialectValidation_CarriageReturnDelimiter) {
  libvroom::Dialect invalid;
  invalid.delimiter = '\r';
  invalid.quote_char = '"';
  EXPECT_FALSE(invalid.is_valid()) << "CR delimiter should be invalid";

  EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

TEST_F(DialectDetectionTest, DialectValidation_CarriageReturnQuote) {
  libvroom::Dialect invalid;
  invalid.delimiter = ',';
  invalid.quote_char = '\r';
  EXPECT_FALSE(invalid.is_valid()) << "CR quote should be invalid";

  EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

TEST_F(DialectDetectionTest, DialectValidation_ControlCharDelimiter) {
  libvroom::Dialect invalid;
  invalid.delimiter = '\x01'; // Control character
  invalid.quote_char = '"';
  EXPECT_FALSE(invalid.is_valid()) << "Control char delimiter should be invalid";
}

TEST_F(DialectDetectionTest, DialectValidation_ControlCharQuote) {
  libvroom::Dialect invalid;
  invalid.delimiter = ',';
  invalid.quote_char = '\x1F'; // Control character
  EXPECT_FALSE(invalid.is_valid()) << "Control char quote should be invalid";
}

TEST_F(DialectDetectionTest, DialectValidation_HighByteDelimiter) {
  libvroom::Dialect invalid;
  invalid.delimiter = static_cast<char>(200); // > 126
  invalid.quote_char = '"';
  EXPECT_FALSE(invalid.is_valid()) << "High-byte delimiter should be invalid";
}

// ============================================================================
// Additional Branch Coverage Tests - Pattern Score Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, PatternScoreTooFewRows) {
  // Less than min_rows (when explicitly set higher than available rows)
  const std::string csv_data = "a,b,c\n1,2,3\n";

  libvroom::DetectionOptions opts;
  opts.min_rows = 5;
  libvroom::DialectDetector strict_detector(opts);

  auto result =
      strict_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should fail or have low confidence
  EXPECT_FALSE(result.success());
}

TEST_F(DialectDetectionTest, PatternScoreEmptyRows) {
  // Rows that are empty
  const std::string csv_data = "a,b,c\n"
                               "\n"
                               "1,2,3\n"
                               "\n"
                               "4,5,6\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should handle empty rows gracefully
  EXPECT_EQ(result.dialect.delimiter, ',');
}

TEST_F(DialectDetectionTest, PatternScoreMaxRows) {
  // Create data with many rows to test max_rows limit
  std::string csv_data = "a,b,c\n";
  for (int i = 0; i < 150; i++) {
    csv_data += std::to_string(i) + ",x,y\n";
  }

  libvroom::DetectionOptions opts;
  opts.max_rows = 50;
  libvroom::DialectDetector limited_detector(opts);

  auto result =
      limited_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_LE(result.rows_analyzed, 50u);
}

// ============================================================================
// Additional Branch Coverage Tests - Extract Fields Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, ExtractFieldsEmptyRow) {
  const std::string csv_data = "a,b,c\n"
                               "1,2,3\n"
                               "4,5,6\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
}

TEST_F(DialectDetectionTest, ExtractFieldsQuotedEmpty) {
  // Fields that are quoted but empty
  const std::string csv_data = "a,b,c\n"
                               "\"\",\"\",\"\"\n"
                               "1,2,3\n"
                               "4,5,6\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, ExtractFieldsTrailingDelimiter) {
  // Row ending with delimiter
  const std::string csv_data = "a,b,c,\n"
                               "1,2,3,\n"
                               "4,5,6,\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.detected_columns, 4);
}

// ============================================================================
// Additional Branch Coverage Tests - Candidate Ordering
// ============================================================================

TEST_F(DialectDetectionTest, CandidateTieBreakColumns) {
  // Test that more columns wins in tie-break
  libvroom::DialectCandidate c1, c2;
  c1.consistency_score = 0.8;
  c1.num_columns = 5;
  c1.dialect.quote_char = '"';
  c1.dialect.double_quote = true;
  c1.dialect.delimiter = ',';

  c2.consistency_score = 0.8;
  c2.num_columns = 3;
  c2.dialect.quote_char = '"';
  c2.dialect.double_quote = true;
  c2.dialect.delimiter = ',';

  EXPECT_TRUE(c1 < c2); // c1 has more columns, should be "better" (comes first)
}

TEST_F(DialectDetectionTest, CandidateTieBreakQuoteChar) {
  // Test that double quote wins in tie-break
  libvroom::DialectCandidate c1, c2;
  c1.consistency_score = 0.8;
  c1.num_columns = 3;
  c1.dialect.quote_char = '"';
  c1.dialect.double_quote = true;
  c1.dialect.delimiter = ',';

  c2.consistency_score = 0.8;
  c2.num_columns = 3;
  c2.dialect.quote_char = '\'';
  c2.dialect.double_quote = true;
  c2.dialect.delimiter = ',';

  EXPECT_TRUE(c1 < c2); // c1 has standard quote, should be "better"
}

TEST_F(DialectDetectionTest, CandidateTieBreakDoubleQuote) {
  // Test that double_quote=true wins in tie-break
  libvroom::DialectCandidate c1, c2;
  c1.consistency_score = 0.8;
  c1.num_columns = 3;
  c1.dialect.quote_char = '"';
  c1.dialect.double_quote = true;
  c1.dialect.delimiter = ',';

  c2.consistency_score = 0.8;
  c2.num_columns = 3;
  c2.dialect.quote_char = '"';
  c2.dialect.double_quote = false;
  c2.dialect.delimiter = ',';

  EXPECT_TRUE(c1 < c2);
}

TEST_F(DialectDetectionTest, CandidateTieBreakDelimiter) {
  // Test that comma delimiter wins in tie-break
  libvroom::DialectCandidate c1, c2;
  c1.consistency_score = 0.8;
  c1.num_columns = 3;
  c1.dialect.quote_char = '"';
  c1.dialect.double_quote = true;
  c1.dialect.delimiter = ',';

  c2.consistency_score = 0.8;
  c2.num_columns = 3;
  c2.dialect.quote_char = '"';
  c2.dialect.double_quote = true;
  c2.dialect.delimiter = ';';

  EXPECT_TRUE(c1 < c2);
}

TEST_F(DialectDetectionTest, CandidateEqualScores) {
  // Test completely equal candidates
  libvroom::DialectCandidate c1, c2;
  c1.consistency_score = 0.8;
  c1.num_columns = 3;
  c1.dialect.quote_char = '"';
  c1.dialect.double_quote = true;
  c1.dialect.delimiter = ',';

  c2.consistency_score = 0.8;
  c2.num_columns = 3;
  c2.dialect.quote_char = '"';
  c2.dialect.double_quote = true;
  c2.dialect.delimiter = ',';

  EXPECT_FALSE(c1 < c2);
  EXPECT_FALSE(c2 < c1);
}

// ============================================================================
// Additional Branch Coverage Tests - Generate Candidates
// ============================================================================

TEST_F(DialectDetectionTest, GenerateCandidatesCustomOptions) {
  libvroom::DetectionOptions opts;
  opts.delimiters = {','};
  opts.quote_chars = {'"'};
  opts.escape_chars = {}; // No escape chars beyond double-quote

  libvroom::DialectDetector custom_detector(opts);

  const std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  auto result =
      custom_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  // Should have limited candidates
  EXPECT_LT(result.candidates.size(), 20u);
}

TEST_F(DialectDetectionTest, GenerateCandidatesMultipleEscapes) {
  libvroom::DetectionOptions opts;
  opts.delimiters = {','};
  opts.quote_chars = {'"'};
  opts.escape_chars = {'\\', '~', '^'};

  libvroom::DialectDetector custom_detector(opts);

  const std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  auto result =
      custom_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
}

// ============================================================================
// Additional Branch Coverage Tests - CRLF Handling in Rows
// ============================================================================

TEST_F(DialectDetectionTest, FindRowsCRLFProper) {
  // Proper CRLF line endings
  const std::string csv_data = "a,b,c\r\n1,2,3\r\n4,5,6\r\n7,8,9\r\n";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::CRLF);
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, FindRowsCROnly) {
  // CR-only line endings (old Mac)
  const std::string csv_data = "a,b,c\r1,2,3\r4,5,6\r7,8,9\r";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::CR);
}

TEST_F(DialectDetectionTest, FindRowsCRAtEndOfBuffer) {
  // CR at very end of buffer (edge case)
  const std::string csv_data = "a,b,c\n1,2,3\n4,5,6\r";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
}

TEST_F(DialectDetectionTest, FindRowsNoTrailingNewline) {
  // No trailing newline
  const std::string csv_data = "a,b,c\n1,2,3\n4,5,6";
  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.detected_columns, 3);
}

// ============================================================================
// Additional Branch Coverage Tests - Quoted Fields with Special Characters
// ============================================================================

TEST_F(DialectDetectionTest, QuotedFieldsWithNewlines) {
  // Newlines inside quoted fields
  const std::string csv_data = "name,description\n"
                               "\"Alice\",\"Line 1\nLine 2\"\n"
                               "\"Bob\",\"Single line\"\n"
                               "\"Charlie\",\"More\nlines\nhere\"\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 2);
}

TEST_F(DialectDetectionTest, QuotedFieldsWithCRLF) {
  // CRLF inside quoted fields
  const std::string csv_data = "name,description\r\n"
                               "\"Alice\",\"Line 1\r\nLine 2\"\r\n"
                               "\"Bob\",\"Single line\"\r\n"
                               "\"Charlie\",\"Normal\"\r\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
}

TEST_F(DialectDetectionTest, QuotedFieldsWithDelimiter) {
  // Delimiter inside quoted fields
  const std::string csv_data = "name,description\n"
                               "\"Alice\",\"Hello, World\"\n"
                               "\"Bob\",\"Test, data, here\"\n"
                               "\"Charlie\",\"Normal text\"\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.detected_columns, 2);
}

// ============================================================================
// Additional Branch Coverage Tests - Sample Size Limit
// ============================================================================

TEST_F(DialectDetectionTest, SampleSizeLimit) {
  // Create data larger than sample size
  std::string csv_data = "a,b,c\n";
  for (int i = 0; i < 1000; i++) {
    csv_data += std::to_string(i) + ",data,value\n";
  }

  libvroom::DetectionOptions opts;
  opts.sample_size = 1024; // Only sample 1KB
  libvroom::DialectDetector limited_detector(opts);

  auto result =
      limited_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  // Should detect correctly even with limited sample
  EXPECT_EQ(result.dialect.delimiter, ',');
}

// ============================================================================
// Additional Branch Coverage Tests - Escape Pattern in find_rows
// ============================================================================

TEST_F(DialectDetectionTest, EscapeCharInFindRows) {
  // Backslash escape affecting row boundaries
  libvroom::Dialect d;
  d.delimiter = ',';
  d.quote_char = '"';
  d.escape_char = '\\';
  d.double_quote = false;

  const std::string csv_data = "a,b\n"
                               "\"line with \\\" quote\",1\n"
                               "\"normal\",2\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
}

TEST_F(DialectDetectionTest, DoubleQuoteEscapeInFindRows) {
  // Double quote escape affecting row boundaries
  const std::string csv_data = "a,b\n"
                               "\"line with \"\" quote\",1\n"
                               "\"normal\",2\n"
                               "\"another\",3\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_TRUE(result.dialect.double_quote);
}

// ============================================================================
// Additional Branch Coverage Tests - Score Calculation Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, ScoreHighPatternLowType) {
  // High pattern score (consistent rows) but low type score (all strings)
  const std::string csv_data = "name,city,country\n"
                               "Alice,Paris,France\n"
                               "Bob,London,UK\n"
                               "Charlie,Berlin,Germany\n"
                               "David,Madrid,Spain\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  // Should still detect correctly despite all-string data
  EXPECT_EQ(result.dialect.delimiter, ',');
}

TEST_F(DialectDetectionTest, ScoreLowPatternHighType) {
  // Low pattern score (ragged) but high type score (all typed)
  const std::string csv_data = "id,value\n"
                               "1,100\n"
                               "2,200,extra\n"
                               "3,300\n"
                               "4,400,more,data\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // May or may not succeed depending on score thresholds
  EXPECT_EQ(result.dialect.delimiter, ',');
}

// ============================================================================
// Wide CSV Tests (Issue #260)
// Tests adaptive sample size for files with many columns where rows are very long
// ============================================================================

TEST_F(DialectDetectionTest, WideCSVAdaptiveSampleSize) {
  // Create a wide CSV with 500 columns - each row is ~2000+ bytes
  // With default 10KB sample size and min_rows=2, this would fail
  // without adaptive sample size adjustment
  std::string csv_data;

  // Header row with 500 columns
  for (int i = 0; i < 500; ++i) {
    if (i > 0)
      csv_data += ",";
    csv_data += "col" + std::to_string(i);
  }
  csv_data += "\n";

  // Data rows with 500 columns each
  for (int row = 0; row < 5; ++row) {
    for (int col = 0; col < 500; ++col) {
      if (col > 0)
        csv_data += ",";
      csv_data += std::to_string(row * 500 + col);
    }
    csv_data += "\n";
  }

  // Use default options (10KB sample size, min_rows=2)
  libvroom::DialectDetector default_detector;
  auto result =
      default_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should succeed with adaptive sample size
  EXPECT_TRUE(result.success()) << "Wide CSV detection should succeed with adaptive sample size";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
  EXPECT_EQ(result.detected_columns, 500) << "Should detect 500 columns";
  EXPECT_GE(result.rows_analyzed, 2) << "Should analyze at least min_rows (2) rows";
}

TEST_F(DialectDetectionTest, VeryWideCSV1000Columns) {
  // Create an extremely wide CSV with 1000 columns
  // Simulates files like COVID-19 time series data
  std::string csv_data;

  // Header row
  for (int i = 0; i < 1000; ++i) {
    if (i > 0)
      csv_data += ",";
    csv_data += "date_" + std::to_string(i);
  }
  csv_data += "\n";

  // Data rows
  for (int row = 0; row < 5; ++row) {
    for (int col = 0; col < 1000; ++col) {
      if (col > 0)
        csv_data += ",";
      csv_data += std::to_string(100 + row * col % 1000);
    }
    csv_data += "\n";
  }

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Very wide CSV (1000 cols) should succeed";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 1000);
}

TEST_F(DialectDetectionTest, WideCSVWithQuotedFields) {
  // Wide CSV with some quoted fields containing commas
  std::string csv_data;

  // Header with 200 columns
  for (int i = 0; i < 200; ++i) {
    if (i > 0)
      csv_data += ",";
    csv_data += "\"Column " + std::to_string(i) + "\"";
  }
  csv_data += "\n";

  // Data rows
  for (int row = 0; row < 5; ++row) {
    for (int col = 0; col < 200; ++col) {
      if (col > 0)
        csv_data += ",";
      if (col % 10 == 0) {
        // Every 10th column has a quoted field with embedded comma
        csv_data += "\"value" + std::to_string(col) + ", extra\"";
      } else {
        csv_data += std::to_string(row * 200 + col);
      }
    }
    csv_data += "\n";
  }

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Wide CSV with quoted fields should succeed";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 200);
}

TEST_F(DialectDetectionTest, WideCSVRowLongerThanDefaultSample) {
  // Create a CSV where a single row is longer than default 10KB sample
  std::string csv_data;

  // Create header with enough columns to exceed 10KB per row
  // Each column name "column_XXXX" is ~12 chars + comma = ~13 chars
  // 10240 / 13 ≈ 788 columns needed
  const int num_cols = 900; // Ensure row > 10KB

  for (int i = 0; i < num_cols; ++i) {
    if (i > 0)
      csv_data += ",";
    csv_data += "column_" + std::to_string(1000 + i); // column_1000, column_1001, etc.
  }
  csv_data += "\n";

  // Add data rows
  for (int row = 0; row < 4; ++row) {
    for (int col = 0; col < num_cols; ++col) {
      if (col > 0)
        csv_data += ",";
      csv_data += std::to_string(row * 1000 + col);
    }
    csv_data += "\n";
  }

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "CSV with rows > 10KB should succeed";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, static_cast<size_t>(num_cols));
}

// ============================================================================
// Two-Row CSV Detection Tests (Issue #293)
// Tests that files with header + 1 data row can be detected with min_rows=2
// ============================================================================

TEST_F(DialectDetectionTest, TwoRowCSVDetection) {
  // Exact example from issue #293: header + single data row
  const std::string csv_data = "\"source id\",\"target id\",\"label\"\n\"60\",\"59\",\"Bob rel\"\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Two-row CSV should be detected with min_rows=2";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
  EXPECT_EQ(result.dialect.quote_char, '"') << "Should detect double-quote";
  EXPECT_EQ(result.detected_columns, 3) << "Should detect 3 columns";
  EXPECT_EQ(result.rows_analyzed, 2) << "Should analyze 2 rows";
}

TEST_F(DialectDetectionTest, TwoRowCSVSemicolonDelimited) {
  // Two-row CSV with semicolon delimiter
  const std::string csv_data = "name;age;city\nAlice;30;Boston\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Two-row semicolon CSV should succeed";
  EXPECT_EQ(result.dialect.delimiter, ';') << "Should detect semicolon delimiter";
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, TwoRowCSVTabDelimited) {
  // Two-row TSV
  const std::string csv_data = "col1\tcol2\tcol3\nval1\tval2\tval3\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Two-row TSV should succeed";
  EXPECT_EQ(result.dialect.delimiter, '\t') << "Should detect tab delimiter";
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, TwoRowCSVWithMixedQuoting) {
  // Header + data row with some quoted and some unquoted fields
  const std::string csv_data = "id,\"full name\",active\n1,\"John Doe\",true\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "Two-row CSV with mixed quoting should succeed";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, SingleRowStillFailsWithDefaultMinRows) {
  // Single row (no data) should still fail with default min_rows=2
  const std::string csv_data = "a,b,c\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Should fail because we only have 1 row, less than min_rows=2
  EXPECT_FALSE(result.success()) << "Single row should fail with default min_rows=2";
}

// ============================================================================
// Comment Line Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectHashCommentLines) {
  // CSV with hash comment header (matches issue #294 example)
  std::string path = getTestDataPath("comments", "hash_comments.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for hash_comments.csv";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
  EXPECT_EQ(result.detected_columns, 2) << "hash_comments.csv has 2 columns";
  EXPECT_EQ(result.comment_char, '#') << "Should detect hash comment character";
  EXPECT_EQ(result.comment_lines_skipped, 2) << "Should skip 2 leading comment lines";
}

TEST_F(DialectDetectionTest, DetectMultiHeaderComments) {
  // The example from issue #294
  std::string path = getTestDataPath("comments", "multi_header_comments.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for multi_header_comments.csv";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
  EXPECT_EQ(result.detected_columns, 4) << "multi_header_comments.csv has 4 columns";
  EXPECT_EQ(result.comment_char, '#') << "Should detect hash comment character";
  EXPECT_EQ(result.comment_lines_skipped, 4) << "Should skip 4 leading comment lines";
  // The key success criteria is that detection succeeds with correct results.
  // An ambiguity warning may still occur if multiple dialects score similarly,
  // but that's a general scoring issue unrelated to comment handling.
}

TEST_F(DialectDetectionTest, CommentLinesWithSemicolon) {
  // Test semicolon comment character
  libvroom::DetectionOptions opts;
  opts.comment_chars = {';'}; // Only semicolon as comment char

  libvroom::DialectDetector custom_detector(opts);
  std::string path = getTestDataPath("comments", "semicolon_comments.csv");
  auto result = custom_detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for semicolon_comments.csv";
  EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
  EXPECT_EQ(result.comment_char, ';') << "Should detect semicolon comment character";
  EXPECT_EQ(result.comment_lines_skipped, 2) << "Should skip 2 leading comment lines";
}

TEST_F(DialectDetectionTest, NoCommentLines) {
  // Standard CSV without comment lines
  std::string path = getTestDataPath("basic", "simple.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for simple.csv";
  EXPECT_EQ(result.comment_char, '\0') << "No comment character should be detected";
  EXPECT_EQ(result.comment_lines_skipped, 0) << "No comment lines to skip";
}

TEST_F(DialectDetectionTest, CommentLinesInMemoryBuffer) {
  // Test comment detection with in-memory buffer
  const std::string csv_data = "# Header comment 1\n"
                               "# Header comment 2\n"
                               "name,value,active\n"
                               "Alice,100,true\n"
                               "Bob,200,false\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success()) << "In-memory detection with comments should succeed";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 3);
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.comment_lines_skipped, 2);
}

TEST_F(DialectDetectionTest, CommentLineWithLeadingWhitespace) {
  // Comment lines may have leading whitespace
  const std::string csv_data = "  # Comment with leading spaces\n"
                               "\t# Comment with leading tab\n"
                               "a,b,c\n"
                               "1,2,3\n"
                               "4,5,6\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.comment_lines_skipped, 2);
}

TEST_F(DialectDetectionTest, AllLinesAreComments) {
  // Edge case: file contains only comment lines
  const std::string csv_data = "# Only comments\n"
                               "# No data here\n"
                               "# Still nothing\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_FALSE(result.success()) << "File with only comments should fail detection";
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.comment_lines_skipped, 3);
  EXPECT_FALSE(result.warning.empty());
}

TEST_F(DialectDetectionTest, EmptyCommentCharsDisablesCommentDetection) {
  // Test that empty comment_chars disables comment detection
  libvroom::DetectionOptions opts;
  opts.comment_chars = {}; // No comment characters

  libvroom::DialectDetector custom_detector(opts);

  const std::string csv_data = "# This is data not a comment\n"
                               "a,b\n"
                               "1,2\n";

  auto result =
      custom_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  // Without comment detection, the first row is treated as data
  // which will likely cause inconsistent column counts
  EXPECT_EQ(result.comment_char, '\0');
  EXPECT_EQ(result.comment_lines_skipped, 0);
}

TEST_F(DialectDetectionTest, QuotedHashIsNotComment) {
  // Hash inside quoted field should not be treated as comment
  std::string path = getTestDataPath("comments", "quoted_hash.csv");
  auto result = detector.detect_file(path);

  EXPECT_TRUE(result.success()) << "Detection should succeed for quoted_hash.csv";
  EXPECT_EQ(result.dialect.delimiter, ',');
  EXPECT_EQ(result.detected_columns, 2);
  // First line is data (name,description), not a comment
  EXPECT_EQ(result.comment_char, '\0') << "No leading comment lines";
  EXPECT_EQ(result.comment_lines_skipped, 0);
}

TEST_F(DialectDetectionTest, MultipleCommentCharTypes) {
  // Test detection with multiple comment character options
  libvroom::DetectionOptions opts;
  opts.comment_chars = {'#', ';', '%'}; // Multiple single-char comment prefixes

  libvroom::DialectDetector custom_detector(opts);

  const std::string csv_data = "; Semicolon comment\n"
                               "a,b,c\n"
                               "1,2,3\n"
                               "4,5,6\n";

  auto result =
      custom_detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.comment_char, ';');
  EXPECT_EQ(result.comment_lines_skipped, 1);
}

TEST_F(DialectDetectionTest, CommentLinesCRLF) {
  // Test comment detection with CRLF line endings
  const std::string csv_data = "# Comment 1\r\n"
                               "# Comment 2\r\n"
                               "a,b,c\r\n"
                               "1,2,3\r\n"
                               "4,5,6\r\n";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.comment_lines_skipped, 2);
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::CRLF);
}

TEST_F(DialectDetectionTest, CommentLinesCR) {
  // Test comment detection with CR line endings (old Mac style)
  const std::string csv_data = "# Comment 1\r"
                               "# Comment 2\r"
                               "a,b,c\r"
                               "1,2,3\r"
                               "4,5,6\r";

  auto result = detector.detect(reinterpret_cast<const uint8_t*>(csv_data.data()), csv_data.size());

  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.comment_char, '#');
  EXPECT_EQ(result.comment_lines_skipped, 2);
  EXPECT_EQ(result.dialect.line_ending, libvroom::Dialect::LineEnding::CR);
}
