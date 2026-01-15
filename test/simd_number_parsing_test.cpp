/**
 * @file simd_number_parsing_test.cpp
 * @brief Unit tests for SIMD-accelerated number parsing.
 */

#include "simd_number_parsing.h"

#include <cmath>
#include <cstring>
#include <gtest/gtest.h>
#include <limits>
#include <vector>

using namespace libvroom;

// =============================================================================
// SIMD Integer Parser Tests
// =============================================================================

class SIMDIntegerParserTest : public ::testing::Test {
protected:
  const char* make_str(const std::string& s) {
    str_storage_ = s;
    return str_storage_.c_str();
  }

private:
  std::string str_storage_;
};

// Basic parsing tests
TEST_F(SIMDIntegerParserTest, ParseZero) {
  auto result = SIMDIntegerParser::parse_int64("0", 1);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0);
}

TEST_F(SIMDIntegerParserTest, ParsePositiveSmall) {
  auto result = SIMDIntegerParser::parse_int64("12345", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDIntegerParserTest, ParsePositiveLarge) {
  auto result = SIMDIntegerParser::parse_int64("123456789012345678", 18);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123456789012345678LL);
}

TEST_F(SIMDIntegerParserTest, ParseNegativeSmall) {
  auto result = SIMDIntegerParser::parse_int64("-12345", 6);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), -12345);
}

TEST_F(SIMDIntegerParserTest, ParseNegativeLarge) {
  auto result = SIMDIntegerParser::parse_int64("-123456789012345678", 19);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), -123456789012345678LL);
}

TEST_F(SIMDIntegerParserTest, ParseWithPlusSign) {
  auto result = SIMDIntegerParser::parse_int64("+42", 3);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 42);
}

// Boundary tests
TEST_F(SIMDIntegerParserTest, Int64Max) {
  auto result = SIMDIntegerParser::parse_int64("9223372036854775807", 19);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), INT64_MAX);
}

TEST_F(SIMDIntegerParserTest, Int64Min) {
  auto result = SIMDIntegerParser::parse_int64("-9223372036854775808", 20);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), INT64_MIN);
}

TEST_F(SIMDIntegerParserTest, Int64Overflow) {
  auto result = SIMDIntegerParser::parse_int64("9223372036854775808", 19);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, Int64Underflow) {
  auto result = SIMDIntegerParser::parse_int64("-9223372036854775809", 20);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

// Uint64 boundary tests
TEST_F(SIMDIntegerParserTest, Uint64MaxBoundary) {
  // 18446744073709551615 is UINT64_MAX
  auto result = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), UINT64_MAX);
}

TEST_F(SIMDIntegerParserTest, Uint64Overflow) {
  // 18446744073709551616 is UINT64_MAX + 1
  auto result = SIMDIntegerParser::parse_uint64("18446744073709551616", 20);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, Uint64OverflowByLastDigit) {
  // Test that the boundary condition at max_before_mul works correctly
  // 1844674407370955161 * 10 + 6 = 18446744073709551616 (overflow)
  auto result = SIMDIntegerParser::parse_uint64("18446744073709551616", 20);
  EXPECT_FALSE(result.ok());

  // 1844674407370955161 * 10 + 5 = 18446744073709551615 (UINT64_MAX, ok)
  auto result2 = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
  EXPECT_TRUE(result2.ok());
}

// Whitespace handling
TEST_F(SIMDIntegerParserTest, WhitespaceTrimming) {
  auto result = SIMDIntegerParser::parse_int64("  42  ", 6);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 42);
}

TEST_F(SIMDIntegerParserTest, LeadingWhitespace) {
  auto result = SIMDIntegerParser::parse_int64("   123", 6);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123);
}

TEST_F(SIMDIntegerParserTest, TrailingWhitespace) {
  auto result = SIMDIntegerParser::parse_int64("456   ", 6);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 456);
}

TEST_F(SIMDIntegerParserTest, TabWhitespace) {
  auto result = SIMDIntegerParser::parse_int64("\t789\t", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 789);
}

TEST_F(SIMDIntegerParserTest, MixedTabsAndSpaces) {
  auto result = SIMDIntegerParser::parse_int64(" \t 42 \t ", 8);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 42);
}

TEST_F(SIMDIntegerParserTest, NoTrimWhitespace) {
  auto result = SIMDIntegerParser::parse_int64("  42  ", 6, false);
  EXPECT_FALSE(result.ok()); // Fails because leading space is not a digit
}

// NA and empty handling
TEST_F(SIMDIntegerParserTest, EmptyIsNA) {
  auto result = SIMDIntegerParser::parse_int64("", 0);
  EXPECT_TRUE(result.is_na());
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, WhitespaceOnlyIsNA) {
  auto result = SIMDIntegerParser::parse_int64("   ", 3);
  EXPECT_TRUE(result.is_na());
}

// Error cases
TEST_F(SIMDIntegerParserTest, InvalidCharacter) {
  auto result = SIMDIntegerParser::parse_int64("12a34", 5);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, DecimalPoint) {
  auto result = SIMDIntegerParser::parse_int64("12.34", 5);
  EXPECT_FALSE(result.ok());
}

TEST_F(SIMDIntegerParserTest, JustSign) {
  auto result = SIMDIntegerParser::parse_int64("-", 1);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, TooManyDigits) {
  auto result = SIMDIntegerParser::parse_int64("12345678901234567890", 20);
  EXPECT_FALSE(result.ok()); // 20 digits is too many for int64
}

// Unsigned integer tests
TEST_F(SIMDIntegerParserTest, ParseUint64) {
  auto result = SIMDIntegerParser::parse_uint64("12345", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345ULL);
}

TEST_F(SIMDIntegerParserTest, Uint64Max) {
  auto result = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), UINT64_MAX);
}

TEST_F(SIMDIntegerParserTest, Uint64NegativeError) {
  auto result = SIMDIntegerParser::parse_uint64("-1", 2);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

// Digit validation
TEST_F(SIMDIntegerParserTest, ValidateDigitsAllValid) {
  const uint8_t data[] = "1234567890";
  EXPECT_TRUE(SIMDIntegerParser::validate_digits_simd(data, 10));
}

TEST_F(SIMDIntegerParserTest, ValidateDigitsWithInvalid) {
  const uint8_t data[] = "12345a6789";
  EXPECT_FALSE(SIMDIntegerParser::validate_digits_simd(data, 10));
}

TEST_F(SIMDIntegerParserTest, ValidateDigitsLongString) {
  // Test SIMD path with 64+ characters
  std::string digits(100, '5');
  EXPECT_TRUE(SIMDIntegerParser::validate_digits_simd(
      reinterpret_cast<const uint8_t*>(digits.c_str()), digits.size()));
}

// Column parsing
TEST_F(SIMDIntegerParserTest, ParseInt64Column) {
  const char* fields[] = {"123", "-456", "789", "", "42"};
  size_t lengths[] = {3, 4, 3, 0, 2};
  int64_t results[5];
  bool valid[5];

  SIMDIntegerParser::parse_int64_column(fields, lengths, 5, results, valid);

  EXPECT_TRUE(valid[0]);
  EXPECT_EQ(results[0], 123);
  EXPECT_TRUE(valid[1]);
  EXPECT_EQ(results[1], -456);
  EXPECT_TRUE(valid[2]);
  EXPECT_EQ(results[2], 789);
  EXPECT_FALSE(valid[3]); // Empty
  EXPECT_TRUE(valid[4]);
  EXPECT_EQ(results[4], 42);
}

TEST_F(SIMDIntegerParserTest, ParseInt64ColumnVector) {
  const char* fields[] = {"100", "200", "invalid", "300"};
  size_t lengths[] = {3, 3, 7, 3};

  auto results = SIMDIntegerParser::parse_int64_column(fields, lengths, 4);

  EXPECT_EQ(results.size(), 4);
  EXPECT_TRUE(results[0].has_value());
  EXPECT_EQ(*results[0], 100);
  EXPECT_TRUE(results[1].has_value());
  EXPECT_EQ(*results[1], 200);
  EXPECT_FALSE(results[2].has_value()); // Invalid
  EXPECT_TRUE(results[3].has_value());
  EXPECT_EQ(*results[3], 300);
}

// =============================================================================
// Integer Size Category Tests (1-8, 9-16, 17-19 digits)
// =============================================================================

class SIMDIntegerSizeCategoryTest
    : public ::testing::TestWithParam<std::pair<std::string, int64_t>> {};

// Test 1-8 digit integers (short path in SIMD parsing)
TEST(SIMDIntegerSizeCategoryTest, ShortIntegers1To8Digits) {
  std::vector<std::pair<std::string, int64_t>> test_cases = {
      {"1", 1},               // 1 digit
      {"9", 9},               // 1 digit max
      {"12", 12},             // 2 digits
      {"99", 99},             // 2 digits max
      {"123", 123},           // 3 digits
      {"999", 999},           // 3 digits max
      {"1234", 1234},         // 4 digits
      {"9999", 9999},         // 4 digits max
      {"12345", 12345},       // 5 digits
      {"99999", 99999},       // 5 digits max
      {"123456", 123456},     // 6 digits
      {"999999", 999999},     // 6 digits max
      {"1234567", 1234567},   // 7 digits
      {"9999999", 9999999},   // 7 digits max
      {"12345678", 12345678}, // 8 digits
      {"99999999", 99999999}, // 8 digits max
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDIntegerParser::parse_int64(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_EQ(result.get(), expected) << "Wrong value for: " << str;

    // Test negative versions
    std::string neg_str = "-" + str;
    auto neg_result = SIMDIntegerParser::parse_int64(neg_str.c_str(), neg_str.size());
    EXPECT_TRUE(neg_result.ok()) << "Failed to parse negative: " << neg_str;
    EXPECT_EQ(neg_result.get(), -expected) << "Wrong value for negative: " << neg_str;
  }
}

// Test 9-16 digit integers (SIMD validation + scalar accumulation path)
TEST(SIMDIntegerSizeCategoryTest, MediumIntegers9To16Digits) {
  std::vector<std::pair<std::string, int64_t>> test_cases = {
      {"123456789", 123456789LL},               // 9 digits
      {"999999999", 999999999LL},               // 9 digits max
      {"1234567890", 1234567890LL},             // 10 digits
      {"9999999999", 9999999999LL},             // 10 digits max
      {"12345678901", 12345678901LL},           // 11 digits
      {"99999999999", 99999999999LL},           // 11 digits max
      {"123456789012", 123456789012LL},         // 12 digits
      {"999999999999", 999999999999LL},         // 12 digits max
      {"1234567890123", 1234567890123LL},       // 13 digits
      {"9999999999999", 9999999999999LL},       // 13 digits max
      {"12345678901234", 12345678901234LL},     // 14 digits
      {"99999999999999", 99999999999999LL},     // 14 digits max
      {"123456789012345", 123456789012345LL},   // 15 digits
      {"999999999999999", 999999999999999LL},   // 15 digits max
      {"1234567890123456", 1234567890123456LL}, // 16 digits
      {"9999999999999999", 9999999999999999LL}, // 16 digits max
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDIntegerParser::parse_int64(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_EQ(result.get(), expected) << "Wrong value for: " << str;

    // Test negative versions
    std::string neg_str = "-" + str;
    auto neg_result = SIMDIntegerParser::parse_int64(neg_str.c_str(), neg_str.size());
    EXPECT_TRUE(neg_result.ok()) << "Failed to parse negative: " << neg_str;
    EXPECT_EQ(neg_result.get(), -expected) << "Wrong value for negative: " << neg_str;
  }
}

// Test 17-19 digit integers (overflow-prone zone)
TEST(SIMDIntegerSizeCategoryTest, LongIntegers17To19Digits) {
  std::vector<std::pair<std::string, int64_t>> test_cases = {
      {"12345678901234567", 12345678901234567LL},     // 17 digits
      {"99999999999999999", 99999999999999999LL},     // 17 digits max
      {"123456789012345678", 123456789012345678LL},   // 18 digits
      {"999999999999999999", 999999999999999999LL},   // 18 digits max
      {"1234567890123456789", 1234567890123456789LL}, // 19 digits
      {"9223372036854775807", INT64_MAX},             // 19 digits (INT64_MAX)
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDIntegerParser::parse_int64(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_EQ(result.get(), expected) << "Wrong value for: " << str;
  }
}

// Test 17-19 digit negative integers (requires careful handling near INT64_MIN)
TEST(SIMDIntegerSizeCategoryTest, LongNegativeIntegers17To19Digits) {
  std::vector<std::pair<std::string, int64_t>> test_cases = {
      {"-12345678901234567", -12345678901234567LL},     // 17 digits
      {"-99999999999999999", -99999999999999999LL},     // 17 digits max
      {"-123456789012345678", -123456789012345678LL},   // 18 digits
      {"-999999999999999999", -999999999999999999LL},   // 18 digits max
      {"-1234567890123456789", -1234567890123456789LL}, // 19 digits
      {"-9223372036854775808", INT64_MIN},              // 19 digits (INT64_MIN)
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDIntegerParser::parse_int64(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_EQ(result.get(), expected) << "Wrong value for: " << str;
  }
}

// Test unsigned integers at various sizes
TEST(SIMDIntegerSizeCategoryTest, UnsignedIntegerSizes) {
  std::vector<std::pair<std::string, uint64_t>> test_cases = {
      {"1", 1ULL},
      {"12345678", 12345678ULL},                         // 8 digits
      {"123456789012345678", 123456789012345678ULL},     // 18 digits
      {"9999999999999999999", 9999999999999999999ULL},   // 19 digits
      {"10000000000000000000", 10000000000000000000ULL}, // 20 digits
      {"18446744073709551615", UINT64_MAX},              // 20 digits (UINT64_MAX)
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDIntegerParser::parse_uint64(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_EQ(result.get(), expected) << "Wrong value for: " << str;
  }
}

// =============================================================================
// SIMD/Scalar Boundary Tests
// =============================================================================

class SIMDBoundaryTest : public ::testing::Test {};

// Test the boundary at 8 characters where SIMD validation kicks in
TEST_F(SIMDBoundaryTest, SIMDValidationBoundary) {
  // Below threshold (7 digits) - scalar validation only
  auto result7 = SIMDIntegerParser::parse_int64("1234567", 7);
  EXPECT_TRUE(result7.ok());
  EXPECT_EQ(result7.get(), 1234567);

  // At threshold (8 digits) - triggers SIMD validation
  auto result8 = SIMDIntegerParser::parse_int64("12345678", 8);
  EXPECT_TRUE(result8.ok());
  EXPECT_EQ(result8.get(), 12345678);

  // Above threshold (9 digits) - SIMD validation
  auto result9 = SIMDIntegerParser::parse_int64("123456789", 9);
  EXPECT_TRUE(result9.ok());
  EXPECT_EQ(result9.get(), 123456789);
}

// Test invalid characters at various positions near SIMD boundary
TEST_F(SIMDBoundaryTest, InvalidCharacterPositions) {
  // Invalid at position 0 (first char)
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("a2345678901234567", 17).ok());
  // Invalid at position 7 (just before SIMD lane boundary in some configs)
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("1234567a901234567", 17).ok());
  // Invalid at position 8 (at common SIMD lane boundary)
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12345678a01234567", 17).ok());
  // Invalid at position 15 (another SIMD boundary)
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("123456789012345a7", 17).ok());
  // Invalid at last position
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("123456789012345678a", 19).ok());
}

// Test SIMD validation with various string lengths around common lane sizes
TEST_F(SIMDBoundaryTest, SIMDLaneSizeBoundaries) {
  // These test lengths around 16 (SSE), 32 (AVX2), and 64 (AVX-512) boundaries
  std::vector<size_t> test_lengths = {7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100};

  for (size_t len : test_lengths) {
    std::string valid_digits(len, '5');
    EXPECT_TRUE(SIMDIntegerParser::validate_digits_simd(
        reinterpret_cast<const uint8_t*>(valid_digits.c_str()), len))
        << "Failed for length: " << len;

    // Put invalid char at the middle
    std::string invalid_middle = valid_digits;
    invalid_middle[len / 2] = 'x';
    EXPECT_FALSE(SIMDIntegerParser::validate_digits_simd(
        reinterpret_cast<const uint8_t*>(invalid_middle.c_str()), len))
        << "False positive for length: " << len;

    // Put invalid char at the end
    std::string invalid_end = valid_digits;
    invalid_end[len - 1] = 'x';
    EXPECT_FALSE(SIMDIntegerParser::validate_digits_simd(
        reinterpret_cast<const uint8_t*>(invalid_end.c_str()), len))
        << "False positive for length: " << len;
  }
}

// Test the scalar fallback for remainder bytes after SIMD processing
TEST_F(SIMDBoundaryTest, ScalarFallbackRemainder) {
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // Create string with length = N + 1 (one byte processed by scalar)
  std::string digits(N + 1, '5');
  EXPECT_TRUE(SIMDIntegerParser::validate_digits_simd(
      reinterpret_cast<const uint8_t*>(digits.c_str()), digits.size()));

  // Invalid character in the scalar-processed remainder
  digits[N] = 'x';
  EXPECT_FALSE(SIMDIntegerParser::validate_digits_simd(
      reinterpret_cast<const uint8_t*>(digits.c_str()), digits.size()));
}

// =============================================================================
// Error Handling Tests
// =============================================================================

class SIMDErrorHandlingTest : public ::testing::Test {};

// Invalid character tests
TEST_F(SIMDErrorHandlingTest, InvalidCharacters) {
  // Letters at various positions
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12a45", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("a2345", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("1234a", 5).ok());

  // Special characters
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12$45", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12!45", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12@45", 5).ok());

  // Unicode/high-byte characters
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12\xFF", 3).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12\x80", 3).ok());
}

// Multiple sign tests
TEST_F(SIMDErrorHandlingTest, MultipleSigns) {
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("--123", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("++123", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("-+123", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("+-123", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("1-23", 4).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("1+23", 4).ok());
}

// Empty and whitespace-only inputs
TEST_F(SIMDErrorHandlingTest, EmptyAndWhitespaceInputs) {
  // Empty string
  EXPECT_TRUE(SIMDIntegerParser::parse_int64("", 0).is_na());

  // Various whitespace combinations
  EXPECT_TRUE(SIMDIntegerParser::parse_int64(" ", 1).is_na());
  EXPECT_TRUE(SIMDIntegerParser::parse_int64("  ", 2).is_na());
  EXPECT_TRUE(SIMDIntegerParser::parse_int64("\t", 1).is_na());
  EXPECT_TRUE(SIMDIntegerParser::parse_int64(" \t ", 3).is_na());
  EXPECT_TRUE(SIMDIntegerParser::parse_int64("\t\t\t", 3).is_na());
}

// Sign-only inputs
TEST_F(SIMDErrorHandlingTest, SignOnlyInputs) {
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("-", 1).ok());
  EXPECT_NE(SIMDIntegerParser::parse_int64("-", 1).error, nullptr);

  EXPECT_FALSE(SIMDIntegerParser::parse_int64("+", 1).ok());
  EXPECT_NE(SIMDIntegerParser::parse_int64("+", 1).error, nullptr);

  // Sign followed by whitespace only
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("- ", 2, false).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("+ ", 2, false).ok());
}

// Truncated input tests
TEST_F(SIMDErrorHandlingTest, TruncatedInputs) {
  // Verify that truncated strings don't access beyond length
  char buffer[10] = "12345XXXX"; // Extra chars should not be read
  auto result = SIMDIntegerParser::parse_int64(buffer, 5);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345);
}

// Overflow boundary tests
TEST_F(SIMDErrorHandlingTest, OverflowBoundaries) {
  // Just below INT64_MAX
  auto below_max = SIMDIntegerParser::parse_int64("9223372036854775806", 19);
  EXPECT_TRUE(below_max.ok());
  EXPECT_EQ(below_max.get(), INT64_MAX - 1);

  // Exactly INT64_MAX
  auto at_max = SIMDIntegerParser::parse_int64("9223372036854775807", 19);
  EXPECT_TRUE(at_max.ok());
  EXPECT_EQ(at_max.get(), INT64_MAX);

  // Just above INT64_MAX
  auto above_max = SIMDIntegerParser::parse_int64("9223372036854775808", 19);
  EXPECT_FALSE(above_max.ok());

  // Just below INT64_MIN (absolute value)
  auto below_min_abs = SIMDIntegerParser::parse_int64("-9223372036854775807", 20);
  EXPECT_TRUE(below_min_abs.ok());
  EXPECT_EQ(below_min_abs.get(), INT64_MIN + 1);

  // Exactly INT64_MIN
  auto at_min = SIMDIntegerParser::parse_int64("-9223372036854775808", 20);
  EXPECT_TRUE(at_min.ok());
  EXPECT_EQ(at_min.get(), INT64_MIN);

  // Just beyond INT64_MIN
  auto beyond_min = SIMDIntegerParser::parse_int64("-9223372036854775809", 20);
  EXPECT_FALSE(beyond_min.ok());
}

// UINT64 overflow boundaries
TEST_F(SIMDErrorHandlingTest, UInt64OverflowBoundaries) {
  // Just below UINT64_MAX
  auto below_max = SIMDIntegerParser::parse_uint64("18446744073709551614", 20);
  EXPECT_TRUE(below_max.ok());
  EXPECT_EQ(below_max.get(), UINT64_MAX - 1);

  // Exactly UINT64_MAX
  auto at_max = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
  EXPECT_TRUE(at_max.ok());
  EXPECT_EQ(at_max.get(), UINT64_MAX);

  // Just above UINT64_MAX (last digit causes overflow)
  auto above_max = SIMDIntegerParser::parse_uint64("18446744073709551616", 20);
  EXPECT_FALSE(above_max.ok());

  // Way above UINT64_MAX
  auto way_above = SIMDIntegerParser::parse_uint64("18446744073709551699", 20);
  EXPECT_FALSE(way_above.ok());

  // Test the exact boundary condition (1844674407370955161 * 10 + 6)
  auto boundary = SIMDIntegerParser::parse_uint64("18446744073709551620", 20);
  EXPECT_FALSE(boundary.ok());
}

// Uint64 sign handling
TEST_F(SIMDErrorHandlingTest, UInt64SignHandling) {
  // Positive sign is allowed
  auto with_plus = SIMDIntegerParser::parse_uint64("+123", 4);
  EXPECT_TRUE(with_plus.ok());
  EXPECT_EQ(with_plus.get(), 123ULL);

  // Negative is not allowed
  auto with_minus = SIMDIntegerParser::parse_uint64("-123", 4);
  EXPECT_FALSE(with_minus.ok());
  EXPECT_NE(with_minus.error, nullptr);

  // Plus then minus
  auto plus_minus = SIMDIntegerParser::parse_uint64("+-1", 3);
  EXPECT_FALSE(plus_minus.ok());
}

// Too many digits tests
TEST_F(SIMDErrorHandlingTest, TooManyDigits) {
  // 20 digits for int64 (max is 19)
  auto int64_20_digits = SIMDIntegerParser::parse_int64("12345678901234567890", 20);
  EXPECT_FALSE(int64_20_digits.ok());

  // 21 digits for uint64 (max is 20)
  auto uint64_21_digits = SIMDIntegerParser::parse_uint64("123456789012345678901", 21);
  EXPECT_FALSE(uint64_21_digits.ok());
}

// =============================================================================
// Whitespace Edge Cases
// =============================================================================

class SIMDWhitespaceTest : public ::testing::Test {};

TEST_F(SIMDWhitespaceTest, LeadingWhitespaceVariations) {
  // Single space
  EXPECT_EQ(SIMDIntegerParser::parse_int64(" 42", 3).get(), 42);
  // Multiple spaces
  EXPECT_EQ(SIMDIntegerParser::parse_int64("    42", 6).get(), 42);
  // Single tab
  EXPECT_EQ(SIMDIntegerParser::parse_int64("\t42", 3).get(), 42);
  // Multiple tabs
  EXPECT_EQ(SIMDIntegerParser::parse_int64("\t\t42", 4).get(), 42);
  // Mixed spaces and tabs
  EXPECT_EQ(SIMDIntegerParser::parse_int64(" \t 42", 5).get(), 42);
}

TEST_F(SIMDWhitespaceTest, TrailingWhitespaceVariations) {
  EXPECT_EQ(SIMDIntegerParser::parse_int64("42 ", 3).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("42    ", 6).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("42\t", 3).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("42\t\t", 4).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("42 \t ", 5).get(), 42);
}

TEST_F(SIMDWhitespaceTest, BothSidesWhitespace) {
  EXPECT_EQ(SIMDIntegerParser::parse_int64(" 42 ", 4).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("  42  ", 6).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("\t42\t", 4).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64(" \t 42 \t ", 8).get(), 42);
}

TEST_F(SIMDWhitespaceTest, WhitespaceWithSigns) {
  EXPECT_EQ(SIMDIntegerParser::parse_int64(" -42 ", 5).get(), -42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64(" +42 ", 5).get(), 42);
  EXPECT_EQ(SIMDIntegerParser::parse_int64("\t-42\t", 5).get(), -42);
}

TEST_F(SIMDWhitespaceTest, EmbeddedWhitespaceInvalid) {
  // Whitespace between digits is not allowed
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("1 2", 3).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("12 34", 5).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("1\t2", 3).ok());
}

TEST_F(SIMDWhitespaceTest, WhitespaceBetweenSignAndDigits) {
  // Whitespace between sign and digits is not allowed (when trim_whitespace=false)
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("- 42", 4, false).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("+ 42", 4, false).ok());

  // But when trim_whitespace=true, leading spaces around the sign get trimmed first,
  // then we see "- 42" with the space inside which fails
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("- 42", 4).ok());
}

TEST_F(SIMDWhitespaceTest, DisabledWhitespaceTrimming) {
  // With trim_whitespace=false, leading/trailing whitespace causes parse failure
  EXPECT_FALSE(SIMDIntegerParser::parse_int64(" 42", 3, false).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64("42 ", 3, false).ok());
  EXPECT_FALSE(SIMDIntegerParser::parse_int64(" 42 ", 4, false).ok());

  // But plain numbers still work
  EXPECT_TRUE(SIMDIntegerParser::parse_int64("42", 2, false).ok());
  EXPECT_EQ(SIMDIntegerParser::parse_int64("42", 2, false).get(), 42);
}

// =============================================================================
// SIMD Double Parser Tests
// =============================================================================

class SIMDDoubleParserTest : public ::testing::Test {};

// Basic parsing tests
TEST_F(SIMDDoubleParserTest, ParseInteger) {
  auto result = SIMDDoubleParser::parse_double("42", 2);
  EXPECT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(result.get(), 42.0);
}

TEST_F(SIMDDoubleParserTest, ParseDecimal) {
  auto result = SIMDDoubleParser::parse_double("3.14", 4);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 3.14, 0.001);
}

TEST_F(SIMDDoubleParserTest, ParseDecimalNoIntPart) {
  auto result = SIMDDoubleParser::parse_double(".5", 2);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 0.5, 0.001);
}

TEST_F(SIMDDoubleParserTest, ParseDecimalNoFracPart) {
  auto result = SIMDDoubleParser::parse_double("5.", 2);
  EXPECT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(result.get(), 5.0);
}

TEST_F(SIMDDoubleParserTest, ParseNegative) {
  auto result = SIMDDoubleParser::parse_double("-3.14", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), -3.14, 0.001);
}

// Scientific notation
TEST_F(SIMDDoubleParserTest, ParseScientificPositive) {
  auto result = SIMDDoubleParser::parse_double("1e10", 4);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 1e10, 1e5);
}

TEST_F(SIMDDoubleParserTest, ParseScientificNegativeExp) {
  auto result = SIMDDoubleParser::parse_double("1e-10", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 1e-10, 1e-15);
}

TEST_F(SIMDDoubleParserTest, ParseScientificWithDecimal) {
  auto result = SIMDDoubleParser::parse_double("1.5e-10", 7);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 1.5e-10, 1e-15);
}

TEST_F(SIMDDoubleParserTest, ParseScientificUpperE) {
  auto result = SIMDDoubleParser::parse_double("2.5E+5", 6);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 2.5e5, 1);
}

// Special values
TEST_F(SIMDDoubleParserTest, ParseNaN) {
  auto result = SIMDDoubleParser::parse_double("NaN", 3);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isnan(result.get()));
}

TEST_F(SIMDDoubleParserTest, ParseNaNLowercase) {
  auto result = SIMDDoubleParser::parse_double("nan", 3);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isnan(result.get()));
}

TEST_F(SIMDDoubleParserTest, ParseInf) {
  auto result = SIMDDoubleParser::parse_double("Inf", 3);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isinf(result.get()));
  EXPECT_GT(result.get(), 0);
}

TEST_F(SIMDDoubleParserTest, ParseInfinity) {
  auto result = SIMDDoubleParser::parse_double("Infinity", 8);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isinf(result.get()));
}

TEST_F(SIMDDoubleParserTest, ParseNegInf) {
  auto result = SIMDDoubleParser::parse_double("-Inf", 4);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isinf(result.get()));
  EXPECT_LT(result.get(), 0);
}

TEST_F(SIMDDoubleParserTest, ParseNegInfinity) {
  auto result = SIMDDoubleParser::parse_double("-Infinity", 9);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isinf(result.get()));
  EXPECT_LT(result.get(), 0);
}

// Zero handling
TEST_F(SIMDDoubleParserTest, ParseZero) {
  auto result = SIMDDoubleParser::parse_double("0", 1);
  EXPECT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(result.get(), 0.0);
}

TEST_F(SIMDDoubleParserTest, ParseNegativeZero) {
  auto result = SIMDDoubleParser::parse_double("-0.0", 4);
  EXPECT_TRUE(result.ok());
  EXPECT_DOUBLE_EQ(result.get(), -0.0);
  EXPECT_TRUE(std::signbit(result.get()));
}

// Whitespace
TEST_F(SIMDDoubleParserTest, WhitespaceTrimming) {
  auto result = SIMDDoubleParser::parse_double("  3.14  ", 8);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 3.14, 0.001);
}

// Error cases
TEST_F(SIMDDoubleParserTest, EmptyIsNA) {
  auto result = SIMDDoubleParser::parse_double("", 0);
  EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDDoubleParserTest, MalformedScientificNoDigits) {
  auto result = SIMDDoubleParser::parse_double("1e", 2);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDDoubleParserTest, MalformedScientificJustSign) {
  auto result = SIMDDoubleParser::parse_double("1e-", 3);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDDoubleParserTest, TrailingCharacters) {
  auto result = SIMDDoubleParser::parse_double("3.14abc", 7);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDDoubleParserTest, InvalidInfinityVariant) {
  auto result = SIMDDoubleParser::parse_double("INFxxxxx", 8);
  EXPECT_FALSE(result.ok());
}

// Column parsing
TEST_F(SIMDDoubleParserTest, ParseDoubleColumn) {
  const char* fields[] = {"1.5", "-2.5", "3e10", "", "nan"};
  size_t lengths[] = {3, 4, 4, 0, 3};
  double results[5];
  bool valid[5];

  SIMDDoubleParser::parse_double_column(fields, lengths, 5, results, valid);

  EXPECT_TRUE(valid[0]);
  EXPECT_NEAR(results[0], 1.5, 0.001);
  EXPECT_TRUE(valid[1]);
  EXPECT_NEAR(results[1], -2.5, 0.001);
  EXPECT_TRUE(valid[2]);
  EXPECT_NEAR(results[2], 3e10, 1e5);
  EXPECT_FALSE(valid[3]); // Empty
  EXPECT_TRUE(valid[4]);
  EXPECT_TRUE(std::isnan(results[4]));
}

// =============================================================================
// Floating-Point Precision Tests (0-17 significant digits)
// =============================================================================

class SIMDDoublePrecisionTest : public ::testing::Test {};

// Test various significant digit counts
TEST_F(SIMDDoublePrecisionTest, ZeroSignificantDigits) {
  // Edge case: numbers like ".0", "0."
  auto result1 = SIMDDoubleParser::parse_double(".0", 2);
  EXPECT_TRUE(result1.ok());
  EXPECT_DOUBLE_EQ(result1.get(), 0.0);

  auto result2 = SIMDDoubleParser::parse_double("0.", 2);
  EXPECT_TRUE(result2.ok());
  EXPECT_DOUBLE_EQ(result2.get(), 0.0);

  auto result3 = SIMDDoubleParser::parse_double("0.0", 3);
  EXPECT_TRUE(result3.ok());
  EXPECT_DOUBLE_EQ(result3.get(), 0.0);
}

TEST_F(SIMDDoublePrecisionTest, OneToSixSignificantDigits) {
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1", 1.0},           // 1 digit
      {"9", 9.0},           // 1 digit max
      {"12", 12.0},         // 2 digits
      {"99", 99.0},         // 2 digits max
      {"1.5", 1.5},         // 2 sig digits with decimal
      {"123", 123.0},       // 3 digits
      {"1.23", 1.23},       // 3 sig digits with decimal
      {"1234", 1234.0},     // 4 digits
      {"1.234", 1.234},     // 4 sig digits with decimal
      {"12345", 12345.0},   // 5 digits
      {"12.345", 12.345},   // 5 sig digits with decimal
      {"123456", 123456.0}, // 6 digits
      {"123.456", 123.456}, // 6 sig digits with decimal
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-15 + 1e-15)
        << "Wrong value for: " << str;
  }
}

TEST_F(SIMDDoublePrecisionTest, SevenToTwelveSignificantDigits) {
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1234567", 1234567.0},           // 7 digits
      {"1234.567", 1234.567},           // 7 sig digits
      {"12345678", 12345678.0},         // 8 digits
      {"1234.5678", 1234.5678},         // 8 sig digits
      {"123456789", 123456789.0},       // 9 digits
      {"123456.789", 123456.789},       // 9 sig digits
      {"1234567890", 1234567890.0},     // 10 digits
      {"1234567.890", 1234567.890},     // 10 sig digits
      {"12345678901", 12345678901.0},   // 11 digits
      {"12345678.901", 12345678.901},   // 11 sig digits
      {"123456789012", 123456789012.0}, // 12 digits
      {"123456789.012", 123456789.012}, // 12 sig digits
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-12 + 1e-12)
        << "Wrong value for: " << str;
  }
}

TEST_F(SIMDDoublePrecisionTest, ThirteenToSeventeenSignificantDigits) {
  // These test the limits of double precision (IEEE 754 has ~15.95 decimal digits of precision)
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1234567890123", 1234567890123.0},         // 13 digits
      {"12345678901234", 12345678901234.0},       // 14 digits
      {"123456789012345", 123456789012345.0},     // 15 digits
      {"1234567890123456", 1234567890123456.0},   // 16 digits
      {"12345678901234567", 12345678901234568.0}, // 17 digits (may lose precision)
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    // Use looser tolerance for high digit counts
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-9) << "Wrong value for: " << str;
  }
}

TEST_F(SIMDDoublePrecisionTest, BeyondSeventeenDigits) {
  // Beyond 17 digits, the parser should gracefully handle overflow of mantissa digits
  auto result19 = SIMDDoubleParser::parse_double("1234567890123456789", 19);
  EXPECT_TRUE(result19.ok());
  // The value will be approximate due to mantissa overflow handling

  // Very long number (triggers mantissa overflow path)
  auto result22 = SIMDDoubleParser::parse_double("1234567890123456789012", 22);
  EXPECT_TRUE(result22.ok());
}

TEST_F(SIMDDoublePrecisionTest, MantissaOverflowShiftsExponent) {
  // Test that mantissa overflow properly shifts the exponent
  // When >19 digits, excess digits should increment exponent instead
  auto result = SIMDDoubleParser::parse_double("12345678901234567890", 20); // 20 digits
  EXPECT_TRUE(result.ok());
  // The exact value depends on mantissa overflow handling

  // Verify it's reasonably close to the expected magnitude
  EXPECT_GT(result.get(), 1e19);
  EXPECT_LT(result.get(), 2e19);
}

TEST_F(SIMDDoublePrecisionTest, FractionalPartMantissaOverflow) {
  // Test fractional digits beyond mantissa capacity
  auto result = SIMDDoubleParser::parse_double("0.12345678901234567890", 22);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 0.12345678901234567890, 1e-14);
}

// =============================================================================
// Exponent Range Tests (-308 to +308)
// =============================================================================

class SIMDExponentRangeTest : public ::testing::Test {};

// Test positive exponents
TEST_F(SIMDExponentRangeTest, SmallPositiveExponents) {
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1e0", 1e0},   {"1e1", 1e1},   {"1e2", 1e2},   {"1e5", 1e5},
      {"1e10", 1e10}, {"1e15", 1e15}, {"1e20", 1e20}, {"1e22", 1e22}, // Last in lookup table
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-14) << "Wrong value for: " << str;
  }
}

TEST_F(SIMDExponentRangeTest, LargePositiveExponents) {
  // These require the std::pow fallback
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1e23", 1e23},   {"1e50", 1e50},   {"1e100", 1e100},     {"1e200", 1e200},
      {"1e300", 1e300}, {"1e307", 1e307}, {"1.7e308", 1.7e308}, // Near DBL_MAX
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-10) << "Wrong value for: " << str;
  }
}

TEST_F(SIMDExponentRangeTest, SmallNegativeExponents) {
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1e-1", 1e-1},   {"1e-2", 1e-2},   {"1e-5", 1e-5},   {"1e-10", 1e-10},
      {"1e-15", 1e-15}, {"1e-20", 1e-20}, {"1e-22", 1e-22}, // Last in lookup table
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-10) << "Wrong value for: " << str;
  }
}

TEST_F(SIMDExponentRangeTest, LargeNegativeExponents) {
  // These require the std::pow fallback
  std::vector<std::pair<std::string, double>> test_cases = {
      {"1e-23", 1e-23},   {"1e-50", 1e-50},   {"1e-100", 1e-100},
      {"1e-200", 1e-200}, {"1e-300", 1e-300}, {"1e-307", 1e-307},
  };

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    // Large negative exponents produce very small numbers
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-5) << "Wrong value for: " << str;
  }
}

TEST_F(SIMDExponentRangeTest, ExponentBoundaries) {
  // DBL_MAX is approximately 1.8e308
  auto result_near_max = SIMDDoubleParser::parse_double("1.79769e308", 11);
  EXPECT_TRUE(result_near_max.ok());
  EXPECT_TRUE(std::isfinite(result_near_max.get()));

  // Beyond DBL_MAX -> Infinity
  auto result_overflow = SIMDDoubleParser::parse_double("1e309", 5);
  EXPECT_TRUE(result_overflow.ok());
  EXPECT_TRUE(std::isinf(result_overflow.get()));

  // DBL_MIN is approximately 2.2e-308
  auto result_near_min = SIMDDoubleParser::parse_double("2.3e-308", 8);
  EXPECT_TRUE(result_near_min.ok());
  EXPECT_GT(result_near_min.get(), 0);
}

TEST_F(SIMDExponentRangeTest, ExplicitPlusInExponent) {
  auto result = SIMDDoubleParser::parse_double("1e+10", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 1e10, 1e-5);

  auto result2 = SIMDDoubleParser::parse_double("1.5E+20", 7);
  EXPECT_TRUE(result2.ok());
  EXPECT_NEAR(result2.get(), 1.5e20, 1e5);
}

TEST_F(SIMDExponentRangeTest, ExponentOverflowProtection) {
  // Test the overflow protection in exponent parsing (exp_value > 400)
  // The parser consumes all exponent digits even when overflow is detected.
  auto result = SIMDDoubleParser::parse_double("1e400", 5);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isinf(result.get()));

  auto result_neg = SIMDDoubleParser::parse_double("1e-400", 6);
  EXPECT_TRUE(result_neg.ok());
  EXPECT_EQ(result_neg.get(), 0.0);

  // Very large exponents parse successfully to infinity
  auto result_large = SIMDDoubleParser::parse_double("1e9999", 6);
  EXPECT_TRUE(result_large.ok());
  EXPECT_TRUE(std::isinf(result_large.get()));

  // Very large negative exponents parse successfully to zero
  auto result_large_neg = SIMDDoubleParser::parse_double("1e-9999", 7);
  EXPECT_TRUE(result_large_neg.ok());
  EXPECT_EQ(result_large_neg.get(), 0.0);

  // Even extremely large exponents work
  auto result_huge = SIMDDoubleParser::parse_double("1e99999", 7);
  EXPECT_TRUE(result_huge.ok());
  EXPECT_TRUE(std::isinf(result_huge.get()));
}

// =============================================================================
// Subnormal Number Tests
// =============================================================================

class SIMDSubnormalTest : public ::testing::Test {};

TEST_F(SIMDSubnormalTest, SmallSubnormalNumbers) {
  // DBL_MIN (smallest normal) is approximately 2.2e-308
  // DBL_TRUE_MIN (smallest subnormal) is approximately 4.9e-324

  // Numbers smaller than DBL_MIN but larger than DBL_TRUE_MIN
  auto result1 = SIMDDoubleParser::parse_double("1e-310", 6);
  EXPECT_TRUE(result1.ok());
  EXPECT_GT(result1.get(), 0); // Should be non-zero

  auto result2 = SIMDDoubleParser::parse_double("1e-320", 6);
  EXPECT_TRUE(result2.ok());
  // Might be zero due to underflow, or a tiny subnormal
}

TEST_F(SIMDSubnormalTest, VerySmallNumbers) {
  // Test numbers at the edge of representable range
  auto result = SIMDDoubleParser::parse_double("5e-324", 6);
  EXPECT_TRUE(result.ok());
  // The exact value depends on IEEE 754 subnormal handling

  // Definitely too small - should underflow to zero
  auto result_zero = SIMDDoubleParser::parse_double("1e-400", 6);
  EXPECT_TRUE(result_zero.ok());
  EXPECT_EQ(result_zero.get(), 0.0);
}

TEST_F(SIMDSubnormalTest, NormalToSubnormalBoundary) {
  // Test at the normal/subnormal boundary
  // DBL_MIN is approximately 2.225073858507201e-308
  auto result_normal = SIMDDoubleParser::parse_double("2.3e-308", 8);
  EXPECT_TRUE(result_normal.ok());
  EXPECT_GT(result_normal.get(), 0);

  auto result_subnormal = SIMDDoubleParser::parse_double("2.2e-308", 8);
  EXPECT_TRUE(result_subnormal.ok());
  EXPECT_GT(result_subnormal.get(), 0);
}

// =============================================================================
// Double Parser Error Cases
// =============================================================================

class SIMDDoubleErrorTest : public ::testing::Test {};

TEST_F(SIMDDoubleErrorTest, MultipleDecimalPoints) {
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1.2.3", 5).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("..1", 3).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1..", 3).ok());
}

TEST_F(SIMDDoubleErrorTest, MultipleSigns) {
  // Sign only allowed at beginning
  EXPECT_FALSE(SIMDDoubleParser::parse_double("--1", 3).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("++1", 3).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("-+1", 3).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1-2", 3).ok());
}

TEST_F(SIMDDoubleErrorTest, InvalidExponents) {
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1e", 2).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1e-", 3).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1e+", 3).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1eabc", 5).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1e-abc", 6).ok());
}

TEST_F(SIMDDoubleErrorTest, NoDigits) {
  EXPECT_FALSE(SIMDDoubleParser::parse_double(".", 1).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("-", 1).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("+", 1).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("-.", 2).ok());
}

TEST_F(SIMDDoubleErrorTest, TrailingInvalidCharacters) {
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1.5abc", 6).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1e10x", 5).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("3.14!", 5).ok());
}

TEST_F(SIMDDoubleErrorTest, EmbeddedWhitespace) {
  // Whitespace within the number is invalid
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1 .5", 4).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1. 5", 4).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("1e 10", 5).ok());
}

TEST_F(SIMDDoubleErrorTest, InvalidInfinityVariants) {
  // Partial or invalid infinity strings
  EXPECT_FALSE(SIMDDoubleParser::parse_double("in", 2).ok());      // Too short
  EXPECT_FALSE(SIMDDoubleParser::parse_double("infinit", 7).ok()); // Incomplete "infinity"
  EXPECT_FALSE(SIMDDoubleParser::parse_double("inff", 4).ok());    // Invalid
}

TEST_F(SIMDDoubleErrorTest, InvalidNaNVariants) {
  EXPECT_FALSE(SIMDDoubleParser::parse_double("na", 2).ok());   // Too short
  EXPECT_FALSE(SIMDDoubleParser::parse_double("nana", 4).ok()); // Invalid
  EXPECT_FALSE(SIMDDoubleParser::parse_double("nanx", 4).ok()); // Trailing char
}

// =============================================================================
// Double Parser Whitespace Tests
// =============================================================================

class SIMDDoubleWhitespaceTest : public ::testing::Test {};

TEST_F(SIMDDoubleWhitespaceTest, LeadingWhitespace) {
  EXPECT_NEAR(SIMDDoubleParser::parse_double(" 3.14", 5).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("  3.14", 6).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("\t3.14", 5).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double(" \t 3.14", 7).get(), 3.14, 0.001);
}

TEST_F(SIMDDoubleWhitespaceTest, TrailingWhitespace) {
  EXPECT_NEAR(SIMDDoubleParser::parse_double("3.14 ", 5).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("3.14  ", 6).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("3.14\t", 5).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("3.14 \t ", 7).get(), 3.14, 0.001);
}

TEST_F(SIMDDoubleWhitespaceTest, BothSidesWhitespace) {
  EXPECT_NEAR(SIMDDoubleParser::parse_double(" 3.14 ", 6).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("  3.14  ", 8).get(), 3.14, 0.001);
  EXPECT_NEAR(SIMDDoubleParser::parse_double("\t3.14\t", 6).get(), 3.14, 0.001);
}

TEST_F(SIMDDoubleWhitespaceTest, WhitespaceOnlyIsNA) {
  EXPECT_TRUE(SIMDDoubleParser::parse_double(" ", 1).is_na());
  EXPECT_TRUE(SIMDDoubleParser::parse_double("  ", 2).is_na());
  EXPECT_TRUE(SIMDDoubleParser::parse_double("\t", 1).is_na());
  EXPECT_TRUE(SIMDDoubleParser::parse_double(" \t ", 3).is_na());
}

TEST_F(SIMDDoubleWhitespaceTest, DisabledWhitespaceTrimming) {
  EXPECT_FALSE(SIMDDoubleParser::parse_double(" 3.14", 5, false).ok());
  EXPECT_FALSE(SIMDDoubleParser::parse_double("3.14 ", 5, false).ok());

  // But plain numbers still work
  auto result = SIMDDoubleParser::parse_double("3.14", 4, false);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 3.14, 0.001);
}

// =============================================================================
// Double Parser Special Values Case Variations
// =============================================================================

class SIMDDoubleSpecialValuesTest : public ::testing::Test {};

TEST_F(SIMDDoubleSpecialValuesTest, NaNCaseVariations) {
  // Various case combinations
  EXPECT_TRUE(std::isnan(SIMDDoubleParser::parse_double("NaN", 3).get()));
  EXPECT_TRUE(std::isnan(SIMDDoubleParser::parse_double("nan", 3).get()));
  EXPECT_TRUE(std::isnan(SIMDDoubleParser::parse_double("NAN", 3).get()));
  EXPECT_TRUE(std::isnan(SIMDDoubleParser::parse_double("naN", 3).get()));
  EXPECT_TRUE(std::isnan(SIMDDoubleParser::parse_double("NAn", 3).get()));
}

TEST_F(SIMDDoubleSpecialValuesTest, InfinityCaseVariations) {
  // "inf" variations
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("Inf", 3).get()));
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("inf", 3).get()));
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("INF", 3).get()));
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("iNf", 3).get()));

  // "infinity" variations
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("Infinity", 8).get()));
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("infinity", 8).get()));
  EXPECT_TRUE(std::isinf(SIMDDoubleParser::parse_double("INFINITY", 8).get()));
}

TEST_F(SIMDDoubleSpecialValuesTest, NegativeInfinityCaseVariations) {
  // -inf variations
  auto result1 = SIMDDoubleParser::parse_double("-Inf", 4);
  EXPECT_TRUE(std::isinf(result1.get()));
  EXPECT_LT(result1.get(), 0);

  auto result2 = SIMDDoubleParser::parse_double("-inf", 4);
  EXPECT_TRUE(std::isinf(result2.get()));
  EXPECT_LT(result2.get(), 0);

  auto result3 = SIMDDoubleParser::parse_double("-INF", 4);
  EXPECT_TRUE(std::isinf(result3.get()));
  EXPECT_LT(result3.get(), 0);

  // -infinity variations
  auto result4 = SIMDDoubleParser::parse_double("-Infinity", 9);
  EXPECT_TRUE(std::isinf(result4.get()));
  EXPECT_LT(result4.get(), 0);

  auto result5 = SIMDDoubleParser::parse_double("-infinity", 9);
  EXPECT_TRUE(std::isinf(result5.get()));
  EXPECT_LT(result5.get(), 0);

  auto result6 = SIMDDoubleParser::parse_double("-INFINITY", 9);
  EXPECT_TRUE(std::isinf(result6.get()));
  EXPECT_LT(result6.get(), 0);
}

TEST_F(SIMDDoubleSpecialValuesTest, PositiveInfinityWithPlusSign) {
  // The parser checks for '-inf' but not '+inf' as a special value
  // '+inf' should be treated as '+' followed by 'inf' which fails
  auto result = SIMDDoubleParser::parse_double("+inf", 4);
  EXPECT_FALSE(result.ok()); // '+inf' is not a recognized special value
}

// =============================================================================
// Double Parser Column Batch Tests
// =============================================================================

class SIMDDoubleColumnTest : public ::testing::Test {};

TEST_F(SIMDDoubleColumnTest, ParseDoubleColumnVector) {
  const char* fields[] = {"1.5", "-2.5", "nan", "", "inf", "1e-10"};
  size_t lengths[] = {3, 4, 3, 0, 3, 5};

  auto results = SIMDDoubleParser::parse_double_column(fields, lengths, 6);

  EXPECT_EQ(results.size(), 6);
  EXPECT_TRUE(results[0].has_value());
  EXPECT_NEAR(*results[0], 1.5, 0.001);
  EXPECT_TRUE(results[1].has_value());
  EXPECT_NEAR(*results[1], -2.5, 0.001);
  EXPECT_TRUE(results[2].has_value());
  EXPECT_TRUE(std::isnan(*results[2]));
  EXPECT_FALSE(results[3].has_value()); // Empty
  EXPECT_TRUE(results[4].has_value());
  EXPECT_TRUE(std::isinf(*results[4]));
  EXPECT_TRUE(results[5].has_value());
  EXPECT_NEAR(*results[5], 1e-10, 1e-15);
}

// =============================================================================
// SIMD Type Validator Tests
// =============================================================================

class SIMDTypeValidatorTest : public ::testing::Test {};

// Integer validation
TEST_F(SIMDTypeValidatorTest, CouldBeIntegerPositive) {
  const uint8_t data[] = "12345";
  EXPECT_TRUE(SIMDTypeValidator::could_be_integer(data, 5));
}

TEST_F(SIMDTypeValidatorTest, CouldBeIntegerNegative) {
  const uint8_t data[] = "-12345";
  EXPECT_TRUE(SIMDTypeValidator::could_be_integer(data, 6));
}

TEST_F(SIMDTypeValidatorTest, CouldBeIntegerWithWhitespace) {
  const uint8_t data[] = "  123  ";
  EXPECT_TRUE(SIMDTypeValidator::could_be_integer(data, 7));
}

TEST_F(SIMDTypeValidatorTest, NotIntegerWithDecimal) {
  const uint8_t data[] = "12.34";
  EXPECT_FALSE(SIMDTypeValidator::could_be_integer(data, 5));
}

TEST_F(SIMDTypeValidatorTest, NotIntegerWithLetters) {
  const uint8_t data[] = "12abc";
  EXPECT_FALSE(SIMDTypeValidator::could_be_integer(data, 5));
}

// Float validation
TEST_F(SIMDTypeValidatorTest, CouldBeFloatDecimal) {
  const uint8_t data[] = "3.14";
  EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 4));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatScientific) {
  const uint8_t data[] = "1e10";
  EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 4));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatNaN) {
  const uint8_t data[] = "nan";
  EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 3));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatInf) {
  const uint8_t data[] = "inf";
  EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 3));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatNegInf) {
  const uint8_t data[] = "-infinity";
  EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 9));
}

TEST_F(SIMDTypeValidatorTest, NotFloatJustInteger) {
  const uint8_t data[] = "12345";
  EXPECT_FALSE(SIMDTypeValidator::could_be_float(data, 5)); // Integer, not float
}

TEST_F(SIMDTypeValidatorTest, NotFloatString) {
  const uint8_t data[] = "hello";
  EXPECT_FALSE(SIMDTypeValidator::could_be_float(data, 5));
}

// Batch validation
TEST_F(SIMDTypeValidatorTest, ValidateBatch) {
  const uint8_t* fields[] = {
      reinterpret_cast<const uint8_t*>("123"), reinterpret_cast<const uint8_t*>("3.14"),
      reinterpret_cast<const uint8_t*>("hello"), reinterpret_cast<const uint8_t*>("-456"),
      reinterpret_cast<const uint8_t*>("1e10")};
  size_t lengths[] = {3, 4, 5, 4, 4};

  size_t int_count, float_count, other_count;
  SIMDTypeValidator::validate_batch(fields, lengths, 5, int_count, float_count, other_count);

  EXPECT_EQ(int_count, 2);   // "123" and "-456"
  EXPECT_EQ(float_count, 2); // "3.14" and "1e10"
  EXPECT_EQ(other_count, 1); // "hello"
}

// =============================================================================
// SIMD DateTime Parser Tests
// =============================================================================

class SIMDDateTimeParserTest : public ::testing::Test {};

// Basic date parsing
TEST_F(SIMDDateTimeParserTest, ParseISODate) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15", 10);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 1);
  EXPECT_EQ(dt.day, 15);
}

TEST_F(SIMDDateTimeParserTest, ParseCompactDate) {
  auto result = SIMDDateTimeParser::parse_datetime("20240115", 8);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 1);
  EXPECT_EQ(dt.day, 15);
}

// Date and time
TEST_F(SIMDDateTimeParserTest, ParseDateTimeT) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45", 19);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 1);
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
  EXPECT_EQ(dt.second, 45);
}

TEST_F(SIMDDateTimeParserTest, ParseDateTimeSpace) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15 14:30:45", 19);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
  EXPECT_EQ(dt.second, 45);
}

// Fractional seconds
TEST_F(SIMDDateTimeParserTest, ParseFractionalSeconds) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45.123", 23);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.nanoseconds, 123000000);
}

TEST_F(SIMDDateTimeParserTest, ParseFractionalSecondsNano) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45.123456789", 29);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.nanoseconds, 123456789);
}

// Timezone
TEST_F(SIMDDateTimeParserTest, ParseTimezoneZ) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45Z", 20);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.tz_offset_minutes, 0);
}

TEST_F(SIMDDateTimeParserTest, ParseTimezonePositive) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45+05:30", 25);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.tz_offset_minutes, 5 * 60 + 30);
}

TEST_F(SIMDDateTimeParserTest, ParseTimezoneNegative) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45-08:00", 25);
  EXPECT_TRUE(result.ok());
  auto dt = result.get();
  EXPECT_EQ(dt.tz_offset_minutes, -(8 * 60));
}

// Date validation
TEST_F(SIMDDateTimeParserTest, InvalidMonth) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-13-15", 10);
  EXPECT_FALSE(result.ok());
}

TEST_F(SIMDDateTimeParserTest, InvalidDay) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-02-30", 10);
  EXPECT_FALSE(result.ok());
}

TEST_F(SIMDDateTimeParserTest, ValidLeapDay) {
  auto result = SIMDDateTimeParser::parse_datetime("2024-02-29", 10);
  EXPECT_TRUE(result.ok());
}

TEST_F(SIMDDateTimeParserTest, InvalidLeapDay) {
  auto result = SIMDDateTimeParser::parse_datetime("2023-02-29", 10);
  EXPECT_FALSE(result.ok());
}

// Timezone limit tests (UTC-12 to UTC+14)
TEST_F(SIMDDateTimeParserTest, TimezoneMaxPositive) {
  // UTC+14:00 (Line Islands, Kiribati)
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45+14:00", 25);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get().tz_offset_minutes, 14 * 60);
}

TEST_F(SIMDDateTimeParserTest, TimezoneMaxNegative) {
  // UTC-12:00 (Baker Island)
  auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45-12:00", 25);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get().tz_offset_minutes, -12 * 60);
}

// NA handling
TEST_F(SIMDDateTimeParserTest, EmptyIsNA) {
  auto result = SIMDDateTimeParser::parse_datetime("", 0);
  EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDDateTimeParserTest, WhitespaceIsNA) {
  auto result = SIMDDateTimeParser::parse_datetime("   ", 3);
  EXPECT_TRUE(result.is_na());
}

// Column parsing
TEST_F(SIMDDateTimeParserTest, ParseDateTimeColumn) {
  const char* fields[] = {"2024-01-15", "2024-02-20", "", "invalid"};
  size_t lengths[] = {10, 10, 0, 7};

  auto results = SIMDDateTimeParser::parse_datetime_column(fields, lengths, 4);

  EXPECT_EQ(results.size(), 4);
  EXPECT_TRUE(results[0].has_value());
  EXPECT_EQ(results[0]->month, 1);
  EXPECT_TRUE(results[1].has_value());
  EXPECT_EQ(results[1]->month, 2);
  EXPECT_FALSE(results[2].has_value()); // Empty
  EXPECT_FALSE(results[3].has_value()); // Invalid
}

// =============================================================================
// SIMDParseResult Tests
// =============================================================================

class SIMDParseResultTest : public ::testing::Test {};

TEST_F(SIMDParseResultTest, SuccessResult) {
  auto result = SIMDParseResult<int>::success(42);
  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(result.is_na());
  EXPECT_EQ(result.get(), 42);
  EXPECT_EQ(result.get_or(0), 42);
}

TEST_F(SIMDParseResultTest, FailureResult) {
  auto result = SIMDParseResult<int>::failure("test error");
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(result.is_na());
  EXPECT_STREQ(result.error, "test error");
  EXPECT_EQ(result.get_or(99), 99);
}

TEST_F(SIMDParseResultTest, NAResult) {
  auto result = SIMDParseResult<int>::na();
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.is_na());
  EXPECT_EQ(result.error, nullptr);
  EXPECT_EQ(result.get_or(99), 99);
}

TEST_F(SIMDParseResultTest, ToExtractResult) {
  auto simd_result = SIMDParseResult<int64_t>::success(42);
  auto extract_result = simd_result.to_extract_result();
  EXPECT_TRUE(extract_result.ok());
  EXPECT_EQ(extract_result.get(), 42);
}

TEST_F(SIMDParseResultTest, GetThrowsOnFailure) {
  auto result = SIMDParseResult<int>::failure("error");
  EXPECT_THROW(result.get(), std::runtime_error);
}

// =============================================================================
// Performance comparison helpers (not benchmarks, just functional tests)
// =============================================================================

class SIMDPerformanceTest : public ::testing::Test {};

TEST_F(SIMDPerformanceTest, ParseManyIntegers) {
  // Test that we can parse many integers correctly
  std::vector<std::string> numbers;
  for (int i = -1000; i <= 1000; ++i) {
    numbers.push_back(std::to_string(i));
  }

  for (int i = -1000; i <= 1000; ++i) {
    const std::string& s = numbers[i + 1000];
    auto result = SIMDIntegerParser::parse_int64(s.c_str(), s.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << s;
    EXPECT_EQ(result.get(), i) << "Wrong value for: " << s;
  }
}

TEST_F(SIMDPerformanceTest, ParseManyDoubles) {
  // Test that we can parse various double formats correctly
  std::vector<std::pair<std::string, double>> test_cases = {
      {"0", 0.0},         {"1", 1.0},
      {"-1", -1.0},       {"0.5", 0.5},
      {"-0.5", -0.5},     {"123.456", 123.456},
      {"1e5", 1e5},       {"1e-5", 1e-5},
      {"1.5e10", 1.5e10}, {"-1.5e-10", -1.5e-10}};

  for (const auto& [str, expected] : test_cases) {
    auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
    EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
    EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-10 + 1e-15)
        << "Wrong value for: " << str;
  }
}

// =============================================================================
// SIMD Value Extraction Integration Tests
// =============================================================================

class SIMDValueExtractionTest : public ::testing::Test {
protected:
  ExtractionConfig config_ = ExtractionConfig::defaults();
};

// Test parse_integer_simd with ExtractionConfig
TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDBasic) {
  auto result = parse_integer_simd<int64_t>("12345", 5, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDNegative) {
  auto result = parse_integer_simd<int64_t>("-12345", 6, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), -12345);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDWithWhitespace) {
  auto result = parse_integer_simd<int64_t>("  42  ", 6, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 42);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDNAValue) {
  auto result = parse_integer_simd<int64_t>("NA", 2, config_);
  EXPECT_TRUE(result.is_na());
  EXPECT_FALSE(result.ok());
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDEmptyIsNA) {
  auto result = parse_integer_simd<int64_t>("", 0, config_);
  EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDInt32) {
  auto result = parse_integer_simd<int32_t>("12345", 5, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDInt32Overflow) {
  auto result = parse_integer_simd<int32_t>("9999999999", 10, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDInt32Underflow) {
  auto result = parse_integer_simd<int32_t>("-2147483649", 11, config_); // INT32_MIN - 1
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDUInt32) {
  auto result = parse_integer_simd<uint32_t>("12345", 5, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345u);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDUInt32Overflow) {
  auto result = parse_integer_simd<uint32_t>("4294967296", 10, config_); // UINT32_MAX + 1
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDUInt32Negative) {
  auto result = parse_integer_simd<uint32_t>("-1", 2, config_);
  EXPECT_FALSE(result.ok());
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDUInt64) {
  auto result = parse_integer_simd<uint64_t>("18446744073709551615", 20, config_); // UINT64_MAX
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), UINT64_MAX);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDWhitespaceOnly) {
  auto result = parse_integer_simd<int64_t>("   ", 3, config_);
  EXPECT_TRUE(result.is_na()); // After trimming, empty = NA
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDWhitespaceOnly) {
  auto result = parse_double_simd("   ", 3, config_);
  EXPECT_TRUE(result.is_na()); // After trimming, empty = NA
}

// Test parse_double_simd with ExtractionConfig
TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDBasic) {
  auto result = parse_double_simd("3.14159", 7, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 3.14159, 0.00001);
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDScientific) {
  auto result = parse_double_simd("1.5e10", 6, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 1.5e10, 1e5);
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDNaN) {
  auto result = parse_double_simd("NaN", 3, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isnan(result.get()));
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDNaNNotTreatedAsNA) {
  // NaN should be parsed as the float value, not as NA
  auto result = parse_double_simd("NaN", 3, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(result.is_na());
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDNAValue) {
  // Note: parse_double_simd doesn't check NA values (matching scalar behavior)
  // It returns a parse error, not NA
  auto result = parse_double_simd("NA", 2, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(result.is_na()); // It's a parse error, not NA
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDEmptyIsNA) {
  auto result = parse_double_simd("", 0, config_);
  EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDWithWhitespace) {
  auto result = parse_double_simd("  3.14  ", 8, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 3.14, 0.001);
}

// Test extract_value_simd
TEST_F(SIMDValueExtractionTest, ExtractValueSIMDInt64) {
  auto result = extract_value_simd<int64_t>("12345", 5, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDValueExtractionTest, ExtractValueSIMDDouble) {
  auto result = extract_value_simd<double>("3.14", 4, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_NEAR(result.get(), 3.14, 0.001);
}

TEST_F(SIMDValueExtractionTest, ExtractValueSIMDBool) {
  auto result = extract_value_simd<bool>("true", 4, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(result.get());
}

TEST_F(SIMDValueExtractionTest, ExtractValueSIMDInt32) {
  auto result = extract_value_simd<int32_t>("42", 2, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 42);
}

// Test that SIMD and scalar produce equivalent results
TEST_F(SIMDValueExtractionTest, SIMDEquivalentToScalar) {
  std::vector<std::string> test_values = {
      "0",
      "1",
      "-1",
      "42",
      "-42",
      "12345",
      "-12345",
      "9223372036854775807", // INT64_MAX
      "-9223372036854775808" // INT64_MIN
  };

  for (const auto& value : test_values) {
    auto scalar_result = parse_integer<int64_t>(value.c_str(), value.size(), config_);
    auto simd_result = parse_integer_simd<int64_t>(value.c_str(), value.size(), config_);

    EXPECT_EQ(scalar_result.ok(), simd_result.ok()) << "Mismatch for: " << value;
    if (scalar_result.ok() && simd_result.ok()) {
      EXPECT_EQ(scalar_result.get(), simd_result.get()) << "Value mismatch for: " << value;
    }
  }
}

// Test that SIMD respects max_integer_digits config (GitHub issue #95)
TEST_F(SIMDValueExtractionTest, SIMDRespectsMaxIntegerDigits) {
  ExtractionConfig custom_config;
  custom_config.max_integer_digits = 10; // Restrict to 10 digits

  // 10-digit number should parse successfully
  auto result_ok = parse_integer_simd<int64_t>("1234567890", 10, custom_config);
  EXPECT_TRUE(result_ok.ok());
  EXPECT_EQ(result_ok.get(), 1234567890);

  // 12-digit number should fail with "Integer too large"
  auto result_fail = parse_integer_simd<int64_t>("123456789012", 12, custom_config);
  EXPECT_FALSE(result_fail.ok());
  EXPECT_NE(result_fail.error, nullptr);
  EXPECT_STREQ(result_fail.error, "Integer too large");

  // Verify SIMD behavior matches scalar behavior
  auto scalar_fail = parse_integer<int64_t>("123456789012", 12, custom_config);
  EXPECT_FALSE(scalar_fail.ok());
  EXPECT_STREQ(scalar_fail.error, "Integer too large");
}

TEST_F(SIMDValueExtractionTest, SIMDRespectsMaxIntegerDigitsWithSign) {
  ExtractionConfig custom_config;
  custom_config.max_integer_digits = 10;

  // Negative 10-digit number should parse successfully (sign doesn't count)
  auto result_ok = parse_integer_simd<int64_t>("-1234567890", 11, custom_config);
  EXPECT_TRUE(result_ok.ok());
  EXPECT_EQ(result_ok.get(), -1234567890);

  // Negative 12-digit number should fail
  auto result_fail = parse_integer_simd<int64_t>("-123456789012", 13, custom_config);
  EXPECT_FALSE(result_fail.ok());
  EXPECT_STREQ(result_fail.error, "Integer too large");

  // With + sign
  auto result_plus_ok = parse_integer_simd<int64_t>("+1234567890", 11, custom_config);
  EXPECT_TRUE(result_plus_ok.ok());
  EXPECT_EQ(result_plus_ok.get(), 1234567890);

  auto result_plus_fail = parse_integer_simd<int64_t>("+123456789012", 13, custom_config);
  EXPECT_FALSE(result_plus_fail.ok());
  EXPECT_STREQ(result_plus_fail.error, "Integer too large");
}

TEST_F(SIMDValueExtractionTest, SIMDRespectsMaxIntegerDigitsDefault) {
  // Default max_integer_digits is 20
  ExtractionConfig default_config = ExtractionConfig::defaults();
  EXPECT_EQ(default_config.max_integer_digits, 20);

  // 20-digit number within default limit (UINT64_MAX is 20 digits)
  auto result_ok = parse_integer_simd<uint64_t>("18446744073709551615", 20, default_config);
  EXPECT_TRUE(result_ok.ok());
  EXPECT_EQ(result_ok.get(), UINT64_MAX);

  // 21-digit number should fail due to max_integer_digits
  auto result_fail = parse_integer_simd<uint64_t>("123456789012345678901", 21, default_config);
  EXPECT_FALSE(result_fail.ok());
  EXPECT_STREQ(result_fail.error, "Integer too large");
}

TEST_F(SIMDValueExtractionTest, SIMDDoubleEquivalentToScalar) {
  std::vector<std::string> test_values = {"0",     "0.0",  "1",        "-1",       "3.14",
                                          "-3.14", "1e10", "1e-10",    "1.5e10",   "-1.5e-10",
                                          "Inf",   "-Inf", "Infinity", "-Infinity"};

  for (const auto& value : test_values) {
    auto scalar_result = parse_double(value.c_str(), value.size(), config_);
    auto simd_result = parse_double_simd(value.c_str(), value.size(), config_);

    EXPECT_EQ(scalar_result.ok(), simd_result.ok()) << "Mismatch for: " << value;
    if (scalar_result.ok() && simd_result.ok()) {
      if (std::isnan(scalar_result.get())) {
        EXPECT_TRUE(std::isnan(simd_result.get())) << "NaN mismatch for: " << value;
      } else if (std::isinf(scalar_result.get())) {
        EXPECT_TRUE(std::isinf(simd_result.get())) << "Inf mismatch for: " << value;
        EXPECT_EQ(std::signbit(scalar_result.get()), std::signbit(simd_result.get()))
            << "Inf sign mismatch for: " << value;
      } else {
        EXPECT_NEAR(scalar_result.get(), simd_result.get(),
                    std::abs(scalar_result.get()) * 1e-10 + 1e-15)
            << "Value mismatch for: " << value;
      }
    }
  }
}

// =============================================================================
// Leading Zeros Validation Tests (parse_integer_simd)
// =============================================================================

TEST_F(SIMDValueExtractionTest, AllowLeadingZerosDefault) {
  // By default, leading zeros are allowed
  EXPECT_TRUE(config_.allow_leading_zeros);

  // Leading zeros should parse successfully with default config
  auto result = parse_integer_simd<int64_t>("007", 3, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 7);

  result = parse_integer_simd<int64_t>("0123", 4, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123);

  result = parse_integer_simd<int64_t>("-007", 4, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), -7);

  result = parse_integer_simd<int64_t>("+007", 4, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 7);

  // Also test unsigned integers with default config (covers A=false branch for uint64_t)
  auto uresult = parse_integer_simd<uint64_t>("007", 3, config_);
  EXPECT_TRUE(uresult.ok());
  EXPECT_EQ(uresult.get(), 7u);

  // Test int32_t and uint32_t with default config too
  auto i32result = parse_integer_simd<int32_t>("007", 3, config_);
  EXPECT_TRUE(i32result.ok());
  EXPECT_EQ(i32result.get(), 7);

  auto u32result = parse_integer_simd<uint32_t>("007", 3, config_);
  EXPECT_TRUE(u32result.ok());
  EXPECT_EQ(u32result.get(), 7u);
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZeros) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Leading zeros should be rejected
  auto result = parse_integer_simd<int64_t>("007", 3, config);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
  EXPECT_STREQ(result.error, "Leading zeros not allowed");

  result = parse_integer_simd<int64_t>("0123", 4, config);
  EXPECT_FALSE(result.ok());

  // With negative sign
  result = parse_integer_simd<int64_t>("-007", 4, config);
  EXPECT_FALSE(result.ok());

  // With positive sign
  result = parse_integer_simd<int64_t>("+007", 4, config);
  EXPECT_FALSE(result.ok());
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZerosSingleZeroAllowed) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Single zero is not a leading zero - it's the number itself
  auto result = parse_integer_simd<int64_t>("0", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0);

  result = parse_integer_simd<int64_t>("-0", 2, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0);

  result = parse_integer_simd<int64_t>("+0", 2, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0);
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZerosRegularNumbers) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Numbers without leading zeros should still parse
  auto result = parse_integer_simd<int64_t>("123", 3, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123);

  result = parse_integer_simd<int64_t>("-456", 4, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), -456);

  result = parse_integer_simd<int64_t>("+789", 4, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 789);

  result = parse_integer_simd<int64_t>("10", 2, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 10);
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZerosUnsigned) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Unsigned integers with leading zeros should be rejected
  auto result = parse_integer_simd<uint64_t>("007", 3, config);
  EXPECT_FALSE(result.ok());

  // Without leading zeros should work
  result = parse_integer_simd<uint64_t>("7", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 7u);

  result = parse_integer_simd<uint64_t>("0", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0u);

  // Multi-digit numbers not starting with 0 should work (covers C=false branch for uint64_t)
  result = parse_integer_simd<uint64_t>("123", 3, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123u);

  result = parse_integer_simd<uint64_t>("10", 2, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 10u);
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZerosWithWhitespace) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Leading zeros should still be detected after whitespace trimming
  auto result = parse_integer_simd<int64_t>("  007  ", 7, config);
  EXPECT_FALSE(result.ok());

  // But valid numbers with whitespace should work
  result = parse_integer_simd<int64_t>("  7  ", 5, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 7);
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZerosInt32) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Test with int32_t type - covers A=true, B=true, C=true (error case)
  auto result = parse_integer_simd<int32_t>("007", 3, config);
  EXPECT_FALSE(result.ok());

  // Covers A=true, B=true, C=false (multi-digit not starting with 0)
  result = parse_integer_simd<int32_t>("123", 3, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123);

  // Covers A=true, B=false (single digit)
  result = parse_integer_simd<int32_t>("0", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0);

  result = parse_integer_simd<int32_t>("5", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 5);
}

TEST_F(SIMDValueExtractionTest, DisallowLeadingZerosUint32) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Test with uint32_t type - covers A=true, B=true, C=true (error case)
  auto result = parse_integer_simd<uint32_t>("007", 3, config);
  EXPECT_FALSE(result.ok());

  // Covers A=true, B=true, C=false (multi-digit not starting with 0)
  result = parse_integer_simd<uint32_t>("123", 3, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 123u);

  // Covers A=true, B=false (single digit)
  result = parse_integer_simd<uint32_t>("0", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 0u);

  result = parse_integer_simd<uint32_t>("7", 1, config);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 7u);
}

TEST_F(SIMDValueExtractionTest, LeadingZerosEquivalentToScalar) {
  // Test that SIMD and scalar parsers behave identically for leading zeros
  std::vector<std::string> test_values = {"0",    "00",  "007", "0123", "-007",
                                          "+007", "123", "-0",  "+0",   "10"};

  // Test with allow_leading_zeros = true
  for (const auto& value : test_values) {
    auto scalar_result = parse_integer<int64_t>(value.c_str(), value.size(), config_);
    auto simd_result = parse_integer_simd<int64_t>(value.c_str(), value.size(), config_);

    EXPECT_EQ(scalar_result.ok(), simd_result.ok())
        << "Mismatch for: " << value << " (allow_leading_zeros=true)";
    if (scalar_result.ok() && simd_result.ok()) {
      EXPECT_EQ(scalar_result.get(), simd_result.get())
          << "Value mismatch for: " << value << " (allow_leading_zeros=true)";
    }
  }

  // Test with allow_leading_zeros = false
  ExtractionConfig no_leading_zeros_config;
  no_leading_zeros_config.allow_leading_zeros = false;

  for (const auto& value : test_values) {
    auto scalar_result =
        parse_integer<int64_t>(value.c_str(), value.size(), no_leading_zeros_config);
    auto simd_result =
        parse_integer_simd<int64_t>(value.c_str(), value.size(), no_leading_zeros_config);

    EXPECT_EQ(scalar_result.ok(), simd_result.ok())
        << "Mismatch for: " << value << " (allow_leading_zeros=false)";
    if (scalar_result.ok() && simd_result.ok()) {
      EXPECT_EQ(scalar_result.get(), simd_result.get())
          << "Value mismatch for: " << value << " (allow_leading_zeros=false)";
    }
    if (!scalar_result.ok() && !simd_result.ok() && scalar_result.error && simd_result.error) {
      EXPECT_STREQ(scalar_result.error, simd_result.error)
          << "Error message mismatch for: " << value;
    }
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
