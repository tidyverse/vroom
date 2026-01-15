#include "libvroom.h"

#include "mem_util.h"
#include "value_extraction.h"

#include <cmath>
#include <cstring>
#include <gtest/gtest.h>
#include <limits>

using namespace libvroom;

class TestBuffer {
public:
  explicit TestBuffer(const std::string& content) : content_(content) {
    buffer_ = new uint8_t[content.size() + 64];
    std::memcpy(buffer_, content.data(), content.size());
    std::memset(buffer_ + content.size(), 0, 64);
  }
  ~TestBuffer() { delete[] buffer_; }
  const uint8_t* data() const { return buffer_; }
  size_t size() const { return content_.size(); }

private:
  std::string content_;
  uint8_t* buffer_;
};

class ExtractResultTest : public ::testing::Test {};

TEST_F(ExtractResultTest, OkResult) {
  ExtractResult<int64_t> result{42, nullptr};
  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(result.is_na());
  EXPECT_EQ(result.get(), 42);
  EXPECT_EQ(result.get_or(0), 42);
}

TEST_F(ExtractResultTest, NAResult) {
  ExtractResult<int64_t> result{std::nullopt, nullptr};
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.is_na());
  EXPECT_THROW(result.get(), std::runtime_error);
  EXPECT_EQ(result.get_or(-1), -1);
}

TEST_F(ExtractResultTest, ErrorResult) {
  ExtractResult<int64_t> result{std::nullopt, "Some error"};
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(result.is_na());
  EXPECT_THROW(result.get(), std::runtime_error);
  EXPECT_EQ(result.get_or(-1), -1);
}

TEST_F(ExtractResultTest, GetWithErrorMessage) {
  ExtractResult<int64_t> result{std::nullopt, "Custom error message"};
  EXPECT_THROW(
      {
        try {
          result.get();
        } catch (const std::runtime_error& e) {
          EXPECT_STREQ(e.what(), "Custom error message");
          throw;
        }
      },
      std::runtime_error);
}

class IntegerParsingTest : public ::testing::Test {
protected:
  ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(IntegerParsingTest, ParseZero) {
  EXPECT_EQ(parse_integer<int64_t>("0", 1, config_).get(), 0);
}

TEST_F(IntegerParsingTest, ParsePositive) {
  EXPECT_EQ(parse_integer<int64_t>("12345", 5, config_).get(), 12345);
}

TEST_F(IntegerParsingTest, ParseNegative) {
  EXPECT_EQ(parse_integer<int64_t>("-12345", 6, config_).get(), -12345);
}

TEST_F(IntegerParsingTest, EmptyIsNA) {
  EXPECT_TRUE(parse_integer<int64_t>("", 0, config_).is_na());
}

TEST_F(IntegerParsingTest, Int64Max) {
  EXPECT_EQ(parse_integer<int64_t>("9223372036854775807", 19, config_).get(), INT64_MAX);
}

TEST_F(IntegerParsingTest, Int64Min) {
  EXPECT_EQ(parse_integer<int64_t>("-9223372036854775808", 20, config_).get(), INT64_MIN);
}

TEST_F(IntegerParsingTest, Int64Overflow) {
  auto result = parse_integer<int64_t>("9223372036854775808", 19, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, Int64Underflow) {
  auto result = parse_integer<int64_t>("-9223372036854775809", 20, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, Int32Max) {
  EXPECT_EQ(parse_integer<int32_t>("2147483647", 10, config_).get(), INT32_MAX);
}

TEST_F(IntegerParsingTest, Int32Min) {
  EXPECT_EQ(parse_integer<int32_t>("-2147483648", 11, config_).get(), INT32_MIN);
}

TEST_F(IntegerParsingTest, Int32Overflow) {
  auto result = parse_integer<int32_t>("2147483648", 10, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, UnsignedNegative) {
  auto result = parse_integer<uint64_t>("-1", 2, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, WhitespaceTrimming) {
  EXPECT_EQ(parse_integer<int64_t>("  42  ", 6, config_).get(), 42);
}

TEST_F(IntegerParsingTest, PositiveSign) {
  EXPECT_EQ(parse_integer<int64_t>("+12345", 6, config_).get(), 12345);
}

TEST_F(IntegerParsingTest, PositiveSignUnsigned) {
  EXPECT_EQ(parse_integer<uint64_t>("+999", 4, config_).get(), 999u);
}

TEST_F(IntegerParsingTest, UInt64Max) {
  EXPECT_EQ(parse_integer<uint64_t>("18446744073709551615", 20, config_).get(), UINT64_MAX);
}

TEST_F(IntegerParsingTest, UInt64Overflow) {
  auto result = parse_integer<uint64_t>("18446744073709551616", 20, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, TooManyDigits) {
  auto result = parse_integer<int64_t>("123456789012345678901", 21, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, InvalidCharacter) {
  auto result = parse_integer<int64_t>("12a34", 5, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, JustSign) {
  auto result = parse_integer<int64_t>("-", 1, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, JustPlusSign) {
  auto result = parse_integer<int64_t>("+", 1, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, NAValue) {
  EXPECT_TRUE(parse_integer<int64_t>("NA", 2, config_).is_na());
  EXPECT_TRUE(parse_integer<int64_t>("N/A", 3, config_).is_na());
  EXPECT_TRUE(parse_integer<int64_t>("null", 4, config_).is_na());
  EXPECT_TRUE(parse_integer<int64_t>("NULL", 4, config_).is_na());
  EXPECT_TRUE(parse_integer<int64_t>("None", 4, config_).is_na());
}

TEST_F(IntegerParsingTest, WhitespaceOnly) {
  EXPECT_TRUE(parse_integer<int64_t>("   ", 3, config_).is_na());
}

TEST_F(IntegerParsingTest, TabWhitespace) {
  EXPECT_EQ(parse_integer<int64_t>("\t42\t", 4, config_).get(), 42);
}

TEST_F(IntegerParsingTest, Int32Underflow) {
  auto result = parse_integer<int32_t>("-2147483649", 11, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, Int16Max) {
  EXPECT_EQ(parse_integer<int16_t>("32767", 5, config_).get(), INT16_MAX);
}

TEST_F(IntegerParsingTest, Int16Overflow) {
  auto result = parse_integer<int16_t>("32768", 5, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(IntegerParsingTest, UInt32Max) {
  EXPECT_EQ(parse_integer<uint32_t>("4294967295", 10, config_).get(), UINT32_MAX);
}

TEST_F(IntegerParsingTest, UInt32Overflow) {
  auto result = parse_integer<uint32_t>("4294967296", 10, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

class DoubleParsingTest : public ::testing::Test {
protected:
  ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(DoubleParsingTest, ParseDecimal) {
  EXPECT_NEAR(parse_double("3.14", 4, config_).get(), 3.14, 0.01);
}

TEST_F(DoubleParsingTest, ParseScientific) {
  EXPECT_NEAR(parse_double("1e10", 4, config_).get(), 1e10, 1e5);
}

TEST_F(DoubleParsingTest, ParseNaN) {
  EXPECT_TRUE(std::isnan(parse_double("NaN", 3, config_).get()));
}

TEST_F(DoubleParsingTest, ParseNaNCaseInsensitive) {
  EXPECT_TRUE(std::isnan(parse_double("nan", 3, config_).get()));
  EXPECT_TRUE(std::isnan(parse_double("NAN", 3, config_).get()));
}

TEST_F(DoubleParsingTest, ParseInf) {
  EXPECT_TRUE(std::isinf(parse_double("Inf", 3, config_).get()));
  EXPECT_GT(parse_double("Inf", 3, config_).get(), 0);
}

TEST_F(DoubleParsingTest, ParseInfinity) {
  EXPECT_TRUE(std::isinf(parse_double("Infinity", 8, config_).get()));
  EXPECT_TRUE(std::isinf(parse_double("INFINITY", 8, config_).get()));
  EXPECT_TRUE(std::isinf(parse_double("infinity", 8, config_).get()));
}

TEST_F(DoubleParsingTest, ParseNegativeInf) {
  EXPECT_TRUE(std::isinf(parse_double("-Inf", 4, config_).get()));
  EXPECT_LT(parse_double("-Inf", 4, config_).get(), 0);
}

TEST_F(DoubleParsingTest, ParseNegativeInfinity) {
  EXPECT_TRUE(std::isinf(parse_double("-Infinity", 9, config_).get()));
  EXPECT_LT(parse_double("-Infinity", 9, config_).get(), 0);
}

TEST_F(DoubleParsingTest, InvalidInfinityVariant) {
  // "INFxxxxx" should not be parsed as infinity
  auto result = parse_double("INFxxxxx", 8, config_);
  EXPECT_FALSE(result.ok());
}

TEST_F(DoubleParsingTest, MalformedScientificNoExponentDigits) {
  auto result = parse_double("1e", 2, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(DoubleParsingTest, MalformedScientificJustSign) {
  auto result = parse_double("1e-", 3, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(DoubleParsingTest, TrailingCharacters) {
  auto result = parse_double("3.14abc", 7, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(DoubleParsingTest, NegativeZero) {
  double result = parse_double("-0.0", 4, config_).get();
  EXPECT_EQ(result, -0.0);
  EXPECT_TRUE(std::signbit(result));
}

TEST_F(DoubleParsingTest, PositiveSign) {
  EXPECT_NEAR(parse_double("+3.14", 5, config_).get(), 3.14, 0.01);
}

TEST_F(DoubleParsingTest, LeadingDecimalPoint) {
  EXPECT_NEAR(parse_double(".5", 2, config_).get(), 0.5, 0.001);
}

TEST_F(DoubleParsingTest, TrailingDecimalPoint) {
  EXPECT_NEAR(parse_double("5.", 2, config_).get(), 5.0, 0.001);
}

TEST_F(DoubleParsingTest, VeryLongMantissa) {
  // More than 19 digits in mantissa - should still work
  EXPECT_NEAR(parse_double("12345678901234567890.5", 22, config_).get(), 1.2345678901234568e19,
              1e5);
}

TEST_F(DoubleParsingTest, LargeExponent) {
  // Exponent > 400 - now parses successfully and returns infinity
  auto result = parse_double("1e500", 5, config_);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(std::isinf(result.get()));
}

TEST_F(DoubleParsingTest, MaxExponentThatWorks) {
  // 400 is the max exponent that parses fully
  auto result = parse_double("1e400", 5, config_);
  EXPECT_TRUE(result.ok());
  // 1e400 overflows to infinity
  EXPECT_TRUE(std::isinf(result.get()));
}

TEST_F(DoubleParsingTest, NegativeExponent) {
  EXPECT_NEAR(parse_double("1e-10", 5, config_).get(), 1e-10, 1e-15);
}

TEST_F(DoubleParsingTest, PositiveExponentSign) {
  EXPECT_NEAR(parse_double("1e+10", 5, config_).get(), 1e10, 1e5);
}

TEST_F(DoubleParsingTest, EmptyIsNA) {
  EXPECT_TRUE(parse_double("", 0, config_).is_na());
}

TEST_F(DoubleParsingTest, WhitespaceOnly) {
  EXPECT_TRUE(parse_double("   ", 3, config_).is_na());
}

TEST_F(DoubleParsingTest, WhitespaceTrimming) {
  EXPECT_NEAR(parse_double("  3.14  ", 8, config_).get(), 3.14, 0.01);
}

TEST_F(DoubleParsingTest, TabWhitespace) {
  EXPECT_NEAR(parse_double("\t3.14\t", 6, config_).get(), 3.14, 0.01);
}

TEST_F(DoubleParsingTest, JustDecimalPoint) {
  auto result = parse_double(".", 1, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(DoubleParsingTest, JustSign) {
  auto result = parse_double("-", 1, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(DoubleParsingTest, JustPlusSign) {
  auto result = parse_double("+", 1, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(DoubleParsingTest, UppercaseE) {
  EXPECT_NEAR(parse_double("1E10", 4, config_).get(), 1E10, 1e5);
}

TEST_F(DoubleParsingTest, PlusSignThenLettersInvalid) {
  // +Inf is not specially recognized (only -Inf is), so +Inf fails as invalid number
  auto result = parse_double("+Inf", 4, config_);
  EXPECT_FALSE(result.ok());
}

TEST_F(DoubleParsingTest, ZeroExponent) {
  EXPECT_NEAR(parse_double("1e0", 3, config_).get(), 1.0, 0.001);
}

TEST_F(DoubleParsingTest, PartialInfinity) {
  // "Infin" - not complete "Infinity"
  auto result = parse_double("Infin", 5, config_);
  EXPECT_FALSE(result.ok());
}

TEST_F(DoubleParsingTest, VerySmallNumber) {
  // Very small number that might underflow
  EXPECT_NEAR(parse_double("1e-300", 6, config_).get(), 1e-300, 1e-310);
}

TEST_F(DoubleParsingTest, DecimalWithExponent) {
  EXPECT_NEAR(parse_double("3.14e2", 6, config_).get(), 314.0, 0.001);
}

TEST_F(DoubleParsingTest, NegativeDecimalWithExponent) {
  EXPECT_NEAR(parse_double("-3.14e-2", 8, config_).get(), -0.0314, 0.0001);
}

class BoolParsingTest : public ::testing::Test {
protected:
  ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(BoolParsingTest, ParseTrue) {
  EXPECT_TRUE(parse_bool("true", 4, config_).get());
}
TEST_F(BoolParsingTest, ParseFalse) {
  EXPECT_FALSE(parse_bool("false", 5, config_).get());
}

TEST_F(BoolParsingTest, ParseTrueVariants) {
  EXPECT_TRUE(parse_bool("True", 4, config_).get());
  EXPECT_TRUE(parse_bool("TRUE", 4, config_).get());
  EXPECT_TRUE(parse_bool("1", 1, config_).get());
  EXPECT_TRUE(parse_bool("yes", 3, config_).get());
  EXPECT_TRUE(parse_bool("Yes", 3, config_).get());
  EXPECT_TRUE(parse_bool("YES", 3, config_).get());
  EXPECT_TRUE(parse_bool("T", 1, config_).get());
}

TEST_F(BoolParsingTest, ParseFalseVariants) {
  EXPECT_FALSE(parse_bool("False", 5, config_).get());
  EXPECT_FALSE(parse_bool("FALSE", 5, config_).get());
  EXPECT_FALSE(parse_bool("0", 1, config_).get());
  EXPECT_FALSE(parse_bool("no", 2, config_).get());
  EXPECT_FALSE(parse_bool("No", 2, config_).get());
  EXPECT_FALSE(parse_bool("NO", 2, config_).get());
  EXPECT_FALSE(parse_bool("F", 1, config_).get());
}

TEST_F(BoolParsingTest, EmptyIsNA) {
  EXPECT_TRUE(parse_bool("", 0, config_).is_na());
}

TEST_F(BoolParsingTest, NAValueIsNA) {
  EXPECT_TRUE(parse_bool("NA", 2, config_).is_na());
  EXPECT_TRUE(parse_bool("null", 4, config_).is_na());
}

TEST_F(BoolParsingTest, InvalidValue) {
  auto result = parse_bool("maybe", 5, config_);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
}

TEST_F(BoolParsingTest, WhitespaceTrimming) {
  EXPECT_TRUE(parse_bool("  true  ", 8, config_).get());
  EXPECT_FALSE(parse_bool("  false  ", 9, config_).get());
}

TEST_F(BoolParsingTest, TabWhitespace) {
  EXPECT_TRUE(parse_bool("\ttrue\t", 6, config_).get());
}

TEST_F(BoolParsingTest, WhitespaceOnly) {
  EXPECT_TRUE(parse_bool("   ", 3, config_).is_na());
}

class NATest : public ::testing::Test {
protected:
  ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(NATest, EmptyIsNA) {
  EXPECT_TRUE(is_na("", 0, config_));
}
TEST_F(NATest, NAIsNA) {
  EXPECT_TRUE(is_na("NA", 2, config_));
}
TEST_F(NATest, ValueNotNA) {
  EXPECT_FALSE(is_na("hello", 5, config_));
}

TEST_F(NATest, AllNAValues) {
  EXPECT_TRUE(is_na("N/A", 3, config_));
  EXPECT_TRUE(is_na("NaN", 3, config_));
  EXPECT_TRUE(is_na("null", 4, config_));
  EXPECT_TRUE(is_na("NULL", 4, config_));
  EXPECT_TRUE(is_na("None", 4, config_));
}

TEST_F(NATest, WhitespaceOnly) {
  EXPECT_TRUE(is_na("   ", 3, config_));
  EXPECT_TRUE(is_na("\t\t", 2, config_));
}

TEST_F(NATest, WhitespaceTrimming) {
  EXPECT_TRUE(is_na("  NA  ", 6, config_));
  EXPECT_TRUE(is_na("\tNA\t", 4, config_));
}

TEST_F(NATest, NumberNotNA) {
  EXPECT_FALSE(is_na("123", 3, config_));
}

class ExtractionConfigTest : public ::testing::Test {};

TEST_F(ExtractionConfigTest, DefaultsFactory) {
  auto config = ExtractionConfig::defaults();
  EXPECT_TRUE(config.trim_whitespace);
  EXPECT_TRUE(config.allow_leading_zeros);
  EXPECT_EQ(config.max_integer_digits, 20);
  EXPECT_FALSE(config.na_values.empty());
  EXPECT_FALSE(config.true_values.empty());
  EXPECT_FALSE(config.false_values.empty());
}

TEST_F(ExtractionConfigTest, NoWhitespaceTrimming) {
  ExtractionConfig config;
  config.trim_whitespace = false;

  // With trimming disabled, leading space makes it invalid
  auto result = parse_integer<int64_t>("  42", 4, config);
  EXPECT_FALSE(result.ok());

  // With trimming disabled, "  " is not treated as empty/NA
  result = parse_integer<int64_t>("  ", 2, config);
  EXPECT_FALSE(result.ok());
}

TEST_F(ExtractionConfigTest, NoWhitespaceTrimmingDouble) {
  ExtractionConfig config;
  config.trim_whitespace = false;

  auto result = parse_double("  3.14", 6, config);
  EXPECT_FALSE(result.ok());
}

TEST_F(ExtractionConfigTest, NoWhitespaceTrimmingBool) {
  ExtractionConfig config;
  config.trim_whitespace = false;

  auto result = parse_bool("  true", 6, config);
  EXPECT_FALSE(result.ok());
}

TEST_F(ExtractionConfigTest, NoWhitespaceTrimmingNA) {
  ExtractionConfig config;
  config.trim_whitespace = false;

  // With no trimming, "  " is not recognized as NA
  EXPECT_FALSE(is_na("  ", 2, config));

  // But empty string still is NA
  EXPECT_TRUE(is_na("", 0, config));
}

TEST_F(ExtractionConfigTest, AllowLeadingZerosDefault) {
  // By default, leading zeros are allowed
  ExtractionConfig config;
  EXPECT_TRUE(config.allow_leading_zeros);

  // Leading zeros should parse successfully with default config
  EXPECT_EQ(parse_integer<int64_t>("007", 3, config).get(), 7);
  EXPECT_EQ(parse_integer<int64_t>("0123", 4, config).get(), 123);
  EXPECT_EQ(parse_integer<int64_t>("-007", 4, config).get(), -7);
  EXPECT_EQ(parse_integer<int64_t>("+007", 4, config).get(), 7);

  // Also test unsigned integers with default config (allow_leading_zeros = true)
  EXPECT_EQ(parse_integer<uint64_t>("007", 3, config).get(), 7u);
  EXPECT_EQ(parse_integer<uint64_t>("0123", 4, config).get(), 123u);
}

TEST_F(ExtractionConfigTest, DisallowLeadingZeros) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Leading zeros should be rejected
  auto result = parse_integer<int64_t>("007", 3, config);
  EXPECT_FALSE(result.ok());
  EXPECT_NE(result.error, nullptr);
  EXPECT_STREQ(result.error, "Leading zeros not allowed");

  result = parse_integer<int64_t>("0123", 4, config);
  EXPECT_FALSE(result.ok());

  // With negative sign
  result = parse_integer<int64_t>("-007", 4, config);
  EXPECT_FALSE(result.ok());

  // With positive sign
  result = parse_integer<int64_t>("+007", 4, config);
  EXPECT_FALSE(result.ok());
}

TEST_F(ExtractionConfigTest, DisallowLeadingZerosSingleZeroAllowed) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Single zero is not a leading zero - it's the number itself
  EXPECT_EQ(parse_integer<int64_t>("0", 1, config).get(), 0);
  EXPECT_EQ(parse_integer<int64_t>("-0", 2, config).get(), 0);
  EXPECT_EQ(parse_integer<int64_t>("+0", 2, config).get(), 0);
}

TEST_F(ExtractionConfigTest, DisallowLeadingZerosRegularNumbers) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Numbers without leading zeros should still parse
  EXPECT_EQ(parse_integer<int64_t>("123", 3, config).get(), 123);
  EXPECT_EQ(parse_integer<int64_t>("-456", 4, config).get(), -456);
  EXPECT_EQ(parse_integer<int64_t>("+789", 4, config).get(), 789);
  EXPECT_EQ(parse_integer<int64_t>("10", 2, config).get(), 10);
}

TEST_F(ExtractionConfigTest, DisallowLeadingZerosUnsigned) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Unsigned integers with leading zeros should be rejected
  auto result = parse_integer<uint64_t>("007", 3, config);
  EXPECT_FALSE(result.ok());

  // Without leading zeros should work
  EXPECT_EQ(parse_integer<uint64_t>("7", 1, config).get(), 7u);
  EXPECT_EQ(parse_integer<uint64_t>("0", 1, config).get(), 0u);

  // Multi-digit numbers not starting with 0 should work (covers C=false branch for uint64_t)
  EXPECT_EQ(parse_integer<uint64_t>("123", 3, config).get(), 123u);
  EXPECT_EQ(parse_integer<uint64_t>("10", 2, config).get(), 10u);
}

TEST_F(ExtractionConfigTest, DisallowLeadingZerosWithWhitespace) {
  ExtractionConfig config;
  config.allow_leading_zeros = false;

  // Leading zeros should still be detected after whitespace trimming
  auto result = parse_integer<int64_t>("  007  ", 7, config);
  EXPECT_FALSE(result.ok());

  // But valid numbers with whitespace should work
  EXPECT_EQ(parse_integer<int64_t>("  7  ", 5, config).get(), 7);
}

class ValueExtractorTest : public ::testing::Test {
protected:
  std::unique_ptr<TestBuffer> buffer_;
  Parser parser_;
  Parser::Result result_;

  void ParseCSV(const std::string& csv) {
    buffer_ = std::make_unique<TestBuffer>(csv);
    result_ = parser_.parse(buffer_->data(), buffer_->size());
  }

  // Accessor for the index to maintain API compatibility with existing tests
  ParseIndex& idx() { return result_.idx; }
};

TEST_F(ValueExtractorTest, SimpleCSV) {
  ParseCSV("name,age\nAlice,30\nBob,25\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.num_columns(), 2);
  EXPECT_EQ(extractor.num_rows(), 2);
  EXPECT_EQ(extractor.get_string_view(0, 0), "Alice");
  EXPECT_EQ(extractor.get<int64_t>(0, 1).get(), 30);
}

TEST_F(ValueExtractorTest, NoHeader) {
  ParseCSV("Alice,30\nBob,25\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  extractor.set_has_header(false);
  EXPECT_EQ(extractor.num_rows(), 2);
  EXPECT_EQ(extractor.get_string_view(0, 0), "Alice");
  EXPECT_EQ(extractor.get_string_view(1, 0), "Bob");
}

TEST_F(ValueExtractorTest, ColumnExtraction) {
  ParseCSV("id\n1\n2\n3\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto ids = extractor.extract_column<int64_t>(0);
  EXPECT_EQ(ids.size(), 3);
  EXPECT_EQ(*ids[0], 1);
  EXPECT_EQ(*ids[1], 2);
  EXPECT_EQ(*ids[2], 3);
}

TEST_F(ValueExtractorTest, EmptyField) {
  ParseCSV("a,b\n1,\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_TRUE(extractor.get<int64_t>(0, 1).is_na());
}

TEST_F(ValueExtractorTest, RowIterator) {
  ParseCSV("id\n1\n2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  int count = 0;
  for (const auto& row : extractor) {
    EXPECT_EQ(row.get<int64_t>(0).get(), count + 1);
    count++;
  }
  EXPECT_EQ(count, 2);
}

TEST_F(ValueExtractorTest, QuotedField) {
  ParseCSV("name,value\n\"Hello\",42\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.get_string_view(0, 0), "Hello");
  EXPECT_EQ(extractor.get<int64_t>(0, 1).get(), 42);
}

TEST_F(ValueExtractorTest, CRLFLineEndings) {
  ParseCSV("a,b\r\n1,2\r\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.get<int64_t>(0, 0).get(), 1);
  EXPECT_EQ(extractor.get<int64_t>(0, 1).get(), 2);
}

TEST_F(ValueExtractorTest, GetHeader) {
  ParseCSV("name,age\nAlice,30\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto headers = extractor.get_header();
  EXPECT_EQ(headers.size(), 2);
  EXPECT_EQ(headers[0], "name");
  EXPECT_EQ(headers[1], "age");
}

TEST_F(ValueExtractorTest, ExtractColumnOr) {
  ParseCSV("val\n1\nNA\n3\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto vals = extractor.extract_column_or<int64_t>(0, -1);
  EXPECT_EQ(vals.size(), 3);
  EXPECT_EQ(vals[0], 1);
  EXPECT_EQ(vals[1], -1); // NA replaced with default
  EXPECT_EQ(vals[2], 3);
}

TEST_F(ValueExtractorTest, RowOutOfRange) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_THROW(extractor.get_string_view(99, 0), std::out_of_range);
}

TEST_F(ValueExtractorTest, ColOutOfRange) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_THROW(extractor.get_string_view(0, 99), std::out_of_range);
}

TEST_F(ValueExtractorTest, ExtractColumnStringViewOutOfRange) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_THROW(extractor.extract_column_string_view(99), std::out_of_range);
}

TEST_F(ValueExtractorTest, ExtractColumnStringOutOfRange) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_THROW(extractor.extract_column_string(99), std::out_of_range);
}

TEST_F(ValueExtractorTest, GetHeaderNoHeader) {
  ParseCSV("1,2\n3,4\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  extractor.set_has_header(false);
  EXPECT_THROW(extractor.get_header(), std::runtime_error);
}

TEST_F(ValueExtractorTest, GetString) {
  ParseCSV("name\n\"Hello\"\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.get_string(0, 0), "Hello");
}

TEST_F(ValueExtractorTest, GetStringWithEscapedQuotes) {
  ParseCSV("name\n\"He said \"\"Hi\"\"\"\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.get_string(0, 0), "He said \"Hi\"");
}

TEST_F(ValueExtractorTest, ExtractColumnStringView) {
  ParseCSV("name\nAlice\nBob\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto names = extractor.extract_column_string_view(0);
  EXPECT_EQ(names.size(), 2);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
}

TEST_F(ValueExtractorTest, ExtractColumnString) {
  ParseCSV("name\n\"Alice\"\n\"Bob\"\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto names = extractor.extract_column_string(0);
  EXPECT_EQ(names.size(), 2);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
}

TEST_F(ValueExtractorTest, GetFieldBounds) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  size_t start, end;
  EXPECT_TRUE(extractor.get_field_bounds(0, 0, start, end));
}

TEST_F(ValueExtractorTest, GetFieldBoundsOutOfRange) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  size_t start, end;
  EXPECT_FALSE(extractor.get_field_bounds(99, 0, start, end));
  EXPECT_FALSE(extractor.get_field_bounds(0, 99, start, end));
}

TEST_F(ValueExtractorTest, ExtractDoubleColumn) {
  ParseCSV("val\n1.5\n2.5\n3.5\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto vals = extractor.extract_column<double>(0);
  EXPECT_EQ(vals.size(), 3);
  EXPECT_NEAR(*vals[0], 1.5, 0.01);
  EXPECT_NEAR(*vals[1], 2.5, 0.01);
  EXPECT_NEAR(*vals[2], 3.5, 0.01);
}

TEST_F(ValueExtractorTest, ExtractBoolColumn) {
  ParseCSV("val\ntrue\nfalse\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto vals = extractor.extract_column<bool>(0);
  EXPECT_EQ(vals.size(), 2);
  EXPECT_TRUE(*vals[0]);
  EXPECT_FALSE(*vals[1]);
}

TEST_F(ValueExtractorTest, ExtractDoubleColumnOr) {
  ParseCSV("val\n1.5\nNA\n3.5\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto vals = extractor.extract_column_or<double>(0, -1.0);
  EXPECT_EQ(vals.size(), 3);
  EXPECT_NEAR(vals[0], 1.5, 0.01);
  EXPECT_NEAR(vals[1], -1.0, 0.01); // NA replaced with default
  EXPECT_NEAR(vals[2], 3.5, 0.01);
}

TEST_F(ValueExtractorTest, GetDouble) {
  ParseCSV("val\n3.14\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_NEAR(extractor.get<double>(0, 0).get(), 3.14, 0.01);
}

TEST_F(ValueExtractorTest, GetBool) {
  ParseCSV("val\ntrue\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_TRUE(extractor.get<bool>(0, 0).get());
}

TEST_F(ValueExtractorTest, SetConfig) {
  ParseCSV("val\nMISSING\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  // Initially "MISSING" is not recognized as NA
  EXPECT_FALSE(extractor.get<int64_t>(0, 0).is_na());

  // Update config to include MISSING as NA value
  ExtractionConfig new_config;
  new_config.na_values = {"MISSING"};
  extractor.set_config(new_config);

  EXPECT_TRUE(extractor.get<int64_t>(0, 0).is_na());
}

TEST_F(ValueExtractorTest, Config) {
  ParseCSV("val\n1\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  const auto& config = extractor.config();
  EXPECT_TRUE(config.trim_whitespace);
}

TEST_F(ValueExtractorTest, RowIteratorMethods) {
  ParseCSV("name,age\nAlice,30\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto it = begin(extractor);
  auto row = *it;
  EXPECT_EQ(row.num_columns(), 2);
  EXPECT_EQ(row.get_string_view(0), "Alice");
  EXPECT_EQ(row.get_string(0), "Alice");
  EXPECT_EQ(row.get<int64_t>(1).get(), 30);
}

TEST_F(ValueExtractorTest, QuotedHeaderWithCRLF) {
  ParseCSV("\"name\",\"age\"\r\nAlice,30\r\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  auto headers = extractor.get_header();
  EXPECT_EQ(headers.size(), 2);
  EXPECT_EQ(headers[0], "name");
  EXPECT_EQ(headers[1], "age");
}

TEST_F(ValueExtractorTest, SingleRowNoData) {
  // Single header row with no data rows
  ParseCSV("header\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.num_rows(), 0);
}

TEST_F(ValueExtractorTest, SingleColumn) {
  ParseCSV("val\n1\n2\n3\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.num_columns(), 1);
  EXPECT_EQ(extractor.num_rows(), 3);
}

TEST_F(ValueExtractorTest, HasHeader) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_TRUE(extractor.has_header());
  extractor.set_has_header(false);
  EXPECT_FALSE(extractor.has_header());
}

TEST_F(ValueExtractorTest, SetHasHeaderSameValue) {
  ParseCSV("a,b\n1,2\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  size_t initial_rows = extractor.num_rows();
  extractor.set_has_header(true);                // Same value as default
  EXPECT_EQ(extractor.num_rows(), initial_rows); // Should not change
}

TEST_F(ValueExtractorTest, GetInt32) {
  ParseCSV("val\n42\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.get<int32_t>(0, 0).get(), 42);
}

TEST_F(ValueExtractorTest, UnescapeFieldNoQuotes) {
  ParseCSV("name\nHello\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  EXPECT_EQ(extractor.get_string(0, 0), "Hello");
}

TEST_F(ValueExtractorTest, UnescapeFieldEmptyQuotedString) {
  // Test empty quoted field
  ParseCSV("name\n\"\"\n");
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());
  std::string result = extractor.get_string(0, 0);
  EXPECT_EQ(result, "");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
