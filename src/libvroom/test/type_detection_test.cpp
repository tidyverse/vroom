#include "libvroom_types.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace libvroom;

class TypeDetectorTest : public ::testing::Test {
protected:
  TypeDetectionOptions options;
  void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(TypeDetectorTest, EmptyString) {
  EXPECT_EQ(TypeDetector::detect_field("", options), FieldType::EMPTY);
}

TEST_F(TypeDetectorTest, WhitespaceOnly) {
  EXPECT_EQ(TypeDetector::detect_field("   ", options), FieldType::EMPTY);
}

TEST_F(TypeDetectorTest, BooleanTrue) {
  EXPECT_EQ(TypeDetector::detect_field("true", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("TRUE", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanFalse) {
  EXPECT_EQ(TypeDetector::detect_field("false", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanYesNo) {
  EXPECT_EQ(TypeDetector::detect_field("yes", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("no", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanNumeric) {
  EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("1", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanNumericDisabled) {
  options.bool_as_int = false;
  EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("1", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerPositive) {
  EXPECT_EQ(TypeDetector::detect_field("42", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("123456789", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerNegative) {
  EXPECT_EQ(TypeDetector::detect_field("-42", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerWithThousandsSeparator) {
  options.allow_thousands_sep = true;
  EXPECT_EQ(TypeDetector::detect_field("1,000", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("1,000,000", options), FieldType::INTEGER);
}

// Bug fix tests for thousands separator validation
TEST_F(TypeDetectorTest, ThousandsSeparatorValidGrouping) {
  options.allow_thousands_sep = true;
  // Valid: first group 1-3 digits, subsequent groups exactly 3 digits
  EXPECT_EQ(TypeDetector::detect_field("1,000", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("12,000", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("123,000", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("1,234,567", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorInvalidGrouping) {
  options.allow_thousands_sep = true;
  // Invalid: first group > 3 digits with separator
  EXPECT_NE(TypeDetector::detect_field("1234,567", options), FieldType::INTEGER);
  // Invalid: group after separator not exactly 3 digits
  EXPECT_NE(TypeDetector::detect_field("1,00", options), FieldType::INTEGER);
  EXPECT_NE(TypeDetector::detect_field("1,0000", options), FieldType::INTEGER);
  EXPECT_NE(TypeDetector::detect_field("1,23,456", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, FloatSimple) {
  EXPECT_EQ(TypeDetector::detect_field("3.14", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatNegative) {
  EXPECT_EQ(TypeDetector::detect_field("-3.14", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatExponential) {
  EXPECT_EQ(TypeDetector::detect_field("1e10", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("1.5e-10", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatSpecialValues) {
  EXPECT_EQ(TypeDetector::detect_field("inf", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("nan", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("-inf", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, DateISO) {
  EXPECT_EQ(TypeDetector::detect_field("2024-01-15", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024/01/15", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateUS) {
  EXPECT_EQ(TypeDetector::detect_field("01/15/2024", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateEU) {
  EXPECT_EQ(TypeDetector::detect_field("15/01/2024", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateCompact) {
  EXPECT_EQ(TypeDetector::detect_field("20240115", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidMonth) {
  EXPECT_NE(TypeDetector::detect_field("2024-13-15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024-00-15", options), FieldType::DATE);
}

// Bug fix tests for date validation
TEST_F(TypeDetectorTest, DateInvalidFebruary30) {
  // February 30 should never be valid
  EXPECT_NE(TypeDetector::detect_field("2024-02-30", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2023-02-30", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidFebruary29NonLeapYear) {
  // February 29 invalid in non-leap years
  EXPECT_NE(TypeDetector::detect_field("2023-02-29", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2100-02-29", options),
            FieldType::DATE); // Century not divisible by 400
}

TEST_F(TypeDetectorTest, DateValidFebruary29LeapYear) {
  // February 29 valid in leap years
  EXPECT_EQ(TypeDetector::detect_field("2024-02-29", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2000-02-29", options),
            FieldType::DATE); // Century divisible by 400
}

TEST_F(TypeDetectorTest, DateInvalidApril31) {
  // April has only 30 days
  EXPECT_NE(TypeDetector::detect_field("2024-04-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidJune31) {
  // June has only 30 days
  EXPECT_NE(TypeDetector::detect_field("2024-06-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidSeptember31) {
  // September has only 30 days
  EXPECT_NE(TypeDetector::detect_field("2024-09-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidNovember31) {
  // November has only 30 days
  EXPECT_NE(TypeDetector::detect_field("2024-11-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateValidMonthsWith31Days) {
  // Months with 31 days should accept day 31
  EXPECT_EQ(TypeDetector::detect_field("2024-01-31", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024-03-31", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024-05-31", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024-07-31", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024-08-31", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024-10-31", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("2024-12-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, StringSimple) {
  EXPECT_EQ(TypeDetector::detect_field("hello", options), FieldType::STRING);
}

// ============================================================================
// Additional Numeric Detection Tests
// ============================================================================

TEST_F(TypeDetectorTest, IntegerWithPlusSign) {
  EXPECT_EQ(TypeDetector::detect_field("+42", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("+0", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerSignOnly) {
  // Just a sign with no digits should be STRING
  EXPECT_EQ(TypeDetector::detect_field("+", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("-", options), FieldType::STRING);
}

TEST_F(TypeDetectorTest, IntegerSignFollowedByNonDigit) {
  EXPECT_EQ(TypeDetector::detect_field("+a", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("-x", options), FieldType::STRING);
}

TEST_F(TypeDetectorTest, IntegerZero) {
  options.bool_as_int = false; // Disable bool_as_int to test pure integer
  EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("00", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerLargeNumber) {
  EXPECT_EQ(TypeDetector::detect_field("999999999999999", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorNoDigitsAfter) {
  options.allow_thousands_sep = true;
  // Separator at end with no following digits
  EXPECT_NE(TypeDetector::detect_field("1,", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorInsufficientDigitsAfter) {
  options.allow_thousands_sep = true;
  // Only 1 or 2 digits after separator (need exactly 3)
  EXPECT_NE(TypeDetector::detect_field("1,2", options), FieldType::INTEGER);
  EXPECT_NE(TypeDetector::detect_field("1,23", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorNonDigitInGroup) {
  options.allow_thousands_sep = true;
  // Non-digit within the 3-digit group after separator
  EXPECT_NE(TypeDetector::detect_field("1,23x", options), FieldType::INTEGER);
  EXPECT_NE(TypeDetector::detect_field("1,2x4", options), FieldType::INTEGER);
  EXPECT_NE(TypeDetector::detect_field("1,x34", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorWithSign) {
  options.allow_thousands_sep = true;
  EXPECT_EQ(TypeDetector::detect_field("-1,000", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("+1,234,567", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorNoDigitsBeforeSeparator) {
  options.allow_thousands_sep = true;
  // No digits before first separator
  EXPECT_NE(TypeDetector::detect_field(",000", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorCustomSeparator) {
  options.allow_thousands_sep = true;
  options.thousands_sep = ' '; // European style with space
  EXPECT_EQ(TypeDetector::detect_field("1 000", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("1 234 567", options), FieldType::INTEGER);
}

// ============================================================================
// Additional Float Detection Tests
// ============================================================================

TEST_F(TypeDetectorTest, FloatWithPlusSign) {
  EXPECT_EQ(TypeDetector::detect_field("+3.14", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("+0.5", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatDecimalOnly) {
  // Decimal point with digits only after
  EXPECT_EQ(TypeDetector::detect_field(".5", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field(".123", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatLeadingDecimal) {
  EXPECT_EQ(TypeDetector::detect_field("0.5", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("-.5", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatTrailingDecimal) {
  // Trailing decimal point (e.g., "5.")
  EXPECT_EQ(TypeDetector::detect_field("5.", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("123.", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatExponentialUppercase) {
  EXPECT_EQ(TypeDetector::detect_field("1E10", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("1.5E-10", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("1E+5", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatExponentialWithSign) {
  EXPECT_EQ(TypeDetector::detect_field("1e+10", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("1e-10", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("-1e+10", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatExponentialNoDigitsAfter) {
  // Exponent marker but no digits following
  EXPECT_EQ(TypeDetector::detect_field("1e", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("1e+", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("1e-", options), FieldType::STRING);
}

TEST_F(TypeDetectorTest, FloatExponentialDisabled) {
  options.allow_exponential = false;
  EXPECT_EQ(TypeDetector::detect_field("1e10", options), FieldType::STRING);
  // But regular floats still work
  EXPECT_EQ(TypeDetector::detect_field("3.14", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatSpecialValuesCase) {
  // Case insensitivity for special values
  EXPECT_EQ(TypeDetector::detect_field("INF", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("Inf", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("NaN", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("NAN", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatInfinity) {
  EXPECT_EQ(TypeDetector::detect_field("infinity", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("INFINITY", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("Infinity", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("-infinity", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("+infinity", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatSignedSpecialValues) {
  EXPECT_EQ(TypeDetector::detect_field("+inf", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("+nan", options), FieldType::FLOAT);
  EXPECT_EQ(TypeDetector::detect_field("-nan", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatCustomDecimalPoint) {
  options.decimal_point = ','; // European style
  EXPECT_EQ(TypeDetector::detect_field("3,14", options), FieldType::FLOAT);
  // With standard decimal point, should be string (or integer if no comma)
  EXPECT_NE(TypeDetector::detect_field("3.14", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatNoDigitsJustDecimal) {
  // Just a decimal point
  EXPECT_EQ(TypeDetector::detect_field(".", options), FieldType::STRING);
}

// ============================================================================
// Additional Boolean Detection Tests
// ============================================================================

TEST_F(TypeDetectorTest, BooleanCaseVariations) {
  // All case variations
  EXPECT_EQ(TypeDetector::detect_field("True", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("FALSE", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("False", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("YES", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("Yes", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("NO", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("No", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanSingleChar) {
  // Single character booleans: t/f/y/n
  EXPECT_EQ(TypeDetector::detect_field("t", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("f", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("y", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("n", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("T", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("F", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("Y", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("N", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanOnOff) {
  // On/Off variations
  EXPECT_EQ(TypeDetector::detect_field("on", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("ON", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("On", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("off", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("OFF", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("Off", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanNotBooleans) {
  // Things that look like booleans but aren't
  EXPECT_NE(TypeDetector::detect_field("tr", options), FieldType::BOOLEAN);
  EXPECT_NE(TypeDetector::detect_field("tru", options), FieldType::BOOLEAN);
  EXPECT_NE(TypeDetector::detect_field("fals", options), FieldType::BOOLEAN);
  EXPECT_NE(TypeDetector::detect_field("ye", options), FieldType::BOOLEAN);
  EXPECT_NE(TypeDetector::detect_field("2", options), FieldType::BOOLEAN); // Only 0/1
}

TEST_F(TypeDetectorTest, BooleanNumericNotBooleanForOtherDigits) {
  // Digits other than 0 and 1 should not be boolean (when bool_as_int=true)
  EXPECT_NE(TypeDetector::detect_field("2", options), FieldType::BOOLEAN);
  EXPECT_NE(TypeDetector::detect_field("9", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanEmpty) {
  // Empty string is not boolean
  EXPECT_FALSE(TypeDetector::is_boolean(reinterpret_cast<const uint8_t*>(""), 0, options));
}

// ============================================================================
// Additional Date Detection Tests
// ============================================================================

TEST_F(TypeDetectorTest, DateISOWithSlash) {
  EXPECT_EQ(TypeDetector::detect_field("2024/12/25", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("1999/01/01", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateUSWithDash) {
  EXPECT_EQ(TypeDetector::detect_field("12-25-2024", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("01-01-1999", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateEUWithDash) {
  EXPECT_EQ(TypeDetector::detect_field("25-12-2024", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateCompactAllMonths) {
  EXPECT_EQ(TypeDetector::detect_field("20240115", options), FieldType::DATE); // Jan
  EXPECT_EQ(TypeDetector::detect_field("20240228", options), FieldType::DATE); // Feb (non-leap)
  EXPECT_EQ(TypeDetector::detect_field("20240315", options), FieldType::DATE); // Mar
  EXPECT_EQ(TypeDetector::detect_field("20240430", options), FieldType::DATE); // Apr (30 days)
  EXPECT_EQ(TypeDetector::detect_field("20240531", options), FieldType::DATE); // May (31 days)
  EXPECT_EQ(TypeDetector::detect_field("20241231", options), FieldType::DATE); // Dec
}

TEST_F(TypeDetectorTest, DateInvalidDay0) {
  EXPECT_NE(TypeDetector::detect_field("2024-01-00", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("20240100", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidDay32) {
  EXPECT_NE(TypeDetector::detect_field("2024-01-32", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("20240132", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidYearTooLow) {
  // Year must be >= 1000
  EXPECT_NE(TypeDetector::detect_field("0999-01-15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("0100-01-15", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateMixedSeparators) {
  // Mixed separators should fail
  EXPECT_NE(TypeDetector::detect_field("2024-01/15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024/01-15", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidSeparator) {
  // Invalid separator characters
  EXPECT_NE(TypeDetector::detect_field("2024.01.15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024_01_15", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateNonDigitCharacters) {
  // Non-digit where digit expected
  EXPECT_NE(TypeDetector::detect_field("202X-01-15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024-0X-15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024-01-1X", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateTooShort) {
  // Too short to be a date
  EXPECT_NE(TypeDetector::detect_field("2024-01", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("202401", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateWrongLength) {
  // Wrong length for ISO format
  EXPECT_NE(TypeDetector::detect_field("2024-1-15", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("2024-01-1", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateLeapYearEdgeCases) {
  // Leap year rules: divisible by 4, but not by 100 unless also by 400
  // 2000 is a leap year (divisible by 400)
  EXPECT_EQ(TypeDetector::detect_field("2000-02-29", options), FieldType::DATE);
  // 2100 is NOT a leap year (divisible by 100 but not by 400)
  EXPECT_NE(TypeDetector::detect_field("2100-02-29", options), FieldType::DATE);
  // 2400 IS a leap year (divisible by 400)
  EXPECT_EQ(TypeDetector::detect_field("2400-02-29", options), FieldType::DATE);
  // 1900 was NOT a leap year
  EXPECT_NE(TypeDetector::detect_field("1900-02-29", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateMaxDayPerMonth) {
  // Each month's maximum valid day
  EXPECT_EQ(TypeDetector::detect_field("2024-01-31", options), FieldType::DATE); // Jan: 31
  EXPECT_EQ(TypeDetector::detect_field("2024-02-29", options), FieldType::DATE); // Feb leap: 29
  EXPECT_EQ(TypeDetector::detect_field("2024-03-31", options), FieldType::DATE); // Mar: 31
  EXPECT_EQ(TypeDetector::detect_field("2024-04-30", options), FieldType::DATE); // Apr: 30
  EXPECT_EQ(TypeDetector::detect_field("2024-05-31", options), FieldType::DATE); // May: 31
  EXPECT_EQ(TypeDetector::detect_field("2024-06-30", options), FieldType::DATE); // Jun: 30
  EXPECT_EQ(TypeDetector::detect_field("2024-07-31", options), FieldType::DATE); // Jul: 31
  EXPECT_EQ(TypeDetector::detect_field("2024-08-31", options), FieldType::DATE); // Aug: 31
  EXPECT_EQ(TypeDetector::detect_field("2024-09-30", options), FieldType::DATE); // Sep: 30
  EXPECT_EQ(TypeDetector::detect_field("2024-10-31", options), FieldType::DATE); // Oct: 31
  EXPECT_EQ(TypeDetector::detect_field("2024-11-30", options), FieldType::DATE); // Nov: 30
  EXPECT_EQ(TypeDetector::detect_field("2024-12-31", options), FieldType::DATE); // Dec: 31
}

// ============================================================================
// Whitespace and Trimming Tests
// ============================================================================

TEST_F(TypeDetectorTest, WhitespaceLeading) {
  EXPECT_EQ(TypeDetector::detect_field("  42", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("\t42", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, WhitespaceTrailing) {
  EXPECT_EQ(TypeDetector::detect_field("42  ", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("42\t", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, WhitespaceBoth) {
  EXPECT_EQ(TypeDetector::detect_field("  42  ", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("\t42\t", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("  true  ", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, WhitespaceTrimDisabled) {
  options.trim_whitespace = false;
  // With trim disabled, leading/trailing whitespace makes it a string
  EXPECT_EQ(TypeDetector::detect_field("  42", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("42  ", options), FieldType::STRING);
}

TEST_F(TypeDetectorTest, WhitespaceNewlineAndCarriageReturn) {
  // Newlines and carriage returns are also whitespace
  EXPECT_EQ(TypeDetector::detect_field("\n42\n", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("\r42\r", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("\r\n42\r\n", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, WhitespaceAllTypes) {
  // All whitespace chars combined
  EXPECT_EQ(TypeDetector::detect_field(" \t\r\n", options), FieldType::EMPTY);
}

TEST_F(TypeDetectorTest, FieldTypeToString) {
  EXPECT_STREQ(field_type_to_string(FieldType::BOOLEAN), "boolean");
  EXPECT_STREQ(field_type_to_string(FieldType::INTEGER), "integer");
  EXPECT_STREQ(field_type_to_string(FieldType::FLOAT), "float");
  EXPECT_STREQ(field_type_to_string(FieldType::DATE), "date");
  EXPECT_STREQ(field_type_to_string(FieldType::STRING), "string");
  EXPECT_STREQ(field_type_to_string(FieldType::EMPTY), "empty");
}

class ColumnTypeStatsTest : public ::testing::Test {
protected:
  ColumnTypeStats stats;
};

TEST_F(ColumnTypeStatsTest, AddTypes) {
  stats.add(FieldType::INTEGER);
  stats.add(FieldType::INTEGER);
  EXPECT_EQ(stats.total_count, 2);
  EXPECT_EQ(stats.integer_count, 2);
}

TEST_F(ColumnTypeStatsTest, DominantType) {
  for (int i = 0; i < 100; ++i)
    stats.add(FieldType::INTEGER);
  EXPECT_EQ(stats.dominant_type(), FieldType::INTEGER);
}

// Bug fix tests for type priority/hierarchy
TEST_F(ColumnTypeStatsTest, DominantTypePriorityBooleanOverInteger) {
  // 95% booleans should return BOOLEAN, not INTEGER
  for (int i = 0; i < 95; ++i)
    stats.add(FieldType::BOOLEAN);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  EXPECT_EQ(stats.dominant_type(), FieldType::BOOLEAN);
}

TEST_F(ColumnTypeStatsTest, DominantTypePriorityIntegerOverFloat) {
  // 95% integers should return INTEGER, not FLOAT
  for (int i = 0; i < 95; ++i)
    stats.add(FieldType::INTEGER);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  EXPECT_EQ(stats.dominant_type(), FieldType::INTEGER);
}

TEST_F(ColumnTypeStatsTest, DominantTypeMixedNumericFloatWins) {
  // Mix of floats and integers should return FLOAT
  for (int i = 0; i < 50; ++i)
    stats.add(FieldType::FLOAT);
  for (int i = 0; i < 45; ++i)
    stats.add(FieldType::INTEGER);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  EXPECT_EQ(stats.dominant_type(), FieldType::FLOAT);
}

TEST_F(ColumnTypeStatsTest, DominantTypeDateNotNumeric) {
  // Dates should not be confused with numerics
  for (int i = 0; i < 95; ++i)
    stats.add(FieldType::DATE);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  EXPECT_EQ(stats.dominant_type(), FieldType::DATE);
}

class ColumnTypeInferenceTest : public ::testing::Test {
protected:
  ColumnTypeInference inference;
};

TEST_F(ColumnTypeInferenceTest, SingleRow) {
  inference.add_row({"123", "3.14", "true", "2024-01-15", "hello"});
  auto types = inference.infer_types();
  EXPECT_EQ(types[0], FieldType::INTEGER);
  EXPECT_EQ(types[1], FieldType::FLOAT);
  EXPECT_EQ(types[2], FieldType::BOOLEAN);
  EXPECT_EQ(types[3], FieldType::DATE);
  EXPECT_EQ(types[4], FieldType::STRING);
}

TEST_F(ColumnTypeInferenceTest, MultipleRows) {
  inference.add_row({"123", "true"});
  inference.add_row({"456", "false"});
  auto types = inference.infer_types();
  EXPECT_EQ(types[0], FieldType::INTEGER);
  EXPECT_EQ(types[1], FieldType::BOOLEAN);
}

class TypeHintsTest : public ::testing::Test {
protected:
  TypeHints hints;
};

TEST_F(TypeHintsTest, AddAndGet) {
  hints.add("age", FieldType::INTEGER);
  EXPECT_EQ(hints.get("age"), FieldType::INTEGER);
  EXPECT_EQ(hints.get("unknown"), FieldType::STRING);
}

TEST_F(TypeHintsTest, HasHint) {
  hints.add("age", FieldType::INTEGER);
  EXPECT_TRUE(hints.has_hint("age"));
  EXPECT_FALSE(hints.has_hint("unknown"));
}

TEST_F(TypeHintsTest, OverwriteHint) {
  // Adding the same column twice should overwrite the previous value
  hints.add("col", FieldType::INTEGER);
  EXPECT_EQ(hints.get("col"), FieldType::INTEGER);
  hints.add("col", FieldType::FLOAT);
  EXPECT_EQ(hints.get("col"), FieldType::FLOAT);
}

TEST_F(TypeHintsTest, ManyColumns) {
  // Test with many columns to verify unordered_map performance
  const int num_columns = 1000;
  for (int i = 0; i < num_columns; ++i) {
    hints.add("column_" + std::to_string(i), FieldType::INTEGER);
  }

  // Verify all columns are accessible
  for (int i = 0; i < num_columns; ++i) {
    EXPECT_TRUE(hints.has_hint("column_" + std::to_string(i)));
    EXPECT_EQ(hints.get("column_" + std::to_string(i)), FieldType::INTEGER);
  }

  // Verify non-existent columns return defaults
  EXPECT_FALSE(hints.has_hint("nonexistent"));
  EXPECT_EQ(hints.get("nonexistent"), FieldType::STRING);
}

TEST_F(TypeHintsTest, AllFieldTypes) {
  hints.add("bool_col", FieldType::BOOLEAN);
  hints.add("int_col", FieldType::INTEGER);
  hints.add("float_col", FieldType::FLOAT);
  hints.add("date_col", FieldType::DATE);
  hints.add("string_col", FieldType::STRING);
  hints.add("empty_col", FieldType::EMPTY);

  EXPECT_EQ(hints.get("bool_col"), FieldType::BOOLEAN);
  EXPECT_EQ(hints.get("int_col"), FieldType::INTEGER);
  EXPECT_EQ(hints.get("float_col"), FieldType::FLOAT);
  EXPECT_EQ(hints.get("date_col"), FieldType::DATE);
  EXPECT_EQ(hints.get("string_col"), FieldType::STRING);
  EXPECT_EQ(hints.get("empty_col"), FieldType::EMPTY);
}

// ============================================================================
// Additional ColumnTypeStats Tests
// ============================================================================

TEST_F(ColumnTypeStatsTest, AddAllTypes) {
  stats.add(FieldType::EMPTY);
  stats.add(FieldType::BOOLEAN);
  stats.add(FieldType::INTEGER);
  stats.add(FieldType::FLOAT);
  stats.add(FieldType::DATE);
  stats.add(FieldType::STRING);

  EXPECT_EQ(stats.total_count, 6);
  EXPECT_EQ(stats.empty_count, 1);
  EXPECT_EQ(stats.boolean_count, 1);
  EXPECT_EQ(stats.integer_count, 1);
  EXPECT_EQ(stats.float_count, 1);
  EXPECT_EQ(stats.date_count, 1);
  EXPECT_EQ(stats.string_count, 1);
}

TEST_F(ColumnTypeStatsTest, DominantTypeAllEmpty) {
  for (int i = 0; i < 100; ++i)
    stats.add(FieldType::EMPTY);
  EXPECT_EQ(stats.dominant_type(), FieldType::EMPTY);
}

TEST_F(ColumnTypeStatsTest, DominantTypeWithCustomThreshold) {
  // 80% integers
  for (int i = 0; i < 80; ++i)
    stats.add(FieldType::INTEGER);
  for (int i = 0; i < 20; ++i)
    stats.add(FieldType::STRING);

  // With 0.9 threshold, falls back to STRING
  EXPECT_EQ(stats.dominant_type(0.9), FieldType::STRING);
  // With 0.8 threshold, returns INTEGER
  EXPECT_EQ(stats.dominant_type(0.8), FieldType::INTEGER);
  // With 0.7 threshold, also returns INTEGER
  EXPECT_EQ(stats.dominant_type(0.7), FieldType::INTEGER);
}

TEST_F(ColumnTypeStatsTest, DominantTypeBooleanPriority) {
  // When booleans dominate, should return BOOLEAN even though
  // booleans (0/1) could be interpreted as integers
  for (int i = 0; i < 95; ++i)
    stats.add(FieldType::BOOLEAN);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  EXPECT_EQ(stats.dominant_type(0.9), FieldType::BOOLEAN);
}

TEST_F(ColumnTypeStatsTest, DominantTypeFloatWithIntegers) {
  // Mix of floats and integers should infer FLOAT
  for (int i = 0; i < 45; ++i)
    stats.add(FieldType::FLOAT);
  for (int i = 0; i < 50; ++i)
    stats.add(FieldType::INTEGER);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  // 50+45 = 95% numeric (floats + integers) - should be FLOAT
  EXPECT_EQ(stats.dominant_type(0.9), FieldType::FLOAT);
}

TEST_F(ColumnTypeStatsTest, DominantTypeDateOverString) {
  for (int i = 0; i < 95; ++i)
    stats.add(FieldType::DATE);
  for (int i = 0; i < 5; ++i)
    stats.add(FieldType::STRING);
  EXPECT_EQ(stats.dominant_type(0.9), FieldType::DATE);
}

TEST_F(ColumnTypeStatsTest, DominantTypeEmptyExcluded) {
  // Empty values should be excluded from the denominator
  for (int i = 0; i < 90; ++i)
    stats.add(FieldType::INTEGER);
  for (int i = 0; i < 10; ++i)
    stats.add(FieldType::EMPTY);
  // 90/90 = 100% integers (empties excluded)
  EXPECT_EQ(stats.dominant_type(0.9), FieldType::INTEGER);
}

// ============================================================================
// Additional ColumnTypeInference Tests
// ============================================================================

TEST_F(ColumnTypeInferenceTest, Constructor) {
  ColumnTypeInference inf(5);
  EXPECT_EQ(inf.num_columns(), 5);
  EXPECT_EQ(inf.num_rows(), 0);
}

TEST_F(ColumnTypeInferenceTest, AddField) {
  inference.add_field(0, reinterpret_cast<const uint8_t*>("123"), 3);
  inference.add_field(0, reinterpret_cast<const uint8_t*>("456"), 3);
  auto types = inference.infer_types();
  EXPECT_EQ(types[0], FieldType::INTEGER);
}

TEST_F(ColumnTypeInferenceTest, AddFieldGrowsColumns) {
  inference.add_field(5, reinterpret_cast<const uint8_t*>("test"), 4);
  EXPECT_EQ(inference.num_columns(), 6);
}

TEST_F(ColumnTypeInferenceTest, NumRows) {
  inference.add_row({"a", "b"});
  inference.add_row({"c", "d"});
  inference.add_row({"e", "f"});
  EXPECT_EQ(inference.num_rows(), 3);
}

TEST_F(ColumnTypeInferenceTest, NumRowsEmpty) {
  EXPECT_EQ(inference.num_rows(), 0);
}

TEST_F(ColumnTypeInferenceTest, Reset) {
  inference.add_row({"123", "456"});
  inference.reset();
  // After reset, stats should be zeroed
  EXPECT_EQ(inference.column_stats(0).total_count, 0);
  EXPECT_EQ(inference.column_stats(1).total_count, 0);
}

TEST_F(ColumnTypeInferenceTest, Merge) {
  ColumnTypeInference other;
  other.add_row({"123", "true"});
  other.add_row({"456", "false"});

  inference.add_row({"789", "yes"});

  inference.merge(other);

  EXPECT_EQ(inference.column_stats(0).total_count, 3);
  EXPECT_EQ(inference.column_stats(1).total_count, 3);
}

TEST_F(ColumnTypeInferenceTest, MergeGrowsColumns) {
  ColumnTypeInference other;
  other.add_row({"a", "b", "c", "d"});

  inference.add_row({"e", "f"});

  inference.merge(other);

  EXPECT_EQ(inference.num_columns(), 4);
}

TEST_F(ColumnTypeInferenceTest, SetOptions) {
  TypeDetectionOptions opts;
  opts.bool_as_int = false;
  inference.set_options(opts);

  inference.add_row({"0", "1"});
  auto types = inference.infer_types();
  // With bool_as_int=false, 0 and 1 should be INTEGER not BOOLEAN
  EXPECT_EQ(types[0], FieldType::INTEGER);
  EXPECT_EQ(types[1], FieldType::INTEGER);
}

TEST_F(ColumnTypeInferenceTest, ColumnStatsAt) {
  inference.add_row({"123", "true"});
  const ColumnTypeStats& stats = inference.column_stats(0);
  EXPECT_EQ(stats.integer_count, 1);
}

TEST_F(ColumnTypeInferenceTest, AllStats) {
  inference.add_row({"123", "true", "3.14"});
  const auto& all = inference.all_stats();
  EXPECT_EQ(all.size(), 3);
}

TEST_F(ColumnTypeInferenceTest, InferTypesWithConfidenceThreshold) {
  TypeDetectionOptions opts;
  opts.confidence_threshold = 0.5;
  ColumnTypeInference inf(0, opts);

  // 60% integers, 40% strings
  for (int i = 0; i < 60; ++i) {
    inf.add_row({"123"});
  }
  for (int i = 0; i < 40; ++i) {
    inf.add_row({"hello"});
  }

  auto types = inf.infer_types();
  // With 0.5 threshold, 60% integers should dominate
  EXPECT_EQ(types[0], FieldType::INTEGER);
}

TEST_F(ColumnTypeInferenceTest, AddRowGrowsColumns) {
  inference.add_row({"a", "b"});
  inference.add_row({"c", "d", "e", "f"}); // More columns
  EXPECT_EQ(inference.num_columns(), 4);
}

// ============================================================================
// Early Termination Tests (GitHub issue #378)
// ============================================================================

TEST_F(ColumnTypeInferenceTest, AllTypesConfirmedNotEnoughSamples) {
  // With only a few samples, types should not be confirmed
  inference.add_row({"123", "hello"});
  inference.add_row({"456", "world"});
  inference.add_row({"789", "test"});

  // With default min_samples=100, should return false
  EXPECT_FALSE(inference.all_types_confirmed());

  // With min_samples=2, should return true
  EXPECT_TRUE(inference.all_types_confirmed(2));
}

TEST_F(ColumnTypeInferenceTest, AllTypesConfirmedEnoughSamples) {
  // Add many consistent samples
  for (int i = 0; i < 150; ++i) {
    inference.add_row({std::to_string(i), "text"});
  }

  // With default min_samples=100, should return true
  EXPECT_TRUE(inference.all_types_confirmed());
}

TEST_F(ColumnTypeInferenceTest, AllTypesConfirmedWithMixedTypes) {
  // Add samples with mixed types (some columns confirmed, some not)
  for (int i = 0; i < 50; ++i) {
    inference.add_row({std::to_string(i), "text"});
  }

  // At min_samples=30, should be confirmed
  EXPECT_TRUE(inference.all_types_confirmed(30));

  // At min_samples=60, should not be confirmed (not enough samples)
  EXPECT_FALSE(inference.all_types_confirmed(60));
}

TEST_F(ColumnTypeInferenceTest, IsColumnTypeConfirmedEmptyColumn) {
  // Test with column index out of bounds
  EXPECT_FALSE(inference.is_column_type_confirmed(0));
  EXPECT_FALSE(inference.is_column_type_confirmed(100));
}

TEST_F(ColumnTypeInferenceTest, IsColumnTypeConfirmedWithData) {
  // Add enough data to one column
  for (int i = 0; i < 150; ++i) {
    inference.add_field(0, reinterpret_cast<const uint8_t*>("123"), 3);
  }

  // Column 0 should be confirmed
  EXPECT_TRUE(inference.is_column_type_confirmed(0));

  // Non-existent column should return false
  EXPECT_FALSE(inference.is_column_type_confirmed(1));
}

TEST_F(ColumnTypeInferenceTest, AllTypesConfirmedWithEmptyInference) {
  // Empty inference should return false
  EXPECT_FALSE(inference.all_types_confirmed());
}

TEST_F(ColumnTypeInferenceTest, AllTypesConfirmedWithOnlyEmptyValues) {
  // If all values are empty, type is not confirmed until enough samples
  for (int i = 0; i < 50; ++i) {
    inference.add_row({""});
  }

  // With min_samples=10 on non_empty, should NOT be confirmed
  // because there are 0 non-empty values
  EXPECT_FALSE(inference.all_types_confirmed(10));
}

class SIMDTypeDetectorTest : public ::testing::Test {
protected:
  std::vector<uint8_t> buffer;
  void SetUp() override { buffer.resize(128, 0); }
};

TEST_F(SIMDTypeDetectorTest, AllDigits) {
  std::string digits = "12345678";
  std::memcpy(buffer.data(), digits.data(), digits.size());
  EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), digits.size()));
}

TEST_F(SIMDTypeDetectorTest, NotAllDigits) {
  std::string mixed = "1234a5678";
  std::memcpy(buffer.data(), mixed.data(), mixed.size());
  EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), mixed.size()));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsEmpty) {
  EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), 0));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsSingleDigit) {
  buffer[0] = '5';
  EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), 1));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsSingleNonDigit) {
  buffer[0] = 'x';
  EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), 1));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsLongString) {
  // Test with a string longer than one SIMD vector (typically 16 or 32 bytes)
  std::string long_digits(100, '7');
  std::memcpy(buffer.data(), long_digits.data(), long_digits.size());
  EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), long_digits.size()));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsLongStringWithNonDigitAtEnd) {
  std::string long_digits(99, '7');
  long_digits += 'x';
  std::memcpy(buffer.data(), long_digits.data(), long_digits.size());
  EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), long_digits.size()));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsExactVectorSize) {
  // Test with exactly 16, 32, and 64 bytes (common SIMD vector sizes)
  for (size_t size : {16, 32, 64}) {
    if (size > buffer.size())
      continue;
    std::string digits(size, '9');
    std::memcpy(buffer.data(), digits.data(), digits.size());
    EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), digits.size()))
        << "Failed for size " << size;
  }
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsEmpty) {
  EXPECT_EQ(SIMDTypeDetector::classify_digits(buffer.data(), 0), 0ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsAllDigits) {
  std::string digits = "12345678";
  std::memcpy(buffer.data(), digits.data(), digits.size());
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), digits.size());
  // All 8 bits should be set
  EXPECT_EQ(result, 0xFFULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsNoDigits) {
  std::string text = "abcdefgh";
  std::memcpy(buffer.data(), text.data(), text.size());
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), text.size());
  EXPECT_EQ(result, 0ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsMixed) {
  std::string mixed = "1a2b3c4d"; // digits at positions 0, 2, 4, 6
  std::memcpy(buffer.data(), mixed.data(), mixed.size());
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), mixed.size());
  // Bits 0, 2, 4, 6 should be set: 0b01010101 = 0x55
  EXPECT_EQ(result, 0x55ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsSingleDigit) {
  buffer[0] = '7';
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 1);
  EXPECT_EQ(result, 1ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsSingleNonDigit) {
  buffer[0] = 'x';
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 1);
  EXPECT_EQ(result, 0ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsBoundaryChars) {
  // Test characters just outside the '0'-'9' range
  buffer[0] = '/'; // '0' - 1
  buffer[1] = '0'; // boundary
  buffer[2] = '9'; // boundary
  buffer[3] = ':'; // '9' + 1
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 4);
  // Only positions 1 and 2 should be digits: 0b0110 = 0x6
  EXPECT_EQ(result, 0x6ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsLongerThan64) {
  // For classify_digits, only first 64 bytes matter
  std::string digits(100, '5');
  std::memcpy(buffer.data(), digits.data(), digits.size());
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 64);
  EXPECT_EQ(result, ~0ULL); // All 64 bits set
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsExact64Bytes) {
  std::string digits(64, '3');
  std::memcpy(buffer.data(), digits.data(), digits.size());
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 64);
  EXPECT_EQ(result, ~0ULL); // All 64 bits set
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsPatternAtVectorBoundary) {
  // Create a pattern that spans SIMD vector boundaries
  // Fill with digits, then put a non-digit at position 16 (common vector boundary)
  std::string pattern(32, '8');
  pattern[15] = 'x'; // just before common 16-byte boundary
  pattern[16] = 'y'; // at common 16-byte boundary
  std::memcpy(buffer.data(), pattern.data(), pattern.size());
  uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), pattern.size());

  // Expected: all bits set except 15 and 16
  uint64_t expected = 0xFFFFFFFFULL & ~(1ULL << 15) & ~(1ULL << 16);
  EXPECT_EQ(result, expected);
}

TEST_F(SIMDTypeDetectorTest, DetectBatch) {
  const char* fields[] = {"123", "3.14", "true", "hello"};
  size_t lengths[] = {3, 4, 4, 5};
  FieldType results[4];
  const uint8_t* field_ptrs[4];
  for (int i = 0; i < 4; ++i) {
    field_ptrs[i] = reinterpret_cast<const uint8_t*>(fields[i]);
  }
  SIMDTypeDetector::detect_batch(field_ptrs, lengths, 4, results);
  EXPECT_EQ(results[0], FieldType::INTEGER);
  EXPECT_EQ(results[1], FieldType::FLOAT);
  EXPECT_EQ(results[2], FieldType::BOOLEAN);
  EXPECT_EQ(results[3], FieldType::STRING);
}

TEST_F(SIMDTypeDetectorTest, DetectBatchEmpty) {
  FieldType results[1];
  const uint8_t* field_ptrs[1];
  size_t lengths[1];
  SIMDTypeDetector::detect_batch(field_ptrs, lengths, 0, results);
  // Should handle empty batch without crashing
}

TEST_F(SIMDTypeDetectorTest, DetectBatchWithOptions) {
  const char* fields[] = {"1,000", "true"};
  size_t lengths[] = {5, 4};
  FieldType results[2];
  const uint8_t* field_ptrs[2];
  for (int i = 0; i < 2; ++i) {
    field_ptrs[i] = reinterpret_cast<const uint8_t*>(fields[i]);
  }

  TypeDetectionOptions opts;
  opts.allow_thousands_sep = true;

  SIMDTypeDetector::detect_batch(field_ptrs, lengths, 2, results, opts);
  EXPECT_EQ(results[0], FieldType::INTEGER);
  EXPECT_EQ(results[1], FieldType::BOOLEAN);
}

// ============================================================================
// Additional String Detection / Fallback Tests
// ============================================================================

class StringFallbackTest : public ::testing::Test {
protected:
  TypeDetectionOptions options;
  void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(StringFallbackTest, AlmostInteger) {
  // Things that look like integers but aren't
  EXPECT_EQ(TypeDetector::detect_field("123abc", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("abc123", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("1 2 3", options), FieldType::STRING);
}

TEST_F(StringFallbackTest, AlmostFloat) {
  // Things that look like floats but aren't
  EXPECT_EQ(TypeDetector::detect_field("3.14abc", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("3.14.15", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("..5", options), FieldType::STRING);
}

TEST_F(StringFallbackTest, AlmostDate) {
  // Things that look like dates but aren't
  EXPECT_EQ(TypeDetector::detect_field("2024-13-01", options), FieldType::STRING); // Invalid month
  EXPECT_EQ(TypeDetector::detect_field("abcd-01-15", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("2024-ab-15", options), FieldType::STRING);
}

TEST_F(StringFallbackTest, MixedContent) {
  EXPECT_EQ(TypeDetector::detect_field("Hello, World!", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("test@example.com", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("https://example.com", options), FieldType::STRING);
}

TEST_F(StringFallbackTest, SpecialCharacters) {
  EXPECT_EQ(TypeDetector::detect_field("!@#$%", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("<html>", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("{\"key\": \"value\"}", options), FieldType::STRING);
}

TEST_F(StringFallbackTest, UnicodeContent) {
  EXPECT_EQ(TypeDetector::detect_field("日本語", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("émoji 🎉", options), FieldType::STRING);
  EXPECT_EQ(TypeDetector::detect_field("Ñoño", options), FieldType::STRING);
}

// ============================================================================
// API Overload Tests (different input types)
// ============================================================================

class APIOverloadTest : public ::testing::Test {
protected:
  TypeDetectionOptions options;
  void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(APIOverloadTest, DetectFieldFromUint8Ptr) {
  const uint8_t data[] = {'1', '2', '3'};
  EXPECT_EQ(TypeDetector::detect_field(data, 3, options), FieldType::INTEGER);
}

TEST_F(APIOverloadTest, DetectFieldFromString) {
  std::string str = "3.14159";
  EXPECT_EQ(TypeDetector::detect_field(str, options), FieldType::FLOAT);
}

TEST_F(APIOverloadTest, DetectFieldFromCString) {
  const char* cstr = "true";
  EXPECT_EQ(TypeDetector::detect_field(cstr, options), FieldType::BOOLEAN);
}

TEST_F(APIOverloadTest, DetectFieldFromEmptyString) {
  std::string str = "";
  EXPECT_EQ(TypeDetector::detect_field(str, options), FieldType::EMPTY);
}

TEST_F(APIOverloadTest, DetectFieldFromEmptyCString) {
  const char* cstr = "";
  EXPECT_EQ(TypeDetector::detect_field(cstr, options), FieldType::EMPTY);
}

// ============================================================================
// Direct is_* method tests
// ============================================================================

class DirectMethodTest : public ::testing::Test {
protected:
  TypeDetectionOptions options;
  void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(DirectMethodTest, IsBooleanDirect) {
  EXPECT_TRUE(TypeDetector::is_boolean(reinterpret_cast<const uint8_t*>("true"), 4, options));
  EXPECT_FALSE(TypeDetector::is_boolean(reinterpret_cast<const uint8_t*>("123"), 3, options));
}

TEST_F(DirectMethodTest, IsIntegerDirect) {
  EXPECT_TRUE(TypeDetector::is_integer(reinterpret_cast<const uint8_t*>("12345"), 5, options));
  EXPECT_FALSE(TypeDetector::is_integer(reinterpret_cast<const uint8_t*>("12.34"), 5, options));
}

TEST_F(DirectMethodTest, IsFloatDirect) {
  EXPECT_TRUE(TypeDetector::is_float(reinterpret_cast<const uint8_t*>("3.14"), 4, options));
  EXPECT_FALSE(TypeDetector::is_float(reinterpret_cast<const uint8_t*>("hello"), 5, options));
}

TEST_F(DirectMethodTest, IsDateDirect) {
  EXPECT_TRUE(TypeDetector::is_date(reinterpret_cast<const uint8_t*>("2024-01-15"), 10, options));
  EXPECT_FALSE(TypeDetector::is_date(reinterpret_cast<const uint8_t*>("hello"), 5, options));
}

TEST_F(DirectMethodTest, IsIntegerEmpty) {
  EXPECT_FALSE(TypeDetector::is_integer(reinterpret_cast<const uint8_t*>(""), 0, options));
}

TEST_F(DirectMethodTest, IsFloatEmpty) {
  EXPECT_FALSE(TypeDetector::is_float(reinterpret_cast<const uint8_t*>(""), 0, options));
}

// ============================================================================
// Edge Cases for Type Priority
// ============================================================================

class TypePriorityTest : public ::testing::Test {
protected:
  TypeDetectionOptions options;
  void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(TypePriorityTest, DateBeforeInteger8Digits) {
  // 8 digit numbers that look like dates should be DATE, not INTEGER
  EXPECT_EQ(TypeDetector::detect_field("20240115", options), FieldType::DATE);
  // But invalid dates should fall through to INTEGER
  EXPECT_EQ(TypeDetector::detect_field("99999999", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("20241315", options), FieldType::INTEGER); // Invalid month
}

TEST_F(TypePriorityTest, BooleanBeforeIntegerSingleDigit) {
  // "0" and "1" are BOOLEAN when bool_as_int is true
  EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::BOOLEAN);
  EXPECT_EQ(TypeDetector::detect_field("1", options), FieldType::BOOLEAN);

  // Other single digits are INTEGER
  options.bool_as_int = false;
  EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::INTEGER);
  EXPECT_EQ(TypeDetector::detect_field("1", options), FieldType::INTEGER);
}

// ============================================================================
// Date Format Preference Tests (GitHub issue #58)
// ============================================================================

class DateFormatPreferenceTest : public ::testing::Test {
protected:
  TypeDetectionOptions options;
  void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(DateFormatPreferenceTest, DefaultIsAuto) {
  // Default should be AUTO
  EXPECT_EQ(options.date_format_preference, DateFormatPreference::AUTO);
}

TEST_F(DateFormatPreferenceTest, ISOFormatAlwaysAccepted) {
  // ISO format should work with all preferences
  for (auto pref : {DateFormatPreference::AUTO, DateFormatPreference::US_FIRST,
                    DateFormatPreference::EU_FIRST, DateFormatPreference::ISO_ONLY}) {
    options.date_format_preference = pref;
    EXPECT_EQ(TypeDetector::detect_field("2024-01-15", options), FieldType::DATE)
        << "ISO format should work with preference " << static_cast<int>(pref);
    EXPECT_EQ(TypeDetector::detect_field("2024/12/25", options), FieldType::DATE)
        << "ISO format with slash should work with preference " << static_cast<int>(pref);
  }
}

TEST_F(DateFormatPreferenceTest, CompactFormatAlwaysAccepted) {
  // Compact format (YYYYMMDD) should work with all preferences
  for (auto pref : {DateFormatPreference::AUTO, DateFormatPreference::US_FIRST,
                    DateFormatPreference::EU_FIRST, DateFormatPreference::ISO_ONLY}) {
    options.date_format_preference = pref;
    EXPECT_EQ(TypeDetector::detect_field("20240115", options), FieldType::DATE)
        << "Compact format should work with preference " << static_cast<int>(pref);
  }
}

TEST_F(DateFormatPreferenceTest, USFormatWithAuto) {
  // AUTO should accept US format (MM/DD/YYYY)
  options.date_format_preference = DateFormatPreference::AUTO;
  EXPECT_EQ(TypeDetector::detect_field("01/15/2024", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("12-25-2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, EUFormatWithAuto) {
  // AUTO should accept EU format (DD/MM/YYYY) when unambiguous
  options.date_format_preference = DateFormatPreference::AUTO;
  // 15/01/2024 can only be EU (day 15 > 12)
  EXPECT_EQ(TypeDetector::detect_field("15/01/2024", options), FieldType::DATE);
  EXPECT_EQ(TypeDetector::detect_field("25-12-2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, USFirstAcceptsBothFormats) {
  options.date_format_preference = DateFormatPreference::US_FIRST;
  // Clear US format
  EXPECT_EQ(TypeDetector::detect_field("01/15/2024", options), FieldType::DATE);
  // Clear EU format (day > 12)
  EXPECT_EQ(TypeDetector::detect_field("25/12/2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, EUFirstAcceptsBothFormats) {
  options.date_format_preference = DateFormatPreference::EU_FIRST;
  // Clear EU format
  EXPECT_EQ(TypeDetector::detect_field("15/01/2024", options), FieldType::DATE);
  // Clear US format (month > 12 would fail, so test valid US that's also valid EU)
  EXPECT_EQ(TypeDetector::detect_field("01/12/2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, ISOOnlyRejectsUSFormat) {
  options.date_format_preference = DateFormatPreference::ISO_ONLY;
  // US format should be rejected
  EXPECT_NE(TypeDetector::detect_field("01/15/2024", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("12-25-2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, ISOOnlyRejectsEUFormat) {
  options.date_format_preference = DateFormatPreference::ISO_ONLY;
  // EU format should be rejected
  EXPECT_NE(TypeDetector::detect_field("15/01/2024", options), FieldType::DATE);
  EXPECT_NE(TypeDetector::detect_field("25-12-2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, AmbiguousDateWithAutoDefaultsToUS) {
  // "01/02/2024" is ambiguous - could be Jan 2 (US) or Feb 1 (EU)
  // With AUTO, US format is checked first, so this is valid as Jan 2
  options.date_format_preference = DateFormatPreference::AUTO;
  EXPECT_EQ(TypeDetector::detect_field("01/02/2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, AmbiguousDateWithUSFirst) {
  // "01/02/2024" with US_FIRST should be detected as date (Jan 2)
  options.date_format_preference = DateFormatPreference::US_FIRST;
  EXPECT_EQ(TypeDetector::detect_field("01/02/2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, AmbiguousDateWithEUFirst) {
  // "01/02/2024" with EU_FIRST should be detected as date (Feb 1)
  options.date_format_preference = DateFormatPreference::EU_FIRST;
  EXPECT_EQ(TypeDetector::detect_field("01/02/2024", options), FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, InvalidDateStillRejected) {
  // Invalid dates should be rejected regardless of preference
  for (auto pref : {DateFormatPreference::AUTO, DateFormatPreference::US_FIRST,
                    DateFormatPreference::EU_FIRST, DateFormatPreference::ISO_ONLY}) {
    options.date_format_preference = pref;
    // Invalid month 13
    EXPECT_NE(TypeDetector::detect_field("2024-13-01", options), FieldType::DATE);
    // Invalid day 32
    EXPECT_NE(TypeDetector::detect_field("2024-01-32", options), FieldType::DATE);
  }
}

TEST_F(DateFormatPreferenceTest, ColumnTypeInferenceWithPreference) {
  // Test that ColumnTypeInference respects date_format_preference
  TypeDetectionOptions opts;
  opts.date_format_preference = DateFormatPreference::ISO_ONLY;

  ColumnTypeInference inference(0, opts);

  // Add rows with US-format dates - should be detected as STRING in ISO_ONLY mode
  for (int i = 0; i < 10; ++i) {
    inference.add_row({"01/15/2024"});
  }

  auto types = inference.infer_types();
  EXPECT_EQ(types[0], FieldType::STRING);
}

TEST_F(DateFormatPreferenceTest, ColumnTypeInferenceWithISODates) {
  // Test that ColumnTypeInference correctly detects ISO dates
  TypeDetectionOptions opts;
  opts.date_format_preference = DateFormatPreference::ISO_ONLY;

  ColumnTypeInference inference(0, opts);

  // Add rows with ISO-format dates
  for (int i = 0; i < 10; ++i) {
    inference.add_row({"2024-01-15"});
  }

  auto types = inference.infer_types();
  EXPECT_EQ(types[0], FieldType::DATE);
}

TEST_F(DateFormatPreferenceTest, DirectIsDateMethodWithPreference) {
  // Test the is_date method directly with different preferences
  const uint8_t* us_date = reinterpret_cast<const uint8_t*>("01/15/2024");
  const uint8_t* eu_date = reinterpret_cast<const uint8_t*>("15/01/2024");
  const uint8_t* iso_date = reinterpret_cast<const uint8_t*>("2024-01-15");

  // AUTO accepts all
  options.date_format_preference = DateFormatPreference::AUTO;
  EXPECT_TRUE(TypeDetector::is_date(us_date, 10, options));
  EXPECT_TRUE(TypeDetector::is_date(eu_date, 10, options));
  EXPECT_TRUE(TypeDetector::is_date(iso_date, 10, options));

  // ISO_ONLY only accepts ISO
  options.date_format_preference = DateFormatPreference::ISO_ONLY;
  EXPECT_FALSE(TypeDetector::is_date(us_date, 10, options));
  EXPECT_FALSE(TypeDetector::is_date(eu_date, 10, options));
  EXPECT_TRUE(TypeDetector::is_date(iso_date, 10, options));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
