/**
 * @file utf8_test.cpp
 * @brief Tests for UTF-8 string utilities (display width and truncation).
 */

#include "utf8.h"

#include <gtest/gtest.h>

using namespace libvroom;

class Utf8Test : public ::testing::Test {};

// =============================================================================
// UTF-8 Decode Tests
// =============================================================================

TEST_F(Utf8Test, DecodeAscii) {
  uint32_t cp;
  std::string_view str = "ABC";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 'A');

  EXPECT_EQ(utf8_decode(str, 1, cp), 1);
  EXPECT_EQ(cp, 'B');

  EXPECT_EQ(utf8_decode(str, 2, cp), 1);
  EXPECT_EQ(cp, 'C');
}

TEST_F(Utf8Test, DecodeTwoByteSequence) {
  uint32_t cp;
  // ñ (U+00F1) is encoded as C3 B1
  std::string_view str = "ñ";

  EXPECT_EQ(utf8_decode(str, 0, cp), 2);
  EXPECT_EQ(cp, 0x00F1);
}

TEST_F(Utf8Test, DecodeThreeByteSequence) {
  uint32_t cp;
  // 日 (U+65E5) is encoded as E6 97 A5
  std::string_view str = "日";

  EXPECT_EQ(utf8_decode(str, 0, cp), 3);
  EXPECT_EQ(cp, 0x65E5);
}

TEST_F(Utf8Test, DecodeFourByteSequence) {
  uint32_t cp;
  // 🎉 (U+1F389) is encoded as F0 9F 8E 89
  std::string_view str = "🎉";

  EXPECT_EQ(utf8_decode(str, 0, cp), 4);
  EXPECT_EQ(cp, 0x1F389);
}

TEST_F(Utf8Test, DecodeInvalidSequence) {
  uint32_t cp;
  // Invalid continuation byte (0x80 alone)
  std::string str = "\x80";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD); // Replacement character
}

TEST_F(Utf8Test, DecodeTruncatedSequence) {
  uint32_t cp;
  // Truncated 3-byte sequence (only first byte)
  std::string str = "\xE6";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD); // Replacement character
}

// =============================================================================
// Codepoint Width Tests
// =============================================================================

TEST_F(Utf8Test, CodepointWidthAscii) {
  // ASCII characters are width 1
  EXPECT_EQ(codepoint_width('A'), 1);
  EXPECT_EQ(codepoint_width('z'), 1);
  EXPECT_EQ(codepoint_width('0'), 1);
  EXPECT_EQ(codepoint_width(' '), 1);
}

TEST_F(Utf8Test, CodepointWidthControlChars) {
  // Control characters have width 0
  EXPECT_EQ(codepoint_width('\0'), 0);
  EXPECT_EQ(codepoint_width('\t'), 0);
  EXPECT_EQ(codepoint_width('\n'), 0);
  EXPECT_EQ(codepoint_width('\r'), 0);
}

TEST_F(Utf8Test, CodepointWidthCJK) {
  // CJK characters are width 2
  EXPECT_EQ(codepoint_width(0x65E5), 2); // 日
  EXPECT_EQ(codepoint_width(0x672C), 2); // 本
  EXPECT_EQ(codepoint_width(0x8A9E), 2); // 語
}

TEST_F(Utf8Test, CodepointWidthHiragana) {
  // Hiragana characters are width 2
  EXPECT_EQ(codepoint_width(0x3042), 2); // あ
  EXPECT_EQ(codepoint_width(0x3044), 2); // い
}

TEST_F(Utf8Test, CodepointWidthKatakana) {
  // Katakana characters are width 2
  EXPECT_EQ(codepoint_width(0x30A2), 2); // ア
  EXPECT_EQ(codepoint_width(0x30A4), 2); // イ
}

TEST_F(Utf8Test, CodepointWidthEmoji) {
  // Emoji are width 2
  EXPECT_EQ(codepoint_width(0x1F389), 2); // 🎉
  EXPECT_EQ(codepoint_width(0x1F600), 2); // 😀
  EXPECT_EQ(codepoint_width(0x1F30D), 2); // 🌍
}

TEST_F(Utf8Test, CodepointWidthCombiningMark) {
  // Combining marks have width 0
  EXPECT_EQ(codepoint_width(0x0301), 0); // Combining acute accent
  EXPECT_EQ(codepoint_width(0x0308), 0); // Combining diaeresis
}

TEST_F(Utf8Test, CodepointWidthZeroWidthChars) {
  // Zero-width characters
  EXPECT_EQ(codepoint_width(0x200B), 0); // Zero Width Space
  EXPECT_EQ(codepoint_width(0x200D), 0); // Zero Width Joiner
  EXPECT_EQ(codepoint_width(0xFEFF), 0); // BOM
}

// =============================================================================
// UTF-8 Display Width Tests
// =============================================================================

TEST_F(Utf8Test, DisplayWidthAscii) {
  EXPECT_EQ(utf8_display_width("Hello"), 5);
  EXPECT_EQ(utf8_display_width(""), 0);
  EXPECT_EQ(utf8_display_width("A"), 1);
}

TEST_F(Utf8Test, DisplayWidthCJK) {
  // Each CJK character is 2 columns
  EXPECT_EQ(utf8_display_width("日本語"), 6); // 3 chars * 2 = 6
}

TEST_F(Utf8Test, DisplayWidthMixed) {
  // "Hello世界" = 5 ASCII + 2 CJK = 5*1 + 2*2 = 9
  EXPECT_EQ(utf8_display_width("Hello世界"), 9);
}

TEST_F(Utf8Test, DisplayWidthEmoji) {
  // Single emoji is 2 columns
  EXPECT_EQ(utf8_display_width("🎉"), 2);
  EXPECT_EQ(utf8_display_width("🎉🎊"), 4);
}

TEST_F(Utf8Test, DisplayWidthWithCombiningMarks) {
  // "é" as e + combining accent = 1 + 0 = 1
  std::string e_accent = "e\xCC\x81"; // e + combining acute
  EXPECT_EQ(utf8_display_width(e_accent), 1);
}

// =============================================================================
// UTF-8 Truncate Tests
// =============================================================================

TEST_F(Utf8Test, TruncateAsciiNoTruncation) {
  // String fits, no truncation needed
  EXPECT_EQ(utf8_truncate("Hello", 10), "Hello");
  EXPECT_EQ(utf8_truncate("Hello", 5), "Hello");
}

TEST_F(Utf8Test, TruncateAsciiWithEllipsis) {
  // String too long, truncate with ellipsis
  std::string result = utf8_truncate("Hello World", 8);
  EXPECT_EQ(result, "Hello...");
  EXPECT_EQ(utf8_display_width(result), 8);
}

TEST_F(Utf8Test, TruncateAsciiTooShortForEllipsis) {
  // Max width too short for ellipsis
  std::string result = utf8_truncate("Hello", 2);
  EXPECT_EQ(result, "He");
  EXPECT_EQ(utf8_display_width(result), 2);
}

TEST_F(Utf8Test, TruncateCJK) {
  // CJK characters are 2 columns each
  // "日本語" = 6 columns, truncate to 5 should give "日..."
  std::string result = utf8_truncate("日本語", 5);
  EXPECT_EQ(result, "日...");
  EXPECT_EQ(utf8_display_width(result), 5);
}

TEST_F(Utf8Test, TruncateCJKExact) {
  // Truncate to 4 should give "..." only (no room for even one 2-col char + ellipsis)
  // Actually: 4 columns allows for 1 CJK char (2 cols) + ... (3 cols) = 5, too big
  // So we get just "..." with display width 3, but wait we have 4 columns
  // Let me recalculate: max_width=4, target_width=1 (4-3)
  // Can fit 0 CJK chars (each is 2), so result is "..."
  std::string result = utf8_truncate("日本語", 4);
  // We can only fit "..." since target_width is 1, and CJK needs 2
  EXPECT_EQ(result, "...");
  EXPECT_EQ(utf8_display_width(result), 3);
}

TEST_F(Utf8Test, TruncateEmoji) {
  // Emoji are 4 bytes but 2 display columns
  std::string input = "Hello🎉World";
  // "Hello" = 5, "🎉" = 2, "World" = 5, total = 12
  // Truncate to 10: we can fit "Hello🎉" (7) + "..." (3) = 10
  std::string result = utf8_truncate(input, 10);
  EXPECT_EQ(result, "Hello🎉...");
  EXPECT_EQ(utf8_display_width(result), 10);
}

TEST_F(Utf8Test, TruncateDoesNotSplitMultibyte) {
  // Ensure we don't split a multi-byte sequence
  // "日本語" = 6 columns (3 CJK chars * 2), truncate to 4
  // Can't fit "日" (2) + "..." (3) = 5 > 4
  // So we get "..." only
  std::string result = utf8_truncate("日本語", 4);
  EXPECT_EQ(result, "...");
}

TEST_F(Utf8Test, TruncateZeroWidth) {
  EXPECT_EQ(utf8_truncate("Hello", 0), "");
}

TEST_F(Utf8Test, TruncateMixedContent) {
  // "Hello世界🌍日本語テスト" - mixed ASCII, CJK, emoji
  std::string input = "Hello世界🌍日本語テスト";
  // Let's truncate to 15 columns
  // H(1) e(1) l(1) l(1) o(1) 世(2) 界(2) 🌍(2) = 11
  // Next would be 日(2) = 13, 本(2) = 15 - but we need room for ...
  // Target: 15 - 3 = 12 columns
  // H(1) e(1) l(1) l(1) o(1) 世(2) 界(2) 🌍(2) = 11, then 日 would make 13 > 12
  // So we get "Hello世界🌍..."
  std::string result = utf8_truncate(input, 15);
  EXPECT_EQ(result, "Hello世界🌍...");
  EXPECT_EQ(utf8_display_width(result), 14); // 11 + 3 = 14
}

TEST_F(Utf8Test, TruncateLongAsciiField) {
  // Simulate the original bug scenario with ASCII that ends with emoji
  std::string input = "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJ🎉🎊";
  // 36 ASCII chars + 2 emoji = 36 + 4 = 40 columns
  // Truncate to 40 (MAX_COLUMN_WIDTH) should be exact fit if width is exactly 40
  // Actually: 36*1 + 2*2 = 40, exactly fits
  EXPECT_EQ(utf8_display_width(input), 40);

  // Should not truncate if it fits exactly
  EXPECT_EQ(utf8_truncate(input, 40), input);

  // Truncate to 39: need to truncate
  std::string result = utf8_truncate(input, 39);
  // Target: 39 - 3 = 36 columns
  // Can fit all 36 ASCII chars exactly
  EXPECT_EQ(result, "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJ...");
  EXPECT_EQ(utf8_display_width(result), 39);
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_F(Utf8Test, EmptyString) {
  EXPECT_EQ(utf8_display_width(""), 0);
  EXPECT_EQ(utf8_truncate("", 10), "");
}

TEST_F(Utf8Test, SingleCharacter) {
  EXPECT_EQ(utf8_truncate("A", 1), "A");
  EXPECT_EQ(utf8_truncate("日", 2), "日");
  EXPECT_EQ(utf8_truncate("🎉", 2), "🎉");
}

TEST_F(Utf8Test, TruncateExactFit) {
  // String exactly fits, no truncation
  EXPECT_EQ(utf8_truncate("Hello", 5), "Hello");
  EXPECT_EQ(utf8_truncate("日本", 4), "日本");
}

TEST_F(Utf8Test, FullwidthForms) {
  // Fullwidth ASCII (U+FF01-U+FF5E) should be width 2
  // Ａ (U+FF21) is fullwidth A
  EXPECT_EQ(codepoint_width(0xFF21), 2);
}

TEST_F(Utf8Test, HangulSyllables) {
  // Korean Hangul syllables (U+AC00-U+D7AF) should be width 2
  // 한 (U+D55C)
  EXPECT_EQ(codepoint_width(0xD55C), 2);
  EXPECT_EQ(utf8_display_width("한글"), 4); // 2 chars * 2 = 4
}

// =============================================================================
// Additional UTF-8 Decode Tests for Coverage
// =============================================================================

TEST_F(Utf8Test, DecodePositionBeyondString) {
  // Test pos >= str.size() returns replacement character and 0 bytes consumed
  uint32_t cp;
  std::string_view str = "ABC";

  // Position at end of string
  EXPECT_EQ(utf8_decode(str, 3, cp), 0);
  EXPECT_EQ(cp, 0xFFFD);

  // Position way beyond string
  EXPECT_EQ(utf8_decode(str, 100, cp), 0);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeEmptyString) {
  uint32_t cp;
  std::string_view str = "";

  EXPECT_EQ(utf8_decode(str, 0, cp), 0);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeInvalidLeadingByte0xFF) {
  uint32_t cp;
  // 0xFF is never valid in UTF-8
  std::string str = "\xFF";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeInvalidLeadingByte0xFE) {
  uint32_t cp;
  // 0xFE is never valid in UTF-8
  std::string str = "\xFE";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeInvalidContinuationByteInTwoByteSequence) {
  uint32_t cp;
  // C3 should be followed by 80-BF, but we use 00 which is invalid
  std::string str = "\xC3\x00";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeInvalidContinuationByteInThreeByteSequence) {
  uint32_t cp;
  // E6 97 should be followed by 80-BF, but we use FF which is invalid
  std::string str = "\xE6\x97\xFF";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeInvalidContinuationByteInFourByteSequence) {
  uint32_t cp;
  // F0 9F 8E should be followed by 80-BF, but we use 7F which is invalid
  std::string str = "\xF0\x9F\x8E\x7F";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeSecondContinuationByteInvalid) {
  uint32_t cp;
  // E6 valid first byte, valid continuation, then invalid
  std::string str = "\xE6\x80\x00";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeTruncatedTwoByteSequence) {
  uint32_t cp;
  // C3 alone needs a continuation byte
  std::string str = "\xC3";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeTruncatedFourByteSequence) {
  uint32_t cp;
  // F0 9F 8E needs one more byte
  std::string str = "\xF0\x9F\x8E";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeOverlongTwoByteSequence) {
  uint32_t cp;
  // C0 80 is overlong encoding of NUL (should be just 0x00)
  std::string str = "\xC0\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 2);
  EXPECT_EQ(cp, 0xFFFD); // Should be replacement due to overlong
}

TEST_F(Utf8Test, DecodeOverlongThreeByteSequence) {
  uint32_t cp;
  // E0 80 80 is overlong encoding of NUL
  std::string str = "\xE0\x80\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 3);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeOverlongFourByteSequence) {
  uint32_t cp;
  // F0 80 80 80 is overlong encoding of NUL
  std::string str = "\xF0\x80\x80\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 4);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeSurrogateHigh) {
  uint32_t cp;
  // U+D800 (high surrogate) encoded as ED A0 80
  std::string str = "\xED\xA0\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 3);
  EXPECT_EQ(cp, 0xFFFD); // Surrogates are invalid in UTF-8
}

TEST_F(Utf8Test, DecodeSurrogateLow) {
  uint32_t cp;
  // U+DFFF (low surrogate) encoded as ED BF BF
  std::string str = "\xED\xBF\xBF";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 3);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeCodepointTooLarge) {
  uint32_t cp;
  // Code point > 0x10FFFF: F4 90 80 80 = U+110000
  std::string str = "\xF4\x90\x80\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 4);
  EXPECT_EQ(cp, 0xFFFD);
}

TEST_F(Utf8Test, DecodeValidBoundaryCodepoint) {
  uint32_t cp;
  // U+10FFFF is the maximum valid code point (F4 8F BF BF)
  std::string str = "\xF4\x8F\xBF\xBF";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 4);
  EXPECT_EQ(cp, 0x10FFFF);
}

TEST_F(Utf8Test, DecodeMinimumTwoByteSequence) {
  uint32_t cp;
  // U+0080 is the minimum 2-byte sequence (C2 80)
  std::string str = "\xC2\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 2);
  EXPECT_EQ(cp, 0x80);
}

TEST_F(Utf8Test, DecodeMinimumThreeByteSequence) {
  uint32_t cp;
  // U+0800 is the minimum 3-byte sequence (E0 A0 80)
  std::string str = "\xE0\xA0\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 3);
  EXPECT_EQ(cp, 0x800);
}

TEST_F(Utf8Test, DecodeMinimumFourByteSequence) {
  uint32_t cp;
  // U+10000 is the minimum 4-byte sequence (F0 90 80 80)
  std::string str = "\xF0\x90\x80\x80";

  size_t len = utf8_decode(str, 0, cp);
  EXPECT_EQ(len, 4);
  EXPECT_EQ(cp, 0x10000);
}

// =============================================================================
// Additional Codepoint Width Tests for Coverage
// =============================================================================

TEST_F(Utf8Test, CodepointWidthControlCharsExtended) {
  // DEL (0x7F) and C1 control characters (0x80-0x9F)
  EXPECT_EQ(codepoint_width(0x7F), 0); // DEL
  EXPECT_EQ(codepoint_width(0x80), 0); // Padding character
  EXPECT_EQ(codepoint_width(0x9F), 0); // Application Program Command
}

TEST_F(Utf8Test, CodepointWidthCombiningDiacriticalMarksExtended) {
  // U+1AB0-U+1AFF: Combining Diacritical Marks Extended
  EXPECT_EQ(codepoint_width(0x1AB0), 0);
  EXPECT_EQ(codepoint_width(0x1AFF), 0);
}

TEST_F(Utf8Test, CodepointWidthCombiningDiacriticalMarksSupplement) {
  // U+1DC0-U+1DFF: Combining Diacritical Marks Supplement
  EXPECT_EQ(codepoint_width(0x1DC0), 0);
  EXPECT_EQ(codepoint_width(0x1DFF), 0);
}

TEST_F(Utf8Test, CodepointWidthCombiningDiacriticalMarksForSymbols) {
  // U+20D0-U+20FF: Combining Diacritical Marks for Symbols
  EXPECT_EQ(codepoint_width(0x20D0), 0);
  EXPECT_EQ(codepoint_width(0x20FF), 0);
}

TEST_F(Utf8Test, CodepointWidthCombiningHalfMarks) {
  // U+FE20-U+FE2F: Combining Half Marks
  EXPECT_EQ(codepoint_width(0xFE20), 0);
  EXPECT_EQ(codepoint_width(0xFE2F), 0);
}

TEST_F(Utf8Test, CodepointWidthZeroWidthNonJoiner) {
  // U+200C: Zero Width Non-Joiner
  EXPECT_EQ(codepoint_width(0x200C), 0);
}

TEST_F(Utf8Test, CodepointWidthWordJoiner) {
  // U+2060: Word Joiner
  EXPECT_EQ(codepoint_width(0x2060), 0);
}

TEST_F(Utf8Test, CodepointWidthCJKRadicalsSupplement) {
  // U+2E80-U+2EFF: CJK Radicals Supplement
  EXPECT_EQ(codepoint_width(0x2E80), 2);
  EXPECT_EQ(codepoint_width(0x2EFF), 2);
}

TEST_F(Utf8Test, CodepointWidthKangxiRadicals) {
  // U+2F00-U+2FDF: Kangxi Radicals
  EXPECT_EQ(codepoint_width(0x2F00), 2);
  EXPECT_EQ(codepoint_width(0x2FDF), 2);
}

TEST_F(Utf8Test, CodepointWidthIdeographicDescriptionCharacters) {
  // U+2FF0-U+2FFF: Ideographic Description Characters
  EXPECT_EQ(codepoint_width(0x2FF0), 2);
  EXPECT_EQ(codepoint_width(0x2FFF), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKSymbolsAndPunctuation) {
  // U+3000-U+303F: CJK Symbols and Punctuation
  EXPECT_EQ(codepoint_width(0x3000), 2); // Ideographic space
  EXPECT_EQ(codepoint_width(0x303F), 2);
}

TEST_F(Utf8Test, CodepointWidthBopomofo) {
  // U+3100-U+312F: Bopomofo
  EXPECT_EQ(codepoint_width(0x3100), 2);
  EXPECT_EQ(codepoint_width(0x312F), 2);
}

TEST_F(Utf8Test, CodepointWidthHangulCompatibilityJamo) {
  // U+3130-U+318F: Hangul Compatibility Jamo
  EXPECT_EQ(codepoint_width(0x3130), 2);
  EXPECT_EQ(codepoint_width(0x318F), 2);
}

TEST_F(Utf8Test, CodepointWidthKanbun) {
  // U+3190-U+319F: Kanbun
  EXPECT_EQ(codepoint_width(0x3190), 2);
  EXPECT_EQ(codepoint_width(0x319F), 2);
}

TEST_F(Utf8Test, CodepointWidthBopomofoExtended) {
  // U+31A0-U+31BF: Bopomofo Extended
  EXPECT_EQ(codepoint_width(0x31A0), 2);
  EXPECT_EQ(codepoint_width(0x31BF), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKStrokes) {
  // U+31C0-U+31EF: CJK Strokes
  EXPECT_EQ(codepoint_width(0x31C0), 2);
  EXPECT_EQ(codepoint_width(0x31EF), 2);
}

TEST_F(Utf8Test, CodepointWidthKatakanaPhoneticExtensions) {
  // U+31F0-U+31FF: Katakana Phonetic Extensions
  EXPECT_EQ(codepoint_width(0x31F0), 2);
  EXPECT_EQ(codepoint_width(0x31FF), 2);
}

TEST_F(Utf8Test, CodepointWidthEnclosedCJKLettersAndMonths) {
  // U+3200-U+32FF: Enclosed CJK Letters and Months
  EXPECT_EQ(codepoint_width(0x3200), 2);
  EXPECT_EQ(codepoint_width(0x32FF), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKCompatibility) {
  // U+3300-U+33FF: CJK Compatibility
  EXPECT_EQ(codepoint_width(0x3300), 2);
  EXPECT_EQ(codepoint_width(0x33FF), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKUnifiedIdeographsExtensionA) {
  // U+3400-U+4DBF: CJK Unified Ideographs Extension A
  EXPECT_EQ(codepoint_width(0x3400), 2);
  EXPECT_EQ(codepoint_width(0x4DBF), 2);
}

TEST_F(Utf8Test, CodepointWidthYijingHexagramSymbols) {
  // U+4DC0-U+4DFF: Yijing Hexagram Symbols
  EXPECT_EQ(codepoint_width(0x4DC0), 2);
  EXPECT_EQ(codepoint_width(0x4DFF), 2);
}

TEST_F(Utf8Test, CodepointWidthYiSyllables) {
  // U+A000-U+A48F: Yi Syllables
  EXPECT_EQ(codepoint_width(0xA000), 2);
  EXPECT_EQ(codepoint_width(0xA48F), 2);
}

TEST_F(Utf8Test, CodepointWidthYiRadicals) {
  // U+A490-U+A4CF: Yi Radicals
  EXPECT_EQ(codepoint_width(0xA490), 2);
  EXPECT_EQ(codepoint_width(0xA4CF), 2);
}

TEST_F(Utf8Test, CodepointWidthHangulJamoExtendedA) {
  // U+A960-U+A97F: Hangul Jamo Extended-A
  EXPECT_EQ(codepoint_width(0xA960), 2);
  EXPECT_EQ(codepoint_width(0xA97F), 2);
}

TEST_F(Utf8Test, CodepointWidthHangulJamoExtendedB) {
  // U+D7B0-U+D7FF: Hangul Jamo Extended-B
  EXPECT_EQ(codepoint_width(0xD7B0), 2);
  EXPECT_EQ(codepoint_width(0xD7FF), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKCompatibilityIdeographs) {
  // U+F900-U+FAFF: CJK Compatibility Ideographs
  EXPECT_EQ(codepoint_width(0xF900), 2);
  EXPECT_EQ(codepoint_width(0xFAFF), 2);
}

TEST_F(Utf8Test, CodepointWidthVerticalForms) {
  // U+FE10-U+FE1F: Vertical Forms
  EXPECT_EQ(codepoint_width(0xFE10), 2);
  EXPECT_EQ(codepoint_width(0xFE1F), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKCompatibilityForms) {
  // U+FE30-U+FE4F: CJK Compatibility Forms
  EXPECT_EQ(codepoint_width(0xFE30), 2);
  EXPECT_EQ(codepoint_width(0xFE4F), 2);
}

TEST_F(Utf8Test, CodepointWidthSmallFormVariants) {
  // U+FE50-U+FE6F: Small Form Variants
  EXPECT_EQ(codepoint_width(0xFE50), 2);
  EXPECT_EQ(codepoint_width(0xFE6F), 2);
}

TEST_F(Utf8Test, CodepointWidthHalfwidthAndFullwidthForms) {
  // U+FF00-U+FF60: Fullwidth forms
  EXPECT_EQ(codepoint_width(0xFF00), 2);
  EXPECT_EQ(codepoint_width(0xFF60), 2);
  // U+FFE0-U+FFE6: Fullwidth currency, etc.
  EXPECT_EQ(codepoint_width(0xFFE0), 2);
  EXPECT_EQ(codepoint_width(0xFFE6), 2);
}

TEST_F(Utf8Test, CodepointWidthCJKUnifiedIdeographsExtensionB) {
  // U+20000-U+2FFFF: CJK Extension B-I and other supplementary CJK
  EXPECT_EQ(codepoint_width(0x20000), 2);
  EXPECT_EQ(codepoint_width(0x2A6DF), 2); // End of Extension B
  EXPECT_EQ(codepoint_width(0x2FFFF), 2);
}

TEST_F(Utf8Test, CodepointWidthTertiaryIdeographicPlane) {
  // U+30000-U+3FFFF
  EXPECT_EQ(codepoint_width(0x30000), 2);
  EXPECT_EQ(codepoint_width(0x3FFFF), 2);
}

TEST_F(Utf8Test, CodepointWidthMiscellaneousSymbolsAndPictographs) {
  // U+1F300-U+1F5FF: Miscellaneous Symbols and Pictographs
  EXPECT_EQ(codepoint_width(0x1F300), 2); // 🌀
  EXPECT_EQ(codepoint_width(0x1F5FF), 2);
}

TEST_F(Utf8Test, CodepointWidthOrnamentalDingbats) {
  // U+1F650-U+1F67F: Ornamental Dingbats
  EXPECT_EQ(codepoint_width(0x1F650), 2);
  EXPECT_EQ(codepoint_width(0x1F67F), 2);
}

TEST_F(Utf8Test, CodepointWidthTransportAndMapSymbols) {
  // U+1F680-U+1F6FF: Transport and Map Symbols
  EXPECT_EQ(codepoint_width(0x1F680), 2); // 🚀
  EXPECT_EQ(codepoint_width(0x1F6FF), 2);
}

TEST_F(Utf8Test, CodepointWidthAlchemicalSymbols) {
  // U+1F700-U+1F77F: Alchemical Symbols
  EXPECT_EQ(codepoint_width(0x1F700), 2);
  EXPECT_EQ(codepoint_width(0x1F77F), 2);
}

TEST_F(Utf8Test, CodepointWidthGeometricShapesExtended) {
  // U+1F780-U+1F7FF: Geometric Shapes Extended
  EXPECT_EQ(codepoint_width(0x1F780), 2);
  EXPECT_EQ(codepoint_width(0x1F7FF), 2);
}

TEST_F(Utf8Test, CodepointWidthSupplementalArrowsC) {
  // U+1F800-U+1F8FF: Supplemental Arrows-C
  EXPECT_EQ(codepoint_width(0x1F800), 2);
  EXPECT_EQ(codepoint_width(0x1F8FF), 2);
}

TEST_F(Utf8Test, CodepointWidthSupplementalSymbolsAndPictographs) {
  // U+1F900-U+1F9FF: Supplemental Symbols and Pictographs
  EXPECT_EQ(codepoint_width(0x1F900), 2);
  EXPECT_EQ(codepoint_width(0x1F9FF), 2);
}

TEST_F(Utf8Test, CodepointWidthChessSymbols) {
  // U+1FA00-U+1FA6F: Chess Symbols
  EXPECT_EQ(codepoint_width(0x1FA00), 2);
  EXPECT_EQ(codepoint_width(0x1FA6F), 2);
}

TEST_F(Utf8Test, CodepointWidthSymbolsAndPictographsExtendedA) {
  // U+1FA70-U+1FAFF: Symbols and Pictographs Extended-A
  EXPECT_EQ(codepoint_width(0x1FA70), 2);
  EXPECT_EQ(codepoint_width(0x1FAFF), 2);
}

TEST_F(Utf8Test, CodepointWidthSymbolsForLegacyComputing) {
  // U+1FB00-U+1FBFF: Symbols for Legacy Computing
  EXPECT_EQ(codepoint_width(0x1FB00), 2);
  EXPECT_EQ(codepoint_width(0x1FBFF), 2);
}

TEST_F(Utf8Test, CodepointWidthDefaultWidth) {
  // Characters not in any special range should be width 1
  EXPECT_EQ(codepoint_width(0x00A1), 1); // Inverted exclamation mark
  EXPECT_EQ(codepoint_width(0x0100), 1); // Latin capital letter A with macron
  EXPECT_EQ(codepoint_width(0x0400), 1); // Cyrillic capital letter Ie with grave
}

// =============================================================================
// Additional UTF-8 Display Width Tests for Coverage
// =============================================================================

TEST_F(Utf8Test, DisplayWidthInvalidSequenceReturnsZeroBytes) {
  // Create a string that will cause utf8_decode to return 0
  // This happens when pos >= str.size(), but in utf8_display_width
  // we start at pos=0, so we need a different approach.
  // Actually, the len == 0 case in utf8_display_width occurs when
  // utf8_decode returns 0 (pos >= str.size()), but since we iterate
  // while pos < str.size(), this shouldn't happen in normal iteration.
  // Let's verify the function handles empty strings correctly.
  EXPECT_EQ(utf8_display_width(""), 0);
}

TEST_F(Utf8Test, DisplayWidthWithInvalidBytes) {
  // Invalid UTF-8 sequences should still advance and contribute to width
  std::string invalid_seq = "\x80\x81\x82"; // Lone continuation bytes
  // Each invalid byte is treated as 1 byte consumed, with replacement char
  // The replacement char (0xFFFD) has width 1 (not in any special range)
  size_t width = utf8_display_width(invalid_seq);
  EXPECT_EQ(width, 3); // 3 replacement chars, each width 1
}

TEST_F(Utf8Test, DisplayWidthLongMixedString) {
  // Test a long string with various character types
  std::string mixed = "Hello世界🌍テスト한글АБВГД";
  // Hello: 5 * 1 = 5
  // 世界: 2 * 2 = 4
  // 🌍: 1 * 2 = 2
  // テスト: 3 * 2 = 6
  // 한글: 2 * 2 = 4
  // АБВГД: 5 * 1 = 5
  // Total: 5 + 4 + 2 + 6 + 4 + 5 = 26
  EXPECT_EQ(utf8_display_width(mixed), 26);
}

// =============================================================================
// Additional UTF-8 Truncate Tests for Coverage
// =============================================================================

TEST_F(Utf8Test, TruncateWidth1) {
  // max_width = 1, too short for ellipsis (needs 3)
  std::string result = utf8_truncate("Hello", 1);
  EXPECT_EQ(result, "H");
  EXPECT_EQ(utf8_display_width(result), 1);
}

TEST_F(Utf8Test, TruncateWidth2) {
  // max_width = 2, still too short for ellipsis
  std::string result = utf8_truncate("Hello", 2);
  EXPECT_EQ(result, "He");
  EXPECT_EQ(utf8_display_width(result), 2);
}

TEST_F(Utf8Test, TruncateWidth3) {
  // max_width = 3, exactly ellipsis width
  std::string result = utf8_truncate("Hello", 3);
  EXPECT_EQ(result, "Hel");
  EXPECT_EQ(utf8_display_width(result), 3);
}

TEST_F(Utf8Test, TruncateWidth4) {
  // max_width = 4, can fit 1 char + ellipsis
  std::string result = utf8_truncate("Hello", 4);
  EXPECT_EQ(result, "H...");
  EXPECT_EQ(utf8_display_width(result), 4);
}

TEST_F(Utf8Test, TruncateCJKWidth3TooShortForEllipsis) {
  // max_width = 3 with CJK, can only fit 1 wide char (2 cols), or 3 narrow
  // Since "日" is 2 cols and we need 3 cols for ellipsis, we can't fit both
  // So we just truncate to fit max_width
  std::string result = utf8_truncate("日本語", 3);
  // Can fit 日 (2) but not 本 (2), so just "日" + nothing = "日"
  // Wait, max_width=3 <= ELLIPSIS_WIDTH=3, so we use the simple truncation
  EXPECT_EQ(result, "日");
  EXPECT_EQ(utf8_display_width(result), 2);
}

TEST_F(Utf8Test, TruncateCJKWidth2TooShortForEllipsis) {
  // max_width = 2, can fit exactly one CJK char
  std::string result = utf8_truncate("日本語", 2);
  EXPECT_EQ(result, "日");
  EXPECT_EQ(utf8_display_width(result), 2);
}

TEST_F(Utf8Test, TruncateCJKWidth1TooShortForAnything) {
  // max_width = 1, CJK chars are 2 cols, so nothing fits
  std::string result = utf8_truncate("日本語", 1);
  EXPECT_EQ(result, "");
  EXPECT_EQ(utf8_display_width(result), 0);
}

TEST_F(Utf8Test, TruncateWithZeroWidthCharacters) {
  // String with zero-width joiners
  std::string input = "A\xE2\x80\x8D"
                      "B"; // A + ZWJ + B
  // Width: A(1) + ZWJ(0) + B(1) = 2
  EXPECT_EQ(utf8_display_width(input), 2);

  // Truncate to width 2 should fit everything
  EXPECT_EQ(utf8_truncate(input, 2), input);
}

TEST_F(Utf8Test, TruncateStringWithControlChars) {
  // Control chars have 0 width
  std::string input = "A\x01\x02"
                      "BC"; // A + 2 control chars + BC
  // Width: A(1) + 0 + 0 + B(1) + C(1) = 3
  EXPECT_EQ(utf8_display_width(input), 3);

  // Truncate to 3 should fit exactly
  EXPECT_EQ(utf8_truncate(input, 3), input);
}

TEST_F(Utf8Test, TruncateWideCharAtBoundary) {
  // Test when a wide character would straddle the truncation boundary
  // "ABC日" = 3 + 2 = 5 cols
  // Truncate to 4: can't fit 日 after "A" in target (4-3=1)
  // But we can fit "A" (1 col) + "..." (3 cols) = 4
  std::string result = utf8_truncate("ABC日", 4);
  EXPECT_EQ(result, "A...");
  EXPECT_EQ(utf8_display_width(result), 4);
}

TEST_F(Utf8Test, TruncateWideCharExactFit) {
  // "日" = 2 cols, fits exactly in width 2
  EXPECT_EQ(utf8_truncate("日", 2), "日");

  // "日本" = 4 cols, fits exactly in width 4
  EXPECT_EQ(utf8_truncate("日本", 4), "日本");
}

TEST_F(Utf8Test, TruncateEmptyStringWithAnyWidth) {
  EXPECT_EQ(utf8_truncate("", 0), "");
  EXPECT_EQ(utf8_truncate("", 1), "");
  EXPECT_EQ(utf8_truncate("", 100), "");
}

TEST_F(Utf8Test, TruncateInvalidUtf8) {
  // Invalid UTF-8 bytes should be handled gracefully
  std::string invalid = "\x80\x81\x82\x83\x84"; // 5 invalid bytes
  // Each invalid byte = 1 replacement char with width 1
  EXPECT_EQ(utf8_display_width(invalid), 5);

  // Truncate to 4
  std::string result = utf8_truncate(invalid, 4);
  // Target width = 4 - 3 = 1, so fits 1 invalid byte + "..."
  EXPECT_EQ(utf8_display_width(result), 4);
}

TEST_F(Utf8Test, TruncateOnlyWidthExactlyEllipsis) {
  // max_width = 3, which is exactly ELLIPSIS_WIDTH
  // String needs truncation but we can only fit ellipsis chars
  std::string result = utf8_truncate("ABCDEFG", 3);
  // max_width <= 3, so no ellipsis, just truncate
  EXPECT_EQ(result, "ABC");
  EXPECT_EQ(utf8_display_width(result), 3);
}

TEST_F(Utf8Test, TruncateVeryLongString) {
  // Create a very long string
  std::string long_str(1000, 'X');
  std::string result = utf8_truncate(long_str, 50);
  // Should be 47 X's + "..."
  EXPECT_EQ(result, std::string(47, 'X') + "...");
  EXPECT_EQ(utf8_display_width(result), 50);
}

TEST_F(Utf8Test, TruncateEmojiSequence) {
  // Multiple emoji
  std::string input = "🎉🎊🎁🎈🎀";
  // Each emoji is 2 cols, total = 10
  EXPECT_EQ(utf8_display_width(input), 10);

  // Truncate to 8: can fit 2 emoji (4 cols) + "..." (3) = 7
  // Wait, target = 8 - 3 = 5, can fit 2 emoji (4 cols), result = 4 + 3 = 7
  std::string result = utf8_truncate(input, 8);
  EXPECT_EQ(result, "🎉🎊...");
  EXPECT_EQ(utf8_display_width(result), 7);
}

// =============================================================================
// Grapheme Cluster Tests
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterSimpleASCII) {
  int width;
  std::string_view str = "Hello";
  EXPECT_EQ(utf8_read_grapheme_cluster(str, 0, width), 1);
  EXPECT_EQ(width, 1);
}

TEST_F(Utf8Test, ReadGraphemeClusterSimpleEmoji) {
  int width;
  std::string_view str = "🎉";
  EXPECT_EQ(utf8_read_grapheme_cluster(str, 0, width), 4);
  EXPECT_EQ(width, 2);
}

TEST_F(Utf8Test, ReadGraphemeClusterEmptyString) {
  int width;
  std::string_view str = "";
  EXPECT_EQ(utf8_read_grapheme_cluster(str, 0, width), 0);
  EXPECT_EQ(width, 0);
}

TEST_F(Utf8Test, ReadGraphemeClusterPositionBeyondEnd) {
  int width;
  std::string_view str = "ABC";
  EXPECT_EQ(utf8_read_grapheme_cluster(str, 10, width), 0);
  EXPECT_EQ(width, 0);
}

// =============================================================================
// ZWJ (Zero-Width Joiner) Sequence Tests
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterFamilyEmoji) {
  int width;
  // Family emoji: 👨‍👩‍👧‍👦 = Man + ZWJ + Woman + ZWJ + Girl + ZWJ + Boy
  // Each person emoji is 4 bytes, ZWJ is 3 bytes
  // 👨 (F0 9F 91 A8) + ZWJ (E2 80 8D) + 👩 (F0 9F 91 A9) + ZWJ + 👧 (F0 9F 91 A7) + ZWJ + 👦 (F0 9F
  // 91 A6)
  std::string family = "\xF0\x9F\x91\xA8"  // 👨
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA9"  // 👩
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA7"  // 👧
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA6"; // 👦

  size_t bytes = utf8_read_grapheme_cluster(family, 0, width);
  // Should consume entire sequence as one grapheme cluster
  EXPECT_EQ(bytes, family.size());
  // Width is the sum of individual widths (even if displayed as one)
  EXPECT_GE(width, 2); // At minimum, the first emoji has width 2
}

TEST_F(Utf8Test, TruncateFamilyEmojiDoesNotSplit) {
  // Family emoji should not be split
  std::string family = "\xF0\x9F\x91\xA8"  // 👨
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA9"  // 👩
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA7"; // 👧
  std::string input = "ABC" + family + "DE";

  // ZWJ sequences render as a single 2-column glyph
  // "ABC" = 3, family = 2, "DE" = 2, total = 7
  // Truncate to 5: target_width = 2, can fit "AB" (2) but not family (2) since 2+2 > 2
  // Result: "AB..."
  std::string result = utf8_truncate(input, 5);
  EXPECT_EQ(result, "AB...");
}

TEST_F(Utf8Test, ReadGraphemeClusterManZWJComputer) {
  int width;
  // Man technologist: 👨‍💻 = Man + ZWJ + Laptop
  std::string technologist = "\xF0\x9F\x91\xA8"  // 👨
                             "\xE2\x80\x8D"      // ZWJ
                             "\xF0\x9F\x92\xBB"; // 💻

  size_t bytes = utf8_read_grapheme_cluster(technologist, 0, width);
  EXPECT_EQ(bytes, technologist.size());
  // ZWJ sequence renders as single 2-column glyph, not sum of widths
  EXPECT_EQ(width, 2);
}

TEST_F(Utf8Test, ZWJSequenceWidthIsTwo) {
  int width;
  // Family emoji: 👨‍👩‍👧 should be width 2, not 6
  std::string family = "\xF0\x9F\x91\xA8"  // 👨
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA9"  // 👩
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA7"; // 👧

  size_t bytes = utf8_read_grapheme_cluster(family, 0, width);
  EXPECT_EQ(bytes, family.size());
  // ZWJ sequences render as a single 2-column glyph
  EXPECT_EQ(width, 2);
}

// =============================================================================
// Emoji Modifier (Skin Tone) Tests
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterEmojiWithSkinTone) {
  int width;
  // Woman with medium skin tone: 👩🏽 = Woman + Medium skin tone modifier
  std::string woman_medium = "\xF0\x9F\x91\xA9"  // 👩
                             "\xF0\x9F\x8F\xBD"; // 🏽 (medium skin tone)

  size_t bytes = utf8_read_grapheme_cluster(woman_medium, 0, width);
  EXPECT_EQ(bytes, woman_medium.size());
  EXPECT_EQ(width, 2); // Base emoji width
}

TEST_F(Utf8Test, TruncateEmojiWithSkinToneDoesNotSplit) {
  // Emoji with skin tone modifier should not be split
  std::string emoji = "\xF0\x9F\x91\xA9"  // 👩
                      "\xF0\x9F\x8F\xBD"; // 🏽 (medium skin tone)
  std::string input = "AB" + emoji + "CD";

  // "AB" = 2, emoji = 2, "CD" = 2, total = 6
  // Truncate to 5: target = 2, can fit "AB" (2), then emoji (2) doesn't fit
  std::string result = utf8_truncate(input, 5);
  // Result should be "AB..." without splitting the skin-toned emoji
  EXPECT_EQ(result, "AB...");
}

TEST_F(Utf8Test, ReadGraphemeClusterAllSkinTones) {
  // Test all Fitzpatrick skin tone modifiers
  std::string base = "\xF0\x9F\x91\x8B"; // 👋 (waving hand)

  std::string skin_tones[] = {
      "\xF0\x9F\x8F\xBB", // Light skin tone (1F3FB)
      "\xF0\x9F\x8F\xBC", // Medium-light skin tone (1F3FC)
      "\xF0\x9F\x8F\xBD", // Medium skin tone (1F3FD)
      "\xF0\x9F\x8F\xBE", // Medium-dark skin tone (1F3FE)
      "\xF0\x9F\x8F\xBF"  // Dark skin tone (1F3FF)
  };

  for (const auto& tone : skin_tones) {
    int width;
    std::string emoji_with_tone = base + tone;
    size_t bytes = utf8_read_grapheme_cluster(emoji_with_tone, 0, width);
    EXPECT_EQ(bytes, emoji_with_tone.size()) << "Failed for skin tone";
  }
}

// =============================================================================
// Regional Indicator (Flag) Tests
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterFlagEmoji) {
  int width;
  // US flag: 🇺🇸 = Regional indicator U + Regional indicator S
  std::string us_flag = "\xF0\x9F\x87\xBA"  // 🇺 (U+1F1FA)
                        "\xF0\x9F\x87\xB8"; // 🇸 (U+1F1F8)

  size_t bytes = utf8_read_grapheme_cluster(us_flag, 0, width);
  EXPECT_EQ(bytes, us_flag.size());
  // Flag emoji displays as a single 2-column character
  EXPECT_EQ(width, 2);
}

TEST_F(Utf8Test, TruncateFlagEmojiDoesNotSplit) {
  // Flag emoji should not be split
  std::string flag = "\xF0\x9F\x87\xFA"  // 🇺 Regional indicator U
                     "\xF0\x9F\x87\xB8"; // 🇸 Regional indicator S
  std::string input = "AB" + flag + "CD";

  // "AB" = 2, flag = 2, "CD" = 2, total = 6
  // The flag (width 2) displays as one emoji
  // Truncate to 5: target = 2, can fit "AB" (2), then flag (2) > remaining 0
  std::string result = utf8_truncate(input, 5);
  // Result should be "AB..." without splitting the flag
  EXPECT_EQ(result, "AB...");
}

TEST_F(Utf8Test, ReadGraphemeClusterMultipleFlags) {
  int width;
  // Two flags in sequence
  std::string us_flag = "\xF0\x9F\x87\xBA\xF0\x9F\x87\xB8"; // 🇺🇸
  std::string jp_flag = "\xF0\x9F\x87\xAF\xF0\x9F\x87\xB5"; // 🇯🇵

  std::string two_flags = us_flag + jp_flag;

  // First call should return just the US flag
  size_t bytes = utf8_read_grapheme_cluster(two_flags, 0, width);
  EXPECT_EQ(bytes, us_flag.size());

  // Second call at US flag end should return JP flag
  bytes = utf8_read_grapheme_cluster(two_flags, us_flag.size(), width);
  EXPECT_EQ(bytes, jp_flag.size());
}

// =============================================================================
// Variation Selector Tests
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterWithVariationSelector) {
  int width;
  // Heart with emoji presentation: ❤️ = Heart + VS16
  std::string heart = "\xE2\x9D\xA4"  // ❤ (U+2764)
                      "\xEF\xB8\x8F"; // VS16 (U+FE0F)

  size_t bytes = utf8_read_grapheme_cluster(heart, 0, width);
  EXPECT_EQ(bytes, heart.size());
  // Heart is width 1 (not in wide char ranges), VS16 has width 0
  EXPECT_EQ(width, 1);
}

TEST_F(Utf8Test, VariationSelectorWidthIsZero) {
  // Verify variation selectors have width 0
  EXPECT_EQ(codepoint_width(0xFE0E), 0); // VS15 (text presentation)
  EXPECT_EQ(codepoint_width(0xFE0F), 0); // VS16 (emoji presentation)
}

// =============================================================================
// Complex ZWJ Sequence Tests
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterWomanScientist) {
  int width;
  // Woman scientist: 👩‍🔬 = Woman + ZWJ + Microscope
  std::string scientist = "\xF0\x9F\x91\xA9"  // 👩
                          "\xE2\x80\x8D"      // ZWJ
                          "\xF0\x9F\x94\xAC"; // 🔬

  size_t bytes = utf8_read_grapheme_cluster(scientist, 0, width);
  EXPECT_EQ(bytes, scientist.size());
}

TEST_F(Utf8Test, ReadGraphemeClusterWomanScientistWithSkinTone) {
  int width;
  // Woman scientist with medium skin: 👩🏽‍🔬 = Woman + Medium skin + ZWJ + Microscope
  std::string scientist = "\xF0\x9F\x91\xA9"  // 👩
                          "\xF0\x9F\x8F\xBD"  // 🏽 (medium skin tone)
                          "\xE2\x80\x8D"      // ZWJ
                          "\xF0\x9F\x94\xAC"; // 🔬

  size_t bytes = utf8_read_grapheme_cluster(scientist, 0, width);
  EXPECT_EQ(bytes, scientist.size());
}

TEST_F(Utf8Test, TruncateComplexZWJSequence) {
  // Man, woman, girl sequence with skin tones
  std::string family = "\xF0\x9F\x91\xA8"  // 👨
                       "\xF0\x9F\x8F\xBB"  // Light skin
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA9"  // 👩
                       "\xF0\x9F\x8F\xBD"  // Medium skin
                       "\xE2\x80\x8D"      // ZWJ
                       "\xF0\x9F\x91\xA7"  // 👧
                       "\xF0\x9F\x8F\xBF"; // Dark skin

  std::string input = "Hello" + family;

  // ZWJ sequences render as a single 2-column glyph
  // "Hello" = 5, family = 2, total = 7
  // Fits in max_width 8
  std::string result = utf8_truncate(input, 8);
  EXPECT_EQ(result, input);

  // Truncate to 6: target_width = 3, "Hel" (3) fits, family (2) > remaining 0
  result = utf8_truncate(input, 6);
  EXPECT_EQ(result, "Hel...");
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_F(Utf8Test, ReadGraphemeClusterSingleRegionalIndicator) {
  int width;
  // Single regional indicator (incomplete flag)
  std::string single = "\xF0\x9F\x87\xBA"; // 🇺 alone

  size_t bytes = utf8_read_grapheme_cluster(single, 0, width);
  EXPECT_EQ(bytes, single.size());
  EXPECT_EQ(width, 2);
}

TEST_F(Utf8Test, TruncatePreservesExistingBehaviorForPlainText) {
  // Verify that plain ASCII and CJK still work correctly
  EXPECT_EQ(utf8_truncate("Hello World", 8), "Hello...");
  EXPECT_EQ(utf8_truncate("日本語", 5), "日...");
  EXPECT_EQ(utf8_truncate("Hello", 5), "Hello");
}

TEST_F(Utf8Test, TruncateMixedTextAndEmoji) {
  // Mix of text and emoji (non-ZWJ emoji)
  std::string input = "Hi🎉Bye";
  // "Hi" = 2, 🎉 = 2, "Bye" = 3, total = 7
  // Truncate to 8: fits entirely
  EXPECT_EQ(utf8_truncate(input, 8), input);

  // Truncate to 7: fits entirely
  EXPECT_EQ(utf8_truncate(input, 7), input);

  // Truncate to 6: needs truncation, target = 3
  // "Hi" = 2, 🎉 = 2 won't fit in 3
  std::string result = utf8_truncate(input, 6);
  EXPECT_EQ(result, "Hi...");
}

TEST_F(Utf8Test, TruncateZWJNotFollowedByEmoji) {
  int width;
  // ZWJ without following emoji (malformed, but should handle gracefully)
  std::string malformed = "\xF0\x9F\x91\xA8" // 👨
                          "\xE2\x80\x8D"     // ZWJ
                          "A";               // ASCII (not emoji)

  // ZWJ not followed by valid emoji should NOT be consumed
  // This prevents malformed sequences from being included in the cluster
  size_t bytes = utf8_read_grapheme_cluster(malformed, 0, width);
  // Should only return the base emoji, not the orphan ZWJ
  EXPECT_EQ(bytes, 4); // Just the 👨 emoji
  EXPECT_EQ(width, 2);
}
