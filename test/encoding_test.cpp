/**
 * @file encoding_test.cpp
 * @brief Tests for encoding detection and transcoding functionality.
 */

#include "encoding.h"
#include "io_util.h"
#include "mem_util.h"

#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace libvroom;

// ============================================================================
// BOM Detection Tests
// ============================================================================

class BomDetectionTest : public ::testing::Test {
protected:
  static std::string test_data_dir() { return "test/data/encoding/"; }
};

TEST_F(BomDetectionTest, DetectsUtf16LeBom) {
  // UTF-16 LE BOM: FF FE
  uint8_t data[] = {0xFF, 0xFE, 'a', 0x00, 'b', 0x00};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
  EXPECT_EQ(result.bom_length, 2);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf16BeBom) {
  // UTF-16 BE BOM: FE FF
  uint8_t data[] = {0xFE, 0xFF, 0x00, 'a', 0x00, 'b'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF16_BE);
  EXPECT_EQ(result.bom_length, 2);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf32LeBom) {
  // UTF-32 LE BOM: FF FE 00 00
  uint8_t data[] = {0xFF, 0xFE, 0x00, 0x00, 'a', 0x00, 0x00, 0x00};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF32_LE);
  EXPECT_EQ(result.bom_length, 4);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf32BeBom) {
  // UTF-32 BE BOM: 00 00 FE FF
  uint8_t data[] = {0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, 'a'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF32_BE);
  EXPECT_EQ(result.bom_length, 4);
  EXPECT_TRUE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf8Bom) {
  // UTF-8 BOM: EF BB BF
  uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'e', 'l', 'l', 'o'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF8_BOM);
  EXPECT_EQ(result.bom_length, 3);
  EXPECT_FALSE(result.needs_transcoding);
  EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, NoBomDefaultsToUtf8) {
  // Plain ASCII - no BOM
  uint8_t data[] = {'h', 'e', 'l', 'l', 'o', '\n'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF8);
  EXPECT_EQ(result.bom_length, 0);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST_F(BomDetectionTest, PartialUtf8BomOneByte) {
  // Only first byte of UTF-8 BOM (EF BB BF) - should not detect as UTF-8 BOM
  uint8_t data[] = {0xEF, 'h', 'e', 'l', 'l', 'o'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_NE(result.encoding, Encoding::UTF8_BOM);
  EXPECT_EQ(result.bom_length, 0);
}

TEST_F(BomDetectionTest, PartialUtf8BomTwoBytes) {
  // First two bytes of UTF-8 BOM (EF BB BF) - should not detect as UTF-8 BOM
  uint8_t data[] = {0xEF, 0xBB, 'h', 'e', 'l', 'l', 'o'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_NE(result.encoding, Encoding::UTF8_BOM);
  EXPECT_EQ(result.bom_length, 0);
}

TEST_F(BomDetectionTest, PartialUtf16BomOneByte) {
  // Only first byte of UTF-16 LE BOM (FF FE) - should not detect as UTF-16
  uint8_t data[] = {0xFF, 'h', 'e', 'l', 'l', 'o'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_NE(result.encoding, Encoding::UTF16_LE);
  EXPECT_EQ(result.bom_length, 0);
}

TEST_F(BomDetectionTest, PartialUtf32BomTwoBytes) {
  // First two bytes of UTF-32 LE BOM (FF FE 00 00) - matches UTF-16 LE BOM
  // This is expected behavior: FF FE is a valid UTF-16 LE BOM
  uint8_t data[] = {0xFF, 0xFE, 'a', 0x00, 'b', 0x00};
  auto result = detect_encoding(data, sizeof(data));
  // Should detect as UTF-16 LE, not UTF-32 LE (which requires 4 bytes)
  EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
  EXPECT_EQ(result.bom_length, 2);
}

TEST_F(BomDetectionTest, PartialUtf32BomThreeBytes) {
  // First three bytes of UTF-32 LE BOM (FF FE 00 00) - still UTF-16 LE
  uint8_t data[] = {0xFF, 0xFE, 0x00, 'a', 0x00, 'b'};
  auto result = detect_encoding(data, sizeof(data));
  // Should detect as UTF-16 LE since FF FE 00 00 pattern not complete
  EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
  EXPECT_EQ(result.bom_length, 2);
}

TEST_F(BomDetectionTest, TinyBufferOneByte) {
  // Buffer too small to contain any BOM
  uint8_t data[] = {0xEF};
  auto result = detect_encoding(data, 1);
  EXPECT_EQ(result.bom_length, 0);
}

// ============================================================================
// Heuristic Detection Tests
// ============================================================================

class HeuristicDetectionTest : public ::testing::Test {};

TEST_F(HeuristicDetectionTest, DetectsUtf16LeWithoutBom) {
  // UTF-16 LE: ASCII characters with null byte after each
  std::vector<uint8_t> data;
  const char* text = "hello";
  for (const char* p = text; *p; ++p) {
    data.push_back(static_cast<uint8_t>(*p));
    data.push_back(0x00);
  }

  auto result = detect_encoding(data.data(), data.size());
  EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsUtf16BeWithoutBom) {
  // UTF-16 BE: null byte before each ASCII character
  std::vector<uint8_t> data;
  const char* text = "hello";
  for (const char* p = text; *p; ++p) {
    data.push_back(0x00);
    data.push_back(static_cast<uint8_t>(*p));
  }

  auto result = detect_encoding(data.data(), data.size());
  EXPECT_EQ(result.encoding, Encoding::UTF16_BE);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsUtf32LeWithoutBom) {
  // UTF-32 LE: ASCII character followed by three null bytes
  std::vector<uint8_t> data;
  const char* text = "hello world test more text"; // Need more data for detection
  for (const char* p = text; *p; ++p) {
    data.push_back(static_cast<uint8_t>(*p));
    data.push_back(0x00);
    data.push_back(0x00);
    data.push_back(0x00);
  }

  auto result = detect_encoding(data.data(), data.size());
  EXPECT_EQ(result.encoding, Encoding::UTF32_LE);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsUtf32BeWithoutBom) {
  // UTF-32 BE: three null bytes followed by ASCII character
  std::vector<uint8_t> data;
  const char* text = "hello world test more text"; // Need more data for detection
  for (const char* p = text; *p; ++p) {
    data.push_back(0x00);
    data.push_back(0x00);
    data.push_back(0x00);
    data.push_back(static_cast<uint8_t>(*p));
  }

  auto result = detect_encoding(data.data(), data.size());
  EXPECT_EQ(result.encoding, Encoding::UTF32_BE);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsValidUtf8) {
  // Valid UTF-8 with multibyte characters
  // "café" in UTF-8: 63 61 66 c3 a9
  uint8_t data[] = {0x63, 0x61, 0x66, 0xc3, 0xa9};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, Encoding::UTF8);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, EmptyDataIsUtf8) {
  auto result = detect_encoding(nullptr, 0);
  EXPECT_EQ(result.encoding, Encoding::UTF8);
}

// ============================================================================
// Transcoding Tests
// ============================================================================

class TranscodingTest : public ::testing::Test {};

TEST_F(TranscodingTest, TranscodesUtf16LeToUtf8) {
  // "AB" in UTF-16 LE: 41 00 42 00
  uint8_t data[] = {0x41, 0x00, 0x42, 0x00};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, 2);
  EXPECT_EQ(result.data[0], 'A');
  EXPECT_EQ(result.data[1], 'B');

  aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf16BeToUtf8) {
  // "AB" in UTF-16 BE: 00 41 00 42
  uint8_t data[] = {0x00, 0x41, 0x00, 0x42};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_BE, 0, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, 2);
  EXPECT_EQ(result.data[0], 'A');
  EXPECT_EQ(result.data[1], 'B');

  aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf16LeWithAccents) {
  // "é" (U+00E9) in UTF-16 LE: E9 00
  // In UTF-8 this becomes: C3 A9
  uint8_t data[] = {0xE9, 0x00};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, 2);
  EXPECT_EQ(result.data[0], 0xC3);
  EXPECT_EQ(result.data[1], 0xA9);

  aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf32LeToUtf8) {
  // "AB" in UTF-32 LE
  uint8_t data[] = {0x41, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF32_LE, 0, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, 2);
  EXPECT_EQ(result.data[0], 'A');
  EXPECT_EQ(result.data[1], 'B');

  aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf32BeToUtf8) {
  // "AB" in UTF-32 BE
  uint8_t data[] = {0x00, 0x00, 0x00, 0x41, 0x00, 0x00, 0x00, 0x42};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF32_BE, 0, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, 2);
  EXPECT_EQ(result.data[0], 'A');
  EXPECT_EQ(result.data[1], 'B');

  aligned_free(result.data);
}

TEST_F(TranscodingTest, HandlesUtf16Surrogate) {
  // Emoji "😀" (U+1F600) in UTF-16 LE: D8 3D DE 00 (surrogate pair)
  // High surrogate: D83D, Low surrogate: DE00
  uint8_t data[] = {0x3D, 0xD8, 0x00, 0xDE}; // Little-endian byte order
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

  ASSERT_TRUE(result.success);
  // U+1F600 in UTF-8: F0 9F 98 80
  EXPECT_EQ(result.length, 4);
  EXPECT_EQ(result.data[0], 0xF0);
  EXPECT_EQ(result.data[1], 0x9F);
  EXPECT_EQ(result.data[2], 0x98);
  EXPECT_EQ(result.data[3], 0x80);

  aligned_free(result.data);
}

TEST_F(TranscodingTest, StripsUtf8Bom) {
  // UTF-8 BOM followed by "hi"
  uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'i'};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF8_BOM, 3, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, 2);
  EXPECT_EQ(result.data[0], 'h');
  EXPECT_EQ(result.data[1], 'i');

  aligned_free(result.data);
}

TEST_F(TranscodingTest, RejectsOddLengthUtf16) {
  uint8_t data[] = {0x41, 0x00, 0x42}; // 3 bytes - invalid for UTF-16
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

  EXPECT_FALSE(result.success);
  EXPECT_FALSE(result.error.empty());
}

TEST_F(TranscodingTest, RejectsNonDivisibleUtf32) {
  uint8_t data[] = {0x41, 0x00, 0x00, 0x00, 0x42}; // 5 bytes - invalid for UTF-32
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF32_LE, 0, 32);

  EXPECT_FALSE(result.success);
  EXPECT_FALSE(result.error.empty());
}

// ============================================================================
// File Loading Tests
// ============================================================================

class FileLoadingTest : public ::testing::Test {
protected:
  static std::string test_data_dir() { return "test/data/encoding/"; }
};

TEST_F(FileLoadingTest, LoadsUtf16LeFile) {
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf16_le_bom.csv", 64);
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF16_LE);
    EXPECT_TRUE(result.encoding.needs_transcoding);

    // The data should now be UTF-8
    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_TRUE(content.find("name") != std::string::npos);
    // RAII handles cleanup automatically
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(FileLoadingTest, LoadsUtf16BeFile) {
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf16_be_bom.csv", 64);
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF16_BE);
    EXPECT_TRUE(result.encoding.needs_transcoding);

    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_TRUE(content.find("name") != std::string::npos);
    // RAII handles cleanup automatically
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(FileLoadingTest, LoadsUtf32LeFile) {
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf32_le_bom.csv", 64);
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF32_LE);
    EXPECT_TRUE(result.encoding.needs_transcoding);

    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_TRUE(content.find("name") != std::string::npos);
    // RAII handles cleanup automatically
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(FileLoadingTest, LoadsUtf32BeFile) {
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf32_be_bom.csv", 64);
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF32_BE);
    EXPECT_TRUE(result.encoding.needs_transcoding);

    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_TRUE(content.find("name") != std::string::npos);
    // RAII handles cleanup automatically
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(FileLoadingTest, LoadsUtf8BomFile) {
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf8_bom.csv", 64);
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF8_BOM);
    EXPECT_EQ(result.encoding.bom_length, 3);

    // BOM should be stripped
    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_FALSE(content.empty());
    // Should not start with BOM
    EXPECT_NE(result.data()[0], 0xEF);
    // RAII handles cleanup automatically
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(FileLoadingTest, LoadsLatin1File) {
  try {
    auto result = read_file_with_encoding(test_data_dir() + "latin1.csv", 64);
    // Latin-1 with non-ASCII bytes needs transcoding to UTF-8
    EXPECT_EQ(result.encoding.encoding, Encoding::LATIN1);
    EXPECT_TRUE(result.encoding.needs_transcoding);

    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_FALSE(content.empty());
    // After transcoding, the content should be valid UTF-8
    // Latin-1 "José" should become UTF-8 "José" (é = 0xC3 0xA9)
    EXPECT_NE(content.find("Jos\xC3\xA9"), std::string::npos);
    // RAII handles cleanup automatically
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

// ============================================================================
// encoding_to_string Tests
// ============================================================================

TEST(EncodingToStringTest, ReturnsCorrectStrings) {
  EXPECT_STREQ(encoding_to_string(Encoding::UTF8), "UTF-8");
  EXPECT_STREQ(encoding_to_string(Encoding::UTF8_BOM), "UTF-8 (BOM)");
  EXPECT_STREQ(encoding_to_string(Encoding::UTF16_LE), "UTF-16LE");
  EXPECT_STREQ(encoding_to_string(Encoding::UTF16_BE), "UTF-16BE");
  EXPECT_STREQ(encoding_to_string(Encoding::UTF32_LE), "UTF-32LE");
  EXPECT_STREQ(encoding_to_string(Encoding::UTF32_BE), "UTF-32BE");
  EXPECT_STREQ(encoding_to_string(Encoding::LATIN1), "Latin-1");
  EXPECT_STREQ(encoding_to_string(Encoding::WINDOWS_1252), "Windows-1252");
  EXPECT_STREQ(encoding_to_string(Encoding::UNKNOWN), "Unknown");
}

// ============================================================================
// parse_encoding_name Tests
// ============================================================================

TEST(ParseEncodingNameTest, ParsesUtf8Variants) {
  EXPECT_EQ(parse_encoding_name("utf-8"), Encoding::UTF8);
  EXPECT_EQ(parse_encoding_name("utf8"), Encoding::UTF8);
  EXPECT_EQ(parse_encoding_name("UTF-8"), Encoding::UTF8);
  EXPECT_EQ(parse_encoding_name("UTF8"), Encoding::UTF8);
}

TEST(ParseEncodingNameTest, ParsesUtf16Variants) {
  EXPECT_EQ(parse_encoding_name("utf-16le"), Encoding::UTF16_LE);
  EXPECT_EQ(parse_encoding_name("utf16le"), Encoding::UTF16_LE);
  EXPECT_EQ(parse_encoding_name("utf-16-le"), Encoding::UTF16_LE);
  EXPECT_EQ(parse_encoding_name("UTF-16LE"), Encoding::UTF16_LE);

  EXPECT_EQ(parse_encoding_name("utf-16be"), Encoding::UTF16_BE);
  EXPECT_EQ(parse_encoding_name("utf16be"), Encoding::UTF16_BE);
  EXPECT_EQ(parse_encoding_name("utf-16-be"), Encoding::UTF16_BE);
  EXPECT_EQ(parse_encoding_name("UTF-16BE"), Encoding::UTF16_BE);
}

TEST(ParseEncodingNameTest, ParsesUtf32Variants) {
  EXPECT_EQ(parse_encoding_name("utf-32le"), Encoding::UTF32_LE);
  EXPECT_EQ(parse_encoding_name("utf32le"), Encoding::UTF32_LE);
  EXPECT_EQ(parse_encoding_name("utf-32-le"), Encoding::UTF32_LE);

  EXPECT_EQ(parse_encoding_name("utf-32be"), Encoding::UTF32_BE);
  EXPECT_EQ(parse_encoding_name("utf32be"), Encoding::UTF32_BE);
  EXPECT_EQ(parse_encoding_name("utf-32-be"), Encoding::UTF32_BE);
}

TEST(ParseEncodingNameTest, ParsesLatin1Variants) {
  EXPECT_EQ(parse_encoding_name("latin1"), Encoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("latin-1"), Encoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("iso-8859-1"), Encoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("iso88591"), Encoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("LATIN1"), Encoding::LATIN1);
}

TEST(ParseEncodingNameTest, ParsesWindows1252Variants) {
  EXPECT_EQ(parse_encoding_name("windows-1252"), Encoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("windows1252"), Encoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("cp1252"), Encoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("CP1252"), Encoding::WINDOWS_1252);
}

TEST(ParseEncodingNameTest, ReturnsUnknownForInvalidNames) {
  EXPECT_EQ(parse_encoding_name("invalid"), Encoding::UNKNOWN);
  EXPECT_EQ(parse_encoding_name(""), Encoding::UNKNOWN);
  EXPECT_EQ(parse_encoding_name("utf-7"), Encoding::UNKNOWN);
  EXPECT_EQ(parse_encoding_name("ascii"), Encoding::UNKNOWN);
}

// ============================================================================
// Forced Encoding Tests
// ============================================================================

class ForcedEncodingTest : public ::testing::Test {
protected:
  static std::string test_data_dir() { return "test/data/encoding/"; }
};

TEST_F(ForcedEncodingTest, ForcedUtf8BomStripsActualBom) {
  // Test that forcing UTF8_BOM encoding strips the BOM when present
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf8_bom.csv", 64, Encoding::UTF8_BOM);

    // Should report UTF8_BOM encoding
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF8_BOM);

    // BOM should be detected and stripped
    EXPECT_EQ(result.encoding.bom_length, 3);

    // Content should NOT start with BOM bytes
    ASSERT_GT(result.size, 0);
    EXPECT_NE(result.data()[0], 0xEF);

    // Confidence should be 1.0 for forced encoding
    EXPECT_DOUBLE_EQ(result.encoding.confidence, 1.0);
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(ForcedEncodingTest, ForcedUtf8BomNoBomPresent) {
  // Test that forcing UTF8_BOM encoding works when no BOM is present
  // Use latin1.csv which doesn't have a BOM
  try {
    auto result = read_file_with_encoding(test_data_dir() + "latin1.csv", 64, Encoding::UTF8_BOM);

    // Should report UTF8_BOM encoding (as forced)
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF8_BOM);

    // No BOM found
    EXPECT_EQ(result.encoding.bom_length, 0);

    // Content should be unchanged
    ASSERT_GT(result.size, 0);

    // Confidence should be 1.0 for forced encoding
    EXPECT_DOUBLE_EQ(result.encoding.confidence, 1.0);
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(ForcedEncodingTest, ForcedUtf8NoTranscoding) {
  // Test that forcing UTF-8 encoding doesn't transcode
  try {
    auto result = read_file_with_encoding(test_data_dir() + "utf8_bom.csv", 64, Encoding::UTF8);

    // Should report UTF-8 encoding
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF8);

    // No transcoding needed
    EXPECT_FALSE(result.encoding.needs_transcoding);

    // Content should be unchanged (including the BOM if present)
    ASSERT_GT(result.size, 0);
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(ForcedEncodingTest, ForcedUtf16LeTranscoding) {
  // Test that forcing UTF-16LE encoding transcodes the file
  try {
    auto result =
        read_file_with_encoding(test_data_dir() + "utf16_le_bom.csv", 64, Encoding::UTF16_LE);

    // Should report UTF-16LE encoding
    EXPECT_EQ(result.encoding.encoding, Encoding::UTF16_LE);

    // Transcoding should be performed
    EXPECT_TRUE(result.encoding.needs_transcoding);

    // Content should be valid UTF-8 after transcoding
    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    EXPECT_TRUE(content.find("name") != std::string::npos);
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

TEST_F(ForcedEncodingTest, ForcedLatin1Transcoding) {
  // Test that forcing Latin-1 encoding transcodes the file
  try {
    auto result = read_file_with_encoding(test_data_dir() + "latin1.csv", 64, Encoding::LATIN1);

    // Should report Latin-1 encoding
    EXPECT_EQ(result.encoding.encoding, Encoding::LATIN1);

    // Transcoding should be performed
    EXPECT_TRUE(result.encoding.needs_transcoding);

    // Content should be valid UTF-8 after transcoding
    std::string content(reinterpret_cast<const char*>(result.data()), result.size);
    // Latin-1 "José" should become UTF-8 "José" (é = 0xC3 0xA9)
    EXPECT_NE(content.find("Jos\xC3\xA9"), std::string::npos);
  } catch (const std::exception& e) {
    FAIL() << "Exception: " << e.what();
  }
}

// ============================================================================
// Windows-1252 Transcoding Tests
// ============================================================================

TEST(Windows1252TranscodingTest, TranscodesSmartQuotes) {
  // Windows-1252 smart quotes (0x93 = left double quote, 0x94 = right double quote)
  // These map to U+201C and U+201D
  uint8_t data[] = {0x93, 'h', 'e', 'l', 'l', 'o', 0x94};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::WINDOWS_1252, 0, 32);

  ASSERT_TRUE(result.success);
  // U+201C in UTF-8: E2 80 9C
  // U+201D in UTF-8: E2 80 9D
  // Plus 5 ASCII chars = 5 bytes
  // Total: 3 + 5 + 3 = 11 bytes
  EXPECT_EQ(result.length, 11);

  // Check left quote: E2 80 9C
  EXPECT_EQ(result.data[0], 0xE2);
  EXPECT_EQ(result.data[1], 0x80);
  EXPECT_EQ(result.data[2], 0x9C);

  // Check "hello"
  EXPECT_EQ(result.data[3], 'h');
  EXPECT_EQ(result.data[4], 'e');
  EXPECT_EQ(result.data[5], 'l');
  EXPECT_EQ(result.data[6], 'l');
  EXPECT_EQ(result.data[7], 'o');

  // Check right quote: E2 80 9D
  EXPECT_EQ(result.data[8], 0xE2);
  EXPECT_EQ(result.data[9], 0x80);
  EXPECT_EQ(result.data[10], 0x9D);

  aligned_free(result.data);
}

TEST(Windows1252TranscodingTest, TranscodesEuroSign) {
  // Windows-1252 Euro sign (0x80) maps to U+20AC
  uint8_t data[] = {0x80, '1', '0', '0'};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::WINDOWS_1252, 0, 32);

  ASSERT_TRUE(result.success);
  // U+20AC in UTF-8: E2 82 AC (3 bytes) + 3 ASCII = 6 bytes
  EXPECT_EQ(result.length, 6);

  // Check Euro sign: E2 82 AC
  EXPECT_EQ(result.data[0], 0xE2);
  EXPECT_EQ(result.data[1], 0x82);
  EXPECT_EQ(result.data[2], 0xAC);

  // Check "100"
  EXPECT_EQ(result.data[3], '1');
  EXPECT_EQ(result.data[4], '0');
  EXPECT_EQ(result.data[5], '0');

  aligned_free(result.data);
}

TEST(Windows1252TranscodingTest, TranscodesEmDash) {
  // Windows-1252 em-dash (0x97) maps to U+2014
  uint8_t data[] = {'a', 0x97, 'b'};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::WINDOWS_1252, 0, 32);

  ASSERT_TRUE(result.success);
  // 1 + 3 + 1 = 5 bytes
  EXPECT_EQ(result.length, 5);

  EXPECT_EQ(result.data[0], 'a');
  // U+2014 in UTF-8: E2 80 94
  EXPECT_EQ(result.data[1], 0xE2);
  EXPECT_EQ(result.data[2], 0x80);
  EXPECT_EQ(result.data[3], 0x94);
  EXPECT_EQ(result.data[4], 'b');

  aligned_free(result.data);
}

TEST(Windows1252TranscodingTest, TranscodesEllipsis) {
  // Windows-1252 ellipsis (0x85) maps to U+2026
  uint8_t data[] = {'a', 0x85};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::WINDOWS_1252, 0, 32);

  ASSERT_TRUE(result.success);
  // 1 + 3 = 4 bytes
  EXPECT_EQ(result.length, 4);

  EXPECT_EQ(result.data[0], 'a');
  // U+2026 in UTF-8: E2 80 A6
  EXPECT_EQ(result.data[1], 0xE2);
  EXPECT_EQ(result.data[2], 0x80);
  EXPECT_EQ(result.data[3], 0xA6);

  aligned_free(result.data);
}

TEST(Windows1252TranscodingTest, TranscodesHighBytes) {
  // Windows-1252 high bytes (0xA0-0xFF) should map to U+00A0-U+00FF (same as Latin-1)
  // Test with 0xE9 (é in Latin-1/Windows-1252)
  uint8_t data[] = {'c', 'a', 'f', 0xE9};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::WINDOWS_1252, 0, 32);

  ASSERT_TRUE(result.success);
  // 3 ASCII + 2 bytes for UTF-8 é = 5 bytes
  EXPECT_EQ(result.length, 5);

  // Check "caf"
  EXPECT_EQ(result.data[0], 'c');
  EXPECT_EQ(result.data[1], 'a');
  EXPECT_EQ(result.data[2], 'f');
  // é in UTF-8: C3 A9
  EXPECT_EQ(result.data[3], 0xC3);
  EXPECT_EQ(result.data[4], 0xA9);

  aligned_free(result.data);
}

TEST(Windows1252TranscodingTest, PreservesAscii) {
  // ASCII bytes should pass through unchanged
  uint8_t data[] = {'H', 'e', 'l', 'l', 'o', ',', ' ', 'W', 'o', 'r', 'l', 'd', '!'};
  auto result = transcode_to_utf8(data, sizeof(data), Encoding::WINDOWS_1252, 0, 32);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.length, sizeof(data));

  for (size_t i = 0; i < sizeof(data); ++i) {
    EXPECT_EQ(result.data[i], data[i]) << "Mismatch at position " << i;
  }

  aligned_free(result.data);
}
