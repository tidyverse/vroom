/**
 * @file encoding_test.cpp
 * @brief Tests for character encoding detection and transcoding (Issue #636).
 */

#include "libvroom/encoding.h"
#include "libvroom/io_util.h"
#include "libvroom/vroom.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace libvroom;

// Helper: path to test data files (copied to build dir by CMake)
static std::string testDataPath(const std::string& subpath) {
  return "test/data/encoding/" + subpath;
}

// =============================================================================
// BOM Detection Tests
// =============================================================================

TEST(EncodingDetection, UTF8BOM) {
  const uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'i'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF8_BOM);
  EXPECT_EQ(result.bom_length, 3u);
  EXPECT_FALSE(result.needs_transcoding);
  EXPECT_TRUE(result.success());
}

TEST(EncodingDetection, UTF16LEBOM) {
  const uint8_t data[] = {0xFF, 0xFE, 'h', 0, 'i', 0};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF16_LE);
  EXPECT_EQ(result.bom_length, 2u);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, UTF16BEBOM) {
  const uint8_t data[] = {0xFE, 0xFF, 0, 'h', 0, 'i'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF16_BE);
  EXPECT_EQ(result.bom_length, 2u);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, UTF32LEBOM) {
  const uint8_t data[] = {0xFF, 0xFE, 0x00, 0x00, 'h', 0, 0, 0};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF32_LE);
  EXPECT_EQ(result.bom_length, 4u);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, UTF32BEBOM) {
  const uint8_t data[] = {0x00, 0x00, 0xFE, 0xFF, 0, 0, 0, 'h'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF32_BE);
  EXPECT_EQ(result.bom_length, 4u);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, UTF32LEBOMNotConfusedWithUTF16LE) {
  // The UTF-32 LE BOM starts with FF FE (same as UTF-16 LE BOM)
  // but is followed by 00 00. Must detect as UTF-32 LE, not UTF-16 LE.
  const uint8_t data[] = {0xFF, 0xFE, 0x00, 0x00, 'A', 0, 0, 0};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF32_LE);
  EXPECT_EQ(result.bom_length, 4u);
}

TEST(EncodingDetection, EmptyBuffer) {
  auto result = detect_encoding(nullptr, 0);
  EXPECT_EQ(result.encoding, CharEncoding::UTF8);
  EXPECT_EQ(result.bom_length, 0u);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST(EncodingDetection, TinyBuffer) {
  const uint8_t data[] = {'A'};
  auto result = detect_encoding(data, 1);
  EXPECT_EQ(result.encoding, CharEncoding::UTF8);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST(EncodingDetection, PartialBOM) {
  // Just the first two bytes of a UTF-8 BOM — not a valid BOM
  const uint8_t data[] = {0xEF, 0xBB, 'h', 'i'};
  auto result = detect_encoding(data, sizeof(data));
  // Should not detect as UTF-8 BOM
  EXPECT_NE(result.encoding, CharEncoding::UTF8_BOM);
}

// =============================================================================
// Heuristic Detection Tests
// =============================================================================

TEST(EncodingDetection, PureASCII) {
  const char* text = "name,value\nAlice,100\nBob,200\n";
  auto result = detect_encoding(reinterpret_cast<const uint8_t*>(text), std::strlen(text));
  EXPECT_EQ(result.encoding, CharEncoding::UTF8);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST(EncodingDetection, ValidUTF8WithHighBytes) {
  // UTF-8 encoded "José" = 4A 6F 73 C3 A9
  const uint8_t data[] = {0x4A, 0x6F, 0x73, 0xC3, 0xA9, 0x0A};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF8);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST(EncodingDetection, UTF16LEWithoutBOM) {
  // ASCII text in UTF-16LE: null bytes at odd positions
  const uint8_t data[] = {'n', 0, 'a', 0, 'm', 0, 'e',  0, ',', 0, 'v', 0, 'a',  0,
                          'l', 0, 'u', 0, 'e', 0, '\n', 0, 'A', 0, 'l', 0, 'i',  0,
                          'c', 0, 'e', 0, ',', 0, '1',  0, '0', 0, '0', 0, '\n', 0};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF16_LE);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, UTF16BEWithoutBOM) {
  // ASCII text in UTF-16BE: null bytes at even positions
  const uint8_t data[] = {0, 'n', 0, 'a', 0, 'm', 0, 'e',  0, ',', 0, 'v', 0, 'a',
                          0, 'l', 0, 'u', 0, 'e', 0, '\n', 0, 'A', 0, 'l', 0, 'i',
                          0, 'c', 0, 'e', 0, ',', 0, '1',  0, '0', 0, '0', 0, '\n'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::UTF16_BE);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, Latin1SingleByteHighBytes) {
  // Latin-1: bytes in 0xA0-0xFF range, no 0x80-0x9F bytes
  // "café" in Latin-1: 63 61 66 E9
  const uint8_t data[] = {'c', 'a', 'f', 0xE9, '\n'};
  auto result = detect_encoding(data, sizeof(data));
  // Should detect as either Latin-1 or Windows-1252 (both valid)
  EXPECT_TRUE(result.encoding == CharEncoding::LATIN1 ||
              result.encoding == CharEncoding::WINDOWS_1252);
  EXPECT_TRUE(result.needs_transcoding);
}

TEST(EncodingDetection, Windows1252SmartQuotes) {
  // Windows-1252 "smart quotes": 0x93 = left double quote, 0x94 = right double quote
  const uint8_t data[] = {'H', 'e', ' ', 's', 'a', 'i', 'd', ' ', 0x93, 'h', 'i', 0x94, '\n'};
  auto result = detect_encoding(data, sizeof(data));
  EXPECT_EQ(result.encoding, CharEncoding::WINDOWS_1252);
  EXPECT_TRUE(result.needs_transcoding);
}

// =============================================================================
// Encoding Name Parsing Tests
// =============================================================================

TEST(EncodingNameParsing, UTF8Variants) {
  EXPECT_EQ(parse_encoding_name("utf-8"), CharEncoding::UTF8);
  EXPECT_EQ(parse_encoding_name("UTF-8"), CharEncoding::UTF8);
  EXPECT_EQ(parse_encoding_name("utf8"), CharEncoding::UTF8);
  EXPECT_EQ(parse_encoding_name("UTF8"), CharEncoding::UTF8);
}

TEST(EncodingNameParsing, UTF16Variants) {
  EXPECT_EQ(parse_encoding_name("utf-16le"), CharEncoding::UTF16_LE);
  EXPECT_EQ(parse_encoding_name("UTF-16LE"), CharEncoding::UTF16_LE);
  EXPECT_EQ(parse_encoding_name("utf-16be"), CharEncoding::UTF16_BE);
  EXPECT_EQ(parse_encoding_name("UTF-16BE"), CharEncoding::UTF16_BE);
  EXPECT_EQ(parse_encoding_name("utf16le"), CharEncoding::UTF16_LE);
  EXPECT_EQ(parse_encoding_name("utf16be"), CharEncoding::UTF16_BE);
}

TEST(EncodingNameParsing, UTF32Variants) {
  EXPECT_EQ(parse_encoding_name("utf-32le"), CharEncoding::UTF32_LE);
  EXPECT_EQ(parse_encoding_name("utf-32be"), CharEncoding::UTF32_BE);
  EXPECT_EQ(parse_encoding_name("UTF-32LE"), CharEncoding::UTF32_LE);
  EXPECT_EQ(parse_encoding_name("UTF-32BE"), CharEncoding::UTF32_BE);
}

TEST(EncodingNameParsing, Latin1Variants) {
  EXPECT_EQ(parse_encoding_name("latin1"), CharEncoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("Latin1"), CharEncoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("iso-8859-1"), CharEncoding::LATIN1);
  EXPECT_EQ(parse_encoding_name("ISO-8859-1"), CharEncoding::LATIN1);
}

TEST(EncodingNameParsing, Windows1252Variants) {
  EXPECT_EQ(parse_encoding_name("windows-1252"), CharEncoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("Windows-1252"), CharEncoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("cp1252"), CharEncoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("CP1252"), CharEncoding::WINDOWS_1252);
  EXPECT_EQ(parse_encoding_name("win-1252"), CharEncoding::WINDOWS_1252);
}

TEST(EncodingNameParsing, Unknown) {
  EXPECT_EQ(parse_encoding_name("ebcdic"), CharEncoding::UNKNOWN);
  EXPECT_EQ(parse_encoding_name(""), CharEncoding::UNKNOWN);
  EXPECT_EQ(parse_encoding_name("invalid"), CharEncoding::UNKNOWN);
}

// =============================================================================
// encoding_to_string Tests
// =============================================================================

TEST(EncodingToString, AllValues) {
  EXPECT_STREQ(encoding_to_string(CharEncoding::UTF8), "UTF-8");
  EXPECT_STREQ(encoding_to_string(CharEncoding::UTF8_BOM), "UTF-8 (BOM)");
  EXPECT_STREQ(encoding_to_string(CharEncoding::UTF16_LE), "UTF-16LE");
  EXPECT_STREQ(encoding_to_string(CharEncoding::UTF16_BE), "UTF-16BE");
  EXPECT_STREQ(encoding_to_string(CharEncoding::UTF32_LE), "UTF-32LE");
  EXPECT_STREQ(encoding_to_string(CharEncoding::UTF32_BE), "UTF-32BE");
  EXPECT_STREQ(encoding_to_string(CharEncoding::LATIN1), "Latin-1");
  EXPECT_STREQ(encoding_to_string(CharEncoding::WINDOWS_1252), "Windows-1252");
  EXPECT_STREQ(encoding_to_string(CharEncoding::UNKNOWN), "Unknown");
}

// =============================================================================
// Transcoding Tests
// =============================================================================

TEST(Transcoding, UTF8BOMStripped) {
  const uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'i'};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF8_BOM, 3);
  ASSERT_TRUE(buf.valid());
  EXPECT_EQ(buf.size(), 2u);
  EXPECT_EQ(buf.data()[0], 'h');
  EXPECT_EQ(buf.data()[1], 'i');
}

TEST(Transcoding, UTF16LEBasic) {
  // "hi\n" in UTF-16LE
  const uint8_t data[] = {'h', 0, 'i', 0, '\n', 0};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF16_LE, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "hi\n");
}

TEST(Transcoding, UTF16LEWithBOM) {
  // BOM + "hi\n" in UTF-16LE
  const uint8_t data[] = {0xFF, 0xFE, 'h', 0, 'i', 0, '\n', 0};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF16_LE, 2);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "hi\n");
}

TEST(Transcoding, UTF16BEBasic) {
  // "hi\n" in UTF-16BE
  const uint8_t data[] = {0, 'h', 0, 'i', 0, '\n'};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF16_BE, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "hi\n");
}

TEST(Transcoding, UTF16LEAccentedChars) {
  // "José\n" in UTF-16LE: J=4A00, o=6F00, s=7300, é=E900, \n=0A00
  const uint8_t data[] = {0x4A, 0x00, 0x6F, 0x00, 0x73, 0x00, 0xE9, 0x00, 0x0A, 0x00};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF16_LE, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "Jos\xC3\xA9\n"); // UTF-8 é = C3 A9
}

TEST(Transcoding, UTF32LEBasic) {
  // "hi\n" in UTF-32LE
  const uint8_t data[] = {'h', 0, 0, 0, 'i', 0, 0, 0, '\n', 0, 0, 0};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF32_LE, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "hi\n");
}

TEST(Transcoding, UTF32BEBasic) {
  // "hi\n" in UTF-32BE
  const uint8_t data[] = {0, 0, 0, 'h', 0, 0, 0, 'i', 0, 0, 0, '\n'};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::UTF32_BE, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "hi\n");
}

TEST(Transcoding, Latin1Basic) {
  // "café\n" in Latin-1: 63 61 66 E9 0A
  const uint8_t data[] = {0x63, 0x61, 0x66, 0xE9, 0x0A};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::LATIN1, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  EXPECT_EQ(result, "caf\xC3\xA9\n"); // UTF-8 é = C3 A9
}

TEST(Transcoding, Windows1252SmartQuotes) {
  // 0x93 = left double quote (U+201C), 0x94 = right double quote (U+201D)
  const uint8_t data[] = {0x93, 'h', 'i', 0x94};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::WINDOWS_1252, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  // U+201C = E2 80 9C, U+201D = E2 80 9D
  EXPECT_EQ(result, "\xE2\x80\x9C"
                    "hi"
                    "\xE2\x80\x9D");
}

TEST(Transcoding, Windows1252EuroSign) {
  // 0x80 = Euro sign (U+20AC)
  const uint8_t data[] = {0x80};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::WINDOWS_1252, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  // U+20AC = E2 82 AC
  EXPECT_EQ(result, "\xE2\x82\xAC");
}

TEST(Transcoding, Windows1252UndefinedByte) {
  // 0x81 is undefined in Windows-1252, should map to U+FFFD
  const uint8_t data[] = {0x81};
  auto buf = transcode_to_utf8(data, sizeof(data), CharEncoding::WINDOWS_1252, 0);
  ASSERT_TRUE(buf.valid());
  std::string result(reinterpret_cast<const char*>(buf.data()), buf.size());
  // U+FFFD = EF BF BD
  EXPECT_EQ(result, "\xEF\xBF\xBD");
}

TEST(Transcoding, UnknownEncodingThrows) {
  const uint8_t data[] = {'h', 'i'};
  EXPECT_THROW(transcode_to_utf8(data, sizeof(data), CharEncoding::UNKNOWN, 0), std::runtime_error);
}

// =============================================================================
// File Loading Tests
// =============================================================================

TEST(EncodingFileLoading, UTF8BOMFile) {
  auto buf = load_file_to_ptr(testDataPath("utf8_bom.csv"));
  auto result = detect_encoding(buf.data(), buf.size());
  EXPECT_EQ(result.encoding, CharEncoding::UTF8_BOM);
  EXPECT_EQ(result.bom_length, 3u);
  EXPECT_FALSE(result.needs_transcoding);
}

TEST(EncodingFileLoading, UTF16LEBOMFile) {
  auto buf = load_file_to_ptr(testDataPath("utf16_le_bom.csv"));
  auto result = detect_encoding(buf.data(), buf.size());
  EXPECT_EQ(result.encoding, CharEncoding::UTF16_LE);
  EXPECT_EQ(result.bom_length, 2u);
  EXPECT_TRUE(result.needs_transcoding);

  // Transcode and verify content
  auto utf8 = transcode_to_utf8(buf.data(), buf.size(), result.encoding, result.bom_length);
  std::string content(reinterpret_cast<const char*>(utf8.data()), utf8.size());
  EXPECT_NE(content.find("name,city,country"), std::string::npos);
  EXPECT_NE(content.find("Jos"), std::string::npos);
}

TEST(EncodingFileLoading, UTF16BEBOMFile) {
  auto buf = load_file_to_ptr(testDataPath("utf16_be_bom.csv"));
  auto result = detect_encoding(buf.data(), buf.size());
  EXPECT_EQ(result.encoding, CharEncoding::UTF16_BE);
  EXPECT_EQ(result.bom_length, 2u);
  EXPECT_TRUE(result.needs_transcoding);

  auto utf8 = transcode_to_utf8(buf.data(), buf.size(), result.encoding, result.bom_length);
  std::string content(reinterpret_cast<const char*>(utf8.data()), utf8.size());
  EXPECT_NE(content.find("name,city,country"), std::string::npos);
}

TEST(EncodingFileLoading, UTF32LEBOMFile) {
  auto buf = load_file_to_ptr(testDataPath("utf32_le_bom.csv"));
  auto result = detect_encoding(buf.data(), buf.size());
  EXPECT_EQ(result.encoding, CharEncoding::UTF32_LE);
  EXPECT_EQ(result.bom_length, 4u);
  EXPECT_TRUE(result.needs_transcoding);

  auto utf8 = transcode_to_utf8(buf.data(), buf.size(), result.encoding, result.bom_length);
  std::string content(reinterpret_cast<const char*>(utf8.data()), utf8.size());
  EXPECT_NE(content.find("name,city,country"), std::string::npos);
}

TEST(EncodingFileLoading, UTF32BEBOMFile) {
  auto buf = load_file_to_ptr(testDataPath("utf32_be_bom.csv"));
  auto result = detect_encoding(buf.data(), buf.size());
  EXPECT_EQ(result.encoding, CharEncoding::UTF32_BE);
  EXPECT_EQ(result.bom_length, 4u);
  EXPECT_TRUE(result.needs_transcoding);

  auto utf8 = transcode_to_utf8(buf.data(), buf.size(), result.encoding, result.bom_length);
  std::string content(reinterpret_cast<const char*>(utf8.data()), utf8.size());
  EXPECT_NE(content.find("name,city,country"), std::string::npos);
}

TEST(EncodingFileLoading, Latin1File) {
  auto buf = load_file_to_ptr(testDataPath("latin1.csv"));
  auto result = detect_encoding(buf.data(), buf.size());
  // Latin-1 with accented chars (0xE9, 0xE1, 0xE7) in 0xA0-0xFF range
  // These are not valid UTF-8, so should be detected as LATIN1 or WINDOWS_1252
  EXPECT_TRUE(result.encoding == CharEncoding::LATIN1 ||
              result.encoding == CharEncoding::WINDOWS_1252);
  EXPECT_TRUE(result.needs_transcoding);

  // Transcode with Latin-1
  auto utf8 = transcode_to_utf8(buf.data(), buf.size(), CharEncoding::LATIN1, 0);
  std::string content(reinterpret_cast<const char*>(utf8.data()), utf8.size());
  EXPECT_NE(content.find("name,city"), std::string::npos);
  // "José" should now be valid UTF-8
  EXPECT_NE(content.find("Jos\xC3\xA9"), std::string::npos);
}

// =============================================================================
// CsvReader Integration Tests
// =============================================================================

TEST(CsvReaderEncoding, UTF8BOMFile) {
  CsvOptions opts;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("utf8_bom.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF8_BOM);
  EXPECT_EQ(reader.encoding().bom_length, 3u);

  // Verify header is parsed correctly (BOM was stripped)
  ASSERT_EQ(reader.schema().size(), 2u);
  EXPECT_EQ(reader.schema()[0].name, "name");
  EXPECT_EQ(reader.schema()[1].name, "value");

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;
  EXPECT_EQ(read_result.value.total_rows, 2u);
}

TEST(CsvReaderEncoding, UTF16LEBOMFile) {
  CsvOptions opts;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("utf16_le_bom.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF16_LE);
  EXPECT_TRUE(reader.encoding().needs_transcoding);

  // Verify header is parsed correctly after transcoding
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "name");
  EXPECT_EQ(reader.schema()[1].name, "city");
  EXPECT_EQ(reader.schema()[2].name, "country");

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;
  EXPECT_GT(read_result.value.total_rows, 0u);
}

TEST(CsvReaderEncoding, UTF16BEBOMFile) {
  CsvOptions opts;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("utf16_be_bom.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF16_BE);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "name");
}

TEST(CsvReaderEncoding, UTF32LEBOMFile) {
  CsvOptions opts;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("utf32_le_bom.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF32_LE);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "name");
}

TEST(CsvReaderEncoding, UTF32BEBOMFile) {
  CsvOptions opts;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("utf32_be_bom.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF32_BE);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "name");
}

TEST(CsvReaderEncoding, Latin1FileWithForcedEncoding) {
  CsvOptions opts;
  opts.encoding = CharEncoding::LATIN1;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("latin1.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::LATIN1);
  ASSERT_EQ(reader.schema().size(), 2u);
  EXPECT_EQ(reader.schema()[0].name, "name");
  EXPECT_EQ(reader.schema()[1].name, "city");

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;
  EXPECT_EQ(read_result.value.total_rows, 2u);
}

TEST(CsvReaderEncoding, ForcedEncodingOverridesAutoDetect) {
  // Force UTF-16LE on a UTF-16 LE BOM file — should still work
  CsvOptions opts;
  opts.encoding = CharEncoding::UTF16_LE;
  CsvReader reader(opts);
  auto result = reader.open(testDataPath("utf16_le_bom.csv"));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF16_LE);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "name");
}

TEST(CsvReaderEncoding, OpenFromBufferUTF16LE) {
  // Load a UTF-16LE file into a buffer and open_from_buffer
  auto file_buf = load_file_to_ptr(testDataPath("utf16_le_bom.csv"));
  CsvOptions opts;
  CsvReader reader(opts);
  auto result = reader.open_from_buffer(std::move(file_buf));
  ASSERT_TRUE(result.ok) << result.error;

  EXPECT_EQ(reader.encoding().encoding, CharEncoding::UTF16_LE);
  ASSERT_EQ(reader.schema().size(), 3u);
  EXPECT_EQ(reader.schema()[0].name, "name");
}
