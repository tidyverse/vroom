/**
 * @file debug_test.cpp
 * @brief Tests for the debug mode functionality.
 */

#include "common_defs.h"
#include "debug.h"
#include "debug_parser.h"
#include "error.h"

#include <cstdio>
#include <cstring>
#include <gtest/gtest.h>
#include <vector>

using namespace libvroom;

class DebugTest : public ::testing::Test {
protected:
  void SetUp() override { output_file_ = tmpfile(); }

  void TearDown() override {
    if (output_file_) {
      fclose(output_file_);
    }
  }

  std::string get_output() {
    if (!output_file_)
      return "";
    fflush(output_file_);
    rewind(output_file_);
    std::string result;
    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), output_file_)) {
      result += buffer;
    }
    return result;
  }

  // Create a padded buffer from a string for SIMD-safe parsing
  std::vector<uint8_t> makeBuffer(const char* str) {
    size_t len = strlen(str);
    std::vector<uint8_t> buf(len + LIBVROOM_PADDING);
    std::memcpy(buf.data(), str, len);
    std::memset(buf.data() + len, 0, LIBVROOM_PADDING);
    return buf;
  }

  FILE* output_file_ = nullptr;
};

TEST_F(DebugTest, DebugConfigDefaults) {
  DebugConfig config;
  EXPECT_FALSE(config.verbose);
  EXPECT_FALSE(config.dump_masks);
  EXPECT_FALSE(config.timing);
  EXPECT_FALSE(config.enabled());
}

TEST_F(DebugTest, DebugConfigAll) {
  DebugConfig config = DebugConfig::all();
  EXPECT_TRUE(config.verbose);
  EXPECT_TRUE(config.dump_masks);
  EXPECT_TRUE(config.timing);
  EXPECT_TRUE(config.enabled());
}

TEST_F(DebugTest, DebugTraceLog) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log("Test message %d", 42);

  std::string output = get_output();
  EXPECT_NE(output.find("[libvroom] Test message 42"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log("This should not appear");

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceDumpMask) {
  DebugConfig config;
  config.dump_masks = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.dump_mask("test_mask", 0xFF, 0);

  std::string output = get_output();
  EXPECT_NE(output.find("MASK test_mask"), std::string::npos);
  EXPECT_NE(output.find("hex:"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceTiming) {
  DebugConfig config;
  config.timing = true;
  DebugTrace trace(config);

  trace.start_phase("test_phase");
  trace.end_phase(1000);

  const auto& times = trace.get_phase_times();
  EXPECT_EQ(times.size(), 1u);
  EXPECT_EQ(times[0].name, "test_phase");
  EXPECT_EQ(times[0].bytes_processed, 1000u);
}

// Test PhaseTime struct methods
TEST_F(DebugTest, PhaseTimeSeconds) {
  PhaseTime pt;
  pt.name = "test";
  pt.duration = std::chrono::nanoseconds(1000000000); // 1 second
  pt.bytes_processed = 1000;

  EXPECT_DOUBLE_EQ(pt.seconds(), 1.0);
}

TEST_F(DebugTest, PhaseTimeThroughputGbps) {
  PhaseTime pt;
  pt.name = "test";
  pt.duration = std::chrono::nanoseconds(1000000000); // 1 second
  pt.bytes_processed = 1000000000;                    // 1 GB

  EXPECT_DOUBLE_EQ(pt.throughput_gbps(), 1.0);
}

TEST_F(DebugTest, PhaseTimeThroughputZeroBytes) {
  PhaseTime pt;
  pt.name = "test";
  pt.duration = std::chrono::nanoseconds(1000000000);
  pt.bytes_processed = 0;

  EXPECT_DOUBLE_EQ(pt.throughput_gbps(), 0.0);
}

TEST_F(DebugTest, PhaseTimeThroughputZeroDuration) {
  PhaseTime pt;
  pt.name = "test";
  pt.duration = std::chrono::nanoseconds(0);
  pt.bytes_processed = 1000;

  EXPECT_DOUBLE_EQ(pt.throughput_gbps(), 0.0);
}

// Test DebugTrace logging methods
TEST_F(DebugTest, DebugTraceLogStr) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_str("Safe string message");

  std::string output = get_output();
  EXPECT_NE(output.find("[libvroom] Safe string message"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogStrDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_str("This should not appear");

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceLogDecision) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_decision("Use SIMD", "Data size is large enough");

  std::string output = get_output();
  EXPECT_NE(output.find("DECISION: Use SIMD"), std::string::npos);
  EXPECT_NE(output.find("Reason: Data size is large enough"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogDecisionDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_decision("Use SIMD", "Data size is large enough");

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceLogSimdPath) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_simd_path("AVX2", 32);

  std::string output = get_output();
  EXPECT_NE(output.find("SIMD: Using AVX2 path"), std::string::npos);
  EXPECT_NE(output.find("32 bytes"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogSimdPathDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_simd_path("AVX2", 32);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceLogThreading) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_threading(4, 65536);

  std::string output = get_output();
  EXPECT_NE(output.find("Threading: 4 threads"), std::string::npos);
  EXPECT_NE(output.find("65536 bytes"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogThreadingDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_threading(4, 65536);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

// Test DebugTrace dump methods
TEST_F(DebugTest, DebugTraceDumpBuffer) {
  DebugConfig config;
  config.dump_masks = true;
  config.output = output_file_;
  DebugTrace trace(config);

  uint8_t buf[] = {0x48, 0x65, 0x6c, 0x6c, 0x6f}; // "Hello"
  trace.dump_buffer("test_buffer", buf, sizeof(buf), 0);

  std::string output = get_output();
  EXPECT_NE(output.find("BUFFER test_buffer"), std::string::npos);
  EXPECT_NE(output.find("hex:"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceDumpBufferDisabled) {
  DebugConfig config;
  config.dump_masks = false;
  config.output = output_file_;
  DebugTrace trace(config);

  uint8_t buf[] = {0x48, 0x65, 0x6c, 0x6c, 0x6f};
  trace.dump_buffer("test_buffer", buf, sizeof(buf), 0);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceDumpBufferLarge) {
  DebugConfig config;
  config.dump_masks = true;
  config.dump_context_bytes = 32; // Limit to 32 bytes
  config.output = output_file_;
  DebugTrace trace(config);

  uint8_t buf[128];
  for (size_t i = 0; i < sizeof(buf); ++i)
    buf[i] = static_cast<uint8_t>(i);
  trace.dump_buffer("large_buffer", buf, sizeof(buf), 100);

  std::string output = get_output();
  EXPECT_NE(output.find("BUFFER large_buffer @ offset 100"), std::string::npos);
  EXPECT_NE(output.find("showing 32 of 128 bytes"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceDumpMaskDisabled) {
  DebugConfig config;
  config.dump_masks = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.dump_mask("test_mask", 0xFF, 0);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceDumpIndexes) {
  DebugConfig config;
  config.dump_masks = true;
  config.max_indexes_dump = 5;
  config.output = output_file_;
  DebugTrace trace(config);

  uint64_t indexes[] = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  trace.dump_indexes(indexes, 10, 0, 1, 10);

  std::string output = get_output();
  EXPECT_NE(output.find("INDEXES thread 0"), std::string::npos);
  EXPECT_NE(output.find("showing 5 of 10"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceDumpIndexesDisabled) {
  DebugConfig config;
  config.dump_masks = false;
  config.output = output_file_;
  DebugTrace trace(config);

  uint64_t indexes[] = {10, 20, 30};
  trace.dump_indexes(indexes, 3, 0, 1, 3);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceDumpIndexesBoundsCheck) {
  DebugConfig config;
  config.dump_masks = true;
  config.max_indexes_dump = 100; // Try to dump more than total_size
  config.output = output_file_;
  DebugTrace trace(config);

  uint64_t indexes[] = {10, 20, 30};
  trace.dump_indexes(indexes, 10, 0, 1, 3); // count=10 but total_size=3

  std::string output = get_output();
  EXPECT_NE(output.find("INDEXES thread 0"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceDumpChunkBoundaries) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  std::vector<uint64_t> chunk_pos = {0, 1000, 2000, 3000};
  trace.dump_chunk_boundaries(chunk_pos, 3);

  std::string output = get_output();
  EXPECT_NE(output.find("CHUNK BOUNDARIES"), std::string::npos);
  EXPECT_NE(output.find("chunk[0]: 0"), std::string::npos);
  EXPECT_NE(output.find("size: 1000 bytes"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceDumpChunkBoundariesDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  std::vector<uint64_t> chunk_pos = {0, 1000, 2000};
  trace.dump_chunk_boundaries(chunk_pos, 2);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

// Test DebugTrace log methods for parsing
TEST_F(DebugTest, DebugTraceLogFirstPassResult) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_first_pass_result(0, 10, 100, 150);

  std::string output = get_output();
  EXPECT_NE(output.find("FIRST PASS chunk 0"), std::string::npos);
  EXPECT_NE(output.find("quotes=10"), std::string::npos);
  EXPECT_NE(output.find("first_even_nl=100"), std::string::npos);
  EXPECT_NE(output.find("first_odd_nl=150"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogFirstPassResultDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_first_pass_result(0, 10, 100, 150);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceLogDialect) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_dialect(',', '"', 0.95);

  std::string output = get_output();
  EXPECT_NE(output.find("DIALECT:"), std::string::npos);
  EXPECT_NE(output.find("delimiter=','"), std::string::npos);
  EXPECT_NE(output.find("quote='\"'"), std::string::npos);
  EXPECT_NE(output.find("95.00%"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogDialectSpecialChars) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_dialect('\t', '"', 0.80);

  std::string output = get_output();
  EXPECT_NE(output.find("delimiter='\\t'"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogDialectDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_dialect(',', '"', 0.95);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceLogStateTransition) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_state_transition("FIELD_START", "QUOTED_FIELD", '"', 42);

  std::string output = get_output();
  EXPECT_NE(output.find("STATE @ 42"), std::string::npos);
  EXPECT_NE(output.find("FIELD_START -> QUOTED_FIELD"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogStateTransitionNewline) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_state_transition("UNQUOTED_FIELD", "RECORD_START", '\n', 100);

  std::string output = get_output();
  EXPECT_NE(output.find("trigger: '\\n'"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogStateTransitionCarriageReturn) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_state_transition("UNQUOTED_FIELD", "RECORD_START", '\r', 100);

  std::string output = get_output();
  EXPECT_NE(output.find("trigger: '\\r'"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogStateTransitionTab) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_state_transition("FIELD_START", "UNQUOTED_FIELD", '\t', 50);

  std::string output = get_output();
  EXPECT_NE(output.find("trigger: '\\t'"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogStateTransitionNonPrintable) {
  DebugConfig config;
  config.verbose = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_state_transition("FIELD_START", "UNQUOTED_FIELD", '\x01', 50);

  std::string output = get_output();
  EXPECT_NE(output.find("trigger: '\\x01'"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogStateTransitionDisabled) {
  DebugConfig config;
  config.verbose = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.log_state_transition("FIELD_START", "QUOTED_FIELD", '"', 42);

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, SimdPathName) {
  const char* path = get_simd_path_name();
  EXPECT_NE(path, nullptr);
  EXPECT_GT(strlen(path), 0u);
}

TEST_F(DebugTest, SimdVectorBytes) {
  size_t bytes = get_simd_vector_bytes();
  EXPECT_GE(bytes, 16u);
  EXPECT_LE(bytes, 64u);
}

TEST_F(DebugTest, DebugParserParse) {
  DebugConfig config;
  config.verbose = true;
  config.timing = true;
  config.output = output_file_;
  DebugTrace trace(config);

  debug_parser parser;
  const char* csv = "a,b,c\n1,2,3\n";
  auto buf = makeBuffer(csv);
  size_t len = strlen(csv);

  libvroom::ParseIndex idx = parser.init(len, 1);
  bool result = parser.parse_debug(buf.data(), idx, len, trace);

  EXPECT_TRUE(result);

  std::string output = get_output();
  EXPECT_NE(output.find("[libvroom]"), std::string::npos);
  EXPECT_NE(output.find("Starting parse"), std::string::npos);
}

// Test ScopedPhaseTimer
TEST_F(DebugTest, ScopedPhaseTimer) {
  DebugConfig config;
  config.timing = true;
  DebugTrace trace(config);

  {
    ScopedPhaseTimer timer(trace, "test_phase", 1000);
    // Timer auto-ends when destroyed
  }

  const auto& times = trace.get_phase_times();
  EXPECT_EQ(times.size(), 1u);
  EXPECT_EQ(times[0].name, "test_phase");
  EXPECT_EQ(times[0].bytes_processed, 1000u);
}

TEST_F(DebugTest, ScopedPhaseTimerSetBytes) {
  DebugConfig config;
  config.timing = true;
  DebugTrace trace(config);

  {
    ScopedPhaseTimer timer(trace, "test_phase", 0);
    timer.set_bytes(2000);
    // Timer auto-ends with updated bytes
  }

  const auto& times = trace.get_phase_times();
  EXPECT_EQ(times.size(), 1u);
  EXPECT_EQ(times[0].bytes_processed, 2000u);
}

// Test clear_timing
TEST_F(DebugTest, DebugTraceClearTiming) {
  DebugConfig config;
  config.timing = true;
  DebugTrace trace(config);

  trace.start_phase("phase1");
  trace.end_phase(100);
  trace.start_phase("phase2");
  trace.end_phase(200);

  EXPECT_EQ(trace.get_phase_times().size(), 2u);

  trace.clear_timing();

  EXPECT_EQ(trace.get_phase_times().size(), 0u);
}

// Test print_timing_summary
TEST_F(DebugTest, DebugTracePrintTimingSummary) {
  DebugConfig config;
  config.timing = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.start_phase("test_phase");
  trace.end_phase(1000000);

  trace.print_timing_summary();

  std::string output = get_output();
  EXPECT_NE(output.find("TIMING SUMMARY"), std::string::npos);
  EXPECT_NE(output.find("test_phase"), std::string::npos);
  EXPECT_NE(output.find("TOTAL"), std::string::npos);
}

TEST_F(DebugTest, DebugTracePrintTimingSummaryEmpty) {
  DebugConfig config;
  config.timing = true;
  config.output = output_file_;
  DebugTrace trace(config);

  // No phases recorded
  trace.print_timing_summary();

  std::string output = get_output();
  EXPECT_TRUE(output.empty()); // Should not print anything when empty
}

TEST_F(DebugTest, DebugTracePrintTimingSummaryDisabled) {
  DebugConfig config;
  config.timing = false;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.print_timing_summary();

  std::string output = get_output();
  EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTracePrintTimingSummaryWithThroughput) {
  DebugConfig config;
  config.timing = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.start_phase("io_phase");
  trace.end_phase(1000000000); // 1GB

  trace.print_timing_summary();

  std::string output = get_output();
  EXPECT_NE(output.find("GB/s"), std::string::npos);
}

// Test timing disabled paths
TEST_F(DebugTest, DebugTraceTimingDisabled) {
  DebugConfig config;
  config.timing = false;
  DebugTrace trace(config);

  trace.start_phase("test_phase");
  trace.end_phase(1000);

  const auto& times = trace.get_phase_times();
  EXPECT_EQ(times.size(), 0u); // Nothing should be recorded
}

// Test global debug config functions
TEST_F(DebugTest, GlobalDebugConfig) {
  DebugConfig config;
  config.verbose = true;
  config.dump_masks = true;
  config.timing = true;

  debug::set_config(config);

  EXPECT_TRUE(debug::global_config().verbose);
  EXPECT_TRUE(debug::global_config().dump_masks);
  EXPECT_TRUE(debug::global_config().timing);
  EXPECT_TRUE(debug::enabled());

  // Reset to defaults
  debug::set_config(DebugConfig());
  EXPECT_FALSE(debug::enabled());
}

TEST_F(DebugTest, GlobalDebugTrace) {
  DebugTrace& trace = debug::global_trace();
  EXPECT_FALSE(trace.enabled()); // Default config is disabled
}

// Test DebugConfig::enabled with partial settings
TEST_F(DebugTest, DebugConfigEnabledVerboseOnly) {
  DebugConfig config;
  config.verbose = true;
  config.dump_masks = false;
  config.timing = false;
  EXPECT_TRUE(config.enabled());
}

TEST_F(DebugTest, DebugConfigEnabledDumpMasksOnly) {
  DebugConfig config;
  config.verbose = false;
  config.dump_masks = true;
  config.timing = false;
  EXPECT_TRUE(config.enabled());
}

TEST_F(DebugTest, DebugConfigEnabledTimingOnly) {
  DebugConfig config;
  config.verbose = false;
  config.dump_masks = false;
  config.timing = true;
  EXPECT_TRUE(config.enabled());
}

// Test DebugTrace accessor methods
TEST_F(DebugTest, DebugTraceAccessors) {
  DebugConfig config = DebugConfig::all();
  DebugTrace trace(config);

  EXPECT_TRUE(trace.enabled());
  EXPECT_TRUE(trace.verbose());
  EXPECT_TRUE(trace.dump_masks());
  EXPECT_TRUE(trace.timing());
}

// Test get_simd_info
TEST_F(DebugTest, GetSimdInfo) {
  std::string info = get_simd_info();
  EXPECT_FALSE(info.empty());
  EXPECT_NE(info.find("-byte vectors"), std::string::npos);
}

// Test debug_parser pass-through methods
TEST_F(DebugTest, DebugParserPassThrough) {
  debug_parser parser;
  const char* csv = "a,b,c\n1,2,3\n";
  auto buf = makeBuffer(csv);
  size_t len = strlen(csv);

  libvroom::ParseIndex idx = parser.init(len, 1);
  bool result = parser.parse(buf.data(), idx, len);

  EXPECT_TRUE(result);
}

TEST_F(DebugTest, DebugParserParseWithErrors) {
  debug_parser parser;
  const char* csv = "a,b,c\n1,2,3\n";
  auto buf = makeBuffer(csv);
  size_t len = strlen(csv);

  libvroom::ParseIndex idx = parser.init(len, 1);
  libvroom::ErrorCollector errors;
  bool result = parser.parse_with_errors(buf.data(), idx, len, errors);

  EXPECT_TRUE(result);
  EXPECT_EQ(errors.error_count(), 0u);
}

TEST_F(DebugTest, DebugParserParseWithErrorsDebug) {
  DebugConfig config;
  config.verbose = true;
  config.dump_masks = true;
  config.timing = true;
  config.output = output_file_;
  DebugTrace trace(config);

  debug_parser parser;
  const char* csv = "a,b,c\n1,2,3\n";
  auto buf = makeBuffer(csv);
  size_t len = strlen(csv);

  libvroom::ParseIndex idx = parser.init(len, 1);
  libvroom::ErrorCollector errors;
  bool result = parser.parse_with_errors_debug(buf.data(), idx, len, errors, trace);

  EXPECT_TRUE(result);

  std::string output = get_output();
  EXPECT_NE(output.find("[libvroom]"), std::string::npos);
  EXPECT_NE(output.find("Starting parse_with_errors"), std::string::npos);
  EXPECT_NE(output.find("Parse complete"), std::string::npos);
}

// Test debug_parser parse_debug with dump_masks enabled
TEST_F(DebugTest, DebugParserParseDebugWithMasks) {
  DebugConfig config;
  config.verbose = true;
  config.dump_masks = true;
  config.timing = true;
  config.output = output_file_;
  DebugTrace trace(config);

  debug_parser parser;
  const char* csv = "a,b,c\n1,2,3\n";
  auto buf = makeBuffer(csv);
  size_t len = strlen(csv);

  libvroom::ParseIndex idx = parser.init(len, 1);
  bool result = parser.parse_debug(buf.data(), idx, len, trace);

  EXPECT_TRUE(result);

  std::string output = get_output();
  EXPECT_NE(output.find("BUFFER"), std::string::npos);
  EXPECT_NE(output.find("INDEXES"), std::string::npos);
}

// Test log with custom output file (stdout fallback)
TEST_F(DebugTest, DebugTraceLogToStdout) {
  DebugConfig config;
  config.verbose = true;
  config.output = nullptr; // Will use stdout
  DebugTrace trace(config);

  // Just verify it doesn't crash when using stdout
  trace.log("Test message to stdout");
}

// Test dump_mask with binary output verification
TEST_F(DebugTest, DebugTraceDumpMaskBinaryFormat) {
  DebugConfig config;
  config.dump_masks = true;
  config.output = output_file_;
  DebugTrace trace(config);

  trace.dump_mask("binary_test", 0x00000000000000FF, 0);

  std::string output = get_output();
  EXPECT_NE(output.find("bin:"), std::string::npos);
  // The mask 0xFF should have 8 ones at the end
  EXPECT_NE(output.find("11111111"), std::string::npos);
}

// Test dump_indexes with strided layout
TEST_F(DebugTest, DebugTraceDumpIndexesStrided) {
  DebugConfig config;
  config.dump_masks = true;
  config.max_indexes_dump = 10;
  config.output = output_file_;
  DebugTrace trace(config);

  // Strided layout: thread 0 at indexes 0, 2, 4; thread 1 at 1, 3, 5
  uint64_t indexes[] = {100, 200, 110, 210, 120, 220};
  trace.dump_indexes(indexes, 3, 1, 2, 6); // Thread 1, stride 2

  std::string output = get_output();
  EXPECT_NE(output.find("INDEXES thread 1"), std::string::npos);
}

// Test DebugConfig default values
TEST_F(DebugTest, DebugConfigDefaultValues) {
  DebugConfig config;
  EXPECT_EQ(config.dump_context_bytes, 64u);
  EXPECT_EQ(config.max_indexes_dump, 100u);
  EXPECT_EQ(config.output, nullptr);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
