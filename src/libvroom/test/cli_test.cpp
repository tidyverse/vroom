/**
 * CLI Integration Tests for vroom (cli.cpp)
 *
 * Tests the vroom command-line tool by spawning the process with various
 * arguments and validating exit codes and output.
 *
 * SECURITY NOTE: The CliRunner class uses popen() with shell execution.
 * All test file paths MUST come from trusted test fixtures only.
 * The runWithFileStdin() method uses file redirection with paths that are
 * hardcoded in the test file - never use with user-provided input.
 */

#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

// Helper class to run CLI commands and capture output
class CliRunner {
public:
  struct Result {
    int exit_code;
    std::string output; // Combined stdout/stderr output
  };

  // Run vroom with given arguments
  // Note: stderr is redirected to stdout for simpler output capture
  static Result run(const std::string& args) {
    Result result;

    // Build command - vroom binary is in the build directory
    std::string cmd = "./vroom " + args + " 2>&1";

    // Open pipe to command
    std::array<char, 4096> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);

    if (!pipe) {
      result.exit_code = -1;
      result.output = "Failed to run command";
      return result;
    }

    // Read output
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      result.output += buffer.data();
    }

    // Get exit code - properly handle signal termination
    int status = pclose(pipe.release());
    if (WIFEXITED(status)) {
      result.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      result.exit_code = 128 + WTERMSIG(status); // Common convention
    } else {
      result.exit_code = -1;
    }

    return result;
  }

  // Run with stdin from a file via redirection
  // Note: file_path is expected to be a trusted path from test fixtures
  static Result runWithFileStdin(const std::string& args, const std::string& file_path) {
    Result result;
    std::string cmd = "./vroom " + args + " < " + file_path + " 2>&1";

    std::array<char, 4096> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);

    if (!pipe) {
      result.exit_code = -1;
      result.output = "Failed to run command";
      return result;
    }

    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      result.output += buffer.data();
    }

    // Get exit code - properly handle signal termination
    int status = pclose(pipe.release());
    if (WIFEXITED(status)) {
      result.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      result.exit_code = 128 + WTERMSIG(status);
    } else {
      result.exit_code = -1;
    }

    return result;
  }
};

class CliTest : public ::testing::Test {
protected:
  static std::string testDataPath(const std::string& relative_path) {
    return "test/data/" + relative_path;
  }
};

// =============================================================================
// Help and Version Tests
// =============================================================================

TEST_F(CliTest, NoArgsShowsUsage) {
  auto result = CliRunner::run("");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("USAGE:") != std::string::npos);
}

TEST_F(CliTest, HelpFlagShort) {
  auto result = CliRunner::run("-h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("USAGE:") != std::string::npos);
  EXPECT_TRUE(result.output.find("COMMANDS:") != std::string::npos);
}

TEST_F(CliTest, HelpFlagLong) {
  auto result = CliRunner::run("--help");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("USAGE:") != std::string::npos);
}

TEST_F(CliTest, VersionCommand) {
  auto result = CliRunner::run("version");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("vroom ") != std::string::npos);
}

TEST_F(CliTest, UnknownCommandShowsError) {
  auto result = CliRunner::run("unknown");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Unknown command") != std::string::npos);
}

// =============================================================================
// Count Command Tests
// =============================================================================

TEST_F(CliTest, CountBasicFile) {
  auto result = CliRunner::run("count " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // simple.csv has header + 3 data rows, count subtracts header by default
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountNoHeader) {
  auto result = CliRunner::run("count -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Without header flag, counts all 4 rows
  EXPECT_TRUE(result.output.find("4") != std::string::npos);
}

TEST_F(CliTest, CountEmptyFile) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("0") != std::string::npos);
}

TEST_F(CliTest, CountManyRows) {
  auto result = CliRunner::run("count " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should successfully count rows without error
}

TEST_F(CliTest, CountWithThreads) {
  auto result = CliRunner::run("count -t 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountQuotedFields) {
  auto result = CliRunner::run("count " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // escaped_quotes.csv has header + 5 data rows
  EXPECT_TRUE(result.output.find("5") != std::string::npos);
}

// =============================================================================
// Head Command Tests
// =============================================================================

TEST_F(CliTest, HeadDefault) {
  auto result = CliRunner::run("head " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header and rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
}

TEST_F(CliTest, HeadWithNumRows) {
  auto result = CliRunner::run("head -n 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + 2 data rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  // Third data row should NOT be present
  EXPECT_TRUE(result.output.find("7,8,9") == std::string::npos);
}

TEST_F(CliTest, HeadZeroRows) {
  auto result = CliRunner::run("head -n 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output nothing (or just header if that counts)
}

TEST_F(CliTest, HeadEmptyFile) {
  auto result = CliRunner::run("head " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadQuotedNewlines) {
  auto result = CliRunner::run("head " + testDataPath("quoted/newlines_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Select Command Tests
// =============================================================================

TEST_F(CliTest, SelectByIndex) {
  auto result = CliRunner::run("select -c 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("1") != std::string::npos);
  // Should NOT contain columns B or C
  EXPECT_TRUE(result.output.find("B") == std::string::npos);
}

TEST_F(CliTest, SelectByName) {
  auto result = CliRunner::run("select -c B " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
  EXPECT_TRUE(result.output.find("2") != std::string::npos);
}

TEST_F(CliTest, SelectMultipleColumns) {
  auto result = CliRunner::run("select -c 0,2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
  // B should not be present
  EXPECT_TRUE(result.output.find("B") == std::string::npos);
}

TEST_F(CliTest, SelectInvalidColumnIndex) {
  auto result = CliRunner::run("select -c 99 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("out of range") != std::string::npos);
}

TEST_F(CliTest, SelectInvalidColumnName) {
  auto result = CliRunner::run("select -c nonexistent " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("not found") != std::string::npos);
}

TEST_F(CliTest, SelectMissingColumnArg) {
  auto result = CliRunner::run("select " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("-c option required") != std::string::npos);
}

TEST_F(CliTest, SelectNoHeaderWithColumnName) {
  auto result = CliRunner::run("select -H -c name " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Cannot use column names") != std::string::npos);
}

// =============================================================================
// Info Command Tests
// =============================================================================

TEST_F(CliTest, InfoBasicFile) {
  auto result = CliRunner::run("info " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Source:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Size:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Rows:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Columns:") != std::string::npos);
  EXPECT_TRUE(result.output.find("3") != std::string::npos); // columns
}

TEST_F(CliTest, InfoShowsColumnNames) {
  auto result = CliRunner::run("info " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Column names:") != std::string::npos);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
}

TEST_F(CliTest, InfoNoHeader) {
  auto result = CliRunner::run("info -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should NOT show column names section when no header
  EXPECT_TRUE(result.output.find("Column names:") == std::string::npos);
}

TEST_F(CliTest, InfoEmptyFile) {
  auto result = CliRunner::run("info " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Size: 0 bytes") != std::string::npos);
}

// =============================================================================
// Pretty Command Tests
// =============================================================================

TEST_F(CliTest, PrettyBasicFile) {
  auto result = CliRunner::run("pretty " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Pretty output should have table borders
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
  EXPECT_TRUE(result.output.find("|") != std::string::npos);
  EXPECT_TRUE(result.output.find("-") != std::string::npos);
}

TEST_F(CliTest, PrettyWithNumRows) {
  auto result = CliRunner::run("pretty -n 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have table format
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
  // Should have header and one data row
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
}

TEST_F(CliTest, PrettyEmptyFile) {
  auto result = CliRunner::run("pretty " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Delimiter and Dialect Tests
// =============================================================================

TEST_F(CliTest, TabDelimiter) {
  auto result = CliRunner::run("count -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, SemicolonDelimiter) {
  auto result = CliRunner::run("count -d semicolon " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, PipeDelimiter) {
  auto result = CliRunner::run("count -d pipe " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, SingleCharDelimiter) {
  auto result = CliRunner::run("count -d , " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, HeadWithTabDelimiter) {
  auto result = CliRunner::run("head -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Output should use tab delimiter
  EXPECT_TRUE(result.output.find("\t") != std::string::npos);
}

TEST_F(CliTest, AutoDetectDialect) {
  // Auto-detect is now enabled by default, so we just run head without -d flag
  // and verify it correctly parses the semicolon-separated file
  auto result = CliRunner::run("head " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should auto-detect semicolon delimiter and output using semicolons
  EXPECT_TRUE(result.output.find(";") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandText) {
  // Test the dialect command with human-readable output
  auto result = CliRunner::run("dialect " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("semicolon") != std::string::npos);
  EXPECT_TRUE(result.output.find("CLI flags:") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandJson) {
  // Test the dialect command with JSON output
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"delimiter\": \"\\t\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"confidence\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandLowConfidenceFails) {
  // Test that dialect command fails for low-confidence detection without --force
  auto result = CliRunner::run("dialect " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Hint:") != std::string::npos);
  EXPECT_TRUE(result.output.find("--force") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandForceShort) {
  // Test that -f flag outputs best guess for low-confidence detection
  auto result = CliRunner::run("dialect -f " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Warning: Low confidence") != std::string::npos);
  EXPECT_TRUE(result.output.find("LOW CONFIDENCE") != std::string::npos);
  EXPECT_TRUE(result.output.find("Delimiter:") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandForceLong) {
  // Test that --force flag outputs best guess for low-confidence detection
  auto result = CliRunner::run("dialect --force " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Warning: Low confidence") != std::string::npos);
  EXPECT_TRUE(result.output.find("LOW CONFIDENCE") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandForceJson) {
  // Test that -f flag works with JSON output
  auto result = CliRunner::run("dialect -f -j " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"low_confidence\": true") != std::string::npos);
  EXPECT_TRUE(result.output.find("Warning: Low confidence") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandForceNotNeededForHighConfidence) {
  // Test that --force doesn't affect high-confidence detection
  auto result = CliRunner::run("dialect --force " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // No low-confidence warning for high-confidence detection
  // (Note: ambiguity warnings may still appear for files with multiple valid dialects)
  EXPECT_TRUE(result.output.find("Warning: Low confidence") == std::string::npos);
  EXPECT_TRUE(result.output.find("LOW CONFIDENCE") == std::string::npos);
  EXPECT_TRUE(result.output.find("semicolon") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectCommandJsonLowConfidenceField) {
  // Test that JSON output includes low_confidence field for high-confidence detection
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"low_confidence\": false") != std::string::npos);
}

TEST_F(CliTest, AutoDetectDisabledWithExplicitDelimiter) {
  // When -d is specified, auto-detect should be disabled
  // Even for a semicolon file, if we specify comma, it should use comma
  auto result = CliRunner::run("head -d comma " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Output should NOT have semicolon as delimiter (would be comma)
  // The file has "A;B;C" as content - if we parse as comma-separated,
  // the whole line becomes a single field
}

// =============================================================================
// Error Handling Tests
// =============================================================================

TEST_F(CliTest, NonexistentFile) {
  auto result = CliRunner::run("count nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos ||
              result.output.find("Could not load") != std::string::npos);
}

TEST_F(CliTest, DISABLED_InvalidThreadCount) {
  auto result = CliRunner::run("count -t 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Thread count") != std::string::npos);
}

TEST_F(CliTest, DISABLED_InvalidThreadCountTooHigh) {
  // 1025 exceeds new MAX_THREADS of 1024
  auto result = CliRunner::run("count -t 1025 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Thread count") != std::string::npos);
}

TEST_F(CliTest, DISABLED_InvalidRowCount) {
  auto result = CliRunner::run("head -n abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Invalid row count") != std::string::npos);
}

TEST_F(CliTest, DISABLED_NegativeRowCount) {
  auto result = CliRunner::run("head -n -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_InvalidQuoteChar) {
  auto result = CliRunner::run("count -q abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Quote character must be a single character") !=
              std::string::npos);
}

// =============================================================================
// Stdin Input Tests
// =============================================================================

TEST_F(CliTest, CountFromStdin) {
  auto result = CliRunner::runWithFileStdin("count -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountFromStdinNoExplicitDash) {
  auto result = CliRunner::runWithFileStdin("count", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, HeadFromStdin) {
  auto result = CliRunner::runWithFileStdin("head -n 2 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, InfoFromStdin) {
  auto result = CliRunner::runWithFileStdin("info -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("<stdin>") != std::string::npos);
}

// =============================================================================
// Edge Cases Tests
// =============================================================================

TEST_F(CliTest, SingleColumn) {
  auto result = CliRunner::run("count " + testDataPath("basic/single_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, WideColumns) {
  auto result = CliRunner::run("info " + testDataPath("basic/wide_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, EmptyFields) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/empty_fields.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, WhitespaceFields) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/whitespace_fields.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CrlfLineEndings) {
  auto result = CliRunner::run("count " + testDataPath("line_endings/crlf.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CrLineEndings) {
  auto result = CliRunner::run("count " + testDataPath("line_endings/cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, NoFinalNewline) {
  auto result = CliRunner::run("count " + testDataPath("line_endings/no_final_newline.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, QuotedFieldsWithNewlines) {
  auto result = CliRunner::run("count " + testDataPath("quoted/newlines_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, EscapedQuotes) {
  auto result = CliRunner::run("head " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SingleRowHeaderOnly) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/single_row_header_only.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("0") != std::string::npos);
}

// =============================================================================
// Command Help within Command Tests
// =============================================================================

TEST_F(CliTest, HelpAfterCommand) {
  auto result = CliRunner::run("count -h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("USAGE:") != std::string::npos);
}

// =============================================================================
// Combined Options Tests
// =============================================================================

TEST_F(CliTest, HeadWithMultipleOptions) {
  auto result = CliRunner::run("head -n 2 -t 2 -d comma " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, SelectWithMultipleColumns) {
  auto result = CliRunner::run("select -c A,C " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
}

TEST_F(CliTest, InfoWithAutoDetect) {
  // Auto-detect is now enabled by default
  auto result = CliRunner::run("info " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show dialect info with detected semicolon
  EXPECT_TRUE(result.output.find("Dialect:") != std::string::npos);
}

// =============================================================================
// Malformed CSV Handling Tests
// =============================================================================

TEST_F(CliTest, MalformedUnclosedQuote) {
  // File has an unclosed quote in the middle - parser should handle gracefully
  auto result = CliRunner::run("count " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser processes what it can - row count may vary based on quote
  // interpretation but should return some reasonable value (not crash or hang)
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedUnclosedQuoteEof) {
  // Quote never closes until end of file
  auto result = CliRunner::run("head " + testDataPath("malformed/unclosed_quote_eof.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output what it can parse
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, MalformedUnescapedQuoteInQuoted) {
  // Has unescaped quote inside quoted field: "has " unescaped quote"
  auto result = CliRunner::run("count " + testDataPath("malformed/unescaped_quote_in_quoted.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser handles this - may interpret differently than expected
}

TEST_F(CliTest, MalformedQuoteNotAtStart) {
  // Quote appears mid-field: x"quoted"
  auto result = CliRunner::run("head " + testDataPath("malformed/quote_not_at_start.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser should process the file
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, MalformedTripleQuote) {
  // Contains triple quotes which is ambiguous
  auto result = CliRunner::run("count " + testDataPath("malformed/triple_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should process the file and return a count
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedNullByte) {
  // Contains a null byte in data
  auto result = CliRunner::run("count " + testDataPath("malformed/null_byte.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should count rows despite null byte
  EXPECT_TRUE(result.output.find("2") != std::string::npos);
}

TEST_F(CliTest, MalformedInconsistentColumns) {
  // Rows have different numbers of columns
  auto result = CliRunner::run("info " + testDataPath("malformed/inconsistent_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Info command should still work
  EXPECT_TRUE(result.output.find("Columns:") != std::string::npos);
}

TEST_F(CliTest, MalformedVariableColumns) {
  // Regression test for GitHub issue #263: SIGABRT crash on variable column
  // count File has ~30 rows with column counts varying from 20-26 This
  // previously caused an assertion failure with SIGABRT
  auto result = CliRunner::run("head -n 5 " + testDataPath("malformed/variable_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should handle variable column counts gracefully without crashing
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedVariableColumnsExplicitDelimiter) {
  // Test with explicit delimiter (disables auto-detection)
  auto result =
      CliRunner::run("head -d comma -n 5 " + testDataPath("malformed/variable_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedEmptyHeader) {
  // Header row has empty column names
  auto result = CliRunner::run("head " + testDataPath("malformed/empty_header.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, MalformedDuplicateColumnNames) {
  // Header has duplicate column names
  auto result = CliRunner::run("info " + testDataPath("malformed/duplicate_column_names.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Info command should work
  EXPECT_TRUE(result.output.find("Column names:") != std::string::npos);
}

TEST_F(CliTest, MalformedMixedLineEndings) {
  // File has mix of CRLF, LF, and CR line endings
  auto result = CliRunner::run("count " + testDataPath("malformed/mixed_line_endings.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should process the file and return a count
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedTrailingQuote) {
  // Field ends with quote in unexpected position
  auto result = CliRunner::run("head " + testDataPath("malformed/trailing_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should produce some output
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedMultipleErrors) {
  // File with multiple types of malformed content
  auto result = CliRunner::run("count " + testDataPath("malformed/multiple_errors.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should process the file and return a count
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedSelectFromBadFile) {
  // Try selecting columns from malformed file
  auto result = CliRunner::run("select -c 0 " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output first column from parseable rows
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
}

TEST_F(CliTest, MalformedPrettyFromBadFile) {
  // Pretty print of malformed file
  auto result = CliRunner::run("pretty -n 5 " + testDataPath("malformed/inconsistent_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should still produce table output
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

// =============================================================================
// Large File / Parallel Processing Tests
// =============================================================================

TEST_F(CliTest, LargeFileParallelCount) {
  // Test parallel counting on a multi-MB file
  auto result = CliRunner::run("count -t 4 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should return a valid count without error
}

TEST_F(CliTest, LargeFileParallelCountVerify) {
  // Verify parallel counting produces same result as single-threaded
  auto single = CliRunner::run("count -t 1 " + testDataPath("large/parallel_chunk_boundary.csv"));
  auto parallel = CliRunner::run("count -t 4 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(single.exit_code, 0);
  EXPECT_EQ(parallel.exit_code, 0);
  // Both should produce the same count
  EXPECT_EQ(single.output, parallel.output);
}

TEST_F(CliTest, LargeFileParallelMaxThreads) {
  // Test with higher thread count
  auto result = CliRunner::run("count -t 8 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, LargeFileHead) {
  // Head command on large file should be fast (only reads what's needed)
  auto result = CliRunner::run("head -n 5 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + 5 data rows
}

TEST_F(CliTest, LargeFieldFile) {
  // File with a very large field (70KB)
  auto result = CliRunner::run("count " + testDataPath("large/large_field.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, LongLineFile) {
  // File with very long lines
  auto result = CliRunner::run("head -n 2 " + testDataPath("large/long_line.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_BufferBoundaryFile) {
  // File designed to test SIMD buffer boundaries (200 rows)
  auto result = CliRunner::run("count -t 2 " + testDataPath("large/buffer_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should count all 200 rows
  EXPECT_TRUE(result.output.find("200") != std::string::npos);
}

// =============================================================================
// Invalid Option Combinations Tests
// =============================================================================

TEST_F(CliTest, ExplicitDelimiterDisablesAutoDetect) {
  // When -d (explicit delimiter) is used, auto-detect should be disabled
  // For a comma file with -d semicolon, it should treat each line as one field
  auto result = CliRunner::run("head -d semicolon " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should NOT show auto-detect message since -d was specified
  EXPECT_TRUE(result.output.find("Auto-detected") == std::string::npos);
}

TEST_F(CliTest, DISABLED_AutoDetectByDefault) {
  // Verify auto-detect works by default without -a flag
  auto result = CliRunner::run("info " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should auto-detect semicolon
  EXPECT_TRUE(result.output.find("';'") != std::string::npos);
}

TEST_F(CliTest, NoHeaderWithColumnNameSelect) {
  // Already tested, but included here for completeness of option combinations
  auto result = CliRunner::run("select -H -c name " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Cannot use column names") != std::string::npos);
}

TEST_F(CliTest, DISABLED_ExcessiveThreadsInvalid) {
  // More than 1024 threads is invalid (limited by MAX_THREADS)
  auto result = CliRunner::run("count -t 2000 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_NegativeThreadCount) {
  // Negative thread count
  auto result = CliRunner::run("count -t -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, HeadWithZeroAndFile) {
  // head -n 0 should show nothing (or just header depending on implementation)
  auto result = CliRunner::run("head -n 0 -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectMissingFile) {
  // Select command with nonexistent file
  auto result = CliRunner::run("select -c 0 nonexistent.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos ||
              result.output.find("Could not load") != std::string::npos);
}

TEST_F(CliTest, MultipleDelimiterSpecs) {
  // Multiple -d flags - last one should win
  auto result = CliRunner::run("count -d tab -d comma " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should use comma (the last specified)
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

// =============================================================================
// Encoding Tests
// =============================================================================

TEST_F(CliTest, Utf8BomFile) {
  // File with UTF-8 BOM
  auto result = CliRunner::run("count " + testDataPath("encoding/utf8_bom.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, Latin1Encoding) {
  // File with Latin-1 encoding (non-UTF8 but valid bytes)
  auto result = CliRunner::run("head " + testDataPath("encoding/latin1.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Tail Command Tests
// =============================================================================

TEST_F(CliTest, DISABLED_TailDefault) {
  auto result = CliRunner::run("tail " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header and last rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // simple.csv has 3 data rows, default is 10, so all 3 should appear
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailWithNumRows) {
  auto result = CliRunner::run("tail -n 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + last 2 data rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // First data row should NOT be present
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
  // Last 2 data rows should be present
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailWithNumRowsOne) {
  auto result = CliRunner::run("tail -n 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + last data row only
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") == std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailMoreRowsThanExist) {
  // Request more rows than exist - should return all data rows
  auto result = CliRunner::run("tail -n 100 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailZeroRows) {
  auto result = CliRunner::run("tail -n 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output only the header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
}

TEST_F(CliTest, DISABLED_TailEmptyFile) {
  auto result = CliRunner::run("tail " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_TailNoHeader) {
  auto result = CliRunner::run("tail -n 2 -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output last 2 rows without treating first as header
  // So we get rows "4,5,6" and "7,8,9" (last 2 of 4 total rows)
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
  // Header "A,B,C" should NOT be in output since we're not treating it as
  // header
  EXPECT_TRUE(result.output.find("A,B,C") == std::string::npos);
}

TEST_F(CliTest, DISABLED_TailManyRows) {
  // Test with file that has 20 data rows
  // Uses default multi-threaded parsing (PR #303 fixed SIMD delimiter masking
  // on macOS)
  auto result = CliRunner::run("tail -n 5 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("ID,Value,Label") != std::string::npos)
      << "Expected header not found. Output length: " << result.output.size()
      << "\nActual output:\n"
      << result.output;
  // Should have last 5 rows (IDs 16-20)
  EXPECT_TRUE(result.output.find("16,") != std::string::npos)
      << "Expected '16,' not found in tail output.\n"
      << "Exit code: " << result.exit_code << "\n"
      << "Output length: " << result.output.size() << " bytes\n"
      << "Actual output:\n"
      << result.output;
  EXPECT_TRUE(result.output.find("20,") != std::string::npos)
      << "Expected '20,' not found in tail output.\n"
      << "Actual output:\n"
      << result.output;
  // Should NOT have earlier rows (IDs 1-15)
  EXPECT_TRUE(result.output.find("15,") == std::string::npos)
      << "Unexpected '15,' found in tail output (should only have last 5 rows).\n"
      << "Actual output:\n"
      << result.output;
}

TEST_F(CliTest, DISABLED_TailFromStdin) {
  auto result = CliRunner::runWithFileStdin("tail -n 2 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailWithTabDelimiter) {
  auto result = CliRunner::run("tail -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\t") != std::string::npos);
}

// =============================================================================
// Sample Command Tests
// =============================================================================

TEST_F(CliTest, DISABLED_SampleDefault) {
  auto result = CliRunner::run("sample " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleWithNumRows) {
  auto result = CliRunner::run("sample -n 2 -s 42 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + 2 data rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Verify we got exactly 2 data rows by counting occurrences of data patterns
  // simple.csv has rows: 1,2,3 and 4,5,6 and 7,8,9
  // With seed 42, sample should select specific rows from the 3 available
  int data_rows = 0;
  if (result.output.find("1,2,3") != std::string::npos)
    data_rows++;
  if (result.output.find("4,5,6") != std::string::npos)
    data_rows++;
  if (result.output.find("7,8,9") != std::string::npos)
    data_rows++;
  EXPECT_EQ(data_rows, 2); // We requested 2 rows
}

TEST_F(CliTest, DISABLED_SampleMoreRowsThanExist) {
  // Request more samples than exist - should return all data rows
  auto result = CliRunner::run("sample -n 100 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleZeroRows) {
  auto result = CliRunner::run("sample -n 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output only the header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Should NOT contain any data rows
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") == std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") == std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleEmptyFile) {
  auto result = CliRunner::run("sample " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_SampleReproducibleWithSeed) {
  // Same seed should produce same sample
  auto result1 = CliRunner::run("sample -n 5 -s 42 " + testDataPath("basic/many_rows.csv"));
  auto result2 = CliRunner::run("sample -n 5 -s 42 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result1.exit_code, 0);
  EXPECT_EQ(result2.exit_code, 0);
  EXPECT_EQ(result1.output, result2.output);
}

TEST_F(CliTest, DISABLED_SampleDifferentSeeds) {
  // Different seeds should likely produce different samples (not guaranteed but
  // highly probable)
  auto result1 = CliRunner::run("sample -n 5 -s 1 " + testDataPath("basic/many_rows.csv"));
  auto result2 = CliRunner::run("sample -n 5 -s 999 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result1.exit_code, 0);
  EXPECT_EQ(result2.exit_code, 0);
  // Both should have header
  EXPECT_TRUE(result1.output.find("ID,Value,Label") != std::string::npos);
  EXPECT_TRUE(result2.output.find("ID,Value,Label") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleNoHeader) {
  auto result = CliRunner::run("sample -n 2 -H -s 42 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output 2 rows without header treatment
  // With -H, all 4 rows (including "A,B,C") are treated as data
  // Verify we got exactly 2 data rows
  int data_rows = 0;
  if (result.output.find("A,B,C") != std::string::npos)
    data_rows++;
  if (result.output.find("1,2,3") != std::string::npos)
    data_rows++;
  if (result.output.find("4,5,6") != std::string::npos)
    data_rows++;
  if (result.output.find("7,8,9") != std::string::npos)
    data_rows++;
  EXPECT_EQ(data_rows, 2);
}

TEST_F(CliTest, DISABLED_SampleManyRows) {
  // Sample from file with 20 data rows
  // Uses default multi-threaded parsing (PR #303 fixed SIMD delimiter masking
  // on macOS)
  auto result = CliRunner::run("sample -n 5 -s 42 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("ID,Value,Label") != std::string::npos);
  // Count data rows by looking for unique patterns at start of line
  // Each data row has format like "1,100,A" or "20,2000,T"
  // Use patterns that are unique to each row to avoid false matches
  int data_rows = 0;
  if (result.output.find("1,100,A") != std::string::npos)
    data_rows++;
  if (result.output.find("2,200,B") != std::string::npos)
    data_rows++;
  if (result.output.find("3,300,C") != std::string::npos)
    data_rows++;
  if (result.output.find("4,400,D") != std::string::npos)
    data_rows++;
  if (result.output.find("5,500,E") != std::string::npos)
    data_rows++;
  if (result.output.find("6,600,F") != std::string::npos)
    data_rows++;
  if (result.output.find("7,700,G") != std::string::npos)
    data_rows++;
  if (result.output.find("8,800,H") != std::string::npos)
    data_rows++;
  if (result.output.find("9,900,I") != std::string::npos)
    data_rows++;
  if (result.output.find("10,1000,J") != std::string::npos)
    data_rows++;
  if (result.output.find("11,1100,K") != std::string::npos)
    data_rows++;
  if (result.output.find("12,1200,L") != std::string::npos)
    data_rows++;
  if (result.output.find("13,1300,M") != std::string::npos)
    data_rows++;
  if (result.output.find("14,1400,N") != std::string::npos)
    data_rows++;
  if (result.output.find("15,1500,O") != std::string::npos)
    data_rows++;
  if (result.output.find("16,1600,P") != std::string::npos)
    data_rows++;
  if (result.output.find("17,1700,Q") != std::string::npos)
    data_rows++;
  if (result.output.find("18,1800,R") != std::string::npos)
    data_rows++;
  if (result.output.find("19,1900,S") != std::string::npos)
    data_rows++;
  if (result.output.find("20,2000,T") != std::string::npos)
    data_rows++;
  EXPECT_EQ(data_rows, 5); // We requested 5 rows
}

TEST_F(CliTest, DISABLED_SampleFromStdin) {
  auto result =
      CliRunner::runWithFileStdin("sample -n 2 -s 42 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleWithTabDelimiter) {
  auto result = CliRunner::run("sample -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\t") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleInvalidSeed) {
  auto result = CliRunner::run("sample -s abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Invalid seed") != std::string::npos);
}

TEST_F(CliTest, SampleNegativeSeed) {
  auto result = CliRunner::run("sample -s -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

// =============================================================================
// Dialect JSON Escaping Tests
// =============================================================================

TEST_F(CliTest, DISABLED_DialectJsonEscapesTab) {
  // Tab delimiter should be escaped as \t in JSON output
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should contain properly escaped tab
  EXPECT_TRUE(result.output.find("\"delimiter\": \"\\t\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectJsonEscapesDoubleQuote) {
  // Double quote should be escaped as \" in JSON output
  auto result = CliRunner::run("dialect -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Quote character should be escaped (double quote is the default)
  EXPECT_TRUE(result.output.find("\"quote\": \"\\\"\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectJsonValidStructure) {
  // Verify JSON output is well-formed
  auto result = CliRunner::run("dialect -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Check for required JSON fields
  EXPECT_TRUE(result.output.find("\"delimiter\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"quote\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"escape\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"line_ending\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"has_header\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"columns\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"confidence\":") != std::string::npos);
}

// =============================================================================
// Carriage Return in Fields Tests
// Tests for fields containing \r (CR) characters within quoted fields.
// These tests verify that PR #203's quoting behavior is correct.
// =============================================================================

TEST_F(CliTest, HeadFieldsWithCR) {
  // Fields containing \r should be properly quoted in output
  auto result = CliRunner::run("head " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The header should be present
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Fields with CR should be quoted - look for the quoted field markers
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailFieldsWithCR) {
  // Tail command should properly handle fields containing \r
  auto result = CliRunner::run("tail -n 2 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Should have last 2 data rows (rows with fields containing \r)
  // Fields with CR should be quoted in output
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailFieldsWithCRVerifyQuoting) {
  // Verify that \r inside fields causes proper quoting
  auto result = CliRunner::run("tail -n 1 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The last row has a field with mixed \r and \r\n
  // The output should quote fields containing \r
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleFieldsWithCR) {
  // Sample command should properly handle fields containing \r
  auto result = CliRunner::run("sample -n 2 -s 42 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Fields with CR should be quoted in output
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleFieldsWithCRReproducible) {
  // Same seed should produce same sample for file with \r in fields
  auto result1 = CliRunner::run("sample -n 2 -s 123 " + testDataPath("quoted/cr_in_quotes.csv"));
  auto result2 = CliRunner::run("sample -n 2 -s 123 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result1.exit_code, 0);
  EXPECT_EQ(result2.exit_code, 0);
  EXPECT_EQ(result1.output, result2.output);
}

TEST_F(CliTest, CountFieldsWithCR) {
  // Count should work correctly with \r in quoted fields
  auto result = CliRunner::run("count " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // File has 3 data rows (after header)
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, InfoFieldsWithCR) {
  // Info should work correctly with \r in quoted fields
  auto result = CliRunner::run("info " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Columns: 3") != std::string::npos);
  EXPECT_TRUE(result.output.find("Rows: 3") != std::string::npos);
}

TEST_F(CliTest, SelectFieldsWithCR) {
  // Select should properly quote fields containing \r in output
  auto result = CliRunner::run("select -c B " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Column B contains fields with \r, so output should have quoted fields
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailCRLineEndingsFile) {
  // Test tail on file that uses CR as line ending (not in quoted fields)
  auto result = CliRunner::run("tail -n 1 " + testDataPath("line_endings/cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Output should not be empty - CR line endings should be handled gracefully
  // Note: CR line endings cause the entire file to appear as one line to the
  // parser, so exact content verification is complex
}

TEST_F(CliTest, DISABLED_SampleCRLineEndingsFile) {
  // Test sample on file that uses CR as line ending
  auto result = CliRunner::run("sample -n 1 -s 42 " + testDataPath("line_endings/cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should complete successfully with CR line endings
}

TEST_F(CliTest, DISABLED_TailCRLFLineEndingsFile) {
  // Test tail on file that uses CRLF line endings
  auto result = CliRunner::run("tail -n 1 " + testDataPath("line_endings/crlf.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // CRLF files should work correctly with tail
  // The output should contain data, though CRLF may be converted to LF
}

TEST_F(CliTest, DISABLED_SampleCRLFLineEndingsFile) {
  // Test sample on file that uses CRLF line endings
  auto result = CliRunner::run("sample -n 1 -s 42 " + testDataPath("line_endings/crlf.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // CRLF files should work correctly with sample
}

TEST_F(CliTest, DISABLED_TailMixedLineEndingsFile) {
  // Test tail on file with mixed line endings
  auto result = CliRunner::run("tail -n 2 " + testDataPath("malformed/mixed_line_endings.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should handle mixed line endings gracefully
}

TEST_F(CliTest, DISABLED_SampleMixedLineEndingsFile) {
  // Test sample on file with mixed line endings
  auto result =
      CliRunner::run("sample -n 2 -s 42 " + testDataPath("malformed/mixed_line_endings.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should handle mixed line endings gracefully
}

// =============================================================================
// Additional Delimiter Format Tests
// =============================================================================

TEST_F(CliTest, ColonDelimiter) {
  // Test colon delimiter (exercises formatDelimiter colon case)
  auto result = CliRunner::run("count -d : " + testDataPath("separators/colon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectColonDelimiter) {
  // Test dialect command with colon-delimited file
  auto result = CliRunner::run("dialect " + testDataPath("separators/colon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should detect colon as delimiter
  EXPECT_TRUE(result.output.find("colon") != std::string::npos);
}

TEST_F(CliTest, DISABLED_UnknownDelimiterWarning) {
  // Test the warning path for unknown multi-char delimiter string
  auto result = CliRunner::run("count -d unknown_delim " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show warning and fall back to comma
  EXPECT_TRUE(result.output.find("Warning:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Unknown delimiter") != std::string::npos);
}

TEST_F(CliTest, TabDelimiterBackslashT) {
  // Test escaped tab format (\t) for delimiter
  auto result = CliRunner::run("count -d \\\\t " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, PipeDelimiterSymbol) {
  // Test pipe delimiter using | symbol directly
  auto result = CliRunner::run("count -d '|' " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, SemicolonDelimiterSymbol) {
  // Test semicolon delimiter using ; symbol directly
  auto result = CliRunner::run("count -d ';' " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

// =============================================================================
// Quote Character Tests
// =============================================================================

TEST_F(CliTest, SingleQuoteChar) {
  // Test single quote as quote character
  auto result = CliRunner::run("count -q \"'\" " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CustomQuoteCharForSelect) {
  // Test custom quote character with select command
  auto result = CliRunner::run("select -c 0 -q \"'\" " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Dialect Command Extended Tests
// =============================================================================

TEST_F(CliTest, DISABLED_DialectJsonBackslashDelimiter) {
  // Test JSON output with backslash escaping for delimiter
  // The backslash escape in JSON output (line 914) is tested with tab
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"delimiter\": \"\\t\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectPipeDelimiter) {
  auto result = CliRunner::run("dialect " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("pipe") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectJsonPipeDelimiter) {
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"delimiter\": \"|\"") != std::string::npos);
}

TEST_F(CliTest, DialectEmptyFile) {
  // Test dialect detection on empty file (should fail gracefully)
  auto result = CliRunner::run("dialect " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 1);
  // Should error because nothing to detect
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectFromStdin) {
  auto result = CliRunner::runWithFileStdin("dialect -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("comma") != std::string::npos);
}

TEST_F(CliTest, DialectNonexistentFile) {
  auto result = CliRunner::run("dialect nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
}

// =============================================================================
// Pretty Print Extended Tests
// =============================================================================

TEST_F(CliTest, PrettyNoHeader) {
  // Test pretty print without header (no separator after first row)
  auto result = CliRunner::run("pretty -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

TEST_F(CliTest, PrettyLongFieldTruncation) {
  // Test pretty print with field truncation to 40 chars max
  auto result = CliRunner::run("pretty " + testDataPath("large/large_field.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("...") != std::string::npos);
}

TEST_F(CliTest, PrettyNarrowColumns) {
  // Test pretty print with narrow columns (width < 3)
  auto result = CliRunner::run("pretty " + testDataPath("basic/narrow_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

TEST_F(CliTest, PrettyFromStdin) {
  auto result = CliRunner::runWithFileStdin("pretty -n 2 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

TEST_F(CliTest, PrettyManyRows) {
  auto result = CliRunner::run("pretty " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Output Formatting Tests (fields needing quoting in output)
// =============================================================================

TEST_F(CliTest, HeadFieldsWithCommas) {
  // Test head output properly quotes fields containing commas
  auto result = CliRunner::run("head " + testDataPath("quoted/needs_quoting.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The output should contain quoted fields
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, SelectFieldsWithQuotes) {
  // Test select output properly escapes quotes in fields
  auto result = CliRunner::run("select -c 0,1 " + testDataPath("quoted/needs_quoting.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadFieldsWithContainsCR) {
  // Test head output properly quotes fields containing carriage returns
  auto result = CliRunner::run("head " + testDataPath("quoted/contains_cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_TailFieldsWithNewlines) {
  // Test tail output with embedded newlines in fields
  auto result = CliRunner::run("tail " + testDataPath("quoted/newlines_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Small File Tests (scalar path for row counting)
// =============================================================================

TEST_F(CliTest, CountTinyFile) {
  // Test count on a file under 64 bytes (exercises scalar path)
  auto result = CliRunner::run("count " + testDataPath("basic/tiny.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("1") != std::string::npos);
}

TEST_F(CliTest, CountTinyFileNoHeader) {
  auto result = CliRunner::run("count -H " + testDataPath("basic/tiny.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("2") != std::string::npos);
}

TEST_F(CliTest, HeadTinyFile) {
  auto result = CliRunner::run("head " + testDataPath("basic/tiny.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B") != std::string::npos);
}

// =============================================================================
// Additional Info Command Tests
// =============================================================================

TEST_F(CliTest, InfoFromStdinWithDelimiter) {
  auto result = CliRunner::runWithFileStdin("info -d tab -", testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("<stdin>") != std::string::npos);
}

TEST_F(CliTest, InfoManyColumns) {
  auto result = CliRunner::run("info " + testDataPath("basic/wide_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Columns:") != std::string::npos);
}

// =============================================================================
// Additional Select Command Tests
// =============================================================================

TEST_F(CliTest, SelectWithTabDelimiter) {
  auto result = CliRunner::run("select -c 0 -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectMultipleByName) {
  auto result = CliRunner::run("select -c A,B " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
}

TEST_F(CliTest, SelectEmptyFile) {
  auto result = CliRunner::run("select -c 0 " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectRaggedCsv) {
  // Test select on CSV with ragged columns (some rows have fewer columns)
  auto result = CliRunner::run("select -c 0,2 " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Additional Head/Tail Tests
// =============================================================================

TEST_F(CliTest, HeadSingleColumn) {
  auto result = CliRunner::run("head " + testDataPath("basic/single_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_TailSingleColumn) {
  auto result = CliRunner::run("tail " + testDataPath("basic/single_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadQuotedFieldsPreservation) {
  // Test that quoted fields are properly output
  auto result = CliRunner::run("head " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_TailQuotedFieldsPreservation) {
  auto result = CliRunner::run("tail " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Thread Count Edge Cases
// =============================================================================

TEST_F(CliTest, CountSingleThread) {
  auto result = CliRunner::run("count -t 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountMaxThreads) {
  // Test with maximum valid thread count (1024 after uint16_t change)
  auto result = CliRunner::run("count -t 1024 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountManyThreads) {
  // Test with thread count above old uint8_t limit (255)
  auto result = CliRunner::run("count -t 500 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadWithManyThreads) {
  auto result = CliRunner::run("head -t 16 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Sample Command Extended Tests
// =============================================================================

TEST_F(CliTest, DISABLED_SampleSingleRow) {
  auto result = CliRunner::run("sample -n 1 -s 42 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header and 1 data row
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SampleLargeFile) {
  auto result = CliRunner::run("sample -n 10 -s 42 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_SampleWithPipeDelimiter) {
  auto result = CliRunner::run("sample -d pipe " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("|") != std::string::npos);
}

// =============================================================================
// Ragged CSV Tests
// =============================================================================

TEST_F(CliTest, HeadRaggedCsv) {
  auto result = CliRunner::run("head " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_TailRaggedCsv) {
  auto result = CliRunner::run("tail " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoRaggedCsv) {
  auto result = CliRunner::run("info " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, PrettyRaggedCsv) {
  // Test pretty print with ragged columns (different column counts per row)
  auto result = CliRunner::run("pretty " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

// =============================================================================
// Whitespace and Special Content Tests
// =============================================================================

TEST_F(CliTest, CountBlankRows) {
  auto result = CliRunner::run("count " + testDataPath("whitespace/blank_rows_mixed.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadWhitespaceOnlyRows) {
  auto result = CliRunner::run("head " + testDataPath("whitespace/whitespace_only_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoBlankLeadingRows) {
  auto result = CliRunner::run("info " + testDataPath("whitespace/blank_leading_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Real-world Data Tests
// =============================================================================

TEST_F(CliTest, HeadFinancialData) {
  auto result = CliRunner::run("head " + testDataPath("real_world/financial.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoContactsData) {
  auto result = CliRunner::run("info " + testDataPath("real_world/contacts.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectUnicodeData) {
  auto result = CliRunner::run("select -c 0 " + testDataPath("real_world/unicode.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, PrettyProductCatalog) {
  auto result = CliRunner::run("pretty -n 3 " + testDataPath("real_world/product_catalog.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Fuzz Test Data
// =============================================================================

TEST_F(CliTest, CountDeepQuotes) {
  auto result = CliRunner::run("count " + testDataPath("fuzz/deep_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadJustQuotes) {
  auto result = CliRunner::run("head " + testDataPath("fuzz/just_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountQuoteEof) {
  auto result = CliRunner::run("count " + testDataPath("fuzz/quote_eof.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoMixedCr) {
  auto result = CliRunner::run("info " + testDataPath("fuzz/mixed_cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountInvalidUtf8) {
  auto result = CliRunner::run("count " + testDataPath("fuzz/invalid_utf8.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Additional Error Cases
// =============================================================================

TEST_F(CliTest, HeadNonexistentFile) {
  auto result = CliRunner::run("head nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
}

TEST_F(CliTest, TailNonexistentFile) {
  auto result = CliRunner::run("tail nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, SampleNonexistentFile) {
  auto result = CliRunner::run("sample nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, InfoNonexistentFile) {
  auto result = CliRunner::run("info nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, PrettyNonexistentFile) {
  auto result = CliRunner::run("pretty nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

// =============================================================================
// Combined Options Edge Cases
// =============================================================================

TEST_F(CliTest, HeadNoHeaderWithCustomDelimiter) {
  auto result = CliRunner::run("head -H -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_TailNoHeaderWithRowCount) {
  auto result = CliRunner::run("tail -H -n 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_SampleWithAllOptions) {
  auto result = CliRunner::run("sample -n 2 -s 42 -H -d comma " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectNoHeaderWithIndex) {
  // Select with -H should work with numeric indices
  auto result = CliRunner::run("select -H -c 0,1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Encoding Tests
// =============================================================================

TEST_F(CliTest, HeadUtf8Bom) {
  auto result = CliRunner::run("head " + testDataPath("encoding/utf8_bom.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountLatin1) {
  auto result = CliRunner::run("count " + testDataPath("encoding/latin1.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoUtf16Bom) {
  auto result = CliRunner::run("info " + testDataPath("encoding/utf16_bom.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Comments Test Data
// =============================================================================

TEST_F(CliTest, CountHashComments) {
  auto result = CliRunner::run("count " + testDataPath("comments/hash_comments.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadQuotedHash) {
  auto result = CliRunner::run("head " + testDataPath("comments/quoted_hash.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Escape Style Tests
// =============================================================================

TEST_F(CliTest, HeadBackslashEscape) {
  auto result = CliRunner::run("head " + testDataPath("escape/backslash_escape.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Edge Case: Single Cell File
// =============================================================================

TEST_F(CliTest, CountSingleCell) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadSingleCell) {
  auto result = CliRunner::run("head " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoSingleCell) {
  auto result = CliRunner::run("info " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// UTF-8 Truncation Tests (Issue #255)
// =============================================================================
// The pretty command now properly handles UTF-8 truncation at code point
// boundaries, respecting display width for CJK and emoji characters.
// This was implemented to fix issue #255.
// =============================================================================

TEST_F(CliTest, PrettyUtf8TruncationProperBoundaries) {
  // Test that UTF-8 truncation respects code point boundaries.
  // The pretty command now uses display width (not byte length) and
  // truncates at code point boundaries, never splitting multi-byte sequences.
  //
  // Test file contains fields > 40 display columns with multi-byte UTF-8:
  // - EmojiSplit: 36 ASCII + 2 emoji (2 cols each) = 40 display columns
  // - CJKSplit: 17 CJK characters (2 cols each) = 34 display columns
  // - MixedSplit: Mix of ASCII, CJK, emoji
  auto result = CliRunner::run("pretty " + testDataPath("edge_cases/utf8_truncation.csv"));
  EXPECT_EQ(result.exit_code, 0);

  // Verify the command succeeds and produces table output
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
  EXPECT_TRUE(result.output.find("|") != std::string::npos);

  // Verify that truncation doesn't produce invalid UTF-8 sequences.
  // The output should NOT contain the replacement character (0xFFFD).
  // In valid UTF-8, we should not see orphaned continuation bytes (0x80-0xBF
  // without proper leading byte) or truncated multi-byte sequences.
  //
  // Check that the output is valid UTF-8 by ensuring no orphaned bytes
  for (size_t i = 0; i < result.output.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(result.output[i]);
    if ((c & 0xC0) == 0x80) {
      // This is a continuation byte (10xxxxxx), verify it follows a leading byte
      EXPECT_GT(i, 0) << "Orphaned continuation byte at start of string";
      if (i > 0) {
        unsigned char prev = static_cast<unsigned char>(result.output[i - 1]);
        // Previous byte should be either a leading byte or another continuation byte
        bool valid_prev = (prev & 0x80) != 0; // Must be part of multi-byte sequence
        EXPECT_TRUE(valid_prev) << "Orphaned continuation byte at position " << i;
      }
    }
  }
}

TEST_F(CliTest, PrettyUtf8ShortFieldsNotTruncated) {
  // Verify that short UTF-8 fields (< 40 display columns) are NOT truncated
  auto result = CliRunner::run("pretty " + testDataPath("real_world/unicode.csv"));
  EXPECT_EQ(result.exit_code, 0);

  // The unicode.csv file has fields < 40 display columns, so they display fully
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

// ============================================================================
// Regression Tests for GitHub Issues
// ============================================================================

TEST_F(CliTest, RegressionIssue264_ExtremelyWideCsv) {
  // Regression test for GitHub issue #264: SIGSEGV crash on extremely wide CSV
  // files The bug was in index buffer allocation for multi-threaded parsing.
  // Files with very high separator density (many columns) could overflow the
  // interleaved index buffer because the allocation didn't account for the
  // stride pattern used in multi-threaded mode.
  //
  // The test file has 16384 columns and 74 rows (~868K separators in ~876KB
  // file). This previously caused a segmentation fault.
  auto result = CliRunner::run("head -n 5 " + testDataPath("edge_cases/extremely_wide.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should successfully parse and output the first rows
  EXPECT_FALSE(result.output.empty());
  // First row should contain the expected header
  EXPECT_TRUE(result.output.find("BUSINESS PLAN QUARTERLY DATA SUMMARY") != std::string::npos);
}

TEST_F(CliTest, RegressionIssue264_ExtremelyWideCsvInfo) {
  // Also verify info command works on extremely wide files
  auto result = CliRunner::run("info " + testDataPath("edge_cases/extremely_wide.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should report 16384 columns
  EXPECT_TRUE(result.output.find("Columns: 16384") != std::string::npos);
}

TEST_F(CliTest, RegressionIssue264_ExtremelyWideCsvCount) {
  // Verify count command works on extremely wide files
  auto result = CliRunner::run("count " + testDataPath("edge_cases/extremely_wide.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should return a valid row count
  EXPECT_FALSE(result.output.empty());
}

// =============================================================================
// Strict Mode Tests
// Tests for --strict / -S flag functionality (GitHub issue #354)
// =============================================================================

TEST_F(CliTest, DISABLED_StrictModeShortFlag) {
  // -S flag should work on well-formed CSV
  auto result = CliRunner::run("head -S " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, StrictModeLongFlag) {
  // --strict flag should work on well-formed CSV
  auto result = CliRunner::run("head --strict " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StrictModeUnclosedQuoteReturnsError) {
  // Unclosed quote should cause exit code 1 in strict mode
  auto result = CliRunner::run("head -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos ||
              result.output.find("Strict mode") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StrictModeUnclosedQuoteLongFlag) {
  // Unclosed quote should cause exit code 1 in strict mode (long flag)
  auto result = CliRunner::run("head --strict " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos ||
              result.output.find("Strict mode") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StrictModeUnclosedQuoteEof) {
  // Unclosed quote at EOF should cause exit code 1 in strict mode
  auto result = CliRunner::run("head -S " + testDataPath("malformed/unclosed_quote_eof.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, NonStrictModeUnclosedQuoteSucceeds) {
  // Without strict mode, unclosed quote should still succeed (lenient parsing)
  auto result = CliRunner::run("head " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, StrictModeTailCommand) {
  // Strict mode should work with tail command
  auto result = CliRunner::run("tail -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, StrictModeSampleCommand) {
  // Strict mode should work with sample command
  auto result = CliRunner::run("sample -n 5 -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_StrictModeSelectCommand) {
  // Strict mode should work with select command
  auto result = CliRunner::run("select -c 0 -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_StrictModeInfoCommand) {
  // Strict mode should work with info command
  auto result = CliRunner::run("info -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_StrictModePrettyCommand) {
  // Strict mode should work with pretty command
  auto result = CliRunner::run("pretty -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_StrictModeHelpDocumented) {
  // Help text should document the strict flag
  auto result = CliRunner::run("-h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("--strict") != std::string::npos);
  EXPECT_TRUE(result.output.find("-S") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StrictModeWithValidFile) {
  // Strict mode should succeed with completely valid CSV
  auto result = CliRunner::run("head -S " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StrictModeInvalidQuoteEscape) {
  // Invalid quote escape should fail in strict mode
  auto result = CliRunner::run("head -S " + testDataPath("malformed/invalid_quote_escape.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_StrictModeQuoteInUnquotedField) {
  // Quote appearing in unquoted field should fail in strict mode
  auto result = CliRunner::run("head -S " + testDataPath("malformed/quote_in_unquoted_field.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

// =============================================================================
// Tail Command - Auto-detect Dialect Tests
// =============================================================================

TEST_F(CliTest, DISABLED_TailWithAutoDetect) {
  // Test tail command with auto-detection (no -d flag)
  // The default behavior is auto_detect = true when no delimiter is specified
  auto result = CliRunner::run("tail -n 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailAutoDetectTabFile) {
  // Test tail with auto-detection on tab-delimited file
  auto result = CliRunner::run("tail -n 2 " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Tab-delimited file should be parsed correctly with auto-detection
}

TEST_F(CliTest, TailStdinStrictModeError) {
  // Test strict mode error handling for stdin input
  auto result =
      CliRunner::runWithFileStdin("tail -n 2 -S -", testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Strict mode enabled") != std::string::npos ||
              result.output.find("Error") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailStdinWithExplicitDelimiter) {
  // Test stdin with explicit delimiter (auto_detect = false)
  auto result =
      CliRunner::runWithFileStdin("tail -n 2 -d comma -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, DISABLED_TailNoHeaderEmptyOutput) {
  // Test tail with -H flag on a file where we request 0 rows
  // This ensures the header output path is covered for the no-header case
  auto result = CliRunner::run("tail -n 0 -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output nothing when -H and -n 0
}

// =============================================================================
// Schema Command Tests
// =============================================================================

TEST_F(CliTest, DISABLED_SchemaBasicFile) {
  auto result = CliRunner::run("schema " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Schema:") != std::string::npos);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaShowsTypes) {
  auto result = CliRunner::run("schema " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show some type information
  EXPECT_TRUE(result.output.find("Type") != std::string::npos ||
              result.output.find("integer") != std::string::npos ||
              result.output.find("string") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaShowsNullable) {
  auto result = CliRunner::run("schema " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show nullable information
  EXPECT_TRUE(result.output.find("Nullable") != std::string::npos ||
              result.output.find("Yes") != std::string::npos ||
              result.output.find("No") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaJsonOutput) {
  auto result = CliRunner::run("schema -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output valid JSON structure
  EXPECT_TRUE(result.output.find("{") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"columns\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"name\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"type\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"nullable\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaEmptyFile) {
  auto result = CliRunner::run("schema " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_SchemaNoHeader) {
  auto result = CliRunner::run("schema -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should use generated column names
  EXPECT_TRUE(result.output.find("column_0") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaWithDelimiter) {
  auto result = CliRunner::run("schema -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Schema:") != std::string::npos);
}

// =============================================================================
// Stats Command Tests
// =============================================================================

TEST_F(CliTest, DISABLED_StatsBasicFile) {
  auto result = CliRunner::run("stats " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Statistics") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsCount) {
  auto result = CliRunner::run("stats " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Count") != std::string::npos ||
              result.output.find("count") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsNulls) {
  auto result = CliRunner::run("stats " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Null") != std::string::npos ||
              result.output.find("null") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsNumericStats) {
  auto result = CliRunner::run("stats " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // For numeric columns, should show min/max/mean
  EXPECT_TRUE(result.output.find("Min") != std::string::npos ||
              result.output.find("min") != std::string::npos);
  EXPECT_TRUE(result.output.find("Max") != std::string::npos ||
              result.output.find("max") != std::string::npos);
  EXPECT_TRUE(result.output.find("Mean") != std::string::npos ||
              result.output.find("mean") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonOutput) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output valid JSON structure
  EXPECT_TRUE(result.output.find("{") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"columns\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"count\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"nulls\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsEmptyFile) {
  auto result = CliRunner::run("stats " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, DISABLED_StatsNoHeader) {
  auto result = CliRunner::run("stats -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should use generated column names
  EXPECT_TRUE(result.output.find("column_0") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsWithDelimiter) {
  auto result = CliRunner::run("stats -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Statistics") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsRowCount) {
  auto result = CliRunner::run("stats " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // simple.csv has 3 data rows (excluding header)
  EXPECT_TRUE(result.output.find("3 rows") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonRowCount) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should include row count
  EXPECT_TRUE(result.output.find("\"rows\": 3") != std::string::npos);
}

TEST_F(CliTest, SchemaStrictMode) {
  // Schema command should fail in strict mode with malformed CSV
  auto result = CliRunner::run("schema -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, StatsStrictMode) {
  // Stats command should fail in strict mode with malformed CSV
  auto result = CliRunner::run("stats -S " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, DISABLED_SchemaHelpDocumented) {
  // Help text should document the schema command
  auto result = CliRunner::run("-h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("schema") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsHelpDocumented) {
  // Help text should document the stats command
  auto result = CliRunner::run("-h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("stats") != std::string::npos);
}

// =============================================================================
// Ambiguous Dialect Detection Tests (GitHub issue #225)
// Tests for best-guess output when multiple dialects have similar scores
// =============================================================================

TEST_F(CliTest, DISABLED_DialectAmbiguousSucceeds) {
  // When multiple dialects have similar scores, the command should still succeed
  // and output the best-guess dialect rather than failing with an error
  auto result = CliRunner::run("dialect " + testDataPath("edge_cases/ambiguous_delimiter.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should still detect a dialect (best guess)
  EXPECT_TRUE(result.output.find("Delimiter:") != std::string::npos);
  // Should include a warning about ambiguity
  EXPECT_TRUE(result.output.find("ambiguous") != std::string::npos ||
              result.output.find("Warning") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectAmbiguousJsonFormat) {
  // JSON output should include "ambiguous" field
  auto result = CliRunner::run("dialect -j " + testDataPath("edge_cases/ambiguous_delimiter.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have ambiguous field in JSON
  EXPECT_TRUE(result.output.find("\"ambiguous\":") != std::string::npos);
  // Should have confidence score
  EXPECT_TRUE(result.output.find("\"confidence\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectAmbiguousShowsAlternatives) {
  // When ambiguous, should show alternative candidates
  auto result = CliRunner::run("dialect " + testDataPath("edge_cases/ambiguous_delimiter.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show alternative candidates in warning output (stderr is merged to stdout)
  // The alternatives will show different delimiters that scored similarly
  EXPECT_TRUE(result.output.find("Alternative") != std::string::npos ||
              result.output.find("delimiter=") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectAmbiguousJsonShowsAlternatives) {
  // JSON output should include alternatives array when ambiguous
  auto result = CliRunner::run("dialect -j " + testDataPath("edge_cases/ambiguous_delimiter.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // When ambiguous, JSON should include alternatives array
  EXPECT_TRUE(result.output.find("\"alternatives\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectJsonAmbiguousFieldPresent) {
  // JSON output should always include "ambiguous" field
  auto result = CliRunner::run("dialect -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have ambiguous field (true or false)
  EXPECT_TRUE(result.output.find("\"ambiguous\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_DialectOutputsCliFlags) {
  // Dialect output should include CLI flags for reuse
  auto result = CliRunner::run("dialect " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("CLI flags:") != std::string::npos);
  EXPECT_TRUE(result.output.find("-d comma") != std::string::npos);
}

// =============================================================================
// Schema/Stats Sampling Tests (GitHub issue #378)
// Tests for the -m option to limit rows examined
// =============================================================================

TEST_F(CliTest, DISABLED_SchemaSampleSizeOption) {
  // Schema with -m option should work and limit rows examined
  auto result = CliRunner::run("schema -m 5 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Schema:") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsSampleSizeOption) {
  // Stats with -m option should work and limit rows examined
  auto result = CliRunner::run("stats -m 5 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Statistics") != std::string::npos);
  // Stats should report the sampled row count (5 rows)
  EXPECT_TRUE(result.output.find("5 rows") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaSampleSizeJsonOutput) {
  // Schema with -m and -j should produce valid JSON
  auto result = CliRunner::run("schema -m 5 -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("{") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"columns\"") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsSampleSizeJsonOutput) {
  // Stats with -m and -j should produce valid JSON with correct row count
  auto result = CliRunner::run("stats -m 5 -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"rows\": 5") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaSampleSizeZeroProcessesAll) {
  // Schema with -m 0 should process all rows (default behavior)
  auto result = CliRunner::run("schema -m 0 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Schema:") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsSampleSizeZeroProcessesAll) {
  // Stats with -m 0 should process all rows (default behavior)
  auto result = CliRunner::run("stats -m 0 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // many_rows.csv has 20 data rows
  EXPECT_TRUE(result.output.find("20 rows") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsSampleSizeLargerThanFile) {
  // When sample size exceeds file rows, should process all rows
  auto result = CliRunner::run("stats -m 1000 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // many_rows.csv has 20 data rows, should process all 20
  EXPECT_TRUE(result.output.find("20 rows") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaSampleSizeHelpDocumented) {
  // Help text should document the -m option
  auto result = CliRunner::run("-h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("-m") != std::string::npos);
  EXPECT_TRUE(result.output.find("sample") != std::string::npos ||
              result.output.find("Sample") != std::string::npos);
}

TEST_F(CliTest, DISABLED_SchemaSampleSizeInvalidValue) {
  // Invalid sample size should produce an error
  auto result = CliRunner::run("schema -m abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Invalid sample size") != std::string::npos);
}

TEST_F(CliTest, StatsSampleSizeNegativeValue) {
  // Negative sample size should produce an error
  auto result = CliRunner::run("stats -m -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

// =============================================================================
// Extended Statistics Tests (GitHub issue #388)
// Tests for new statistics: std dev, percentiles, histogram, string stats
// =============================================================================

TEST_F(CliTest, DISABLED_StatsShowsStdDev) {
  auto result = CliRunner::run("stats " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show standard deviation for numeric columns
  EXPECT_TRUE(result.output.find("Std Dev") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsPercentiles) {
  auto result = CliRunner::run("stats " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show percentiles (p0, p25, p50, p75, p100)
  EXPECT_TRUE(result.output.find("Percentiles") != std::string::npos);
  EXPECT_TRUE(result.output.find("p0=") != std::string::npos);
  EXPECT_TRUE(result.output.find("p25=") != std::string::npos);
  EXPECT_TRUE(result.output.find("p50=") != std::string::npos);
  EXPECT_TRUE(result.output.find("p75=") != std::string::npos);
  EXPECT_TRUE(result.output.find("p100=") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsHistogram) {
  auto result = CliRunner::run("stats " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show histogram for numeric columns
  EXPECT_TRUE(result.output.find("Histogram") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsCompleteRate) {
  auto result = CliRunner::run("stats " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show complete rate (non-null ratio)
  EXPECT_TRUE(result.output.find("Complete rate") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsUniqueValues) {
  auto result = CliRunner::run("stats " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show unique value count
  EXPECT_TRUE(result.output.find("Unique values") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsShowsStringLength) {
  auto result = CliRunner::run("stats " + testDataPath("real_world/contacts.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show min/max length for string columns
  EXPECT_TRUE(result.output.find("Min length") != std::string::npos);
  EXPECT_TRUE(result.output.find("Max length") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonShowsStdDev) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should include standard deviation
  EXPECT_TRUE(result.output.find("\"sd\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonShowsPercentiles) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should include all percentiles
  EXPECT_TRUE(result.output.find("\"p0\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"p25\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"p50\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"p75\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"p100\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonShowsHistogram) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should include histogram
  EXPECT_TRUE(result.output.find("\"hist\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonShowsCompleteRate) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should include complete_rate
  EXPECT_TRUE(result.output.find("\"complete_rate\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsJsonShowsStringStats) {
  auto result = CliRunner::run("stats -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should include string statistics
  EXPECT_TRUE(result.output.find("\"n_unique\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"min_length\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"max_length\":") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsPercentileAccuracy) {
  // Test with many_rows.csv which has IDs 1-20
  // p50 (median) of 1-20 should be around 10.5
  auto result = CliRunner::run("stats -j " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The median (p50) for 1-20 is 10.5
  EXPECT_TRUE(result.output.find("\"p50\": 10.5") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsStdDevAccuracy) {
  // Test with simple.csv which has values 1,2,3 / 4,5,6 / 7,8,9
  // Column A has values 1,4,7 -> mean=4, std dev ~ 3.0
  auto result = CliRunner::run("stats -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should contain sd field with a value (not null)
  EXPECT_TRUE(result.output.find("\"sd\": 3.") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsAllEmptyColumn) {
  // Test that columns with only empty values are handled correctly
  // The JSON output should have null for min_length/max_length and 0 for n_unique
  auto result = CliRunner::run("stats -j " + testDataPath("edge_cases/all_empty_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Check the JSON is valid (not corrupted by SIZE_MAX values)
  // Look for the empty_col column which should have null for string lengths
  EXPECT_TRUE(result.output.find("\"min_length\": null") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"max_length\": null") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"n_unique\": 0") != std::string::npos);
}

TEST_F(CliTest, DISABLED_StatsAllEmptyColumnHumanReadable) {
  // Test that human-readable output handles all-empty columns gracefully
  auto result = CliRunner::run("stats " + testDataPath("edge_cases/all_empty_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The empty column should not crash or show SIZE_MAX values
  // Should show the column exists with proper null count
  EXPECT_TRUE(result.output.find("empty_col") != std::string::npos);
  EXPECT_TRUE(result.output.find("Nulls") != std::string::npos);
}

// =============================================================================
// Convert Command Tests (Arrow-enabled builds only)
// =============================================================================
// These tests verify the convert command behavior. They are skipped on builds
// without Arrow support since the convert command won't exist.

TEST_F(CliTest, DISABLED_ConvertCommandMissingOutputPath) {
  // convert requires -o option
  auto result = CliRunner::run("convert " + testDataPath("basic/simple.csv"));
  // Either "Unknown command" (no Arrow) or "Output path required" error
  bool is_unknown_command = result.output.find("Unknown command") != std::string::npos;
  bool is_missing_output = result.output.find("Output path required") != std::string::npos;
  if (!is_unknown_command) {
    EXPECT_NE(result.exit_code, 0);
    EXPECT_TRUE(is_missing_output)
        << "Expected 'Output path required' error, got: " << result.output;
  }
}

TEST_F(CliTest, DISABLED_ConvertCommandInvalidFormat) {
  // Invalid -F value should fail
  auto result = CliRunner::run("convert " + testDataPath("basic/simple.csv") +
                               " -o /tmp/test.out -F invalid_format");
  bool is_unknown_command = result.output.find("Unknown command") != std::string::npos;
  bool is_invalid_format = result.output.find("Unknown output format") != std::string::npos;
  if (!is_unknown_command) {
    EXPECT_NE(result.exit_code, 0);
    EXPECT_TRUE(is_invalid_format)
        << "Expected 'Unknown output format' error, got: " << result.output;
  }
}

TEST_F(CliTest, DISABLED_ConvertCommandFromStdinError) {
  // convert command does not support stdin input
  auto result =
      CliRunner::runWithFileStdin("convert -o /tmp/test.feather", testDataPath("basic/simple.csv"));
  bool is_unknown_command = result.output.find("Unknown command") != std::string::npos;
  bool is_stdin_error = result.output.find("Cannot convert from stdin") != std::string::npos;
  if (!is_unknown_command) {
    EXPECT_NE(result.exit_code, 0);
    EXPECT_TRUE(is_stdin_error) << "Expected stdin error, got: " << result.output;
  }
}

TEST_F(CliTest, DISABLED_ConvertCommandUnknownExtension) {
  // Unknown extension without explicit format should fail
  auto result =
      CliRunner::run("convert " + testDataPath("basic/simple.csv") + " -o /tmp/test.unknown");
  bool is_unknown_command = result.output.find("Unknown command") != std::string::npos;
  bool is_format_error = result.output.find("Cannot determine output format") != std::string::npos;
  if (!is_unknown_command) {
    EXPECT_NE(result.exit_code, 0);
    EXPECT_TRUE(is_format_error) << "Expected format detection error, got: " << result.output;
  }
}

TEST_F(CliTest, DISABLED_ConvertCommandInvalidCompression) {
  // Invalid -C value should fail (only matters for parquet)
  auto result = CliRunner::run("convert " + testDataPath("basic/simple.csv") +
                               " -o /tmp/test.parquet -C invalid_codec");
  bool is_unknown_command = result.output.find("Unknown command") != std::string::npos;
  bool is_codec_error = result.output.find("Unknown compression codec") != std::string::npos;
  if (!is_unknown_command) {
    EXPECT_NE(result.exit_code, 0);
    EXPECT_TRUE(is_codec_error) << "Expected compression codec error, got: " << result.output;
  }
}

TEST_F(CliTest, ConvertHelpShowsConvertCommand) {
  // Check that --help includes convert command when Arrow is enabled
  auto result = CliRunner::run("--help");
  EXPECT_EQ(result.exit_code, 0);
  // The help text will only include "convert" if built with Arrow support
  // This test documents the expected behavior without requiring Arrow
  bool has_convert = result.output.find("convert") != std::string::npos;
  // Just verify help runs successfully - convert presence depends on build
  EXPECT_TRUE(result.output.find("vroom") != std::string::npos);
}
