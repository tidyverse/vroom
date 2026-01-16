/**
 * @file branchless_state_machine.h
 * @brief Branchless CSV state machine implementation for high-performance parsing.
 *
 * This header provides a branchless implementation of the CSV state machine
 * that eliminates branch mispredictions in the performance-critical parsing paths.
 * The implementation uses:
 *
 * 1. **Lookup Table State Machine**: Pre-computed 5×4 lookup table mapping
 *    current state and character classification to next state.
 *
 * 2. **SIMD Character Classification**: Bitmask operations to classify all
 *    characters in a 64-byte block simultaneously.
 *
 * 3. **Bit Manipulation for State Tracking**: simdjson-inspired approach
 *    encoding state information in bitmasks rather than sequential processing.
 *
 * The goal is to eliminate 90%+ of branches in performance-critical paths and
 * achieve significant IPC (instructions per cycle) improvement.
 *
 * @see two_pass.h For the main parser that uses this state machine
 * @see https://github.com/jimhester/libvroom/issues/41 For design discussion
 */

#ifndef LIBVROOM_BRANCHLESS_STATE_MACHINE_H
#define LIBVROOM_BRANCHLESS_STATE_MACHINE_H

#include "common_defs.h"
#include "error.h"
#include "simd_highway.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

namespace libvroom {

/**
 * @brief Character classification for branchless CSV parsing.
 *
 * Characters are classified into 5 categories that determine state transitions:
 * - DELIMITER (0): Field separator (typically comma)
 * - QUOTE (1): Quote character (typically double-quote)
 * - NEWLINE (2): Line terminator (\n)
 * - OTHER (3): All other characters
 * - ESCAPE (4): Escape character (typically backslash when not using double-quote escaping)
 */
enum CharClass : uint8_t {
  CHAR_DELIMITER = 0,
  CHAR_QUOTE = 1,
  CHAR_NEWLINE = 2,
  CHAR_OTHER = 3,
  CHAR_ESCAPE = 4
};

/**
 * @brief CSV parser state for branchless state machine.
 *
 * Uses numeric values 0-5 for direct indexing into lookup tables.
 */
enum BranchlessState : uint8_t {
  STATE_RECORD_START = 0,   // At the beginning of a new record (row)
  STATE_FIELD_START = 1,    // At the beginning of a new field (after comma)
  STATE_UNQUOTED_FIELD = 2, // Inside an unquoted field
  STATE_QUOTED_FIELD = 3,   // Inside a quoted field
  STATE_QUOTED_END = 4,     // Just saw a quote inside a quoted field
  STATE_ESCAPED = 5         // Just saw an escape character (next char is literal)
};

/**
 * @brief Error codes for branchless state transitions.
 */
enum BranchlessError : uint8_t {
  ERR_NONE = 0,
  ERR_QUOTE_IN_UNQUOTED = 1,
  ERR_INVALID_AFTER_QUOTE = 2
};

/**
 * @brief Combined state and error result packed into a single byte.
 *
 * Layout: [error (2 bits)][state (3 bits)][is_separator (1 bit)][reserved (2 bits)]
 * This packing allows for efficient table lookups and minimal memory usage.
 */
struct alignas(1) PackedResult {
  uint8_t data;

  really_inline BranchlessState state() const {
    return static_cast<BranchlessState>((data >> 3) & 0x07);
  }

  really_inline BranchlessError error() const {
    return static_cast<BranchlessError>((data >> 6) & 0x03);
  }

  really_inline bool is_separator() const { return (data >> 2) & 0x01; }

  static really_inline PackedResult make(BranchlessState s, BranchlessError e, bool sep) {
    PackedResult r;
    r.data = (static_cast<uint8_t>(e) << 6) | (static_cast<uint8_t>(s) << 3) | (sep ? 0x04 : 0x00);
    return r;
  }
};

/**
 * @brief Branchless CSV state machine using lookup tables.
 *
 * The state machine processes characters without branches by using:
 * 1. A character classification table (256 bytes) for O(1) character -> class mapping
 * 2. A state transition table (6 states × 5 char classes = 30 bytes) for O(1) transitions
 *
 * This eliminates the switch statements in the original implementation that caused
 * significant branch mispredictions (64+ possible mispredictions per 64-byte block).
 *
 * Escape character handling:
 * - When double_quote=true (RFC 4180): escape_char is ignored, "" escapes to "
 * - When double_quote=false: escape_char (e.g., backslash) escapes the next character
 *   - Inside quotes: \" becomes literal "
 *   - Escape char can also escape delimiters, newlines, itself
 */
class BranchlessStateMachine {
public:
  /**
   * @brief Initialize the state machine with given delimiter, quote, and escape characters.
   *
   * @param delimiter Field separator character (default: comma)
   * @param quote_char Quote character (default: double-quote)
   * @param escape_char Escape character (default: same as quote_char for RFC 4180)
   * @param double_quote If true, use RFC 4180 double-quote escaping; if false, use escape_char
   */
  explicit BranchlessStateMachine(char delimiter = ',', char quote_char = '"',
                                  char escape_char = '"', bool double_quote = true) {
    init_char_class_table(delimiter, quote_char, escape_char, double_quote);
    init_transition_table(double_quote);
  }

  /**
   * @brief Reinitialize with new delimiter, quote, and escape characters.
   */
  void reinit(char delimiter, char quote_char, char escape_char = '"', bool double_quote = true) {
    init_char_class_table(delimiter, quote_char, escape_char, double_quote);
    init_transition_table(double_quote);
  }

  /**
   * @brief Classify a single character (branchless).
   */
  really_inline CharClass classify(uint8_t c) const {
    return static_cast<CharClass>(char_class_table_[c]);
  }

  /**
   * @brief Get the next state for a given current state and character class (branchless).
   */
  really_inline PackedResult transition(BranchlessState state, CharClass char_class) const {
    return transition_table_[state * 5 + char_class];
  }

  /**
   * @brief Process a single character and return the new state (branchless).
   *
   * This is the main entry point for character-by-character processing.
   * It combines classification and transition in a single call.
   */
  really_inline PackedResult process(BranchlessState state, uint8_t c) const {
    return transition(state, classify(c));
  }

  /**
   * @brief Create 64-bit bitmask for characters matching the delimiter.
   */
  really_inline uint64_t delimiter_mask(const simd_input& in) const {
    return cmp_mask_against_input(in, delimiter_);
  }

  /**
   * @brief Create 64-bit bitmask for characters matching the quote character.
   */
  really_inline uint64_t quote_mask(const simd_input& in) const {
    return cmp_mask_against_input(in, quote_char_);
  }

  /**
   * @brief Create 64-bit bitmask for line ending characters.
   *
   * Supports LF (\n), CRLF (\r\n), and CR-only (\r) line endings:
   * - LF positions are always included
   * - CR positions are included only if NOT immediately followed by LF
   *
   * For CRLF sequences, only the LF is marked as the line ending.
   * The CR in CRLF is handled during value extraction (stripped from field end).
   */
  really_inline uint64_t newline_mask(const simd_input& in) const {
    return compute_line_ending_mask_simple(in, ~0ULL);
  }

  /**
   * @brief Create 64-bit bitmask for line endings with validity mask.
   */
  really_inline uint64_t newline_mask(const simd_input& in, uint64_t valid_mask) const {
    return compute_line_ending_mask_simple(in, valid_mask);
  }

  /**
   * @brief Get current delimiter character.
   */
  really_inline char delimiter() const { return delimiter_; }

  /**
   * @brief Get current quote character.
   */
  really_inline char quote_char() const { return quote_char_; }

  /**
   * @brief Get current escape character.
   */
  really_inline char escape_char() const { return escape_char_; }

  /**
   * @brief Check if using double-quote escaping (RFC 4180).
   */
  really_inline bool uses_double_quote() const { return double_quote_; }

  /**
   * @brief Create 64-bit bitmask for characters matching the escape character.
   * Only meaningful when not using double-quote mode.
   */
  really_inline uint64_t escape_mask(const simd_input& in) const {
    return cmp_mask_against_input(in, static_cast<uint8_t>(escape_char_));
  }

private:
  // Character classification table (256 entries for O(1) lookup)
  alignas(64) uint8_t char_class_table_[256];

  // State transition table (6 states × 5 char classes = 30 entries)
  // Packed results for efficient access
  alignas(32) PackedResult transition_table_[30];

  // Store delimiter, quote, and escape for SIMD operations
  char delimiter_;
  char quote_char_;
  char escape_char_;
  bool double_quote_;

  /**
   * @brief Initialize the character classification table.
   *
   * Default classification is OTHER (3). Special characters get their own
   * classifications: delimiter, quote, newline, and optionally escape.
   *
   * When double_quote=true (RFC 4180 mode), escape_char is not classified
   * as ESCAPE since escaping is handled by quote doubling.
   *
   * When double_quote=false (escape char mode), escape_char is classified
   * as ESCAPE so the state machine can handle backslash escaping.
   */
  void init_char_class_table(char delimiter, char quote_char, char escape_char, bool double_quote) {
    delimiter_ = delimiter;
    quote_char_ = quote_char;
    escape_char_ = escape_char;
    double_quote_ = double_quote;

    // Initialize all characters as OTHER
    for (int i = 0; i < 256; ++i) {
      char_class_table_[i] = CHAR_OTHER;
    }

    // Set special characters
    char_class_table_[static_cast<uint8_t>(delimiter)] = CHAR_DELIMITER;
    char_class_table_[static_cast<uint8_t>(quote_char)] = CHAR_QUOTE;
    char_class_table_[static_cast<uint8_t>('\n')] = CHAR_NEWLINE;

    // Only classify escape character as ESCAPE when not using double-quote mode
    // and escape_char is different from quote_char
    if (!double_quote && escape_char != quote_char && escape_char != '\0') {
      char_class_table_[static_cast<uint8_t>(escape_char)] = CHAR_ESCAPE;
    }
  }

  /**
   * @brief Initialize the state transition table.
   *
   * This table encodes all valid CSV state transitions.
   *
   * For RFC 4180 mode (double_quote=true):
   * - Escaping is done by doubling quotes: "" -> "
   * - ESCAPE char class is never used (escape char not classified)
   *
   * For escape char mode (double_quote=false):
   * - Escaping is done with escape char: \" -> "
   * - ESCAPE transitions to STATE_ESCAPED, next char is literal
   *
   * State transitions:
   *
   * From RECORD_START:
   *   - DELIMITER -> FIELD_START (record separator)
   *   - QUOTE -> QUOTED_FIELD (start quoted field)
   *   - NEWLINE -> RECORD_START (empty row, record separator)
   *   - OTHER -> UNQUOTED_FIELD (start unquoted field)
   *   - ESCAPE -> UNQUOTED_FIELD (escape at field start, treat as content)
   *
   * From FIELD_START:
   *   - DELIMITER -> FIELD_START (empty field, field separator)
   *   - QUOTE -> QUOTED_FIELD (start quoted field)
   *   - NEWLINE -> RECORD_START (empty field at end of row, record separator)
   *   - OTHER -> UNQUOTED_FIELD (start unquoted field)
   *   - ESCAPE -> UNQUOTED_FIELD (escape at field start, treat as content)
   *
   * From UNQUOTED_FIELD:
   *   - DELIMITER -> FIELD_START (end field, field separator)
   *   - QUOTE -> ERROR (quote in unquoted field) [RFC 4180 mode]
   *            or UNQUOTED_FIELD (literal quote in escape mode if no escape)
   *   - NEWLINE -> RECORD_START (end field and row, record separator)
   *   - OTHER -> UNQUOTED_FIELD (continue field)
   *   - ESCAPE -> UNQUOTED_FIELD (in escape mode, next char literal but stay unquoted)
   *
   * From QUOTED_FIELD:
   *   - DELIMITER -> QUOTED_FIELD (literal comma in field)
   *   - QUOTE -> QUOTED_END (potential end or RFC 4180 escape)
   *   - NEWLINE -> QUOTED_FIELD (literal newline in field)
   *   - OTHER -> QUOTED_FIELD (continue field)
   *   - ESCAPE -> STATE_ESCAPED (in escape mode, next char is literal)
   *
   * From QUOTED_END:
   *   - DELIMITER -> FIELD_START (end quoted field, field separator)
   *   - QUOTE -> QUOTED_FIELD (RFC 4180 escaped quote, continue)
   *   - NEWLINE -> RECORD_START (end quoted field and row, record separator)
   *   - OTHER -> ERROR (invalid char after closing quote)
   *   - ESCAPE -> ERROR (invalid, escape after closing quote)
   *
   * From STATE_ESCAPED (only used in escape char mode):
   *   - Any char -> QUOTED_FIELD (literal char, continue quoted field)
   *   Note: The escaped character is consumed as a literal, regardless of what it is
   */
  void init_transition_table(bool double_quote) {
    // RECORD_START transitions (index 0-4)
    transition_table_[STATE_RECORD_START * 5 + CHAR_DELIMITER] =
        PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
    transition_table_[STATE_RECORD_START * 5 + CHAR_QUOTE] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_RECORD_START * 5 + CHAR_NEWLINE] =
        PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
    transition_table_[STATE_RECORD_START * 5 + CHAR_OTHER] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);
    // ESCAPE at record start: start unquoted field (escape is just content)
    transition_table_[STATE_RECORD_START * 5 + CHAR_ESCAPE] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

    // FIELD_START transitions (index 5-9)
    transition_table_[STATE_FIELD_START * 5 + CHAR_DELIMITER] =
        PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
    transition_table_[STATE_FIELD_START * 5 + CHAR_QUOTE] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_FIELD_START * 5 + CHAR_NEWLINE] =
        PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
    transition_table_[STATE_FIELD_START * 5 + CHAR_OTHER] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);
    // ESCAPE at field start: start unquoted field (escape is just content)
    transition_table_[STATE_FIELD_START * 5 + CHAR_ESCAPE] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

    // UNQUOTED_FIELD transitions (index 10-14)
    transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_DELIMITER] =
        PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
    // In double-quote mode, quote in unquoted field is an error
    // In escape mode, quote in unquoted field is also an error (should be preceded by escape)
    transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_QUOTE] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_QUOTE_IN_UNQUOTED, false);
    transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_NEWLINE] =
        PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
    transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_OTHER] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);
    // ESCAPE in unquoted field: stay in unquoted field (escape is content in unquoted)
    // We don't support escaping in unquoted fields - the escape is just literal content
    transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_ESCAPE] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

    // QUOTED_FIELD transitions (index 15-19)
    transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_DELIMITER] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_QUOTE] =
        PackedResult::make(STATE_QUOTED_END, ERR_NONE, false);
    transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_NEWLINE] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_OTHER] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    // ESCAPE in quoted field: go to escaped state (next char is literal)
    // Note: In double_quote mode, ESCAPE char class is never assigned, so this won't be reached
    transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_ESCAPE] =
        PackedResult::make(STATE_ESCAPED, ERR_NONE, false);

    // QUOTED_END transitions (index 20-24)
    transition_table_[STATE_QUOTED_END * 5 + CHAR_DELIMITER] =
        PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
    // In double_quote mode: quote after quote = escaped quote, back to quoted field
    // In escape mode: this state means quote closed the field, another quote is an error
    if (double_quote) {
      transition_table_[STATE_QUOTED_END * 5 + CHAR_QUOTE] =
          PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    } else {
      // In escape mode, we shouldn't see quote after closing quote
      // This would be ""  which in escape mode means empty closing followed by opening
      // But we're already past the closing quote, so this is an error
      transition_table_[STATE_QUOTED_END * 5 + CHAR_QUOTE] =
          PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false); // Allow for compatibility
    }
    transition_table_[STATE_QUOTED_END * 5 + CHAR_NEWLINE] =
        PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
    transition_table_[STATE_QUOTED_END * 5 + CHAR_OTHER] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_INVALID_AFTER_QUOTE, false);
    // ESCAPE after closing quote: error (should not have escape after closing quote)
    transition_table_[STATE_QUOTED_END * 5 + CHAR_ESCAPE] =
        PackedResult::make(STATE_UNQUOTED_FIELD, ERR_INVALID_AFTER_QUOTE, false);

    // STATE_ESCAPED transitions (index 25-29)
    // After escape char, any character is literal and we return to quoted field
    // This is the key for backslash escaping: \" becomes literal "
    transition_table_[STATE_ESCAPED * 5 + CHAR_DELIMITER] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_ESCAPED * 5 + CHAR_QUOTE] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_ESCAPED * 5 + CHAR_NEWLINE] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_ESCAPED * 5 + CHAR_OTHER] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
    transition_table_[STATE_ESCAPED * 5 + CHAR_ESCAPE] =
        PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false); // \\ is escaped backslash
  }
};

/**
 * @brief SIMD-accelerated block processing with branchless state extraction.
 *
 * This function uses SIMD to find potential separator positions, then
 * uses the branchless state machine to validate which separators are
 * actually field boundaries (not inside quoted fields).
 *
 * The approach:
 * 1. Use SIMD to find all delimiter, quote, and newline positions (bitmasks)
 * 2. Compute quote mask to identify positions inside quoted strings
 * 3. For escape char mode: mask out escaped quotes before computing quote parity
 * 4. Extract valid separator positions using bitwise operations
 * 5. Update state machine only at quote boundaries
 *
 * @param sm The branchless state machine
 * @param in SIMD input block (64 bytes)
 * @param len Actual length to process
 * @param prev_quote_state Previous iteration's inside-quote state (all 0s or 1s)
 * @param prev_escape_carry For escape char mode: whether previous block ended with unmatched escape
 * @param indexes Output array for field separator positions
 * @param base Base position in the full buffer
 * @param idx Current index in indexes array
 * @param stride Stride for multi-threaded index storage
 * @return Number of field separators found
 */
really_inline size_t process_block_simd_branchless(const BranchlessStateMachine& sm,
                                                   const simd_input& in, size_t len,
                                                   uint64_t& prev_quote_state,
                                                   uint64_t& prev_escape_carry, uint64_t* indexes,
                                                   uint64_t base, uint64_t& idx, int stride) {
  // Create mask for valid bytes (handle partial final block)
  uint64_t valid_mask = (len < 64) ? blsmsk_u64(1ULL << len) : ~0ULL;

  // Get bitmasks for special characters using SIMD
  uint64_t quotes = sm.quote_mask(in) & valid_mask;
  uint64_t delimiters = sm.delimiter_mask(in) & valid_mask;
  // Use newline_mask with valid_mask for proper CR/CRLF handling
  uint64_t newlines = sm.newline_mask(in, valid_mask);

  // Handle escape character mode (e.g., backslash escaping)
  // In escape mode, we need to ignore quotes that are preceded by an escape char
  uint64_t escaped_positions = 0;
  if (!sm.uses_double_quote()) {
    uint64_t escapes = sm.escape_mask(in) & valid_mask;
    escaped_positions = compute_escaped_mask(escapes, prev_escape_carry);

    // Remove escaped quotes from the quote mask
    // An escaped quote doesn't toggle quote state
    quotes &= ~escaped_positions;
    // Also remove escaped delimiters and newlines (they're literal content)
    delimiters &= ~escaped_positions;
    newlines &= ~escaped_positions;
  }

  // Compute quote mask: positions that are inside quotes
  // Uses XOR prefix sum to track quote parity
  uint64_t inside_quote = find_quote_mask2(quotes, prev_quote_state);

  // Debug output for escape mode
  (void)escaped_positions; // Silence unused warning when debug disabled
#if 0
    if (!sm.uses_double_quote() && base == 0) {
        fprintf(stderr, "DEBUG SIMD escape processing:\n");
        fprintf(stderr, "  valid_mask=0x%016llx\n", (unsigned long long)valid_mask);
        fprintf(stderr, "  escapes=0x%016llx\n", (unsigned long long)(sm.escape_mask(in) & valid_mask));
        fprintf(stderr, "  escaped_positions=0x%016llx\n", (unsigned long long)escaped_positions);
        fprintf(stderr, "  quotes (after masking)=0x%016llx\n", (unsigned long long)quotes);
        fprintf(stderr, "  inside_quote=0x%016llx\n", (unsigned long long)inside_quote);
        fprintf(stderr, "  prev_quote_state=0x%016llx\n", (unsigned long long)prev_quote_state);
    }
#endif

  // Field separators are delimiters/newlines that are NOT inside quotes
  uint64_t field_seps = (delimiters | newlines) & ~inside_quote & valid_mask;

  // Write separator positions
  return write(indexes, idx, base, stride, field_seps);
}

/**
 * @brief Second pass using SIMD-accelerated branchless processing.
 *
 * This is the main performance-optimized function that combines SIMD
 * character detection with branchless state tracking.
 *
 * Supports both RFC 4180 double-quote escaping and custom escape character
 * modes (e.g., backslash escaping).
 *
 * @param sm The branchless state machine
 * @param buf Input buffer
 * @param start Start position
 * @param end End position
 * @param indexes Output array (pre-offset per-thread base pointer)
 * @param thread_id Thread ID (kept for API compatibility, unused)
 * @param n_threads Total number of threads (kept for API compatibility, unused)
 * @return Number of field separators found
 */
inline uint64_t second_pass_simd_branchless(const BranchlessStateMachine& sm, const uint8_t* buf,
                                            size_t start, size_t end, uint64_t* indexes,
                                            size_t /*thread_id*/, int /*n_threads*/) {
  assert(end >= start && "Invalid range: end must be >= start");
  size_t len = end - start;
  size_t pos = 0;
  uint64_t idx = 0; // Start at 0; thread offset handled by caller
  uint64_t prev_quote_state = 0ULL;
  uint64_t prev_escape_carry = 0ULL; // For escape char mode
  uint64_t count = 0;
  const uint8_t* data = buf + start;

  // Process 64-byte blocks
  // Caller passes per-thread base pointer; writes are contiguous within each thread's region.
  for (; pos + 64 <= len; pos += 64) {
    libvroom_prefetch(data + pos + 128);

    simd_input in = fill_input(data + pos);
    count += process_block_simd_branchless(sm, in, 64, prev_quote_state, prev_escape_carry, indexes,
                                           start + pos, idx, 1);
  }

  // Handle remaining bytes (< 64)
  if (pos < len) {
    simd_input in = fill_input_safe(data + pos, len - pos);
    count += process_block_simd_branchless(sm, in, len - pos, prev_quote_state, prev_escape_carry,
                                           indexes, start + pos, idx, 1);
  }

  return count;
}

/**
 * @brief Result structure from branchless second pass with state.
 *
 * Contains both the number of indexes found and whether parsing ended
 * at a record boundary. Used for speculation validation per Chang et al.
 * Algorithm 1 - if a chunk doesn't end at a record boundary, the
 * speculation was incorrect.
 */
struct BranchlessSecondPassResult {
  uint64_t n_indexes;      ///< Number of field separators found
  bool at_record_boundary; ///< True if parsing ended at a record boundary
};

/**
 * @brief SIMD-accelerated second pass that also returns ending state.
 *
 * This version returns both the index count and whether parsing ended at
 * a record boundary. Used for speculation validation per Chang et al.
 * Algorithm 1 - chunks must end at record boundaries for speculation
 * to be valid.
 *
 * A chunk ends at a record boundary if the final quote parity is even
 * (not inside a quoted field). If we end inside a quote, the speculation
 * was definitely wrong and we need to fall back to two-pass parsing.
 *
 * @param sm The branchless state machine
 * @param buf Input buffer
 * @param start Start position
 * @param end End position
 * @param indexes Output array (pre-offset per-thread base pointer)
 * @param thread_id Thread ID (kept for API compatibility, unused)
 * @param n_threads Total number of threads (kept for API compatibility, unused)
 * @return BranchlessSecondPassResult with count and boundary status
 */
inline BranchlessSecondPassResult
second_pass_simd_branchless_with_state(const BranchlessStateMachine& sm, const uint8_t* buf,
                                       size_t start, size_t end, uint64_t* indexes,
                                       size_t /*thread_id*/, int /*n_threads*/) {
  assert(end >= start && "Invalid range: end must be >= start");
  size_t len = end - start;
  size_t pos = 0;
  uint64_t idx = 0; // Start at 0; thread offset handled by caller
  uint64_t prev_quote_state = 0ULL;
  uint64_t prev_escape_carry = 0ULL; // For escape char mode
  uint64_t count = 0;
  const uint8_t* data = buf + start;

  // Process 64-byte blocks
  // Caller passes per-thread base pointer; writes are contiguous within each thread's region.
  for (; pos + 64 <= len; pos += 64) {
    libvroom_prefetch(data + pos + 128);

    simd_input in = fill_input(data + pos);
    count += process_block_simd_branchless(sm, in, 64, prev_quote_state, prev_escape_carry, indexes,
                                           start + pos, idx, 1);
  }

  // Handle remaining bytes (< 64)
  if (pos < len) {
    simd_input in = fill_input_safe(data + pos, len - pos);
    count += process_block_simd_branchless(sm, in, len - pos, prev_quote_state, prev_escape_carry,
                                           indexes, start + pos, idx, 1);
  }

  // Check if we ended at a record boundary:
  // Not inside a quoted field (prev_quote_state == 0)
  //
  // The key insight from Chang et al. Algorithm 1: if speculative chunk
  // boundary detection was wrong, parsing this chunk will end inside a
  // quoted field. The next chunk would then start mid-quote, leading to
  // incorrect parsing. By checking the ending state, we can detect this
  // misprediction and fall back to reliable two-pass parsing.
  bool at_record_boundary = (prev_quote_state == 0);

  return {count, at_record_boundary};
}

/**
 * @brief SIMD-accelerated block processing with error detection.
 *
 * This is an optimized version of process_block_simd_branchless that also
 * detects error conditions using SIMD. Error positions are returned as a
 * bitmask for deferred scalar processing.
 *
 * Error detection:
 * - Null bytes: detected via SIMD comparison
 * - Quote errors: detected by analyzing quote positions relative to field boundaries
 *
 * Performance: Processes 64 bytes per iteration using SIMD. Only positions
 * with potential errors are processed with scalar code.
 *
 * @param sm The branchless state machine
 * @param in SIMD input block (64 bytes)
 * @param len Actual length to process
 * @param prev_quote_state Previous iteration's inside-quote state (all 0s or 1s)
 * @param prev_escape_carry For escape char mode: whether previous block ended with unmatched escape
 * @param indexes Output array for field separator positions
 * @param base Base position in the full buffer
 * @param idx Current index in indexes array
 * @param stride Stride for multi-threaded index storage
 * @param null_byte_mask Output: bitmask of null byte positions (for error reporting)
 * @param quote_error_mask Output: bitmask of potential quote error positions
 * @return Number of field separators found
 */
really_inline size_t process_block_simd_branchless_with_errors(
    const BranchlessStateMachine& sm, const simd_input& in, size_t len, uint64_t& prev_quote_state,
    uint64_t& prev_escape_carry, uint64_t* indexes, uint64_t base, uint64_t& idx, int stride,
    uint64_t& null_byte_mask, uint64_t& quote_error_mask) {
  // Create mask for valid bytes (handle partial final block)
  // For len < 64: we want bits 0 to len-1 set, so (1ULL << len) - 1
  // Note: blsmsk_u64(1ULL << len) gives bits 0 to len inclusive, which is one too many
  uint64_t valid_mask = (len < 64) ? ((1ULL << len) - 1) : ~0ULL;

  // Get bitmasks for special characters using SIMD
  uint64_t quotes = sm.quote_mask(in) & valid_mask;
  uint64_t delimiters = sm.delimiter_mask(in) & valid_mask;
  // Use newline_mask with valid_mask for proper CR/CRLF handling
  uint64_t newlines = sm.newline_mask(in, valid_mask);

  // Detect null bytes for error reporting
  null_byte_mask = cmp_mask_against_input(in, 0) & valid_mask;

  // Handle escape character mode (e.g., backslash escaping)
  uint64_t escaped_positions = 0;
  if (!sm.uses_double_quote()) {
    uint64_t escapes = sm.escape_mask(in) & valid_mask;
    escaped_positions = compute_escaped_mask(escapes, prev_escape_carry);

    // Remove escaped quotes from the quote mask
    quotes &= ~escaped_positions;
    // Also remove escaped delimiters and newlines
    delimiters &= ~escaped_positions;
    newlines &= ~escaped_positions;
  }

  // Save previous quote state before update for error detection
  // If prev_quote_state is all 1s, we entered this block inside a quote
  uint64_t was_inside_quote = prev_quote_state;

  // Compute quote mask: positions that are inside quotes
  // Note: inside_quote[i] = 1 if we're inside a quote AT position i
  // (after processing quotes[0..i])
  uint64_t inside_quote = find_quote_mask2(quotes, prev_quote_state);

  // Field separators are delimiters/newlines that are NOT inside quotes
  uint64_t field_seps = (delimiters | newlines) & ~inside_quote & valid_mask;

  // Detect potential quote errors:
  // A quote is valid if:
  //   - It starts a field (after delimiter, newline, or at start of block)
  //   - It was already inside a quoted field BEFORE this quote (content or closing)
  //
  // The key insight: inside_quote[i] tells us state AFTER processing position i.
  // To check if we were inside BEFORE a quote at position i, we need:
  //   - For i > 0: inside_quote[i-1]
  //   - For i == 0: was_inside_quote (from previous block)
  //
  // inside_quote_before[i] = (inside_quote >> 1) | (was_inside_quote ? 0x8000...0 : 0)
  // But this doesn't work because shifts lose information.
  //
  // Alternative approach: check what's at the position BEFORE the quote.
  // A quote is valid if immediately preceded by:
  //   - Start of buffer (position 0 and not continuing from prev block inside quote)
  //   - A field separator (comma or newline)
  //   - Another quote (escaped or closing)
  //   - Any character if we're inside a quoted field
  //
  // Simplest check: the character before the quote must be a field separator,
  // another quote, or the quote must be at position 0 (start of block with fresh state).

  // Get positions right before each quote
  // quotes_shifted_right[i] = quotes[i+1] (positions before quotes)
  // If quotes has bit i set, then position i-1 is right before a quote
  uint64_t before_quotes = (quotes >> 1);

  // Valid positions for quotes:
  // 1. Position 0 if we weren't inside a quote from previous block
  uint64_t pos0_valid = (was_inside_quote == 0) ? 1ULL : 0ULL;

  // 2. Immediately after field separators (position after sep, before current quote)
  //    If field_seps[i] is set, then position i+1 is a valid quote position
  uint64_t after_seps = (field_seps << 1) & valid_mask;

  // 3. Immediately after another quote (for escaped quotes "" or closing then opening)
  //    This is handled by the inside_quote check below

  // 4. Inside an already-quoted field (check inside_quote at position BEFORE the quote)
  //    For quote at position i, we need inside_quote[i-1]
  //    Shifting LEFT: (x << 1)[i] = x[i-1] for i > 0
  //    inside_before[0] = was_inside_quote (which is all 1s if inside, all 0s if outside)
  uint64_t inside_before = (inside_quote << 1) | (was_inside_quote & 1ULL);

  // A quote at position i is valid if:
  //   - i == 0 and pos0_valid
  //   - field_seps[i-1] (right before us is a separator)
  //   - inside_before[i] (we were already inside a quote)
  //   - quotes[i-1] (previous char was also a quote - for "" escapes)
  uint64_t valid_quote_at_pos = (pos0_valid) | after_seps | inside_before | before_quotes;

  // Quote errors: quotes not at valid positions
  quote_error_mask = quotes & ~valid_quote_at_pos;

  // 2. Invalid character after closing quote
  // This requires tracking the previous character state, which is expensive.
  // For now, we only detect this at block boundaries or defer to scalar.
  // The quote_error_mask will catch most cases since an invalid quote sequence
  // like 'abc"def' will have the quote flagged as not at field start.

  // Write separator positions
  return write(indexes, idx, base, stride, field_seps);
}

/**
 * @brief SIMD-accelerated second pass with error collection.
 *
 * Uses SIMD for the main processing loop. Errors are detected using SIMD
 * bitmasks, and only error positions are processed with scalar code.
 *
 * @param sm The branchless state machine
 * @param buf Input buffer
 * @param start Start position
 * @param end End position
 * @param indexes Output array
 * @param thread_id Thread ID for interleaved storage
 * @param n_threads Total number of threads
 * @param errors ErrorCollector to accumulate errors (may be nullptr)
 * @param total_len Total buffer length for bounds checking
 * @return Number of field separators found
 */
uint64_t second_pass_simd_branchless_with_errors(const BranchlessStateMachine& sm,
                                                 const uint8_t* buf, size_t start, size_t end,
                                                 uint64_t* indexes, size_t thread_id,
                                                 size_t n_threads, ErrorCollector* errors,
                                                 size_t total_len);

/**
 * @brief Convert BranchlessError to ErrorCode.
 *
 * Maps the compact branchless error codes to the full ErrorCode enum for
 * compatibility with the error collection framework.
 */
ErrorCode branchless_error_to_error_code(BranchlessError err);

/**
 * @brief Helper to get context around an error position.
 *
 * Returns a string representation of the buffer content near the given position.
 */
std::string get_error_context(const uint8_t* buf, size_t len, size_t pos, size_t context_size = 20);

/**
 * @brief Helper to calculate line and column from byte offset.
 */
void get_error_line_column(const uint8_t* buf, size_t buf_len, size_t offset, size_t& line,
                           size_t& column);

} // namespace libvroom

#endif // LIBVROOM_BRANCHLESS_STATE_MACHINE_H
