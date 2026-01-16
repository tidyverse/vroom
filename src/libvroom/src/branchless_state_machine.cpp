/**
 * @file branchless_state_machine.cpp
 * @brief Implementation of branchless state machine functions.
 *
 * This file contains implementations that were moved from the header to reduce
 * header size and improve compilation times. The performance-critical SIMD inline
 * functions remain in the header.
 *
 * Functions in this file:
 * - second_pass_simd_branchless_with_errors() - SIMD second pass with error collection
 * - get_error_context(), get_error_line_column() - diagnostic utilities
 * - branchless_error_to_error_code() - error code conversion
 */

#include "branchless_state_machine.h"

#include <algorithm>

namespace libvroom {

ErrorCode branchless_error_to_error_code(BranchlessError err) {
  switch (err) {
  case ERR_NONE:
    return ErrorCode::NONE;
  case ERR_QUOTE_IN_UNQUOTED:
    return ErrorCode::QUOTE_IN_UNQUOTED_FIELD;
  case ERR_INVALID_AFTER_QUOTE:
    return ErrorCode::INVALID_QUOTE_ESCAPE;
  default:
    return ErrorCode::INTERNAL_ERROR;
  }
}

std::string get_error_context(const uint8_t* buf, size_t len, size_t pos, size_t context_size) {
  if (len == 0 || buf == nullptr)
    return "";
  size_t safe_pos = pos < len ? pos : len - 1;
  size_t ctx_start = safe_pos > context_size ? safe_pos - context_size : 0;
  size_t ctx_end = std::min(safe_pos + context_size, len);

  std::string ctx;
  ctx.reserve((ctx_end - ctx_start) * 2);

  for (size_t i = ctx_start; i < ctx_end; ++i) {
    char c = static_cast<char>(buf[i]);
    if (c == '\n')
      ctx += "\\n";
    else if (c == '\r')
      ctx += "\\r";
    else if (c == '\0')
      ctx += "\\0";
    else if (c >= 32 && c < 127)
      ctx += c;
    else
      ctx += "?";
  }
  return ctx;
}

void get_error_line_column(const uint8_t* buf, size_t buf_len, size_t offset, size_t& line,
                           size_t& column) {
  line = 1;
  column = 1;
  size_t safe_offset = offset < buf_len ? offset : buf_len;
  for (size_t i = 0; i < safe_offset; ++i) {
    if (buf[i] == '\n') {
      ++line;
      column = 1;
    } else if (buf[i] != '\r') {
      ++column;
    }
  }
}

uint64_t second_pass_simd_branchless_with_errors(const BranchlessStateMachine& sm,
                                                 const uint8_t* buf, size_t start, size_t end,
                                                 uint64_t* indexes, size_t /*thread_id*/,
                                                 size_t /*n_threads*/, ErrorCollector* errors,
                                                 size_t total_len) {
  assert(end >= start && "Invalid range: end must be >= start");
  size_t len = end - start;
  size_t pos = 0;
  uint64_t idx = 0;
  uint64_t prev_quote_state = 0ULL;
  uint64_t prev_escape_carry = 0ULL;
  uint64_t count = 0;
  const uint8_t* data = buf + start;

  // Track state for unclosed quote detection at end
  bool ends_inside_quote = false;

  // Use effective buffer length for bounds checking
  size_t buf_len = total_len > 0 ? total_len : end;
  char quote_char = sm.quote_char();

  // Process 64-byte blocks with SIMD
  // Caller passes per-thread base pointer; writes are contiguous within each thread's region.
  for (; pos + 64 <= len; pos += 64) {
    libvroom_prefetch(data + pos + 128);

    simd_input in = fill_input(data + pos);

    uint64_t null_byte_mask = 0;
    uint64_t quote_error_mask = 0;

    count += process_block_simd_branchless_with_errors(sm, in, 64, prev_quote_state,
                                                       prev_escape_carry, indexes, start + pos, idx,
                                                       1, null_byte_mask, quote_error_mask);

    // Report errors for this block (only if there are any)
    if ((null_byte_mask != 0 || quote_error_mask != 0) && errors) {
      // Process null bytes
      while (null_byte_mask != 0) {
        int bit_pos = trailing_zeroes(null_byte_mask);
        size_t error_pos = start + pos + static_cast<size_t>(bit_pos);
        size_t line, col;
        get_error_line_column(buf, buf_len, error_pos, line, col);
        errors->add_error(ErrorCode::NULL_BYTE, ErrorSeverity::RECOVERABLE, line, col, error_pos,
                          "Null byte in data", get_error_context(buf, buf_len, error_pos));
        if (errors->should_stop())
          return count;
        null_byte_mask = clear_lowest_bit(null_byte_mask);
      }

      // Process quote errors
      while (quote_error_mask != 0) {
        int bit_pos = trailing_zeroes(quote_error_mask);
        size_t error_pos = start + pos + static_cast<size_t>(bit_pos);
        size_t line, col;
        get_error_line_column(buf, buf_len, error_pos, line, col);
        std::string msg = "Quote character '";
        msg += quote_char;
        msg += "' in unquoted field";
        errors->add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE, line, col,
                          error_pos, msg, get_error_context(buf, buf_len, error_pos));
        if (errors->should_stop())
          return count;
        quote_error_mask = clear_lowest_bit(quote_error_mask);
      }
    }

    // Track if we're inside a quote at end of this block
    ends_inside_quote = (prev_quote_state != 0);
  }

  // Handle remaining bytes (< 64)
  if (pos < len) {
    simd_input in = fill_input(data + pos);

    uint64_t null_byte_mask = 0;
    uint64_t quote_error_mask = 0;

    count += process_block_simd_branchless_with_errors(sm, in, len - pos, prev_quote_state,
                                                       prev_escape_carry, indexes, start + pos, idx,
                                                       1, null_byte_mask, quote_error_mask);

    // Report errors for this block
    if ((null_byte_mask != 0 || quote_error_mask != 0) && errors) {
      while (null_byte_mask != 0) {
        int bit_pos = trailing_zeroes(null_byte_mask);
        size_t error_pos = start + pos + static_cast<size_t>(bit_pos);
        size_t line, col;
        get_error_line_column(buf, buf_len, error_pos, line, col);
        errors->add_error(ErrorCode::NULL_BYTE, ErrorSeverity::RECOVERABLE, line, col, error_pos,
                          "Null byte in data", get_error_context(buf, buf_len, error_pos));
        if (errors->should_stop())
          return count;
        null_byte_mask = clear_lowest_bit(null_byte_mask);
      }

      while (quote_error_mask != 0) {
        int bit_pos = trailing_zeroes(quote_error_mask);
        size_t error_pos = start + pos + static_cast<size_t>(bit_pos);
        size_t line, col;
        get_error_line_column(buf, buf_len, error_pos, line, col);
        std::string msg = "Quote character '";
        msg += quote_char;
        msg += "' in unquoted field";
        errors->add_error(ErrorCode::QUOTE_IN_UNQUOTED_FIELD, ErrorSeverity::RECOVERABLE, line, col,
                          error_pos, msg, get_error_context(buf, buf_len, error_pos));
        if (errors->should_stop())
          return count;
        quote_error_mask = clear_lowest_bit(quote_error_mask);
      }
    }

    ends_inside_quote = (prev_quote_state != 0);
  }

  // Check for unclosed quote at end of chunk
  if (ends_inside_quote && errors && end == buf_len) {
    size_t line, col;
    size_t error_pos = end > 0 ? end - 1 : 0;
    get_error_line_column(buf, buf_len, error_pos, line, col);
    std::string msg = "Unclosed quote '";
    msg += quote_char;
    msg += "' at end of file";
    errors->add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, line, col, end, msg,
                      get_error_context(buf, buf_len, error_pos > 20 ? error_pos - 20 : 0));
  }

  return count;
}

} // namespace libvroom
