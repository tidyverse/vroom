#pragma once

#include <charconv>
#include <cstdint>
#include <fast_float/fast_float.h>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace libvroom {

// Forward declarations from type_parsers.cpp
bool parse_date(std::string_view value, int32_t& days_since_epoch);
bool parse_timestamp(std::string_view value, int64_t& micros_since_epoch);

// FastColumnContext - devirtualized column appending for the hot path
// Uses function pointers resolved once at setup time to avoid virtual dispatch
class FastColumnContext {
public:
  // Context data - type-specific pointers
  union {
    std::vector<std::string>* string_values;
    std::vector<int32_t>* int32_values;
    std::vector<int64_t>* int64_values;
    std::vector<double>* float64_values;
    std::vector<bool>* bool_values;
  };
  std::vector<bool>* null_bitmap;

  // Function pointers for parsing operations
  using AppendFn = void (*)(FastColumnContext& ctx, std::string_view value);
  using AppendNullFn = void (*)(FastColumnContext& ctx);

  AppendFn append_fn;
  AppendNullFn append_null_fn;

  // Static inline append implementations - these get inlined and avoid virtual dispatch

  // String
  static void append_string(FastColumnContext& ctx, std::string_view value) {
    ctx.string_values->emplace_back(value);
    ctx.null_bitmap->push_back(false);
  }
  static void append_null_string(FastColumnContext& ctx) {
    ctx.string_values->emplace_back();
    ctx.null_bitmap->push_back(true);
  }

  // Fast integer parsing - handles common cases inline
  // Returns true if parsing succeeded, result in 'out'
  static inline bool parse_int32_fast(const char* p, size_t len, int32_t& out) {
    if (len == 0 || len > 11)
      return false; // Too long for int32

    const char* end = p + len;
    bool negative = false;

    if (*p == '-') {
      negative = true;
      p++;
      len--;
      if (len == 0)
        return false;
    } else if (*p == '+') {
      p++;
      len--;
      if (len == 0)
        return false;
    }

    // Fast path for small positive integers (up to 9 digits)
    if (len <= 9) {
      uint32_t result = 0;
      while (p < end) {
        unsigned char c = static_cast<unsigned char>(*p) - '0';
        if (c > 9)
          return false;
        result = result * 10 + c;
        p++;
      }
      out = negative ? -static_cast<int32_t>(result) : static_cast<int32_t>(result);
      return true;
    }

    // For 10+ digits, use std::from_chars for overflow checking
    auto [ptr, ec] = std::from_chars(p, end, out);
    if (ec != std::errc() || ptr != end)
      return false;
    if (negative)
      out = -out;
    return true;
  }

  // Int32
  // Note: empty strings are handled as nulls by caller, so we don't check here
  static void append_int32(FastColumnContext& ctx, std::string_view value) {
    int32_t result;
    if (parse_int32_fast(value.data(), value.size(), result)) {
      ctx.int32_values->push_back(result);
      ctx.null_bitmap->push_back(false);
    } else {
      ctx.int32_values->push_back(0);
      ctx.null_bitmap->push_back(true);
    }
  }
  static void append_null_int32(FastColumnContext& ctx) {
    ctx.int32_values->push_back(0);
    ctx.null_bitmap->push_back(true);
  }

  // Fast int64 parsing - handles common cases inline
  static inline bool parse_int64_fast(const char* p, size_t len, int64_t& out) {
    if (len == 0 || len > 20)
      return false; // Too long for int64

    const char* end = p + len;
    bool negative = false;

    if (*p == '-') {
      negative = true;
      p++;
      len--;
      if (len == 0)
        return false;
    } else if (*p == '+') {
      p++;
      len--;
      if (len == 0)
        return false;
    }

    // Fast path for integers up to 18 digits (safe from overflow)
    if (len <= 18) {
      uint64_t result = 0;
      while (p < end) {
        unsigned char c = static_cast<unsigned char>(*p) - '0';
        if (c > 9)
          return false;
        result = result * 10 + c;
        p++;
      }
      out = negative ? -static_cast<int64_t>(result) : static_cast<int64_t>(result);
      return true;
    }

    // For 19+ digits, use std::from_chars for overflow checking
    auto [ptr, ec] = std::from_chars(p, end, out);
    if (ec != std::errc() || ptr != end)
      return false;
    if (negative)
      out = -out;
    return true;
  }

  // Int64
  // Note: empty strings are handled as nulls by caller, so we don't check here
  static void append_int64(FastColumnContext& ctx, std::string_view value) {
    int64_t result;
    if (parse_int64_fast(value.data(), value.size(), result)) {
      ctx.int64_values->push_back(result);
      ctx.null_bitmap->push_back(false);
    } else {
      ctx.int64_values->push_back(0);
      ctx.null_bitmap->push_back(true);
    }
  }
  static void append_null_int64(FastColumnContext& ctx) {
    ctx.int64_values->push_back(0);
    ctx.null_bitmap->push_back(true);
  }

  // Float64
  // Note: empty strings are handled as nulls by caller, so we don't check here
  static void append_float64(FastColumnContext& ctx, std::string_view value) {
    double result;
    auto [ptr, ec] = fast_float::from_chars(value.data(), value.data() + value.size(), result);
    if (ec == std::errc() && ptr == value.data() + value.size()) {
      ctx.float64_values->push_back(result);
      ctx.null_bitmap->push_back(false);
    } else {
      ctx.float64_values->push_back(std::numeric_limits<double>::quiet_NaN());
      ctx.null_bitmap->push_back(true);
    }
  }
  static void append_null_float64(FastColumnContext& ctx) {
    ctx.float64_values->push_back(std::numeric_limits<double>::quiet_NaN());
    ctx.null_bitmap->push_back(true);
  }

  // Bool
  // Note: empty strings are handled as nulls by caller, so we don't check here
  static void append_bool(FastColumnContext& ctx, std::string_view value) {
    // Check for common true values
    if (value == "true" || value == "TRUE" || value == "True" || value == "1" || value == "yes" ||
        value == "YES") {
      ctx.bool_values->push_back(true);
      ctx.null_bitmap->push_back(false);
      return;
    }
    // Check for common false values
    if (value == "false" || value == "FALSE" || value == "False" || value == "0" || value == "no" ||
        value == "NO") {
      ctx.bool_values->push_back(false);
      ctx.null_bitmap->push_back(false);
      return;
    }
    // Unknown value - treat as null
    ctx.bool_values->push_back(false);
    ctx.null_bitmap->push_back(true);
  }
  static void append_null_bool(FastColumnContext& ctx) {
    ctx.bool_values->push_back(false);
    ctx.null_bitmap->push_back(true);
  }

  // Date (stores days since epoch as int32)
  static void append_date(FastColumnContext& ctx, std::string_view value) {
    if (value.empty()) {
      ctx.int32_values->push_back(0);
      ctx.null_bitmap->push_back(true);
      return;
    }
    int32_t days;
    if (parse_date(value, days)) {
      ctx.int32_values->push_back(days);
      ctx.null_bitmap->push_back(false);
    } else {
      ctx.int32_values->push_back(0);
      ctx.null_bitmap->push_back(true);
    }
  }
  static void append_null_date(FastColumnContext& ctx) {
    ctx.int32_values->push_back(0);
    ctx.null_bitmap->push_back(true);
  }

  // Timestamp (stores microseconds since epoch as int64)
  static void append_timestamp(FastColumnContext& ctx, std::string_view value) {
    if (value.empty()) {
      ctx.int64_values->push_back(0);
      ctx.null_bitmap->push_back(true);
      return;
    }
    int64_t micros;
    if (parse_timestamp(value, micros)) {
      ctx.int64_values->push_back(micros);
      ctx.null_bitmap->push_back(false);
    } else {
      ctx.int64_values->push_back(0);
      ctx.null_bitmap->push_back(true);
    }
  }
  static void append_null_timestamp(FastColumnContext& ctx) {
    ctx.int64_values->push_back(0);
    ctx.null_bitmap->push_back(true);
  }

  // Inline append/append_null that dispatch through function pointer
  inline void append(std::string_view value) { append_fn(*this, value); }

  inline void append_null() { append_null_fn(*this); }
};

} // namespace libvroom
