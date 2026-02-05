#pragma once

#include "arrow_buffer.h"
#include "simd_atoi.h"

#include <charconv>
#include <fast_float/fast_float.h>
#include <limits>
#include <string_view>

namespace libvroom {

// Forward declarations
bool parse_date(std::string_view value, int32_t& days_since_epoch);
bool parse_timestamp(std::string_view value, int64_t& micros_since_epoch);

// FastArrowContext - uses Arrow-style buffers for zero-copy batch operations
// Key differences from FastColumnContext:
// 1. Uses packed NullBitmap instead of std::vector<bool>
// 2. Uses StringBuffer (data + offsets) instead of std::vector<std::string>
// 3. All appends are to contiguous memory
class FastArrowContext {
public:
  // Type-specific storage pointers (only one is active at a time)
  union {
    StringBuffer* string_buffer;
    NumericBuffer<int32_t>* int32_buffer;
    NumericBuffer<int64_t>* int64_buffer;
    NumericBuffer<double>* float64_buffer;
    NumericBuffer<uint8_t>* bool_buffer;
  };
  NullBitmap* null_bitmap;

  // Function pointers for type-specific operations
  using AppendFn = void (*)(FastArrowContext& ctx, std::string_view value);
  using AppendNullFn = void (*)(FastArrowContext& ctx);

  AppendFn append_fn;
  AppendNullFn append_null_fn;

  // ============================================
  // Static append implementations
  // ============================================

  // String - zero-copy append to contiguous buffer
  static void append_string(FastArrowContext& ctx, std::string_view value) {
    ctx.string_buffer->push_back(value);
    ctx.null_bitmap->push_back_valid();
  }
  static void append_null_string(FastArrowContext& ctx) {
    ctx.string_buffer->push_back_empty();
    ctx.null_bitmap->push_back_null();
  }

  // Int32 parsing
  static inline bool parse_int32_fast(const char* p, size_t len, int32_t& out) {
    if (len == 0 || len > 11)
      return false;

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

    auto [ptr, ec] = std::from_chars(p, end, out);
    if (ec != std::errc() || ptr != end)
      return false;
    if (negative)
      out = -out;
    return true;
  }

  VROOM_FORCE_INLINE static void append_int32(FastArrowContext& ctx, std::string_view value) {
    int32_t result;
    if (VROOM_LIKELY(simd::parse_int32_simd(value.data(), value.size(), result))) {
      ctx.int32_buffer->push_back(result);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.int32_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
    }
  }
  static void append_null_int32(FastArrowContext& ctx) {
    ctx.int32_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }

  // Int64 parsing
  static inline bool parse_int64_fast(const char* p, size_t len, int64_t& out) {
    if (len == 0 || len > 20)
      return false;

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

    auto [ptr, ec] = std::from_chars(p, end, out);
    if (ec != std::errc() || ptr != end)
      return false;
    if (negative)
      out = -out;
    return true;
  }

  static void append_int64(FastArrowContext& ctx, std::string_view value) {
    int64_t result;
    if (simd::parse_int64_simd(value.data(), value.size(), result)) {
      ctx.int64_buffer->push_back(result);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.int64_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
    }
  }
  static void append_null_int64(FastArrowContext& ctx) {
    ctx.int64_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }

  // Float64
  static void append_float64(FastArrowContext& ctx, std::string_view value) {
    double result;
    auto [ptr, ec] = fast_float::from_chars(value.data(), value.data() + value.size(), result);
    if (ec == std::errc() && ptr == value.data() + value.size()) {
      ctx.float64_buffer->push_back(result);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.float64_buffer->push_back(std::numeric_limits<double>::quiet_NaN());
      ctx.null_bitmap->push_back_null();
    }
  }
  static void append_null_float64(FastArrowContext& ctx) {
    ctx.float64_buffer->push_back(std::numeric_limits<double>::quiet_NaN());
    ctx.null_bitmap->push_back_null();
  }

  // Bool
  static void append_bool(FastArrowContext& ctx, std::string_view value) {
    if (value == "true" || value == "TRUE" || value == "True" || value == "1" || value == "yes" ||
        value == "YES") {
      ctx.bool_buffer->push_back(1);
      ctx.null_bitmap->push_back_valid();
      return;
    }
    if (value == "false" || value == "FALSE" || value == "False" || value == "0" || value == "no" ||
        value == "NO") {
      ctx.bool_buffer->push_back(0);
      ctx.null_bitmap->push_back_valid();
      return;
    }
    ctx.bool_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }
  static void append_null_bool(FastArrowContext& ctx) {
    ctx.bool_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }

  // Date
  static void append_date(FastArrowContext& ctx, std::string_view value) {
    if (value.empty()) {
      ctx.int32_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
      return;
    }
    int32_t days;
    if (parse_date(value, days)) {
      ctx.int32_buffer->push_back(days);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.int32_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
    }
  }
  static void append_null_date(FastArrowContext& ctx) {
    ctx.int32_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }

  // Timestamp
  static void append_timestamp(FastArrowContext& ctx, std::string_view value) {
    if (value.empty()) {
      ctx.int64_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
      return;
    }
    int64_t micros;
    if (parse_timestamp(value, micros)) {
      ctx.int64_buffer->push_back(micros);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.int64_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
    }
  }
  static void append_null_timestamp(FastArrowContext& ctx) {
    ctx.int64_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }

  // Dispatch methods
  inline void append(std::string_view value) { append_fn(*this, value); }

  inline void append_null() { append_null_fn(*this); }
};

} // namespace libvroom
