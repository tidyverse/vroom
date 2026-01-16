/**
 * @file debug_parser.h
 * @brief Debug-enabled parsing methods for the libvroom CSV parser.
 */

#ifndef LIBVROOM_DEBUG_PARSER_H
#define LIBVROOM_DEBUG_PARSER_H

#include "debug.h"
#include "two_pass.h"

#include <string>

namespace libvroom {

inline const char* get_simd_path_name() {
#if defined(__AVX512F__)
  return "AVX512";
#elif defined(__AVX2__)
  return "AVX2";
#elif defined(__SSE4_2__)
  return "SSE4.2";
#elif defined(__ARM_NEON)
  return "NEON";
#else
  return "Scalar";
#endif
}

inline size_t get_simd_vector_bytes() {
  namespace hn = hwy::HWY_NAMESPACE;
  const hn::ScalableTag<uint8_t> d;
  return hn::Lanes(d);
}

inline std::string get_simd_info() {
  std::string info = get_simd_path_name();
  info += " (";
  info += std::to_string(get_simd_vector_bytes());
  info += "-byte vectors)";
  return info;
}

class debug_parser {
public:
  debug_parser() = default;

  ParseIndex init(size_t len, size_t n_threads) { return parser_.init(len, n_threads); }

  bool parse_debug(const uint8_t* buf, ParseIndex& out, size_t len, DebugTrace& trace,
                   const Dialect& dialect = Dialect::csv()) {
    trace.log("Starting parse: %zu bytes, %u threads", len, out.n_threads);
    trace.log_threading(out.n_threads, len / (out.n_threads > 0 ? out.n_threads : 1));
    trace.log_dialect(dialect.delimiter, dialect.quote_char, 1.0);
    trace.log_simd_path(get_simd_path_name(), get_simd_vector_bytes());

    if (trace.dump_masks()) {
      trace.dump_buffer("input (start)", buf, len < 64 ? len : 64, 0);
    }

    trace.start_phase("parse");
    bool result = parser_.parse(buf, out, len, dialect);
    trace.end_phase(len);

    if (trace.dump_masks() && result) {
      // Calculate safe total_size: max index accessed + 1
      // For strided layout: max_idx = thread_id + (count-1) * stride
      size_t max_count = 0;
      for (uint16_t t = 0; t < out.n_threads; ++t) {
        if (out.n_indexes[t] > max_count)
          max_count = out.n_indexes[t];
      }
      size_t total_size = max_count * out.n_threads;
      for (uint16_t t = 0; t < out.n_threads; ++t) {
        if (out.n_indexes[t] > 0) {
          trace.dump_indexes(out.indexes, out.n_indexes[t], t, out.n_threads, total_size);
        }
      }
    }

    trace.print_timing_summary();
    return result;
  }

  bool parse_with_errors_debug(const uint8_t* buf, ParseIndex& out, size_t len,
                               ErrorCollector& errors, DebugTrace& trace,
                               const Dialect& dialect = Dialect::csv()) {
    trace.log("Starting parse_with_errors: %zu bytes", len);
    trace.log_dialect(dialect.delimiter, dialect.quote_char, 1.0);
    trace.log_simd_path(get_simd_path_name(), get_simd_vector_bytes());

    if (trace.dump_masks()) {
      trace.dump_buffer("input (start)", buf, len < 64 ? len : 64, 0);
    }

    trace.start_phase("parse_with_errors");
    bool result = parser_.parse_with_errors(buf, out, len, errors, dialect);
    trace.end_phase(len);

    if (trace.dump_masks()) {
      // Calculate safe total_size for strided layout
      size_t max_count = 0;
      for (uint16_t t = 0; t < out.n_threads; ++t) {
        if (out.n_indexes[t] > max_count)
          max_count = out.n_indexes[t];
      }
      size_t total_size = max_count * out.n_threads;
      trace.dump_indexes(out.indexes, out.n_indexes[0], 0, out.n_threads, total_size);
    }

    trace.log("Parse complete: %zu errors, %s", errors.error_count(),
              errors.has_fatal_errors() ? "has fatal errors" : "no fatal errors");

    trace.print_timing_summary();
    return result;
  }

  bool parse(const uint8_t* buf, ParseIndex& out, size_t len,
             const Dialect& dialect = Dialect::csv()) {
    return parser_.parse(buf, out, len, dialect);
  }

  bool parse_with_errors(const uint8_t* buf, ParseIndex& out, size_t len, ErrorCollector& errors,
                         const Dialect& dialect = Dialect::csv()) {
    return parser_.parse_with_errors(buf, out, len, errors, dialect);
  }

private:
  TwoPass parser_;
};

} // namespace libvroom

#endif // LIBVROOM_DEBUG_PARSER_H
