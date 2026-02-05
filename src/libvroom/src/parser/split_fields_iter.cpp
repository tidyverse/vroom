// SIMD SplitFields iterator using Google Highway.
//
// This file uses Highway's dynamic dispatch to select the optimal
// implementation at runtime based on CPU capabilities.

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "parser/split_fields_iter-inl.h"
#include "hwy/foreach_target.h"
#include "parser/split_fields_iter-inl.h"

// Generate dispatch tables and public API (only once)
#if HWY_ONCE

namespace libvroom {

// Export implementations for dynamic dispatch
HWY_EXPORT(ScanForCharImpl);
HWY_EXPORT(ScanForTwoCharsImpl);

// Public API functions
uint64_t scan_for_char_simd(const char* data, size_t len, char c) {
  return HWY_DYNAMIC_DISPATCH(ScanForCharImpl)(data, len, c);
}

uint64_t scan_for_two_chars_simd(const char* data, size_t len, char c1, char c2) {
  return HWY_DYNAMIC_DISPATCH(ScanForTwoCharsImpl)(data, len, c1, c2);
}

} // namespace libvroom

#endif // HWY_ONCE
