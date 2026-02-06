// SIMD integer parsing using Google Highway.
//
// This file uses Highway's dynamic dispatch to select the optimal
// implementation at runtime based on CPU capabilities.

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "parser/simd_atoi-inl.h"
#include "hwy/foreach_target.h"
#include "parser/simd_atoi-inl.h"

// Generate dispatch tables and public API (only once)
#if HWY_ONCE

#include "libvroom/simd_atoi.h"

namespace libvroom {
namespace simd {

// Export implementations for dynamic dispatch
HWY_EXPORT(ParseUint64SimdImpl);
HWY_EXPORT(ParseInt32SimdImpl);
HWY_EXPORT(ParseInt64SimdImpl);

// Public API functions
bool parse_uint64_highway(const char* p, size_t len, uint64_t& out) {
  return HWY_DYNAMIC_DISPATCH(ParseUint64SimdImpl)(p, len, out);
}

bool parse_int32_highway(const char* p, size_t len, int32_t& out) {
  return HWY_DYNAMIC_DISPATCH(ParseInt32SimdImpl)(p, len, out);
}

bool parse_int64_highway(const char* p, size_t len, int64_t& out) {
  return HWY_DYNAMIC_DISPATCH(ParseInt64SimdImpl)(p, len, out);
}

} // namespace simd
} // namespace libvroom

#endif // HWY_ONCE
