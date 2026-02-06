/**
 * @file common_defs.h
 * @brief Common definitions and compiler macros for libvroom.
 *
 * Provides platform-independent macros for SIMD operations, memory alignment,
 * and compiler hints.
 */

#ifndef LIBVROOM_COMMON_DEFS_H
#define LIBVROOM_COMMON_DEFS_H

#include <cassert>

// The input buffer must be readable up to buf + LIBVROOM_PADDING.
// This must be at least 64 bytes since SIMD operations load 64-byte blocks
// and may read past the logical end of the data (masked results are discarded).
#define LIBVROOM_PADDING 64

// Align to N-byte boundary
#define ROUNDUP_N(a, n) (((a) + ((n) - 1)) & ~((n) - 1))
#define ROUNDDOWN_N(a, n) ((a) & ~((n) - 1))

#define ISALIGNED_N(ptr, n) (((uintptr_t)(ptr) & ((n) - 1)) == 0)

#ifdef _MSC_VER

// Prevent Windows.h from defining min/max macros that conflict with std::numeric_limits
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <intrin.h>

#define really_inline inline
#define never_inline __declspec(noinline)

#define UNUSED
#define WARN_UNUSED

#ifndef likely
#define likely(x) x
#endif
#ifndef unlikely
#define unlikely(x) x
#endif

// MSVC uses _mm_prefetch with _MM_HINT_T0 for prefetch
// Note: This requires SSE, which is available on all x64 Windows
#define libvroom_prefetch(addr) _mm_prefetch(reinterpret_cast<const char*>(addr), _MM_HINT_T0)

#else

#define really_inline inline __attribute__((always_inline, unused))
#define never_inline inline __attribute__((noinline, unused))

#define UNUSED __attribute__((unused))
#define WARN_UNUSED __attribute__((warn_unused_result))

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

// GCC/Clang prefetch intrinsic
#define libvroom_prefetch(addr) __builtin_prefetch(addr)

#endif // MSC_VER

#endif // LIBVROOM_COMMON_DEFS_H
