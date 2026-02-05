// Highway SIMD implementation for statistics computation
// Using Highway's foreach_target pattern for multi-architecture support

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "simd/statistics_simd.cpp"
#include <hwy/foreach_target.h>
#include <hwy/highway.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>

HWY_BEFORE_NAMESPACE();
namespace libvroom {
namespace simd {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// SIMD min/max computation for int32_t
void ComputeMinMaxInt32(const int32_t* data, size_t count, int32_t& min_out, int32_t& max_out) {
  if (count == 0) {
    min_out = std::numeric_limits<int32_t>::max();
    max_out = std::numeric_limits<int32_t>::min();
    return;
  }

  const hn::ScalableTag<int32_t> d;
  const size_t N = hn::Lanes(d);

  // Initialize with first element
  auto vmin = hn::Set(d, data[0]);
  auto vmax = vmin;

  size_t i = 0;
  // Process full vectors
  for (; i + N <= count; i += N) {
    const auto v = hn::LoadU(d, data + i);
    vmin = hn::Min(vmin, v);
    vmax = hn::Max(vmax, v);
  }

  // Reduce vectors to scalars
  min_out = hn::ReduceMin(d, vmin);
  max_out = hn::ReduceMax(d, vmax);

  // Handle remaining elements
  for (; i < count; ++i) {
    min_out = std::min(min_out, data[i]);
    max_out = std::max(max_out, data[i]);
  }
}

// SIMD min/max computation for int64_t
void ComputeMinMaxInt64(const int64_t* data, size_t count, int64_t& min_out, int64_t& max_out) {
  if (count == 0) {
    min_out = std::numeric_limits<int64_t>::max();
    max_out = std::numeric_limits<int64_t>::min();
    return;
  }

  const hn::ScalableTag<int64_t> d;
  const size_t N = hn::Lanes(d);

  // Initialize with first element
  auto vmin = hn::Set(d, data[0]);
  auto vmax = vmin;

  size_t i = 0;
  // Process full vectors
  for (; i + N <= count; i += N) {
    const auto v = hn::LoadU(d, data + i);
    vmin = hn::Min(vmin, v);
    vmax = hn::Max(vmax, v);
  }

  // Reduce vectors to scalars
  min_out = hn::ReduceMin(d, vmin);
  max_out = hn::ReduceMax(d, vmax);

  // Handle remaining elements
  for (; i < count; ++i) {
    min_out = std::min(min_out, data[i]);
    max_out = std::max(max_out, data[i]);
  }
}

// SIMD min/max computation for double (with NaN handling)
void ComputeMinMaxFloat64(const double* data, size_t count, double& min_out, double& max_out) {
  if (count == 0) {
    min_out = std::numeric_limits<double>::quiet_NaN();
    max_out = std::numeric_limits<double>::quiet_NaN();
    return;
  }

  // Find first non-NaN value for initialization
  size_t first_valid = 0;
  for (; first_valid < count; ++first_valid) {
    if (!std::isnan(data[first_valid]))
      break;
  }

  if (first_valid >= count) {
    // All NaN
    min_out = std::numeric_limits<double>::quiet_NaN();
    max_out = std::numeric_limits<double>::quiet_NaN();
    return;
  }

  const hn::ScalableTag<double> d;
  const size_t N = hn::Lanes(d);

  // Initialize with first valid value
  auto vmin = hn::Set(d, data[first_valid]);
  auto vmax = vmin;

  size_t i = first_valid + 1;

  // Process full vectors
  // Note: Highway's Min/Max propagate NaN, so we need to filter later
  for (; i + N <= count; i += N) {
    const auto v = hn::LoadU(d, data + i);
    vmin = hn::Min(vmin, v);
    vmax = hn::Max(vmax, v);
  }

  // Reduce vectors to scalars
  min_out = hn::ReduceMin(d, vmin);
  max_out = hn::ReduceMax(d, vmax);

  // Handle remaining elements
  for (; i < count; ++i) {
    if (!std::isnan(data[i])) {
      min_out = std::min(min_out, data[i]);
      max_out = std::max(max_out, data[i]);
    }
  }

  // If NaN was picked up during SIMD processing, fall back to scalar
  if (std::isnan(min_out) || std::isnan(max_out)) {
    bool found = false;
    for (size_t j = 0; j < count; ++j) {
      if (!std::isnan(data[j])) {
        if (!found) {
          min_out = max_out = data[j];
          found = true;
        } else {
          min_out = std::min(min_out, data[j]);
          max_out = std::max(max_out, data[j]);
        }
      }
    }
  }
}

} // namespace HWY_NAMESPACE
} // namespace simd
} // namespace libvroom
HWY_AFTER_NAMESPACE();

#if HWY_ONCE

namespace libvroom {
namespace simd {

// Export the best available implementation
HWY_EXPORT(ComputeMinMaxInt32);
HWY_EXPORT(ComputeMinMaxInt64);
HWY_EXPORT(ComputeMinMaxFloat64);

// Public interface functions
void compute_minmax_int32(const int32_t* data, size_t count, int32_t& min_out, int32_t& max_out) {
  HWY_DYNAMIC_DISPATCH(ComputeMinMaxInt32)(data, count, min_out, max_out);
}

void compute_minmax_int64(const int64_t* data, size_t count, int64_t& min_out, int64_t& max_out) {
  HWY_DYNAMIC_DISPATCH(ComputeMinMaxInt64)(data, count, min_out, max_out);
}

void compute_minmax_float64(const double* data, size_t count, double& min_out, double& max_out) {
  HWY_DYNAMIC_DISPATCH(ComputeMinMaxFloat64)(data, count, min_out, max_out);
}

} // namespace simd
} // namespace libvroom

#endif // HWY_ONCE
