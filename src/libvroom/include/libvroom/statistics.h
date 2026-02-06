#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace libvroom {

// Forward declaration for SIMD batch operations
namespace simd {
void compute_minmax_int32(const int32_t* data, size_t count, int32_t& min_out, int32_t& max_out);
void compute_minmax_int64(const int64_t* data, size_t count, int64_t& min_out, int64_t& max_out);
void compute_minmax_float64(const double* data, size_t count, double& min_out, double& max_out);
} // namespace simd

// Template-based statistics tracker for incremental computation
template <typename T> class Statistics {
public:
  Statistics() = default;

  // Update with a single non-null value
  void update(const T& value) {
    if (!has_value_) {
      min_ = max_ = value;
      has_value_ = true;
    } else {
      if (value < min_)
        min_ = value;
      if (value > max_)
        max_ = value;
    }
    ++value_count_;
  }

  // Update with a null value
  void update_null() { ++null_count_; }

  // Batch update with array of values (no nulls) - scalar fallback
  void update_batch(const T* values, size_t count) {
    if (count == 0)
      return;
    for (size_t i = 0; i < count; ++i) {
      update(values[i]);
    }
  }

  // Batch update with null bitmap (std::vector<bool>: true = null, false = valid)
  void update_batch_with_nulls(const T* values, const std::vector<bool>& null_bitmap,
                               size_t count) {
    if (count == 0)
      return;
    for (size_t i = 0; i < count; ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        update_null();
      } else {
        update(values[i]);
      }
    }
  }

  // Merge with another Statistics object (for reduction across chunks)
  void merge(const Statistics& other) {
    if (other.has_value_) {
      if (!has_value_) {
        min_ = other.min_;
        max_ = other.max_;
        has_value_ = true;
      } else {
        if (other.min_ < min_)
          min_ = other.min_;
        if (other.max_ > max_)
          max_ = other.max_;
      }
    }
    null_count_ += other.null_count_;
    value_count_ += other.value_count_;
  }

  // Getters
  bool has_value() const { return has_value_; }
  const T& min() const { return min_; }
  const T& max() const { return max_; }
  int64_t null_count() const { return null_count_; }
  int64_t value_count() const { return value_count_; }
  int64_t total_count() const { return value_count_ + null_count_; }
  bool has_null() const { return null_count_ > 0; }

  // Reset to initial state
  void reset() {
    has_value_ = false;
    min_ = T{};
    max_ = T{};
    null_count_ = 0;
    value_count_ = 0;
  }

private:
  T min_{};
  T max_{};
  int64_t null_count_ = 0;
  int64_t value_count_ = 0;
  bool has_value_ = false;
};

// Specialization for int32_t with SIMD batch support
template <> class Statistics<int32_t> {
public:
  Statistics() = default;

  void update(int32_t value) {
    if (!has_value_) {
      min_ = max_ = value;
      has_value_ = true;
    } else {
      if (value < min_)
        min_ = value;
      if (value > max_)
        max_ = value;
    }
    ++value_count_;
  }

  void update_null() { ++null_count_; }

  // SIMD-optimized batch update
  void update_batch(const int32_t* values, size_t count) {
    if (count == 0)
      return;

    int32_t batch_min, batch_max;
    simd::compute_minmax_int32(values, count, batch_min, batch_max);

    if (!has_value_) {
      min_ = batch_min;
      max_ = batch_max;
      has_value_ = true;
    } else {
      if (batch_min < min_)
        min_ = batch_min;
      if (batch_max > max_)
        max_ = batch_max;
    }
    value_count_ += static_cast<int64_t>(count);
  }

  void update_batch_with_nulls(const int32_t* values, const std::vector<bool>& null_bitmap,
                               size_t count) {
    if (count == 0)
      return;

    // Extract non-null values for SIMD processing
    std::vector<int32_t> non_null_values;
    non_null_values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        ++null_count_;
      } else {
        non_null_values.push_back(values[i]);
      }
    }

    if (!non_null_values.empty()) {
      update_batch(non_null_values.data(), non_null_values.size());
    }
  }

  void merge(const Statistics& other) {
    if (other.has_value_) {
      if (!has_value_) {
        min_ = other.min_;
        max_ = other.max_;
        has_value_ = true;
      } else {
        if (other.min_ < min_)
          min_ = other.min_;
        if (other.max_ > max_)
          max_ = other.max_;
      }
    }
    null_count_ += other.null_count_;
    value_count_ += other.value_count_;
  }

  bool has_value() const { return has_value_; }
  int32_t min() const { return min_; }
  int32_t max() const { return max_; }
  int64_t null_count() const { return null_count_; }
  int64_t value_count() const { return value_count_; }
  int64_t total_count() const { return value_count_ + null_count_; }
  bool has_null() const { return null_count_ > 0; }

  void reset() {
    has_value_ = false;
    min_ = 0;
    max_ = 0;
    null_count_ = 0;
    value_count_ = 0;
  }

private:
  int32_t min_ = 0;
  int32_t max_ = 0;
  int64_t null_count_ = 0;
  int64_t value_count_ = 0;
  bool has_value_ = false;
};

// Specialization for int64_t with SIMD batch support
template <> class Statistics<int64_t> {
public:
  Statistics() = default;

  void update(int64_t value) {
    if (!has_value_) {
      min_ = max_ = value;
      has_value_ = true;
    } else {
      if (value < min_)
        min_ = value;
      if (value > max_)
        max_ = value;
    }
    ++value_count_;
  }

  void update_null() { ++null_count_; }

  // SIMD-optimized batch update
  void update_batch(const int64_t* values, size_t count) {
    if (count == 0)
      return;

    int64_t batch_min, batch_max;
    simd::compute_minmax_int64(values, count, batch_min, batch_max);

    if (!has_value_) {
      min_ = batch_min;
      max_ = batch_max;
      has_value_ = true;
    } else {
      if (batch_min < min_)
        min_ = batch_min;
      if (batch_max > max_)
        max_ = batch_max;
    }
    value_count_ += static_cast<int64_t>(count);
  }

  void update_batch_with_nulls(const int64_t* values, const std::vector<bool>& null_bitmap,
                               size_t count) {
    if (count == 0)
      return;

    std::vector<int64_t> non_null_values;
    non_null_values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        ++null_count_;
      } else {
        non_null_values.push_back(values[i]);
      }
    }

    if (!non_null_values.empty()) {
      update_batch(non_null_values.data(), non_null_values.size());
    }
  }

  void merge(const Statistics& other) {
    if (other.has_value_) {
      if (!has_value_) {
        min_ = other.min_;
        max_ = other.max_;
        has_value_ = true;
      } else {
        if (other.min_ < min_)
          min_ = other.min_;
        if (other.max_ > max_)
          max_ = other.max_;
      }
    }
    null_count_ += other.null_count_;
    value_count_ += other.value_count_;
  }

  bool has_value() const { return has_value_; }
  int64_t min() const { return min_; }
  int64_t max() const { return max_; }
  int64_t null_count() const { return null_count_; }
  int64_t value_count() const { return value_count_; }
  int64_t total_count() const { return value_count_ + null_count_; }
  bool has_null() const { return null_count_ > 0; }

  void reset() {
    has_value_ = false;
    min_ = 0;
    max_ = 0;
    null_count_ = 0;
    value_count_ = 0;
  }

private:
  int64_t min_ = 0;
  int64_t max_ = 0;
  int64_t null_count_ = 0;
  int64_t value_count_ = 0;
  bool has_value_ = false;
};

// Specialization for double with SIMD batch support and NaN handling
template <> class Statistics<double> {
public:
  Statistics() = default;

  void update(double value) {
    if (std::isnan(value)) {
      update_null();
      return;
    }

    if (!has_value_) {
      min_ = max_ = value;
      has_value_ = true;
    } else {
      if (value < min_)
        min_ = value;
      if (value > max_)
        max_ = value;
    }
    ++value_count_;
  }

  void update_null() { ++null_count_; }

  // SIMD-optimized batch update
  void update_batch(const double* values, size_t count) {
    if (count == 0)
      return;

    double batch_min, batch_max;
    simd::compute_minmax_float64(values, count, batch_min, batch_max);

    // Check if we got valid results (not NaN)
    if (!std::isnan(batch_min)) {
      if (!has_value_) {
        min_ = batch_min;
        max_ = batch_max;
        has_value_ = true;
      } else {
        if (batch_min < min_)
          min_ = batch_min;
        if (batch_max > max_)
          max_ = batch_max;
      }
      value_count_ += static_cast<int64_t>(count);
    }
  }

  void update_batch_with_nulls(const double* values, const std::vector<bool>& null_bitmap,
                               size_t count) {
    if (count == 0)
      return;

    std::vector<double> non_null_values;
    non_null_values.reserve(count);

    for (size_t i = 0; i < count; ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        ++null_count_;
      } else if (!std::isnan(values[i])) {
        non_null_values.push_back(values[i]);
      } else {
        ++null_count_; // Treat NaN as null
      }
    }

    if (!non_null_values.empty()) {
      double batch_min, batch_max;
      simd::compute_minmax_float64(non_null_values.data(), non_null_values.size(), batch_min,
                                   batch_max);

      if (!std::isnan(batch_min)) {
        if (!has_value_) {
          min_ = batch_min;
          max_ = batch_max;
          has_value_ = true;
        } else {
          if (batch_min < min_)
            min_ = batch_min;
          if (batch_max > max_)
            max_ = batch_max;
        }
        value_count_ += static_cast<int64_t>(non_null_values.size());
      }
    }
  }

  void merge(const Statistics& other) {
    if (other.has_value_) {
      if (!has_value_) {
        min_ = other.min_;
        max_ = other.max_;
        has_value_ = true;
      } else {
        if (other.min_ < min_)
          min_ = other.min_;
        if (other.max_ > max_)
          max_ = other.max_;
      }
    }
    null_count_ += other.null_count_;
    value_count_ += other.value_count_;
  }

  bool has_value() const { return has_value_; }
  double min() const { return min_; }
  double max() const { return max_; }
  int64_t null_count() const { return null_count_; }
  int64_t value_count() const { return value_count_; }
  int64_t total_count() const { return value_count_ + null_count_; }
  bool has_null() const { return null_count_ > 0; }

  void reset() {
    has_value_ = false;
    min_ = std::numeric_limits<double>::max();
    max_ = std::numeric_limits<double>::lowest();
    null_count_ = 0;
    value_count_ = 0;
  }

private:
  double min_ = std::numeric_limits<double>::max();
  double max_ = std::numeric_limits<double>::lowest();
  int64_t null_count_ = 0;
  int64_t value_count_ = 0;
  bool has_value_ = false;
};

// Specialization for std::string (no SIMD, lexicographic comparison)
template <> class Statistics<std::string> {
public:
  Statistics() = default;

  void update(const std::string& value) {
    if (!has_value_) {
      min_ = max_ = value;
      has_value_ = true;
    } else {
      if (value < min_)
        min_ = value;
      if (value > max_)
        max_ = value;
    }
    ++value_count_;
  }

  void update(std::string_view value) {
    if (!has_value_) {
      min_ = max_ = std::string(value);
      has_value_ = true;
    } else {
      if (value < min_)
        min_ = std::string(value);
      if (value > max_)
        max_ = std::string(value);
    }
    ++value_count_;
  }

  void update_null() { ++null_count_; }

  void update_batch(const std::string* values, size_t count) {
    for (size_t i = 0; i < count; ++i) {
      update(values[i]);
    }
  }

  void update_batch_with_nulls(const std::string* values, const std::vector<bool>& null_bitmap,
                               size_t count) {
    for (size_t i = 0; i < count; ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        update_null();
      } else {
        update(values[i]);
      }
    }
  }

  void merge(const Statistics& other) {
    if (other.has_value_) {
      if (!has_value_) {
        min_ = other.min_;
        max_ = other.max_;
        has_value_ = true;
      } else {
        if (other.min_ < min_)
          min_ = other.min_;
        if (other.max_ > max_)
          max_ = other.max_;
      }
    }
    null_count_ += other.null_count_;
    value_count_ += other.value_count_;
  }

  bool has_value() const { return has_value_; }
  const std::string& min() const { return min_; }
  const std::string& max() const { return max_; }
  int64_t null_count() const { return null_count_; }
  int64_t value_count() const { return value_count_; }
  int64_t total_count() const { return value_count_ + null_count_; }
  bool has_null() const { return null_count_ > 0; }

  void reset() {
    has_value_ = false;
    min_.clear();
    max_.clear();
    null_count_ = 0;
    value_count_ = 0;
  }

private:
  std::string min_;
  std::string max_;
  int64_t null_count_ = 0;
  int64_t value_count_ = 0;
  bool has_value_ = false;
};

// Specialization for bool
template <> class Statistics<bool> {
public:
  Statistics() = default;

  void update(bool value) {
    if (value) {
      has_true_ = true;
    } else {
      has_false_ = true;
    }
    ++value_count_;
  }

  void update_null() { ++null_count_; }

  void update_batch(const bool* values, size_t count) {
    for (size_t i = 0; i < count; ++i) {
      update(values[i]);
    }
  }

  // Update from std::vector<bool> directly
  void update_batch(const std::vector<bool>& values) {
    for (size_t i = 0; i < values.size(); ++i) {
      update(values[i]);
    }
  }

  void update_batch_with_nulls(const bool* values, const std::vector<bool>& null_bitmap,
                               size_t count) {
    for (size_t i = 0; i < count; ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        update_null();
      } else {
        update(values[i]);
      }
    }
  }

  // Update from std::vector<bool> with null bitmap
  void update_batch_with_nulls(const std::vector<bool>& values,
                               const std::vector<bool>& null_bitmap) {
    for (size_t i = 0; i < values.size(); ++i) {
      if (i < null_bitmap.size() && null_bitmap[i]) {
        update_null();
      } else {
        update(values[i]);
      }
    }
  }

  void merge(const Statistics& other) {
    has_true_ = has_true_ || other.has_true_;
    has_false_ = has_false_ || other.has_false_;
    null_count_ += other.null_count_;
    value_count_ += other.value_count_;
  }

  bool has_value() const { return has_true_ || has_false_; }
  bool min() const { return has_false_ ? false : true; }
  bool max() const { return has_true_ ? true : false; }
  int64_t null_count() const { return null_count_; }
  int64_t value_count() const { return value_count_; }
  int64_t total_count() const { return value_count_ + null_count_; }
  bool has_null() const { return null_count_ > 0; }

  void reset() {
    has_true_ = false;
    has_false_ = false;
    null_count_ = 0;
    value_count_ = 0;
  }

private:
  bool has_true_ = false;
  bool has_false_ = false;
  int64_t null_count_ = 0;
  int64_t value_count_ = 0;
};

// Type aliases for convenience
using Int32Statistics = Statistics<int32_t>;
using Int64Statistics = Statistics<int64_t>;
using Float64Statistics = Statistics<double>;
using StringStatistics = Statistics<std::string>;
using BoolStatistics = Statistics<bool>;

} // namespace libvroom
