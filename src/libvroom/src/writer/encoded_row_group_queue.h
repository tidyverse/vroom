#pragma once

#include "encoded_row_group.h"

#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

namespace libvroom {
namespace writer {

// Thread-safe bounded queue for passing encoded row groups from encoder to writer
// Provides backpressure when queue is full (blocks producer)
class EncodedRowGroupQueue {
public:
  explicit EncodedRowGroupQueue(size_t max_size = 4) : max_size_(max_size), closed_(false) {}

  // Producer: Add encoded row group to queue (blocks if full)
  // Returns false if queue was closed
  bool push(EncodedRowGroup&& row_group) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this] { return queue_.size() < max_size_ || closed_; });

    if (closed_)
      return false;

    queue_.push(std::move(row_group));
    not_empty_.notify_one();
    return true;
  }

  // Consumer: Get next row group (blocks if empty)
  // Returns nullopt if queue is empty AND closed
  std::optional<EncodedRowGroup> pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return !queue_.empty() || closed_; });

    if (queue_.empty())
      return std::nullopt;

    EncodedRowGroup result = std::move(queue_.front());
    queue_.pop();
    not_full_.notify_one();
    return result;
  }

  // Signal that no more items will be added
  void close() {
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();
  }

  bool is_closed() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return closed_;
  }

private:
  std::queue<EncodedRowGroup> queue_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  size_t max_size_;
  bool closed_;
};

} // namespace writer
} // namespace libvroom
