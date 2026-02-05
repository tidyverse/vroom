#pragma once

#include "arrow_column_builder.h"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace libvroom {

/// Thread-safe bounded queue that delivers parsed chunks in sequential order.
///
/// Producers push chunks by index (out of order as parsing completes).
/// The consumer pops chunks in order: 0, 1, 2, ...
/// Backpressure: producer blocks when its chunk_idx >= next_pop_idx + max_buffered.
/// This distance-based backpressure avoids deadlock: chunks near the consumer's
/// read position always get through, while chunks far ahead block to limit memory.
/// Consumer blocks when the next sequential chunk hasn't arrived yet.
/// close() unblocks all waiting threads.
class ParsedChunkQueue {
public:
  /// @param num_chunks  Total number of chunks expected (determines end-of-stream).
  /// @param max_buffered  Maximum distance ahead of consumer before producers block.
  explicit ParsedChunkQueue(size_t num_chunks, size_t max_buffered = 4)
      : num_chunks_(num_chunks), max_buffered_(max_buffered) {}

  /// Producer: push a parsed chunk by its index.
  /// Blocks if chunk_idx >= next_pop_idx + max_buffered (distance-based backpressure).
  /// Returns false if the queue was closed.
  bool push(size_t chunk_idx, std::vector<std::unique_ptr<ArrowColumnBuilder>>&& columns) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(
        lock, [this, chunk_idx] { return chunk_idx < next_pop_idx_ + max_buffered_ || closed_; });

    if (closed_)
      return false;

    ready_chunks_[chunk_idx] = std::move(columns);
    not_empty_.notify_all();
    return true;
  }

  /// Consumer: pop the next sequential chunk.
  /// Blocks until chunk next_pop_idx_ is available.
  /// Returns nullopt when all chunks have been consumed or the queue is closed.
  std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] {
      return ready_chunks_.count(next_pop_idx_) > 0 || next_pop_idx_ >= num_chunks_ || closed_;
    });

    if (next_pop_idx_ >= num_chunks_)
      return std::nullopt;

    auto it = ready_chunks_.find(next_pop_idx_);
    if (it == ready_chunks_.end())
      return std::nullopt; // closed without the next chunk available

    auto result = std::move(it->second);
    ready_chunks_.erase(it);
    next_pop_idx_++;
    not_full_.notify_all();
    return result;
  }

  /// Signal that no more items will be added. Unblocks all waiting threads.
  void close() {
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    not_empty_.notify_all();
    not_full_.notify_all();
  }

  /// Check whether the queue has been closed.
  bool is_closed() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return closed_;
  }

private:
  std::map<size_t, std::vector<std::unique_ptr<ArrowColumnBuilder>>> ready_chunks_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  size_t num_chunks_;
  size_t max_buffered_;
  size_t next_pop_idx_ = 0;
  bool closed_ = false;
};

} // namespace libvroom
