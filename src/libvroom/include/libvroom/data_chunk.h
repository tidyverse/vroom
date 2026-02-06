#pragma once

#include <cstddef>
#include <memory>
#include <vector>

namespace libvroom {

// DataChunk - a contiguous block of typed data with null bitmap
// Inspired by Arrow's Array concept from Polars' ChunkedArray
// Each chunk is immutable after creation and can be moved efficiently
template <typename T> struct DataChunk {
  std::vector<T> values;
  std::vector<bool> null_bitmap;

  size_t size() const { return values.size(); }
  bool empty() const { return values.empty(); }

  // Move constructor and assignment
  DataChunk() = default;
  DataChunk(DataChunk&&) = default;
  DataChunk& operator=(DataChunk&&) = default;

  // No copy (chunks are immutable and moved, not copied)
  DataChunk(const DataChunk&) = delete;
  DataChunk& operator=(const DataChunk&) = delete;

  // Take ownership of vectors
  DataChunk(std::vector<T>&& vals, std::vector<bool>&& nulls)
      : values(std::move(vals)), null_bitmap(std::move(nulls)) {}
};

// ChunkedStorage - stores data as a vector of chunks
// This is the key optimization: merge_from() just moves chunk pointers O(1)
// instead of copying all data O(n)
//
// Design: Uses a single contiguous "active" vector for parsing (so FastColumnContext
// works efficiently), and only creates immutable chunks during merge operations.
template <typename T> class ChunkedStorage {
public:
  ChunkedStorage() = default;

  // Get total number of values across all chunks (including active vectors)
  size_t size() const {
    size_t total = active_values_.size();
    for (const auto& chunk : chunks_) {
      total += chunk->size();
    }
    return total;
  }

  // Get number of chunks (after finalization)
  size_t num_chunks() const { return chunks_.size(); }

  // Check if empty
  bool empty() const {
    if (!active_values_.empty()) {
      return false;
    }
    for (const auto& chunk : chunks_) {
      if (!chunk->empty()) {
        return false;
      }
    }
    return true;
  }

  // Reserve space in the active vectors
  void reserve(size_t capacity) {
    active_values_.reserve(capacity);
    active_null_bitmap_.reserve(capacity);
  }

  // Append value to active vectors
  void append(const T& value, bool is_null = false) {
    active_values_.push_back(value);
    active_null_bitmap_.push_back(is_null);
  }

  // Append value (move version)
  void append(T&& value, bool is_null = false) {
    active_values_.push_back(std::move(value));
    active_null_bitmap_.push_back(is_null);
  }

  // Direct access to active vectors for FastColumnContext
  std::vector<T>& active_values() { return active_values_; }
  std::vector<bool>& active_null_bitmap() { return active_null_bitmap_; }
  const std::vector<T>& active_values() const { return active_values_; }
  const std::vector<bool>& active_null_bitmap() const { return active_null_bitmap_; }

  // Merge from another ChunkedStorage (O(1) - just moves chunk pointers!)
  // This is the key optimization that matches Polars' append performance
  void merge_from(ChunkedStorage& other) {
    // First, finalize both our active vectors and other's
    finalize_active();
    other.finalize_active();

    // Then move all chunks from other to us (O(1) per chunk)
    chunks_.reserve(chunks_.size() + other.chunks_.size());
    for (auto& chunk : other.chunks_) {
      if (!chunk->empty()) {
        chunks_.push_back(std::move(chunk));
      }
    }
    other.chunks_.clear();
  }

  // Finalize the active vectors into a chunk (call before reading chunks)
  void finalize_active() {
    if (!active_values_.empty()) {
      auto chunk =
          std::make_unique<DataChunk<T>>(std::move(active_values_), std::move(active_null_bitmap_));
      chunks_.push_back(std::move(chunk));
      // Vectors are now in moved-from state, but still valid and empty
      active_values_.clear();
      active_null_bitmap_.clear();
    }
  }

  // Access chunks for reading (call finalize_active first!)
  const std::vector<std::unique_ptr<DataChunk<T>>>& chunks() const { return chunks_; }

  // Iterator interface for reading across all chunks
  class iterator {
  public:
    using value_type = std::pair<const T&, bool>; // (value, is_null)

    iterator(const ChunkedStorage* storage, size_t chunk_idx, size_t offset)
        : storage_(storage), chunk_idx_(chunk_idx), offset_(offset) {}

    value_type operator*() const {
      const auto& chunk = *storage_->chunks_[chunk_idx_];
      return {chunk.values[offset_], chunk.null_bitmap[offset_]};
    }

    iterator& operator++() {
      offset_++;
      if (offset_ >= storage_->chunks_[chunk_idx_]->size()) {
        chunk_idx_++;
        offset_ = 0;
      }
      return *this;
    }

    bool operator!=(const iterator& other) const {
      return chunk_idx_ != other.chunk_idx_ || offset_ != other.offset_;
    }

  private:
    const ChunkedStorage* storage_;
    size_t chunk_idx_;
    size_t offset_;
  };

  iterator begin() const {
    if (chunks_.empty())
      return end();
    return iterator(this, 0, 0);
  }

  iterator end() const { return iterator(this, chunks_.size(), 0); }

private:
  // Active vectors for parsing - FastColumnContext writes directly here
  std::vector<T> active_values_;
  std::vector<bool> active_null_bitmap_;

  // Finalized immutable chunks - created during merge
  std::vector<std::unique_ptr<DataChunk<T>>> chunks_;
};

} // namespace libvroom
