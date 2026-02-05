# Streaming CsvReader API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `start_streaming()` and `next_chunk()` methods to `CsvReader` that emit parsed chunks via a bounded ordered queue, enabling pipeline parallelism between CSV parsing and downstream consumers.

**Architecture:** After `open()`, `start_streaming()` runs the SIMD analysis phase (phases 1-2) synchronously, then dispatches chunk parse tasks to the thread pool. Each completed chunk is placed into an ordered bounded queue (`ParsedChunkQueue`). `next_chunk()` pops chunks in order, blocking until the next sequential chunk is ready. Backpressure from the bounded queue prevents unbounded memory growth.

**Tech Stack:** C++20, BS::thread_pool (existing), std::mutex/condition_variable (existing pattern from `EncodedRowGroupQueue`)

---

### Task 1: Create ParsedChunkQueue — ordered bounded queue

**Files:**
- Create: `include/libvroom/parsed_chunk_queue.h`

**Step 1: Write the failing test**

Create `test/parsed_chunk_queue_test.cpp`:

```cpp
#include "libvroom/parsed_chunk_queue.h"

#include <gtest/gtest.h>
#include <thread>

using namespace libvroom;

TEST(ParsedChunkQueueTest, BasicPushPop) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);

  std::vector<std::unique_ptr<ArrowColumnBuilder>> chunk0;
  EXPECT_TRUE(queue.push(0, std::move(chunk0)));

  auto result = queue.pop();
  ASSERT_TRUE(result.has_value());
  // Chunk 0 popped successfully
}

TEST(ParsedChunkQueueTest, OrderedDelivery) {
  // Chunks pushed out of order are delivered in order
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);

  std::vector<std::unique_ptr<ArrowColumnBuilder>> chunk2, chunk0, chunk1;
  EXPECT_TRUE(queue.push(2, std::move(chunk2)));
  EXPECT_TRUE(queue.push(0, std::move(chunk0)));
  EXPECT_TRUE(queue.push(1, std::move(chunk1)));

  // Should get chunks 0, 1, 2 in order
  auto r0 = queue.pop();
  ASSERT_TRUE(r0.has_value());
  auto r1 = queue.pop();
  ASSERT_TRUE(r1.has_value());
  auto r2 = queue.pop();
  ASSERT_TRUE(r2.has_value());

  // No more chunks
  auto r3 = queue.pop();
  EXPECT_FALSE(r3.has_value());
}

TEST(ParsedChunkQueueTest, BlocksUntilReady) {
  ParsedChunkQueue queue(/*num_chunks=*/2, /*max_buffered=*/4);

  // Push chunk 1 first (chunk 0 not ready yet)
  std::vector<std::unique_ptr<ArrowColumnBuilder>> chunk1;
  EXPECT_TRUE(queue.push(1, std::move(chunk1)));

  // Pop should block until chunk 0 arrives — push it from another thread
  std::thread producer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::vector<std::unique_ptr<ArrowColumnBuilder>> chunk0;
    queue.push(0, std::move(chunk0));
  });

  auto r0 = queue.pop();
  ASSERT_TRUE(r0.has_value());
  producer.join();
}

TEST(ParsedChunkQueueTest, BackpressureBlocks) {
  // Queue with max_buffered=1 should block on second push
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/1);

  std::vector<std::unique_ptr<ArrowColumnBuilder>> chunk0, chunk1;
  EXPECT_TRUE(queue.push(0, std::move(chunk0)));

  // Second push should block — pop from another thread to unblock
  std::atomic<bool> push_done{false};
  std::thread producer([&] {
    std::vector<std::unique_ptr<ArrowColumnBuilder>> c1;
    queue.push(1, std::move(c1));
    push_done = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(push_done.load()); // Still blocked

  queue.pop(); // Pop chunk 0, unblocking producer
  producer.join();
  EXPECT_TRUE(push_done.load());
}

TEST(ParsedChunkQueueTest, CloseUnblocks) {
  ParsedChunkQueue queue(/*num_chunks=*/2, /*max_buffered=*/4);

  // Close without pushing anything
  queue.close();

  auto result = queue.pop();
  EXPECT_FALSE(result.has_value());
}

TEST(ParsedChunkQueueTest, CloseUnblocksWaitingProducer) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/1);

  std::vector<std::unique_ptr<ArrowColumnBuilder>> chunk0;
  EXPECT_TRUE(queue.push(0, std::move(chunk0)));

  // Producer blocks on full queue
  std::thread producer([&] {
    std::vector<std::unique_ptr<ArrowColumnBuilder>> c1;
    bool result = queue.push(1, std::move(c1));
    // Should return false after close
    EXPECT_FALSE(result);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  queue.close(); // Unblock producer
  producer.join();
}
```

**Step 2: Add test target to CMakeLists.txt**

In `CMakeLists.txt` after the `streaming_test` target (around line 675), add:

```cmake
    add_executable(parsed_chunk_queue_test test/parsed_chunk_queue_test.cpp)
    target_link_libraries(parsed_chunk_queue_test PRIVATE vroom GTest::gtest_main pthread)
    target_include_directories(parsed_chunk_queue_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    gtest_discover_tests(parsed_chunk_queue_test)
```

**Step 3: Run test to verify it fails**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: Compilation failure — `parsed_chunk_queue.h` not found

**Step 4: Write minimal implementation**

Create `include/libvroom/parsed_chunk_queue.h`:

```cpp
#pragma once

#include "arrow_column_builder.h"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace libvroom {

// Thread-safe bounded queue that delivers parsed chunks in order.
// Producers push chunks by index (out of order); consumers pop in sequential order.
// Provides backpressure when buffered chunk count exceeds max_buffered.
class ParsedChunkQueue {
public:
  // num_chunks: total number of chunks expected
  // max_buffered: maximum chunks buffered before producers block
  explicit ParsedChunkQueue(size_t num_chunks, size_t max_buffered = 4)
      : num_chunks_(num_chunks), max_buffered_(max_buffered) {}

  // Producer: push a completed chunk by its index. Blocks if buffer is full.
  // Returns false if the queue was closed.
  bool push(size_t chunk_idx, std::vector<std::unique_ptr<ArrowColumnBuilder>>&& columns) {
    std::unique_lock<std::mutex> lock(mutex_);
    not_full_.wait(lock, [this] { return buffered_count_ < max_buffered_ || closed_; });

    if (closed_)
      return false;

    ready_chunks_[chunk_idx] = std::move(columns);
    buffered_count_++;
    not_empty_.notify_all();
    return true;
  }

  // Consumer: pop the next chunk in order. Blocks if not yet available.
  // Returns nullopt when all chunks have been consumed or queue is closed.
  std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] {
      return ready_chunks_.count(next_pop_idx_) > 0 || next_pop_idx_ >= num_chunks_ || closed_;
    });

    if (next_pop_idx_ >= num_chunks_)
      return std::nullopt;

    auto it = ready_chunks_.find(next_pop_idx_);
    if (it == ready_chunks_.end())
      return std::nullopt; // closed without all chunks

    auto result = std::move(it->second);
    ready_chunks_.erase(it);
    next_pop_idx_++;
    buffered_count_--;
    not_full_.notify_one();
    return result;
  }

  // Signal that no more items will be added (e.g., on error).
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
  std::map<size_t, std::vector<std::unique_ptr<ArrowColumnBuilder>>> ready_chunks_;
  mutable std::mutex mutex_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  size_t num_chunks_;
  size_t max_buffered_;
  size_t next_pop_idx_ = 0;
  size_t buffered_count_ = 0;
  bool closed_ = false;
};

} // namespace libvroom
```

**Step 5: Run tests to verify they pass**

Run: `cmake --build build -j$(nproc) && cd build && ctest -R parsed_chunk_queue --output-on-failure -j$(nproc)`
Expected: All 6 tests PASS

**Step 6: Commit**

```bash
git add include/libvroom/parsed_chunk_queue.h test/parsed_chunk_queue_test.cpp CMakeLists.txt
git commit -m "feat: add ParsedChunkQueue for ordered bounded chunk delivery"
```

---

### Task 2: Add streaming API to CsvReader header

**Files:**
- Modify: `include/libvroom/vroom.h:116-153` (CsvReader class)

**Step 1: Write the failing test**

Create `test/streaming_csvreader_test.cpp`:

```cpp
#include "libvroom.h"
#include "test_util.h"

#include <gtest/gtest.h>
#include <string>

class StreamingCsvReaderTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }
};

TEST_F(StreamingCsvReaderTest, BasicStreaming) {
  // Small file that will be parsed serially (single chunk)
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(f.path());
  ASSERT_TRUE(open_result.ok);

  auto start_result = reader.start_streaming();
  ASSERT_TRUE(start_result.ok);

  // Should get exactly one chunk for a small file
  auto chunk = reader.next_chunk();
  ASSERT_TRUE(chunk.has_value());
  EXPECT_EQ(chunk->size(), 3u); // 3 columns

  // No more chunks
  auto end = reader.next_chunk();
  EXPECT_FALSE(end.has_value());
}

TEST_F(StreamingCsvReaderTest, StreamingMatchesReadAll) {
  // Compare streaming results to read_all() for a test file
  std::string csv = "x,y\n1,hello\n2,world\n3,foo\n4,bar\n5,baz\n";
  test_util::TempCsvFile f(csv);

  // read_all path
  libvroom::CsvReader reader1(libvroom::CsvOptions{});
  reader1.open(f.path());
  auto all = reader1.read_all();
  ASSERT_TRUE(all.ok);

  // streaming path
  libvroom::CsvReader reader2(libvroom::CsvOptions{});
  reader2.open(f.path());
  auto start = reader2.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t streaming_rows = 0;
  while (auto chunk = reader2.next_chunk()) {
    if (!chunk->empty()) {
      streaming_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(streaming_rows, all.value.total_rows);
}
```

**Step 2: Add test target to CMakeLists.txt**

After the `parsed_chunk_queue_test` target:

```cmake
    # Streaming CsvReader API tests
    add_executable(streaming_csvreader_test test/streaming_csvreader_test.cpp)
    target_link_libraries(streaming_csvreader_test PRIVATE vroom GTest::gtest_main pthread)
    target_include_directories(streaming_csvreader_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    add_dependencies(streaming_csvreader_test copy_test_data)
    gtest_discover_tests(streaming_csvreader_test)
```

**Step 3: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: Compilation failure — `start_streaming` and `next_chunk` are not members of `CsvReader`

**Step 4: Add method declarations to vroom.h**

In `include/libvroom/vroom.h`, add to the CsvReader public section (after `read_all()` on line 134):

```cpp
  // Streaming API: parse chunks on background threads, consume one at a time.
  // Call open() first, then start_streaming() to begin, then next_chunk() in a loop.
  // start_streaming() runs SIMD analysis (phases 1-2) synchronously, then
  // dispatches chunk parsing to the thread pool.
  Result<bool> start_streaming();

  // Returns the next parsed chunk in order, or nullopt when all chunks are consumed.
  // Blocks if the next sequential chunk hasn't finished parsing yet.
  std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> next_chunk();
```

**Step 5: Add stub implementations to csv_reader.cpp**

At the end of `src/reader/csv_reader.cpp` (before the `convert_csv_to_parquet` function around line 1204):

```cpp
Result<bool> CsvReader::start_streaming() {
  return Result<bool>::failure("Not implemented");
}

std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> CsvReader::next_chunk() {
  return std::nullopt;
}
```

**Step 6: Run tests — basic test should fail (not implemented), but it should compile**

Run: `cmake --build build -j$(nproc) && cd build && ctest -R streaming_csvreader --output-on-failure`
Expected: Tests FAIL with "Not implemented" or wrong results

**Step 7: Commit**

```bash
git add include/libvroom/vroom.h src/reader/csv_reader.cpp test/streaming_csvreader_test.cpp CMakeLists.txt
git commit -m "feat: add streaming API stubs to CsvReader (start_streaming, next_chunk)"
```

---

### Task 3: Implement start_streaming() — analysis phases + dispatch

**Files:**
- Modify: `src/reader/csv_reader.cpp:237-262` (CsvReader::Impl — add streaming state)
- Modify: `src/reader/csv_reader.cpp` (implement start_streaming)

**Step 1: Add streaming state to CsvReader::Impl**

In `src/reader/csv_reader.cpp`, add to the `Impl` struct (around line 250, before the constructor):

```cpp
  // Streaming state
  std::unique_ptr<ParsedChunkQueue> streaming_queue;
  std::unique_ptr<BS::thread_pool> streaming_pool;
  std::vector<ErrorCollector> streaming_error_collectors;
  std::vector<ChunkAnalysisResult> streaming_analysis;
  std::vector<bool> streaming_use_inside;
  std::vector<std::pair<size_t, size_t>> streaming_chunk_ranges;
  bool streaming_active = false;
```

Also add the include at the top of `csv_reader.cpp`:

```cpp
#include "libvroom/parsed_chunk_queue.h"
```

**Step 2: Implement start_streaming()**

Replace the stub with the full implementation. The logic is extracted from `read_all()` — phases 1 and 2 run synchronously, then phase 3 tasks are dispatched but NOT waited on:

```cpp
Result<bool> CsvReader::start_streaming() {
  if (impl_->schema.empty()) {
    return Result<bool>::failure("No schema — call open() first");
  }
  if (impl_->streaming_active) {
    return Result<bool>::failure("Streaming already started");
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;
  size_t data_start = impl_->header_end_offset;
  size_t data_size = size - data_start;

  // For small files, produce a single chunk
  constexpr size_t PARALLEL_THRESHOLD = 1024 * 1024; // 1MB
  if (data_size < PARALLEL_THRESHOLD) {
    // Serial: parse everything now, push single chunk to queue
    auto serial_result = read_all_serial();
    if (!serial_result.ok) {
      return Result<bool>::failure(serial_result.error);
    }
    size_t num_chunks = serial_result.value.chunks.size();
    impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, 4);
    for (size_t i = 0; i < num_chunks; ++i) {
      impl_->streaming_queue->push(i, std::move(serial_result.value.chunks[i]));
    }
    impl_->streaming_active = true;
    return Result<bool>::success(true);
  }

  // Calculate chunk boundaries (same as read_all)
  size_t n_cols = impl_->schema.size();
  size_t chunk_size = calculate_chunk_size(data_size, n_cols, impl_->num_threads);
  auto& chunk_ranges = impl_->streaming_chunk_ranges;
  chunk_ranges.clear();
  size_t offset = data_start;
  ChunkFinder finder(impl_->options.separator, impl_->options.quote);

  while (offset < size) {
    size_t target_end = std::min(offset + chunk_size, size);
    size_t chunk_end;
    if (target_end >= size) {
      chunk_end = size;
    } else {
      chunk_end = finder.find_row_end(data, size, target_end);
      while (chunk_end == target_end && chunk_end < size) {
        target_end = std::min(target_end + chunk_size, size);
        chunk_end = finder.find_row_end(data, size, target_end);
      }
    }
    chunk_ranges.emplace_back(offset, chunk_end);
    offset = chunk_end;
  }

  size_t num_chunks = chunk_ranges.size();
  if (num_chunks <= 1) {
    // Single chunk — serial parse
    auto serial_result = read_all_serial();
    if (!serial_result.ok) {
      return Result<bool>::failure(serial_result.error);
    }
    size_t n = serial_result.value.chunks.size();
    impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(n, 4);
    for (size_t i = 0; i < n; ++i) {
      impl_->streaming_queue->push(i, std::move(serial_result.value.chunks[i]));
    }
    impl_->streaming_active = true;
    return Result<bool>::success(true);
  }

  // Phase 1: Analyze all chunks (SIMD, parallel)
  size_t pool_threads = std::min(impl_->num_threads, num_chunks);
  impl_->streaming_pool = std::make_unique<BS::thread_pool>(pool_threads);
  auto& pool = *impl_->streaming_pool;
  const CsvOptions options = impl_->options;

  auto& analysis_results = impl_->streaming_analysis;
  analysis_results.resize(num_chunks);
  {
    std::vector<std::future<void>> futures;
    futures.reserve(num_chunks);
    for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
      size_t start_offset = chunk_ranges[chunk_idx].first;
      size_t end_offset = chunk_ranges[chunk_idx].second;
      futures.push_back(pool.submit_task(
          [&analysis_results, data, size, chunk_idx, start_offset, end_offset, options]() {
            if (start_offset >= size || end_offset > size || start_offset >= end_offset)
              return;
            auto stats = analyze_chunk_dual_state_simd(
                data + start_offset, end_offset - start_offset, options.quote);
            auto& result = analysis_results[chunk_idx];
            result.row_count_outside = stats.row_count_outside;
            result.row_count_inside = stats.row_count_inside;
            result.ends_inside_starting_outside = stats.ends_inside_quote_from_outside;
          }));
    }
    for (auto& f : futures) f.get();
  }

  // Phase 2: Link chunks (serial)
  auto& use_inside_state = impl_->streaming_use_inside;
  use_inside_state.resize(num_chunks, false);
  use_inside_state[0] = false;
  for (size_t i = 1; i < num_chunks; ++i) {
    bool prev_used_inside = use_inside_state[i - 1];
    bool prev_ends_inside;
    if (prev_used_inside) {
      prev_ends_inside = !analysis_results[i - 1].ends_inside_starting_outside;
    } else {
      prev_ends_inside = analysis_results[i - 1].ends_inside_starting_outside;
    }
    use_inside_state[i] = prev_ends_inside;
  }

  // Compute total row count
  size_t total_row_count = 0;
  for (size_t i = 0; i < num_chunks; ++i) {
    total_row_count += use_inside_state[i] ? analysis_results[i].row_count_inside
                                           : analysis_results[i].row_count_outside;
  }
  impl_->row_count = total_row_count;

  // Set up error collectors
  const bool check_errors = impl_->error_collector.is_enabled();
  if (check_errors) {
    impl_->streaming_error_collectors.clear();
    impl_->streaming_error_collectors.reserve(num_chunks);
    for (size_t i = 0; i < num_chunks; ++i) {
      impl_->streaming_error_collectors.emplace_back(
          impl_->error_collector.mode(), impl_->error_collector.max_errors());
    }
  }

  // Create the bounded queue
  impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, /*max_buffered=*/4);

  // Phase 3: Dispatch parse tasks (NOT waited on — they push to queue)
  const std::vector<ColumnSchema> schema = impl_->schema;
  auto* queue = impl_->streaming_queue.get();
  auto* error_collectors = check_errors ? &impl_->streaming_error_collectors : nullptr;

  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t start_offset = chunk_ranges[chunk_idx].first;
    size_t end_offset = chunk_ranges[chunk_idx].second;
    bool start_inside = use_inside_state[chunk_idx];
    size_t expected_rows = start_inside ? analysis_results[chunk_idx].row_count_inside
                                        : analysis_results[chunk_idx].row_count_outside;
    ErrorCollector* chunk_error_collector =
        check_errors ? &(*error_collectors)[chunk_idx] : nullptr;

    pool.detach_task([queue, data, size, chunk_idx, start_offset, end_offset,
                      start_inside, expected_rows, options, schema,
                      chunk_error_collector]() {
      if (start_offset >= size || end_offset > size || start_offset >= end_offset) {
        std::vector<std::unique_ptr<ArrowColumnBuilder>> empty;
        queue->push(chunk_idx, std::move(empty));
        return;
      }

      NullChecker null_checker(options);
      std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
      for (const auto& col_schema : schema) {
        auto builder = ArrowColumnBuilder::create(col_schema.type);
        builder->reserve(expected_rows);
        columns.push_back(std::move(builder));
      }

      auto [rows, ends_inside] = parse_chunk_with_state(
          data + start_offset, end_offset - start_offset, options, null_checker,
          columns, start_inside, chunk_error_collector, start_offset);
      (void)ends_inside;

      queue->push(chunk_idx, std::move(columns));
    });
  }

  impl_->streaming_active = true;
  return Result<bool>::success(true);
}
```

**Step 3: Implement next_chunk()**

Replace the stub:

```cpp
std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> CsvReader::next_chunk() {
  if (!impl_->streaming_active || !impl_->streaming_queue) {
    return std::nullopt;
  }

  auto result = impl_->streaming_queue->pop();

  if (!result.has_value()) {
    // All chunks consumed — finalize
    // Merge error collectors if enabled
    if (impl_->error_collector.is_enabled() && !impl_->streaming_error_collectors.empty()) {
      // Check for unclosed quote in last chunk
      size_t num_chunks = impl_->streaming_analysis.size();
      if (num_chunks > 0) {
        bool last_used_inside = impl_->streaming_use_inside[num_chunks - 1];
        bool last_ends_inside = last_used_inside
            ? !impl_->streaming_analysis[num_chunks - 1].ends_inside_starting_outside
            : impl_->streaming_analysis[num_chunks - 1].ends_inside_starting_outside;
        if (last_ends_inside) {
          size_t last_start = impl_->streaming_chunk_ranges[num_chunks - 1].first;
          impl_->streaming_error_collectors.back().add_error(
              ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::RECOVERABLE, 0, 0,
              last_start, "Quoted field not closed before end of data");
        }
      }
      impl_->error_collector.merge_sorted(impl_->streaming_error_collectors);
      impl_->streaming_error_collectors.clear();
    }

    // Clean up streaming state
    impl_->streaming_pool.reset();
    impl_->streaming_queue.reset();
    impl_->streaming_active = false;
  }

  return result;
}
```

**Step 4: Run tests**

Run: `cmake --build build -j$(nproc) && cd build && ctest -R streaming_csvreader --output-on-failure`
Expected: Both BasicStreaming and StreamingMatchesReadAll PASS

**Step 5: Commit**

```bash
git add src/reader/csv_reader.cpp
git commit -m "feat: implement start_streaming() and next_chunk() for pipelined chunk delivery"
```

---

### Task 4: Add comprehensive streaming tests

**Files:**
- Modify: `test/streaming_csvreader_test.cpp`

**Step 1: Add tests for large files, multi-chunk, and error handling**

Append to `test/streaming_csvreader_test.cpp`:

```cpp
TEST_F(StreamingCsvReaderTest, LargeFileMultipleChunks) {
  // Generate a file >1MB to trigger parallel parsing
  std::string csv = "id,name,value\n";
  for (int i = 0; i < 50000; ++i) {
    csv += std::to_string(i) + ",name_" + std::to_string(i) + "," +
           std::to_string(i * 1.5) + "\n";
  }
  test_util::TempCsvFile f(csv);

  // Streaming
  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());
  auto start = reader.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t total_rows = 0;
  size_t chunk_count = 0;
  while (auto chunk = reader.next_chunk()) {
    chunk_count++;
    if (!chunk->empty()) {
      total_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(total_rows, 50000u);
  EXPECT_GE(chunk_count, 1u); // At least 1 chunk
}

TEST_F(StreamingCsvReaderTest, StreamingMatchesReadAllLargeFile) {
  // Generate large file and verify streaming matches read_all
  std::string csv = "a,b\n";
  for (int i = 0; i < 50000; ++i) {
    csv += std::to_string(i) + ",val_" + std::to_string(i) + "\n";
  }
  test_util::TempCsvFile f(csv);

  // read_all
  libvroom::CsvReader reader1(libvroom::CsvOptions{});
  reader1.open(f.path());
  auto all = reader1.read_all();
  ASSERT_TRUE(all.ok);

  // streaming
  libvroom::CsvReader reader2(libvroom::CsvOptions{});
  reader2.open(f.path());
  reader2.start_streaming();

  size_t streaming_rows = 0;
  while (auto chunk = reader2.next_chunk()) {
    if (!chunk->empty()) {
      streaming_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(streaming_rows, all.value.total_rows);
}

TEST_F(StreamingCsvReaderTest, StreamingWithQuotedFields) {
  // Quoted fields spanning chunks (when file is large enough)
  std::string csv = "a,b\n";
  for (int i = 0; i < 50000; ++i) {
    csv += "\"val_" + std::to_string(i) + "\",\"hello \"\"world\"\" " +
           std::to_string(i) + "\"\n";
  }
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());
  reader.start_streaming();

  size_t total_rows = 0;
  while (auto chunk = reader.next_chunk()) {
    if (!chunk->empty()) {
      total_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(total_rows, 50000u);
}

TEST_F(StreamingCsvReaderTest, StartStreamingBeforeOpen) {
  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto result = reader.start_streaming();
  EXPECT_FALSE(result.ok);
}

TEST_F(StreamingCsvReaderTest, DoubleStartStreaming) {
  std::string csv = "a\n1\n2\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());
  auto start1 = reader.start_streaming();
  ASSERT_TRUE(start1.ok);

  auto start2 = reader.start_streaming();
  EXPECT_FALSE(start2.ok); // Already streaming
}

TEST_F(StreamingCsvReaderTest, NextChunkWithoutStartStreaming) {
  std::string csv = "a\n1\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());

  auto chunk = reader.next_chunk();
  EXPECT_FALSE(chunk.has_value()); // Not streaming
}

TEST_F(StreamingCsvReaderTest, StreamingFromBuffer) {
  std::string csv = "x,y\n1,2\n3,4\n";
  libvroom::AlignedBuffer buf(csv.size());
  std::memcpy(buf.data(), csv.data(), csv.size());

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open_from_buffer(std::move(buf));
  auto start = reader.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t total = 0;
  while (auto chunk = reader.next_chunk()) {
    if (!chunk->empty()) total += (*chunk)[0]->size();
  }
  EXPECT_EQ(total, 2u);
}
```

**Step 2: Run tests**

Run: `cmake --build build -j$(nproc) && cd build && ctest -R streaming_csvreader --output-on-failure -j$(nproc)`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add test/streaming_csvreader_test.cpp
git commit -m "test: add comprehensive streaming CsvReader tests"
```

---

### Task 5: Run full test suite and verify no regressions

**Step 1: Build and run all tests**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`
Expected: All tests pass, no regressions

**Step 2: Fix any failures**

If any existing tests fail, investigate and fix.

**Step 3: Commit if any fixes were needed**

---

### Task 6: Include ParsedChunkQueue in public header

**Files:**
- Modify: `include/libvroom.h`

**Step 1: Add include**

Add after the streaming include (line 45):

```cpp
// Parsed chunk queue (for streaming CsvReader API)
#include "libvroom/parsed_chunk_queue.h"
```

**Step 2: Verify build**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`
Expected: All pass

**Step 3: Commit**

```bash
git add include/libvroom.h
git commit -m "feat: expose ParsedChunkQueue in public header"
```
