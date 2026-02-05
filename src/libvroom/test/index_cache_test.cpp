#include "libvroom/cache.h"
#include "libvroom/io_util.h"
#include "libvroom/vroom.h"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

using namespace libvroom;
namespace fs = std::filesystem;

// Helper to get path to test data files
static std::string test_data_path(const std::string& filename) {
  return "test/data/" + filename;
}

// Helper to create a temporary directory for tests
class CacheTestFixture : public ::testing::Test {
protected:
  std::string tmp_dir;

  void SetUp() override {
    // Use test name + PID to ensure unique temp dirs when CTest runs tests in parallel
    // (gtest_discover_tests runs each test as a separate process with same random_seed)
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    tmp_dir = fs::temp_directory_path().string() + "/libvroom_cache_test_" + info->name() + "_" +
              std::to_string(getpid());
    fs::create_directories(tmp_dir);
  }

  void TearDown() override {
    std::error_code ec;
    fs::remove_all(tmp_dir, ec);
  }

  // Create a small CSV file for testing
  std::string create_test_csv(const std::string& name, const std::string& content) {
    std::string path = tmp_dir + "/" + name;
    std::ofstream f(path);
    f << content;
    f.close();
    return path;
  }
};

// =============================================================================
// Path computation tests
// =============================================================================

TEST_F(CacheTestFixture, ComputePathSameDir) {
  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");
  CacheConfig cfg = CacheConfig::defaults();

  std::string cache_path = IndexCache::compute_path(csv_path, cfg);
  EXPECT_EQ(cache_path, csv_path + ".vidx");
}

TEST_F(CacheTestFixture, ComputePathCustomDir) {
  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");
  std::string cache_dir = tmp_dir + "/cache";
  fs::create_directories(cache_dir);

  CacheConfig cfg = CacheConfig::custom(cache_dir);
  std::string cache_path = IndexCache::compute_path(csv_path, cfg);
  EXPECT_TRUE(cache_path.find(cache_dir) == 0);
  EXPECT_TRUE(cache_path.find(".vidx") != std::string::npos);
}

TEST_F(CacheTestFixture, ComputePathXdgCache) {
  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");
  CacheConfig cfg = CacheConfig::xdg_cache();

  std::string cache_path = IndexCache::compute_path(csv_path, cfg);
  // Should be under ~/.cache/libvroom/ or XDG_CACHE_HOME
  EXPECT_TRUE(cache_path.find(".vidx") != std::string::npos);
  EXPECT_FALSE(cache_path.empty());
}

TEST_F(CacheTestFixture, ComputePathEmptySource) {
  CacheConfig cfg = CacheConfig::defaults();
  std::string cache_path = IndexCache::compute_path("", cfg);
  EXPECT_TRUE(cache_path.empty());
}

// =============================================================================
// Hash path tests
// =============================================================================

TEST_F(CacheTestFixture, HashPathDeterministic) {
  std::string hash1 = IndexCache::hash_path("/some/path/data.csv");
  std::string hash2 = IndexCache::hash_path("/some/path/data.csv");
  EXPECT_EQ(hash1, hash2);
}

TEST_F(CacheTestFixture, HashPathDifferentForDifferentPaths) {
  std::string hash1 = IndexCache::hash_path("/path/a.csv");
  std::string hash2 = IndexCache::hash_path("/path/b.csv");
  EXPECT_NE(hash1, hash2);
}

// =============================================================================
// Directory writability tests
// =============================================================================

TEST_F(CacheTestFixture, IsDirectoryWritable) {
  EXPECT_TRUE(IndexCache::is_directory_writable(tmp_dir));
}

TEST_F(CacheTestFixture, NonexistentDirNotWritable) {
  EXPECT_FALSE(IndexCache::is_directory_writable("/nonexistent/path/that/does/not/exist"));
}

// =============================================================================
// Staleness detection tests
// =============================================================================

TEST_F(CacheTestFixture, IsFreshMatchingStats) {
  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");

  struct stat st;
  stat(csv_path.c_str(), &st);
  EXPECT_TRUE(IndexCache::is_fresh(csv_path, st.st_mtime, st.st_size));
}

TEST_F(CacheTestFixture, IsStaleWrongSize) {
  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");

  struct stat st;
  stat(csv_path.c_str(), &st);
  EXPECT_FALSE(IndexCache::is_fresh(csv_path, st.st_mtime, st.st_size + 100));
}

TEST_F(CacheTestFixture, IsStaleWrongMtime) {
  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");

  struct stat st;
  stat(csv_path.c_str(), &st);
  EXPECT_FALSE(IndexCache::is_fresh(csv_path, st.st_mtime + 1, st.st_size));
}

TEST_F(CacheTestFixture, IsStaleMissingFile) {
  EXPECT_FALSE(IndexCache::is_fresh("/nonexistent/file.csv", 0, 0));
}

// =============================================================================
// Load failure tests (corruption, not found, etc.)
// =============================================================================

TEST_F(CacheTestFixture, LoadNotFound) {
  auto result = IndexCache::load(tmp_dir + "/nonexistent.vidx", tmp_dir + "/data.csv");
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, CacheError::NotFound);
}

TEST_F(CacheTestFixture, LoadCorruptedBadMagic) {
  std::string cache_path = tmp_dir + "/bad.vidx";
  {
    std::ofstream f(cache_path, std::ios::binary);
    // Write 48 bytes of zeros (bad magic)
    std::vector<uint8_t> data(48, 0);
    f.write(reinterpret_cast<const char*>(data.data()), data.size());
  }

  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");
  auto result = IndexCache::load(cache_path, csv_path);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, CacheError::Corrupted);
}

TEST_F(CacheTestFixture, LoadCorruptedTruncated) {
  std::string cache_path = tmp_dir + "/truncated.vidx";
  {
    std::ofstream f(cache_path, std::ios::binary);
    // Write just 10 bytes (too small for header)
    std::vector<uint8_t> data(10, 0);
    f.write(reinterpret_cast<const char*>(data.data()), data.size());
  }

  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");
  auto result = IndexCache::load(cache_path, csv_path);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, CacheError::Corrupted);
}

TEST_F(CacheTestFixture, LoadVersionMismatch) {
  std::string cache_path = tmp_dir + "/badversion.vidx";
  {
    std::ofstream f(cache_path, std::ios::binary);
    std::vector<uint8_t> data(48, 0);
    // Set correct magic
    uint32_t magic = VIDX_MAGIC;
    std::memcpy(data.data(), &magic, 4);
    // Set wrong version
    data[4] = 99;
    f.write(reinterpret_cast<const char*>(data.data()), data.size());
  }

  auto csv_path = create_test_csv("data.csv", "a,b\n1,2\n");
  auto result = IndexCache::load(cache_path, csv_path);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, CacheError::VersionMismatch);
}

// =============================================================================
// Write and load roundtrip
// =============================================================================

TEST_F(CacheTestFixture, WriteAndLoadRoundtrip) {
  auto csv_path = create_test_csv("roundtrip.csv", "name,age\nAlice,30\nBob,25\nCarol,35\n");
  std::string cache_path = csv_path + ".vidx";

  // Build a CachedIndex
  CachedIndex index;
  index.header_end_offset = 9; // "name,age\n"
  index.num_columns = 2;
  index.total_rows = 3;
  index.sample_interval = 32;

  // Schema
  ColumnSchema col1;
  col1.name = "name";
  col1.type = DataType::STRING;
  col1.index = 0;
  ColumnSchema col2;
  col2.name = "age";
  col2.type = DataType::INT32;
  col2.index = 1;
  index.schema = {col1, col2};

  // Single chunk
  index.chunk_boundaries = {{9, 39}}; // Approximate
  ChunkMeta meta;
  meta.row_count = 3;
  meta.ends_inside_starting_outside = false;
  index.chunk_analysis = {meta};

  // Empty sampled offsets
  index.sampled_offsets = EliasFano::encode({}, 0);

  // Write
  EXPECT_TRUE(IndexCache::write_atomic(cache_path, index, csv_path));
  EXPECT_TRUE(fs::exists(cache_path));

  // Load
  auto result = IndexCache::load(cache_path, csv_path);
  ASSERT_TRUE(result.ok()) << result.message;

  EXPECT_EQ(result.index.header_end_offset, 9u);
  EXPECT_EQ(result.index.num_columns, 2u);
  EXPECT_EQ(result.index.total_rows, 3u);
  EXPECT_EQ(result.index.sample_interval, 32u);
  ASSERT_EQ(result.index.chunk_boundaries.size(), 1u);
  EXPECT_EQ(result.index.chunk_boundaries[0].first, 9u);
  ASSERT_EQ(result.index.chunk_analysis.size(), 1u);
  EXPECT_EQ(result.index.chunk_analysis[0].row_count, 3u);
  EXPECT_FALSE(result.index.chunk_analysis[0].ends_inside_starting_outside);
  ASSERT_EQ(result.index.schema.size(), 2u);
  EXPECT_EQ(result.index.schema[0].name, "name");
  EXPECT_EQ(result.index.schema[0].type, DataType::STRING);
  EXPECT_EQ(result.index.schema[1].name, "age");
  EXPECT_EQ(result.index.schema[1].type, DataType::INT32);
}

TEST_F(CacheTestFixture, WriteAtomicTempFileCleaned) {
  auto csv_path = create_test_csv("atomic.csv", "a\n1\n");
  std::string cache_path = csv_path + ".vidx";

  CachedIndex index;
  index.num_columns = 1;
  index.total_rows = 1;
  index.sample_interval = 32;
  ColumnSchema col;
  col.name = "a";
  col.type = DataType::INT32;
  index.schema = {col};
  index.chunk_boundaries = {{2, 4}};
  ChunkMeta meta;
  meta.row_count = 1;
  index.chunk_analysis = {meta};
  index.sampled_offsets = EliasFano::encode({}, 0);

  EXPECT_TRUE(IndexCache::write_atomic(cache_path, index, csv_path));
  EXPECT_TRUE(fs::exists(cache_path));

  // Verify no .tmp files remain in the directory
  for (const auto& entry : fs::directory_iterator(tmp_dir)) {
    EXPECT_TRUE(entry.path().string().find(".tmp.") == std::string::npos)
        << "Temp file not cleaned up: " << entry.path();
  }
}

// =============================================================================
// Source changed detection
// =============================================================================

TEST_F(CacheTestFixture, SourceChangedAfterCacheWrite) {
  auto csv_path = create_test_csv("changing.csv", "a\n1\n");
  std::string cache_path = csv_path + ".vidx";

  CachedIndex index;
  index.num_columns = 1;
  index.total_rows = 1;
  index.sample_interval = 32;
  ColumnSchema col;
  col.name = "a";
  col.type = DataType::INT32;
  index.schema = {col};
  index.chunk_boundaries = {{2, 4}};
  ChunkMeta meta;
  meta.row_count = 1;
  index.chunk_analysis = {meta};
  index.sampled_offsets = EliasFano::encode({}, 0);

  EXPECT_TRUE(IndexCache::write_atomic(cache_path, index, csv_path));

  // Modify the source file
  {
    std::ofstream f(csv_path);
    f << "a\n1\n2\n3\n"; // Different content
  }

  // Load should fail due to source changed
  auto result = IndexCache::load(cache_path, csv_path);
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error, CacheError::SourceChanged);
}

// =============================================================================
// Integration: CsvReader with caching
// =============================================================================

TEST_F(CacheTestFixture, CsvReaderCacheHitProducesSameResult) {
  // Create a test file large enough to not be empty
  std::string content = "name,value\n";
  for (int i = 0; i < 100; ++i) {
    content += "item" + std::to_string(i) + "," + std::to_string(i * 10) + "\n";
  }
  auto csv_path = create_test_csv("cached.csv", content);

  // First read with cache enabled
  CsvOptions opts;
  opts.cache = CacheConfig::defaults();
  {
    CsvReader reader(opts);
    auto open_result = reader.open(csv_path);
    ASSERT_TRUE(open_result.ok);
    auto read_result = reader.read_all();
    ASSERT_TRUE(read_result.ok);
    EXPECT_EQ(read_result.value.total_rows, 100u);
    // Cache should have been written
    EXPECT_FALSE(read_result.value.cache_path.empty());
    EXPECT_TRUE(fs::exists(read_result.value.cache_path));
    EXPECT_FALSE(read_result.value.used_cache); // First read is uncached
  }

  // Second read should use cache
  {
    CsvReader reader(opts);
    auto open_result = reader.open(csv_path);
    ASSERT_TRUE(open_result.ok);
    auto read_result = reader.read_all();
    ASSERT_TRUE(read_result.ok);
    EXPECT_EQ(read_result.value.total_rows, 100u);
    EXPECT_TRUE(read_result.value.used_cache); // Should be a cache hit
  }
}

TEST_F(CacheTestFixture, CsvReaderNoCacheByDefault) {
  auto csv_path = create_test_csv("nocache.csv", "a,b\n1,2\n");

  CsvOptions opts; // No cache configured
  CsvReader reader(opts);
  auto open_result = reader.open(csv_path);
  ASSERT_TRUE(open_result.ok);
  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok);
  EXPECT_TRUE(read_result.value.cache_path.empty());
  EXPECT_FALSE(read_result.value.used_cache);
  // No .vidx file should exist
  EXPECT_FALSE(fs::exists(csv_path + ".vidx"));
}

TEST_F(CacheTestFixture, CsvReaderForceRefresh) {
  std::string content = "x,y\n";
  for (int i = 0; i < 50; ++i) {
    content += std::to_string(i) + "," + std::to_string(i * 2) + "\n";
  }
  auto csv_path = create_test_csv("refresh.csv", content);

  CsvOptions opts;
  opts.cache = CacheConfig::defaults();

  // First read: creates cache
  {
    CsvReader reader(opts);
    auto open_result = reader.open(csv_path);
    ASSERT_TRUE(open_result.ok);
    auto read_result = reader.read_all();
    ASSERT_TRUE(read_result.ok);
  }

  // Second read with force_cache_refresh: should NOT use cache
  opts.force_cache_refresh = true;
  {
    CsvReader reader(opts);
    auto open_result = reader.open(csv_path);
    ASSERT_TRUE(open_result.ok);
    auto read_result = reader.read_all();
    ASSERT_TRUE(read_result.ok);
    EXPECT_FALSE(read_result.value.used_cache);
  }
}

TEST_F(CacheTestFixture, CsvReaderCustomCacheDir) {
  auto csv_path = create_test_csv("custom.csv", "a\n1\n2\n");
  std::string cache_dir = tmp_dir + "/my_cache";

  CsvOptions opts;
  opts.cache = CacheConfig::custom(cache_dir);

  CsvReader reader(opts);
  auto open_result = reader.open(csv_path);
  ASSERT_TRUE(open_result.ok);
  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok);

  // Cache should be in custom directory
  if (!read_result.value.cache_path.empty()) {
    EXPECT_TRUE(read_result.value.cache_path.find(cache_dir) == 0);
  }
}

TEST_F(CacheTestFixture, CsvReaderStdinNoCaching) {
  // Buffer-based reading (simulating stdin) should not create cache
  std::string content = "a,b\n1,2\n3,4\n";
  AlignedBuffer buffer = AlignedBuffer::allocate(content.size());
  std::memcpy(buffer.data(), content.data(), content.size());

  CsvOptions opts;
  opts.cache = CacheConfig::defaults(); // Cache enabled but no file path

  CsvReader reader(opts);
  auto open_result = reader.open_from_buffer(std::move(buffer));
  ASSERT_TRUE(open_result.ok);
  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok);
  EXPECT_TRUE(read_result.value.cache_path.empty()); // No cache for buffer reads
  EXPECT_FALSE(read_result.value.used_cache);
}

// =============================================================================
// Sampled offsets with Elias-Fano in cache
// =============================================================================

TEST_F(CacheTestFixture, CachedIndexWithSampledOffsets) {
  auto csv_path = create_test_csv("sampled.csv", "a\n1\n2\n3\n4\n5\n");
  std::string cache_path = csv_path + ".vidx";

  CachedIndex index;
  index.header_end_offset = 2;
  index.num_columns = 1;
  index.total_rows = 5;
  index.sample_interval = 2;

  ColumnSchema col;
  col.name = "a";
  col.type = DataType::INT32;
  index.schema = {col};

  index.chunk_boundaries = {{2, 12}};
  ChunkMeta meta;
  meta.row_count = 5;
  index.chunk_analysis = {meta};

  // Sampled offsets: every 2nd row
  std::vector<uint64_t> offsets = {2, 6, 10};
  index.sampled_offsets = EliasFano::encode(offsets, 12);
  index.sample_quote_states.resize(1, 0); // 3 samples, all outside quotes

  EXPECT_TRUE(IndexCache::write_atomic(cache_path, index, csv_path));

  auto result = IndexCache::load(cache_path, csv_path);
  ASSERT_TRUE(result.ok()) << result.message;
  EXPECT_EQ(result.index.sampled_offsets.size(), 3u);
  EXPECT_EQ(result.index.sampled_offsets.select(0), 2u);
  EXPECT_EQ(result.index.sampled_offsets.select(1), 6u);
  EXPECT_EQ(result.index.sampled_offsets.select(2), 10u);
  EXPECT_EQ(result.index.sample_interval, 2u);
}
