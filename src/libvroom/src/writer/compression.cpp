#include "libvroom/vroom.h"

#include <algorithm>
#include <climits>
#include <zlib.h>
#ifdef VROOM_HAVE_ZSTD
#include <zstd.h>
#endif

#ifdef VROOM_HAVE_SNAPPY
#include <snappy.h>
#endif

#ifdef VROOM_HAVE_LZ4
#include <lz4.h>
#endif

namespace libvroom {
namespace writer {

// Note: zlib uses uInt (typically 32-bit) for buffer sizes, limiting single
// compression calls to ~4GB. This is acceptable for Parquet pages which are
// typically 1MB. LZ4 has a similar INT_MAX limitation which we explicitly check.

// Compression buffer pool for reuse across multiple compressions
// Thread-local to avoid synchronization overhead
class CompressionBufferPool {
public:
  static CompressionBufferPool& instance() {
    static thread_local CompressionBufferPool pool;
    return pool;
  }

  // Get a buffer for compression, resizing if needed
  // Returns a reference to an internal buffer to avoid allocations
  std::vector<uint8_t>& get_buffer(size_t min_size) {
    if (buffer_.capacity() < min_size) {
      // Reserve with some headroom to avoid frequent reallocations
      buffer_.reserve(min_size + min_size / 4);
    }
    buffer_.resize(min_size);
    return buffer_;
  }

  // Get raw buffer reference (for cases where we manage size externally)
  std::vector<uint8_t>& get_buffer() { return buffer_; }

private:
  std::vector<uint8_t> buffer_;
};

// Compress data using the specified codec
std::vector<uint8_t> compress(const uint8_t* data, size_t size, Compression codec, int level) {
  std::vector<uint8_t> output;

  // Handle empty input explicitly
  if (size == 0) {
    return output;
  }

  switch (codec) {
  case Compression::NONE:
    output.assign(data, data + size);
    break;

#ifdef VROOM_HAVE_ZSTD
  case Compression::ZSTD: {
    size_t max_size = ZSTD_compressBound(size);
    output.resize(max_size);

    size_t compressed_size = ZSTD_compress(output.data(), max_size, data, size, level);

    if (ZSTD_isError(compressed_size)) {
      // Compression failed - return uncompressed
      output.assign(data, data + size);
    } else {
      output.resize(compressed_size);
    }
    break;
  }
#endif

  case Compression::GZIP: {
    // Use zlib for gzip compression
    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    // windowBits = 15 + 16 for gzip format
    if (deflateInit2(&stream, level, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
      output.assign(data, data + size);
      break;
    }

    stream.next_in = const_cast<Bytef*>(data);
    stream.avail_in = static_cast<uInt>(size);

    output.resize(deflateBound(&stream, size));
    stream.next_out = output.data();
    stream.avail_out = static_cast<uInt>(output.size());

    int result = deflate(&stream, Z_FINISH);
    deflateEnd(&stream);

    if (result == Z_STREAM_END) {
      output.resize(stream.total_out);
    } else {
      output.assign(data, data + size);
    }
    break;
  }

#ifdef VROOM_HAVE_SNAPPY
  case Compression::SNAPPY: {
    size_t max_size = snappy::MaxCompressedLength(size);
    output.resize(max_size);

    size_t compressed_size;
    snappy::RawCompress(reinterpret_cast<const char*>(data), size,
                        reinterpret_cast<char*>(output.data()), &compressed_size);

    output.resize(compressed_size);
    break;
  }
#endif

#ifdef VROOM_HAVE_LZ4
  case Compression::LZ4: {
    // LZ4 requires int sizes, validate input doesn't overflow
    constexpr size_t max_lz4_size = static_cast<size_t>(INT32_MAX);
    if (size > max_lz4_size) {
      output.assign(data, data + size);
      break;
    }
    int max_size = LZ4_compressBound(static_cast<int>(size));
    output.resize(max_size);

    int compressed_size = LZ4_compress_default(reinterpret_cast<const char*>(data),
                                               reinterpret_cast<char*>(output.data()),
                                               static_cast<int>(size), max_size);

    if (compressed_size > 0) {
      output.resize(compressed_size);
    } else {
      output.assign(data, data + size);
    }
    break;
  }
#endif

  default:
    // Unknown codec - return uncompressed
    output.assign(data, data + size);
    break;
  }

  return output;
}

// Compress with buffer reuse - this is the preferred API for performance
// Uses thread-local buffer pool to avoid allocations on each compression
void compress_into(const uint8_t* data, size_t size, Compression codec, int level,
                   std::vector<uint8_t>& output) {
  // Handle empty input explicitly
  if (size == 0) {
    output.clear();
    return;
  }

  auto& pool = CompressionBufferPool::instance();

  switch (codec) {
  case Compression::NONE:
    output.assign(data, data + size);
    return;

#ifdef VROOM_HAVE_ZSTD
  case Compression::ZSTD: {
    size_t max_size = ZSTD_compressBound(size);
    auto& buffer = pool.get_buffer(max_size);

    size_t compressed_size = ZSTD_compress(buffer.data(), max_size, data, size, level);

    if (ZSTD_isError(compressed_size)) {
      output.assign(data, data + size);
    } else {
      output.assign(buffer.begin(), buffer.begin() + compressed_size);
    }
    break;
  }
#endif

  case Compression::GZIP: {
    // Use zlib for gzip compression with buffer pooling
    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    // windowBits = 15 + 16 for gzip format
    if (deflateInit2(&stream, level, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
      output.assign(data, data + size);
      return;
    }

    stream.next_in = const_cast<Bytef*>(data);
    stream.avail_in = static_cast<uInt>(size);

    size_t max_size = deflateBound(&stream, size);
    auto& buffer = pool.get_buffer(max_size);

    stream.next_out = buffer.data();
    stream.avail_out = static_cast<uInt>(buffer.size());

    int result = deflate(&stream, Z_FINISH);
    deflateEnd(&stream);

    if (result == Z_STREAM_END) {
      output.assign(buffer.begin(), buffer.begin() + stream.total_out);
    } else {
      output.assign(data, data + size);
    }
    break;
  }

#ifdef VROOM_HAVE_SNAPPY
  case Compression::SNAPPY: {
    size_t max_size = snappy::MaxCompressedLength(size);
    auto& buffer = pool.get_buffer(max_size);

    size_t compressed_size;
    snappy::RawCompress(reinterpret_cast<const char*>(data), size,
                        reinterpret_cast<char*>(buffer.data()), &compressed_size);

    output.assign(buffer.begin(), buffer.begin() + compressed_size);
    break;
  }
#endif

#ifdef VROOM_HAVE_LZ4
  case Compression::LZ4: {
    // LZ4 requires int sizes, validate input doesn't overflow
    constexpr size_t max_lz4_size = static_cast<size_t>(INT32_MAX);
    if (size > max_lz4_size) {
      output.assign(data, data + size);
      break;
    }
    int max_size = LZ4_compressBound(static_cast<int>(size));
    auto& buffer = pool.get_buffer(max_size);

    int compressed_size = LZ4_compress_default(reinterpret_cast<const char*>(data),
                                               reinterpret_cast<char*>(buffer.data()),
                                               static_cast<int>(size), max_size);

    if (compressed_size > 0) {
      output.assign(buffer.begin(), buffer.begin() + compressed_size);
    } else {
      output.assign(data, data + size);
    }
    break;
  }
#endif

  default:
    // Unknown codec - return uncompressed
    output.assign(data, data + size);
    break;
  }
}

// Compress directly into provided buffer, returning actual compressed size
// Returns 0 on failure (output contains uncompressed data in that case)
size_t compress_into_buffer(const uint8_t* data, size_t size, Compression codec, int level,
                            uint8_t* output, size_t output_capacity) {
  // Handle empty input explicitly
  if (size == 0) {
    return 0;
  }

  switch (codec) {
  case Compression::NONE:
    if (output_capacity >= size) {
      std::copy(data, data + size, output);
      return size;
    }
    return 0;

#ifdef VROOM_HAVE_ZSTD
  case Compression::ZSTD: {
    size_t compressed_size = ZSTD_compress(output, output_capacity, data, size, level);

    if (ZSTD_isError(compressed_size)) {
      return 0;
    }
    return compressed_size;
  }
#endif

  case Compression::GZIP: {
    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    if (deflateInit2(&stream, level, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
      return 0;
    }

    stream.next_in = const_cast<Bytef*>(data);
    stream.avail_in = static_cast<uInt>(size);
    stream.next_out = output;
    stream.avail_out = static_cast<uInt>(output_capacity);

    int result = deflate(&stream, Z_FINISH);
    deflateEnd(&stream);

    if (result == Z_STREAM_END) {
      return stream.total_out;
    }
    return 0;
  }

#ifdef VROOM_HAVE_SNAPPY
  case Compression::SNAPPY: {
    // Check if output capacity is sufficient
    size_t max_size = snappy::MaxCompressedLength(size);
    if (output_capacity < max_size) {
      return 0;
    }
    size_t compressed_size;
    snappy::RawCompress(reinterpret_cast<const char*>(data), size, reinterpret_cast<char*>(output),
                        &compressed_size);
    return compressed_size;
  }
#endif

#ifdef VROOM_HAVE_LZ4
  case Compression::LZ4: {
    // LZ4 requires int sizes, validate input doesn't overflow
    constexpr size_t max_lz4_size = static_cast<size_t>(INT32_MAX);
    if (size > max_lz4_size || output_capacity > max_lz4_size) {
      return 0;
    }
    int compressed_size =
        LZ4_compress_default(reinterpret_cast<const char*>(data), reinterpret_cast<char*>(output),
                             static_cast<int>(size), static_cast<int>(output_capacity));
    return compressed_size > 0 ? static_cast<size_t>(compressed_size) : 0;
  }
#endif

  default:
    return 0;
  }
}

// Get the maximum compressed size for a given codec and input size
size_t max_compressed_size(Compression codec, size_t input_size) {
  switch (codec) {
  case Compression::NONE:
    return input_size;

#ifdef VROOM_HAVE_ZSTD
  case Compression::ZSTD:
    return ZSTD_compressBound(input_size);
#endif

  case Compression::GZIP: {
    // zlib's deflateBound requires a stream, use conservative estimate
    // gzip overhead is ~18 bytes header/trailer + 5 bytes per 16KB block
    return input_size + (input_size / 16384 + 1) * 5 + 18;
  }

#ifdef VROOM_HAVE_SNAPPY
  case Compression::SNAPPY:
    return snappy::MaxCompressedLength(input_size);
#endif

#ifdef VROOM_HAVE_LZ4
  case Compression::LZ4: {
    // LZ4 requires int sizes, return input_size if it would overflow
    constexpr size_t max_lz4_size = static_cast<size_t>(INT32_MAX);
    if (input_size > max_lz4_size) {
      return input_size; // Fallback for oversized input
    }
    return LZ4_compressBound(static_cast<int>(input_size));
  }
#endif

  default:
    return input_size;
  }
}

} // namespace writer
} // namespace libvroom
