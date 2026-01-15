#include "io_util.h"

#include "encoding.h"
#include "mem_util.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>
#include <vector>

// Windows compatibility for S_ISREG macro
#ifdef _WIN32
#ifndef S_ISREG
#define S_ISREG(m) (((m) & _S_IFMT) == _S_IFREG)
#endif
#endif

uint8_t* allocate_padded_buffer(size_t length, size_t padding) {
  // Check for integer overflow before addition
  if (length > SIZE_MAX - padding) {
    return nullptr;
  }
  // we could do a simple malloc
  // return (char *) malloc(length + padding);
  // However, we might as well align to cache lines...
  size_t totalpaddedlength = length + padding;
  uint8_t* padded_buffer = (uint8_t*)aligned_malloc(64, totalpaddedlength);
  return padded_buffer;
}

std::pair<AlignedPtr, size_t> read_stdin(size_t padding) {
  // Read stdin in chunks since we don't know the size upfront
  const size_t chunk_size = 64 * 1024; // 64KB chunks
  std::vector<uint8_t> data;
  data.reserve(chunk_size * 16); // Reserve ~1MB upfront to reduce reallocations

  uint8_t buffer[chunk_size];
  while (true) {
    size_t bytes_read = std::fread(buffer, 1, chunk_size, stdin);
    if (bytes_read > 0) {
      data.insert(data.end(), buffer, buffer + bytes_read);
    }
    if (bytes_read < chunk_size) {
      if (std::ferror(stdin)) {
        throw std::runtime_error("could not read from stdin");
      }
      break; // EOF reached
    }
  }

  if (data.empty()) {
    throw std::runtime_error("no data read from stdin");
  }

  // Allocate properly aligned buffer with padding
  uint8_t* buf = allocate_padded_buffer(data.size(), padding);
  if (buf == nullptr) {
    throw std::runtime_error("could not allocate memory");
  }

  // Copy data to aligned buffer
  std::memcpy(buf, data.data(), data.size());

  return {AlignedPtr(buf), data.size()};
}

std::pair<AlignedPtr, size_t> read_file(const std::string& filename, size_t padding) {
  // Check if the path is a regular file (not a directory or special file)
  struct stat path_stat;
  if (stat(filename.c_str(), &path_stat) != 0 || !S_ISREG(path_stat.st_mode)) {
    throw std::runtime_error("could not load corpus");
  }

  std::FILE* fp = std::fopen(filename.c_str(), "rb");
  if (fp != nullptr) {
    std::fseek(fp, 0, SEEK_END);
    size_t len = std::ftell(fp);
    uint8_t* buf = allocate_padded_buffer(len, padding);
    if (buf == nullptr) {
      std::fclose(fp);
      throw std::runtime_error("could not allocate memory");
    }
    std::rewind(fp);
    size_t readb = std::fread(buf, 1, len, fp);
    std::fclose(fp);
    if (readb != len) {
      aligned_free(buf);
      throw std::runtime_error("could not read the data");
    }
    return {AlignedPtr(buf), len};
  }
  throw std::runtime_error("could not load corpus");
}

// Helper to transcode data if needed and return result
static LoadResult process_with_encoding(AlignedPtr raw_buf, size_t raw_len, size_t padding) {
  LoadResult result;

  // Detect encoding
  result.encoding = libvroom::detect_encoding(raw_buf.get(), raw_len);

  // If transcoding is needed, transcode and free original buffer
  if (result.encoding.needs_transcoding) {
    auto transcoded = libvroom::transcode_to_utf8(raw_buf.get(), raw_len, result.encoding.encoding,
                                                  result.encoding.bom_length, padding);

    // Release old buffer (will be freed by AlignedPtr going out of scope)
    raw_buf.reset();

    if (!transcoded.success) {
      throw std::runtime_error("Transcoding failed: " + transcoded.error);
    }

    result.buffer = AlignedPtr(transcoded.data);
    result.size = transcoded.length;
  } else if (result.encoding.bom_length > 0) {
    // UTF-8 with BOM - need to strip the BOM
    auto stripped = libvroom::transcode_to_utf8(raw_buf.get(), raw_len, result.encoding.encoding,
                                                result.encoding.bom_length, padding);

    // Release old buffer
    raw_buf.reset();

    if (!stripped.success) {
      throw std::runtime_error("Failed to strip BOM: " + stripped.error);
    }

    result.buffer = AlignedPtr(stripped.data);
    result.size = stripped.length;
  } else {
    // Already UTF-8 without BOM - use as-is
    result.buffer = std::move(raw_buf);
    result.size = raw_len;
  }

  return result;
}

LoadResult read_file_with_encoding(const std::string& filename, size_t padding) {
  // Check if the path is a regular file (not a directory or special file)
  struct stat path_stat;
  if (stat(filename.c_str(), &path_stat) != 0 || !S_ISREG(path_stat.st_mode)) {
    throw std::runtime_error("could not load corpus");
  }

  std::FILE* fp = std::fopen(filename.c_str(), "rb");
  if (fp == nullptr) {
    throw std::runtime_error("could not load corpus");
  }

  std::fseek(fp, 0, SEEK_END);
  size_t len = std::ftell(fp);
  uint8_t* buf = allocate_padded_buffer(len, padding);
  if (buf == nullptr) {
    std::fclose(fp);
    throw std::runtime_error("could not allocate memory");
  }

  std::rewind(fp);
  size_t readb = std::fread(buf, 1, len, fp);
  std::fclose(fp);

  if (readb != len) {
    aligned_free(buf);
    throw std::runtime_error("could not read the data");
  }

  return process_with_encoding(AlignedPtr(buf), len, padding);
}

LoadResult read_stdin_with_encoding(size_t padding) {
  // Read stdin in chunks since we don't know the size upfront
  const size_t chunk_size = 64 * 1024; // 64KB chunks
  std::vector<uint8_t> data;
  data.reserve(chunk_size * 16); // Reserve ~1MB upfront

  uint8_t buffer[chunk_size];
  while (true) {
    size_t bytes_read = std::fread(buffer, 1, chunk_size, stdin);
    if (bytes_read > 0) {
      data.insert(data.end(), buffer, buffer + bytes_read);
    }
    if (bytes_read < chunk_size) {
      if (std::ferror(stdin)) {
        throw std::runtime_error("could not read from stdin");
      }
      break; // EOF reached
    }
  }

  if (data.empty()) {
    throw std::runtime_error("no data read from stdin");
  }

  // Allocate properly aligned buffer
  uint8_t* buf = allocate_padded_buffer(data.size(), padding);
  if (buf == nullptr) {
    throw std::runtime_error("could not allocate memory");
  }

  // Copy data to aligned buffer
  std::memcpy(buf, data.data(), data.size());

  return process_with_encoding(AlignedPtr(buf), data.size(), padding);
}

// Helper to process data with a forced encoding (no auto-detection)
static LoadResult process_with_forced_encoding(AlignedPtr raw_buf, size_t raw_len, size_t padding,
                                               libvroom::Encoding forced_encoding) {
  LoadResult result;

  // Set encoding result with user-specified encoding
  result.encoding.encoding = forced_encoding;
  result.encoding.confidence = 1.0; // User-specified, so full confidence

  // UTF-8 BOM bytes: EF BB BF
  static constexpr uint8_t UTF8_BOM[] = {0xEF, 0xBB, 0xBF};

  // Handle UTF8_BOM encoding: check for and strip BOM if present
  if (forced_encoding == libvroom::Encoding::UTF8_BOM) {
    // Check if buffer actually has UTF-8 BOM
    size_t bom_len = 0;
    if (raw_len >= 3 && raw_buf.get()[0] == UTF8_BOM[0] && raw_buf.get()[1] == UTF8_BOM[1] &&
        raw_buf.get()[2] == UTF8_BOM[2]) {
      bom_len = 3;
    }

    result.encoding.bom_length = bom_len;
    result.encoding.needs_transcoding = false;

    if (bom_len > 0) {
      // Strip the BOM using transcode_to_utf8 (which handles BOM stripping)
      auto stripped =
          libvroom::transcode_to_utf8(raw_buf.get(), raw_len, forced_encoding, bom_len, padding);

      raw_buf.reset();

      if (!stripped.success) {
        throw std::runtime_error("Failed to strip BOM: " + stripped.error);
      }

      result.buffer = AlignedPtr(stripped.data);
      result.size = stripped.length;
    } else {
      // No BOM found - use as-is
      result.buffer = std::move(raw_buf);
      result.size = raw_len;
    }
    return result;
  }

  // For other encodings
  result.encoding.bom_length = 0;

  // Determine if transcoding is needed
  bool needs_transcoding = (forced_encoding != libvroom::Encoding::UTF8);
  result.encoding.needs_transcoding = needs_transcoding;

  if (needs_transcoding) {
    auto transcoded =
        libvroom::transcode_to_utf8(raw_buf.get(), raw_len, forced_encoding, 0, padding);

    raw_buf.reset();

    if (!transcoded.success) {
      throw std::runtime_error("Transcoding failed: " + transcoded.error);
    }

    result.buffer = AlignedPtr(transcoded.data);
    result.size = transcoded.length;
  } else {
    // Already UTF-8 - use as-is
    result.buffer = std::move(raw_buf);
    result.size = raw_len;
  }

  return result;
}

LoadResult read_file_with_encoding(const std::string& filename, size_t padding,
                                   libvroom::Encoding forced_encoding) {
  // Check if the path is a regular file
  struct stat path_stat;
  if (stat(filename.c_str(), &path_stat) != 0 || !S_ISREG(path_stat.st_mode)) {
    throw std::runtime_error("could not load corpus");
  }

  std::FILE* fp = std::fopen(filename.c_str(), "rb");
  if (fp == nullptr) {
    throw std::runtime_error("could not load corpus");
  }

  std::fseek(fp, 0, SEEK_END);
  size_t len = std::ftell(fp);
  uint8_t* buf = allocate_padded_buffer(len, padding);
  if (buf == nullptr) {
    std::fclose(fp);
    throw std::runtime_error("could not allocate memory");
  }

  std::rewind(fp);
  size_t readb = std::fread(buf, 1, len, fp);
  std::fclose(fp);

  if (readb != len) {
    aligned_free(buf);
    throw std::runtime_error("could not read the data");
  }

  return process_with_forced_encoding(AlignedPtr(buf), len, padding, forced_encoding);
}

LoadResult read_stdin_with_encoding(size_t padding, libvroom::Encoding forced_encoding) {
  const size_t chunk_size = 64 * 1024;
  std::vector<uint8_t> data;
  data.reserve(chunk_size * 16);

  uint8_t buffer[chunk_size];
  while (true) {
    size_t bytes_read = std::fread(buffer, 1, chunk_size, stdin);
    if (bytes_read > 0) {
      data.insert(data.end(), buffer, buffer + bytes_read);
    }
    if (bytes_read < chunk_size) {
      if (std::ferror(stdin)) {
        throw std::runtime_error("could not read from stdin");
      }
      break;
    }
  }

  if (data.empty()) {
    throw std::runtime_error("no data read from stdin");
  }

  uint8_t* buf = allocate_padded_buffer(data.size(), padding);
  if (buf == nullptr) {
    throw std::runtime_error("could not allocate memory");
  }

  std::memcpy(buf, data.data(), data.size());

  return process_with_forced_encoding(AlignedPtr(buf), data.size(), padding, forced_encoding);
}
