/**
 * @file mmap_util.cpp
 * @brief Implementation of cross-platform memory-mapped file utilities.
 */

#include "mmap_util.h"

namespace libvroom {

MmapBuffer::MmapBuffer(MmapBuffer&& other) noexcept
    : data_(other.data_), size_(other.size_)
#ifdef _WIN32
      ,
      file_handle_(other.file_handle_), map_handle_(other.map_handle_)
#else
      ,
      fd_(other.fd_)
#endif
{
  other.data_ = nullptr;
  other.size_ = 0;
#ifdef _WIN32
  other.file_handle_ = INVALID_HANDLE_VALUE;
  other.map_handle_ = nullptr;
#else
  other.fd_ = -1;
#endif
}

MmapBuffer& MmapBuffer::operator=(MmapBuffer&& other) noexcept {
  if (this != &other) {
    unmap();
    data_ = other.data_;
    size_ = other.size_;
#ifdef _WIN32
    file_handle_ = other.file_handle_;
    map_handle_ = other.map_handle_;
    other.file_handle_ = INVALID_HANDLE_VALUE;
    other.map_handle_ = nullptr;
#else
    fd_ = other.fd_;
    other.fd_ = -1;
#endif
    other.data_ = nullptr;
    other.size_ = 0;
  }
  return *this;
}

bool MmapBuffer::open(const std::string& path) {
  unmap();

#ifdef _WIN32
  file_handle_ = CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING,
                             FILE_ATTRIBUTE_NORMAL, nullptr);
  if (file_handle_ == INVALID_HANDLE_VALUE) {
    return false;
  }

  LARGE_INTEGER file_size;
  if (!GetFileSizeEx(file_handle_, &file_size)) {
    CloseHandle(file_handle_);
    file_handle_ = INVALID_HANDLE_VALUE;
    return false;
  }
  size_ = static_cast<size_t>(file_size.QuadPart);

  // Cannot mmap empty files
  if (size_ == 0) {
    CloseHandle(file_handle_);
    file_handle_ = INVALID_HANDLE_VALUE;
    return false;
  }

  map_handle_ = CreateFileMappingA(file_handle_, nullptr, PAGE_READONLY, 0, 0, nullptr);
  if (!map_handle_) {
    CloseHandle(file_handle_);
    file_handle_ = INVALID_HANDLE_VALUE;
    return false;
  }

  data_ = MapViewOfFile(map_handle_, FILE_MAP_READ, 0, 0, 0);
  if (!data_) {
    CloseHandle(map_handle_);
    CloseHandle(file_handle_);
    map_handle_ = nullptr;
    file_handle_ = INVALID_HANDLE_VALUE;
    return false;
  }
#else
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) {
    return false;
  }

  struct stat st;
  if (fstat(fd_, &st) != 0) {
    ::close(fd_);
    fd_ = -1;
    return false;
  }

  // Cannot mmap empty files
  if (st.st_size == 0) {
    ::close(fd_);
    fd_ = -1;
    return false;
  }

  size_ = static_cast<size_t>(st.st_size);

  data_ = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
  if (data_ == MAP_FAILED) {
    data_ = nullptr;
    ::close(fd_);
    fd_ = -1;
    return false;
  }
#endif
  return true;
}

void MmapBuffer::unmap() {
#ifdef _WIN32
  if (data_) {
    UnmapViewOfFile(data_);
  }
  if (map_handle_) {
    CloseHandle(map_handle_);
  }
  if (file_handle_ != INVALID_HANDLE_VALUE) {
    CloseHandle(file_handle_);
  }
  file_handle_ = INVALID_HANDLE_VALUE;
  map_handle_ = nullptr;
#else
  if (data_) {
    munmap(data_, size_);
  }
  if (fd_ >= 0) {
    ::close(fd_);
  }
  fd_ = -1;
#endif
  data_ = nullptr;
  size_ = 0;
}

SourceMetadata SourceMetadata::from_file(const std::string& path) {
  SourceMetadata meta;

#ifdef _WIN32
  WIN32_FILE_ATTRIBUTE_DATA attr;
  if (GetFileAttributesExA(path.c_str(), GetFileExInfoStandard, &attr)) {
    // Check if it's a regular file (not a directory)
    if (!(attr.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
      // Convert FILETIME to seconds since epoch
      // FILETIME is 100-nanosecond intervals since January 1, 1601
      ULARGE_INTEGER ull;
      ull.LowPart = attr.ftLastWriteTime.dwLowDateTime;
      ull.HighPart = attr.ftLastWriteTime.dwHighDateTime;
      // Convert to Unix epoch (seconds since 1970-01-01)
      // 11644473600 seconds between 1601-01-01 and 1970-01-01
      meta.mtime = (ull.QuadPart / 10000000ULL) - 11644473600ULL;

      ULARGE_INTEGER size;
      size.LowPart = attr.nFileSizeLow;
      size.HighPart = attr.nFileSizeHigh;
      meta.size = size.QuadPart;

      meta.valid = true;
    }
  }
#else
  struct stat st;
  if (stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode)) {
    meta.mtime = static_cast<uint64_t>(st.st_mtime);
    meta.size = static_cast<uint64_t>(st.st_size);
    meta.valid = true;
  }
#endif

  return meta;
}

std::string get_cache_path(const std::string& source_path) {
  return source_path + ".vidx";
}

} // namespace libvroom
