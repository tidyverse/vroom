#include "libvroom/vroom.h"

#ifdef _WIN32

// =============================================================================
// Windows implementation using CreateFileMapping / MapViewOfFile
// =============================================================================

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>

namespace libvroom {

struct MmapSource::Impl {
  HANDLE file_handle = INVALID_HANDLE_VALUE;
  HANDLE mapping_handle = nullptr;
  const char* data = nullptr;
  size_t size = 0;
};

MmapSource::MmapSource() : impl_(std::make_unique<Impl>()) {}

MmapSource::~MmapSource() {
  close();
}

Result<bool> MmapSource::open(const std::string& path) {
  if (impl_->file_handle != INVALID_HANDLE_VALUE) {
    close();
  }

  impl_->file_handle = CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr,
                                   OPEN_EXISTING, FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
  if (impl_->file_handle == INVALID_HANDLE_VALUE) {
    return Result<bool>::failure("Failed to open file: " + path);
  }

  LARGE_INTEGER file_size;
  if (!GetFileSizeEx(impl_->file_handle, &file_size)) {
    CloseHandle(impl_->file_handle);
    impl_->file_handle = INVALID_HANDLE_VALUE;
    return Result<bool>::failure("Failed to get file size: " + path);
  }

  impl_->size = static_cast<size_t>(file_size.QuadPart);

  if (impl_->size == 0) {
    // Empty file - no need to map
    return Result<bool>::success(true);
  }

  impl_->mapping_handle =
      CreateFileMappingA(impl_->file_handle, nullptr, PAGE_READONLY, 0, 0, nullptr);
  if (impl_->mapping_handle == nullptr) {
    CloseHandle(impl_->file_handle);
    impl_->file_handle = INVALID_HANDLE_VALUE;
    return Result<bool>::failure("Failed to create file mapping: " + path);
  }

  void* ptr = MapViewOfFile(impl_->mapping_handle, FILE_MAP_READ, 0, 0, 0);
  if (ptr == nullptr) {
    CloseHandle(impl_->mapping_handle);
    impl_->mapping_handle = nullptr;
    CloseHandle(impl_->file_handle);
    impl_->file_handle = INVALID_HANDLE_VALUE;
    return Result<bool>::failure("Failed to map view of file: " + path);
  }

  impl_->data = static_cast<const char*>(ptr);
  return Result<bool>::success(true);
}

const char* MmapSource::data() const {
  return impl_->data;
}

size_t MmapSource::size() const {
  return impl_->size;
}

bool MmapSource::is_open() const {
  return impl_->file_handle != INVALID_HANDLE_VALUE;
}

void MmapSource::close() {
  if (impl_->data != nullptr) {
    UnmapViewOfFile(impl_->data);
    impl_->data = nullptr;
  }
  if (impl_->mapping_handle != nullptr) {
    CloseHandle(impl_->mapping_handle);
    impl_->mapping_handle = nullptr;
  }
  if (impl_->file_handle != INVALID_HANDLE_VALUE) {
    CloseHandle(impl_->file_handle);
    impl_->file_handle = INVALID_HANDLE_VALUE;
  }
  impl_->size = 0;
}

} // namespace libvroom

#else // POSIX

// =============================================================================
// POSIX implementation using mmap / munmap
// =============================================================================

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace libvroom {

struct MmapSource::Impl {
  int fd = -1;
  const char* data = nullptr;
  size_t size = 0;
};

MmapSource::MmapSource() : impl_(std::make_unique<Impl>()) {}

MmapSource::~MmapSource() {
  close();
}

Result<bool> MmapSource::open(const std::string& path) {
  if (impl_->fd >= 0) {
    close();
  }

  impl_->fd = ::open(path.c_str(), O_RDONLY);
  if (impl_->fd < 0) {
    return Result<bool>::failure("Failed to open file: " + path);
  }

  struct stat st;
  if (fstat(impl_->fd, &st) < 0) {
    ::close(impl_->fd);
    impl_->fd = -1;
    return Result<bool>::failure("Failed to stat file: " + path);
  }

  impl_->size = static_cast<size_t>(st.st_size);

  if (impl_->size == 0) {
    // Empty file - no need to mmap
    return Result<bool>::success(true);
  }

  void* ptr = mmap(nullptr, impl_->size, PROT_READ, MAP_PRIVATE, impl_->fd, 0);
  if (ptr == MAP_FAILED) {
    ::close(impl_->fd);
    impl_->fd = -1;
    return Result<bool>::failure("Failed to mmap file: " + path);
  }

  impl_->data = static_cast<const char*>(ptr);

  // Advise kernel that we'll read sequentially
  madvise(const_cast<char*>(impl_->data), impl_->size, MADV_SEQUENTIAL);

  return Result<bool>::success(true);
}

const char* MmapSource::data() const {
  return impl_->data;
}

size_t MmapSource::size() const {
  return impl_->size;
}

bool MmapSource::is_open() const {
  return impl_->fd >= 0;
}

void MmapSource::close() {
  if (impl_->data != nullptr && impl_->size > 0) {
    munmap(const_cast<char*>(impl_->data), impl_->size);
    impl_->data = nullptr;
  }
  if (impl_->fd >= 0) {
    ::close(impl_->fd);
    impl_->fd = -1;
  }
  impl_->size = 0;
}

} // namespace libvroom

#endif // _WIN32
