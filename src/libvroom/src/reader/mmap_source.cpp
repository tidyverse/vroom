#include "libvroom/vroom.h"

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
