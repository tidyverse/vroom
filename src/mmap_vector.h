#pragma once

#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mio/shared_mmap.hpp>
#include <stdlib.h>
#include <string>

bool fallocate(int fd, size_t offset, size_t len);

class mmap_vector {
  static int num;

public:
  mmap_vector() : pos_(0), size_(0) {
    filename_ = "foo" + std::to_string(num++);
    // Truncate any existing file;
    std::ofstream ofs(
        filename_.c_str(), std::ios::binary | std::ios::out | std::ios::trunc);
    ofs.close();
    reserve(16);
  }

  void reserve(size_t size) {
    std::error_code error;

    size_t len, offset;

    if (size < size_) {
      return;
    }

    if (size_ > 0) {
      sink_.sync(error);
      if (error) {
        throw std::runtime_error(error.message());
      }
      sink_.unmap();
      close(fd_);
    }

    offset = 0;
    len = size * sizeof(size_t);

    fd_ = open(filename_.c_str(), O_RDWR);
    if (fd_ < 0) {
      throw std::runtime_error(
          std::string("open() failed: ") + strerror(errno));
    }
    fallocate(fd_, offset, len);

    sink_ = mio::make_mmap_sink(fd_, 0, len, error);

    if (error) {
      throw std::runtime_error(error.message());
    }

    size_ = size;
  }
  void push_back(size_t value) {
    if (pos_ < size_) {
      std::copy(
          (char*)&value,
          (char*)&value + sizeof(size_t),
          sink_.data() + (pos_++ * sizeof(size_t)));
      // std::error_code error;
    } else {
      reserve(size_ * 1.1);
      push_back(value);
    }
  }
  size_t pop_back() {
    --pos_;
    return 0;
  }

  size_t size() const { return pos_; }

  size_t operator[](size_t idx) const {
    size_t out;
    std::copy(
        sink_.data() + (idx * sizeof(size_t)),
        sink_.data() + ((idx + 1) * sizeof(size_t)),
        (char*)&out);
    return out;
  }

  ~mmap_vector() {
    if (fd_ >= 0) {
      close(fd_);
      unlink(filename_.c_str());
    }
  }

private:
  size_t pos_;
  size_t size_;
  int fd_;
  mio::mmap_sink sink_;
  std::string filename_;
};
