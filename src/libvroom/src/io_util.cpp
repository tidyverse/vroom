/**
 * @file io_util.cpp
 * @brief File I/O utilities implementation.
 */

#include "libvroom/io_util.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

namespace libvroom {

AlignedBuffer load_file_to_ptr(const std::string& filename, size_t padding) {
  // Open file
  std::ifstream file(filename, std::ios::binary | std::ios::ate);
  if (!file.is_open()) {
    throw std::runtime_error("Could not open file: " + filename);
  }

  // Get file size
  std::streamsize size = file.tellg();
  if (size < 0) {
    throw std::runtime_error("Could not determine file size: " + filename);
  }
  file.seekg(0, std::ios::beg);

  // Allocate aligned buffer
  AlignedBuffer buf = AlignedBuffer::allocate(static_cast<size_t>(size), padding);

  // Read file contents
  if (size > 0 && !file.read(reinterpret_cast<char*>(buf.data()), size)) {
    throw std::runtime_error("Could not read file: " + filename);
  }

  return buf;
}

AlignedBuffer read_stdin_to_ptr(size_t padding) {
  // Read all of stdin into a stringstream first (since we don't know the size)
  std::stringstream ss;
  ss << std::cin.rdbuf();
  std::string content = ss.str();

  if (std::cin.bad()) {
    throw std::runtime_error("Error reading from stdin");
  }

  // Allocate aligned buffer and copy
  AlignedBuffer buf = AlignedBuffer::allocate(content.size(), padding);
  std::memcpy(buf.data(), content.data(), content.size());

  return buf;
}

} // namespace libvroom
