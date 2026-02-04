/**
 * Test helpers for libvroom unit tests.
 *
 * Provides RAII wrappers and utilities to prevent memory leaks in test code.
 */

#ifndef LIBVROOM_TEST_HELPERS_H
#define LIBVROOM_TEST_HELPERS_H

#include "libvroom.h"

#include <string>

/**
 * RAII wrapper for exception-safe memory management of corpus data.
 *
 * Uses libvroom::load_file_to_ptr() internally which manages memory via
 * RAII (AlignedBuffer). Memory is automatically freed when the guard
 * goes out of scope, preventing memory leaks even when tests throw
 * exceptions or use early returns.
 *
 * Usage:
 *   CorpusGuard corpus("path/to/file.csv");
 *   parser.parse(corpus.data.data(), idx, corpus.data.size());
 *   // No need to manually free - automatically freed on scope exit
 */
struct CorpusGuard {
  libvroom::AlignedBuffer data;

  explicit CorpusGuard(const std::string& path)
      : data(libvroom::load_file_to_ptr(path, LIBVROOM_PADDING)) {}

  // Non-copyable, non-movable
  CorpusGuard(const CorpusGuard&) = delete;
  CorpusGuard& operator=(const CorpusGuard&) = delete;
  CorpusGuard(CorpusGuard&&) = delete;
  CorpusGuard& operator=(CorpusGuard&&) = delete;
};

#endif // LIBVROOM_TEST_HELPERS_H
