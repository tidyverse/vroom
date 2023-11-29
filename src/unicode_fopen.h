#pragma once

#include <stdio.h>
// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
#else
#include <mio/shared_mmap.hpp>
#endif
// clang-format on

#include <Rinternals.h>

#ifdef _WIN32
#include <windows.h>
#else
#include "cpp11/r_string.hpp"
#endif

// useful for print debugging file path encoding
// inline void print_hex(const char* string) {
//   unsigned char* p = (unsigned char*) string;
//   for (int i = 0; i < 300 ; i++) {
//     if (p[i] == '\0') break;
//     Rprintf("%c 0x%02x ", p[i], p[i]);
//     if ((i%16 == 0) && i)
//       Rprintf("\n");
//   }
//   Rprintf("\n");
// }

// This is needed to support wide character paths on windows
inline FILE* unicode_fopen(const char* path, const char* mode) {
  FILE* out;
#ifdef _WIN32
  // First convert the mode to the wide equivalent
  // Only usage is 2 characters so max 8 bytes + 2 byte null.
  wchar_t mode_w[10];
  MultiByteToWideChar(CP_UTF8, 0, mode, -1, mode_w, 9);

  // Then convert the path
  wchar_t* buf;
  size_t len = MultiByteToWideChar(CP_UTF8, 0, path, -1, NULL, 0);
  if (len <= 0) {
    Rf_error("Cannot convert file to Unicode: %s", path);
  }
  buf = (wchar_t*)R_alloc(len, sizeof(wchar_t));
  if (buf == NULL) {
    Rf_error("Could not allocate buffer of size: %zu", len);
  }

  MultiByteToWideChar(CP_UTF8, 0, path, -1, buf, len);
  out = _wfopen(buf, mode_w);
#else
  // the path has UTF-8 encoding, because we do that unconditionally on the R
  // side (but also because cpp11 is eager to use UTF-8)
  // however, we need to pass the path to fopen() in the native encoding
  const char* native_path = Rf_translateChar(cpp11::r_string(path));
  out = fopen(native_path, mode);
#endif

  return out;
}

inline mio::mmap_source
make_mmap_source(const char* file, std::error_code& error) {
#ifdef __WIN32
  wchar_t* buf;
  size_t len = MultiByteToWideChar(CP_UTF8, 0, file, -1, NULL, 0);
  if (len <= 0) {
    Rf_error("Cannot convert file to Unicode: %s", file);
  }
  buf = (wchar_t*)malloc(len * sizeof(wchar_t));
  if (buf == NULL) {
    Rf_error("Could not allocate buffer of size: %zu", len);
  }

  MultiByteToWideChar(CP_UTF8, 0, file, -1, buf, len);
  mio::mmap_source out = mio::make_mmap_source(buf, error);
  free(buf);
  return out;
#else
  // the path has UTF-8 encoding, because we do that unconditionally on the R
  // side (but also because cpp11 is eager to use UTF-8)
  // however, we need to pass the path to fopen() in the native encoding
  const char* native_path = Rf_translateChar(cpp11::r_string(file));
  return mio::make_mmap_source(native_path, error);
#endif
}
