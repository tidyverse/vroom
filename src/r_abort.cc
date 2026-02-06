// R-compatible implementation of Highway's abort functions.
// This replaces libvroom/third_party/hwy/abort.cc to avoid calling
// abort() and stderr, which are not allowed in R packages.
// See: https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Error-signaling-1

#include "hwy/base.h"

#include <stdarg.h>
#include <stdio.h>

#include <atomic>
#include <string>

#include <cpp11/R.hpp>

namespace hwy {

namespace {

std::atomic<WarnFunc>& AtomicWarnFunc() {
  static std::atomic<WarnFunc> func;
  return func;
}

std::atomic<AbortFunc>& AtomicAbortFunc() {
  static std::atomic<AbortFunc> func;
  return func;
}

std::string GetBaseName(std::string const& file_name) {
  auto last_slash = file_name.find_last_of("/\\");
  return file_name.substr(last_slash + 1);
}

}  // namespace

HWY_DLLEXPORT WarnFunc& GetWarnFunc() {
  static WarnFunc func;
  func = AtomicWarnFunc().load();
  return func;
}

HWY_DLLEXPORT AbortFunc& GetAbortFunc() {
  static AbortFunc func;
  func = AtomicAbortFunc().load();
  return func;
}

HWY_DLLEXPORT WarnFunc SetWarnFunc(WarnFunc func) {
  return AtomicWarnFunc().exchange(func);
}

HWY_DLLEXPORT AbortFunc SetAbortFunc(AbortFunc func) {
  return AtomicAbortFunc().exchange(func);
}

HWY_DLLEXPORT void HWY_FORMAT(3, 4)
    Warn(const char* file, int line, const char* format, ...) {
  char buf[800];
  va_list args;
  va_start(args, format);
  vsnprintf(buf, sizeof(buf), format, args);
  va_end(args);

  WarnFunc handler = AtomicWarnFunc().load();
  if (handler != nullptr) {
    handler(file, line, buf);
  } else {
    // Use R's warning function instead of fprintf(stderr, ...)
    REprintf("Highway warning at %s:%d: %s\n", GetBaseName(file).data(), line, buf);
  }
}

HWY_DLLEXPORT HWY_NORETURN void HWY_FORMAT(3, 4)
    Abort(const char* file, int line, const char* format, ...) {
  char buf[800];
  va_list args;
  va_start(args, format);
  vsnprintf(buf, sizeof(buf), format, args);
  va_end(args);

  AbortFunc handler = AtomicAbortFunc().load();
  if (handler != nullptr) {
    handler(file, line, buf);
  } else {
    // Use R's error function instead of fprintf(stderr, ...)
    REprintf("Highway error at %s:%d: %s\n", GetBaseName(file).data(), line, buf);
  }

  // Use R's error function instead of abort()
  // Rf_error performs a longjmp and does not return
  Rf_error("Highway SIMD library error at %s:%d: %s", GetBaseName(file).data(), line, buf);
}

}  // namespace hwy
