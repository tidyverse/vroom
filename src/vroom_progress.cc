#include "vroom_progress.h"

#include <cli/progress.h>
#include <cpp11/protect.hpp>
#include <R_ext/Utils.h>

namespace {

void progress_done(void* data) {
  cli_progress_done(static_cast<SEXP>(data));
}

} // namespace

namespace vroom {

progress_bar::progress_bar(
    bool enabled,
    progress_type type,
    double total,
    const std::string& filename)
    : enabled_(enabled),
      bar_(enabled
               ? cpp11::unwind_protect(
                     [&] { return cli_progress_bar(total, R_NilValue); })
               : R_NilValue) {
  if (!enabled_) {
    return;
  }

  try {
    cpp11::unwind_protect([&] {
      cli_progress_set_type(bar_, "download");

      switch (type) {
      case progress_type::file: {
        auto name = basename(filename);
        cli_progress_set_name(bar_, name.c_str());
        cli_progress_set_format(
            bar_,
            "{.strong indexing} {.blue {cli::pb_name}}[{cli::pb_bar}] "
            "{.green {cli::pb_rate_bytes}}, eta: {.cyan {cli::pb_eta}}");
        break;
      }
      case progress_type::connection:
        cli_progress_set_format(
            bar_,
            "{.strong indexed} {.green {cli::pb_current_bytes}} in {.cyan "
            "{cli::pb_elapsed}}, {.green {cli::pb_rate_bytes}}");
        break;
      case progress_type::write:
        cli_progress_set_format(
            bar_,
            "{.strong wrote} {.green {cli::pb_current_bytes}} in {.cyan "
            "{cli::pb_elapsed}}, {.green {cli::pb_rate_bytes}}");
        break;
      }

      cli_progress_set_clear(bar_, 1);
    });
  } catch (...) {
    cleanup();
    throw;
  }
}

progress_bar::~progress_bar() { cleanup(); }

bool progress_bar::should_tick() const {
  return enabled_ && CLI_SHOULD_TICK;
}

void progress_bar::tick(double increment) {
  if (!enabled_) {
    return;
  }

  try {
    cpp11::unwind_protect([&] { cli_progress_add(bar_, increment); });
  } catch (...) {
    cleanup();
    throw;
  }
}

void progress_bar::done() {
  if (!enabled_) {
    return;
  }

  enabled_ = false;
  cpp11::unwind_protect([&] { cli_progress_done(bar_); });
}

void progress_bar::cleanup() noexcept {
  if (!enabled_) {
    return;
  }

  enabled_ = false;
  static_cast<void>(
      R_ToplevelExec(progress_done, static_cast<SEXP>(bar_)));
}

std::string progress_bar::basename(const std::string& path) {
  auto separator = path.find_last_of("/\\");
  return separator == std::string::npos ? path : path.substr(separator + 1);
}

} // namespace vroom
