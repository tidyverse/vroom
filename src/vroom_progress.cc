#include "vroom_progress.h"

#include <cli/progress.h>

namespace vroom {

progress_bar::progress_bar(
    bool enabled,
    progress_type type,
    double total,
    const std::string& filename)
    : enabled_(enabled),
      bar_(enabled ? cli_progress_bar(total, R_NilValue) : R_NilValue) {
  if (!enabled_) {
    return;
  }

  cli_progress_set_type(bar_, "download");

  switch (type) {
  case progress_type::file:
    cli_progress_set_format(
        bar_,
        "{.strong indexing} {.blue %s} [{cli::pb_bar}] {.green "
        "{cli::pb_rate_bytes}}, eta: {.cyan {cli::pb_eta}}",
        basename(filename).c_str());
    break;
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
}

progress_bar::~progress_bar() { done(); }

void progress_bar::tick(double increment) {
  if (enabled_) {
    cli_progress_add(bar_, increment);
  }
}

void progress_bar::done() {
  if (enabled_) {
    cli_progress_done(bar_);
    enabled_ = false;
  }
}

std::string progress_bar::basename(const std::string& path) {
  auto separator = path.find_last_of("/\\");
  return separator == std::string::npos ? path : path.substr(separator + 1);
}

} // namespace vroom
