#pragma once

#include <condition_variable>
#include <mutex>

#include <Rcpp.h>

#include "RProgress.h"

class multi_progress {
public:
  multi_progress(
      std::string format = "[:bar] :percent",
      size_t total = 100,
      int width = Rf_GetOptionWidth() - 2,
      const char complete_char = '=',
      const char incomplete_char = '-',
      bool clear = true,
      double show_after = 0.2)
      : pb_(RProgress::RProgress(
            format,
            total,
            width,
            complete_char,
            incomplete_char,
            clear,
            show_after)),
        progress_(0),
        total_progress_(0),
        total_(total) {}

  void tick(size_t progress) {
    std::lock_guard<std::mutex> guard(mutex_);
    progress_ += progress;
    mutex_.unlock();
    cv_.notify_one();
  }

  void finish() {
    std::lock_guard<std::mutex> guard(mutex_);
    total_progress_ = total_;
    mutex_.unlock();
    cv_.notify_one();
  }

  void display_progress() {
    while (total_progress_ < total_) {
      std::unique_lock<std::mutex> lk(mutex_);
      cv_.wait(lk);
      pb_.tick(progress_);
      total_progress_ += progress_;
      progress_ = 0;
    }
    pb_.update(1);
  }

private:
  RProgress::RProgress pb_;
  size_t progress_;
  size_t total_progress_;
  size_t total_;
  std::mutex mutex_;
  std::condition_variable cv_;
};
