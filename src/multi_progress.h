#pragma once

#include <condition_variable>
#include <mutex>

#ifdef VROOM_STANDALONE

// A stub class that doesn't do anything

namespace RProgress {

class RProgress {
public:
  RProgress(
      std::string format = "[:bar] :percent",
      double total = 100,
      int width = 80 - 2,
      char complete_char = '=',
      char incomplete_char = '-',
      bool clear = true,
      double show_after = 0.2) {}

  void update(double) {}
  void tick(double) {}
};

} // namespace RProgress

#else

#include "Rcpp.h"

#include "RProgress.h"

#endif

class multi_progress {
public:
  multi_progress(
      std::string format = "[:bar] :percent",
      size_t total = 100,
      int width = 78,
      const char* complete_char = "=",
      const char* incomplete_char = "-",
      bool clear = true,
      double show_after = 0.2)
      : pb_(new RProgress::RProgress(
            format,
            total,
            width,
            complete_char,
            complete_char,
            incomplete_char,
            clear,
            show_after)),
        progress_(0),
        total_(total),
        last_progress_(0),
        last_time_(std::chrono::system_clock::now()),
        update_interval_(10) {
    pb_->set_reverse(false);
  }

  void tick(size_t progress) {
    std::lock_guard<std::mutex> guard(mutex_);
    progress_ += progress;
    mutex_.unlock();
    cv_.notify_one();
  }

  void finish() {
    std::lock_guard<std::mutex> guard(mutex_);
    progress_ = total_;
    mutex_.unlock();
    cv_.notify_one();
  }

  void display_progress() {
    while (true) {
      std::unique_lock<std::mutex> lk(mutex_);
      if (progress_ < total_) {
        cv_.wait(lk);
        auto now = std::chrono::system_clock::now();
        std::chrono::duration<float, std::milli> diff = now - last_time_;
        if (diff > update_interval_) {
          pb_->tick(progress_ - last_progress_);
          last_progress_ = progress_;
          last_time_ = std::chrono::system_clock::now();
        }
      } else {
        break;
      }
    }
    pb_->update(1);
  }

private:
  std::unique_ptr<RProgress::RProgress> pb_;
  size_t progress_;
  size_t total_;
  size_t last_progress_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
  std::chrono::milliseconds update_interval_;
  std::mutex mutex_;
  std::condition_variable cv_;
};
