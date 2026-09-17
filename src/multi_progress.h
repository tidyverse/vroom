#pragma once

#include "vroom_progress.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>

class multi_progress {
public:
  multi_progress(size_t total, const std::string& filename)
      : pb_(true, vroom::progress_type::file, total, filename),
        progress_(0),
        total_(total),
        last_progress_(0),
        last_time_(std::chrono::system_clock::now()),
        update_interval_(10) {}

  void tick(size_t progress) {
    std::lock_guard<std::mutex> guard(mutex_);
    progress_ += progress;
    cv_.notify_one();
  }

  void finish() {
    std::lock_guard<std::mutex> guard(mutex_);
    progress_ = total_;
    cv_.notify_one();
  }

  void display_progress() {
    while (true) {
      std::unique_lock<std::mutex> lk(mutex_);
      if (progress_ < total_ - 1) {
        cv_.wait(lk);
        auto now = std::chrono::system_clock::now();
        std::chrono::duration<float, std::milli> diff = now - last_time_;
        if (diff > update_interval_) {
          pb_.tick(progress_ - last_progress_);
          last_progress_ = progress_;
          last_time_ = std::chrono::system_clock::now();
        }
      } else {
        break;
      }
    }
    if (last_progress_ < total_) {
      pb_.tick(total_ - last_progress_);
    }
    pb_.done();
  }

private:
  vroom::progress_bar pb_;
  size_t progress_;
  size_t total_;
  size_t last_progress_;
  std::chrono::time_point<std::chrono::system_clock> last_time_;
  std::chrono::milliseconds update_interval_;
  std::mutex mutex_;
  std::condition_variable cv_;
};
