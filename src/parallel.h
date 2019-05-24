#pragma once

#include <algorithm>
#include <functional>
#include <future>
#include <thread>
#include <vector>

// adapted from https://stackoverflow.com/a/49188371/2055486

/// @param[in] nb_elements : size of your for loop
/// @param[in] functor(start, end) :
/// your function processing a sub chunk of the for loop.
/// "start" is the first index to process (included) until the index "end"
/// (excluded)
/// @code
///     for(int i = start; i < end; ++i)
///         computation(i);
/// @endcode
/// @param use_threads : enable / disable threads.
///
///
static std::vector<std::thread> parallel_for(
    size_t nb_elements,
    std::function<void(size_t start, size_t end, size_t thread_id)> functor,
    size_t nb_threads,
    bool use_threads = true,
    bool cleanup = true) {
  // -------

  size_t batch_size = nb_elements / nb_threads;

  size_t batch_remainder = nb_elements % nb_threads;

  auto my_threads = std::vector<std::thread>(nb_threads);

  if (use_threads) {
    // Multithread execution
    for (size_t i = 0; i < (nb_threads - 1); ++i) {
      size_t start = i * batch_size;
      my_threads[i] = std::thread(functor, start, start + batch_size, i);
    }

    // Last batch includes the remainder
    size_t start = (nb_threads - 1) * batch_size;
    my_threads[nb_threads - 1] = std::thread(
        functor, start, start + batch_size + batch_remainder, nb_threads - 1);
  } else {
    // Single thread execution (for easy debugging)
    for (size_t i = 0; i < (nb_threads - 1); ++i) {
      size_t start = i * batch_size;
      functor(start, start + batch_size, i);
    }
    // Last batch includes the remainder
    size_t start = (nb_threads - 1) * batch_size;
    functor(start, start + batch_size + batch_remainder, nb_threads - 1);

    return std::vector<std::thread>();
  }

  // Wait for the other thread to finish their task
  if (use_threads && cleanup) {
    for (auto& t : my_threads) {
      t.join();
    }
  }
  return my_threads;
}
