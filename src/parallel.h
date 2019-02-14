#include <algorithm>
#include <functional>
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
    unsigned nb_elements,
    std::function<void(int start, int end, int thread_id)> functor,
    unsigned nb_threads,
    bool use_threads = true,
    bool cleanup = true) {
  // -------

  unsigned batch_size = nb_elements / nb_threads;

  unsigned batch_remainder = nb_elements % nb_threads;

  auto my_threads = std::vector<std::thread>(nb_threads);

  if (use_threads) {
    // Multithread execution
    for (unsigned i = 0; i < (nb_threads - 1); ++i) {
      int start = i * batch_size;
      my_threads[i] = std::thread(functor, start, start + batch_size, i);
    }

    // Last batch includes the remainder
    int start = (nb_threads - 1) * batch_size;
    my_threads[nb_threads - 1] = std::thread(
        functor, start, start + batch_size + batch_remainder, nb_threads - 1);
  } else {
    // Single thread execution (for easy debugging)
    for (unsigned i = 0; i < (nb_threads - 1); ++i) {
      int start = i * batch_size;
      functor(start, start + batch_size, i);
    }
    // Last batch includes the remainder
    int start = (nb_threads - 1) * batch_size;
    functor(start, start + batch_size + batch_remainder, nb_threads - 1);

    return std::vector<std::thread>();
  }

  // Wait for the other thread to finish their task
  if (use_threads && cleanup) {
    std::for_each(
        my_threads.begin(), my_threads.end(), std::mem_fn(&std::thread::join));
  }
  return my_threads;
}
