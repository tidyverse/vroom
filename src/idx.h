#ifndef READIDX_IDX_HEADER
#define READIDX_IDX_HEADER

#include <Rcpp.h>

// clang-format off
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
// clang-format on

std::tuple<
    std::shared_ptr<std::vector<size_t> >,
    size_t,
    mio::shared_mmap_source>
create_index(const std::string& filename, const char delim, int num_threads);

#endif /* READIDX_IDX_HEADER */
