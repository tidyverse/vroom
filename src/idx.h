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
create_index(const char* filename, char delim, int num_threads);

std::tuple<
    std::shared_ptr<std::vector<size_t> >,
    size_t,
    mio::shared_mmap_source>
create_index_connection(
    SEXP con,
    const std::string& out_file,
    const char delim,
    R_xlen_t chunk_size = 64 * 1024);

#endif /* READIDX_IDX_HEADER */
