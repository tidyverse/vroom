#ifndef READIDX_IDX_HEADER
#define READIDX_IDX_HEADER

#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

std::tuple<
    std::shared_ptr<std::vector<size_t> >,
    size_t,
    mio::shared_mmap_source>
create_index(const std::string& filename);

#endif /* READIDX_IDX_HEADER */
