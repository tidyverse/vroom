#pragma once

#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

#include "idx.h"
#include "vroom_vec.h"

Rcpp::LogicalVector read_lgl(vroom_vec_info* info);

Rcpp::IntegerVector read_fctr(
    std::shared_ptr<std::vector<size_t> > offsets,
    mio::shared_mmap_source mmap,
    R_xlen_t column,
    R_xlen_t num_columns,
    R_xlen_t num_threads);
