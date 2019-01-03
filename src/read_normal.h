#pragma once

#include "idx.h"
#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

Rcpp::LogicalVector read_lgl(
    std::shared_ptr<std::vector<size_t> > idx,
    mio::shared_mmap_source mmap,
    R_xlen_t column,
    R_xlen_t num_columns,
    R_xlen_t skip,
    R_xlen_t num_threads);

Rcpp::IntegerVector read_fctr(
    std::shared_ptr<std::vector<size_t> > offsets,
    mio::shared_mmap_source mmap,
    R_xlen_t column,
    R_xlen_t num_columns,
    R_xlen_t skip,
    R_xlen_t num_threads);
