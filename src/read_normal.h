#pragma once

#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

#include "index.h"
#include "vroom_vec.h"

Rcpp::LogicalVector read_lgl(vroom_vec_info* info);

Rcpp::IntegerVector read_fctr(vroom_vec_info* info);
