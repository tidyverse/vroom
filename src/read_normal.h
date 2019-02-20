#pragma once

#include <Rcpp.h>
#include <mio/shared_mmap.hpp>

#include "index_collection.h"
#include "vroom_vec.h"

Rcpp::LogicalVector read_lgl(vroom_vec_info* info);

Rcpp::IntegerVector read_fctr(vroom_vec_info* info, bool include_na);

Rcpp::NumericVector
read_date(vroom_vec_info* info, Rcpp::List locale, std::string format);

Rcpp::NumericVector
read_datetime(vroom_vec_info* info, Rcpp::List locale, std::string format);

Rcpp::NumericVector
read_time(vroom_vec_info* info, Rcpp::List locale, std::string format);
