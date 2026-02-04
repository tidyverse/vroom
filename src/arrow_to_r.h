#pragma once

#include <cpp11.hpp>
#include <libvroom/arrow_column_builder.h>
#include <libvroom/types.h>
#include <memory>
#include <vector>

// Convert a vector of ArrowColumnBuilders to an R data frame (tibble)
cpp11::writable::list columns_to_r(
    const std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>& columns,
    const std::vector<libvroom::ColumnSchema>& schema,
    size_t nrows);

// Convert a single ArrowColumnBuilder to an R SEXP based on its type
SEXP column_to_r(const libvroom::ArrowColumnBuilder& column, size_t nrows);
