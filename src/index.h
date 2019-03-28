#pragma once

#include "iterator.h"
#include "vroom.h"

namespace vroom {
class index {

public:
  class column {
  public:
    virtual iterator begin() const = 0;
    virtual iterator end() const = 0;
    virtual ~column() {}
  };

  class row {
  public:
    virtual iterator begin() const = 0;
    virtual iterator end() const = 0;
    virtual ~row() {}
  };

  virtual std::shared_ptr<row> get_row(size_t row) const = 0;
  virtual std::shared_ptr<row> get_header() const = 0;

  virtual std::shared_ptr<column> get_column(size_t col) const = 0;

  virtual size_t num_columns() const = 0;
  virtual size_t num_rows() const = 0;

  virtual string get(size_t row, size_t col) const = 0;
  virtual ~index() {}
};
} // namespace vroom
