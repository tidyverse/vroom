#pragma once

// clang-format off
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wsign-compare"
#include <mio/shared_mmap.hpp>
# pragma clang diagnostic pop
#else
#include <mio/shared_mmap.hpp>
#endif
// clang-format on

#include "index.h"
#include "vroom_errors.h"

#include <libvroom.h>

namespace vroom {

/**
 * @brief Adapter class that wraps libvroom's Parser to implement vroom's index interface.
 *
 * This allows vroom's ALTREP vectors and column iteration to work with libvroom's
 * SIMD-optimized CSV parsing backend.
 */
class libvroom_index : public index,
                       public std::enable_shared_from_this<libvroom_index> {
public:
  /**
   * @brief Construct a libvroom_index from a file.
   *
   * @param filename Path to the CSV file
   * @param delim Delimiter character (nullptr for auto-detect)
   * @param quote Quote character
   * @param trim_ws Whether to trim whitespace
   * @param escape_double Whether quotes are escaped by doubling
   * @param escape_backslash Whether backslash escaping is used
   * @param has_header Whether the file has a header row
   * @param skip Number of lines to skip at the start
   * @param n_max Maximum number of rows to read
   * @param comment Comment character prefix
   * @param skip_empty_rows Whether to skip empty rows
   * @param errors Error collector for parse errors
   * @param num_threads Number of threads for parsing
   * @param progress Whether to show progress
   */
  libvroom_index(
      const char* filename,
      const char* delim,
      char quote,
      bool trim_ws,
      bool escape_double,
      bool escape_backslash,
      bool has_header,
      size_t skip,
      size_t n_max,
      const char* comment,
      bool skip_empty_rows,
      std::shared_ptr<vroom_errors> errors,
      size_t num_threads,
      bool progress);

  // Default constructor for empty index
  libvroom_index() : rows_(0), columns_(0), has_header_(false) {}

  /**
   * @brief Column iterator for ALTREP vector access.
   */
  class column_iterator : public base_iterator {
    std::shared_ptr<const libvroom_index> idx_;
    size_t column_;
    size_t row_;  // Current row (0-based, data rows only)

  public:
    column_iterator(std::shared_ptr<const libvroom_index> idx, size_t column)
        : idx_(idx), column_(column), row_(0) {}

    void next() override { ++row_; }
    void advance(ptrdiff_t n) override { row_ += n; }

    bool equal_to(const base_iterator& it) const override {
      return row_ == static_cast<const column_iterator*>(&it)->row_;
    }

    ptrdiff_t distance_to(const base_iterator& it) const override {
      return static_cast<ptrdiff_t>(static_cast<const column_iterator*>(&it)->row_) -
             static_cast<ptrdiff_t>(row_);
    }

    string value() const override;
    column_iterator* clone() const override { return new column_iterator(*this); }
    string at(ptrdiff_t n) const override;
    std::string filename() const override { return idx_->filename_; }
    size_t index() const override { return row_; }
    size_t position() const override;

    virtual ~column_iterator() = default;
  };

  /**
   * @brief Row iterator for accessing fields within a row.
   */
  class row_iterator : public base_iterator {
    std::shared_ptr<const libvroom_index> idx_;
    size_t row_;
    size_t col_;  // Current column within the row

  public:
    row_iterator(std::shared_ptr<const libvroom_index> idx, size_t row)
        : idx_(idx), row_(row), col_(0) {}

    void next() override { ++col_; }
    void advance(ptrdiff_t n) override { col_ += n; }

    bool equal_to(const base_iterator& it) const override {
      return col_ == static_cast<const row_iterator*>(&it)->col_;
    }

    ptrdiff_t distance_to(const base_iterator& it) const override {
      return static_cast<ptrdiff_t>(static_cast<const row_iterator*>(&it)->col_) -
             static_cast<ptrdiff_t>(col_);
    }

    string value() const override;
    row_iterator* clone() const override { return new row_iterator(*this); }
    string at(ptrdiff_t n) const override;
    std::string filename() const override { return idx_->filename_; }
    size_t index() const override { return col_; }
    size_t position() const override;

    virtual ~row_iterator() = default;
  };

  // vroom::index interface implementation
  string get(size_t row, size_t col) const override;
  size_t num_columns() const override { return columns_; }
  size_t num_rows() const override { return rows_; }
  std::string get_delim() const override { return delim_; }

  std::shared_ptr<vroom::index::column> get_column(size_t column) const override {
    auto begin = new column_iterator(shared_from_this(), column);
    auto end = new column_iterator(shared_from_this(), column);
    end->advance(num_rows());
    return std::make_shared<vroom::index::column>(begin, end, column);
  }

  std::shared_ptr<vroom::index::row> get_row(size_t row) const override {
    auto begin = new row_iterator(shared_from_this(), row);
    auto end = new row_iterator(shared_from_this(), row);
    end->advance(num_columns());
    return std::make_shared<vroom::index::row>(begin, end, row);
  }

  std::shared_ptr<vroom::index::row> get_header() const override {
    // For header, we use row -1 which maps to row 0 in libvroom when has_header is true
    auto begin = new row_iterator(shared_from_this(), static_cast<size_t>(-1));
    auto end = new row_iterator(shared_from_this(), static_cast<size_t>(-1));
    end->advance(num_columns());
    return std::make_shared<vroom::index::row>(begin, end, 0);
  }

  std::string filename() const { return filename_; }

private:
  // File data
  std::string filename_;
  libvroom::FileBuffer buffer_;
  mio::mmap_source mmap_;  // For direct memory access to field data

  // Parsed result
  libvroom::Parser::Result result_;

  // Cached metadata
  std::string delim_;
  size_t rows_;
  size_t columns_;
  bool has_header_;
  char quote_;
  bool trim_ws_;
  bool escape_double_;
  bool escape_backslash_;

  // Cached header values for efficient header access
  std::vector<std::string> headers_;

  // Helper to get string view for a cell
  std::string_view get_field(size_t row, size_t col) const;

  // Helper to get header value
  std::string get_header_field(size_t col) const;

  // Helper to get field with quote/escape processing
  string get_processed_field(size_t row, size_t col) const;
};

} // namespace vroom
