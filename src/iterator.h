#pragma once

#include "vroom.h"

namespace vroom {

class base_iterator {
public:
  virtual void next() = 0;
  virtual void prev() = 0;
  virtual void advance(ptrdiff_t n) = 0;
  virtual bool equal_to(const base_iterator& it) const = 0;
  virtual ptrdiff_t distance_to(const base_iterator& it) const = 0;
  virtual string value() const = 0;
  virtual base_iterator* clone() const = 0;
  virtual string at(ptrdiff_t n) const = 0;
  virtual ~base_iterator() {
    SPDLOG_TRACE("{0:x}: base_iterator dtor", (size_t)this);
  }
};

class iterator {
  base_iterator* it_;

public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = string;
  using pointer = string*;
  using reference = string&;

  iterator(base_iterator* it) : it_(it) {
    SPDLOG_TRACE("{0:x}: iterator ctor", (size_t)this);
  }

  iterator& operator=(const iterator& other) {
    SPDLOG_TRACE("{0:x}: iterator assignment", (size_t)this);

    base_iterator* original = it_;
    it_ = other.it_->clone();
    delete original;

    return *this;
  }

  iterator(const iterator& other) : it_(other.it_->clone()) {
    SPDLOG_TRACE("{0:x}: iterator cctor", (size_t)this);
  }

  iterator operator++(int) { /* postfix */
    iterator copy(*this);
    it_->next();
    return copy;
  }

  iterator& operator++() /* prefix */ {
    it_->next();
    return *this;
  }

  iterator operator--(int) { /* postfix */
    iterator copy(*this);
    it_->prev();
    return copy;
  }

  iterator& operator--() /* prefix */ {
    it_->prev();
    return *this;
  }

  bool operator!=(const iterator& other) const {
    return !it_->equal_to(*other.it_);
  }

  bool operator==(const iterator& other) const {
    return it_->equal_to(*other.it_);
  }

  string operator*() const { return it_->value(); }

  iterator& operator+=(ptrdiff_t n) {
    it_->advance(n);
    return *this;
  }

  iterator& operator-=(ptrdiff_t n) {
    it_->advance(-n);
    return *this;
  }

  iterator operator+(ptrdiff_t n) const {

    SPDLOG_TRACE("{0:x}: iterator operator+({1})", (size_t)this, n);

    iterator copy(*this);
    copy.it_->advance(n);
    return copy;
  }

  iterator operator-(ptrdiff_t n) const {
    iterator copy(*this);
    copy.it_->advance(-n);
    return copy;
  }

  ptrdiff_t operator-(const iterator& other) const {
    return -it_->distance_to(*other.it_);
  }

  string operator[](ptrdiff_t n) const { return it_->at(n); }

  ~iterator() {
    SPDLOG_TRACE("{0:x}: iterator dtor", (size_t)this);
    delete it_;
  }
};

} // namespace vroom
