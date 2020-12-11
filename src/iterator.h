#pragma once

#include "vroom.h"

#include <stddef.h>

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
  virtual ~base_iterator() {}
  virtual std::string filename() const = 0;
  virtual size_t index() const = 0;
  virtual size_t position() const = 0;
};

class iterator {
  base_iterator* it_;

public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = string;
  using pointer = string*;
  using reference = string&;

  iterator() : it_(nullptr){};

  iterator(base_iterator* it) : it_(it) {}

  iterator& operator=(const iterator& other) {

    base_iterator* original = it_;
    it_ = other.it_->clone();
    delete original;

    return *this;
  }

  iterator(const iterator& other) : it_(other.it_->clone()) {}

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

  std::string filename() const { return it_->filename(); }

  size_t index() const { return it_->index(); }

  size_t position() const { return it_->position(); }

  ~iterator() {
    if (it_ != nullptr) {
      delete it_;
    }
  }
};

} // namespace vroom
