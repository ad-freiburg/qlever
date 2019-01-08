// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include "../global/Id.h"

template <int COLS = 0>
class IdTableStatic;
using IdTable = IdTableStatic<>;

template <>
class IdTableStatic<0> {
 private:
  static constexpr float GROWTH_FACTOR = 1.5;

 public:
  class iterator {
   public:
    iterator(Id* data, size_t row, size_t cols)
        : _data(data), _row(row), _cols(cols) {}
    virtual ~iterator() {}

    // Copy and move constructors and assignment operators
    iterator(const iterator& other)
        : _data(other._data), _row(other._row), _cols(other._cols) {}
    iterator(iterator&& other)
        : _data(other._data), _row(other._row), _cols(other._cols) {}
    iterator& operator=(const iterator& other) {
      new (this) iterator(other);
      return *this;
    }
    iterator& operator=(iterator&& other) {
      new (this) iterator(other);
      return *this;
    }

    // prefix increment
    iterator& operator++() {
      ++_row;
      return *this;
    }

    // postfix increment
    iterator operator++(int) {
      iterator tmp(*this);
      ++_row;
      return tmp;
    }

    // prefix increment
    iterator& operator--() {
      --_row;
      return *this;
    }

    // postfix increment
    iterator operator--(int) {
      iterator tmp(*this);
      --_row;
      return tmp;
    }

    iterator operator+(size_t i) { return iterator(_data, _row + i, _cols); }
    iterator operator-(size_t i) { return iterator(_data, _row - i, _cols); }

    bool operator==(iterator const& other) const {
      return _data == other._data && _row == other._row && _cols == other._cols;
    }

    bool operator!=(iterator const& other) const {
      return _data != other._data || _row != other._row || _cols != other._cols;
    }

    Id* operator*() { return _data + _row * _cols; }
    const Id* operator*() const { return _data + _row * _cols; }

    Id& operator[](size_t i) { return *(_data + _row * _cols + i); }
    const Id& operator[](size_t i) const { return *(_data + _row * _cols + i); }

    size_t row() const { return _row; }

   private:
    Id* _data;
    size_t _row;
    size_t _cols;
  };

  using const_iterator = const iterator;

  IdTableStatic(size_t cols)
      : _data(nullptr), _size(0), _cols(cols), _capacity(0) {}
  virtual ~IdTableStatic() { delete[] _data; }

  // Copy constructor
  IdTableStatic<0>(const IdTableStatic<0>& other)
      : _data(other._data),
        _size(other._size),
        _cols(other._cols),
        _capacity(other._capacity) {
    if (other._data != nullptr) {
      _data = new Id[_capacity * _cols];
      std::memcpy(_data, other._data, _size * sizeof(Id) * _cols);
    }
  }

  // Move constructor
  IdTableStatic<0>(IdTableStatic<0>&& other)
      : _data(other._data),
        _size(other._size),
        _cols(other._cols),
        _capacity(other._capacity) {
    other._data = nullptr;
    other._size = 0;
    other._capacity = 0;
  }

  // copy assignment
  IdTableStatic<0>& operator=(const IdTableStatic<0>& other) {
    delete[] _data;
    // Use a placement new to call the copy constructor
    new (this) IdTableStatic<0>(other);
    return *this;
  }

  // move assignment
  IdTableStatic<0>& operator=(IdTableStatic<0>&& other) {
    delete[] _data;
    // Use a placement new to call the move constructor
    new (this) IdTableStatic<0>(other);
    return *this;
  }

  // Element access
  Id& operator()(size_t row, size_t col) { return _data[row * _cols + col]; }
  const Id& operator()(size_t row, size_t col) const {
    return _data[row * _cols + col];
  }

  // Row access
  iterator operator[](size_t row) { return iterator(_data, row, _cols); }
  const_iterator operator[](size_t row) const {
    return iterator(_data, row, _cols);
  }

  // Begin iterator
  iterator begin() { return iterator(_data, 0, _cols); }
  const_iterator begin() const { return iterator(_data, 0, _cols); }
  const_iterator cbegin() const { return iterator(_data, 0, _cols); }

  // End iterator
  iterator end() { return iterator(_data, _size, _cols); }
  const_iterator end() const { return iterator(_data, _size, _cols); }
  const_iterator cend() const { return iterator(_data, _size, _cols); }

  // Size access
  size_t rows() const { return _size; }
  size_t cols() const { return _cols; }

  size_t size() const { return _size; }

  void push_back() {
    if (_size + 1 >= _capacity) {
      grow();
    }
    _size++;
  }

  void push_back(const std::initializer_list<Id>& init) {
    assert(init.size() == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * _cols, init.begin(), sizeof(Id) * _cols);
    _size++;
  }

  /**
   * @brief Erases all rows in the range [begin;end)
   **/
  void erase(const_iterator& begin,
             const_iterator& end = iterator(nullptr, 0, 0)) {
    iterator actualEnd = end;
    if (actualEnd == iterator(nullptr, 0, 0)) {
      actualEnd = iterator(_data, begin.row() + 1, _cols);
    }
    if (actualEnd.row() > IdTableStatic<0>::end().row()) {
      actualEnd = IdTableStatic<0>::end();
    }
    if (actualEnd.row() <= begin.row()) {
      return;
    }
    size_t numErased = actualEnd.row() - begin.row();
    // Move the elements from actualEnd() to end() forward
    for (size_t row = begin.row(); row + numErased < _size; row++) {
      // copy a single row
      std::memcpy(_data + row * _cols, _data + (row + numErased) * _cols,
                  sizeof(Id) * _cols);
    }
    _size -= numErased;
  }

  /**
   * @brief Ensures this table has enough space allocated to store rows many
   *        rows of data
   **/
  void reserve(size_t rows) {
    if (_capacity < rows) {
      // Add rows - _capacity many new rows
      grow(rows - _capacity);
    }
  }

  /**
   * @brief Resizes this IdTableStatic to have at least row many rows. This
   *method never makes the IdTableStatic smaller.
   **/
  void resize(size_t rows) {
    if (rows > _size) {
      reserve(rows);
      _size = rows;
    }
  }

  /**
   * Returns a dyamically sized IdTableStatic that now owns this tables data
   **/
  template <int COLS>
  IdTableStatic<COLS> moveToStatic() {
    IdTableStatic<COLS> tmp();
    tmp._data = _data;
    tmp._size = _size;
    tmp._capacity = _capacity;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    return tmp;
  };

  /**
   * Returns a dyamically sized IdTableStatic that conatains a copy of this
   *tables data
   **/
  template <int COLS>
  IdTableStatic<COLS> copyToStatic() const {
    IdTableStatic<COLS> tmp();
    tmp._data = new Id[_capacity * COLS];
    std::memcpy(tmp._data, _data, sizeof(Id) * _size * COLS);
    tmp._size = _size;
    tmp._capacity = _capacity;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    return tmp;
  };

 private:
  /**
   * @brief Grows the storage of this IdTableStatic
   * @param newRows If newRows is 0 the storage is grown by GROWTH_FACTOR. If
   *        newRows is any other number the vector is grown by newRows many
   *        rows.
   **/
  void grow(size_t newRows = 0) {
    // If _data is nullptr so is _size which makes this safe
    Id* tmp = _data;
    size_t new_capacity;
    if (newRows == 0) {
      new_capacity = _capacity * GROWTH_FACTOR + 1;
    } else {
      new_capacity = _capacity + newRows;
    }
    _data = new Id[new_capacity * _cols];
    // copy over the old data
    std::memcpy(_data, tmp, sizeof(Id) * _size * _cols);
    _capacity = new_capacity;
    // free the old data if nobody is using it anymore
    delete[] tmp;
  }

  Id* _data;
  size_t _size;
  size_t _cols;
  size_t _capacity;
};

template <int COLS>
class IdTableStatic {
 private:
  static constexpr float GROWTH_FACTOR = 1.5;

 public:
  class iterator {
   public:
    iterator(Id* data, size_t row) : _data(data), _row(row) {}
    virtual ~iterator() {}

    // Copy and move constructors and assignment operators
    iterator(const iterator& other) : _data(other._data), _row(other._row) {}
    iterator(iterator&& other) : _data(other._data), _row(other._row) {}
    iterator& operator=(const iterator& other) {
      new (this) iterator(other);
      return *this;
    }
    iterator& operator=(iterator&& other) {
      new (this) iterator(other);
      return *this;
    }

    // prefix increment
    iterator& operator++() {
      ++_row;
      return *this;
    }

    // postfix increment
    iterator operator++(int) {
      iterator tmp(*this);
      ++_row;
      return tmp;
    }

    // prefix increment
    iterator& operator--() {
      --_row;
      return *this;
    }

    // postfix increment
    iterator operator--(int) {
      iterator tmp(*this);
      --_row;
      return tmp;
    }

    iterator operator+(size_t i) { return iterator(_data, _row + i); }
    iterator operator-(size_t i) { return iterator(_data, _row - i); }

    bool operator==(const iterator& other) const {
      return _data == other._data && _row == other._row;
    }

    bool operator!=(const iterator& other) const {
      return _data != other._data || _row != other._row;
    }

    Id* operator*() { return _data + _row * COLS; }
    const Id* operator*() const { return _data + _row * COLS; }

    Id& operator[](size_t i) { return *(_data + _row * COLS + i); }
    const Id& operator[](size_t i) const { return *(_data + _row * COLS + i); }

    size_t row() const { return _row; }

   private:
    Id* _data;
    size_t _row;
  };

  using const_iterator = const iterator;

  IdTableStatic() : _data(nullptr), _size(0), _capacity(0) {}
  virtual ~IdTableStatic() { delete[] _data; }

  // Copy constructor
  IdTableStatic<COLS>(const IdTableStatic<COLS>& other)
      : _data(other._data), _size(other._size), _capacity(other._capacity) {
    if (other._data != nullptr) {
      _data = new Id[_capacity * COLS];
      std::memcpy(_data, other._data, _size * sizeof(Id) * COLS);
    }
  }

  // Move constructor
  IdTableStatic<COLS>(IdTableStatic<COLS>&& other)
      : _data(other._data), _size(other._size), _capacity(other._capacity) {
    other._data = nullptr;
    other._size = 0;
    other._capacity = 0;
  }

  // copy assignment
  IdTableStatic<COLS>& operator=(const IdTableStatic<COLS>& other) {
    delete[] _data;
    // Use a placement new to call the copy constructor
    new (this) IdTableStatic<COLS>(other);
    return *this;
  }

  // move assignment
  IdTableStatic<COLS>& operator=(IdTableStatic<COLS>&& other) {
    delete[] _data;
    // Use a placement new to call the move constructor
    new (this) IdTableStatic<COLS>(other);
    return *this;
  }

  // Element access
  Id& operator()(size_t row, size_t col) { return _data[row * COLS + col]; }
  const Id& operator()(size_t row, size_t col) const {
    return _data[row * COLS + col];
  }

  // Row access
  iterator operator[](size_t row) { return iterator(_data, row); }
  const_iterator operator[](size_t row) const { return iterator(_data, row); }

  // Begin iterator
  iterator begin() { return iterator(_data, 0); }
  const_iterator begin() const { return iterator(_data, 0); }
  const_iterator cbegin() const { return iterator(_data, 0); }

  // End iterator
  iterator end() { return iterator(_data, _size); }
  const_iterator end() const { return iterator(_data, _size); }
  const_iterator cend() const { return iterator(_data, _size); }

  // Size access
  size_t rows() const { return _size; }
  size_t cols() const { return COLS; }

  size_t size() const { return _size; }

  void push_back() {
    if (_size + 1 >= _capacity) {
      grow();
    }
    _size++;
  }

  void push_back(const std::array<Id, COLS>& init) {
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * COLS, init.data(), sizeof(Id) * COLS);
    _size++;
  }

  /**
   * @brief Erases all rows in the range [begin;end)
   **/
  void erase(const_iterator& begin,
             const_iterator& end = iterator(nullptr, 0)) {
    iterator actualEnd = end;
    if (actualEnd == iterator(nullptr, 0)) {
      actualEnd = iterator(_data, begin.row() + 1);
    }
    if (actualEnd.row() > IdTableStatic<COLS>::end().row()) {
      actualEnd = IdTableStatic<COLS>::end();
    }
    if (actualEnd.row() <= begin.row()) {
      return;
    }
    size_t numErased = actualEnd.row() - begin.row();
    // Move the elements from actualEnd() to end() forward
    for (size_t row = begin.row(); row + numErased < _size; row++) {
      // copy a single row
      std::memcpy(_data + row * COLS, _data + (row + numErased) * COLS,
                  sizeof(Id) * COLS);
    }
    _size -= numErased;
  }

  /**
   * @brief Ensures this table has enough space allocated to store rows many
   *        rows of data
   **/
  void reserve(size_t rows) {
    if (_capacity < rows) {
      // Add rows - _capacity many new rows
      grow(rows - _capacity);
    }
  }

  /**
   * @brief Resizes this IdTableStatic to have at least row many rows. This
   *method never makes the IdTableStatic smaller.
   **/
  void resize(size_t rows) {
    if (rows > _size) {
      reserve(rows);
      _size = rows;
    }
  }

  /**
   * Returns a dyamically sized IdTableStatic that now owns this tables data
   **/
  IdTableStatic<0> moveToDynamic() {
    IdTableStatic<0> tmp(COLS);
    tmp._data = _data;
    tmp._size = _size;
    tmp._capacity = _capacity;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    return tmp;
  };

  /**
   * Returns a dyamically sized IdTableStatic that conatains a copy of this
   *tables data
   **/
  IdTableStatic<0> copyToDynamic() const {
    IdTableStatic<0> tmp(COLS);
    tmp._data = new Id[_capacity * COLS];
    std::memcpy(tmp._data, _data, sizeof(Id) * _size * COLS);
    tmp._size = _size;
    tmp._capacity = _capacity;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    return tmp;
  };

 private:
  /**
   * @brief Grows the storage of this IdTableStatic
   * @param newRows If newRows is 0 the storage is grown by GROWTH_FACTOR. If
   *        newRows is any other number the vector is grown by newRows many
   *        rows.
   **/
  void grow(size_t newRows = 0) {
    // If _data is nullptr so is _size which makes this safe
    Id* tmp = _data;
    size_t new_capacity;
    if (newRows == 0) {
      new_capacity = _capacity * GROWTH_FACTOR + 1;
    } else {
      new_capacity = _capacity + newRows;
    }
    _data = new Id[new_capacity * COLS];
    // copy over the old data
    std::memcpy(_data, tmp, sizeof(Id) * _size * COLS);
    _capacity = new_capacity;
    // free the old data if nobody is using it anymore
    delete[] tmp;
  }

  Id* _data;
  size_t _size;
  size_t _capacity;
};
