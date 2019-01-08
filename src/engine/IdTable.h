// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <ostream>
#include "../global/Id.h"

template <int COLS = 0>
class IdTableStatic;
using IdTable = IdTableStatic<>;

template <>
class IdTableStatic<0> {
 private:
  static constexpr float GROWTH_FACTOR = 1.5;

 public:
  /**
   * This provides access to a single row of a Table. The class can optionally
   * manage its own data, allowing for it to be swappable (otherwise swapping
   * two rows during e.g. a std::sort would lead to bad behaviour).
   **/
  class Row {
   public:
    Row(size_t cols) : _data(new Id[cols]), _cols(cols), _allocated(true) {}
    Row(Id* data, size_t cols) : _data(data), _cols(cols), _allocated(false) {}
    virtual ~Row() {
      if (_allocated) {
        delete[] _data;
      }
    }
    Row(const Row& other)
        : _data(new Id[other._cols]), _cols(other._cols), _allocated(true) {
      std::memcpy(_data, other._data, sizeof(Id) * _cols);
    }
    Row(Row&& other)
        : _data(other._allocated ? other._data : new Id[other._cols]),
          _cols(other._cols),
          _allocated(true) {
      if (other._allocated) {
        other._data = nullptr;
      } else {
        std::memcpy(_data, other._data, sizeof(Id) * _cols);
      }
    }

    Row& operator=(const Row& other) {
      if (_allocated) {
        // If we manage our own storage recreate that to fit the other row
        delete[] _data;
        _data = new Id[other._cols];
        _cols = other._cols;
      }
      // Copy over the data from the other row to this row
      if (_cols == other._cols && _data != nullptr && other._data != nullptr) {
        std::memcpy(_data, other._data, sizeof(Id) * _cols);
      }
      return *this;
    }

    Row& operator=(Row&& other) {
      // This class cannot use move semantics if at least one of the two
      // rows invovled in an assigment does not manage it's data, but rather
      // functions as a view into an IdTable
      if (_allocated) {
        // If we manage our own storage recreate that to fit the other row
        delete[] _data;
        if (other._allocated) {
          // Both rows manage their own storage so we can take advantage
          // of move semantics.
          _data = other._data;
          _cols = other._cols;
          return *this;
        } else {
          _data = new Id[other._cols];
          _cols = other._cols;
        }
      }
      // Copy over the data from the other row to this row
      if (_cols == other._cols && _data != nullptr && other._data != nullptr) {
        std::memcpy(_data, other._data, sizeof(Id) * _cols);
      }
      return *this;
    }

    bool operator==(const Row& other) const {
      bool matches = _cols == other._cols;
      for (size_t i = 0; matches && i < _cols; i++) {
        matches &= _data[i] == other._data[i];
      }
      return matches;
    }

    Id& operator[](size_t i) { return *(_data + i); }
    const Id& operator[](size_t i) const { return *(_data + i); }
    Id* data() { return _data; }
    const Id* data() const { return _data; }

    size_t size() const { return _cols; }
    size_t cols() const { return _cols; }

    Id* _data;
    size_t _cols;
    bool _allocated;
  };

  class ConstRow {
   public:
    ConstRow(const Id* data, size_t cols) : _data(data), _cols(cols) {}
    virtual ~ConstRow() {}
    ConstRow(const ConstRow& other) : _data(other._data), _cols(other._cols) {}
    ConstRow(ConstRow&& other) : _data(other._data), _cols(other._cols) {}
    ConstRow& operator=(const ConstRow& other) = delete;
    ConstRow& operator=(ConstRow&& other) = delete;
    const Id& operator[](size_t i) const { return *(_data + i); }
    const Id* data() const { return _data; }
    size_t size() const { return _cols; }

    const Id* _data;
    size_t _cols;
  };

  class iterator {
   public:
    iterator() : _data(nullptr), _row(0), _cols(0), _rowView(nullptr, 0) {}
    iterator(Id* data, size_t row, size_t cols)
        : _data(data),
          _row(row),
          _cols(cols),
          _rowView(data + (cols * row), cols) {}
    virtual ~iterator() {}

    // Copy and move constructors and assignment operators
    iterator(const iterator& other)
        : _data(other._data),
          _row(other._row),
          _cols(other._cols),
          _rowView(_data + (_cols * _row), _cols) {}

    iterator(iterator&& other)
        : _data(other._data),
          _row(other._row),
          _cols(other._cols),
          _rowView(_data + (_cols * _row), _cols) {}

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
      _rowView._data = _data + (_row * _cols);
      return *this;
    }

    // postfix increment
    iterator operator++(int) {
      iterator tmp(*this);
      ++_row;
      _rowView._data = _data + (_row * _cols);
      return tmp;
    }

    // prefix increment
    iterator& operator--() {
      --_row;
      _rowView._data = _data + (_row * _cols);
      return *this;
    }

    // postfix increment
    iterator operator--(int) {
      iterator tmp(*this);
      --_row;
      _rowView._data = _data + (_row * _cols);
      return tmp;
    }

    iterator operator+(size_t i) const {
      return iterator(_data, _row + i, _cols);
    }
    iterator operator-(size_t i) const {
      return iterator(_data, _row - i, _cols);
    }
    ssize_t operator-(const iterator& other) const { return _row - other._row; }

    bool operator==(iterator const& other) const {
      return _data == other._data && _row == other._row && _cols == other._cols;
    }

    bool operator!=(iterator const& other) const {
      return _data != other._data || _row != other._row || _cols != other._cols;
    }

    bool operator<(const iterator& other) const { return _row < other._row; }
    bool operator>(const iterator& other) const { return _row > other._row; }
    bool operator<=(const iterator& other) const { return _row <= other._row; }
    bool operator>=(const iterator& other) const { return _row >= other._row; }

    Row& operator*() { return _rowView; }
    const Row& operator*() const { return _rowView; }

    Id& operator[](size_t i) { return *(_data + _row * _cols + i); }
    const Id& operator[](size_t i) const { return *(_data + _row * _cols + i); }

    size_t row() const { return _row; }
    size_t cols() const { return _cols; }
    size_t size() const { return _cols; }

   private:
    Id* _data;
    size_t _row;
    size_t _cols;
    Row _rowView;
  };

  using const_iterator = const iterator;

  IdTableStatic() : _data(nullptr), _size(0), _cols(0), _capacity(0) {}
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
  Row operator[](size_t row) { return Row(_data + row * _cols, _cols); }

  ConstRow operator[](size_t row) const {
    return ConstRow(_data + row * _cols, _cols);
  }

  // Begin iterator
  iterator begin() { return iterator(_data, 0, _cols); }
  const_iterator begin() const { return iterator(_data, 0, _cols); }
  const_iterator cbegin() const { return iterator(_data, 0, _cols); }

  // End iterator
  iterator end() { return iterator(_data, _size, _cols); }
  const_iterator end() const { return iterator(_data, _size, _cols); }
  const_iterator cend() const { return iterator(_data, _size, _cols); }

  Row back() { return Row(_data + (_size - 1) * _cols, _cols); }

  ConstRow back() const { return ConstRow(_data + (_size - 1) * _cols, _cols); }

  Id* data() { return _data; }
  const Id* data() const { return _data; }

  // Size access
  size_t rows() const { return _size; }
  size_t cols() const { return _cols; }

  size_t size() const { return _size; }

  /**
   * @brief Sets the number of columns. Should only be called while data is
   *        still nullptr.
   **/
  void setCols(size_t cols) {
    assert(_data == nullptr);
    _cols = cols;
  }

  void emplace_back() { push_back(); }

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
   * @brief Read cols() elements from init and stores them in a new row
   **/
  void push_back(const Id* init) {
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * _cols, init, sizeof(Id) * _cols);
    _size++;
  }

  /**
   * @brief Read cols() elements from init and stores them in a new row
   **/
  void push_back(const Row& init) {
    assert(init._cols == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * _cols, init.data(), sizeof(Id) * _cols);
    _size++;
  }

  /**
   * @brief Read cols() elements from init and stores them in a new row
   **/
  void push_back(ConstRow& init) {
    assert(init._cols == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * _cols, init.data(), sizeof(Id) * _cols);
    _size++;
  }

  /**
   * @brief Read cols() elements from init and stores them in a new row
   **/
  void push_back(ConstRow init) {
    assert(init._cols == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * _cols, init.data(), sizeof(Id) * _cols);
    _size++;
  }

  void push_back(const IdTable init, size_t row) {
    assert(init._cols == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * _cols, init._data + row * _cols,
                sizeof(Id) * _cols);
    _size++;
  }

  void pop_back() {
    if (_size > 0) {
      _size--;
    }
  }

  /**
   * @brief Inserts the elements in the range [begin;end) before pos.
   **/
  void insert(const iterator& pos, const iterator& begin, const iterator& end) {
    assert(begin.cols() == cols());
    if (begin.row() >= end.row()) {
      return;
    }
    size_t target = std::min(pos.row(), this->end().row());
    size_t numNewRows = end.row() - begin.row();
    if (_capacity < _size + numNewRows) {
      size_t numMissing = _size + numNewRows - _capacity;
      grow(numMissing);
    }
    // Move the data currently in the way back. As the src and target
    // of the copy might overlap the rows are copied individually starting
    // at the back. The loop still counts up to avoid underflows.
    for (size_t i = target; i < target + numNewRows && i < _size; i++) {
      size_t row = target + numNewRows - 1 - i;
      std::memcpy(_data + row * _cols, _data + (row + numNewRows) * _cols,
                  sizeof(Id) * _cols);
    }
    _size += numNewRows;
    // Copy the new data
    std::memcpy(_data + target * _cols, (*begin).data(),
                sizeof(Id) * _cols * numNewRows);
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

  void clear() {
    delete[] _data;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
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
   * @brief Resizes this IdTableStatic to have at least row many rows.
   **/
  void resize(size_t rows) {
    if (rows > _size) {
      reserve(rows);
      _size = rows;
    } else if (rows < _size) {
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
    /**
     * This provides a view onto a single row of the id table. Assigning to an
     * instance of this class changes the data in the actual table
     * This class is used as the value type of iterators.
     **/
    class Row {
     public:
      Row(Id* data) : _data(data) {}
      virtual ~Row() {}
      Row(const Row& other) : _data(other._data) {}
      Row(Row&& other) : _data(other._data) {}

      Row& operator=(const Row& other) {
        // Copy over the data from the other row to this row
        if (_data != nullptr && other._data != nullptr) {
          std::memcpy(_data, other._data, sizeof(Id) * COLS);
        }
        return *this;
      }

      Row& operator=(Row&& other) {
        // Copy over the data from the other row to this row
        if (_data != nullptr && other._data != nullptr) {
          std::memcpy(_data, other._data, sizeof(Id) * COLS);
        }
        return *this;
      }

      Id& operator[](size_t i) { return *(_data + i); }
      const Id& operator[](size_t i) const { return *(_data + i); }
      Id* data() { return _data; }
      const Id* data() const { return _data; }

      size_t size() const { return COLS; }

     private:
      Id* _data;
    };

    iterator() : _data(nullptr), _row(0), _rowView(nullptr, 0) {}
    iterator(Id* data, size_t row)
        : _data(data), _row(row), _rowView(data + row * COLS) {}
    virtual ~iterator() {}

    // Copy and move constructors and assignment operators
    iterator(const iterator& other)
        : _data(other._data), _row(other._row), _rowView(_data + _row * COLS) {}
    iterator(iterator&& other)
        : _data(other._data), _row(other._row), _rowView(_data + _row * COLS) {}
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
      _rowView.data = data + _row * COLS;
      return *this;
    }

    // postfix increment
    iterator operator++(int) {
      iterator tmp(*this);
      ++_row;
      _rowView.data = data + _row * COLS;
      return tmp;
    }

    // prefix increment
    iterator& operator--() {
      --_row;
      _rowView.data = data + _row * COLS;
      return *this;
    }

    // postfix increment
    iterator operator--(int) {
      iterator tmp(*this);
      --_row;
      _rowView.data = data + _row * COLS;
      return tmp;
    }

    iterator operator+(size_t i) const { return iterator(_data, _row + i); }
    iterator operator-(size_t i) const { return iterator(_data, _row - i); }
    ssize_t operator-(const iterator& other) const { return _row - other._row; }

    bool operator==(const iterator& other) const {
      return _data == other._data && _row == other._row;
    }

    bool operator!=(const iterator& other) const {
      return _data != other._data || _row != other._row;
    }

    bool operator<(const iterator& other) const { return _row < other._row; }
    bool operator>(const iterator& other) const { return _row > other._row; }
    bool operator<=(const iterator& other) const { return _row <= other._row; }
    bool operator>=(const iterator& other) const { return _row >= other._row; }

    Row& operator*() { return _rowView; }
    const Row& operator*() const { return _rowView; }

    Id& operator[](size_t i) { return *(_data + _row * COLS + i); }
    const Id& operator[](size_t i) const { return *(_data + _row * COLS + i); }

    size_t row() const { return _row; }
    size_t cols() const { return COLS; }
    size_t size() const { return COLS; }

   private:
    Id* _data;
    size_t _row;
    Row _rowView;
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
  typename iterator::Row operator[](size_t row) {
    return iterator::Row(_data + row * COLS);
  }
  typename iterator::Row operator[](size_t row) const {
    return iterator::Row(_data + row * COLS);
  }

  // Begin iterator
  iterator begin() { return iterator(_data, 0); }
  const_iterator begin() const { return iterator(_data, 0); }
  const_iterator cbegin() const { return iterator(_data, 0); }

  // End iterator
  iterator end() { return iterator(_data, _size); }
  const_iterator end() const { return iterator(_data, _size); }
  const_iterator cend() const { return iterator(_data, _size); }

  Id* back() { return _data + (_size - 1) * COLS; }
  const Id* back() const { return _data + (_size - 1) * COLS; }

  Id* data() { return _data; }
  const Id* data() const { return _data; }

  // Size access
  size_t rows() const { return _size; }
  size_t cols() const { return COLS; }

  size_t size() const { return _size; }

  void emplace_back() { push_back(); }

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

  void push_back(const typename iterator::Row& init) {
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data + _size * COLS, init.data(), sizeof(Id) * COLS);
    _size++;
  }

  void pop_back() {
    if (_size > 0) {
      _size--;
    }
  }

  /**
   * @brief Inserts the elements in the range [begin;end) before pos.
   **/
  void insert(const iterator& pos, const iterator& begin, const iterator& end) {
    if (begin.row() <= end.row()) {
      return;
    }
    size_t target = std::min(pos.row(), this->end().row());
    size_t numNewRows = end.row() - begin.row();
    if (_capacity < _size + numNewRows) {
      size_t numMissing = _size + numNewRows - _capacity;
      grow(numMissing);
    }
    _size += numNewRows;
    // Move the data currently in the way back. As the src and target
    // of the copy might overlap the rows are copied individually starting
    // at the back. The loop still counts up to avoid underflows.
    for (size_t i = target; i < target + numNewRows; i++) {
      size_t row = target + numNewRows - 1 - i;
      std::memcpy(_data + row * COLS, _data + (row + numNewRows) * COLS,
                  sizeof(Id) * COLS);
    }
    // Copy the new data
    std::memcpy(_data + target * COLS, (*begin).data(),
                sizeof(Id) * COLS * numNewRows);
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

  void clear() {
    delete[] _data;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
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
   * @brief Resizes this IdTableStatic to have at least row many rows.
   **/
  void resize(size_t rows) {
    if (rows > _size) {
      reserve(rows);
      _size = rows;
    } else if (rows < _size) {
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

// Define the iterator traits for the iterators to make them compatible with
// code requirering legacy iterators (e.g. std::lower_bound)
template <>
struct std::iterator_traits<IdTable::iterator> {
  using difference_type = ssize_t;
  using value_type = IdTable::Row;
  using pointer = IdTable::Row*;
  using reference = IdTable::Row&;
  using iterator_category = std::forward_iterator_tag;
};

// TODO(florian): find a way to define iterator traits for the templated
//                iterator
// struct std::iterator_traits<IdTableStatic<>::iterator> {
//   using difference_type = ssize_t;
//   using value_type = Id*;
//   using pointer = Id**;
//   using reference = Id*&;
//   using iterator_category = std::forward_iterator_tag;
// };

// Support for ostreams
inline std::ostream& operator<<(std::ostream& out, const IdTable& table) {
  out << "IdTable(" << ((void*)table.data()) << ") with " << table.size()
      << " rows and " << table.cols() << " columns" << std::endl;
  for (size_t row = 0; row < table.size(); row++) {
    for (size_t col = 0; col < table.cols(); col++) {
      out << table(row, col) << ", ";
    }
    out << std::endl;
  }
  return out;
}

inline std::ostream& operator<<(std::ostream& out, const IdTable::Row& row) {
  for (size_t col = 0; col < row.size(); col++) {
    out << row[col] << ", ";
  }
  out << std::endl;
  return out;
}
