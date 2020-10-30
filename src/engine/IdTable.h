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
#include "../util/Log.h"

/**
 * @brief This struct is used to store all the template specialized data and
 * methods of the IdTable class. This way less code of the IdTable has to
 * be duplicated.
 **/
template <typename T, int COLS>
class IdTableImpl {
 public:
  IdTableImpl()
      : _data(nullptr), _size(0), _capacity(0), _manage_storage(true) {}

  class iterator {
   public:
    // iterator traits types
    using difference_type = ssize_t;
    using value_type = std::array<T, COLS>;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::random_access_iterator_tag;

    iterator() : _data(nullptr), _row(0) {}
    // This constructor has to take three arguments for compatibility with
    // IdTableImpl<0> which doesn't know the number of columns at compile time.
    // To prevent compiler warnings about unused parameters the third parameter
    // (the number of columns) is not given a name.
    iterator(T* data, size_t row, size_t) : _data(data), _row(row) {}
    virtual ~iterator() {}

    // Copy and move constructors and assignment operators
    iterator(const iterator& other) : _data(other._data), _row(other._row) {}

    iterator(iterator&& other) : _data(other._data), _row(other._row) {}

    iterator& operator=(const iterator& other) {
      _data = other._data;
      _row = other._row;
      return *this;
    }

    // <joka92> I don't think iterators usually need or have move semantics,
    // But they do not hurt.
    iterator& operator=(iterator&& other) {
      _data = other._data;
      _row = other._row;
      return *this;
    }

    // prefix increment
    iterator& operator++() {
      ++_row;
      return *this;
    }

    // multistep increment
    iterator& operator+=(difference_type i) {
      _row += i;
      return *this;
    }

    // postfix increment
    iterator operator++(int) {
      iterator tmp(*this);
      ++_row;
      return tmp;
    }

    // prefix decrement
    iterator& operator--() {
      --_row;
      return *this;
    }

    // multistep decrement
    iterator& operator-=(difference_type i) {
      _row -= i;
      return *this;
    }

    // postfix decrement
    iterator operator--(int) {
      iterator tmp(*this);
      --_row;
      return tmp;
    }

    iterator operator+(difference_type i) const {
      return iterator(_data, _row + i, COLS);
    }
    iterator operator-(difference_type i) const {
      return iterator(_data, _row - i, COLS);
    }
    difference_type operator-(const iterator& other) const {
      return _row - other._row;
    }

    bool operator==(iterator const& other) const {
      return _data == other._data && _row == other._row;
    }

    bool operator!=(iterator const& other) const {
      return _data != other._data || _row != other._row;
    }

    bool operator<(const iterator& other) const { return _row < other._row; }
    bool operator>(const iterator& other) const { return _row > other._row; }
    bool operator<=(const iterator& other) const { return _row <= other._row; }
    bool operator>=(const iterator& other) const { return _row >= other._row; }

    value_type& operator*() {
      return *reinterpret_cast<value_type*>(_data + (_row * COLS));
    }
    const value_type& operator*() const {
      return *reinterpret_cast<const value_type*>(_data + (_row * COLS));
    }

    // This has to return a pointer to the current element to meet standard
    // semantics.
    pointer operator->() { return _data + (_row * COLS); }

    const value_type* operator->() const { return _data + (_row * COLS); }

    // access the element that is i steps ahead
    // used by the parallel sorting in GCC
    reference operator[](difference_type i) {
      return *reinterpret_cast<value_type*>(_data + (_row + i) * COLS);
    }

    reference operator[](difference_type i) const {
      return *reinterpret_cast<const value_type*>(_data + (_row + i) * COLS);
    }

    size_t row() const { return _row; }
    size_t cols() const { return COLS; }
    size_t size() const { return COLS; }

   private:
    T* _data;
    size_t _row;
  };

  using const_row_type = const std::array<T, COLS>;
  using row_type = const std::array<T, COLS>;
  using const_row_reference = const_row_type&;
  using row_reference = row_type&;

  const_row_reference getConstRow(size_t row) const {
    return *reinterpret_cast<const_row_type*>(_data + (row * COLS));
  }

  row_reference getRow(size_t row) {
    return *reinterpret_cast<row_type*>(_data + (row * COLS));
  }

  constexpr size_t cols() const { return COLS; }

  T* _data;
  size_t _size;
  size_t _capacity;
  // This ensures the name _cols is always defined within the IdTableImpl
  // and allows for uniform usage.
  static constexpr int _cols = COLS;
  bool _manage_storage;

  void setCols(size_t cols) { (void)cols; };
};

template <typename T>
class IdTableImpl<T, 0> {
 public:
  IdTableImpl()
      : _data(nullptr),
        _size(0),
        _capacity(0),
        _cols(0),
        _manage_storage(true) {}

  class ConstRow final {
   public:
    ConstRow(const T* data, size_t cols) : _data(data), _cols(cols) {}
    ConstRow(const ConstRow& other) : _data(other._data), _cols(other._cols) {}
    ConstRow(ConstRow&& other) : _data(other._data), _cols(other._cols) {}
    ConstRow& operator=(const ConstRow& other) = delete;
    ConstRow& operator=(ConstRow&& other) = delete;
    bool operator==(ConstRow& other) const {
      bool matches = _cols == other._cols;
      for (size_t i = 0; matches && i < _cols; i++) {
        matches &= _data[i] == other._data[i];
      }
      return matches;
    }
    const T& operator[](size_t i) const { return *(_data + i); }
    const T* data() const { return _data; }
    size_t size() const { return _cols; }

    const T* _data;
    size_t _cols;
  };

  /**
   * This provides access to a single row of a Table. The class can optionally
   * manage its own data, allowing for it to be swappable (otherwise swapping
   * two rows during e.g. a std::sort would lead to bad behaviour).
   **/
  class Row {
   public:
    explicit Row(size_t cols)
        : _data(new T[cols]), _cols(cols), _allocated(true) {}
    Row(T* data, size_t cols) : _data(data), _cols(cols), _allocated(false) {}
    virtual ~Row() {
      if (_allocated) {
        delete[] _data;
      }
    }
    Row(const Row& other)
        : _data(new T[other._cols]), _cols(other._cols), _allocated(true) {
      std::memcpy(_data, other._data, sizeof(T) * _cols);
    }

    Row(Row&& other)
        : _data(other._allocated ? other._data : new T[other._cols]),
          _cols(other._cols),
          _allocated(true) {
      if (other._allocated) {
        other._data = nullptr;
      } else {
        std::memcpy(_data, other._data, sizeof(T) * _cols);
      }
    }

    Row& operator=(const Row& other) {
      // Check for self assignment.
      if (&other == this) {
        return *this;
      }
      if (_allocated) {
        // If we manage our own storage recreate that to fit the other row
        delete[] _data;
        _data = new T[other._cols];
        _cols = other._cols;
      }
      // Copy over the data from the other row to this row
      if (_cols == other._cols && _data != nullptr && other._data != nullptr) {
        std::memcpy(_data, other._data, sizeof(T) * _cols);
      }
      return *this;
    }

    Row& operator=(Row&& other) {
      // Check for self assignment.
      if (&other == this) {
        return *this;
      }
      // This class cannot use move semantics if at least one of the two
      // rows invovlved in an assigment does not manage it's data, but rather
      // functions as a view into an IdTable
      if (_allocated) {
        // If we manage our own storage recreate that to fit the other row
        delete[] _data;
        if (other._allocated) {
          // Both rows manage their own storage so we can take advantage
          // of move semantics.
          _data = other._data;
          _cols = other._cols;

          // otherwise the data will be deleted unexpectedly
          other._data = nullptr;

          return *this;
        } else {
          _data = new T[other._cols];
          _cols = other._cols;
        }
      }
      // Copy over the data from the other row to this row
      if (_cols == other._cols && _data != nullptr && other._data != nullptr) {
        std::memcpy(_data, other._data, sizeof(T) * _cols);
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

    T& operator[](size_t i) { return *(_data + i); }
    const T& operator[](size_t i) const { return *(_data + i); }
    T* data() { return _data; }
    const T* data() const { return _data; }

    size_t size() const { return _cols; }
    size_t cols() const { return _cols; }

    T* _data;
    size_t _cols;
    bool _allocated;
  };

  friend std::ostream& operator<<(std::ostream& out,
                                  const typename IdTableImpl<T, 0>::Row& row) {
    for (size_t col = 0; col < row.size(); col++) {
      out << row[col] << ", ";
    }
    out << std::endl;
    return out;
  }

  friend std::ostream& operator<<(
      std::ostream& out, const typename IdTableImpl<T, 0>::ConstRow&& row) {
    for (size_t col = 0; col < row.size(); col++) {
      out << row[col] << ", ";
    }
    out << std::endl;
    return out;
  }

  class iterator {
   public:
    using difference_type = ssize_t;
    using value_type = Row;
    using pointer = Row*;
    using reference = Row&;
    using iterator_category = std::random_access_iterator_tag;

    iterator() : _data(nullptr), _row(0), _cols(0), _rowView(nullptr, 0) {}
    iterator(T* data, size_t row, size_t cols)
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

    // multi-step increment
    iterator& operator+=(difference_type i) {
      _row += i;
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

    // prefix decrement
    iterator& operator--() {
      --_row;
      _rowView._data = _data + (_row * _cols);
      return *this;
    }

    // multi-step decrement
    iterator& operator-=(difference_type i) {
      _row -= i;
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

    iterator operator+(difference_type i) const {
      return iterator(_data, _row + i, _cols);
    }
    iterator operator-(difference_type i) const {
      return iterator(_data, _row - i, _cols);
    }
    difference_type operator-(const iterator& other) const {
      return _row - other._row;
    }

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

    pointer operator->() { return &_rowView; }
    const value_type* operator->() const { return &_rowView; }

    // access the element the is i steps ahead
    // we need to construct new rows for this which should not be too expensive
    // In addition: Non const rows behave like references since they hold
    // pointers to specific parts of the _data. Thus they behave according to
    // the standard.
    Row operator[](difference_type i) {
      return Row(_data + (_row + i) * _cols, _cols);
    }

    const Row operator[](difference_type i) const {
      return Row(_data + (_row + i) * _cols, _cols);
    }

    size_t row() const { return _row; }
    size_t cols() const { return _cols; }
    size_t size() const { return _cols; }

   private:
    T* _data;
    size_t _row;
    size_t _cols;
    Row _rowView;
  };

  using const_row_type = ConstRow;
  using row_type = Row;
  using const_row_reference = const_row_type;
  using row_reference = row_type;

  const_row_reference getConstRow(size_t row) const {
    return ConstRow(_data + (row * _cols), _cols);
  }

  row_reference getRow(size_t row) { return Row(_data + (row * _cols), _cols); }

  size_t cols() const { return _cols; }

  void setCols(size_t cols) { _cols = cols; }

  T* _data;
  size_t _size;
  size_t _capacity;
  size_t _cols;
  bool _manage_storage;
};

template <typename T, int COLS>
class IdTableStatic;


template <typename T, int COLS>
class IdTableStatic : private IdTableImpl<T, COLS> {
  // Make all other instantiations of this template friends of this.
  template <typename, int>
  friend class IdTableStatic;
  template <int I>
  friend void swap(IdTableStatic<T, I>& left, IdTableStatic<T, I>& right);

 private:
  static constexpr float GROWTH_FACTOR = 1.5;

 public:
  using Base = IdTableImpl<T, COLS>;
  using Row = typename IdTableImpl<T, COLS>::row_type;
  using ConstRow = typename IdTableImpl<T, COLS>::const_row_type;
  using iterator = typename IdTableImpl<T, COLS>::iterator;
  using const_iterator = typename IdTableImpl<T, COLS>::iterator;
  using row_reference = typename IdTableImpl<T, COLS>::row_reference;
  using const_row_reference = typename IdTableImpl<T, COLS>::const_row_reference;

  using IdTableImpl<T, COLS>::_data;
  using IdTableImpl<T, COLS>::_size;
  using IdTableImpl<T, COLS>::_capacity;
  using IdTableImpl<T, COLS>::_manage_storage;
  using IdTableImpl<T, COLS>::_cols;
  using IdTableImpl<T, COLS>::cols;
  using IdTableImpl<T, COLS>::getRow;



  IdTableStatic() {
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    setCols(0);
    _manage_storage = true;
  }

  IdTableStatic(size_t cols) {
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    setCols(cols);
    _manage_storage = true;
  }

  virtual ~IdTableStatic() {
    if (_manage_storage) {
      free(_data);
    }
  }

  // Copy constructor
  IdTableStatic(const IdTableStatic& other) {
    _data = other._data;
    _size = other._size;
    setCols(other._cols);
    _capacity = other._capacity;
    if (other._data != nullptr) {
      _data =
          reinterpret_cast<T*>(malloc(_capacity *
                                       _cols * sizeof(T)));
      std::memcpy(
          _data, other._data,
          _size * sizeof(T) * _cols);
    }
    _manage_storage = true;
  }

  // Move constructor
  IdTableStatic(IdTableStatic&& other) {
    swap(*this, other);
    other._data = nullptr;
    other._size = 0;
    other._capacity = 0;
  }

  // copy assignment
  IdTableStatic& operator=(IdTableStatic other) {
    swap(*this, other);
    return *this;
  }

  bool operator==(const IdTableStatic& other) const {
    if (other.size() != size()) {
      return false;
    }
    for (size_t row = 0; row < size(); row++) {
      for (size_t col = 0; col < cols(); col++) {
        if ((*this)(row, col) != other(row, col)) {
          return false;
        }
      }
    }
    return true;
  }

  // Element access
  T& operator()(size_t row, size_t col) {
    return _data[row * _cols + col];
  }
  const T& operator()(size_t row, size_t col) const {
    return _data[row * _cols + col];
  }

  // Row access
  row_reference operator[](size_t row) {
    return getRow(row);
  }

  const_row_reference operator[](size_t row) const {
    // Moving this method to impl allows for efficient ConstRow types when
    // using non dynamic IdTables.
    return Base::getConstRow(row);
  }

  // Begin iterator
  iterator begin() {
    return iterator(_data, 0, _cols);
  }
  const_iterator begin() const {
    return iterator(_data, 0, _cols);
  }
  const_iterator cbegin() const {
    return iterator(_data, 0, _cols);
  }

  // End iterator
  iterator end() {
    return iterator(_data, _size,
                    _cols);
  }
  const_iterator end() const {
    return iterator(_data, _size,
                    _cols);
  }
  const_iterator cend() const {
    return iterator(_data, _size,
                    _cols);
  }

  row_reference back() {
    return getRow((end() - 1).row());
  }

  const_row_reference back() const {
    return getConstRow((end() - 1).row());
  }

  T* data() { return _data; }
  const T* data() const { return _data; }

  // Size access
  size_t rows() const { return _size; }

  /**
   * The template parameter here is used to allow for creating
   * a version of the function that is constexpr for COLS != 0
   * and one that is not constexpt for COLS == 0
   */

  size_t size() const { return _size; }

  /**
   * @brief Sets the number of columns. Should only be called while data is
   *        still nullptr.
   **/
  void setCols(size_t cols) {
    //assert(_data == nullptr); // TODO(joka921) This assertion does not hold bc of the swap method.
    Base::setCols(cols);
  }

  void emplace_back() { push_back(); }

  void push_back() {
    if (_size + 1 >= _capacity) {
      grow();
    }
    _size++;
  }

  void push_back(const std::initializer_list<T>& init) {
    assert(init.size() == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data +
                    _size * _cols,
                init.begin(), sizeof(T) * _cols);
    _size++;
  }

  /**
   * @brief Read cols() elements from init and stores them in a new row
   **/
  void push_back(const T* init) {
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data +
                    _size * _cols,
                init, sizeof(T) * _cols);
    _size++;
  }

  /**
   * @brief Read cols() elements from init and stores them in a new row
   **/
  void push_back(const Row& init) {
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data +
                    _size * _cols,
                init.data(), sizeof(T) * _cols);
    _size++;
  }

  void push_back(const IdTableStatic& init, size_t row) {
    assert(init._cols == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(_data +
                    _size * _cols,
                init._data + row * _cols,
                sizeof(T) * _cols);
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
      size_t numMissing =
          _size + numNewRows - _capacity;
      grow(numMissing);
    }

    size_t afterTarget = size() - target;
    // Move the data currently in the way back.
    std::memmove(_data +
                     (target + numNewRows) * _cols,
                 _data + target * _cols,
                 sizeof(T) * _cols * afterTarget);

    _size += numNewRows;
    // Copy the new data
    std::memcpy(_data + target * _cols,
                (*begin).data(),
                sizeof(T) * _cols * numNewRows);
  }

  /**
   * @brief Erases all rows in the range [begin;end)
   **/
  void erase(const const_iterator& begin,
             const const_iterator& end = iterator(nullptr, 0, 0)) {
    iterator actualEnd = end;
    if (actualEnd == iterator(nullptr, 0, 0)) {
      actualEnd = iterator(_data, begin.row() + 1,
                           _cols);
    }
    if (actualEnd.row() > this->end().row()) {
      actualEnd = this->end();
    }
    if (actualEnd.row() <= begin.row()) {
      return;
    }
    size_t numErased = actualEnd.row() - begin.row();
    size_t numToMove = size() - actualEnd.row();
    std::memmove(
        _data + begin.row() * _cols,
        _data + actualEnd.row() * _cols,
        numToMove * _cols * sizeof(T));
    _size -= numErased;
  }

  void clear() { _size = 0; }

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
   * @brief Creates an IdTableStatic<NEW_COLS> that now owns this
   * id tables data. This is effectively a move operation that also
   * changes the type to its equivalent dynamic variant.
   **/
  template <int NEW_COLS>
  IdTableStatic<T, NEW_COLS> moveToStatic() {
    IdTableStatic<T, NEW_COLS> tmp;
    tmp.setCols(cols());
    tmp._data = _data;
    tmp._size = _size;
    tmp._capacity = _capacity;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    return tmp;
  };

  template <int NEW_COLS>
  const IdTableStatic<T, NEW_COLS> asStaticView() const {
    IdTableStatic<T, NEW_COLS> tmp;
    tmp.setCols(cols());
    tmp._data = _data;
    tmp._size = _size;
    tmp._capacity = _capacity;
    // This prevents the view from freeing the memory
    tmp._manage_storage = false;
    return tmp;
  };

  /**
   * @brief Creates an IdTable that now owns this
   * id tables data. This is effectively a move operation that also
   * changes the type to an equibalent one.
   **/
  IdTableStatic<T, 0> moveToDynamic() {
    IdTableStatic<T, 0> tmp(cols());
    tmp._data = _data;
    tmp._size = _size;
    tmp._capacity = _capacity;
    _data = nullptr;
    _size = 0;
    _capacity = 0;
    return tmp;
  };

  // Support for ostreams
  inline friend std::ostream& operator<<(std::ostream& out,
                                  const IdTableStatic<T, COLS>& table) {
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


  friend void swap(IdTableStatic<T, COLS>& left, IdTableStatic<T, COLS>& right) {
    using std::swap;
    swap(left._data, right._data);
    swap(left._size, right._size);
    swap(left._capacity, right._capacity);
    // As the cols member only exists for COLS = 0 we use the setCols
    // method here. setCols is guaranteed to always be defined even
    // if COLS != 0 (It might not have any effect though).
    size_t rcols = right.cols();
    right.setCols(left.cols());
    left.setCols(rcols);
    swap(left._manage_storage, right._manage_storage);
  }

 private:
  /**
   * @brief Grows the storage of this IdTableStatic
   * @param newRows If newRows is 0 the storage is grown by GROWTH_FACTOR. If
   *        newRows is any other number the vector is grown by newRows many
   *        rows.
   **/
  void grow(size_t newRows = 0) {
    // If _data is nullptr so is _size
    // which makes this safe
    size_t new_capacity;
    if (newRows == 0) {
      new_capacity = _capacity * GROWTH_FACTOR + 1;
    } else {
      new_capacity = _capacity + newRows;
    }
    T* larger = reinterpret_cast<T*>(
        realloc(_data,
                new_capacity * _cols * sizeof(T)));
    if (larger == nullptr) {
      // We were unable to acquire the memory
      free(_data);
      _data = nullptr;
      _capacity = 0;
      _size = 0;
      LOG(ERROR) << "Unable to grow the IDTable at "
                 << (reinterpret_cast<void*>(this)) << std::endl;
      throw std::bad_alloc();
    }
    _data = larger;
    _capacity = new_capacity;
  }
};



// typedef to make things simples
template<int COLS>
using FancyTableStatic = IdTableStatic<FancyId, COLS>;
using FancyTable = IdTableStatic<FancyId, 0>;

using IdTable = FancyTable;
