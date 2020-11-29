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

namespace detail {
// The actual data storage of the Id Tables, basically a wrapper around a
// std::vector<Id>
struct IdTableVectorWrapper {
  static constexpr bool ManagesStorage = true;  // is able to grow/allocate
  std::vector<Id> _data;

  IdTableVectorWrapper() = default;

  // construct from c-style array by copying the content
  explicit IdTableVectorWrapper(const Id* const ptr, size_t sz) {
    _data.assign(ptr, ptr + sz);
  }

  // unified interface to the data
  Id* data() noexcept { return _data.data(); }
  [[nodiscard]] const Id* data() const noexcept { return _data.data(); }

  bool empty() const { return _data.empty(); }
};

// similar interface to the IdTableVectorWrapper but doesn't own storage, used
// for cheap to copy const views into IdTables
struct IdTableViewWrapper {
  static constexpr bool ManagesStorage =
      false;  // is not able to grow and allocate
  const Id* _data;
  const size_t _size;

  IdTableViewWrapper() = delete;

  IdTableViewWrapper(const IdTableViewWrapper&) noexcept = default;

  // construct as a view into an owning VectorWrapper
  explicit IdTableViewWrapper(const IdTableVectorWrapper& rhs) noexcept
      : _data(rhs.data()), _size(rhs._data.size()) {}

  // convert to an owning VectorWrapper by making a copy. Explicit since
  // expensive
  explicit operator IdTableVectorWrapper() const {
    return IdTableVectorWrapper(_data, _size);
  }

  // access interface to the data
  [[nodiscard]] const Id* data() const noexcept { return _data; }

  bool empty() const noexcept { return _size == 0; }
};

// Random Access Iterator for an IdTable (basically a 2d array with compile-time
// fixed width only depends on the number of columns and not on the underlying
// storage model, so create this as a separate class
// with CONST = true we get a const_iterator
template <int COLS, bool CONST = false>
class IdTableIterator {
 public:
  // iterator traits types
  using difference_type = ssize_t;
  using value_type = std::array<Id, COLS>;
  using pointer = std::conditional_t<CONST, const value_type*, value_type*>;
  using reference = std::conditional_t<CONST, const value_type&, value_type&>;
  using iterator_category = std::random_access_iterator_tag;

 private:
  using data_t = std::conditional_t<CONST, const Id*, Id*>;
  data_t _data = nullptr;
  size_t _row = 0;

 public:
  IdTableIterator() = default;
  ~IdTableIterator() = default;
  // copy constructor and assignment are auto-generated

  // This constructor has to take three arguments for compatibility with
  // IdTableImpl<0> which doesn't know the number of columns at compile time.
  // To prevent compiler warnings about unused parameters the third parameter
  // (the number of columns) is not given a name.
  IdTableIterator(data_t data, size_t row, size_t) noexcept
      : _data(data), _row(row) {}

  // the const and non-const iterators are friends
  friend class IdTableIterator<COLS, !CONST>;

  // Allow "copy" construction of const iterator from
  // non-const iterator
  template <bool OTHERC, bool MYC = CONST, typename = std::enable_if_t<MYC>>
  IdTableIterator(const IdTableIterator<COLS, OTHERC>& other)
      : _data(other._data), _row(other._row) {}

  // Allow assignment of const iterator from non-const
  // iterator
  template <bool OTHERC, bool MYC = CONST, typename = std::enable_if_t<MYC>>
  IdTableIterator& operator=(const IdTableIterator<COLS, OTHERC>& other) {
    _data = other._data;
    _row = other._row;
    return *this;
  }

  // prefix increment
  IdTableIterator& operator++() {
    ++_row;
    return *this;
  }

  // multistep increment
  IdTableIterator& operator+=(difference_type i) {
    _row += i;
    return *this;
  }

  // postfix increment
  IdTableIterator operator++(int) & {
    IdTableIterator tmp(*this);
    ++_row;
    return tmp;
  }

  // prefix decrement
  IdTableIterator& operator--() {
    --_row;
    return *this;
  }

  // multistep decrement
  IdTableIterator& operator-=(difference_type i) {
    _row -= i;
    return *this;
  }

  // postfix decrement
  IdTableIterator operator--(int) & {
    IdTableIterator tmp(*this);
    --_row;
    return tmp;
  }

  IdTableIterator operator+(difference_type i) const {
    return IdTableIterator(_data, _row + i, COLS);
  }

  IdTableIterator operator-(difference_type i) const {
    return IdTableIterator(_data, _row - i, COLS);
  }

  difference_type operator-(const IdTableIterator& other) const {
    return _row - other._row;
  }

  bool operator==(IdTableIterator const& other) const {
    return _data == other._data && _row == other._row;
  }

  bool operator!=(IdTableIterator const& other) const {
    return _data != other._data || _row != other._row;
  }

  bool operator<(const IdTableIterator& other) const {
    return _row < other._row;
  }

  bool operator>(const IdTableIterator& other) const {
    return _row > other._row;
  }

  bool operator<=(const IdTableIterator& other) const {
    return _row <= other._row;
  }

  bool operator>=(const IdTableIterator& other) const {
    return _row >= other._row;
  }

  // disable for the const_iterator
  template <bool C = CONST, typename = std::enable_if_t<!C>>
  value_type& operator*() {
    return *reinterpret_cast<value_type*>(_data + (_row * COLS));
  }

  const value_type& operator*() const {
    return *reinterpret_cast<const value_type*>(_data + (_row * COLS));
  }

  // This has to return a pointer to the current element to meet standard
  // semantics.
  template <bool C = CONST, typename = std::enable_if_t<!C>>
  pointer operator->() {
    return _data + (_row * COLS);
  }

  const value_type* operator->() const { return _data + (_row * COLS); }

  // access the element that is i steps ahead
  // used by the parallel sorting in GCC
  template <bool C = CONST, typename = std::enable_if_t<!C>>
  reference operator[](difference_type i) {
    return *reinterpret_cast<pointer>(_data + (_row + i) * COLS);
  }

  const value_type& operator[](difference_type i) const {
    return *reinterpret_cast<const value_type*>(_data + (_row + i) * COLS);
  }

  [[nodiscard]] size_t row() const { return _row; }

  [[nodiscard]] size_t cols() const { return COLS; }

  [[nodiscard]] size_t size() const { return COLS; }
};

/**
 * @brief This struct is used to store all the template specialized data and
 * methods of the IdTable class. This way less code of the IdTable has to
 * be duplicated.
 *
 * @tparam COLS The number of columns (> 0 for this implementation)
 * @tparam DATA IdTableVectorWrapper or IdTableViewWrapper (data storage that
 *allows access to an (const) Id* )
 **/
template <int COLS, typename DATA>
class IdTableImpl {
 protected:
  size_t _size = 0;
  size_t _capacity = 0;
  DATA _data;  // something that lets us access a (const) Id*, might or might
               // not own its storage

  template <int INCOLS, typename INDATA>
  friend class IdTableImpl;  // make the conversions work from static to dynamic
                             // etc.

  // these constructors are protected so they can only be used by the
  // moveToStatic and moveToStaticView functions of the IdTableStatic class
  template <int INCOLS, typename INDATA,
            typename = std::enable_if_t<INCOLS == 0 || INCOLS == COLS>>
  IdTableImpl(const IdTableImpl<INCOLS, INDATA>& o)
      : _size(o._size), _capacity(o._capacity), _data(DATA(o._data)) {
    assert(cols() == o.cols());
  }

  template <int INCOLS, typename INDATA,
            typename = std::enable_if_t<INCOLS == 0 || INCOLS == COLS>>
  IdTableImpl(IdTableImpl<INCOLS, INDATA>&& o) noexcept
      : _size(std::move(o._size)),
        _capacity(std::move(o._capacity)),
        _data(std::move(o._data)) {
    assert(cols() == o.cols());
  }

 public:
  IdTableImpl() = default;

  using const_row_type = const std::array<Id, COLS>;
  using row_type = const std::array<Id, COLS>;
  using const_row_reference = const_row_type&;
  using row_reference = row_type&;
  using iterator = IdTableIterator<COLS, false>;
  using const_iterator = IdTableIterator<COLS, true>;

  const_row_reference getConstRow(size_t row) const {
    return *reinterpret_cast<const_row_type*>(data() + (row * COLS));
  }

  row_reference getRow(size_t row) {
    return *reinterpret_cast<row_type*>(data() + (row * COLS));
  }

 public:
  constexpr size_t cols() const { return COLS; }

 protected:
  // Access to the storage as raw pointers
  const Id* data() const { return _data.data(); }

  // This ensures the name _cols is always defined within the IdTableImpl
  // and allows for uniform usage.
  static constexpr int _cols = COLS;

  void setCols(size_t cols) { (void)cols; };
};

// ConstRow is a read-only view into a dynamic width IdTable which also stores
// its width
class ConstRow final {
 private:
  const Id* _data;
  size_t _cols;
  template <bool B>
  friend class IdTableDynamicIterator;

 public:
  ConstRow(const Id* data, size_t cols) : _data(data), _cols(cols) {}

  ConstRow(const ConstRow& other) = default;

  ConstRow(ConstRow&& other) = default;

  ConstRow& operator=(const ConstRow& other) = delete;
  ConstRow& operator=(ConstRow&& other) = delete;

  bool operator==(ConstRow& other) const {
    bool matches = _cols == other._cols;
    for (size_t i = 0; matches && i < _cols; i++) {
      matches &= _data[i] == other._data[i];
    }
    return matches;
  }

  const Id& operator[](size_t i) const { return *(_data + i); }

  const Id* data() const { return _data; }

  size_t size() const { return _cols; }

  inline friend std::ostream& operator<<(std::ostream& out,
                                         const ConstRow&& row) {
    for (size_t col = 0; col < row.size(); col++) {
      out << row[col] << ", ";
    }
    out << std::endl;
    return out;
  }
};

/**
 * This provides access to a single row of a Table. The class can optionally
 * manage its own data, allowing for it to be swappable (otherwise swapping
 * two rows during e.g. a std::sort would lead to bad behaviour).
 **/
class Row {
  Id* _data;
  size_t _cols;
  bool _allocated;

  template <bool C>
  friend class IdTableDynamicIterator;

 public:
  explicit Row(size_t cols)
      : _data(new Id[cols]), _cols(cols), _allocated(true) {}

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
    // Check for self assignment.
    if (&other == this) {
      return *this;
    }
    if (_allocated) {
      // If we manage our own storage recreate that to fit the other row
      delete[] _data;
      _data = new Id[other._cols];
      _cols = other._cols;
    }
    // Copy over the data from the other row to this row
    if (_cols == other._cols && _data != nullptr && other._data != nullptr) {
      std::memcpy(_data, other._data, sizeof(Id) * _cols);
    } else {
    }

    return *this;
  }

  Row& operator=(Row&& other) {
    // Check for self assignment.
    if (&other == this) {
      return *this;
    }
    // This class cannot use move semantics if at least one of the two
    // rows invovlved in an assigment does not manage its data, but rather
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
    return _cols == other._cols && std::equal(_data, _data + _cols, other._data);
  }

  Id& operator[](size_t i) { return *(_data + i); }

  const Id& operator[](size_t i) const { return *(_data + i); }

  Id* data() { return _data; }

  const Id* data() const { return _data; }

  size_t size() const { return _cols; }

  size_t cols() const { return _cols; }

  inline friend std::ostream& operator<<(std::ostream& out, const Row& row) {
    for (size_t col = 0; col < row.size(); col++) {
      out << row[col] << ", ";
    }
    out << std::endl;
    return out;
  }
};

// the Iterator for the dynamic IdTable that deals with a number of columns only
// known at runtime
template <bool CONST = false>
class IdTableDynamicIterator {
 public:
  using difference_type = ssize_t;
  using value_type = std::conditional_t<CONST, ConstRow, Row>;
  using pointer = std::conditional_t<CONST, const value_type*, value_type*>;
  using const_pointer = const value_type*;
  using reference = std::conditional_t<CONST, const value_type&, value_type&>;
  using const_reference = const value_type&;
  using iterator_category = std::random_access_iterator_tag;

 private:
  using data_t = std::conditional_t<CONST, const Id*, Id*>;
  data_t _data = nullptr;
  size_t _row = 0;
  size_t _cols = 0;
  value_type _rowView{static_cast<data_t>(nullptr), 0};

 public:
  IdTableDynamicIterator() = default;

  IdTableDynamicIterator(data_t data, size_t row, size_t cols)
      : _data(data),
        _row(row),
        _cols(cols),
        _rowView(data + (cols * row), cols) {}

  /// Allow upgrading from Non-Const to const iterator
  template <bool C = CONST, typename = std::enable_if_t<!C>>
  operator IdTableDynamicIterator<true>() const {
    return {_data, _row, _cols};
  }

  ~IdTableDynamicIterator() = default;

  // Copy and move constructors and assignment operators
  IdTableDynamicIterator(const IdTableDynamicIterator& other) noexcept
      : _data(other._data),
        _row(other._row),
        _cols(other._cols),
        _rowView(_data + (_cols * _row), _cols) {}

  IdTableDynamicIterator& operator=(const IdTableDynamicIterator& other) {
    this->~IdTableDynamicIterator();
    new (this) IdTableDynamicIterator(
        other);  // the copy constructor is noexcept thus this is safe
    return *this;
  }

  // prefix increment
  IdTableDynamicIterator& operator++() {
    ++_row;
    _rowView._data = _data + (_row * _cols);
    return *this;
  }

  // multi-step increment
  IdTableDynamicIterator& operator+=(difference_type i) {
    _row += i;
    _rowView._data = _data + (_row * _cols);
    return *this;
  }

  // postfix increment
  IdTableDynamicIterator operator++(int) {
    IdTableDynamicIterator tmp(*this);
    ++_row;
    _rowView._data = _data + (_row * _cols);
    return tmp;
  }

  // prefix decrement
  IdTableDynamicIterator& operator--() {
    --_row;
    _rowView._data = _data + (_row * _cols);
    return *this;
  }

  // multi-step decrement
  IdTableDynamicIterator& operator-=(difference_type i) {
    _row -= i;
    _rowView._data = _data + (_row * _cols);
    return *this;
  }

  // postfix increment
  IdTableDynamicIterator operator--(int) {
    IdTableDynamicIterator tmp(*this);
    --_row;
    _rowView._data = _data + (_row * _cols);
    return tmp;
  }

  IdTableDynamicIterator operator+(difference_type i) const {
    return IdTableDynamicIterator(_data, _row + i, _cols);
  }

  IdTableDynamicIterator operator-(difference_type i) const {
    return IdTableDynamicIterator(_data, _row - i, _cols);
  }

  difference_type operator-(const IdTableDynamicIterator& other) const {
    return _row - other._row;
  }

  bool operator==(IdTableDynamicIterator const& other) const {
    return _data == other._data && _row == other._row && _cols == other._cols;
  }

  bool operator!=(IdTableDynamicIterator const& other) const {
    return _data != other._data || _row != other._row || _cols != other._cols;
  }

  bool operator<(const IdTableDynamicIterator& other) const {
    return _row < other._row;
  }

  bool operator>(const IdTableDynamicIterator& other) const {
    return _row > other._row;
  }

  bool operator<=(const IdTableDynamicIterator& other) const {
    return _row <= other._row;
  }

  bool operator>=(const IdTableDynamicIterator& other) const {
    return _row >= other._row;
  }

  template <bool C = CONST, typename = std::enable_if_t<!C>>
  reference operator*() {
    return _rowView;
  }

  const_reference operator*() const { return _rowView; }

  template <bool C = CONST, typename = std::enable_if_t<!C>>
  pointer operator->() {
    return &_rowView;
  }

  const value_type* operator->() const { return &_rowView; }

  // access the element that is i steps ahead
  // we need to construct new rows for this which should not be too expensive
  // In addition: Non const rows behave like references since they hold
  // pointers to specific parts of the _data. Thus they behave according to
  // the standard.
  template <bool C = CONST, typename = std::enable_if_t<!C>>
  value_type operator[](difference_type i) {
    return Row(_data + (_row + i) * _cols, _cols);
  }

  const value_type operator[](difference_type i) const {
    return Row(_data + (_row + i) * _cols, _cols);
  }

  size_t row() const { return _row; }

  size_t cols() const { return _cols; }

  size_t size() const { return _cols; }
};

// Common interface specialication for the dynamic (number of columns only known
// at runtime) IdTable
template <typename DATA>
class IdTableImpl<0, DATA> {
  template <int INCOLS, typename INDATA>
  friend class IdTableImpl;

 protected:
  size_t _size = 0;
  size_t _capacity = 0;
  DATA _data;
  size_t _cols = 0;

 public:
  IdTableImpl() = default;

 protected:
  template <int INCOLS, typename INDATA>
  explicit IdTableImpl(const IdTableImpl<INCOLS, INDATA>& o)
      : _size(o._size),
        _capacity(o._capacity),
        _data(o._data),
        _cols(o._cols) {}

  template <int INCOLS, typename INDATA>
  explicit IdTableImpl(IdTableImpl<INCOLS, INDATA>&& o)
      : _size(o._size),
        _capacity(o._capacity),
        _data(std::move(o._data)),
        _cols(o._cols) {}

  Id* data() { return _data.data(); }

  const Id* data() const { return _data.data(); }

  using const_row_type = ConstRow;
  using row_type = Row;
  using iterator = IdTableDynamicIterator<false>;
  using const_iterator = IdTableDynamicIterator<true>;

  using const_row_reference = const_row_type;
  using row_reference = row_type;

  const_row_reference getConstRow(size_t row) const {
    return ConstRow(data() + (row * _cols), _cols);
  }

  row_reference getRow(size_t row) {
    return row_reference(data() + (row * _cols), _cols);
  }

  size_t cols() const { return _cols; }

  /**
   * @brief Sets the number of columns. Should only be called while data is
   *        still empty.
   **/
  void setCols(size_t cols) {
    assert(_data.empty());
    _cols = cols;
  }
};

/**
 * @brief the actual IdTable class that uses all of the above
 * @tparam COLS The number of Columns, 0 means "dynamic (const but runtime)
 * number of columns)
 * @tparam DATA A data Accessor like IdTableVectorWrapper or IdTableViewWrapper
 */
template <int COLS, typename DATA>
class IdTableTemplated : private IdTableImpl<COLS, DATA> {
  // Make all other instantiations of this template friends of this.
  template <int, typename>
  friend class IdTableTemplated;

 protected:
  using Base = IdTableImpl<COLS, DATA>;
  // make stuff from the templated base class accessible
  using Base::_capacity;
  using Base::_cols;
  using Base::_data;
  using Base::_size;

  using Base::getConstRow;
  using Base::getRow;
  using typename Base::const_row_reference;
  using typename Base::row_reference;
  static constexpr float GROWTH_FACTOR = 1.5;

 public:
  using typename Base::const_iterator;
  using typename Base::const_row_type;
  using typename Base::iterator;
  using typename Base::row_type;

  const Id* data() const { return Base::data(); }

  template <typename C = DATA, typename = std::enable_if_t<C::ManagesStorage>>
  Id* data() {
    return const_cast<Id*>(Base::data());
  }
  using Base::cols;
  using Base::setCols;

  IdTableTemplated() = default;

  IdTableTemplated(size_t cols) { setCols(cols); }

  virtual ~IdTableTemplated() = default;

  // Copy constructor
  IdTableTemplated(const IdTableTemplated& other) = default;
  // Move constructor
  IdTableTemplated(IdTableTemplated&& other) noexcept = default;
  // copy assignment
  IdTableTemplated& operator=(const IdTableTemplated& other) = default;
  // move assignment
  IdTableTemplated& operator=(IdTableTemplated&& other) noexcept = default;

 protected:
  // internally convert between "dynamic" and "static" mode and possibly
  // convert from Owning to non owning storage, is used in the conversion
  // functions Base class asserts that number of columns match.
  template <int INCOLS, typename INDATA>
  explicit IdTableTemplated(const IdTableTemplated<INCOLS, INDATA>& o)
      : Base(o) {}

  template <int INCOLS, typename INDATA>
  explicit IdTableTemplated(IdTableTemplated<INCOLS, INDATA>&& o) noexcept
      : Base(std::move(o)) {}

 public:
  bool operator==(const IdTableTemplated& other) const {
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

  // Element access, use the const overload
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  Id& operator()(size_t row, size_t col) {
    return const_cast<Id&>(std::as_const(*this)(row, col));
  }

  const Id& operator()(size_t row, size_t col) const {
    return data()[row * _cols + col];
  }

  // Row access
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  row_reference operator[](size_t row) {
    return getRow(row);
  }

  const_row_reference operator[](size_t row) const {
    // Moving this method to impl allows for efficient ConstRow types when
    // using non dynamic IdTables.
    return getConstRow(row);
  }

  const_iterator begin() const { return cbegin(); }

  // Begin iterator
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  iterator begin() {
    return iterator{data(), 0, _cols};
  }

  const_iterator cbegin() const { return const_iterator{data(), 0, _cols}; }

  const_iterator end() const { return cend(); }

  // End iterator
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  iterator end() {
    return iterator{data(), _size, _cols};
  }

  const_iterator cend() const { return const_iterator{data(), _size, _cols}; }

  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  row_reference back() {
    return getRow((end() - 1).row());
  }

  const_row_reference back() const { return getConstRow((end() - 1).row()); }

  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void emplace_back() {
    push_back();
  }

  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void push_back() {
    if (_size + 1 >= _capacity) {
      grow();
    }
    _size++;
  }

  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void push_back(const std::initializer_list<Id>& init) {
    assert(init.size() == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(data() + _size * _cols, init.begin(), sizeof(Id) * _cols);
    _size++;
  }

  /**
   * @brief Read cols() elements from init and stores them in a new row
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
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
  template <
      typename T,
      typename = std::enable_if_t<
          DATA::ManagesStorage &&
          (std::is_same_v<std::decay_t<T>, std::decay_t<row_type>> ||
           std::is_same_v<std::decay_t<T>, std::decay_t<const_row_type>>)>>
  void push_back(const T& init) {
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(data() + _size * _cols, init.data(), sizeof(Id) * _cols);
    _size++;
  }

  template <typename INDATA, typename = std::enable_if_t<DATA::ManagesStorage>>
  void push_back(const IdTableTemplated<COLS, INDATA>& init, size_t row) {
    assert(init._cols == _cols);
    if (_size + 1 >= _capacity) {
      grow();
    }
    std::memcpy(data() + _size * _cols, init.data() + row * _cols,
                sizeof(Id) * _cols);
    _size++;
  }

  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void pop_back() {
    if (_size > 0) {
      _size--;
    }
  }

  /**
   * @brief Inserts the elements in the range [begin;end) before pos.
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void insert(const iterator& pos, const const_iterator& begin,
              const const_iterator& end) {
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

    size_t afterTarget = size() - target;
    // Move the data currently in the way back.
    std::memmove(data() + (target + numNewRows) * _cols,
                 data() + target * _cols, sizeof(Id) * _cols * afterTarget);

    _size += numNewRows;
    // Copy the new data
    std::memcpy(data() + target * _cols, (*begin).data(),
                sizeof(Id) * _cols * numNewRows);
  }

  /**
   * @brief Erases all rows in the range [begin;end)
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void erase(const iterator& beginIt,
             const iterator& endIt = iterator(nullptr, 0, 0)) {
    iterator actualEnd = endIt;
    if (actualEnd == iterator(nullptr, 0, 0)) {
      actualEnd = iterator(data(), beginIt.row() + 1, _cols);
    }
    if (actualEnd.row() > this->end().row()) {
      actualEnd = this->end();
    }
    if (actualEnd.row() <= beginIt.row()) {
      return;
    }
    size_t numErased = actualEnd.row() - beginIt.row();
    size_t numToMove = size() - actualEnd.row();
    std::memmove(data() + beginIt.row() * _cols,
                 data() + actualEnd.row() * _cols,
                 numToMove * _cols * sizeof(Id));
    _size -= numErased;
  }

  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void clear() {
    _size = 0;
  }

  /**
   * @brief Ensures this table has enough space allocated to store rows many
   *        rows of data
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void reserve(size_t rows) {
    if (_capacity < rows) {
      // Add rows - _capacity many new rows
      grow(rows - _capacity);
    }
  }

  /**
   * @brief Resizes this IdTableTemplated to have at least row many rows.
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void resize(size_t rows) {
    if (rows > _size) {
      reserve(rows);
      _size = rows;
    } else if (rows < _size) {
      _size = rows;
    }
  }

  /**
   * @brief Creates an IdTableTemplated<NEW_COLS> that now owns this
   * id tables data. This is effectively a move operation that also
   * changes the type to its equivalent dynamic variant.
   **/
  template <int NEW_COLS, typename = std::enable_if_t<DATA::ManagesStorage>>
  IdTableTemplated<NEW_COLS, DATA> moveToStatic() {
    return IdTableTemplated<NEW_COLS, DATA>(
        std::move(*this));  // let the private conversions do all the work
  };

  /**
   * @brief Creates an IdTable that now owns this
   * id tables data. This is effectively a move operation that also
   * changes the type to an equivalent one.
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  IdTableTemplated<0, DATA> moveToDynamic() {
    return IdTableTemplated<0, DATA>(std::move(*this));
  };

  /**
   * @brief Create a non-owning and readOnly view into this data that has a
   * static width.
   * @tparam NEW_COLS The number of Columns. Must be the actual number of
   * columns this table already has or an assertion will fail
   */
  template <int NEW_COLS>
  const IdTableTemplated<NEW_COLS, IdTableViewWrapper> asStaticView() const {
    return IdTableTemplated<NEW_COLS, IdTableViewWrapper>(
        *static_cast<const IdTableTemplated<COLS, DATA>*>(this));
  };

  /**
   * @brief Create a copy of a non-owning view that copies all the data and thus
   * is owning again.
   *
   * This is an expensive operation and so it has an explicit name.
   * @return
   */
  template <typename = std::enable_if_t<!DATA::ManagesStorage>>
  const IdTableTemplated<COLS, IdTableVectorWrapper> clone() const {
    return IdTableTemplated<COLS, IdTableVectorWrapper>(*this);
  };

  // Size access
  size_t rows() const { return _size; }
  size_t size() const { return _size; }

 private:
  /**
   * @brief Grows the storage of this IdTableTemplated
   * @param newRows If newRows is 0 the storage is grown by GROWTH_FACTOR. If
   *        newRows is any other number the vector is grown by newRows many
   *        rows.
   **/
  template <typename = std::enable_if_t<DATA::ManagesStorage>>
  void grow(size_t newRows = 0) {
    size_t new_capacity;
    if (newRows == 0) {
      new_capacity = _capacity * GROWTH_FACTOR + 1;
    } else {
      new_capacity = _capacity + newRows;
    }

    this->_data._data.resize(new_capacity * this->_cols);
    _capacity = new_capacity;
  }

  // Support for ostreams
  friend std::ostream& operator<<(std::ostream& out,
                                  const IdTableTemplated<COLS, DATA>& table) {
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
};

}  // namespace detail

/// The general IdTable class. Can be modified and owns its data. If COLS > 0,
/// COLS specifies the compile-time number of columns COLS == 0 means "runtime
/// number of cols"
template <int COLS>
using IdTableStatic =
    detail::IdTableTemplated<COLS, detail::IdTableVectorWrapper>;

// the "runtime number of cols" variant
using IdTable = IdTableStatic<0>;

/// A constant view into an IdTable that does not own its data
template <int COLS>
using IdTableView = detail::IdTableTemplated<COLS, detail::IdTableViewWrapper>;
