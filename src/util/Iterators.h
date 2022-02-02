//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ITERATORS_H
#define QLEVER_ITERATORS_H

namespace ad_utility {

inline auto accessViaBracketOperator = [](auto&& dataStructure, auto index) {
  return dataStructure[index];
};
using AccessViaBracketOperator = decltype(accessViaBracketOperator);

template <typename DataStructure, typename Accessor = AccessViaBracketOperator , typename index_type = uint64_t, typename DifferenceType = int64_t>
struct IteratorForAccessOperator {
 private:
  const DataStructure* _vector = nullptr;
  index_type _index{0};
  Accessor _accessor;

 public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = std::remove_reference_t<std::invoke_result_t<Accessor, const DataStructure&, index_type>>;
  using difference_type = DifferenceType;

  IteratorForAccessOperator(const DataStructure* vec, index_type index)
      : _vector{vec}, _index{index} {}
  IteratorForAccessOperator() = default;

  auto operator<=>(const IteratorForAccessOperator& rhs) const {
    return (_index <=> rhs._index);
  }

  bool operator==(const IteratorForAccessOperator& rhs) const { return _index == rhs._index; }

  IteratorForAccessOperator& operator+=(difference_type n) {
    _index += n;
    return *this;
  }
  IteratorForAccessOperator operator+(difference_type n) const {
    IteratorForAccessOperator result{*this};
    result += n;
    return result;
  }

  IteratorForAccessOperator& operator++() {
    ++_index;
    return *this;
  }
  IteratorForAccessOperator operator++(int) & {
    IteratorForAccessOperator result{*this};
    ++_index;
    return result;
  }

  IteratorForAccessOperator& operator--() {
    --_index;
    return *this;
  }
  IteratorForAccessOperator operator--(int) & {
    IteratorForAccessOperator result{*this};
    --_index;
    return result;
  }

  friend IteratorForAccessOperator operator+(difference_type n, const IteratorForAccessOperator& it) {
    return it + n;
  }

  IteratorForAccessOperator& operator-=(difference_type n) {
    _index -= n;
    return *this;
  }

  IteratorForAccessOperator operator-(difference_type n) const {
    IteratorForAccessOperator result{*this};
    result -= n;
    return result;
  }

  difference_type operator-(const IteratorForAccessOperator& rhs) const {
    return static_cast<difference_type>(_index) - static_cast<difference_type>(rhs._index);
  }

  decltype(auto) operator*() const { return _accessor(*_vector, _index); }

  decltype(auto) operator[](difference_type n) const { return _accessor(*_vector, _index + n); }
};

}

#endif  // QLEVER_ITERATORS_H
