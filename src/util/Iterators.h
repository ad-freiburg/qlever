//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ITERATORS_H
#define QLEVER_ITERATORS_H

namespace ad_utility {

/// A lambda that accesses the `i`-th element in a `randomAccessContainer`
/// using `operator[]`
inline auto accessViaBracketOperator = [](auto&& randomAccessContainer,
                                          auto i) {
  return randomAccessContainer[i];
};
using AccessViaBracketOperator = decltype(accessViaBracketOperator);

/**
 * @brief Provide random access iterators for a random access container that
 * allows direct access to the `i-th` element in the structure.
 * @tparam RandomAccessContainer A random access container that can be randomly
 * accessed using consecutive indices (see below).
 * @tparam Accessor A function such that `Accessor(RandomAccessContainer,
 * uint64_t i)` returns the `i`-th element from the random access container. If
 * iterators for indices `a` and `b` can be obtained from the random access
 * container (typically by `begin()` and `end()` member functions, then it must
 * be legal to call the accessor for all `i` in `[a, b)`.
 */
template <typename RandomAccessContainer,
          typename Accessor = AccessViaBracketOperator>
class IteratorForAccessOperator {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using index_type = uint64_t;
  using value_type = std::remove_reference_t<
      std::invoke_result_t<Accessor, const RandomAccessContainer&, index_type>>;
  using difference_type = int64_t;

 private:
  const RandomAccessContainer* _vector = nullptr;
  index_type _index{0};
  Accessor _accessor{};

 public:
  IteratorForAccessOperator() = default;
  IteratorForAccessOperator(const RandomAccessContainer* vec, index_type index)
      : _vector{vec}, _index{index} {}

  // Comparisons
  auto operator<=>(const IteratorForAccessOperator& rhs) const {
    return (_index <=> rhs._index);
  }
  bool operator==(const IteratorForAccessOperator& rhs) const {
    return _index == rhs._index;
  }

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

  friend IteratorForAccessOperator operator+(
      difference_type n, const IteratorForAccessOperator& it) {
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
    return static_cast<difference_type>(_index) -
           static_cast<difference_type>(rhs._index);
  }

  decltype(auto) operator*() const { return _accessor(*_vector, _index); }

  decltype(auto) operator[](difference_type n) const {
    return _accessor(*_vector, _index + n);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_ITERATORS_H
