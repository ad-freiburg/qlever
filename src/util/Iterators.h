//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ITERATORS_H
#define QLEVER_ITERATORS_H

#include <iterator>
#include <type_traits>

namespace ad_utility {

/// A lambda that accesses the `i`-th element in a `randomAccessContainer`
/// using `operator[]`
inline auto accessViaBracketOperator = [](auto&& randomAccessContainer,
                                          auto i) -> decltype(auto) {
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
 *
 * Note: Many STL algorithms (like `std::sort` require iterator types to be
 * assignable. `IteratorForAccessOperator` is only assignable if the `Accessor`
 * type is assignable. If you want to use a lambda as the accessor (which is not
 * assignable), consider using `ad_utility::makeAssignableLambda` in
 * `LambdaHelpers.h`.
 */
template <typename RandomAccessContainer,
          typename Accessor = AccessViaBracketOperator, bool IsConst = true>
class IteratorForAccessOperator {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;
  using index_type = uint64_t;
  using value_type = std::remove_reference_t<
      std::invoke_result_t<Accessor, const RandomAccessContainer&, index_type>>;
  using pointer = value_type*;
  using reference = value_type&;

 private:
  using RandomAccessContainerPtr =
      std::conditional_t<IsConst, const RandomAccessContainer*,
                         RandomAccessContainer*>;
  RandomAccessContainerPtr _vector = nullptr;
  index_type _index{0};
  Accessor _accessor{};

 public:
  IteratorForAccessOperator() = default;
  IteratorForAccessOperator(RandomAccessContainerPtr vec, index_type index,
                            Accessor accessor = Accessor{})
      : _vector{vec}, _index{index}, _accessor{std::move(accessor)} {}

  IteratorForAccessOperator(const IteratorForAccessOperator&) = default;
  IteratorForAccessOperator(IteratorForAccessOperator&&) noexcept = default;
  IteratorForAccessOperator& operator=(const IteratorForAccessOperator& other) =
      default;
  IteratorForAccessOperator& operator=(IteratorForAccessOperator&&) noexcept =
      default;

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
  decltype(auto) operator*() { return _accessor(*_vector, _index); }

  // Only allowed, if `RandomAccessContainer` yields references and not values
  template <typename A = Accessor, typename P = RandomAccessContainerPtr>
  requires requires(A a, P p, uint64_t i) {
    {&a(*p, i)};
  }
  auto operator->() { return &(*(*this)); }
  template <typename A = Accessor, typename P = RandomAccessContainerPtr>
  requires requires(A a, P p, uint64_t i) {
    {&a(*p, i)};
  }
  auto operator->() const { return &(*(*this)); }

  decltype(auto) operator[](difference_type n) const {
    return _accessor(*_vector, _index + n);
  }
};

/// If `T` is a type that can safely be moved from (e.g. std::vector<int> or
/// std::vector<int>&&), then return `std::make_move_iterator(iterator)`. Else
/// (for example if `T` is `std::vector<int>&` or `const std::vector<int>&` the
/// iterator is returned unchanged. Typically used in generic code where we need
/// the semantics of "if `std::forward` would move the container, we can also
/// safely move the single elements of the container."
template <typename T, typename It>
auto makeForwardingIterator(It iterator) {
  if constexpr (std::is_rvalue_reference_v<T&&>) {
    return std::make_move_iterator(iterator);
  } else {
    return iterator;
  }
}

}  // namespace ad_utility

#endif  // QLEVER_ITERATORS_H
