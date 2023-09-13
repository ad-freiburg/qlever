//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ITERATORS_H
#define QLEVER_ITERATORS_H

#include <cstdint>
#include <iterator>
#include <type_traits>

#include "util/Enums.h"

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
 // TODO<joka921> Document the mixin.
template <typename Derived, typename RandomAccessContainer,
          typename Accessor = AccessViaBracketOperator,
          IsConst isConstTag = IsConst::True, typename ValueType = void,
          typename Reference = void>
class IteratorForAccessOperatorMixin {
 public:
  static constexpr bool isConst = isConstTag == IsConst::True;
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;
  using index_type = uint64_t;
  // It is possible to explicitly specify the `value_type` and `reference_type`
  // if they differ from the defaults. For an example, see the `IdTable` class
  // which uses a proxy type as its `reference`.
  using value_type = std::conditional_t<
      !std::is_void_v<ValueType>, ValueType,
      std::remove_reference_t<std::invoke_result_t<
          Accessor,
          std::conditional_t<isConst, const RandomAccessContainer&,
                             RandomAccessContainer&>,
          index_type>>>;
  using reference =
      std::conditional_t<!std::is_void_v<Reference>, Reference, value_type&>;
  using pointer = value_type*;

 private:
  using RandomAccessContainerPtr =
      std::conditional_t<isConst, const RandomAccessContainer*,
                         RandomAccessContainer*>;
  RandomAccessContainerPtr _vector = nullptr;
  index_type _index{0};
  Accessor _accessor{};

 public:
  IteratorForAccessOperatorMixin() requires std::is_default_constructible_v<Accessor>
  = default;
  IteratorForAccessOperatorMixin(RandomAccessContainerPtr vec, index_type index,
                            Accessor accessor = Accessor{})
      : _vector{vec}, _index{index}, _accessor{std::move(accessor)} {}

  IteratorForAccessOperatorMixin(const IteratorForAccessOperatorMixin&) = default;
  IteratorForAccessOperatorMixin(IteratorForAccessOperatorMixin&&) noexcept = default;
  IteratorForAccessOperatorMixin& operator=(const IteratorForAccessOperatorMixin& other) =
      default;
  IteratorForAccessOperatorMixin& operator=(IteratorForAccessOperatorMixin&&) noexcept =
      default;

  /*
  template <IsConst tagA, IsConst tagB>
  friend auto operator<=>(const IteratorForAccessOperator<RandomAccessContainer, Accessor, tagA, ValueType, Reference>& lhs, const IteratorForAccessOperator<RandomAccessContainer, Accessor, tagB, ValueType, Reference>& rhs) {
    return (lhs._index <=> rhs._index);
  }
   */
  friend auto operator<=>(const Derived& lhs, const Derived& rhs) {
      return (lhs._index <=> rhs._index);
  }
  bool operator==(const IteratorForAccessOperatorMixin& rhs) const {
    return _index == rhs._index;
  }

  Derived& operator+=(difference_type n) {
    _index += n;
    return static_cast<Derived&>(*this);
  }
  Derived operator+(difference_type n) const {
    Derived result{*this};
    result += n;
    return result;
  }

  Derived& operator++() {
    ++_index;
   return static_cast<Derived&>(*this);
  }
  Derived operator++(int) & {
    Derived result{*this};
    ++_index;
    return result;
  }

  Derived& operator--() {
    --_index;
    return static_cast<Derived&>(*this);
  }
  Derived operator--(int) & {
    Derived result{*this};
    --_index;
    return result;
  }

  friend Derived operator+(
      difference_type n, const Derived& it) {
    return it + n;
  }

  Derived& operator-=(difference_type n) {
    _index -= n;
    return static_cast<Derived&>(*this);
  }

  Derived operator-(difference_type n) const {
    Derived result{*this};
    result -= n;
    return result;
  }

  difference_type operator-(const Derived& rhs) const {
    return static_cast<difference_type>(_index) -
           static_cast<difference_type>(rhs._index);
  }

  decltype(auto) operator*() const { return _accessor(*_vector, _index); }
  decltype(auto) operator*() requires(!isConst) {
    return _accessor(*_vector, _index);
  }

  // Only allowed, if `RandomAccessContainer` yields references and not values
  template <typename A = Accessor, typename P = RandomAccessContainerPtr>
  requires requires(A a, P p, uint64_t i) {
    { &a(*p, i) };
  } auto operator->() requires(!isConst) {
    return &(*(*this));
  }
  template <typename A = Accessor, typename P = RandomAccessContainerPtr>
  requires requires(A a, P p, uint64_t i) {
    { &a(*p, i) };
  } auto operator->() const {
    return &(*(*this));
  }

  decltype(auto) operator[](difference_type n) const {
    return _accessor(*_vector, _index + n);
  }
};

    template <typename RandomAccessContainer,
            typename Accessor = AccessViaBracketOperator,
            IsConst isConstTag = IsConst::True, typename ValueType = void,
            typename Reference = void>
    struct IteratorForAccessOperator : public IteratorForAccessOperatorMixin<IteratorForAccessOperator<RandomAccessContainer, Accessor, isConstTag, ValueType, Reference>, RandomAccessContainer, Accessor, isConstTag, ValueType, Reference> {
  using Base = IteratorForAccessOperatorMixin<IteratorForAccessOperator<RandomAccessContainer, Accessor, isConstTag, ValueType, Reference>, RandomAccessContainer, Accessor, isConstTag, ValueType, Reference>;
  using Base::Base;
  IteratorForAccessOperator(const Base& b) : Base{b}{}
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
