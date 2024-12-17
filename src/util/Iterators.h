//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ITERATORS_H
#define QLEVER_ITERATORS_H

#include <cstdint>
#include <iterator>
#include <type_traits>

#include "util/Enums.h"
#include "util/TypeTraits.h"

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
          typename Accessor = AccessViaBracketOperator,
          IsConst isConstTag = IsConst::True, typename ValueType = void,
          typename Reference = void>
class IteratorForAccessOperator {
 public:
  static constexpr bool isConst = isConstTag == IsConst::True;
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;
  using index_type = uint64_t;
  // It is possible to explicitly specify the `value_type` and `reference`
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
  IteratorForAccessOperator() requires std::is_default_constructible_v<Accessor>
  = default;
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

// This CRTP-Mixin can be used to add iterators to a simple state-machine like
// class, s.t. it behaves like an `InputRange`. The derived class needs the
// following functions: `start()`, `isFinished()`, `get()` , `next()`.
// * `void start()` -> called when `begin()` is called to allow for deferred
// initialization. After calling `start()` either `get()` must return the first
// element, or `isFinished()` must return true ( for an empty range).
// * `bool isFinished()` -> has to return true if there are no more values, and
// calls to `get()` are thus impossible.
// * `reference get()` -> get the current value (typically as a reference).
// * `void next()` advance to the next value. After calling `next()` either
// `isFinished()` must be true, or `get()` must return the next value.
template <typename Derived>
class InputRangeMixin {
 public:
  // Cast `this` to the derived class for easier access.
  Derived& derived() { return static_cast<Derived&>(*this); }
  const Derived& derived() const { return static_cast<const Derived&>(*this); }

  // A simple sentinel which is returned by the call to `end()`.
  struct Sentinel {};

  // The iterator class.
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::int64_t;
    using reference = decltype(std::declval<Derived&>().get());
    using value_type = std::remove_reference_t<reference>;
    using pointer = value_type*;
    InputRangeMixin* mixin_ = nullptr;

   public:
    Iterator() = default;
    explicit Iterator(InputRangeMixin* mixin) : mixin_{mixin} {}
    Iterator& operator++() {
      mixin_->derived().next();
      return *this;
    }

    // Needed for the `range` concept.
    void operator++(int) { (void)operator++(); }

    decltype(auto) operator*() { return mixin_->derived().get(); }
    decltype(auto) operator*() const { return mixin_->derived().get(); }
    decltype(auto) operator->() { return std::addressof(operator*()); }
    decltype(auto) operator->() const { return std::addressof(operator*()); }

    // The comparison `it == end()` just queries `isFinished()` , so an empty
    // `Sentinel` suffices.
    friend bool operator==(const Iterator& it, Sentinel) {
      return it.mixin_->derived().isFinished();
    }
    friend bool operator==(Sentinel s, const Iterator& it) { return it == s; }
    friend bool operator!=(const Iterator& it, Sentinel s) {
      return !(it == s);
    }
    friend bool operator!=(Sentinel s, const Iterator& it) {
      return !(it == s);
    }
  };

 public:
  // The only functions needed to make this a proper range: `begin()` and
  // `end()`.
  Iterator begin() {
    derived().start();
    return Iterator{this};
  }
  Sentinel end() const { return {}; };
};

// A similar mixin to the above, with slightly different characteristics:
// 1. It only requires a single function `std::optional<ValueType> get()
// override`
// 2. It uses simple inheritance with virtual functions, which allows for type
// erasure of different ranges with the same `ValueType`.
// 3. While the interface is simpler (see 1.+2.) each step in iterating is a
// little bit more complex, as the mixin has to store the value. This might be
// less efficient for very simple generators, because the compiler might be able
// to optimize this mixin as well as the one above.
template <typename ValueType>
class InputRangeFromGet {
 public:
  using Storage = std::optional<ValueType>;
  Storage storage_ = std::nullopt;

 private:
  // The single virtual function which has to be overloaded. `std::nullopt`
  // means that there will be no more values.
  virtual Storage get() = 0;

 public:
  virtual ~InputRangeFromGet() = default;

  // Get the next value and store it.
  void getNextAndStore() { storage_ = get(); }

  struct Sentinel {};
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::int64_t;
    using value_type = typename InputRangeFromGet::Storage::value_type;
    using pointer = value_type*;
    using reference = std::add_lvalue_reference_t<value_type>;
    using const_reference = std::add_const_t<reference>;
    InputRangeFromGet* mixin_ = nullptr;

   public:
    Iterator() = default;
    explicit Iterator(InputRangeFromGet* mixin) : mixin_{mixin} {}
    Iterator& operator++() {
      mixin_->getNextAndStore();
      return *this;
    }

    // Needed for the `range` concept.
    void operator++(int) { (void)operator++(); }

    reference operator*() { return mixin_->storage_.value(); }
    const_reference operator*() const { return mixin_->storage_.value(); }
    decltype(auto) operator->() { return std::addressof(operator*()); }
    decltype(auto) operator->() const { return std::addressof(operator*()); }

    friend bool operator==(const Iterator& it, Sentinel) {
      return !it.mixin_->storage_.has_value();
    }
    friend bool operator==(Sentinel s, const Iterator& it) { return it == s; }
    friend bool operator!=(const Iterator& it, Sentinel s) {
      return !(it == s);
    }
    friend bool operator!=(Sentinel s, const Iterator& it) {
      return !(it == s);
    }
  };

  Iterator begin() {
    getNextAndStore();
    return Iterator{this};
  }
  Sentinel end() const { return {}; };
};

// This class takes an arbitrary input range, and turns it into a class that
// inherits from `InputRangeFromGet` (see above). While this adds a layer of
// indirection, it makes type erasure between input ranges with the same value
// type very simple.
template <typename Range>
class RangeToInputRangeFromGet
    : public InputRangeFromGet<ql::ranges::range_value_t<Range>> {
  Range range_;
  using Iterator = ql::ranges::iterator_t<Range>;
  std::optional<Iterator> iterator_ = std::nullopt;
  bool isDone() { return iterator_ == ql::ranges::end(range_); }

 public:
  explicit RangeToInputRangeFromGet(Range range) : range_{std::move(range)} {}

  // As we use the `InputRangeOptionalMixin`, we only have to override the
  // single `get()` method.
  std::optional<ql::ranges::range_value_t<Range>> get() override {
    if (!iterator_.has_value()) {
      // For the very first value we have to call `begin()`.
      iterator_ = ql::ranges::begin(range_);
      if (isDone()) {
        return std::nullopt;
      }
    } else {
      // Not the first value, so we have to advance the iterator.
      if (isDone()) {
        return std::nullopt;
      }
      ++iterator_.value();
    }

    // We now  have advanced the iterator to the next value, so we can return it
    // if existing.
    if (isDone()) {
      return std::nullopt;
    }
    return std::move(*iterator_.value());
  }
};

// A simple type-erased input range (that is, one class for *any* input range
// with the given `ValueType`). It internally uses the `InputRangeOptionalMixin`
// from above as an implementation detail.
template <typename ValueType>
class InputRangeTypeErased {
  // Unique (and therefore owning) pointer to the virtual base class.
  std::unique_ptr<InputRangeFromGet<ValueType>> impl_;

 public:
  // Constructor for ranges that directly inherit from
  // `InputRangeOptionalMixin`.
  template <typename Range>
  requires std::is_base_of_v<InputRangeFromGet<ValueType>, Range>
  explicit InputRangeTypeErased(Range range)
      : impl_{std::make_unique<Range>(std::move(range))} {}

  // Constructor for all other ranges. We first pass them through the
  // `InputRangeToOptional` class from above to make it compatible with the base
  // class.
  template <typename Range>
  requires(!std::is_base_of_v<InputRangeFromGet<ValueType>, Range> &&
           ql::ranges::range<Range> &&
           std::same_as<ql::ranges::range_value_t<Range>, ValueType>)
  explicit InputRangeTypeErased(Range range)
      : impl_{std::make_unique<RangeToInputRangeFromGet<Range>>(
            std::move(range))} {}

  decltype(auto) begin() { return impl_->begin(); }
  decltype(auto) end() { return impl_->end(); }
  using iterator = typename InputRangeFromGet<ValueType>::Iterator;
};
}  // namespace ad_utility

#endif  // QLEVER_ITERATORS_H
