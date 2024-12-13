//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ITERATORS_H
#define QLEVER_ITERATORS_H

#include <c++/11/bits/ranges_base.h>

#include <boost/range/any_range.hpp>
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

// TODO <joka921> Comment.
template <typename Derived>
class InputRangeMixin {
 public:
  Derived& derived() { return static_cast<Derived&>(*this); }
  const Derived& derived() const { return static_cast<const Derived&>(*this); }

  struct Sentinel {};
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::int64_t;
    using reference_type = decltype(std::declval<Derived&>().get());
    using value_type = std::remove_reference_t<reference_type>;
    InputRangeMixin* mixin_;

   public:
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
  Iterator begin() {
    derived().start();
    return Iterator{this};
  }
  Sentinel end() const { return {}; };
};

// TODO <joka921> Comment.
template <typename ValueType>
class InputRangeOptionalMixin {
 public:
  using Storage = std::optional<ValueType>;
  Storage storage = std::nullopt;
  bool isDone = false;

 private:
  virtual Storage get() = 0;

 public:
  virtual ~InputRangeOptionalMixin() = default;

  void getNext() {
    storage = get();
    isDone = !storage.has_value();
  }

  struct Sentinel {};
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::int64_t;
    using value_type = typename InputRangeOptionalMixin::Storage::value_type;
    using reference_type = std::add_lvalue_reference_t<value_type>;
    using const_reference_type = std::add_const_t<reference_type>;
    InputRangeOptionalMixin* mixin_;

   public:
    explicit Iterator(InputRangeOptionalMixin* mixin) : mixin_{mixin} {}
    Iterator& operator++() {
      mixin_->getNext();
      return *this;
    }

    // Needed for the `range` concept.
    void operator++(int) { (void)operator++(); }

    reference_type operator*() { return mixin_->storage.value(); }
    const_reference_type operator*() const { return mixin_->storage.value(); }
    decltype(auto) operator->() { return std::addressof(operator*()); }
    decltype(auto) operator->() const { return std::addressof(operator*()); }

    friend bool operator==(const Iterator& it, Sentinel) {
      return it.mixin_->isDone;
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
  Iterator begin() {
    getNext();
    return Iterator{this};
  }
  Sentinel end() const { return {}; };
};

template <typename Range>
class InputRangeToOptional
    : public InputRangeOptionalMixin<std::ranges::range_value_t<Range>> {
  Range range_;
  using Iterator = std::ranges::iterator_t<Range>;
  std::optional<Iterator> iterator_ = std::nullopt;
  bool isDone() const { return iterator_ == std::ranges::end(range_); }

 public:
  explicit InputRangeToOptional(Range range) : range_{std::move(range)} {}
  std::optional<std::ranges::range_value_t<Range>> get() override {
    if (!iterator_.has_value()) {
      iterator_ = std::ranges::begin(range_);
      if (isDone()) {
        return std::nullopt;
      }
    } else {
      if (isDone()) {
        return std::nullopt;
      }
      ++iterator_.value();
    }
    if (isDone()) {
      return std::nullopt;
    }
    return std::move(*iterator_.value());
  }
};

template <typename ValueType>
class TypeEraseInputRangeOptionalMixin {
  std::unique_ptr<InputRangeOptionalMixin<ValueType>> impl_;

 public:
  template <typename Range>
  requires std::is_base_of_v<InputRangeOptionalMixin<ValueType>, Range>
  explicit TypeEraseInputRangeOptionalMixin(Range range)
      : impl_{std::make_unique<Range>(std::move(range))} {}

  template <typename Range>
  requires(!std::is_base_of_v<InputRangeOptionalMixin<ValueType>, Range> &&
           std::ranges::range<Range> &&
           std::same_as<std::ranges::range_value_t<Range>, ValueType>)
  explicit TypeEraseInputRangeOptionalMixin(Range range)
      : impl_{std::make_unique<InputRangeToOptional<Range>>(std::move(range))} {
  }
  // TODO<joka921> For performance implications we could implement a function
  // that perfectly forwards the arguments. This would also work for non-movable
  // ranges...
  decltype(auto) begin() { return impl_->begin(); }
  decltype(auto) end() { return impl_->end(); }
};

/*
template <typename ValueType>
class TypeErasedRange {
  std::unique_ptr<InputRangeOptionalMixin<ValueType>> impl_;
}

namespace typeErasedIterators {
  struct Sentinel {};

  template <typename ValueType, typename ReferenceType,
            typename ConstReferenceType>
  class Iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using difference_type = std::int64_t;
    using value_type = ValueType;
    using reference_type = ReferenceType;
    using const_reference_type = ConstReferenceType;

    // TODO<joka921> Currently we require the underlying iterators to be
    // copyable...
    std::any underlyingIterator_;
    std::any underlyingSentinel_;

    using Increment = std::function<void(Iterator&)>;
    Increment increment_;
    using IsDone = std::function<bool(const Iterator&)>;
    IsDone isDone_;
    using Get = std::function<reference_type(Iterator&)>;
    Get get_;

    template <typename It>
    static Increment makeIncrement() {
      return [](Iterator& iterator) {
        auto* it = std::any_cast<It>(&iterator.underlyingIterator_);
        AD_CORRECTNESS_CHECK(it != nullptr);
        ++(*it);
      };
    }

    template <typename It, typename Sentinel>
    static IsDone makeIsDone() {
      return [](Iterator& iterator) {
        auto* it = std::any_cast<It>(&iterator.underlyingIterator_);
        auto* sentinel = std::any_cast<Sentinel>(&iterator.underlyingSentinel_);
        AD_CORRECTNESS_CHECK(it != nullptr && sentinel != nullptr);
        return (*it == *sentinel);
      };
    }

    template <typename It>
    static Get makeGet() {
      return [](Iterator& iterator) -> decltype(auto) {
        auto* it = std::any_cast<It>(&iterator.underlyingIterator_);
        AD_CORRECTNESS_CHECK(it != nullptr);
        return *it;
      };
    }

   public:
    template <typename It, typename Sentinel>
    explicit Iterator(It iterator, Sentinel sentinel)
        : underlyingIterator_(std::move(iterator)),
          underlyingSentinel_(std::move(sentinel)),
          increment_(makeIncrement<It>()),
          isDone_(makeIsDone<It, Sentinel>()),
          get_(makeGet<It>) {}
    Iterator& operator++() {
      std::invoke(increment_, *this);
      return *this;
    }

    // Needed for the `range` concept.
    void operator++(int) { (void)operator++(); }

    reference_type operator*() { return std::invoke(get_, *this); }
    // TODO<joka921> Does this adding of constness always work?
    const_reference_type operator*() const { return std::invoke(get_, *this); }
    decltype(auto) operator->() { return std::addressof(operator*()); }
    decltype(auto) operator->() const { return std::addressof(operator*()); }

    friend bool operator==(const Iterator& it, Sentinel) {
      return it.isDone_(it);
    }
    friend bool operator==(Sentinel s, const Iterator& it) { return it == s; }
    friend bool operator!=(const Iterator& it, Sentinel s) {
      return !(it == s);
    }
    friend bool operator!=(Sentinel s, const Iterator& it) {
      return !(it == s);
    }
  };
};  // namespace typeErasedIterators

template <typename ValueType,
          typename ReferenceType = std::add_lvalue_reference_t<ValueType>>
class TypeErasedInputRange {
 public:
  void* range_;
  using Iterator =
      typeErasedIterators::Iterator<ValueType, ReferenceType,
                                    std::add_const_t<ReferenceType>>;
  using Sentinel = typeErasedIterators::Sentinel;
  std::function<Iterator()> begin_;

 public:
  Iterator begin() {
    getNext();
    return Iterator{this};
  }
  Sentinel end() const { return {}; };
};
*/

/*
template <typename ValueType,
          typename ReferenceType = std::add_lvalue_reference_t<ValueType>>
class TypeErasedInputRange {
  using AnyRange = boost::any_range<ValueType,
boost::single_pass_traversal_tag, ReferenceType>; struct TypeErasedDeleter {
  private:
    std::function<void(void*)> delete_;
    explicit TypeErasedDeleter(std::function<void(void*)> deleter)
        : delete_{std::move(deleter)} {}

  public:
    void operator()(void* ptr) const { delete_(ptr); }
    template <typename T>
    static TypeErasedDeleter make() {
      return TypeErasedDeleter{[](void* ptr) { delete static_cast<T*>(ptr);
}};
    }
  };
  std::unique_ptr<void, TypeErasedDeleter> underlyingRange_;
  AnyRange anyRange_;

public:
  // TODO<joka921> Constrain by concepts.
  template <typename Range>
  explicit TypeErasedInputRange(Range r)
      : underlyingRange_{new Range{std::move(r)},
                         TypeErasedDeleter::template make<Range>()},
        anyRange_{*static_cast<Range*>(underlyingRange_.get())} {}

  auto begin() {return anyRange_.begin(); }
  auto end() { return anyRange_.end(); }
  // TODO<joka921> If this works, add const overloads.
};
*/

}  // namespace ad_utility

#endif  // QLEVER_ITERATORS_H
