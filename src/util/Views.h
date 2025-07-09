//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_VIEWS_H
#define QLEVER_SRC_UTIL_VIEWS_H

#include <future>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "util/ExceptionHandling.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ResetWhenMoved.h"

namespace ad_utility {

namespace detail {
template <typename R>
CPP_requires(can_empty_, requires(const R& range)(ql::ranges::empty(range)));

template <typename R>
CPP_concept RangeCanEmpty = CPP_requires_ref(can_empty_, R);
}  // namespace detail

// Takes a view of blocks and yields the elements of the same view, but removes
// consecutive duplicates inside the blocks and across block boundaries.
template <typename SortedBlockView,
          typename ValueType = ql::ranges::range_value_t<
              ql::ranges::range_value_t<SortedBlockView>>>
cppcoro::generator<typename SortedBlockView::value_type> uniqueBlockView(
    SortedBlockView view) {
  size_t numInputs = 0;
  size_t numUnique = 0;
  std::optional<ValueType> lastValueFromPreviousBlock = std::nullopt;

  for (auto& block : view) {
    if (block.empty()) {
      continue;
    }
    numInputs += block.size();
    auto beg = lastValueFromPreviousBlock
                   ? ql::ranges::find_if(
                         block, [&p = lastValueFromPreviousBlock.value()](
                                    const auto& el) { return el != p; })
                   : block.begin();
    lastValueFromPreviousBlock = *(block.end() - 1);
    auto it = std::unique(beg, block.end());
    block.erase(it, block.end());
    block.erase(block.begin(), beg);
    numUnique += block.size();
    co_yield block;
  }
  LOG(INFO) << "Number of inputs to `uniqueView`: " << numInputs << '\n';
  LOG(INFO) << "Number of unique elements: " << numUnique << std::endl;
}

// A view that owns its underlying storage. It is a replacement for
// `ranges::owning_view` which is not yet supported by `GCC 11` and
// `range-v3`. The implementation is taken from libstdc++-13. The additional
// optional `supportsConst` argument explicitly disables const iteration for
// this view when set to false, see `OwningViewNoConst` below for details.
CPP_template(typename UnderlyingRange, bool supportConst = true)(
    requires ql::ranges::range<UnderlyingRange> CPP_and
        ql::concepts::movable<UnderlyingRange>) class OwningView
    : public ql::ranges::view_interface<OwningView<UnderlyingRange>> {
 private:
  UnderlyingRange underlyingRange_;

 public:
  OwningView() = default;

  constexpr OwningView(UnderlyingRange&& underlyingRange) noexcept(
      std::is_nothrow_move_constructible_v<UnderlyingRange>)
      : underlyingRange_(std::move(underlyingRange)) {}

  OwningView(OwningView&&) = default;
  OwningView& operator=(OwningView&&) = default;

  constexpr UnderlyingRange& base() & noexcept { return underlyingRange_; }

  constexpr const UnderlyingRange& base() const& noexcept {
    return underlyingRange_;
  }

  constexpr UnderlyingRange&& base() && noexcept {
    return std::move(underlyingRange_);
  }

  constexpr const UnderlyingRange&& base() const&& noexcept {
    return std::move(underlyingRange_);
  }

  constexpr auto begin() { return ql::ranges::begin(underlyingRange_); }

  constexpr auto end() { return ql::ranges::end(underlyingRange_); }

  CPP_auto_member constexpr auto CPP_fun(begin)()(
      const  //
      requires(supportConst&& ql::ranges::range<const UnderlyingRange>)) {
    return ql::ranges::begin(underlyingRange_);
  }

  CPP_auto_member constexpr auto CPP_fun(end)()(
      const  //
      requires(supportConst&& ql::ranges::range<const UnderlyingRange>)) {
    return ql::ranges::end(underlyingRange_);
  }

  CPP_member constexpr auto empty() const
      -> CPP_ret(bool)(requires detail::RangeCanEmpty<const UnderlyingRange>) {
    return ql::ranges::empty(underlyingRange_);
  }

  CPP_member constexpr auto size()
      -> CPP_ret(size_t)(requires ql::ranges::sized_range<UnderlyingRange>) {
    return ql::ranges::size(underlyingRange_);
  }

  CPP_member constexpr auto size() const -> CPP_ret(size_t)(
      requires ql::ranges::sized_range<const UnderlyingRange>) {
    return ql::ranges::size(underlyingRange_);
  }

  CPP_auto_member constexpr auto CPP_fun(data)()(
      requires ql::ranges::contiguous_range<UnderlyingRange>) {
    return ql::ranges::data(underlyingRange_);
  }

  CPP_auto_member constexpr auto CPP_fun(data)()(
      const  //
      requires ql::ranges::contiguous_range<const UnderlyingRange>) {
    return ql::ranges::data(underlyingRange_);
  }
};

// Like `OwningView` above, but the const overloads to `begin()` and `end()` do
// not exist. This is currently used in the `CompressedExternalIdTable.h`, where
// have a deeply nested stack of views, one of which is `OnwingView<vector>`
// which doesn't properly propagate the possibility of const iteration in
// range-v3`.
template <typename T>
struct OwningViewNoConst : OwningView<T, false> {
  using OwningView<T, false>::OwningView;
};

template <typename T>
OwningViewNoConst(T&&) -> OwningViewNoConst<T>;

// Helper concept for `ad_utility::allView`.
namespace detail {
template <typename Range>
CPP_requires(can_ref_view,
             requires(Range&& range)(ql::ranges::ref_view{AD_FWD(range)}));
template <typename Range>
CPP_concept can_ref_view = CPP_requires_ref(can_ref_view, Range);
}  // namespace detail

// A simple drop-in replacement for `ql::views::all` which is required because
// GCC 11 and range-v3 currently don't support `std::owning_view` (see above).
// As soon as we don't support GCC 11 anymore, we can throw out those
// implementations.
template <typename Range>
constexpr auto allView(Range&& range) {
  if constexpr (ql::ranges::view<std::decay_t<Range>>) {
    return AD_FWD(range);
  } else if constexpr (detail::can_ref_view<Range>) {
    return ql::ranges::ref_view{AD_FWD(range)};
  } else {
    // return std::ranges::owning_view{AD_FWD(range)};
    return ad_utility::OwningView<std::remove_reference_t<Range>>{
        AD_FWD(range)};
  }
}
template <typename Range>
using all_t = decltype(allView(std::declval<Range>()));

// This view is an input view that wraps another `view` transparently. When the
// view is destroyed, or the iteration reaches `end`, whichever happens first,
// the given `callback` is invoked.
CPP_template(typename V, typename F)(
    requires ql::ranges::input_range<V> CPP_and
        ql::ranges::view<V>&& std::invocable<F&>) class CallbackOnEndView
    : public ql::ranges::view_interface<CallbackOnEndView<V, F>> {
 private:
  V base_;
  F callback_;
  // Don't invoke the callback if the view was moved from.
  ad_utility::ResetWhenMoved<bool, true> called_ = false;

  // Invoke the `callback` iff it hasn't been invoked yet.
  void maybeInvoke() {
    if (!std::exchange(called_, true)) {
      callback_();
    }
  }

  class Iterator {
   private:
    ql::ranges::iterator_t<V> current_;
    CallbackOnEndView* parent_ = nullptr;

   public:
    using value_type = ql::ranges::range_value_t<V>;
    using reference_type = ql::ranges::range_reference_t<V>;
    using difference_type = ql::ranges::range_difference_t<V>;

    Iterator() = default;
    Iterator(ql::ranges::iterator_t<V> current, CallbackOnEndView* parent)
        : current_(current), parent_(parent) {}

    decltype(auto) operator*() const { return *current_; }

    Iterator& operator++() {
      ++current_;
      if (current_ == ql::ranges::end(parent_->base_)) {
        parent_->maybeInvoke();
      }
      return *this;
    }

    void operator++(int) { ++(*this); }

    bool operator==(ql::ranges::sentinel_t<V> s) const { return current_ == s; }
  };

 public:
  CallbackOnEndView() = default;
  CallbackOnEndView(V base, F callback)
      : base_(std::move(base)), callback_(std::move(callback)) {}

  CallbackOnEndView(const CallbackOnEndView&) = delete;
  CallbackOnEndView& operator=(const CallbackOnEndView&) = delete;

  CallbackOnEndView(CallbackOnEndView&&) = default;
  CallbackOnEndView& operator=(CallbackOnEndView&&) = default;

  ~CallbackOnEndView() { maybeInvoke(); }

  auto begin() { return Iterator{ql::ranges::begin(base_), this}; }

  auto end() { return ql::ranges::end(base_); }
};

// Deduction guide
template <class R, class F>
CallbackOnEndView(R&&, F) -> CallbackOnEndView<all_t<R>, F>;

namespace detail {
// The implementation of `bufferedAsyncView` (see below). It yields its result
// in blocks.
template <typename View>
struct BufferedAsyncView : InputRangeMixin<BufferedAsyncView<View>> {
  View view_;
  uint64_t blockSize_;
  mutable bool finished_ = false;

  explicit BufferedAsyncView(View&& view, uint64_t blockSize)
      : view_{std::move(view)}, blockSize_{blockSize} {
    AD_CONTRACT_CHECK(blockSize_ > 0);
  }

  mutable ql::ranges::iterator_t<View> it_;
  ql::ranges::sentinel_t<View> end_ = ql::ranges::end(view_);
  using value_type = ql::ranges::range_value_t<View>;
  mutable std::future<std::vector<value_type>> future_;

  mutable std::vector<value_type> buffer_;
  std::vector<value_type> getNextBlock() const {
    std::vector<value_type> buffer;
    buffer.reserve(blockSize_);
    size_t i = 0;
    while (i < blockSize_ && it_ != end_) {
      buffer.push_back(*it_);
      ++it_;
      ++i;
    }
    return buffer;
  };

  void start() {
    it_ = view_.begin();
    buffer_ = getNextBlock();
    finished_ = buffer_.empty();
    future_ =
        std::async(std::launch::async, [this]() { return getNextBlock(); });
  }
  bool isFinished() { return finished_; }
  auto& get() { return buffer_; }
  const auto& get() const { return buffer_; }

  void next() {
    buffer_ = future_.get();
    finished_ = buffer_.empty();
    future_ =
        std::async(std::launch::async, [this]() { return getNextBlock(); });
  }
};
}  // namespace detail

/// Takes a input-iterable and yields the elements of that view (no visible
/// effect). The iteration over the input view is done on a separate thread with
/// a buffer size of `blockSize`. This might speed up the computation when the
/// values of the input view are expensive to compute.
///
template <typename View>
auto bufferedAsyncView(View view, uint64_t blockSize) {
  return ql::views::join(
      allView(detail::BufferedAsyncView<View>{std::move(view), blockSize}));
}

// Returns a view that contains all the values in `[0, upperBound)`, similar to
// Python's `range` function. Avoids the common pitfall in `ql::views::iota`
// that the count variable is only derived from the first argument. For example,
// `ql::views::iota(0, size_t(INT_MAX) + 1)` leads to undefined behavior
// because of an integer overflow, but `ad_utility::integerRange(size_t(INT_MAX)
// + 1)` is perfectly safe and behaves as expected.
CPP_template(typename Int)(
    requires std::unsigned_integral<Int>) auto integerRange(Int upperBound) {
  return ql::views::iota(Int{0}, upperBound);
}

// The implementation of `inPlaceTransformView`, see below for details.
namespace detail {
CPP_template(typename Range, typename Transformation)(
    requires ad_utility::InvocableWithExactReturnType<
        Transformation, void, ql::ranges::range_reference_t<Range>>
        CPP_and ql::ranges::view<Range>
            CPP_and ql::ranges::input_range<
                Range>) auto inPlaceTransformViewImpl(Range range,
                                                      Transformation
                                                          transformation) {
  // Take a range and yield pairs of [pointerToElementOfRange,
  // boolThatIsInitiallyFalse]. The bool is yielded as a reference and if its
  // value is changed, that change will be stored until the next element is
  // yielded. This is made use of further below.
  // Note that instead of taking the element by pointer/reference we could also
  // copy or move it. This implementation never takes a copy, but modifies the
  // input.
  auto makeElementPtrAndBool = [](auto range)
      -> cppcoro::generator<
          std::pair<decltype(std::addressof(*range.begin())), bool>> {
    for (auto& el : range) {
      auto pair = std::pair{std::addressof(el), false};
      co_yield pair;
    }
  };

  // Lift the transformation to work on the result of `makePtrAndBool` and to
  // only apply the transformation once for each element.
  // Note: This works because `ql::views::transform` calls the transformation
  // each time an iterator is dereferenced, so the following lambda is called
  // multiple times for the same element if the same iterator is dereferenced
  // multiple times and we therefore have to remember whether the transformation
  // was already applied, because it changes the element in place. See the unit
  // tests in `ViewsTest.cpp` for examples.
  auto actualTransformation = [transformation = std::move(transformation)](
                                  auto& ptrAndBool) -> decltype(auto) {
    auto& [ptr, alreadyTransformed] = ptrAndBool;
    if (!alreadyTransformed) {
      alreadyTransformed = true;
      std::invoke(transformation, *ptr);
    }
    return *ptr;
  };

  // Combine everything to the actual result range.
  return ql::views::transform(
      ad_utility::OwningView{makeElementPtrAndBool(std::move(range))},
      actualTransformation);
}
}  // namespace detail

// Similar to `ql::views::transform` but for transformation functions that
// transform a value in place. The result is always only an input range,
// independent of the actual range category of the input.
CPP_template(typename Range, typename Transformation)(
    requires ql::ranges::input_range<Range> CPP_and
        ad_utility::InvocableWithExactReturnType<
            Transformation, void,
            ql::ranges::range_reference_t<
                Range>>) auto inPlaceTransformView(Range&& range,
                                                   Transformation
                                                       transformation) {
  return detail::inPlaceTransformViewImpl(ql::views::all(AD_FWD(range)),
                                          std::move(transformation));
}

/// Create a generator the consumes the input generator until it finds the given
/// separator and the yields spans of the chunks of data received inbetween.
CPP_template(typename Range, typename ElementType)(
    requires ql::ranges::input_range<Range>) inline cppcoro::
    generator<ql::span<ElementType>> reChunkAtSeparator(Range generator,
                                                        ElementType separator) {
  std::vector<ElementType> buffer;
  for (QL_CONCEPT_OR_NOTHING(ql::ranges::input_range) auto const& chunk :
       generator) {
    for (ElementType c : chunk) {
      if (c == separator) {
        co_yield ql::span{buffer.data(), buffer.size()};
        buffer.clear();
      } else {
        buffer.push_back(c);
      }
    }
  }
  if (!buffer.empty()) {
    co_yield ql::span{buffer.data(), buffer.size()};
  }
}
}  // namespace ad_utility

// Enabling of "borrowed" ranges for `OwningView`.
#ifdef QLEVER_CPP_17
template <typename T>
inline constexpr bool ::ranges::enable_borrowed_range<
    ad_utility::OwningView<T>> = enable_borrowed_range<T>;

#else
template <typename T>
inline constexpr bool
    std::ranges::enable_borrowed_range<ad_utility::OwningView<T>> =
        std::ranges::enable_borrowed_range<T>;
#endif

#endif  // QLEVER_SRC_UTIL_VIEWS_H
