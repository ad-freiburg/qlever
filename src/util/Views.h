//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_VIEWS_H
#define QLEVER_SRC_UTIL_VIEWS_H

#include <future>
#include <iterator>
#include <memory>
#include <optional>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/span.h"
#include "range/v3/range/access.hpp"
#include "range/v3/range/conversion.hpp"
#include "range/v3/range/traits.hpp"
#include "range/v3/view/subrange.hpp"
#include "util/ExceptionHandling.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Log.h"
#include "util/ResetWhenMoved.h"

namespace {}  // namespace

namespace ad_utility {

namespace detail {
template <typename R>
CPP_requires(can_empty_, requires(const R& range)(ql::ranges::empty(range)));

template <typename R>
CPP_concept RangeCanEmpty = CPP_requires_ref(can_empty_, R);
}  // namespace detail

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

  CPP_auto_member constexpr auto CPP_fun(begin) ()(
      const  //
      requires(supportConst&& ql::ranges::range<const UnderlyingRange>)) {
    return ql::ranges::begin(underlyingRange_);
  }

  CPP_auto_member constexpr auto CPP_fun(end) ()(
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

  CPP_auto_member constexpr auto
      CPP_fun(data) ()(requires ql::ranges::contiguous_range<UnderlyingRange>) {
    return ql::ranges::data(underlyingRange_);
  }

  CPP_auto_member constexpr auto CPP_fun(data) ()(
      const  //
      requires ql::ranges::contiguous_range<const UnderlyingRange>) {
    return ql::ranges::data(underlyingRange_);
  }
};

// Takes a view of blocks and yields the elements of the same view, but removes
// consecutive duplicates inside the blocks and across block boundaries.
template <typename SortedBlockView,
          typename BlockType = ql::ranges::range_value_t<SortedBlockView>,
          typename ValueType = ql::ranges::range_value_t<BlockType>>

auto uniqueBlockView(SortedBlockView view) {
  struct uniqueBlockViewFromGet : ad_utility::InputRangeFromGet<BlockType> {
    SortedBlockView view_;

    decltype(ql::views::filter(view_,
                               std::not_fn(ql::ranges::empty))) nonEmptyView_;
    decltype(ql::ranges::begin(nonEmptyView_)) iter_;

    std::optional<ValueType> lastValueFromPreviousBlock_{std::nullopt};
    size_t numInputs_{0};
    size_t numUnique_{0};

    uniqueBlockViewFromGet(SortedBlockView view)
        : view_{std::move(view)},
          nonEmptyView_(
              ql::views::filter(view_, std::not_fn(ql::ranges::empty))),
          iter_{ql::ranges::begin(nonEmptyView_)} {}

    std::optional<BlockType> get() override {
      if (iter_ == ql::ranges::end(nonEmptyView_)) {
        LOG(INFO) << "Number of inputs to `uniqueView`: " << numInputs_ << '\n';
        LOG(INFO) << "Number of unique elements: " << numUnique_ << std::endl;
        return std::nullopt;
      }

      auto block = std::move(*iter_);
      ql::ranges::advance(iter_, 1);
      numInputs_ += block.size();
      auto beg = lastValueFromPreviousBlock_
                     ? ql::ranges::find_if(
                           block, [&p = lastValueFromPreviousBlock_.value()](
                                      const auto& el) { return el != p; })
                     : block.begin();
      lastValueFromPreviousBlock_ = block.back();
      auto it = std::unique(beg, block.end());
      block.erase(it, block.end());
      block.erase(block.begin(), beg);
      numUnique_ += block.size();
      return block;
    }
  };
  auto generator = std::make_unique<uniqueBlockViewFromGet>(std::move(view));
  return ad_utility::InputRangeTypeErased{std::move(generator)};
}

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

/// Create a generator the consumes the input generator until it finds the given
/// separator and the yields spans of the chunks of data received inbetween.
CPP_template(typename Range, typename ElementType)(
    requires ql::ranges::input_range<
        Range>) inline auto reChunkAtSeparator(Range generator,
                                               ElementType separator) {
  // Flattens double container and splits it at separator
  struct ReChunkAtSeparatorFromGet
      : ad_utility::InputRangeFromGet<ql::span<ElementType>> {
    Range generator_;
    ElementType separator_;
    std::vector<ElementType> buffer_;
    decltype(ranges::views::split(
        ql::views::join(generator_) | ranges::views::common,
        separator_)) splitView_;
    decltype(splitView_.begin()) splitIter_;

    ReChunkAtSeparatorFromGet(Range generator, ElementType separator)
        : generator_{generator},
          separator_{separator},
          splitView_{ranges::views::split(
              ql::views::join(generator_) | ranges::views::common, separator)},
          splitIter_{ql::ranges::begin(splitView_)} {}

    std::optional<ql::span<ElementType>> get() override {
      if (splitIter_ != ql::ranges::end(splitView_)) {
        buffer_ = ql::ranges::to<std::vector<ElementType>>(*splitIter_);
        ++splitIter_;
        return ql::span(buffer_.begin(), buffer_.size());
      }

      return std::nullopt;
    }
  };

  return ReChunkAtSeparatorFromGet{generator, separator};
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
