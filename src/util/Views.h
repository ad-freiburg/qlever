//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <future>
#include <ranges>
#include <span>

#include "util/Generator.h"
#include "util/Log.h"

namespace ad_utility {

/// Takes a input-iterable and yields the elements of that view (no visible
/// effect). The iteration over the input view is done on a separate thread with
/// a buffer size of `blockSize`. This might speed up the computation when the
/// values of the input view are expensive to compute.
template <typename View>
cppcoro::generator<typename View::value_type> bufferedAsyncView(
    View view, uint64_t blockSize) {
  using value_type = typename View::value_type;
  auto it = view.begin();
  auto end = view.end();
  auto getNextBlock = [&it, &end, blockSize] {
    std::vector<value_type> buffer;
    buffer.reserve(blockSize);
    size_t i = 0;
    while (i < blockSize && it != end) {
      buffer.push_back(*it);
      ++it;
      ++i;
    }
    return buffer;
  };

  auto block = getNextBlock();
  auto future = std::async(std::launch::async, getNextBlock);
  while (true) {
    for (auto& element : block) {
      co_yield element;
    }
    block = future.get();
    if (block.empty()) {
      co_return;
    }
    future = std::async(std::launch::async, getNextBlock);
  }
}

/// Takes a view and yields the elements of the same view, but skips over
/// consecutive duplicates.
template <typename SortedView, typename ValueType = SortedView::value_type>
cppcoro::generator<ValueType> uniqueView(SortedView view) {
  size_t numInputs = 0;
  size_t numUnique = 0;
  auto it = view.begin();
  if (it == view.end()) {
    co_return;
  }
  ValueType previousValue = std::move(*it);
  ValueType previousValueCopy = previousValue;
  co_yield previousValueCopy;
  numInputs = 1;
  numUnique = 1;
  ++it;

  for (; it != view.end(); ++it) {
    ++numInputs;
    if (*it != previousValue) {
      previousValue = std::move(*it);
      previousValueCopy = previousValue;
      ++numUnique;
      co_yield previousValueCopy;
    }
  }
  LOG(DEBUG) << "Number of inputs to `uniqueView`: " << numInputs << '\n';
  LOG(DEBUG) << "Number of unique outputs of `uniqueView`: " << numUnique
             << std::endl;
}

// Takes a view of blocks and yields the elements of the same view, but removes
// consecutive duplicates inside the blocks and across block boundaries.
template <typename SortedBlockView,
          typename ValueType = std::ranges::range_value_t<
              std::ranges::range_value_t<SortedBlockView>>>
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
                   ? std::ranges::find_if(
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
// `std::ranges::owning_view` which is not yet supported by `GCC 11`. The
// implementation is taken from libstdc++-13.
template <std::ranges::range UnderlyingRange>
requires std::movable<UnderlyingRange> class OwningView
    : public std::ranges::view_interface<OwningView<UnderlyingRange>> {
 private:
  UnderlyingRange underlyingRange_ = UnderlyingRange();

 public:
  OwningView() requires std::default_initializable<UnderlyingRange> = default;

  constexpr explicit OwningView(UnderlyingRange&& underlyingRange) noexcept(
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

  constexpr std::ranges::iterator_t<UnderlyingRange> begin() {
    return std::ranges::begin(underlyingRange_);
  }

  constexpr std::ranges::sentinel_t<UnderlyingRange> end() {
    return std::ranges::end(underlyingRange_);
  }

  constexpr auto begin() const
      requires std::ranges::range<const UnderlyingRange> {
    return std::ranges::begin(underlyingRange_);
  }

  constexpr auto end() const requires std::ranges::range<const UnderlyingRange>
  {
    return std::ranges::end(underlyingRange_);
  }

  constexpr bool empty()
      requires requires { std::ranges::empty(underlyingRange_); } {
    return std::ranges::empty(underlyingRange_);
  }

  constexpr bool empty() const
      requires requires { std::ranges::empty(underlyingRange_); } {
    return std::ranges::empty(underlyingRange_);
  }

  constexpr auto size() requires std::ranges::sized_range<UnderlyingRange> {
    return std::ranges::size(underlyingRange_);
  }

  constexpr auto size() const
      requires std::ranges::sized_range<const UnderlyingRange> {
    return std::ranges::size(underlyingRange_);
  }

  constexpr auto data() requires std::ranges::contiguous_range<UnderlyingRange>
  {
    return std::ranges::data(underlyingRange_);
  }

  constexpr auto data() const
      requires std::ranges::contiguous_range<const UnderlyingRange> {
    return std::ranges::data(underlyingRange_);
  }
};

// Helper concept for `ad_utility::allView`.
namespace detail {
template <typename Range>
concept can_ref_view =
    requires(Range&& range) { std::ranges::ref_view{AD_FWD(range)}; };
}

// A simple drop-in replacement for `std::views::all` which is required because
// GCC 11 doesn't support `std::owning_view` (see above). As soon as we don't
// support GCC 11 anymore, we can throw out those implementations.
template <typename Range>
constexpr auto allView(Range&& range) {
  if constexpr (std::ranges::view<std::decay_t<Range>>) {
    return AD_FWD(range);
  } else if constexpr (detail::can_ref_view<Range>) {
    return std::ranges::ref_view{AD_FWD(range)};
  } else {
    return ad_utility::OwningView{AD_FWD(range)};
  }
}

// Returns a view that contains all the values in `[0, upperBound)`, similar to
// Python's `range` function. Avoids the common pitfall in `std::views::iota`
// that the count variable is only derived from the first argument. For example,
// `std::views::iota(0, size_t(INT_MAX) + 1)` leads to undefined behavior
// because of an integer overflow, but `ad_utility::integerRange(size_t(INT_MAX)
// + 1)` is perfectly safe and behaves as expected.
template <std::unsigned_integral Int>
auto integerRange(Int upperBound) {
  return std::views::iota(Int{0}, upperBound);
}

// The implementation of `inPlaceTransformView`, see below for details.
namespace detail {
template <std::ranges::input_range Range,
          ad_utility::InvocableWithExactReturnType<
              void, std::ranges::range_reference_t<Range>>
              Transformation>
requires std::ranges::view<Range>
auto inPlaceTransformViewImpl(Range range, Transformation transformation) {
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
  // Note: This works because `std::views::transform` calls the transformation
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
  return std::views::transform(
      ad_utility::OwningView{makeElementPtrAndBool(std::move(range))},
      actualTransformation);
}
}  // namespace detail

// Similar to `std::views::transform` but for transformation functions that
// transform a value in place. The result is always only an input range,
// independent of the actual range category of the input.
template <std::ranges::input_range Range,
          ad_utility::InvocableWithExactReturnType<
              void, std::ranges::range_reference_t<Range>>
              Transformation>
auto inPlaceTransformView(Range&& range, Transformation transformation) {
  return detail::inPlaceTransformViewImpl(std::views::all(AD_FWD(range)),
                                          std::move(transformation));
}

/// Create a generator the consumes the input generator until it finds the given
/// separator and the yields spans of the chunks of data received inbetween.
template <std::ranges::input_range Range, typename ElementType>
inline cppcoro::generator<std::span<ElementType>> reChunkAtSeparator(
    Range generator, ElementType separator) {
  std::vector<ElementType> buffer;
  for (std::ranges::input_range auto chunk : generator) {
    for (ElementType c : chunk) {
      if (c == separator) {
        co_yield std::span{buffer.data(), buffer.size()};
        buffer.clear();
      } else {
        buffer.push_back(c);
      }
    }
  }
  if (!buffer.empty()) {
    co_yield std::span{buffer.data(), buffer.size()};
  }
}

}  // namespace ad_utility

// Enabling of "borrowed" ranges for `OwningView`.
template <typename T>
inline constexpr bool
    std::ranges::enable_borrowed_range<ad_utility::OwningView<T>> =
        std::ranges::enable_borrowed_range<T>;
