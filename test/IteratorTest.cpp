//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>

#include <algorithm>
#include <string>

#include "backports/algorithm.h"
#include "util/CompilerWarnings.h"
#include "util/Exception.h"
#include "util/Iterators.h"

auto testIterator = [](const auto& input, auto begin, auto end) {
  auto it = begin;
  ASSERT_EQ(input[0], *it);
  ASSERT_EQ(input[0], *it++);
  ASSERT_EQ(input[1], *it);
  ASSERT_EQ(input[2], *++it);
  ASSERT_EQ(input[2], *it);
  ASSERT_EQ(input[2], *it--);
  ASSERT_EQ(input[1], *it);
  ASSERT_EQ(input[0], *--it);

  ASSERT_EQ(input[2], it[2]);
  ASSERT_EQ(input[2], *(it + 2));
  ASSERT_EQ(input[2], *(2 + it));
  ASSERT_EQ(input[3], *(it += 3));
  ASSERT_EQ(input[2], *(it += -1));
  ASSERT_EQ(input[2], *(it));
  ASSERT_EQ(input[0], *(it -= 2));
  ASSERT_EQ(input[2], *(it -= -2));
  ASSERT_EQ(input[2], *(it));

  it = end + (-1);
  ASSERT_EQ(input.back(), *it);

  ASSERT_EQ(static_cast<int64_t>(input.size()), end - begin);
};

TEST(RandomAccessIterator, Vector) {
  std::vector<int> f{3, 62, 1023, -43817, 14, 42};
  using Iterator = ad_utility::IteratorForAccessOperator<
      std::vector<int>, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::False>;

  using ConstIterator = ad_utility::IteratorForAccessOperator<
      std::vector<int>, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::True>;

  Iterator begin = Iterator{&f, 0};
  Iterator end = Iterator{&f, f.size()};
  testIterator(f, begin, end);

  ConstIterator cbegin{&f, 0};
  ConstIterator cend{&f, f.size()};
  testIterator(f, cbegin, cend);
}

TEST(RandomAccessIterator, DummyRandomAccessContainer) {
  struct TestRandomAccessContainer {
    uint64_t get(uint64_t index) const { return 42 * index; }

    // operator[] is required for the test lambda.
    uint64_t operator[](uint64_t index) const { return get(index); }

    [[nodiscard]] auto front() const { return get(0); }
    [[nodiscard]] auto back() const { return get(42); }
    [[nodiscard]] auto size() const { return 43; }
  };

  auto getFromTestContainer = [](const TestRandomAccessContainer& container,
                                 uint64_t i) { return container.get(i); };

  using Iterator =
      ad_utility::IteratorForAccessOperator<TestRandomAccessContainer,
                                            decltype(getFromTestContainer)>;
  TestRandomAccessContainer d;
  Iterator begin = Iterator{&d, 0};
  Iterator end = Iterator{&d, 43};
  testIterator(d, begin, end);
}

struct ForwardFirstElement {
  template <typename T>
  auto operator()(T&& vector) const {
    return *ad_utility::makeForwardingIterator<T>(vector.begin());
  }
};

TEST(Iterator, makeForwardingIterator) {
  ForwardFirstElement forwardFirstElement;

  std::vector<std::string> vector{"hello"};
  auto vector2 = vector;
  ASSERT_EQ("hello", forwardFirstElement(vector));
  // Nothing was moved.
  ASSERT_EQ(vector, vector2);

  ASSERT_EQ("hello", forwardFirstElement(std::move(vector)));
  // The first element in the vector was now moved from.
  ASSERT_EQ(1u, vector.size());
  ASSERT_TRUE(vector[0].empty());
}

namespace {
template <typename MakeIotaRange>
// This function tests a view that behaves like `ql::views::iota`.
// The argument `makeIotaRange` is given a lower bound (size_t, `0` if not
// specified) and an upper bound (`optional<size_t>`, unlimited (nullopt) if not
// specified) and must return a `ql::ranges::input_range` that yields the
// elements in the range `(lower, upper]`.
void testIota(MakeIotaRange makeIotaRange) {
  size_t sum = 0;
  // Test manual iteration.
  for (auto s : makeIotaRange(0, 5)) {
    sum += s;
  }
  EXPECT_EQ(sum, 10);

  // Check that the range is an input range, but fulfills none of the stricter
  // categories.
  auto iota = makeIotaRange();
  using Iota = decltype(iota);
  static_assert(ql::ranges::input_range<Iota>);
  static_assert(!ql::ranges::forward_range<Iota>);

  // Test the interaction with the `ql::views` and `ql::ranges` machinery.
  auto view = iota | ql::views::drop(3) | ql::views::take(7);
  static_assert(ql::ranges::input_range<decltype(view)>);
  sum = 0;
  auto add = [&sum](auto val) { sum += val; };
  ql::ranges::for_each(view, add);

  // 42 == 3 + 4 + ... + 9
  EXPECT_EQ(sum, 42);
}
}  // namespace

// _____________________________________________________________________________
TEST(Iterator, InputRangeMixin) {
  using namespace ad_utility;
  struct Iota : InputRangeMixin<Iota> {
    size_t value_ = 0;
    std::optional<size_t> upper_;
    explicit Iota(size_t lower = 0, std::optional<size_t> upper = {})
        : value_{lower}, upper_{upper} {}
    void start() {}
    bool isFinished() const { return value_ == upper_; }
    size_t get() const { return value_; }
    void next() { ++value_; }
  };

  auto makeIota = [](size_t lower = 0, std::optional<size_t> upper = {}) {
    return Iota{lower, upper};
  };
  testIota(makeIota);
}

//_____________________________________________________________________________
TEST(Iterator, InputRangeFromGet) {
  using namespace ad_utility;
  struct Iota : InputRangeFromGet<size_t> {
    size_t value_ = 0;
    std::optional<size_t> upper_;
    explicit Iota(size_t lower = 0, std::optional<size_t> upper = {})
        : value_{lower}, upper_{upper} {}
    std::optional<size_t> get() override {
      if (value_ == upper_) {
        return std::nullopt;
      }
      return value_++;
    }
  };
  auto makeIota = [](size_t lower = 0, std::optional<size_t> upper = {}) {
    return Iota{lower, upper};
  };
  testIota(makeIota);
}

//_____________________________________________________________________________
TEST(Iterator, InputRangeFromGetCallable) {
  using namespace ad_utility;
  auto makeLambda = [](size_t lower = 0, std::optional<size_t> upper = {}) {
    return [value = lower, upper]() mutable -> std::optional<size_t> {
      if (value == upper) {
        return std::nullopt;
      }
      return value++;
    };
  };
  auto makeIota = [&makeLambda](size_t lower = 0,
                                std::optional<size_t> upper = {}) {
    return InputRangeFromGetCallable{makeLambda(lower, upper)};
    /*
    return InputRangeFromGetCallable<size_t, decltype(makeLambda(0, 3))>{
        makeLambda(lower, upper)};
        */
  };
  testIota(makeIota);
}

//_____________________________________________________________________________
TEST(Iterator, InputRangeTypeErased) {
  using namespace ad_utility;
  struct IotaImpl : InputRangeFromGet<size_t> {
    size_t value_ = 0;
    std::optional<size_t> upper_;
    explicit IotaImpl(size_t lower = 0, std::optional<size_t> upper = {})
        : value_{lower}, upper_{upper} {}
    std::optional<size_t> get() override {
      if (value_ == upper_) {
        return std::nullopt;
      }
      return value_++;
    }
  };

  using Iota = InputRangeTypeErased<size_t>;
  auto makeIota = [](size_t lower = 0, std::optional<size_t> upper = {}) {
    return Iota{IotaImpl{lower, upper}};
  };
  testIota(makeIota);

  // We can also type-erase any input range with the correct value type, in
  // particular ranges and views from the standard library.
  auto makeIotaFromStdIota = [](size_t lower = 0,
                                std::optional<size_t> upper = {}) {
    if (!upper.has_value()) {
      return Iota{ql::views::iota(lower)};
    } else {
      return Iota{ql::views::iota(lower, upper.value())};
    }
  };
  testIota(makeIotaFromStdIota);
}

TEST(Iterator, IteratorRange) {
  using ad_utility::IteratorRange;
  std::vector v{1, 3, 5, 7};
  auto testAndReturn = [&]() {
    IteratorRange r{v.begin(), v.end()};
    EXPECT_EQ(*r.begin(), v.begin());
    EXPECT_EQ(*(r.begin() + 3), v.begin() + 3);

    using R = std::decay_t<decltype(r)>;
    using It = ql::ranges::iterator_t<std::vector<int>>;
    // auto e = ql::ranges::end(r);
    using Value = ql::ranges::range_value_t<R>;
    using Ref = ql::ranges::range_reference_t<R>;
    static_assert(std::is_same_v<It, Value>);
    static_assert(std::is_same_v<It, Ref>);
    return std::pair{r.begin(), r.end()};
  };
  auto [beg, end] = testAndReturn();
  EXPECT_EQ(*beg, v.begin());
  EXPECT_EQ(**beg, 1);
  EXPECT_EQ(beg[3], v.begin() + 3);
  EXPECT_EQ(*beg[3], 7);
}

// _____________________________________________________________________________
TEST(Iterator, IteratorForAssigmentOperator) {
  std::vector<int> resultVector;

  ad_utility::IteratorForAssigmentOperator iterator{
      [&resultVector](int value) { resultVector.push_back(value); }};

  *iterator = 3;
  EXPECT_THAT(resultVector, ::testing::ElementsAre(3));
  *iterator = 62;
  EXPECT_THAT(resultVector, ::testing::ElementsAre(3, 62));

  // These should do nothing
  ++iterator;
  (void)iterator++;

  *iterator = 1023;
  EXPECT_THAT(resultVector, ::testing::ElementsAre(3, 62, 1023));

  // Typical usage pattern:
  std::vector<int> otherValues{1337, 42};
  ql::ranges::copy(otherValues, iterator);
  EXPECT_THAT(resultVector, ::testing::ElementsAre(3, 62, 1023, 1337, 42));
}
