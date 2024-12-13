//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>
#include <c++/11/numeric>
#include <ranges>
#include <string>

#include "../src/util/Iterators.h"

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

TEST(Iterator, makeForwardingIterator) {
  auto forwardFirstElement = []<typename T>(T&& vector) {
    return *ad_utility::makeForwardingIterator<T>(vector.begin());
  };

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

// _____________________________________________________________________________
TEST(Iterator, InputRangeMixin) {
  using namespace ad_utility;
  struct Iota : InputRangeMixin<Iota> {
    size_t value_ = 0;
    std::optional<size_t> upper_;
    Iota(size_t value = 0, std::optional<size_t> upper = {})
        : value_{value}, upper_{upper} {}
    void start() {}
    bool isFinished() { return value_ == upper_; }
    size_t get() const { return value_; }
    void next() { ++value_; }
  };
  static_assert(std::ranges::input_range<Iota>);
  static_assert(!std::ranges::forward_range<Iota>);
  size_t sum = 0;
  for (auto s : Iota{0, 5}) {
    sum += s;
  }
  EXPECT_EQ(sum, 10);

  Iota iota;
  auto view = iota | std::views::drop(3) | std::views::take(7);
  sum = 0;
  auto add = [&sum](auto val) { sum += val; };
  std::ranges::for_each(view, add);

  // 42 == 3 + 4 + ... + 9
  EXPECT_EQ(sum, 42);
}

// _____________________________________________________________________________
TEST(Iterator, InputRangeOptionalMixin) {
  using namespace ad_utility;
  struct Iota : InputRangeOptionalMixin<size_t> {
    size_t value_ = 0;
    std::optional<size_t> upper_;
    explicit Iota(size_t value = 0, std::optional<size_t> upper = {})
        : value_{value}, upper_{upper} {}
    std::optional<size_t> get() override {
      if (value_ == upper_) {
        return std::nullopt;
      }
      return value_++;
    }
  };
  static_assert(std::ranges::input_range<Iota>);
  static_assert(!std::ranges::forward_range<Iota>);
  size_t sum = 0;
  for (auto s : Iota{0, 5}) {
    sum += s;
  }
  EXPECT_EQ(sum, 10);

  Iota iota;
  auto view = iota | std::views::drop(3) | std::views::take(7);
  sum = 0;
  auto add = [&sum](auto val) { sum += val; };
  std::ranges::for_each(view, add);

  // 42 == 3 + 4 + ... + 9
  EXPECT_EQ(sum, 42);
}

// _____________________________________________________________________________
TEST(Iterator, TypeErasedInputRangeOptionalMixin) {
  using namespace ad_utility;
  struct IotaImpl : InputRangeOptionalMixin<size_t> {
    size_t value_ = 0;
    std::optional<size_t> upper_;
    explicit IotaImpl(size_t value = 0, std::optional<size_t> upper = {})
        : value_{value}, upper_{upper} {}
    std::optional<size_t> get() override {
      if (value_ == upper_) {
        return std::nullopt;
      }
      return value_++;
    }
  };

  using Iota = TypeEraseInputRangeOptionalMixin<size_t>;
  static_assert(std::ranges::input_range<Iota>);
  static_assert(!std::ranges::forward_range<Iota>);
  {
    size_t sum = 0;
    for (auto s : Iota{IotaImpl{0, 5}}) {
      sum += s;
    }
    EXPECT_EQ(sum, 10);
  }
  {
    size_t sum = 0;
    [[maybe_unused]] InputRangeToOptional blubb{
        std::views::iota(size_t{0}, size_t{5})};
    auto ptr = std::make_unique<decltype(blubb)>(std::move(blubb));
    static_assert(
        std::is_base_of_v<InputRangeOptionalMixin<size_t>, decltype(blubb)>);
    static_assert(
        std::derived_from<decltype(blubb), InputRangeOptionalMixin<size_t>>);
    std::unique_ptr<InputRangeOptionalMixin<size_t>> ptr2 = std::move(ptr);
    // Iota iota{std::move(blubb)};
    for (auto s : Iota{std::views::iota(size_t{0}, size_t{5})}) {
      sum += s;
    }
    EXPECT_EQ(sum, 10);
  }

  Iota iota{IotaImpl{}};
  auto view = iota | std::views::drop(3) | std::views::take(7);
  size_t sum = 0;
  auto add = [&sum](auto val) { sum += val; };
  std::ranges::for_each(view, add);

  // 42 == 3 + 4 + ... + 9
  EXPECT_EQ(sum, 42);
}
