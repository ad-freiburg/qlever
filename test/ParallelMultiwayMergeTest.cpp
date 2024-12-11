//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/ParallelMultiwayMerge.h"
#include "util/Random.h"

namespace {
using namespace ad_utility;
using namespace ad_utility::memory_literals;

// Join a range of ranges into a single vector, e.g. `array<generator<size_t>>
// -> vector<size_t>`.
auto join = []<typename Range>(Range&& range) {
  std::vector<ql::ranges::range_value_t<ql::ranges::range_value_t<Range>>>
      result;
  auto view = ql::views::join(ad_utility::OwningView{AD_FWD(range)});
  ql::ranges::copy(view, std::back_inserter(result));
  return result;
};

// Run a test for a parallel multiway merge with random inputs. The input to the
// merge are `numVecs` many `vector<size_t>` objects, each of which consists of
// random numbers. The size of each vector is also random and taken from the
// interval `[minVecSize, maxVecSize]`.
template <size_t blocksize, size_t numVecs, size_t minVecSize,
          size_t maxVecSize>
void testRandomInts() {
  auto generateRandomVec =
      [gen = ad_utility::FastRandomIntGenerator<uint64_t>{},
       numRowsGen = ad_utility::SlowRandomIntGenerator<size_t>(
           minVecSize, maxVecSize)]() mutable {
        std::vector<size_t> res(numRowsGen());
        ql::ranges::generate(res, gen);
        ql::ranges::sort(res);
        return res;
      };

  std::vector<std::vector<size_t>> input(numVecs);
  ql::ranges::generate(input, generateRandomVec);

  auto expected = join(std::vector<std::vector<size_t>>{input});
  ql::ranges::sort(expected);

  std::vector<size_t> result;
  ql::ranges::copy(ql::views::join(ad_utility::OwningView{
                       ad_utility::parallelMultiwayMerge<size_t, false>(
                           1_GB, input, std::less<>{}, blocksize)}),
                   std::back_inserter(result));

  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}
}  // namespace

// ____________________________________________________________________________________________
TEST(ParallelMultiwayMerge, binaryMerge) {
  using V = std::vector<size_t>;
  V v1{1, 3, 5};
  V v2{2, 4, 6};
  auto result = join(parallelMultiwayMerge<size_t, false>(
      1_GB, std::array{v1, v2}, std::less<>{}, 3));
  EXPECT_THAT(result, ::testing::ElementsAre(1, 2, 3, 4, 5, 6));

  v2 = V{};
  result = join(parallelMultiwayMerge<size_t, false>(1_GB, std::array{v1, v2},
                                                     std::less<>{}, 3));
  EXPECT_THAT(result, ::testing::ElementsAre(1, 3, 5));

  std::swap(v1, v2);
  result = join(parallelMultiwayMerge<size_t, false>(1_GB, std::array{v1, v2},
                                                     std::less<>{}, 3));
  EXPECT_THAT(result, ::testing::ElementsAre(1, 3, 5));
}

// _______________________________________________________________________________________________
TEST(ParallelMultiwayMerge, moveOfElements) {
  using V = std::vector<std::string>;
  auto arr = std::array<std::vector<std::string>, 2>{
      V{"alphaalpha", "deltadelta"}, V{"betabeta", "epsilonepsilon"}};
  auto& [v1, v2] = arr;
  EXPECT_THAT(v1, ::testing::ElementsAre("alphaalpha", "deltadelta"));
  EXPECT_THAT(v2, ::testing::ElementsAre("betabeta", "epsilonepsilon"));
  auto result = join(
      parallelMultiwayMerge<std::string, false>(1_GB, arr, std::less<>{}, 3));
  EXPECT_THAT(result, ::testing::ElementsAre("alphaalpha", "betabeta",
                                             "deltadelta", "epsilonepsilon"));

  // The strings weren't moved.
  EXPECT_THAT(v1, ::testing::ElementsAre("alphaalpha", "deltadelta"));
  EXPECT_THAT(v2, ::testing::ElementsAre("betabeta", "epsilonepsilon"));

  result.clear();
  result = join(
      parallelMultiwayMerge<std::string, true>(1_GB, arr, std::less<>{}, 3));
  EXPECT_THAT(result, ::testing::ElementsAre("alphaalpha", "betabeta",
                                             "deltadelta", "epsilonepsilon"));

  // The vectors were moved.
  EXPECT_TRUE(v1.empty());
  EXPECT_TRUE(v2.empty());
}

// _______________________________________________________________________________________________
TEST(ParallelMultiwayMerge, randomInputs) {
  testRandomInts<12, 2000, 20, 50>();
  testRandomInts<13, 1, 40, 40>();
  testRandomInts<5, 2, 40, 50>();
  testRandomInts<1, 3, 30, 50>();
}
