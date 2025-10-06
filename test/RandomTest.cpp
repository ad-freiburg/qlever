// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (October of 2023, schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <ctre.hpp>
#include <random>
#include <ranges>
#include <unordered_set>

#include "../test/util/RandomTestHelpers.h"
#include "backports/type_traits.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/Random.h"
#include "util/SourceLocation.h"
#include "util/TypeTraits.h"

namespace ad_utility {

/*
@brief Test, if random number generators, that take a seed, produce the same
numbers for the same seed.

@param randomNumberGeneratorFactory An invocable object, that should return a
random number generator, using the given seed.
*/
CPP_template(typename T)(requires std::invocable<T, RandomSeed>) void testSeed(
    T randomNumberGeneratorFactory,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "testSeed")};

  // For how many random seeds should the test be done for?
  constexpr size_t NUM_SEEDS = 5;
  static_assert(NUM_SEEDS > 1);

  // How many instances of the random number generator, with the given
  // seed, should we compare?
  constexpr size_t NUM_GENERATORS = 3;
  static_assert(NUM_GENERATORS > 1);

  // How many random numbers should be generated for comparison?
  constexpr size_t NUM_RANDOM_NUMBER = 50;
  static_assert(NUM_RANDOM_NUMBER > 1);

  // For every seed test, if the random number generators return the same
  // numbers.
  ql::ranges::for_each(
      createArrayOfRandomSeeds<NUM_SEEDS>(),
      [&randomNumberGeneratorFactory](const RandomSeed seed) {
        // What type of generator does the factory create?
        using GeneratorType = std::invoke_result_t<T, RandomSeed>;

        // What kind of number does the generator return?
        using NumberType = std::invoke_result_t<GeneratorType>;

        // The generators, that should create the same numbers.
        std::array<GeneratorType, NUM_GENERATORS> generators;
        ql::ranges::generate(
            generators, [&randomNumberGeneratorFactory, &seed]() {
              return std::invoke(randomNumberGeneratorFactory, seed);
            });

        // Do they generators create the same numbers?
        for (size_t numCall = 0; numCall < NUM_RANDOM_NUMBER; numCall++) {
          const NumberType expectedNumber = std::invoke(generators.front());

          ql::ranges::for_each(ql::views::drop(generators, 1),
                               [&expectedNumber](GeneratorType& g) {
                                 ASSERT_EQ(std::invoke(g), expectedNumber);
                               });
        }
      });
}

TEST(RandomNumberGeneratorTest, FastRandomIntGenerator) {
  testSeed(
      [](RandomSeed seed) { return FastRandomIntGenerator<size_t>{seed}; });
}

// Describes an inclusive numerical range `[min, max]`.
template <typename T>
struct NumericalRange {
  const T minimum_;
  const T maximum_;

  NumericalRange(const T minimum, const T maximum)
      : minimum_{minimum}, maximum_{maximum} {
    AD_CORRECTNESS_CHECK(minimum <= maximum);
  }
};

/*
@brief Test, if random number generators, that take a seed and a range, produce
the same numbers for the same seed and range.

@param randomNumberGeneratorFactory An invocable object, that should return a
random number generator, using the given range and seed. Functionparameters
should be `RangeNumberType rangeMin, RangeNumberType rangeMax, Seed seed`.
@param ranges The ranges, that should be used.
*/
CPP_template(typename RangeNumberType, typename GeneratorFactory)(
    requires std::invocable<
        GeneratorFactory, RangeNumberType, RangeNumberType,
        RandomSeed>) void testSeedWithRange(GeneratorFactory
                                                randomNumberGeneratorFactory,
                                            const std::vector<NumericalRange<
                                                RangeNumberType>>& ranges,
                                            ad_utility::source_location l =
                                                AD_CURRENT_SOURCE_LOC()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "testSeedWithRange")};

  ql::ranges::for_each(ranges, [&randomNumberGeneratorFactory](
                                   const NumericalRange<RangeNumberType>& r) {
    testSeed([&r, &randomNumberGeneratorFactory](RandomSeed seed) {
      return std::invoke(randomNumberGeneratorFactory, r.minimum_, r.maximum_,
                         seed);
    });
  });
}

/*
@brief Test, if a given random number generator only creates numbers in a range.

@tparam Generator The random number generator. Must have a constructor, whose
first argument is the minimum of the range and the second argument the maximum
of the range.

@param ranges The ranges, for which should be tested for.
*/
template <typename Generator, typename RangeNumberType>
requires std::constructible_from<Generator, RangeNumberType, RangeNumberType> &&
         std::invocable<Generator> &&
         ad_utility::isSimilar<std::invoke_result_t<Generator>, RangeNumberType>
void testRange(const std::vector<NumericalRange<RangeNumberType>>& ranges,
               ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "testRange")};

  // How many random numbers should be generated?
  constexpr size_t NUM_RANDOM_NUMBER = 500;
  static_assert(NUM_RANDOM_NUMBER > 1);

  ql::ranges::for_each(ranges, [](const NumericalRange<RangeNumberType>& r) {
    Generator generator(r.minimum_, r.maximum_);
    const auto& generatedNumber = std::invoke(generator);
    ASSERT_LE(generatedNumber, r.maximum_);
    ASSERT_GE(generatedNumber, r.minimum_);
  });
}

TEST(RandomNumberGeneratorTest, SlowRandomIntGenerator) {
  testSeed([](RandomSeed seed) {
    return SlowRandomIntGenerator<size_t>{std::numeric_limits<size_t>::min(),
                                          std::numeric_limits<size_t>::max(),
                                          seed};
  });

  // For use within the range tests.
  const std::vector<NumericalRange<size_t>> ranges{
      {4ul, 7ul}, {200ul, 70171ul}, {71747ul, 1936556173ul}};

  testSeedWithRange(
      [](const auto rangeMin, const auto rangeMax, RandomSeed seed) {
        return SlowRandomIntGenerator{rangeMin, rangeMax, seed};
      },
      ranges);

  testRange<SlowRandomIntGenerator<size_t>>(ranges);
}

TEST(RandomNumberGeneratorTest, RandomDoubleGenerator) {
  testSeed([](RandomSeed seed) {
    return RandomDoubleGenerator{std::numeric_limits<double>::min(),
                                 std::numeric_limits<double>::max(), seed};
  });

  // For use within the range tests.
  const std::vector<NumericalRange<double>> ranges{
      {4.74717, 7.4}, {-200.0771370, -70.77713}, {-71747.6666, 1936556173.}};

  // Repeat this test, but this time do it inside of a range.
  testSeedWithRange(
      [](const auto rangeMin, const auto rangeMax, RandomSeed seed) {
        return RandomDoubleGenerator{rangeMin, rangeMax, seed};
      },
      ranges);

  testRange<RandomDoubleGenerator>(ranges);
}

// Test for the performance of `FastRandomIntGenerator` and
// `RandomDoubleGenerator`.
// NOTE: This does not actually test anything. It's just here to measure the
// performance of the random number generators.
TEST(RandomNumberGeneratorTest, PerformanceTes) {
  // Create random number generators.
  FastRandomIntGenerator<size_t> fastIntGenerator;
  RandomDoubleGenerator doubleGenerator(0.0, 1.0);
  size_t n = 1'000'000;
  // Lambda for measuring the speed of a given random number generator.
  auto measureAndShowSpeed = [&n](auto& generator, std::string name) {
    auto start = std::chrono::high_resolution_clock::now();
    decltype(generator()) sum = 0;
    for (size_t i = 0; i < n; i++) {
      sum += generator();
    }
    // Show in ns per number with one digit after the comma.
    std::cout << "Speed of " << name << ": " << std::fixed
              << std::setprecision(1)
              << (static_cast<double>(
                      std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::high_resolution_clock::now() - start)
                          .count()) /
                  n)
              << " ns per number" << std::setprecision(4)
              << " [average value: " << (sum / n) << "]" << std::endl;
  };
  // Measure the time of our two random number generators.
  measureAndShowSpeed(fastIntGenerator, "FastRandomIntGenerator");
  measureAndShowSpeed(doubleGenerator, "RandomDoubleGenerator");
}

// Small test, if `randomShuffle` shuffles things the same way, if given the
// same seed.
TEST(RandomShuffleTest, Seed) {
  // For how many random seeds should the test be done for?
  constexpr size_t NUM_SEEDS = 5;
  static_assert(NUM_SEEDS > 1);

  // How many shuffled `std::array` should we compare per seed? And how big
  // should they be?
  constexpr size_t NUM_SHUFFLED_ARRAY = 3;
  constexpr size_t ARRAY_LENGTH = 100;
  static_assert(NUM_SHUFFLED_ARRAY > 1 && ARRAY_LENGTH > 1);

  /*
  For every random seed test, if the shuffled array is the same, if given
  identical input and seed.
  */
  ql::ranges::for_each(
      createArrayOfRandomSeeds<NUM_SEEDS>(), [](const RandomSeed seed) {
        std::array<std::array<int, ARRAY_LENGTH>, NUM_SHUFFLED_ARRAY>
            inputArrays{};

        // Fill the first input array with random values, then copy it into the
        // other 'slots'.
        ql::ranges::generate(inputArrays.front(),
                             FastRandomIntGenerator<int>{});
        ql::ranges::fill(ql::views::drop(inputArrays, 1), inputArrays.front());

        // Shuffle and compare, if they are all the same.
        ql::ranges::for_each(
            inputArrays, [&seed](std::array<int, ARRAY_LENGTH>& inputArray) {
              randomShuffle(inputArray.begin(), inputArray.end(), seed);
            });
        ql::ranges::for_each(
            ql::views::drop(inputArrays, 1),
            [&inputArrays](const std::array<int, ARRAY_LENGTH>& inputArray) {
              ASSERT_EQ(inputArrays.front(), inputArray);
            });
      });
}

TEST(UuidGeneratorTest, StrUuidGeneratorTest) {
  // Test few times that the returned UUID str is not
  // "00000000-0000-0000-0000-000000000000" (nil-UUID)
  // and that none of the str-UUIDS is rquivalent to the already
  // created ones.
  // Pattern for checking that UUID is properly formatted
  static constexpr auto uuidPattern = ctll::fixed_string{
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{"
      "3}-[0-9a-fA-F]{12}$"};
  boost::uuids::string_generator getUuid;
  UuidGenerator gen = UuidGenerator();
  std::unordered_set<std::string> setUuids;
  size_t i = 0;
  while (i < 100) {
    std::string strUuid = gen();
    boost::uuids::uuid uuid = getUuid(strUuid.data());
    ASSERT_EQ(uuid.is_nil(), false);
    ASSERT_EQ(setUuids.find(strUuid), setUuids.end());
    ASSERT_TRUE(ctre::match<uuidPattern>(strUuid));
    setUuids.insert(strUuid);
    i++;
  }
}

}  // namespace ad_utility
