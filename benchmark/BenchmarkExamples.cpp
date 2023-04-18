// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/Random.h"

namespace ad_benchmark {
/*
A typical problem in benchmarking is that the result of a computation is
not used and thus the whole computation gets optimized out by the compiler.
To avoid this, the `BMSingleMeasurements` example prints the result of a
computation to the screen.

A more elegant solution to prevent such unwanted compiler optimizations, can be
found in google benchmark:
`https://github.com/google/benchmark/blob/main/docs/user_guide.md#preventing-
optimization`.

Using the functions described there would require including
`Google Benchmark` as a  third-party library.
*/

// Single Measurements
class BMSingleMeasurements : public BenchmarkInterface {
 public:
  void parseConfiguration(const BenchmarkConfiguration&) final {
    // Nothing to actually do here.
  }

  BenchmarkMetadata getMetadata() const final {
    // Again, nothing to really do here.
    return BenchmarkMetadata{};
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Setup.
    const size_t number = SlowRandomIntGenerator<size_t>(10, 1'000)();
    auto exponentiate = [](const size_t numberToExponentiate) {
      return numberToExponentiate * numberToExponentiate;
    };

    // Measuring functions.
    results.addMeasurement("Exponentiate once", [&exponentiate, &number]() {
      exponentiate(number);
    });
    auto& multipleTimes = results.addMeasurement(
        "Recursivly exponentiate multiple times", [&number, &exponentiate]() {
          size_t toExponentiate = number;
          for (size_t i = 0; i < 10'000'000'000; i++) {
            toExponentiate = exponentiate(toExponentiate);
          }
          // Too much optimization without the line. Alternative can be found
          // under the `DoNotOptimize(...)` of google benchmark.
          std::cout << toExponentiate;
        });

    // Adding some basic metadata.
    multipleTimes.metadata().addKeyValuePair("Amount of exponentiations",
                                             10'000'000'000);

    return results;
  }
};

// Groups
class BMGroups : public BenchmarkInterface {
 public:
  void parseConfiguration(const BenchmarkConfiguration&) final {
    // Nothing to actually do here.
  }

  BenchmarkMetadata getMetadata() const final {
    // Again, nothing to really do here.
    return BenchmarkMetadata{};
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Setup.
    auto loopAdd = [](const size_t a, const size_t b) {
      size_t toReturn = a;
      for (size_t i = 0; i < b; i++) {
        toReturn += 1;
      }
      return toReturn;
    };

    auto loopMultiply = [](const size_t a, const size_t b) {
      size_t toReturn = a;
      for (size_t i = 0; i < b; i++) {
        toReturn += a;
      }
      return toReturn;
    };

    // Measuring functions.
    auto& loopAddGroup = results.addGroup("loopAdd");
    loopAddGroup.metadata().addKeyValuePair("Operator", '+');

    auto& loopMultiplyGroup = results.addGroup("loopMultiply");
    loopMultiplyGroup.metadata().addKeyValuePair("Operator", '*');

    auto& addMember1 =
        loopAddGroup.addMeasurement("1+1", [&loopAdd]() { loopAdd(1, 1); });
    addMember1.metadata().addKeyValuePair("Result", 2);

    auto& addMember2 =
        loopAddGroup.addMeasurement("42+69", [&loopAdd]() { loopAdd(42, 69); });
    addMember2.metadata().addKeyValuePair("Result", 42 + 69);

    auto& addMember3 = loopAddGroup.addMeasurement(
        "10775+24502", [&loopAdd]() { loopAdd(10775, 24502); });
    addMember3.metadata().addKeyValuePair("Result", 10775 + 24502);

    auto& multiplicationMember1 = loopMultiplyGroup.addMeasurement(
        "1*1", [&loopMultiply]() { loopMultiply(1, 1); });
    multiplicationMember1.metadata().addKeyValuePair("Result", 1);

    auto& multiplicationMember2 = loopMultiplyGroup.addMeasurement(
        "42*69", [&loopMultiply]() { loopMultiply(42, 69); });
    multiplicationMember2.metadata().addKeyValuePair("Result", 42 * 69);

    auto& multiplicationMember3 = loopMultiplyGroup.addMeasurement(
        "10775*24502", [&loopMultiply]() { loopMultiply(10775, 24502); });
    multiplicationMember3.metadata().addKeyValuePair("Result", 10775 * 24502);

    return results;
  }
};

// Tables
class BMTables : public BenchmarkInterface {
 public:
  void parseConfiguration(const BenchmarkConfiguration&) final {
    // Nothing to actually do here.
  }

  BenchmarkMetadata getMetadata() const final {
    // Again, nothing to really do here.
    return BenchmarkMetadata{};
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Setup.
    auto exponentiateNTimes = [](const size_t number, const size_t n) {
      size_t toReturn = 1;
      for (size_t i = 0; i < n; i++) {
        toReturn *= number;
      }
      return toReturn;
    };

    // Measuring functions.
    auto& tableExponentsWithBasis = results.addTable(
        "Exponents with the given basis", {"2", "3", "Time difference"},
        {"0", "1", "2", "3", "4"});

    auto& tableAddingExponents = results.addTable(
        "Adding exponents", {"2^10", "2^11", "Values written out"},
        {"2^10", "2^11"});
    // Adding some metadata.
    tableAddingExponents.metadata().addKeyValuePair("Manually set fields",
                                                    "Row 2");

    // Measure the calculating of the exponents.
    for (int i = 0; i < 5; i++) {
      tableExponentsWithBasis.addMeasurement(
          0, i, [&exponentiateNTimes, &i]() { exponentiateNTimes(2, i); });
    }
    for (int i = 0; i < 5; i++) {
      tableExponentsWithBasis.addMeasurement(
          1, i, [&exponentiateNTimes, &i]() { exponentiateNTimes(3, i); });
    }
    // Manually adding the entries of the third column by computing the timing
    // difference between the first two columns.
    for (size_t column = 0; column < 5; column++) {
      float entryWithBasis2 =
          tableExponentsWithBasis.getEntry<float>(0, column);
      float entryWithBasis3 =
          tableExponentsWithBasis.getEntry<float>(1, column);
      tableExponentsWithBasis.setEntry(
          2, column, std::abs(entryWithBasis3 - entryWithBasis2));
    }

    // Measuers for calculating and adding the exponents.
    for (int row = 0; row < 2; row++) {
      for (int column = 0; column < 2; column++) {
        tableAddingExponents.addMeasurement(
            row, column, [&exponentiateNTimes, &row, &column]() {
              size_t temp __attribute__((unused));
              temp = exponentiateNTimes(2, row + 10) +
                     exponentiateNTimes(2, column + 10);
            });
      }
    }

    // Manually setting strings.
    tableAddingExponents.setEntry(2, 0, "1024+1024 and 1024+2048");
    tableAddingExponents.setEntry(2, 1, "1024+2048 and 2048+2048");

    return results;
  }
};

// A simple example of the usage of the `BenchmarkConfiguration` and the
// general `BenchmarkMetadata`.
class BMConfigurationAndMetadataExample : public BenchmarkInterface {
  // This class will simply transcribe specific configuration options
  // to this `BenchmarkMetadta` object and return it later with the
  // `getMetadata()` function.
  BenchmarkMetadata generalMetadata_;

 public:
  void parseConfiguration(const BenchmarkConfiguration& config) final {
    // Collect some arbitrary values.
    std::string dateString{
        config.getValueByNestedKeys<std::string>("exampleDate")
            .value_or("22.3.2023")};
    size_t numberOfStreetSigns{
        config.getValueByNestedKeys<size_t>("numSigns").value_or(10)};

    std::vector<bool> wonOnTryX{};
    wonOnTryX.reserve(5);
    for (size_t i = 0; i < 5; i++) {
      wonOnTryX.push_back(config.getValueByNestedKeys<bool>("Coin_flip_try", i)
                              .value_or(false));
    }

    float balanceOnStevesSavingAccount{
        config.getValueByNestedKeys<float>("Accounts", "Personal", "Steve")
            .value_or(-41.9)};

    // Transcribe the collected values.
    generalMetadata_.addKeyValuePair("date", dateString);
    generalMetadata_.addKeyValuePair("numberOfStreetSigns",
                                     numberOfStreetSigns);
    generalMetadata_.addKeyValuePair("wonOnTryX", wonOnTryX);
    generalMetadata_.addKeyValuePair("Balance on Steves saving account",
                                     balanceOnStevesSavingAccount);
  }

  BenchmarkMetadata getMetadata() const final { return generalMetadata_; }

  // This is just a dummy, because this class is only an example for other
  // features of the benchmark infrastructure.
  BenchmarkResults runAllBenchmarks() final { return BenchmarkResults{}; }
};

// Registering the benchmarks.
AD_REGISTER_BENCHMARK(BMSingleMeasurements);
AD_REGISTER_BENCHMARK(BMGroups);
AD_REGISTER_BENCHMARK(BMTables);
AD_REGISTER_BENCHMARK(BMConfigurationAndMetadataExample);
}  // namespace ad_benchmark
