// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/Random.h"

using namespace std::string_literals;

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

/*
The general configuration options. Only used by
`BMConfigurationAndMetadataExample`, but because of how we pass the
configuration, every benchmark needs to have them, even if they don't use it.
*/
class ConfigOptions : public BenchmarkInterface {
 protected:
  // For storing the configuration option values.
  std::string dateString_;
  int numberOfStreetSigns_;
  std::vector<bool> wonOnTryX_;
  float balanceOnStevesSavingAccount_;

 public:
  /*
  Just adds some arbitrary configuration options.
  */
  ConfigOptions() {
    ad_utility::ConfigManager& manager = getConfigManager();

    manager.addOption("date", "The current date.", &dateString_, "22.3.2023"s);

    auto numSigns = manager.addOption("numSigns", "The number of street signs.",
                                      &numberOfStreetSigns_, 10000);
    manager.addValidator([](const int& num) { return num >= 0; },
                         "The number of street signs must be at least 0!",
                         "Negative numbers, or floating point numbers, are not "
                         "allowed for the configuration option \"numSigns\".",
                         numSigns);

    manager.addOption("CoinFlipTry", "The number of succesful coin flips.",
                      &wonOnTryX_, {false, false, false, false, false});

    // Sub manager can be used to organize things better. They are basically
    // just a path prefix for all the options inside it.
    ad_utility::ConfigManager& subManager{
        manager.addSubManager({"Accounts"s, "Personal"s})};
    subManager.addOption("Steve"s, "Steves saving account balance.",
                         &balanceOnStevesSavingAccount_, -41.9f);
  }
};

// Single Measurements
class BMSingleMeasurements : public ConfigOptions {
 public:
  std::string name() const final { return "Example for single measurements"; }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    // Setup.
    const size_t number =
        ad_utility::SlowRandomIntGenerator<size_t>(10, 1'000)();
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
class BMGroups : public ConfigOptions {
 public:
  std::string name() const final { return "Example for group benchmarks"; }

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
    loopAddGroup.metadata().addKeyValuePair("Operator", "+");

    auto& loopMultiplyGroup = results.addGroup("loopMultiply");
    loopMultiplyGroup.metadata().addKeyValuePair("Operator", "*");

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

    // Addtables.
    ResultTable& addTable =
        loopAddGroup.addTable("Addition", {"42", "24"}, {"+", "42", "24"});
    addTable.addMeasurement(0, 1, [&loopAdd]() { loopAdd(42, 42); });
    addTable.addMeasurement(0, 2, [&loopAdd]() { loopAdd(42, 24); });
    addTable.addMeasurement(1, 1, [&loopAdd]() { loopAdd(24, 42); });
    addTable.addMeasurement(1, 2, [&loopAdd]() { loopAdd(24, 24); });

    ResultTable& multiplyTable = loopMultiplyGroup.addTable(
        "Multiplication", {"42", "24"}, {"*", "42", "24"});
    multiplyTable.addMeasurement(0, 1,
                                 [&loopMultiply]() { loopMultiply(42, 42); });
    multiplyTable.addMeasurement(0, 2,
                                 [&loopMultiply]() { loopMultiply(42, 24); });
    multiplyTable.addMeasurement(1, 1,
                                 [&loopMultiply]() { loopMultiply(24, 42); });
    multiplyTable.addMeasurement(1, 2,
                                 [&loopMultiply]() { loopMultiply(24, 24); });

    return results;
  }
};

// Tables
class BMTables : public ConfigOptions {
 public:
  std::string name() const final { return "Example for table benchmarks"; }

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
        {"Basis", "0", "1", "2", "3", "4"});

    auto& tableAddingExponents = results.addTable(
        "Adding exponents", {"2^10", "2^11", "Values written out"},
        {"", "2^10", "2^11"});
    // Adding some metadata.
    tableAddingExponents.metadata().addKeyValuePair("Manually set fields",
                                                    "Row 2");

    // Measure the calculating of the exponents.
    for (int i = 0; i < 5; i++) {
      tableExponentsWithBasis.addMeasurement(
          0, i + 1, [&exponentiateNTimes, &i]() { exponentiateNTimes(2, i); });
    }
    for (int i = 0; i < 5; i++) {
      tableExponentsWithBasis.addMeasurement(
          1, i + 1, [&exponentiateNTimes, &i]() { exponentiateNTimes(3, i); });
    }
    // Manually adding the entries of the third column by computing the timing
    // difference between the first two columns.
    for (size_t column = 0; column < 5; column++) {
      float entryWithBasis2 =
          tableExponentsWithBasis.getEntry<float>(0, column + 1);
      float entryWithBasis3 =
          tableExponentsWithBasis.getEntry<float>(1, column + 1);
      tableExponentsWithBasis.setEntry(
          2, column + 1, std::abs(entryWithBasis3 - entryWithBasis2));
    }

    // Measuers for calculating and adding the exponents.
    for (int row = 0; row < 2; row++) {
      for (int column = 0; column < 2; column++) {
        tableAddingExponents.addMeasurement(
            row, column + 1, [&exponentiateNTimes, &row, &column]() {
              size_t temp __attribute__((unused));
              temp = exponentiateNTimes(2, row + 10) +
                     exponentiateNTimes(2, column + 10);
            });
      }
    }

    // Manually setting strings.
    tableAddingExponents.setEntry(2, 1, "1024+1024 and 1024+2048");
    tableAddingExponents.setEntry(2, 2, "1024+2048 and 2048+2048");

    return results;
  }
};

// A simple example of the usage of the `ad_utility::ConfigManager` and the
// general `BenchmarkMetadata`.
class BMConfigurationAndMetadataExample : public ConfigOptions {
 public:
  std::string name() const final {
    return "Example for the usage of configuration and metadata";
  }

  /*
  This class will simply transcribe the data of the configuration options
  to the general metadata of the class.
  */
  BenchmarkResults runAllBenchmarks() final {
    BenchmarkMetadata& meta{getGeneralMetadata()};
    meta.addKeyValuePair("date", dateString_);
    meta.addKeyValuePair("numberOfStreetSigns", numberOfStreetSigns_);
    meta.addKeyValuePair("wonOnTryX", wonOnTryX_);
    meta.addKeyValuePair("Balance on Steves saving account",
                         balanceOnStevesSavingAccount_);
    return BenchmarkResults{};
  }
};

// Registering the benchmarks.
AD_REGISTER_BENCHMARK(BMSingleMeasurements);
AD_REGISTER_BENCHMARK(BMGroups);
AD_REGISTER_BENCHMARK(BMTables);
AD_REGISTER_BENCHMARK(BMConfigurationAndMetadataExample);
}  // namespace ad_benchmark
