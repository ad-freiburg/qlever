// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
#include <cmath>
#include <string>
#include <memory>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/Random.h"

/*
TODO Currently there is no way to prevent the result of expressions from being
discarded, if it's not used, which sabotages the measured execution times of
benchmarks. Because an expression, who's result got discarded and which has no
side effects, can also be safely discarded, if the compiler optimizes.
One solution would be to use the functons in google benchmark, that prevent
optimization:
`https://github.com/google/benchmark/blob/main/docs/user_guide.md#preventing-optimization`

For that however, we first would have to include their benchmarking system as
third party.
*/

// Single Measurements
class BM_SingeMeasurements: public BenchmarkClassInterface{
  public:

  void parseConfiguration(const BenchmarkConfiguration&){
    // Nothing to actually do here.
  }

  virtual const BenchmarkMetadata getMetadata() const{
    // Again, nothing to really do here.
    return BenchmarkMetadata{};
  }

  virtual BenchmarkRecords runAllBenchmarks(){
    BenchmarkRecords records{};
    
    // Setup.
    const size_t number = SlowRandomIntGenerator<size_t>(10,1'000)();
    auto exponentiate = [](const size_t number){
      return number*number;
    };

    // Measuring functions.
    records.addMeasurement("Exponentiate once", [&](){exponentiate(number);});
    auto& multipleTimes =
      records.addMeasurement("Recursivly exponentiate multiple times",
      [&](){
          size_t to_exponentiate = number;
          for (size_t i = 0; i < 10'000'000'000; i++) {
            to_exponentiate = exponentiate(to_exponentiate);
          }
          // TODO Too much optimization without the line. Alternative can be found
          // under the `DoNotOptimize(...)` of google benchmark.
          std::cout << to_exponentiate;
        });

    // Adding some basic metadata.
    multipleTimes.metadata_.addKeyValuePair("Amount of exponentiations",
      10'000'000'000);

    return records;
  } 
};

// Groups
class BM_Groups: public BenchmarkClassInterface{
  public:

  void parseConfiguration(const BenchmarkConfiguration&){
    // Nothing to actually do here.
  }

  virtual const BenchmarkMetadata getMetadata() const{
    // Again, nothing to really do here.
    return BenchmarkMetadata{};
  }

  virtual BenchmarkRecords runAllBenchmarks(){
    BenchmarkRecords records{};

    // Setup.
    auto loopAdd = [](const size_t a, const size_t b) {
      size_t to_return = a;
      for (size_t i = 0; i < b; i++) {
        to_return += 1;
      }
      return to_return;
    };

    auto loopMultiply = [](const size_t a, const size_t b) {
      size_t to_return = a;
      for (size_t i = 0; i < b; i++) {
        to_return += a;
      }
      return to_return;
    };

    // Measuring functions.
    auto& loopAddGroup{records.addGroup("loopAdd")};
    auto& loopMultiplyGroup{records.addGroup("loopMultiply")};

    auto& addMember1{loopAddGroup.addMeasurement(
      std::make_unique<BenchmarkMeasurementContainer::RecordEntry>(
      "1+1", [&](){loopAdd(1,1);}))};
    auto& addMember2{loopAddGroup.addMeasurement(
      std::make_unique<BenchmarkMeasurementContainer::RecordEntry>(
      "42+69", [&](){loopAdd(42,69);}))};
    auto& addMember3{loopAddGroup.addMeasurement(
      std::make_unique<BenchmarkMeasurementContainer::RecordEntry>(
      "10775+24502", [&](){loopAdd(10775, 24502);}))};

    auto& multiplicationMember1{loopMultiplyGroup.addMeasurement(
      std::make_unique<BenchmarkMeasurementContainer::RecordEntry>(
      "1*1", [&](){loopMultiply(1,1);}))};
    auto& multiplicationMember2{loopMultiplyGroup.addMeasurement(
      std::make_unique<BenchmarkMeasurementContainer::RecordEntry>(
      "42*69", [&](){loopMultiply(42,69);}))};
    auto& multiplicationMember3{loopMultiplyGroup.addMeasurement(
      std::make_unique<BenchmarkMeasurementContainer::RecordEntry>(
      "10775*24502", [&](){loopMultiply(10775, 24502);}))};

    // Adding some metadata.
    loopAddGroup.metadata_.addKeyValuePair("Operator", '+');
    addMember1.metadata_.addKeyValuePair("Result", 2);
    addMember2.metadata_.addKeyValuePair("Result", 42+69);
    addMember3.metadata_.addKeyValuePair("Result", 10775+24502);

    loopMultiplyGroup.metadata_.addKeyValuePair("Operator", '*');
    multiplicationMember1.metadata_.addKeyValuePair("Result", 1);
    multiplicationMember2.metadata_.addKeyValuePair("Result", 42*69);
    multiplicationMember3.metadata_.addKeyValuePair("Result", 10775*24502);
    
    return records;
  } 
};

// Tables
class BM_Tables: public BenchmarkClassInterface{
  public:

  void parseConfiguration(const BenchmarkConfiguration&){
    // Nothing to actually do here.
  }

  virtual const BenchmarkMetadata getMetadata() const{
    // Again, nothing to really do here.
    return BenchmarkMetadata{};
  }

  virtual BenchmarkRecords runAllBenchmarks(){
    BenchmarkRecords records{};

    // Setup.
    auto exponentiateNTimes = [](const size_t number, const size_t n){
      size_t to_return = 1;
      for (size_t i = 0; i < n; i++) {
        to_return *= number;
      }
      return to_return;
    };

    // Measuring functions.
    auto& tableExponentsWithBasis{records.addTable(
      "Exponents with the given basis", {"2", "3", "Time difference"},
      {"0", "1", "2", "3", "4"})};
    auto& tableAddingExponents{records.addTable("Adding exponents",
      {"2^10", "2^11", "Values written out"}, {"2^10", "2^11"})};

    // Measuere the calculating of the exponents.
    for (int i = 0; i < 5; i++) {
      tableExponentsWithBasis.addMeasurement(0, i,
        [&](){exponentiateNTimes(2,i);});
    }
    for (int i = 0; i < 5; i++) {
      tableExponentsWithBasis.addMeasurement(1, i,
        [&](){exponentiateNTimes(3,i);});
    }
    // Calculating the time differnce between them.
    for (size_t column = 0; column < 5; column++){
      float entryWithBasis2 = tableExponentsWithBasis.getEntry<float>(0,
        column);
      float entryWithBasis3 = tableExponentsWithBasis.getEntry<float>(1,
        column);
      tableExponentsWithBasis.setEntry(2, column,
        std::abs(entryWithBasis3 - entryWithBasis2));
    }

    // Measuers for calculating and adding the exponents.
    for (int row = 0; row < 2; row++) {
      for (int column = 0; column < 2; column++) {
        tableAddingExponents.addMeasurement(row, column,
          [&](){size_t temp __attribute__((unused));
            temp =
            exponentiateNTimes(2, row+10)+exponentiateNTimes(2, column+10);});
      }
    }

    // Manually setting strings.
    tableAddingExponents.setEntry(2, 0, "1024+1024 and 1024+2048");
    tableAddingExponents.setEntry(2, 1, "1024+2048 and 2048+2048");

    // Adding some metadata.
    tableAddingExponents.metadata_.addKeyValuePair("Manually set fields",
      "Row 2");
    
    return records;
  } 
};

// A simple example of the usage of the `BenchmarkConfiguration` and the
// general `BenchmarkMetadata`.
class BM_ConfigurationAndMetadataExample: public BenchmarkClassInterface{
  // This class will simply transcribe specific configuration options
  // to this `BenchmarkMetadta` object and return it later with the
  // `getMetadata()` function.
  BenchmarkMetadata generalMetadata_;

  public:
  void parseConfiguration(const BenchmarkConfiguration& config){
    // Collect some arbitrary values.
    std::string dateString{
      config.getValueByNestedKeys<std::string>("exampleDate").value_or(
      "22.3.2023")};
    size_t numberOfStreetSigns{
      config.getValueByNestedKeys<size_t>("numSigns").value_or(10)};

    std::vector<bool> wonOnTryX{};
    wonOnTryX.reserve(5);
    for (size_t i = 0; i < 5; i++){
      wonOnTryX.push_back(config.getValueByNestedKeys<bool>("Coin_flip_try",
        i).value_or(false));
    }

    float balanceOnStevesSavingAccount{config.getValueByNestedKeys<float>(
      "Accounts", "Personal", "Steve").value_or(-41.9)};

    // Transcribe the collected values.
    generalMetadata_.addKeyValuePair("date", dateString);
    generalMetadata_.addKeyValuePair("numberOfStreetSigns",
      numberOfStreetSigns);
    generalMetadata_.addKeyValuePair("wonOnTryX", wonOnTryX);
    generalMetadata_.addKeyValuePair("Balance on Steves saving account",
      balanceOnStevesSavingAccount);
  }

  const BenchmarkMetadata getMetadata() const{
    return generalMetadata_;
  }

  // This is just a dummy, because this class is only an example for other
  // features of the benchmark infrastructure.
  BenchmarkRecords runAllBenchmarks(){
    return BenchmarkRecords{};
  }
};


// Registering the benchmarks.
registerBenchmark(BM_SingeMeasurements);
registerBenchmark(BM_Groups);
registerBenchmark(BM_Tables);
registerBenchmark(BM_ConfigurationAndMetadataExample);
