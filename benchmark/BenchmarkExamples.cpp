// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
#include <cmath>
#include <string>

#include "../benchmark/Benchmark.h"
#include "BenchmarkConfiguration.h"
#include "BenchmarkMetadata.h"
#include "util/Random.h"

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
    records.addSingleMeasurement("Exponentiate once", [&](){exponentiate(number);});
    records.addSingleMeasurement("Recursivly exponentiate multiple times", [&](){
          size_t to_exponentiate = number;
          for (size_t i = 0; i < 10'000'000'000; i++) {
            to_exponentiate = exponentiate(to_exponentiate);
          }
          // TODO Too much optimization without the line. Alternative can be found
          // under the `DoNotOptimize(...)` of google benchmark.
          std::cout << to_exponentiate;
        });

    // Adding some basic metadata.
    records.getMetadataOfSingleMeasurment(
        "Recursivly exponentiate multiple times").addKeyValuePair(
          "Amount of exponentiations", 10'000'000'000);

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
    records.addGroup("loopAdd");
    records.addGroup("loopMultiply");

    records.addToExistingGroup("loopAdd", "1+1", [&](){loopAdd(1,1);});
    records.addToExistingGroup("loopAdd", "42+69", [&](){loopAdd(42,69);});
    records.addToExistingGroup("loopAdd", "10775+24502",
        [&](){loopAdd(10775, 24502);});

    records.addToExistingGroup("loopMultiply", "1*1", [&](){loopMultiply(1,1);});
    records.addToExistingGroup("loopMultiply", "42*69",
        [&](){loopMultiply(42,69);});
    records.addToExistingGroup("loopMultiply", "10775*24502",
        [&](){loopMultiply(10775, 24502);});

    // Adding some metadata.
    records.getMetadataOfGroup("loopAdd").addKeyValuePair(
        "Operator", '+');
    records.getMetadataOfGroupMember("loopAdd",
        "1+1").addKeyValuePair("Result", 2);
    records.getMetadataOfGroupMember("loopAdd",
        "42+69").addKeyValuePair("Result", 42+69);
    records.getMetadataOfGroupMember("loopAdd",
        "10775+24502").addKeyValuePair("Result", 10775+24502);

    records.getMetadataOfGroup("loopMultiply").addKeyValuePair(
        "Operator", '*');
    records.getMetadataOfGroupMember("loopMultiply",
        "1*1").addKeyValuePair("Result", 1);
    records.getMetadataOfGroupMember("loopMultiply",
        "42*69").addKeyValuePair("Result", 42*69);
    records.getMetadataOfGroupMember("loopMultiply",
        "10775*24502").addKeyValuePair("Result", 10775*24502);
    
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
    records.addTable("Exponents with the given basis",
        {"2", "3", "Time difference"}, {"0", "1", "2", "3", "4"});
    records.addTable("Adding exponents", {"2^10", "2^11", "Values written out"},
        {"2^10", "2^11"});

    // Measuere the calculating of the exponents.
    for (int i = 0; i < 5; i++) {
      records.addToExistingTable("Exponents with the given basis", 0, i,
          [&](){exponentiateNTimes(2,i);});
    }
    for (int i = 0; i < 5; i++) {
      records.addToExistingTable("Exponents with the given basis", 1, i,
          [&](){exponentiateNTimes(3,i);});
    }
    // Calculating the time differnce between them.
    for (size_t column = 0; column < 5; column++){
      float entryWithBasis2 =
        records.getEntryOfExistingTable<float>("Exponents with the given basis",
            0, column);
      float entryWithBasis3 =
        records.getEntryOfExistingTable<float>("Exponents with the given basis",
            1, column);
      records.setEntryOfExistingTable("Exponents with the given basis",
            2, column, std::abs(entryWithBasis3 - entryWithBasis2));
    }

    // Measuers for calculating and adding the exponents.
    for (int row = 0; row < 2; row++) {
      for (int column = 0; column < 2; column++) {
        records.addToExistingTable("Adding exponents", row, column,
            [&](){size_t temp __attribute__((unused));
            temp =
            exponentiateNTimes(2, row+10)+exponentiateNTimes(2, column+10);});
      }
    }

    // Manually setting strings.
    records.setEntryOfExistingTable("Adding exponents", 2, 0,
        std::string{"1024+1024 and 1024+2048"});
    records.setEntryOfExistingTable("Adding exponents", 2, 1,
        std::string{"1024+2048 and 2048+2048"});

    // Adding some metadata.
    records.getMetadataOfTable("Adding exponents").addKeyValuePair(
        "Manually set fields", "Row 2");
    
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
    std::string dateString{config.getValueOrDefault<std::string>("22.3.2023",
      "exampleDate")};
    size_t numberOfStreetSigns{
      config.getValueOrDefault<size_t>(10, "numSigns")};

    std::vector<bool> wonOnTryX{};
    wonOnTryX.reserve(5);
    for (size_t i = 0; i < 5; i++){
      wonOnTryX.push_back(config.getValueOrDefault(false, "Coin_flip_try", i));
    }

    float balanceOnStevesSavingAccount{config.getValueOrDefault<float>(-41.9,
      "Accounts", "Personal", "Steve")};

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


BenchmarkRegister temp{{new BM_SingeMeasurements, new BM_Groups,
  new BM_Tables, new BM_ConfigurationAndMetadataExample}};
