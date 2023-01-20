// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
#include "../benchmark/Benchmark.h"
#include "util/Random.h"

// Single Measurements
void BM_SingeMeasurements(BenchmarkRecords* records) {
  // Setup.
  const size_t number = SlowRandomIntGenerator<size_t>(10,20)();
  auto exponentiate = [](const size_t number){
    return number*number;
  };

  // Measuring functions.
  records->addSingleMeasurement("Exponentiate once", [&](){exponentiate(number);});
  records->addSingleMeasurement("Recursivly exponentiate ten times", [&](){
        size_t to_exponentiate = number;
        for (int i = 0; i < 10000000; i++) {
          to_exponentiate = exponentiate(to_exponentiate);
        }
      });
}

// Groups
void BM_Groups(BenchmarkRecords* records) {
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
  records->addGroup("loopAdd");
  records->addGroup("loopMultiply");

  records->addToExistingGroup("loopAdd", "1+1", [&](){loopAdd(1,1);});
  records->addToExistingGroup("loopAdd", "42+69", [&](){loopAdd(42,69);});
  records->addToExistingGroup("loopAdd", "10775+24502",
      [&](){loopAdd(10775, 24502);});

  records->addToExistingGroup("loopMultiply", "1*1", [&](){loopMultiply(1,1);});
  records->addToExistingGroup("loopMultiply", "42*69",
      [&](){loopMultiply(42,69);});
  records->addToExistingGroup("loopMultiply", "10775*24502",
      [&](){loopMultiply(10775, 24502);});
}

// Tables
void BM_Tables(BenchmarkRecords* records) {
  // Setup.
  auto exponentiateNTimes = [](const size_t number, const size_t n){
    size_t to_return = 1;
    for (size_t i = 0; i < n; i++) {
      to_return *= number;
    }
    return to_return;
  };

  // Measuring functions.
  records->addTable("Exponents with the given basis", {"2", "3"}, {"0", "1", "2", "3", "4"});
  records->addTable("Adding exponents", {"2^10", "2^11"}, {"2^10", "2^11"});

  for (int i = 0; i < 5; i++) {
    records->addToExistingTable("Exponents with the given basis", 0, i,
        [&](){exponentiateNTimes(2,i);});
  }
  for (int i = 0; i < 5; i++) {
    records->addToExistingTable("Exponents with the given basis", 1, i,
        [&](){exponentiateNTimes(3,i);});
  }

  for (int row = 0; row < 2; row++) {
    for (int column = 0; column < 2; column++) {
      records->addToExistingTable("Adding exponents", row, column,
          [&](){exponentiateNTimes(2, row+10)+exponentiateNTimes(2, column+10);});
    }
  }
}

BenchmarkRegister temp{{BM_SingeMeasurements, BM_Groups, BM_Tables}};
