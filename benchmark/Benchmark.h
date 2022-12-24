// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <functional>
#include <string>

#include "util/Timer.h"

/*
 * 
 */
class BenchmarkRecords {

  public:

    struct RecordEntry {
      std::string descriptor;
      off_t measuredTime;
    };

  private:

    std::vector<RecordEntry> _records;

  public:

  void measureTime(std::string descriptor, std::function<void(void)> functionToMeasure);

  const std::vector<RecordEntry>& getRecords() const {
    return _records;
  }

};

std::vector<std::function<void(BenchmarkRecords*)>> registerdBenchmarks;

void REGISTER_BENCHMARK(std::function<void(BenchmarkRecords*)>& benchmark) {
  registerdBenchmarks.push_back(benchmark);
}
