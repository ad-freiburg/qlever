// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <algorithm>
#include <chrono>
#include <iostream>
#include <vector>

#include "engine/BranchlessStreamCompactor.h"
#include "global/Id.h"

using namespace ql::engine::vector;

template <typename T>
inline void escape(T&& val) {
#if defined(__GNUC__) || defined(__clang__)
  asm volatile("" : "+r,m"(val) : : "memory");
#endif
}

int main() {
  constexpr size_t NUM_REPS = 5;

  // =========================================================================
  // Benchmark 1: Single-Column Filter Selection (50,000,000 rows x 5 reps)
  // =========================================================================
  {
    constexpr size_t NUM_ROWS = 50'000'000;
    std::cout << "============================================================="
                 "====\n";
    std::cout << "Microbenchmark: Single-Column Filter Compaction (" << NUM_ROWS
              << " rows x " << NUM_REPS << " reps, 50% selectivity)\n";
    std::cout << "============================================================="
                 "====\n";

    std::vector<Id> input(NUM_ROWS);
    for (size_t i = 0; i < NUM_ROWS; ++i) {
      input[i] = Id::makeFromInt(static_cast<int>(i));
    }
    std::vector<Id> output(NUM_ROWS);

    std::vector<double> scalarTimes;
    std::vector<double> vectorTimes;

    for (size_t rep = 0; rep < NUM_REPS; ++rep) {
      // Scalar filter
      auto t0 = std::chrono::high_resolution_clock::now();
      size_t scalarCount = 0;
      for (size_t i = 0; i < NUM_ROWS; ++i) {
        if (input[i].getInt() % 2 == 0) {
          output[scalarCount++] = input[i];
          escape(output[scalarCount - 1]);
        }
      }
      auto t1 = std::chrono::high_resolution_clock::now();
      scalarTimes.push_back(
          std::chrono::duration<double, std::milli>(t1 - t0).count());

      // Branchless Stream Compactor
      auto t2 = std::chrono::high_resolution_clock::now();
      size_t vectorCount = BranchlessStreamCompactor::compact(
          input, output, [](Id id) { return id.getInt() % 2 == 0; });
      escape(vectorCount);
      auto t3 = std::chrono::high_resolution_clock::now();
      vectorTimes.push_back(
          std::chrono::duration<double, std::milli>(t3 - t2).count());
    }

    std::sort(scalarTimes.begin(), scalarTimes.end());
    std::sort(vectorTimes.begin(), vectorTimes.end());

    double medScalar = scalarTimes[NUM_REPS / 2];
    double medVector = vectorTimes[NUM_REPS / 2];

    std::cout << "Scalar Filter Latency (Median):  " << medScalar << " ms ("
              << (NUM_ROWS / (medScalar / 1000.0)) / 1e6 << " M rows/sec)\n";
    std::cout << "Branchless Compactor Latency (Median): " << medVector
              << " ms (" << (NUM_ROWS / (medVector / 1000.0)) / 1e6
              << " M rows/sec)\n";
    std::cout << "Speedup (Median):                " << (medScalar / medVector)
              << "x\n\n";
  }

  // =========================================================================
  // Benchmark 2: Multi-Column Tabular Compaction (20,000,000 rows x 3 columns x
  // 5 reps)
  // =========================================================================
  {
    constexpr size_t NUM_ROWS = 20'000'000;
    std::cout << "============================================================="
                 "====\n";
    std::cout << "Macrobenchmark: 3-Column Tabular Compaction (" << NUM_ROWS
              << " rows x 3 cols x " << NUM_REPS << " reps)\n";
    std::cout << "============================================================="
                 "====\n";

    std::vector<Id> col0(NUM_ROWS), col1(NUM_ROWS), col2(NUM_ROWS);
    std::vector<Id> out0(NUM_ROWS), out1(NUM_ROWS), out2(NUM_ROWS);

    for (size_t i = 0; i < NUM_ROWS; ++i) {
      col0[i] = Id::makeFromInt(static_cast<int>(i));
      col1[i] = Id::makeFromInt(static_cast<int>(i * 3));
      col2[i] = Id::makeFromInt(static_cast<int>(i * 7 + 1));
    }

    std::vector<double> scalarTimes;
    std::vector<double> vectorTimes;

    for (size_t rep = 0; rep < NUM_REPS; ++rep) {
      auto t0 = std::chrono::high_resolution_clock::now();
      // Compact each column independently with its own predicate, mirroring
      // the vector path below so both sides measure the same work.
      size_t c0 = 0, c1 = 0, c2 = 0;
      for (size_t i = 0; i < NUM_ROWS; ++i) {
        if (col0[i].getInt() % 2 == 0) {
          out0[c0++] = col0[i];
        }
      }
      for (size_t i = 0; i < NUM_ROWS; ++i) {
        if (col1[i].getInt() % 2 == 0) {
          out1[c1++] = col1[i];
        }
      }
      for (size_t i = 0; i < NUM_ROWS; ++i) {
        if (col2[i].getInt() % 2 == 0) {
          out2[c2++] = col2[i];
        }
      }
      escape(out0[c0 > 0 ? c0 - 1 : 0]);
      escape(out1[c1 > 0 ? c1 - 1 : 0]);
      escape(out2[c2 > 0 ? c2 - 1 : 0]);
      auto t1 = std::chrono::high_resolution_clock::now();
      scalarTimes.push_back(
          std::chrono::duration<double, std::milli>(t1 - t0).count());

      auto t2 = std::chrono::high_resolution_clock::now();
      size_t countVec = BranchlessStreamCompactor::compact(
          col0, out0, [](Id id) { return id.getInt() % 2 == 0; });
      BranchlessStreamCompactor::compact(
          col1, out1, [](Id id) { return id.getInt() % 2 == 0; });
      BranchlessStreamCompactor::compact(
          col2, out2, [](Id id) { return id.getInt() % 2 == 0; });
      escape(countVec);
      auto t3 = std::chrono::high_resolution_clock::now();
      vectorTimes.push_back(
          std::chrono::duration<double, std::milli>(t3 - t2).count());
    }

    std::sort(scalarTimes.begin(), scalarTimes.end());
    std::sort(vectorTimes.begin(), vectorTimes.end());

    double medScalar = scalarTimes[NUM_REPS / 2];
    double medVector = vectorTimes[NUM_REPS / 2];

    std::cout << "Scalar 3-Col Latency (Median):   " << medScalar << " ms ("
              << (NUM_ROWS / (medScalar / 1000.0)) / 1e6 << " M rows/sec)\n";
    std::cout << "Branchless 3-Col Latency (Median):     " << medVector
              << " ms (" << (NUM_ROWS / (medVector / 1000.0)) / 1e6
              << " M rows/sec)\n";
    std::cout << "Speedup (Median):                " << (medScalar / medVector)
              << "x\n";
    std::cout << "============================================================="
                 "====\n";
  }

  return 0;
}
