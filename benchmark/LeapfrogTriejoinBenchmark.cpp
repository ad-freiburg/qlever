// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <chrono>
#include <iostream>
#include <vector>

#include "engine/LeapfrogTriejoin.h"
#include "global/Id.h"

using namespace ql::engine::wcoj;

int main() {
  constexpr size_t N = 2'000'000;
  std::cout
      << "=================================================================\n";
  std::cout << "Comparative Benchmark: Pairwise Binary Join vs Leapfrog "
               "Triejoin (WCOJ)\n";
  std::cout << "3 sorted streams of " << N << " elements\n";
  std::cout
      << "=================================================================\n";

  std::vector<Id> listA;
  std::vector<Id> listB;
  std::vector<Id> listC;

  listA.reserve(N);
  listB.reserve(N);
  listC.reserve(N);

  for (size_t i = 0; i < N; ++i) {
    listA.push_back(Id::makeFromInt(static_cast<int>(i * 2)));
    listB.push_back(Id::makeFromInt(static_cast<int>(i * 3)));
    listC.push_back(Id::makeFromInt(static_cast<int>(i * 6)));
  }

  // 1. BASELINE: Pairwise Binary Merge Join (A ⋈ B -> Temp ⋈ C)
  auto b0 = std::chrono::high_resolution_clock::now();
  // Step 1: Intersect A and B into intermediate vector
  std::vector<Id> intermediateAB;
  size_t iA = 0, iB = 0;
  while (iA < listA.size() && iB < listB.size()) {
    if (listA[iA] == listB[iB]) {
      intermediateAB.push_back(listA[iA]);
      iA++;
      iB++;
    } else if (listA[iA] < listB[iB]) {
      iA++;
    } else {
      iB++;
    }
  }
  // Step 2: Intersect intermediate with C
  std::vector<Id> binaryResult;
  size_t iAB = 0, iC = 0;
  while (iAB < intermediateAB.size() && iC < listC.size()) {
    if (intermediateAB[iAB] == listC[iC]) {
      binaryResult.push_back(intermediateAB[iAB]);
      iAB++;
      iC++;
    } else if (intermediateAB[iAB] < listC[iC]) {
      iAB++;
    } else {
      iC++;
    }
  }
  auto b1 = std::chrono::high_resolution_clock::now();
  double binaryMs = std::chrono::duration<double, std::milli>(b1 - b0).count();

  // 2. PROTOTYPE: Leapfrog Triejoin (WCOJ simultaneous 3-way intersection)
  auto p0 = std::chrono::high_resolution_clock::now();
  std::vector<LeapfrogIterator> iterators;
  iterators.emplace_back(listA);
  iterators.emplace_back(listB);
  iterators.emplace_back(listC);

  auto leapfrogResult = LeapfrogJoin::intersect(iterators);
  auto p1 = std::chrono::high_resolution_clock::now();
  double leapfrogMs =
      std::chrono::duration<double, std::milli>(p1 - p0).count();

  std::cout << "\n--- Baseline: Pairwise Binary Merge Join (Materializes "
               "Intermediate) ---\n";
  std::cout << "Runtime: " << binaryMs << " ms ("
            << (N / (binaryMs / 1000.0)) / 1e6
            << " M items/sec, intermediate size: " << intermediateAB.size()
            << " rows, matches: " << binaryResult.size() << ")\n";
  if (binaryResult.size() != leapfrogResult.size()) {
    std::cerr << "MISMATCH: baseline matches (" << binaryResult.size()
              << ") != leapfrog matches (" << leapfrogResult.size() << ")\n";
    return 1;
  }

  std::cout << "\n--- Prototype: Leapfrog Triejoin (No Intermediate "
               "Table) ---\n";
  std::cout << "Runtime: " << leapfrogMs << " ms ("
            << (N / (leapfrogMs / 1000.0)) / 1e6
            << " M items/sec, matches: " << leapfrogResult.size() << ")\n";

  std::cout << "\n============================================================="
               "====\n";
  std::cout << ">>> Leapfrog WCOJ Speedup: " << (binaryMs / leapfrogMs)
            << "x faster\n";
  std::cout << ">>> Intermediate Memory Saved: "
            << (intermediateAB.size() * sizeof(Id)) / (1024 * 1024) << " MB\n";
  std::cout
      << "=================================================================\n";

  return 0;
}
