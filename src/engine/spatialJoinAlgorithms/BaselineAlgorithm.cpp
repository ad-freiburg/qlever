// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2025 Jonathan Zeller github@Jonathan24680, UFR
// 2024 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/spatialJoinAlgorithms/BaselineAlgorithm.h"

#include <queue>

// ____________________________________________________________________________
Result BaselineAlgorithm::run() {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  throw std::runtime_error("not supported in C++17 mode currently");
#else
  const auto [idTableLeft, resultLeft, idTableRight, resultRight, leftJoinCol,
              rightJoinCol, leftSelectedCols, rightSelectedCols, numColumns] =
      params_;
  IdTable result{numColumns, qec_->getAllocator()};

  // cartesian product between the two tables, pairs are restricted according to
  // `maxDistance_` and `maxResults_`
  for (size_t rowLeft = 0; rowLeft < idTableLeft->size(); rowLeft++) {
    // This priority queue stores the intermediate best results if `maxResults_`
    // is used. Each intermediate result is stored as a pair of its `rowRight`
    // and distance. Since the queue will hold at most `maxResults_ + 1` items,
    // it is not a memory concern.
    auto compare = [](std::pair<size_t, double> a,
                      std::pair<size_t, double> b) {
      return a.second < b.second;
    };
    std::priority_queue<std::pair<size_t, double>,
                        std::vector<std::pair<size_t, double> >,
                        decltype(compare)>
        intermediate(compare);

    auto entryLeft = getRtreeEntry(idTableLeft, rowLeft, leftJoinCol);

    // Inner loop of cartesian product
    for (size_t rowRight = 0; rowRight < idTableRight->size(); rowRight++) {
      auto entryRight = getRtreeEntry(idTableRight, rowRight, rightJoinCol);

      if (!entryLeft || !entryRight) {
        continue;
      }

      Id dist = computeDist(entryLeft.value(), entryRight.value());

      // Ensure `maxDist_` constraint
      if (dist.getDatatype() != Datatype::Double ||
          (maxDist_.has_value() &&
           (dist.getDouble() * 1000) > maxDist_.value())) {
        continue;
      }

      // If there is no `maxResults_` we can add the result row immediately
      if (!maxResults_.has_value()) {
        addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                            rowRight, dist);
        continue;
      }

      // Ensure `maxResults_` constraint using priority queue
      intermediate.push(std::pair{rowRight, dist.getDouble()});
      // Too many results? Drop the worst one
      if (intermediate.size() > maxResults_.value()) {
        intermediate.pop();
      }
    }

    // If we are using the priority queue, we didn't add the results in the
    // inner loop, so we do it now.
    if (maxResults_.has_value()) {
      size_t numResults = intermediate.size();
      for (size_t item = 0; item < numResults; item++) {
        // Get and remove largest item from priority queue
        auto [rowRight, dist] = intermediate.top();
        intermediate.pop();

        addResultTableEntry(&result, idTableLeft, idTableRight, rowLeft,
                            rowRight, Id::makeFromDouble(dist));
      }
    }
  }
  return Result(std::move(result), std::vector<ColumnIndex>{},
                Result::getMergedLocalVocab(*resultLeft, *resultRight));
#endif
}
