//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/QueryExecutionTree.h"

// TODO<joka921> comment.

struct JoinEstimates {
  size_t sizeEstimate_;
  double multiplicityJoinColumn_;
  std::function<double(QueryExecutionTree&, ColumnIndex)> getMultiplicityNonJoinColumn_;
};

JoinEstimates computeSizeEstimateAndMultiplicitiesForJoin(
    QueryExecutionTree& left,
    QueryExecutionTree& right, ColumnIndex joinColLeft,
    ColumnIndex joinColRight, double corrFactor) {
  auto getNumDistinct = [](auto& qet, ColumnIndex col) {
    return std::max(size_t(1), static_cast<size_t>(qet.getSizeEstimate() /
                                                   qet.getMultiplicity(col)));
  };
  size_t nofDistinctLeft = getNumDistinct(left, joinColLeft);
  size_t nofDistinctRight = getNumDistinct(right, joinColRight);
  size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

  auto adaptSize = [nofDistinctInResult](auto& qet, size_t numDistinct) {
    return qet.getSizeEstimate() *
           (static_cast<double>(nofDistinctInResult) / numDistinct);
  };

  double adaptSizeLeft = adaptSize(left, nofDistinctLeft);
  double adaptSizeRight = adaptSize(right, nofDistinctRight);

  double jcMultiplicityInResult =
      left.getMultiplicity(joinColLeft) * right.getMultiplicity(joinColRight);
  auto sizeEstimate = std::max(
      size_t(1), static_cast<size_t>(corrFactor * jcMultiplicityInResult *
                                     nofDistinctInResult));

  auto getMultiplicityNonJoinCol = [corrFactor, getNumDistinct, adaptSize,
                                    sizeEstimate](auto& qet,
                                                  ColumnIndex colIdx) {
    double oldDist = getNumDistinct(qet, colIdx);
    double newDist = std::min(oldDist, adaptSize(qet, oldDist));
    return sizeEstimate / corrFactor / newDist;
  };

  double multiplicityJoinCol = left.getMultiplicity(joinColLeft) *
                               right.getMultiplicity(joinColRight) * corrFactor;
  multiplicityJoinCol = std::max(1.0, multiplicityJoinCol);

  return {sizeEstimate, multiplicityJoinCol, getMultiplicityNonJoinCol};
}
