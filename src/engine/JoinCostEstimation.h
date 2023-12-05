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

struct Estimates {
  std::vector<float> multiplicities_;
  uint64_t sizeEstimate_;
  ColumnIndex joinColumn_;

  size_t numDistinct(ColumnIndex  col) const {
    return std::max(
        size_t(1),
        static_cast<size_t>(sizeEstimate_ / multiplicities_.at(col)));
  }
  size_t numDistinctInJoinCol() const { return numDistinct(joinColumn_); }
  float multiplicityJoinCol() const { return multiplicities_.at(joinColumn_); }
};

inline JoinEstimates computeSizeEstimateAndMultiplicitiesForJoin(
    Estimates left,
    Estimates right,
    double corrFactor) {
  size_t nofDistinctLeft = left.numDistinctInJoinCol();
  size_t nofDistinctRight = right.numDistinctInJoinCol());
  size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

  auto adaptSize = [nofDistinctInResult](const Estimates& est, ColumnIndex colIdx) {
    return est.sizeEstimate_ *
           (static_cast<double>(nofDistinctInResult) / est.numDistinct(colIdx));
  };

  double multiplicityJoinCol = left.multiplicityJoinCol() *
                               right.multiplicityJoinCol() * corrFactor;
  multiplicityJoinCol = std::max(1.0, multiplicityJoinCol);
  auto sizeEstimate = std::max(
      size_t(1), static_cast<size_t>(multiplicityJoinCol *
                                     nofDistinctInResult);


  auto getMultiplicityJoinColOrFromSmallerTree = [corrFactor](const Estimates& estimates, ColumnIndex colIdx, const Estimates& other) {
    auto otherJoinCol = isLeft ? joinColRight : joinColLeft;
    double oldMult = estimates.multiplicities_.at(colIdx);
    return std::max(1.0, oldMult * other.multiplicityJoinCol() * corrFactor);
  };



  auto getMultiplicityNonJoinCol = [corrFactor, getNumDistinct, adaptSize,nofDistinctInResult,
                                    sizeEstimate, smallerDistinctnessTrees, &left, &right, getMultiplicityJoinColOrFromSmallerTree](const Estimates& estimates,
                                                  ColumnIndex colIdx, const Estimates& other) {
    if (colIdx = estimates.joinColumn_ || estimates.numDistinctInJoinCol() == nofDistinctInResult) {
      return getMultiplicityJoinColOrFromSmallerTree(estimates, colIdx, other);
    }
    double oldDist = estimates.numDistinct(colIdx);
    double newDist = std::min(oldDist, adaptSize(estimates, colIdx));
    return sizeEstimate / corrFactor / newDist;
  };



  return {sizeEstimate, multiplicityJoinCol, getMultiplicityNonJoinCol};
}
