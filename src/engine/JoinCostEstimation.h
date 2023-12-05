//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/QueryExecutionTree.h"

// TODO<joka921> comment.

struct JoinEstimates {
  size_t sizeEstimate_;
  std::vector<double> multiplicitesLeft_;
  std::vector<double> multiplicitesRight_;
};

struct Estimates {
  std::vector<double> multiplicities_;
  uint64_t sizeEstimate_;
  ColumnIndex joinColumn_;

  size_t numDistinct(ColumnIndex col) const {
    return std::max(size_t(1), static_cast<size_t>(sizeEstimate_ /
                                                   multiplicities_.at(col)));
  }
  size_t numDistinctInJoinCol() const { return numDistinct(joinColumn_); }
  float multiplicityJoinCol() const { return multiplicities_.at(joinColumn_); }
};

inline Estimates estimatesFromQet(QueryExecutionTree& qet,
                                  ColumnIndex joinCol) {
  std::vector<double> multiplicites;
  for (size_t i : ad_utility::integerRange(qet.getResultWidth())) {
    multiplicites.push_back(qet.getMultiplicity(i));
  }
  return {std::move(multiplicites), qet.getSizeEstimate(), joinCol};
}

inline JoinEstimates computeSizeEstimateAndMultiplicitiesForJoin(
    const Estimates& left, const Estimates& right, double corrFactor) {
  size_t nofDistinctInResult =
      std::min(left.numDistinctInJoinCol(), right.numDistinctInJoinCol());

  auto adaptSize = [nofDistinctInResult](const Estimates& est,
                                         ColumnIndex colIdx) {
    return est.sizeEstimate_ *
           (static_cast<double>(nofDistinctInResult) / est.numDistinct(colIdx));
  };

  double multiplicityJoinCol =
      left.multiplicityJoinCol() * right.multiplicityJoinCol() * corrFactor;
  multiplicityJoinCol = std::max(1.0, multiplicityJoinCol);
  auto sizeEstimate =
      std::max(size_t(1),
               static_cast<size_t>(multiplicityJoinCol * nofDistinctInResult));

  auto getMultiplicityJoinColOrFromSmallerTree = [corrFactor](
                                                     const Estimates& estimates,
                                                     ColumnIndex colIdx,
                                                     const Estimates& other) {
    double oldMult = estimates.multiplicities_.at(colIdx);
    return std::max(1.0, oldMult * other.multiplicityJoinCol() * corrFactor);
  };

  auto getMultiplicityForCol = [corrFactor, adaptSize, nofDistinctInResult,
                                sizeEstimate,
                                getMultiplicityJoinColOrFromSmallerTree](
                                   const Estimates& estimates,
                                   ColumnIndex colIdx, const Estimates& other) {
    if (colIdx == estimates.joinColumn_ ||
        estimates.numDistinctInJoinCol() == nofDistinctInResult) {
      return getMultiplicityJoinColOrFromSmallerTree(estimates, colIdx, other);
    }
    double oldDist = estimates.numDistinct(colIdx);
    double newDist = std::min(oldDist, adaptSize(estimates, colIdx));
    return sizeEstimate / corrFactor / newDist;
  };

  auto getAllMultiplicities = [&getMultiplicityForCol](
                                  const Estimates& estimates,
                                  const Estimates& other) {
    std::vector<double> result;
    for (size_t i :
         ad_utility::integerRange(estimates.multiplicities_.size())) {
      result.push_back(getMultiplicityForCol(estimates, i, other));
    }
    return result;
  };

  return {sizeEstimate, getAllMultiplicities(left, right),
          getAllMultiplicities(right, left)};
}
