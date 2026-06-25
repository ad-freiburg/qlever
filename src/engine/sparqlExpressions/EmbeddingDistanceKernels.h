// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <sebastian.walter98@gmail.com>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGDISTANCEKERNELS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGDISTANCEKERNELS_H

#include <cstddef>

#include "backports/span.h"

namespace sparqlExpression::detail {

// Raw distance kernels used by the `embf:distance` expression
// (`EmbeddingExpression.cpp`). Kept header-only and free of the metric/sign
// conventions; the metric selection, the negated-dot and `1 - cos` conventions
// and the strict validation all stay in `EmbeddingDistance.h`.
//
// `double` accumulation is used for numerical stability.
inline double dotProduct(ql::span<const float> a, ql::span<const float> b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return sum;
}

inline double squaredL2(ql::span<const float> a, ql::span<const float> b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    sum += diff * diff;
  }
  return sum;
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGDISTANCEKERNELS_H
