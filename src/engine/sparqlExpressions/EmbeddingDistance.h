// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <sebastian.walter98@gmail.com>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGDISTANCE_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGDISTANCE_H

#include <absl/strings/str_cat.h>

#include <cmath>
#include <optional>
#include <stdexcept>
#include <vector>

#include "backports/span.h"
#include "engine/sparqlExpressions/EmbeddingDistanceKernels.h"
#include "global/Id.h"
#include "index/EmbeddingTypeRegistry.h"

namespace sparqlExpression::detail {

// Compute the distance between two operand vectors under the type's metric, or
// throw a hard query error if an operand is not an embedding vector or has the
// wrong dimension. This is the metric policy of the `embf:distance` expression.
inline Id computeDistance(const EmbeddingTypeConfig& config,
                          const std::optional<std::vector<float>>& optA,
                          const std::optional<std::vector<float>>& optB) {
  if (!optA.has_value() || !optB.has_value()) {
    throw std::runtime_error{
        "embf:distance: an argument is not an embedding vector literal of the "
        "type's precision (emb:fp32Vector)"};
  }
  ql::span<const float> a{*optA};
  ql::span<const float> b{*optB};
  if (a.size() != config.dimension_ || b.size() != config.dimension_) {
    throw std::runtime_error{absl::StrCat(
        "embf:distance: a vector's dimension (", a.size(), " resp. ", b.size(),
        ") does not match the embedding type's emb:hasDimension (",
        config.dimension_, ")")};
  }
  double distance;
  switch (config.metric_) {
    case EmbeddingMetric::DotProduct:
      // Negated so that `ORDER BY ASC` still yields the nearest neighbours.
      distance = -dotProduct(a, b);
      break;
    case EmbeddingMetric::SquaredL2:
      distance = squaredL2(a, b);
      break;
    case EmbeddingMetric::L2:
      distance = std::sqrt(squaredL2(a, b));
      break;
    case EmbeddingMetric::Cosine: {
      double dot = dotProduct(a, b);
      double denom = std::sqrt(dotProduct(a, a)) * std::sqrt(dotProduct(b, b));
      if (denom == 0.0) {
        throw std::runtime_error{
            "embf:distance: cosine distance is undefined for a zero-norm "
            "vector"};
      }
      distance = 1.0 - dot / denom;
      break;
    }
  }
  return Id::makeFromDouble(distance);
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EMBEDDINGDISTANCE_H
