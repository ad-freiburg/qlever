// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <sebastian.walter98@gmail.com>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/EmbeddingTypeRegistry.h"

#include "global/Constants.h"
#include "util/Exception.h"

// _____________________________________________________________________________
std::optional<EmbeddingMetric> embeddingMetricFromString(
    std::string_view metric) {
  if (metric == EMBEDDING_METRIC_COSINE) {
    return EmbeddingMetric::Cosine;
  }
  if (metric == EMBEDDING_METRIC_L2) {
    return EmbeddingMetric::L2;
  }
  if (metric == EMBEDDING_METRIC_SQUARED_L2) {
    return EmbeddingMetric::SquaredL2;
  }
  if (metric == EMBEDDING_METRIC_DOT_PRODUCT) {
    return EmbeddingMetric::DotProduct;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
void EmbeddingTypeRegistry::addType(Id typeIri, EmbeddingTypeConfig config) {
  auto [it, inserted] = types_.try_emplace(typeIri, std::move(config));
  AD_CORRECTNESS_CHECK(inserted,
                       "An embedding type was registered more than once");
}

// _____________________________________________________________________________
const EmbeddingTypeConfig* EmbeddingTypeRegistry::getConfig(Id typeIri) const {
  auto it = types_.find(typeIri);
  return it == types_.end() ? nullptr : &it->second;
}
