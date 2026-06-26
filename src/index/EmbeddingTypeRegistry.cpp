// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/EmbeddingTypeRegistry.h"

#include "global/Constants.h"
#include "util/Exception.h"

// _____________________________________________________________________________
std::optional<EmbeddingMetric> embeddingMetricFromIri(std::string_view iri) {
  if (iri == EMBEDDING_METRIC_COSINE_IRI) {
    return EmbeddingMetric::Cosine;
  }
  if (iri == EMBEDDING_METRIC_L2_IRI) {
    return EmbeddingMetric::L2;
  }
  if (iri == EMBEDDING_METRIC_SQUARED_L2_IRI) {
    return EmbeddingMetric::SquaredL2;
  }
  if (iri == EMBEDDING_METRIC_DOT_PRODUCT_IRI) {
    return EmbeddingMetric::DotProduct;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<EmbeddingPrecision> embeddingPrecisionFromIri(
    std::string_view iri) {
  if (iri == EMBEDDING_PRECISION_FP32_IRI) {
    return EmbeddingPrecision::Fp32;
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
