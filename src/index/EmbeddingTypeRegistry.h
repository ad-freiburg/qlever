// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_EMBEDDINGTYPEREGISTRY_H
#define QLEVER_SRC_INDEX_EMBEDDINGTYPEREGISTRY_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "global/Id.h"
#include "util/HashMap.h"

// The similarity metric of an embedding type. Each is computed as an exact
// *distance* by `embf:distance` (smaller = closer).
enum class EmbeddingMetric {
  Cosine,      // `1 − dot/(‖a‖·‖b‖)`
  L2,          // true Euclidean distance `√Σ(aᵢ−bᵢ)²`
  SquaredL2,   // `Σ(aᵢ−bᵢ)²` (no `√`; same kNN ranking as L2)
  DotProduct,  // `−dot` (negated so `ORDER BY ASC` still yields nearest)
};

// The element precision of an embedding type's vectors. The MVP supports only
// `Fp32` (matching the `emb:fp32Vector` datatype); further precisions
// (`Fp16`, a binary type, ...) are future additions.
enum class EmbeddingPrecision {
  Fp32,
};

// Map an `emb:hasMetric` IRI (its `<...>` string representation, e.g.
// `<...embeddings/cosine>`) to its `EmbeddingMetric`, or `std::nullopt` if it
// is not one of the (MVP-)supported metrics. The single place that knows the
// metric vocabulary (used by the load-time validation).
std::optional<EmbeddingMetric> embeddingMetricFromIri(std::string_view iri);

// Map an `emb:hasPrecision` IRI (its `<...>` string representation, e.g.
// `<...embeddings/fp32>`) to its `EmbeddingPrecision`, or `std::nullopt` if it
// is not one of the (MVP-)supported precisions. The single place that knows the
// precision vocabulary (used by the load-time validation).
std::optional<EmbeddingPrecision> embeddingPrecisionFromIri(
    std::string_view iri);

// The validated metadata of a single embedding type. All fields are mandatory;
// the `EmbeddingTypeRegistry` only ever holds configs that passed strict
// validation at index load time. For now each embedding instance is assumed to
// have exactly one vector, of the type's `precision_`.
struct EmbeddingTypeConfig {
  // The vector dimension every operand of `embf:distance` must have.
  uint64_t dimension_;
  // The element precision (already validated/parsed from `emb:hasPrecision`;
  // MVP: always `Fp32`).
  EmbeddingPrecision precision_;
  // The similarity metric (already validated/parsed from `emb:hasMetric`).
  EmbeddingMetric metric_;

  bool operator==(const EmbeddingTypeConfig&) const = default;
};

// An in-memory map `embedding-type IRI → EmbeddingTypeConfig`, built at index
// **load** time (not persisted) by scanning the `emb:type` declarations and the
// per-type `emb:has*` metadata from the loaded permutations. The query-side
// `embf:distance` function resolves a type IRI to its config through this
// registry. The type IRI is keyed by its `VocabIndex` `Id` so the expression
// can look it up cheaply from a constant operand.
class EmbeddingTypeRegistry {
 private:
  ad_utility::HashMap<Id, EmbeddingTypeConfig> types_;

 public:
  // Register a validated type. The `typeIri` is the `Id` of the embedding-type
  // IRI (the object of an `emb:type` triple). Throws if the same type is added
  // twice (a builder invariant, not user-facing).
  void addType(Id typeIri, EmbeddingTypeConfig config);

  // Look up the config for a type IRI `Id`, or `nullptr` if it is not a
  // declared embedding type. Used by the `embf:distance(a, b, type)`
  // expression.
  const EmbeddingTypeConfig* getConfig(Id typeIri) const;

  // The number of declared embedding types.
  size_t numTypes() const { return types_.size(); }

  bool empty() const { return types_.empty(); }
};

#endif  // QLEVER_SRC_INDEX_EMBEDDINGTYPEREGISTRY_H
