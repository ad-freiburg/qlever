// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_ENGINE_TENSORINDEXCONFIG_H
#define QLEVER_SRC_ENGINE_TENSORINDEXCONFIG_H

#include <array>
#include <cstddef>
#include <optional>
#include <string_view>
#include <variant>

#include "parser/PayloadVariables.h"
#include "rdfTypes/Variable.h"

// This header contains enums and configuration structs for the tensor search
// operation. It allows including these types without also including the whole
// class declaration of the tensor search operation.

enum class TensorDistanceAlgorithm {
  DOT_PRODUCT,
  COSINE_SIMILARITY,
  ANGULAR_DISTANCE,
  EUCLIDEAN_DISTANCE,
  MANHATTAN_DISTANCE,
  HAMMING_DISTANCE,
};
enum class TensorIndexAlgorithm { NAIVE, FAISS_IVF, FAISS_HNSW };
const TensorIndexAlgorithm TENSOR_INDEX_DEFAULT_ALGORITHM =
    TensorIndexAlgorithm::FAISS_IVF;
const TensorDistanceAlgorithm TENSOR_INDEX_DEFAULT_DISTANCE =
    TensorDistanceAlgorithm::DOT_PRODUCT;

// The configuration object that will be provided by the special SERVICE.
struct TensorIndexConfiguration {
  // The variables for the two tables to be joined
  Variable left_;
  Variable right_;

  // If given, the distance will be added to the result and be bound to this
  // variable.
  std::optional<Variable> distanceVariable_ = std::nullopt;

  // If given a vector of variables, the selected variables will be part of the
  // result table - the join column will automatically be part of the result.
  // You may use PayloadAllVariables to select all columns of the right table.
  PayloadVariables payloadVariables_ = PayloadVariables::all();

  // Choice of algorithm.
  TensorIndexAlgorithm algo_ = TENSOR_INDEX_DEFAULT_ALGORITHM;
  TensorDistanceAlgorithm dist_ = TENSOR_INDEX_DEFAULT_DISTANCE;

  size_t maxResults_ = 100;
  std::optional<size_t> searchK_ = std::nullopt;
  std::optional<size_t> kIVF_ = std::nullopt;
  std::optional<size_t> nNeighbours_ = std::nullopt;

  // Cache name for precomputed tensor search index. This is an experimental
  // parameter that is only used for the `faiss` algorithm and allows to specify
  // a precomputed index on the right side of the join. The index has to be
  // built beforehand and stored under the given name in the cache directory. If
  // this parameter is not given, the index will be built on the fly for each
  // query.
  std::optional<std::string> rightCacheName_ = std::nullopt;
};

#endif  // QLEVER_SRC_ENGINE_TENSORINDEXCONFIG_H
