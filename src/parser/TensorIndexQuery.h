// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

#ifndef QLEVER_SRC_PARSER_TENSORINDEXQUERY_H
#define QLEVER_SRC_PARSER_TENSORINDEXQUERY_H

#include "engine/TensorIndexConfig.h"
#include "parser/MagicServiceQuery.h"
#include "parser/PayloadVariables.h"

namespace parsedQuery {

class TensorIndexException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// Tensor Search feature via SERVICE. This struct holds intermediate or
// incomplete configuration during the parsing process.
struct TensorIndexQuery : MagicServiceQuery {
  // Required after everything has been added: the left and right join
  // variables.
  std::optional<Variable> left_;
  std::optional<Variable> right_;

  // The tensor search task definition: number of results, and search_k (defaults to -1).
  // One of both - or both - must be provided.
  std::optional<size_t> maxResults_;
  std::optional<size_t> searchK_;
  std::optional<size_t> kIVF_;
  std::optional<size_t> nNeighbors_;

  // A list of variables from the right join table that shall be part of the
  // result table. If empty, only the join column - given by `right_` - will
  // be in the result together with the all left columns and (optionally) the
  // distance variable. If the `right_` variable is selected outside of the
  // SERVICE statement (which is allowed for only maxDist spatial joins), than
  // this vector is required to be empty - the user may not specify the payload
  // configuration parameter. It will then be automatically set to
  // `PayloadAllVariables` to ensure appropriate semantics.
  PayloadVariables payloadVariables_;

  // Optional further argument: a variable to which the number of results is bound.
  std::optional<Variable> distanceVariable_;

  // Optional further argument: the join algorithm. If it is not given, the
  // default algorithm is used implicitly.
  std::optional<TensorIndexAlgorithm> algo_;
  std::optional<TensorDistanceAlgorithm> dist_;

  // This parameter indicates the name of the cache entry to be used.
  std::optional<std::string> rightCacheName_;

  // Helper: if the spatial query was constructed from a special triple
  // <nearest-neighbors:...> for backward compatibility, we need to bypass the
  // check for the case of a nearest neighbors search with the right child not
  // declared inside the service (despite confusing semantics).
  bool ignoreMissingRightChild_ = false;

  TensorIndexQuery() = default;
  TensorIndexQuery(TensorIndexQuery&& other) noexcept = default;
  TensorIndexQuery(const TensorIndexQuery& other) = default;
  TensorIndexQuery& operator=(const TensorIndexQuery& other) = default;
  TensorIndexQuery& operator=(TensorIndexQuery&& a) noexcept = default;
  ~TensorIndexQuery() noexcept override = default;

  // Alternative constructor for backward compatibility (allows initializing a
  // TensorIndexQuery using a magic predicate)
  explicit TensorIndexQuery(const SparqlTriple& triple);

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  // Convert this TensorIndexQuery to a proper TensorJoinConfiguration. This will
  // check if all required values have been provided and otherwise throw.
  TensorIndexConfiguration toTensorIndexConfiguration() const;

  // Throw if the current configuration is invalid.
  void validate() const override;

  constexpr std::string_view name() const override { return "tensor search"; };

 private:
  // If `throwCondition` is `true`, throw `TensorIndexException{message}`.
  void throwIf(bool throwCondition, std::string_view message) const;
};


}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_TENSORINDEXQUERY_H
