// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_SPATIALQUERY_H
#define QLEVER_SRC_PARSER_SPATIALQUERY_H

#include "engine/SpatialJoinConfig.h"
#include "parser/MagicServiceQuery.h"
#include "parser/PayloadVariables.h"

namespace parsedQuery {

class SpatialSearchException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

// Spatial Search feature via SERVICE. This struct holds intermediate or
// incomplete configuration during the parsing process.
struct SpatialQuery : MagicServiceQuery {
  // Required after everything has been added: the left and right join
  // variables.
  std::optional<Variable> left_;
  std::optional<Variable> right_;

  // The spatial join task definition: maximum distance and number of results.
  // One of both - or both - must be provided.
  std::optional<double> maxDist_;
  std::optional<size_t> maxResults_;

  // Optional further argument: a variable to which the distance between spatial
  // search results is bound
  std::optional<Variable> distanceVariable_;

  // A list of variables from the right join table that shall be part of the
  // result table. If empty, only the join column - given by `right_` - will
  // be in the result together with the all left columns and (optionally) the
  // distance variable. If the `right_` variable is selected outside of the
  // SERVICE statement (which is allowed for only maxDist spatial joins), than
  // this vector is required to be empty - the user may not specify the payload
  // configuration parameter. It will then be automatically set to
  // `PayloadAllVariables` to ensure appropriate semantics.
  PayloadVariables payloadVariables_;

  // Optional further argument: the join algorithm. If it is not given, the
  // default algorithm is used implicitly.
  std::optional<SpatialJoinAlgorithm> algo_;

  // Optional join type for libspatialjoin. If it is not given, INTERSECT
  // is used implicitly.
  std::optional<SpatialJoinType> joinType_;

  // If the s2-point-polyline algorithm is used, the right side of the spatial
  // join will be an already existing s2 index together with the fully
  // materialized child result table. Both are pinned to the named query cache.
  // This parameter indicates the name of the cache entry to be used.
  std::optional<std::string> rightCacheName_;

  // Helper: if the spatial query was constructed from a special triple
  // <nearest-neighbors:...> for backward compatibility, we need to bypass the
  // check for the case of a nearest neighbors search with the right child not
  // declared inside the service (despite confusing semantics).
  bool ignoreMissingRightChild_ = false;

  SpatialQuery() = default;
  SpatialQuery(SpatialQuery&& other) noexcept = default;
  SpatialQuery(const SpatialQuery& other) = default;
  SpatialQuery& operator=(const SpatialQuery& other) = default;
  SpatialQuery& operator=(SpatialQuery&& a) noexcept = default;
  ~SpatialQuery() noexcept override = default;

  // Alternative constructor for backward compatibility (allows initializing a
  // SpatialJoin using a magic predicate)
  explicit SpatialQuery(const SparqlTriple& triple);

  // See MagicServiceQuery
  void addParameter(const SparqlTriple& triple) override;

  // Convert this SpatialQuery to a proper SpatialJoinConfiguration. This will
  // check if all required values have been provided and otherwise throw.
  SpatialJoinConfiguration toSpatialJoinConfiguration() const;

  // Throw if the current configuration is invalid.
  void validate() const override;

  constexpr std::string_view name() const override { return "spatial join"; };

 private:
  // If `throwCondition` is `true`, throw `SpatialSearchException{message}`.
  void throwIf(bool throwCondition, std::string_view message) const;
};

namespace detail {

// Convert a string like `libspatialjoin` to the corresponding enum element.
// Throws a `SpatialSearchException` for invalid inputs.
SpatialJoinAlgorithm spatialJoinAlgorithmFromString(
    std::string_view identifier);

}  // namespace detail

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_SPATIALQUERY_H
