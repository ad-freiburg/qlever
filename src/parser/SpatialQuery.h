// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_SPATIALQUERY_H
#define QLEVER_SRC_PARSER_SPATIALQUERY_H

#include "engine/SpatialJoinConfig.h"
#include "parser/MagicServiceQuery.h"
#include "parser/PayloadVariables.h"

// If `filter` is a syntactically valid DE-9IM filter pattern (i.e. exactly 9
// characters, each one of `0`-`2`, `T`/`t`, `F`/`f`, or `*`, see
// `De9imFilterString` above), return it as a `De9imFilterString`, else
// `std::nullopt`. Note: this does not check whether the pattern can match
// disjoint geometries, see `de9imFilterCanMatchDisjoint` below for that.
std::optional<De9imFilterString> parseDe9imFilterString(
    std::string_view filter);

// Whether the given (syntactically valid) DE-9IM `filter` could match a
// disjoint pair of geometries. Patterns for which this holds (e.g.
// `*********` or the literal disjoint pattern `FF*FF****`) are unsupported:
// the pinned `libspatialjoin` never enumerates disjoint candidate pairs to
// its callback (see `Sweeper::doDE9IMCheck`), regardless of the configured
// filter, so accepting such a pattern would silently omit matching disjoint
// pairs from the result.
//
// The DE-9IM matrix entries are ordered II, IB, IE, BI, BB, BE, EI, EB, EE. A
// pair of geometries is disjoint iff II, IB, BI, and BB (indices 0, 1, 3, 4)
// are all `F`. A filter character only excludes `F` if it is a digit, `T`, or
// `t`; `*` and `F`/`f` both admit it. If all four of these positions admit
// `F`, the pattern could match a disjoint pair.
bool de9imFilterCanMatchDisjoint(const De9imFilterString& filter);

// If `filter` is a syntactically valid DE-9IM filter pattern that cannot
// match a disjoint pair of geometries, return it as a `De9imFilterString`,
// else `std::nullopt`. See `parseDe9imFilterString` and
// `de9imFilterCanMatchDisjoint` above, which this combines and which should
// be used directly if the two failure cases need to be reported separately
// (as in `SpatialQuery.cpp`).
std::optional<De9imFilterString> validateDe9imFilterString(
    std::string_view filter);

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

  // The DE-9IM filter pattern, mandatory if and only if `joinType_` is
  // `SpatialJoinType::DE9IM`.
  std::optional<De9imFilterString> de9imFilter_;

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
