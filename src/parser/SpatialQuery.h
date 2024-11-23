// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#pragma once

#include "parser/GraphPattern.h"
#include "parser/MagicServiceQuery.h"

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
  std::optional<size_t> maxDist_;
  std::optional<size_t> maxResults_;

  // Optional further arguments
  std::optional<Variable> bindDist_;
  std::optional<SpatialJoinAlgorithm> algo_;

  SpatialQuery() = default;
  SpatialQuery(SpatialQuery&& other) noexcept = default;
  SpatialQuery(const SpatialQuery& other) noexcept = default;
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
};

}  // namespace parsedQuery
