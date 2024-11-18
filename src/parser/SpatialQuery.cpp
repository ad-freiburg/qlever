// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "parser/SpatialQuery.h"

namespace parsedQuery {

// ____________________________________________________________________________
void SpatialQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  if (!predicate.isIri()) {
    throw SpatialSearchException("Predicates must be IRIs");
  }

  std::string predString = predicate.getIri().toStringRepresentation();
  if (predString.ends_with("left>")) {
    setVariable("left", object, left_);
  } else if (predString.ends_with("right>")) {
    setVariable("right", object, right_);
  } else if (predString.ends_with("nearestNeighbors>")) {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter 'nearestNeighbors' expects an integer (the maximum "
          "number of nearest neighbors)");
    }
    maxResults_ = object.getInt();
  } else if (predString.ends_with("maxDistance>")) {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter 'maxDistance' expects an integer (the maximum "
          "distance in meters)");
    }
    maxDist_ = object.getInt();
  } else if (predString.ends_with("bindDistance>")) {
    setVariable("bindDistance", object, bindDist_);
  } else if (predString.ends_with("algorithm>")) {
    if (!object.isIri()) {
      throw SpatialSearchException(
          "The parameter 'algorithm' needs an IRI that selects the algorithm "
          "to employ. Currently supported are 'spatialSearch:baseline' and "
          "'spatialSearch:s2'.");
    }
    auto algoIri = object.getIri().toStringRepresentation();
    if (algoIri.ends_with("baseline>")) {
      algo_ = SpatialJoinAlgorithm::BASELINE;
    } else if (algoIri.ends_with("s2>")) {
      algo_ = SpatialJoinAlgorithm::S2_GEOMETRY;
    } else {
      throw SpatialSearchException(
          "The IRI given for the parameter 'algorithm' does not refer to a "
          "supported spatial search algorithm. Please select either "
          "'spatialSearch:baseline' or 'spatialSearch:s2'.");
    }
  } else {
    throw SpatialSearchException(
        "Unsupported argument " + predString +
        " in Spatial Search. Supported Arguments: left, right, "
        "nearestNeighbors, maxDistance, bindDistance and algorithm.");
  }
}

// ____________________________________________________________________________
SpatialJoinConfiguration SpatialQuery::toSpatialJoinConfiguration() const {
  if (!left_.has_value()) {
    throw SpatialSearchException("Missing parameter 'left' in spatial search.");
  } else if (!maxDist_.has_value() && !maxResults_.has_value()) {
    throw SpatialSearchException(
        "Neither 'nearestNeighbors' nor 'maxDistance' were provided. At least "
        "one of both is required.");
  } else if (!right_.has_value() && maxResults_.has_value()) {
    // Only if the number of results is limited, it is mandatory that the right
    // variable must be selected inside the service. If only the distance is
    // limited, it may be declared inside or outside of the service.
    throw SpatialSearchException(
        "Missing parameter 'right' in spatial search. A spatial search with "
        "a maximum number of results must have its right variable declared "
        "inside the service using a graph pattern: SERVICE spatialSearch: { "
        "... { ... ?right } }.");
  }

  // Default algorithm
  SpatialJoinAlgorithm algo = SPATIAL_JOIN_DEFAULT_ALGORITHM;
  if (algo_.has_value()) {
    algo = algo_.value();
  }

  if (maxResults_.has_value()) {
    return SpatialJoinConfiguration{
        NearestNeighborsConfig{maxResults_.value(), maxDist_}, left_.value(),
        right_.value(), bindDist_, algo};
  } else {
    return SpatialJoinConfiguration{MaxDistanceConfig{maxDist_.value()},
                                    left_.value(), right_.value(), bindDist_,
                                    algo};
  }
}

// ____________________________________________________________________________
SpatialQuery::SpatialQuery(const SparqlTriple& triple) {
  AD_CONTRACT_CHECK(triple.p_.isIri(),
                    "The config triple for SpatialJoin must have a special IRI "
                    "as predicate.");
  const std::string& input = triple.p_._iri;

  if (input.starts_with(NEAREST_NEIGHBORS)) {
    throw SpatialSearchException(
        "The special predicate <nearest-neighbors:...> is no longer supported "
        "due to confusing semantics. Please use SERVICE spatialSearch: {...} "
        "instead. For information on its usage, see the QLever Wiki.");
  }

  // Add variables to configuration object
  AD_CONTRACT_CHECK(triple.s_.isVariable() && triple.o_.isVariable(),
                    "Currently, SpatialJoin needs two variables");
  setVariable("left", triple.s_, left_);
  setVariable("right", triple.o_, right_);

  // Helper to convert a ctre match to an integer
  auto matchToInt = [](std::string_view match) -> std::optional<size_t> {
    if (match.size() > 0) {
      size_t res = 0;
      std::from_chars(match.data(), match.data() + match.size(), res);
      return res;
    }
    return std::nullopt;
  };

  // Extract max distance from predicate
  if (auto match = ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    maxDist_ = matchToInt(match.get<"dist">());
    AD_CORRECTNESS_CHECK(maxDist_.has_value());
  } else {
    AD_THROW(absl::StrCat("Tried to perform spatial join with unknown triple ",
                          input,
                          ". This must be a valid spatial condition like ",
                          "<max-distance-in-meters:50>."));
  }
}

}  // namespace parsedQuery
