// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "parser/SpatialQuery.h"

#include "engine/SpatialJoin.h"
#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void SpatialQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, SPATIAL_SEARCH_IRI);

  if (predString == "left") {
    setVariable("left", object, left_);
  } else if (predString == "right") {
    setVariable("right", object, right_);
  } else if (predString == "nearestNeighbors") {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter <nearestNeighbors> expects an integer (the maximum "
          "number of nearest neighbors)");
    }
    maxResults_ = object.getInt();
  } else if (predString == "maxDistance") {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter <maxDistance> expects an integer (the maximum "
          "distance in meters)");
    }
    maxDist_ = object.getInt();
  } else if (predString == "bindDistance") {
    setVariable("bindDistance", object, distanceVariable_);
  } else if (predString == "algorithm") {
    if (!object.isIri()) {
      // This 'if' is redundant with extractParameterName, but we want to throw
      // a more precise error description
      throw SpatialSearchException(
          "The parameter <algorithm> needs an IRI that selects the algorithm "
          "to employ. Currently supported are <baseline>, <s2> or "
          "<boundingBox>.");
    }
    auto algo = extractParameterName(object, SPATIAL_SEARCH_IRI);
    if (algo == "baseline") {
      algo_ = SpatialJoinAlgorithm::BASELINE;
    } else if (algo == "s2") {
      algo_ = SpatialJoinAlgorithm::S2_GEOMETRY;
    } else if (algo == "boundingBox") {
      algo_ = SpatialJoinAlgorithm::BOUNDING_BOX;
    } else {
      throw SpatialSearchException(
          "The IRI given for the parameter <algorithm> does not refer to a "
          "supported spatial search algorithm. Please select either "
          "<baseline>, <s2> or <boundingBox>.");
    }
  } else if (predString == "payload") {
    payloadVariables_.push_back(getVariable("payload", object));
  } else {
    throw SpatialSearchException(
        "Unsupported argument " + predString +
        " in Spatial Search. Supported Arguments: <left>, <right>, "
        "<nearestNeighbors>, <maxDistance>, <bindDistance>, <payload> and "
        "<algorithm>.");
  }
}

// ____________________________________________________________________________
SpatialJoinConfiguration SpatialQuery::toSpatialJoinConfiguration() const {
  if (!left_.has_value()) {
    throw SpatialSearchException("Missing parameter <left> in spatial search.");
  } else if (!maxDist_.has_value() && !maxResults_.has_value()) {
    throw SpatialSearchException(
        "Neither <nearestNeighbors> nor <maxDistance> were provided. At least "
        "one of them is required.");
  } else if (!right_.has_value()) {
    throw SpatialSearchException(
        "Missing parameter <right> in spatial search.");
  }

  // Only if the number of results is limited, it is mandatory that the right
  // variable must be selected inside the service. If only the distance is
  // limited, it may be declared inside or outside of the service.
  if (maxResults_.has_value() && !childGraphPattern_.has_value()) {
    throw SpatialSearchException(
        "A spatial search with a maximum number of results must have its right "
        "variable declared inside the service using a graph pattern: SERVICE "
        "spatialSearch: { [Config Triples] { <Something> <ThatSelects> ?right "
        "} }.");
  } else if (!childGraphPattern_.has_value() && !payloadVariables_.empty()) {
    throw SpatialSearchException(
        "The right variable for the spatial search is declared outside the "
        "SERVICE, but the <payload> parameter was set. Please move the "
        "declaration of the right variable into the SERVICE if you wish to use "
        "<payload>.");
  }

  // Default algorithm
  SpatialJoinAlgorithm algo = SPATIAL_JOIN_DEFAULT_ALGORITHM;
  if (algo_.has_value()) {
    algo = algo_.value();
  }

  // Payload variables
  PayloadVariables pv;
  if (!childGraphPattern_.has_value()) {
    pv = PayloadAllVariables{};
  } else {
    pv = payloadVariables_;
  }

  // Task specification
  SpatialJoinTask task;
  if (maxResults_.has_value()) {
    task = NearestNeighborsConfig{maxResults_.value(), maxDist_};
  } else {
    task = MaxDistanceConfig{maxDist_.value()};
  }

  return SpatialJoinConfiguration{
      task, left_.value(), right_.value(), distanceVariable_, pv, algo};
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
                    "Currently, both the subject and the object of the triple "
                    "that specifices a spatial join have to be variables.");
  setVariable("left", triple.s_, left_);
  setVariable("right", triple.o_, right_);

  // Helper to convert a ctre match to an integer
  auto matchToInt = [](std::string_view match) {
    AD_CORRECTNESS_CHECK(match.size() > 0);
    size_t res = 0;
    std::from_chars(match.data(), match.data() + match.size(), res);
    return res;
  };

  // Extract max distance from predicate
  if (auto match = ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    maxDist_ = matchToInt(match.get<"dist">());
  } else {
    AD_THROW(absl::StrCat("Tried to perform spatial join with unknown triple ",
                          input,
                          ". This must be a valid spatial condition like ",
                          "<max-distance-in-meters:50>."));
  }
}

}  // namespace parsedQuery