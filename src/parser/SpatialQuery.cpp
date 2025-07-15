// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>

#include "parser/SpatialQuery.h"

#include "engine/SpatialJoinConfig.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/PayloadVariables.h"

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
  } else if (predString == "numNearestNeighbors") {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter `<numNearestNeighbors>` expects an integer (the "
          "maximum "
          "number of nearest neighbors)");
    }
    maxResults_ = object.getInt();
  } else if (predString == "maxDistance") {
    if (object.isInt()) {
      maxDist_ = static_cast<double>(object.getInt());
    } else if (object.isDouble()) {
      maxDist_ = object.getDouble();
    } else {
      throw SpatialSearchException(
          "The parameter `<maxDistance>` expects an integer or decimal (the "
          "maximum distance in meters)");
    }
  } else if (predString == "bindDistance") {
    setVariable("bindDistance", object, distanceVariable_);
  } else if (predString == "joinType") {
    if (!object.isIri()) {
      // This case is already covered in `extractParameterName` below, but we
      // want to throw a more precise error description
      throw SpatialSearchException(
          "The parameter `<joinType>` needs an IRI that selects the algorithm "
          "to employ. Currently supported are `<intersects>`, `<covers>`, "
          "`<contains>`, `<touches>`, `<crosses>`, `<overlaps>`, `<equals>`, "
          "`<within-dist>`");
    }
    auto type = extractParameterName(object, SPATIAL_SEARCH_IRI);
    if (type == "intersects") {
      joinType_ = SpatialJoinType::INTERSECTS;
    } else if (type == "covers") {
      joinType_ = SpatialJoinType::COVERS;
    } else if (type == "contains") {
      joinType_ = SpatialJoinType::CONTAINS;
    } else if (type == "touches") {
      joinType_ = SpatialJoinType::TOUCHES;
    } else if (type == "crosses") {
      joinType_ = SpatialJoinType::CROSSES;
    } else if (type == "overlaps") {
      joinType_ = SpatialJoinType::OVERLAPS;
    } else if (type == "equals") {
      joinType_ = SpatialJoinType::EQUALS;
    } else if (type == "within") {
      joinType_ = SpatialJoinType::WITHIN;
    } else if (type == "within-dist") {
      joinType_ = SpatialJoinType::WITHIN_DIST;
    } else {
      throw SpatialSearchException(
          "The IRI given for the parameter `<joinType>` does not refer to a "
          "supported join type. Currently supported are `<intersects>`, "
          "`<covers>`, `<contains>`, `<touches>`, `<crosses>`, `<overlaps>`, "
          "`<equals>`, `<within>`, `<within-dist>`");
    }
  } else if (predString == "algorithm") {
    if (!object.isIri()) {
      // This case is already covered in `extractParameterName` below, but we
      // want to throw a more precise error description
      throw SpatialSearchException(
          "The parameter `<algorithm>` needs an IRI that selects the algorithm "
          "to employ. Currently supported are `<baseline>`, `<s2>`, "
          "`<libspatialjoin>`, or `<boundingBox>`");
    }
    auto algo = extractParameterName(object, SPATIAL_SEARCH_IRI);
    if (algo == "baseline") {
      algo_ = SpatialJoinAlgorithm::BASELINE;
    } else if (algo == "s2") {
      algo_ = SpatialJoinAlgorithm::S2_GEOMETRY;
    } else if (algo == "boundingBox") {
      algo_ = SpatialJoinAlgorithm::BOUNDING_BOX;
    } else if (algo == "libspatialjoin") {
      algo_ = SpatialJoinAlgorithm::LIBSPATIALJOIN;
    } else {
      throw SpatialSearchException(
          "The IRI given for the parameter `<algorithm>` does not refer to a "
          "supported spatial search algorithm. Please select either "
          "`<baseline>`, `<s2>`, `<libspatialjoin>`, or `<boundingBox>`");
    }
  } else if (predString == "payload") {
    if (object.isVariable()) {
      // Single selected variable

      // If we have already selected all payload variables, we can ignore
      // another explicitly selected variable.
      payloadVariables_.addVariable(getVariable("payload", object));
    } else if (object.isIri() &&
               extractParameterName(object, SPATIAL_SEARCH_IRI) == "all") {
      // All variables selected
      payloadVariables_.setToAll();
    } else {
      throw SpatialSearchException(
          "The argument to the `<payload>` parameter must be either a variable "
          "to be selected or `<all>`");
    }

  } else {
    throw SpatialSearchException(absl::StrCat(
        "Unsupported argument ", predString,
        " in ppatial search; supported arguments are: `<left>`, `<right>`, "
        "`<numNearestNeighbors>`, `<maxDistance>`, `<bindDistance>`, "
        "`<joinType>`, `<payload>`, and `<algorithm>`"));
  }
}

// ____________________________________________________________________________
SpatialJoinConfiguration SpatialQuery::toSpatialJoinConfiguration() const {
  // Default algorithm
  SpatialJoinAlgorithm algo = SPATIAL_JOIN_DEFAULT_ALGORITHM;
  if (algo_.has_value()) {
    algo = algo_.value();
  }

  if (!left_.has_value()) {
    throw SpatialSearchException(
        "Missing parameter `<left>` in spatial search.");
  }

  if (algo != SpatialJoinAlgorithm::LIBSPATIALJOIN && !maxDist_.has_value() &&
      !maxResults_.has_value()) {
    throw SpatialSearchException(
        "Neither `<numNearestNeighbors>` nor `<maxDistance>` were provided but "
        "at least one of them is required for the selected algorithm");
  }

  if (!right_.has_value()) {
    throw SpatialSearchException(
        "Missing parameter `<right>` in spatial search.");
  }

  // Only if the number of results is limited, it is mandatory that the right
  // variable must be selected inside the service. If only the distance is
  // limited, it may be declared inside or outside of the service.
  if (!ignoreMissingRightChild_ && maxResults_.has_value() &&
      !childGraphPattern_.has_value()) {
    throw SpatialSearchException(
        "A spatial search with a maximum number of results must have its right "
        "variable declared inside the service using a graph pattern: SERVICE "
        "spatialSearch: { [Config Triples] { <Something> <ThatSelects> ?right "
        "} }.");
  } else if (!ignoreMissingRightChild_ && !childGraphPattern_.has_value() &&
             !payloadVariables_.isAll() && !payloadVariables_.empty()) {
    throw SpatialSearchException(
        "The right variable for the spatial search is declared outside the "
        "SERVICE, but the <payload> parameter was set. Please move the "
        "declaration of the right variable into the SERVICE if you wish to use "
        "`<payload>`");
  }

  std::optional<SpatialJoinType> joinType = std::nullopt;
  if (algo == SpatialJoinAlgorithm::LIBSPATIALJOIN) {
    // Default join type if `libspatialjoin` is selected as algorithm
    joinType = SpatialJoinType::INTERSECTS;
    if (joinType_.has_value()) {
      joinType = joinType_.value();
    }
  }

  // Payload variables
  PayloadVariables pv;
  if (!childGraphPattern_.has_value()) {
    pv = PayloadVariables::all();
  } else {
    pv = payloadVariables_;
  }

  // Task specification
  SpatialJoinTask task;
  if (algo == SpatialJoinAlgorithm::LIBSPATIALJOIN) {
    task = SpatialJoinConfig{joinType.value_or(SpatialJoinType::INTERSECTS),
                             maxDist_};
  } else if (maxResults_.has_value()) {
    task = NearestNeighborsConfig{maxResults_.value(), maxDist_};
  } else {
    task = MaxDistanceConfig{maxDist_.value()};
  }

  return SpatialJoinConfiguration{
      task, left_.value(), right_.value(), distanceVariable_,
      pv,   algo,          joinType};
}

// ____________________________________________________________________________
SpatialQuery::SpatialQuery(const SparqlTriple& triple) {
  auto predicate = triple.getSimplePredicate();
  AD_CONTRACT_CHECK(predicate.has_value(),
                    "The config triple for SpatialJoin must have a special IRI "
                    "as predicate");
  std::string_view input = predicate.value();

  // Add variables to configuration object
  AD_CONTRACT_CHECK(triple.s_.isVariable() && triple.o_.isVariable(),
                    "Currently, both the subject and the object of the triple "
                    "that specifies a spatial join have to be variables.");
  setVariable("left", triple.s_, left_);
  setVariable("right", triple.o_, right_);

  // Helper to convert a ctre match to an integer
  auto matchToInt = [](std::string_view match) -> std::optional<size_t> {
    if (match.size() == 0) {
      // We need to accept empty matches because the maximum distance argument
      // to a <nearest-neighbors:...> predicate is optional.
      return std::nullopt;
    }
    size_t res = 0;
    std::from_chars(match.data(), match.data() + match.size(), res);
    return res;
  };

  // Check if one of the regexes matches
  if (auto match = ctre::match<MAX_DIST_IN_METERS_REGEX>(input)) {
    maxDist_ = matchToInt(match.get<"dist">());
    AD_CORRECTNESS_CHECK(maxDist_.has_value());
  } else if (auto match = ctre::search<NEAREST_NEIGHBORS_REGEX>(input)) {
    maxResults_ = matchToInt(match.get<"results">());
    maxDist_ = matchToInt(match.get<"dist">());
    ignoreMissingRightChild_ = true;
    AD_CORRECTNESS_CHECK(maxResults_.has_value());
  } else {
    AD_THROW(absl::StrCat("Tried to perform spatial join with unknown triple ",
                          input,
                          ". This must be a valid spatial condition like ",
                          "`<max-distance-in-meters:50>`"));
  }
}

}  // namespace parsedQuery
