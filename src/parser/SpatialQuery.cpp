// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Christoph Ullinger <ullingec@cs.uni-freiburg.de>
//          Patrick Brosi <brosi@cs.uni-freiburg.de>

#include "parser/SpatialQuery.h"

#include <absl/strings/str_join.h>

#include <type_traits>
#include <variant>

#include "engine/SpatialJoin.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/PayloadVariables.h"
#include "parser/data/Variable.h"

namespace parsedQuery {

// ____________________________________________________________________________
void SpatialQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, SPATIAL_SEARCH_IRI);

  if (predString == SJ_LEFT_VAR) {
    setVariable(SJ_LEFT_VAR, object, left_);
  } else if (predString == SJ_RIGHT_VAR) {
    setVariable(SJ_RIGHT_VAR, object, right_);
  } else if (predString == SJ_MAX_RESULTS) {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter " + esc(SJ_MAX_RESULTS) +
          " expects an integer (the maximum  number of nearest neighbors)");
    }
    maxResults_ = object.getInt();
  } else if (predString == SJ_MAX_DIST) {
    if (!object.isInt()) {
      throw SpatialSearchException(
          "The parameter " + esc(SJ_MAX_DIST) +
          " expects an integer (the maximum distance in meters)");
    }
    maxDist_ = object.getInt();
  } else if (predString == SJ_DIST_VAR) {
    setVariable(SJ_DIST_VAR, object, distanceVariable_);
  } else if (predString == SJ_JOIN_TYPE) {
    if (!object.isIri()) {
      // This case is already covered in `extractParameterName` below, but we
      // want to throw a more precise error description
      throw SpatialSearchException("The parameter " + esc(SJ_JOIN_TYPE) +
                                   " needs an IRI that selects the algorithm"
                                   " to employ. Currently supported are " +
                                   allJoinTypesAsStr());
    }
    auto type = stringToSpatialJoinType(
        extractParameterName(object, SPATIAL_SEARCH_IRI));
    if (type.has_value()) {
      joinType_ = type.value();
    } else {
      throw SpatialSearchException(
          "The IRI given for the parameter " + esc(SJ_JOIN_TYPE) +
          " does not refer to a supported join type. Currently supported are " +
          allJoinTypesAsStr());
    }
  } else if (predString == SJ_ALGORITHM) {
    if (!object.isIri()) {
      // This case is already covered in `extractParameterName` below, but we
      // want to throw a more precise error description
      throw SpatialSearchException("The parameter " + esc(SJ_ALGORITHM) +
                                   " needs an IRI that selects the algorithm "
                                   "to employ. Currently supported are " +
                                   allAlgorithmsAsStr());
    }
    auto algo = stringToSpatialJoinAlgorithm(
        extractParameterName(object, SPATIAL_SEARCH_IRI));
    if (algo.has_value()) {
      algo_ = algo.value();
    } else {
      throw SpatialSearchException(
          "The IRI given for the parameter " + esc(SJ_ALGORITHM) +
          " does not refer to a supported spatial search algorithm. Please "
          "select one of: " +
          allAlgorithmsAsStr());
    }
  } else if (predString == SJ_PAYLOAD) {
    if (object.isVariable()) {
      // Single selected variable

      // If we have already selected all payload variables, we can ignore
      // another explicitly selected variable.
      payloadVariables_.addVariable(getVariable(SJ_PAYLOAD, object));
    } else if (object.isIri() &&
               extractParameterName(object, SPATIAL_SEARCH_IRI) ==
                   SJ_PAYLOAD_ALL) {
      // All variables selected
      payloadVariables_.setToAll();
    } else {
      throw SpatialSearchException("The argument to the " + esc(SJ_PAYLOAD) +
                                   " parameter must be either a variable "
                                   "to be selected or " +
                                   esc(SJ_PAYLOAD_ALL));
    }

  } else {
    throw SpatialSearchException(
        absl::StrCat("Unsupported argument ", predString,
                     " in spatial search; supported arguments are: " +
                         allSpatialQueryArgs()));
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

  if (algo == SpatialJoinAlgorithm::LIBSPATIALJOIN && maxResults_.has_value()) {
    throw SpatialSearchException(
        "The algorithm `<libspatialjoin>` does not support the option "
        "`<numNearestNeighbors>`");
  }

  if (algo == SpatialJoinAlgorithm::LIBSPATIALJOIN &&
      joinType_ != SpatialJoinType::WITHIN_DIST && maxDist_.has_value()) {
    throw SpatialSearchException(
        "The algorithm `<libspatialjoin>` supports the "
        "`<maxDistance>` option only if `<joinType>` is set to "
        "`<within-dist>`.");
  }

  if (joinType_.has_value() && algo != SpatialJoinAlgorithm::LIBSPATIALJOIN) {
    throw SpatialSearchException(
        "The selected algorithm does not support the `<joinType>` option. Only "
        "the `<libspatialjoin>` algorithm is suitable for this option.");
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
  AD_CONTRACT_CHECK(triple.p_.isIri(),
                    "The config triple for SpatialJoin must have a special IRI "
                    "as predicate");
  const std::string& input = triple.p_.iri_;

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

// ____________________________________________________________________________
std::string SpatialQuery::esc(const std::string_view rawIri) {
  return absl::StrCat("`<", rawIri, ">`");
};

// ____________________________________________________________________________
std::string SpatialQuery::allJoinTypesAsStr() {
  std::vector<std::string> joinTypes;
  for (const auto& joinType : allSpatialJoinTypes()) {
    joinTypes.push_back(esc(spatialJoinTypeToString(joinType)));
  }
  return absl::StrJoin(joinTypes, ", ");
}

// ____________________________________________________________________________
std::string SpatialQuery::allAlgorithmsAsStr() {
  std::vector<std::string> algorithms;
  for (const auto& algorithm : allSpatialJoinAlgorithms()) {
    algorithms.push_back(esc(spatialJoinAlgorithmToString(algorithm)));
  }
  return absl::StrJoin(algorithms, ", ");
}

// ____________________________________________________________________________
std::string SpatialQuery::allSpatialQueryArgs() {
  return "`<left>`, `<right>`, "
         "`<numNearestNeighbors>`, `<maxDistance>`, `<bindDistance>`, "
         "`<joinType>`, `<payload>`, `<algorithm>`";
  // TODO
}

}  // namespace parsedQuery
