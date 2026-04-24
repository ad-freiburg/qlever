// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
// Adapted from Spatial Join Logic

#include "parser/TensorSearchQuery.h"

#include "parser/MagicServiceIriConstants.h"
#include "parser/NormalizedString.h"
#include "parser/PayloadVariables.h"
#include "parser/SparqlTriple.h"

namespace parsedQuery {

namespace detail {
// CTRE named capture group identifiers for C++17 compatibility
constexpr ctll::fixed_string resultsCaptureGroup = "results";
}  // namespace detail

// ____________________________________________________________________________
void TensorSearchQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, TENSOR_SEARCH_IRI);

  if (predString == "left") {
    setVariable("left", object, left_);
  } else if (predString == "right") {
    setVariable("right", object, right_);
  } else if (predString == "bindDistance") {
    setVariable("bindDistance", object, distanceVariable_);
  } else if (predString == "numNN") {
    throwIf(!object.isInt(),
            "The parameter `<numNN>` expects an integer (the "
            "maximum "
            "number of nearest neighbors)");
    maxResults_ = object.getInt();
  } else if (predString == "numNeighbors") {
    throwIf(!object.isInt(),
            "The parameter `<numNeighbours>` expects an integer (the "
            "number "
            "nearest neighbors in the tree)");
    nNeighbors_ = object.getInt();
  } else if (predString == "searchK") {
    if (object.isInt()) {
      searchK_ = static_cast<size_t>(object.getInt());
    } else {
      throw TensorSearchException(
          "The parameter `<searchK>` expects an integer (the number of nearest "
          "trees to search for, which may be higher than the number of "
          "neighbors to return).");
    }
  } else if (predString == "kIVF") {
    if (object.isInt()) {
      kIVF_ = static_cast<size_t>(object.getInt());
    } else {
      throw TensorSearchException(
          "The parameter `<kIVF>` expects an integer (the number of trees to "
          "build for the index).");
    }
  } else if (predString == "algorithm") {
    // This case is already covered in `extractParameterName` below, but we
    // want to throw a more precise error description
    throwIf(
        !object.isIri(),
        "The parameter `<algorithm>` needs an IRI that selects the algorithm "
        "to employ. Currently supported are `<default>`, `<hsnw>`, and "
        "`<ivf>`");
    auto type = extractParameterName(object, TENSOR_SEARCH_IRI);
    if (type == "naive") {
      algo_ = TensorSearchAlgorithm::NAIVE;
    } else if (type == "hsnw") {
      algo_ = TensorSearchAlgorithm::FAISS_HSNW;
    } else if (type == "ivf") {
      algo_ = TensorSearchAlgorithm::FAISS_IVF;
    } else {
      throw TensorSearchException{
          "The IRI given for the parameter `<algorithm>` does not refer to a "
          "supported tensor search algorithm. Currently supported are "
          "`<default>`, `<hsnw>`, and `<ivf>`"};
    }
  } else if (predString == "distance") {
    // This case is already covered in `extractParameterName` below, but we
    // want to throw a more precise error description
    throwIf(!object.isIri(),
            "The parameter `<distance>` needs an IRI that selects the distance "
            "metric to employ. Currently supported are `<angular>`, "
            "`<cosine>`, `<dot>`, "
            "`<euclidean>`, `<manhattan>`, or  `<hamming>` ");
    auto dist = extractParameterName(object, TENSOR_SEARCH_IRI);
    if (dist == "angular") {
      dist_ = TensorDistanceAlgorithm::ANGULAR_DISTANCE;
    } else if (dist == "cosine") {
      dist_ = TensorDistanceAlgorithm::COSINE_SIMILARITY;
    } else if (dist == "dot") {
      dist_ = TensorDistanceAlgorithm::DOT_PRODUCT;
    } else if (dist == "euclidean") {
      dist_ = TensorDistanceAlgorithm::EUCLIDEAN_DISTANCE;
    } else if (dist == "manhattan") {
      dist_ = TensorDistanceAlgorithm::MANHATTAN_DISTANCE;
    } else if (dist == "hamming") {
      dist_ = TensorDistanceAlgorithm::HAMMING_DISTANCE;
    } else {
      throw TensorSearchException{
          "The IRI given for the parameter `<distance>` does not refer to a "
          "supported distance metric. Currently supported are `<angular>`, "
          "`<cosine>`, `<dot>`, "
          "`<euclidean>`, `<manhattan>`, or  `<hamming>`"};
    }
  } else if (predString == "payload") {
    if (object.isVariable()) {
      // Single selected variable

      // If we have already selected all payload variables, we can ignore
      // another explicitly selected variable.

      payloadVariables_.addVariable(getVariable("payload", object));
    } else if (object.isIri() &&
               extractParameterName(object, TENSOR_SEARCH_IRI) == "all") {
      // All variables selected
      payloadVariables_.setToAll();
    } else {
      throw TensorSearchException(
          "The argument to the `<payload>` parameter must be either a variable "
          "to be selected or `<all>`");
    }

  } else if (predString == "experimentalRightCacheName") {
    throwIf(!object.isLiteral(),
            "The argument to the `<experimentalRightCacheName>` parameter must "
            "be the name of a pinned cache entry as a string literal.");
    rightCacheName_ = asStringViewUnsafe(object.getLiteral().getContent());
  } else {
    throw TensorSearchException(absl::StrCat(
        "Unsupported argument ", predString,
        " in tensor search; supported arguments are: `<left>`, `<right>`, "
        "`<numNearestNeighbors>`, `<searchK>`, `<nTrees>`, "
        "`<algo>`, `<distance>`, `<payload>`, and "
        "`<experimentalRightCacheName>`"));
  }
}

// ____________________________________________________________________________
TensorSearchConfiguration TensorSearchQuery::toTensorSearchConfiguration()
    const {
  // Default algorithm
  TensorSearchAlgorithm algo = TENSOR_SEARCH_DEFAULT_ALGORITHM;
  if (algo_.has_value()) {
    algo = algo_.value();
  }

  throwIf(!left_.has_value(), "Missing parameter `<left>` in tensor search.");
  throwIf(!right_.has_value(), "Missing parameter `<right>` in tensor search.");

  throwIf(
      rightCacheName_.has_value() &&
          (algo != TensorSearchAlgorithm::FAISS_HSNW &&
           algo != TensorSearchAlgorithm::FAISS_IVF),
      "The parameter `<experimentalRightCacheName>` is only supported by the "
      "`<faiss>` algorithm.");
  // the cache parameter is automatically imputed for faiss
  // if (algo == TensorSearchAlgorithm::FAISS) {
  //   throwIf(!rightCacheName_.has_value(),
  //           "The parameter `<experimentalRightCacheName>` is mandatory for
  //           the "
  //           "`<faiss>` algorithm.");
  //   throwIf(childGraphPattern_.has_value(),
  //           "The parameter `<faiss>` algorithm uses a cached "
  //           "query result as its right child. Therefore a group graph pattern
  //           " "for the right side may not be specified in the `SERVICE`.");
  // }

  // Only if the number of results is limited, it is mandatory that the right
  // variable must be selected inside the service. If only the distance is
  // limited, it may be declared inside or outside of the service.
  throwIf(
      !ignoreMissingRightChild_ && maxResults_.has_value() &&
          !childGraphPattern_.has_value(),
      "A tensor search with a maximum number of results must have its right "
      "variable declared inside the service using a graph pattern: SERVICE "
      "tensorSearch: { [Config Triples] { <Something> <ThatSelects> ?right "
      "} }.");
  throwIf(
      !ignoreMissingRightChild_ && !childGraphPattern_.has_value() &&
          !payloadVariables_.isAll() && !payloadVariables_.empty(),
      "The right variable for the tensor search is declared outside the "
      "SERVICE, but the <payload> parameter was set. Please move the "
      "declaration of the right variable into the SERVICE if you wish to use "
      "`<payload>`");

  // Payload variables
  PayloadVariables pv;
  if (!childGraphPattern_.has_value()) {
    pv = PayloadVariables::all();
  } else {
    pv = payloadVariables_;
  }

  return TensorSearchConfiguration{
      left_.value(),
      right_.value(),
      distanceVariable_,
      pv,
      algo,
      dist_.value_or(TENSOR_SEARCH_DEFAULT_DISTANCE),
      maxResults_.value_or(static_cast<size_t>(100)),
      searchK_,
      kIVF_,
      nNeighbors_,
      rightCacheName_};
}

// ____________________________________________________________________________
TensorSearchQuery::TensorSearchQuery(const SparqlTriple& triple) {
  auto predicate = triple.getSimplePredicate();
  AD_CONTRACT_CHECK(
      predicate.has_value(),
      "The config triple for TensorSearch must have a special IRI "
      "as predicate");
  std::string_view input = predicate.value();

  // Add variables to configuration object
  AD_CONTRACT_CHECK(triple.s_.isVariable() && triple.o_.isVariable(),
                    "Currently, both the subject and the object of the triple "
                    "that specifies a tensor join have to be variables.");
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
  if (auto match = ctre::match<TENSOR_NEAREST_NEIGHBORS_REGEX>(input)) {
    maxResults_ = matchToInt(match.get<detail::resultsCaptureGroup>());
    ignoreMissingRightChild_ = true;
    AD_CORRECTNESS_CHECK(maxResults_.has_value());
  } else {
    AD_THROW(absl::StrCat(
        "Tried to perform tensor search with unknown triple ", input,
        ". This must be a valid tensor search condition like ",
        "`<tensor-nearest-neighbors:300>`"));
  }
}

// _____________________________________________________________________________
void TensorSearchQuery::throwIf(bool throwCondition,
                                std::string_view message) const {
  if (throwCondition) {
    throw TensorSearchException{std::string(message)};
  }
}

// _____________________________________________________________________________
void TensorSearchQuery::validate() const {
  // We convert the tensor search query to a tensor search configuration and
  // discard its result here to detect errors early and report them to the user
  // with highlighting. It's only a small struct so not much is wasted.
  [[maybe_unused]] auto&& _ = toTensorSearchConfiguration();
}

}  // namespace parsedQuery
