// Copyright 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "parser/GraphPatternOperation.h"

#include <optional>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "engine/SpatialJoin.h"
#include "parser/ParsedQuery.h"
#include "parser/TripleComponent.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace parsedQuery {

// _____________________________________________________________________________
std::string SparqlValues::variablesToString() const {
  return absl::StrJoin(_variables, "\t", Variable::AbslFormatter);
}

// _____________________________________________________________________________
std::string SparqlValues::valuesToString() const {
  auto tripleComponentVectorAsValueTuple =
      [](const std::vector<TripleComponent>& v) -> std::string {
    return absl::StrCat(
        "(",
        absl::StrJoin(v, " ",
                      [](std::string* out, const TripleComponent& tc) {
                        out->append(tc.toString());
                      }),
        ")");
  };
  return absl::StrJoin(
      _values, " ",
      [&](std::string* out, const std::vector<TripleComponent>& v) {
        out->append(tripleComponentVectorAsValueTuple(v));
      });
}

// Small anonymous helper function that is used in the definition of the member
// functions of the `Subquery` class.
namespace {
auto m(auto&&... args) {
  return std::make_unique<ParsedQuery>(AD_FWD(args)...);
}
}  // namespace

// Special member functions for the `Subquery` class
Subquery::Subquery() : _subquery{m()} {}
Subquery::Subquery(const ParsedQuery& pq) : _subquery{m(pq)} {}
Subquery::Subquery(ParsedQuery&& pq) : _subquery{m(std::move(pq))} {}
Subquery::Subquery(Subquery&& pq) : _subquery{m(std::move(pq.get()))} {}
Subquery::Subquery(const Subquery& pq) : _subquery{m(pq.get())} {}
Subquery& Subquery::operator=(const Subquery& pq) {
  _subquery = m(pq.get());
  return *this;
}
Subquery& Subquery::operator=(Subquery&& pq) {
  _subquery = m(std::move(pq.get()));
  return *this;
}
Subquery::~Subquery() = default;
ParsedQuery& Subquery::get() { return *_subquery; }
const ParsedQuery& Subquery::get() const { return *_subquery; }

// _____________________________________________________________________________
void BasicGraphPattern::appendTriples(BasicGraphPattern other) {
  ad_utility::appendVector(_triples, std::move(other._triples));
}

// ____________________________________________________________________________
void PathQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  if (!predicate.isIri()) {
    throw PathSearchException("Predicates must be IRIs");
  }

  std::string predString = predicate.getIri().toStringRepresentation();
  if (predString.ends_with("source>")) {
    sources_.push_back(std::move(object));
  } else if (predString.ends_with("target>")) {
    targets_.push_back(std::move(object));
  } else if (predString.ends_with("start>")) {
    setVariable("start", object, start_);
  } else if (predString.ends_with("end>")) {
    setVariable("end", object, end_);
  } else if (predString.ends_with("pathColumn>")) {
    setVariable("pathColumn", object, pathColumn_);
  } else if (predString.ends_with("edgeColumn>")) {
    setVariable("edgeColumn", object, edgeColumn_);
  } else if (predString.ends_with("edgeProperty>")) {
    edgeProperties_.push_back(getVariable("edgeProperty", object));
  } else if (predString.ends_with("cartesian>")) {
    if (!object.isBool()) {
      throw PathSearchException("The parameter 'cartesian' expects a boolean");
    }
    cartesian_ = object.getBool();
  } else if (predString.ends_with("numPathsPerTarget>")) {
    if (!object.isInt()) {
      throw PathSearchException(
          "The parameter 'numPathsPerTarget' expects an integer");
    }
    numPathsPerTarget_ = object.getInt();
  } else if (predString.ends_with("algorithm>")) {
    if (!object.isIri()) {
      throw PathSearchException("The 'algorithm' value has to be an Iri");
    }
    auto objString = object.getIri().toStringRepresentation();

    if (objString.ends_with("allPaths>")) {
      algorithm_ = PathSearchAlgorithm::ALL_PATHS;
    } else {
      throw PathSearchException(
          "Unsupported algorithm in pathSearch: " + objString +
          ". Supported Algorithms: "
          "allPaths.");
    }
  } else {
    throw PathSearchException(
        "Unsupported argument " + predString +
        " in PathSearch. "
        "Supported Arguments: source, target, start, end, "
        "pathColumn, edgeColumn, "
        "edgeProperty, algorithm.");
  }
}

// ____________________________________________________________________________
std::variant<Variable, std::vector<Id>> PathQuery::toSearchSide(
    std::vector<TripleComponent> side, const Index::Vocab& vocab) const {
  if (side.size() == 1 && side[0].isVariable()) {
    return side[0].getVariable();
  } else {
    std::vector<Id> sideIds;
    for (const auto& comp : side) {
      if (comp.isVariable()) {
        throw PathSearchException(
            "Only one variable is allowed per search side");
      }
      auto opt = comp.toValueId(vocab);
      if (opt.has_value()) {
        sideIds.push_back(opt.value());
      } else {
        throw PathSearchException("No vocabulary entry for " + comp.toString());
      }
    }
    return sideIds;
  }
}

// ____________________________________________________________________________
void MagicServiceQuery::addBasicPattern(const BasicGraphPattern& pattern) {
  for (SparqlTriple triple : pattern._triples) {
    addParameter(triple);
  }
}

// ____________________________________________________________________________
void MagicServiceQuery::addGraph(const GraphPatternOperation& op) {
  if (childGraphPattern_._graphPatterns.empty()) {
    auto pattern = std::get<parsedQuery::GroupGraphPattern>(op);
    childGraphPattern_ = std::move(pattern._child);
  }
}

// ____________________________________________________________________________
PathSearchConfiguration PathQuery::toPathSearchConfiguration(
    const Index::Vocab& vocab) const {
  auto sources = toSearchSide(sources_, vocab);
  auto targets = toSearchSide(targets_, vocab);

  if (!start_.has_value()) {
    throw PathSearchException("Missing parameter 'start' in path search.");
  } else if (!end_.has_value()) {
    throw PathSearchException("Missing parameter 'end' in path search.");
  } else if (!pathColumn_.has_value()) {
    throw PathSearchException("Missing parameter 'pathColumn' in path search.");
  } else if (!edgeColumn_.has_value()) {
    throw PathSearchException("Missing parameter 'edgeColumn' in path search.");
  }

  return PathSearchConfiguration{
      algorithm_,          sources,         targets,
      start_.value(),      end_.value(),    pathColumn_.value(),
      edgeColumn_.value(), edgeProperties_, cartesian_,
      numPathsPerTarget_};
}

// ____________________________________________________________________________
Variable MagicServiceQuery::getVariable(std::string_view parameter,
                                        const TripleComponent& object) {
  if (!object.isVariable()) {
    throw MagicServiceException(absl::StrCat("The value ", object.toString(),
                                             " for parameter '", parameter,
                                             "' has to be a variable"));
  }

  return object.getVariable();
};

// ____________________________________________________________________________
void MagicServiceQuery::setVariable(std::string_view parameter,
                                    const TripleComponent& object,
                                    std::optional<Variable>& existingValue) {
  auto variable = getVariable(parameter, object);

  if (existingValue.has_value()) {
    throw MagicServiceException(absl::StrCat(
        "The parameter '", parameter, "' has already been set to variable: '",
        existingValue.value().toSparql(), "'. New variable: '",
        object.toString(), "'."));
  }

  existingValue = object.getVariable();
};

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
  } else if (!right_.has_value()) {
    throw SpatialSearchException(
        "Missing parameter 'right' in spatial search.");
  } else if (!maxDist_.has_value() && !maxResults_.has_value()) {
    throw SpatialSearchException(
        "Neither 'nearestNeighbors' nor 'maxDistance' were provided. At least "
        "one of both is required.");
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
cppcoro::generator<const Variable> Bind::containedVariables() const {
  for (const auto* ptr : _expression.containedVariables()) {
    co_yield *ptr;
  }
  co_yield _target;
}

// ____________________________________________________________________________
[[nodiscard]] string Bind::getDescriptor() const {
  auto inner = _expression.getDescriptor();
  return "BIND (" + inner + " AS " + _target.name() + ")";
}
}  // namespace parsedQuery
