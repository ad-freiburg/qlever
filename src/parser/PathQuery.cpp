// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Herrmann <johannes.r.herrmann(at)gmail.com>
//          Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "parser/PathQuery.h"

#include <string_view>

#include "parser/MagicServiceIriConstants.h"

namespace parsedQuery {

// ____________________________________________________________________________
void PathQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto predString = extractParameterName(predicate, PATH_SEARCH_IRI);

  if (predString == "source") {
    sources_.push_back(std::move(object));
  } else if (predString == "target") {
    targets_.push_back(std::move(object));
  } else if (predString == "start") {
    setVariable("start", object, start_);
  } else if (predString == "end") {
    setVariable("end", object, end_);
  } else if (predString == "pathColumn") {
    setVariable("pathColumn", object, pathColumn_);
  } else if (predString == "edgeColumn") {
    setVariable("edgeColumn", object, edgeColumn_);
  } else if (predString == "edgeProperty") {
    edgeProperties_.push_back(getVariable("edgeProperty", object));
  } else if (predString == "cartesian") {
    if (!object.isBool()) {
      throw PathSearchException("The parameter <cartesian> expects a boolean");
    }
    cartesian_ = object.getBool();
  } else if (predString == "numPathsPerTarget") {
    if (!object.isInt()) {
      throw PathSearchException(
          "The parameter <numPathsPerTarget> expects an integer");
    }
    numPathsPerTarget_ = object.getInt();
  } else if (predString == "algorithm") {
    if (!object.isIri()) {
      throw PathSearchException("The <algorithm> value has to be an IRI");
    }
    auto objString = extractParameterName(object, PATH_SEARCH_IRI);

    if (objString == "allPaths") {
      algorithm_ = PathSearchAlgorithm::ALL_PATHS;
    } else {
      throw PathSearchException(
          absl::StrCat("Unsupported algorithm in pathSearch: ", objString,
                       ". Supported Algorithms: <allPaths>."));
    }
  } else {
    throw PathSearchException(absl::StrCat(
        "Unsupported argument <", predString,
        "> in PathSearch. Supported Arguments: <source>, <target>, <start>, "
        "<end>, <pathColumn>, <edgeColumn>, <edgeProperty>, <algorithm>."));
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
PathSearchConfiguration PathQuery::toPathSearchConfiguration(
    const Index::Vocab& vocab) const {
  auto sources = toSearchSide(sources_, vocab);
  auto targets = toSearchSide(targets_, vocab);

  if (!start_.has_value()) {
    throw PathSearchException("Missing parameter <start> in path search.");
  } else if (!end_.has_value()) {
    throw PathSearchException("Missing parameter <end> in path search.");
  } else if (!pathColumn_.has_value()) {
    throw PathSearchException("Missing parameter <pathColumn> in path search.");
  } else if (!edgeColumn_.has_value()) {
    throw PathSearchException("Missing parameter <edgeColumn> in path search.");
  }

  return PathSearchConfiguration{
      algorithm_,          sources,         targets,
      start_.value(),      end_.value(),    pathColumn_.value(),
      edgeColumn_.value(), edgeProperties_, cartesian_,
      numPathsPerTarget_};
}

}  // namespace parsedQuery
