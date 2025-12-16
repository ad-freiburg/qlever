// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "parser/MaterializedViewQuery.h"

#include <absl/strings/str_cat.h>

#include "parser/MagicServiceIriConstants.h"
#include "parser/SparqlTriple.h"
#include "util/Exception.h"

namespace parsedQuery {

static constexpr std::string_view prefixColumnParam = "column-";

// _____________________________________________________________________________
void MaterializedViewQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto parameter = extractParameterName(predicate, MATERIALIZED_VIEW_IRI);
  if (parameter.starts_with(prefixColumnParam)) {
    Variable column{
        absl::StrCat("?", parameter.substr(prefixColumnParam.length()))};
    addRequestedColumn(column, object);
  } else {
    throw MaterializedViewConfigException(
        absl::StrCat("Unknown parameter <", parameter,
                     ">. Expected parameter of the form "
                     "<",
                     prefixColumnParam, "-COLNAME>."));
  }
}

// _____________________________________________________________________________
void MaterializedViewQuery::setScanCol(const TripleComponent& object) {
  if (object.isUndef()) {
    throw MaterializedViewConfigException(
        "The subject of the magic predicate for reading from a materialized "
        "view may not be undef.");
  }
  AD_CORRECTNESS_CHECK(!scanCol_.has_value(),
                       "Only one value may be set for the scan column. This "
                       "can be a literal, IRI or variable.");
  scanCol_ = object;
}

// _____________________________________________________________________________
void MaterializedViewQuery::addRequestedColumn(const Variable& column,
                                               const TripleComponent& object) {
  if (requestedColumns_.contains(column)) {
    throw MaterializedViewConfigException(
        absl::StrCat("Each column may only be requested once, but '",
                     column.name(), "' was requested again."));
  }
  requestedColumns_.insert({column, object});
}

// _____________________________________________________________________________
MaterializedViewQuery::MaterializedViewQuery(
    const ad_utility::triple_component::Iri& iri) {
  viewName_ = extractParameterName(iri, MATERIALIZED_VIEW_IRI);
  if (viewName_.value().empty()) {
    throw MaterializedViewConfigException(absl::StrCat(
        "The IRI for the materialized view SERVICE should specify the view "
        "name, like `SERVICE ",
        MATERIALIZED_VIEW_IRI_WITHOUT_CLOSING_BRACKET, "VIEWNAME> {...}`."));
  }
}

// _____________________________________________________________________________
MaterializedViewQuery::MaterializedViewQuery(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  auto predicate = extractParameterName(simpleTriple.p_, MATERIALIZED_VIEW_IRI);
  auto sep = predicate.find("-");

  if (sep == std::string::npos) {
    throw MaterializedViewConfigException(absl::StrCat(
        "Special triple for materialized view has an invalid predicate '",
        predicate, "'. Expected ",
        MATERIALIZED_VIEW_IRI_WITHOUT_CLOSING_BRACKET, "VIEWNAME-COLNAME>."));
  }

  Variable requestedColumn{absl::StrCat("?", predicate.substr(sep + 1))};

  setScanCol(simpleTriple.s_);
  viewName_ = predicate.substr(0, sep);
  addRequestedColumn(requestedColumn, simpleTriple.o_);
}

// _____________________________________________________________________________
ad_utility::HashSet<Variable> MaterializedViewQuery::getVarsToKeep() const {
  ad_utility::HashSet<Variable> varsToKeep;
  if (scanCol_.has_value() && scanCol_.value().isVariable()) {
    varsToKeep.insert(scanCol_.value().getVariable());
  }
  for (const auto& [original, target] : requestedColumns_) {
    if (target.isVariable()) {
      varsToKeep.insert(target.getVariable());
    }
  }
  return varsToKeep;
}

}  // namespace parsedQuery
