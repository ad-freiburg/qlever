// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "parser/MaterializedViewQuery.h"

#include <absl/strings/str_cat.h>

#include "engine/MaterializedViews.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/NormalizedString.h"

namespace parsedQuery {

static constexpr std::string_view prefixPayloadParam = "payload-";

// _____________________________________________________________________________
void MaterializedViewQuery::addParameter(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  TripleComponent predicate = simpleTriple.p_;
  TripleComponent object = simpleTriple.o_;

  auto parameter = extractParameterName(predicate, MATERIALIZED_VIEW_IRI);

  if (parameter == "name") {
    setViewName(object);
  } else if (parameter == "scan-column") {
    setScanCol(object);
  } else if (parameter.starts_with(prefixPayloadParam)) {
    Variable column{
        absl::StrCat("?", parameter.substr(prefixPayloadParam.length()))};
    addPayloadVariable(column, object);
  } else {
    throw MaterializedViewConfigException(
        absl::StrCat("Unknown parameter <", parameter,
                     ">. Supported parameters are <name>, <scan-column> and "
                     "<payload-COLNAME>."));
  }
}

// _____________________________________________________________________________
void MaterializedViewQuery::setViewName(const TripleComponent& object) {
  if (!object.isLiteral()) {
    throw MaterializedViewConfigException(
        "Parameter <name> to materialized view query needs to be a literal");
  }
  viewName_ = asStringViewUnsafe(object.getLiteral().getContent());
}

// _____________________________________________________________________________
void MaterializedViewQuery::setScanCol(const TripleComponent& object) {
  if (object.isUndef()) {
    throw MaterializedViewConfigException(
        "Parameter <scan-column> may not be undef.");
  }
  scanCol_ = object;
}

// _____________________________________________________________________________
void MaterializedViewQuery::addPayloadVariable(const Variable& column,
                                               const TripleComponent& object) {
  if (!object.isVariable()) {
    throw MaterializedViewConfigException(
        "Payload columns can not be filtered, they can only be read into "
        "columns. Therefore they must be specified with a variable.");
  }
  if (requestedVariables_.contains(column)) {
    throw MaterializedViewConfigException(
        "Each payload column may only be requested once.");
  }
  // TODO<ullingerc> check that no two payload columns are mapped to the same
  // output variable.
  requestedVariables_.insert({column, object.getVariable()});
}

// _____________________________________________________________________________
MaterializedViewQuery::MaterializedViewQuery(const SparqlTriple& triple) {
  auto simpleTriple = triple.getSimple();
  auto predicate = extractParameterName(simpleTriple.p_, MATERIALIZED_VIEW_IRI);
  auto sep = predicate.find(":");

  if (sep == std::string::npos) {
    throw MaterializedViewConfigException(absl::StrCat(
        "Special triple for materialized view has an invalid predicate '",
        simpleTriple.p_.getString(),
        "'. Expected <https://qlever.cs.uni-freiburg.de/materializedView/"
        "VIEWNAME:COLNAME>."));
  }

  Variable requestedColumn{absl::StrCat("?", predicate.substr(sep + 1))};

  setScanCol(simpleTriple.s_);
  viewName_ = predicate.substr(0, sep);
  addPayloadVariable(requestedColumn, simpleTriple.o_);
}

}  // namespace parsedQuery
