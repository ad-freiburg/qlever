// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "SparqlQleverVisitor.h"

#include <string>
#include <vector>

// ___________________________________________________________________________
antlrcpp::Any SparqlQleverVisitor::processIriFunctionCall(
    std::string iri, std::vector<SparqlQleverVisitor::ExpressionPtr> argList) {
  constexpr static std::string_view geofPrefix =
      "<http://www.opengis.net/def/function/geosparql/";
  std::string_view iriView = iri;
  if (iriView.starts_with(geofPrefix)) {
    iriView.remove_prefix(geofPrefix.size());
    if (iriView == "distance>") {
      if (argList.size() == 2) {
        return createExpression<sparqlExpression::DistExpression>(
            std::move(argList[0]), std::move(argList[1]));
      } else {
        throw SparqlParseException{
            "Function geof:distance requires two arguments"};
      }
    } else if (iriView == "longitude>") {
      if (argList.size() == 1) {
        return createExpression<sparqlExpression::LongitudeExpression>(
            std::move(argList[0]));
      } else {
        throw SparqlParseException{
            "Function geof:longitude requires one argument"};
      }
    } else if (iriView == "latitude>") {
      if (argList.size() == 1) {
        return createExpression<sparqlExpression::LatitudeExpression>(
            std::move(argList[0]));
      } else {
        throw SparqlParseException{
            "Function geof:longitude requires one argument"};
      }
    }
  }
  throw SparqlParseException{"Function \"" + iri + "\" not supported"};
}
