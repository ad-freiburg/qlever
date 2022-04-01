// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "SparqlQleverVisitor.h"

#include <string>
#include <vector>

// ___________________________________________________________________________
antlrcpp::Any SparqlQleverVisitor::processIriFunctionCall(
    const std::string& iri,
    std::vector<SparqlQleverVisitor::ExpressionPtr> argList) {
  // Lambda that checks the number of arguments and throws an error if it's
  // not right.
  auto checkNumArgs = [&argList](const std::string_view prefix,
                                 const std::string_view functionName,
                                 size_t numArgs) {
    static std::array<std::string, 6> wordForNumArgs = {
        "no", "one", "two", "three", "four", "five"};
    if (argList.size() != numArgs) {
      throw SparqlParseException{absl::StrCat(
          "Function ", prefix, functionName, " takes ",
          numArgs < 5 ? wordForNumArgs[numArgs] : std::to_string(numArgs),
          numArgs == 1 ? " argument" : " arguments")};
    }
  };

  constexpr static std::string_view geofPrefix =
      "<http://www.opengis.net/def/function/geosparql/";
  std::string_view iriView = iri;
  if (iriView.starts_with(geofPrefix)) {
    iriView.remove_prefix(geofPrefix.size());
    AD_CHECK(iriView.ends_with('>'));
    iriView.remove_suffix(1);
    if (iriView == "distance") {
      checkNumArgs("geof:", iriView, 2);
      return createExpression<sparqlExpression::DistExpression>(
          std::move(argList[0]), std::move(argList[1]));
    } else if (iriView == "longitude") {
      checkNumArgs("geof:", iriView, 1);
      return createExpression<sparqlExpression::LongitudeExpression>(
          std::move(argList[0]));
    } else if (iriView == "latitude") {
      checkNumArgs("geof:", iriView, 1);
      return createExpression<sparqlExpression::LatitudeExpression>(
          std::move(argList[0]));
    }
  }
  throw SparqlParseException{"Function \"" + iri + "\" not supported"};
}
