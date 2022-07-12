// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "SparqlQleverVisitor.h"
#include "../../engine/sparqlExpressions/RelationalExpressions.h"

#include <string>
#include <vector>

using namespace sparqlExpression;
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
      throw ParseException{absl::StrCat(
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
  throw ParseException{"Function \"" + iri + "\" not supported"};
}
// ___________________________________________________________________________
auto SparqlQleverVisitor::visitTypesafe(SparqlAutomaticParser::RelationalExpressionContext* ctx) -> ExpressionPtr {
  auto children = visitVector<ExpressionPtr>(ctx->numericExpression());

  if (ctx->expressionList()) {
    throw ParseException{"IN/ NOT IN in expressions is currently not supported by QLever."};
  }
  AD_CHECK(children.size() == 1 || children.size() == 2);
  if (children.size() == 1) {
    return std::move(children[0]);
  }

  auto make = [&]<typename Expr>() {
    return createExpression<Expr>(std::move(children[0]), std::move(children[1]));
  };
  std::string relation = ctx->children[1]->getText();
   if (relation == "=") {
     return make.operator()<EqualExpression>();
   } else if (relation == "!=") {
     return make.operator()<NotEqualExpression>();
   } else if (relation == "<") {
     return make.operator()<LessThanExpression>();
  } else if (relation == ">") {
    return make.operator()<GreaterThanExpression>();
  } else if (relation == "<=") {
    return make.operator()<LessEqualExpression>();
  } else if (relation == ">=") {
    return make.operator()<relational::GreaterEqualExpression>();
  } else {
    AD_FAIL();
  }
}
