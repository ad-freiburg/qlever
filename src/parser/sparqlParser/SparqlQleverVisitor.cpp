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

// ____________________________________________________________________________________
ParsedQuery::SelectClause SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::SelectClauseContext* ctx) {
  ParsedQuery::SelectClause select;

  select._distinct = static_cast<bool>(ctx->DISTINCT());
  select._reduced = static_cast<bool>(ctx->REDUCED());

  if (ctx->asterisk) {
    select._varsOrAsterisk.setAllVariablesSelected();
  } else {
    // TODO: remove select._aliases and let the manuallySelectedVariables accept
    // the variant<Variable, Alias> directly. Then all this code goes away
    std::vector<std::string> selectedVariables;

    auto processVariable = [&selectedVariables](const Variable& var) {
      selectedVariables.push_back(var.name());
    };
    auto processAlias = [&selectedVariables,
                         &select](ParsedQuery::Alias alias) {
      selectedVariables.push_back(alias._outVarName);
      select._aliases.push_back(std::move(alias));
    };

    for (auto& varOrAlias : ctx->varOrAlias()) {
      std::visit(
          ad_utility::OverloadCallOperator{processVariable, processAlias},
          visitTypesafe(varOrAlias));
    }
    select._varsOrAsterisk.setManuallySelected(std::move(selectedVariables));
  }

  return select;
}

// ____________________________________________________________________________________
std::variant<Variable, ParsedQuery::Alias> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VarOrAliasContext* ctx) {
  if (ctx->var())
    return visitTypesafe(ctx->var());
  else if (ctx->alias())
    return visitTypesafe(ctx->alias());
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
ParsedQuery::Alias SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::AliasContext* ctx) {
  // A SPARQL alias has only one child, namely the contents within
  // parentheses.
  return visitTypesafe(ctx->aliasWithoutBrackets());
}

// ____________________________________________________________________________________
ParsedQuery::Alias SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::AliasWithoutBracketsContext* ctx) {
  auto wrapper = makeExpressionPimpl(visit(ctx->expression()));
  return ParsedQuery::Alias{std::move(wrapper), ctx->var()->getText()};
}

// ____________________________________________________________________________________
Variable SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VarContext* ctx) {
  return Variable{ctx->getText()};
}
