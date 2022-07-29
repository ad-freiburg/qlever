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

// ____________________________________________________________________________________
GraphPatternOperation::Bind SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BindContext* ctx) {
  auto wrapper = makeExpressionPimpl(visit(ctx->expression()));
  return GraphPatternOperation::Bind{std::move(wrapper), ctx->var()->getText()};
}

// ____________________________________________________________________________________
GraphPatternOperation::Values SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::InlineDataContext* ctx) {
  return visitTypesafe(ctx->dataBlock());
}

// ____________________________________________________________________________________
GraphPatternOperation::Values SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::DataBlockContext* ctx) {
  if (ctx->inlineDataOneVar()) {
    return {visitTypesafe(ctx->inlineDataOneVar())};
  } else if (ctx->inlineDataFull()) {
    return {visitTypesafe(ctx->inlineDataFull())};
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
std::optional<GraphPatternOperation::Values> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ValuesClauseContext* ctx) {
  if (ctx->dataBlock()) {
    return visitTypesafe(ctx->dataBlock());
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
sparqlExpression::SparqlExpression::Ptr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ExpressionContext* ctx) {
  return std::move(visitConditionalOrExpression(ctx->conditionalOrExpression())
                       .as<sparqlExpression::SparqlExpression::Ptr>());
}

// ____________________________________________________________________________________
LimitOffsetClause SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::LimitOffsetClausesContext* ctx) {
  LimitOffsetClause clause{};
  if (ctx->limitClause()) {
    clause._limit =
        visitLimitClause(ctx->limitClause()).as<unsigned long long>();
  }
  if (ctx->offsetClause()) {
    clause._offset =
        visitOffsetClause(ctx->offsetClause()).as<unsigned long long>();
  }
  if (ctx->textLimitClause()) {
    clause._textLimit =
        visitTextLimitClause(ctx->textLimitClause()).as<unsigned long long>();
  }
  return clause;
}

// ____________________________________________________________________________________
vector<OrderKey> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::OrderClauseContext* ctx) {
  return visitVector<OrderKey>(ctx->orderCondition());
}

// ____________________________________________________________________________________
vector<GroupKey> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::GroupClauseContext* ctx) {
  return visitVector<GroupKey>(ctx->groupCondition());
}

// ____________________________________________________________________________________
SparqlQleverVisitor::Triples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ConstructTemplateContext* ctx) {
  return ctx->constructTriples()
             ? ctx->constructTriples()->accept(this).as<Triples>()
             : Triples{};
}

// ____________________________________________________________________________________
string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::IriContext* ctx) {
  string langtag =
      ctx->PREFIX_LANGTAG() ? ctx->PREFIX_LANGTAG()->getText() : "";
  if (ctx->iriref()) {
    return langtag + visitTypesafe(ctx->iriref());
  } else {
    AD_CHECK(ctx->prefixedName())
    return langtag + visitTypesafe(ctx->prefixedName());
  }
}

// ____________________________________________________________________________________
string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::IrirefContext* ctx) {
  return RdfEscaping::unescapeIriref(ctx->getText());
}

// ____________________________________________________________________________________
string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PrefixedNameContext* ctx) {
  if (ctx->pnameLn()) {
    return visitTypesafe(ctx->pnameLn());
  } else {
    AD_CHECK(ctx->pnameNs());
    return visitTypesafe(ctx->pnameNs());
  }
}

// ____________________________________________________________________________________
string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PnameLnContext* ctx) {
  string text = ctx->getText();
  auto pos = text.find(':');
  auto pnameNS = text.substr(0, pos);
  auto pnLocal = text.substr(pos + 1);
  if (!_prefixMap.contains(pnameNS)) {
    // TODO<joka921> : proper name
    throw ParseException{"Prefix " + pnameNS +
                         " was not registered using a PREFIX declaration"};
  }
  auto inner = _prefixMap[pnameNS];
  // strip the trailing ">"
  inner = inner.substr(0, inner.size() - 1);
  return inner + RdfEscaping::unescapePrefixedIri(pnLocal) + ">";
}

// ____________________________________________________________________________________
string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PnameNsContext* ctx) {
  auto text = ctx->getText();
  auto prefix = text.substr(0, text.length() - 1);
  if (!_prefixMap.contains(prefix)) {
    // TODO<joka921> : proper name
    throw ParseException{"Prefix " + prefix +
                         " was not registered using a PREFIX declaration"};
  }
  return _prefixMap[prefix];
}
