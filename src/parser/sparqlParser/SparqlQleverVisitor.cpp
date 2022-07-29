// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "SparqlQleverVisitor.h"

#include <string>
#include <vector>

using Triples = ad_utility::sparql_types::Triples;
using Node = ad_utility::sparql_types::Node;
using ObjectList = ad_utility::sparql_types::ObjectList;
using PropertyList = ad_utility::sparql_types::PropertyList;
using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;

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
  auto wrapper = makeExpressionPimpl(visitTypesafe(ctx->expression()));
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
  auto wrapper = makeExpressionPimpl(visitTypesafe(ctx->expression()));
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
  return visitTypesafe(ctx->conditionalOrExpression());
}

// ____________________________________________________________________________________
LimitOffsetClause SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::LimitOffsetClausesContext* ctx) {
  LimitOffsetClause clause{};
  if (ctx->limitClause()) {
    clause._limit = visitTypesafe(ctx->limitClause());
  }
  if (ctx->offsetClause()) {
    clause._offset = visitTypesafe(ctx->offsetClause());
  }
  if (ctx->textLimitClause()) {
    clause._textLimit = visitTypesafe(ctx->textLimitClause());
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
  return ctx->constructTriples() ? visitTypesafe(ctx->constructTriples())
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

// ____________________________________________________________________________________
void SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BaseDeclContext* ctx) {
  _prefixMap[""] = visitTypesafe(ctx->iriref());
}

// ____________________________________________________________________________________
void SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PrefixDeclContext* ctx) {
  auto text = ctx->PNAME_NS()->getText();
  // Strip trailing ':'.
  _prefixMap[text.substr(0, text.length() - 1)] = visitTypesafe(ctx->iriref());
}

// ____________________________________________________________________________________
GroupKey SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::GroupConditionContext* ctx) {
  if (ctx->var() && !ctx->expression()) {
    return GroupKey{Variable{ctx->var()->getText()}};
  } else if (ctx->builtInCall() || ctx->functionCall()) {
    // builtInCall and functionCall are both also an Expression
    auto subCtx =
        (ctx->builtInCall() ? (antlr4::tree::ParseTree*)ctx->builtInCall()
                            : (antlr4::tree::ParseTree*)ctx->functionCall());
    // TODO: extract + template
    auto expr = makeExpressionPimpl(visit(subCtx));
    expr.setDescriptor(ctx->getText());
    return GroupKey{std::move(expr)};
  } else if (ctx->expression()) {
    auto expr = makeExpressionPimpl(visitTypesafe(ctx->expression()));
    if (ctx->AS() && ctx->var()) {
      return GroupKey{
          ParsedQuery::Alias{std::move(expr), ctx->var()->getText()}};
    } else {
      return GroupKey{std::move(expr)};
    }
  }
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
OrderKey SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::OrderConditionContext* ctx) {
  auto visitExprOrderKey = [this](bool isDescending,
                                  antlr4::tree::ParseTree* context) {
    auto expr = makeExpressionPimpl(visit(context));
    if (auto exprIsVariable = expr.getVariableOrNullopt();
        exprIsVariable.has_value()) {
      return OrderKey{VariableOrderKey(exprIsVariable.value(), isDescending)};
    } else {
      return OrderKey{ExpressionOrderKey{std::move(expr), isDescending}};
    }
  };

  if (ctx->var()) {
    return OrderKey{VariableOrderKey(ctx->var()->getText())};
  } else if (ctx->constraint()) {
    return visitExprOrderKey(false, ctx->constraint());
  } else if (ctx->brackettedExpression()) {
    return visitExprOrderKey(ctx->DESC() != nullptr,
                             ctx->brackettedExpression());
  }
  AD_FAIL();  // Should be unreachable.
}

// ____________________________________________________________________________________
unsigned long long int SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::LimitClauseContext* ctx) {
  return visitTypesafe(ctx->integer());
}

// ____________________________________________________________________________________
unsigned long long int SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::OffsetClauseContext* ctx) {
  return visitTypesafe(ctx->integer());
}

// ____________________________________________________________________________________
unsigned long long int SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TextLimitClauseContext* ctx) {
  return visitTypesafe(ctx->integer());
}

// ____________________________________________________________________________________
SparqlValues SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::InlineDataOneVarContext* ctx) {
  SparqlValues values;
  auto var = visitTypesafe(ctx->var());
  values._variables.push_back(var.name());
  if (ctx->dataBlockValue().empty())
    throw ParseException(
        "No values were specified in Values "
        "clause. This is not supported by QLever. Got: " +
        ctx->getText());
  for (auto& dataBlockValue : ctx->dataBlockValue()) {
    values._values.push_back({visitTypesafe(dataBlockValue)});
  }
  return values;
}

// ____________________________________________________________________________________
SparqlValues SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::InlineDataFullContext* ctx) {
  SparqlValues values;
  if (ctx->dataBlockSingle().empty())
    throw ParseException(
        "No values were specified in Values "
        "clause. This is not supported by QLever. Got: " +
        ctx->getText());
  if (ctx->NIL())
    throw ParseException(
        "No variables were specified in Values "
        "clause. This is not supported by QLever. Got: " +
        ctx->getText());
  for (auto& var : ctx->var()) {
    values._variables.push_back(visitTypesafe(var).name());
  }
  values._values = visitVector<vector<std::string>>(ctx->dataBlockSingle());
  if (std::any_of(values._values.begin(), values._values.end(),
                  [numVars = values._variables.size()](const auto& inner) {
                    return inner.size() != numVars;
                  })) {
    throw ParseException(
        "The number of values in every data block must "
        "match the number of variables in a values clause. Got: " +
        ctx->getText());
  }
  return values;
}

// ____________________________________________________________________________________
vector<std::string> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::DataBlockSingleContext* ctx) {
  if (ctx->NIL())
    throw ParseException(
        "No values were specified in DataBlock."
        "This is not supported by QLever. Got: " +
        ctx->getText());
  return visitVector<std::string>(ctx->dataBlockValue());
}

// ____________________________________________________________________________________
std::string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::DataBlockValueContext* ctx) {
  // Return a string
  if (ctx->iri()) {
    return visitTypesafe(ctx->iri());
  } else if (ctx->rdfLiteral()) {
    return visitTypesafe(ctx->rdfLiteral());
  } else if (ctx->numericLiteral()) {
    // TODO implement
    throw ParseException("Numbers in values clauses are not supported. Got: " +
                         ctx->getText() + ".");
  } else if (ctx->booleanLiteral()) {
    // TODO implement
    throw ParseException("Booleans in values clauses are not supported. Got: " +
                         ctx->getText() + ".");
  } else if (ctx->UNDEF()) {
    // TODO implement
    throw ParseException(
        "UNDEF in values clauses is not supported. Got: " + ctx->getText() +
        ""
        ".");
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
vector<SparqlQleverVisitor::ExpressionPtr> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ArgListContext* ctx) {
  // If no arguments, return empty expression vector.
  if (ctx->NIL()) {
    return std::vector<ExpressionPtr>{};
  }
  // The grammar allows an optional DISTICT before the argument list (the
  // whole list, not the individual arguments), but we currently don't support
  // it.
  if (ctx->DISTINCT()) {
    throw ParseException{
        "DISTINCT for argument lists of IRI functions are not supported"};
  }
  // Visit the expression of each argument.
  return visitVector<ExpressionPtr>(ctx->expression());
}

// ____________________________________________________________________________________
Triples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ConstructTriplesContext* ctx) {
  auto result = visitTypesafe(ctx->triplesSameSubject());
  if (ctx->constructTriples()) {
    auto newTriples = visitTypesafe(ctx->constructTriples());
    appendVector(result, std::move(newTriples));
  }
  return result;
}

// ____________________________________________________________________________________
Triples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TriplesSameSubjectContext* ctx) {
  Triples triples;
  if (ctx->varOrTerm()) {
    VarOrTerm subject = visitTypesafe(ctx->varOrTerm());
    AD_CHECK(ctx->propertyListNotEmpty());
    auto propertyList = visitTypesafe(ctx->propertyListNotEmpty());
    for (auto& tuple : propertyList.first) {
      triples.push_back({subject, std::move(tuple[0]), std::move(tuple[1])});
    }
    appendVector(triples, std::move(propertyList.second));
  } else if (ctx->triplesNode()) {
    auto tripleNodes = visitTriplesNode(ctx->triplesNode()).as<Node>();
    appendVector(triples, std::move(tripleNodes.second));
    AD_CHECK(ctx->propertyList());
    auto propertyList = visitTypesafe(ctx->propertyList());
    for (auto& tuple : propertyList.first) {
      triples.push_back(
          {tripleNodes.first, std::move(tuple[0]), std::move(tuple[1])});
    }
    appendVector(triples, std::move(propertyList.second));
  } else {
    // Invalid grammar
    AD_CHECK(false);
  }
  return triples;
}

// ____________________________________________________________________________________
PropertyList SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListContext* ctx) {
  return ctx->propertyListNotEmpty()
             ? visitTypesafe(ctx->propertyListNotEmpty())
             : PropertyList{Tuples{}, Triples{}};
}

// ____________________________________________________________________________________
PropertyList SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListNotEmptyContext* ctx) {
  Tuples triplesWithoutSubject;
  Triples additionalTriples;
  auto verbs = ctx->verb();
  auto objectLists = ctx->objectList();
  for (size_t i = 0; i < verbs.size(); i++) {
    // TODO use zip-style approach once C++ supports ranges
    auto objectList = visitTypesafe(objectLists.at(i));
    auto verb = visitTypesafe(verbs.at(i));
    for (auto& object : objectList.first) {
      triplesWithoutSubject.push_back({verb, std::move(object)});
    }
    appendVector(additionalTriples, std::move(objectList.second));
  }
  return PropertyList{std::move(triplesWithoutSubject),
                      std::move(additionalTriples)};
}

// ____________________________________________________________________________________
VarOrTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbContext* ctx) {
  if (ctx->varOrIri()) {
    return visitTypesafe(ctx->varOrIri());
  }
  if (ctx->getText() == "a") {
    // Special keyword 'a'
    return VarOrTerm{
        GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"}}};
  }
  throw ParseException{"Invalid verb "s + ctx->getText()};
}

// ____________________________________________________________________________________
ObjectList SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ObjectListContext* ctx) {
  Objects objects;
  Triples additionalTriples;
  auto objectContexts = ctx->objectR();
  for (auto& objectContext : objectContexts) {
    auto graphNode = visitTypesafe(objectContext);
    appendVector(additionalTriples, std::move(graphNode.second));
    objects.push_back(std::move(graphNode.first));
  }
  return ObjectList{std::move(objects), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
Node SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ObjectRContext* ctx) {
  return visitTypesafe(ctx->graphNode());
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbPathContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->path());
  p.computeCanBeNull();
  return p;
}

// ____________________________________________________________________________________
Variable SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbSimpleContext* ctx) {
  return visitTypesafe(ctx->var());
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) {
  if (ctx->verbPath()) {
    return visitTypesafe(ctx->verbPath());
  } else if (ctx->verbSimple()) {
    return PropertyPath::fromVariable(visitTypesafe(ctx->verbSimple()));
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathContext* ctx) {
  return visitTypesafe(ctx->pathAlternative());
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathAlternativeContext* ctx) {
  auto paths = visitVector<PropertyPath>(ctx->pathSequence());

  if (paths.size() == 1) {
    return paths[0];
  } else {
    return PropertyPath::makeAlternative(std::move(paths));
  }
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathSequenceContext* ctx) {
  auto paths = visitVector<PropertyPath>(ctx->pathEltOrInverse());

  if (paths.size() == 1) {
    return paths[0];
  } else {
    return PropertyPath::makeSequence(std::move(paths));
  }
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathEltContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->pathPrimary());

  if (ctx->pathMod()) {
    if (ctx->pathMod()->getText() == "+") {
      p = PropertyPath::makeTransitiveMin(p, 1);
    } else if (ctx->pathMod()->getText() == "?") {
      p = PropertyPath::makeTransitiveMax(p, 1);
    } else if (ctx->pathMod()->getText() == "*") {
      p = PropertyPath::makeTransitive(p);
    } else {
      AD_FAIL()  // Should be unreachable.
    }
  }

  return p;
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathEltOrInverseContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->pathElt());

  if (ctx->negationOperator) {
    p = PropertyPath::makeInverse(p);
  }

  return p;
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathPrimaryContext* ctx) {
  if (ctx->iri()) {
    auto iri = visitTypesafe(ctx->iri());
    return PropertyPath::fromIri(std::move(iri));
  } else if (ctx->path()) {
    return visitTypesafe(ctx->path());
  } else if (ctx->pathNegatedPropertySet()) {
    return visitTypesafe(ctx->pathNegatedPropertySet());
  } else if (ctx->getText() == "a") {
    // Special keyword 'a'
    return PropertyPath::fromIri(
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathNegatedPropertySetContext*) {
  throw ParseException(
      "\"!\" inside a property path is not supported by QLever.");
}

// ____________________________________________________________________________________
unsigned long long int SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::IntegerContext* ctx) {
  try {
    return std::stoull(ctx->getText());
  } catch (const std::out_of_range&) {
    throw ParseException{"Integer " + ctx->getText() +
                         " does not fit"
                         " into 64 bits. This is not supported by QLever."};
  }
}

// ____________________________________________________________________________________
Node SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BlankNodePropertyListContext* ctx) {
  VarOrTerm var{GraphTerm{newBlankNode()}};
  Triples triples;
  auto propertyList = visitTypesafe(ctx->propertyListNotEmpty());
  for (auto& tuple : propertyList.first) {
    triples.push_back({var, std::move(tuple[0]), std::move(tuple[1])});
  }
  appendVector(triples, std::move(propertyList.second));
  return Node{std::move(var), std::move(triples)};
}

// ____________________________________________________________________________________
Node SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::CollectionContext* ctx) {
  Triples triples;
  VarOrTerm nextElement{
      GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}}};
  auto nodes = ctx->graphNode();
  for (auto context : Reversed{nodes}) {
    VarOrTerm currentVar{GraphTerm{newBlankNode()}};
    auto graphNode = visitTypesafe(context);

    triples.push_back(
        {currentVar,
         VarOrTerm{GraphTerm{
             Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"}}},
         std::move(graphNode.first)});
    triples.push_back(
        {currentVar,
         VarOrTerm{GraphTerm{
             Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"}}},
         std::move(nextElement)});
    nextElement = std::move(currentVar);

    appendVector(triples, std::move(graphNode.second));
  }
  return Node{std::move(nextElement), std::move(triples)};
}

// ____________________________________________________________________________________
Node SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::GraphNodeContext* ctx) {
  if (ctx->varOrTerm()) {
    return Node{visitTypesafe(ctx->varOrTerm()), Triples{}};
  } else if (ctx->triplesNode()) {
    return visitTriplesNode(ctx->triplesNode()).as<Node>();
  }
  AD_CHECK(false);
}

// ____________________________________________________________________________________
VarOrTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VarOrTermContext* ctx) {
  if (ctx->var()) {
    return VarOrTerm{visitTypesafe(ctx->var())};
  }
  if (ctx->graphTerm()) {
    return VarOrTerm{visitTypesafe(ctx->graphTerm())};
  }

  // invalid grammar
  AD_CHECK(false);
}

// ____________________________________________________________________________________
VarOrTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VarOrIriContext* ctx) {
  if (ctx->var()) {
    return VarOrTerm{visitTypesafe(ctx->var())};
  }
  if (ctx->iri()) {
    return VarOrTerm{GraphTerm{Iri{visitTypesafe(ctx->iri())}}};
  }
  // invalid grammar
  AD_CHECK(false);
}

// ____________________________________________________________________________________
GraphTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::GraphTermContext* ctx) {
  if (ctx->numericLiteral()) {
    auto literalAny = visitNumericLiteral(ctx->numericLiteral());
    try {
      auto intLiteral = literalAny.as<unsigned long long>();
      return GraphTerm{Literal{intLiteral}};
    } catch (...) {
    }
    try {
      auto intLiteral = literalAny.as<long long>();
      return GraphTerm{Literal{intLiteral}};
    } catch (...) {
    }
    try {
      auto intLiteral = literalAny.as<double>();
      return GraphTerm{Literal{intLiteral}};
    } catch (...) {
    }
    AD_CHECK(false);
  }
  if (ctx->booleanLiteral()) {
    return GraphTerm{Literal{visitTypesafe(ctx->booleanLiteral())}};
  }
  if (ctx->blankNode()) {
    return GraphTerm{visitTypesafe(ctx->blankNode())};
  }
  if (ctx->iri()) {
    return GraphTerm{Iri{visitTypesafe(ctx->iri())}};
  }
  if (ctx->rdfLiteral()) {
    return GraphTerm{Literal{visitTypesafe(ctx->rdfLiteral())}};
  }
  if (ctx->NIL()) {
    return GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}};
  }
  AD_CHECK(false);
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ConditionalOrExpressionContext* ctx) {
  auto childContexts = ctx->conditionalAndExpression();
  auto children = visitVector<ExpressionPtr>(ctx->conditionalAndExpression());
  AD_CHECK(!children.empty());
  auto result = std::move(children.front());
  using C = sparqlExpression::OrExpression::Children;
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = std::make_unique<sparqlExpression::OrExpression>(
                      C{std::move(result), std::move(ptr)});
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ConditionalAndExpressionContext* ctx) {
  auto children = visitVector<ExpressionPtr>(ctx->valueLogical());
  AD_CHECK(!children.empty());
  auto result = std::move(children.front());
  using C = sparqlExpression::AndExpression::Children;
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = std::make_unique<sparqlExpression::AndExpression>(
                      C{std::move(result), std::move(ptr)});
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ValueLogicalContext* ctx) {
  return visitTypesafe(ctx->relationalExpression());
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::RelationalExpressionContext* ctx) {
  auto childContexts = ctx->numericExpression();

  if (childContexts.size() == 1) {
    return visitTypesafe(childContexts[0]);
  }
  if (false) {
    // TODO<joka921> Once we have reviewed and merged the EqualsExpression,
    // this can be uncommented.
    /*
   if (ctx->children[1]->getText() == "=") {
     auto leftChild = std::move(
         visitNumericExpression(childContexts[0]).as<ExpressionPtr>());
     auto rightChild = std::move(
         visitNumericExpression(childContexts[1]).as<ExpressionPtr>());

     return
   ExpressionPtr{std::make_unique<sparqlExpression::EqualsExpression>(
         std::move(leftChild), std::move(rightChild))};

     */
  } else {
    throw std::runtime_error(
        "This parser does not yet support relational expressions = < etc.");
  }
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::NumericExpressionContext* ctx) {
  return visitTypesafe(ctx->additiveExpression());
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::AdditiveExpressionContext* ctx) {
  auto children = visitVector<ExpressionPtr>(ctx->multiplicativeExpression());
  auto opTypes = visitOperationTags(ctx->children, {"+", "-"});

  if (!ctx->strangeMultiplicativeSubexprOfAdditive().empty()) {
    throw std::runtime_error{
        "You currently have to put a space between a +/- and the number "
        "after it."};
  }

  AD_CHECK(!children.empty());
  AD_CHECK(children.size() == opTypes.size() + 1);

  auto result = std::move(children.front());
  auto childIt = children.begin() + 1;
  auto opIt = opTypes.begin();
  while (childIt != children.end()) {
    if (*opIt == "+") {
      result = createExpression<sparqlExpression::AddExpression>(
          std::move(result), std::move(*childIt));
    } else if (*opIt == "-") {
      result = createExpression<sparqlExpression::SubtractExpression>(
          std::move(result), std::move(*childIt));
    } else {
      AD_CHECK(false);
    }
    ++childIt;
    ++opIt;
  }
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::MultiplicativeExpressionContext* ctx) {
  auto children = visitVector<ExpressionPtr>(ctx->unaryExpression());
  auto opTypes = visitOperationTags(ctx->children, {"*", "/"});

  AD_CHECK(!children.empty());
  AD_CHECK(children.size() == opTypes.size() + 1);

  auto result = std::move(children.front());
  auto childIt = children.begin() + 1;
  auto opIt = opTypes.begin();
  while (childIt != children.end()) {
    if (*opIt == "*") {
      result = createExpression<sparqlExpression::MultiplyExpression>(
          std::move(result), std::move(*childIt));
    } else if (*opIt == "/") {
      result = createExpression<sparqlExpression::DivideExpression>(
          std::move(result), std::move(*childIt));
    } else {
      AD_CHECK(false);
    }
    ++childIt;
    ++opIt;
  }
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::UnaryExpressionContext* ctx) {
  auto child = visitTypesafe(ctx->primaryExpression());
  if (ctx->children[0]->getText() == "-") {
    return createExpression<sparqlExpression::UnaryMinusExpression>(
        std::move(child));
  } else if (ctx->getText() == "!") {
    return createExpression<sparqlExpression::UnaryNegateExpression>(
        std::move(child));
  } else {
    // no sign or an explicit '+'
    return child;
  }
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PrimaryExpressionContext* ctx) {
  if (ctx->builtInCall()) {
    return visitTypesafe(ctx->builtInCall());
  }
  if (ctx->rdfLiteral()) {
    // TODO<joka921> : handle strings with value datatype that are
    // not in the knowledge base correctly.
    return ExpressionPtr{
        std::make_unique<sparqlExpression::StringOrIriExpression>(
            ctx->rdfLiteral()->getText())};
  }
  if (ctx->iriOrFunction()) {
    return visitTypesafe(ctx->iriOrFunction());
  }

  if (ctx->brackettedExpression()) {
    return visitTypesafe(ctx->brackettedExpression());
  }

  // TODO<joka921> Refactor s.t. try/catch becomes if/else here
  if (ctx->numericLiteral()) {
    auto literalAny = visitNumericLiteral(ctx->numericLiteral());
    try {
      auto intLiteral = literalAny.as<unsigned long long>();
      return ExpressionPtr{std::make_unique<sparqlExpression::IntExpression>(
          static_cast<int64_t>(intLiteral))};
    } catch (...) {
    }
    try {
      auto intLiteral = literalAny.as<long long>();
      return ExpressionPtr{std::make_unique<sparqlExpression::IntExpression>(
          static_cast<int64_t>(intLiteral))};
    } catch (...) {
    }
    try {
      auto intLiteral = literalAny.as<double>();
      return ExpressionPtr{std::make_unique<sparqlExpression::DoubleExpression>(
          static_cast<double>(intLiteral))};
    } catch (...) {
    }
    AD_CHECK(false);
  }

  if (ctx->booleanLiteral()) {
    auto b = visitTypesafe(ctx->booleanLiteral());
    return ExpressionPtr{std::make_unique<sparqlExpression::BoolExpression>(b)};
  }

  if (ctx->var()) {
    sparqlExpression::Variable v;
    v._variable = ctx->var()->getText();
    return ExpressionPtr{
        std::make_unique<sparqlExpression::VariableExpression>(v)};
  }
  // We should have returned by now
  AD_CHECK(false);
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BrackettedExpressionContext* ctx) {
  return visitTypesafe(ctx->expression());
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    [[maybe_unused]] SparqlAutomaticParser::BuiltInCallContext* ctx) {
  if (ctx->aggregate()) {
    return visitTypesafe(ctx->aggregate());
    // TODO: Implement built-in calls according to the following examples.
    //
    // } else if (ad_utility::getLowercase(ctx->children[0]->getText()) ==
    //            "sqr") {
    //   if (ctx->expression().size() != 1) {
    //     throw ParseException{"SQR needs one argument"};
    //   }
    //   auto children = visitVector<ExpressionPtr>(ctx->expression());
    //   return createExpression<sparqlExpression::SquareExpression>(
    //       std::move(children[0]));
    // } else if (ad_utility::getLowercase(ctx->children[0]->getText()) ==
    //            "dist") {
    //   if (ctx->expression().size() != 2) {
    //     throw ParseException{"DIST needs two arguments"};
    //   }
    //   auto children = visitVector<ExpressionPtr>(ctx->expression());
    //   return createExpression<sparqlExpression::DistExpression>(
    //       std::move(children[0]), std::move(children[1]));
  } else {
    throw ParseException{
        "Built-in function not yet implemented (only aggregates like COUNT "
        "so far)"};
  }
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::AggregateContext* ctx) {
  // the only case that there is no child expression is COUNT(*), so we can
  // check this outside the if below.
  if (!ctx->expression()) {
    throw ParseException{
        "This parser currently doesn't support COUNT(*), please specify an "
        "explicit expression for the COUNT"};
  }
  auto childExpression = visitTypesafe(ctx->expression());
  auto children = ctx->children;
  bool distinct = false;
  for (const auto& child : children) {
    if (ad_utility::getLowercase(child->getText()) == "distinct") {
      distinct = true;
    }
  }
  auto makePtr = [&]<typename ExpressionType>(auto&&... additionalArgs) {
    ExpressionPtr result{std::make_unique<ExpressionType>(
        distinct, std::move(childExpression), AD_FWD(additionalArgs)...)};
    result->descriptor() = ctx->getText();
    return result;
  };
  if (ad_utility::getLowercase(children[0]->getText()) == "count") {
    return makePtr.operator()<sparqlExpression::CountExpression>();
  } else if (ad_utility::getLowercase(children[0]->getText()) == "sum") {
    return makePtr.operator()<sparqlExpression::SumExpression>();
  } else if (ad_utility::getLowercase(children[0]->getText()) == "max") {
    return makePtr.operator()<sparqlExpression::MaxExpression>();
  } else if (ad_utility::getLowercase(children[0]->getText()) == "min") {
    return makePtr.operator()<sparqlExpression::MinExpression>();
  } else if (ad_utility::getLowercase(children[0]->getText()) == "avg") {
    return makePtr.operator()<sparqlExpression::AvgExpression>();
  } else if (ad_utility::getLowercase(children[0]->getText()) ==
             "group_concat") {
    // Use a space as a default separator

    std::string separator;
    if (ctx->string()) {
      separator = ctx->string()->getText();
      // If there was a separator, we have to strip the quotation marks
      AD_CHECK(separator.size() >= 2);
      separator = separator.substr(1, separator.size() - 2);
    } else {
      separator = " "s;
    }

    return makePtr.operator()<sparqlExpression::GroupConcatExpression>(
        std::move(separator));
  } else if (ad_utility::getLowercase(children[0]->getText()) == "sample") {
    return makePtr.operator()<sparqlExpression::SampleExpression>();
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
ExpressionPtr SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::IriOrFunctionContext* ctx) {
  // Case 1: Just an IRI.
  if (ctx->argList() == nullptr) {
    return ExpressionPtr{
        std::make_unique<sparqlExpression::StringOrIriExpression>(
            ctx->getText())};
  }
  // Case 2: Function call, where the function name is an IRI.
  return std::move(processIriFunctionCall(visitTypesafe(ctx->iri()),
                                          visitTypesafe(ctx->argList()))
                       .as<ExpressionPtr>());
}

// ____________________________________________________________________________________
std::string SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::RdfLiteralContext* ctx) {
  return ctx->getText();
}

// ____________________________________________________________________________________
bool SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BooleanLiteralContext* ctx) {
  return ctx->getText() == "true";
}

// ____________________________________________________________________________________
BlankNode SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::BlankNodeContext* ctx) {
  if (ctx->ANON()) {
    return newBlankNode();
  }
  if (ctx->BLANK_NODE_LABEL()) {
    // strip _: prefix from string
    constexpr size_t length = std::string_view{"_:"}.length();
    const string label = ctx->BLANK_NODE_LABEL()->getText().substr(length);
    // false means the query explicitly contains a blank node label
    return BlankNode{false, label};
  }
  // invalid grammar
  AD_CHECK(false);
}
