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

// ___________________________________________________________________________
namespace {
string stripAndLowercaseKeywordLiteral(std::string_view lit) {
  if (lit.size() > 2 && lit[0] == '"' && lit.back() == '"') {
    auto stripped = lit.substr(1, lit.size() - 2);
    return ad_utility::getLowercaseUtf8(stripped);
  }
  return std::string{lit};
}

// ___________________________________________________________________________
template <typename Current, typename... Others>
constexpr const ad_utility::Last<Current, Others...>* unwrapVariant(
    const auto& arg) {
  if constexpr (sizeof...(Others) > 0) {
    if constexpr (ad_utility::isSimilar<decltype(arg), Current>) {
      if (const auto ptr = std::get_if<ad_utility::First<Others...>>(&arg)) {
        return unwrapVariant<Others...>(*ptr);
      }
      return nullptr;
    } else {
      return unwrapVariant<Others...>(arg);
    }
  } else {
    return &arg;
  }
}
}  // namespace

// ___________________________________________________________________________
using PathTuples = ad_utility::sparql_types::PathTuples;
using VarOrPath = ad_utility::sparql_types::VarOrPath;
using ObjectList = ad_utility::sparql_types::ObjectList;
using TripleWithPropertyPath = ad_utility::sparql_types::TripleWithPropertyPath;

vector<TripleWithPropertyPath> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TriplesSameSubjectPathContext* ctx) {
  vector<TripleWithPropertyPath> triples;
  if (ctx->varOrTerm()) {
    auto subject = visitTypesafe(ctx->varOrTerm());
    auto tuples = visitTypesafe(ctx->propertyListPathNotEmpty());
    for (auto& tuple : tuples) {
      triples.push_back(
          TripleWithPropertyPath{subject, tuple.first, tuple.second});
    }
  } else if (ctx->triplesNodePath()) {
    throwCollectionsAndBlankNodePathsNotSupported(ctx->triplesNodePath());
  }
  return triples;
}

// ___________________________________________________________________________
std::optional<PathTuples> SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListPathContext* ctx) {
  if (ctx->propertyListPathNotEmpty()) {
    return visitTypesafe(ctx->propertyListPathNotEmpty());
  } else {
    return std::nullopt;
  }
}

// ___________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbPathContext* ctx) {
  PropertyPath p = visitTypesafe(ctx->path());
  p.computeCanBeNull();
  return p;
}

// ___________________________________________________________________________
Variable SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbSimpleContext* ctx) {
  return visitTypesafe(ctx->var());
}

// ___________________________________________________________________________
ObjectList SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ObjectListPathContext* ctx) {
  // The second parameter is empty because collections and blank not paths,
  // which might add additional triples, are currently not supported.
  return ObjectList{visitVector<VarOrTerm>(ctx->objectPath()), {}};
}

// ___________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathContext* ctx) {
  return visitTypesafe(ctx->pathAlternative());
}

// ___________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathAlternativeContext* ctx) {
  auto paths = visitVector<PropertyPath>(ctx->pathSequence());

  if (paths.size() == 1) {
    return paths[0];
  } else {
    return PropertyPath::makeAlternative(std::move(paths));
  }
}

// ___________________________________________________________________________
PropertyPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PathSequenceContext* ctx) {
  auto paths = visitVector<PropertyPath>(ctx->pathEltOrInverse());

  if (paths.size() == 1) {
    return paths[0];
  } else {
    return PropertyPath::makeSequence(std::move(paths));
  }
}

// ___________________________________________________________________________
ad_utility::sparql_types::VarOrPath SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) {
  if (ctx->verbPath()) {
    return visitTypesafe(ctx->verbPath());
  } else if (ctx->verbSimple()) {
    return visitTypesafe(ctx->verbSimple());
  }
  AD_FAIL()  // Should be unreachable.
}

// ___________________________________________________________________________
PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::PropertyListPathNotEmptyContext* ctx) {
  PathTuples tuples = visitTypesafe(ctx->tupleWithPath());
  vector<PathTuples> tuplesWithoutPaths =
      visitVector<PathTuples>(ctx->tupleWithoutPath());
  for (auto& tuplesWithoutPath : tuplesWithoutPaths) {
    tuples.insert(tuples.end(), tuplesWithoutPath.begin(),
                  tuplesWithoutPath.end());
  }
  return tuples;
}

// ___________________________________________________________________________
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
    auto iri = visit(ctx->iri()).as<std::string>();
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
VarOrTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::GraphNodePathContext* ctx) {
  if (ctx->varOrTerm()) {
    return visitTypesafe(ctx->varOrTerm());
  } else if (ctx->triplesNodePath()) {
    throwCollectionsAndBlankNodePathsNotSupported(ctx->triplesNodePath());
    AD_FAIL()  // Should be unreachable.
  }
  AD_FAIL()  // Should be unreachable.
}

// ____________________________________________________________________________________
VarOrTerm SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::VarOrTermContext* ctx) {
  if (ctx->var()) {
    return VarOrTerm{ctx->var()->accept(this).as<Variable>()};
  }
  if (ctx->graphTerm()) {
    return VarOrTerm{ctx->graphTerm()->accept(this).as<GraphTerm>()};
  }

  // invalid grammar
  AD_CHECK(false);
}

// ____________________________________________________________________________________
ObjectList SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::ObjectListContext* ctx) {
  Objects objects;
  Triples additionalTriples;
  auto objectContexts = ctx->objectR();
  for (auto& objectContext : objectContexts) {
    auto graphNode = objectContext->accept(this).as<Node>();
    appendVector(additionalTriples, std::move(graphNode.second));
    objects.push_back(std::move(graphNode.first));
  }
  return ObjectList{std::move(objects), std::move(additionalTriples)};
}

// ___________________________________________________________________________
PathTuples joinPredicateAndObject(VarOrPath predicate, ObjectList objectList) {
  PathTuples tuples;
  for (auto& object : objectList.first) {
    // TODO The fulltext index should perform the splitting of its keywords,
    //  and not the SparqlParser.
    if (PropertyPath* path = std::get_if<PropertyPath>(&predicate)) {
      if (path->asString() == CONTAINS_WORD_PREDICATE ||
          // TODO _NS no longer needed?
          path->asString() == CONTAINS_WORD_PREDICATE_NS) {
        if (const Literal* literal =
                unwrapVariant<VarOrTerm, GraphTerm, Literal>(object)) {
          object = Literal{stripAndLowercaseKeywordLiteral(literal->literal())};
        }
      }
    }
    tuples.push_back({predicate, std::move(object)});
  }
  return tuples;
}

PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TupleWithoutPathContext* ctx) {
  VarOrPath predicate = visitTypesafe(ctx->verbPathOrSimple());
  ObjectList objectList = visitTypesafe(ctx->objectList());
  return joinPredicateAndObject(predicate, objectList);
}

PathTuples SparqlQleverVisitor::visitTypesafe(
    SparqlAutomaticParser::TupleWithPathContext* ctx) {
  VarOrPath predicate = visitTypesafe(ctx->verbPathOrSimple());
  ObjectList objectList = visitTypesafe(ctx->objectListPath());
  return joinPredicateAndObject(predicate, objectList);
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
