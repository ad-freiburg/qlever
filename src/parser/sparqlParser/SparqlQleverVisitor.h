
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include <gtest/gtest.h>

#include "../../engine/sparqlExpressions/AggregateExpression.h"
#include "../../engine/sparqlExpressions/GroupConcatExpression.h"
#include "../../engine/sparqlExpressions/LiteralExpression.h"
#include "../../engine/sparqlExpressions/NaryExpression.h"
#include "../../engine/sparqlExpressions/SparqlExpressionPimpl.h"
//#include "../../engine/sparqlExpressions/RelationalExpression.h"
#include "../../engine/sparqlExpressions/SampleExpression.h"
#include "../../util/HashMap.h"
#include "../../util/OverloadCallOperator.h"
#include "../../util/StringUtils.h"
#include "../ParsedQuery.h"
#include "../RdfEscaping.h"
#include "../data/BlankNode.h"
#include "../data/Iri.h"
#include "../data/Types.h"
#include "../data/VarOrTerm.h"
#include "antlr4-runtime.h"
#include "generated/SparqlAutomaticVisitor.h"

template <typename T>
class Reversed {
  T& _iterable;

 public:
  explicit Reversed(T& iterable) : _iterable(iterable) {}

  auto begin() { return _iterable.rbegin(); };

  auto end() { return _iterable.rend(); }
};

/**
 * This class provides an empty implementation of SparqlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class SparqlQleverVisitor : public SparqlAutomaticVisitor {
  using Objects = ad_utility::sparql_types::Objects;
  using Tuples = ad_utility::sparql_types::Tuples;
  using Triples = ad_utility::sparql_types::Triples;
  using Node = ad_utility::sparql_types::Node;
  using ObjectList = ad_utility::sparql_types::ObjectList;
  using PropertyList = ad_utility::sparql_types::PropertyList;
  size_t _blankNodeCounter = 0;

 public:
  using PrefixMap = ad_utility::HashMap<string, string>;
  const PrefixMap& prefixMap() const { return _prefixMap; }
  SparqlQleverVisitor() = default;
  SparqlQleverVisitor(PrefixMap prefixMap) : _prefixMap{std::move(prefixMap)} {}
  using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;

  // The inherited default behavior of `visitChildren` does not work with
  // move-only types like `SparqlExpression::Ptr`. This overriding
  // implementation adds std::move, but is otherwise the same as the default.
  antlrcpp::Any visitChildren(antlr4::tree::ParseTree* node) override {
    antlrcpp::Any result = nullptr;
    size_t n = node->children.size();
    for (size_t i = 0; i < n; i++) {
      antlrcpp::Any childResult = node->children[i]->accept(this);
      result = std::move(childResult);
    }

    return result;
  }

  void setPrefixMapManually(PrefixMap map) { _prefixMap = std::move(map); }

 private:
  // For the unit tests
  PrefixMap& prefixMap() { return _prefixMap; }
  FRIEND_TEST(SparqlParser, Prefix);

  PrefixMap _prefixMap{{"", "<>"}};

  template <typename T>
  void appendVector(std::vector<T>& destination, std::vector<T>&& source) {
    destination.insert(destination.end(),
                       std::make_move_iterator(source.begin()),
                       std::make_move_iterator(source.end()));
  }

  BlankNode newBlankNode() {
    std::string label = std::to_string(_blankNodeCounter);
    _blankNodeCounter++;
    // true means automatically generated
    return {true, std::move(label)};
  }

  // Process an IRI function call. This is used in both `visitFunctionCall` and
  // `visitIriOrFunction`.
  antlrcpp::Any processIriFunctionCall(const std::string& iri,
                                       std::vector<ExpressionPtr> argList);

 public:
  // ___________________________________________________________________________
  sparqlExpression::SparqlExpressionPimpl makeExpressionPimpl(
      antlrcpp::Any any) {
    return sparqlExpression::SparqlExpressionPimpl{
        std::move(any.as<ExpressionPtr>())};
  }
  // ___________________________________________________________________________
  antlrcpp::Any visitQuery(SparqlAutomaticParser::QueryContext* ctx) override {
    // The prologue (BASE and PREFIX declarations)  only affects the internal
    // state of the visitor.
    visitPrologue(ctx->prologue());
    if (ctx->selectQuery()) {
      return visitSelectQuery(ctx->selectQuery());
    }
    if (ctx->constructQuery()) {
      return ctx->constructQuery()->accept(this);
    }
    throw ParseException{"QLever only supports select and construct queries"};
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitPrologue(
      SparqlAutomaticParser::PrologueContext* ctx) override {
    // Default implementation is ok here, simply handle all PREFIX and BASE
    // declarations.
    return visitChildren(ctx);
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitBaseDecl(
      SparqlAutomaticParser::BaseDeclContext* ctx) override {
    _prefixMap[""] = visitIriref(ctx->iriref()).as<string>();
    return nullptr;
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitPrefixDecl(
      SparqlAutomaticParser::PrefixDeclContext* ctx) override {
    auto text = ctx->PNAME_NS()->getText();
    // Strip trailing ':'.
    _prefixMap[text.substr(0, text.length() - 1)] =
        visitIriref(ctx->iriref()).as<string>();
    return nullptr;
  }

  antlrcpp::Any visitSelectQuery(
      SparqlAutomaticParser::SelectQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSubSelect(
      SparqlAutomaticParser::SubSelectContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSelectClause(
      SparqlAutomaticParser::SelectClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::SelectClause visitTypesafe(
      SparqlAutomaticParser::SelectClauseContext* ctx);

  antlrcpp::Any visitVarOrAlias(
      SparqlAutomaticParser::VarOrAliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::variant<Variable, ParsedQuery::Alias> visitTypesafe(
      SparqlAutomaticParser::VarOrAliasContext* ctx);

  antlrcpp::Any visitAlias(SparqlAutomaticParser::AliasContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::Alias visitTypesafe(SparqlAutomaticParser::AliasContext* ctx);

  antlrcpp::Any visitAliasWithoutBrackets(
      SparqlAutomaticParser::AliasWithoutBracketsContext* ctx) override {
    return visitTypesafe(ctx);
  }

  ParsedQuery::Alias visitTypesafe(
      SparqlAutomaticParser::AliasWithoutBracketsContext* ctx);

  antlrcpp::Any visitConstructQuery(
      SparqlAutomaticParser::ConstructQueryContext* ctx) override {
    if (!ctx->datasetClause().empty()) {
      throw ParseException{"Datasets are not supported"};
    }
    if (ctx->constructTemplate()) {
      return ctx->constructTemplate()->accept(this);
    }
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDescribeQuery(
      SparqlAutomaticParser::DescribeQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAskQuery(
      SparqlAutomaticParser::AskQueryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDatasetClause(
      SparqlAutomaticParser::DatasetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDefaultGraphClause(
      SparqlAutomaticParser::DefaultGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNamedGraphClause(
      SparqlAutomaticParser::NamedGraphClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSourceSelector(
      SparqlAutomaticParser::SourceSelectorContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitWhereClause(
      SparqlAutomaticParser::WhereClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitSolutionModifier(
      SparqlAutomaticParser::SolutionModifierContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupClause(
      SparqlAutomaticParser::GroupClauseContext* ctx) override {
    return visitVector<GroupKey>(ctx->groupCondition());
  }

  antlrcpp::Any visitGroupCondition(
      SparqlAutomaticParser::GroupConditionContext* ctx) override {
    if (ctx->var() && !ctx->expression()) {
      return GroupKey{Variable{ctx->var()->getText()}};
    } else if (ctx->builtInCall() || ctx->functionCall()) {
      // builtInCall and functionCall are both also an Expression
      auto subCtx =
          (ctx->builtInCall() ? (antlr4::tree::ParseTree*)ctx->builtInCall()
                              : (antlr4::tree::ParseTree*)ctx->functionCall());
      auto expr = makeExpressionPimpl(visit(subCtx));
      expr.setDescriptor(ctx->getText());
      return GroupKey{std::move(expr)};
    } else if (ctx->expression()) {
      auto expr = makeExpressionPimpl(visit(ctx->expression()));
      if (ctx->AS() && ctx->var()) {
        return GroupKey{
            ParsedQuery::Alias{std::move(expr), ctx->var()->getText()}};
      } else {
        return GroupKey{std::move(expr)};
      }
    }
    AD_FAIL();  // Should be unreachable.
  }

  antlrcpp::Any visitHavingClause(
      SparqlAutomaticParser::HavingClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitHavingCondition(
      SparqlAutomaticParser::HavingConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOrderClause(
      SparqlAutomaticParser::OrderClauseContext* ctx) override {
    return visitVector<OrderKey>(ctx->orderCondition());
  }

  antlrcpp::Any visitOrderCondition(
      SparqlAutomaticParser::OrderConditionContext* ctx) override {
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

  antlrcpp::Any visitLimitOffsetClauses(
      SparqlAutomaticParser::LimitOffsetClausesContext* ctx) override {
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

  antlrcpp::Any visitLimitClause(
      SparqlAutomaticParser::LimitClauseContext* ctx) override {
    return visitInteger(ctx->integer());
  }

  antlrcpp::Any visitOffsetClause(
      SparqlAutomaticParser::OffsetClauseContext* ctx) override {
    return visitInteger(ctx->integer());
  }

  antlrcpp::Any visitTextLimitClause(
      SparqlAutomaticParser::TextLimitClauseContext* ctx) override {
    return visitInteger(ctx->integer());
  }

  antlrcpp::Any visitValuesClause(
      SparqlAutomaticParser::ValuesClauseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::optional<GraphPatternOperation::Values> visitTypesafe(
      SparqlAutomaticParser::ValuesClauseContext* ctx) {
    if (ctx->dataBlock()) {
      return visitTypesafe(ctx->dataBlock());
    } else {
      return std::nullopt;
    }
  }

  antlrcpp::Any visitTriplesTemplate(
      SparqlAutomaticParser::TriplesTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPattern(
      SparqlAutomaticParser::GroupGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupGraphPatternSub(
      SparqlAutomaticParser::GroupGraphPatternSubContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesBlock(
      SparqlAutomaticParser::TriplesBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphPatternNotTriples(
      SparqlAutomaticParser::GraphPatternNotTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOptionalGraphPattern(
      SparqlAutomaticParser::OptionalGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphGraphPattern(
      SparqlAutomaticParser::GraphGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitServiceGraphPattern(
      SparqlAutomaticParser::ServiceGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBind(SparqlAutomaticParser::BindContext* ctx) override {
    auto wrapper = makeExpressionPimpl(visit(ctx->expression()));
    return GraphPatternOperation::Bind{std::move(wrapper),
                                       ctx->var()->getText()};
  }

  antlrcpp::Any visitInlineData(
      SparqlAutomaticParser::InlineDataContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlock(
      SparqlAutomaticParser::DataBlockContext* ctx) override {
    return visitTypesafe(ctx);
  }

  GraphPatternOperation::Values visitTypesafe(
      SparqlAutomaticParser::DataBlockContext* ctx) {
    if (ctx->inlineDataOneVar()) {
      return GraphPatternOperation::Values{
          std::move(visit(ctx->inlineDataOneVar()).as<SparqlValues>())};
    } else if (ctx->inlineDataFull()) {
      return GraphPatternOperation::Values{
          std::move(visit(ctx->inlineDataFull()).as<SparqlValues>())};
    }
    AD_FAIL()  // Should be unreachable.
  }

  antlrcpp::Any visitInlineDataOneVar(
      SparqlAutomaticParser::InlineDataOneVarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(
      SparqlAutomaticParser::InlineDataOneVarContext* ctx) {
    SparqlValues values;
    auto var = visit(ctx->var()).as<Variable>();
    values._variables.push_back(var.name());
    if (ctx->dataBlockValue().empty())
      throw ParseException(
          "No values were specified in Values "
          "clause. This is not supported by QLever. Got: " +
          ctx->getText());
    for (auto& dataBlockValue : ctx->dataBlockValue()) {
      values._values.push_back(
          {std::move(visit(dataBlockValue).as<std::string>())});
    }
    return values;
  }

  antlrcpp::Any visitInlineDataFull(
      SparqlAutomaticParser::InlineDataFullContext* ctx) override {
    return visitTypesafe(ctx);
  }

  SparqlValues visitTypesafe(
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
      values._variables.push_back(visit(var).as<Variable>().name());
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

  antlrcpp::Any visitDataBlockSingle(
      SparqlAutomaticParser::DataBlockSingleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  vector<std::string> visitTypesafe(
      SparqlAutomaticParser::DataBlockSingleContext* ctx) {
    if (ctx->NIL())
      throw ParseException(
          "No values were specified in DataBlock."
          "This is not supported by QLever. Got: " +
          ctx->getText());
    return visitVector<std::string>(ctx->dataBlockValue());
  }

  antlrcpp::Any visitDataBlockValue(
      SparqlAutomaticParser::DataBlockValueContext* ctx) override {
    return visitTypesafe(ctx);
  }

  std::string visitTypesafe(SparqlAutomaticParser::DataBlockValueContext* ctx) {
    // Return a string
    if (ctx->iri()) {
      return visit(ctx->iri()).as<std::string>();
    } else if (ctx->rdfLiteral()) {
      return visit(ctx->rdfLiteral()).as<std::string>();
    } else if (ctx->numericLiteral()) {
      // TODO implement
      throw ParseException(
          "Numbers in values clauses are not supported. Got: " +
          ctx->getText() + ".");
    } else if (ctx->booleanLiteral()) {
      // TODO implement
      throw ParseException(
          "Booleans in values clauses are not supported. Got: " +
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

  antlrcpp::Any visitMinusGraphPattern(
      SparqlAutomaticParser::MinusGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupOrUnionGraphPattern(
      SparqlAutomaticParser::GroupOrUnionGraphPatternContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFilterR(
      SparqlAutomaticParser::FilterRContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstraint(
      SparqlAutomaticParser::ConstraintContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitFunctionCall(
      SparqlAutomaticParser::FunctionCallContext* ctx) override {
    return processIriFunctionCall(
        visitIri(ctx->iri()).as<std::string>(),
        std::move(
            visitArgList(ctx->argList()).as<std::vector<ExpressionPtr>>()));
  }

  antlrcpp::Any visitArgList(
      SparqlAutomaticParser::ArgListContext* ctx) override {
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

  antlrcpp::Any visitExpressionList(
      SparqlAutomaticParser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTemplate(
      SparqlAutomaticParser::ConstructTemplateContext* ctx) override {
    return ctx->constructTriples() ? ctx->constructTriples()->accept(this)
                                   : Triples{};
  }

  antlrcpp::Any visitConstructTriples(
      SparqlAutomaticParser::ConstructTriplesContext* ctx) override {
    auto result = ctx->triplesSameSubject()->accept(this).as<Triples>();
    if (ctx->constructTriples()) {
      auto newTriples = ctx->constructTriples()->accept(this).as<Triples>();
      appendVector(result, std::move(newTriples));
    }
    return result;
  }

  antlrcpp::Any visitTriplesSameSubject(
      SparqlAutomaticParser::TriplesSameSubjectContext* ctx) override {
    Triples triples;
    if (ctx->varOrTerm()) {
      VarOrTerm subject = ctx->varOrTerm()->accept(this).as<VarOrTerm>();
      AD_CHECK(ctx->propertyListNotEmpty());
      auto propertyList =
          ctx->propertyListNotEmpty()->accept(this).as<PropertyList>();
      for (auto& tuple : propertyList.first) {
        triples.push_back({subject, std::move(tuple[0]), std::move(tuple[1])});
      }
      appendVector(triples, std::move(propertyList.second));
    } else if (ctx->triplesNode()) {
      auto tripleNodes = ctx->triplesNode()->accept(this).as<Node>();
      appendVector(triples, std::move(tripleNodes.second));
      AD_CHECK(ctx->propertyList());
      auto propertyList = ctx->propertyList()->accept(this).as<PropertyList>();
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

  antlrcpp::Any visitPropertyList(
      SparqlAutomaticParser::PropertyListContext* ctx) override {
    return ctx->propertyListNotEmpty()
               ? ctx->propertyListNotEmpty()->accept(this)
               : PropertyList{Tuples{}, Triples{}};
  }

  antlrcpp::Any visitPropertyListNotEmpty(
      SparqlAutomaticParser::PropertyListNotEmptyContext* ctx) override {
    Tuples triplesWithoutSubject;
    Triples additionalTriples;
    auto verbs = ctx->verb();
    auto objectLists = ctx->objectList();
    for (size_t i = 0; i < verbs.size(); i++) {
      // TODO use zip-style approach once C++ supports ranges
      auto objectList = objectLists.at(i)->accept(this).as<ObjectList>();
      auto verb = verbs.at(i)->accept(this).as<VarOrTerm>();
      for (auto& object : objectList.first) {
        triplesWithoutSubject.push_back({verb, std::move(object)});
      }
      appendVector(additionalTriples, std::move(objectList.second));
    }
    return PropertyList{std::move(triplesWithoutSubject),
                        std::move(additionalTriples)};
  }

  antlrcpp::Any visitVerb(SparqlAutomaticParser::VerbContext* ctx) override {
    if (ctx->varOrIri()) {
      return ctx->varOrIri()->accept(this);
    }
    if (ctx->getText() == "a") {
      // Special keyword 'a'
      return VarOrTerm{
          GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"}}};
    }
    throw ParseException{"Invalid verb "s + ctx->getText()};
  }

  antlrcpp::Any visitObjectList(
      SparqlAutomaticParser::ObjectListContext* ctx) override {
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

  antlrcpp::Any visitObjectR(
      SparqlAutomaticParser::ObjectRContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesSameSubjectPath(
      SparqlAutomaticParser::TriplesSameSubjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListPath(
      SparqlAutomaticParser::PropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListPathNotEmpty(
      SparqlAutomaticParser::PropertyListPathNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbPath(
      SparqlAutomaticParser::VerbPathContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::VerbPathContext* ctx) {
    PropertyPath p = visitTypesafe(ctx->path());
    p.computeCanBeNull();
    return p;
  }

  antlrcpp::Any visitVerbSimple(
      SparqlAutomaticParser::VerbSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  Variable visitTypesafe(SparqlAutomaticParser::VerbSimpleContext* ctx) {
    return visitChildren(ctx).as<Variable>();
  }

  antlrcpp::Any visitVerbPathOrSimple(
      SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) {
    if (ctx->verbPath()) {
      return visitTypesafe(ctx->verbPath());
    } else if (ctx->verbSimple()) {
      return PropertyPath::fromVariable(visitTypesafe(ctx->verbSimple()));
    }
    AD_FAIL()  // Should be unreachable.
  }

  antlrcpp::Any visitObjectListPath(
      SparqlAutomaticParser::ObjectListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectPath(
      SparqlAutomaticParser::ObjectPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPath(SparqlAutomaticParser::PathContext* ctx) override {
    // returns PropertyPath
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathContext* ctx) {
    return visitChildren(ctx).as<PropertyPath>();
  }

  antlrcpp::Any visitPathAlternative(
      SparqlAutomaticParser::PathAlternativeContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::PathAlternativeContext* ctx) {
    auto paths = visitVector<PropertyPath>(ctx->pathSequence());

    if (paths.size() == 1) {
      return paths[0];
    } else {
      return PropertyPath::makeAlternative(std::move(paths));
    }
  }

  antlrcpp::Any visitPathSequence(
      SparqlAutomaticParser::PathSequenceContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathSequenceContext* ctx) {
    auto paths = visitVector<PropertyPath>(ctx->pathEltOrInverse());

    if (paths.size() == 1) {
      return paths[0];
    } else {
      return PropertyPath::makeSequence(std::move(paths));
    }
  }

  antlrcpp::Any visitPathElt(
      SparqlAutomaticParser::PathEltContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathEltContext* ctx) {
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

  antlrcpp::Any visitPathEltOrInverse(
      SparqlAutomaticParser::PathEltOrInverseContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::PathEltOrInverseContext* ctx) {
    PropertyPath p = visitTypesafe(ctx->pathElt());

    if (ctx->negationOperator) {
      p = PropertyPath::makeInverse(p);
    }

    return p;
  }

  antlrcpp::Any visitPathMod(
      SparqlAutomaticParser::PathModContext* ctx) override {
    // Handled in visitPathElt.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathPrimary(
      SparqlAutomaticParser::PathPrimaryContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(SparqlAutomaticParser::PathPrimaryContext* ctx) {
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
          std::move("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
    }
    AD_FAIL()  // Should be unreachable.
  }

  antlrcpp::Any visitPathNegatedPropertySet(
      SparqlAutomaticParser::PathNegatedPropertySetContext* ctx) override {
    return visitTypesafe(ctx);
  }

  PropertyPath visitTypesafe(
      SparqlAutomaticParser::PathNegatedPropertySetContext*) {
    throw ParseException(
        "\"!\" inside a property path is not supported by QLever.");
  }

  antlrcpp::Any visitPathOneInPropertySet(
      SparqlAutomaticParser::PathOneInPropertySetContext*) override {
    throw ParseException(
        R"("!" and "^" inside a property path is not supported by QLever.)");
  }

  /// Note that in the SPARQL grammar the INTEGER rule refers to positive
  /// integers without an explicit sign.
  antlrcpp::Any visitInteger(
      SparqlAutomaticParser::IntegerContext* ctx) override {
    try {
      return std::stoull(ctx->getText());
    } catch (const std::out_of_range&) {
      throw ParseException{"Integer " + ctx->getText() +
                           " does not fit"
                           " into 64 bits. This is not supported by QLever."};
    }
  }

  antlrcpp::Any visitTriplesNode(
      SparqlAutomaticParser::TriplesNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyList(
      SparqlAutomaticParser::BlankNodePropertyListContext* ctx) override {
    VarOrTerm var{GraphTerm{newBlankNode()}};
    Triples triples;
    auto propertyList =
        ctx->propertyListNotEmpty()->accept(this).as<PropertyList>();
    for (auto& tuple : propertyList.first) {
      triples.push_back({var, std::move(tuple[0]), std::move(tuple[1])});
    }
    appendVector(triples, std::move(propertyList.second));
    return Node{std::move(var), std::move(triples)};
  }

  antlrcpp::Any visitTriplesNodePath(
      SparqlAutomaticParser::TriplesNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyListPath(
      SparqlAutomaticParser::BlankNodePropertyListPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitCollection(
      SparqlAutomaticParser::CollectionContext* ctx) override {
    Triples triples;
    VarOrTerm nextElement{
        GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}}};
    auto nodes = ctx->graphNode();
    for (auto context : Reversed{nodes}) {
      VarOrTerm currentVar{GraphTerm{newBlankNode()}};
      auto graphNode = context->accept(this).as<Node>();

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

  antlrcpp::Any visitCollectionPath(
      SparqlAutomaticParser::CollectionPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphNode(
      SparqlAutomaticParser::GraphNodeContext* ctx) override {
    if (ctx->varOrTerm()) {
      return Node{ctx->varOrTerm()->accept(this).as<VarOrTerm>(), Triples{}};
    } else if (ctx->triplesNode()) {
      return ctx->triplesNode()->accept(this);
    }
    AD_CHECK(false);
  }

  antlrcpp::Any visitGraphNodePath(
      SparqlAutomaticParser::GraphNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVarOrTerm(
      SparqlAutomaticParser::VarOrTermContext* ctx) override {
    if (ctx->var()) {
      return VarOrTerm{ctx->var()->accept(this).as<Variable>()};
    }
    if (ctx->graphTerm()) {
      return VarOrTerm{ctx->graphTerm()->accept(this).as<GraphTerm>()};
    }

    // invalid grammar
    AD_CHECK(false);
  }

  antlrcpp::Any visitVarOrIri(
      SparqlAutomaticParser::VarOrIriContext* ctx) override {
    if (ctx->var()) {
      return VarOrTerm{ctx->var()->accept(this).as<Variable>()};
    }
    if (ctx->iri()) {
      return VarOrTerm{
          GraphTerm{Iri{ctx->iri()->accept(this).as<std::string>()}}};
    }
    // invalid grammar
    AD_CHECK(false);
  }

  antlrcpp::Any visitVar(SparqlAutomaticParser::VarContext* ctx) override {
    return visitTypesafe(ctx);
  }

  Variable visitTypesafe(SparqlAutomaticParser::VarContext* ctx);

  antlrcpp::Any visitGraphTerm(
      SparqlAutomaticParser::GraphTermContext* ctx) override {
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
      return GraphTerm{Literal{ctx->booleanLiteral()->accept(this).as<bool>()}};
    }
    if (ctx->blankNode()) {
      return GraphTerm{ctx->blankNode()->accept(this).as<BlankNode>()};
    }
    if (ctx->iri()) {
      return GraphTerm{Iri{ctx->iri()->accept(this).as<std::string>()}};
    }
    if (ctx->rdfLiteral()) {
      return GraphTerm{
          Literal{ctx->rdfLiteral()->accept(this).as<std::string>()}};
    }
    if (ctx->NIL()) {
      return GraphTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}};
    }
    AD_CHECK(false);
  }

  antlrcpp::Any visitExpression(
      SparqlAutomaticParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  template <typename Out, typename Ctx>
  std::vector<Out> visitVector(const std::vector<Ctx*>& childContexts) {
    std::vector<Out> children;
    for (const auto& child : childContexts) {
      children.emplace_back(std::move(child->accept(this).template as<Out>()));
    }
    return children;
  }

  std::vector<std::string> visitOperationTags(
      const std::vector<antlr4::tree::ParseTree*>& childContexts,
      const ad_utility::HashSet<string>& allowedTags) {
    std::vector<std::string> operations;

    for (const auto& c : childContexts) {
      if (allowedTags.contains(c->getText())) {
        operations.emplace_back(c->getText());
      }
    }
    return operations;
  }

  antlrcpp::Any visitConditionalOrExpression(
      SparqlAutomaticParser::ConditionalOrExpressionContext* context) override {
    auto childContexts = context->conditionalAndExpression();
    auto children =
        visitVector<ExpressionPtr>(context->conditionalAndExpression());
    AD_CHECK(!children.empty());
    auto result = std::move(children.front());
    using C = sparqlExpression::OrExpression::Children;
    std::for_each(children.begin() + 1, children.end(),
                  [&result](ExpressionPtr& ptr) {
                    result = std::make_unique<sparqlExpression::OrExpression>(
                        C{std::move(result), std::move(ptr)});
                  });
    result->descriptor() = context->getText();
    return result;
  }

  antlrcpp::Any visitConditionalAndExpression(
      SparqlAutomaticParser::ConditionalAndExpressionContext* context)
      override {
    auto children = visitVector<ExpressionPtr>(context->valueLogical());
    AD_CHECK(!children.empty());
    auto result = std::move(children.front());
    using C = sparqlExpression::AndExpression::Children;
    std::for_each(children.begin() + 1, children.end(),
                  [&result](ExpressionPtr& ptr) {
                    result = std::make_unique<sparqlExpression::AndExpression>(
                        C{std::move(result), std::move(ptr)});
                  });
    result->descriptor() = context->getText();
    return result;
  }

  antlrcpp::Any visitValueLogical(
      SparqlAutomaticParser::ValueLogicalContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitRelationalExpression(
      SparqlAutomaticParser::RelationalExpressionContext* ctx) override {
    auto childContexts = ctx->numericExpression();

    if (childContexts.size() == 1) {
      return std::move(
          visitNumericExpression(childContexts[0]).as<ExpressionPtr>());
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

  antlrcpp::Any visitNumericExpression(
      SparqlAutomaticParser::NumericExpressionContext* ctx) override {
    return std::move(visitChildren(ctx).as<ExpressionPtr>());
  }

  template <typename Expr>
  ExpressionPtr createExpression(auto... children) {
    return std::make_unique<Expr>(
        std::array<ExpressionPtr, sizeof...(children)>{std::move(children)...});
  }

  antlrcpp::Any visitAdditiveExpression(
      SparqlAutomaticParser::AdditiveExpressionContext* context) override {
    auto children =
        visitVector<ExpressionPtr>(context->multiplicativeExpression());
    auto opTypes = visitOperationTags(context->children, {"+", "-"});

    if (!context->strangeMultiplicativeSubexprOfAdditive().empty()) {
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
  antlrcpp::Any visitStrangeMultiplicativeSubexprOfAdditive(
      SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext*
          context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitMultiplicativeExpression(
      SparqlAutomaticParser::MultiplicativeExpressionContext* context)
      override {
    auto children = visitVector<ExpressionPtr>(context->unaryExpression());
    auto opTypes = visitOperationTags(context->children, {"*", "/"});

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

  antlrcpp::Any visitUnaryExpression(
      SparqlAutomaticParser::UnaryExpressionContext* context) override {
    auto child = std::move(visitPrimaryExpression(context->primaryExpression())
                               .as<ExpressionPtr>());
    if (context->children[0]->getText() == "-") {
      return createExpression<sparqlExpression::UnaryMinusExpression>(
          std::move(child));
    } else if (context->getText() == "!") {
      return createExpression<sparqlExpression::UnaryNegateExpression>(
          std::move(child));
    } else {
      // no sign or an explicit '+'
      return child;
    }
  }

  antlrcpp::Any visitPrimaryExpression(
      SparqlAutomaticParser::PrimaryExpressionContext* context) override {
    if (context->builtInCall()) {
      return context->builtInCall()->accept(this);
    }
    if (context->rdfLiteral()) {
      // TODO<joka921> : handle strings with value datatype that are
      // not in the knowledge base correctly.
      return ExpressionPtr{
          std::make_unique<sparqlExpression::StringOrIriExpression>(
              context->rdfLiteral()->getText())};
    }
    if (context->iriOrFunction()) {
      return visitIriOrFunction(context->iriOrFunction());
    }

    if (context->brackettedExpression()) {
      return visitBrackettedExpression(context->brackettedExpression());
    }

    // TODO<joka921> Refactor s.t. try/catch becomes if/else here
    if (context->numericLiteral()) {
      auto literalAny = visitNumericLiteral(context->numericLiteral());
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
        return ExpressionPtr{
            std::make_unique<sparqlExpression::DoubleExpression>(
                static_cast<double>(intLiteral))};
      } catch (...) {
      }
      AD_CHECK(false);
    }

    if (context->booleanLiteral()) {
      auto b = visitBooleanLiteral(context->booleanLiteral()).as<bool>();
      return ExpressionPtr{
          std::make_unique<sparqlExpression::BoolExpression>(b)};
    }

    if (context->var()) {
      sparqlExpression::Variable v;
      v._variable = context->var()->getText();
      return ExpressionPtr{
          std::make_unique<sparqlExpression::VariableExpression>(v)};
    }
    // We should have returned by now
    AD_CHECK(false);
  }

  antlrcpp::Any visitBrackettedExpression(
      SparqlAutomaticParser::BrackettedExpressionContext* context) override {
    return visitExpression(context->expression());
  }

  antlrcpp::Any visitBuiltInCall(
      [[maybe_unused]] SparqlAutomaticParser::BuiltInCallContext* context)
      override {
    if (context->aggregate()) {
      return context->aggregate()->accept(this);
      // TODO: Implement built-in calls according to the following examples.
      //
      // } else if (ad_utility::getLowercase(context->children[0]->getText()) ==
      //            "sqr") {
      //   if (context->expression().size() != 1) {
      //     throw ParseException{"SQR needs one argument"};
      //   }
      //   auto children = visitVector<ExpressionPtr>(context->expression());
      //   return createExpression<sparqlExpression::SquareExpression>(
      //       std::move(children[0]));
      // } else if (ad_utility::getLowercase(context->children[0]->getText()) ==
      //            "dist") {
      //   if (context->expression().size() != 2) {
      //     throw ParseException{"DIST needs two arguments"};
      //   }
      //   auto children = visitVector<ExpressionPtr>(context->expression());
      //   return createExpression<sparqlExpression::DistExpression>(
      //       std::move(children[0]), std::move(children[1]));
    } else {
      throw ParseException{
          "Built-in function not yet implemented (only aggregates like COUNT "
          "so far)"};
    }
  }

  antlrcpp::Any visitRegexExpression(
      SparqlAutomaticParser::RegexExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitSubstringExpression(
      SparqlAutomaticParser::SubstringExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitStrReplaceExpression(
      SparqlAutomaticParser::StrReplaceExpressionContext* context) override {
    return visitChildren(context);
  }

  antlrcpp::Any visitExistsFunc(
      SparqlAutomaticParser::ExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNotExistsFunc(
      SparqlAutomaticParser::NotExistsFuncContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAggregate(
      SparqlAutomaticParser::AggregateContext* context) override {
    // the only case that there is no child expression is COUNT(*), so we can
    // check this outside the if below.
    if (!context->expression()) {
      throw ParseException{
          "This parser currently doesn't support COUNT(*), please specify an "
          "explicit expression for the COUNT"};
    }
    auto childExpression =
        std::move(context->expression()->accept(this).as<ExpressionPtr>());
    auto children = context->children;
    bool distinct = false;
    for (const auto& child : children) {
      if (ad_utility::getLowercase(child->getText()) == "distinct") {
        distinct = true;
      }
    }
    auto makePtr = [&]<typename ExpressionType>(auto&&... additionalArgs) {
      ExpressionPtr result{std::make_unique<ExpressionType>(
          distinct, std::move(childExpression), AD_FWD(additionalArgs)...)};
      result->descriptor() = context->getText();
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
      if (context->string()) {
        separator = context->string()->getText();
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

  antlrcpp::Any visitIriOrFunction(
      SparqlAutomaticParser::IriOrFunctionContext* ctx) override {
    // Case 1: Just an IRI.
    if (ctx->argList() == nullptr) {
      return ExpressionPtr{
          std::make_unique<sparqlExpression::StringOrIriExpression>(
              ctx->getText())};
    }
    // Case 2: Function call, where the function name is an IRI.
    return processIriFunctionCall(
        visitIri(ctx->iri()).as<std::string>(),
        std::move(
            visitArgList(ctx->argList()).as<std::vector<ExpressionPtr>>()));
  }

  antlrcpp::Any visitRdfLiteral(
      SparqlAutomaticParser::RdfLiteralContext* ctx) override {
    return ctx->getText();
  }

  antlrcpp::Any visitNumericLiteral(
      SparqlAutomaticParser::NumericLiteralContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitNumericLiteralUnsigned(
      SparqlAutomaticParser::NumericLiteralUnsignedContext* ctx) override {
    if (ctx->INTEGER()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralPositive(
      SparqlAutomaticParser::NumericLiteralPositiveContext* ctx) override {
    if (ctx->INTEGER_POSITIVE()) {
      return std::stoull(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitNumericLiteralNegative(
      SparqlAutomaticParser::NumericLiteralNegativeContext* ctx) override {
    if (ctx->INTEGER_NEGATIVE()) {
      return std::stoll(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  }

  antlrcpp::Any visitBooleanLiteral(
      SparqlAutomaticParser::BooleanLiteralContext* ctx) override {
    return ctx->getText() == "true";
  }

  antlrcpp::Any visitString(
      SparqlAutomaticParser::StringContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitIri(SparqlAutomaticParser::IriContext* ctx) override {
    string langtag = ctx->LANGTAG() ? ctx->LANGTAG()->getText() + '@' : "";
    if (ctx->iriref()) {
      return langtag + visitIriref(ctx->iriref()).as<string>();
    } else {
      AD_CHECK(ctx->prefixedName())
      return langtag + visitPrefixedName(ctx->prefixedName()).as<string>();
    }
  }

  antlrcpp::Any visitIriref(
      SparqlAutomaticParser::IrirefContext* ctx) override {
    return RdfEscaping::unescapeIriref(ctx->getText());
  }

  antlrcpp::Any visitPrefixedName(
      SparqlAutomaticParser::PrefixedNameContext* ctx) override {
    if (ctx->pnameLn()) {
      return visitPnameLn(ctx->pnameLn());
    } else {
      AD_CHECK(ctx->pnameNs());
      return visitPnameNs(ctx->pnameNs());
    }
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNode(
      SparqlAutomaticParser::BlankNodeContext* ctx) override {
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

  antlrcpp::Any visitPnameLn(
      SparqlAutomaticParser::PnameLnContext* ctx) override {
    string text = ctx->getText();
    auto pos = text.find(':');
    auto pnameNS = text.substr(0, pos);
    auto pnLocal = text.substr(pos + 1);
    if (!_prefixMap.contains(pnameNS)) {
      // TODO<joka921> : proper name
      throw ParseException{
          ""
          "Prefix " +
          pnameNS + " was not registered using a PREFIX declaration"};
    }
    auto inner = _prefixMap[pnameNS];
    // strip the trailing ">"
    inner = inner.substr(0, inner.size() - 1);
    return inner + RdfEscaping::unescapePrefixedIri(pnLocal) + ">";
  }

  antlrcpp::Any visitPnameNs(
      SparqlAutomaticParser::PnameNsContext* ctx) override {
    auto text = ctx->getText();
    auto prefix = text.substr(0, text.length() - 1);
    if (!_prefixMap.contains(prefix)) {
      // TODO<joka921> : proper name
      throw ParseException{
          ""
          "Prefix " +
          prefix + " was not registered using a PREFIX declaration"};
    }
    return _prefixMap[prefix];
  }
};
