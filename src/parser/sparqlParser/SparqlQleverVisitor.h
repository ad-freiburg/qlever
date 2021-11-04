
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
#include "../../util/StringUtils.h"
#include "../ParsedQuery.h"
#include "../RdfEscaping.h"
#include "antlr4-runtime.h"
#include "generated/SparqlAutomaticVisitor.h"

class SparqlParseException : public std::exception {
  string _message;

 public:
  SparqlParseException(std::string message) : _message{std::move(message)} {}
  const char* what() const noexcept override { return _message.c_str(); }
};

/**
 * This class provides an empty implementation of SparqlVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the
 * available methods.
 */
class SparqlQleverVisitor : public SparqlAutomaticVisitor {
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

 private:
  // For the unit tests
  PrefixMap& prefixMap() { return _prefixMap; }
  FRIEND_TEST(SparqlParser, Prefix);

  PrefixMap _prefixMap{{":", "<>"}};

 public:
  // ___________________________________________________________________________
  antlrcpp::Any visitQuery(SparqlAutomaticParser::QueryContext* ctx) override {
    // The prologue (BASE and PREFIX declarations)  only affects the internal
    // state of the visitor.
    visitPrologue(ctx->prologue());
    if (ctx->selectQuery()) {
      return visitSelectQuery(ctx->selectQuery());
    } else {
      throw SparqlParseException{"QLever only supports select queries"};
    }
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
    _prefixMap[":"] = visitIriref(ctx->iriref()).as<string>();
    return nullptr;
  }

  // ___________________________________________________________________________
  antlrcpp::Any visitPrefixDecl(
      SparqlAutomaticParser::PrefixDeclContext* ctx) override {
    _prefixMap[ctx->PNAME_NS()->getText()] =
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAlias(SparqlAutomaticParser::AliasContext* ctx) override {
    // A SPARQL alias has only one child, namely the contents within
    // parentheses.
    return visitChildren(ctx);
  }

  antlrcpp::Any visitAliasWithouBrackes(
      [[maybe_unused]] SparqlAutomaticParser::AliasWithouBrackesContext* ctx)
      override {
    auto expressionPtr =
        std::move(ctx->expression()->accept(this).as<ExpressionPtr>());
    auto wrapper =
        sparqlExpression::SparqlExpressionPimpl{std::move(expressionPtr)};
    return ParsedQuery::Alias{std::move(wrapper), ctx->var()->getText()};
  }

  antlrcpp::Any visitConstructQuery(
      SparqlAutomaticParser::ConstructQueryContext* ctx) override {
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGroupCondition(
      SparqlAutomaticParser::GroupConditionContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOrderCondition(
      SparqlAutomaticParser::OrderConditionContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitLimitOffsetClauses(
      SparqlAutomaticParser::LimitOffsetClausesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitLimitClause(
      SparqlAutomaticParser::LimitClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitOffsetClause(
      SparqlAutomaticParser::OffsetClauseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitValuesClause(
      SparqlAutomaticParser::ValuesClauseContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInlineData(
      SparqlAutomaticParser::InlineDataContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlock(
      SparqlAutomaticParser::DataBlockContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInlineDataOneVar(
      SparqlAutomaticParser::InlineDataOneVarContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInlineDataFull(
      SparqlAutomaticParser::InlineDataFullContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlockSingle(
      SparqlAutomaticParser::DataBlockSingleContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitDataBlockValue(
      SparqlAutomaticParser::DataBlockValueContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitArgList(
      SparqlAutomaticParser::ArgListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitExpressionList(
      SparqlAutomaticParser::ExpressionListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTemplate(
      SparqlAutomaticParser::ConstructTemplateContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitConstructTriples(
      SparqlAutomaticParser::ConstructTriplesContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesSameSubject(
      SparqlAutomaticParser::TriplesSameSubjectContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyList(
      SparqlAutomaticParser::PropertyListContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPropertyListNotEmpty(
      SparqlAutomaticParser::PropertyListNotEmptyContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerb(SparqlAutomaticParser::VerbContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitObjectList(
      SparqlAutomaticParser::ObjectListContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbSimple(
      SparqlAutomaticParser::VerbSimpleContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVerbPathOrSimple(
      SparqlAutomaticParser::VerbPathOrSimpleContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathAlternative(
      SparqlAutomaticParser::PathAlternativeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathSequence(
      SparqlAutomaticParser::PathSequenceContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathElt(
      SparqlAutomaticParser::PathEltContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathEltOrInverse(
      SparqlAutomaticParser::PathEltOrInverseContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathMod(
      SparqlAutomaticParser::PathModContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathPrimary(
      SparqlAutomaticParser::PathPrimaryContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathNegatedPropertySet(
      SparqlAutomaticParser::PathNegatedPropertySetContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPathOneInPropertySet(
      SparqlAutomaticParser::PathOneInPropertySetContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitInteger(
      SparqlAutomaticParser::IntegerContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitTriplesNode(
      SparqlAutomaticParser::TriplesNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitBlankNodePropertyList(
      SparqlAutomaticParser::BlankNodePropertyListContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitCollectionPath(
      SparqlAutomaticParser::CollectionPathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphNode(
      SparqlAutomaticParser::GraphNodeContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphNodePath(
      SparqlAutomaticParser::GraphNodePathContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVarOrTerm(
      SparqlAutomaticParser::VarOrTermContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVarOrIri(
      SparqlAutomaticParser::VarOrIriContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitVar(SparqlAutomaticParser::VarContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitGraphTerm(
      SparqlAutomaticParser::GraphTermContext* ctx) override {
    return visitChildren(ctx);
  }

  antlrcpp::Any visitExpression(
      SparqlAutomaticParser::ExpressionContext* ctx) override {
    return visitChildren(ctx);
  }

  template <typename T>
  std::vector<ExpressionPtr> visitExpressionChildren(
      const std::vector<T*>& childContexts) {
    std::vector<ExpressionPtr> children;
    for (const auto& child : childContexts) {
      children.emplace_back(
          std::move(child->accept(this).template as<ExpressionPtr>()));
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
        visitExpressionChildren(context->conditionalAndExpression());
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
    std::vector<ExpressionPtr> children =
        visitExpressionChildren(context->valueLogical());
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
    std::vector<ExpressionPtr> children =
        visitExpressionChildren(context->multiplicativeExpression());
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
    std::vector<ExpressionPtr> children =
        visitExpressionChildren(context->unaryExpression());
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
    } else {
      throw SparqlParseException{
          "aggregates like COUNT are the only 'builtInCalls' that are "
          "supported by this parser"};
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
      throw SparqlParseException{
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
    if (ad_utility::getLowercase(children[0]->getText()) == "count") {
      return ExpressionPtr{std::make_unique<sparqlExpression::CountExpression>(
          distinct, std::move(childExpression))};
    } else if (ad_utility::getLowercase(children[0]->getText()) == "sum") {
      return ExpressionPtr{std::make_unique<sparqlExpression::SumExpression>(
          distinct, std::move(childExpression))};
    } else if (ad_utility::getLowercase(children[0]->getText()) == "max") {
      return ExpressionPtr{std::make_unique<sparqlExpression::MaxExpression>(
          distinct, std::move(childExpression))};
    } else if (ad_utility::getLowercase(children[0]->getText()) == "min") {
      return ExpressionPtr{std::make_unique<sparqlExpression::MinExpression>(
          distinct, std::move(childExpression))};
    } else if (ad_utility::getLowercase(children[0]->getText()) == "avg") {
      return ExpressionPtr{std::make_unique<sparqlExpression::AvgExpression>(
          distinct, std::move(childExpression))};
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

      return ExpressionPtr{
          std::make_unique<sparqlExpression::GroupConcatExpression>(
              distinct, std::move(childExpression), std::move(separator))};
    } else {
      AD_CHECK(ad_utility::getLowercase(children[0]->getText()) == "sample");
      return ExpressionPtr{std::make_unique<sparqlExpression::SampleExpression>(
          distinct, std::move(childExpression))};
    }
  }

  antlrcpp::Any visitIriOrFunction(
      SparqlAutomaticParser::IriOrFunctionContext* ctx) override {
    if (ctx->argList()) {
      throw SparqlParseException{
          "calls to non-built-in functions in expressions are not supported by "
          "this parser"};
    }

    return ExpressionPtr{
        std::make_unique<sparqlExpression::StringOrIriExpression>(
            ctx->getText())};
    return visitChildren(ctx);
  }

  antlrcpp::Any visitRdfLiteral(
      SparqlAutomaticParser::RdfLiteralContext* ctx) override {
    return visitChildren(ctx);
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
    return visitChildren(ctx);
  }

  antlrcpp::Any visitPnameLn(
      SparqlAutomaticParser::PnameLnContext* ctx) override {
    string text = ctx->getText();
    auto pos = text.find(':');
    auto pnameNS = text.substr(0, pos + 1);
    auto pnLocal = text.substr(pos + 1);
    if (!_prefixMap.contains(pnameNS)) {
      // TODO<joka921> : proper name
      throw SparqlParseException{
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
    auto prefix = ctx->getText();
    if (!_prefixMap.contains(prefix)) {
      // TODO<joka921> : proper name
      throw SparqlParseException{
          ""
          "Prefix " +
          prefix + " was not registered using a PREFIX declaration"};
    }
    return _prefixMap[prefix];
  }
};

/*
namespace SparqlAutomaticParserHelpers {

struct ParserAndVisitor {
 private:
  string input;
  antlr4::ANTLRInputStream stream{input};
  SparqlAutomaticLexer lexer{&stream};
  antlr4::CommonTokenStream tokens{&lexer};

 public:
  SparqlAutomaticParser parser{&tokens};
  SparqlQleverVisitor visitor;
  explicit ParserAndVisitor(string toParse) : input{std::move(toParse)} {}
  explicit ParserAndVisitor(string toParse, SparqlQleverVisitor::PrefixMap
prefixMap) : input{std::move(toParse)}, visitor{std::move(prefixMap)} {}
};

//
______________________________________________________________________________
std::pair<SparqlQleverVisitor::PrefixMap, size_t> parsePrologue(const string&
input) { ParserAndVisitor p{input}; auto context = p.parser.prologue(); auto
parsedSize = context->getText().size(); p.visitor.visitPrologue(context); const
auto& constVisitor = p.visitor; return {constVisitor.prefixMap(), parsedSize};
}

// _____________________________________________________________________________
std::pair<string, size_t> parseIri(const string& input,
SparqlQleverVisitor::PrefixMap prefixMap) { ParserAndVisitor p{input,
std::move(prefixMap)}; auto context = p.parser.iri(); auto parsedSize =
context->getText().size(); auto resultString =
p.visitor.visitIri(context).as<string>();
  //const auto& constVisitor = p.visitor;
  return {std::move(resultString), parsedSize};
}

}
 */
