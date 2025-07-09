// Copyright 2021 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "parser/sparqlParser/SparqlQleverVisitor.h"

#include <absl/functional/function_ref.h>
#include <absl/strings/str_split.h>
#include <absl/time/time.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/BlankNodeExpression.h"
#include "engine/sparqlExpressions/CountStarExpression.h"
#include "engine/sparqlExpressions/ExistsExpression.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/RandomExpression.h"
#include "engine/sparqlExpressions/RegexExpression.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/StdevExpression.h"
#include "engine/sparqlExpressions/UuidExpressions.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "parser/MagicServiceIriConstants.h"
#include "parser/MagicServiceQuery.h"
#include "parser/Quads.h"
#include "parser/RdfParser.h"
#include "parser/SparqlParser.h"
#include "parser/SpatialQuery.h"
#include "parser/TokenizerCtre.h"
#include "rdfTypes/Variable.h"
#include "util/StringUtils.h"
#include "util/TransparentFunctors.h"
#include "util/TypeIdentity.h"
#include "util/antlr/GenerateAntlrExceptionMetadata.h"

using namespace ad_utility::sparql_types;
using namespace ad_utility::use_type_identity;
using namespace sparqlExpression;
using namespace updateClause;
using ExpressionPtr = sparqlExpression::SparqlExpression::Ptr;
using SparqlExpressionPimpl = sparqlExpression::SparqlExpressionPimpl;
using SelectClause = parsedQuery::SelectClause;
using GraphPattern = parsedQuery::GraphPattern;
using Bind = parsedQuery::Bind;
using Values = parsedQuery::Values;
using BasicGraphPattern = parsedQuery::BasicGraphPattern;
using GraphPatternOperation = parsedQuery::GraphPatternOperation;
using SparqlValues = parsedQuery::SparqlValues;

using Visitor = SparqlQleverVisitor;
using Parser = SparqlAutomaticParser;

namespace {
const ad_utility::triple_component::Iri a =
    ad_utility::triple_component::Iri::fromIriref(
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
}  // namespace

// _____________________________________________________________________________
BlankNode Visitor::newBlankNode() {
  std::string label = std::to_string(_blankNodeCounter);
  _blankNodeCounter++;
  // true means automatically generated
  return {true, std::move(label)};
}

// _____________________________________________________________________________
GraphTerm Visitor::newBlankNodeOrVariable() {
  if (isInsideConstructTriples_) {
    return GraphTerm{newBlankNode()};
  } else {
    return parsedQuery_.getNewInternalVariable();
  }
}

// _____________________________________________________________________________
std::string Visitor::getOriginalInputForContext(
    const antlr4::ParserRuleContext* context) {
  const auto& fullInput = context->getStart()->getInputStream()->toString();
  size_t posBeg = context->getStart()->getStartIndex();
  size_t posEnd = context->getStop()->getStopIndex();
  // Note that `getUTF8Substring` returns a `std::string_view`. We copy this to
  // a `std::string` because it's not clear whether the original string still
  // exists when the result of this call is used. Not performance-critical.
  return std::string{
      ad_utility::getUTF8Substring(fullInput, posBeg, posEnd - posBeg + 1)};
}

// _____________________________________________________________________________
std::string Visitor::currentTimeAsXsdString() {
  return absl::FormatTime("%Y-%m-%dT%H:%M:%E3S%Ez", absl::Now(),
                          absl::LocalTimeZone());
}

// ___________________________________________________________________________
ExpressionPtr Visitor::processIriFunctionCall(
    const TripleComponent::Iri& iri, std::vector<ExpressionPtr> argList,
    const antlr4::ParserRuleContext* ctx) {
  std::string_view functionName = asStringViewUnsafe(iri.getContent());
  std::string_view prefixName;
  // Helper lambda that checks if `functionName` starts with the given prefix.
  // If yes, remove the prefix from `functionName` and set
  // `prefixName` to the short name of the prefix; see `global/Constants.h`.
  auto checkPrefix = [&functionName, &prefixName](
                         std::pair<std::string_view, std::string_view> prefix) {
    if (functionName.starts_with(prefix.second)) {
      prefixName = prefix.first;
      functionName.remove_prefix(prefix.second.size());
      return true;
    } else {
      return false;
    }
  };

  // Helper lambda that checks the number of arguments and throws an error
  // if it's not right. The `functionName` and `prefixName` are used for the
  // error message.
  auto checkNumArgs = [&argList, &ctx, &functionName,
                       &prefixName](size_t numArgs) {
    static std::array<std::string, 6> wordForNumArgs = {
        "no", "one", "two", "three", "four", "five"};
    if (argList.size() != numArgs) {
      reportError(ctx,
                  absl::StrCat("Function ", prefixName, functionName, " takes ",
                               numArgs < 5 ? wordForNumArgs[numArgs]
                                           : std::to_string(numArgs),
                               numArgs == 1 ? " argument" : " arguments"));
    }
  };

  using namespace sparqlExpression;
  // Create `SparqlExpression` with one child.
  auto createUnary =
      CPP_template_lambda(&argList, &checkNumArgs)(typename F)(F function)(
          requires std::is_invocable_r_v<ExpressionPtr, F, ExpressionPtr>) {
    checkNumArgs(1);  // Check is unary.
    return function(std::move(argList[0]));
  };
  // Create `SparqlExpression` with two children.
  auto createBinary =
      CPP_template_lambda(&argList, &checkNumArgs)(typename F)(F function)(
          requires std::is_invocable_r_v<ExpressionPtr, F, ExpressionPtr,
                                         ExpressionPtr>) {
    checkNumArgs(2);  // Check is binary.
    return function(std::move(argList[0]), std::move(argList[1]));
  };
  // Create `SparqlExpression` with two or three children (currently used for
  // backward-compatible geof:distance function)
  auto createBinaryOrTernary =
      CPP_template_lambda(&argList)(typename F)(F function)(
          requires std::is_invocable_r_v<ExpressionPtr, F, ExpressionPtr,
                                         ExpressionPtr,
                                         std::optional<ExpressionPtr>>) {
    if (argList.size() == 2) {
      return function(std::move(argList[0]), std::move(argList[1]),
                      std::nullopt);
    } else if (argList.size() == 3) {
      return function(std::move(argList[0]), std::move(argList[1]),
                      std::move(argList[2]));
    } else {
      AD_THROW(
          "Incorrect number of arguments: two or optionally three required");
    }
  };

  using Ptr = sparqlExpression::SparqlExpression::Ptr;
  using UnaryFuncTable =
      std::unordered_map<std::string_view, absl::FunctionRef<Ptr(Ptr)>>;
  using BinaryFuncTable =
      std::unordered_map<std::string_view, absl::FunctionRef<Ptr(Ptr, Ptr)>>;

  // Geo functions.
  static const UnaryFuncTable geoUnaryFuncs{
      {"longitude", &makeLongitudeExpression},
      {"latitude", &makeLatitudeExpression},
      {"centroid", &makeCentroidExpression},
      {"envelope", &makeEnvelopeExpression},
      {"geometryType", &makeGeometryTypeExpression}};
  using enum SpatialJoinType;
  static const BinaryFuncTable geoBinaryFuncs{
      {"metricDistance", &makeMetricDistExpression},
      // Geometric relation functions
      {"sfIntersects", &makeGeoRelationExpression<INTERSECTS>},
      {"sfContains", &makeGeoRelationExpression<CONTAINS>},
      {"sfCovers", &makeGeoRelationExpression<COVERS>},
      {"sfCrosses", &makeGeoRelationExpression<CROSSES>},
      {"sfTouches", &makeGeoRelationExpression<TOUCHES>},
      {"sfEquals", &makeGeoRelationExpression<EQUALS>},
      {"sfOverlaps", &makeGeoRelationExpression<OVERLAPS>}};
  if (checkPrefix(GEOF_PREFIX)) {
    if (functionName == "distance") {
      return createBinaryOrTernary(&makeDistWithUnitExpression);
    } else if (geoUnaryFuncs.contains(functionName)) {
      return createUnary(geoUnaryFuncs.at(functionName));
    } else if (geoBinaryFuncs.contains(functionName)) {
      return createBinary(geoBinaryFuncs.at(functionName));
    }
  }

  // Math functions.
  static const UnaryFuncTable mathFuncs{
      {"log", &makeLogExpression},   {"exp", &makeExpExpression},
      {"sqrt", &makeSqrtExpression}, {"sin", &makeSinExpression},
      {"cos", &makeCosExpression},   {"tan", &makeTanExpression},
  };
  if (checkPrefix(MATH_PREFIX)) {
    if (mathFuncs.contains(functionName)) {
      return createUnary(mathFuncs.at(functionName));
    } else if (functionName == "pow") {
      return createBinary(&makePowExpression);
    }
  }

  // XSD conversion functions.
  static const UnaryFuncTable convertFuncs{
      {"integer", &makeConvertToIntExpression},
      {"int", &makeConvertToIntExpression},
      {"decimal", &makeConvertToDecimalExpression},
      {"double", &makeConvertToDoubleExpression},
      // We currently don't have a float type, so we just convert to double.
      {"float", &makeConvertToDoubleExpression},
      {"boolean", &makeConvertToBooleanExpression},
      {"string", &makeConvertToStringExpression},
      {"dateTime", &makeConvertToDateTimeExpression},
      {"date", &makeConvertToDateExpression},
  };
  if (checkPrefix(XSD_PREFIX) && convertFuncs.contains(functionName)) {
    return createUnary(convertFuncs.at(functionName));
  }

  // QLever-internal functions.
  //
  // NOTE: Predicates like `ql:has-predicate` etc. are handled elsewhere.
  if (checkPrefix(QL_PREFIX)) {
    if (functionName == "isGeoPoint") {
      return createUnary(&makeIsGeoPointExpression);
    }
  }

  if (RuntimeParameters().get<"syntax-test-mode">()) {
    // In the syntax test mode we silently create an expression that always
    // returns `UNDEF`.
    return std::make_unique<sparqlExpression::IdExpression>(
        Id::makeUndefined());
  } else {
    // If none of the above matched, report unknown function.
    reportNotSupported(ctx,
                       "Function \""s + iri.toStringRepresentation() + "\" is");
  }
}

void Visitor::addVisibleVariable(Variable var) {
  visibleVariables_.emplace_back(std::move(var));
}

// ___________________________________________________________________________
template <typename List>
PathObjectPairs joinPredicateAndObject(const VarOrPath& predicate,
                                       List objectList) {
  PathObjectPairs tuples;
  tuples.reserve(objectList.first.size());
  for (auto& object : objectList.first) {
    tuples.emplace_back(predicate, std::move(object));
  }
  return tuples;
}

// ___________________________________________________________________________
template <typename Context>
SparqlExpressionPimpl Visitor::visitExpressionPimpl(Context* ctx) {
  return {visit(ctx), getOriginalInputForContext(ctx)};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::QueryContext* ctx) {
  // The prologue (BASE and PREFIX declarations)  only affects the internal
  // state of the visitor.
  visit(ctx->prologue());
  auto query =
      visitAlternative<ParsedQuery>(ctx->selectQuery(), ctx->constructQuery(),
                                    ctx->describeQuery(), ctx->askQuery());

  query.postQueryValuesClause_ = visit(ctx->valuesClause());

  query._originalString = ctx->getStart()->getInputStream()->toString();

  return query;
}

// ____________________________________________________________________________________
void SparqlQleverVisitor::resetStateForMultipleUpdates() {
  // The following fields are not reset:
  // - prefixMap_ and baseIri_: prefixes carry over between chained updates
  // - datasetsAreFixed_: set for the whole request which can contain multiple
  // operations
  // - activeDatasetClauses_: if `datasetsAreFixed_` is true
  _blankNodeCounter = 0;
  numGraphPatterns_ = 0;
  visibleVariables_ = {};
  // When fixed datasets are given for a request (see SPARQL Protocol), these
  // cannot be changed by a SPARQL operation but are also constant for chained
  // updates.
  if (!datasetsAreFixed_) {
    activeDatasetClauses_ = {};
  }
  prologueString_ = {};
  parsedQuery_ = {};
  isInsideConstructTriples_ = false;
}

// ____________________________________________________________________________________
SelectClause Visitor::visit(Parser::SelectClauseContext* ctx) {
  SelectClause select;

  select.distinct_ = static_cast<bool>(ctx->DISTINCT());
  select.reduced_ = static_cast<bool>(ctx->REDUCED());

  if (ctx->asterisk) {
    select.setAsterisk();
  } else {
    select.setSelected(visitVector(ctx->varOrAlias()));
  }
  return select;
}

// ____________________________________________________________________________________
VarOrAlias Visitor::visit(Parser::VarOrAliasContext* ctx) {
  return visitAlternative<VarOrAlias>(ctx->var(), ctx->alias());
}

// ____________________________________________________________________________________
Alias Visitor::visit(Parser::AliasContext* ctx) {
  // A SPARQL alias has only one child, namely the contents within
  // parentheses.
  return visit(ctx->aliasWithoutBrackets());
}

// ____________________________________________________________________________________
Alias Visitor::visit(Parser::AliasWithoutBracketsContext* ctx) {
  return {visitExpressionPimpl(ctx->expression()), visit(ctx->var())};
}

// ____________________________________________________________________________________
parsedQuery::BasicGraphPattern Visitor::toGraphPattern(
    const ad_utility::sparql_types::Triples& triples) {
  parsedQuery::BasicGraphPattern pattern{};
  pattern._triples.reserve(triples.size());
  auto toTripleComponent = [](const auto& item) {
    using T = std::decay_t<decltype(item)>;
    namespace tc = ad_utility::triple_component;
    if constexpr (ad_utility::isSimilar<T, Variable>) {
      return TripleComponent{item};
    } else if constexpr (ad_utility::isSimilar<T, BlankNode>) {
      // Blank Nodes in the pattern are to be treated as internal variables
      // inside WHERE.
      return TripleComponent{
          ParsedQuery::blankNodeToInternalVariable(item.toSparql())};
    } else {
      static_assert(ad_utility::SimilarToAny<T, Literal, Iri>);
      return RdfStringParser<TurtleParser<Tokenizer>>::parseTripleObject(
          item.toSparql());
    }
  };
  auto toPredicate = [](const auto& item) -> VarOrPath {
    using T = std::decay_t<decltype(item)>;
    if constexpr (ad_utility::isSimilar<T, Variable>) {
      return item;
    } else if constexpr (ad_utility::isSimilar<T, Iri>) {
      return PropertyPath::fromIri(
          ad_utility::triple_component::Iri::fromStringRepresentation(
              item.toSparql()));
    } else {
      static_assert(ad_utility::SimilarToAny<T, Literal, BlankNode>);
      // This case can only happen if there's a bug in the SPARQL parser.
      AD_THROW("Literals or blank nodes are not valid predicates.");
    }
  };
  for (const auto& triple : triples) {
    auto subject = std::visit(toTripleComponent, triple.at(0));
    auto predicate = std::visit(toPredicate, triple.at(1));
    auto object = std::visit(toTripleComponent, triple.at(2));
    pattern._triples.emplace_back(std::move(subject), std::move(predicate),
                                  std::move(object));
  }
  return pattern;
}

// ____________________________________________________________________________________
const parsedQuery::DatasetClauses& SparqlQleverVisitor::setAndGetDatasetClauses(
    const std::vector<DatasetClause>& clauses) {
  if (!datasetsAreFixed_) {
    activeDatasetClauses_ = parsedQuery::DatasetClauses::fromClauses(clauses);
  }
  return activeDatasetClauses_;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::ConstructQueryContext* ctx) {
  ParsedQuery query;
  query.datasetClauses_ =
      setAndGetDatasetClauses(visitVector(ctx->datasetClause()));
  if (ctx->constructTemplate()) {
    query._clause = visit(ctx->constructTemplate())
                        .value_or(parsedQuery::ConstructClause{});
    visitWhereClause(ctx->whereClause(), query);
  } else {
    // For `CONSTRUCT WHERE`, the CONSTRUCT template and the WHERE clause are
    // syntactically the same, so we set the flag to true to keep the blank
    // nodes, and convert them into variables during `toGraphPattern`.
    isInsideConstructTriples_ = true;
    auto cleanup =
        absl::Cleanup{[this]() { isInsideConstructTriples_ = false; }};
    query._clause = parsedQuery::ConstructClause{
        visitOptional(ctx->triplesTemplate()).value_or(Triples{})};
    query._rootGraphPattern._graphPatterns.emplace_back(
        toGraphPattern(query.constructClause().triples_));
  }
  query.addSolutionModifiers(visit(ctx->solutionModifier()));

  return query;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::DescribeQueryContext* ctx) {
  auto describeClause = parsedQuery::Describe{};
  auto describedResources = visitVector(ctx->varOrIri());

  // Convert the describe resources (variables or IRIs) from the format that the
  // parser delivers to the one that the `parsedQuery::Describe` struct expects.
  std::vector<Variable> describedVariables;
  for (VarOrIri& resource : describedResources) {
    std::visit(ad_utility::OverloadCallOperator{
                   [&describeClause, &describedVariables](const Variable& var) {
                     describeClause.resources_.emplace_back(var);
                     describedVariables.push_back(var);
                   },
                   [&describeClause](const TripleComponent::Iri& iri) {
                     describeClause.resources_.emplace_back(iri);
                   }},
               resource);
  }

  // Parse the FROM and FROM NAMED clauses.
  describeClause.datasetClauses_ =
      setAndGetDatasetClauses(visitVector(ctx->datasetClause()));

  // Parse the WHERE clause and construct a SELECT query from it. For `DESCRIBE
  // *`, add each visible variable as a resource to describe.
  visitWhereClause(ctx->whereClause(), parsedQuery_);
  if (describedResources.empty()) {
    const auto& visibleVariables =
        parsedQuery_.selectClause().getVisibleVariables();
    ql::ranges::copy(visibleVariables,
                     std::back_inserter(describeClause.resources_));
    describedVariables = visibleVariables;
  }
  auto& selectClause = parsedQuery_.selectClause();
  selectClause.setSelected(std::move(describedVariables));
  describeClause.whereClause_ = std::move(parsedQuery_);

  // Set up the final `ParsedQuery` object for the DESCRIBE query. The clause is
  // a CONSTRUCT query of the form `CONSTRUCT { ?subject ?predicate ?object} {
  // ... }`, with the `parsedQuery::Describe` object from above as the root
  // graph pattern. The solution modifiers (in particular ORDER BY) are part of
  // the CONSTRUCT query.
  //
  // NOTE: The dataset clauses are stored once in `parsedQuery_.datasetClauses_`
  // (which pertains to the CONSTRUCT query that computes the result of the
  // DESCRIBE), and once in `parsedQuery_.describeClause_.datasetClauses_`
  // (which pertains to the SELECT query that computes the resources to be
  // described).
  parsedQuery_ = ParsedQuery{};
  parsedQuery_.addSolutionModifiers(visit(ctx->solutionModifier()));
  parsedQuery_._rootGraphPattern._graphPatterns.emplace_back(
      std::move(describeClause));
  parsedQuery_.datasetClauses_ = activeDatasetClauses_;
  auto constructClause = ParsedQuery::ConstructClause{};
  using G = GraphTerm;
  using V = Variable;
  constructClause.triples_.push_back(
      std::array{G(V("?subject")), G(V("?predicate")), G(V("?object"))});
  parsedQuery_._clause = std::move(constructClause);

  return parsedQuery_;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::AskQueryContext* ctx) {
  parsedQuery_._clause = ParsedQuery::AskClause{};
  parsedQuery_.datasetClauses_ =
      setAndGetDatasetClauses(visitVector(ctx->datasetClause()));
  visitWhereClause(ctx->whereClause(), parsedQuery_);
  // NOTE: It can make sense to have solution modifiers with an ASK query, for
  // example, a GROUP BY with a HAVING.
  auto getSolutionModifiers = [this, ctx]() {
    auto solutionModifiers = visit(ctx->solutionModifier());
    const auto& limitOffset = solutionModifiers.limitOffset_;
    if (!limitOffset.isUnconstrained() || limitOffset.textLimit_.has_value()) {
      reportError(
          ctx->solutionModifier(),
          "ASK queries may not contain LIMIT, OFFSET, or TEXTLIMIT clauses");
    }
    solutionModifiers.limitOffset_._limit = 1;
    return solutionModifiers;
  };
  parsedQuery_.addSolutionModifiers(getSolutionModifiers());
  return parsedQuery_;
}

// ____________________________________________________________________________________
DatasetClause Visitor::visit(Parser::DatasetClauseContext* ctx) {
  if (ctx->defaultGraphClause()) {
    return {.dataset_ = visit(ctx->defaultGraphClause()), .isNamed_ = false};
  } else {
    AD_CORRECTNESS_CHECK(ctx->namedGraphClause());
    return {.dataset_ = visit(ctx->namedGraphClause()), .isNamed_ = true};
  }
}

// ____________________________________________________________________________________
TripleComponent::Iri Visitor::visit(Parser::DefaultGraphClauseContext* ctx) {
  return visit(ctx->sourceSelector());
}

// ____________________________________________________________________________________
TripleComponent::Iri Visitor::visit(Parser::NamedGraphClauseContext* ctx) {
  return visit(ctx->sourceSelector());
}

// ____________________________________________________________________________________
TripleComponent::Iri Visitor::visit(Parser::SourceSelectorContext* ctx) {
  return visit(ctx->iri());
}

// ____________________________________________________________________________________
Variable Visitor::visit(Parser::VarContext* ctx) {
  // `false` for the second argument means: The variable name is already
  // validated by the grammar, no need to check it again (which would lead to an
  // infinite loop here).
  return Variable{ctx->getText(), false};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::BindContext* ctx) {
  Variable target = visit(ctx->var());
  if (ad_utility::contains(visibleVariables_, target)) {
    reportError(
        ctx,
        absl::StrCat(
            "The target variable ", target.name(),
            " of an AS clause was already used before in the query body."));
  }

  auto expression = visitExpressionPimpl(ctx->expression());
  warnOrThrowIfUnboundVariables(ctx, expression, "BIND");
  addVisibleVariable(target);
  return GraphPatternOperation{Bind{std::move(expression), std::move(target)}};
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::InlineDataContext* ctx) {
  Values values = visit(ctx->dataBlock());
  for (const auto& variable : values._inlineValues._variables) {
    addVisibleVariable(variable);
  }
  return GraphPatternOperation{std::move(values)};
}

// ____________________________________________________________________________________
Values Visitor::visit(Parser::DataBlockContext* ctx) {
  return visitAlternative<Values>(ctx->inlineDataOneVar(),
                                  ctx->inlineDataFull());
}

// ____________________________________________________________________________________
std::optional<Values> Visitor::visit(Parser::ValuesClauseContext* ctx) {
  return visitOptional(ctx->dataBlock());
}

// ____________________________________________________________________________
std::vector<ParsedQuery> Visitor::visit(Parser::UpdateContext* ctx) {
  std::vector<ParsedQuery> updates{};

  AD_CORRECTNESS_CHECK(ctx->prologue().size() >= ctx->update1().size());
  for (size_t i = 0; i < ctx->update1().size(); ++i) {
    // The prologue (BASE and PREFIX declarations) only affects the internal
    // state of the visitor. The standard mentions that prefixes are shared
    // between consecutive updates.
    visit(ctx->prologue(i));
    auto thisUpdates = visit(ctx->update1(i));
    // The string representation of the Update is from the beginning of that
    // updates prologue to the end of the update. The `;` between queries is
    // ignored in the string representation.
    const size_t updateStartPos = ctx->prologue(i)->getStart()->getStartIndex();
    const size_t updateEndPos = ctx->update1(i)->getStop()->getStopIndex();
    auto updateStringRepr = std::string{ad_utility::getUTF8Substring(
        ctx->getStart()->getInputStream()->toString(), updateStartPos,
        updateEndPos - updateStartPos + 1)};
    // Many graph management operations are desugared into multiple updates. We
    // set the string representation of the graph management operation for all
    // the simple update operations.
    ql::ranges::for_each(thisUpdates, [updateStringRepr](ParsedQuery& update) {
      update._originalString = updateStringRepr;
    });
    ad_utility::appendVector(updates, thisUpdates);
    resetStateForMultipleUpdates();
  }

  return updates;
}

// ____________________________________________________________________________________
std::vector<ParsedQuery> Visitor::visit(Parser::Update1Context* ctx) {
  if (ctx->deleteWhere() || ctx->modify() || ctx->clear() || ctx->drop() ||
      ctx->create() || ctx->copy() || ctx->move() || ctx->add() ||
      ctx->load()) {
    return visitAlternative<std::vector<ParsedQuery>>(
        ctx->deleteWhere(), ctx->modify(), ctx->clear(), ctx->drop(),
        ctx->create(), ctx->copy(), ctx->move(), ctx->add(), ctx->load());
  }
  AD_CORRECTNESS_CHECK(ctx->insertData() || ctx->deleteData());
  parsedQuery_._clause = visitAlternative<parsedQuery::UpdateClause>(
      ctx->insertData(), ctx->deleteData());
  parsedQuery_.datasetClauses_ = activeDatasetClauses_;
  return {std::move(parsedQuery_)};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::LoadContext* ctx) {
  AD_CORRECTNESS_CHECK(visibleVariables_.empty());
  GraphPattern pattern;
  auto iri = visit(ctx->iri());
  // The `LOAD` Update operation is translated into something like
  // `INSERT { ?s ?p ?o } WHERE { LOAD_OP <iri> [SILENT] }`. Where `LOAD_OP` is
  // an internal operation that binds the result of parsing the given RDF
  // document into the variables `?s`, `?p`, and `?o`.
  pattern._graphPatterns.emplace_back(
      parsedQuery::Load{iri, static_cast<bool>(ctx->SILENT())});
  parsedQuery_._rootGraphPattern = std::move(pattern);
  addVisibleVariable(Variable("?s"));
  addVisibleVariable(Variable("?p"));
  addVisibleVariable(Variable("?o"));
  parsedQuery_.registerVariablesVisibleInQueryBody(visibleVariables_);
  visibleVariables_.clear();
  using Quad = SparqlTripleSimpleWithGraph;
  std::vector<Quad> toInsert{
      Quad{Variable("?s"), Variable("?p"), Variable("?o"),
           ctx->graphRef() ? Quad::Graph(visit(ctx->graphRef()))
                           : Quad::Graph(std::monostate{})}};
  parsedQuery_._clause = parsedQuery::UpdateClause{GraphUpdate{toInsert, {}}};
  return parsedQuery_;
}

// Helper functions for some inner parts of graph management operations.
namespace {
// ____________________________________________________________________________________
// Transform a `GraphRefAll` or `GraphOrDefault` into a
// `SparqlTripleSimpleWithGraph::Graph`/`parsedQuery::GroupGraphPattern::GraphSpec`
// (which are the same type).
SparqlTripleSimpleWithGraph::Graph transformGraph(const GraphRefAll& graph) {
  using Graph = SparqlTripleSimpleWithGraph::Graph;
  // This case cannot be handled in this function and must be handled before.
  AD_CORRECTNESS_CHECK(!std::holds_alternative<NAMED>(graph));
  return std::visit(
      ad_utility::OverloadCallOperator{
          [](const ad_utility::triple_component::Iri& iri) -> Graph {
            return iri;
          },
          [](const ALL&) -> Graph { return Variable("?g"); },
          [](const DEFAULT&) -> Graph {
            return ad_utility::triple_component::Iri::fromIriref(
                DEFAULT_GRAPH_IRI);
          },
          [](const NAMED&) -> Graph { AD_FAIL(); }},
      graph);
}

// ____________________________________________________________________________________
SparqlTripleSimpleWithGraph::Graph transformGraph(const GraphOrDefault& graph) {
  using Graph = SparqlTripleSimpleWithGraph::Graph;
  return std::visit(
      ad_utility::OverloadCallOperator{
          [](const ad_utility::triple_component::Iri& iri) -> Graph {
            return iri;
          },
          [](const DEFAULT&) -> Graph {
            return ad_utility::triple_component::Iri::fromIriref(
                DEFAULT_GRAPH_IRI);
          }},
      graph);
}

// ____________________________________________________________________________________
// Make a `GraphPatternOperation` that matches all triples in the given graph.
GraphPatternOperation makeAllTripleGraphPattern(
    parsedQuery::GroupGraphPattern::GraphSpec graph) {
  GraphPattern inner;
  inner._graphPatterns.emplace_back(BasicGraphPattern{
      {{{Variable("?s")}, Variable("?p"), {Variable("?o")}}}});
  return {parsedQuery::GroupGraphPattern{std::move(inner), std::move(graph)}};
}

// ____________________________________________________________________________________
// Make a `SparqlTripleSimpleWithGraph` that templates all triples in the graph.
SparqlTripleSimpleWithGraph makeAllTripleTemplate(
    SparqlTripleSimpleWithGraph::Graph graph) {
  return {
      {Variable("?s")}, {Variable("?p")}, {Variable("?o")}, std::move(graph)};
}
}  // namespace

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::ClearContext* ctx) {
  return makeClear(visit(ctx->graphRefAll()));
}

// ____________________________________________________________________________________
ParsedQuery Visitor::makeClear(SparqlTripleSimpleWithGraph::Graph graph) {
  parsedQuery_._rootGraphPattern._graphPatterns.push_back(
      makeAllTripleGraphPattern(graph));
  parsedQuery_._clause = parsedQuery::UpdateClause{
      GraphUpdate{{}, {makeAllTripleTemplate(std::move(graph))}}};
  parsedQuery_.datasetClauses_ = activeDatasetClauses_;
  return parsedQuery_;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::makeClear(const GraphRefAll& graph) {
  if (std::holds_alternative<NAMED>(graph)) {
    // We first select all graphs and then filter out the default graph, to get
    // only the named graphs. `Variable("?g")` selects all graphs.
    parsedQuery_._rootGraphPattern._graphPatterns.push_back(
        makeAllTripleGraphPattern(Variable("?g")));
    // TODO<joka921,qup> Extend the graph filtering s.t. we can exclude a single
    //  graph more efficiently
    auto e = SparqlExpressionPimpl{
        createExpression<sparqlExpression::NotEqualExpression>(
            std::make_unique<sparqlExpression::VariableExpression>(
                Variable("?g")),
            std::make_unique<sparqlExpression::IriExpression>(
                TripleComponent::Iri::fromIriref(DEFAULT_GRAPH_IRI))),
        absl::StrCat("?g != ", DEFAULT_GRAPH_IRI)};
    parsedQuery_._rootGraphPattern._filters.emplace_back(std::move(e));
    parsedQuery_._clause = parsedQuery::UpdateClause{
        GraphUpdate{{}, {makeAllTripleTemplate(Variable("?g"))}}};
    parsedQuery_.datasetClauses_ = activeDatasetClauses_;
    return parsedQuery_;
  }

  return makeClear(transformGraph(graph));
}

// ____________________________________________________________________________________
ParsedQuery Visitor::makeAdd(const GraphOrDefault& source,
                             const GraphOrDefault& target) {
  parsedQuery_._rootGraphPattern._graphPatterns.push_back(
      makeAllTripleGraphPattern(transformGraph(source)));
  parsedQuery_._clause = parsedQuery::UpdateClause{
      GraphUpdate{{makeAllTripleTemplate(transformGraph(target))}, {}}};
  parsedQuery_.datasetClauses_ = activeDatasetClauses_;
  return parsedQuery_;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::DropContext* ctx) {
  return makeClear(visit(ctx->graphRefAll()));
}

// ____________________________________________________________________________________
std::vector<ParsedQuery> Visitor::visit(const Parser::CreateContext*) {
  // Create is a no-op because we don't explicitly record the existence of empty
  // graphs.
  return {};
}

// ____________________________________________________________________________________
std::vector<ParsedQuery> Visitor::visit(Parser::AddContext* ctx) {
  AD_CORRECTNESS_CHECK(ctx->graphOrDefault().size() == 2);
  auto from = visit(ctx->graphOrDefault()[0]);
  auto to = visit(ctx->graphOrDefault()[1]);

  if (from == to) {
    return {};
  }

  return {makeAdd(from, to)};
}

// _____________________________________________________________________________
std::vector<ParsedQuery> Visitor::makeCopy(const GraphOrDefault& from,
                                           const GraphOrDefault& to) {
  std::vector<ParsedQuery> updates{makeClear(transformGraph(to))};
  resetStateForMultipleUpdates();
  updates.push_back(makeAdd(from, to));

  return updates;
}

// ____________________________________________________________________________________
std::pair<GraphOrDefault, GraphOrDefault> Visitor::visitFromTo(
    std::vector<Parser::GraphOrDefaultContext*> ctxs) {
  AD_CORRECTNESS_CHECK(ctxs.size() == 2);
  return {visit(ctxs[0]), visit(ctxs[1])};
};

// ____________________________________________________________________________________
std::vector<ParsedQuery> Visitor::visit(Parser::MoveContext* ctx) {
  auto [from, to] = visitFromTo(ctx->graphOrDefault());

  if (from == to) {
    return {};
  }

  std::vector<ParsedQuery> updates = makeCopy(from, to);
  resetStateForMultipleUpdates();
  updates.push_back(makeClear(transformGraph(from)));

  return updates;
}

// ____________________________________________________________________________________
std::vector<ParsedQuery> Visitor::visit(Parser::CopyContext* ctx) {
  auto [from, to] = visitFromTo(ctx->graphOrDefault());

  if (from == to) {
    return {};
  }

  return makeCopy(from, to);
}

// ____________________________________________________________________________________
GraphUpdate Visitor::visit(Parser::InsertDataContext* ctx) {
  return {visit(ctx->quadData()).toTriplesWithGraph(std::monostate{}), {}};
}

// ____________________________________________________________________________________
GraphUpdate Visitor::visit(Parser::DeleteDataContext* ctx) {
  return {{}, visit(ctx->quadData()).toTriplesWithGraph(std::monostate{})};
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::DeleteWhereContext* ctx) {
  AD_CORRECTNESS_CHECK(visibleVariables_.empty());
  parsedQuery_.datasetClauses_ = activeDatasetClauses_;
  GraphPattern pattern;
  auto triples = visit(ctx->quadPattern());
  pattern._graphPatterns = triples.toGraphPatternOperations();
  parsedQuery_._rootGraphPattern = std::move(pattern);
  // The query body and template are identical. No need to check that variables
  // are visible. But they need to be registered.
  triples.forAllVariables([this](const Variable& v) { addVisibleVariable(v); });
  parsedQuery_.registerVariablesVisibleInQueryBody(visibleVariables_);
  visibleVariables_.clear();
  parsedQuery_._clause = parsedQuery::UpdateClause{
      GraphUpdate{{}, triples.toTriplesWithGraph(std::monostate{})}};
  return parsedQuery_;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::ModifyContext* ctx) {
  auto ensureVariableIsVisible = [&ctx, this](const Variable& v) {
    if (!ad_utility::contains(parsedQuery_.getVisibleVariables(), v)) {
      reportError(ctx, absl::StrCat("Variable ", v.name(),
                                    " was not bound in the query body."));
    }
  };
  auto visitTemplateClause = [&ensureVariableIsVisible, this](
                                 auto* ctx, auto* target,
                                 const auto& defaultGraph) {
    if (ctx) {
      auto quads = this->visit(ctx);
      quads.forAllVariables(ensureVariableIsVisible);
      *target = quads.toTriplesWithGraph(defaultGraph);
    }
  };

  using Iri = TripleComponent::Iri;
  // The graph specified in the `WITH` clause or `std::monostate{}` if there was
  // no with clause.
  auto withGraph = [&ctx, this]() -> SparqlTripleSimpleWithGraph::Graph {
    std::optional<Iri> with;
    if (ctx->iri() && datasetsAreFixed_) {
      reportError(ctx->iri(),
                  "`WITH` is disallowed in section 2.2.3 of the SPARQL "
                  "1.1 protocol standard if the `using-graph-uri` or "
                  "`using-named-graph-uri` http parameters are used");
    }
    visitIf(&with, ctx->iri());
    if (with.has_value()) {
      return std::move(with.value());
    }
    return std::monostate{};
  }();

  AD_CORRECTNESS_CHECK(visibleVariables_.empty());
  parsedQuery_.datasetClauses_ =
      setAndGetDatasetClauses(visitVector(ctx->usingClause()));

  // If there is no USING clause, but a WITH clause, then the graph specified in
  // the WITH clause is used as the default graph in the WHERE clause of this
  // update.
  if (const auto* withGraphIri = std::get_if<Iri>(&withGraph);
      parsedQuery_.datasetClauses_.isUnconstrainedOrWithClause() &&
      withGraphIri != nullptr) {
    parsedQuery_.datasetClauses_ =
        parsedQuery::DatasetClauses::fromWithClause(*withGraphIri);
  }

  auto graphPattern = visit(ctx->groupGraphPattern());
  parsedQuery_._rootGraphPattern = std::move(graphPattern);
  parsedQuery_.registerVariablesVisibleInQueryBody(visibleVariables_);
  visibleVariables_.clear();
  auto op = GraphUpdate{};

  // If there was a `WITH` clause, then the specified graph is used for all
  // triples inside the INSERT/DELETE templates that are outside explicit `GRAPH
  // {}` clauses.
  visitTemplateClause(ctx->insertClause(), &op.toInsert_, withGraph);
  visitTemplateClause(ctx->deleteClause(), &op.toDelete_, withGraph);
  parsedQuery_._clause = parsedQuery::UpdateClause{op};

  return parsedQuery_;
}

// ____________________________________________________________________________________
Quads Visitor::visit(Parser::DeleteClauseContext* ctx) {
  return visit(ctx->quadPattern());
}

// ____________________________________________________________________________________
Quads Visitor::visit(Parser::InsertClauseContext* ctx) {
  return visit(ctx->quadPattern());
}

// ____________________________________________________________________________________
GraphOrDefault Visitor::visit(Parser::GraphOrDefaultContext* ctx) {
  if (ctx->iri()) {
    return visit(ctx->iri());
  } else {
    return DEFAULT{};
  }
}

// ____________________________________________________________________________________
GraphRef Visitor::visit(Parser::GraphRefContext* ctx) {
  return visit(ctx->iri());
}

// ____________________________________________________________________________________
GraphRefAll Visitor::visit(Parser::GraphRefAllContext* ctx) {
  if (ctx->graphRef()) {
    return visit(ctx->graphRef());
  } else if (ctx->DEFAULT()) {
    return DEFAULT{};
  } else if (ctx->NAMED()) {
    return NAMED{};
  } else if (ctx->ALL()) {
    return ALL{};
  } else {
    AD_FAIL();
  }
}

// ____________________________________________________________________________________
Quads Visitor::visit(Parser::QuadPatternContext* ctx) {
  return visit(ctx->quads());
}

// ____________________________________________________________________________________
Quads Visitor::visit(Parser::QuadDataContext* ctx) {
  auto quads = visit(ctx->quads());
  quads.forAllVariables([&ctx](const Variable& v) {
    reportError(ctx->quads(),
                "Variables (" + v.name() + ") are not allowed here.");
  });
  return quads;
}

// ____________________________________________________________________________________
Quads Visitor::visit(Parser::QuadsContext* ctx) {
  // The ordering of the individual triplesTemplate and quadsNotTriples is not
  // relevant and also not known.
  Quads quads;
  quads.freeTriples_ = ad_utility::flatten(visitVector(ctx->triplesTemplate()));
  for (auto& [graph, triples] : visitVector(ctx->quadsNotTriples())) {
    quads.graphTriples_.emplace_back(std::move(graph), std::move(triples));
  }
  return quads;
}

// ____________________________________________________________________________________
Quads::GraphBlock Visitor::visit(Parser::QuadsNotTriplesContext* ctx) {
  auto graph = visit(ctx->varOrIri());
  // Short circuit when the triples section is empty
  if (!ctx->triplesTemplate()) {
    return {graph, {}};
  }

  return {graph, visit(ctx->triplesTemplate())};
}

// _____________________________________________________________________________
void Visitor::selectExistsVariables(SparqlFilter& filter) const {
  for (SparqlExpression* sparqlExpression :
       filter.expression_.getExistsExpressions()) {
    auto* existsExpression = dynamic_cast<ExistsExpression*>(sparqlExpression);
    AD_CORRECTNESS_CHECK(existsExpression);
    existsExpression->selectVariables(visibleVariables_);
  }
}

// _____________________________________________________________________________
GraphPattern Visitor::visit(Parser::GroupGraphPatternContext* ctx) {
  GraphPattern pattern;

  // The following code makes sure that the variables from outside the graph
  // pattern are NOT visible inside the graph pattern, but the variables from
  // the graph pattern are visible outside the graph pattern.
  auto visibleVariablesSoFar = std::move(visibleVariables_);
  visibleVariables_.clear();
  auto mergeVariables =
      ad_utility::makeOnDestructionDontThrowDuringStackUnwinding(
          [this, &visibleVariablesSoFar]() {
            std::swap(visibleVariables_, visibleVariablesSoFar);
            visibleVariables_.insert(visibleVariables_.end(),
                                     visibleVariablesSoFar.begin(),
                                     visibleVariablesSoFar.end());
          });
  if (ctx->subSelect()) {
    auto parsedQuerySoFar = std::exchange(parsedQuery_, ParsedQuery{});
    auto [subquery, valuesOpt] = visit(ctx->subSelect());
    pattern._graphPatterns.emplace_back(std::move(subquery));
    if (valuesOpt.has_value()) {
      pattern._graphPatterns.emplace_back(std::move(valuesOpt.value()));
    }
    parsedQuery_ = std::move(parsedQuerySoFar);
    return pattern;
  }
  AD_CORRECTNESS_CHECK(ctx->groupGraphPatternSub());
  auto [subOps, filters] = visit(ctx->groupGraphPatternSub());
  pattern._graphPatterns = std::move(subOps);
  for (auto& filter : filters) {
    selectExistsVariables(filter);
    if (auto langFilterData = filter.expression_.getLanguageFilterExpression();
        langFilterData.has_value()) {
      const auto& [variable, language] = langFilterData.value();
      if (pattern.addLanguageFilter(variable, language)) {
        continue;
      }
    }
    pattern._filters.push_back(std::move(filter));
  }
  return pattern;
}

Visitor::OperationsAndFilters Visitor::visit(
    Parser::GroupGraphPatternSubContext* ctx) {
  vector<GraphPatternOperation> ops;
  vector<SparqlFilter> filters;

  auto filter = [&filters](SparqlFilter filter) {
    filters.emplace_back(std::move(filter));
  };
  auto op = [&ops](GraphPatternOperation op) {
    ops.emplace_back(std::move(op));
  };

  if (ctx->triplesBlock()) {
    ops.emplace_back(visit(ctx->triplesBlock()));
  }
  for (auto& [graphPattern, triples] :
       visitVector(ctx->graphPatternNotTriplesAndMaybeTriples())) {
    std::visit(ad_utility::OverloadCallOperator{filter, op},
               std::move(graphPattern));

    // TODO<C++23>: use `optional.transform` for this pattern.
    if (!triples.has_value()) {
      continue;
    }
    if (ops.empty() || !std::holds_alternative<BasicGraphPattern>(ops.back())) {
      ops.emplace_back(BasicGraphPattern{});
    }
    std::get<BasicGraphPattern>(ops.back())
        .appendTriples(std::move(triples.value()));
  }
  return {std::move(ops), std::move(filters)};
}

Visitor::OperationOrFilterAndMaybeTriples Visitor::visit(
    Parser::GraphPatternNotTriplesAndMaybeTriplesContext* ctx) {
  return {visit(ctx->graphPatternNotTriples()),
          visitOptional(ctx->triplesBlock())};
}

// ____________________________________________________________________________________
BasicGraphPattern Visitor::visit(Parser::TriplesBlockContext* ctx) {
  auto registerIfVariable = [this](const auto& variant) {
    if (holds_alternative<Variable>(variant)) {
      addVisibleVariable(std::get<Variable>(variant));
    }
  };
  auto convertAndRegisterTriple =
      [&registerIfVariable](
          const TripleWithPropertyPath& triple) -> SparqlTriple {
    registerIfVariable(triple.subject_);
    registerIfVariable(triple.predicate_);
    registerIfVariable(triple.object_);

    return {triple.subject_.toTripleComponent(), triple.predicate_,
            triple.object_.toTripleComponent()};
  };

  BasicGraphPattern triples = {ad_utility::transform(
      visit(ctx->triplesSameSubjectPath()), convertAndRegisterTriple)};
  if (ctx->triplesBlock()) {
    triples.appendTriples(visit(ctx->triplesBlock()));
  }
  return triples;
}

// ____________________________________________________________________________________
Visitor::OperationOrFilter Visitor::visit(
    Parser::GraphPatternNotTriplesContext* ctx) {
  return visitAlternative<std::variant<GraphPatternOperation, SparqlFilter>>(
      ctx->filterR(), ctx->optionalGraphPattern(), ctx->minusGraphPattern(),
      ctx->bind(), ctx->inlineData(), ctx->groupOrUnionGraphPattern(),
      ctx->graphGraphPattern(), ctx->serviceGraphPattern());
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::OptionalGraphPatternContext* ctx) {
  auto pattern = visit(ctx->groupGraphPattern());
  return GraphPatternOperation{parsedQuery::Optional{std::move(pattern)}};
}

GraphPatternOperation Visitor::visitPathQuery(
    Parser::ServiceGraphPatternContext* ctx) {
  auto parsePathQuery = [](parsedQuery::PathQuery& pathQuery,
                           const parsedQuery::GraphPatternOperation& op) {
    if (std::holds_alternative<parsedQuery::BasicGraphPattern>(op)) {
      pathQuery.addBasicPattern(std::get<parsedQuery::BasicGraphPattern>(op));
    } else if (std::holds_alternative<parsedQuery::GroupGraphPattern>(op)) {
      pathQuery.addGraph(op);
    } else {
      throw parsedQuery::PathSearchException(
          "Unsupported element in pathSearch."
          "PathQuery may only consist of triples for configuration"
          "And a { group graph pattern } specifying edges.");
    }
  };

  parsedQuery::GraphPattern graphPattern = visit(ctx->groupGraphPattern());
  parsedQuery::PathQuery pathQuery;
  for (const auto& op : graphPattern._graphPatterns) {
    parsePathQuery(pathQuery, op);
  }

  return pathQuery;
}

GraphPatternOperation Visitor::visitSpatialQuery(
    Parser::ServiceGraphPatternContext* ctx) {
  auto parseSpatialQuery = [ctx](parsedQuery::SpatialQuery& spatialQuery,
                                 const parsedQuery::GraphPatternOperation& op) {
    if (std::holds_alternative<parsedQuery::BasicGraphPattern>(op)) {
      spatialQuery.addBasicPattern(
          std::get<parsedQuery::BasicGraphPattern>(op));
    } else if (std::holds_alternative<parsedQuery::GroupGraphPattern>(op)) {
      spatialQuery.addGraph(op);
    } else {
      reportError(
          ctx,
          "Unsupported element in spatialQuery."
          "spatialQuery may only consist of triples for configuration"
          "And a { group graph pattern } specifying the right join table.");
    }
  };

  parsedQuery::GraphPattern graphPattern = visit(ctx->groupGraphPattern());
  parsedQuery::SpatialQuery spatialQuery;
  for (const auto& op : graphPattern._graphPatterns) {
    parseSpatialQuery(spatialQuery, op);
  }

  try {
    // We convert the spatial query to a spatial join configuration and discard
    // its result here to detect errors early and report them to the user with
    // highlighting. It's only a small struct so not much is wasted.
    spatialQuery.toSpatialJoinConfiguration();
  } catch (const std::exception& ex) {
    reportError(ctx, ex.what());
  }

  return spatialQuery;
}

GraphPatternOperation Visitor::visitTextSearchQuery(
    Parser::ServiceGraphPatternContext* ctx) {
  auto parseTextSearchQuery =
      [ctx](parsedQuery::TextSearchQuery& textSearchQuery,
            const parsedQuery::GraphPatternOperation& op) {
        if (std::holds_alternative<parsedQuery::BasicGraphPattern>(op)) {
          textSearchQuery.addBasicPattern(
              std::get<parsedQuery::BasicGraphPattern>(op));
        } else {
          reportError(
              ctx,
              "Unsupported element in textSearchQuery. "
              "textSearchQuery may only consist of triples for configuration");
        }
      };

  parsedQuery::GraphPattern graphPattern = visit(ctx->groupGraphPattern());
  parsedQuery::TextSearchQuery textSearchQuery;
  for (const auto& op : graphPattern._graphPatterns) {
    parseTextSearchQuery(textSearchQuery, op);
  }

  return textSearchQuery;
}

// Parsing for the `serviceGraphPattern` rule.
GraphPatternOperation Visitor::visit(Parser::ServiceGraphPatternContext* ctx) {
  // Get the IRI and if a variable is specified, report that we do not support
  // it yet.
  //
  // NOTE: According to the grammar, this should either be a `Variable` or an
  // `Iri`, but due to (not very good) technical reasons, the `visit` returns a
  // `std::variant<Variable, GraphTerm>`, where `GraphTerm` is a
  // `std::variant<Literal, BlankNode, Iri>`, hence the `AD_CONTRACT_CHECK`.
  //
  // TODO: Also support variables. The semantics is to make a connection for
  // each IRI matching the variable and take the union of the results.
  VarOrIri varOrIri = visit(ctx->varOrIri());
  using Iri = TripleComponent::Iri;
  auto serviceIri =
      std::visit(ad_utility::OverloadCallOperator{
                     [&ctx](const Variable&) -> Iri {
                       reportNotSupported(ctx->varOrIri(),
                                          "Variable endpoint in SERVICE is");
                     },
                     [](const Iri& iri) -> Iri { return iri; }},
                 varOrIri);

  if (serviceIri.toStringRepresentation() == PATH_SEARCH_IRI) {
    return visitPathQuery(ctx);
  } else if (serviceIri.toStringRepresentation() == SPATIAL_SEARCH_IRI) {
    return visitSpatialQuery(ctx);
  } else if (serviceIri.toStringRepresentation() == TEXT_SEARCH_IRI) {
    return visitTextSearchQuery(ctx);
  }
  // Parse the body of the SERVICE query. Add the visible variables from the
  // SERVICE clause to the visible variables so far, but also remember them
  // separately (with duplicates removed) because we need them in `Service.cpp`
  // when computing the result for this operation.
  std::vector<Variable> visibleVariablesSoFar = std::move(visibleVariables_);
  parsedQuery::GraphPattern graphPattern = visit(ctx->groupGraphPattern());
  // Note: The `visit` call in the line above has filled the `visibleVariables_`
  // member with all the variables visible inside the graph pattern.
  std::vector<Variable> visibleVariablesServiceQuery =
      ad_utility::removeDuplicates(visibleVariables_);
  visibleVariables_ = std::move(visibleVariablesSoFar);
  visibleVariables_.insert(visibleVariables_.end(),
                           visibleVariablesServiceQuery.begin(),
                           visibleVariablesServiceQuery.end());
  // Create suitable `parsedQuery::Service` object and return it.
  return parsedQuery::Service{
      std::move(visibleVariablesServiceQuery), std::move(serviceIri),
      prologueString_, getOriginalInputForContext(ctx->groupGraphPattern()),
      static_cast<bool>(ctx->SILENT())};
}

// ____________________________________________________________________________
parsedQuery::GraphPatternOperation Visitor::visit(
    Parser::GraphGraphPatternContext* ctx) {
  auto varOrIri = visit(ctx->varOrIri());
  auto group = visit(ctx->groupGraphPattern());
  return std::visit(
      ad_utility::OverloadCallOperator{
          [this, &group](const Variable& graphVar) {
            addVisibleVariable(graphVar);
            return parsedQuery::GroupGraphPattern{std::move(group), graphVar};
          },
          [&group](const TripleComponent::Iri& graphIri) {
            return parsedQuery::GroupGraphPattern{std::move(group), graphIri};
          }},
      varOrIri);
}

// Parsing for the `expression` rule.
sparqlExpression::SparqlExpression::Ptr Visitor::visit(
    Parser::ExpressionContext* ctx) {
  return visit(ctx->conditionalOrExpression());
}

// Parsing for the `whereClause` rule.
Visitor::PatternAndVisibleVariables Visitor::visit(
    Parser::WhereClauseContext* ctx) {
  // Get the variables visible in this WHERE clause separately from the visible
  // variables so far because they might not all be visible in the outer query.
  // Adding appropriately to the visible variables so far is then taken care of
  // in `visit(SubSelectContext*)`.
  std::vector<Variable> visibleVariablesSoFar = std::move(visibleVariables_);
  auto graphPatternWhereClause = visit(ctx->groupGraphPattern());
  // Using `std::exchange` as per Johannes' suggestion. I am slightly irritated
  // that this calls the move constructor AND the move assignment operator for
  // the second argument, since this is a potential performance issue (not in
  // this case though).
  auto visibleVariablesWhereClause =
      std::exchange(visibleVariables_, std::move(visibleVariablesSoFar));
  return {std::move(graphPatternWhereClause),
          std::move(visibleVariablesWhereClause)};
}

// ____________________________________________________________________________________
SolutionModifiers Visitor::visit(Parser::SolutionModifierContext* ctx) {
  SolutionModifiers modifiers;
  visitIf(&modifiers.groupByVariables_, ctx->groupClause());
  visitIf(&modifiers.havingClauses_, ctx->havingClause());
  visitIf(&modifiers.orderBy_, ctx->orderClause());
  visitIf(&modifiers.limitOffset_, ctx->limitOffsetClauses());
  return modifiers;
}

// ____________________________________________________________________________________
LimitOffsetClause Visitor::visit(Parser::LimitOffsetClausesContext* ctx) {
  LimitOffsetClause clause{};
  visitIf(&clause._limit, ctx->limitClause());
  visitIf(&clause._offset, ctx->offsetClause());
  visitIf(&clause.textLimit_, ctx->textLimitClause());
  return clause;
}

// ____________________________________________________________________________________
vector<SparqlFilter> Visitor::visit(Parser::HavingClauseContext* ctx) {
  return visitVector(ctx->havingCondition());
}

// ____________________________________________________________________________________
SparqlFilter Visitor::visit(Parser::HavingConditionContext* ctx) {
  return {visitExpressionPimpl(ctx->constraint())};
}

// ____________________________________________________________________________________
OrderClause Visitor::visit(Parser::OrderClauseContext* ctx) {
  auto orderKeys = visitVector(ctx->orderCondition());

  if (ctx->internalSortBy) {
    auto isDescending = [](const auto& variant) {
      return std::visit([](const auto& k) { return k.isDescending_; }, variant);
    };
    if (ql::ranges::any_of(orderKeys, isDescending)) {
      reportError(ctx,
                  "When using the `INTERNAL SORT BY` modifier, all sorted "
                  "variables have to be ascending");
    }
    return {IsInternalSort::True, std::move(orderKeys)};
  } else {
    AD_CONTRACT_CHECK(ctx->orderBy);
    return {IsInternalSort::False, std::move(orderKeys)};
  }
}

// ____________________________________________________________________________________
vector<GroupKey> Visitor::visit(Parser::GroupClauseContext* ctx) {
  return visitVector(ctx->groupCondition());
}

// ____________________________________________________________________________________
std::optional<parsedQuery::ConstructClause> Visitor::visit(
    Parser::ConstructTemplateContext* ctx) {
  if (ctx->constructTriples()) {
    isInsideConstructTriples_ = true;
    auto cleanup =
        absl::Cleanup{[this]() { isInsideConstructTriples_ = false; }};
    return parsedQuery::ConstructClause{visit(ctx->constructTriples())};
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
RdfEscaping::NormalizedRDFString Visitor::visit(Parser::StringContext* ctx) {
  return RdfEscaping::normalizeRDFLiteral(ctx->getText());
}

// ____________________________________________________________________________________
TripleComponent::Iri Visitor::visit(Parser::IriContext* ctx) {
  string langtag =
      ctx->PREFIX_LANGTAG() ? ctx->PREFIX_LANGTAG()->getText() : "";
  return TripleComponent::Iri::fromIriref(
      langtag + visitAlternative<string>(ctx->iriref(), ctx->prefixedName()));
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::IrirefContext* ctx) const {
  if (baseIri_.empty()) {
    return ctx->getText();
  }
  // TODO<RobinTF> Avoid unnecessary string copies because of conversion.
  // Handle IRIs with base IRI.
  return std::move(
      ad_utility::triple_component::Iri::fromIrirefConsiderBase(
          ctx->getText(), baseIri_.getBaseIri(false), baseIri_.getBaseIri(true))
          .toStringRepresentation());
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::PrefixedNameContext* ctx) {
  return visitAlternative<std::string>(ctx->pnameLn(), ctx->pnameNs());
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::PnameLnContext* ctx) {
  string text = ctx->getText();
  auto pos = text.find(':');
  auto pnameNS = text.substr(0, pos);
  auto pnLocal = text.substr(pos + 1);
  if (!prefixMap_.contains(pnameNS)) {
    // TODO<joka921> : proper name
    reportError(ctx, "Prefix " + pnameNS +
                         " was not registered using a PREFIX declaration");
  }
  auto inner = prefixMap_[pnameNS];
  // strip the trailing ">"
  inner = inner.substr(0, inner.size() - 1);
  return inner + RdfEscaping::unescapePrefixedIri(pnLocal) + ">";
}

// ____________________________________________________________________________________
string Visitor::visit(Parser::PnameNsContext* ctx) {
  auto text = ctx->getText();
  auto prefix = text.substr(0, text.length() - 1);
  if (!prefixMap_.contains(prefix)) {
    // TODO<joka921> : proper name
    reportError(ctx, "Prefix " + prefix +
                         " was not registered using a PREFIX declaration");
  }
  return prefixMap_[prefix];
}

// ____________________________________________________________________________________
DatasetClause SparqlQleverVisitor::visit(Parser::UsingClauseContext* ctx) {
  if (datasetsAreFixed_) {
    reportError(ctx,
                "`USING [NAMED]` is disallowed in section 2.2.3 of the SPARQL "
                "1.1 protocol standard if the `using-graph-uri` or "
                "`using-named-graph-uri` http parameters are used");
  }
  if (ctx->NAMED()) {
    return {.dataset_ = visit(ctx->iri()), .isNamed_ = true};
  } else {
    return {.dataset_ = visit(ctx->iri()), .isNamed_ = false};
  }
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::PrologueContext* ctx) {
  // Process in an interleaved way, so PREFIX statements are processed correctly
  // to only use the BASE IRIs defined before them, not after them.
  for (auto* child : ctx->children) {
    if (auto* baseDecl = dynamic_cast<Parser::BaseDeclContext*>(child)) {
      visit(baseDecl);
    } else {
      auto* prefixDecl = dynamic_cast<Parser::PrefixDeclContext*>(child);
      AD_CORRECTNESS_CHECK(prefixDecl != nullptr);
      visit(prefixDecl);
    }
  }
  // Remember the whole prologue (we need this when we encounter a SERVICE
  // clause, see `visit(ServiceGraphPatternContext*)` below.
  if (ctx->getStart() && ctx->getStop()) {
    prologueString_ = getOriginalInputForContext(ctx);
  }
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::BaseDeclContext* ctx) {
  auto rawIri = ctx->iriref()->getText();
  bool hasScheme = ctre::starts_with<"<[A-Za-z]*[A-Za-z0-9+-.]:">(rawIri);
  if (!hasScheme) {
    reportError(
        ctx,
        "The base IRI must be an absolute IRI with a scheme, was: " + rawIri);
  }
  baseIri_ = TripleComponent::Iri::fromIriref(visit(ctx->iriref()));
}

// ____________________________________________________________________________________
void Visitor::visit(Parser::PrefixDeclContext* ctx) {
  auto text = ctx->PNAME_NS()->getText();
  // Remove the ':' at the end of the PNAME_NS
  auto prefixLabel = text.substr(0, text.length() - 1);
  auto prefixIri = visit(ctx->iriref());
  prefixMap_[prefixLabel] = prefixIri;
}

// ____________________________________________________________________________________
ParsedQuery Visitor::visit(Parser::SelectQueryContext* ctx) {
  parsedQuery_._clause = visit(ctx->selectClause());
  parsedQuery_.datasetClauses_ =
      setAndGetDatasetClauses(visitVector(ctx->datasetClause()));
  visitWhereClause(ctx->whereClause(), parsedQuery_);
  parsedQuery_.addSolutionModifiers(visit(ctx->solutionModifier()));
  return parsedQuery_;
}

// ____________________________________________________________________________________
Visitor::SubQueryAndMaybeValues Visitor::visit(Parser::SubSelectContext* ctx) {
  ParsedQuery& query = parsedQuery_;
  query._clause = visit(ctx->selectClause());
  visitWhereClause(ctx->whereClause(), query);
  query.addSolutionModifiers(visit(ctx->solutionModifier()));
  auto values = visit(ctx->valuesClause());
  // Variables that are selected in this query are visible in the parent query.
  for (const auto& variable : query.selectClause().getSelectedVariables()) {
    addVisibleVariable(variable);
  }
  return {parsedQuery::Subquery{std::move(query)}, std::move(values)};
}

// ____________________________________________________________________________________
GroupKey Visitor::visit(Parser::GroupConditionContext* ctx) {
  if (ctx->var() && !ctx->expression()) {
    return Variable{ctx->var()->getText()};
  } else if (ctx->builtInCall() || ctx->functionCall()) {
    // builtInCall and functionCall are both also an Expression
    return (ctx->builtInCall() ? visitExpressionPimpl(ctx->builtInCall())
                               : visitExpressionPimpl(ctx->functionCall()));
  } else {
    AD_CORRECTNESS_CHECK(ctx->expression());
    auto expr = visitExpressionPimpl(ctx->expression());
    if (ctx->AS() && ctx->var()) {
      return Alias{std::move(expr), visit(ctx->var())};
    } else {
      return expr;
    }
  }
}

// ____________________________________________________________________________________
OrderKey Visitor::visit(Parser::OrderConditionContext* ctx) {
  auto visitExprOrderKey = [this](bool isDescending,
                                  auto* context) -> OrderKey {
    auto expr = visitExpressionPimpl(context);
    if (auto exprIsVariable = expr.getVariableOrNullopt();
        exprIsVariable.has_value()) {
      return VariableOrderKey{exprIsVariable.value(), isDescending};
    } else {
      return ExpressionOrderKey{std::move(expr), isDescending};
    }
  };

  if (ctx->var()) {
    return VariableOrderKey(visit(ctx->var()));
  } else if (ctx->constraint()) {
    return visitExprOrderKey(false, ctx->constraint());
  } else {
    AD_CORRECTNESS_CHECK(ctx->brackettedExpression());
    return visitExprOrderKey(ctx->DESC() != nullptr,
                             ctx->brackettedExpression());
  }
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::LimitClauseContext* ctx) {
  return visit(ctx->integer());
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::OffsetClauseContext* ctx) {
  return visit(ctx->integer());
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::TextLimitClauseContext* ctx) {
  return visit(ctx->integer());
}

// ____________________________________________________________________________________
SparqlValues Visitor::visit(Parser::InlineDataOneVarContext* ctx) {
  SparqlValues values;
  values._variables.push_back(visit(ctx->var()));
  for (auto& dataBlockValue : ctx->dataBlockValue()) {
    values._values.push_back({visit(dataBlockValue)});
  }
  return values;
}

// ____________________________________________________________________________________
SparqlValues Visitor::visit(Parser::InlineDataFullContext* ctx) {
  SparqlValues values;
  values._variables = visitVector(ctx->var());
  values._values = visitVector(ctx->dataBlockSingle());
  if (std::any_of(values._values.begin(), values._values.end(),
                  [numVars = values._variables.size()](const auto& inner) {
                    return inner.size() != numVars;
                  })) {
    reportError(ctx,
                "The number of values in every data block must "
                "match the number of variables in a values clause.");
  }
  return values;
}

// ____________________________________________________________________________________
vector<TripleComponent> Visitor::visit(Parser::DataBlockSingleContext* ctx) {
  if (ctx->NIL()) {
    return {};
  }
  return visitVector(ctx->dataBlockValue());
}

// ____________________________________________________________________________________
TripleComponent Visitor::visit(Parser::DataBlockValueContext* ctx) {
  // Return a string
  if (ctx->iri()) {
    return visit(ctx->iri());
  } else if (ctx->rdfLiteral()) {
    return RdfStringParser<TurtleParser<Tokenizer>>::parseTripleObject(
        visit(ctx->rdfLiteral()));
  } else if (ctx->numericLiteral()) {
    return std::visit(
        [](auto intOrDouble) { return TripleComponent{intOrDouble}; },
        visit(ctx->numericLiteral()));
  } else if (ctx->UNDEF()) {
    return TripleComponent::UNDEF{};
  } else {
    AD_CORRECTNESS_CHECK(ctx->booleanLiteral());
    return TripleComponent{visit(ctx->booleanLiteral())};
  }
}

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(Parser::MinusGraphPatternContext* ctx) {
  auto visibleVariables = std::move(visibleVariables_);
  GraphPatternOperation operation{
      parsedQuery::Minus{visit(ctx->groupGraphPattern())}};
  // Make sure that the variables from the minus graph pattern are NOT added to
  // visible variables.
  visibleVariables_ = std::move(visibleVariables);
  return operation;
}

// ____________________________________________________________________________________
namespace {
GraphPattern wrap(GraphPatternOperation op) {
  auto pattern = GraphPattern();
  pattern._graphPatterns.emplace_back(std::move(op));
  return pattern;
}
}  // namespace

// ____________________________________________________________________________________
GraphPatternOperation Visitor::visit(
    Parser::GroupOrUnionGraphPatternContext* ctx) {
  auto children = visitVector(ctx->groupGraphPattern());
  if (children.size() > 1) {
    // https://en.cppreference.com/w/cpp/algorithm/accumulate
    // a similar thing is done in QueryPlaner::uniteGraphPatterns
    auto foldOp = [](GraphPatternOperation op1, GraphPattern op2) {
      return GraphPatternOperation{
          parsedQuery::Union{wrap(std::move(op1)), std::move(op2)}};
    };
    // TODO<joka921> QLever should support Nary UNIONs directly.
    return std::accumulate(std::next(children.begin(), 2), children.end(),
                           GraphPatternOperation{parsedQuery::Union{
                               std::move(children[0]), std::move(children[1])}},
                           foldOp);
  } else {
    return GraphPatternOperation{
        parsedQuery::GroupGraphPattern{std::move(children[0])}};
  }
}

// ____________________________________________________________________________________
template <typename Context>
void Visitor::warnOrThrowIfUnboundVariables(
    Context* ctx, const SparqlExpressionPimpl& expression,
    std::string_view clauseName) {
  for (const auto& var : expression.containedVariables()) {
    if (!ad_utility::contains(visibleVariables_, *var)) {
      auto message = absl::StrCat(
          "The variable ", var->name(), " was used in the expression of a ",
          clauseName, " clause but was not previously bound in the query");
      if (RuntimeParameters().get<"throw-on-unbound-variables">()) {
        reportError(ctx, message);
      } else {
        parsedQuery_.addWarning(std::move(message));
      }
    }
  }
}

// ____________________________________________________________________________________
SparqlFilter Visitor::visit(Parser::FilterRContext* ctx) {
  // NOTE: We cannot add a warning or throw an exception if the FILTER
  // expression contains unbound variables, because the variables of the FILTER
  // might be bound after the filter appears in the query (which is perfectly
  // legal).
  return SparqlFilter{visitExpressionPimpl(ctx->constraint())};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConstraintContext* ctx) {
  return visitAlternative<ExpressionPtr>(
      ctx->brackettedExpression(), ctx->builtInCall(), ctx->functionCall());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::FunctionCallContext* ctx) {
  return processIriFunctionCall(visit(ctx->iri()), visit(ctx->argList()), ctx);
}

// ____________________________________________________________________________________
vector<Visitor::ExpressionPtr> Visitor::visit(Parser::ArgListContext* ctx) {
  // If no arguments, return empty expression vector.
  if (ctx->NIL()) {
    return std::vector<ExpressionPtr>{};
  }
  // The grammar allows an optional DISTINCT before the argument list (the
  // whole list, not the individual arguments), but we currently don't support
  // it.
  if (ctx->DISTINCT()) {
    reportNotSupported(
        ctx, "DISTINCT for the argument lists of an IRI functions is ");
  }
  // Visit the expression of each argument.
  return visitVector(ctx->expression());
}

// ____________________________________________________________________________________
std::vector<ExpressionPtr> Visitor::visit(Parser::ExpressionListContext* ctx) {
  if (ctx->NIL()) {
    return {};
  }
  return visitVector(ctx->expression());
}

// ____________________________________________________________________________________
Triples Visitor::visit(Parser::ConstructTriplesContext* ctx) {
  auto result = visit(ctx->triplesSameSubject());
  if (ctx->constructTriples()) {
    ad_utility::appendVector(result, visit(ctx->constructTriples()));
  }
  return result;
}

// ____________________________________________________________________________________
Triples Visitor::visit(Parser::TriplesTemplateContext* ctx) {
  return ad_utility::flatten(visitVector(ctx->triplesSameSubject()));
}

// ____________________________________________________________________________________
Triples Visitor::visit(Parser::TriplesSameSubjectContext* ctx) {
  Triples triples;
  if (ctx->varOrTerm()) {
    GraphTerm subject = visit(ctx->varOrTerm());
    AD_CONTRACT_CHECK(ctx->propertyListNotEmpty());
    auto propertyList = visit(ctx->propertyListNotEmpty());
    for (auto& tuple : propertyList.first) {
      triples.push_back({subject, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNode());
    auto tripleNodes = visit(ctx->triplesNode());
    ad_utility::appendVector(triples, std::move(tripleNodes.second));
    AD_CONTRACT_CHECK(ctx->propertyList());
    auto propertyList = visit(ctx->propertyList());
    for (auto& tuple : propertyList.first) {
      triples.push_back(
          {tripleNodes.first, std::move(tuple[0]), std::move(tuple[1])});
    }
    ad_utility::appendVector(triples, std::move(propertyList.second));
  }
  return triples;
}

// ____________________________________________________________________________________
PredicateObjectPairsAndTriples Visitor::visit(
    Parser::PropertyListContext* ctx) {
  return ctx->propertyListNotEmpty() ? visit(ctx->propertyListNotEmpty())
                                     : PredicateObjectPairsAndTriples{
                                           PredicateObjectPairs{}, Triples{}};
}

// ____________________________________________________________________________________
PredicateObjectPairsAndTriples Visitor::visit(
    Parser::PropertyListNotEmptyContext* ctx) {
  PredicateObjectPairs triplesWithoutSubject;
  Triples additionalTriples;
  auto verbs = ctx->verb();
  auto objectLists = ctx->objectList();
  for (size_t i = 0; i < verbs.size(); i++) {
    // TODO use zip-style approach once C++ supports ranges
    auto objectList = visit(objectLists.at(i));
    auto verb = visit(verbs.at(i));
    for (auto& object : objectList.first) {
      triplesWithoutSubject.push_back({verb, std::move(object)});
    }
    ad_utility::appendVector(additionalTriples, std::move(objectList.second));
  }
  return {std::move(triplesWithoutSubject), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::VerbContext* ctx) {
  if (ctx->varOrIri()) {
    // This is an artefact of there being two distinct Iri types.
    return std::visit(ad_utility::OverloadCallOperator{
                          [](const Variable& v) -> GraphTerm { return v; },
                          [](const TripleComponent::Iri& i) -> GraphTerm {
                            return Iri(i.toStringRepresentation());
                          }},
                      visit(ctx->varOrIri()));
  } else {
    // Special keyword 'a'
    AD_CORRECTNESS_CHECK(ctx->getText() == "a");
    return GraphTerm{Iri{a.toStringRepresentation()}};
  }
}

// ____________________________________________________________________________________
ObjectsAndTriples Visitor::visit(Parser::ObjectListContext* ctx) {
  Objects objects;
  Triples additionalTriples;
  auto objectContexts = ctx->objectR();
  for (auto& objectContext : objectContexts) {
    auto graphNode = visit(objectContext);
    ad_utility::appendVector(additionalTriples, std::move(graphNode.second));
    objects.push_back(std::move(graphNode.first));
  }
  return {std::move(objects), std::move(additionalTriples)};
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::ObjectRContext* ctx) {
  return visit(ctx->graphNode());
}

// ____________________________________________________________________________________
template <typename Context>
void Visitor::setMatchingWordAndScoreVisibleIfPresent(
    // If a triple `?var ql:contains-word "words"` or `?var ql:contains-entity
    // <entity>` is contained in the query, then the variable
    // `?ql_textscore_var` is implicitly created and visible in the query body.
    // Similarly, if a triple `?var ql:contains-word "words"` is contained in
    // the query, then the variable `ql_matchingword_var` is implicitly created
    // and visible in the query body.
    Context* ctx, const TripleWithPropertyPath& triple) {
  const auto& [subject, predicate, object] = triple;

  auto* var = std::get_if<Variable>(&subject);
  auto* propertyPath = std::get_if<PropertyPath>(&predicate);

  if (!var || !propertyPath) {
    return;
  }

  if (propertyPath->asString() == CONTAINS_WORD_PREDICATE) {
    string name = object.toSparql();
    if (!((name.starts_with('"') && name.ends_with('"')) ||
          (name.starts_with('\'') && name.ends_with('\'')))) {
      reportError(ctx,
                  "ql:contains-word has to be followed by a string in quotes");
    }
    for (std::string_view s : std::vector<std::string>(
             absl::StrSplit(name.substr(1, name.size() - 2), ' '))) {
      addVisibleVariable(var->getWordScoreVariable(s, s.ends_with('*')));
      if (!s.ends_with('*')) {
        continue;
      }
      addVisibleVariable(var->getMatchingWordVariable(
          ad_utility::utf8ToLower(s.substr(0, s.size() - 1))));
    }
  } else if (propertyPath->asString() == CONTAINS_ENTITY_PREDICATE) {
    if (const auto* entVar = std::get_if<Variable>(&object)) {
      addVisibleVariable(var->getEntityScoreVariable(*entVar));
    } else {
      addVisibleVariable(var->getEntityScoreVariable(object.toSparql()));
    }
  }
}

// ___________________________________________________________________________
vector<TripleWithPropertyPath> Visitor::visit(
    Parser::TriplesSameSubjectPathContext* ctx) {
  /*
  // If a triple `?var ql:contains-word "words"` or `?var ql:contains-entity
  // <entity>` is contained in the query, then the variable `?ql_textscore_var`
  // is implicitly created and visible in the query body.
  // Similarly if a triple `?var ql:contains-word "words"` is contained in the
  // query, then the variable `ql_matchingword_var` is implicitly created and
  // visible in the query body.
  auto setMatchingWordAndScoreVisibleIfPresent =
      [this, ctx](GraphTerm& subject, VarOrPath& predicate, GraphTerm& object) {
        auto* var = std::get_if<Variable>(&subject);
        auto* propertyPath = std::get_if<PropertyPath>(&predicate);

        if (!var || !propertyPath) {
          return;
        }

        if (propertyPath->asString() == CONTAINS_WORD_PREDICATE) {
          string name = object.toSparql();
          if (!((name.starts_with('"') && name.ends_with('"')) ||
                (name.starts_with('\'') && name.ends_with('\'')))) {
            reportError(
                ctx,
                "ql:contains-word has to be followed by a string in quotes");
          }
          for (std::string_view s : std::vector<std::string>(
                   absl::StrSplit(name.substr(1, name.size() - 2), ' '))) {
            if (!s.ends_with('*')) {
              continue;
            }
            addVisibleVariable(var->getMatchingWordVariable(
                ad_utility::utf8ToLower(s.substr(0, s.size() - 1))));
          }
        } else if (propertyPath->asString() == CONTAINS_ENTITY_PREDICATE) {
          if (const auto* entVar = std::get_if<Variable>(&object)) {
            addVisibleVariable(var->getScoreVariable(*entVar));
          } else {
            addVisibleVariable(var->getScoreVariable(object.toSparql()));
          }
        }
      };
      */

  // Assemble the final result from a set of given `triples` and possibly empty
  // `additionalTriples`, the given `subject` and the given pairs of
  // `[predicate, object]`
  using TripleVec = std::vector<TripleWithPropertyPath>;
  auto assembleResult = [this, ctx](TripleVec triples, GraphTerm subject,
                                    PathObjectPairs predicateObjectPairs,
                                    TripleVec additionalTriples) {
    for (auto&& [predicate, object] : std::move(predicateObjectPairs)) {
      triples.emplace_back(subject, std::move(predicate), std::move(object));
    }
    ql::ranges::copy(additionalTriples, std::back_inserter(triples));
    for (const auto& triple : triples) {
      setMatchingWordAndScoreVisibleIfPresent(ctx, triple);
    }
    return triples;
  };
  if (ctx->varOrTerm()) {
    auto subject = visit(ctx->varOrTerm());
    auto [tuples, triples] = visit(ctx->propertyListPathNotEmpty());
    return assembleResult(std::move(triples), std::move(subject),
                          std::move(tuples), {});
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNodePath());
    auto [subject, result] = visit(ctx->triplesNodePath());
    auto additionalTriples = visit(ctx->propertyListPath());
    if (additionalTriples.has_value()) {
      auto& [tuples, triples] = additionalTriples.value();
      return assembleResult(std::move(result), std::move(subject),
                            std::move(tuples), std::move(triples));
    } else {
      return assembleResult(std::move(result), std::move(subject), {}, {});
    }
  }
}

// ___________________________________________________________________________
std::optional<PathObjectPairsAndTriples> Visitor::visit(
    Parser::PropertyListPathContext* ctx) {
  return visitOptional(ctx->propertyListPathNotEmpty());
}

// ___________________________________________________________________________
PathObjectPairsAndTriples Visitor::visit(
    Parser::PropertyListPathNotEmptyContext* ctx) {
  PathObjectPairsAndTriples result = visit(ctx->tupleWithPath());
  auto& [pairs, triples] = result;
  vector<PathObjectPairsAndTriples> pairsAndTriples =
      visitVector(ctx->tupleWithoutPath());
  for (auto& [newPairs, newTriples] : pairsAndTriples) {
    ql::ranges::move(newPairs, std::back_inserter(pairs));
    ql::ranges::move(newTriples, std::back_inserter(triples));
  }
  return result;
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::VerbPathContext* ctx) {
  return visit(ctx->path());
}

// ____________________________________________________________________________________
Variable Visitor::visit(Parser::VerbSimpleContext* ctx) {
  return visit(ctx->var());
}

// ____________________________________________________________________________________
PathObjectPairsAndTriples Visitor::visit(Parser::TupleWithoutPathContext* ctx) {
  VarOrPath predicate = visit(ctx->verbPathOrSimple());
  ObjectsAndTriples objectList = visit(ctx->objectList());
  auto predicateObjectPairs = joinPredicateAndObject(predicate, objectList);
  std::vector<TripleWithPropertyPath> triples;
  auto toVarOrPath = [](GraphTerm term) -> VarOrPath {
    if (std::holds_alternative<Variable>(term)) {
      return std::get<Variable>(term);
    } else {
      return PropertyPath::fromIri(
          ad_utility::triple_component::Iri::fromStringRepresentation(
              term.toSparql()));
    }
  };
  for (auto& triple : objectList.second) {
    triples.emplace_back(triple[0], toVarOrPath(triple[1]), triple[2]);
  }
  return {std::move(predicateObjectPairs), std::move(triples)};
}

// ____________________________________________________________________________________
PathObjectPairsAndTriples Visitor::visit(Parser::TupleWithPathContext* ctx) {
  VarOrPath predicate = visit(ctx->verbPathOrSimple());
  ObjectsAndPathTriples objectList = visit(ctx->objectListPath());
  auto predicateObjectPairs = joinPredicateAndObject(predicate, objectList);
  return {predicateObjectPairs, std::move(objectList.second)};
}

// ____________________________________________________________________________________
VarOrPath Visitor::visit(Parser::VerbPathOrSimpleContext* ctx) {
  return visitAlternative<ad_utility::sparql_types::VarOrPath>(
      ctx->verbPath(), ctx->verbSimple());
}

// ___________________________________________________________________________
ObjectsAndPathTriples Visitor::visit(Parser::ObjectListPathContext* ctx) {
  auto objectAndTriplesVec = visitVector(ctx->objectPath());
  // First collect all the objects.
  std::vector<GraphTerm> objects;
  ql::ranges::copy(
      objectAndTriplesVec | ql::views::transform(ad_utility::first),
      std::back_inserter(objects));

  // Collect all the triples. Node: `views::join` flattens the input.
  std::vector<TripleWithPropertyPath> triples;
  ql::ranges::copy(objectAndTriplesVec |
                       ql::views::transform(ad_utility::second) |
                       ql::views::join,
                   std::back_inserter(triples));
  return {std::move(objects), std::move(triples)};
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(Parser::ObjectPathContext* ctx) {
  return visit(ctx->graphNodePath());
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathContext* ctx) {
  return visit(ctx->pathAlternative());
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathAlternativeContext* ctx) {
  auto alternatives = visitVector(ctx->pathSequence());
  if (alternatives.size() == 1) {
    return std::move(alternatives.at(0));
  }
  return PropertyPath::makeAlternative(std::move(alternatives));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathSequenceContext* ctx) {
  auto sequence = visitVector(ctx->pathEltOrInverse());
  if (sequence.size() == 1) {
    return std::move(sequence.at(0));
  }
  return PropertyPath::makeSequence(std::move(sequence));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathEltContext* ctx) {
  PropertyPath p = visit(ctx->pathPrimary());

  if (ctx->pathMod()) {
    auto [min, max] = visit(ctx->pathMod());
    p = PropertyPath::makeWithLength(std::move(p), min, max);
  }
  return p;
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathEltOrInverseContext* ctx) {
  PropertyPath p = visit(ctx->pathElt());

  if (ctx->negationOperator) {
    p = PropertyPath::makeInverse(std::move(p));
  }

  return p;
}

// ____________________________________________________________________________________
std::pair<size_t, size_t> Visitor::visit(Parser::PathModContext* ctx) {
  std::string mod = ctx->getText();
  if (mod == "*") {
    return {0, std::numeric_limits<size_t>::max()};
  } else if (mod == "+") {
    return {1, std::numeric_limits<size_t>::max()};
  } else {
    AD_CORRECTNESS_CHECK(mod == "?");
    return {0, 1};
  }
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathPrimaryContext* ctx) {
  if (ctx->iri()) {
    return PropertyPath::fromIri(visit(ctx->iri()));
  } else if (ctx->path()) {
    return visit(ctx->path());
  } else if (ctx->pathNegatedPropertySet()) {
    return visit(ctx->pathNegatedPropertySet());
  } else {
    AD_CORRECTNESS_CHECK(ctx->getText() == "a");
    // Special keyword 'a'
    return PropertyPath::fromIri(a);
  }
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathNegatedPropertySetContext* ctx) {
  return PropertyPath::makeNegated(visitVector(ctx->pathOneInPropertySet()));
}

// ____________________________________________________________________________________
PropertyPath Visitor::visit(Parser::PathOneInPropertySetContext* ctx) {
  auto iri = ctx->iri() ? visit(ctx->iri()) : a;
  const std::string& text = ctx->getText();
  AD_CORRECTNESS_CHECK((iri == a) == (text == "a" || text == "^a"));
  auto propertyPath = PropertyPath::fromIri(std::move(iri));
  if (text.starts_with("^")) {
    return PropertyPath::makeInverse(propertyPath);
  }
  return propertyPath;
}

// ____________________________________________________________________________________
uint64_t Visitor::visit(Parser::IntegerContext* ctx) {
  try {
    // unsigned long long int might be larger than 8 bytes as per the standard.
    // If that were the case this could lead to overflows.
    // TODO<joka921> Use `std::from_chars` but first check for the compiler
    //  support.
    static_assert(sizeof(unsigned long long int) == sizeof(uint64_t));
    return std::stoull(ctx->getText());
  } catch (const std::out_of_range&) {
    reportNotSupported(ctx, "Integer " + ctx->getText() +
                                " does not fit into 64 bits. This is ");
  }
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::TriplesNodeContext* ctx) {
  return visitAlternative<SubjectOrObjectAndTriples>(
      ctx->collection(), ctx->blankNodePropertyList());
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(
    Parser::BlankNodePropertyListContext* ctx) {
  GraphTerm term = newBlankNodeOrVariable();
  Triples triples;
  auto propertyList = visit(ctx->propertyListNotEmpty());
  for (auto& [predicate, object] : propertyList.first) {
    triples.push_back({term, std::move(predicate), std::move(object)});
  }
  ad_utility::appendVector(triples, std::move(propertyList.second));
  return {std::move(term), std::move(triples)};
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::TriplesNodePathContext* ctx) {
  return visitAlternative<SubjectOrObjectAndPathTriples>(
      ctx->blankNodePropertyListPath(), ctx->collectionPath());
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::BlankNodePropertyListPathContext* ctx) {
  auto subject = parsedQuery_.getNewInternalVariable();
  auto [predicateObjects, triples] = visit(ctx->propertyListPathNotEmpty());
  for (auto& [predicate, object] : predicateObjects) {
    triples.emplace_back(subject, std::move(predicate), std::move(object));
  }
  return {std::move(subject), triples};
}

// _____________________________________________________________________________
template <typename TripleType, typename Func>
TripleType Visitor::toRdfCollection(std::vector<TripleType> elements,
                                    Func iriStringToPredicate) {
  typename TripleType::second_type triples;
  GraphTerm nextTerm{Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"}};
  for (auto& graphNode : ql::ranges::reverse_view(elements)) {
    GraphTerm currentTerm = newBlankNodeOrVariable();
    triples.push_back(
        {currentTerm,
         iriStringToPredicate(
             "<http://www.w3.org/1999/02/22-rdf-syntax-ns#first>"),
         std::move(graphNode.first)});
    triples.push_back({currentTerm,
                       iriStringToPredicate(
                           "<http://www.w3.org/1999/02/22-rdf-syntax-ns#rest>"),
                       std::move(nextTerm)});
    nextTerm = std::move(currentTerm);

    ad_utility::appendVector(triples, std::move(graphNode.second));
  }
  return {std::move(nextTerm), std::move(triples)};
}

// _____________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::CollectionContext* ctx) {
  return toRdfCollection(visitVector(ctx->graphNode()), [](std::string iri) {
    return GraphTerm{Iri{std::move(iri)}};
  });
}

// _____________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::CollectionPathContext* ctx) {
  return toRdfCollection(
      visitVector(ctx->graphNodePath()), [](std::string_view iri) {
        return PropertyPath::fromIri(
            ad_utility::triple_component::Iri::fromIriref(iri));
      });
}

// ____________________________________________________________________________________
SubjectOrObjectAndTriples Visitor::visit(Parser::GraphNodeContext* ctx) {
  if (ctx->varOrTerm()) {
    return {visit(ctx->varOrTerm()), Triples{}};
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNode());
    return visit(ctx->triplesNode());
  }
}

// ____________________________________________________________________________________
SubjectOrObjectAndPathTriples Visitor::visit(
    Parser::GraphNodePathContext* ctx) {
  if (ctx->varOrTerm()) {
    return {visit(ctx->varOrTerm()), {}};
  } else {
    AD_CORRECTNESS_CHECK(ctx->triplesNodePath());
    return visit(ctx->triplesNodePath());
  }
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::VarOrTermContext* ctx) {
  return visitAlternative<GraphTerm>(ctx->var(), ctx->graphTerm());
}

// ____________________________________________________________________________________
VarOrIri Visitor::visit(Parser::VarOrIriContext* ctx) {
  return visitAlternative<VarOrIri>(ctx->var(), ctx->iri());
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::GraphTermContext* ctx) {
  if (ctx->blankNode()) {
    return visit(ctx->blankNode());
  } else if (ctx->iri()) {
    // TODO<joka921> Unify.
    return Iri{std::string{visit(ctx->iri()).toStringRepresentation()}};
  } else if (ctx->NIL()) {
    return Iri{"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"};
  } else {
    return visitAlternative<Literal>(ctx->numericLiteral(),
                                     ctx->booleanLiteral(), ctx->rdfLiteral());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConditionalOrExpressionContext* ctx) {
  auto childContexts = ctx->conditionalAndExpression();
  auto children = visitVector(ctx->conditionalAndExpression());
  AD_CONTRACT_CHECK(!children.empty());
  auto result = std::move(children.front());
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = sparqlExpression::makeOrExpression(std::move(result),
                                                              std::move(ptr));
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ConditionalAndExpressionContext* ctx) {
  auto children = visitVector(ctx->valueLogical());
  AD_CONTRACT_CHECK(!children.empty());
  auto result = std::move(children.front());
  std::for_each(children.begin() + 1, children.end(),
                [&result](ExpressionPtr& ptr) {
                  result = sparqlExpression::makeAndExpression(
                      std::move(result), std::move(ptr));
                });
  result->descriptor() = ctx->getText();
  return result;
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ValueLogicalContext* ctx) {
  return visit(ctx->relationalExpression());
}

// ___________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::RelationalExpressionContext* ctx) {
  auto children = visitVector(ctx->numericExpression());

  if (ctx->expressionList()) {
    auto lhs = visitVector(ctx->numericExpression());
    AD_CORRECTNESS_CHECK(lhs.size() == 1);
    auto expressions = visit(ctx->expressionList());
    auto inExpression = std::make_unique<InExpression>(std::move(lhs.at(0)),
                                                       std::move(expressions));
    if (ctx->notToken) {
      return makeUnaryNegateExpression(std::move(inExpression));
    } else {
      return inExpression;
    }
  }
  AD_CONTRACT_CHECK(children.size() == 1 || children.size() == 2);
  if (children.size() == 1) {
    return std::move(children[0]);
  }

  auto make = [&](auto t) {
    using Expr = typename decltype(t)::type;
    return createExpression<Expr>(std::move(children[0]),
                                  std::move(children[1]));
  };

  std::string relation = ctx->children[1]->getText();
  if (relation == "=") {
    return make(ti<EqualExpression>);
  } else if (relation == "!=") {
    return make(ti<NotEqualExpression>);
  } else if (relation == "<") {
    return make(ti<LessThanExpression>);
  } else if (relation == ">") {
    return make(ti<GreaterThanExpression>);
  } else if (relation == "<=") {
    return make(ti<LessEqualExpression>);
  } else {
    AD_CORRECTNESS_CHECK(relation == ">=");
    return make(ti<GreaterEqualExpression>);
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::NumericExpressionContext* ctx) {
  return visit(ctx->additiveExpression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::AdditiveExpressionContext* ctx) {
  auto result = visit(ctx->multiplicativeExpression());

  for (OperatorAndExpression& signAndExpression :
       visitVector(ctx->multiplicativeExpressionWithSign())) {
    switch (signAndExpression.operator_) {
      case Operator::Plus:
        result = sparqlExpression::makeAddExpression(
            std::move(result), std::move(signAndExpression.expression_));
        break;
      case Operator::Minus:
        result = sparqlExpression::makeSubtractExpression(
            std::move(result), std::move(signAndExpression.expression_));
        break;
      default:
        AD_FAIL();
    }
  }
  return result;
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplicativeExpressionWithSignContext* ctx) {
  return visitAlternative<OperatorAndExpression>(
      ctx->plusSubexpression(), ctx->minusSubexpression(),
      ctx->multiplicativeExpressionWithLeadingSignButNoSpace());
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::PlusSubexpressionContext* ctx) {
  return {Operator::Plus, visit(ctx->multiplicativeExpression())};
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MinusSubexpressionContext* ctx) {
  return {Operator::Minus, visit(ctx->multiplicativeExpression())};
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext* ctx) {
  Operator op =
      ctx->numericLiteralPositive() ? Operator::Plus : Operator::Minus;

  // Helper function that inverts a number if  the leading sign of this
  // expression is `-`
  auto invertIfNecessary = [ctx](auto number) {
    return ctx->numericLiteralPositive() ? number : -number;
  };

  // Create the initial expression from a double literal
  auto createFromDouble = [&](double d) -> ExpressionPtr {
    return std::make_unique<sparqlExpression::IdExpression>(
        Id::makeFromDouble(invertIfNecessary(d)));
  };
  auto createFromInt = [&](int64_t i) -> ExpressionPtr {
    return std::make_unique<sparqlExpression::IdExpression>(
        Id::makeFromInt(invertIfNecessary(i)));
  };

  auto literalAsVariant = visitAlternative<IntOrDouble>(
      ctx->numericLiteralPositive(), ctx->numericLiteralNegative());

  auto expression = std::visit(
      ad_utility::OverloadCallOperator{createFromInt, createFromDouble},
      literalAsVariant);

  for (OperatorAndExpression& opAndExp :
       visitVector(ctx->multiplyOrDivideExpression())) {
    switch (opAndExp.operator_) {
      case Operator::Multiply:
        expression = sparqlExpression::makeMultiplyExpression(
            std::move(expression), std::move(opAndExp.expression_));
        break;
      case Operator::Divide:
        expression = sparqlExpression::makeDivideExpression(
            std::move(expression), std::move(opAndExp.expression_));
        break;
      default:
        AD_FAIL();
    }
  }
  return {op, std::move(expression)};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::MultiplicativeExpressionContext* ctx) {
  auto result = visit(ctx->unaryExpression());

  for (OperatorAndExpression& opAndExp :
       visitVector(ctx->multiplyOrDivideExpression())) {
    switch (opAndExp.operator_) {
      case Operator::Multiply:
        result = sparqlExpression::makeMultiplyExpression(
            std::move(result), std::move(opAndExp.expression_));
        break;
      case Operator::Divide:
        result = sparqlExpression::makeDivideExpression(
            std::move(result), std::move(opAndExp.expression_));
        break;
      default:
        AD_FAIL();
    }
  }
  return result;
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplyOrDivideExpressionContext* ctx) {
  return visitAlternative<OperatorAndExpression>(ctx->multiplyExpression(),
                                                 ctx->divideExpression());
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::MultiplyExpressionContext* ctx) {
  return {Operator::Multiply, visit(ctx->unaryExpression())};
}

// ____________________________________________________________________________________
Visitor::OperatorAndExpression Visitor::visit(
    Parser::DivideExpressionContext* ctx) {
  return {Operator::Divide, visit(ctx->unaryExpression())};
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::UnaryExpressionContext* ctx) {
  auto child = visit(ctx->primaryExpression());
  if (ctx->children[0]->getText() == "-") {
    return sparqlExpression::makeUnaryMinusExpression(std::move(child));
  } else if (ctx->children[0]->getText() == "!") {
    return sparqlExpression::makeUnaryNegateExpression(std::move(child));
  } else {
    // no sign or an explicit '+'
    return child;
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::PrimaryExpressionContext* ctx) {
  using std::make_unique;
  using namespace sparqlExpression;

  if (ctx->rdfLiteral()) {
    auto tripleComponent =
        RdfStringParser<TurtleParser<TokenizerCtre>>::parseTripleObject(
            visit(ctx->rdfLiteral()));
    AD_CORRECTNESS_CHECK(!tripleComponent.isIri() &&
                         !tripleComponent.isString());
    if (tripleComponent.isLiteral()) {
      return make_unique<StringLiteralExpression>(tripleComponent.getLiteral());
    } else {
      return make_unique<IdExpression>(
          tripleComponent.toValueIdIfNotString().value());
    }
  } else if (ctx->numericLiteral()) {
    auto integralWrapper = [](int64_t x) {
      return ExpressionPtr{make_unique<IdExpression>(Id::makeFromInt(x))};
    };
    auto doubleWrapper = [](double x) {
      return ExpressionPtr{make_unique<IdExpression>(Id::makeFromDouble(x))};
    };
    return std::visit(
        ad_utility::OverloadCallOperator{integralWrapper, doubleWrapper},
        visit(ctx->numericLiteral()));
  } else if (ctx->booleanLiteral()) {
    return make_unique<IdExpression>(
        Id::makeFromBool(visit(ctx->booleanLiteral())));
  } else if (ctx->var()) {
    return make_unique<VariableExpression>(visit(ctx->var()));
  } else {
    return visitAlternative<ExpressionPtr>(
        ctx->builtInCall(), ctx->iriOrFunction(), ctx->brackettedExpression());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::BrackettedExpressionContext* ctx) {
  return visit(ctx->expression());
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit([[maybe_unused]] Parser::BuiltInCallContext* ctx) {
  if (ctx->aggregate()) {
    return visit(ctx->aggregate());
  } else if (ctx->regexExpression()) {
    return visit(ctx->regexExpression());
  } else if (ctx->langExpression()) {
    return visit(ctx->langExpression());
  } else if (ctx->substringExpression()) {
    return visit(ctx->substringExpression());
  } else if (ctx->strReplaceExpression()) {
    return visit(ctx->strReplaceExpression());
  } else if (ctx->existsFunc()) {
    return visit(ctx->existsFunc());
  } else if (ctx->notExistsFunc()) {
    return visit(ctx->notExistsFunc());
  }
  // Get the function name and the arguments. Note that we do not have to check
  // the number of arguments like for `processIriFunctionCall`, since the number
  // of arguments is fixed by the grammar and we wouldn't even get here if the
  // number were wrong. Hence only the `AD_CONTRACT_CHECK`s.
  AD_CONTRACT_CHECK(!ctx->children.empty());
  auto functionName = ad_utility::getLowercase(ctx->children[0]->getText());
  auto argList = visitVector(ctx->expression());
  using namespace sparqlExpression;
  // Create the expression using the matching factory function from
  // `NaryExpression.h`.
  auto createUnary = CPP_template_lambda(&argList)(typename F)(F function)(
      requires std::is_invocable_r_v<ExpressionPtr, F, ExpressionPtr>) {
    AD_CORRECTNESS_CHECK(argList.size() == 1, argList.size());
    return function(std::move(argList[0]));
  };

  auto createBinary = CPP_template_lambda(&argList)(typename F)(F function)(
      requires std::is_invocable_r_v<ExpressionPtr, F, ExpressionPtr,
                                     ExpressionPtr>) {
    AD_CORRECTNESS_CHECK(argList.size() == 2);
    return function(std::move(argList[0]), std::move(argList[1]));
  };

  auto createTernary = CPP_template_lambda(&argList)(typename F)(F function)(
      requires std::is_invocable_r_v<ExpressionPtr, F, ExpressionPtr,
                                     ExpressionPtr, ExpressionPtr>) {
    AD_CORRECTNESS_CHECK(argList.size() == 3);
    return function(std::move(argList[0]), std::move(argList[1]),
                    std::move(argList[2]));
  };
  if (functionName == "str") {
    return createUnary(&makeStrExpression);
  } else if (functionName == "iri" || functionName == "uri") {
    AD_CORRECTNESS_CHECK(argList.size() == 1, argList.size());
    return makeIriOrUriExpression(std::move(argList[0]),
                                  std::make_unique<IriExpression>(baseIri_));
  } else if (functionName == "strlang") {
    return createBinary(&makeStrLangTagExpression);
  } else if (functionName == "strdt") {
    return createBinary(&makeStrIriDtExpression);
  } else if (functionName == "strlen") {
    return createUnary(&makeStrlenExpression);
  } else if (functionName == "strbefore") {
    return createBinary(&makeStrBeforeExpression);
  } else if (functionName == "strafter") {
    return createBinary(&makeStrAfterExpression);
  } else if (functionName == "contains") {
    return createBinary(&makeContainsExpression);
  } else if (functionName == "strends") {
    return createBinary(&makeStrEndsExpression);
  } else if (functionName == "strstarts") {
    return createBinary(&makeStrStartsExpression);
  } else if (functionName == "ucase") {
    return createUnary(&makeUppercaseExpression);
  } else if (functionName == "lcase") {
    return createUnary(&makeLowercaseExpression);
  } else if (functionName == "year") {
    return createUnary(&makeYearExpression);
  } else if (functionName == "month") {
    return createUnary(&makeMonthExpression);
  } else if (functionName == "day") {
    return createUnary(&makeDayExpression);
  } else if (functionName == "tz") {
    return createUnary(&makeTimezoneStrExpression);
  } else if (functionName == "timezone") {
    return createUnary(&makeTimezoneExpression);
  } else if (functionName == "now") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<NowDatetimeExpression>(startTime_);
  } else if (functionName == "hours") {
    return createUnary(&makeHoursExpression);
  } else if (functionName == "minutes") {
    return createUnary(&makeMinutesExpression);
  } else if (functionName == "seconds") {
    return createUnary(&makeSecondsExpression);
  } else if (functionName == "md5") {
    return createUnary(&makeMD5Expression);
  } else if (functionName == "sha1") {
    return createUnary(&makeSHA1Expression);
  } else if (functionName == "sha256") {
    return createUnary(&makeSHA256Expression);
  } else if (functionName == "sha384") {
    return createUnary(&makeSHA384Expression);
  } else if (functionName == "sha512") {
    return createUnary(&makeSHA512Expression);
  } else if (functionName == "rand") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<RandomExpression>();
  } else if (functionName == "uuid") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<UuidExpression>();
  } else if (functionName == "struuid") {
    AD_CONTRACT_CHECK(argList.empty());
    return std::make_unique<StrUuidExpression>();
  } else if (functionName == "ceil") {
    return createUnary(&makeCeilExpression);
  } else if (functionName == "abs") {
    return createUnary(&makeAbsExpression);
  } else if (functionName == "round") {
    return createUnary(&makeRoundExpression);
  } else if (functionName == "floor") {
    return createUnary(&makeFloorExpression);
  } else if (functionName == "if") {
    return createTernary(&makeIfExpression);
  } else if (functionName == "coalesce") {
    AD_CORRECTNESS_CHECK(ctx->expressionList());
    return makeCoalesceExpression(visit(ctx->expressionList()));
  } else if (functionName == "encode_for_uri") {
    return createUnary(&makeEncodeForUriExpression);
  } else if (functionName == "concat") {
    AD_CORRECTNESS_CHECK(ctx->expressionList());
    return makeConcatExpression(visit(ctx->expressionList()));
  } else if (functionName == "isiri" || functionName == "isuri") {
    return createUnary(&makeIsIriExpression);
  } else if (functionName == "isblank") {
    return createUnary(&makeIsBlankExpression);
  } else if (functionName == "isliteral") {
    return createUnary(&makeIsLiteralExpression);
  } else if (functionName == "isnumeric") {
    return createUnary(&makeIsNumericExpression);
  } else if (functionName == "datatype") {
    return createUnary(&makeDatatypeExpression);
  } else if (functionName == "langmatches") {
    return createBinary(&makeLangMatchesExpression);
  } else if (functionName == "bound") {
    return makeBoundExpression(
        std::make_unique<VariableExpression>(visit(ctx->var())));
  } else if (functionName == "bnode") {
    if (ctx->NIL()) {
      return makeUniqueBlankNodeExpression();
    } else {
      return createUnary(&makeBlankNodeExpression);
    }
  } else {
    reportError(
        ctx,
        absl::StrCat("Built-in function \"", functionName,
                     "\"  not yet implemented; if you need it, just add it to ",
                     "SparqlQleverVisitor.cpp::visitTypesafe(Parser::"
                     "BuiltInCallContext ",
                     "following the already implemented functions there"));
  }
}

// _____________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::RegexExpressionContext* ctx) {
  const auto& exp = ctx->expression();
  const auto& numArgs = exp.size();
  AD_CONTRACT_CHECK(numArgs >= 2 && numArgs <= 3);
  auto flags = numArgs == 3 ? visitOptional(exp[2]) : std::nullopt;
  try {
    return makeRegexExpression(visit(exp[0]), visit(exp[1]),
                               std::move(flags).value_or(nullptr));
  } catch (const std::exception& e) {
    reportError(ctx, e.what());
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::LangExpressionContext* ctx) {
  // The number of children for expression LANG() is fixed to one by the
  // grammar (or definition of the parser).
  return sparqlExpression::makeLangExpression(visit(ctx->expression()));
}

// ____________________________________________________________________________________
SparqlExpression::Ptr Visitor::visit(Parser::SubstringExpressionContext* ctx) {
  auto children = visitVector(ctx->expression());
  AD_CORRECTNESS_CHECK(children.size() == 2 || children.size() == 3);
  if (children.size() == 2) {
    children.push_back(
        std::make_unique<IdExpression>(Id::makeFromInt(Id::maxInt)));
  }
  AD_CONTRACT_CHECK(children.size() == 3);
  return sparqlExpression::makeSubstrExpression(std::move(children.at(0)),
                                                std::move(children.at(1)),
                                                std::move(children.at(2)));
}

// ____________________________________________________________________________________
SparqlExpression::Ptr Visitor::visit(Parser::StrReplaceExpressionContext* ctx) {
  auto children = visitVector(ctx->expression());
  AD_CORRECTNESS_CHECK(children.size() == 3 || children.size() == 4);
  return makeReplaceExpression(
      std::move(children.at(0)), std::move(children.at(1)),
      std::move(children.at(2)),
      children.size() == 4 ? std::move(children.at(3)) : nullptr);
}

// ____________________________________________________________________________
ExpressionPtr Visitor::visitExists(Parser::GroupGraphPatternContext* pattern,
                                   bool negate) {
  // The argument of 'EXISTS` is a `GroupGraphPattern` that is independent from
  // the rest of the query (except for the `FROM` and `FROM NAMED` clauses,
  // which also apply to the argument of `EXISTS`). We therefore have to back up
  // and restore all global state when parsing `EXISTS`.
  auto queryBackup = std::exchange(parsedQuery_, ParsedQuery{});
  auto visibleVariablesBackup = std::move(visibleVariables_);
  visibleVariables_.clear();

  // Parse the argument of `EXISTS`.
  auto group = visit(pattern);
  ParsedQuery argumentOfExists =
      std::exchange(parsedQuery_, std::move(queryBackup));
  SelectClause& selectClause = argumentOfExists.selectClause();
  // Even though we set the `SELECT` clause to `*`, we will limit the visible
  // variables to a potentially smaller subset when finishing the parsing of the
  // current group.
  selectClause.setAsterisk();
  // `ExistsExpression`s are not parsed like regular `SparqlExpression`s, so
  // they don't have a proper hierarchy of dependent variables. Because of that,
  // we need to manually add all variables that are visible after parsing the
  // body of `EXISTS`.
  for (const Variable& variable : visibleVariables_) {
    selectClause.addVisibleVariable(variable);
  }
  argumentOfExists._rootGraphPattern = std::move(group);

  // The argument of `EXISTS` inherits the `FROM` and `FROM NAMED` clauses from
  // the outer query.
  argumentOfExists.datasetClauses_ = activeDatasetClauses_;
  visibleVariables_ = std::move(visibleVariablesBackup);
  auto exists = std::make_unique<sparqlExpression::ExistsExpression>(
      std::move(argumentOfExists));

  // Handle `NOT EXISTS` (which is syntactically distinct from `! EXISTS`) by
  // simply negating the `ExistsExpression`.
  if (negate) {
    return sparqlExpression::makeUnaryNegateExpression(std::move(exists));
  } else {
    return exists;
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::ExistsFuncContext* ctx) {
  return visitExists(ctx->groupGraphPattern(), false);
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::NotExistsFuncContext* ctx) {
  return visitExists(ctx->groupGraphPattern(), true);
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::AggregateContext* ctx) {
  using namespace sparqlExpression;
  const auto& children = ctx->children;
  std::string functionName =
      ad_utility::getLowercase(children.at(0)->getText());

  const bool distinct = ql::ranges::any_of(children, [](auto* child) {
    return ad_utility::getLowercase(child->getText()) == "distinct";
  });
  // the only case that there is no child expression is COUNT(*), so we can
  // check this outside the if below.
  if (!ctx->expression()) {
    AD_CORRECTNESS_CHECK(functionName == "count");
    return makeCountStarExpression(distinct);
  }
  auto childExpression = visit(ctx->expression());
  auto makePtr = [&](auto t, auto&&... additionalArgs) {
    using ExpressionType = typename decltype(t)::type;
    ExpressionPtr result{std::make_unique<ExpressionType>(
        distinct, std::move(childExpression), AD_FWD(additionalArgs)...)};
    result->descriptor() = ctx->getText();
    return result;
  };

  if (functionName == "count") {
    return makePtr(ti<CountExpression>);
  } else if (functionName == "sum") {
    return makePtr(ti<SumExpression>);
  } else if (functionName == "max") {
    return makePtr(ti<MaxExpression>);
  } else if (functionName == "min") {
    return makePtr(ti<MinExpression>);
  } else if (functionName == "avg") {
    return makePtr(ti<AvgExpression>);
  } else if (functionName == "group_concat") {
    // Use a space as a default separator

    std::string separator;
    if (ctx->string()) {
      // TODO: The string rule also allow triple quoted strings with different
      //  escaping rules. These are currently not handled. They should be
      //  parsed into a typesafe format with a unique representation.
      separator = visit(ctx->string()).get();
      // If there was a separator, we have to strip the quotation marks
      AD_CONTRACT_CHECK(separator.size() >= 2);
      separator = separator.substr(1, separator.size() - 2);
    } else {
      separator = " "s;
    }

    return makePtr(ti<GroupConcatExpression>, std::move(separator));
  } else if (functionName == "stdev") {
    return makePtr(ti<StdevExpression>);
  } else {
    AD_CORRECTNESS_CHECK(functionName == "sample");
    return makePtr(ti<SampleExpression>);
  }
}

// ____________________________________________________________________________________
ExpressionPtr Visitor::visit(Parser::IriOrFunctionContext* ctx) {
  // Case 1: Just an IRI.
  if (ctx->argList() == nullptr) {
    return std::make_unique<sparqlExpression::IriExpression>(visit(ctx->iri()));
  }
  // Case 2: Function call, where the function name is an IRI.
  return processIriFunctionCall(visit(ctx->iri()), visit(ctx->argList()), ctx);
}

// ____________________________________________________________________________________
std::string Visitor::visit(Parser::RdfLiteralContext* ctx) {
  // TODO: This should really be an RdfLiteral class that stores a unified
  //  version of the string, and the langtag/datatype separately.
  string ret = ctx->string()->getText();
  if (ctx->LANGTAG()) {
    ret += ctx->LANGTAG()->getText();
  } else if (ctx->iri()) {
    // TODO<joka921> Also unify the two Literal classes...
    ret += ("^^" + std::string{visit(ctx->iri()).toStringRepresentation()});
  }
  return ret;
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralContext* ctx) {
  return visitAlternative<std::variant<int64_t, double>>(
      ctx->numericLiteralUnsigned(), ctx->numericLiteralPositive(),
      ctx->numericLiteralNegative());
}

// ____________________________________________________________________________________
namespace {
template <typename Ctx>
std::variant<int64_t, double> parseNumericLiteral(Ctx* ctx, bool parseAsInt) {
  try {
    if (parseAsInt) {
      return std::stoll(ctx->getText());
    } else {
      return std::stod(ctx->getText());
    }
  } catch (const std::out_of_range& range) {
    SparqlQleverVisitor::reportError(ctx, "Could not parse numeric literal \"" +
                                              ctx->getText() +
                                              "\" because it is out of range.");
  }
}
}  // namespace

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralUnsignedContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER());
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralPositiveContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER_POSITIVE());
}

// ____________________________________________________________________________________
std::variant<int64_t, double> Visitor::visit(
    Parser::NumericLiteralNegativeContext* ctx) {
  return parseNumericLiteral(ctx, ctx->INTEGER_NEGATIVE());
}

// ____________________________________________________________________________________
bool Visitor::visit(Parser::BooleanLiteralContext* ctx) {
  return ctx->getText() == "true";
}

// ____________________________________________________________________________________
GraphTerm Visitor::visit(Parser::BlankNodeContext* ctx) {
  if (ctx->ANON()) {
    return newBlankNodeOrVariable();
  } else {
    AD_CORRECTNESS_CHECK(ctx->BLANK_NODE_LABEL());
    if (isInsideConstructTriples_) {
      // Strip `_:` prefix from string.
      constexpr size_t length = std::string_view{"_:"}.length();
      const string label = ctx->BLANK_NODE_LABEL()->getText().substr(length);
      // `False` means the blank node is not automatically generated, but
      // explicitly specified in the query.
      return BlankNode{false, label};
    } else {
      return ParsedQuery::blankNodeToInternalVariable(
          ctx->BLANK_NODE_LABEL()->getText());
    }
  }
}

// ____________________________________________________________________________________
CPP_template_def(typename Ctx)(
    requires Visitor::voidWhenVisited<Visitor, Ctx>) void Visitor::
    visitVector(const vector<Ctx*>& childContexts) {
  for (const auto& child : childContexts) {
    visit(child);
  }
}

// ____________________________________________________________________________________
CPP_template_def(typename Ctx)(
    requires CPP_NOT(Visitor::voidWhenVisited<Visitor, Ctx>))
    [[nodiscard]] auto Visitor::visitVector(
        const std::vector<Ctx*>& childContexts)
        -> std::vector<decltype(visit(childContexts[0]))> {
  std::vector<decltype(visit(childContexts[0]))> children;
  for (const auto& child : childContexts) {
    children.emplace_back(visit(child));
  }
  return children;
}

// ____________________________________________________________________________________
template <typename Out, typename... Contexts>
Out Visitor::visitAlternative(Contexts*... ctxs) {
  // Check that exactly one of the `ctxs` is not `nullptr`.
  AD_CONTRACT_CHECK(1u == (... + static_cast<bool>(ctxs)));
  if constexpr (std::is_void_v<Out>) {
    (..., visitIf(ctxs));
  } else {
    std::optional<Out> out;
    // Visit the one `context` which is not null and write the result to
    // `out`.
    (..., visitIf<std::optional<Out>, Out>(&out, ctxs));
    return std::move(out.value());
  }
}

// ____________________________________________________________________________________
template <typename Ctx>
auto Visitor::visitOptional(Ctx* ctx) -> std::optional<decltype(visit(ctx))> {
  if (ctx) {
    return visit(ctx);
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________________
template <typename Target, typename Intermediate, typename Ctx>
void Visitor::visitIf(Target* target, Ctx* ctx) {
  if (ctx) {
    *target = Intermediate{visit(ctx)};
  }
}

// _____________________________________________________________________________
CPP_template_def(typename Ctx)(requires Visitor::voidWhenVisited<
                               Visitor, Ctx>) void Visitor::visitIf(Ctx* ctx) {
  if (ctx) {
    visit(ctx);
  }
}

// _____________________________________________________________________________
void Visitor::reportError(const antlr4::ParserRuleContext* ctx,
                          const std::string& msg) {
  throw InvalidSparqlQueryException{
      msg, ad_utility::antlr_utility::generateAntlrExceptionMetadata(ctx)};
}

// _____________________________________________________________________________
void Visitor::reportNotSupported(const antlr4::ParserRuleContext* ctx,
                                 const std::string& feature) {
  throw NotSupportedException{
      feature + " currently not supported by QLever.",
      ad_utility::antlr_utility::generateAntlrExceptionMetadata(ctx)};
}

// _____________________________________________________________________________
void SparqlQleverVisitor::visitWhereClause(
    Parser::WhereClauseContext* whereClauseContext, ParsedQuery& query) {
  if (!whereClauseContext) {
    return;
  }
  auto [pattern, visibleVariables] = visit(whereClauseContext);
  query._rootGraphPattern = std::move(pattern);
  query.registerVariablesVisibleInQueryBody(visibleVariables);
}
