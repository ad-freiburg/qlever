//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "RelationalExpressions.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/GeoSparqlHelpers.h"
#include "util/LambdaHelpers.h"
#include "util/TypeTraits.h"

using namespace sparqlExpression;

namespace {

constexpr int reductionFactorEquals = 1000;
constexpr int reductionFactorNotEquals = 1;
constexpr int reductionFactorDefault = 50;

using valueIdComparators::Comparison;

// Several concepts used to choose the proper evaluation methods for different
// input types.

// For the various `SingleExpressionResult`s the `idGenerator` function returns
// a generator that yields `targetSize` many `ValueId`s. One exception is the
// case where `S` is `string` or `vector<string>`. In this case the generator
// yields `pair<ValueId, ValueId>` (see `makeValueId` and `getRangeFromVocab`
// above for details).

// First the `idGenerator` for constants (string, int, double). It yields the
// same ID `targetSize` many times.
CPP_template(typename S)(
    requires SingleExpressionResult<S> CPP_and
        isConstantResult<S>) auto idGenerator(const S& value, size_t targetSize,
                                              const EvaluationContext* context)
    -> cppcoro::generator<const decltype(makeValueId(value, context))> {
  auto id = makeValueId(value, context);
  for (size_t i = 0; i < targetSize; ++i) {
    co_yield id;
  }
}

// Version of `idGenerator` for vectors. Asserts that the size of the vector is
// equal to `targetSize` and the yields the corresponding ID for each of the
// elements in the vector.
CPP_template(typename S)(
    requires SingleExpressionResult<S> CPP_and
        isVectorResult<S>) auto idGenerator(const S& values, size_t targetSize,
                                            const EvaluationContext* context)
    -> cppcoro::generator<decltype(makeValueId(values[0], context))> {
  AD_CONTRACT_CHECK(targetSize == values.size());
  for (const auto& el : values) {
    auto id = makeValueId(el, context);
    co_yield id;
  }
}

// For the `Variable` and `SetOfIntervals` class, the generator from the
// `sparqlExpressions` module already yields the `ValueIds`.
CPP_template(typename S)(
    requires ad_utility::SimilarToAny<
        S, Variable,
        ad_utility::SetOfIntervals>) auto idGenerator(S input,
                                                      size_t targetSize,
                                                      const EvaluationContext*
                                                          context) {
  return sparqlExpression::detail::makeGenerator(std::move(input), targetSize,
                                                 context);
}

// Return a pair of generators that generate the values from `value1` and
// `value2`. The type of generators is chosen to meet the needs of comparing
// `value1` and `value2`. If any of them logically stores `ValueId`s (true for
// `ValueId, vector<ValueId>, Variable`), then `idGenerator`s are returned for
// both inputs. Else the "plain" generators from `sparqlExpression::detail` are
// returned. These simply yield the values unchanged.
CPP_template(typename S1, typename S2)(
    requires SingleExpressionResult<S1> CPP_and SingleExpressionResult<
        S2>) auto getGenerators(S1&& value1, S2&& value2, size_t targetSize,
                                const EvaluationContext* context) {
  if constexpr (StoresValueId<S1> || StoresValueId<S2>) {
    return std::pair{idGenerator(AD_FWD(value1), targetSize, context),
                     idGenerator(AD_FWD(value2), targetSize, context)};
  } else {
    return std::pair{sparqlExpression::detail::makeGenerator(
                         AD_FWD(value1), targetSize, context),
                     sparqlExpression::detail::makeGenerator(
                         AD_FWD(value2), targetSize, context)};
  }
}

// Efficiently (using binary search) computes the result of `aVal Comparator
// valueId` for each `ValueId` that the Variable `variable` is bound to. This
// only works, if the input (as stored in the `context`) is sorted by the
// Variable `variable`. If `valueIdUpper` is nullopt, we only have a single
// `valueId`, otherwise we have a range, [`valueId`, `valueIdUpper`).
template <Comparison Comp>
ad_utility::SetOfIntervals evaluateWithBinarySearch(
    const Variable& variable, ValueId valueId,
    std::optional<ValueId> valueIdUpper, const EvaluationContext* context) {
  // Set up iterators into the `IdTable` that only access the column where the
  // `variable` is stored.
  auto columnIndex = context->getColumnIndexForVariable(variable);
  AD_CORRECTNESS_CHECK(
      columnIndex.has_value(),
      "The input must be sorted to evaluate a relational expression using "
      "binary search. This should never happen for unbound variables");

  auto getIdFromColumn = ad_utility::makeAssignableLambda(
      [columnIndex = columnIndex.value()](const auto& idTable, auto i) {
        return idTable(i, columnIndex);
      });

  using Iterator = ad_utility::IteratorForAccessOperator<
      std::decay_t<decltype(context->_inputTable)>, decltype(getIdFromColumn)>;
  auto begin =
      Iterator{&context->_inputTable, context->_beginIndex, getIdFromColumn};
  auto end =
      Iterator{&context->_inputTable, context->_endIndex, getIdFromColumn};

  // Perform the actual evaluation.
  const auto resultRanges = [&]() {
    if (valueIdUpper) {
      return valueIdComparators::getRangesForEqualIds(
          begin, end, valueId, valueIdUpper.value(), Comp);
    } else {
      return valueIdComparators::getRangesForId(begin, end, valueId, Comp);
    }
  }();

  // Convert pairs of iterators to pairs of indexes.
  ad_utility::SetOfIntervals s;
  for (const auto& [rangeBegin, rangeEnd] : resultRanges) {
    s._intervals.emplace_back(rangeBegin - begin, rangeEnd - begin);
  }
  return s;
}

// The actual comparison function for the `SingleExpressionResult`'s which are
// `AreComparable` (see above), which means that the comparison between them is
// supported and not always false.
CPP_template(Comparison Comp, typename S1, typename S2)(
    requires SingleExpressionResult<S1> CPP_and SingleExpressionResult<S2>
        CPP_and AreComparable<S1, S2>) ExpressionResult
    evaluateRelationalExpression(S1&& value1, S2&& value2,
                                 const EvaluationContext* context) {
  auto resultSize =
      sparqlExpression::detail::getResultSize(*context, value1, value2);
  constexpr static bool resultIsConstant =
      (isConstantResult<S1> && isConstantResult<S2>);
  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(resultSize);

  // TODO<joka921> Make this simpler by factoring out the whole binary search
  // stuff.
  if constexpr (ad_utility::isSimilar<S1, Variable> && isConstantResult<S2>) {
    auto impl = [&](const auto& value2) -> std::optional<ExpressionResult> {
      auto columnIndex = context->getColumnIndexForVariable(value1);
      auto valueId = makeValueId(value2, context);
      // TODO<C++23> Use `ql::ranges::starts_with`.
      if (const auto& cols = context->_columnsByWhichResultIsSorted;
          !cols.empty() && cols[0] == columnIndex) {
        constexpr static bool value2IsString =
            !ad_utility::isSimilar<decltype(valueId), Id>;
        if constexpr (value2IsString) {
          return evaluateWithBinarySearch<Comp>(value1, valueId.first,
                                                valueId.second, context);
        } else {
          return evaluateWithBinarySearch<Comp>(value1, valueId, std::nullopt,
                                                context);
        }
      }
      context->cancellationHandle_->throwIfCancelled();
      return std::nullopt;
    };
    std::optional<ExpressionResult> resultFromBinarySearch;
    if constexpr (ad_utility::isSimilar<S2, IdOrLiteralOrIri>) {
      resultFromBinarySearch =
          std::visit([&impl](const auto& x) { return impl(x); }, value2);
    } else {
      resultFromBinarySearch = impl(value2);
    }
    if (resultFromBinarySearch.has_value()) {
      return std::move(resultFromBinarySearch.value());
    }
  }

  auto [generatorA, generatorB] =
      getGenerators(AD_FWD(value1), AD_FWD(value2), resultSize, context);
  auto itA = generatorA.begin();
  auto itB = generatorB.begin();

  for (size_t i = 0; i < resultSize; ++i) {
    auto impl = [&](const auto& x, const auto& y) {
      using X = std::decay_t<decltype(x)>;
      using Y = std::decay_t<decltype(y)>;
      if constexpr (AreIncomparable<X, Y>) {
        result.push_back(Id::makeUndefined());
      } else {
        result.push_back(
            toValueId(sparqlExpression::compareIdsOrStrings<
                      Comp, valueIdComparators::ComparisonForIncompatibleTypes::
                                AlwaysUndef>(x, y, context)));
      }
    };
    ad_utility::visitWithVariantsAndParameters(impl, *itA, *itB);
    ++itA;
    ++itB;
    context->cancellationHandle_->throwIfCancelled();
  }

  if constexpr (resultIsConstant) {
    AD_CONTRACT_CHECK(result.size() == 1);
    return result[0];
  } else {
    return result;
  }
}

// The relational comparisons like `less than` are not useful for booleans and
// thus currently throw an exception.
CPP_template(Comparison, typename A,
             typename B)(requires(StoresBoolean<A> || StoresBoolean<B>)) Id
    evaluateRelationalExpression(const A&, const B&, const EvaluationContext*) {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported for "
      "boolean arguments");
}

CPP_template(Comparison Comp, typename A,
             typename B)(requires AreIncomparable<A, B>) Id
    evaluateRelationalExpression(const A&, const B&, const EvaluationContext*) {
  // TODO<joka921> We should probably return `undefined` here.
  if constexpr (Comp == Comparison::NE) {
    return Id::makeFromBool(true);
  } else {
    return Id::makeFromBool(false);
  }
}

// For comparisons where exactly one of the operands is a variable, the variable
// must come first.
CPP_template(Comparison Comp, typename A, typename B)(
    requires SingleExpressionResult<A> CPP_and SingleExpressionResult<B> CPP_and
        CPP_NOT(AreComparable<A, B>)
            CPP_and AreComparable<B, A>) ExpressionResult
    evaluateRelationalExpression(A&& a, B&& b,
                                 const EvaluationContext* context) {
  return evaluateRelationalExpression<getComparisonForSwappedArguments(Comp)>(
      AD_FWD(b), AD_FWD(a), context);
}

}  // namespace

// Implementation of the member functions of the `RelationalExpression` class
// using the above functions.
namespace sparqlExpression::relational {
// ____________________________________________________________________________
template <Comparison Comp>
ExpressionResult RelationalExpression<Comp>::evaluate(
    EvaluationContext* context) const {
  auto resA = children_[0]->evaluate(context);
  auto resB = children_[1]->evaluate(context);

  // `resA` and `resB` are variants, so we need `std::visit`.
  auto visitor = [context](auto a, auto b) -> ExpressionResult {
    return evaluateRelationalExpression<Comp>(std::move(a), std::move(b),
                                              context);
  };

  return std::visit(visitor, std::move(resA), std::move(resB));
}

// _____________________________________________________________________________
template <Comparison Comp>
string RelationalExpression<Comp>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  static_assert(std::tuple_size_v<decltype(children_)> == 2);
  return absl::StrCat(typeid(*this).name(),
                      children_[0]->getCacheKey(varColMap),
                      children_[1]->getCacheKey(varColMap));
}

// _____________________________________________________________________________
template <Comparison Comp>
ql::span<SparqlExpression::Ptr> RelationalExpression<Comp>::childrenImpl() {
  return {children_.data(), children_.size()};
}

// _____________________________________________________________________________
template <Comparison Comp>
std::optional<SparqlExpression::LangFilterData>
RelationalExpression<Comp>::getLanguageFilterExpression() const {
  if (Comp != valueIdComparators::Comparison::EQ) {
    return std::nullopt;
  }

  // We support both directions: LANG(?x) = "en" and "en" = LANG(?x).
  auto getLangFilterData =
      [](const auto& left, const auto& right) -> std::optional<LangFilterData> {
    std::optional<Variable> optVar = getVariableFromLangExpression(left.get());
    const auto* langPtr =
        dynamic_cast<const StringLiteralExpression*>(right.get());

    if (!optVar.has_value() || !langPtr) {
      return std::nullopt;
    }

    // TODO<joka921> Check that the language string doesn't contain a datatype
    // etc.
    // TODO<joka921> Is this even allowed by the grammar?
    return LangFilterData{
        std::move(optVar.value()),
        std::string{asStringViewUnsafe(langPtr->value().getContent())}};
  };

  const auto& child1 = children_[0];
  const auto& child2 = children_[1];

  if (auto langFilterData = getLangFilterData(child1, child2)) {
    return langFilterData;
  } else {
    return getLangFilterData(child2, child1);
  }
}

namespace {
// _____________________________________________________________________________
SparqlExpression::Estimates getEstimatesForFilterExpressionImpl(
    uint64_t inputSizeEstimate, uint64_t reductionFactor, const auto& children,
    const std::optional<Variable>& firstSortedVariable) {
  AD_CORRECTNESS_CHECK(children.size() >= 1);
  // For the binary expressions `=` `<=`, etc., we have exactly two children, so
  // the following line is a noop. For the `IN` expression we expect to have
  // more results if we have more arguments on the right side that can possibly
  // match, so we reduce the `reductionFactor`.
  reductionFactor /= children.size() - 1;
  auto sizeEstimate = inputSizeEstimate / reductionFactor;

  // By default, we have to linearly scan over the input and write the output.
  size_t costEstimate = inputSizeEstimate + sizeEstimate;

  // Returns true iff `left` is a variable by which the input is sorted, and
  // `right` is a constant.
  auto canBeEvaluatedWithBinarySearch =
      [&firstSortedVariable](const SparqlExpression::Ptr& left,
                             const SparqlExpression::Ptr& right) {
        auto varPtr = dynamic_cast<const VariableExpression*>(left.get());
        return varPtr && varPtr->value() == firstSortedVariable &&
               right->isConstantExpression();
      };
  // TODO<joka921> This check has to be more complex once we support proper
  // filtering on the `LocalVocab`.
  // Check iff all the pairs `(children[0], someOtherChild)` can be evaluated
  // using binary search.
  if (ql::ranges::all_of(children | ql::views::drop(1),
                         [&lhs = children.at(0),
                          &canBeEvaluatedWithBinarySearch](const auto& child) {
                           // The implementation automatically chooses the
                           // cheaper direction, so we can do the same when
                           // estimating the cost.
                           return canBeEvaluatedWithBinarySearch(lhs, child) ||
                                  canBeEvaluatedWithBinarySearch(child, lhs);
                         })) {
    // When evaluating via binary search, the only significant cost that occurs
    // is that of writing the output.
    costEstimate = sizeEstimate;
  }
  return {sizeEstimate, costEstimate};
}
}  // namespace

// _____________________________________________________________________________
template <Comparison comp>
SparqlExpression::Estimates
RelationalExpression<comp>::getEstimatesForFilterExpression(
    uint64_t inputSizeEstimate,
    const std::optional<Variable>& firstSortedVariable) const {
  uint64_t reductionFactor = 0;

  if (comp == valueIdComparators::Comparison::EQ) {
    reductionFactor = reductionFactorEquals;
  } else if (comp == valueIdComparators::Comparison::NE) {
    reductionFactor = reductionFactorNotEquals;
  } else {
    reductionFactor = reductionFactorDefault;
  }

  return getEstimatesForFilterExpressionImpl(inputSizeEstimate, reductionFactor,
                                             children_, firstSortedVariable);
}

// _____________________________________________________________________________
ExpressionResult InExpression::evaluate(
    sparqlExpression::EvaluationContext* context) const {
  auto lhs = children_.at(0)->evaluate(context);
  ExpressionResult result{ad_utility::SetOfIntervals{}};
  bool firstChild = true;
  for (const auto& child : children_ | ql::views::drop(1)) {
    auto rhs = child->evaluate(context);
    auto evaluateEqualsExpression = [context](const auto& a,
                                              auto b) -> ExpressionResult {
      return evaluateRelationalExpression<Comparison::EQ>(a, std::move(b),
                                                          context);
    };
    auto subRes = std::visit(evaluateEqualsExpression, lhs, std::move(rhs));
    if (firstChild) {
      firstChild = false;
      result = std::move(subRes);
      continue;
    }
    // TODO We could implement early stopping for entries which are already
    // true, This could be especially beneficial if some of the `==` expressions
    // are more expensive than others. Same goes for the `logical or` and
    // `logical and` expressions.
    auto expressionForSubRes =
        std::make_unique<SingleUseExpression>(std::move(subRes));
    auto expressionForPreviousResult =
        std::make_unique<SingleUseExpression>(std::move(result));
    result = makeOrExpression(std::move(expressionForSubRes),
                              std::move(expressionForPreviousResult))
                 ->evaluate(context);
  }
  return result;
}

// _____________________________________________________________________________
// (1) the provided `SparqlExpression* child` is a direct `Variable` expression
// (e.g. `?x`), return `<Variable, false>`.
// (2) the provided `SparqlExpression* child` is an expression `YEAR` which
// refers to a `Variable` value (e.g. `YEAR(?x)`), return `<Variable, true>`.
// (3) None of the previous expression cases, return default `std::nullopt`.
// The `bool` flag is relevant later on to differentiate w.r.t. the logic that
// needs to be applied for the creation of `PrefilterExpression`.
static std::optional<std::pair<Variable, bool>> getOptVariableAndIsYear(
    const SparqlExpression* child) {
  bool isYear = false;
  if (child->isYearExpression()) {
    // The direct child is an expression YEAR();
    isYear = true;
    const auto& grandChild = child->children();
    // The expression YEAR() should by definition hold a single child.
    AD_CORRECTNESS_CHECK(grandChild.size() == 1);
    child = grandChild[0].get();
  }
  if (auto optVariable = child->getVariableOrNullopt(); optVariable) {
    // (1) isYear is false: The direct child is already a Variable (expression).
    // (2) isYear is true: The direct child is expression YEAR(). The actual
    // pre-filter reference Variable is the child of expression YEAR().
    return std::make_pair(optVariable.value(), isYear);
  }
  // No Variable for pre-filtering available.
  return std::nullopt;
}

// _____________________________________________________________________________
template <Comparison comp>
std::vector<PrefilterExprVariablePair>
RelationalExpression<comp>::getPrefilterExpressionForMetadata(
    [[maybe_unused]] bool isNegated) const {
  AD_CORRECTNESS_CHECK(children_.size() == 2);
  const SparqlExpression* child0 = children_.at(0).get();
  const SparqlExpression* child1 = children_.at(1).get();

  const auto tryGetPrefilterExprVariablePairVec =
      [](const SparqlExpression* child0, const SparqlExpression* child1,
         bool reversed) -> std::vector<PrefilterExprVariablePair> {
    const auto& optVariableIsYearPair = getOptVariableAndIsYear(child0);
    if (!optVariableIsYearPair.has_value()) return {};
    const auto& [variable, prefilterDate] = optVariableIsYearPair.value();
    const auto& optReferenceValue =
        detail::getIdOrLocalVocabEntryFromLiteralExpression(child1);
    if (!optReferenceValue.has_value()) return {};
    return prefilterExpressions::detail::makePrefilterExpressionVec<comp>(
        optReferenceValue.value(), variable, reversed, prefilterDate);
  };
  // Option 1:
  // RelationalExpression containing a VariableExpression as the first child
  // and an IdExpression, IdExpression or IriExpression as the second child.
  // E.g. for ?x >= 10 (RelationalExpression Sparql), we obtain the following
  // pair with PrefilterExpression and Variable: <(>= 10), ?x>
  auto resVec = tryGetPrefilterExprVariablePairVec(child0, child1, false);
  if (!resVec.empty()) {
    return resVec;
  }
  // Option 2:
  // RelationalExpression containing an IdExpression, LiteralExpression or
  // IriExpression as the first child and a VariableExpression as the second
  // child. (1) 10 >= ?x (RelationalExpression Sparql), we obtain the following
  // pair with PrefilterExpression and Variable: <(<= 10), ?x>
  // (2) 10 != ?x (RelationalExpression Sparql), we obtain the following
  // pair with PrefilterExpression and Variable: <(!= 10), ?x>;
  // Option 3:
  // If no PrefilterExpressions could be constructed for this
  // RelationalExpression, just return the empty vector.
  return tryGetPrefilterExprVariablePairVec(child1, child0, true);
}

// _____________________________________________________________________________
ql::span<SparqlExpression::Ptr> InExpression::childrenImpl() {
  return children_;
}

// _____________________________________________________________________________
string InExpression::getCacheKey(const VariableToColumnMap& varColMap) const {
  std::stringstream result;
  result << "IN Expression with (";
  for (const auto& child : children_) {
    result << ' ' << child->getCacheKey(varColMap);
  }
  result << ')';
  return std::move(result).str();
}

// _____________________________________________________________________________
// Brief explanation why we ignore the argument `isNegated` here.
// (1) In the case of `isNegated = false`, the correct `IsInExpression` is
// constructed by default here, since its default parameter for `isNegated` is
// false as well.
// (2) `isNegated = true` implies that a parent node is a NOT expression (see
// `UnaryNegateExpressionImpl` in NumericUnaryExpressions.cpp). In this case,
// the `UnaryNegateExpressionImpl` will subsequently correctly negate the here
// returned `IsInExpression` by implicitly calling `->logicalComplement()` on
// it (see `NotExpression` in PrefilterExpressionIndex.h).
std::vector<PrefilterExprVariablePair>
InExpression::getPrefilterExpressionForMetadata(
    [[maybe_unused]] bool isNegated) const {
  AD_CORRECTNESS_CHECK(children_.size() >= 1);
  auto var = children_.front()->getVariableOrNullopt();
  if (!var.has_value()) {
    return {};
  }

  std::vector<prefilterExpressions::IdOrLocalVocabEntry> referenceValues;
  referenceValues.reserve(children_.size());
  for (const auto& expr : children_ | ql::ranges::views::drop(1)) {
    auto optReferenceValue =
        sparqlExpression::detail::getIdOrLocalVocabEntryFromLiteralExpression(
            expr.get());
    if (!optReferenceValue.has_value()) {
      return {};
    }
    referenceValues.push_back(optReferenceValue.value());
  }

  std::vector<PrefilterExprVariablePair> resPrefilter;
  resPrefilter.emplace_back(
      std::make_unique<prefilterExpressions::IsInExpression>(referenceValues),
      var.value());
  return resPrefilter;
}

// _____________________________________________________________________________
auto InExpression::getEstimatesForFilterExpression(
    uint64_t inputSizeEstimate,
    const std::optional<Variable>& firstSortedVariable) const -> Estimates {
  return getEstimatesForFilterExpressionImpl(
      inputSizeEstimate, reductionFactorEquals, children_, firstSortedVariable);
}

// Explicit instantiations
template class RelationalExpression<Comparison::LT>;
template class RelationalExpression<Comparison::LE>;
template class RelationalExpression<Comparison::EQ>;
template class RelationalExpression<Comparison::NE>;
template class RelationalExpression<Comparison::GT>;
template class RelationalExpression<Comparison::GE>;
}  // namespace sparqlExpression::relational

namespace sparqlExpression {

// _____________________________________________________________________________
std::optional<std::pair<sparqlExpression::GeoFunctionCall, double>>
getGeoDistanceFilter(const SparqlExpression& expr) {
  // TODO<ullingerc> Add support for more optimizable filters:
  // * `geof:distance() < constant`
  // * `constant > geof:distance()`
  // * `constant >= geof:distance()`
  // The supported patterns are documented in the header declaration of this
  // function.

  // Supported comparison operator
  auto compareExpr = dynamic_cast<const LessEqualExpression*>(&expr);
  if (compareExpr == nullptr) {
    return std::nullopt;
  }

  // Left child must be distance function
  auto children = compareExpr->children();
  const auto& leftChild = *children[0];

  // Right child must be constant
  auto rightChild = children[1].get();
  auto literalExpr =
      dynamic_cast<detail::LiteralExpression<ValueId>*>(rightChild);
  if (literalExpr == nullptr) {
    return std::nullopt;
  }
  ValueId constant = literalExpr->value();
  // Extract distance. Here we don't know the unit of this number yet. It is
  // extracted from the function call in the next step.
  double maxDistAnyUnit = 0;
  if (constant.getDatatype() == Datatype::Double) {
    maxDistAnyUnit = constant.getDouble();
  } else if (constant.getDatatype() == Datatype::Int) {
    maxDistAnyUnit = static_cast<double>(constant.getInt());
  } else {
    return std::nullopt;
  }

  // Extract variables and distance unit from function call
  auto geoFuncCall = getGeoDistanceExpressionParameters(leftChild);
  if (!geoFuncCall.has_value()) {
    return std::nullopt;
  }

  // Convert unit to meters
  double maxDist = ad_utility::detail::valueInUnitToKilometer(
                       maxDistAnyUnit, geoFuncCall.value().unit_) *
                   1000;

  return std::pair<GeoFunctionCall, double>{geoFuncCall.value(), maxDist};
}

}  // namespace sparqlExpression
