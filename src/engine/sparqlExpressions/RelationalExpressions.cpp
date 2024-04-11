//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "RelationalExpressions.h"

#include "engine/sparqlExpressions/LangExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/LambdaHelpers.h"
#include "util/TypeTraits.h"

using namespace sparqlExpression;

namespace {

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
template <SingleExpressionResult S>
requires isConstantResult<S>
auto idGenerator(S value, size_t targetSize, const EvaluationContext* context)
    -> cppcoro::generator<const decltype(makeValueId(value, context))> {
  auto id = makeValueId(value, context);
  for (size_t i = 0; i < targetSize; ++i) {
    co_yield id;
  }
}

// Version of `idGenerator` for vectors. Asserts that the size of the vector is
// equal to `targetSize` and the yields the corresponding ID for each of the
// elements in the vector.
template <SingleExpressionResult S>
requires isVectorResult<S>
auto idGenerator(S values, size_t targetSize, const EvaluationContext* context)
    -> cppcoro::generator<decltype(makeValueId(values[0], context))> {
  AD_CONTRACT_CHECK(targetSize == values.size());
  for (const auto& el : values) {
    auto id = makeValueId(el, context);
    co_yield id;
  }
}

// For the `Variable` class, the generator from the `sparqlExpressions` module
// already yields the `ValueIds`.
auto idGenerator(Variable variable, size_t targetSize,
                 const EvaluationContext* context) {
  return sparqlExpression::detail::makeGenerator(std::move(variable),
                                                 targetSize, context);
}

// Return a pair of generators that generate the values from `value1` and
// `value2`. The type of generators is chosen to meet the needs of comparing
// `value1` and `value2`. If any of them logically stores `ValueId`s (true for
// `ValueId, vector<ValueId>, Variable`), then `idGenerator`s are returned for
// both inputs. Else the "plain" generators from `sparqlExpression::detail` are
// returned. These simply yield the values unchanged.
template <SingleExpressionResult S1, SingleExpressionResult S2>
auto getGenerators(S1 value1, S2 value2, size_t targetSize,
                   const EvaluationContext* context) {
  if constexpr (StoresValueId<S1> || StoresValueId<S2>) {
    return std::pair{idGenerator(std::move(value1), targetSize, context),
                     idGenerator(std::move(value2), targetSize, context)};
  } else {
    return std::pair{sparqlExpression::detail::makeGenerator(
                         std::move(value1), targetSize, context),
                     sparqlExpression::detail::makeGenerator(
                         std::move(value2), targetSize, context)};
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

  auto getIdFromColumn = ad_utility::makeAssignableLambda(
      [columnIndex](const auto& idTable, auto i) {
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
template <Comparison Comp, SingleExpressionResult S1, SingleExpressionResult S2>
requires AreComparable<S1, S2> ExpressionResult evaluateRelationalExpression(
    S1 value1, S2 value2, const EvaluationContext* context) {
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
      // TODO<C++23> Use `std::ranges::starts_with`.
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
      getGenerators(std::move(value1), std::move(value2), resultSize, context);
  auto itA = generatorA.begin();
  auto itB = generatorB.begin();

  for (size_t i = 0; i < resultSize; ++i) {
    auto impl = [&]<typename X, typename Y>(const X& x, const Y& y) {
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
template <Comparison, typename A, typename B>
Id evaluateRelationalExpression(const A&, const B&, const EvaluationContext*)
    requires StoresBoolean<A> || StoresBoolean<B> {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported for "
      "boolean arguments");
}

template <Comparison Comp, typename A, typename B>
requires AreIncomparable<A, B>
Id evaluateRelationalExpression(const A&, const B&, const EvaluationContext*) {
  // TODO<joka921> We should probably return `undefined` here.
  if constexpr (Comp == Comparison::NE) {
    return Id::makeFromBool(true);
  } else {
    return Id::makeFromBool(false);
  }
}

// For comparisons where exactly one of the operands is a variable, the variable
// must come first.
template <Comparison Comp, SingleExpressionResult A, SingleExpressionResult B>
requires(!AreComparable<A, B> && AreComparable<B, A>)
ExpressionResult evaluateRelationalExpression(
    A a, B b, const EvaluationContext* context) {
  return evaluateRelationalExpression<getComparisonForSwappedArguments(Comp)>(
      std::move(b), std::move(a), context);
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
std::span<SparqlExpression::Ptr> RelationalExpression<Comp>::childrenImpl() {
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
    const auto* varPtr = dynamic_cast<const LangExpression*>(left.get());
    const auto* langPtr =
        dynamic_cast<const StringLiteralExpression*>(right.get());

    if (!varPtr || !langPtr) {
      return std::nullopt;
    }

    // TODO<joka921> Check that the language string doesn't contain a datatype
    // etc.
    // TODO<joka921> Is this even allowed by the grammar?
    return LangFilterData{
        varPtr->variable(),
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

template <Comparison comp>
SparqlExpression::Estimates
RelationalExpression<comp>::getEstimatesForFilterExpression(
    uint64_t inputSizeEstimate,
    [[maybe_unused]] const std::optional<Variable>& firstSortedVariable) const {
  size_t sizeEstimate = 0;

  if (comp == valueIdComparators::Comparison::EQ) {
    sizeEstimate = inputSizeEstimate / 1000;
  } else if (comp == valueIdComparators::Comparison::NE) {
    sizeEstimate = inputSizeEstimate;
  } else {
    sizeEstimate = inputSizeEstimate / 50;
  }

  size_t costEstimate = sizeEstimate;

  auto canBeEvaluatedWithBinarySearch = [&firstSortedVariable](
                                            const Ptr& left, const Ptr& right) {
    auto varPtr = dynamic_cast<const VariableExpression*>(left.get());
    if (!varPtr || varPtr->value() != firstSortedVariable) {
      return false;
    }
    return right->isConstantExpression();
  };

  // TODO<joka921> This check has to be more complex once we support proper
  // filtering on the `LocalVocab`.
  if (canBeEvaluatedWithBinarySearch(children_[0], children_[1]) ||
      canBeEvaluatedWithBinarySearch(children_[1], children_[0])) {
    costEstimate = 0;
  }
  return {sizeEstimate, costEstimate};
}

// Explicit instantiations
template class RelationalExpression<Comparison::LT>;
template class RelationalExpression<Comparison::LE>;
template class RelationalExpression<Comparison::EQ>;
template class RelationalExpression<Comparison::NE>;
template class RelationalExpression<Comparison::GT>;
template class RelationalExpression<Comparison::GE>;
}  // namespace sparqlExpression::relational
