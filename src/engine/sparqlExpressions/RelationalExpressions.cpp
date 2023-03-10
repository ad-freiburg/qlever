//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "RelationalExpressions.h"

#include "engine/sparqlExpressions/LangExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/LambdaHelpers.h"
#include "util/TypeTraits.h"

using namespace sparqlExpression;

namespace {

using valueIdComparators::Comparison;

// Several concepts used to choose the proper evaluation methods for different
// input types.

// For `T == VectorWithMemoryLimit<U>`, `ValueType<T>` is `U`. For any other
// type `T`, `ValueType<T>` is `T`.
namespace detail {
// TODO<joka921> This helper function may never be called and could in principle
// be formulated directly within the `decltype` statement below. However, this
// doesn't compile with G++11. Find out, why.
template <typename T>
constexpr auto getObjectOfValueTypeHelper(T&& t) {
  if constexpr (ad_utility::similarToInstantiation<T, VectorWithMemoryLimit>) {
    return std::move(t[0]);
  } else {
    return AD_FWD(t);
  }
}
}  // namespace detail
template <typename T>
using ValueType =
    decltype(detail::getObjectOfValueTypeHelper<T>(std::declval<T>()));

// Concept that requires that `T` logically stores numeric values.
template <typename T>
concept StoresNumeric =
    std::integral<ValueType<T>> || std::floating_point<ValueType<T>>;

// Concept that requires that `T` logically stores `std::string`s.
template <typename T>
concept StoresStrings = ad_utility::SimilarTo<ValueType<T>, std::string>;

// Concept that requires that `T` logically stores boolean values.
template <typename T>
concept StoresBoolean =
    std::is_same_v<T, Bool> || std::is_same_v<T, ad_utility::SetOfIntervals> ||
    std::is_same_v<VectorWithMemoryLimit<Bool>, T>;

// Concept that requires that `T` logically stores `ValueId`s.
template <typename T>
concept StoresValueId = ad_utility::SimilarTo<T, Variable> ||
    ad_utility::SimilarTo<T, VectorWithMemoryLimit<ValueId>> ||
    ad_utility::SimilarTo<T, ValueId>;

// When `A` and `B` are `AreIncomparable`, comparisons between them will always
// yield `not equal`, independent of the concrete values.
template <typename A, typename B>
concept AreIncomparable = (StoresNumeric<A> && StoresStrings<B>) ||
                          (StoresNumeric<B> && StoresStrings<A>);

// True iff any of `A, B` is `StoresBoolean` (see above).
template <typename A, typename B>
concept AtLeastOneIsBoolean = StoresBoolean<A> || StoresBoolean<B>;

// The types for which comparisons like `<` are supported and not always false.
template <typename A, typename B>
concept AreComparable = !AtLeastOneIsBoolean<A, B> && !AreIncomparable<A, B> &&
                        (StoresValueId<A> || !StoresValueId<B>);

// Apply the given `Comparison` to `a` and `b`. For example, if the `Comparison`
// is `LT`, returns `a < b`. Note that the second template argument `Dummy` is
// only needed to make the static check for the exhaustiveness of the if-else
// cascade possible.
template <Comparison Comp, typename Dummy = int>
bool applyComparison(const auto& a, const auto& b) {
  using enum Comparison;
  if constexpr (Comp == LT) {
    return a < b;
  } else if constexpr (Comp == LE) {
    return a <= b;
  } else if constexpr (Comp == EQ) {
    return a == b;
  } else if constexpr (Comp == NE) {
    return a != b;
  } else if constexpr (Comp == GE) {
    return a >= b;
  } else if constexpr (Comp == GT) {
    return a > b;
  } else {
    static_assert(ad_utility::alwaysFalse<Dummy>);
  }
}

// Get the comparison that yields the same result when the arguments are
// swapped. For example the swapped comparison of `less than` is `greater than`
// because `a < b` if and only if `b > a`.
constexpr Comparison getComparisonForSwappedArguments(Comparison comp) {
  switch (comp) {
    case Comparison::LE:
      return Comparison::GE;
    case Comparison::LT:
      return Comparison::GT;
    case Comparison::EQ:
    case Comparison::NE:
      return comp;
    case Comparison::GE:
      return Comparison::LE;
    case Comparison::GT:
      return Comparison::LT;
  }
}

// Return the ID range `[begin, end)` in which the entries of the vocabulary
// compare equal to `s`. This is a range because words that are different on
// the byte level can still logically be equal, depending on the chosen Unicode
// collation level.
// TODO<joka921> Make the collation level configurable.
std::pair<ValueId, ValueId> getRangeFromVocab(const std::string& s,
                                              EvaluationContext* context) {
  auto level = TripleComponentComparator::Level::QUARTERNARY;
  // TODO<joka921> This should be `Vocab::equal_range`
  const ValueId lower = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().lower_bound(s, level));
  const ValueId upper = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().upper_bound(s, level));
  return {lower, upper};
}

// Convert an int, double, or string value into a `ValueId`. For int and double
// this is a single `ValueId`, for strings it is a `pair<ValueId, ValueId>` that
// denotes a range (see `getRangeFromVocab` above).
template <SingleExpressionResult S>
requires isConstantResult<S>
auto makeValueId(const S& value, EvaluationContext* context) {
  if constexpr (std::is_integral_v<S>) {
    return ValueId::makeFromInt(value);
  } else if constexpr (std::is_floating_point_v<S>) {
    return ValueId::makeFromDouble(value);
  } else if constexpr (ad_utility::isSimilar<S, ValueId>) {
    return value;
  } else {
    static_assert(ad_utility::isSimilar<S, std::string>);
    return getRangeFromVocab(value, context);
  }
}

// For the various `SingleExpressionResult`s the `idGenerator` function returns
// a generator that yields `targetSize` many `ValueId`s. One exception is the
// case where `S` is `string` or `vector<string>`. In this case the generator
// yields `pair<ValueId, ValueId>` (see `makeValueId` and `getRangeFromVocab`
// above for details).

// First the `idGenerator` for constants (string, int, double). It yields the
// same ID `targetSize` many times.
template <SingleExpressionResult S>
requires isConstantResult<S>
auto idGenerator(S value, size_t targetSize, EvaluationContext* context)
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
auto idGenerator(S values, size_t targetSize, EvaluationContext* context)
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
                 EvaluationContext* context) {
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
                   EvaluationContext* context) {
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

// Efficiently (using binary search) computes the result of `aVal Comp valueId`
// for each `ValueId` that the Variable `variable` is bound to. This only works,
// if the input (as stored in the `context`) is sorted by the Variable
// `variable`. If `valueIdUpper` is nullopt, we only have a single `valueId`,
// otherwise we have a range, [`valueId`, `valueIdUpper`).
template <Comparison Comp>
ad_utility::SetOfIntervals evaluateWithBinarySearch(
    const Variable& variable, ValueId valueId,
    std::optional<ValueId> valueIdUpper, EvaluationContext* context) {
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
requires AreComparable<S1, S2> ExpressionResult
evaluateRelationalExpression(S1 value1, S2 value2, EvaluationContext* context) {
  auto resultSize =
      sparqlExpression::detail::getResultSize(*context, value1, value2);
  constexpr static bool resultIsConstant =
      (isConstantResult<S1> && isConstantResult<S2>);
  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(resultSize);

  constexpr static bool value2IsString = ad_utility::isSimilar<S2, std::string>;
  if constexpr (ad_utility::isSimilar<S1, Variable> && isConstantResult<S2>) {
    auto columnIndex = context->getColumnIndexForVariable(value1);
    auto valueId = makeValueId(value2, context);
    const auto& cols = context->_columnsByWhichResultIsSorted;
    if (!cols.empty() && cols[0] == columnIndex) {
      if constexpr (value2IsString) {
        return evaluateWithBinarySearch<Comp>(value1, valueId.first,
                                              valueId.second, context);
      } else {
        return evaluateWithBinarySearch<Comp>(value1, valueId, std::nullopt,
                                              context);
      }
    }
  }

  auto [generatorA, generatorB] =
      getGenerators(std::move(value1), std::move(value2), resultSize, context);
  auto itA = generatorA.begin();
  auto itB = generatorB.begin();

  for (size_t i = 0; i < resultSize; ++i) {
    if constexpr (requires {
                    valueIdComparators::compareIds(*itA, *itB, Comp);
                  }) {
      // Compare two `ValueId`s
      result.push_back(valueIdComparators::compareIds(*itA, *itB, Comp));
    } else if constexpr (requires {
                           valueIdComparators::compareWithEqualIds(
                               *itA, itB->first, itB->second, Comp);
                         }) {
      // Compare `ValueId` with range of equal `ValueId`s (used when `value2` is
      // `string` or `vector<string>`.
      result.push_back(valueIdComparators::compareWithEqualIds(
          *itA, itB->first, itB->second, Comp));
    } else {
      // Compare two numeric values, or two string values.
      result.push_back(applyComparison<Comp>(*itA, *itB));
    }
    ++itA;
    ++itB;
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
Bool evaluateRelationalExpression(const A&, const B&,
                                  EvaluationContext*) requires
    StoresBoolean<A> || StoresBoolean<B> {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported for "
      "boolean arguments");
}

template <Comparison Comp, typename A, typename B>
requires AreIncomparable<A, B> Bool
evaluateRelationalExpression(const A&, const B&, EvaluationContext*) {
  if constexpr (Comp == Comparison::NE) {
    return true;
  } else {
    return false;
  }
}

// For comparisons where exactly one of the operands is a variable, the variable
// must come first.
template <Comparison Comp, SingleExpressionResult A, SingleExpressionResult B>
requires(!AreComparable<A, B> && AreComparable<B, A>) ExpressionResult
    evaluateRelationalExpression(A a, B b, EvaluationContext* context) {
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
std::span<SparqlExpression::Ptr> RelationalExpression<Comp>::children() {
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
        dynamic_cast<const StringOrIriExpression*>(right.get());

    if (!varPtr || !langPtr) {
      return std::nullopt;
    }

    return LangFilterData{varPtr->variable(), langPtr->value()};
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
