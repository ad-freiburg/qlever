//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "RelationalExpressions.h"

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "util/LambdaHelpers.h"
#include "util/TypeTraits.h"

using namespace sparqlExpression;

namespace {

using valueIdComparators::Comparison;

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

// Get the inverse comparison for a given comparison. For example the inverse of
// `less than` is `greater than` because `a < b` if and only if `b > a`.
constexpr Comparison getComplement(Comparison comp) {
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

// Return the ID range /[begin, end)` in which the entries of the vocabulary
// compare equal to `s`. This is a range, because words that are different on
// the byte level still can logically be equal depending on the chose unicode
// collation level.
// TODO<joka921> Make the collation level configurable.
std::pair<ValueId, ValueId> getRangeFromVocab(const std::string& s,
                                              EvaluationContext* context) {
  // TODO<joka921> In theory we could make the `level` configurable.
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
// above for details). First the `idGenerator` for constants (string, int,
// double). It yields the same ID `targetSize` many times.
template <SingleExpressionResult S>
requires isConstantResult<S>
auto idGenerator(S value, size_t targetSize, EvaluationContext* context)
    -> cppcoro::generator<decltype(makeValueId(value, context))> {
  auto id = makeValueId(value, context);
  for (size_t i = 0; i < targetSize; ++i) {
    auto cpy = id;
    co_yield cpy;
  }
}

// Version of `idGenerator` for vectors. Asserts that the size of the vector is
// equal to `targetSize` and the yields the corresponding ID for each of the
// elements in the vector.
template <SingleExpressionResult S>
requires isVectorResult<S>
auto idGenerator(S value, size_t targetSize, EvaluationContext* context)
    -> cppcoro::generator<decltype(makeValueId(value[0], context))> {
  AD_CHECK(targetSize == value.size());
  for (const auto& el : value) {
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

// Return a pair of generators that generate the values from `a` and `b`. The
// type of generators is chosen to met the needs of comparing `a` and `b`. If
// any of them logically stores `ValueId`s (true for `ValueId, vector<ValueId>,
// Variable`, then `idGenerator`s are returned for both inputs. Else the "plain`
// generators from `sparqlExpression::detail` are returned, that simply yield
// the values unchanged.
template <SingleExpressionResult A, SingleExpressionResult B>
auto getGenerators(A a, B b, size_t targetSize, EvaluationContext* context) {
  if constexpr (
      ad_utility::isTypeContainedIn<
          A, std::tuple<Variable, ValueId, VectorWithMemoryLimit<ValueId>>> ||
      ad_utility::isTypeContainedIn<
          B, std::tuple<Variable, ValueId, VectorWithMemoryLimit<ValueId>>>) {
    return std::pair{idGenerator(std::move(a), targetSize, context),
                     idGenerator(std::move(b), targetSize, context)};
  } else {
    return std::pair{sparqlExpression::detail::makeGenerator(
                         std::move(a), targetSize, context),
                     sparqlExpression::detail::makeGenerator(
                         std::move(b), targetSize, context)};
  }
}

// Efficiently (using binary search) computes the result of `aVal Comp idB` for
// each `ValueId` that the Variable `a` is bound to. This only works, if the
// input (as stored in the `context`) is sorted by the Variable `a`.
template <Comparison Comp>
ad_utility::SetOfIntervals evaluateWithBinarySearch(
    const Variable& a, ValueId idB, std::optional<ValueId> idBUpper,
    EvaluationContext* context) {
  auto idxA = getIndexForVariable(a, context);

  auto accessColumnLambda = ad_utility::makeAssignableLambda(
      [idxA](const auto& idTable, auto i) { return idTable(i, idxA); });

  using Iterator = ad_utility::IteratorForAccessOperator<
      std::decay_t<decltype(context->_inputTable)>,
      decltype(accessColumnLambda)>;
  auto begin =
      Iterator{&context->_inputTable, context->_beginIndex, accessColumnLambda};
  auto end =
      Iterator{&context->_inputTable, context->_endIndex, accessColumnLambda};

  const auto resultRanges = [&]() {
    if (idBUpper) {
      return valueIdComparators::getRangesForEqualIds(begin, end, idB,
                                                      idBUpper.value(), Comp);
    } else {
      return valueIdComparators::getRangesForId(begin, end, idB, Comp);
    }
  }();
  ad_utility::SetOfIntervals s;
  for (const auto& [rangeBegin, rangeEnd] : resultRanges) {
    s._intervals.emplace_back(rangeBegin - begin, rangeEnd - begin);
  }
  return s;
}

// Several concepts used to chose the proper evaluation methods for different
// input types.

template <typename T>
concept Arithmetic = std::integral<T> || std::floating_point<T>;

// Any of the `SingleExpressionResult`s that logically stores boolean values.
template <typename T>
concept Boolean =
    std::is_same_v<T, Bool> || std::is_same_v<T, ad_utility::SetOfIntervals> ||
    std::is_same_v<VectorWithMemoryLimit<Bool>, T>;

// Any of the `SingleExpressionResult`s that logically stores `ValueId`s
// TODO<joka921> rename this concept and use it above in the `Generator`
// function.
template <typename T>
concept VarOrIdVec = ad_utility::SimilarTo<T, Variable> ||
    ad_utility::SimilarTo<T, VectorWithMemoryLimit<ValueId>> ||
    ad_utility::SimilarTo<T, ValueId>;

// When `A` and `B` are `Incompatible`, comparisons between them will always
// yield `false`, independent of the concrete values.
template <typename A, typename B>
concept Incompatible = (Arithmetic<A> && std::is_same_v<B, std::string>) ||
                       (Arithmetic<B> && std::is_same_v<std::string, A>);

// True iff any of `A, B` is `Boolean` (see above).
template <typename A, typename B>
concept AnyBoolean = Boolean<A> || Boolean<B>;

// The types for which comparisons like `<` are supported and not always false.
template <typename A, typename B>
concept Compatible = !AnyBoolean<A, B> && !Incompatible<A, B> &&
                     (VarOrIdVec<A> || !VarOrIdVec<B>);

// Helper templates to get the `logical value type` for several types. For a
// vector, it yields the value_type of the vector. For any other type it yields
// the type itself.
// TODO<joka921> Can be simplified using a function that returns a value and
// then using decltype.
// TODO<joka921> Should be further above and then used to make `vector<double>`
// `Arithmetic`.
template <typename T>
struct ValueTypeImpl {
  using type = T;
};

template <typename T>
struct ValueTypeImpl<VectorWithMemoryLimit<T>> {
  using type = T;
};

template <typename T>
using ValueType = typename ValueTypeImpl<T>::type;

// The actual comparison function for the `SingleExpressionResult`'s which are
// `Compatible` (see above), which means that the comparison between them is
// supported and not always false.
template <Comparison Comp, SingleExpressionResult A, SingleExpressionResult B>
requires Compatible<ValueType<A>, ValueType<B>> ExpressionResult
evaluateR(A a, B b, EvaluationContext* context) {
  auto targetSize = sparqlExpression::detail::getResultSize(*context, a, b);
  constexpr static bool resultIsConstant =
      (isConstantResult<A> && isConstantResult<B>);
  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(targetSize);

  constexpr static bool bIsString = ad_utility::isSimilar<B, std::string>;
  if constexpr (ad_utility::isSimilar<A, Variable> && isConstantResult<B>) {
    auto idxA = getIndexForVariable(a, context);
    auto bId = makeValueId(b, context);
    const auto& cols = context->_columnsByWhichResultIsSorted;
    if (!cols.empty() && cols[0] == idxA) {
      if constexpr (bIsString) {
        return evaluateWithBinarySearch<Comp>(a, bId.first, bId.second,
                                              context);
      } else {
        return evaluateWithBinarySearch<Comp>(a, bId, std::nullopt, context);
      }
    }
  }

  auto [generatorA, generatorB] =
      getGenerators(std::move(a), std::move(b), targetSize, context);
  auto itA = generatorA.begin();
  auto itB = generatorB.begin();

  for (size_t i = 0; i < targetSize; ++i) {
    if constexpr (requires {
                    valueIdComparators::compareIds(*itA, *itB, Comp);
                  }) {
      result.push_back(valueIdComparators::compareIds(*itA, *itB, Comp));
    } else if constexpr (requires {
                           valueIdComparators::compareWithEqualIds(
                               *itA, itB->first, itB->second, Comp);
                         }) {
      result.push_back(valueIdComparators::compareWithEqualIds(
          *itA, itB->first, itB->second, Comp));
    } else {
      result.push_back(applyComparison<Comp>(*itA, *itB));
    }
    ++itA;
    ++itB;
  }

  if constexpr (resultIsConstant) {
    return result[0];
  } else {
    return result;
  }
}

// The relational comparisons like `less than` are not useful for booleans and
// thus currently throw an exception.
// TODO<joka921> Discuss with Hannah what should be the proper semantics,
// implicit conversion to ints 0 and 1?
// TODO<joka921> Give the `evaluateR` function a proper name (e.g.
// evaluateRelationalExpression).
template <Comparison, typename A, typename B>
Bool evaluateR(const A&, const B&,
               EvaluationContext*) requires Boolean<A> || Boolean<B> {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported for "
      "boolean arguments");
}

template <Comparison Comp, typename A, typename B>
requires Incompatible<ValueType<A>, ValueType<B>> Bool
evaluateR(const A&, const B&, EvaluationContext*) {
  if constexpr (Comp == Comparison::NE) {
    return true;
  } else {
    return false;
  }
}

template <Comparison Comp, SingleExpressionResult A, SingleExpressionResult B>
requires(!Compatible<ValueType<A>, ValueType<B>> &&
         Compatible<ValueType<B>, ValueType<A>>) ExpressionResult
    evaluateR(A a, B b, EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(std::move(b), std::move(a), context);
}

}  // namespace

// Implementation of the member functions of the `RelationalExpression` class
// using the above functions.
namespace sparqlExpression::relational {
// ____________________________________________________________________________
template <Comparison Comp>
ExpressionResult RelationalExpression<Comp>::evaluate(
    EvaluationContext* context) const {
  auto resA = _children[0]->evaluate(context);
  auto resB = _children[1]->evaluate(context);

  auto visitor = [context](auto a, auto b) -> ExpressionResult {
    return evaluateR<Comp>(std::move(a), std::move(b), context);
  };

  return std::visit(visitor, std::move(resA), std::move(resB));
}

// _____________________________________________________________________________
template <Comparison Comp>
string RelationalExpression<Comp>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  string key = typeid(*this).name();
  for (const auto& child : _children) {
    key += child->getCacheKey(varColMap);
  }
  return key;
}

// _____________________________________________________________________________
template <Comparison Comp>
std::span<SparqlExpression::Ptr> RelationalExpression<Comp>::children() {
  return {_children.data(), _children.size()};
}

// Explicit instantiations
template class RelationalExpression<Comparison::LT>;
template class RelationalExpression<Comparison::LE>;
template class RelationalExpression<Comparison::EQ>;
template class RelationalExpression<Comparison::NE>;
template class RelationalExpression<Comparison::GT>;
template class RelationalExpression<Comparison::GE>;
}  // namespace sparqlExpression::relational
