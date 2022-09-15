//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "RelationalExpressions.h"

#include "../../util/LambdaHelpers.h"
#include "./SparqlExpressionGenerators.h"

using namespace sparqlExpression;

namespace {

using valueIdComparators::Comparison;

template <valueIdComparators::Comparison Comp, typename Dummy = int>
bool applyComparison(const auto& a, const auto& b) requires requires {
  a <= b;
}
{
  if constexpr (Comp == valueIdComparators::Comparison::LT) {
    return a < b;
  } else if constexpr (Comp == valueIdComparators::Comparison::LE) {
    return a <= b;
  } else if constexpr (Comp == valueIdComparators::Comparison::EQ) {
    return a == b;
  } else if constexpr (Comp == valueIdComparators::Comparison::NE) {
    return a != b;
  } else if constexpr (Comp == valueIdComparators::Comparison::GE) {
    return a >= b;
  } else if constexpr (Comp == valueIdComparators::Comparison::GT) {
    return a > b;
  } else {
    static_assert(ad_utility::alwaysFalse<Dummy>);
  }
}

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

template <typename T>
concept Arithmetic = std::integral<T> || std::floating_point<T>;

template <typename A, typename B>
concept BothVariables =
    std::is_same_v<Variable, A> && std::is_same_v<Variable, B>;

template <typename A, typename B>
concept VariableAndVectorImpl = std::is_same_v<Variable, A> &&
    (Arithmetic<B> || std::is_same_v<ValueId, B>);

template <typename A, typename B>
concept VariableAndVector =
    VariableAndVectorImpl<A, B> || VariableAndVectorImpl<B, A>;

template <typename A, typename B>
concept NeitherString =
    !std::is_same_v<A, std::string> && !std::is_same_v<B, std::string>;
template <typename A, typename B>
concept anyStrongId = (std::is_same_v<ValueId, A> ||
                       std::is_same_v<ValueId, B>)&&NeitherString<A, B>;

template <typename T>
concept Boolean =
    std::is_same_v<T, Bool> || std::is_same_v<T, ad_utility::SetOfIntervals> ||
    std::is_same_v<VectorWithMemoryLimit<Bool>, T>;

template <typename T>
concept VarOrIdVec = ad_utility::SimilarTo<T, Variable> ||
                     ad_utility::SimilarTo<T, VectorWithMemoryLimit<ValueId>> || ad_utility::SimilarTo<T, ValueId>;


template <typename A, typename B>
concept Incompatible = (Arithmetic<A> && std::is_same_v<B, std::string>) ||
                       (Arithmetic<B> && std::is_same_v<std::string, A>);

template <typename A, typename B>
concept AnyBoolean = Boolean<A> || Boolean<B>;

template<typename A, typename B>
concept Compatible = !AnyBoolean<A, B> && !Incompatible<A, B> && (VarOrIdVec<A> || !VarOrIdVec<B>);

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

static_assert(std::is_same_v<double, ValueType<VectorWithMemoryLimit<double>>>);

static_assert(
    Compatible<ValueType<VectorWithMemoryLimit<double>>, ValueType<double>>);


std::pair<ValueId, ValueId> getRangeFromVocab(const std::string& s,
                                              EvaluationContext* context) {
  // TODO<joka921> In theory we could make the `level` comparable.
  auto level = TripleComponentComparator::Level::QUARTERNARY;
  // TODO<joka921> This should be `Vocab::equal_range`
  const ValueId lower = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().lower_bound(s, level));
  const ValueId upper = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().upper_bound(s, level));
  return {lower, upper};
}

template <SingleExpressionResult S> requires isConstantResult<S>
auto makeValueId(const S& value, EvaluationContext* context) {
  if constexpr (std::is_integral_v<S>) {
    return ValueId::makeFromInt(value);
  } else if constexpr (std::is_floating_point_v<S>) {
    return ValueId::makeFromDouble(value);
  } else if constexpr (ad_utility::isSimilar<S, ValueId>) {
    return value;
  } else if constexpr (ad_utility::isSimilar<S, Variable>) {
    // should never be reachable.
    // TODO<joka921> make sure how to fix this.
    return ValueId::makeUndefined();
  } else {
    static_assert(ad_utility::isSimilar<S, std::string>);
    return getRangeFromVocab(value, context);
  }
}

static_assert(!isConstantResult<Variable>);
template <SingleExpressionResult S> requires isConstantResult<S>
auto idGenerator(S value, size_t targetSize, EvaluationContext* context) -> cppcoro::generator<decltype(makeValueId(value, context))> {
  auto id = makeValueId(value, context);
  for (size_t i = 0; i < targetSize; ++i) {
    auto cpy = id;
    co_yield cpy;
  }
}

template<SingleExpressionResult S> requires isVectorResult<S>
auto idGenerator(S value, size_t targetSize, EvaluationContext* context) -> cppcoro::generator<decltype(makeValueId(value[0], context))> {
  AD_CHECK(targetSize == value.size());
  for (const auto& el : value) {
    auto id = makeValueId(el, context);
    co_yield id;
  }
}

auto idGenerator(Variable variable, size_t targetSize, EvaluationContext* context) {
  return sparqlExpression::detail::makeGenerator(std::move(variable), targetSize, context);
}





template <SingleExpressionResult A, SingleExpressionResult B>
auto getGenerators(A a, B b, size_t targetSize, EvaluationContext* context) {
  if constexpr (ad_utility::isTypeContainedIn<A, std::tuple<Variable, ValueId, VectorWithMemoryLimit<ValueId>>> ||ad_utility::isTypeContainedIn<B, std::tuple<Variable, ValueId, VectorWithMemoryLimit<ValueId>>>) {
    return std::pair{idGenerator(std::move(a), targetSize, context), idGenerator(std::move(b), targetSize, context)};
  } else {
    return std::pair{sparqlExpression::detail::makeGenerator(std::move(a), targetSize, context), sparqlExpression::detail::makeGenerator(std::move(b), targetSize, context)};
  }
}

template <Comparison Comp>
ExpressionResult evaluateWithBinarySearch(const Variable& a, ValueId idB,
                                          std::optional<ValueId> idBUpper,
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

template <Comparison Comp, typename A, typename B>
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
      if constexpr(bIsString) {
        return evaluateWithBinarySearch<Comp>(a, bId.first, bId.second, context);
      } else {
        return evaluateWithBinarySearch<Comp>(a, bId, std::nullopt, context);
      }
    }
  }

  auto [generatorA, generatorB] = getGenerators(std::move(a), std::move(b), targetSize, context);
  auto itA = generatorA.begin();
  auto itB = generatorB.begin();

  for (size_t i = 0; i < targetSize; ++i) {
    if constexpr (requires {
                    valueIdComparators::compareIds(*itA, *itB, Comp);
                  }) {
      result.push_back(valueIdComparators::compareIds(*itA, *itB, Comp));
    }  else if constexpr (requires {
                    valueIdComparators::compareWithEqualIds(*itA, itB->first, itB->second, Comp);
                  }) {
      result.push_back(valueIdComparators::compareWithEqualIds(*itA, itB->first, itB->second, Comp));
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

template <Comparison, typename A, typename B>
Bool evaluateR(const A&, const B&,
               EvaluationContext*) requires Boolean<A> || Boolean<B> {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported for "
      "boolean arguments");
}

template <Comparison, typename A, typename B>
requires Incompatible<ValueType<A>, ValueType<B>> Bool
evaluateR(const A&, const B&, EvaluationContext*) {
  return false;
}

template <Comparison Comp, SingleExpressionResult A, SingleExpressionResult B> requires (!Compatible<ValueType<A>, ValueType<B>> && Compatible<ValueType<B>, ValueType<A>>)
ExpressionResult evaluateR(A a, B b, EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(std::move(b), std::move(a), context);
}

}  // namespace

namespace sparqlExpression::relational {
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

template <Comparison Comp>
string RelationalExpression<Comp>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  string key = typeid(*this).name();
  for (const auto& child : _children) {
    key += child->getCacheKey(varColMap);
  }
  return key;
}

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
