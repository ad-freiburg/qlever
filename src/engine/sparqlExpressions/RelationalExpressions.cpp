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

template <typename T>
concept ArithmeticOrString =
    Arithmetic<T> || ad_utility::SimilarTo<T, std::string>;

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

template <typename A, typename B>
concept Compatible = (Arithmetic<A> && Arithmetic<B>) ||
                     (std::is_same_v<std::string, A> &&
                      std::is_same_v<std::string, B>) ||
                     BothVariables<A, B> || VariableAndVector<A, B> ||
                     (anyStrongId<A, B> && !Boolean<A> && !Boolean<B>);

template <typename A, typename B>
concept Incompatible = (Arithmetic<A> && std::is_same_v<B, std::string>) ||
                       (Arithmetic<B> && std::is_same_v<std::string, A>);

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

template <Comparison Comp, typename A, typename B>
requires Compatible<ValueType<A>, ValueType<B>> ExpressionResult
evaluateR(A a, B b, EvaluationContext* context) {
  auto targetSize = sparqlExpression::detail::getResultSize(*context, a, b);
  constexpr static bool resultIsConstant =
      (isConstantResult<A> && isConstantResult<B>);
  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(targetSize);

  auto generatorA = sparqlExpression::detail::makeGenerator(
      std::move(a), targetSize, context);
  auto generatorB = sparqlExpression::detail::makeGenerator(
      std::move(b), targetSize, context);

  auto itA = generatorA.begin();
  auto itB = generatorB.begin();

  for (size_t i = 0; i < targetSize; ++i) {
    if constexpr (requires {
                    valueIdComparators::compareIds(*itA, *itB, Comp);
                  }) {
      result.push_back(valueIdComparators::compareIds(*itA, *itB, Comp));
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

template <typename T>
concept VarOrIdVec = ad_utility::SimilarTo<T, Variable> ||
    ad_utility::SimilarTo<T, VectorWithMemoryLimit<ValueId>>;

// TODO: This can also be integrated into the "compatible" case when dealing
// with the strings properly there.
template <Comparison Comp, VarOrIdVec A, ArithmeticOrString B>
requires(!Arithmetic<std::decay_t<B>>) ExpressionResult
    evaluateR(A a, const B& b, EvaluationContext* context) {
  const auto [lower, upper] =
      [&b, context]() -> std::pair<ValueId, std::optional<ValueId>> {
    if constexpr (std::is_integral_v<B>) {
      return {ValueId::makeFromInt(b), std::nullopt};
    } else if constexpr (std::is_floating_point_v<B>) {
      return {ValueId::makeFromDouble(b), std::nullopt};
    } else {
      static_assert(ad_utility::isSimilar<B, std::string>);
      auto [l, u] = getRangeFromVocab(b, context);
      return {l, u};
    }
  }();

  if constexpr (ad_utility::isSimilar<A, Variable>) {
    auto idxA = getIndexForVariable(a, context);
    const auto& cols = context->_columnsByWhichResultIsSorted;
    if (!cols.empty() && cols[0] == idxA) {
      return evaluateWithBinarySearch<Comp>(a, lower, upper, context);
    }
  }

  VectorWithMemoryLimit<Bool> result{context->_allocator};
  auto targetSize = context->_endIndex - context->_beginIndex;
  result.reserve(targetSize);

  if (ad_utility::isSimilar<B, std::string>) {
    AD_CHECK(upper.has_value());
  }
  for (auto idA : sparqlExpression::detail::makeGenerator(
           std::move(a), targetSize, context)) {
    if constexpr (ad_utility::isSimilar<B, std::string>) {
      result.push_back(
          valueIdComparators::compareWithEqualIds(idA, lower, *upper, Comp));
    } else {
      result.push_back(valueIdComparators::compareIds(idA, lower, Comp));
    }
  }
  return result;
}

template <Comparison Comp>
ExpressionResult evaluateR(
    const ArithmeticOrString auto& a, VarOrIdVec auto b,
    EvaluationContext*
        context) requires(!Arithmetic<std::decay_t<decltype(a)>>) {
  return evaluateR<getComplement(Comp)>(std::move(b), a, context);
}

template <Comparison Comp>
Bool evaluateR(const Id& id, const std::string& s, EvaluationContext* context) {
  auto [lower, upper] = getRangeFromVocab(s, context);
  return valueIdComparators::compareWithEqualIds(id, lower, upper, Comp);
}

template <Comparison Comp>
Bool evaluateR(const std::string& a, const Id& b, EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(b, a, context);
}

template <Comparison Comp>
ExpressionResult evaluateR(VarOrIdVec auto a,
                           VectorWithMemoryLimit<std::string> b,
                           EvaluationContext* context) {
  auto targetSize = sparqlExpression::detail::getResultSize(*context, a, b);
  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(targetSize);

  auto generatorA = sparqlExpression::detail::makeGenerator(
      std::move(a), targetSize, context);
  auto generatorB = sparqlExpression::detail::makeGenerator(
      std::move(b), targetSize, context);

  auto itA = generatorA.begin();
  auto itB = generatorB.begin();
  // TODO<joka921> If the variable doesn't store string values and we know this,
  //  we can immediately return "always false" here.
  for (size_t i = 0; i < targetSize; ++i) {
    auto [lower, upper] = getRangeFromVocab(*itB, context);
    result.push_back(
        valueIdComparators::compareWithEqualIds(*itA, lower, upper, Comp));
  }
  return result;
}

template <Comparison Comp>
ExpressionResult evaluateR(VectorWithMemoryLimit<std::string> a,
                           VarOrIdVec auto b, EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(std::move(b), std::move(a), context);
}

template <Comparison Comp>
ExpressionResult evaluateR(const ValueId& a,
                           const VectorWithMemoryLimit<std::string>& b,
                           EvaluationContext* context) {
  if (a.getDatatype() != Datatype::VocabIndex) {
    return ad_utility::SetOfIntervals{};
  }
  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(b.size());
  for (const auto& str : b) {
    auto [lower, upper] = getRangeFromVocab(str, context);
    result.push_back(
        valueIdComparators::compareWithEqualIds(a, lower, upper, Comp));
  }
  return result;
}

template <Comparison Comp>
ExpressionResult evaluateR(const VectorWithMemoryLimit<std::string>& a,
                           const ValueId& b, EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(b, a, context);
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
