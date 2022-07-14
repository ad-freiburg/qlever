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
    std::is_same_v<Variable, A>&& std::is_same_v<Variable, B>;

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
                     anyStrongId<A, B> && !Boolean<A> && !Boolean<B>;

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
                   EvaluationContext*) requires Boolean<A> ||
    Boolean<B> {
  throw std::runtime_error(
      "Relational expressions like <, >, == are currently not supported for "
      "boolean arguments");
}

template <Comparison, typename A, typename B>
requires Incompatible<ValueType<A>, ValueType<B>> Bool
evaluateR(const A&, const B&, EvaluationContext*) {
  return false;
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

template <Comparison Comp, Arithmetic B>
ExpressionResult evaluateR(const Variable& a, const B& b,
                           EvaluationContext* context) {
  auto idxA = getIndexForVariable(a, context);

  const ValueId idB = [&b]() {
    if constexpr (std::is_integral_v<B>) {
      return ValueId::makeFromInt(b);
    } else {
      static_assert(std::is_floating_point_v<B>);
      return ValueId::makeFromDouble(b);
    }
  }();
  const auto& cols = context->_columnsByWhichResultIsSorted;
  if (!cols.empty() && cols[0] == idxA) {
    return evaluateWithBinarySearch<Comp>(a, idB, std::nullopt, context);
  }

  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(context->_endIndex - context->_beginIndex);

  // TODO<joka921> Use the CALL_FIXED_SIZE optimization here
  for (auto i = context->_beginIndex; i < context->_endIndex; ++i) {
    auto idA = context->_inputTable(i, idxA);
    result.push_back(valueIdComparators::compareIds(idA, idB, Comp));
  }
  return result;
}

template <Comparison Comp>
ExpressionResult evaluateR(const Arithmetic auto& a, const Variable& b,
                           EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(b, a, context);
}

template <Comparison Comp>
ExpressionResult evaluateR(const Variable& a, const std::string& b,
                           EvaluationContext* context) {
  auto idxA = getIndexForVariable(a, context);

  auto level = TripleComponentComparator::Level::QUARTERNARY;
  const ValueId lower = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().lower_bound(b, level));
  const ValueId upper = Id::makeFromVocabIndex(
      context->_qec.getIndex().getVocab().upper_bound(b, level));

  const auto& cols = context->_columnsByWhichResultIsSorted;
  if (!cols.empty() && cols[0] == idxA) {
    return evaluateWithBinarySearch<Comp>(a, lower, upper, context);
  }

  VectorWithMemoryLimit<Bool> result{context->_allocator};
  result.reserve(context->_endIndex - context->_beginIndex);

  // TODO<joka921> Use the CALL_FIXED_SIZE optimization here
  for (auto i = context->_beginIndex; i < context->_endIndex; ++i) {
    auto idA = context->_inputTable(i, idxA);

    // TODO<joka921> This can definitely be optimized by only looking at the
    // bit values of the IDs!
    // Also it is not quite correct, because it ignores the unicode RANGE
    // TODO<joka921> Should the unicode behavior be somewhat configurable?
    result.push_back(valueIdComparators::compareIds(idA, lower, Comp));
  }
  return result;
}

template <Comparison Comp>
ExpressionResult evaluateR(const std::string& a, const Variable& b,
                           EvaluationContext* context) {
  return evaluateR<getComplement(Comp)>(b, a, context);
}

template <Comparison>
Bool evaluateR(const StrongIdWithResultType&, const std::string&,
               EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}

template <Comparison>
Bool evaluateR(const std::string&, const StrongIdWithResultType&,
               EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}

template <Comparison>
Bool evaluateR(const Variable&, const VectorWithMemoryLimit<std::string>&,
               EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}

template <Comparison>
Bool evaluateR(const VectorWithMemoryLimit<std::string>&, const Variable&,
               EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}

template <Comparison>
Bool evaluateR(const ValueId&, const VectorWithMemoryLimit<std::string>&,
               EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}

template <Comparison>
Bool evaluateR(const VectorWithMemoryLimit<std::string>&, const ValueId&,
               EvaluationContext*) {
  throw std::runtime_error("Not implemented, TODO");
}

}  // namespace

namespace sparqlExpression::relational {
template <Comparison Comp>
ExpressionResult RelationalExpression<Comp>::evaluate(
    EvaluationContext* context) const {
  auto resA = _children[0]->evaluate(context);
  auto resB = _children[1]->evaluate(context);

  auto visitor = [this, context](auto a, auto b) -> ExpressionResult {
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
