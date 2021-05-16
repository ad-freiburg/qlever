//
// Created by johannes on 15.05.21.
//
#include "./SparqlExpression.h"

namespace sparqlExpression {

template <bool RangeCalculationAllowed, typename RangeCalculation,
    typename ValueExtractor, typename BinaryOperation>
auto liftBinaryCalculationToEvaluateResults(RangeCalculation rangeCalculation,
                                            ValueExtractor valueExtractor,
                                            BinaryOperation binaryOperation,
                                            evaluationInput* input) {
  return [input, rangeCalculation, valueExtractor,
      binaryOperation](auto&&... args) -> SparqlExpression::EvaluateResult {
    if constexpr (RangeCalculationAllowed &&
                  (... &&
                      std::is_same_v<std::decay_t<decltype(args)>, Ranges>)) {
      return rangeCalculation(std::forward<decltype(args)>(args)...);
    } else {
      auto getSize = [&input](const auto& x) -> std::pair<size_t, bool> {
        using T = std::decay_t<decltype(x)>;
        if constexpr (isVector<T>) {
          return {x.size(), true};
        } else if constexpr (std::is_same_v<Ranges, T>) {
          return {input->_end - input->_begin, true};
        } else {
          return {1, false};
        }
      };

      auto targetSize = std::max({getSize(args).first...});

      // If any of the inputs is a list/vector/range of results, the result will be a std::vector
      bool resultIsAVector = (... || getSize(args).second);

      auto extractors =
          std::make_tuple([expanded = expandRanges(
              std::forward<decltype(args)>(args), targetSize),
                              &valueExtractor, input](size_t index) {
            using A = std::decay_t<decltype(expanded)>;
            if constexpr (isVector<A>) {
              return valueExtractor(expanded[index], input);
            } else if constexpr (std::is_same_v<Variable, A>) {
              return valueExtractor(getNumericValueFromVariable(expanded,index, input), input);
            } else {
              return valueExtractor(expanded, input);
            }
          }...);

      auto create = [&](auto&&... extractors) {
        using ResultType =
        std::decay_t<decltype(binaryOperation(extractors(0)...))>;
        std::vector<ResultType> result;
        result.reserve(targetSize);
        for (size_t i = 0; i < targetSize; ++i) {
          result.push_back(binaryOperation(extractors(i)...));
        }
        return result;
      };

      auto result = std::apply(create, std::move(extractors));

      if (!resultIsAVector) {
        return SparqlExpression::EvaluateResult {std::move(result[0])};
      } else {
        return SparqlExpression::EvaluateResult(std::move(result));
      }
    }
  };
}
template <bool RangeCalculationAllowed, typename RangeCalculation, typename ValueExtractor, typename BinaryOperation>
SparqlExpression::EvaluateResult
              BinaryExpression<RangeCalculationAllowed, RangeCalculation, ValueExtractor, BinaryOperation>::evaluate(evaluationInput* input) const {
auto firstResult = _children[0]->evaluate(input);

auto calculator =
    liftBinaryCalculationToEvaluateResults<RangeCalculationAllowed>(
        RangeCalculation{}, ValueExtractor{}, BinaryOperation{}, input);
for (size_t i = 1; i < _children.size(); ++i) {
firstResult =
std::visit(calculator, firstResult, _children[i]->evaluate(input));
}
return firstResult;
}

template <bool RangeCalculationAllowed, typename RangeCalculation,
    typename ValueExtractor, typename UnaryOperation>
  SparqlExpression::EvaluateResult UnaryExpression<RangeCalculationAllowed, RangeCalculation, ValueExtractor, UnaryOperation>::evaluate(evaluationInput* input) const {
    auto firstResult = _child->evaluate(input);

    auto calculator =
        liftBinaryCalculationToEvaluateResults<RangeCalculationAllowed>(
            RangeCalculation{}, ValueExtractor{}, UnaryOperation{}, input);
    firstResult = std::visit(calculator, firstResult);
    return firstResult;
  };

  template <size_t MaxValue>
struct LambdaVisitor {
  template <typename Tuple, typename Enum, typename ValueExtractor,
            typename... Args>
  auto operator()(Tuple t, Enum r, ValueExtractor v, evaluationInput* input,
                  Args&&... args) -> SparqlExpression::EvaluateResult {
    static_assert(MaxValue < std::tuple_size_v<Tuple>);
    AD_CHECK(static_cast<size_t>(r) < std::tuple_size_v<Tuple>);
    if (static_cast<size_t>(r) == MaxValue) {
      return liftBinaryCalculationToEvaluateResults<false>(
          0, std::move(v), std::get<MaxValue>(t),
          input)(std::forward<Args>(args)...);
    } else {
      return LambdaVisitor<MaxValue - 1>{}(t, r, v, input,
                                           std::forward<Args>(args)...);
    }
  }
};

template <>
struct LambdaVisitor<0> {
  template <typename Tuple, typename Enum, typename ValueExtractor,
      typename... Args>
  auto operator()(Tuple t, Enum r, ValueExtractor v, evaluationInput* input,
                  Args&&... args) {
    if (static_cast<size_t>(r) == 0) {
      return liftBinaryCalculationToEvaluateResults<false>(
          0, std::move(v), std::get<0>(t), input)(std::forward<Args>(args)...);
    } else {
      AD_CHECK(false);
    }
  }
};

template <typename TagAndFunction, typename... TagAndFunctions>
struct LambdaVisitor2 {
  template <typename ValueExtractor,
      typename... Args>
  auto operator()(conststr identifier, ValueExtractor v, evaluationInput* input,
                  Args&&... args) -> SparqlExpression::EvaluateResult {
    if (identifier == TagAndFunction::tag) {
      return liftBinaryCalculationToEvaluateResults<false>(
          0, std::move(v), typename TagAndFunction::functionType{},
          input)(std::forward<Args>(args)...);
    } else {
      if constexpr (sizeof...(TagAndFunctions) > 0) {
        return LambdaVisitor2<TagAndFunctions...>{}(identifier, v, input,
                                                   std::forward<Args>(args)...);
      }
      // Todo proper exception
      AD_CHECK(false);
    }
  }
};


template <typename ValueExtractor, typename... TagAndFunctions>
SparqlExpression::EvaluateResult
DispatchedBinaryExpression2<ValueExtractor, TagAndFunctions...>::
evaluate(evaluationInput* input) const {
  auto firstResult = _children[0]->evaluate(input);

  for (size_t i = 1; i < _children.size(); ++i) {
// auto calculator = liftBinaryCalculationToEvaluateResults<false>(0, ValueExtractor{}, BinaryOperation{} , input);
    auto calculator = [&]<typename... Args>(Args&&... args) {
      return LambdaVisitor2<TagAndFunctions...>{}(
           _relations[i - 1], ValueExtractor{},
          input, std::forward<Args>(args)...);
    };
    firstResult =
        std::visit(calculator, firstResult, _children[i]->evaluate(input));
  }
  return firstResult;
};

template class UnaryExpression<false, EmptyType, BooleanValueGetter,
    decltype(unaryNegate)>;
template class UnaryExpression<false, EmptyType, NumericValueGetter,
    decltype(unaryMinus)>;
template class BinaryExpression<true, RangeMerger, BooleanValueGetter,
        decltype(orLambda)>;

template class BinaryExpression<true, RangeIntersector, BooleanValueGetter,
    decltype(andLambda)>;

template class DispatchedBinaryExpression2<NumericValueGetter, TaggedFunction<"+", decltype(add)>, TaggedFunction<"-", decltype(subtract)>>;

template class DispatchedBinaryExpression2<NumericValueGetter, TaggedFunction<"*", decltype(multiply)>, TaggedFunction<"/", decltype(divide)>>;
}

