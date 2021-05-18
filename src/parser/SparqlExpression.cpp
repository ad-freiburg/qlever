//
// Created by johannes on 15.05.21.
//
#include "./SparqlExpression.h"
#include <cmath>
#include "../engine/CallFixedSize.h"

namespace sparqlExpression::detail {
using namespace setOfIntervals;

// _____________________________________________________________________________
double NumericValueGetter::operator()(StrongId strongId,
                                      ResultTable::ResultType type,
                                      EvaluationInput* input) const {
  const Id id = strongId._value;
  // This code is borrowed from the original QLever code.
  if (type == ResultTable::ResultType::VERBATIM) {
    return static_cast<double>(id);
  } else if (type == ResultTable::ResultType::FLOAT) {
    // used to store the id value of the entry interpreted as a float
    float tempF;
    std::memcpy(&tempF, &id, sizeof(float));
    return static_cast<double>(tempF);
  } else if (type == ResultTable::ResultType::TEXT ||
             type == ResultTable::ResultType::LOCAL_VOCAB) {
    return std::numeric_limits<float>::quiet_NaN();
  } else {
    // load the string, parse it as an xsd::int or float
    std::string entity =
        input->_qec.getIndex().idToOptionalString(id).value_or("");
    if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
      return std::numeric_limits<float>::quiet_NaN();
    } else {
      return ad_utility::convertIndexWordToFloat(entity);
    }
  }
}

// ____________________________________________________________________________
bool BooleanValueGetter::operator()(StrongId strongId,
                                    ResultTable::ResultType type,
                                    EvaluationInput* input) const {
  double floatResult = NumericValueGetter{}(strongId, type, input);
  // convert "nan" to false for now. TODO<joka921> proper error handling in
  // expressions.
  return static_cast<bool>(floatResult) && !std::isnan(floatResult);
}

// Implementation of the getValuesFromVariable method (see below). Optimized for
// the different IdTable specializations.
template <size_t WIDTH, typename ValueExtractor, typename ResultType>
void getValuesFromVariableImpl(std::vector<ResultType>& result,
                               ValueExtractor valueExtractor,
                               const SparqlExpression::Variable& variable,
                               EvaluationInput* input) {
  AD_CHECK(result.empty());
  auto staticInput = input->_inputTable.asStaticView<WIDTH>();

  const size_t beginIndex = input->_beginIndex;
  size_t resultSize = input->_endIndex - input->_beginIndex;

  if (!input->_variableColumnMap.contains(variable._variable)) {
    throw std::runtime_error(
        "Variable " + variable._variable +
        " could not be mapped to input column of expression evaluation");
  }

  size_t columnIndex = input->_variableColumnMap[variable._variable].first;
  auto type = input->_variableColumnMap[variable._variable].second;

  result.reserve(resultSize);
  for (size_t i = 0; i < resultSize; ++i) {
    const auto id = StrongId{staticInput(beginIndex + i, columnIndex)};
    result.push_back(valueExtractor(id, type, input));
  }
}

// Convert a variable to a std::vector of all the values it takes in the input
// range specified by `input`. The `valueExtractor` is used convert QLever IDs
// to the appropriate value type.
template <typename ValueExtractor>
auto getValuesFromVariable(const SparqlExpression::Variable& variable,
                           ValueExtractor valueExtractor,
                           EvaluationInput* input) {
  auto cols = input->_inputTable.cols();
  using ResultType =
      std::invoke_result_t<ValueExtractor, StrongId, ResultTable::ResultType,
                           EvaluationInput*>;
  std::vector<ResultType> result;
  CALL_FIXED_SIZE_1(cols, getValuesFromVariableImpl, result, valueExtractor,
                    variable, input);
  return result;
}

// Convert a NaryOperation that works on single elements (e.g. two doubles ->
// double) into an operation works on all the possible input types (single
// elements, vectors of elements, variables, sets). Works for n-ary operations
// with arbitrary n and is currently used for unary and binary operations.
template <typename RangeCalculation, typename ValueExtractor,
          typename NaryOperation>
auto liftBinaryCalculationToEvaluateResults(RangeCalculation rangeCalculation,
                                            ValueExtractor valueExtractor,
                                            NaryOperation naryOperation,
                                            EvaluationInput* input) {
  // We return the "lifted" operation which works on all the different variants
  // of `EvaluateResult` and returns an `EvaluateResult` again.
  return [input, rangeCalculation, valueExtractor,
          naryOperation](auto&&... args) -> SparqlExpression::EvaluateResult {
    // Perform the more efficient range calculation if it is possible.
    constexpr bool RangeCalculationAllowed =
        !std::is_same_v<RangeCalculation, NoRangeCalculation>;
    if constexpr (RangeCalculationAllowed &&
                  (... && std::is_same_v<std::decay_t<decltype(args)>, Set>)) {
      return rangeCalculation(std::forward<decltype(args)>(args)...);
    } else {
      // We have to first determine, whether the result will be a constant or a
      // vector of elements, as well as the number of results we will produce.

      // Returns a pair of <resultSize, willTheResultBeAVector>
      auto getSize = [&input]<typename T>(const T& x)->std::pair<size_t, bool> {
        if constexpr (ad_utility::isVector<T>) {
          return {x.size(), true};
        } else if constexpr (std::is_same_v<Set, T> ||
                             std::is_same_v<Variable, T>) {
          return {input->_endIndex - input->_beginIndex, true};
        } else {
          return {1, false};
        }
      };

      // If we combine constants and vectors of inputs, then the result will be
      // a vector again.
      auto targetSize = std::max({getSize(args).first...});
      bool resultIsAVector = (... || getSize(args).second);

      //  We have to convert the arguments which are variables or sets into
      //  std::vector,
      // the following lambda is able to perform this task
      auto possiblyExpand = [&input, valueExtractor ]<typename T>(
          T childResult, [[maybe_unused]] size_t targetSize) {
        if constexpr (std::is_same_v<Set, std::decay_t<T>>) {
          return expandSet(std::move(childResult), targetSize);
        } else if constexpr (std::is_same_v<Variable, std::decay_t<T>>) {
          return getValuesFromVariable(childResult, valueExtractor, input);
        } else {
          return childResult;
        }
      };

      // Create a tuple of lambdas, one lambda for each input expression with
      // the following semantics: Each lambda takes a single argument (the
      // index) and returns the index-th element (For constants, the index is of
      // course ignored).
      auto extractors =
          std::make_tuple([expanded = possiblyExpand(
                               std::forward<decltype(args)>(args), targetSize),
                           &valueExtractor, input](size_t index) {
            using A = std::decay_t<decltype(expanded)>;
            if constexpr (ad_utility::isVector<A>) {
              return valueExtractor(expanded[index], input);
            } else {
              static_assert(!std::is_same_v<Variable, A>);
              return valueExtractor(expanded, input);
            }
          }...);

      /// This lambda takes all the previously created extractors as input and
      /// creates the actual result of the computation.
      auto create = [&](auto&&... extractors) {
        using ResultType =
            std::decay_t<decltype(naryOperation(extractors(0)...))>;
        std::vector<ResultType> result;
        result.reserve(targetSize);
        for (size_t i = 0; i < targetSize; ++i) {
          result.push_back(naryOperation(extractors(i)...));
        }
        return result;
      };

      // calculate the actual result (the extractors are stored in a tuple, so
      // we have to use std::apply)
      auto result = std::apply(create, std::move(extractors));

      // The create-Lambda always produces a vector for simplicity. If the
      // result of this calculation is actually a constant, only return a
      // constant.
      if (!resultIsAVector) {
        AD_CHECK(result.size() == 1);
        return SparqlExpression::EvaluateResult{std::move(result[0])};
      } else {
        return SparqlExpression::EvaluateResult(std::move(result));
      }
    }
  };
}

// ____________________________________________________________________________
template <typename RangeCalculation, typename ValueExtractor,
          typename BinaryOperation>
SparqlExpression::EvaluateResult
BinaryExpression<RangeCalculation, ValueExtractor, BinaryOperation>::evaluate(
    EvaluationInput* input) const {
  AD_CHECK(!_children.empty())
  auto result = _children[0]->evaluate(input);

  auto calculator = liftBinaryCalculationToEvaluateResults(
      RangeCalculation{}, ValueExtractor{}, BinaryOperation{}, input);
  for (size_t i = 1; i < _children.size(); ++i) {
    result = std::visit(calculator, std::move(result),
                        _children[i]->evaluate(input));
  }
  return result;
}

// ___________________________________________________________________________
template <typename RangeCalculation, typename ValueExtractor,
          typename UnaryOperation>
SparqlExpression::EvaluateResult
UnaryExpression<RangeCalculation, ValueExtractor, UnaryOperation>::evaluate(
    EvaluationInput* input) const {
  auto result = _child->evaluate(input);

  auto calculator = liftBinaryCalculationToEvaluateResults(
      RangeCalculation{}, ValueExtractor{}, UnaryOperation{}, input);
  result = std::visit(calculator, result);
  return result;
};

/// Find out, which of the TaggedFunctions matches the `identifier`, and perform
/// it on the given input
template <typename TagAndFunction, typename... TagAndFunctions>
struct TaggedFunctionVisitor {
  template <typename ValueExtractor, typename... Args>
  auto operator()(TagString identifier, ValueExtractor v,
                  EvaluationInput* input, Args&&... args)
      -> SparqlExpression::EvaluateResult {
    // Does the first tag match?
    if (identifier == TagAndFunction::tag) {
      return liftBinaryCalculationToEvaluateResults(
          NoRangeCalculation{}, std::move(v),
          typename TagAndFunction::functionType{},
          input)(std::forward<Args>(args)...);
    } else {
      // The first tag did not match, recurse!
      if constexpr (sizeof...(TagAndFunctions) > 0) {
        return TaggedFunctionVisitor<TagAndFunctions...>{}(
            identifier, v, input, std::forward<Args>(args)...);
      }
      AD_CHECK(false);
    }
  }
};

// ___________________________________________________________________________
template <typename ValueExtractor, typename... TagAndFunctions>
SparqlExpression::EvaluateResult
DispatchedBinaryExpression<ValueExtractor, TagAndFunctions...>::evaluate(
    EvaluationInput* input) const {
  auto result = _children[0]->evaluate(input);

  for (size_t i = 1; i < _children.size(); ++i) {
    auto calculator = [&]<typename... Args>(Args && ... args) {
      return TaggedFunctionVisitor<TagAndFunctions...>{}(
          _relations[i - 1], ValueExtractor{}, input,
          std::forward<Args>(args)...);
    };
    result = std::visit(calculator, std::move(result),
                        _children[i]->evaluate(input));
  }
  return result;
};

// Explicit instantiations.
template class UnaryExpression<NoRangeCalculation, BooleanValueGetter,
                               decltype(unaryNegate)>;
template class UnaryExpression<NoRangeCalculation, NumericValueGetter,
                               decltype(unaryMinus)>;
template class BinaryExpression<Union, BooleanValueGetter, decltype(orLambda)>;

template class BinaryExpression<Intersection, BooleanValueGetter,
                                decltype(andLambda)>;

template class DispatchedBinaryExpression<
    NumericValueGetter, TaggedFunction<"+", decltype(add)>,
    TaggedFunction<"-", decltype(subtract)>>;

template class DispatchedBinaryExpression<
    NumericValueGetter, TaggedFunction<"*", decltype(multiply)>,
    TaggedFunction<"/", decltype(divide)>>;
}  // namespace sparqlExpression::detail
