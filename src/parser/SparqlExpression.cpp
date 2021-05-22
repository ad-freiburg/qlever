//
// Created by johannes on 15.05.21.
//
#include "./SparqlExpression.h"
#include <cmath>
#include "../engine/CallFixedSize.h"
#include "../util/TupleHelpers.h"

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

string StringValueGetter::operator()(StrongId strongId,
                                     ResultTable::ResultType type,
                                     EvaluationInput* input) const {
  const Id id = strongId._value;
  // This code is borrowed from the original QLever code.
  if (type == ResultTable::ResultType::VERBATIM) {
    return std::to_string(id);
  } else if (type == ResultTable::ResultType::FLOAT) {
    // used to store the id value of the entry interpreted as a float
    float tempF;
    std::memcpy(&tempF, &id, sizeof(float));
    return std::to_string(tempF);
  } else if (type == ResultTable::ResultType::TEXT ||
             type == ResultTable::ResultType::LOCAL_VOCAB) {
    // TODO<joka921> support local vocab. The use-case it not so important, but
    // it is easy.
    throw std::runtime_error{
        "Performing further expressions on a text variable of a LocalVocab "
        "entry (typically GROUP_CONCAT result) is currently not supported"};
  } else {
    // load the string, parse it as an xsd::int or float
    std::string entity =
        input->_qec.getIndex().idToOptionalString(id).value_or("");
    if (ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
      return std::to_string(ad_utility::convertIndexWordToFloat(entity));
    } else if (ad_utility::startsWith(entity, VALUE_DATE_PREFIX)) {
      return ad_utility::convertDateToIndexWord(entity);
    } else {
      return entity;
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
// TODO<joka921> Comment is out of date
template <size_t WIDTH>
void getIdsFromVariableImpl(std::vector<StrongId>& result,
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

  result.reserve(resultSize);
  for (size_t i = 0; i < resultSize; ++i) {
    result.push_back(StrongId{staticInput(beginIndex + i, columnIndex)});
  }
}

// Convert a variable to a std::vector of all the values it takes in the input
// range specified by `input`. The `valueExtractor` is used convert QLever IDs
// to the appropriate value type.
auto getIdsFromVariable(const SparqlExpression::Variable& variable,
                        EvaluationInput* input) {
  auto cols = input->_inputTable.cols();
  std::vector<StrongId> result;
  CALL_FIXED_SIZE_1(cols, getIdsFromVariableImpl, result, variable, input);
  return result;
}

/// TODO<joka921> Comment
template <typename T>
auto possiblyExpand(T&& childResult, [[maybe_unused]] size_t targetSize,
                    EvaluationInput* input) {
  if constexpr (std::is_same_v<Set, std::decay_t<T>>) {
    return std::pair{expandSet(std::move(childResult), targetSize),
                     ResultTable::ResultType{}};
  } else if constexpr (std::is_same_v<Variable, std::decay_t<T>>) {
    return std::pair{getIdsFromVariable(std::forward<T>(childResult), input),
                     input->_variableColumnMap[childResult._variable].second};
  } else {
    return std::pair{std::forward<T>(childResult), ResultTable::ResultType{}};
  }
};

/// TODO<joka921> Comment
template <typename T>
auto makeExtractor(T&& expandedResult) {
  return [expanded = std::move(expandedResult)](size_t index) {
    if constexpr (ad_utility::isVector<T>) {
      return expanded[index];
    } else {
      static_assert(!std::is_same_v<Variable, T>);
      return expanded;
    }
  };
}

/// TODO<comment>
template <typename T, typename ValueExtractor>
auto extractValue(
    T&& singleValue, ValueExtractor valueExtractor,
    [[maybe_unused]] ResultTable::ResultType resultTypeOfInputVariable,
    EvaluationInput* input) {
  if constexpr (std::is_same_v<StrongId, std::decay_t<T>>) {
    return valueExtractor(singleValue, resultTypeOfInputVariable, input);
  } else if constexpr (std::is_same_v<StrongIdAndDatatype, std::decay_t<T>>) {
    return valueExtractor(singleValue._id, singleValue._type, input);
  } else {
    return valueExtractor(singleValue, input);
  }
};

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

      auto expandedAndVariableType = std::make_tuple(
          possiblyExpand(std::move(args), targetSize, input)...);

      auto extractorsAndVariableType = std::apply(
          [&](auto&&... els) {
            return std::make_tuple(
                std::pair{makeExtractor(std::move(els.first)), els.second}...);
          },
          std::move(expandedAndVariableType));

      /// This lambda takes all the previously created extractors as input and
      /// creates the actual result of the computation.
      auto create = [&](auto&&... extractors) {
        using ResultType = std::decay_t<decltype(
            naryOperation(extractValue(extractors.first(0), valueExtractor,
                                       extractors.second, input)...))>;
        LimitedVector<ResultType> result{input->_allocator};
        result.reserve(targetSize);
        for (size_t i = 0; i < targetSize; ++i) {
          result.push_back(
              naryOperation(extractValue(extractors.first(i), valueExtractor,
                                         extractors.second, input)...));
        }
        return result;
      };

      // calculate the actual result (the extractors are stored in a tuple, so
      // we have to use std::apply)
      auto result = std::apply(create, std::move(extractorsAndVariableType));

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

/// TODO<joka921>Comment
template <bool distinct, typename RangeCalculation, typename ValueExtractor,
          typename AggregateOperation, typename FinalOperation>
auto liftAggregateCalculationToEvaluateResults(
    RangeCalculation rangeCalculation, ValueExtractor valueExtractor,
    AggregateOperation aggregateOperation, FinalOperation finalOperation,
    EvaluationInput* input) {
  // We return the "lifted" operation which works on all the different variants
  // of `EvaluateResult` and returns an `EvaluateResult` again.
  return [input, rangeCalculation, valueExtractor, aggregateOperation,
          finalOperation](auto&& args) -> SparqlExpression::EvaluateResult {
    // Perform the more efficient range calculation if it is possible.
    constexpr bool RangeCalculationAllowed =
        !std::is_same_v<RangeCalculation, NoRangeCalculation>;
    if constexpr (RangeCalculationAllowed &&
                  (std::is_same_v<std::decay_t<decltype(args)>, Set>)) {
      return rangeCalculation(std::forward<decltype(args)>(args));
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
      auto targetSize = std::max({getSize(args).first});

      //  We have to convert the arguments which are variables or sets into
      //  std::vector,
      // the following lambda is able to perform this task
      // TODO Comment the distinct business

      // Create a tuple of lambdas, one lambda for each input expression with
      // the following semantics: Each lambda takes a single argument (the
      // index) and returns the index-th element (For constants, the index is of
      // course ignored).

      // This variable is only needed for the case of a distinct variable
      auto expandedAndResultType =
          possiblyExpand(std::move(args), targetSize, input);

      auto& expanded = expandedAndResultType.first;
      auto& resultTypeOfInputVariable = expandedAndResultType.second;

      auto extractor = makeExtractor(std::move(expanded));

      auto extractValueLocal = [&](auto&& singleValue) {
        return extractValue(singleValue, valueExtractor,
                            resultTypeOfInputVariable, input);
      };
      /// This lambda takes all the previously created extractors as input and
      /// creates the actual result of the computation.
      auto create = [&](auto&& extractor) {
        using ResultType = std::decay_t<decltype(aggregateOperation(
            extractValueLocal(extractor(0)), extractValueLocal(extractor(0))))>;
        ResultType result{};
        if constexpr (!distinct) {
          for (size_t i = 0; i < targetSize; ++i) {
            result = aggregateOperation(std::move(result),
                                        extractValueLocal(extractor(i)));
          }
          result = finalOperation(std::move(result), targetSize);
          return result;
        } else {
          // TODO<joka921> hash set or sort + unique, what is more efficient?
          ad_utility::HashSet<std::decay_t<decltype(extractor(0))>>
              uniqueHashSet;
          for (size_t i = 0; i < targetSize; ++i) {
            uniqueHashSet.insert(extractor(i));
          }

          for (const auto& distinctEl : uniqueHashSet) {
            result = aggregateOperation(std::move(result),
                                        extractValueLocal(distinctEl));
          }
          result = finalOperation(std::move(result), uniqueHashSet.size());
          return result;
        }
      };

      // calculate the actual result (the extractors are stored in a tuple, so
      // we have to use std::apply)
      auto result = create(std::move(extractor));
      return SparqlExpression::EvaluateResult{std::move(result)};
    }
  };
}

// ____________________________________________________________________________
template <typename RangeCalculation, typename ValueExtractor,
          typename BinaryOperation, TagString Tag>
SparqlExpression::EvaluateResult
BinaryExpression<RangeCalculation, ValueExtractor, BinaryOperation,
                 Tag>::evaluate(EvaluationInput* input) const {
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
          typename UnaryOperation, TagString Tag>
SparqlExpression::EvaluateResult
UnaryExpression<RangeCalculation, ValueExtractor, UnaryOperation,
                Tag>::evaluate(EvaluationInput* input) const {
  auto result = _child->evaluate(input);

  auto calculator = liftBinaryCalculationToEvaluateResults(
      RangeCalculation{}, ValueExtractor{}, UnaryOperation{}, input);
  result = std::move(std::visit(calculator, result));
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
template <typename ValueExtractor, TaggedFunctionConcept... TagAndFunctions>
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
    result = std::move(std::visit(calculator, std::move(result),
                                  _children[i]->evaluate(input)));
  }
  return result;
};

bool isKbVariable(const SparqlExpression::EvaluateResult& result,
                  EvaluationInput* input) {
  auto ptr = std::get_if<SparqlExpression::Variable>(&result);
  if (!ptr) {
    return false;
  }
  return input->_variableColumnMap[ptr->_variable].second ==
         ResultTable::ResultType::KB;
}

bool isConstant(const SparqlExpression::EvaluateResult& x) {
  auto v = []<typename T>(const T&) {
    return !ad_utility::isVector<T> && !std::is_same_v<Variable, T>;
  };
  return std::visit(v, x);
}

double getDoubleFromConstant(const SparqlExpression::EvaluateResult& x,
                             [[maybe_unused]] EvaluationInput* input) {
  auto v = [input]<typename T>(const T& t)->double {
    if constexpr (ad_utility::isVector<T> || std::is_same_v<Variable, T>) {
      AD_CHECK(false);
    } else if constexpr (std::is_same_v<string, T>) {
      return std::numeric_limits<double>::quiet_NaN();
    } else if constexpr (std::is_same_v<StrongIdAndDatatype, T>) {
      return NumericValueGetter{}(t._id, t._type, input);
    } else {
      return static_cast<double>(t);
    }
  };
  return std::visit(v, x);
}

// ___________________________________________________________________________
SparqlExpression::EvaluateResult EqualsExpression::evaluate(
    EvaluationInput* input) const {
  auto left = _childLeft->evaluate(input);
  auto right = _childRight->evaluate(input);

  if (isKbVariable(left, input) && isKbVariable(right, input)) {
    auto leftVariable = std::get<Variable>(left)._variable;
    auto rightVariable = std::get<Variable>(right)._variable;
    auto leftColumn = input->_variableColumnMap[leftVariable].first;
    auto rightColumn = input->_variableColumnMap[rightVariable].first;
    LimitedVector<bool> result{input->_allocator};
    result.reserve(input->_endIndex - input->_beginIndex);
    for (size_t i = input->_beginIndex; i < input->_endIndex; ++i) {
      result.push_back(input->_inputTable(i, leftColumn) ==
                       input->_inputTable(i, rightColumn));
    }
    return std::move(result);
  } else if (isKbVariable(left, input) && isConstant(right)) {
    auto valueString = ad_utility::convertFloatStringToIndexWord(
        std::to_string(getDoubleFromConstant(right, input)));
    Id idOfConstant;
    auto leftVariable = std::get<Variable>(left)._variable;
    auto leftColumn = input->_variableColumnMap[leftVariable].first;
    if (!input->_qec.getIndex().getVocab().getId(valueString, &idOfConstant)) {
      // empty result
      return Set{};
    }
    if (input->_resultSortedOn.size() > 0 &&
        input->_resultSortedOn[0] == leftColumn) {
      auto comp = [&](const auto& l, const auto& r) {
        return l[leftColumn] < r;
      };
      auto upperComp = [&](const auto& l, const auto& r) {
        return r[leftColumn] < l;
      };
      auto lb = std::lower_bound(
          input->_inputTable.begin() + input->_beginIndex,
          input->_inputTable.begin() + input->_endIndex, idOfConstant, comp);
      auto ub =
          std::upper_bound(input->_inputTable.begin() + input->_beginIndex,
                           input->_inputTable.begin() + input->_endIndex,
                           idOfConstant, upperComp);

      size_t lowerConverted =
          (lb - input->_inputTable.begin()) - input->_beginIndex;
      size_t upperConverted =
          (ub - input->_inputTable.begin()) - input->_beginIndex;

      return Set{std::pair(lowerConverted, upperConverted)};
    } else {
      LimitedVector<double> result{input->_allocator};
      result.reserve(input->_endIndex - input->_beginIndex);
      for (size_t i = input->_beginIndex; i < input->_endIndex; ++i) {
        result.push_back(input->_inputTable(i, leftColumn) == idOfConstant);
      }
      return std::move(result);
    }
  } else {
    // currently always assume numeric values
    auto equals = [](const auto& a, const auto& b) -> bool { return a == b; };
    auto calculator = liftBinaryCalculationToEvaluateResults(
        NoRangeCalculation{}, NumericValueGetter{}, equals, input);
    return std::visit(calculator, std::move(left), std::move(right));
  }
  // TODO<joka921> Continue here
}

template <typename RangeCalculation, typename ValueGetter, typename AggregateOp,
          typename FinalOp, TagString Tag>
SparqlExpression::EvaluateResult
AggregateExpression<RangeCalculation, ValueGetter, AggregateOp, FinalOp,
                    Tag>::evaluate(EvaluationInput* input) const {
  auto childResult = _child->evaluate(input);

  if (_distinct) {
    auto calculator = liftAggregateCalculationToEvaluateResults<true>(
        RangeCalculation{}, ValueGetter{}, _aggregateOp, FinalOp{}, input);
    return std::visit(calculator, std::move(childResult));
  } else {
    auto calculator = liftAggregateCalculationToEvaluateResults<false>(
        RangeCalculation{}, ValueGetter{}, _aggregateOp, FinalOp{}, input);
    return std::visit(calculator, std::move(childResult));
  }
}

SparqlExpression::EvaluateResult SampleExpression::evaluate(
    EvaluationInput* input) const {
  // The child is already set up to perform all the work.
  auto childResultVariant = _child->evaluate(input);
  auto evaluator = [input]<typename T>(const T& childResult)->EvaluateResult {
    if constexpr (std::is_same_v<T, setOfIntervals::Set>) {
      // If any element is true, then we sample this element.
      return !childResult.empty();
    } else if constexpr (ad_utility::isVector<T>) {
      AD_CHECK(!childResult.empty());
      return childResult[0];
    } else if constexpr (std::is_same_v<T, Variable>) {
      AD_CHECK(input->_endIndex > input->_beginIndex);
      EvaluationInput newInput{input->_qec,

                               input->_variableColumnMap,
                               input->_inputTable,
                               input->_beginIndex,
                               input->_beginIndex + 1,
                               input->_allocator};
      auto idOfFirstAsVector = getIdsFromVariable(childResult, &newInput);
      return StrongIdAndDatatype{
          idOfFirstAsVector[0],
          input->_variableColumnMap.at(childResult._variable).second};
    } else {
      // this is a constant
      return childResult;
    }
  };

  return std::visit(evaluator, std::move(childResultVariant));
}

// Explicit instantiations.
template class UnaryExpression<NoRangeCalculation, BooleanValueGetter,
                               decltype(unaryNegate), "!">;
template class UnaryExpression<NoRangeCalculation, NumericValueGetter,
                               decltype(unaryMinus), "unary-">;
template class BinaryExpression<Union, BooleanValueGetter, decltype(orLambda),
                                "||">;

template class BinaryExpression<Intersection, BooleanValueGetter,
                                decltype(andLambda), "&&">;

template class DispatchedBinaryExpression<
    NumericValueGetter, TaggedFunction<"+", decltype(add)>,
    TaggedFunction<"-", decltype(subtract)>>;

template class DispatchedBinaryExpression<
    NumericValueGetter, TaggedFunction<"*", decltype(multiply)>,
    TaggedFunction<"/", decltype(divide)>>;

template class AggregateExpression<NoRangeCalculation, BooleanValueGetter,
                                   decltype(count), decltype(noop), "COUNT">;

template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(add), decltype(noop), "SUM">;

template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(add), decltype(averageFinalOp),
                                   "AVG">;

template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(minLambda), decltype(noop), "MIN">;
template class AggregateExpression<NoRangeCalculation, NumericValueGetter,
                                   decltype(maxLambda), decltype(noop), "MAX">;
template class AggregateExpression<NoRangeCalculation, StringValueGetter,
                                   PerformConcat, decltype(noop),
                                   "GROUP_CONCAT">;
}  // namespace sparqlExpression::detail
