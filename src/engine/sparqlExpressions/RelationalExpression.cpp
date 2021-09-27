//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
//

#include "RelationalExpression.h"

using namespace sparqlExpression;
using namespace sparqlExpression::detail;
using ad_utility::SetOfIntervals;

// Some helper functions, TODO<joka921> check if they are still needed.
bool isKbVariable(const ExpressionResult& result, EvaluationContext* context) {
  auto ptr = std::get_if<Variable>(&result);
  if (!ptr) {
    return false;
  }
  return context->_variableToColumnAndResultTypeMap.at(ptr->_variable).second ==
         ResultTable::ResultType::KB;
}

bool isConstant(const ExpressionResult& x) {
  auto v = []<typename T>(const T&) {
    return !ad_utility::isVector<T> && !std::is_same_v<Variable, T>;
  };
  return std::visit(v, x);
}

double getDoubleFromConstant(const ExpressionResult& x,
                             [[maybe_unused]] EvaluationContext* context) {
  auto v = [context]<typename T>(const T& t) -> double {
    if constexpr (isVectorResult<T> || std::is_same_v<Variable, T> ||
                  std::is_same_v<SetOfIntervals, T>) {
      AD_CHECK(false);
    } else if constexpr (std::is_same_v<string, T>) {
      return std::numeric_limits<double>::quiet_NaN();
    } else if constexpr (std::is_same_v<StrongIdWithResultType, T>) {
      return NumericValueGetter{}(t, context);
    } else {
      return static_cast<double>(t);
    }
  };
  return std::visit(v, x);
}

// ___________________________________________________________________________
ExpressionResult EqualsExpression::evaluate(EvaluationContext* context) const {
  auto left = _childLeft->evaluate(context);
  auto right = _childRight->evaluate(context);

  if (isKbVariable(left, context) && isKbVariable(right, context)) {
    auto leftVariable = std::get<Variable>(left)._variable;
    auto rightVariable = std::get<Variable>(right)._variable;
    auto leftColumn =
        context->_variableToColumnAndResultTypeMap.at(leftVariable).first;
    auto rightColumn =
        context->_variableToColumnAndResultTypeMap.at(rightVariable).first;
    VectorWithMemoryLimit<Bool> result{context->_allocator};
    result.reserve(context->_endIndex - context->_beginIndex);
    for (size_t i = context->_beginIndex; i < context->_endIndex; ++i) {
      result.push_back(context->_inputTable(i, leftColumn) ==
                       context->_inputTable(i, rightColumn));
    }
    return result;
  } else if (isKbVariable(left, context) &&
             std::visit(
                 [](const auto& x) { return isConstantResult<decltype(x)>; },
                 right)) {
    auto valueString = ad_utility::convertFloatStringToIndexWord(
        std::to_string(getDoubleFromConstant(right, context)));
    Id idOfConstant;
    auto leftVariable = std::get<Variable>(left)._variable;
    auto leftColumn =
        context->_variableToColumnAndResultTypeMap.at(leftVariable).first;
    if (!context->_qec.getIndex().getVocab().getId(valueString,
                                                   &idOfConstant)) {
      // empty result
      return SetOfIntervals{};
    }
    if (context->_columnsByWhichResultIsSorted.size() > 0 &&
        context->_columnsByWhichResultIsSorted[0] == leftColumn) {
      auto comp = [&](const auto& l, const auto& r) {
        return l[leftColumn] < r;
      };
      auto upperComp = [&](const auto& l, const auto& r) {
        return r[leftColumn] < l;
      };
      auto lb =
          std::lower_bound(context->_inputTable.begin() + context->_beginIndex,
                           context->_inputTable.begin() + context->_endIndex,
                           idOfConstant, comp);
      auto ub =
          std::upper_bound(context->_inputTable.begin() + context->_beginIndex,
                           context->_inputTable.begin() + context->_endIndex,
                           idOfConstant, upperComp);

      size_t lowerConverted =
          (lb - context->_inputTable.begin()) - context->_beginIndex;
      size_t upperConverted =
          (ub - context->_inputTable.begin()) - context->_beginIndex;

      return SetOfIntervals{{std::pair(lowerConverted, upperConverted)}};
    } else {
      VectorWithMemoryLimit<double> result{context->_allocator};
      result.reserve(context->_endIndex - context->_beginIndex);
      for (size_t i = context->_beginIndex; i < context->_endIndex; ++i) {
        result.push_back(context->_inputTable(i, leftColumn) == idOfConstant);
      }
      return result;
    }
  } else {
    throw std::runtime_error(
        "Equality expressions without a variable on one side are currently not "
        "supported");
    // TODO<joka921> fix this before merging!!
    /*
    // currently always assume numeric values
    auto equals = [](const auto& a, const auto& b) -> bool { return a == b; };
    return evaluateNaryOperation(NoRangeCalculation{}, NumericValueGetter{},
                                 equals, context, std::move(left),
                                 std::move(right));
    */
  }
  // TODO<joka921> Continue here
}
