//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./SparqlExpressionHelpers.h"

namespace sparqlExpression {
namespace detail {

// ___________________________________________________________________________
template <size_t WIDTH>
void getIdsFromVariableImpl(VectorWithMemoryLimit<StrongId>& result,
                            const Variable& variable,
                            EvaluationContext* context) {
  AD_CHECK(result.empty());
  auto staticInput = context->_inputTable.asStaticView<WIDTH>();

  const size_t beginIndex = context->_beginIndex;
  size_t resultSize = context->_endIndex - context->_beginIndex;

  if (!context->_variableToColumnAndResultTypeMap.contains(
          variable._variable)) {
    throw std::runtime_error(
        "Variable " + variable._variable +
        " could not be mapped to context column of expression evaluation");
  }

  size_t columnIndex =
      context->_variableToColumnAndResultTypeMap.at(variable._variable).first;

  result.reserve(resultSize);
  for (size_t i = 0; i < resultSize; ++i) {
    result.push_back(StrongId{staticInput(beginIndex + i, columnIndex)});
  }
}

// ____________________________________________________________________________
VectorWithMemoryLimit<StrongId> getIdsFromVariable(const Variable& variable,
                                                   EvaluationContext* context) {
  auto cols = context->_inputTable.cols();
  VectorWithMemoryLimit<StrongId> result{context->_allocator};
  CALL_FIXED_SIZE_1(cols, getIdsFromVariableImpl, result, variable, context);
  return result;
}

}  // namespace detail
}  // namespace sparqlExpression