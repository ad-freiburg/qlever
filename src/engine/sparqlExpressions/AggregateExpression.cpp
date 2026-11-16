// Copyright 2024, University of Freiburg,
//                 Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/AggregateExpressionImpl.h"

#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/StdevExpression.h"

namespace sparqlExpression::detail {

// Explicit instantiation for the AVG expression.
template class AggregateExpression<AvgOperation, AvgFinalOperation>;

// Explicit instantiation for the STDEV expression.
template class AggregateExpression<AvgOperation, StdevFinalOperation>;
template class DeviationAggExpression<AvgOperation, StdevFinalOperation>;

// Explicit instantiations for the other aggregate expressions.
#define INSTANTIATE_AGG_EXP(Function, ValueGetter) \
  template class AggregateExpression<              \
      Operation<2, FunctionAndValueGetters<Function, ValueGetter>>>;
INSTANTIATE_AGG_EXP(AddForSum, NumericValueGetter);
INSTANTIATE_AGG_EXP(Count, IsValidValueGetter);
INSTANTIATE_AGG_EXP(MinLambdaForAllTypes, ActualValueGetter);
INSTANTIATE_AGG_EXP(MaxLambdaForAllTypes, ActualValueGetter);

// Explicit instantiations for the tensor aggregate expressions (dta:sum,
// dta:avg, dta:var). These are needed because the member functions of
// `AggregateExpression` are only defined in `AggregateExpressionImpl.h`.
template class AggregateExpression<TensorAvgOperation, Identity>;
template class AggregateExpression<TensorAvgOperation, TensorAvgFinalOperation>;
template class AggregateExpression<TensorAvgOperation,
                                   sparqlExpression::TensorVarFinalOperation>;


}  // namespace sparqlExpression::detail
