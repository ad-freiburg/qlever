//
// Created by Benedikt Kantz on 16.11.26.
//

#include "engine/tensor/aggregate/TensorAggregate.h"

#include "engine/sparqlExpressions/AggregateExpressionImpl.h"
#include "engine/tensor/aggregate/TensorVar.h"

namespace sparqlExpression::detail {

// Explicit instantiations for the tensor aggregate expressions. The member
// definitions of `AggregateExpression` (including the conversion of the final
// `OptTensor` to an `ExpressionResult`) are in `AggregateExpressionImpl.h`.

// dta:sum
template class AggregateExpression<TensorAvgOperation, Identity>;

// dta:avg
template class AggregateExpression<TensorAvgOperation, TensorAvgFinalOperation>;

// dta:var
template class AggregateExpression<TensorAvgOperation, TensorVarFinalOperation>;


}  // namespace sparqlExpression::detail
