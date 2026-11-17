// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

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

// dta:std
template class AggregateExpression<TensorAvgOperation, TensorStdFinalOperation>;


}  // namespace sparqlExpression::detail
