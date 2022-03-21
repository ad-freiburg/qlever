//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 23.09.21.
//

#include "SparqlExpressionValueGetters.h"

#include "../../global/Constants.h"
#include "../../util/Conversions.h"

using namespace sparqlExpression::detail;
// _____________________________________________________________________________
double NumericValueGetter::operator()(StrongIdWithResultType strongId,
                                      EvaluationContext* context) const {
  const Id id = strongId._id._value;

  if (id.isInteger()) {
    return static_cast<double>(id.getIntegerUnchecked());
  }
  if (id.isDouble()) {
    return id.getDoubleUnchecked();
  }
  // TODO<joka921> Other datatypes like bool? Should we make the above a switch?
  AD_CHECK(false);
}

// _____________________________________________________________________________
bool EffectiveBooleanValueGetter::operator()(StrongIdWithResultType strongId,
                                             EvaluationContext* context) const {
  const Id id = strongId._id._value;

  if (id.isInteger()) {
    return id.getIntegerUnchecked() != 0;
  }
  if (id.isDouble()) {
    auto d = id.getDoubleUnchecked();
    return d !=0.0 && !std::isnan(d);
  }
  if (id.isVocab()) {
    std::string entity =
        context->_qec.getIndex().idToOptionalString(id.getVocabUnchecked()).value_or("");
    return (!entity.empty());
  }
  // TODO<joka921> Other types like Text/Data/localVocab etc.
  AD_CHECK(false);
}

// ____________________________________________________________________________
string StringValueGetter::operator()(StrongIdWithResultType strongId,
                                     EvaluationContext* context) const {
  const Id id = strongId._id._value;

  if (id.isInteger()) {
    return std::to_string(id.getIntegerUnchecked());
  }
  if (id.isDouble()) {
    return std::to_string(id.getDoubleUnchecked());
  }
  if (id.isVocab()) {
    return context->_qec.getIndex().idToOptionalString(id.getVocabUnchecked()).value_or("");
  }
  // TODO<joka921> Other types like Text/Data/localVocab etc.
}

// ____________________________________________________________________________
bool IsValidValueGetter::operator()(
    StrongIdWithResultType strongId,
    [[maybe_unused]] EvaluationContext* context) const {
  // Every knowledge base value that is bound converts to "True"
  // TODO<joka921> check for the correct semantics of the error handling and
  // implement it in a further version.
  return strongId._id._value != ID_NO_VALUE;
}
