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
  switch (strongId._type) {
    // This code is borrowed from the original QLever code.
    case ResultTable::ResultType::VERBATIM:
      return static_cast<double>(id.get());
    case ResultTable::ResultType::FLOAT:
      // used to store the id value of the entry interpreted as a float
      float tempF;
      std::memcpy(&tempF, &id, sizeof(float));
      return static_cast<double>(tempF);
    case ResultTable::ResultType::TEXT:
      [[fallthrough]];
    case ResultTable::ResultType::LOCAL_VOCAB:
      return std::numeric_limits<float>::quiet_NaN();
    case ResultTable::ResultType::KB:
      // load the string, parse it as an xsd::int or float
      std::string entity =
          context->_qec.getIndex().idToOptionalString(id).value_or("");
      if (!entity.starts_with(VALUE_FLOAT_PREFIX)) {
        return std::numeric_limits<float>::quiet_NaN();
      } else {
        return ad_utility::convertIndexWordToFloat(entity);
      }
  }
  AD_CHECK(false);
}

// _____________________________________________________________________________
bool EffectiveBooleanValueGetter::operator()(StrongIdWithResultType strongId,
                                             EvaluationContext* context) const {
  const Id id = strongId._id._value;
  switch (strongId._type) {
    case ResultTable::ResultType::VERBATIM:
      return id.get() != 0;
    case ResultTable::ResultType::FLOAT:
      // used to store the id value of the entry interpreted as a float
      float tempF;
      std::memcpy(&tempF, &id, sizeof(float));
      return tempF != 0.0 && !std::isnan(tempF);
    case ResultTable::ResultType::TEXT:
      return true;
    case ResultTable::ResultType::LOCAL_VOCAB:
      AD_CHECK(id.get() < context->_localVocab.size());
      return !(context->_localVocab[id.get()].empty());
    case ResultTable::ResultType::KB:
      // Load the string.
      std::string entity =
          context->_qec.getIndex().idToOptionalString(id).value_or("");
      // Empty or unbound strings are false.
      if (entity.empty()) {
        return false;
      }
      // Non-numeric non-empty values are always true
      if (!entity.starts_with(VALUE_FLOAT_PREFIX)) {
        return true;
      } else {
        // 0 and nan values are considered false.
        float f = ad_utility::convertIndexWordToFloat(entity);
        return f != 0.0 && !std::isnan(f);
      }
  }
  AD_CHECK(false);
}

// ____________________________________________________________________________
string StringValueGetter::operator()(StrongIdWithResultType strongId,
                                     EvaluationContext* context) const {
  const Id id = strongId._id._value;
  switch (strongId._type) {
    case ResultTable::ResultType::VERBATIM:
      return std::to_string(id.get());
    case ResultTable::ResultType::FLOAT:
      // used to store the id value of the entry interpreted as a float
      float tempF;
      std::memcpy(&tempF, &id, sizeof(float));
      return std::to_string(tempF);
    case ResultTable::ResultType::TEXT:
      return context->_qec.getIndex().getTextExcerpt(id.get());
    case ResultTable::ResultType::LOCAL_VOCAB:
      AD_CHECK(id.get() < context->_localVocab.size());
      return context->_localVocab[id.get()];
    case ResultTable::ResultType::KB:
      // load the string, parse it as an xsd::int or float
      std::string entity =
          context->_qec.getIndex().idToOptionalString(id).value_or("");
      if (entity.starts_with(VALUE_FLOAT_PREFIX)) {
        return std::to_string(ad_utility::convertIndexWordToFloat(entity));
      } else if (entity.starts_with(VALUE_DATE_PREFIX)) {
        return ad_utility::convertDateToIndexWord(entity);
      } else {
        return entity;
      }
  }
  AD_CHECK(false);
}

// ____________________________________________________________________________
bool IsValidValueGetter::operator()(
    StrongIdWithResultType strongId,
    [[maybe_unused]] EvaluationContext* context) const {
  // Every knowledge base value that is bound converts to "True"
  // TODO<joka921> check for the correct semantics of the error handling and
  // implement it in a further version.
  return strongId._type != ResultTable::ResultType::KB ||
         strongId._id._value != ID_NO_VALUE;
}
