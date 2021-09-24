//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 23.09.21.
//

#include "SparqlExpressionValueGetters.h"

#include "../global/Constants.h"
#include "../util/Conversions.h"

using namespace sparqlExpression::detail;
// _____________________________________________________________________________
double NumericValueGetter::operator()(StrongId strongId,
                                      ResultTable::ResultType type,
                                      EvaluationContext* context) const {
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
        context->_qec.getIndex().idToOptionalString(id).value_or("");
    if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
      return std::numeric_limits<float>::quiet_NaN();
    } else {
      return ad_utility::convertIndexWordToFloat(entity);
    }
  }
}

string StringValueGetter::operator()(StrongId strongId,
                                     ResultTable::ResultType type,
                                     EvaluationContext* context) const {
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
        context->_qec.getIndex().idToOptionalString(id).value_or("");
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
bool BooleanValueGetter::operator()(
    StrongId strongId, ResultTable::ResultType type,
    [[maybe_unused]] EvaluationContext* context) const {
  // Every knowledge base value that is bound converts to "True"
  // TODO<joka921> check for the correct semantics of the error handling and
  // implement it in a further version.
  return type != ResultTable::ResultType::KB || strongId._value != ID_NO_VALUE;
}
