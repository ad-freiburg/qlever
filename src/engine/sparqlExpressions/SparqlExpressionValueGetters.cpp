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
                                      EvaluationContext*) const {
  const Id id = strongId._id._value;
  switch (id.getDatatype()) {
    case Datatype::Double:
      return id.getDouble();
    case Datatype::Int:
      return static_cast<double>(id.getInt());
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
      return std::numeric_limits<double>::quiet_NaN();
  }
  AD_CHECK(false);
}

// _____________________________________________________________________________
bool EffectiveBooleanValueGetter::operator()(StrongIdWithResultType strongId,
                                             EvaluationContext* context) const {
  const Id id = strongId._id._value;
  switch (id.getDatatype()) {
    case Datatype::Double: {
      auto d = id.getDouble();
      return d != 0.0 && !std::isnan(d);
    }
    case Datatype::Int:
      return id.getInt() != 0;
    case Datatype::Undefined:
      return false;
    case Datatype::VocabIndex: {
      auto index = id.getVocabIndex();
      // TODO<joka921> We could precompute whether the empty literal or empty
      // iri are contained in the KB.
      return !context->_qec.getIndex()
                  .getVocab()
                  .indexToOptionalString(index)
                  .value_or("")
                  .empty();
    }
    case Datatype::LocalVocabIndex: {
      auto index = id.getLocalVocabIndex();
      AD_CHECK(index.get() < context->_localVocab.size());
      return !(context->_localVocab[index.get()].empty());
    }
    case Datatype::TextRecordIndex:
      return true;
  }
  AD_CHECK(false);
}

// ____________________________________________________________________________
string StringValueGetter::operator()(StrongIdWithResultType strongId,
                                     EvaluationContext* context) const {
  const Id id = strongId._id._value;
  switch (id.getDatatype()) {
    case Datatype::Undefined:
      return "";
    case Datatype::Double:
      return std::to_string(id.getDouble());
    case Datatype::Int:
      return std::to_string(id.getInt());
    case Datatype::VocabIndex:
      return context->_qec.getIndex()
          .getVocab()
          .indexToOptionalString(id.getVocabIndex())
          .value_or("");
    case Datatype::LocalVocabIndex: {
      auto index = id.getLocalVocabIndex().get();
      AD_CHECK(index < context->_localVocab.size());
      return context->_localVocab[index];
    }
    case Datatype::TextRecordIndex:
      context->_qec.getIndex().getTextExcerpt(id.getTextRecordIndex());
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
  return strongId._id._value != ValueId::makeUndefined();
}
