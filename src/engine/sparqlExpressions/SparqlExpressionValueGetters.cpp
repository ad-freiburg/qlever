//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "SparqlExpressionValueGetters.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "global/Constants.h"
#include "util/Conversions.h"

using namespace sparqlExpression::detail;
// _____________________________________________________________________________
double NumericValueGetter::operator()(ValueId id, EvaluationContext*) const {
  switch (id.getDatatype()) {
    case Datatype::Double:
      return id.getDouble();
    case Datatype::Int:
      return static_cast<double>(id.getInt());
    case Datatype::Bool:
      // TODO<joka921> Check in the specification what the correct behavior is
      // here.
      return static_cast<double>(id.getBool());
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
    case Datatype::Date:
      return std::numeric_limits<double>::quiet_NaN();
  }
  AD_FAIL();
}

// _____________________________________________________________________________
bool EffectiveBooleanValueGetter::operator()(ValueId id,
                                             EvaluationContext* context) const {
  switch (id.getDatatype()) {
    case Datatype::Double: {
      auto d = id.getDouble();
      return d != 0.0 && !std::isnan(d);
    }
    case Datatype::Int:
      return id.getInt() != 0;
    case Datatype::Bool:
      return id.getBool();
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
      return !(context->_localVocab.getWord(id.getLocalVocabIndex()).empty());
    }
    case Datatype::TextRecordIndex:
      return true;
    case Datatype::Date:
      return true;
  }
  AD_FAIL();
}

// ____________________________________________________________________________
string StringValueGetter::operator()(Id id, EvaluationContext* context) const {
  auto optionalStringAndType =
      ExportQueryExecutionTrees::idToStringAndType<true>(
          context->_qec.getIndex(), id, context->_localVocab);
  if (optionalStringAndType.has_value()) {
    return std::move(optionalStringAndType.value().first);
  } else {
    return "";
  }
}

// ____________________________________________________________________________
std::optional<string> LiteralFromIdGetter::operator()(
    ValueId id, const sparqlExpression::EvaluationContext* context) const {
  auto optionalStringAndType =
      ExportQueryExecutionTrees::idToStringAndType<true, true>(
          context->_qec.getIndex(), id, context->_localVocab);
  if (optionalStringAndType.has_value()) {
    return std::move(optionalStringAndType.value().first);
  } else {
    return std::nullopt;
  }
}

// ____________________________________________________________________________
bool IsValidValueGetter::operator()(
    ValueId id, [[maybe_unused]] EvaluationContext* context) const {
  // Every knowledge base value that is bound converts to "True"
  // TODO<joka921> check for the correct semantics of the error handling and
  // implement it in a further version.
  return id != ValueId::makeUndefined();
}
