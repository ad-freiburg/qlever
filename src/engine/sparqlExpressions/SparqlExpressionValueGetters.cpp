//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "SparqlExpressionValueGetters.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "global/Constants.h"
#include "util/Conversions.h"

using namespace sparqlExpression::detail;

// _____________________________________________________________________________
NumericValue NumericValueGetter::operator()(
    ValueId id, const sparqlExpression::EvaluationContext*) const {
  switch (id.getDatatype()) {
    case Datatype::Double:
      return id.getDouble();
    case Datatype::Int:
      return id.getInt();
    case Datatype::Bool:
      // TODO<joka921> Check in the specification what the correct behavior is
      // here. They probably should be UNDEF as soon as we have conversion
      // functions.
      return static_cast<int64_t>(id.getBool());
    case Datatype::Undefined:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
    case Datatype::Date:
      return NotNumeric{};
  }
  AD_FAIL();
}

// _____________________________________________________________________________
auto EffectiveBooleanValueGetter::operator()(
    ValueId id, const EvaluationContext* context) const -> Result {
  using enum Result;
  switch (id.getDatatype()) {
    case Datatype::Double: {
      auto d = id.getDouble();
      return (d != 0.0 && !std::isnan(d)) ? True : False;
    }
    case Datatype::Int:
      return (id.getInt() != 0) ? True : False;
    case Datatype::Bool:
      return id.getBool() ? True : False;
    case Datatype::Undefined:
      return Undef;
    case Datatype::VocabIndex: {
      auto index = id.getVocabIndex();
      // TODO<joka921> We could precompute whether the empty literal or empty
      // iri are contained in the KB.
      return context->_qec.getIndex()
                     .getVocab()
                     .indexToOptionalString(index)
                     .value_or("")
                     .empty()
                 ? False
                 : True;
    }
    case Datatype::LocalVocabIndex: {
      return (context->_localVocab.getWord(id.getLocalVocabIndex()).empty())
                 ? False
                 : True;
    }
    case Datatype::TextRecordIndex:
    case Datatype::Date:
      return True;
  }
  AD_FAIL();
}

// ____________________________________________________________________________
std::optional<std::string> StringValueGetter::operator()(
    Id id, const EvaluationContext* context) const {
  auto optionalStringAndType =
      ExportQueryExecutionTrees::idToStringAndType<true>(
          context->_qec.getIndex(), id, context->_localVocab);
  if (optionalStringAndType.has_value()) {
    return std::move(optionalStringAndType.value().first);
  } else {
    return std::nullopt;
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
    ValueId id, [[maybe_unused]] const EvaluationContext* context) const {
  // Every knowledge base value that is bound converts to "True"
  // TODO<joka921> check for the correct semantics of the error handling and
  // implement it in a further version.
  return id != ValueId::makeUndefined();
}
