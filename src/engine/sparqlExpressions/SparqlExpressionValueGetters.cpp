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
    case Datatype::WordVocabIndex:
    case Datatype::Date:
    case Datatype::BlankNodeIndex:
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
    case Datatype::BlankNodeIndex:
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
      return (context->_localVocab.getWord(id.getLocalVocabIndex())
                  .getContent()
                  .empty())
                 ? False
                 : True;
    }
    case Datatype::WordVocabIndex:
    case Datatype::TextRecordIndex:
    case Datatype::Date:
      return True;
  }
  AD_FAIL();
}

// ____________________________________________________________________________
std::optional<std::string> StringValueGetter::operator()(
    Id id, const EvaluationContext* context) const {
  // `true` means that we remove the quotes and angle brackets.
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
template <auto isSomethingFunction, auto prefix>
Id IsSomethingValueGetter<isSomethingFunction, prefix>::operator()(
    ValueId id, const EvaluationContext* context) const {
  if (id.getDatatype() == Datatype::VocabIndex) {
    // See instantiations below for what `isSomethingFunction` is.
    return Id::makeFromBool(std::invoke(isSomethingFunction,
                                        context->_qec.getIndex().getVocab(),
                                        id.getVocabIndex()));
  } else if (id.getDatatype() == Datatype::LocalVocabIndex) {
    auto word = ExportQueryExecutionTrees::idToStringAndType<false>(
        context->_qec.getIndex(), id, context->_localVocab);
    return Id::makeFromBool(word.has_value() &&
                            word.value().first.starts_with(prefix));
  } else {
    return Id::makeFromBool(false);
  }
}
template struct sparqlExpression::detail::IsSomethingValueGetter<
    &Index::Vocab::isIri, isIriPrefix>;
template struct sparqlExpression::detail::IsSomethingValueGetter<
    &Index::Vocab::isLiteral, isLiteralPrefix>;

// _____________________________________________________________________________
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

// _____________________________________________________________________________
bool IsValidValueGetter::operator()(
    ValueId id, [[maybe_unused]] const EvaluationContext* context) const {
  // Every knowledge base value that is bound converts to "True"
  // TODO<joka921> check for the correct semantics of the error handling and
  // implement it in a further version.
  return id != ValueId::makeUndefined();
}

// _____________________________________________________________________________
IntDoubleStr ToNumericValueGetter::operator()(
    ValueId id, [[maybe_unused]] const EvaluationContext* context) const {
  switch (id.getDatatype()) {
    case Datatype::Undefined:
      return std::monostate{};
    case Datatype::Int:
      return id.getInt();
    case Datatype::Double:
      return id.getDouble();
    case Datatype::Bool:
      return static_cast<int>(id.getBool());
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
    case Datatype::WordVocabIndex:
    case Datatype::Date:
    case Datatype::BlankNodeIndex:
      auto optString = LiteralFromIdGetter{}(id, context);
      if (optString.has_value()) {
        return std::move(optString.value());
      } else {
        return std::monostate{};
      }
  }
  AD_FAIL();
}

// _____________________________________________________________________________
IntDoubleStr ToNumericValueGetter::operator()(
    const LiteralOrIri& s,
    [[maybe_unused]] const EvaluationContext* context) const {
  return std::string(asStringViewUnsafe(s.getContent()));
}

// _____________________________________________________________________________
OptIri DatatypeValueGetter::operator()(ValueId id,
                                       const EvaluationContext* context) const {
  using enum Datatype;
  auto datatype = id.getDatatype();
  std::optional<std::string> entity;
  switch (datatype) {
    case Bool:
      return Iri::fromIrirefWithoutBrackets(XSD_BOOLEAN_TYPE);
    case Double:
      return Iri::fromIrirefWithoutBrackets(XSD_DOUBLE_TYPE);
    case Int:
      return Iri::fromIrirefWithoutBrackets(XSD_INT_TYPE);
    case Date: {
      auto dateType = id.getDate().toStringAndType().second;
      AD_CORRECTNESS_CHECK(dateType != nullptr);
      return Iri::fromIrirefWithoutBrackets(dateType);
    }
    case LocalVocabIndex:
    case VocabIndex:
      return (*this)(ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
                         context->_qec.getIndex(), id, context->_localVocab),
                     context);
    case Undefined:
    case BlankNodeIndex:
    case TextRecordIndex:
    case WordVocabIndex:
      return std::nullopt;
  }
  AD_FAIL();
}

// _____________________________________________________________________________
OptIri DatatypeValueGetter::operator()(
    const LiteralOrIri& litOrIri,
    [[maybe_unused]] const EvaluationContext* context) const {
  if (litOrIri.isLiteral()) {
    const auto& literal = litOrIri.getLiteral();
    if (literal.hasLanguageTag()) {
      return Iri::fromIrirefWithoutBrackets(RDF_LANGTAG_STRING);
    } else if (literal.hasDatatype()) {
      return Iri::fromIrirefWithoutBrackets(
          asStringViewUnsafe(literal.getDatatype()));
    } else {
      return Iri::fromIrirefWithoutBrackets(XSD_STRING);
    }
  } else {
    return std::nullopt;
  }
}

// _____________________________________________________________________________
OptIri IriValueGetter::operator()(
    const LiteralOrIri& s,
    [[maybe_unused]] const EvaluationContext* context) const {
  if (s.isIri()) {
    return s.getIri();
  } else {
    return std::nullopt;
  }
}
