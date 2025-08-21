//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "SparqlExpressionValueGetters.h"

#include <type_traits>

#include "engine/ExportQueryExecutionTrees.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
#include "util/Conversions.h"
#include "util/GeoSparqlHelpers.h"

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
    case Datatype::EncodedVal:
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
    case Datatype::WordVocabIndex:
    case Datatype::Date:
    case Datatype::GeoPoint:
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
    case Datatype::EncodedVal:
      // This assumes that we never use this for empty IRIs.
      return True;
    case Datatype::VocabIndex: {
      auto index = id.getVocabIndex();
      // TODO<joka921> We could precompute whether the empty literal or empty
      // iri are contained in the KB.
      return context->_qec.getIndex().indexToString(index).empty() ? False
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
    case Datatype::GeoPoint:
      return True;
  }
  AD_FAIL();
}

// ____________________________________________________________________________
std::optional<std::string> StringValueGetter::operator()(
    Id id, const EvaluationContext* context) const {
  if (id.getDatatype() == Datatype::Bool) {
    // Always use canonical representation when converting to string.
    return id.getBool() ? "true" : "false";
  }
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
std::optional<ad_utility::triple_component::Literal>
LiteralValueGetterWithStrFunction::operator()(
    Id id, const EvaluationContext* context) const {
  return ExportQueryExecutionTrees::idToLiteral(context->_qec.getIndex(), id,
                                                context->_localVocab);
}

// ____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
LiteralValueGetterWithStrFunction::operator()(const LiteralOrIri& s,
                                              const EvaluationContext*) const {
  return ExportQueryExecutionTrees::handleIriOrLiteral(s, false);
}

// ____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
LiteralValueGetterWithoutStrFunction::operator()(
    Id id, const EvaluationContext* context) const {
  return ExportQueryExecutionTrees::idToLiteral(context->_qec.getIndex(), id,
                                                context->_localVocab, true);
}

// ____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
LiteralValueGetterWithoutStrFunction::operator()(
    const LiteralOrIri& s, const EvaluationContext*) const {
  return ExportQueryExecutionTrees::handleIriOrLiteral(s, true);
}

// ____________________________________________________________________________
std::optional<std::string> ReplacementStringGetter::operator()(
    Id id, const EvaluationContext* context) const {
  std::optional<std::string> originalString =
      LiteralFromIdGetter{}(id, context);
  if (!originalString.has_value()) {
    return originalString;
  }
  return convertToReplacementString(originalString.value());
}

// ____________________________________________________________________________
std::optional<std::string> ReplacementStringGetter::operator()(
    const LiteralOrIri& s, const EvaluationContext*) const {
  return convertToReplacementString(asStringViewUnsafe(s.getContent()));
}

// ____________________________________________________________________________
std::string ReplacementStringGetter::convertToReplacementString(
    std::string_view view) {
  std::string result;
  // Rough estimate of the size of the result string.
  result.reserve(view.size());
  for (size_t i = 0; i < view.size(); i++) {
    char c = view.at(i);
    switch (c) {
      case '$':
        // Re2 used \1, \2, ... for backreferences, so we change $ to \.
        result.push_back('\\');
        break;
      case '\\':
        // "\$" is unescaped to "$"
        if (i + 1 < view.size() && view.at(i + 1) == '$') {
          result.push_back('$');
          i++;
        } else {
          // Escape existing backslashes.
          result.push_back(c);
          result.push_back(c);
        }
        break;
      default:
        result.push_back(c);
        break;
    }
  }
  return result;
}

// ____________________________________________________________________________
template <auto isSomethingFunction, auto prefix>
Id IsSomethingValueGetter<isSomethingFunction, prefix>::operator()(
    ValueId id, const EvaluationContext* context) const {
  switch (id.getDatatype()) {
    case Datatype::VocabIndex:
      // See instantiations below for what `isSomethingFunction` is.
      return Id::makeFromBool(std::invoke(isSomethingFunction,
                                          context->_qec.getIndex().getVocab(),
                                          id.getVocabIndex()));
    case Datatype::LocalVocabIndex: {
      auto word = ExportQueryExecutionTrees::idToStringAndType<false>(
          context->_qec.getIndex(), id, context->_localVocab);
      return Id::makeFromBool(word.has_value() &&
                              word.value().first.starts_with(prefix));
    }
    case Datatype::EncodedVal:
      // We currently only encode IRIs.
      return Id::makeFromBool(prefix == isIriPrefix);
    case Datatype::Bool:
    case Datatype::Int:
    case Datatype::Double:
    case Datatype::Date:
    case Datatype::GeoPoint:
      if constexpr (prefix == isLiteralPrefix) {
        return Id::makeFromBool(true);
      }
    case Datatype::Undefined:
    case Datatype::TextRecordIndex:
    case Datatype::WordVocabIndex:
    case Datatype::BlankNodeIndex:
      return Id::makeFromBool(false);
  }
  AD_FAIL();
}
template struct sparqlExpression::detail::IsSomethingValueGetter<
    &Index::Vocab::isIri, isIriPrefix>;
template struct sparqlExpression::detail::IsSomethingValueGetter<
    &Index::Vocab::isLiteral, isLiteralPrefix>;

// _____________________________________________________________________________
std::optional<std::string> LiteralFromIdGetter::operator()(
    ValueId id, const EvaluationContext* context) const {
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
    case Datatype::GeoPoint:
      return id.getGeoPoint().toStringRepresentation();
    case Datatype::VocabIndex:
    case Datatype::LocalVocabIndex:
    case Datatype::TextRecordIndex:
    case Datatype::WordVocabIndex:
    case Datatype::Date:
    case Datatype::BlankNodeIndex:
    case Datatype::EncodedVal:
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
    case GeoPoint:
      return Iri::fromIrirefWithoutBrackets(GEO_WKT_LITERAL);
    case Date: {
      auto dateType = id.getDate().toStringAndType().second;
      AD_CORRECTNESS_CHECK(dateType != nullptr);
      return Iri::fromIrirefWithoutBrackets(dateType);
    }
    case EncodedVal:
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

// _____________________________________________________________________________
UnitOfMeasurement UnitOfMeasurementValueGetter::operator()(
    ValueId id, const EvaluationContext* context) const {
  // Use cache to remember fully parsed units for reoccurring ValueIds
  return cache_.getOrCompute(
      id, [&context](const ValueId& value) -> UnitOfMeasurement {
        // Get string content of ValueId
        auto str = ExportQueryExecutionTrees::idToLiteralOrIri(
            context->_qec.getIndex(), value, context->_localVocab, true);
        // Use LiteralOrIri overload for actual computation
        if (str.has_value()) {
          return UnitOfMeasurementValueGetter{}(str.value(), context);
        }
        return UnitOfMeasurement::UNKNOWN;
      });
}

// _____________________________________________________________________________
UnitOfMeasurement UnitOfMeasurementValueGetter::operator()(
    const LiteralOrIri& s, const EvaluationContext*) const {
  return litOrIriToUnit(s);
}

// _____________________________________________________________________________
UnitOfMeasurement UnitOfMeasurementValueGetter::litOrIriToUnit(
    const LiteralOrIri& s) {
  // The GeoSPARQL standard requires literals of datatype xsd:anyURI for units
  // of measurement. Because this is a rather obscure requirement, we support
  // IRIs also.
  if (s.isIri() ||
      (s.isLiteral() && s.getLiteral().hasDatatype() &&
       asStringViewUnsafe(s.getLiteral().getDatatype()) == XSD_ANYURI_TYPE)) {
    return ad_utility::detail::iriToUnitOfMeasurement(
        asStringViewUnsafe(s.getContent()));
  }
  return UnitOfMeasurement::UNKNOWN;
}

//______________________________________________________________________________
CPP_template(typename T, typename ValueGetter)(
    requires(concepts::same_as<sparqlExpression::IdOrLiteralOrIri, T> ||
             concepts::same_as<std::optional<std::string>, T>)) T
    getValue(ValueId id, const sparqlExpression::EvaluationContext* context,
             ValueGetter& valueGetter) {
  using enum Datatype;
  switch (id.getDatatype()) {
    case LocalVocabIndex:
    case EncodedVal:
    case VocabIndex:
      return valueGetter(
          ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
              context->_qec.getIndex(), id, context->_localVocab),
          context);
    case TextRecordIndex:
    case WordVocabIndex:
    case BlankNodeIndex:
    case Bool:
    case Int:
    case Double:
    case Date:
    case GeoPoint:
    case Undefined:
      if constexpr (std::is_same_v<T, sparqlExpression::IdOrLiteralOrIri>) {
        return Id::makeUndefined();
      } else {
        return std::nullopt;
      }
  }
  AD_FAIL();
}

//_____________________________________________________________________________
sparqlExpression::IdOrLiteralOrIri IriOrUriValueGetter::operator()(
    ValueId id, const EvaluationContext* context) const {
  return getValue<sparqlExpression::IdOrLiteralOrIri>(id, context, *this);
}

//______________________________________________________________________________
std::optional<std::string> LanguageTagValueGetter::operator()(
    ValueId id, const EvaluationContext* context) const {
  using enum Datatype;
  switch (id.getDatatype()) {
    case Bool:
    case Int:
    case Double:
    case Date:
    case GeoPoint:
      // For literals without language tag, we return an empty string per
      // standard.
      return {""};
    case Undefined:
    case EncodedVal:
    case VocabIndex:
    case LocalVocabIndex:
    case TextRecordIndex:
    case WordVocabIndex:
    case BlankNodeIndex:
      return getValue<std::optional<std::string>>(id, context, *this);
  }
  AD_FAIL();
}

//______________________________________________________________________________
sparqlExpression::IdOrLiteralOrIri IriOrUriValueGetter::operator()(
    const LiteralOrIri& litOrIri,
    [[maybe_unused]] const EvaluationContext* context) const {
  return LiteralOrIri{litOrIri.isIri()
                          ? litOrIri.getIri()
                          : Iri::fromIrirefWithoutBrackets(asStringViewUnsafe(
                                litOrIri.getLiteral().getContent()))};
}

//______________________________________________________________________________
template <typename RequestedInfo>
requires ad_utility::RequestedInfoT<RequestedInfo>
std::optional<ad_utility::GeometryInfo>
GeometryInfoValueGetter<RequestedInfo>::getPrecomputedGeometryInfo(
    ValueId id, const EvaluationContext* context) {
  auto datatype = id.getDatatype();
  if (datatype == Datatype::VocabIndex) {
    // All geometry strings encountered during index build have a precomputed
    // geometry info object.
    return context->_qec.getIndex().getVocab().getGeoInfo(id.getVocabIndex());
  }
  return std::nullopt;
}

//______________________________________________________________________________
template <typename RequestedInfo>
requires ad_utility::RequestedInfoT<RequestedInfo>
std::optional<RequestedInfo> GeometryInfoValueGetter<RequestedInfo>::operator()(
    ValueId id, const EvaluationContext* context) const {
  using enum Datatype;
  switch (id.getDatatype()) {
    case EncodedVal:
    case LocalVocabIndex:
    case VocabIndex: {
      auto precomputed = getPrecomputedGeometryInfo(id, context);
      if (precomputed.has_value()) {
        return precomputed.value().getRequestedInfo<RequestedInfo>();
      } else {
        // No precomputed geometry info available: we have to fetch and parse
        // the string.
        auto lit = ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
            context->_qec.getIndex(), id, context->_localVocab);
        return GeometryInfoValueGetter{}(lit, context);
      }
    }
    case GeoPoint:
      return ad_utility::GeometryInfo::fromGeoPoint(id.getGeoPoint())
          .getRequestedInfo<RequestedInfo>();
    case TextRecordIndex:
    case WordVocabIndex:
    case BlankNodeIndex:
    case Bool:
    case Int:
    case Double:
    case Date:
    case Undefined:
      return std::nullopt;
  }
  AD_FAIL();
};

//______________________________________________________________________________
template <typename RequestedInfo>
requires ad_utility::RequestedInfoT<RequestedInfo>
std::optional<RequestedInfo> GeometryInfoValueGetter<RequestedInfo>::operator()(
    const LiteralOrIri& litOrIri,
    [[maybe_unused]] const EvaluationContext* context) const {
  // If we receive only a literal, we have no choice but to parse it and compute
  // the geometry info ad hoc.
  if (litOrIri.isLiteral() && litOrIri.hasDatatype() &&
      asStringViewUnsafe(litOrIri.getDatatype()) == GEO_WKT_LITERAL) {
    auto wktLiteral = litOrIri.getLiteral().toStringRepresentation();
    return ad_utility::GeometryInfo::getRequestedInfo<RequestedInfo>(
        wktLiteral);
  }
  return std::nullopt;
};

// Explicit instantiations
namespace sparqlExpression::detail {
template struct GeometryInfoValueGetter<ad_utility::GeometryInfo>;
template struct GeometryInfoValueGetter<ad_utility::GeometryType>;
template struct GeometryInfoValueGetter<ad_utility::Centroid>;
template struct GeometryInfoValueGetter<ad_utility::BoundingBox>;
}  // namespace sparqlExpression::detail
