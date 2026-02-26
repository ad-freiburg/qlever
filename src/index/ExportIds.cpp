// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "index/ExportIds.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "index/EncodedIriManager.h"
#include "index/Index.h"
#include "index/IndexImpl.h"
#include "util/ValueIdentity.h"

namespace {
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using Literal = ad_utility::triple_component::Literal;

// _____________________________________________________________________________
LiteralOrIri encodedIdToLiteralOrIri(Id id, const IndexImpl& index) {
  const auto& mgr = index.encodedIriManager();
  return LiteralOrIri::fromStringRepresentation(mgr.toString(id));
}
}  // namespace

// _____________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
ExportIds::idToStringAndTypeForEncodedValue(Id id) {
  using enum Datatype;
  switch (id.getDatatype()) {
    case Undefined:
      return std::nullopt;
    case Double:
      // We use the immediately invoked lambda here because putting this block
      // in braces confuses the test coverage tool.
      return [id] {
        double d = id.getDouble();
        if (!std::isfinite(d)) {
          // NOTE: We used `std::stringstream` before which is bad for two
          // reasons. First, it would output "nan" or "inf" in lowercase, which
          // is not legal RDF syntax. Second, creating a `std::stringstream`
          // object is unnecessarily expensive.
          std::string literal = [d]() {
            if (std::isnan(d)) {
              return "NaN";
            }
            AD_CORRECTNESS_CHECK(std::isinf(d));
            return d > 0 ? "INF" : "-INF";
          }();
          return std::pair{std::move(literal), XSD_DOUBLE_TYPE};
        }
        double dIntPart;
        // If the fractional part is zero, write number with one decimal place
        // to make it distinct from integers. Otherwise, use `%.13g`, which
        // uses fixed-size or exponential notation, whichever is more compact.
        std::string out;
        if (std::modf(d, &dIntPart) == 0.0) {
          out = absl::StrFormat("%.1f", d);
        } else {
          out = absl::StrFormat("%.13g", d);
          // For some values `modf` evaluates to zero, but rounding still leads
          // to a value without a trailing '.0'.
          if (out.find_last_of(".e") == std::string::npos) {
            out += ".0";
          }
        }
        return std::pair{std::move(out), XSD_DECIMAL_TYPE};
      }();
    case Bool:
      return std::pair{std::string{id.getBoolLiteral()}, XSD_BOOLEAN_TYPE};
    case Int:
      return std::pair{std::to_string(id.getInt()), XSD_INT_TYPE};
    case Date:
      return id.getDate().toStringAndType();
    case GeoPoint:
      return id.getGeoPoint().toStringAndType();
    case BlankNodeIndex:
      return std::pair{absl::StrCat("_:bn", id.getBlankNodeIndex().get()),
                       nullptr};
      // TODO<joka921> This is only to make the strange `toRdfLiteral` function
      // work in the triple component class, which is only used to create cache
      // keys etc. Consider removing it in the future.
    case EncodedVal:
      return std::pair{absl::StrCat("encodedId: ", id.getBits()), nullptr};
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
ExportIds::idToLiteralForEncodedValue(Id id,
                                      bool onlyReturnLiteralsWithXsdString) {
  if (onlyReturnLiteralsWithXsdString) {
    return std::nullopt;
  }
  auto optionalStringAndType = idToStringAndTypeForEncodedValue(id);
  if (!optionalStringAndType) {
    return std::nullopt;
  }

  return Literal::literalWithoutQuotes(optionalStringAndType->first);
}

// _____________________________________________________________________________
bool ExportIds::isPlainLiteralOrLiteralWithXsdString(const LiteralOrIri& word) {
  AD_CORRECTNESS_CHECK(word.isLiteral());
  return !word.hasDatatype() ||
         asStringViewUnsafe(word.getDatatype()) == XSD_STRING;
}

// _____________________________________________________________________________
std::string ExportIds::replaceAnglesByQuotes(std::string iriString) {
  AD_CORRECTNESS_CHECK(ql::starts_with(iriString, '<'));
  AD_CORRECTNESS_CHECK(ql::ends_with(iriString, '>'));
  iriString[0] = '"';
  iriString[iriString.size() - 1] = '"';
  return iriString;
}

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
ExportIds::handleIriOrLiteral(LiteralOrIri word,
                              bool onlyReturnLiteralsWithXsdString) {
  if (word.isIri()) {
    if (onlyReturnLiteralsWithXsdString) {
      return std::nullopt;
    }
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        replaceAnglesByQuotes(
            std::move(word.getIri()).toStringRepresentation()));
  }
  AD_CORRECTNESS_CHECK(word.isLiteral());
  if (onlyReturnLiteralsWithXsdString) {
    if (isPlainLiteralOrLiteralWithXsdString(word)) {
      if (word.hasDatatype()) {
        word.getLiteral().removeDatatypeOrLanguageTag();
      }
      return std::move(word.getLiteral());
    }
    return std::nullopt;
  }
  // Note: `removeDatatypeOrLanguageTag` also correctly works if the literal
  // has neither a datatype nor a language tag, hence we don't need an `if`
  // here.
  word.getLiteral().removeDatatypeOrLanguageTag();
  return std::move(word.getLiteral());
}

// _____________________________________________________________________________
LiteralOrIri ExportIds::getLiteralOrIriFromVocabIndex(
    const IndexImpl& index, Id id, const LocalVocab& localVocab) {
  switch (id.getDatatype()) {
    case Datatype::LocalVocabIndex:
      return localVocab.getWord(id.getLocalVocabIndex()).asLiteralOrIri();
    case Datatype::VocabIndex: {
      auto getEntity = [&index, id]() {
        return index.indexToString(id.getVocabIndex());
      };
      // The type of entity might be `string_view` (If the vocabulary is stored
      // uncompressed in RAM) or `string` (if it is on-disk, or compressed or
      // both). The following code works and is efficient in all cases. In
      // particular, the `std::string` constructor is compiled out because of
      // RVO if `getEntity()` already returns a `string`.
      static_assert(ad_utility::SameAsAny<decltype(getEntity()), std::string,
                                          std::string_view>);
      return LiteralOrIri::fromStringRepresentation(std::string(getEntity()));
    }
    case Datatype::EncodedVal:
      return encodedIdToLiteralOrIri(id, index);
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
std::optional<std::string> ExportIds::blankNodeIriToString(
    const ad_utility::triple_component::Iri& iri) {
  const auto& representation = iri.toStringRepresentation();
  if (ql::starts_with(representation, QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX)) {
    std::string_view view = representation;
    view.remove_prefix(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX.size());
    view.remove_suffix(1);
    AD_CORRECTNESS_CHECK(ql::starts_with(view, "_:"));
    return std::string{view};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
template <bool removeQuotesAndAngleBrackets, bool onlyReturnLiterals,
          typename EscapeFunction>
std::optional<std::pair<std::string, const char*>> ExportIds::idToStringAndType(
    const Index& index, Id id, const LocalVocab& localVocab,
    EscapeFunction&& escapeFunction) {
  using enum Datatype;
  auto datatype = id.getDatatype();
  if constexpr (onlyReturnLiterals) {
    if (!(datatype == VocabIndex || datatype == LocalVocabIndex)) {
      return std::nullopt;
    }
  }

  auto handleIriOrLiteralLocal = [&escapeFunction](const LiteralOrIri& word)
      -> std::optional<std::pair<std::string, const char*>> {
    if constexpr (onlyReturnLiterals) {
      if (!word.isLiteral()) {
        return std::nullopt;
      }
    }
    if (word.isIri()) {
      if (auto blankNodeString = blankNodeIriToString(word.getIri())) {
        return std::pair{std::move(blankNodeString.value()), nullptr};
      }
    }
    if constexpr (removeQuotesAndAngleBrackets) {
      // TODO<joka921> Can we get rid of the string copying here?
      return std::pair{
          escapeFunction(std::string{asStringViewUnsafe(word.getContent())}),
          nullptr};
    }
    return std::pair{escapeFunction(word.toStringRepresentation()), nullptr};
  };
  switch (id.getDatatype()) {
    case WordVocabIndex: {
      std::string_view entity = index.indexToString(id.getWordVocabIndex());
      return std::pair{escapeFunction(std::string{entity}), nullptr};
    }
    case VocabIndex:
    case LocalVocabIndex:
      return handleIriOrLiteralLocal(
          getLiteralOrIriFromVocabIndex(index.getImpl(), id, localVocab));
    case EncodedVal:
      return handleIriOrLiteralLocal(
          encodedIdToLiteralOrIri(id, index.getImpl()));
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return idToStringAndTypeForEncodedValue(id);
  }
}

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal> ExportIds::idToLiteral(
    const IndexImpl& index, Id id, const LocalVocab& localVocab,
    bool onlyReturnLiteralsWithXsdString) {
  using enum Datatype;
  auto datatype = id.getDatatype();

  switch (datatype) {
    case WordVocabIndex:
      return getLiteralOrNullopt(getLiteralOrIriFromWordVocabIndex(index, id));
    case EncodedVal:
      return handleIriOrLiteral(encodedIdToLiteralOrIri(id, index),
                                onlyReturnLiteralsWithXsdString);
    case VocabIndex:
    case LocalVocabIndex:
      return handleIriOrLiteral(
          getLiteralOrIriFromVocabIndex(index, id, localVocab),
          onlyReturnLiteralsWithXsdString);
    case TextRecordIndex:
      return getLiteralOrNullopt(getLiteralOrIriFromTextRecordIndex(index, id));
    default:
      return idToLiteralForEncodedValue(id, onlyReturnLiteralsWithXsdString);
  }
}

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
ExportIds::getLiteralOrNullopt(std::optional<LiteralOrIri> litOrIri) {
  if (litOrIri.has_value() && litOrIri.value().isLiteral()) {
    return std::move(litOrIri.value().getLiteral());
  }
  return std::nullopt;
};

// _____________________________________________________________________________
std::optional<LiteralOrIri> ExportIds::idToLiteralOrIriForEncodedValue(Id id) {
  // TODO<RobinTF> This returns a `nullptr` for the datatype when the `id`
  // represents a `BlankNode` or an `EncodedVal`. The latter case is typically
  // no problem, because the only caller of this function already properly
  // handles this case. The former case is also fine, because `BlankNode`s are
  // neither IRIs nor literals, so returning `std::nullopt` is the correct
  // behavior. However, this is somewhat fragile and should be kept in mind if
  // this function is used in other contexts.
  auto [literal, type] = idToStringAndTypeForEncodedValue(id).value_or(
      std::make_pair(std::string{}, nullptr));
  if (type == nullptr) {
    return std::nullopt;
  }
  auto lit =
      ad_utility::triple_component::Literal::literalWithoutQuotes(literal);
  lit.addDatatype(
      ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(type));
  return LiteralOrIri{std::move(lit)};
}

// _____________________________________________________________________________
std::optional<LiteralOrIri> ExportIds::getLiteralOrIriFromWordVocabIndex(
    const IndexImpl& index, Id id) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithoutQuotes(
          index.indexToString(id.getWordVocabIndex()))};
};

// _____________________________________________________________________________
std::optional<LiteralOrIri> ExportIds::getLiteralOrIriFromTextRecordIndex(
    const IndexImpl& index, Id id) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithoutQuotes(
          index.getTextExcerpt(id.getTextRecordIndex()))};
};

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::LiteralOrIri>
ExportIds::idToLiteralOrIri(const IndexImpl& index, Id id,
                            const LocalVocab& localVocab,
                            bool skipEncodedValues) {
  using enum Datatype;
  switch (id.getDatatype()) {
    case WordVocabIndex:
      return getLiteralOrIriFromWordVocabIndex(index, id);
    case VocabIndex:
    case LocalVocabIndex:
    case EncodedVal:
      return getLiteralOrIriFromVocabIndex(index, id, localVocab);
    case TextRecordIndex:
      return getLiteralOrIriFromTextRecordIndex(index, id);
    default:
      if (skipEncodedValues) {
        return std::nullopt;
      }
      return idToLiteralOrIriForEncodedValue(id);
  }
}

// ___________________________________________________________________________
template std::optional<std::pair<std::string, const char*>>
ExportIds::idToStringAndType<true, false, ql::identity>(
    const Index& index, Id id, const LocalVocab& localVocab,
    ql::identity&& escapeFunction);

// ___________________________________________________________________________
template std::optional<std::pair<std::string, const char*>>
ExportIds::idToStringAndType<true, true, ql::identity>(
    const Index& index, Id id, const LocalVocab& localVocab,
    ql::identity&& escapeFunction);

// This explicit instantiation is necessary because the `Variable` class
// currently still uses it.
// TODO<joka921> Refactor the CONSTRUCT export, then this is no longer
// needed.
template std::optional<std::pair<std::string, const char*>>
ExportIds::idToStringAndType(const Index& index, Id id,
                             const LocalVocab& localVocab,
                             ql::identity&& escapeFunction);

// Explicit instantiations for the escape functions used in CSV/TSV export
// (defined in RdfEscaping.h). The escape function type is deduced as a
// reference-to-function.
template std::optional<std::pair<std::string, const char*>>
ExportIds::idToStringAndType<false, false, std::string (&)(std::string)>(
    const Index& index, Id id, const LocalVocab& localVocab,
    std::string (&escapeFunction)(std::string));

template std::optional<std::pair<std::string, const char*>>
ExportIds::idToStringAndType<true, false, std::string (&)(std::string)>(
    const Index& index, Id id, const LocalVocab& localVocab,
    std::string (&escapeFunction)(std::string));
