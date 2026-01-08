#include "ConstructQueryEvaluator.h"

std::optional<std::string> ConstructQueryEvaluator::evaluate(const Iri& iri) {
    return iri.toStringRepresentation();
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Literal& literal,
    PositionInTriple role) {
  if (role == PositionInTriple::OBJECT) {
    return literal.toStringRepresentation();
  }
  return std::nullopt;
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Variable& var,
    const ConstructQueryExportContext& context) {

  size_t resultTableRow = context._resultTableRow;
  const auto& variableColumns = context._variableColumns;
  const Index& qecIndex = context._qecIndex;
  const IdTable& idTable = context.idTable_;

  if (variableColumns.contains(var)) {

    size_t index = variableColumns.at(var).columnIndex_;
    auto id = idTable(resultTableRow, index);
    auto optionalStringAndType = ConstructQueryEvaluator::idToStringAndType(
        qecIndex, id, context.localVocab_);

    if (!optionalStringAndType.has_value()) {
      return std::nullopt;
    }

    auto& [literal, type] = optionalStringAndType.value();
    const char* i = XSD_INT_TYPE;
    const char* d = XSD_DECIMAL_TYPE;
    const char* b = XSD_BOOLEAN_TYPE;

    // Note: If `type` is `XSD_DOUBLE_TYPE`, `literal` is always "NaN", "INF" or
    // "-INF", which doesn't have a short form notation.
    if (type == nullptr || type == i || type == d ||
        (type == b && literal.length() > 1)) {
      return std::move(literal);
    } else {
      return absl::StrCat("\"", literal, "\"^^<", type, ">");
    }
  }
  return std::nullopt;
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const BlankNode& node,
    const ConstructQueryExportContext& context
)  {
  std::ostringstream stream;
  stream << "_:";
  stream << (node.isGenerated() ? 'g' : 'u'); // generated or user-defined
  stream << context._rowOffset + context._resultTableRow << '_';
  stream << node.label();
  return stream.str();
}

// HELPERS _____________________________________________________________________
template <bool removeQuotesAndAngleBrackets, bool onlyReturnLiterals,
    typename EscapeFunction>
std::optional<std::pair<std::string, const char*>>
ConstructQueryEvaluator::idToStringAndType(const Index& index, Id id,
                                             const LocalVocab& localVocab,
                                             EscapeFunction&& escapeFunction) {
  using enum Datatype;
  auto datatype = id.getDatatype();
  if constexpr (onlyReturnLiterals) {
    if (!(datatype == VocabIndex || datatype == LocalVocabIndex)) {
      return std::nullopt;
    }
  }

  auto handleIriOrLiteral = [&escapeFunction](const LocalVocabEntry::LiteralOrIri& word)
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
      // TODO<ms2144> take a look  at this.
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
      return handleIriOrLiteral(
          getLiteralOrIriFromVocabIndex(index.getImpl(), id, localVocab));
    case EncodedVal:
      return handleIriOrLiteral(encodedIdToLiteralOrIri(id, index.getImpl()));
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return ConstructQueryEvaluator::idToStringAndTypeForEncodedValue(id);
  }
}

// _____________________________________________________________________________
std::optional<std::string> ConstructQueryEvaluator::blankNodeIriToString(
    const ad_utility::triple_component::Iri& iri) {

  const boost_swap_impl::string& representation = iri.toStringRepresentation();

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
LocalVocabEntry::LiteralOrIri
ConstructQueryEvaluator::getLiteralOrIriFromVocabIndex(
    const IndexImpl& index,
    Id id,
    const LocalVocab& localVocab) {

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
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
LiteralOrIri
ConstructQueryEvaluator::encodedIdToLiteralOrIri(
    Id id,
    const IndexImpl& index) {

  const auto& mgr = index.encodedIriManager();

  return LiteralOrIri::fromStringRepresentation(mgr.toString(id));
}

// _____________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
ConstructQueryEvaluator::idToStringAndTypeForEncodedValue(Id id) {
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
        // to make it distinct from integers. Otherwise, use `%.13g`, which uses
        // fixed-size or exponential notation, whichever is more compact.
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
      // TOODO<ms2144> take a look at the comment above.
    case EncodedVal:
      return std::pair{absl::StrCat("encodedId: ", id.getBits()), nullptr};
    default:
      AD_FAIL();
  }
}
