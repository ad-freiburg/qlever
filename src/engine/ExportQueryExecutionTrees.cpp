// Copyright 2022 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Robin Textor-Falconi <textorr@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "engine/ExportQueryExecutionTrees.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>

#include <ranges>

#include "backports/StartsWith.h"
#include "backports/algorithm.h"
#include "engine/BinaryExport.h"
#include "index/EncodedIriManager.h"
#include "index/IndexImpl.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/ConstexprUtils.h"
#include "util/ValueIdentity.h"
#include "util/http/MediaTypes.h"
#include "util/json.h"

namespace {
using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

// Return true iff the `result` is nonempty.
bool getResultForAsk(const std::shared_ptr<const Result>& result) {
  if (result->isFullyMaterialized()) {
    return !result->idTable().empty();
  } else {
    return ql::ranges::any_of(result->idTables(), [](const auto& pair) {
      return !pair.idTable_.empty();
    });
  }
}

LiteralOrIri encodedIdToLiteralOrIri(Id id, const Index& index) {
  const auto& mgr = index.getImpl().encodedIriManager();
  return LiteralOrIri::fromStringRepresentation(mgr.toString(id));
}

// _____________________________________________________________________________
STREAMABLE_GENERATOR_TYPE computeResultForAsk(
    [[maybe_unused]] const ParsedQuery& parsedQuery,
    const QueryExecutionTree& qet, ad_utility::MediaType mediaType,
    [[maybe_unused]] const ad_utility::Timer& requestTimer,
    STREAMABLE_YIELDER_ARG_DECL) {
  // Compute the result of the ASK query.
  bool result = getResultForAsk(qet.getResult(true));

  // Lambda that returns the result bool in XML format.
  auto getXmlResult = [result]() {
    std::string xmlTemplate = R"(<?xml version="1.0"?>
<sparql xmlns="http://www.w3.org/2005/sparql-results#">
  <head/>
  <boolean>true</boolean>
</sparql>)";

    if (result) {
      return xmlTemplate;
    } else {
      return absl::StrReplaceAll(xmlTemplate, {{"true", "false"}});
    }
  };

  // Lambda that returns the result bool in SPARQL JSON format.
  auto getSparqlJsonResult = [result]() {
    nlohmann::json j;
    j["head"] = nlohmann::json::object_t{};
    j["boolean"] = result;
    return j.dump();
  };

  // Return the result in the requested format.
  using enum ad_utility::MediaType;
  switch (mediaType) {
    case sparqlXml:
      STREAMABLE_YIELD(getXmlResult());
      break;
    case sparqlJson:
      STREAMABLE_YIELD(getSparqlJsonResult());
      break;
    default:
      throw std::runtime_error{
          "ASK queries are not supported for TSV or CSV or binary format."};
  }
}
}  // namespace

// __________________________________________________________________________
cppcoro::generator<ExportQueryExecutionTrees::TableConstRefWithVocab>
ExportQueryExecutionTrees::getIdTables(const Result& result) {
  if (result.isFullyMaterialized()) {
    TableConstRefWithVocab pair{result.idTable(), result.localVocab()};
    co_yield pair;
  } else {
    for (const Result::IdTableVocabPair& pair : result.idTables()) {
      TableConstRefWithVocab tableWithVocab{pair.idTable_, pair.localVocab_};
      co_yield tableWithVocab;
    }
  }
}

// _____________________________________________________________________________
cppcoro::generator<ExportQueryExecutionTrees::TableWithRange>
ExportQueryExecutionTrees::getRowIndices(LimitOffsetClause limitOffset,
                                         const Result& result,
                                         uint64_t& resultSize) {
  // The first call initializes the `resultSize` to zero (no need to
  // initialize it outside of the function).
  resultSize = 0;

  // If the LIMIT is zero, there are no blocks to yield and the total result
  // size is zero.
  if (limitOffset._limit.value_or(1) == 0) {
    co_return;
  }

  // The effective offset, limit, and export limit. These will be updated after
  // each block, see `updateEffectiveOffsetAndLimits` below. If they were not
  // specified, they are initialized to their default values (0 for the offset
  // and `std::numeric_limits<uint64_t>::max()` for the two limits).
  uint64_t effectiveOffset = limitOffset._offset;
  uint64_t effectiveLimit = limitOffset.limitOrDefault();
  uint64_t effectiveExportLimit = limitOffset.exportLimitOrDefault();

  // Make sure that the export limit is at most the limit (increasing the
  // export limit beyond the limit has no effect).
  effectiveExportLimit = std::min(effectiveExportLimit, effectiveLimit);

  // Iterate over the result in blocks.
  for (TableConstRefWithVocab& tableWithVocab : getIdTables(result)) {
    // If all rows in the current block are before the effective offset, we can
    // skip the block entirely. If not, there is at least something to count
    // and maybe also something to yield.
    uint64_t currentBlockSize = tableWithVocab.idTable_.numRows();
    if (effectiveOffset >= currentBlockSize) {
      effectiveOffset -= currentBlockSize;
      continue;
    }
    AD_CORRECTNESS_CHECK(effectiveOffset < currentBlockSize);
    AD_CORRECTNESS_CHECK(effectiveLimit > 0);

    // Compute the range of rows to be exported (can by zero) and to be counted
    // (always non-zero at this point).
    uint64_t rangeBegin = effectiveOffset;
    uint64_t numRowsToBeExported =
        std::min(effectiveExportLimit, currentBlockSize - rangeBegin);
    uint64_t numRowsToBeCounted =
        std::min(effectiveLimit, currentBlockSize - rangeBegin);
    AD_CORRECTNESS_CHECK(rangeBegin + numRowsToBeExported <= currentBlockSize);
    AD_CORRECTNESS_CHECK(rangeBegin + numRowsToBeCounted <= currentBlockSize);
    AD_CORRECTNESS_CHECK(numRowsToBeCounted > 0);

    // If there is something to be exported, yield it.
    if (numRowsToBeExported > 0) {
      co_yield {std::move(tableWithVocab),
                ql::views::iota(rangeBegin, rangeBegin + numRowsToBeExported)};
    }

    // Add to `resultSize` and update the effective offset (which becomes zero
    // after the first non-skipped block) and limits (make sure to never go
    // below zero and `std::numeric_limits<uint64_t>::max()` stays there).
    resultSize += numRowsToBeCounted;
    effectiveOffset = 0;
    auto reduceLimit = [&](uint64_t& limit, uint64_t subtrahend) {
      if (limit != std::numeric_limits<uint64_t>::max()) {
        limit = limit > subtrahend ? limit - subtrahend : 0;
      }
    };
    reduceLimit(effectiveLimit, numRowsToBeCounted);
    reduceLimit(effectiveExportLimit, numRowsToBeCounted);

    // If the effective limit is zero, there is nothing to yield and nothing
    // to count anymore. This should come at the end of this loop and not at
    // the beginning, to avoid unnecessarily fetching another block from
    // `result`.
    if (effectiveLimit == 0) {
      co_return;
    }
  }
}

// _____________________________________________________________________________
cppcoro::generator<QueryExecutionTree::StringTriple>
ExportQueryExecutionTrees::constructQueryResultToTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    uint64_t& resultSize, CancellationHandle cancellationHandle) {
  size_t rowOffset = 0;
  for (const auto& [pair, range] :
       getRowIndices(limitAndOffset, *result, resultSize)) {
    auto& idTable = pair.idTable_;
    for (uint64_t i : range) {
      ConstructQueryExportContext context{i,
                                          idTable,
                                          pair.localVocab_,
                                          qet.getVariableColumns(),
                                          qet.getQec()->getIndex(),
                                          rowOffset};
      using enum PositionInTriple;
      for (const auto& triple : constructTriples) {
        auto subject = triple[0].evaluate(context, SUBJECT);
        auto predicate = triple[1].evaluate(context, PREDICATE);
        auto object = triple[2].evaluate(context, OBJECT);
        if (!subject.has_value() || !predicate.has_value() ||
            !object.has_value()) {
          continue;
        }
        co_yield {std::move(subject.value()), std::move(predicate.value()),
                  std::move(object.value())};
        cancellationHandle->throwIfCancelled();
      }
    }
    rowOffset += idTable.size();
  }
  // For each result from the WHERE clause, we produce up to
  // `constructTriples.size()` triples. We do not account for triples that are
  // filtered out because one of the components is UNDEF (it would require
  // materializing the whole result).
  resultSize *= constructTriples.size();
}

// _____________________________________________________________________________
template <>
STREAMABLE_GENERATOR_TYPE ExportQueryExecutionTrees::
    constructQueryResultToStream<ad_utility::MediaType::turtle>(
        const QueryExecutionTree& qet,
        const ad_utility::sparql_types::Triples& constructTriples,
        LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
        CancellationHandle cancellationHandle,
        [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  result->logResultSize();
  [[maybe_unused]] uint64_t resultSize = 0;
  auto generator = constructQueryResultToTriples(
      qet, constructTriples, limitAndOffset, result, resultSize,
      std::move(cancellationHandle));
  for (const auto& triple : generator) {
    STREAMABLE_YIELD(triple.subject_);
    STREAMABLE_YIELD(' ');
    STREAMABLE_YIELD(triple.predicate_);
    STREAMABLE_YIELD(' ');
    // NOTE: It's tempting to STREAMABLE_YIELD an expression using a ternary
    // operator: STREAMABLE_YIELD triple._object.starts_with('"')
    //     ? RdfEscaping::validRDFLiteralFromNormalized(triple._object)
    //     : triple._object;
    // but this leads to 1. segfaults in GCC (probably a compiler bug) and 2.
    // to unnecessary copies of `triple._object` in the `else` case because
    // the ternary always has to create a new prvalue.
    if (ql::starts_with(triple.object_, '"')) {
      std::string objectAsValidRdfLiteral =
          RdfEscaping::validRDFLiteralFromNormalized(triple.object_);
      STREAMABLE_YIELD(objectAsValidRdfLiteral);
    } else {
      STREAMABLE_YIELD(triple.object_);
    }
    STREAMABLE_YIELD(" .\n");
  }
}

// _____________________________________________________________________________
cppcoro::generator<std::string>
ExportQueryExecutionTrees::constructQueryResultBindingsToQLeverJSON(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
  auto generator = constructQueryResultToTriples(
      qet, constructTriples, limitAndOffset, std::move(result), resultSize,
      std::move(cancellationHandle));
  for (auto& triple : generator) {
    auto binding = nlohmann::json::array({std::move(triple.subject_),
                                          std::move(triple.predicate_),
                                          std::move(triple.object_)});
    co_yield binding.dump();
  }
}

// _____________________________________________________________________________
// Create the row indicated by rowIndex from IdTable in QLeverJSON format.
nlohmann::json idTableToQLeverJSONRow(
    const QueryExecutionTree& qet,
    const QueryExecutionTree::ColumnIndicesAndTypes& columns,
    const LocalVocab& localVocab, const size_t rowIndex, const IdTable& data) {
  // We need the explicit `array` constructor for the special case of zero
  // variables.
  auto row = nlohmann::json::array();
  for (const auto& opt : columns) {
    if (!opt) {
      row.emplace_back(nullptr);
      continue;
    }
    const auto& currentId = data(rowIndex, opt->columnIndex_);
    const auto& optionalStringAndXsdType =
        ExportQueryExecutionTrees::idToStringAndType(qet.getQec()->getIndex(),
                                                     currentId, localVocab);
    if (!optionalStringAndXsdType.has_value()) {
      row.emplace_back(nullptr);
      continue;
    }
    const auto& [stringValue, xsdType] = optionalStringAndXsdType.value();
    if (xsdType) {
      row.emplace_back('"' + stringValue + "\"^^<" + xsdType + '>');
    } else {
      row.emplace_back(stringValue);
    }
  }
  return row;
}

// _____________________________________________________________________________
cppcoro::generator<std::string>
ExportQueryExecutionTrees::idTableToQLeverJSONBindings(
    const QueryExecutionTree& qet, LimitOffsetClause limitAndOffset,
    const QueryExecutionTree::ColumnIndicesAndTypes columns,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
  AD_CORRECTNESS_CHECK(result != nullptr);
  for (const auto& [pair, range] :
       getRowIndices(limitAndOffset, *result, resultSize)) {
    for (uint64_t rowIndex : range) {
      co_yield idTableToQLeverJSONRow(qet, columns, pair.localVocab_, rowIndex,
                                      pair.idTable_)
          .dump();
      cancellationHandle->throwIfCancelled();
    }
  }
}

// _____________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndTypeForEncodedValue(Id id) {
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
        // to make it distinct from integers. Otherwise, use `%g`, which uses
        // fixed-size or exponential notation, whichever is more compact.
        std::string out = std::modf(d, &dIntPart) == 0.0
                              ? absl::StrFormat("%.1f", d)
                              : absl::StrFormat("%g", d);
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
ExportQueryExecutionTrees::idToLiteralForEncodedValue(
    Id id, bool onlyReturnLiteralsWithXsdString) {
  if (onlyReturnLiteralsWithXsdString) {
    return std::nullopt;
  }
  auto optionalStringAndType = idToStringAndTypeForEncodedValue(id);
  if (!optionalStringAndType) {
    return std::nullopt;
  }

  return ad_utility::triple_component::Literal::literalWithoutQuotes(
      optionalStringAndType->first);
}

// _____________________________________________________________________________
bool ExportQueryExecutionTrees::isPlainLiteralOrLiteralWithXsdString(
    const LiteralOrIri& word) {
  AD_CORRECTNESS_CHECK(word.isLiteral());
  return !word.hasDatatype() ||
         asStringViewUnsafe(word.getDatatype()) == XSD_STRING;
}

// _____________________________________________________________________________
std::string ExportQueryExecutionTrees::replaceAnglesByQuotes(
    std::string iriString) {
  AD_CORRECTNESS_CHECK(ql::starts_with(iriString, '<'));
  AD_CORRECTNESS_CHECK(iriString.ends_with('>'));
  iriString[0] = '"';
  iriString[iriString.size() - 1] = '"';
  return iriString;
}

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
ExportQueryExecutionTrees::handleIriOrLiteral(
    LiteralOrIri word, bool onlyReturnLiteralsWithXsdString) {
  if (word.isIri()) {
    if (onlyReturnLiteralsWithXsdString) {
      return std::nullopt;
    }
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        replaceAnglesByQuotes(
            std::move(word.getIri().toStringRepresentation())));
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
  // Note: `removeDatatypeOrLanguageTag` also correctly works if the literal has
  // neither a datatype nor a language tag, hence we don't need an `if` here.
  word.getLiteral().removeDatatypeOrLanguageTag();
  return std::move(word.getLiteral());
}

// _____________________________________________________________________________
LiteralOrIri ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
    const Index& index, Id id, const LocalVocab& localVocab) {
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
std::optional<std::string> ExportQueryExecutionTrees::blankNodeIriToString(
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
std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndType(const Index& index, Id id,
                                             const LocalVocab& localVocab,
                                             EscapeFunction&& escapeFunction) {
  using enum Datatype;
  auto datatype = id.getDatatype();
  if constexpr (onlyReturnLiterals) {
    if (!(datatype == VocabIndex || datatype == LocalVocabIndex)) {
      return std::nullopt;
    }
  }

  auto handleIriOrLiteral = [&escapeFunction](const LiteralOrIri& word)
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
      return handleIriOrLiteral(
          getLiteralOrIriFromVocabIndex(index, id, localVocab));
    case EncodedVal:
      return handleIriOrLiteral(encodedIdToLiteralOrIri(id, index));
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return idToStringAndTypeForEncodedValue(id);
  }
}

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::Literal>
ExportQueryExecutionTrees::idToLiteral(const Index& index, Id id,
                                       const LocalVocab& localVocab,
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
ExportQueryExecutionTrees::getLiteralOrNullopt(
    std::optional<LiteralOrIri> litOrIri) {
  if (litOrIri.has_value() && litOrIri.value().isLiteral()) {
    return std::move(litOrIri.value().getLiteral());
  }
  return std::nullopt;
};

// _____________________________________________________________________________
std::optional<LiteralOrIri>
ExportQueryExecutionTrees::idToLiteralOrIriForEncodedValue(Id id) {
  auto idLiteralAndType = idToStringAndTypeForEncodedValue(id);
  if (idLiteralAndType.has_value()) {
    auto lit = ad_utility::triple_component::Literal::literalWithoutQuotes(
        idLiteralAndType.value().first);
    lit.addDatatype(
        ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(
            idLiteralAndType.value().second));
    return LiteralOrIri{lit};
  }
  return std::nullopt;
};

// _____________________________________________________________________________
std::optional<LiteralOrIri>
ExportQueryExecutionTrees::getLiteralOrIriFromWordVocabIndex(const Index& index,
                                                             Id id) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithoutQuotes(
          index.indexToString(id.getWordVocabIndex()))};
};

// _____________________________________________________________________________
std::optional<LiteralOrIri>
ExportQueryExecutionTrees::getLiteralOrIriFromTextRecordIndex(
    const Index& index, Id id) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithoutQuotes(
          index.getTextExcerpt(id.getTextRecordIndex()))};
};

// _____________________________________________________________________________
std::optional<ad_utility::triple_component::LiteralOrIri>
ExportQueryExecutionTrees::idToLiteralOrIri(const Index& index, Id id,
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
ExportQueryExecutionTrees::idToStringAndType<true, false, std::identity>(
    const Index& index, Id id, const LocalVocab& localVocab,
    std::identity&& escapeFunction);

// ___________________________________________________________________________
template std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndType<true, true, std::identity>(
    const Index& index, Id id, const LocalVocab& localVocab,
    std::identity&& escapeFunction);

// This explicit instantiation is necessary because the `Variable` class
// currently still uses it.
// TODO<joka921> Refactor the CONSTRUCT export, then this is no longer
// needed
template std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndType(const Index& index, Id id,
                                             const LocalVocab& localVocab,
                                             std::identity&& escapeFunction);

// Convert a stringvalue and optional type to JSON binding.
static nlohmann::json stringAndTypeToBinding(std::string_view entitystr,
                                             const char* xsdType) {
  nlohmann::ordered_json b;
  if (xsdType) {
    b["value"] = entitystr;
    b["type"] = "literal";
    b["datatype"] = xsdType;
    return b;
  }

  // The string is an IRI or literal.
  if (ql::starts_with(entitystr, '<')) {
    // Strip the <> surrounding the iri.
    b["value"] = entitystr.substr(1, entitystr.size() - 2);
    // Even if they are technically IRIs, the format needs the type to be
    // "uri".
    b["type"] = "uri";
  } else if (ql::starts_with(entitystr, "_:")) {
    b["value"] = entitystr.substr(2);
    b["type"] = "bnode";
  } else {
    // TODO<joka921> This is probably not quite correct in the corner case
    // that there are datatype IRIs which contain quotes.
    size_t quotePos = entitystr.rfind('"');
    if (quotePos == std::string::npos) {
      // TEXT entries are currently not surrounded by quotes
      b["value"] = entitystr;
      b["type"] = "literal";
    } else {
      b["value"] = entitystr.substr(1, quotePos - 1);
      b["type"] = "literal";
      // Look for a language tag or type.
      if (quotePos < entitystr.size() - 1 && entitystr[quotePos + 1] == '@') {
        b["xml:lang"] = entitystr.substr(quotePos + 2);
      } else if (quotePos < entitystr.size() - 2 &&
                 // TODO<joka921> This can be a `AD_CONTRACT_CHECK` once the
                 // fulltext index vocabulary is stored in a consistent format.
                 entitystr[quotePos + 1] == '^') {
        AD_CONTRACT_CHECK(entitystr[quotePos + 2] == '^');
        std::string_view datatype{entitystr};
        // remove the <angledBrackets> around the datatype IRI
        AD_CONTRACT_CHECK(datatype.size() >= quotePos + 5);
        datatype.remove_prefix(quotePos + 4);
        datatype.remove_suffix(1);
        b["datatype"] = datatype;
      }
    }
  }
  return b;
}

// _____________________________________________________________________________
cppcoro::generator<std::string> askQueryResultToQLeverJSON(
    std::shared_ptr<const Result> result) {
  AD_CORRECTNESS_CHECK(result != nullptr);
  std::string_view value = getResultForAsk(result) ? "true" : "false";
  std::string resultLit =
      absl::StrCat("\"", value, "\"^^<", XSD_BOOLEAN_TYPE, ">");
  nlohmann::json resultJson = std::vector{std::move(resultLit)};
  co_yield resultJson.dump();
}

// _____________________________________________________________________________
cppcoro::generator<std::string>
ExportQueryExecutionTrees::selectQueryResultBindingsToQLeverJSON(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
  AD_CORRECTNESS_CHECK(result != nullptr);
  AD_LOG_DEBUG << "Resolving strings for finished binary result...\n";
  QueryExecutionTree::ColumnIndicesAndTypes selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, true);

  return idTableToQLeverJSONBindings(qet, limitAndOffset, selectedColumnIndices,
                                     std::move(result), resultSize,
                                     std::move(cancellationHandle));
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
STREAMABLE_GENERATOR_TYPE ExportQueryExecutionTrees::selectQueryResultToStream(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  static_assert(format == MediaType::octetStream || format == MediaType::csv ||
                format == MediaType::tsv || format == MediaType::turtle ||
                format == MediaType::qleverJson);

  // TODO<joka921> Use a proper error message, or check that we get a more
  // reasonable error from upstream.
  AD_CONTRACT_CHECK(format != MediaType::turtle);
  AD_CONTRACT_CHECK(format != MediaType::qleverJson);

  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();
  AD_LOG_DEBUG << "Converting result IDs to their corresponding strings ..."
               << std::endl;
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, true);

  // special case : binary export of IdTable
  if constexpr (format == MediaType::octetStream) {
    std::erase(selectedColumnIndices, std::nullopt);
    uint64_t resultSize = 0;
    for (const auto& [pair, range] :
         getRowIndices(limitAndOffset, *result, resultSize)) {
      for (uint64_t i : range) {
        for (const auto& columnIndex : selectedColumnIndices) {
          if (columnIndex.has_value()) {
            STREAMABLE_YIELD(
                std::string_view{reinterpret_cast<const char*>(&pair.idTable_(
                                     i, columnIndex.value().columnIndex_)),
                                 sizeof(Id)});
          }
        }
        cancellationHandle->throwIfCancelled();
      }
    }
    STREAMABLE_RETURN;
  }

  static constexpr char separator = format == MediaType::tsv ? '\t' : ',';
  // Print header line
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  // In the CSV format, the variables don't include the question mark.
  if (format == MediaType::csv) {
    ql::ranges::for_each(variables,
                         [](std::string& var) { var = var.substr(1); });
  }
  STREAMABLE_YIELD(absl::StrJoin(variables, std::string_view{&separator, 1}));
  STREAMABLE_YIELD('\n');

  constexpr auto& escapeFunction = format == MediaType::tsv
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;
  uint64_t resultSize = 0;
  for (const auto& [pair, range] :
       getRowIndices(limitAndOffset, *result, resultSize)) {
    for (uint64_t i : range) {
      for (size_t j = 0; j < selectedColumnIndices.size(); ++j) {
        if (selectedColumnIndices[j].has_value()) {
          const auto& val = selectedColumnIndices[j].value();
          Id id = pair.idTable_(i, val.columnIndex_);
          auto optionalStringAndType =
              idToStringAndType<format == MediaType::csv>(
                  qet.getQec()->getIndex(), id, pair.localVocab_,
                  escapeFunction);
          if (optionalStringAndType.has_value()) [[likely]] {
            STREAMABLE_YIELD(optionalStringAndType.value().first);
          }
        }
        if (j + 1 < selectedColumnIndices.size()) {
          STREAMABLE_YIELD(separator);
        }
      }
      STREAMABLE_YIELD('\n');
      cancellationHandle->throwIfCancelled();
    }
  }
  AD_LOG_DEBUG << "Done creating readable result.\n";
}

// Convert a single ID to an XML binding of the given `variable`.
template <typename IndexType, typename LocalVocabType>
static std::string idToXMLBinding(std::string_view variable, Id id,
                                  const IndexType& index,
                                  const LocalVocabType& localVocab) {
  using namespace std::string_view_literals;
  using namespace std::string_literals;
  const auto& optionalValue =
      ExportQueryExecutionTrees::idToStringAndType(index, id, localVocab);
  if (!optionalValue.has_value()) {
    return ""s;
  }
  const auto& [stringValue, xsdType] = optionalValue.value();
  std::string result = absl::StrCat("\n    <binding name=\"", variable, "\">");
  auto append = [&](const auto&... values) {
    absl::StrAppend(&result, values...);
  };

  auto escape = [](std::string_view sv) {
    return RdfEscaping::escapeForXml(std::string{sv});
  };
  // Lambda that creates the inner content of the binding for the various
  // datatypes.
  auto strToBinding = [&result, &append, &escape](std::string_view entitystr) {
    // The string is an IRI or literal.
    if (ql::starts_with(entitystr, '<')) {
      // Strip the <> surrounding the iri.
      append("<uri>"sv, escape(entitystr.substr(1, entitystr.size() - 2)),
             "</uri>"sv);
    } else if (ql::starts_with(entitystr, "_:")) {
      append("<bnode>"sv, entitystr.substr(2), "</bnode>"sv);
    } else {
      size_t quotePos = entitystr.rfind('"');
      if (quotePos == std::string::npos) {
        absl::StrAppend(&result, "<literal>"sv, escape(entitystr),
                        "</literal>"sv);
      } else {
        std::string_view innerValue = entitystr.substr(1, quotePos - 1);
        // Look for a language tag or type.
        if (quotePos < entitystr.size() - 1 && entitystr[quotePos + 1] == '@') {
          std::string_view langtag = entitystr.substr(quotePos + 2);
          append("<literal xml:lang=\""sv, langtag, "\">"sv, escape(innerValue),
                 "</literal>"sv);
        } else if (quotePos < entitystr.size() - 2 &&
                   entitystr[quotePos + 1] == '^') {
          AD_CORRECTNESS_CHECK(entitystr[quotePos + 2] == '^');
          std::string_view datatype{entitystr};
          // remove the <angledBrackets> around the datatype IRI
          AD_CONTRACT_CHECK(datatype.size() >= quotePos + 5);
          datatype.remove_prefix(quotePos + 4);
          datatype.remove_suffix(1);
          append("<literal datatype=\""sv, escape(datatype), "\">"sv,
                 escape(innerValue), "</literal>"sv);
        } else {
          // A plain literal that contains neither a language tag nor a datatype
          append("<literal>"sv, escape(innerValue), "</literal>"sv);
        }
      }
    }
  };
  if (!xsdType) {
    // No xsdType, this means that `stringValue` is a plain string literal
    // or entity.
    strToBinding(stringValue);
  } else {
    append("<literal datatype=\""sv, xsdType, "\">"sv, stringValue,
           "</literal>");
  }
  append("</binding>");
  return result;
}

// _____________________________________________________________________________
template <>
STREAMABLE_GENERATOR_TYPE ExportQueryExecutionTrees::selectQueryResultToStream<
    ad_utility::MediaType::sparqlXml>(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  using namespace std::string_view_literals;
  STREAMABLE_YIELD(
      "<?xml version=\"1.0\"?>\n"
      "<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">");

  STREAMABLE_YIELD("\n<head>");
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  std::shared_ptr<const Result> result = qet.getResult(true);

  // In the XML format, the variables don't include the question mark.
  auto varsWithoutQuestionMark = ql::views::transform(
      variables, [](std::string_view var) { return var.substr(1); });
  for (std::string_view var : varsWithoutQuestionMark) {
    STREAMABLE_YIELD(absl::StrCat("\n  <variable name=\""sv, var, "\"/>"sv));
  }
  STREAMABLE_YIELD("\n</head>");

  STREAMABLE_YIELD("\n<results>");

  result->logResultSize();
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, false);
  // TODO<joka921> we could prefilter for the nonexisting variables.
  uint64_t resultSize = 0;
  for (const auto& [pair, range] :
       getRowIndices(limitAndOffset, *result, resultSize)) {
    for (uint64_t i : range) {
      STREAMABLE_YIELD("\n  <result>");
      for (size_t j = 0; j < selectedColumnIndices.size(); ++j) {
        if (selectedColumnIndices[j].has_value()) {
          const auto& val = selectedColumnIndices[j].value();
          Id id = pair.idTable_(i, val.columnIndex_);
          STREAMABLE_YIELD(idToXMLBinding(
              val.variable_, id, qet.getQec()->getIndex(), pair.localVocab_));
        }
      }
      STREAMABLE_YIELD("\n  </result>");
      cancellationHandle->throwIfCancelled();
    }
  }
  STREAMABLE_YIELD("\n</results>");
  STREAMABLE_YIELD("\n</sparql>");
}

// _____________________________________________________________________________
template <>
STREAMABLE_GENERATOR_TYPE ExportQueryExecutionTrees::selectQueryResultToStream<
    ad_utility::MediaType::sparqlJson>(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();
  AD_LOG_DEBUG << "Converting result IDs to their corresponding strings ..."
               << std::endl;
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, false);

  auto vars = selectClause.getSelectedVariablesAsStrings();
  ql::ranges::for_each(vars, [](std::string& var) { var = var.substr(1); });
  nlohmann::json jsonVars = vars;
  STREAMABLE_YIELD(absl::StrCat(R"({"head":{"vars":)", jsonVars.dump(),
                                R"(},"results":{"bindings":[)"));

  // Get all columns with defined variables.
  QueryExecutionTree::ColumnIndicesAndTypes columns =
      qet.selectedVariablesToColumnIndices(selectClause, false);
  ql::erase(columns, std::nullopt);

  auto getBinding = [&](const IdTable& idTable, const uint64_t& i,
                        const LocalVocab& localVocab) {
    nlohmann::ordered_json binding = {};
    for (const auto& column : columns) {
      auto optionalStringAndType =
          idToStringAndType(qet.getQec()->getIndex(),
                            idTable(i, column->columnIndex_), localVocab);
      if (optionalStringAndType.has_value()) [[likely]] {
        const auto& [stringValue, xsdType] = optionalStringAndType.value();
        binding[column->variable_] =
            stringAndTypeToBinding(stringValue, xsdType);
      }
    }
    return binding.dump();
  };

  // Iterate over the result and yield the bindings. Note that when `columns`
  // is empty, we have to output an empty set of bindings per row.
  bool isFirstRow = true;
  uint64_t resultSize = 0;
  for (const auto& [pair, range] :
       getRowIndices(limitAndOffset, *result, resultSize)) {
    for (uint64_t i : range) {
      if (!isFirstRow) [[likely]] {
        STREAMABLE_YIELD(",");
      }
      if (columns.empty()) {
        STREAMABLE_YIELD("{}");
      } else {
        STREAMABLE_YIELD(getBinding(pair.idTable_, i, pair.localVocab_));
      }
      cancellationHandle->throwIfCancelled();
      isFirstRow = false;
    }
  }

  STREAMABLE_YIELD("]}}");
  STREAMABLE_RETURN;
}

// _____________________________________________________________________________
template <>
STREAMABLE_GENERATOR_TYPE ExportQueryExecutionTrees::selectQueryResultToStream<
    ad_utility::MediaType::binaryQleverExport>(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  // TODO<joka921> fix this
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  return qlever::binary_export::exportAsQLeverBinary(
      qet, selectClause, limitAndOffset, std::move(cancellationHandle));
#else
  throw std::runtime_error{
      "The special binary export is currently not supported in the reduced "
      "feature mode for C++17"};
#endif
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
STREAMABLE_GENERATOR_TYPE
ExportQueryExecutionTrees::constructQueryResultToStream(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  static_assert(format == MediaType::octetStream || format == MediaType::csv ||
                format == MediaType::tsv || format == MediaType::sparqlXml ||
                format == MediaType::sparqlJson ||
                format == MediaType::qleverJson ||
                format == MediaType::binaryQleverExport);
  if constexpr (format == MediaType::octetStream ||
                format == MediaType::binaryQleverExport) {
    AD_THROW("Binary export is not supported for CONSTRUCT queries");
  } else if constexpr (format == MediaType::sparqlXml) {
    AD_THROW("XML export is currently not supported for CONSTRUCT queries");
  } else if constexpr (format == MediaType::sparqlJson) {
    AD_THROW("SparqlJSON export is not supported for CONSTRUCT queries");
  }
  AD_CONTRACT_CHECK(format != MediaType::qleverJson);

  result->logResultSize();
  constexpr auto& escapeFunction = format == MediaType::tsv
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;
  constexpr char sep = format == MediaType::tsv ? '\t' : ',';
  [[maybe_unused]] uint64_t resultSize = 0;
  auto generator = constructQueryResultToTriples(
      qet, constructTriples, limitAndOffset, result, resultSize,
      std::move(cancellationHandle));
  for (auto& triple : generator) {
    STREAMABLE_YIELD(escapeFunction(std::move(triple.subject_)));
    STREAMABLE_YIELD(sep);
    STREAMABLE_YIELD(escapeFunction(std::move(triple.predicate_)));
    STREAMABLE_YIELD(sep);
    STREAMABLE_YIELD(escapeFunction(std::move(triple.object_)));
    STREAMABLE_YIELD("\n");
  }
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// _____________________________________________________________________________
cppcoro::generator<std::string>
ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
    STREAMABLE_GENERATOR_TYPE streamGenerator) {
  // Immediately throw any exceptions that occur during the computation of the
  // first block outside the actual generator. That way we get a proper HTTP
  // response with error status codes etc. at least for those exceptions.
  // Note: `begin` advances until the first block.
  auto it = streamGenerator.begin();
  return [](auto innerGenerator, auto it) -> cppcoro::generator<std::string> {
    std::optional<std::string> exceptionMessage;
    try {
      for (; it != innerGenerator.end(); ++it) {
        co_yield std::string{*it};
      }
    } catch (const std::exception& e) {
      exceptionMessage = e.what();
    } catch (...) {
      exceptionMessage = "A very strange exception, please report this";
    }
    // TODO<joka921, RobinTF> Think of a better way to propagate and log those
    // errors. We can additionally send them via the websocket connection, but
    // that doesn't solve the problem for users of the plain HTTP 1.1 endpoint.
    if (exceptionMessage.has_value()) {
      std::string prefix =
          "\n !!!!>># An error has occurred while exporting the query result. "
          "Unfortunately due to limitations in the HTTP 1.1 protocol, there is "
          "no better way to report this than to append it to the incomplete "
          "result. The error message was:\n";
      co_yield prefix;
      co_yield exceptionMessage.value();
    }
  }(std::move(streamGenerator), std::move(it));
}
#endif

void ExportQueryExecutionTrees::compensateForLimitOffsetClause(
    LimitOffsetClause& limitOffsetClause, const QueryExecutionTree& qet) {
  // See the comment in `QueryPlanner::createExecutionTrees` on why this is safe
  // to do
  if (qet.supportsLimit()) {
    limitOffsetClause._offset = 0;
  }
}

// _____________________________________________________________________________
ExportQueryExecutionTrees::ComputeResultReturnType
ExportQueryExecutionTrees::computeResult(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType, const ad_utility::Timer& requestTimer,
    CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  auto limit = parsedQuery._limitOffset;
  compensateForLimitOffsetClause(limit, qet);
  auto compute = ad_utility::ApplyAsValueIdentity{[&](auto format) {
    if constexpr (format == MediaType::qleverJson) {
      return computeResultAsQLeverJSON(parsedQuery, qet, requestTimer,
                                       std::move(cancellationHandle),
                                       streamableYielder);
    } else {
      if (parsedQuery.hasAskClause()) {
        return computeResultForAsk(parsedQuery, qet, mediaType, requestTimer,
                                   streamableYielder);
      }
      return parsedQuery.hasSelectClause()
                 ? selectQueryResultToStream<format>(
                       qet, parsedQuery.selectClause(), limit,
                       std::move(cancellationHandle), streamableYielder)
                 : constructQueryResultToStream<format>(
                       qet, parsedQuery.constructClause().triples_, limit,
                       qet.getResult(true), std::move(cancellationHandle),
                       streamableYielder);
    }
  }};

  using enum MediaType;

  static constexpr std::array supportedTypes{
      csv,       tsv,        octetStream, turtle,
      sparqlXml, sparqlJson, qleverJson,  binaryQleverExport};
  AD_CORRECTNESS_CHECK(ad_utility::contains(supportedTypes, mediaType));

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto inner =
      ad_utility::ConstexprSwitch<csv, tsv, octetStream, turtle, sparqlXml,
                                  sparqlJson, qleverJson, binaryQleverExport>{}(
          compute, mediaType);
  return convertStreamGeneratorForChunkedTransfer(std::move(inner));
#else
  ad_utility::ConstexprSwitch<csv, tsv, octetStream, turtle, sparqlXml,
                              sparqlJson, qleverJson>{}(compute, mediaType);
#endif
}

// _____________________________________________________________________________
STREAMABLE_GENERATOR_TYPE
ExportQueryExecutionTrees::computeResultAsQLeverJSON(
    const ParsedQuery& query, const QueryExecutionTree& qet,
    const ad_utility::Timer& requestTimer,
    CancellationHandle cancellationHandle,
    [[maybe_unused]] STREAMABLE_YIELDER_TYPE streamableYielder) {
  auto timeUntilFunctionCall = requestTimer.msecs();
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  nlohmann::json jsonPrefix;

  jsonPrefix["query"] =
      ad_utility::truncateOperationString(query._originalString);
  jsonPrefix["status"] = "OK";
  jsonPrefix["warnings"] = qet.collectWarnings();
  if (query.hasSelectClause()) {
    jsonPrefix["selected"] =
        query.selectClause().getSelectedVariablesAsStrings();
  } else if (query.hasConstructClause()) {
    jsonPrefix["selected"] =
        std::vector<std::string>{"?subject", "?predicate", "?object"};
  } else {
    AD_CORRECTNESS_CHECK(query.hasAskClause());
    jsonPrefix["selected"] = std::vector<std::string>{"?result"};
  }

  std::string prefixStr = jsonPrefix.dump();
  STREAMABLE_YIELD(
      absl::StrCat(prefixStr.substr(0, prefixStr.size() - 1), R"(,"res":[)"));

  // Yield the bindings and compute the result size.
  uint64_t resultSize = 0;
  auto bindings = [&]() {
    if (query.hasSelectClause()) {
      return selectQueryResultBindingsToQLeverJSON(
          qet, query.selectClause(), query._limitOffset, std::move(result),
          resultSize, std::move(cancellationHandle));
    } else if (query.hasConstructClause()) {
      return constructQueryResultBindingsToQLeverJSON(
          qet, query.constructClause().triples_, query._limitOffset,
          std::move(result), resultSize, std::move(cancellationHandle));
    } else {
      // TODO<joka921>: Refactor this to use std::visit.
      return askQueryResultToQLeverJSON(std::move(result));
    }
  }();

  size_t numBindingsExported = 0;
  for (const std::string& b : bindings) {
    if (numBindingsExported > 0) [[likely]] {
      STREAMABLE_YIELD(",");
    }
    STREAMABLE_YIELD(b);
    ++numBindingsExported;
  }
  if (numBindingsExported < resultSize) {
    AD_LOG_INFO << "Number of bindings exported: " << numBindingsExported
                << " of " << resultSize << std::endl;
  }

  RuntimeInformation runtimeInformation = qet.getRootOperation()->runtimeInfo();
  runtimeInformation.addLimitOffsetRow(query._limitOffset, false);

  auto timeResultComputation =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          timeUntilFunctionCall + runtimeInformation.totalTime_);

  // NOTE: We report three "results sizes" in the QLever JSON output, for the
  // following reasons:
  //
  // The `resultSizeExported` is the number of bindings exported. This is
  // redundant information (we could simply count the number of entries in the
  // `res` array), but it is useful for testing and emphasizes the conceptual
  // difference to `resultSizeTotal`.
  //
  // The `resultSizeTotal` is the number of results of the WHOLE query. For
  // CONSTRUCT queries, it can be an overestimate because it also includes
  // triples, where one of the components is UNDEF, which are not included
  // in the final result of a CONSTRUCT query.
  //
  // The `resultsize` is equal to `resultSizeTotal`. It is included for
  // backwards compatibility, in particular, because the QLever UI uses it
  // at many places.
  nlohmann::json jsonSuffix;
  jsonSuffix["runtimeInformation"]["meta"] = nlohmann::ordered_json(
      qet.getRootOperation()->getRuntimeInfoWholeQuery());
  jsonSuffix["runtimeInformation"]["query_execution_tree"] =
      nlohmann::ordered_json(runtimeInformation);
  jsonSuffix["resultSizeExported"] = numBindingsExported;
  jsonSuffix["resultSizeTotal"] = resultSize;
  jsonSuffix["resultsize"] = resultSize;
  jsonSuffix["time"]["total"] =
      absl::StrCat(requestTimer.msecs().count(), "ms");
  jsonSuffix["time"]["computeResult"] =
      absl::StrCat(timeResultComputation.count(), "ms");

  STREAMABLE_YIELD(absl::StrCat("],", jsonSuffix.dump().substr(1)));
}

// This function evaluates a `Variable` in the context of the `CONSTRUCT`
// export.
[[nodiscard]] static std::optional<std::string> evaluateVariableForConstruct(
    const Variable& var, const ConstructQueryExportContext& context,
    [[maybe_unused]] PositionInTriple positionInTriple) {
  size_t row = context._row;
  const auto& variableColumns = context._variableColumns;
  const Index& qecIndex = context._qecIndex;
  const auto& idTable = context.idTable_;
  if (variableColumns.contains(var)) {
    size_t index = variableColumns.at(var).columnIndex_;
    auto id = idTable(row, index);
    auto optionalStringAndType = ExportQueryExecutionTrees::idToStringAndType(
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

// The following trick has the effect that `Variable::evaluate()` calls the
// above function, without `Variable` having to link against the (heavy) export
// module. This is a bit of a hack and will be removed in the future when we
// improve the CONSTRUCT module for better performance.
[[maybe_unused]] static const int initializeVariableEvaluationDummy = []() {
  Variable::decoupledEvaluateFuncPtr() = &evaluateVariableForConstruct;
  return 42;
}();
