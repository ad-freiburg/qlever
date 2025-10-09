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

#include <optional>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "index/EncodedIriManager.h"
#include "index/IndexImpl.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/ConstexprUtils.h"
#include "util/ValueIdentity.h"
#include "util/http/MediaTypes.h"
#include "util/json.h"
#include "util/views/TakeUntilInclusiveView.h"

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
  static constexpr auto textPlain = ad_utility::MediaType::textPlain;
  static constexpr auto json = ad_utility::MediaType::json;
  static constexpr auto sparqlJson = ad_utility::MediaType::sparqlJson;
  static constexpr auto sparqlXml = ad_utility::MediaType::sparqlXml;
  static constexpr auto qleverJson = ad_utility::MediaType::qleverJson;
  static constexpr auto tsv = ad_utility::MediaType::tsv;
  static constexpr auto csv = ad_utility::MediaType::csv;
  static constexpr auto turtle = ad_utility::MediaType::turtle;
  static constexpr auto ntriples = ad_utility::MediaType::ntriples;
  static constexpr auto octetStream = ad_utility::MediaType::octetStream;
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
ad_utility::InputRangeTypeErased<
    ExportQueryExecutionTrees::TableConstRefWithVocab>
ExportQueryExecutionTrees::getIdTables(const Result& result) {
  using namespace ad_utility;
  if (result.isFullyMaterialized()) {
    return InputRangeTypeErased(lazySingleValueRange([&result]() {
      return TableConstRefWithVocab{result.idTable(), result.localVocab()};
    }));
  }

  return InputRangeTypeErased(CachingTransformInputRange(
      result.idTables(), [](const Result::IdTableVocabPair& pair) {
        return TableConstRefWithVocab{pair.idTable_, pair.localVocab_};
      }));
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<ExportQueryExecutionTrees::TableWithRange>
ExportQueryExecutionTrees::getRowIndices(LimitOffsetClause limitOffset,
                                         const Result& result,
                                         uint64_t& resultSize,
                                         uint64_t resultSizeMultiplicator) {
  using namespace ad_utility;
  // The first call initializes the `resultSize` to zero (no need to
  // initialize it outside of the function).
  resultSize = 0;

  // If the LIMIT is zero, there are no blocks to yield and the total result
  // size is zero.
  if (limitOffset._limit.value_or(1) == 0) {
    return InputRangeTypeErased(ql::span<TableWithRange>());
  }

  // The following structs and variant is used to encode the subrange of an
  // `IdTable` of the actual `result` that has to be part of the result. An
  // `IdTable` that is completely before the result (because of `Offset`)
  struct BeforeOffset {};
  // An `IdTable` that is completely after the `ExportLimit`, but not yet after
  // the `Limit`. For these tables we only have to report the number of lines in
  // the table.
  struct OnlyCountForExport {};

  // An `IdTable` that completely comes after the `LIMIT` has been fully
  // exhausted. Once we have reached this point, we can stop the iteration.
  struct AfterLimit {};

  // An `IdTable` of which a certain subrange becomes part of the result,
  // together with a bool that indicates whether this is the last `IdTable` (to
  // stop the computation of possibly expensive results as soon as possible.
  struct Export {
    TableWithRange tableWithRange_;
    bool isLast_;
  };

  // Encode all the possible states in a variant.
  using State =
      std::variant<BeforeOffset, OnlyCountForExport, AfterLimit, Export>;

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

  auto reduceLimit = [](uint64_t& limit, uint64_t subtrahend) {
    if (limit != std::numeric_limits<uint64_t>::max()) {
      limit = limit > subtrahend ? limit - subtrahend : 0;
    }
  };

  // Convert a `TableConstRefWithVocab` to a `State`. As this modifies the
  // `limit` and `offset`, this has to be called exactly once per block in the
  // `result` and strictly in order.
  auto tableToState =
      [reduceLimit, &resultSize, resultSizeMultiplicator,
       limit = effectiveLimit, exportLimit = effectiveExportLimit,
       offset = effectiveOffset](
          TableConstRefWithVocab& tableWithVocab) mutable -> State {
    uint64_t blockSize = tableWithVocab.idTable().numRows();
    if (offset >= blockSize) {
      offset -= blockSize;
      return BeforeOffset{};
    }
    if (limit == 0) {
      return AfterLimit{};
    }

    AD_CORRECTNESS_CHECK(offset < blockSize);
    AD_CORRECTNESS_CHECK(limit > 0);

    // Compute the range of rows to be exported (can be zero) and to be
    // counted.
    uint64_t rangeBegin = std::exchange(offset, 0);
    uint64_t numRowsToBeExported =
        std::min(exportLimit, blockSize - rangeBegin);
    uint64_t numRowsToBeCounted = std::min(limit, blockSize - rangeBegin);

    AD_CORRECTNESS_CHECK(rangeBegin + numRowsToBeExported <= blockSize);
    AD_CORRECTNESS_CHECK(rangeBegin + numRowsToBeCounted <= blockSize);

    // Add to `resultSize` and update the effective offset (which becomes
    // zero after the first non-skipped block) and limits (make sure to
    // never go below zero and `std::numeric_limits<uint64_t>::max()` stays
    // there).
    resultSize += numRowsToBeCounted * resultSizeMultiplicator;
    reduceLimit(limit, numRowsToBeCounted);
    reduceLimit(exportLimit, numRowsToBeCounted);

    if (numRowsToBeExported == 0) {
      return OnlyCountForExport{};
    }
    return Export{
        TableWithRange{
            tableWithVocab,
            ql::views::iota(rangeBegin, rangeBegin + numRowsToBeExported)},
        (limit == 0 && exportLimit == 0)};
  };

  // Now transform all the blocks in the result to a `State`, and only yield
  // the blocks of which some part has to be exported. Consume as few blocks
  // of the result as possible.
  namespace v = ql::views;
  return InputRangeTypeErased{
      OwningView{getIdTables(result)} |
      v::transform(tableToState)
      // The caching is required to make the pattern of a modifying transform
      // (where the operator* may be called at most once per element) work with
      // a `filter` down the line (which evaluates operator* of its subrange
      // multiple times). It is also cheap, as we only store reference types.
      | ::ranges::views::cache1
      // We have to consume, but do nothing for `BeforeOffset`
      | v::drop_while(ad_utility::holdsAlternative<BeforeOffset>)
      // As soon as we encoounter `AfterLimit`, we can immediately stop.
      | v::take_while(std::not_fn(holdsAlternative<AfterLimit>))
      // Also make sure to not trigger the result computation of the first
      // (unneeded) block after the last needed block. Note: With this, the
      // `take_while` above seems redundant, but it might be that no IdTable is
      // yielded at all.
      | ad_utility::views::takeUntilInclusive([](const State& state) {
          auto ptr = std::get_if<Export>(&state);
          return ptr && ptr->isLast_;
        })
      // At this stage we only see `Export` or `OnlyCountForExport`. For the
      // latter we don't have to do anything, because the `transformToState`
      // lambda above has already done the counting for us.
      | v::filter([](const State& state) {
          return std::holds_alternative<Export>(state);
        })
      // We now only have `Exports`, extract the `TableWithRange`, because that
      // is what we need as a value for the output.
      | v::transform([](State&& state) -> TableWithRange {
          return std::get<Export>(state).tableWithRange_;
        })};
}

namespace {
// Evaluate a `ConstructTriple` on the `context`. If the evaluation fails (e.g.
// because an entry of the triple would be invalid), return an empty
// `StringTriple`.
auto evaluateTripleForConstruct =
    [](const auto& triple, const ConstructQueryExportContext& context) {
      constexpr auto SUBJECT = PositionInTriple::SUBJECT;
      constexpr auto PREDICATE = PositionInTriple::PREDICATE;
      constexpr auto OBJECT = PositionInTriple::OBJECT;
      auto subject = triple[0].evaluate(context, SUBJECT);
      auto predicate = triple[1].evaluate(context, PREDICATE);
      auto object = triple[2].evaluate(context, OBJECT);
      if (!subject.has_value() || !predicate.has_value() ||
          !object.has_value()) {
        return QueryExecutionTree::StringTriple();
      }
      return QueryExecutionTree::StringTriple(std::move(subject.value()),
                                              std::move(predicate.value()),
                                              std::move(object.value()));
    };

}  // namespace
// _____________________________________________________________________________
auto ExportQueryExecutionTrees::constructQueryResultToTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    uint64_t& resultSize, CancellationHandle cancellationHandle) {
  // The `resultSizeMultiplicator`(last argument of `getRowIndices`) is
  // explained by the following: For each result from the WHERE clause, we
  // produce up to `constructTriples.size()` triples. We do not account for
  // triples that are filtered out because one of the components is UNDEF (it
  // would require materializing the whole result)
  auto rowIndices = getRowIndices(limitAndOffset, *result, resultSize,
                                  constructTriples.size());
  return ad_utility::InputRangeTypeErased(
      ad_utility::OwningView{std::move(rowIndices)} |
      ql::views::transform(
          [&qet, &constructTriples, result = std::move(result),
           cancellationHandle = std::move(cancellationHandle),
           rowOffset = size_t{0}](const auto& tableWithView) mutable {
            // NOTE: This reference is captured in the following lambda.
            // Normally it would be dangling, but as the `idTable` is not owned
            // by the `tableWithView`, but backed by the outermost range, This
            // is fine.
            auto& idTable = tableWithView.tableWithVocab_.idTable();
            auto currentRowOffset = rowOffset;
            rowOffset += idTable.size();
            return ql::ranges::transform_view(
                tableWithView.view_, [&, currentRowOffset](uint64_t i) {
                  auto& localVocab = tableWithView.tableWithVocab_.localVocab();
                  return ql::ranges::transform_view(
                      constructTriples,
                      [&cancellationHandle,
                       context = ConstructQueryExportContext{
                           i, idTable, localVocab, qet.getVariableColumns(),
                           qet.getQec()->getIndex(),
                           currentRowOffset}](const auto& triple) mutable {
                        cancellationHandle->throwIfCancelled();
                        return evaluateTripleForConstruct(triple, context);
                      });
                });
          }) |
      ql::views::join | ql::views::join |
      ql::views::filter([](const auto& triple) { return !triple.isEmpty(); }));
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
ad_utility::InputRangeTypeErased<std::string>
ExportQueryExecutionTrees::constructQueryResultBindingsToQLeverJSON(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
  auto generator = constructQueryResultToTriples(
      qet, constructTriples, limitAndOffset, std::move(result), resultSize,
      std::move(cancellationHandle));

  return ad_utility::InputRangeTypeErased<std::string>(
      ad_utility::CachingTransformInputRange(
          std::move(generator), [](QueryExecutionTree::StringTriple& triple) {
            auto binding = nlohmann::json::array({std::move(triple.subject_),
                                                  std::move(triple.predicate_),
                                                  std::move(triple.object_)});
            return binding.dump();
          }));
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
auto ExportQueryExecutionTrees::idTableToQLeverJSONBindings(
    const QueryExecutionTree& qet, LimitOffsetClause limitAndOffset,
    QueryExecutionTree::ColumnIndicesAndTypes columns,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
  AD_CORRECTNESS_CHECK(result != nullptr);

  auto rowIndicies = getRowIndices(limitAndOffset, *result, resultSize);
  return ad_utility::OwningView(std::move(rowIndicies)) |
         ql::views::transform(
             [&qet, columns = std::move(columns), result = std::move(result),
              cancellationHandle =
                  std::move(cancellationHandle)](const auto& tableWithView) {
               return ql::ranges::transform_view(
                   tableWithView.view_, [&](uint64_t rowIndex) {
                     cancellationHandle->throwIfCancelled();
                     TableConstRefWithVocab tableWithVocab =
                         tableWithView.tableWithVocab_;
                     return idTableToQLeverJSONRow(
                                qet, columns, tableWithVocab.localVocab(),
                                rowIndex, tableWithVocab.idTable())
                         .dump();
                   });
             }) |
         ql::views::join;
}

// _____________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndTypeForEncodedValue(Id id) {
  static constexpr auto Undefined = Datatype::Undefined;
  static constexpr auto Bool = Datatype::Bool;
  static constexpr auto Int = Datatype::Int;
  static constexpr auto Double = Datatype::Double;
  static constexpr auto VocabIndex = Datatype::VocabIndex;
  static constexpr auto LocalVocabIndex = Datatype::LocalVocabIndex;
  static constexpr auto TextRecordIndex = Datatype::TextRecordIndex;
  static constexpr auto Date = Datatype::Date;
  static constexpr auto GeoPoint = Datatype::GeoPoint;
  static constexpr auto WordVocabIndex = Datatype::WordVocabIndex;
  static constexpr auto BlankNodeIndex = Datatype::BlankNodeIndex;
  static constexpr auto EncodedVal = Datatype::EncodedVal;
  static constexpr auto MaxValue = Datatype::MaxValue;
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
  AD_CORRECTNESS_CHECK(ql::ends_with(iriString, '>'));
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
  static constexpr auto Undefined = Datatype::Undefined;
  static constexpr auto Bool = Datatype::Bool;
  static constexpr auto Int = Datatype::Int;
  static constexpr auto Double = Datatype::Double;
  static constexpr auto VocabIndex = Datatype::VocabIndex;
  static constexpr auto LocalVocabIndex = Datatype::LocalVocabIndex;
  static constexpr auto TextRecordIndex = Datatype::TextRecordIndex;
  static constexpr auto Date = Datatype::Date;
  static constexpr auto GeoPoint = Datatype::GeoPoint;
  static constexpr auto WordVocabIndex = Datatype::WordVocabIndex;
  static constexpr auto BlankNodeIndex = Datatype::BlankNodeIndex;
  static constexpr auto EncodedVal = Datatype::EncodedVal;
  static constexpr auto MaxValue = Datatype::MaxValue;
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
  static constexpr auto Undefined = Datatype::Undefined;
  static constexpr auto Bool = Datatype::Bool;
  static constexpr auto Int = Datatype::Int;
  static constexpr auto Double = Datatype::Double;
  static constexpr auto VocabIndex = Datatype::VocabIndex;
  static constexpr auto LocalVocabIndex = Datatype::LocalVocabIndex;
  static constexpr auto TextRecordIndex = Datatype::TextRecordIndex;
  static constexpr auto Date = Datatype::Date;
  static constexpr auto GeoPoint = Datatype::GeoPoint;
  static constexpr auto WordVocabIndex = Datatype::WordVocabIndex;
  static constexpr auto BlankNodeIndex = Datatype::BlankNodeIndex;
  static constexpr auto EncodedVal = Datatype::EncodedVal;
  static constexpr auto MaxValue = Datatype::MaxValue;
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
  static constexpr auto Undefined = Datatype::Undefined;
  static constexpr auto Bool = Datatype::Bool;
  static constexpr auto Int = Datatype::Int;
  static constexpr auto Double = Datatype::Double;
  static constexpr auto VocabIndex = Datatype::VocabIndex;
  static constexpr auto LocalVocabIndex = Datatype::LocalVocabIndex;
  static constexpr auto TextRecordIndex = Datatype::TextRecordIndex;
  static constexpr auto Date = Datatype::Date;
  static constexpr auto GeoPoint = Datatype::GeoPoint;
  static constexpr auto WordVocabIndex = Datatype::WordVocabIndex;
  static constexpr auto BlankNodeIndex = Datatype::BlankNodeIndex;
  static constexpr auto EncodedVal = Datatype::EncodedVal;
  static constexpr auto MaxValue = Datatype::MaxValue;
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
ExportQueryExecutionTrees::idToStringAndType<true, false, ql::identity>(
    const Index& index, Id id, const LocalVocab& localVocab,
    ql::identity&& escapeFunction);

// ___________________________________________________________________________
template std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndType<true, true, ql::identity>(
    const Index& index, Id id, const LocalVocab& localVocab,
    ql::identity&& escapeFunction);

// This explicit instantiation is necessary because the `Variable` class
// currently still uses it.
// TODO<joka921> Refactor the CONSTRUCT export, then this is no longer
// needed
template std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndType(const Index& index, Id id,
                                             const LocalVocab& localVocab,
                                             ql::identity&& escapeFunction);

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
ad_utility::InputRangeTypeErased<std::string> askQueryResultToQLeverJSON(
    std::shared_ptr<const Result> result) {
  return ad_utility::InputRangeTypeErased(
      ad_utility::lazySingleValueRange([result = std::move(result)]() {
        AD_CORRECTNESS_CHECK(result != nullptr);
        std::string_view value = getResultForAsk(result) ? "true" : "false";
        std::string resultLit =
            absl::StrCat("\"", value, "\"^^<", XSD_BOOLEAN_TYPE, ">");
        nlohmann::json resultJson = std::vector{std::move(resultLit)};
        return resultJson.dump();
      }));
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<std::string>
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

  return ad_utility::InputRangeTypeErased(idTableToQLeverJSONBindings(
      qet, limitAndOffset, std::move(selectedColumnIndices), std::move(result),
      resultSize, std::move(cancellationHandle)));
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
    uint64_t resultSize = 0;
    for (const auto& [pair, range] :
         getRowIndices(limitAndOffset, *result, resultSize)) {
      for (uint64_t i : range) {
        for (const auto& columnIndex : selectedColumnIndices) {
          if (columnIndex.has_value()) {
            STREAMABLE_YIELD(
                std::string_view{reinterpret_cast<const char*>(&pair.idTable()(
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
          Id id = pair.idTable()(i, val.columnIndex_);
          auto optionalStringAndType =
              idToStringAndType<format == MediaType::csv>(
                  qet.getQec()->getIndex(), id, pair.localVocab(),
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
          Id id = pair.idTable()(i, val.columnIndex_);
          STREAMABLE_YIELD(idToXMLBinding(
              val.variable_, id, qet.getQec()->getIndex(), pair.localVocab()));
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

  auto getBinding = [&](const TableConstRefWithVocab& pair, const uint64_t& i) {
    nlohmann::ordered_json binding = {};
    for (const auto& column : columns) {
      auto optionalStringAndType = idToStringAndType(
          qet.getQec()->getIndex(), pair.idTable()(i, column->columnIndex_),
          pair.localVocab());
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
        STREAMABLE_YIELD(getBinding(pair, i));
      }
      cancellationHandle->throwIfCancelled();
      isFirstRow = false;
    }
  }

  STREAMABLE_YIELD("]}}");
  STREAMABLE_RETURN;
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
                format == MediaType::qleverJson);
  if constexpr (format == MediaType::octetStream) {
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
ad_utility::InputRangeTypeErased<std::string>
ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
    STREAMABLE_GENERATOR_TYPE streamGenerator) {
  using namespace ad_utility;
  using LoopControl = ad_utility::LoopControl<std::string>;
  // Immediately throw any exceptions that occur during the computation of the
  // first block outside the actual generator. That way we get a proper HTTP
  // response with error status codes etc. at least for those exceptions.
  // Note: `begin` advances until the first block.
  auto it = streamGenerator.begin();
  return InputRangeTypeErased(InputRangeFromLoopControlGet(
      [it = std::move(it), streamGenerator = std::move(streamGenerator),
       exceptionMessage = std::optional<std::string>(std::nullopt)]() mutable {
        // TODO<joka921, RobinTF> Think of a better way to propagate and log
        // those errors. We can additionally send them via the
        // websocketconnection,but that doesn't solve the problem for users of
        // the plain HTTP 1.1 endpoint.
        if (it == streamGenerator.end()) {
          return LoopControl::makeBreak();
        }

        try {
          std::string output{*it};
          ++it;
          return LoopControl::yieldValue(std::move(output));
        } catch (const std::exception& e) {
          exceptionMessage = e.what();
        } catch (...) {
          exceptionMessage = "A very strange exception, please report this";
        }

        static constexpr std::string_view prefix =
            "\n !!!!>># An error has occurred while exporting the query "
            "result. Unfortunately due to limitations in the HTTP 1.1 "
            "protocol, there is no better way to report this than to append it "
            "to the incomplete result. The error message was:\n";
        return LoopControl::breakWithValue(
            absl::StrCat(prefix, exceptionMessage.value()));
      }));
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

  static constexpr auto textPlain = ad_utility::MediaType::textPlain;
  static constexpr auto json = ad_utility::MediaType::json;
  static constexpr auto sparqlJson = ad_utility::MediaType::sparqlJson;
  static constexpr auto sparqlXml = ad_utility::MediaType::sparqlXml;
  static constexpr auto qleverJson = ad_utility::MediaType::qleverJson;
  static constexpr auto tsv = ad_utility::MediaType::tsv;
  static constexpr auto csv = ad_utility::MediaType::csv;
  static constexpr auto turtle = ad_utility::MediaType::turtle;
  static constexpr auto ntriples = ad_utility::MediaType::ntriples;
  static constexpr auto octetStream = ad_utility::MediaType::octetStream;

  static constexpr std::array supportedTypes{
      csv, tsv, octetStream, turtle, sparqlXml, sparqlJson, qleverJson};
  AD_CORRECTNESS_CHECK(ad_utility::contains(supportedTypes, mediaType));

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto inner =
      ad_utility::ConstexprSwitch<csv, tsv, octetStream, turtle, sparqlXml,
                                  sparqlJson, qleverJson>{}(compute, mediaType);

  return [](auto range) -> cppcoro::generator<std::string> {
    for (auto&& item : range) {
      co_yield item;
    }
  }(convertStreamGeneratorForChunkedTransfer(std::move(inner)));

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
