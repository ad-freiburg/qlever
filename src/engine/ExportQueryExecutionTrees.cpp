//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "ExportQueryExecutionTrees.h"

#include <absl/strings/str_cat.h>

#include <ranges>

#include "parser/RdfEscaping.h"
#include "util/ConstexprUtils.h"
#include "util/http/MediaTypes.h"

// __________________________________________________________________________
cppcoro::generator<const IdTable&> ExportQueryExecutionTrees::getIdTables(
    const Result& result) {
  if (result.isFullyMaterialized()) {
    co_yield result.idTable();
  } else {
    for (const IdTable& idTable : result.idTables()) {
      co_yield idTable;
    }
  }
}

// Return a range that contains the indices of the rows that have to be exported
// from the `idTable` given the `LimitOffsetClause`. It takes into account the
// LIMIT, the OFFSET, and the actual size of the `idTable`
cppcoro::generator<ExportQueryExecutionTrees::TableWithRange>
ExportQueryExecutionTrees::getRowIndices(LimitOffsetClause limitOffset,
                                         const Result& result) {
  if (limitOffset._limit.value_or(1) == 0) {
    co_return;
  }
  for (const IdTable& idTable : getIdTables(result)) {
    uint64_t currentOffset = limitOffset.actualOffset(idTable.numRows());
    uint64_t upperBound = limitOffset.upperBound(idTable.numRows());
    if (currentOffset != upperBound) {
      co_yield {idTable, std::views::iota(currentOffset, upperBound)};
    }
    limitOffset._offset -= currentOffset;
    if (limitOffset._limit.has_value()) {
      limitOffset._limit =
          limitOffset._limit.value() - (upperBound - currentOffset);
    }
    if (limitOffset._limit.value_or(1) == 0) {
      break;
    }
  }
}

// _____________________________________________________________________________
cppcoro::generator<QueryExecutionTree::StringTriple>
ExportQueryExecutionTrees::constructQueryResultToTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    CancellationHandle cancellationHandle) {
  for (const auto& [idTable, range] : getRowIndices(limitAndOffset, *result)) {
    for (uint64_t i : range) {
      ConstructQueryExportContext context{i, idTable, result->localVocab(),
                                          qet.getVariableColumns(),
                                          qet.getQec()->getIndex()};
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
  }
}

// _____________________________________________________________________________
template <>
ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    constructQueryResultToStream<ad_utility::MediaType::turtle>(
        const QueryExecutionTree& qet,
        const ad_utility::sparql_types::Triples& constructTriples,
        LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
        CancellationHandle cancellationHandle) {
  result->logResultSize();
  auto generator =
      constructQueryResultToTriples(qet, constructTriples, limitAndOffset,
                                    result, std::move(cancellationHandle));
  for (const auto& triple : generator) {
    co_yield triple.subject_;
    co_yield ' ';
    co_yield triple.predicate_;
    co_yield ' ';
    // NOTE: It's tempting to co_yield an expression using a ternary operator:
    // co_yield triple._object.starts_with('"')
    //     ? RdfEscaping::validRDFLiteralFromNormalized(triple._object)
    //     : triple._object;
    // but this leads to 1. segfaults in GCC (probably a compiler bug) and 2.
    // to unnecessary copies of `triple._object` in the `else` case because
    // the ternary always has to create a new prvalue.
    if (triple.object_.starts_with('"')) {
      std::string objectAsValidRdfLiteral =
          RdfEscaping::validRDFLiteralFromNormalized(triple.object_);
      co_yield objectAsValidRdfLiteral;
    } else {
      co_yield triple.object_;
    }
    co_yield " .\n";
  }
}

// _____________________________________________________________________________
cppcoro::generator<std::string>
ExportQueryExecutionTrees::constructQueryResultBindingsToQLeverJSON(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset, std::shared_ptr<const Result> res,
    CancellationHandle cancellationHandle) {
  auto generator = constructQueryResultToTriples(qet, constructTriples,
                                                 limitAndOffset, std::move(res),
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
    const QueryExecutionTree& qet, const LimitOffsetClause& limitAndOffset,
    const QueryExecutionTree::ColumnIndicesAndTypes columns,
    std::shared_ptr<const Result> result,
    CancellationHandle cancellationHandle) {
  AD_CORRECTNESS_CHECK(result != nullptr);
  for (const auto& [idTable, range] : getRowIndices(limitAndOffset, *result)) {
    for (uint64_t rowIndex : range) {
      co_yield idTableToQLeverJSONRow(qet, columns, result->localVocab(),
                                      rowIndex, idTable)
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
        // Format as integer if fractional part is zero, let C++ decide
        // otherwise.
        std::stringstream ss;
        double d = id.getDouble();
        double dIntPart;
        if (std::modf(d, &dIntPart) == 0.0) {
          ss << std::fixed << std::setprecision(0) << id.getDouble();
        } else {
          ss << d;
        }
        return std::pair{std::move(ss).str(), XSD_DECIMAL_TYPE};
      }();
    case Bool:
      return id.getBool() ? std::pair{"true", XSD_BOOLEAN_TYPE}
                          : std::pair{"false", XSD_BOOLEAN_TYPE};
    case Int:
      return std::pair{std::to_string(id.getInt()), XSD_INT_TYPE};
    case Date:
      return id.getDate().toStringAndType();
    case GeoPoint:
      return id.getGeoPoint().toStringAndType();
    case BlankNodeIndex:
      return std::pair{absl::StrCat("_:bn", id.getBlankNodeIndex().get()),
                       nullptr};
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
ad_utility::triple_component::LiteralOrIri
ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
    const Index& index, Id id, const LocalVocab& localVocab) {
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  switch (id.getDatatype()) {
    case Datatype::LocalVocabIndex:
      return localVocab.getWord(id.getLocalVocabIndex()).asLiteralOrIri();
    case Datatype::VocabIndex: {
      auto entity = index.indexToString(id.getVocabIndex());
      return LiteralOrIri::fromStringRepresentation(entity);
    }
    default:
      AD_FAIL();
  }
};

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

  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  auto handleIriOrLiteral = [&escapeFunction](const LiteralOrIri& word)
      -> std::optional<std::pair<std::string, const char*>> {
    if constexpr (onlyReturnLiterals) {
      if (!word.isLiteral()) {
        return std::nullopt;
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
    case TextRecordIndex:
      return std::pair{
          escapeFunction(index.getTextExcerpt(id.getTextRecordIndex())),
          nullptr};
    default:
      return idToStringAndTypeForEncodedValue(id);
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
  if (entitystr.starts_with('<')) {
    // Strip the <> surrounding the iri.
    b["value"] = entitystr.substr(1, entitystr.size() - 2);
    // Even if they are technically IRIs, the format needs the type to be
    // "uri".
    b["type"] = "uri";
  } else if (entitystr.starts_with("_:")) {
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
cppcoro::generator<std::string>
ExportQueryExecutionTrees::selectQueryResultBindingsToQLeverJSON(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result,
    CancellationHandle cancellationHandle) {
  AD_CORRECTNESS_CHECK(result != nullptr);
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  QueryExecutionTree::ColumnIndicesAndTypes selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, true);

  return idTableToQLeverJSONBindings(qet, limitAndOffset, selectedColumnIndices,
                                     std::move(result),
                                     std::move(cancellationHandle));
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::selectQueryResultToStream(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause,
    LimitOffsetClause limitAndOffset, CancellationHandle cancellationHandle) {
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
  LOG(DEBUG) << "Converting result IDs to their corresponding strings ..."
             << std::endl;
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, true);

  // special case : binary export of IdTable
  if constexpr (format == MediaType::octetStream) {
    for (const auto& [idTable, range] :
         getRowIndices(limitAndOffset, *result)) {
      for (uint64_t i : range) {
        for (const auto& columnIndex : selectedColumnIndices) {
          if (columnIndex.has_value()) {
            co_yield std::string_view{reinterpret_cast<const char*>(&idTable(
                                          i, columnIndex.value().columnIndex_)),
                                      sizeof(Id)};
          }
        }
        cancellationHandle->throwIfCancelled();
      }
    }
    co_return;
  }

  static constexpr char separator = format == MediaType::tsv ? '\t' : ',';
  // Print header line
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  // In the CSV format, the variables don't include the question mark.
  if (format == MediaType::csv) {
    std::ranges::for_each(variables,
                          [](std::string& var) { var = var.substr(1); });
  }
  co_yield absl::StrJoin(variables, std::string_view{&separator, 1});
  co_yield '\n';

  constexpr auto& escapeFunction = format == MediaType::tsv
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;
  for (const auto& [idTable, range] : getRowIndices(limitAndOffset, *result)) {
    for (uint64_t i : range) {
      for (size_t j = 0; j < selectedColumnIndices.size(); ++j) {
        if (selectedColumnIndices[j].has_value()) {
          const auto& val = selectedColumnIndices[j].value();
          Id id = idTable(i, val.columnIndex_);
          auto optionalStringAndType =
              idToStringAndType<format == MediaType::csv>(
                  qet.getQec()->getIndex(), id, result->localVocab(),
                  escapeFunction);
          if (optionalStringAndType.has_value()) [[likely]] {
            co_yield optionalStringAndType.value().first;
          }
        }
        co_yield j + 1 < selectedColumnIndices.size() ? separator : '\n';
      }
      cancellationHandle->throwIfCancelled();
    }
  }
  LOG(DEBUG) << "Done creating readable result.\n";
}

// Convert a single ID to an XML binding of the given `variable`.
static std::string idToXMLBinding(std::string_view variable, Id id,
                                  const auto& index, const auto& localVocab) {
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
    if (entitystr.starts_with('<')) {
      // Strip the <> surrounding the iri.
      append("<uri>"sv, escape(entitystr.substr(1, entitystr.size() - 2)),
             "</uri>"sv);
    } else if (entitystr.starts_with("_:")) {
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
ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    selectQueryResultToStream<ad_utility::MediaType::sparqlXml>(
        const QueryExecutionTree& qet,
        const parsedQuery::SelectClause& selectClause,
        LimitOffsetClause limitAndOffset,
        CancellationHandle cancellationHandle) {
  using namespace std::string_view_literals;
  co_yield "<?xml version=\"1.0\"?>\n"
      "<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">";

  co_yield "\n<head>";
  std::vector<std::string> variables =
      selectClause.getSelectedVariablesAsStrings();
  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  std::shared_ptr<const Result> result = qet.getResult(true);

  // In the XML format, the variables don't include the question mark.
  auto varsWithoutQuestionMark = std::views::transform(
      variables, [](std::string_view var) { return var.substr(1); });
  for (std::string_view var : varsWithoutQuestionMark) {
    co_yield absl::StrCat("\n  <variable name=\""sv, var, "\"/>"sv);
  }
  co_yield "\n</head>";

  co_yield "\n<results>";

  result->logResultSize();
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, false);
  // TODO<joka921> we could prefilter for the nonexisting variables.
  for (const auto& [idTable, range] : getRowIndices(limitAndOffset, *result)) {
    for (uint64_t i : range) {
      co_yield "\n  <result>";
      for (size_t j = 0; j < selectedColumnIndices.size(); ++j) {
        if (selectedColumnIndices[j].has_value()) {
          const auto& val = selectedColumnIndices[j].value();
          Id id = idTable(i, val.columnIndex_);
          co_yield idToXMLBinding(val.variable_, id, qet.getQec()->getIndex(),
                                  result->localVocab());
        }
      }
      co_yield "\n  </result>";
      cancellationHandle->throwIfCancelled();
    }
  }
  co_yield "\n</results>";
  co_yield "\n</sparql>";
}

// _____________________________________________________________________________
template <>
ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    selectQueryResultToStream<ad_utility::MediaType::sparqlJson>(
        const QueryExecutionTree& qet,
        const parsedQuery::SelectClause& selectClause,
        LimitOffsetClause limitAndOffset,
        CancellationHandle cancellationHandle) {
  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();
  LOG(DEBUG) << "Converting result IDs to their corresponding strings ..."
             << std::endl;
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, false);

  auto vars = selectClause.getSelectedVariablesAsStrings();
  std::ranges::for_each(vars, [](std::string& var) { var = var.substr(1); });
  nlohmann::json jsonVars = vars;
  co_yield absl::StrCat(R"({"head":{"vars":)", jsonVars.dump(),
                        R"(},"results":{"bindings":[)");

  QueryExecutionTree::ColumnIndicesAndTypes columns =
      qet.selectedVariablesToColumnIndices(selectClause, false);
  std::erase(columns, std::nullopt);
  if (columns.empty()) {
    co_yield "]}}";
    co_return;
  }

  auto getBinding = [&](const IdTable& idTable, const uint64_t& i) {
    nlohmann::ordered_json binding = {};
    for (const auto& column : columns) {
      auto optionalStringAndType = idToStringAndType(
          qet.getQec()->getIndex(), idTable(i, column->columnIndex_),
          result->localVocab());
      if (optionalStringAndType.has_value()) [[likely]] {
        const auto& [stringValue, xsdType] = optionalStringAndType.value();
        binding[column->variable_] =
            stringAndTypeToBinding(stringValue, xsdType);
      }
    }
    return binding.dump();
  };

  bool isFirstRow = true;
  for (const auto& [idTable, range] : getRowIndices(limitAndOffset, *result)) {
    for (uint64_t i : range) {
      if (!isFirstRow) [[likely]] {
        co_yield ",";
      }
      co_yield getBinding(idTable, i);
      cancellationHandle->throwIfCancelled();
      isFirstRow = false;
    }
  }

  co_yield "]}}";
  co_return;
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::constructQueryResultToStream(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    LimitOffsetClause limitAndOffset, std::shared_ptr<const Result> result,
    CancellationHandle cancellationHandle) {
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
  auto generator =
      constructQueryResultToTriples(qet, constructTriples, limitAndOffset,
                                    result, std::move(cancellationHandle));
  for (auto& triple : generator) {
    co_yield escapeFunction(std::move(triple.subject_));
    co_yield sep;
    co_yield escapeFunction(std::move(triple.predicate_));
    co_yield sep;
    co_yield escapeFunction(std::move(triple.object_));
    co_yield "\n";
  }
}

// _____________________________________________________________________________
cppcoro::generator<std::string>
ExportQueryExecutionTrees::convertStreamGeneratorForChunkedTransfer(
    ad_utility::streams::stream_generator streamGenerator) {
  // Immediately throw any exceptions that occur during the computation of the
  // first block outside the actual generator. That way we get a proper HTTP
  // response with error status codes etc. at least for those exceptions.
  // Note: `begin` advances until the first block.
  auto it = streamGenerator.begin();
  return [](auto innerGenerator, auto it) -> cppcoro::generator<std::string> {
    std::optional<std::string> exceptionMessage;
    try {
      for (; it != innerGenerator.end(); ++it) {
        co_yield std::move(*it);
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

// _____________________________________________________________________________
cppcoro::generator<std::string> ExportQueryExecutionTrees::computeResult(
    const ParsedQuery& parsedQuery, const QueryExecutionTree& qet,
    ad_utility::MediaType mediaType, const ad_utility::Timer& requestTimer,
    CancellationHandle cancellationHandle) {
  auto compute = [&]<MediaType format> {
    if constexpr (format == MediaType::qleverJson) {
      return computeResultAsQLeverJSON(parsedQuery, qet, requestTimer,
                                       std::move(cancellationHandle));
    }
    return parsedQuery.hasSelectClause()
               ? selectQueryResultToStream<format>(
                     qet, parsedQuery.selectClause(), parsedQuery._limitOffset,
                     std::move(cancellationHandle))
               : constructQueryResultToStream<format>(
                     qet, parsedQuery.constructClause().triples_,
                     parsedQuery._limitOffset, qet.getResult(true),
                     std::move(cancellationHandle));
  };

  using enum MediaType;

  static constexpr std::array supportedTypes{
      csv, tsv, octetStream, turtle, sparqlXml, sparqlJson, qleverJson};
  AD_CORRECTNESS_CHECK(ad_utility::contains(supportedTypes, mediaType));

  auto inner =
      ad_utility::ConstexprSwitch<csv, tsv, octetStream, turtle, sparqlXml,
                                  sparqlJson, qleverJson>(compute, mediaType);
  return convertStreamGeneratorForChunkedTransfer(std::move(inner));
}

// _____________________________________________________________________________
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::computeResultAsQLeverJSON(
    const ParsedQuery& query, const QueryExecutionTree& qet,
    const ad_utility::Timer& requestTimer,
    CancellationHandle cancellationHandle) {
  auto timeUntilFunctionCall = requestTimer.msecs();
  std::shared_ptr<const Result> result = qet.getResult(true);
  result->logResultSize();

  nlohmann::json jsonPrefix;

  jsonPrefix["query"] = query._originalString;
  jsonPrefix["status"] = "OK";
  jsonPrefix["warnings"] = qet.collectWarnings();
  if (query.hasSelectClause()) {
    jsonPrefix["selected"] =
        query.selectClause().getSelectedVariablesAsStrings();
  } else {
    jsonPrefix["selected"] =
        std::vector<std::string>{"?subject", "?predicate", "?object"};
  }

  std::string prefixStr = jsonPrefix.dump();
  co_yield absl::StrCat(prefixStr.substr(0, prefixStr.size() - 1),
                        R"(,"res":[)");

  auto bindings =
      query.hasSelectClause()
          ? selectQueryResultBindingsToQLeverJSON(
                qet, query.selectClause(), query._limitOffset,
                std::move(result), std::move(cancellationHandle))
          : constructQueryResultBindingsToQLeverJSON(
                qet, query.constructClause().triples_, query._limitOffset,
                std::move(result), std::move(cancellationHandle));

  size_t resultSize = 0;
  for (const std::string& b : bindings) {
    if (resultSize > 0) [[likely]] {
      co_yield ",";
    }
    co_yield b;
    ++resultSize;
  }

  RuntimeInformation runtimeInformation = qet.getRootOperation()->runtimeInfo();
  runtimeInformation.addLimitOffsetRow(query._limitOffset, false);

  auto timeResultComputation =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          timeUntilFunctionCall + runtimeInformation.totalTime_);

  nlohmann::json jsonSuffix;
  jsonSuffix["runtimeInformation"]["meta"] = nlohmann::ordered_json(
      qet.getRootOperation()->getRuntimeInfoWholeQuery());
  jsonSuffix["runtimeInformation"]["query_execution_tree"] =
      nlohmann::ordered_json(runtimeInformation);
  jsonSuffix["resultsize"] = resultSize;
  jsonSuffix["time"]["total"] =
      absl::StrCat(requestTimer.msecs().count(), "ms");
  jsonSuffix["time"]["computeResult"] =
      absl::StrCat(timeResultComputation.count(), "ms");

  co_yield absl::StrCat("],", jsonSuffix.dump().substr(1));
}
