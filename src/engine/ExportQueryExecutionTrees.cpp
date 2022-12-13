//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "ExportQueryExecutionTrees.h"

#include "parser/RdfEscaping.h"
#include "util/http/MediaTypes.h"

// _____________________________________________________________________________
cppcoro::generator<QueryExecutionTree::StringTriple>
ExportQueryExecutionTrees::constructQueryToTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> res) {
  size_t upperBound = std::min<size_t>(offset + limit, res->_idTable.size());
  auto variableColumns = qet.getVariableColumns();
  for (size_t i = offset; i < upperBound; i++) {
    ConstructQueryEvaluationContext context{i, *res, variableColumns,
                                            qet.getQec()->getIndex()};
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
    }
  }
}

// _____________________________________________________________________________
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::constructQueryToTurtle(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> resultTable) {
  resultTable->logResultSize();
  auto generator = ExportQueryExecutionTrees::constructQueryToTriples(
      qet, constructTriples, limit, offset, resultTable);
  for (const auto& triple : generator) {
    co_yield triple._subject;
    co_yield ' ';
    co_yield triple._predicate;
    co_yield ' ';
    // NOTE: It's tempting to co_yield an expression using a ternary operator:
    // co_yield triple._object.starts_with('"')
    //     ? RdfEscaping::validRDFLiteralFromNormalized(triple._object)
    //     : triple._object;
    // but this leads to 1. segfaults in GCC (probably a compiler bug) and 2.
    // to unnecessary copies of `triple._object` in the `else` case because
    // the ternary always has to create a new prvalue.
    if (triple._object.starts_with('"')) {
      std::string objectAsValidRdfLiteral =
          RdfEscaping::validRDFLiteralFromNormalized(triple._object);
      co_yield objectAsValidRdfLiteral;
    } else {
      co_yield triple._object;
    }
    co_yield " .\n";
  }
}

// _____________________________________________________________________________
nlohmann::json ExportQueryExecutionTrees::constructQueryToQLeverJSONArray(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> res) {
  auto generator = constructQueryToTriples(qet, constructTriples, limit, offset,
                                           std::move(res));
  std::vector<std::array<std::string, 3>> jsonArray;
  for (auto& triple : generator) {
    jsonArray.push_back({std::move(triple._subject),
                         std::move(triple._predicate),
                         std::move(triple._object)});
  }
  return jsonArray;
}

// __________________________________________________________________________________________________________
nlohmann::json ExportQueryExecutionTrees::writeQLeverJsonTable(
    const QueryExecutionTree& qet, size_t from, size_t limit,
    const QueryExecutionTree::ColumnIndicesAndTypes& columns,
    shared_ptr<const ResultTable> resultTable) {
  if (!resultTable) {
    resultTable = qet.getResult();
  }
  const IdTable& data = resultTable->_idTable;
  nlohmann::json json = nlohmann::json::array();

  const auto upperBound = std::min(data.size(), limit + from);

  for (size_t rowIndex = from; rowIndex < upperBound; ++rowIndex) {
    json.emplace_back();
    auto& row = json.back();
    for (const auto& opt : columns) {
      if (!opt) {
        row.emplace_back(nullptr);
        continue;
      }
      const auto& currentId = data(rowIndex, opt->_columnIndex);
      const auto& optionalStringAndXsdType =
          idToStringAndType(qet, currentId, *resultTable);
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
  }
  return json;
}

// ___________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
ExportQueryExecutionTrees::idToStringAndType(const QueryExecutionTree& qet,
                                             Id id,
                                             const ResultTable& resultTable) {
  switch (id.getDatatype()) {
    case Datatype::Undefined:
      return std::nullopt;
    case Datatype::Double: {
      // Format as integer if fractional part is zero, let C++ decide otherwise.
      std::stringstream ss;
      double d = id.getDouble();
      double dIntPart;
      if (std::modf(d, &dIntPart) == 0.0) {
        ss << std::fixed << std::setprecision(0) << id.getDouble();
      } else {
        ss << d;
      }
      return std::pair{std::move(ss).str(), XSD_DECIMAL_TYPE};
    }
    case Datatype::Int:
      return std::pair{std::to_string(id.getInt()), XSD_INT_TYPE};
    case Datatype::VocabIndex: {
      std::optional<string> entity =
          qet.getQec()->getIndex().idToOptionalString(id);
      if (!entity.has_value()) {
        return std::nullopt;
      }
      return std::pair{std::move(entity.value()), nullptr};
    }
    case Datatype::LocalVocabIndex: {
      return std::pair{
          resultTable._localVocab->getWord(id.getLocalVocabIndex()), nullptr};
    }
    case Datatype::TextRecordIndex:
      return std::pair{
          qet.getQec()->getIndex().getTextExcerpt(id.getTextRecordIndex()),
          nullptr};
  }
  AD_FAIL();
}

// _____________________________________________________________________________
nlohmann::json ExportQueryExecutionTrees::selectQueryToSparqlJSON(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause, size_t limit, size_t offset,
    shared_ptr<const ResultTable> resultTable) {
  using nlohmann::json;

  // This might trigger the actual query processing.
  if (!resultTable) {
    resultTable = qet.getResult();
  }
  LOG(DEBUG) << "Finished computing the query result in the ID space. "
                "Resolving strings in result...\n";

  // Don't include the question mark in the variable names.
  QueryExecutionTree::ColumnIndicesAndTypes columns =
      qet.selectedVariablesToColumnIndices(selectClause, *resultTable, false);

  std::erase(columns, std::nullopt);

  if (columns.empty()) {
    return {std::vector<std::string>()};
  }

  const IdTable& idTable = resultTable->_idTable;

  json result;
  std::vector<std::string> selectedVars =
      selectClause.getSelectedVariablesAsStrings();
  // Strip the leading '?' from the variables, it is not part of the SPARQL JSON
  // output format.
  for (auto& var : selectedVars) {
    if (std::string_view{var}.starts_with('?')) {
      var = var.substr(1);
    }
  }
  result["head"]["vars"] = selectedVars;

  json bindings = json::array();

  const auto upperBound = std::min(idTable.size(), limit + offset);

  // Take a string from the vocabulary, deduce the type and
  // return a json dict that describes the binding
  auto stringToBinding = [](std::string_view entitystr) -> nlohmann::json {
    nlohmann::ordered_json b;
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
      size_t quote_pos = entitystr.rfind('"');
      if (quote_pos == std::string::npos) {
        // TEXT entries are currently not surrounded by quotes
        b["value"] = entitystr;
        b["type"] = "literal";
      } else {
        b["value"] = entitystr.substr(1, quote_pos - 1);
        b["type"] = "literal";
        // Look for a language tag or type.
        if (quote_pos < entitystr.size() - 1 &&
            entitystr[quote_pos + 1] == '@') {
          b["xml:lang"] = entitystr.substr(quote_pos + 2);
        } else if (quote_pos < entitystr.size() - 2 &&
                   entitystr[quote_pos + 1] == '^') {
          AD_CHECK(entitystr[quote_pos + 2] == '^');
          std::string_view datatype{entitystr};
          // remove the < angledBrackets> around the datatype IRI
          AD_CHECK(datatype.size() >= quote_pos + 5);
          datatype.remove_prefix(quote_pos + 4);
          datatype.remove_suffix(1);
          b["datatype"] = datatype;
          ;
        }
      }
    }
    return b;
  };

  for (size_t rowIndex = offset; rowIndex < upperBound; ++rowIndex) {
    // TODO: ordered_json` entries are ordered alphabetically, but insertion
    // order would be preferable.
    nlohmann::ordered_json binding;
    for (const auto& column : columns) {
      const auto& currentId = idTable(rowIndex, column->_columnIndex);
      const auto& optionalValue =
          idToStringAndType(qet, currentId, *resultTable);
      if (!optionalValue.has_value()) {
        continue;
      }
      const auto& [stringValue, xsdType] = optionalValue.value();
      nlohmann::ordered_json b;
      if (!xsdType) {
        // no xsdType, this means that `stringValue` is a plain string literal
        // or entity
        b = stringToBinding(stringValue);
      } else {
        b["value"] = stringValue;
        b["type"] = "literal";
        b["datatype"] = xsdType;
      }
      binding[column->_variable] = std::move(b);
    }
    bindings.emplace_back(std::move(binding));
  }
  result["results"]["bindings"] = std::move(bindings);
  return result;
}

// _____________________________________________________________________________
nlohmann::json ExportQueryExecutionTrees::selectQueryToQLeverJSONArray(
    const QueryExecutionTree& qet,
    const parsedQuery::SelectClause& selectClause, size_t limit, size_t offset,
    shared_ptr<const ResultTable> resultTable) {
  // They may trigger computation (but does not have to).
  if (!resultTable) {
    resultTable = qet.getResult();
  }
  LOG(DEBUG) << "Resolving strings for finished binary result...\n";
  QueryExecutionTree::ColumnIndicesAndTypes validIndices =
      qet.selectedVariablesToColumnIndices(selectClause, *resultTable, true);
  if (validIndices.empty()) {
    return {std::vector<std::string>()};
  }

  return ExportQueryExecutionTrees::writeQLeverJsonTable(
      qet, offset, limit, validIndices, std::move(resultTable));
}

using ExportSubFormat = QueryExecutionTree::ExportSubFormat;
using parsedQuery::SelectClause;

// _____________________________________________________________________________
template <QueryExecutionTree::ExportSubFormat format>
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::generateResults(const QueryExecutionTree& qet,
                                           const SelectClause& selectClause,
                                           size_t limit, size_t offset) {
  static_assert(format == ExportSubFormat::BINARY ||
                format == ExportSubFormat::CSV ||
                format == ExportSubFormat::TSV);

  // This call triggers the possibly expensive computation of the query result
  // unless the result is already cached.
  shared_ptr<const ResultTable> resultTable = qet.getResult();
  resultTable->logResultSize();
  LOG(DEBUG) << "Converting result IDs to their corresponding strings ..."
             << std::endl;
  auto selectedColumnIndices =
      qet.selectedVariablesToColumnIndices(selectClause, *resultTable, true);

  const auto& idTable = resultTable->_idTable;
  size_t upperBound = std::min<size_t>(offset + limit, idTable.size());

  // special case : binary export of IdTable
  if constexpr (format == ExportSubFormat::BINARY) {
    for (size_t i = offset; i < upperBound; ++i) {
      for (const auto& columnIndex : selectedColumnIndices) {
        if (columnIndex.has_value()) {
          co_yield std::string_view{reinterpret_cast<const char*>(&idTable(
                                        i, columnIndex.value()._columnIndex)),
                                    sizeof(Id)};
        }
      }
    }
    co_return;
  }

  static constexpr char sep = format == ExportSubFormat::TSV ? '\t' : ',';
  constexpr std::string_view sepView{&sep, 1};
  // Print header line
  const auto& variables = selectClause.getSelectedVariablesAsStrings();
  co_yield absl::StrJoin(variables, sepView);
  co_yield '\n';

  constexpr auto& escapeFunction = format == ExportSubFormat::TSV
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;

  const auto& qec = qet.getQec();
  for (size_t i = offset; i < upperBound; ++i) {
    for (size_t j = 0; j < selectedColumnIndices.size(); ++j) {
      if (selectedColumnIndices[j].has_value()) {
        const auto& val = selectedColumnIndices[j].value();
        Id id = idTable(i, val._columnIndex);
        switch (id.getDatatype()) {
          case Datatype::Undefined:
            break;
          case Datatype::Double:
            co_yield id.getDouble();
            break;
          case Datatype::Int:
            co_yield id.getInt();
            break;
          case Datatype::VocabIndex:
            co_yield escapeFunction(
                qec->getIndex()
                    .getVocab()
                    .indexToOptionalString(id.getVocabIndex())
                    .value_or(""));
            break;
          case Datatype::LocalVocabIndex:
            co_yield escapeFunction(
                resultTable->_localVocab->getWord(id.getLocalVocabIndex()));
            break;
          case Datatype::TextRecordIndex:
            co_yield escapeFunction(
                qec->getIndex().getTextExcerpt(id.getTextRecordIndex()));
            break;
          default:
            AD_THROW(ad_semsearch::Exception::INVALID_PARAMETER_VALUE,
                     "Cannot deduce output type.");
        }
      }
      co_yield j + 1 < selectedColumnIndices.size() ? sep : '\n';
    }
  }
  LOG(DEBUG) << "Done creating readable result.\n";
}

// Instantiate template function for all enum types
// TODO<joka921> Move the function that calls those instantiations also into
// this file, then we can get rid of the explicit instantiations.
template ad_utility::streams::stream_generator
ExportQueryExecutionTrees::generateResults<
    QueryExecutionTree::ExportSubFormat::CSV>(const QueryExecutionTree& qet,
                                              const SelectClause& selectClause,
                                              size_t limit, size_t offset);

template ad_utility::streams::stream_generator
ExportQueryExecutionTrees::generateResults<
    QueryExecutionTree::ExportSubFormat::TSV>(const QueryExecutionTree& qet,
                                              const SelectClause& selectClause,
                                              size_t limit, size_t offset);

template ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    generateResults<QueryExecutionTree::ExportSubFormat::BINARY>(
        const QueryExecutionTree& qet, const SelectClause& selectClause,
        size_t limit, size_t offset);

// _____________________________________________________________________________

// _____________________________________________________________________________
template <QueryExecutionTree::ExportSubFormat format>
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::writeRdfGraphSeparatedValues(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
    size_t offset, std::shared_ptr<const ResultTable> resultTable) {
  static_assert(format == ExportSubFormat::BINARY ||
                format == ExportSubFormat::CSV ||
                format == ExportSubFormat::TSV);
  if constexpr (format == ExportSubFormat::BINARY) {
    throw std::runtime_error{
        "Binary export is not supported for CONSTRUCT queries"};
  }
  resultTable->logResultSize();
  constexpr auto& escapeFunction = format == ExportSubFormat::TSV
                                       ? RdfEscaping::escapeForTsv
                                       : RdfEscaping::escapeForCsv;
  constexpr char sep = format == ExportSubFormat::TSV ? '\t' : ',';
  auto generator = ExportQueryExecutionTrees::constructQueryToTriples(
      qet, constructTriples, limit, offset, resultTable);
  for (auto& triple : generator) {
    co_yield escapeFunction(std::move(triple._subject));
    co_yield sep;
    co_yield escapeFunction(std::move(triple._predicate));
    co_yield sep;
    co_yield escapeFunction(std::move(triple._object));
    co_yield "\n";
  }
}

// Instantiate template function for all enum types

// TODO<joka921> Move the function that calls those instantiations also into
// this file, then we can get rid of the explicit instantiations.
template ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    writeRdfGraphSeparatedValues<QueryExecutionTree::ExportSubFormat::CSV>(
        const QueryExecutionTree& qet,
        const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
        size_t offset, std::shared_ptr<const ResultTable> res);

template ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    writeRdfGraphSeparatedValues<QueryExecutionTree::ExportSubFormat::TSV>(
        const QueryExecutionTree& qet,
        const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
        size_t offset, std::shared_ptr<const ResultTable> res);

template ad_utility::streams::stream_generator ExportQueryExecutionTrees::
    writeRdfGraphSeparatedValues<QueryExecutionTree::ExportSubFormat::BINARY>(
        const QueryExecutionTree& qet,
        const ad_utility::sparql_types::Triples& constructTriples, size_t limit,
        size_t offset, std::shared_ptr<const ResultTable> res);

// _____________________________________________________________________________
nlohmann::json ExportQueryExecutionTrees::queryToQLeverJSON(
    const ParsedQuery& query, const QueryExecutionTree& qet,
    ad_utility::Timer& requestTimer, size_t maxSend) {
  shared_ptr<const ResultTable> resultTable = qet.getResult();
  requestTimer.stop();
  resultTable->logResultSize();
  off_t compResultUsecs = requestTimer.usecs();
  size_t resultSize = resultTable->size();

  nlohmann::json j;

  j["query"] = query._originalString;
  j["status"] = "OK";
  j["warnings"] = qet.collectWarnings();
  if (query.hasSelectClause()) {
    j["selected"] = query.selectClause().getSelectedVariablesAsStrings();
  } else {
    j["selected"] =
        std::vector<std::string>{"?subject", "?predicate", "?object"};
  }

  j["runtimeInformation"] =
      nlohmann::ordered_json(qet.getRootOperation()->getRuntimeInfo());

  {
    size_t limit = std::min(query._limitOffset._limit, maxSend);
    size_t offset = query._limitOffset._offset;
    requestTimer.cont();
    j["res"] = query.hasSelectClause()
                   ? ExportQueryExecutionTrees::selectQueryToQLeverJSONArray(
                         qet, query.selectClause(), limit, offset,
                         std::move(resultTable))
                   : ExportQueryExecutionTrees::constructQueryToQLeverJSONArray(
                         qet, query.constructClause().triples_, limit, offset,
                         std::move(resultTable));
    requestTimer.stop();
  }
  j["resultsize"] = query.hasSelectClause() ? resultSize : j["res"].size();

  requestTimer.stop();
  j["time"]["total"] =
      std::to_string(static_cast<double>(requestTimer.usecs()) / 1000.0) + "ms";
  j["time"]["computeResult"] =
      std::to_string(static_cast<double>(compResultUsecs) / 1000.0) + "ms";

  return j;
}

// _____________________________________________________________________________
ad_utility::streams::stream_generator
ExportQueryExecutionTrees::composeResponseSepValues(
    const ParsedQuery& query, const QueryExecutionTree& qet,
    ad_utility::MediaType type) {
  auto compute = [&]<QueryExecutionTree::ExportSubFormat format> {
    size_t limit = query._limitOffset._limit;
    size_t offset = query._limitOffset._offset;
    return query.hasSelectClause()
               ? ExportQueryExecutionTrees::generateResults<format>(
                     qet, query.selectClause(), limit, offset)
               : ExportQueryExecutionTrees::writeRdfGraphSeparatedValues<
                     format>(qet, query.constructClause().triples_, limit,
                             offset, qet.getResult());
  };
  // TODO<joka921> Clean this up by removing the (unused) `ExportSubFormat`
  // enum, And maybe by a `switch-constexpr`-abstraction
  if (type == ad_utility::MediaType::csv) {
    return compute
        .template operator()<QueryExecutionTree::ExportSubFormat::CSV>();
  } else if (type == ad_utility::MediaType::tsv) {
    return compute
        .template operator()<QueryExecutionTree::ExportSubFormat::TSV>();
  } else if (type == ad_utility::MediaType::octetStream) {
    return compute
        .template operator()<QueryExecutionTree::ExportSubFormat::BINARY>();
  } else if (type == ad_utility::MediaType::turtle) {
    return composeTurtleResponse(query, qet);
  }
  AD_FAIL();
}

ad_utility::streams::stream_generator
ExportQueryExecutionTrees::composeTurtleResponse(
    const ParsedQuery& query, const QueryExecutionTree& qet) {
  if (!query.hasConstructClause()) {
    throw std::runtime_error{
        "RDF Turtle as an export format is only supported for CONSTRUCT "
        "queries"};
  }
  size_t limit = query._limitOffset._limit;
  size_t offset = query._limitOffset._offset;
  return ExportQueryExecutionTrees::constructQueryToTurtle(
      qet, query.constructClause().triples_, limit, offset, qet.getResult());
}
