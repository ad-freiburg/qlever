// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/Proxy.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <boost/url.hpp>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/CallFixedSize.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "global/RuntimeParameters.h"
#include "util/Exception.h"
#include "util/LazyJsonParser.h"
#include "util/SparqlJsonBindingUtils.h"
#include "util/http/HttpUtils.h"
#include "util/json.h"

// ____________________________________________________________________________
Proxy::Proxy(QueryExecutionContext* qec, parsedQuery::ProxyConfiguration config,
             std::optional<std::shared_ptr<QueryExecutionTree>> childOperation,
             SendRequestType sendRequestFunction)
    : Operation{qec},
      config_{std::move(config)},
      childOperation_{std::move(childOperation)},
      sendRequestFunction_{std::move(sendRequestFunction)} {}

// ____________________________________________________________________________
std::shared_ptr<Proxy> Proxy::addChild(
    std::shared_ptr<QueryExecutionTree> child) const {
  return std::make_shared<Proxy>(getExecutionContext(), config_,
                                 std::move(child), sendRequestFunction_);
}

// ____________________________________________________________________________
std::string Proxy::getCacheKeyImpl() const {
  if (getRuntimeParameter<&RuntimeParameters::cacheServiceResults_>()) {
    // Build a cache key from the configuration.
    std::string key = absl::StrCat("PROXY ", config_.endpoint_);
    for (const auto& [name, var] : config_.inputVariables_) {
      absl::StrAppend(&key, " INPUT:", name, "=", var.name());
    }
    for (const auto& [name, var] : config_.outputVariables_) {
      absl::StrAppend(&key, " OUTPUT:", name, "=", var.name());
    }
    absl::StrAppend(&key, " ROW:", config_.rowVariable_.first, "=",
                    config_.rowVariable_.second.name());
    for (const auto& [name, value] : config_.parameters_) {
      absl::StrAppend(&key, " PARAM:", name, "=", value);
    }
    // Include the child's cache key since the input bindings depend on it.
    if (childOperation_.has_value()) {
      absl::StrAppend(
          &key, " CHILD:{",
          childOperation_.value()->getRootOperation()->getCacheKey(), "}");
    }
    return key;
  }
  // Don't cache proxy results as they depend on external state.
  return absl::StrCat("PROXY ", cacheBreaker_);
}

// ____________________________________________________________________________
std::string Proxy::getDescriptor() const {
  return absl::StrCat("Proxy to ", config_.endpoint_);
}

// ____________________________________________________________________________
size_t Proxy::getResultWidth() const {
  if (!childOperation_.has_value() && !config_.inputVariables_.empty()) {
    // Before construction and we need input variables: advertise them for
    // joining.
    return config_.inputVariables_.size();
  } else if (!childOperation_.has_value()) {
    // No child and no input variables: output variables + row variable.
    return config_.outputVariables_.size() + 1;
  }
  // After construction: child columns + output columns + row variable.
  return childOperation_.value()->getResultWidth() +
         config_.outputVariables_.size() + 1;
}

// ____________________________________________________________________________
VariableToColumnMap Proxy::computeVariableToColumnMap() const {
  VariableToColumnMap map;

  if (!childOperation_.has_value() && !config_.inputVariables_.empty()) {
    // When not yet constructed and we need input variables, advertise them
    // so the query planner knows what to join with.
    for (size_t i = 0; i < config_.inputVariables_.size(); ++i) {
      const auto& [name, var] = config_.inputVariables_[i];
      map[var] = makePossiblyUndefinedColumn(i);
    }
  } else if (childOperation_.has_value()) {
    // When constructed, return child variables + output variables + row
    // variable.
    const auto& childVarColMap = childOperation_.value()->getVariableColumns();
    for (const auto& [var, colInfo] : childVarColMap) {
      map[var] = colInfo;
    }
    // Output variables come after child columns.
    size_t childWidth = childOperation_.value()->getResultWidth();
    for (size_t i = 0; i < config_.outputVariables_.size(); ++i) {
      const auto& [name, var] = config_.outputVariables_[i];
      map[var] = makePossiblyUndefinedColumn(childWidth + i);
    }
    // Row variable comes last.
    map[config_.rowVariable_.second] = makePossiblyUndefinedColumn(
        childWidth + config_.outputVariables_.size());
  } else {
    // No input variables and no child: just output variables + row variable.
    for (size_t i = 0; i < config_.outputVariables_.size(); ++i) {
      const auto& [name, var] = config_.outputVariables_[i];
      map[var] = makePossiblyUndefinedColumn(i);
    }
    map[config_.rowVariable_.second] =
        makePossiblyUndefinedColumn(config_.outputVariables_.size());
  }
  return map;
}

// ____________________________________________________________________________
float Proxy::getMultiplicity([[maybe_unused]] size_t col) { return 1.0f; }

// ____________________________________________________________________________
uint64_t Proxy::getSizeEstimateBeforeLimit() {
  // We don't know the result size, use a conservative estimate.
  return 100'000;
}

// ____________________________________________________________________________
size_t Proxy::getCostEstimate() { return 10 * getSizeEstimateBeforeLimit(); }

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> Proxy::getChildren() {
  if (childOperation_.has_value()) {
    return {childOperation_.value().get()};
  }
  return {};
}

// ____________________________________________________________________________
std::unique_ptr<Operation> Proxy::cloneImpl() const {
  return std::make_unique<Proxy>(getExecutionContext(), config_,
                                 childOperation_, sendRequestFunction_);
}

// ____________________________________________________________________________
std::string Proxy::buildUrlWithParams() const {
  std::string url = config_.endpoint_;
  if (!config_.parameters_.empty()) {
    url += "?";
    bool first = true;
    for (const auto& [name, value] : config_.parameters_) {
      if (!first) {
        url += "&";
      }
      first = false;
      // URL-encode the value (simple encoding for common characters).
      url += name + "=" +
             boost::urls::encode(value, boost::urls::unreserved_chars);
    }
  }
  return url;
}

// ____________________________________________________________________________
std::string Proxy::serializeInputAsJson(const Result& childResult) const {
  nlohmann::json result;

  // Build the "head" section with variable names, including the row variable.
  std::vector<std::string> varNames;
  varNames.push_back(config_.rowVariable_.first);  // Row variable first
  for (const auto& [name, var] : config_.inputVariables_) {
    varNames.push_back(name);
  }
  result["head"]["vars"] = varNames;

  // Get the column indices for input variables from the child result.
  const auto& childVarColMap = childOperation_.value()->getVariableColumns();
  std::vector<std::pair<std::string, ColumnIndex>> inputColumns;
  for (const auto& [name, var] : config_.inputVariables_) {
    auto it = childVarColMap.find(var);
    if (it == childVarColMap.end()) {
      throw std::runtime_error(
          absl::StrCat("Input variable ", var.name(),
                       " not found in input. Available variables: ",
                       absl::StrJoin(childVarColMap, ", ",
                                     [](std::string* out, const auto& p) {
                                       absl::StrAppend(out, p.first.name());
                                     })));
    }
    inputColumns.emplace_back(name, it->second.columnIndex_);
  }

  // Build the "results.bindings" array.
  nlohmann::json bindings = nlohmann::json::array();
  const auto& idTable = childResult.idTable();
  const auto& localVocab = childResult.localVocab();

  for (size_t row = 0; row < idTable.size(); ++row) {
    nlohmann::json binding;

    // Add the row index as an integer literal (1-based for user visibility).
    binding[config_.rowVariable_.first] = {
        {"type", "literal"},
        {"value", std::to_string(row + 1)},
        {"datatype", "http://www.w3.org/2001/XMLSchema#integer"}};

    for (const auto& [name, colIdx] : inputColumns) {
      Id id = idTable(row, colIdx);
      if (id.isUndefined()) {
        // Skip undefined values (they won't appear in the binding).
        continue;
      }

      // Convert the ID to a JSON binding.
      nlohmann::json valueObj;
      auto optStringAndType =
          ExportQueryExecutionTrees::idToStringAndType<true>(getIndex(), id,
                                                             localVocab);

      if (!optStringAndType.has_value()) {
        continue;
      }

      const auto& [value, type] = optStringAndType.value();

      // Determine the RDF term type for the JSON.
      switch (id.getDatatype()) {
        case Datatype::VocabIndex:
        case Datatype::LocalVocabIndex: {
          // Could be IRI or literal - check the string representation.
          auto litOrIri =
              ExportQueryExecutionTrees::getLiteralOrIriFromVocabIndex(
                  getIndex(), id, localVocab);
          if (litOrIri.isIri()) {
            valueObj["type"] = "uri";
            valueObj["value"] = value;
          } else {
            valueObj["type"] = "literal";
            valueObj["value"] = value;
            if (litOrIri.hasLanguageTag()) {
              valueObj["xml:lang"] =
                  std::string(asStringViewUnsafe(litOrIri.getLanguageTag()));
            } else if (litOrIri.hasDatatype()) {
              valueObj["datatype"] =
                  std::string(asStringViewUnsafe(litOrIri.getDatatype()));
            }
          }
          break;
        }
        case Datatype::BlankNodeIndex:
          valueObj["type"] = "bnode";
          valueObj["value"] = value;
          break;
        default:
          // For encoded values (int, double, bool, date, etc.), treat as
          // literal.
          valueObj["type"] = "literal";
          valueObj["value"] = value;
          if (type != nullptr) {
            valueObj["datatype"] = type;
          }
          break;
      }
      binding[name] = valueObj;
    }
    bindings.push_back(binding);
    checkCancellation();
  }

  result["results"]["bindings"] = bindings;
  return result.dump();
}

// ____________________________________________________________________________
IdTable Proxy::joinResponseWithChild(const IdTable& responseTable,
                                     ColumnIndex responseRowCol,
                                     const IdTable& childTable,
                                     const LocalVocab& childLocalVocab,
                                     LocalVocab& resultLocalVocab) const {
  // Result width: child columns + output columns + row variable.
  size_t childWidth = childTable.numColumns();
  size_t outputWidth = config_.outputVariables_.size();
  IdTable result{childWidth + outputWidth + 1,
                 getExecutionContext()->getAllocator()};

  // Merge local vocabs.
  resultLocalVocab.mergeWith(std::span{&childLocalVocab, 1});

  // For each response row, look up the child row by the row index and combine.
  for (size_t respRow = 0; respRow < responseTable.size(); ++respRow) {
    checkCancellation();

    // Get the row index from the response (1-based).
    Id rowId = responseTable(respRow, responseRowCol);
    if (rowId.getDatatype() != Datatype::Int) {
      throw std::runtime_error(
          "qlproxy endpoint returned non-integer row index");
    }
    int64_t rowIdx1Based = rowId.getInt();
    if (rowIdx1Based < 1 ||
        static_cast<size_t>(rowIdx1Based) > childTable.size()) {
      throw std::runtime_error(absl::StrCat(
          "qlproxy endpoint returned invalid row index: ", rowIdx1Based,
          " (expected 1 to ", childTable.size(), ")"));
    }
    size_t rowIdx = static_cast<size_t>(rowIdx1Based - 1);

    // Add a new row to the result.
    result.emplace_back();
    size_t resultRow = result.size() - 1;

    // Copy child columns.
    for (size_t col = 0; col < childWidth; ++col) {
      result(resultRow, col) = childTable(rowIdx, col);
    }

    // Copy output columns (skip the row column in response).
    size_t outputCol = 0;
    for (size_t col = 0; col < responseTable.numColumns(); ++col) {
      if (col == responseRowCol) {
        continue;  // Skip the row variable column.
      }
      result(resultRow, childWidth + outputCol) = responseTable(respRow, col);
      ++outputCol;
    }

    // Row variable comes last.
    result(resultRow, childWidth + outputWidth) = rowId;
  }

  return result;
}

// ____________________________________________________________________________
Result Proxy::computeResult([[maybe_unused]] bool requestLaziness) {
  // First, compute the child result to get input bindings.
  std::shared_ptr<const Result> childResult;
  if (childOperation_.has_value()) {
    childResult = childOperation_.value()->getResult();
  }

  // Build the URL with query parameters.
  std::string urlStr = buildUrlWithParams();
  ad_utility::httpUtils::Url url{urlStr};

  // Serialize input as SPARQL Results JSON (includes row indices).
  std::string payload;
  if (childResult) {
    payload = serializeInputAsJson(*childResult);
  } else {
    // Empty payload - still send valid JSON structure with row variable.
    nlohmann::json emptyResult;
    std::vector<std::string> varNames;
    varNames.push_back(config_.rowVariable_.first);
    for (const auto& [name, var] : config_.inputVariables_) {
      varNames.push_back(name);
    }
    emptyResult["head"]["vars"] = varNames;
    emptyResult["results"]["bindings"] = nlohmann::json::array();
    payload = emptyResult.dump();
  }

  AD_LOG_INFO << "Sending qlproxy request to " << urlStr << std::endl;
  AD_LOG_DEBUG << "Payload: " << payload << std::endl;

  // Send the request.
  HttpOrHttpsResponse response = sendRequestFunction_(
      url, cancellationHandle_, boost::beast::http::verb::post, payload,
      "application/sparql-results+json", "application/sparql-results+json");

  // Check response status.
  if (response.status_ != boost::beast::http::status::ok) {
    auto first100 = std::move(response).readResponseHead(100);
    throw std::runtime_error(absl::StrCat(
        "qlproxy endpoint responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_)),
        ". Response: ", first100));
  }

  // Check content type.
  if (!ql::starts_with(ad_utility::utf8ToLower(response.contentType_),
                       "application/sparql-results+json")) {
    auto first100 = std::move(response).readResponseHead(100);
    throw std::runtime_error(absl::StrCat(
        "qlproxy endpoint sent unexpected content type: '",
        response.contentType_,
        "'. Expected 'application/sparql-results+json'. Response: ", first100));
  }

  // Parse the response.
  auto body = ad_utility::LazyJsonParser::parse(std::move(response.body_),
                                                {"results", "bindings"});

  // Build the response variable names: row variable first, then output vars.
  const std::string& rowVarName = config_.rowVariable_.first;
  std::vector<std::string> responseVarNames;
  responseVarNames.push_back(rowVarName);
  for (const auto& [name, var] : config_.outputVariables_) {
    responseVarNames.push_back(name);
  }

  // Create a temporary table for the response (row var + output vars).
  // The row variable is at column 0.
  size_t responseWidth = responseVarNames.size();
  IdTable responseTable{responseWidth, getExecutionContext()->getAllocator()};
  LocalVocab responseLocalVocab;
  ad_utility::HashMap<std::string, Id> blankNodeMap;

  for (const nlohmann::json& partJson : body) {
    checkCancellation();

    if (!partJson.contains("results") ||
        !partJson["results"].contains("bindings") ||
        !partJson["results"]["bindings"].is_array()) {
      continue;
    }

    for (const auto& binding : partJson["results"]["bindings"]) {
      // Check that the row variable is present.
      if (!binding.contains(rowVarName)) {
        throw std::runtime_error(absl::StrCat(
            "qlproxy endpoint response missing required row variable '",
            rowVarName, "'"));
      }

      responseTable.emplace_back();
      size_t rowIdx = responseTable.size() - 1;

      for (size_t colIdx = 0; colIdx < responseVarNames.size(); ++colIdx) {
        const std::string& varName = responseVarNames[colIdx];
        TripleComponent tc;
        if (binding.contains(varName)) {
          tc = sparqlJsonBindingUtils::bindingToTripleComponent(
              binding[varName], getIndex(), blankNodeMap, &responseLocalVocab,
              getIndex().getBlankNodeManager());
        } else {
          tc = TripleComponent::UNDEF();
        }
        Id id =
            std::move(tc).toValueId(getIndex().getVocab(), responseLocalVocab,
                                    getIndex().encodedIriManager());
        responseTable(rowIdx, colIdx) = id;
      }
      checkCancellation();
    }
  }

  // If there's no child (no input variables), return output columns + row
  // variable.
  if (!childResult) {
    // Reorder: output columns first, then row variable (which is at col 0 in
    // response).
    IdTable outputWithRow{config_.outputVariables_.size() + 1,
                          getExecutionContext()->getAllocator()};
    for (size_t row = 0; row < responseTable.size(); ++row) {
      outputWithRow.emplace_back();
      // Output columns (cols 1..n in response).
      for (size_t col = 1; col < responseTable.numColumns(); ++col) {
        outputWithRow(row, col - 1) = responseTable(row, col);
      }
      // Row variable last.
      outputWithRow(row, config_.outputVariables_.size()) =
          responseTable(row, 0);
    }
    return {std::move(outputWithRow), resultSortedOn(),
            std::move(responseLocalVocab)};
  }

  // Join the response with the child result based on the row variable.
  LocalVocab resultLocalVocab = std::move(responseLocalVocab);
  IdTable resultTable =
      joinResponseWithChild(responseTable, 0, childResult->idTable(),
                            childResult->localVocab(), resultLocalVocab);

  return {std::move(resultTable), resultSortedOn(),
          std::move(resultLocalVocab)};
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
