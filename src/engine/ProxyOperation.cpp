// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "engine/ProxyOperation.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <boost/url.hpp>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/CallFixedSize.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "util/Exception.h"
#include "util/LazyJsonParser.h"
#include "util/SparqlJsonBindingUtils.h"
#include "util/http/HttpUtils.h"
#include "util/json.h"

// ____________________________________________________________________________
ProxyOperation::ProxyOperation(
    QueryExecutionContext* qec, parsedQuery::ProxyConfiguration config,
    std::optional<std::shared_ptr<QueryExecutionTree>> childOperation,
    SendRequestType sendRequestFunction)
    : Operation{qec},
      config_{std::move(config)},
      childOperation_{std::move(childOperation)},
      sendRequestFunction_{std::move(sendRequestFunction)} {}

// ____________________________________________________________________________
std::shared_ptr<ProxyOperation> ProxyOperation::addChild(
    std::shared_ptr<QueryExecutionTree> child) const {
  return std::make_shared<ProxyOperation>(
      getExecutionContext(), config_, std::move(child), sendRequestFunction_);
}

// ____________________________________________________________________________
std::string ProxyOperation::getCacheKeyImpl() const {
  // Don't cache proxy results as they depend on external state.
  return absl::StrCat("PROXY ", cacheBreaker_);
}

// ____________________________________________________________________________
std::string ProxyOperation::getDescriptor() const {
  return absl::StrCat("Proxy to ", config_.endpoint_);
}

// ____________________________________________________________________________
size_t ProxyOperation::getResultWidth() const {
  return config_.resultVariables_.size();
}

// ____________________________________________________________________________
VariableToColumnMap ProxyOperation::computeVariableToColumnMap() const {
  VariableToColumnMap map;

  if (!childOperation_.has_value() && !config_.payloadVariables_.empty()) {
    // When not yet constructed and we need payload variables, advertise them
    // so the query planner knows what to join with.
    for (size_t i = 0; i < config_.payloadVariables_.size(); ++i) {
      const auto& [name, var] = config_.payloadVariables_[i];
      map[var] = makePossiblyUndefinedColumn(i);
    }
  } else {
    // When constructed (or no payload variables needed), return result
    // variables.
    for (size_t i = 0; i < config_.resultVariables_.size(); ++i) {
      const auto& [name, var] = config_.resultVariables_[i];
      map[var] = makePossiblyUndefinedColumn(i);
    }
  }
  return map;
}

// ____________________________________________________________________________
float ProxyOperation::getMultiplicity([[maybe_unused]] size_t col) {
  return 1.0f;
}

// ____________________________________________________________________________
uint64_t ProxyOperation::getSizeEstimateBeforeLimit() {
  // We don't know the result size, use a conservative estimate.
  return 100'000;
}

// ____________________________________________________________________________
size_t ProxyOperation::getCostEstimate() {
  return 10 * getSizeEstimateBeforeLimit();
}

// ____________________________________________________________________________
std::vector<QueryExecutionTree*> ProxyOperation::getChildren() {
  if (childOperation_.has_value()) {
    return {childOperation_.value().get()};
  }
  return {};
}

// ____________________________________________________________________________
std::unique_ptr<Operation> ProxyOperation::cloneImpl() const {
  return std::make_unique<ProxyOperation>(
      getExecutionContext(), config_, childOperation_, sendRequestFunction_);
}

// ____________________________________________________________________________
std::string ProxyOperation::buildUrlWithParams() const {
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
std::string ProxyOperation::serializePayloadAsJson(
    const Result& childResult) const {
  nlohmann::json result;

  // Build the "head" section with variable names.
  std::vector<std::string> varNames;
  for (const auto& [name, var] : config_.payloadVariables_) {
    varNames.push_back(name);
  }
  result["head"]["vars"] = varNames;

  // Get the column indices for payload variables from the child result.
  const auto& childVarColMap = childOperation_.value()->getVariableColumns();
  std::vector<std::pair<std::string, ColumnIndex>> payloadColumns;
  for (const auto& [name, var] : config_.payloadVariables_) {
    auto it = childVarColMap.find(var);
    if (it == childVarColMap.end()) {
      throw std::runtime_error(
          absl::StrCat("Payload variable ", var.name(),
                       " not found in input. Available variables: ",
                       absl::StrJoin(childVarColMap, ", ",
                                     [](std::string* out, const auto& p) {
                                       absl::StrAppend(out, p.first.name());
                                     })));
    }
    payloadColumns.emplace_back(name, it->second.columnIndex_);
  }

  // Build the "results.bindings" array.
  nlohmann::json bindings = nlohmann::json::array();
  const auto& idTable = childResult.idTable();
  const auto& localVocab = childResult.localVocab();

  for (size_t row = 0; row < idTable.size(); ++row) {
    nlohmann::json binding;
    for (const auto& [name, colIdx] : payloadColumns) {
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
Result ProxyOperation::computeResult([[maybe_unused]] bool requestLaziness) {
  // First, compute the child result to get payload bindings.
  std::shared_ptr<const Result> childResult;
  if (childOperation_.has_value()) {
    childResult = childOperation_.value()->getResult();
  }

  // Build the URL with query parameters.
  std::string urlStr = buildUrlWithParams();
  ad_utility::httpUtils::Url url{urlStr};

  // Serialize payload as SPARQL Results JSON.
  std::string payload;
  if (childResult && !config_.payloadVariables_.empty()) {
    payload = serializePayloadAsJson(*childResult);
  } else {
    // Empty payload - still send valid JSON structure.
    nlohmann::json emptyResult;
    std::vector<std::string> varNames;
    for (const auto& [name, var] : config_.payloadVariables_) {
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

  // Build the result variable names (without ? prefix).
  std::vector<std::string> resultVarNames;
  for (const auto& [name, var] : config_.resultVariables_) {
    resultVarNames.push_back(name);
  }

  // Create the result table.
  IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};
  LocalVocab localVocab;
  ad_utility::HashMap<std::string, Id> blankNodeMap;

  for (const nlohmann::json& partJson : body) {
    checkCancellation();

    if (!partJson.contains("results") ||
        !partJson["results"].contains("bindings") ||
        !partJson["results"]["bindings"].is_array()) {
      continue;
    }

    for (const auto& binding : partJson["results"]["bindings"]) {
      idTable.emplace_back();
      size_t rowIdx = idTable.size() - 1;

      for (size_t colIdx = 0; colIdx < resultVarNames.size(); ++colIdx) {
        const std::string& varName = resultVarNames[colIdx];
        TripleComponent tc;
        if (binding.contains(varName)) {
          tc = sparqlJsonBindingUtils::bindingToTripleComponent(
              binding[varName], getIndex(), blankNodeMap, &localVocab,
              getIndex().getBlankNodeManager());
        } else {
          tc = TripleComponent::UNDEF();
        }
        Id id = std::move(tc).toValueId(getIndex().getVocab(), localVocab,
                                        getIndex().encodedIriManager());
        idTable(rowIdx, colIdx) = id;
      }
      checkCancellation();
    }
  }

  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
