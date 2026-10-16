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
#include "backports/algorithm.h"
#include "engine/CallFixedSize.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "global/RuntimeParameters.h"
#include "index/TripleComponentConversions.h"
#include "parser/BlankNodeAdder.h"
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
bool Proxy::bindsAllInputVariables(
    const VariableToColumnMap& variableColumns) const {
  return ql::ranges::all_of(config_.inputVariables_, [&](const auto& pair) {
    return variableColumns.contains(pair.second);
  });
}

// ____________________________________________________________________________
std::shared_ptr<Proxy> Proxy::addChild(
    std::shared_ptr<QueryExecutionTree> child) const {
  AD_CONTRACT_CHECK(!childOperation_.has_value());
  const auto& childVariables = child->getVariableColumns();
  AD_CONTRACT_CHECK(bindsAllInputVariables(childVariables));

  // The output variables and the row variable are bound by the response, so
  // they must not be bound by the child as well.
  auto checkNotBound = [&childVariables](const Variable& var,
                                         std::string_view parameter) {
    if (childVariables.contains(var)) {
      throw std::runtime_error(absl::StrCat(
          "The variable ", var.name(), " of the parameter `qlproxy:", parameter,
          "` is already bound by the enclosing graph pattern"));
    }
  };
  for (const auto& [name, var] : config_.outputVariables_) {
    checkNotBound(var, absl::StrCat("output-", name));
  }
  checkNotBound(config_.rowVariable_.second, "output-row");

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
  return getChildWidth() + config_.outputVariables_.size() + 1;
}

// ____________________________________________________________________________
size_t Proxy::getChildWidth() const {
  return childOperation_.has_value() ? childOperation_.value()->getResultWidth()
                                     : config_.inputVariables_.size();
}

// ____________________________________________________________________________
VariableToColumnMap Proxy::computeVariableToColumnMap() const {
  VariableToColumnMap map;

  // The columns of the child come first. Without a child, the input variables
  // stand in for them, so that the query planner connects the proxy with the
  // operations that bind them (and, via the output variables and the row
  // variable below, with the proxies that consume its output).
  if (childOperation_.has_value()) {
    map = childOperation_.value()->getVariableColumns();
  } else {
    for (size_t i = 0; i < config_.inputVariables_.size(); ++i) {
      map[config_.inputVariables_[i].second] = makePossiblyUndefinedColumn(i);
    }
  }

  // Then the output variables, then the row variable.
  size_t childWidth = getChildWidth();
  for (size_t i = 0; i < config_.outputVariables_.size(); ++i) {
    map[config_.outputVariables_[i].second] =
        makePossiblyUndefinedColumn(childWidth + i);
  }
  map[config_.rowVariable_.second] =
      makePossiblyUndefinedColumn(childWidth + config_.outputVariables_.size());
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
qlm::vector<QueryExecutionTree*> Proxy::getChildrenImpl() const {
  if (childOperation_.has_value()) {
    return {{childOperation_.value().get()}, allocator()};
  }
  return {{}, allocator()};
}

// ____________________________________________________________________________
bool Proxy::isDeterministicImpl() const {
  return getRuntimeParameter<&RuntimeParameters::cacheServiceResults_>();
}

// ____________________________________________________________________________
std::unique_ptr<Operation> Proxy::cloneImpl() const {
  auto clonedChild = childOperation_.has_value()
                         ? std::optional{childOperation_.value()->clone()}
                         : std::nullopt;
  return std::make_unique<Proxy>(getExecutionContext(), config_,
                                 std::move(clonedChild), sendRequestFunction_);
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
      url += boost::urls::encode(name, boost::urls::unreserved_chars) + "=" +
             boost::urls::encode(value, boost::urls::unreserved_chars);
    }
  }
  return url;
}

// ____________________________________________________________________________
std::string Proxy::serializeInputAsJson(const Result& childResult) const {
  nlohmann::json result;

  // Build the "head" section with the row variable first, then the input
  // variables.
  std::vector<std::string> varNames;
  varNames.push_back(config_.rowVariable_.first);
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
  const auto& idTable = childResult.idTableView();
  const auto& localVocab = childResult.localVocab();

  for (size_t row = 0; row < idTable.size(); ++row) {
    nlohmann::json binding;

    // Add the row index as an integer literal (1-based for user visibility).
    binding[config_.rowVariable_.first] = {
        {"type", "literal"},
        {"value", std::to_string(row + 1)},
        {"datatype", "http://www.w3.org/2001/XMLSchema#integer"}};

    // An undefined value has no binding, as in the export of a query result.
    for (const auto& [name, colIdx] : inputColumns) {
      auto valueObj = ExportQueryExecutionTrees::idToSparqlJsonBinding(
          getIndex(), idTable(row, colIdx), localVocab);
      if (valueObj.has_value()) {
        binding[name] = std::move(valueObj.value());
      }
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
                                     const IdTableView<0>& childTable,
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

    // Copy the output columns (all columns of the response except the row
    // column).
    size_t outputCol = 0;
    for (size_t col = 0; col < responseTable.numColumns(); ++col) {
      if (col == responseRowCol) {
        continue;
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
  // The query planner adds the enclosing graph pattern as the child only if it
  // binds all input variables (see `QueryPlanner::createProxyOperation`).
  if (!isConstructed()) {
    throw std::runtime_error(absl::StrCat(
        "The input variables ",
        absl::StrJoin(config_.inputVariables_, ", ",
                      [](std::string* out, const auto& pair) {
                        absl::StrAppend(out, pair.second.name());
                      }),
        " of the qlproxy service are not all bound by the enclosing graph "
        "pattern"));
  }

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
      "application/sparql-results+json", "application/sparql-results+json",
      /*maxRedirects=*/0);

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
  BlankNodeAdder blankNodeAdder{getIndex().getBlankNodeManager(),
                                getExecutionContext()->getAllocator()};

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
              binding[varName], getIndex(), blankNodeAdder);
        } else {
          tc = TripleComponent::UNDEF();
        }
        Id id = toValueId(std::move(tc), getIndex(), responseLocalVocab);
        responseTable(rowIdx, colIdx) = id;
      }
      checkCancellation();
    }
  }
  // The blank node `Id`s of the response are owned by the `LocalVocab` of the
  // `blankNodeAdder`.
  responseLocalVocab.mergeWith(blankNodeAdder.localVocab_);

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
      joinResponseWithChild(responseTable, 0, childResult->idTableView(),
                            childResult->localVocab(), resultLocalVocab);

  return {std::move(resultTable), resultSortedOn(),
          std::move(resultLocalVocab)};
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
