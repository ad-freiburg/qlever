// Copyright 2022 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include "engine/Service.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include "engine/CallFixedSize.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/VariableToColumnMap.h"
#include "global/RuntimeParameters.h"
#include "parser/RdfParser.h"
#include "parser/TokenizerCtre.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/Views.h"
#include "util/http/HttpUtils.h"

// ____________________________________________________________________________
Service::Service(QueryExecutionContext* qec,
                 parsedQuery::Service parsedServiceClause,
                 GetResultFunction getResultFunction,
                 std::shared_ptr<QueryExecutionTree> siblingTree)
    : Operation{qec},
      parsedServiceClause_{std::move(parsedServiceClause)},
      getResultFunction_{std::move(getResultFunction)},
      siblingTree_{std::move(siblingTree)} {}

// ____________________________________________________________________________
std::string Service::getCacheKeyImpl() const {
  std::ostringstream os;
  // TODO: This duplicates code in GraphPatternOperation.cpp .
  os << "SERVICE " << parsedServiceClause_.serviceIri_.toSparql() << " {\n"
     << parsedServiceClause_.prologue_ << "\n"
     << parsedServiceClause_.graphPatternAsString_ << "\n";
  if (siblingTree_ != nullptr) {
    os << siblingTree_->getRootOperation()->getCacheKey() << "\n";
  }
  os << "}\n";
  return std::move(os).str();
}

// ____________________________________________________________________________
std::string Service::getDescriptor() const {
  return absl::StrCat("Service with IRI ",
                      parsedServiceClause_.serviceIri_.toSparql());
}

// ____________________________________________________________________________
size_t Service::getResultWidth() const {
  return parsedServiceClause_.visibleVariables_.size();
}

// ____________________________________________________________________________
VariableToColumnMap Service::computeVariableToColumnMap() const {
  VariableToColumnMap map;
  const auto& visibleVariables = parsedServiceClause_.visibleVariables_;
  for (size_t i = 0; i < visibleVariables.size(); i++) {
    // We do not know which of the columns in the subresult contain undefined
    // VALUES.
    // TODO<joka921> We could parse the contained graph pattern to extract this
    // information.
    map[visibleVariables[i]] = makePossiblyUndefinedColumn(i);
  }
  return map;
}

// ____________________________________________________________________________
float Service::getMultiplicity([[maybe_unused]] size_t col) {
  // TODO: For now, we don't have any information about the multiplicities at
  // query planning time, so we just return `1` for each column.
  return 1;
}

// ____________________________________________________________________________
uint64_t Service::getSizeEstimateBeforeLimit() {
  // TODO: For now, we don't have any information about the result size at
  // query planning time, so we just return `100'000`.
  return 100'000;
}

// ____________________________________________________________________________
size_t Service::getCostEstimate() {
  // TODO: For now, we don't have any information about the cost at query
  // planning time, so we just return ten times the estimated size.
  return 10 * getSizeEstimateBeforeLimit();
}

// ____________________________________________________________________________
ProtoResult Service::computeResult([[maybe_unused]] bool requestLaziness) {
  // Get the URL of the SPARQL endpoint.
  std::string_view serviceIriString = parsedServiceClause_.serviceIri_.iri();
  AD_CONTRACT_CHECK(serviceIriString.starts_with("<") &&
                    serviceIriString.ends_with(">"));
  serviceIriString.remove_prefix(1);
  serviceIriString.remove_suffix(1);
  ad_utility::httpUtils::Url serviceUrl{serviceIriString};

  // Try to simplify the Service Query using it's sibling Operation.
  if (auto valuesClause = getSiblingValuesClause(); valuesClause.has_value()) {
    auto openBracketPos = parsedServiceClause_.graphPatternAsString_.find('{');
    parsedServiceClause_.graphPatternAsString_ =
        "{\n" + valuesClause.value() + '\n' +
        parsedServiceClause_.graphPatternAsString_.substr(openBracketPos + 1);
  }

  // Construct the query to be sent to the SPARQL endpoint.
  std::string variablesForSelectClause = absl::StrJoin(
      parsedServiceClause_.visibleVariables_, " ", Variable::AbslFormatter);
  std::string serviceQuery = absl::StrCat(
      parsedServiceClause_.prologue_, "\nSELECT ", variablesForSelectClause,
      " WHERE ", parsedServiceClause_.graphPatternAsString_);
  LOG(INFO) << "Sending SERVICE query to remote endpoint "
            << "(protocol: " << serviceUrl.protocolAsString()
            << ", host: " << serviceUrl.host()
            << ", port: " << serviceUrl.port()
            << ", target: " << serviceUrl.target() << ")" << std::endl
            << serviceQuery << std::endl;

  HttpOrHttpsResponse response = getResultFunction_(
      serviceUrl, cancellationHandle_, boost::beast::http::verb::post,
      serviceQuery, "application/sparql-query",
      "application/sparql-results+json");

  std::basic_string<char, std::char_traits<char>,
                    ad_utility::AllocatorWithLimit<char>>
      jsonStr(_executionContext->getAllocator());
  for (std::span<std::byte> bytes : response.body_) {
    jsonStr.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    checkCancellation();
  }

  auto throwErrorWithContext = [&jsonStr, &serviceUrl](std::string_view sv) {
    throw std::runtime_error(absl::StrCat(
        "Error while executing a SERVICE request to <", serviceUrl.asString(),
        ">: ", sv, ". First 100 bytes of the response: ",
        std::string_view{jsonStr.data()}.substr(0, 100)));
  };

  // Verify status and content-type of the response.
  if (response.status_ != boost::beast::http::status::ok) {
    throwErrorWithContext(absl::StrCat(
        "SERVICE responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_))));
  }

  if (response.contentType_ != "application/sparql-results+json") {
    throwErrorWithContext(absl::StrCat(
        "QLever requires the endpoint of a SERVICE to send the result as "
        "'application/sparql-results+json' but the endpoint sent '",
        response.contentType_, "'"));
  }

  // Parse the received result.
  std::vector<std::string> resVariables;
  std::vector<nlohmann::json> resBindings;
  try {
    auto jsonResult = nlohmann::json::parse(jsonStr);

    if (jsonResult.empty()) {
      throwErrorWithContext("Response from SPARQL endpoint is empty");
    }

    resVariables = jsonResult["head"]["vars"].get<std::vector<std::string>>();
    resBindings =
        jsonResult["results"]["bindings"].get<std::vector<nlohmann::json>>();
  } catch (const nlohmann::json::parse_error&) {
    throwErrorWithContext("Failed to parse the SERVICE result as JSON");
  } catch (const nlohmann::json::type_error&) {
    throwErrorWithContext("JSON result does not have the expected structure");
  }

  // Check if result header row is expected.
  std::string headerRow = absl::StrCat("?", absl::StrJoin(resVariables, " ?"));
  std::string expectedHeaderRow = absl::StrJoin(
      parsedServiceClause_.visibleVariables_, " ", Variable::AbslFormatter);
  if (headerRow != expectedHeaderRow) {
    throwErrorWithContext(absl::StrCat(
        "Header row of JSON result for SERVICE query is \"", headerRow,
        "\", but expected \"", expectedHeaderRow, "\""));
  }

  // Set basic properties of the result table.
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  LocalVocab localVocab{};
  // Fill the result table using the `writeJsonResult` method below.
  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE(resWidth, &Service::writeJsonResult, this, resVariables,
                  resBindings, &idTable, &localVocab);

  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// ____________________________________________________________________________
template <size_t I>
void Service::writeJsonResult(const std::vector<std::string>& vars,
                              const std::vector<nlohmann::json>& bindings,
                              IdTable* idTablePtr, LocalVocab* localVocab) {
  IdTableStatic<I> idTable = std::move(*idTablePtr).toStatic<I>();
  checkCancellation();
  std::vector<size_t> numLocalVocabPerColumn(idTable.numColumns());

  size_t rowIdx = 0;
  for (const auto& binding : bindings) {
    idTable.emplace_back();
    for (size_t colIdx = 0; colIdx < vars.size(); ++colIdx) {
      TripleComponent tc = binding.contains(vars[colIdx])
                               ? bindingToTripleComponent(binding[vars[colIdx]])
                               : TripleComponent::UNDEF();

      Id id = std::move(tc).toValueId(getIndex().getVocab(), *localVocab);
      idTable(rowIdx, colIdx) = id;
      if (id.getDatatype() == Datatype::LocalVocabIndex) {
        ++numLocalVocabPerColumn[colIdx];
      }
    }
    rowIdx++;
    checkCancellation();
  }

  AD_CORRECTNESS_CHECK(rowIdx == idTable.size());
  LOG(INFO) << "Number of rows in result: " << idTable.size() << std::endl;
  LOG(INFO) << "Number of entries in local vocabulary per column: "
            << absl::StrJoin(numLocalVocabPerColumn, ", ") << std::endl;
  *idTablePtr = std::move(idTable).toDynamic();
  checkCancellation();
}

// ____________________________________________________________________________
std::optional<std::string> Service::getSiblingValuesClause() const {
  if (siblingTree_ == nullptr) {
    return std::nullopt;
  }

  const auto& siblingResult = siblingTree_->getResult();
  if (siblingResult->idTable().size() >
      RuntimeParameters().get<"service-max-value-rows">()) {
    return std::nullopt;
  }

  checkCancellation();

  std::vector<ColumnIndex> commonColumnIndices;
  const auto& siblingVars = siblingTree_->getVariableColumns();
  std::string vars = "(";
  for (const auto& localVar : parsedServiceClause_.visibleVariables_) {
    auto it = siblingVars.find(localVar);
    if (it == siblingVars.end()) {
      continue;
    }
    absl::StrAppend(&vars, it->first.name(), " ");
    commonColumnIndices.push_back(it->second.columnIndex_);
  }
  vars.back() = ')';

  checkCancellation();

  ad_utility::HashSet<std::string> rowSet;

  std::string values = " { ";
  for (size_t rowIndex = 0; rowIndex < siblingResult->idTable().size();
       ++rowIndex) {
    std::string row = "(";
    for (const auto& columnIdx : commonColumnIndices) {
      const auto& optionalString = ExportQueryExecutionTrees::idToStringAndType(
          siblingTree_->getRootOperation()->getIndex(),
          siblingResult->idTable()(rowIndex, columnIdx),
          siblingResult->localVocab());

      if (optionalString.has_value()) {
        absl::StrAppend(&row, optionalString.value().first, " ");
      }
    }
    row.back() = ')';

    if (rowSet.contains(row)) {
      continue;
    }

    rowSet.insert(row);
    absl::StrAppend(&values, row, " ");

    checkCancellation();
  }

  return "VALUES " + vars + values + "} . ";
}

// ____________________________________________________________________________
TripleComponent Service::bindingToTripleComponent(const nlohmann::json& cell) {
  if (!cell.contains("type") || !cell.contains("value")) {
    throw std::runtime_error("Missing type or value field in binding.");
  }

  const auto type = cell["type"].get<std::string_view>();
  const auto value = cell["value"].get<std::string_view>();

  TripleComponent tc;
  if (type == "literal") {
    if (cell.contains("datatype")) {
      tc = TurtleParser<TokenizerCtre>::literalAndDatatypeToTripleComponent(
          value, TripleComponent::Iri::fromIrirefWithoutBrackets(
                     cell["datatype"].get<std::string_view>()));
    } else if (cell.contains("xml:lang")) {
      tc = TripleComponent::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(value),
          cell["xml:lang"].get<std::string>());
    } else {
      tc = TripleComponent::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(value));
    }
  } else if (type == "uri") {
    tc = TripleComponent::Iri::fromIrirefWithoutBrackets(value);
  } else if (type == "bnode") {
    throw std::runtime_error(
        "Blank nodes in the result of a SERVICE are currently not supported. "
        "For now, consider filtering them out using the ISBLANK function or "
        "converting them via the STR function.");
  } else {
    throw std::runtime_error(absl::StrCat("Type ", type, " is undefined."));
  }
  return tc;
}
