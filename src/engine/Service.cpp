// Copyright 2022 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Hannah Bast (bast@cs.uni-freiburg.de)

#include "engine/Service.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include "engine/CallFixedSize.h"
#include "engine/Values.h"
#include "engine/VariableToColumnMap.h"
#include "parser/TokenizerCtre.h"
#include "parser/TurtleParser.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/Views.h"
#include "util/http/HttpClient.h"
#include "util/http/HttpUtils.h"

// ____________________________________________________________________________
Service::Service(QueryExecutionContext* qec,
                 parsedQuery::Service parsedServiceClause,
                 GetTsvFunction getTsvFunction)
    : Operation{qec},
      parsedServiceClause_{std::move(parsedServiceClause)},
      getTsvFunction_{std::move(getTsvFunction)} {}

// ____________________________________________________________________________
std::string Service::getCacheKeyImpl() const {
  std::ostringstream os;
  // TODO: This duplicates code in GraphPatternOperation.cpp .
  os << "SERVICE " << parsedServiceClause_.serviceIri_.toSparql() << " {\n"
     << parsedServiceClause_.prologue_ << "\n"
     << parsedServiceClause_.graphPatternAsString_ << "\n}\n";
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
ResultTable Service::computeResult() {
  // Get the URL of the SPARQL endpoint.
  std::string_view serviceIriString = parsedServiceClause_.serviceIri_.iri();
  AD_CONTRACT_CHECK(serviceIriString.starts_with("<") &&
                    serviceIriString.ends_with(">"));
  serviceIriString.remove_prefix(1);
  serviceIriString.remove_suffix(1);
  ad_utility::httpUtils::Url serviceUrl{serviceIriString};

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

  // Send the query to the remote SPARQL endpoint via a POST request and get the
  // result as TSV.
  //
  // TODO: We ask for the result as TSV because that is a compact and
  // easy-to-parse format. It might not be the best choice regarding robustness
  // and portability though. In particular, we are not sure how deterministic
  // the TSV output is with respect to the precise encoding of literals.
  cppcoro::generator<std::span<std::byte>> tsvByteResult =
      ad_utility::reChunkAtSeparator(
          getTsvFunction_(serviceUrl, cancellationHandle_,
                          boost::beast::http::verb::post, serviceQuery,
                          "application/sparql-query",
                          "text/tab-separated-values"),
          static_cast<std::byte>('\n'));

  // TODO<GCC12> Use `std::views::transform` instead.
  auto tsvResult = [](auto byteResult) -> cppcoro::generator<std::string_view> {
    for (std::span<std::byte> bytes : byteResult) {
      co_yield std::string_view{reinterpret_cast<const char*>(bytes.data()),
                                bytes.size()};
    }
  }(std::move(tsvByteResult));

  // The first line of the TSV result contains the variable names.
  auto begin = tsvResult.begin();
  if (begin == tsvResult.end()) {
    throw std::runtime_error(absl::StrCat("Response from SPARQL endpoint ",
                                          serviceUrl.host(), " is empty"));
  }
  std::string_view tsvHeaderRow = *begin;
  LOG(INFO) << "Header row of TSV result: " << tsvHeaderRow << std::endl;

  // Check that the variables in the header row agree with those requested by
  // the SERVICE query.
  std::string expectedHeaderRow = absl::StrJoin(
      parsedServiceClause_.visibleVariables_, "\t", Variable::AbslFormatter);
  if (tsvHeaderRow != expectedHeaderRow) {
    throw std::runtime_error(absl::StrCat(
        "Header row of TSV result for SERVICE query is \"", tsvHeaderRow,
        "\", but expected \"", expectedHeaderRow, "\""));
  }

  // Set basic properties of the result table.
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  LocalVocab localVocab{};
  // Fill the result table using the `writeTsvResult` method below.
  size_t resWidth = getResultWidth();
  CALL_FIXED_SIZE(resWidth, &Service::writeTsvResult, this,
                  std::move(tsvResult), &idTable, &localVocab);

  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// ____________________________________________________________________________
template <size_t I>
void Service::writeTsvResult(cppcoro::generator<std::string_view> tsvResult,
                             IdTable* idTablePtr, LocalVocab* localVocab) {
  IdTableStatic<I> idTable = std::move(*idTablePtr).toStatic<I>();
  checkCancellation();
  size_t rowIdx = 0;
  std::vector<size_t> numLocalVocabPerColumn(idTable.numColumns());
  std::string lastLine;
  const size_t numVariables = parsedServiceClause_.visibleVariables_.size();
  for (std::string_view line : tsvResult) {
    // Print first line.
    if (rowIdx == 0) {
      LOG(INFO) << "First non-header row of TSV result: " << line << std::endl;
    }
    std::vector<std::string_view> valueStrings = absl::StrSplit(line, "\t");
    if (valueStrings.size() != numVariables) {
      throw std::runtime_error(absl::StrCat(
          "Number of columns in row ", rowIdx + 1, " of TSV result is ",
          valueStrings.size(), " but number of variables in header row is ",
          numVariables, ". Line: ", line));
    }
    idTable.emplace_back();
    for (size_t colIdx = 0; colIdx < valueStrings.size(); colIdx++) {
      TripleComponent tc = TurtleStringParser<TokenizerCtre>::parseTripleObject(
          valueStrings[colIdx]);
      Id id = std::move(tc).toValueId(getIndex().getVocab(), *localVocab);
      idTable(rowIdx, colIdx) = id;
      if (id.getDatatype() == Datatype::LocalVocabIndex) {
        ++numLocalVocabPerColumn[colIdx];
      }
    }
    rowIdx++;
    checkCancellation();
    lastLine = line;
  }
  if (idTable.size() > 1) {
    LOG(INFO) << "Last non-header row of TSV result: " << lastLine << std::endl;
  }
  AD_CORRECTNESS_CHECK(rowIdx == idTable.size());
  LOG(INFO) << "Number of rows in result: " << idTable.size() << std::endl;
  LOG(INFO) << "Number of entries in local vocabulary per column: "
            << absl::StrJoin(numLocalVocabPerColumn, ", ") << std::endl;
  *idTablePtr = std::move(idTable).toDynamic();
  checkCancellation();
}
