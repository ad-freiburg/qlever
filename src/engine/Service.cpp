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
  os << "SERVICE ";
  if (parsedServiceClause_.silent_) {
    os << "SILENT ";
  }
  os << parsedServiceClause_.serviceIri_.toStringRepresentation() << " {\n"
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
  return absl::StrCat(
      "Service with IRI ",
      parsedServiceClause_.serviceIri_.toStringRepresentation());
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
  // Try to simplify the Service Query using it's sibling Operation.
  if (auto valuesClause = getSiblingValuesClause(); valuesClause.has_value()) {
    auto openBracketPos = parsedServiceClause_.graphPatternAsString_.find('{');
    parsedServiceClause_.graphPatternAsString_ =
        "{\n" + valuesClause.value() + '\n' +
        parsedServiceClause_.graphPatternAsString_.substr(openBracketPos + 1);
  }

  try {
    return computeResultImpl(requestLaziness);
  } catch (const ad_utility::CancellationException&) {
    throw;
  } catch (const ad_utility::detail::AllocationExceedsLimitException&) {
    throw;
  } catch (const std::exception&) {
    // if the `SILENT` keyword is set in the service clause, catch the error and
    // return a neutral Element.
    if (parsedServiceClause_.silent_) {
      return makeNeutralElementResultForSilentFail();
    }
    throw;
  }
}

// ____________________________________________________________________________
ProtoResult Service::computeResultImpl([[maybe_unused]] bool requestLaziness) {
  // Get the URL of the SPARQL endpoint.
  ad_utility::httpUtils::Url serviceUrl{
      asStringViewUnsafe(parsedServiceClause_.serviceIri_.getContent())};

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

  auto throwErrorWithContext = [this, &response](std::string_view sv) {
    std::string ctx;
    ctx.reserve(100);
    for (const auto& bytes : std::move(response.body_)) {
      ctx += std::string(reinterpret_cast<const char*>(bytes.data()),
                         bytes.size());
      if (ctx.size() >= 100) {
        break;
      }
    }
    this->throwErrorWithContext(sv,
                                ctx.substr(0, std::min(100, (int)ctx.size())));
  };

  // Verify status and content-type of the response.
  if (response.status_ != boost::beast::http::status::ok) {
    LOG(INFO) << serviceUrl.asString() << '\n';
    throwErrorWithContext(absl::StrCat(
        "SERVICE responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_))));
  }
  if (!response.contentType_.starts_with("application/sparql-results+json")) {
    throwErrorWithContext(absl::StrCat(
        "QLever requires the endpoint of a SERVICE to send the result as "
        "'application/sparql-results+json' but the endpoint sent '",
        response.contentType_, "'"));
  }

  // Prepare the expected Variables as keys for the JSON-bindings. We can't wait
  // for the variables sent in the response as they're maybe not read before
  // the bindings.
  std::vector<std::string> expVariableKeys;
  std::ranges::transform(parsedServiceClause_.visibleVariables_,
                         std::back_inserter(expVariableKeys),
                         [](const Variable& v) { return v.name().substr(1); });

  // Set basic properties of the result table.
  IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};
  LocalVocab localVocab{};

  auto body = ad_utility::LazyJsonParser::parse(std::move(response.body_),
                                                {"results", "bindings"});

  // Fill the result table using the `writeJsonResult` method below.
  CALL_FIXED_SIZE(getResultWidth(), &Service::writeJsonResult, this,
                  expVariableKeys, body, &idTable, &localVocab);

  return {std::move(idTable), resultSortedOn(), std::move(localVocab)};
}

// ____________________________________________________________________________
template <size_t I>
void Service::writeJsonResult(const std::vector<std::string>& vars,
                              ad_utility::LazyJsonParser::Generator& body,
                              IdTable* idTablePtr, LocalVocab* localVocab) {
  IdTableStatic<I> idTable = std::move(*idTablePtr).toStatic<I>();
  checkCancellation();
  std::vector<size_t> numLocalVocabPerColumn(idTable.numColumns());

  auto writeBindings = [&](const nlohmann::json& bindings, size_t& rowIdx) {
    for (const auto& binding : bindings) {
      idTable.emplace_back();
      for (size_t colIdx = 0; colIdx < vars.size(); ++colIdx) {
        TripleComponent tc =
            binding.contains(vars[colIdx])
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
  };

  size_t rowIdx = 0;
  bool resultExists{false};
  bool varsChecked{false};
  for (const nlohmann::json& part : body) {
    if (part.contains("head")) {
      AD_CORRECTNESS_CHECK(!varsChecked);
      verifyVariables(part, body);
      varsChecked = true;
    }
    // The LazyJsonParser only yields parts containing the "bindings" array,
    // therefore we can assume its existence here.
    AD_CORRECTNESS_CHECK(part.contains("results") &&
                         part["results"].contains("bindings") &&
                         part["results"]["bindings"].is_array());
    writeBindings(part["results"]["bindings"], rowIdx);
    resultExists = true;
  }

  // As the LazyJsonParser only passes parts of the result that match the
  // expected structure, no result implies an unexpected structure.
  if (!resultExists || !varsChecked) {
    throwErrorWithContext("JSON result does not have the expected structure",
                          body.details().first100_);
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

// ____________________________________________________________________________
ProtoResult Service::makeNeutralElementResultForSilentFail() const {
  IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};
  idTable.emplace_back();
  for (size_t colIdx = 0; colIdx < getResultWidth(); ++colIdx) {
    idTable(0, colIdx) = Id::makeUndefined();
  }
  return {std::move(idTable), resultSortedOn(), LocalVocab{}};
}

// ____________________________________________________________________________
void Service::verifyVariables(
    const nlohmann::json& j,
    const ad_utility::LazyJsonParser::Generator& gen) const {
  if (!j["head"].contains("vars") || !j["head"]["vars"].is_array()) {
    throwErrorWithContext("JSON result does not have the expected structure",
                          gen.details().first100_);
  }

  auto vars = j["head"]["vars"].get<std::vector<std::string>>();
  ad_utility::HashSet<Variable> responseVars;
  for (const auto& v : vars) {
    responseVars.emplace("?" + v);
  }
  ad_utility::HashSet<Variable> expectedVars(
      parsedServiceClause_.visibleVariables_.begin(),
      parsedServiceClause_.visibleVariables_.end());

  if (responseVars != expectedVars) {
    throwErrorWithContext(
        absl::StrCat("Header row of JSON result for SERVICE query is \"",
                     absl::StrCat("?", absl::StrJoin(vars, " ?")),
                     "\", but expected \"",
                     absl::StrJoin(parsedServiceClause_.visibleVariables_, " ",
                                   Variable::AbslFormatter),
                     "\""),
        gen.details().first100_);
  }
}

// ____________________________________________________________________________
void Service::throwErrorWithContext(std::string_view msg,
                                    std::string_view first100) const {
  const ad_utility::httpUtils::Url serviceUrl{
      asStringViewUnsafe(parsedServiceClause_.serviceIri_.getContent())};

  throw std::runtime_error(absl::StrCat(
      "Error while executing a SERVICE request to <", serviceUrl.asString(),
      ">: ", msg, ". First 100 bytes of the response: '", first100, "'"));
}
