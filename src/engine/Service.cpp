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
#include "util/HashMap.h"
#include "util/HashSet.h"
#include "util/StringUtils.h"
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
    this->throwErrorWithContext(sv, std::string_view(ctx).substr(0, 100));
  };

  // Verify status and content-type of the response.
  if (response.status_ != boost::beast::http::status::ok) {
    throwErrorWithContext(absl::StrCat(
        "SERVICE responded with HTTP status code: ",
        static_cast<int>(response.status_), ", ",
        toStd(boost::beast::http::obsolete_reason(response.status_))));
  }
  if (!ad_utility::utf8ToLower(response.contentType_)
           .starts_with("application/sparql-results+json")) {
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

  auto body = ad_utility::LazyJsonParser::parse(std::move(response.body_),
                                                {"results", "bindings"});

  // Note: The `body`-generator also keeps the complete response connection
  // alive, so we have no lifetime issue here(see `HttpRequest::send` for
  // details).
  auto localVocabPtr = std::make_shared<LocalVocab>();
  auto generator = computeResultLazily(expVariableKeys, std::move(body),
                                       localVocabPtr, !requestLaziness);

  return requestLaziness
             ? ProtoResult{std::move(generator), resultSortedOn(),
                           std::move(localVocabPtr)}
             : ProtoResult{cppcoro::getSingleElement(std::move(generator)),
                           resultSortedOn(), std::move(*localVocabPtr)};
}

template <size_t I>
void Service::writeJsonResult(const std::vector<std::string>& vars,
                              const nlohmann::json& partJson,
                              IdTable* idTablePtr, LocalVocab* localVocab,
                              size_t& rowIdx) {
  IdTableStatic<I> idTable = std::move(*idTablePtr).toStatic<I>();
  checkCancellation();
  std::vector<size_t> numLocalVocabPerColumn(idTable.numColumns());
  // TODO<joka921> We should include a memory limit, as soon as we can do proper
  // memory-limited HashMaps.
  ad_utility::HashMap<std::string, Id> blankNodeMap;

  auto writeBindings = [&](const nlohmann::json& bindings, size_t& rowIdx) {
    for (const auto& binding : bindings) {
      idTable.emplace_back();
      for (size_t colIdx = 0; colIdx < vars.size(); ++colIdx) {
        TripleComponent tc =
            binding.contains(vars[colIdx])
                ? bindingToTripleComponent(binding[vars[colIdx]], blankNodeMap,
                                           localVocab)
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

  // The LazyJsonParser only yields partJsons containing the "bindings" array,
  // therefore we can assume its existence here.
  AD_CORRECTNESS_CHECK(partJson.contains("results") &&
                       partJson["results"].contains("bindings") &&
                       partJson["results"]["bindings"].is_array());
  writeBindings(partJson["results"]["bindings"], rowIdx);

  *idTablePtr = std::move(idTable).toDynamic();
  checkCancellation();
}

// ____________________________________________________________________________
cppcoro::generator<IdTable> Service::computeResultLazily(
    const std::vector<std::string> vars,
    ad_utility::LazyJsonParser::Generator body,
    std::shared_ptr<LocalVocab> localVocab, bool singleIdTable) {
  IdTable idTable{getResultWidth(), getExecutionContext()->getAllocator()};

  size_t rowIdx = 0;
  bool varsChecked{false};
  bool resultExists{false};
  try {
    for (const nlohmann::json& partJson : body) {
      if (partJson.contains("head")) {
        AD_CORRECTNESS_CHECK(!varsChecked);
        verifyVariables(partJson["head"], body.details());
        varsChecked = true;
      }

      CALL_FIXED_SIZE(getResultWidth(), &Service::writeJsonResult, this, vars,
                      partJson, &idTable, localVocab.get(), rowIdx);
      if (!singleIdTable) {
        co_yield idTable;
        idTable.clear();
        rowIdx = 0;
      }
      resultExists = true;
    }
  } catch (const ad_utility::LazyJsonParser::Error& e) {
    throwErrorWithContext(
        absl::StrCat("Parser failed with error: '", e.what(), "'"),
        body.details().first100_, body.details().last100_);
  }

  // As the LazyJsonParser only passes parts of the result that match
  // the expected structure, no result implies an unexpected
  // structure.
  if (!resultExists) {
    throwErrorWithContext(
        "JSON result does not have the expected structure (results "
        "section "
        "missing)",
        body.details().first100_, body.details().last100_);
  }

  if (!varsChecked) {
    throwErrorWithContext(
        "JSON result does not have the expected structure (head "
        "section "
        "missing)",
        body.details().first100_, body.details().last100_);
  }

  if (singleIdTable) {
    co_yield idTable;
  }
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

  // Creates a single row of the values clause or an empty string on error.
  auto createValueRow = [&](size_t rowIndex) -> std::string {
    std::string row = "(";
    for (const auto& columnIdx : commonColumnIndices) {
      const auto& optStr = idToValueForValuesClause(
          siblingTree_->getRootOperation()->getIndex(),
          siblingResult->idTable()(rowIndex, columnIdx),
          siblingResult->localVocab());

      if (!optStr.has_value()) {
        return "";
      }
      absl::StrAppend(&row, optStr.value(), " ");
    }
    row.back() = ')';
    return row;
  };

  ad_utility::HashSet<std::string> rowSet;
  std::string values = " { ";
  for (size_t rowIndex = 0; rowIndex < siblingResult->idTable().size();
       ++rowIndex) {
    std::string row = createValueRow(rowIndex);
    if (row.empty() || rowSet.contains(row)) {
      continue;
    }
    rowSet.insert(row);

    absl::StrAppend(&values, row, " ");
    checkCancellation();
  }

  return "VALUES " + vars + values + "} . ";
}

// ____________________________________________________________________________
TripleComponent Service::bindingToTripleComponent(
    const nlohmann::json& binding,
    ad_utility::HashMap<std::string, Id>& blankNodeMap,
    LocalVocab* localVocab) const {
  if (!binding.contains("type") || !binding.contains("value")) {
    throw std::runtime_error(absl::StrCat(
        "Missing type or value field in binding. The binding is: '",
        binding.dump(), "'"));
  }

  const auto type = binding["type"].get<std::string_view>();
  const auto value = binding["value"].get<std::string_view>();
  auto blankNodeManagerPtr =
      getExecutionContext()->getIndex().getBlankNodeManager();

  TripleComponent tc;
  if (type == "literal") {
    if (binding.contains("datatype")) {
      tc = TurtleParser<TokenizerCtre>::literalAndDatatypeToTripleComponent(
          value, TripleComponent::Iri::fromIrirefWithoutBrackets(
                     binding["datatype"].get<std::string_view>()));
    } else if (binding.contains("xml:lang")) {
      tc = TripleComponent::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(value),
          binding["xml:lang"].get<std::string>());
    } else {
      tc = TripleComponent::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(value));
    }
  } else if (type == "uri") {
    tc = TripleComponent::Iri::fromIrirefWithoutBrackets(value);
  } else if (type == "bnode") {
    auto [it, wasNew] = blankNodeMap.try_emplace(value, Id());
    if (wasNew) {
      it->second = Id::makeFromBlankNodeIndex(
          localVocab->getBlankNodeIndex(blankNodeManagerPtr));
    }
    tc = it->second;
  } else {
    throw std::runtime_error(absl::StrCat("Type ", type,
                                          " is undefined. The binding is: '",
                                          binding.dump(), "'"));
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
    const nlohmann::json& head,
    const ad_utility::LazyJsonParser::Details& details) const {
  std::vector<std::string> vars;
  try {
    vars = head.at("vars").get<std::vector<std::string>>();
  } catch (...) {
    throw std::runtime_error(
        absl::StrCat("JSON result does not have the expected structure, as its "
                     "\"head\" section is not according to the SPARQL "
                     "standard. The \"head\" section is: '",
                     head.dump(), "'."));
  }

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
                     "\". Probable cause: The remote endpoint sent a JSON "
                     "response that is not according to the SPARQL Standard"),
        details.first100_, details.last100_);
  }
}

// ____________________________________________________________________________
void Service::throwErrorWithContext(std::string_view msg,
                                    std::string_view first100,
                                    std::string_view last100) const {
  const ad_utility::httpUtils::Url serviceUrl{
      asStringViewUnsafe(parsedServiceClause_.serviceIri_.getContent())};

  throw std::runtime_error(absl::StrCat(
      "Error while executing a SERVICE request to <", serviceUrl.asString(),
      ">: ", msg, ". First 100 bytes of the response: '", first100,
      (last100.empty() ? "'"
                       : absl::StrCat(", last 100 bytes: '", last100, "'"))));
}

// ____________________________________________________________________________
std::optional<std::string> Service::idToValueForValuesClause(
    const Index& index, Id id, const LocalVocab& localVocab) {
  using enum Datatype;
  const auto& optionalStringAndXsdType =
      ExportQueryExecutionTrees::idToStringAndType(index, id, localVocab);
  if (!optionalStringAndXsdType.has_value()) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Undefined);
    return "UNDEF";
  }
  const auto& [value, xsdType] = optionalStringAndXsdType.value();

  switch (id.getDatatype()) {
    case BlankNodeIndex:
      // Blank nodes are not allowed in a values clause. Additionally blank
      // nodes across a SERVICE endpoint are disjoint anyway, so rows that
      // contain blank nodes will never create matches and we can safely omit
      // them.
      return std::nullopt;
    case Int:
    case Double:
    case Bool:
      return value;
    default:
      if (xsdType) {
        return absl::StrCat("\"", value, "\"^^<", xsdType, ">");
      } else if (value.starts_with('<')) {
        return value;
      } else {
        return RdfEscaping::validRDFLiteralFromNormalized(value);
      }
  }
}
