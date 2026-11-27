// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/blobDiff/BlobDiffQueries.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "parser/ParserAndVisitorBase.h"
#include "util/Algorithm.h"

namespace qlever::blobDiff {

namespace {
using PinOptions = QueryExecutionContext::PinResultWithName;

// Return the selected variables of the `parsedQuery`, sorted by name and
// without duplicates. Throw if the `parsedQuery` is not a `SELECT` query.
std::vector<std::string> sortedSelectedVariables(const ParsedQuery& parsedQuery,
                                                 std::string_view query) {
  if (!parsedQuery.hasSelectClause()) {
    throw std::runtime_error{absl::StrCat(
        "Only SELECT queries can be pinned, but the following query is not "
        "one: ",
        query)};
  }
  auto variables = parsedQuery.selectClause().getSelectedVariablesAsStrings();
  ql::ranges::sort(variables);
  variables.erase(std::unique(variables.begin(), variables.end()),
                  variables.end());
  return variables;
}

// Implementation of `withCanonicalSortOrder` for the already parsed `query`.
std::string withCanonicalSortOrderImpl(const ParsedQuery& parsedQuery,
                                       const std::string& query) {
  auto variables = sortedSelectedVariables(parsedQuery, query);
  if (!parsedQuery.datasetClauses_.isUnconstrainedOrWithClause()) {
    throw std::runtime_error{absl::StrCat(
        "A pinned query cannot have a `FROM` or `FROM NAMED` clause, because "
        "it is wrapped as a subquery. The query is: ",
        query)};
  }
  if (variables.empty()) {
    return query;
  }

  // The keys of an explicit `INTERNAL SORT BY` of the `query` come first, such
  // that its deliberate sort order is kept.
  std::vector<std::string> sortKeys;
  auto addSortKey = [&sortKeys](const std::string& variable) {
    if (!ad_utility::contains(sortKeys, variable)) {
      sortKeys.push_back(variable);
    }
  };
  if (parsedQuery._isInternalSort == IsInternalSort::True) {
    for (const auto& key : parsedQuery._orderBy) {
      const auto& name = key.variable_.name();
      if (!ql::ranges::binary_search(variables, name)) {
        throw std::runtime_error{absl::StrCat(
            "The variable ", name,
            " of the `INTERNAL SORT BY` clause of a pinned query has to be "
            "selected, because otherwise the sort order cannot be kept. The "
            "query is: ",
            query)};
      }
      addSortKey(name);
    }
  }
  ql::ranges::for_each(variables, addSortKey);

  // NOTE: The newline after the `body` ends a comment at the end of the
  // `query`, which would otherwise also comment out the closing braces.
  std::string_view queryView{query};
  auto prologue = queryView.substr(0, prologueLength(queryView));
  auto body = queryView.substr(prologue.size());
  return absl::StrCat(prologue, " SELECT * WHERE { { ", body,
                      "\n} } INTERNAL SORT BY ", absl::StrJoin(sortKeys, " "));
}

// Return the options of `Qlever::queryAndPinResultWithName` for the `query`
// and its query string with the canonical sort order. Throw if the query
// cannot be wrapped, or if its `geoIndexVar_` is not one of its selected
// variables.
std::pair<PinOptions, std::string> preparePinning(
    const Qlever& qlever, const BlobDiffConfig::NamedQuery& query) {
  auto parsed = qlever.parseQuery(query.query_);
  const auto& parsedQuery = parsed.parsedQuery();
  auto canonicalQuery = withCanonicalSortOrderImpl(parsedQuery, query.query_);
  PinOptions options;
  options.name_ = query.name_;
  if (query.geoIndexVar_.has_value()) {
    const auto& var = query.geoIndexVar_.value();
    if (!ql::ranges::binary_search(
            sortedSelectedVariables(parsedQuery, query.query_), var)) {
      throw std::runtime_error{
          absl::StrCat("The `geoIndexVar` ", var, " of the named query `",
                       query.name_, "` is not one of its selected variables.")};
    }
    options.geoIndexVar_ = Variable{var};
  }
  options.geoIndexSimplificationInMeters_ = query.simplificationMeters_;
  return {std::move(options), std::move(canonicalQuery)};
}
}  // namespace

// _____________________________________________________________________________
size_t prologueLength(std::string_view query) {
  // The visitor only has to accept the prologue, its content is not needed.
  struct PrologueVisitor {
    bool visit([[maybe_unused]] SparqlAutomaticParser::PrologueContext* ctx) {
      return true;
    }
  };
  sparqlParserHelpers::ParserAndVisitorBase<PrologueVisitor> parserAndVisitor{
      std::string{query}};
  auto remaining =
      parserAndVisitor.parseTypesafe(&SparqlAutomaticParser::prologue)
          .remainingText_;
  AD_CORRECTNESS_CHECK(remaining.size() <= query.size());
  return query.size() - remaining.size();
}

// _____________________________________________________________________________
std::string withCanonicalSortOrder(const Qlever& qlever,
                                   const std::string& query) {
  auto parsed = qlever.parseQuery(query);
  return withCanonicalSortOrderImpl(parsed.parsedQuery(), query);
}

// _____________________________________________________________________________
void pinAllQueries(Qlever& qlever, const BlobDiffConfig& config) {
  // Check all queries before the first pinned result is dropped.
  std::vector<std::pair<PinOptions, std::string>> preparedQueries;
  for (const auto& query : config.namedQueries_) {
    preparedQueries.push_back(preparePinning(qlever, query));
  }
  qlever.clearNamedResultCache();
  qlever.clearQueryResultCache();
  for (auto& [options, query] : preparedQueries) {
    qlever.queryAndPinResultWithName(std::move(options), std::move(query));
  }
}

}  // namespace qlever::blobDiff
