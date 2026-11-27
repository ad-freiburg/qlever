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
#include <vector>

#include "backports/algorithm.h"
#include "parser/ParserAndVisitorBase.h"
#include "util/Algorithm.h"

namespace qlever::blobDiff {

namespace {
using PinOptions = QueryExecutionContext::PinResultWithName;

// Return the options of `Qlever::queryAndPinResultWithName` for `query`.
PinOptions pinOptionsOf(const BlobDiffConfig::NamedQuery& query) {
  PinOptions options;
  options.name_ = query.name_;
  if (query.geoIndexVar_.has_value()) {
    options.geoIndexVar_ = Variable{query.geoIndexVar_.value()};
  }
  options.geoIndexSimplificationInMeters_ = query.simplificationMeters_;
  return options;
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
  const auto& parsedQuery = parsed.parsedQuery();
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

  std::string_view queryView{query};
  auto prologue = queryView.substr(0, prologueLength(queryView));
  auto body = queryView.substr(prologue.size());
  return absl::StrCat(prologue, " SELECT * WHERE { { ", body,
                      " } } INTERNAL SORT BY ", absl::StrJoin(sortKeys, " "));
}

// _____________________________________________________________________________
void pinAllQueries(Qlever& qlever, const BlobDiffConfig& config) {
  qlever.clearNamedResultCache();
  qlever.clearQueryResultCache();
  for (const auto& query : config.namedQueries_) {
    qlever.queryAndPinResultWithName(
        pinOptionsOf(query), withCanonicalSortOrder(qlever, query.query_));
  }
}

}  // namespace qlever::blobDiff
