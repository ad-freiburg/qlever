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
#include <cctype>
#include <stdexcept>
#include <vector>

#include "backports/algorithm.h"

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
  size_t pos = 0;
  auto startsWithKeyword = [&query, &pos](std::string_view keyword) {
    return query.size() - pos >= keyword.size() &&
           ql::ranges::equal(
               query.substr(pos, keyword.size()), keyword, [](char a, char b) {
                 return std::tolower(static_cast<unsigned char>(a)) == b;
               });
  };
  while (true) {
    while (pos < query.size() &&
           std::isspace(static_cast<unsigned char>(query[pos]))) {
      ++pos;
    }
    if (!startsWithKeyword("prefix") && !startsWithKeyword("base")) {
      return pos;
    }
    auto end = query.find('>', pos);
    if (end == std::string_view::npos) {
      return pos;
    }
    pos = end + 1;
  }
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
  std::string_view queryView{query};
  auto prologue = queryView.substr(0, prologueLength(queryView));
  auto body = queryView.substr(prologue.size());
  return absl::StrCat(prologue, " SELECT * WHERE { { ", body,
                      " } } INTERNAL SORT BY ", absl::StrJoin(variables, " "));
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
