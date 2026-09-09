// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_PARSEDQUERYTESTHELPERS_H
#define QLEVER_TEST_UTIL_PARSEDQUERYTESTHELPERS_H

#include <initializer_list>
#include <iterator>
#include <string>
#include <vector>

#include "AllocatorTestHelpers.h"
#include "IndexTestHelpers.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SparqlParser.h"
#include "util/Allocator.h"
#include "util/AllocatorTypes.h"

namespace ad_utility::testing {
// Parse a SPARQL query string into a typed `ParsedQuery` object.
inline auto parseQuery(std::string query,
                       const std::vector<DatasetClause>& datasets = {},
                       qlever::Allocator<Id> allocator = makeAllocator()) {
  return SparqlParser::parseQuery(encodedIriManager(), std::move(query),
                                  datasets, std::move(allocator));
}

// Convert a plain `std::vector<T>` into a `qlever::vector<T>`.
template <typename T>
qlever::vector<T> toQVec(std::vector<T> vec) {
  return qlever::vector<T>(std::make_move_iterator(vec.begin()),
                           std::make_move_iterator(vec.end()),
                           qlever::makeUnlimitedAllocator<T>());
}

// Overload of `toQVec` for braced-init-list literals (e.g. `{a, b}`), since
// template argument deduction does not work for `std::initializer_list`
// arguments when passed as a bare braced list.
template <typename T>
qlever::vector<T> toQVec(std::initializer_list<T> ilist) {
  return qlever::vector<T>(ilist.begin(), ilist.end(),
                           qlever::makeUnlimitedAllocator<T>());
}

// Convert a plain `std::vector<std::vector<T>>` into a
// `qlever::vector<qlever::vector<T>>`.
template <typename T>
qlever::vector<qlever::vector<T>> toQVecOfVec(
    std::vector<std::vector<T>> vec) {
  qlever::vector<qlever::vector<T>> result{
      qlever::makeUnlimitedAllocator<qlever::vector<T>>()};
  result.reserve(vec.size());
  for (auto& inner : vec) {
    result.push_back(toQVec(std::move(inner)));
  }
  return result;
}

// Build a `parsedQuery::SparqlValues` object from a plain vector of
// `variables` and a plain vector of `values` (each inner vector being one
// row of the `VALUES` clause). Useful in tests, since aggregate- or
// brace-initializing `SparqlValues` directly with plain vector literals does
// not compile (its members are allocator-aware and have no default
// constructor).
inline parsedQuery::SparqlValues makeSparqlValues(
    std::vector<Variable> variables,
    std::vector<std::vector<TripleComponent>> values = {}) {
  parsedQuery::SparqlValues result{qlever::makeUnlimitedAllocator<Id>()};
  result._variables = toQVec(std::move(variables));
  result._values = toQVecOfVec(std::move(values));
  return result;
}

}  // namespace ad_utility::testing

#endif  // QLEVER_TEST_UTIL_PARSEDQUERYTESTHELPERS_H
