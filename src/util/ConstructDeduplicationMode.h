// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H
#define QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H

#include <absl/strings/str_cat.h>

#include <stdexcept>
#include <string>
#include <variant>

#include "util/Exception.h"
#include "util/OverloadCallOperator.h"
#include "util/TypeTraits.h"

namespace ad_utility {

// Controls whether and how triples are deduplicated in CONSTRUCT query results.
//
// Per the SPARQL 1.1 specification
// (https://www.w3.org/TR/sparql-query/#construct), the result of a construct
// query is an RDF graph formed by set union of all instantiated triples,
// meaning duplicates are eliminated by definition.
// However, it is unclear whether a SPARQL engine is required to enforce this.
// See also: https://github.com/w3c-cg/sparql-dev/issues/86
//
// `None` is the default: it preserves the original QLever behaviour (no
// deduplication, constant-memory streaming) for users who need minimal memory
// overhead and no deduplication cost.
// `Global` is the strictly spec-compliant mode, but requires memory
// proportional to the number of unique triples in the result, which can be
// prohibitive for large result sets.
// `BatchWise` keeps a single LRU cache of the `batchSize_` most recently seen
// unique triple keys (shared across all template triples and keyed on the full
// instantiated triple), and suppresses a triple only if its key is still in
// that cache. Memory is bounded (O(batchSize_)). Because the cache evicts its
// least recently used key once full, a duplicate that recurs after more than
// `batchSize_` other unique keys have been seen is no longer remembered and is
// emitted again.
struct DeduplicationMode {
  struct None {};  // Every triple is emitted, no duplicate tracking.
  struct Global {
  };  // A triple is emitted at most once across the entire result.
  struct BatchWise {    // Deduplicates against the `batchSize_` most recently
    size_t batchSize_;  // seen unique triples (one shared cache).
  };
  std::variant<None, Global, BatchWise> value_;

  static DeduplicationMode none() { return {None{}}; }
  static DeduplicationMode global() { return {Global{}}; }
  static DeduplicationMode batchWise(size_t batchSize) {
    return {BatchWise{batchSize}};
  }
};

// Serializers for use with ad_utility::Parameter<DeduplicationMode, ...>.
struct DeduplicationModeFromString {
  DeduplicationMode operator()(const std::string& s) const {
    if (s == "false") return {DeduplicationMode::None{}};
    if (s == "global") return {DeduplicationMode::Global{}};

    size_t batchSize = 0;
    const char* begin = s.data();
    const char* end = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(begin, end, batchSize);
    // require the entire string to be a valid, in-range unsigned integer.
    if (ec == std::errc{} && ptr == end && batchSize != 0) {
      return {DeduplicationMode::BatchWise{batchSize}};
    }
    throw std::runtime_error(absl::StrCat(
        "Invalid value for construct-deduplicate: \"", s,
        "\" Expected \"false\", \"global\", or a positive integer."));
  }
};

struct DeduplicationModeToString {
  std::string operator()(const DeduplicationMode& m) const {
    return std::visit(ad_utility::OverloadCallOperator{
                          [](const DeduplicationMode::None&) -> std::string {
                            return "false";
                          },
                          [](const DeduplicationMode::Global&) -> std::string {
                            return "global";
                          },
                          [](const DeduplicationMode::BatchWise& bw) {
                            return std::to_string(bw.batchSize_);
                          }},
                      m.value_);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H
