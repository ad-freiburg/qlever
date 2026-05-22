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
#include "util/Parameters.h"
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
// `Global` is the strictly spec-compliant mode, but requires memory
// proportional to the number of unique triples in the result, which can be
// prohibitive for large result sets.
// `BatchWise` is the recommended default: it significantly reduces duplicates
// in practice with bounded memory usage, at the cost of not guaranteeing a
// fully deduplicated result.
// `None` preserves the original QLever behaviour (no deduplication) for users
// who need minimal memory overhead and no deduplication cost.
//
// TODO<ms2144>: a future improvement could be to spill the deduplication hash
// set to disk when memory usage grows too large, allowing `Global`
// deduplication with bounded memory usage while retaining streaming. The hash
// set is used to track already yielded triples, and spilling it to disk would
// introduce disk I/O overhead.
struct DeduplicationMode {
  struct None {};  // Every triple is emitted, no duplicate tracking.
  struct Global {
  };  // A triple is emitted at most once across the entire result.
  struct BatchWise {   // Deduplication within non-overlapping windows of
    size_t batchSize;  // `batchSize` consecutive rows.
  };
  std::variant<None, Global, BatchWise> value;
};

// Serializers for use with ad_utility::Parameter<DeduplicationMode, ...>.
struct DeduplicationModeFromString {
  DeduplicationMode operator()(const std::string& s) const {
    if (s == "false") return {DeduplicationMode::None{}};
    if (s == "global") return {DeduplicationMode::Global{}};
    try {
      size_t deduplicationBatchSize = std::stoull(s);
      if (deduplicationBatchSize == 0) {
        throw std::runtime_error(
            "Deduplication batch size must be a positive integer.");
      }
      return {DeduplicationMode::BatchWise{deduplicationBatchSize}};
    } catch (const std::exception&) {
      throw std::runtime_error(absl::StrCat(
          "Invalid value for construct-deduplicate: \"", s,
          "\" Expected \"false\", \"global\", or a positive integer."));
    }
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
                            return std::to_string(bw.batchSize);
                          }},
                      m.value);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H
