// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H
#define QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <charconv>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>

#include "backports/StartsWithAndEndsWith.h"
#include "util/OverloadCallOperator.h"

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
// `Full` is the strictly spec-compliant mode, but requires memory
// proportional to the number of unique triples in the result, which can be
// prohibitive for large result sets. Triples containing a blank node are
// exempt from deduplication, which is not an approximation: every solution
// instantiates a template blank node as a fresh blank node, so two such
// triples are never duplicates of each other.
// `Lru` keeps a single LRU cache of the `capacity_` most recently seen
// unique triple keys (shared across all template triples and keyed on the full
// instantiated triple), and suppresses a triple only if its key is still in
// that cache. Memory is bounded (O(capacity_)). Because the cache evicts its
// least recently used key once full, a duplicate that recurs after more than
// `capacity_` other unique keys have been seen is no longer remembered and is
// emitted again.
struct DeduplicationMode {
  static constexpr std::string_view none_ = "none";
  static constexpr std::string_view full_ = "full";
  static constexpr std::string_view lru_ = "lru";

  struct None {};  // Every triple is emitted, no duplicate tracking.
  struct Full {};  // A triple is emitted at most once across the entire result.
  struct Lru {     // Deduplicates against the `capacity_` most recently
    size_t capacity_;  // seen unique triples (one shared cache).
  };
  std::variant<None, Full, Lru> value_;

  static DeduplicationMode none() { return {None{}}; }
  static DeduplicationMode full() { return {Full{}}; }
  static DeduplicationMode lru(size_t capacity) { return {Lru{capacity}}; }
};

// Serializers for use with ad_utility::Parameter<DeduplicationMode, ...>.
struct DeduplicationModeFromString {
  DeduplicationMode operator()(const std::string& s) const {
    if (s == DeduplicationMode::none_) return {DeduplicationMode::None{}};
    if (s == DeduplicationMode::full_) return {DeduplicationMode::Full{}};

    // `lru:<positive integer>`.
    constexpr std::string_view prefix = "lru:";
    if (ql::starts_with(s, prefix)) {
      size_t capacity = 0;
      const char* begin = s.data() + prefix.size();
      const char* end = s.data() + s.size();
      auto [ptr, ec] = std::from_chars(begin, end, capacity);
      // require the suffix to be a valid, in-range, positive unsigned integer.
      if (ec == std::errc{} && ptr == end && capacity != 0) {
        return {DeduplicationMode::Lru{capacity}};
      }
    }
    throw std::runtime_error(absl::StrFormat(
        R"(Invalid value for construct-deduplication: "%s" Expected "%s", "%s", or "%s:<positive integer>".)",
        s, DeduplicationMode::none_, DeduplicationMode::full_,
        DeduplicationMode::lru_));
  }
};

struct DeduplicationModeToString {
  std::string operator()(const DeduplicationMode& m) const {
    return std::visit(ad_utility::OverloadCallOperator{
                          [](const DeduplicationMode::None&) {
                            return std::string{DeduplicationMode::none_};
                          },
                          [](const DeduplicationMode::Full&) {
                            return std::string{DeduplicationMode::full_};
                          },
                          [](const DeduplicationMode::Lru& bw) {
                            return absl::StrCat(DeduplicationMode::lru_, ":",
                                                bw.capacity_);
                          }},
                      m.value_);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONSTRUCTDEDUPLICATIONMODE_H
