// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_RLEPREFIXCOMPRESSOR_H
#define QLEVER_SRC_ENGINE_RLEPREFIXCOMPRESSOR_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "backports/span.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Exception.h"

namespace ql::engine::rle {

// _____________________________________________________________________________
// Statistics tracking for RLE constant folding effectiveness.
struct RleStats {
  size_t totalTerms_ = 0;
  size_t cacheHits_ = 0;
  size_t cacheMisses_ = 0;

  [[nodiscard]] constexpr double reductionRatio() const noexcept {
    if (totalTerms_ == 0) {
      return 0.0;
    }
    return static_cast<double>(cacheHits_) / static_cast<double>(totalTerms_);
  }

  [[nodiscard]] constexpr double reductionPercentage() const noexcept {
    return reductionRatio() * 100.0;
  }

  constexpr void reset() noexcept {
    totalTerms_ = 0;
    cacheHits_ = 0;
    cacheMisses_ = 0;
  }
};

// _____________________________________________________________________________
// Slice splicing helper.
// Copies `len` formatted prefix bytes from `src` to `out` with `std::memcpy`
// and returns the end of the written range.
inline char* spliceSlice(const char* src, size_t len, char* out) noexcept {
  AD_CONTRACT_CHECK(out != nullptr || len == 0);
  if (len == 0) {
    return out;
  }
  std::memcpy(out, src, len);
  return out + len;
}

// _____________________________________________________________________________
// Internal thread-local prefix slice storage.
// Holds the pre-formatted bytes for the currently cached ValueId.
template <size_t MaxBufferSize = 2048>
class RlePrefixSlice {
 private:
  std::array<char, MaxBufferSize> buffer_{};
  size_t length_ = 0;
  ValueId cachedId_{};
  bool valid_ = false;

 public:
  constexpr RlePrefixSlice() noexcept = default;

  [[nodiscard]] constexpr bool isValid() const noexcept { return valid_; }
  [[nodiscard]] constexpr ValueId cachedId() const noexcept {
    return cachedId_;
  }
  [[nodiscard]] constexpr size_t length() const noexcept { return length_; }
  [[nodiscard]] const char* data() const noexcept { return buffer_.data(); }

  [[nodiscard]] std::string_view view() const noexcept {
    return std::string_view(buffer_.data(), length_);
  }

  // Splice the cached formatted slice directly into destination buffer.
  inline char* spliceInto(char* dest) const noexcept {
    AD_CONTRACT_CHECK(dest != nullptr || length_ == 0);
    return spliceSlice(buffer_.data(), length_, dest);
  }

  // Store new pre-formatted content into slice cache.
  void assign(ValueId id, std::string_view formattedContent) noexcept {
    AD_CONTRACT_CHECK(formattedContent.size() <= MaxBufferSize);
    length_ = formattedContent.size();
    if (length_ > 0) {
      std::memcpy(buffer_.data(), formattedContent.data(), length_);
    }
    cachedId_ = id;
    valid_ = true;
  }

  // Invalidate slice cache.
  void invalidate() noexcept {
    valid_ = false;
    length_ = 0;
  }
};

// _____________________________________________________________________________
// Configuration options for RlePrefixFormatter delimiters and formatting.
struct RleFormatterConfig {
  std::string_view prefix_{"<"};
  std::string_view suffix_{">"};
  std::string_view delimiter_{" "};
};

// _____________________________________________________________________________
// Deep Module: RlePrefixFormatter
//
// In sorted query result tables (e.g. SPO or PSO permutation scans), the
// subject or predicate is identical across thousands of consecutive triples.
// Standard serializers repeatedly resolve and format the exact same IRI string
// on every row.
//
// DuckDB uses Run-Length Encoded (RLE) constant folding to format repeated
// column prefixes once.
//
// RlePrefixFormatter:
// 1. Detects consecutive runs of identical ValueIds in sorted columns.
// 2. Formats the constant IRI once into a thread-local prefix slice, and
// splices
//    it into subsequent output rows with a single 64-bit/128-bit word copy.
// 3. Seamlessly switches back to dynamic formatting when the run ends.
class RlePrefixFormatter {
 private:
  RlePrefixSlice<2048> slice_{};
  RleFormatterConfig config_{};
  RleStats stats_{};

 public:
  explicit RlePrefixFormatter(RleFormatterConfig config = RleFormatterConfig{})
      : config_{config} {}

  // ___________________________________________________________________________
  [[nodiscard]] const RleStats& stats() const noexcept { return stats_; }
  [[nodiscard]] const RleFormatterConfig& config() const noexcept {
    return config_;
  }

  void resetStats() noexcept { stats_.reset(); }

  void reset() noexcept {
    slice_.invalidate();
    stats_.reset();
  }

  // ___________________________________________________________________________
  // Format a column prefix when raw term string_view is provided.
  // If `id` matches the active cached run, skips formatting and splices slice.
  // When run ends (`id != cachedId`), re-formats into slice and updates cache.
  inline char* formatPrefix(ValueId id, std::string_view rawTerm,
                            char* out) noexcept {
    AD_CONTRACT_CHECK(out != nullptr);
    ++stats_.totalTerms_;

    if (slice_.isValid() && id == slice_.cachedId()) {
      ++stats_.cacheHits_;
      return slice_.spliceInto(out);
    }

    // Cache miss: format new prefix slice
    ++stats_.cacheMisses_;
    std::array<char, 2048> tempBuf{};
    // Fail fast instead of overflowing the stack buffer below: terms longer
    // than the scratch capacity (e.g. very long literals) are not supported
    // by this fixed-size fast path.
    AD_CONTRACT_CHECK(config_.prefix_.size() + rawTerm.size() +
                          config_.suffix_.size() + config_.delimiter_.size() <=
                      tempBuf.size());
    char* curr = tempBuf.data();

    // Opening delimiter (e.g. "<")
    if (!config_.prefix_.empty()) {
      std::memcpy(curr, config_.prefix_.data(), config_.prefix_.size());
      curr += config_.prefix_.size();
    }
    // Term content
    if (!rawTerm.empty()) {
      std::memcpy(curr, rawTerm.data(), rawTerm.size());
      curr += rawTerm.size();
    }
    // Closing delimiter (e.g. ">")
    if (!config_.suffix_.empty()) {
      std::memcpy(curr, config_.suffix_.data(), config_.suffix_.size());
      curr += config_.suffix_.size();
    }
    // Trailing column delimiter (e.g. " " or "\t")
    if (!config_.delimiter_.empty()) {
      std::memcpy(curr, config_.delimiter_.data(), config_.delimiter_.size());
      curr += config_.delimiter_.size();
    }

    size_t formattedLen = static_cast<size_t>(curr - tempBuf.data());
    slice_.assign(id, std::string_view(tempBuf.data(), formattedLen));
    return slice_.spliceInto(out);
  }

  // ___________________________________________________________________________
  // Format a column prefix with lazy ID-to-string lookup.
  // If `id` matches the active run, `lookupFunc` is NOT called (100% lookup
  // savings). When run ends, `lookupFunc(id)` is invoked exactly once for the
  // new run.
  template <typename LookupFunc>
  inline char* formatPrefixWithLookup(ValueId id, LookupFunc&& lookupFunc,
                                      char* out) {
    AD_CONTRACT_CHECK(out != nullptr);
    ++stats_.totalTerms_;

    if (slice_.isValid() && id == slice_.cachedId()) {
      ++stats_.cacheHits_;
      return slice_.spliceInto(out);
    }

    // Cache miss: invoke lookup once
    ++stats_.cacheMisses_;
    std::string_view rawTerm = lookupFunc(id);

    std::array<char, 2048> tempBuf{};
    // Fail fast instead of overflowing the stack buffer below: terms longer
    // than the scratch capacity (e.g. very long literals) are not supported
    // by this fixed-size fast path.
    AD_CONTRACT_CHECK(config_.prefix_.size() + rawTerm.size() +
                          config_.suffix_.size() + config_.delimiter_.size() <=
                      tempBuf.size());
    char* curr = tempBuf.data();

    if (!config_.prefix_.empty()) {
      std::memcpy(curr, config_.prefix_.data(), config_.prefix_.size());
      curr += config_.prefix_.size();
    }
    if (!rawTerm.empty()) {
      std::memcpy(curr, rawTerm.data(), rawTerm.size());
      curr += rawTerm.size();
    }
    if (!config_.suffix_.empty()) {
      std::memcpy(curr, config_.suffix_.data(), config_.suffix_.size());
      curr += config_.suffix_.size();
    }
    if (!config_.delimiter_.empty()) {
      std::memcpy(curr, config_.delimiter_.data(), config_.delimiter_.size());
      curr += config_.delimiter_.size();
    }

    size_t formattedLen = static_cast<size_t>(curr - tempBuf.data());
    slice_.assign(id, std::string_view(tempBuf.data(), formattedLen));
    return slice_.spliceInto(out);
  }

  // ___________________________________________________________________________
  // Batch format a sorted column slice.
  // Precondition: `ids` and `rawTerms` must have identical sizes.
  // Returns: Pointer past the last byte written.
  inline char* formatBatch(ql::span<const ValueId> ids,
                           ql::span<const std::string_view> rawTerms,
                           char* out) noexcept {
    AD_CONTRACT_CHECK(ids.size() == rawTerms.size());
    AD_CONTRACT_CHECK(out != nullptr || ids.empty());

    char* curr = out;
    const size_t n = ids.size();
    for (size_t i = 0; i < n; ++i) {
      curr = formatPrefix(ids[i], rawTerms[i], curr);
    }
    return curr;
  }

  // ___________________________________________________________________________
  // Batch format a sorted column slice with lazy lookup functor.
  template <typename LookupFunc>
  inline char* formatBatchWithLookup(ql::span<const ValueId> ids,
                                     LookupFunc&& lookupFunc, char* out) {
    AD_CONTRACT_CHECK(out != nullptr || ids.empty());

    char* curr = out;
    const size_t n = ids.size();
    for (size_t i = 0; i < n; ++i) {
      curr = formatPrefixWithLookup(ids[i], lookupFunc, curr);
    }
    return curr;
  }
};

// _____________________________________________________________________________
// Deep Module: RleTripleFormatter
//
// Specializes RLE constant folding for sorted RDF triple streams (SPO / PSO
// scans). Folds repeated Subject and Predicate column runs into cached prefix
// slices, formatting the full triple `<s> <p> <o> .\n` (or TSV/CSV format) with
// single-pass word copies for the repeated columns.
class RleTripleFormatter {
 private:
  RlePrefixFormatter subjectFormatter_;
  RlePrefixFormatter predicateFormatter_;
  std::string_view objectPrefix_{"<"};
  std::string_view objectSuffix_{">"};
  std::string_view rowTerminator_{" .\n"};

 public:
  explicit RleTripleFormatter(
      RleFormatterConfig subjectConfig = RleFormatterConfig{.prefix_ = "<",
                                                            .suffix_ = ">",
                                                            .delimiter_ = " "},
      RleFormatterConfig predicateConfig =
          RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "},
      std::string_view objectPrefix = "<", std::string_view objectSuffix = ">",
      std::string_view rowTerminator = " .\n")
      : subjectFormatter_{subjectConfig},
        predicateFormatter_{predicateConfig},
        objectPrefix_{objectPrefix},
        objectSuffix_{objectSuffix},
        rowTerminator_{rowTerminator} {}

  // ___________________________________________________________________________
  // Factory methods for standard export formats.
  [[nodiscard]] static RleTripleFormatter makeNTriplesFormatter() {
    return RleTripleFormatter(
        RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "},
        RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = " "},
        "<", ">", " .\n");
  }

  [[nodiscard]] static RleTripleFormatter makeTsvFormatter() {
    return RleTripleFormatter(
        RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = "\t"},
        RleFormatterConfig{.prefix_ = "<", .suffix_ = ">", .delimiter_ = "\t"},
        "<", ">", "\n");
  }

  [[nodiscard]] static RleTripleFormatter makeCsvFormatter() {
    return RleTripleFormatter(
        RleFormatterConfig{.prefix_ = "\"", .suffix_ = "\"", .delimiter_ = ","},
        RleFormatterConfig{.prefix_ = "\"", .suffix_ = "\"", .delimiter_ = ","},
        "\"", "\"", "\n");
  }

  // ___________________________________________________________________________
  [[nodiscard]] const RlePrefixFormatter& subjectFormatter() const noexcept {
    return subjectFormatter_;
  }
  [[nodiscard]] const RlePrefixFormatter& predicateFormatter() const noexcept {
    return predicateFormatter_;
  }

  void reset() noexcept {
    subjectFormatter_.reset();
    predicateFormatter_.reset();
  }

  // ___________________________________________________________________________
  // Format a single sorted triple (S, P, O) with raw term strings.
  inline char* formatTriple(ValueId subjId, std::string_view subjTerm,
                            ValueId predId, std::string_view predTerm,
                            ValueId objId, std::string_view objTerm,
                            char* out) noexcept {
    (void)objId;
    AD_CONTRACT_CHECK(out != nullptr);

    // 1. Spliced Subject prefix (RLE folded)
    char* curr = subjectFormatter_.formatPrefix(subjId, subjTerm, out);

    // 2. Spliced Predicate prefix (RLE folded)
    curr = predicateFormatter_.formatPrefix(predId, predTerm, curr);

    // 3. Object term
    if (!objectPrefix_.empty()) {
      std::memcpy(curr, objectPrefix_.data(), objectPrefix_.size());
      curr += objectPrefix_.size();
    }
    if (!objTerm.empty()) {
      std::memcpy(curr, objTerm.data(), objTerm.size());
      curr += objTerm.size();
    }
    if (!objectSuffix_.empty()) {
      std::memcpy(curr, objectSuffix_.data(), objectSuffix_.size());
      curr += objectSuffix_.size();
    }

    // 4. Row terminator
    if (!rowTerminator_.empty()) {
      std::memcpy(curr, rowTerminator_.data(), rowTerminator_.size());
      curr += rowTerminator_.size();
    }

    return curr;
  }

  // ___________________________________________________________________________
  // Format a single sorted triple (S, P, O) with lazy lookup functor.
  template <typename LookupFunc>
  inline char* formatTripleWithLookup(ValueId subjId, ValueId predId,
                                      ValueId objId, LookupFunc&& lookupFunc,
                                      char* out) {
    AD_CONTRACT_CHECK(out != nullptr);

    // 1. Spliced Subject prefix with lazy lookup
    char* curr =
        subjectFormatter_.formatPrefixWithLookup(subjId, lookupFunc, out);

    // 2. Spliced Predicate prefix with lazy lookup
    curr = predicateFormatter_.formatPrefixWithLookup(predId, lookupFunc, curr);

    // 3. Object term lookup & write
    std::string_view objTerm = lookupFunc(objId);
    if (!objectPrefix_.empty()) {
      std::memcpy(curr, objectPrefix_.data(), objectPrefix_.size());
      curr += objectPrefix_.size();
    }
    if (!objTerm.empty()) {
      std::memcpy(curr, objTerm.data(), objTerm.size());
      curr += objTerm.size();
    }
    if (!objectSuffix_.empty()) {
      std::memcpy(curr, objectSuffix_.data(), objectSuffix_.size());
      curr += objectSuffix_.size();
    }

    // 4. Row terminator
    if (!rowTerminator_.empty()) {
      std::memcpy(curr, rowTerminator_.data(), rowTerminator_.size());
      curr += rowTerminator_.size();
    }

    return curr;
  }

  // ___________________________________________________________________________
  // Batch format a collection of sorted triples.
  template <typename LookupFunc>
  inline char* formatTriplesBatch(ql::span<const ValueId> subjects,
                                  ql::span<const ValueId> predicates,
                                  ql::span<const ValueId> objects,
                                  LookupFunc&& lookupFunc, char* out) {
    AD_CONTRACT_CHECK(subjects.size() == predicates.size());
    AD_CONTRACT_CHECK(subjects.size() == objects.size());
    AD_CONTRACT_CHECK(out != nullptr || subjects.empty());

    char* curr = out;
    const size_t n = subjects.size();
    for (size_t i = 0; i < n; ++i) {
      curr = formatTripleWithLookup(subjects[i], predicates[i], objects[i],
                                    lookupFunc, curr);
    }
    return curr;
  }
};

}  // namespace ql::engine::rle

#endif  // QLEVER_SRC_ENGINE_RLEPREFIXCOMPRESSOR_H
