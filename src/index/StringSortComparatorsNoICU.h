// Copyright 2026 The QLever Authors, in particular:
//
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_STRINGSORTCOMPARATORSNOICU_H
#define QLEVER_SRC_INDEX_STRINGSORTCOMPARATORSNOICU_H

#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "index/StringSortComparatorTypes.h"
#include "util/Exception.h"
#include "util/StringUtils.h"

// Bytewise-comparison replacement for `LocaleManager` when building without
// ICU. All locale parameters are accepted but ignored; comparison is done
// bytewise via `std::string_view::compare`.
class LocaleManagerNoICU : public LocaleManagerBase {
 public:
  LocaleManagerNoICU() = default;
  LocaleManagerNoICU(const LocaleManagerNoICU&) = default;
  LocaleManagerNoICU& operator=(const LocaleManagerNoICU&) = default;

  LocaleManagerNoICU(const std::string& /*lang*/,
                     const std::string& /*country*/,
                     bool /*ignorePunctuationAtFirstLevel*/) {}

  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level /*level*/) const {
    int res = a.compare(b);
    return (res < 0) ? -1 : (res > 0) ? 1 : 0;
  }

  template <typename T, typename U>
  static int compare(const SortKeyImpl<T>& a, const SortKeyImpl<U>& b,
                     [[maybe_unused]] const Level = Level::PRIMARY) {
    return a.compare(b);
  }

  [[nodiscard]] SortKey getSortKey(std::string_view s,
                                   const Level /*level*/) const {
    SortKey result;
    result.get() = ::ranges::to<U8String>(s | ql::views::transform([](char c) {
                                            return static_cast<uint8_t>(c);
                                          }));
    return result;
  }

  [[nodiscard]] std::pair<size_t, SortKey> getPrefixSortKey(
      std::string_view s, size_t prefixLength) const {
    return getPrefixSortKeyImpl<false>(*this, s, prefixLength);
  }

  [[nodiscard]] std::string getLowercaseUtf8(std::string_view s) const {
    return ad_utility::utf8ToLower<false>(s);
  }

  [[nodiscard]] std::string normalizeUtf8(std::string_view input) const {
    return std::string(input);
  }
};

// The ICU-free comparators are the generic comparator templates (see
// `StringSortComparatorTypes.h`) instantiated with the `LocaleManagerNoICU`.
using SimpleStringComparatorNoICU =
    SimpleStringComparatorImpl<LocaleManagerNoICU>;
using TripleComponentComparatorNoICU =
    TripleComponentComparatorImpl<LocaleManagerNoICU>;

#endif  // QLEVER_SRC_INDEX_STRINGSORTCOMPARATORSNOICU_H
