// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_INDEX_STRINGSORTCOMPARATORSNOICU_H
#define QLEVER_SRC_INDEX_STRINGSORTCOMPARATORSNOICU_H

#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "index/StringSortComparatorTypes.h"
#include "util/Exception.h"
#include "util/StringUtils.h"

/**
 * @brief Bytewise-comparison replacement for `LocaleManager` when building
 * without ICU. All locale parameters are accepted but ignored; comparison is
 * done bytewise via `std::string_view::compare`.
 */
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
    U8String& resultView = result.get();
    const auto* begin = reinterpret_cast<const uint8_t*>(s.data());
    resultView.insert(resultView.end(), begin, begin + s.size());
    return result;
  }

  [[nodiscard]] std::pair<size_t, SortKey> getPrefixSortKey(
      std::string_view s, size_t prefixLength) const {
    size_t numContributingCodepoints = 0;
    SortKey sortKey;
    size_t prefixLengthSoFar = 1;
    SortKey completeSortKey = getSortKey(s, Level::PRIMARY);
    while (numContributingCodepoints < prefixLength ||
           !completeSortKey.starts_with(sortKey)) {
      auto [numCodepoints, prefix] =
          ad_utility::getUTF8Prefix(s, prefixLengthSoFar);
      auto nextLongerSortKey = getSortKey(prefix, Level::PRIMARY);
      if (nextLongerSortKey != sortKey) {
        numContributingCodepoints++;
        sortKey = std::move(nextLongerSortKey);
      }
      if (numCodepoints < prefixLengthSoFar) {
        break;
      }
      prefixLengthSoFar++;
    }
    return {numContributingCodepoints, std::move(sortKey)};
  }

  [[nodiscard]] std::string getLowercaseUtf8(const std::string_view s) const {
    std::string result(s);
    for (auto& c : result) {
      if (c >= 'A' && c <= 'Z') {
        c = static_cast<char>(c + ('a' - 'A'));
      }
    }
    return result;
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
