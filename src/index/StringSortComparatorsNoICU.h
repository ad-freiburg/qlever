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

/**
 * @brief Bytewise-comparison replacement for `SimpleStringComparator` when
 * building without ICU.
 */
class SimpleStringComparatorNoICU {
 public:
  using Level = LocaleManagerBase::Level;

  SimpleStringComparatorNoICU(const std::string& lang,
                              const std::string& country,
                              bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}

  SimpleStringComparatorNoICU() = default;

  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto cmpRes = _locManager.compare(a, b, level);
    if (cmpRes != 0 || level != Level::TOTAL) {
      return cmpRes;
    }
    return a.compare(b);
  }

  bool operator()(std::string_view a, const LocaleManagerBase::SortKey& b,
                  [[maybe_unused]] const Level l) const {
    auto aTrans = _locManager.getSortKey(a, Level::PRIMARY);
    auto cmp = LocaleManagerNoICU::compare(aTrans, b, Level::PRIMARY);
    return cmp < 0;
  }

  // Undefined on purpose (only used for constraint checking in <ranges>).
  bool operator()(const LocaleManagerBase::SortKey& a, std::string_view s,
                  [[maybe_unused]] const Level l = Level::PRIMARY) const;

  // Undefined on purpose.
  bool operator()(const LocaleManagerBase::SortKey& a,
                  const LocaleManagerBase::SortKey& b,
                  [[maybe_unused]] Level level = Level::PRIMARY) const;

  [[nodiscard]] LocaleManagerBase::SortKey transformToFirstPossibleBiggerValue(
      std::string_view s, const Level level) const {
    AD_CONTRACT_CHECK(level == Level::PRIMARY);
    auto transformed = _locManager.getSortKey(s, Level::PRIMARY);
    unsigned char last = transformed.get().back();
    if (last < std::numeric_limits<unsigned char>::max()) {
      transformed.get().back() += 1;
    } else {
      transformed.get().push_back('\0');
    }
    return transformed;
  }

  [[nodiscard]] const LocaleManagerNoICU& getLocaleManager() const {
    return _locManager;
  }

 private:
  LocaleManagerNoICU _locManager;
};

/**
 * @brief Bytewise-comparison replacement for `TripleComponentComparator` when
 * building without ICU.
 */
class TripleComponentComparatorNoICU {
 public:
  using Level = LocaleManagerBase::Level;

  TripleComponentComparatorNoICU(const std::string& lang,
                                 const std::string& country,
                                 bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}

  TripleComponentComparatorNoICU() = default;

  template <class InnerString, class LanguageTag, class FullString>
  struct SplitValBase {
    SplitValBase() = default;
    SplitValBase(char fst, InnerString trans, LanguageTag l,
                 FullString fullInputForTotalComparison)
        : firstOriginalChar_(fst),
          transformedVal_(std::move(trans)),
          langtag_(std::move(l)),
          fullInput_{std::move(fullInputForTotalComparison)} {}

    char firstOriginalChar_ = '\0';
    InnerString transformedVal_;
    LanguageTag langtag_;
    FullString fullInput_;
  };

  using SplitVal =
      SplitValBase<LocaleManagerBase::SortKey, std::string, std::string>;

  using SplitValNonOwning =
      SplitValBase<std::string_view, std::string_view, std::string_view>;

  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  bool operator()(std::string_view a, const SplitVal& spB,
                  const Level level) const {
    auto spA = extractAndTransformComparable(a, level);
    return compare(spA, spB, level) < 0;
  }

  bool operator()(const SplitVal& spA, std::string_view b,
                  const Level level) const {
    auto spB = extractAndTransformComparable(b, level);
    return compare(spA, spB, level) < 0;
  }

  template <typename A, typename B, typename C>
  bool operator()(const SplitValBase<A, B, C>& a,
                  const SplitValBase<A, B, C>& b, const Level level) const {
    return compare(a, b, level) < 0;
  }

  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto splitA = extractComparable<SplitValNonOwning>(a, level);
    auto splitB = extractComparable<SplitValNonOwning>(b, level);
    auto cmp = compare(splitA, splitB, level);
    return cmp;
  }

  // Total comparison, using the "is external" flags as a tiebreaker. The
  // direction of the tiebreaker (external before non-external) is arbitrary:
  // downstream the merge ORs the flag across all duplicates of the same word
  // (see `VocabularyMergerImpl.h`), so the order of duplicates does not affect
  // the final result, only the deterministic shape of the sort.
  bool isLessInTotalWithExternalFlag(std::string_view a, bool aIsExternal,
                                     std::string_view b,
                                     bool bIsExternal) const {
    int cmp = compare(a, b, Level::TOTAL);
    if (cmp != 0) {
      return cmp < 0;
    }
    return aIsExternal && !bIsExternal;
  }

  [[nodiscard]] SplitVal extractAndTransformComparable(
      std::string_view a, const Level level) const {
    return extractComparable<SplitVal>(a, level);
  }

  template <class A, class B, typename C>
  [[nodiscard]] int compare(const SplitValBase<A, B, C>& a,
                            const SplitValBase<A, B, C>& b,
                            const Level level) const {
    if (auto res =
            std::strncmp(&a.firstOriginalChar_, &b.firstOriginalChar_, 1);
        res != 0) {
      return res;
    }

    if (int res =
            _locManager.compare(a.transformedVal_, b.transformedVal_, level);
        res != 0 || level != Level::TOTAL) {
      return res;
    }

    if (int res = a.fullInput_.compare(b.fullInput_); res != 0) {
      return res;
    }

    return a.langtag_.compare(b.langtag_);
  }

  [[nodiscard]] SplitVal transformToFirstPossibleBiggerValue(
      std::string_view s, const Level level) const {
    AD_CONTRACT_CHECK(level == Level::PRIMARY);
    auto transformed = extractAndTransformComparable(s, Level::PRIMARY);
    AD_CONTRACT_CHECK(
        static_cast<unsigned char>(transformed.firstOriginalChar_) <
        std::numeric_limits<unsigned char>::max());
    if (transformed.transformedVal_.get().empty()) {
      transformed.firstOriginalChar_ += 1;
    } else {
      unsigned char last = transformed.transformedVal_.get().back();
      if (last < std::numeric_limits<unsigned char>::max()) {
        transformed.transformedVal_.get().back() += 1;
      } else {
        transformed.transformedVal_.get().push_back('\1');
      }
    }
    return transformed;
  }

  [[nodiscard]] const LocaleManagerNoICU& getLocaleManager() const {
    return _locManager;
  }

  [[nodiscard]] std::string normalizeUtf8(std::string_view sv) const {
    return _locManager.normalizeUtf8(sv);
  }

  Level& defaultLevel() { return _defaultLevel; }
  [[nodiscard]] const Level& defaultLevel() const { return _defaultLevel; }

 private:
  LocaleManagerNoICU _locManager;
  Level _defaultLevel = Level::IDENTICAL;

  template <class SplitValType>
  [[nodiscard]] SplitValType extractComparable(
      std::string_view a, [[maybe_unused]] const Level level) const {
    std::string_view res = a;
    const char first = a.empty() ? char(0) : a[0];
    std::string_view langtag;
    if (ql::starts_with(res, '"')) {
      res.remove_prefix(1);
      auto endPos = ad_utility::findLiteralEnd(res, "\"");
      if (endPos != std::string::npos) {
        langtag = res.substr(endPos + 1);
        res.remove_suffix(res.size() - endPos);
      } else {
        langtag = "";
      }
    }
    if constexpr (std::is_same_v<SplitValType, SplitVal>) {
      return {first, _locManager.getSortKey(res, level), std::string(langtag),
              std::string{a}};
    } else if constexpr (std::is_same_v<SplitValType, SplitValNonOwning>) {
      return {first, res, langtag, a};
    } else {
      static_assert(ad_utility::alwaysFalse<SplitValType>);
    }
  }
};

#endif  // QLEVER_SRC_INDEX_STRINGSORTCOMPARATORSNOICU_H
