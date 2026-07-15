// Copyright 2026 The QLever Authors, in particular:
//
// 2019 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_STRINGSORTCOMPARATORTYPES_H
#define QLEVER_SRC_INDEX_STRINGSORTCOMPARATORTYPES_H

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "util/Exception.h"
#include "util/GenericCharTraits.h"
#include "util/StringUtils.h"
#include "util/TypeTraits.h"

// Base class holding the types shared by both the ICU and the NoICU variants of
// the `LocaleManager` (see `StringSortComparator.h` and
// `StringSortComparatorsNoICU.h`). None of these types depend on ICU.
class LocaleManagerBase {
 public:
  // The five collation levels supported by icu, forwarded in a typesafe manner.
  enum class Level : uint8_t {
    PRIMARY = 0,
    SECONDARY = 1,
    TERTIARY = 2,
    QUARTERNARY = 3,
    IDENTICAL = 4,
    TOTAL =
        5  // if the identical level returns equal, we take the language  tag
           // into account and then the result by strcmp. that way two strings
           // that have a different byte representation never compare equal
  };

  // A strong typedef for a string that contains unicode collation weights for
  // another string. The actual storage can be a `std::string` or a
  // `std::string_view`.
  // TODO<GCC12> As soon as we have constexpr std::string, this class can
  //  become constexpr.
  // A `uint8_t` behaves like a `char`, so we use `GenericCharTraits` (see there
  // for why we cannot rely on `std::char_traits` directly).
  using U8CharTraits = ad_utility::GenericCharTraits<uint8_t>;
  using U8String = std::basic_string<uint8_t, U8CharTraits>;
  using U8StringView = std::basic_string_view<uint8_t, U8CharTraits>;

  CPP_template(typename T)(requires ad_utility::SimilarToAny<
                           T, U8String, U8StringView>) class SortKeyImpl {
   public:
    SortKeyImpl() = default;
    explicit SortKeyImpl(U8StringView sortKey) : sortKey_(sortKey) {}
    [[nodiscard]] constexpr const T& get() const noexcept { return sortKey_; }
    constexpr T& get() noexcept { return sortKey_; }

    // Comparison of sort key is done lexicographically on the byte values
    // of member `sortKey_`
    template <typename U>
    [[nodiscard]] int compare(const SortKeyImpl<U>& rhs) const noexcept {
      return U8StringView{sortKey_}.compare(U8StringView{rhs.sortKey_});
    }

    QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(SortKeyImpl, sortKey_)

    // Is this sort key a prefix of another sort key. Note: This does not imply
    // any guarantees on the relation of the underlying strings.
    bool starts_with(const SortKeyImpl& rhs) const noexcept {
      return ql::starts_with(get(), rhs.get());
    }

    // Return the number of bytes in the `SortKey`.
    std::string::size_type size() const noexcept { return get().size(); }

   private:
    T sortKey_;
  };
  using SortKey = SortKeyImpl<U8String>;
  using SortKeyView = SortKeyImpl<U8StringView>;
};

// Compute a `SortKey` for `Level::PRIMARY` that corresponds to a prefix of `s`,
// using `manager` to compute the sort keys. Shared by the ICU and the ICU-free
// locale managers (see `getPrefixSortKey` there).
//
// `prefixLength` is the number of relevant characters (see below). Return a
// `SortKey` that is a prefix of the `SortKey` for `s` w.r.t `Level::PRIMARY`
// and that also is a `SortKey` for a prefix "p" of `s`. "p" is the minimal
// prefix of `s` which consists of at least `prefixLength` codepoints and whose
// SortKey fulfills the first condition. Codepoints, which do not contribute to
// the `SortKey` because they are irrelevant for the `PRIMARY` level do not
// count towards `prefixLength`. The first element of the return value is the
// actual number of (contributing) codepoints in "p". If `s` contains less than
// `prefixLength` contributing codepoints, then
// {totalNumberOfContributingCodepoints, completeSortKey} is returned.
//
// The `useICU` parameter selects the matching `getUTF8Prefix` variant, so that
// each manager keeps its own behavior (proper codepoints with ICU, bytes
// without).
template <bool useICU, typename LocaleManagerT>
std::pair<size_t, LocaleManagerBase::SortKey> getPrefixSortKeyImpl(
    const LocaleManagerT& manager, std::string_view s, size_t prefixLength) {
  using SortKey = LocaleManagerBase::SortKey;
  using Level = LocaleManagerBase::Level;
  size_t numContributingCodepoints = 0;
  SortKey sortKey;
  size_t prefixLengthSoFar = 1;
  SortKey completeSortKey = manager.getSortKey(s, Level::PRIMARY);
  while (numContributingCodepoints < prefixLength ||
         !completeSortKey.starts_with(sortKey)) {
    auto [numCodepoints, prefix] =
        ad_utility::getUTF8Prefix<useICU>(s, prefixLengthSoFar);
    auto nextLongerSortKey = manager.getSortKey(prefix, Level::PRIMARY);
    if (nextLongerSortKey != sortKey) {
      // The `SortKey` changed by adding a codepoint, so that codepoint was
      // contributing.
      numContributingCodepoints++;
      sortKey = std::move(nextLongerSortKey);
    }
    if (numCodepoints < prefixLengthSoFar) {
      // We have checked the complete string without finding a sufficiently long
      // contributing prefix.
      break;
    }
    prefixLengthSoFar++;
  }
  return {numContributingCodepoints, std::move(sortKey)};
}

// Compares strings according to the collation of the held `LocaleManagerT`,
// e.g. strings from the text index vocabulary. To compare components of RDF
// triples use the `TripleComponentComparatorImpl` defined below.
//
// `LocaleManagerT` is the locale manager whose collation is used. This is
// `LocaleManagerICU` in a regular build and `LocaleManagerNoICU` in an ICU-free
// build.
template <typename LocaleManagerT>
class SimpleStringComparatorImpl {
 public:
  using Level = LocaleManagerBase::Level;

  // `lang` is the language of the locale, e.g. "en" or "de". `country` is the
  // country of the locale, e.g. "US" or "CA". If
  // `ignorePunctuationAtFirstLevel` is true then spaces/punctuation etc. will
  // only be considered for comparisons if strings match otherwise. Throws
  // `std::runtime_error` if the locale cannot be constructed from `lang` and
  // `country`. \todo(joka921): make the exact punctuation level configurable.
  SimpleStringComparatorImpl(const std::string& lang,
                             const std::string& country,
                             bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}

  // Construct according to the default locale specified in
  // ../global/Constants.h
  SimpleStringComparatorImpl() = default;

  // Compare two UTF-8 encoded strings. Return true iff a comes before b.
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  // Compare the strings given the sortLevel. Return the same returning
  // convention as std::strcmp.
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto cmpRes = _locManager.compare(a, b, level);
    if (cmpRes != 0 || level != Level::TOTAL) {
      return cmpRes;
    }
    return a.compare(b);
  }

  // Compare a UTF-8 encoded string and a SortKey on the Primary Level.
  // CAVEAT: The Level l argument IS IGNORED
  //
  // Since this class only exports WeightStrings on the PRIMARY level via the
  // transformToFirstPossibleBiggerValue method, we also always use the PRIMARY
  // level for this function to avoid mistakes
  // The Level argument is therefore ignored but left in as a dummy to make the
  // getLowerBoundLambda api of the Vocabulary easier.
  // @TODO<joka921> Allow prefix ranges on different levels.
  // `a` is a UTF-8 encoded string, `b` a Weight string that has to be obtained
  // by a previous call to transformToFirstPossibleBiggerValue. Return true iff
  // a comes before the string whose SortKey is b.
  bool operator()(std::string_view a, const LocaleManagerBase::SortKey& b,
                  [[maybe_unused]] const Level l) const {
    auto aTrans = _locManager.getSortKey(a, Level::PRIMARY);
    auto cmp = LocaleManagerT::compare(aTrans, b, Level::PRIMARY);
    return cmp < 0;
  }

  // This method is left undefined on purpose, as it is only used for
  // constraints checking in the <ranges> header for the UnicodeVocabulary
  // class.
  bool operator()(const LocaleManagerBase::SortKey& a, std::string_view s,
                  [[maybe_unused]] const Level l = Level::PRIMARY) const;

  // Same goes for this function (undefined on purpose).
  bool operator()(const LocaleManagerBase::SortKey& a,
                  const LocaleManagerBase::SortKey& b,
                  [[maybe_unused]] Level level = Level::PRIMARY) const;

  // Transform a string s to the SortKey of the first possible string that
  // compares greater to s according to the held locale on the PRIMARY level
  // (other levels will cause an assertion fail.
  //
  // This is needed for calculating whether one string is a prefix of another
  // CAVEAT: This currently only supports the primary collation Level!!!
  // <TODO<joka921>: Implement this on every level, either by fixing ICU or by
  // hacking the collation strings
  //
  // `s` is a UTF-8 encoded string. Return the PRIMARY level SortKey of the
  // first possible string greater than s.
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

  // Obtain access to the held `LocaleManagerT`.
  [[nodiscard]] const LocaleManagerT& getLocaleManager() const {
    return _locManager;
  }

 private:
  LocaleManagerT _locManager;
};

// Handles the comparisons between RDFS triple elements according to their data
// types and the collation of the held `LocaleManagerT`.
//
//  General Approach: First Sort by the datatype, then by the actual value and
// then by the language tag.
//
// `LocaleManagerT` see `SimpleStringComparatorImpl` above.
template <typename LocaleManagerT>
class TripleComponentComparatorImpl {
 public:
  using Level = LocaleManagerBase::Level;

  // `lang` is the language of the locale, e.g. "en" or "de". `country` is the
  // country of the locale, e.g. "US" or "CA". If
  // `ignorePunctuationAtFirstLevel` is true then spaces/punctuation etc. will
  // only be considered for comparisons if strings match otherwise. Throws
  // `std::runtime_error` if the locale cannot be constructed from `lang` and
  // `country`. \todo(joka921): make the exact punctuation level configurable.
  TripleComponentComparatorImpl(const std::string& lang,
                                const std::string& country,
                                bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}

  // Construct according to the default locale in "../global/Constants.h"
  TripleComponentComparatorImpl() = default;

  // An entry of the Vocabulary, split up into its components and possibly
  // converted to a format that is easier to compare.
  //
  // `InnerString` is either LocaleManagerBase::SortKey or std::string_view.
  // Both variants differ greatly in their usage. Details can be found after the
  // class definition, together with the explicit aliases `SplitVal` and
  // `SplitValOwning` for the template instantiations that are actually used.
  // `LanguageTag` and `FullString` are either `std::string` or
  // `std::string_view`. They are used as deterministic tie breaks on the
  // `TOTAL` sort level.
  template <class InnerString, class LanguageTag, class FullString>
  struct SplitValBase {
    SplitValBase() = default;
    SplitValBase(char fst, InnerString trans, LanguageTag l,
                 FullString fullInputForTotalComparison)
        : firstOriginalChar_(fst),
          transformedVal_(std::move(trans)),
          langtag_(std::move(l)),
          fullInput_{std::move(fullInputForTotalComparison)} {}

    // The first char of the original value, used to distinguish between
    // different datatypes.
    char firstOriginalChar_ = '\0';
    // The original inner value, possibly transformed by a locale().
    InnerString transformedVal_;
    // The language tag, possibly empty.
    LanguageTag langtag_;
    FullString fullInput_;
  };

  // This value owns all its contents.
  // The inner value is the SortKey of the original inner value according to the
  // held Locale. This is used to transform the inner value and to safely pass
  // it around, e.g. when performing prefix comparisons in the vocabulary.
  using SplitVal =
      SplitValBase<LocaleManagerBase::SortKey, std::string, std::string>;

  // This only holds string_views to substrings of a string.
  // Currently we only use this inside this class.
  using SplitValNonOwning =
      SplitValBase<std::string_view, std::string_view, std::string_view>;

  // Compare two elements from the Vocabulary. Return false iff a comes before b
  // in the vocabulary.
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  // Compare a string_view from the vocabulary to a SplitVal that was previously
  // transformed. `a` is an element of the vocabulary, `spB` a splitVal that
  // must have been obtained by a call to extractAndTransformComparable. Return
  // whether a comes before the original value of spB in the vocabulary.
  bool operator()(std::string_view a, const SplitVal& spB,
                  const Level level) const {
    auto spA = extractAndTransformComparable(a, level);
    return compare(spA, spB, level) < 0;
  }

  // Same operator, but with switched argument types.
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

  // Compare two string_views from the Vocabulary. Return value according to
  // std::strcmp.
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto splitA = extractComparable<SplitValNonOwning>(a, level);
    auto splitB = extractComparable<SplitValNonOwning>(b, level);
    // We have to have a total ordering of unique elements in the vocabulary,
    // so if they compare equal according to the locale, use strcmp
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

  // Split a literal or iri into its components and convert the inner value
  // according to the held locale.
  [[nodiscard]] SplitVal extractAndTransformComparable(
      std::string_view a, const Level level) const {
    return extractComparable<SplitVal>(a, level);
  }

  // The inner comparison logic.
  //
  // First compares the datatypes by the firstOriginalChar_, then the inner
  // value and then the language tags. Return <0 iff a<b, 0 iff a==b, >0 iff
  // a>b.
  template <class A, class B, typename C>
  [[nodiscard]] int compare(const SplitValBase<A, B, C>& a,
                            const SplitValBase<A, B, C>& b,
                            const Level level) const {
    if (auto res =
            std::strncmp(&a.firstOriginalChar_, &b.firstOriginalChar_, 1);
        res != 0) {
      return res;  // different data types, decide on the datatype
    }

    if (int res =
            // this correctly dispatches between SortKeys (already transformed)
            // and string_views (not-transformed, perform unicode collation)
        _locManager.compare(a.transformedVal_, b.transformedVal_, level);
        res != 0 || level != Level::TOTAL) {
      return res;  // actual value differs
    }

    // On the TOTAL level we then compare on the level of bytes.
    if (int res = a.fullInput_.compare(b.fullInput_); res != 0) {
      return res;
    }

    // Only if two literals are bytewise equal, we compare by the langtag or
    // datatype.
    return a.langtag_.compare(b.langtag_);
  }

  // Transform a string s from the vocabulary to the SplitVal of the first
  // possible vocabulary string that compares greater to s according to the held
  // locale on the PRIMARY level (other levels will cause an assertion fail.)
  //
  // This is needed for calculating whether one string is a prefix of another
  // CAVEAT: This currently only supports the primary collation Level!!!
  // <TODO<joka921>: Implement this on every level, either by fixing ICU or by
  // hacking the collation strings
  //
  // `s` is a UTF-8 encoded string that contains an element of an RDF triple,
  // `level` must be Level::PRIMARY. Return the PRIMARY level SortKey of the
  // first possible string greater than s.
  [[nodiscard]] SplitVal transformToFirstPossibleBiggerValue(
      std::string_view s, const Level level) const {
    AD_CONTRACT_CHECK(level == Level::PRIMARY);
    auto transformed = extractAndTransformComparable(s, Level::PRIMARY);
    // The `firstOriginalChar_` is either " or < or @
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

  // Obtain const access to the held `LocaleManagerT`.
  [[nodiscard]] const LocaleManagerT& getLocaleManager() const {
    return _locManager;
  }

  // Trivialy wraps `LocaleManagerT::normalizeUtf8`, see there for
  // documentation.
  [[nodiscard]] std::string normalizeUtf8(std::string_view sv) const {
    return _locManager.normalizeUtf8(sv);
  }

  // Handle to the default collation level.
  Level& defaultLevel() { return _defaultLevel; }
  [[nodiscard]] const Level& defaultLevel() const { return _defaultLevel; }

 private:
  LocaleManagerT _locManager;
  Level _defaultLevel = Level::IDENTICAL;

  // Split a string into its components to prepare collation.
  // SplitValType = SplitVal will transform the inner string according to the
  // locale SplitValTye = SplitValNonOwning will leave the inner string as is.
  template <class SplitValType>
  [[nodiscard]] SplitValType extractComparable(
      std::string_view a, [[maybe_unused]] const Level level) const {
    std::string_view res = a;
    const char first = a.empty() ? char{0} : a[0];
    std::string_view langtag;
    if (first == '"') {
      // only remove the first character in case of literals that always start
      // with a quotation mark. For all other types we need this. <TODO> rework
      // the vocabulary's data type to remove ALL of those hacks
      res.remove_prefix(1);
      // In the case of prefix filters we might also have
      // Literals that do not have the closing quotation mark
      auto endPos = ad_utility::findLiteralEnd(res, "\"");
      if (endPos != std::string::npos) {
        // this should also be fine if there is no langtag (endPos == size()
        // according to cppreference.com
        langtag = res.substr(endPos + 1);
        res.remove_suffix(res.size() - endPos);
      } else {
        langtag = "";
      }
    }
    if constexpr (std::is_same_v<SplitValType, SplitVal>) {
      return {first, _locManager.getSortKey(res, level), std::string{langtag},
              std::string{a}};
    } else if constexpr (std::is_same_v<SplitValType, SplitValNonOwning>) {
      return {first, res, langtag, a};
    } else {
      static_assert(ad_utility::alwaysFalse<SplitValType>);
    }
  }
};

#endif  // QLEVER_SRC_INDEX_STRINGSORTCOMPARATORTYPES_H
