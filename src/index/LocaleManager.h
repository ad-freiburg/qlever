// Copyright 2026 The QLever Authors, in particular:
//
// 2019 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_LOCALEMANAGER_H
#define QLEVER_SRC_INDEX_LOCALEMANAGER_H

#ifndef QLEVER_NO_UNICODE
#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/utypes.h>
#endif  // QLEVER_NO_UNICODE

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/GenericCharTraits.h"
#include "util/StringUtils.h"

// Base class holding the types shared by both the ICU and the NoICU variants of
// the `LocaleManager` (see below). None of these types depend on ICU.
class LocaleManagerBase {
 public:
  // The five collation levels supported by icu, forwarded in a typesafe manner.
  enum class Level : uint8_t {
    PRIMARY = 0,
    SECONDARY = 1,
    TERTIARY = 2,
    QUARTERNARY = 3,
    IDENTICAL = 4,
    // If the `IDENTICAL` level returns equal, we take the language tag into
    // account and then the result of `strcmp`; that way two strings that have a
    // different byte representation never compare equal.
    TOTAL = 5
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

  class SortKey {
   public:
    SortKey() = default;
    explicit SortKey(U8String sortKey) : sortKey_(std::move(sortKey)) {}
    [[nodiscard]] constexpr const U8String& get() const noexcept {
      return sortKey_;
    }
    constexpr U8String& get() noexcept { return sortKey_; }

    // Comparison of sort key is done lexicographically on the byte values
    // of member `sortKey_`
    [[nodiscard]] int compare(const SortKey& rhs) const noexcept {
      return U8StringView{sortKey_}.compare(U8StringView{rhs.sortKey_});
    }

    QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(SortKey, sortKey_)

    // Is this sort key a prefix of another sort key. Note: This does not imply
    // any guarantees on the relation of the underlying strings.
    bool starts_with(const SortKey& rhs) const noexcept {
      return ql::starts_with(get(), rhs.get());
    }

    // Return the number of bytes in the `SortKey`.
    std::string::size_type size() const noexcept { return get().size(); }

   private:
    U8String sortKey_;
  };
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
template <typename LocaleManagerT>
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
        ad_utility::getUTF8Prefix(s, prefixLengthSoFar);
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

#ifndef QLEVER_NO_UNICODE

// This class wraps all calls to the ICU library that are required by QLever
// when comparing strings according to a Locale.
class LocaleManagerICU : public LocaleManagerBase {
 public:
  // Copy constructor.
  LocaleManagerICU(const LocaleManagerICU& rhs)
      : LocaleManagerBase(),
        _icuLocale(rhs._icuLocale),
        _ignorePunctuationStatus(rhs._ignorePunctuationStatus) {
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  // Default constructor. Use the settings from "../global/Constants.h"
  LocaleManagerICU()
      : LocaleManagerICU(std::string{LOCALE_DEFAULT_LANG},
                         std::string{LOCALE_DEFAULT_COUNTRY},
                         LOCALE_DEFAULT_IGNORE_PUNCTUATION) {}

  // `lang` is the language of the locale, e.g. "en" or "de". `country` is the
  // country of the locale, e.g. "US" or "CA". If
  // `ignorePunctuationAtFirstLevel` is true then spaces/punctuation etc. will
  // only be considered for comparisons if strings match otherwise. Throws
  // `std::runtime_error` if the locale cannot be constructed from `lang` and
  // `country`. \todo(joka921): make the exact punctuation level configurable.
  LocaleManagerICU(const std::string& lang, const std::string& country,
                   bool ignorePunctuationAtFirstLevel) {
    _icuLocale = icu::Locale(lang.c_str(), country.c_str());
    _ignorePunctuationStatus =
        ignorePunctuationAtFirstLevel ? UCOL_SHIFTED : UCOL_NON_IGNORABLE;

    if (_icuLocale.isBogus()) {
      throw std::runtime_error("Could not create locale with language " + lang +
                               " and Country " + country);
    }
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  // Assign from another LocaleManagerICU.
  LocaleManagerICU& operator=(const LocaleManagerICU& other) {
    if (this == &other) return *this;
    _icuLocale = other._icuLocale;
    _ignorePunctuationStatus = other._ignorePunctuationStatus;
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
    return *this;
  }

  // Compare two UTF-8 encoded string_views according to the held Locale.
  // Compare according to the collation Level `level`. Return <0 iff a<b, >0 iff
  // a>b, 0 iff a==b.
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level) const {
    UErrorCode err = U_ZERO_ERROR;
    auto idx = static_cast<uint8_t>(level);
    auto res = compToInd(
        _collator[idx]->compareUTF8(toStringPiece(a), toStringPiece(b), err));
    raise(err);
    return res;
  }

  // Compare two WeightStrings. These have to be extracted by a call to
  // getSortKey using the same level specification and on the same LocaleManager
  // otherwise the behavior is undefined. The `level` parameter is ignored but
  // required to have a symmetric interface. Return <0 iff a<b, >0 iff a>b, 0
  // iff a==b.
  static int compare(const SortKey& a, const SortKey& b,
                     [[maybe_unused]] const Level = Level::PRIMARY) {
    return a.compare(b);
  }

  // Transform a UTF-8 string into a `SortKey`.
  //
  // We need this wrapper because ICU internally only works on utf16 and does
  // not create c++ strings in large parts of the API. `s` is a UTF-8 encoded
  // string, `level` the Collation Level for which we want to create the
  // SortKey. Return a `SortKey` s.t. compare(s, t, level) ==
  // compare(getSortKey(s, level), getSortKey(t, level)).
  SortKey getSortKey(std::string_view s, const Level level) const {
    auto utf16 = icu::UnicodeString::fromUTF8(toStringPiece(s));
    auto& col = *_collator[static_cast<uint8_t>(level)];
    std::vector<uint8_t> sortKeyBuffer;
    // The actual computation of the sort key is very expensive, so we first
    // allocate a buffer that is typically large enough to store the sort key.
    static constexpr size_t maxBufferSize = std::numeric_limits<int32_t>::max();
    sortKeyBuffer.resize(std::min(50 * s.size(), maxBufferSize));
    static_assert(sizeof(uint8_t) == sizeof(std::string::value_type));
    static constexpr auto intMax = std::numeric_limits<int32_t>::max();
    auto sz = col.getSortKey(utf16, sortKeyBuffer.data(),
                             static_cast<int32_t>(sortKeyBuffer.size()));
    AD_CONTRACT_CHECK(sz >= 0);
    // If the buffer was large enough, we only have to copy the sort key to the
    // destination. Otherwise, we now know the exact size of the sort key and
    // can retrigger the computation.
    if (static_cast<size_t>(sz) > sortKeyBuffer.size()) {
      sortKeyBuffer.clear();
      sortKeyBuffer.resize(sz);
      AD_CORRECTNESS_CHECK(sortKeyBuffer.size() <= static_cast<size_t>(intMax));
      auto actualSz =
          col.getSortKey(utf16, (sortKeyBuffer.data()),
                         static_cast<int32_t>(sortKeyBuffer.size()));
      AD_CONTRACT_CHECK(actualSz ==
                        static_cast<decltype(sz)>(sortKeyBuffer.size()));
    }
    // since this is a c-api we still have a trailing '\0'. Trimming this is
    // necessary for the prefix range to work correct.
    AD_CORRECTNESS_CHECK(sz > 0);
    --sz;
    SortKey result;
    U8String& resultView = result.get();
    resultView.insert(resultView.end(), sortKeyBuffer.data(),
                      sortKeyBuffer.data() + sz);
    return result;
  }

  // Get a `SortKey` for `Level::PRIMARY` that corresponds to a prefix of `s`.
  // For the exact semantics see `getPrefixSortKeyImpl` above.
  [[nodiscard]] std::pair<size_t, SortKey> getPrefixSortKey(
      std::string_view s, size_t prefixLength) const {
    return getPrefixSortKeyImpl(*this, s, prefixLength);
  }

  // Convert a UTF-8 String to lowercase according to the held locale. `s` is a
  // UTF-8 encoded string; return the lowercase version of s, also encoded as
  // UTF-8.
  [[nodiscard]] std::string getLowercaseUtf8(std::string_view s) const {
    return ad_utility::utf8ToLower(s, _icuLocale.getName());
  }

  // Normalize a Utf8 string to a canonical representation.
  // Maps e.g. single codepoint é and e + accent aigu to single codepoint é by
  // applying the UNICODE NFC (Normalization form C) This is independent from
  // the locale. `input` must be UTF-8 encoded; return the NFC canonical form in
  // UTF-8 encoding.
  [[nodiscard]] std::string normalizeUtf8(std::string_view input) const {
    std::string res;
    icu::StringByteSink<std::string> sink(&res);
    UErrorCode err = U_ZERO_ERROR;
    _normalizer->normalizeUTF8(0, toStringPiece(input), sink, nullptr, err);
    raise(err);
    return res;
  }

 private:
  icu::Locale _icuLocale;  // the held locale
  /* One collator for each collation Level to make this class threadsafe.
   * Needed because setting the collation level and comparing strings are 2
   * different steps in icu. */
  std::unique_ptr<icu::Collator> _collator[6];
  UColAttributeValue _ignorePunctuationStatus =
      UCOL_NON_IGNORABLE;  // how to sort punctuation etc.

  // Actually locale-independent, but useful to place here since it wraps ICU.
  // Initialized by the `setupCollators()` method.
  const icu::Normalizer2* _normalizer = nullptr;

  // raise an exception if the error code holds an error.
  static void raise(const UErrorCode& err) {
    if (U_FAILURE(err)) {
      throw std::runtime_error(u_errorName(err));
    }
  }

  /* create one collator for each of the possible collation levels.
   * has to be called each time the locale is changed. */
  void setupCollators() {
    for (auto& col : _collator) {
      UErrorCode err = U_ZERO_ERROR;
      col.reset(icu::Collator::createInstance(_icuLocale, err));
      raise(err);
    }
    _collator[static_cast<uint8_t>(Level::PRIMARY)]->setStrength(
        icu::Collator::PRIMARY);
    _collator[static_cast<uint8_t>(Level::SECONDARY)]->setStrength(
        icu::Collator::SECONDARY);
    _collator[static_cast<uint8_t>(Level::TERTIARY)]->setStrength(
        icu::Collator::TERTIARY);
    _collator[static_cast<uint8_t>(Level::QUARTERNARY)]->setStrength(
        icu::Collator::QUATERNARY);
    _collator[static_cast<uint8_t>(Level::IDENTICAL)]->setStrength(
        icu::Collator::IDENTICAL);
    // as far as the locale is concerned, the total and the identical level are
    // equivalent.
    _collator[static_cast<uint8_t>(Level::TOTAL)]->setStrength(
        icu::Collator::IDENTICAL);

    // also setup the normalizer
    UErrorCode err = U_ZERO_ERROR;
    _normalizer =
        icu::Normalizer2::getInstance(nullptr, "nfc", UNORM2_COMPOSE, err);
    raise(err);
  }

  // ______________________________________________________________________________
  void setIgnorePunctuationOnFirstLevels(UColAttributeValue val) {
    _ignorePunctuationStatus = val;
    UErrorCode err = U_ZERO_ERROR;
    for (auto& col : _collator) {
      col->setAttribute(UCOL_ALTERNATE_HANDLING, val, err);
      raise(err);
      // todo<joka921> : make this customizable for future versions
      col->setMaxVariable(UCOL_REORDER_CODE_SYMBOL, err);
      raise(err);
    }
  }

  // convert LESS EQUAL GREATER from icu to -1, 0, +1 to make results compatible
  // to std::strcmp
  static int compToInd(const UCollationResult res) {
    switch (res) {
      case UCOL_LESS:
        return -1;
      case UCOL_EQUAL:
        return 0;
      case UCOL_GREATER:
        return 1;
    }
    throw std::runtime_error(
        "Illegal value for UCollationResult. This should never happen!");
  }

  /* This conversion is needed for "older" versions of ICU, e.g. ICU60 which is
   * contained in Ubuntu's LTS repositories */
  static icu::StringPiece toStringPiece(std::string_view s) {
    return icu::StringPiece(s.data(), s.size());
  }
};

#endif  // QLEVER_NO_UNICODE

// A `LocaleManager` that completely ignores the locale, and only compares
// strings byte-wise (no UTF handling). Can be used if ICU is not available and
// correct unicode handling is not important.
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

  static int compare(const SortKey& a, const SortKey& b,
                     [[maybe_unused]] const Level = Level::PRIMARY) {
    return a.compare(b);
  }

  [[nodiscard]] SortKey getSortKey(std::string_view s,
                                   const Level /*level*/) const {
    return SortKey{::ranges::to<U8String>(s | ql::views::transform([](char c) {
                                            return static_cast<uint8_t>(c);
                                          }))};
  }

  [[nodiscard]] std::pair<size_t, SortKey> getPrefixSortKey(
      std::string_view s, size_t prefixLength) const {
    return getPrefixSortKeyImpl(*this, s, prefixLength);
  }

  // Lowercase `s`. As a preparatory step this still reuses the ICU-based
  // `ad_utility::utf8ToLower`; a truly ICU-free implementation will be added
  // when ICU is actually made optional.
  [[nodiscard]] std::string getLowercaseUtf8(std::string_view s) const {
    return ad_utility::utf8ToLower(s);
  }

  // As this class does no unicode handling, normalization is a no-op.
  [[nodiscard]] std::string normalizeUtf8(std::string_view input) const {
    return std::string(input);
  }
};

// Select the ICU or the NoICU locale manager depending on the build
// configuration (the `QLEVER_NO_UNICODE` macro is defined via the `NO_UNICODE`
// CMake option).
#ifdef QLEVER_NO_UNICODE
using LocaleManager = LocaleManagerNoICU;
#else
using LocaleManager = LocaleManagerICU;
#endif

#endif  // QLEVER_SRC_INDEX_LOCALEMANAGER_H
