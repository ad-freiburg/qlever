//  Copyright 2019, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_STRINGSORTCOMPARATOR_H
#define QLEVER_STRINGSORTCOMPARATOR_H

#include <unicode/casemap.h>
#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/utypes.h>

#include <cstring>
#include <memory>
#include <memory_resource>

#include "global/Constants.h"
#include "util/Exception.h"
#include "util/StringUtils.h"

/**
 * @brief This class wraps all calls to the ICU library that are required by
 * QLever It internally handles all conversion to and from UTF-8 and from c++ to
 * c-strings where they are required by ICU
 */
class LocaleManager {
 public:
  /// The five collation levels supported by icu, forwarded in a typesafe manner
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

  /**
   * A strong typedef for a string that contains unicode collation weights for
   * another string. The actual storage can be a `std::string` or a
   * `std::string_view`.
   */
  // TODO<GCC12> As soon as we have constexpr std::string, this class can
  //  become constexpr.
  using U8String = std::basic_string<uint8_t>;
  using U8StringView = std::basic_string_view<uint8_t>;

  template <ad_utility::SimilarToAny<U8String, U8StringView> T>
  class SortKeyImpl {
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

    auto operator<=>(const SortKeyImpl&) const = default;

    /// Is this sort key a prefix of another sort key. Note: This does not imply
    /// any guarantees on the relation of the underlying strings.
    bool starts_with(const SortKeyImpl& rhs) const noexcept {
      return get().starts_with(rhs.get());
    }

    /// Return the number of bytes in the `SortKey`
    std::string::size_type size() const noexcept { return get().size(); }

   private:
    T sortKey_;
  };
  using SortKey = SortKeyImpl<std::basic_string<uint8_t>>;
  using SortKeyView = SortKeyImpl<std::basic_string_view<uint8_t>>;

  /// Copy constructor
  LocaleManager(const LocaleManager& rhs)
      : _icuLocale(rhs._icuLocale),
        _ignorePunctuationStatus(rhs._ignorePunctuationStatus) {
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  /// Default constructor. Use the settings from "../global/Constants.h"
  LocaleManager()
      : LocaleManager(LOCALE_DEFAULT_LANG, LOCALE_DEFAULT_COUNTRY,
                      LOCALE_DEFAULT_IGNORE_PUNCTUATION){};

  /**
   * @param lang The language of the locale, e.g. "en" or "de"
   * @param country The country of the locale, e.g. "US" or "CA"
   * @param ignorePunctuationAtFirstLevel If true then spaces/punctuation etc.
   * will only be considered for comparisons if strings match otherwise Throws
   * std::runtime_error if the locale cannot be constructed from lang and
   * country args
   *
   * \todo(joka921): make the exact punctuation level configurable.
   */
  LocaleManager(const std::string& lang, const std::string& country,
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

  /// Assign from another LocaleManager
  LocaleManager& operator=(const LocaleManager& other) {
    if (this == &other) return *this;
    _icuLocale = other._icuLocale;
    _ignorePunctuationStatus = other._ignorePunctuationStatus;
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
    return *this;
  }

  /**
   * @brief Compare two UTF-8 encoded string_views according to the held Locale
   * @param a
   * @param b
   * @param level Compare according to this collation Level
   * @return <0 iff a<b , >0 iff a>b,  0 iff a==b
   */
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level) const {
    UErrorCode err = U_ZERO_ERROR;
    auto idx = static_cast<uint8_t>(level);
    auto res = compToInd(
        _collator[idx]->compareUTF8(toStringPiece(a), toStringPiece(b), err));
    raise(err);
    return res;
  }

  /**
   * @brief Compare two WeightStrings. These have to be extracted by a call to
   * getSortKey using the same level specification and on the same LocaleManager
   * otherwise the behavior is undefined
   * @param a
   * @param b
   * @param level This parameter is ignored but required to have a symmetric
   * interface
   * @return <0 iff a<b , >0 iff a>b,  0 iff a==b
   */
  template <typename T, typename U>
  static int compare(const SortKeyImpl<T>& a, const SortKeyImpl<U>& b,
                     [[maybe_unused]] const Level) {
    return a.compare(b);
  }

  /**
   * @brief Transform a UTF-8 string into a SortKey that can be compared using
   * std::strcmp.
   *
   * We need this wrapper because ICU internally only works on utf16 and does
   * not create c++ strings in large parts of the API
   * @param s A UTF-8 encoded string.
   * @param level The Collation Level for which we want to create the SortKey
   * @param resultFunction Will be called with a c-string (pointer and size) of
   * the result.
   * @return A weight string s.t. compare(s, t, level) ==
   * std::strcmp(getSortKey(s, level), getSortKey(t, level)
   */
  void getSortKey(
      std::string_view s, const Level level,
      std::invocable<const uint8_t*, size_t> auto resultFunction) const {
    // TODO<joka921> This function is one of the bottlenecks of the first pass
    // of the IndexBuilder One possible improvement is to reuse the memory
    // allocations for the `sortKeyBuffer`.
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
    resultFunction(sortKeyBuffer.data(), sz);
  }

  // Overload of `getSortKey` that returns a `SortKey` directly.
  [[nodiscard]] SortKey getSortKey(std::string_view s,
                                   const Level level) const {
    SortKey result;
    auto writeSortKey = [&result](const uint8_t* begin, size_t sz) {
      U8String& s = result.get();
      s.insert(s.end(), begin, begin + sz);
    };
    getSortKey(s, level, writeSortKey);
    return result;
  }

  /// Get a `SortKey` for `Level::PRIMARY` that corresponds to a prefix of `s`.
  /// \param s The input of which we want to obtain a sort key.
  /// \param prefixLength Obtain a SortKey for `prefixLength` many relevant
  /// characters
  ///               (see below).
  /// \return A `SortKey` that is a prefix of the `SortKey` for `s` w.r.t
  ///         `Level::PRIMARY` and that also is a `SortKey` for a prefix "p"
  ///         of `s`. "p" is the minimal prefix of `s` which consists of
  ///         at least `prefixLength` codepoints and whose SortKey fulfills the
  ///         first condition. Codepoints, which do not contribute to the
  ///         `SortKey` because they are irrelevant for the `PRIMARY` level do
  ///         not count towards `prefixLength`. The first element of the return
  ///         value is the actual number of (contributing) codepoints in "p". If
  ///         `s` contains less than `prefixLength` contributing codepoints,
  ///         then {totalNumberOfContributingCodepoints, completeSortKey} is
  ///         returned.
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
        // The `SortKey` changed by adding a codepoint, so that codepoint
        // was contributing.
        numContributingCodepoints++;
        sortKey = std::move(nextLongerSortKey);
      }
      if (numCodepoints < prefixLengthSoFar) {
        // We have checked the complete string without finding a sufficiently
        // long contributing prefix.
        break;
      }
      prefixLengthSoFar++;
    }
    return {numContributingCodepoints, std::move(sortKey)};
  }

  /**
   * @brief convert a UTF-8 String to lowercase according to the held locale
   * @param s UTF-8 encoded string
   * @return The lowercase version of s, also encoded as UTF-8
   */
  [[nodiscard]] std::string getLowercaseUtf8(const std::string_view s) const {
    std::string res;
    icu::StringByteSink<std::string> sink(&res);
    UErrorCode err = U_ZERO_ERROR;
    // The reason for the `icu::StringPiece` is that older versions of ICU (for
    // example, the standard version on Ubuntu 18.04) do not accept an
    // std::string_view here (newer versions do).
    icu::CaseMap::utf8ToLower(_icuLocale.getName(), 0,
                              icu::StringPiece(s.data(), s.size()), sink,
                              nullptr, err);
    raise(err);
    return res;
  }

  /**
   * @brief Normalize a Utf8 string to a canonical representation.
   * Maps e.g. single codepoint é and e + accent aigu to single codepoint é by
   * applying the UNICODE NFC (Normalization form C) This is independent from
   * the locale
   * @param input The String to be normalized. Must be UTF-8 encoded
   * @return The NFC canonical form of NFC in UTF-8 encoding.
   */
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
      UCOL_NON_IGNORABLE;  // how to sort punctuations etc.

  const icu::Normalizer2* _normalizer =
      nullptr;  // actually locale-independent but useful to be placed here
                // since it wraps ICU. Initialized by the setupCollators()
                // method

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

/**
 * @brief This class compares strings according to proper Unicode collation,
 * e.g. strings from the text index vocabulary. To compare components of
 * RDFS triples use the `TripleComponentComparator` defined below
 */
class SimpleStringComparator {
 public:
  using Level = LocaleManager::Level;
  /**
   * @param lang The language of the locale, e.g. "en" or "de"
   * @param country The country of the locale, e.g. "US" or "CA"
   * @param ignorePunctuationAtFirstLevel If true then spaces/punctuation etc.
   * will only be considered for comparisons if strings match otherwise Throws
   * std::runtime_error if the locale cannot be constructed from lang and
   * country args
   *
   * \todo(joka921): make the exact punctuation level configurable.
   */
  SimpleStringComparator(const std::string& lang, const std::string& country,
                         bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}

  /// Construct according to the default locale specified in
  /// ../global/Constants.h
  SimpleStringComparator() = default;

  /**
   * @brief Compare two UTF-8 encoded strings
   * @return True iff a comes before b
   */
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  /**
   * @brief compare the strings given the sortLevel
   * @return the same returning convention as std::strcmp
   */
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto cmpRes = _locManager.compare(a, b, level);
    if (cmpRes != 0 || level != Level::TOTAL) {
      return cmpRes;
    }
    return a.compare(b);
  }

  /**
   * @brief Compare a UTF-8 encoded string and a SortKey on the Primary Level
   * CAVEAT: The Level l argument IS IGNORED
   *
   * Since this class only exports WeightStrings on the PRIMARY level via the
   * transformToFirstPossibleBiggerValue method, we also always use the PRIMARY
   * level for this function to avoid mistakes
   * The Level argument is therefore ignored but left in as a dummy to make the
   * getLowerBoundLambda api of the Vocabulary easier.
   * @TODO<joka921> Allow prefix ranges on different levels.
   * @param a A UTF-8 encoded string
   * @param b This Weight string has to be obtained by a previous call to
   * transformToFirstPossibleBiggerValue
   * @
   * @return true iff a comes before the string whose SortKey is b
   */
  bool operator()(std::string_view a, const LocaleManager::SortKey& b,
                  [[maybe_unused]] const Level l) const {
    auto aTrans = _locManager.getSortKey(a, Level::PRIMARY);
    auto cmp = LocaleManager::compare(aTrans, b, Level::PRIMARY);
    return cmp < 0;
  }

  /**
   * @brief Transform a string s to the SortKey of the first possible
   * string that compares greater to s according to the held locale on the
   * PRIMARY level (other levels will cause an assertion fail.
   *
   * This is needed for calculating whether one string is a prefix of another
   * CAVEAT: This currently only supports the primary collation Level!!!
   * <TODO<joka921>: Implement this on every level, either by fixing ICU or by
   * hacking the collation strings
   *
   * @param s A UTF-8 encoded string
   * @return the PRIMARY level SortKey of the first possible string greater than
   * s
   */
  [[nodiscard]] LocaleManager::SortKey transformToFirstPossibleBiggerValue(
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

  /// Obtain access to the held LocaleManager
  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _locManager;
  }

 private:
  LocaleManager _locManager;
};
/**
 * @brief Handles the comparisons between RDFS triple elements according to
 * their data types and proper Unicode collation.
 *
 *  General Approach: First Sort by the datatype, then by the actual value
 * and then by the language tag.
 */
class TripleComponentComparator {
 public:
  using Level = LocaleManager::Level;

  /**
   * @param lang The language of the locale, e.g. "en" or "de"
   * @param country The country of the locale, e.g. "US" or "CA"
   * @param ignorePunctuationAtFirstLevel If true then spaces/punctuation etc.
   * will only be considered for comparisons if strings match otherwise Throws
   * std::runtime_error if the locale cannot be constructed from lang and
   * country args
   *
   * \todo(joka921): make the exact punctuation level configurable.
   */
  TripleComponentComparator(const std::string& lang, const std::string& country,
                            bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}

  /// Construct according to the default locale in "../global/Constants.h"
  TripleComponentComparator() = default;

  /**
   * @brief An entry of the Vocabulary, split up into its components and
   * possibly converted to a format that is easier to compare
   *
   * @tparam InnerString either LocaleManager::SortKey or std::string_view.
   * Both variants differ greatly in their usage. Details can be found after the
   * class definition, together with the explicit aliases `SplitVal` and
   * `SplitValOwning` for the template instantiations that are actually used.
   * the template instantiations
   * @tparam LanguageTag and FullString, either `std::string` or
   * `std::string_view`. They are used as deterministic tie breaks on the
   * `TOTAL` sort level.
   */
  template <class InnerString, class LanguageTag, class FullString>
  struct SplitValBase {
    SplitValBase() = default;
    SplitValBase(char fst, InnerString trans, LanguageTag l, bool externalized,
                 FullString fullInputForTotalComparison)
        : firstOriginalChar_(fst),
          transformedVal_(std::move(trans)),
          langtag_(std::move(l)),
          isExternalized_{externalized},
          fullInput_{std::move(fullInputForTotalComparison)} {}

    /// The first char of the original value, used to distinguish between
    /// different datatypes
    char firstOriginalChar_ = '\0';
    InnerString transformedVal_;   /// The original inner value, possibly
                                   /// transformed by a locale().
    LanguageTag langtag_;          /// The language tag, possibly empty.
    bool isExternalized_ = false;  /// Does this word belong to the externalized
                                   /// vocabulary.
    FullString fullInput_;
  };

  /**
   * This value owns all its contents.
   * The inner value is the SortKey of the original inner value according to the
   * held Locale. This is used to transform the inner value and to safely pass
   * it around, e.g. when performing prefix comparisons in the vocabulary
   */
  using SplitVal =
      SplitValBase<LocaleManager::SortKey, std::string, std::string>;

  /**
   * This only holds string_views to substrings of a string.
   * Currently we only use this inside this class
   */
  using SplitValNonOwning =
      SplitValBase<std::string_view, std::string_view, std::string_view>;

  // This class only holds `string_view`s, but the contents are stored as a view
  // to a sort key. This is used during index building for more efficient memory
  // allocations.
  using SplitValNonOwningWithSortKey =
      SplitValBase<LocaleManager::SortKeyView, std::string_view,
                   std::string_view>;

  /**
   * \brief Compare two elements from the Vocabulary.
   * @return false iff a comes before b in the vocabulary
   */
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  /**
   * @brief Compare a string_view from the vocabulary to a SplitVal that was
   * previously transformed
   * @param a Element of the vocabulary
   * @param spB this splitVal must have been obtained by a call to
   * extractAndTransformComparable
   * @param level
   * @return a comes before the original value of spB in the vocabulary
   */
  bool operator()(std::string_view a, const SplitVal& spB,
                  const Level level) const {
    auto spA = extractAndTransformComparable(a, level, false);
    return compare(spA, spB, level) < 0;
  }

  template <typename A, typename B, typename C>
  bool operator()(const SplitValBase<A, B, C>& a,
                  const SplitValBase<A, B, C>& b, const Level level) const {
    return compare(a, b, level) < 0;
  }

  /// Compare two string_views from the Vocabulary. Return value according to
  /// std::strcmp
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto splitA = extractComparable<SplitValNonOwning>(a, level, false);
    auto splitB = extractComparable<SplitValNonOwning>(b, level, false);
    // We have to have a total ordering of unique elements in the vocabulary,
    // so if they compare equal according to the locale, use strcmp
    auto cmp = compare(splitA, splitB, level);
    return cmp;
  }

  /**
   * @brief Split a literal or iri into its components and convert the inner
   * value according to the held locale
   */
  [[nodiscard]] SplitVal extractAndTransformComparable(
      std::string_view a, const Level level, bool isExternal = false) const {
    return extractComparable<SplitVal>(a, level, isExternal);
  }

  // Similar to `extractAndTransformComparable` but returns a `SplitVal` that
  // contains of `string_views` the contents of which will be allocated using
  // the `allocator`. Note: the user has to take care of the deallocation
  // manually, otherwise the memory will leak. This is currently used during the
  // index building.
  [[nodiscard]] SplitValNonOwningWithSortKey
  extractAndTransformComparableNonOwning(
      std::string_view a, const Level level, bool isExternal,
      std::pmr::polymorphic_allocator<char>* allocator) const {
    return extractComparable<SplitValNonOwningWithSortKey>(a, level, isExternal,
                                                           allocator);
  }

  /**
   * @brief the inner comparison logic
   *
   * First compares the datatypes by the firstOriginalChar_, then the inner
   * value and then the language tags
   * @return <0 iff a<b, 0 iff a==b, >0 iff a>b
   */
  template <class A, class B, typename C>
  [[nodiscard]] int compare(const SplitValBase<A, B, C>& a,
                            const SplitValBase<A, B, C>& b,
                            const Level level) const {
    // Currently all internal words stand before all external words.
    // TODO<joka921> This has to be changed once we have the IDs interleaved
    // via the MilestoneIdManager.
    if (a.isExternalized_ != b.isExternalized_) {
      return a.isExternalized_ ? 1 : -1;
    }

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

  /**
   *
   * @brief Transform a string s from the vocabulary to the SplitVal of the
   * first possible vocabulary string that compares greater to s according to
   * the held locale on the PRIMARY level (other levels will cause an assertion
   * fail.)
   *
   * This is needed for calculating whether one string is a prefix of another
   * CAVEAT: This currently only supports the primary collation Level!!!
   * <TODO<joka921>: Implement this on every level, either by fixing ICU or by
   * hacking the collation strings
   *
   * @param s A UTF-8 encoded string that contains an element of an RDF triple
   * @param level must be Level::PRIMARY
   * @return the PRIMARY level SortKey of the first possible string greater than
   * s
   */
  [[nodiscard]] SplitVal transformToFirstPossibleBiggerValue(
      std::string_view s, const Level level) const {
    AD_CONTRACT_CHECK(level == Level::PRIMARY);
    auto transformed = extractAndTransformComparable(s, Level::PRIMARY, false);
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

  /// obtain const access to the held LocaleManager
  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _locManager;
  }

  /**
   * @brief trivialy wraps LocaleManager::normalizeUtf8, see there for
   * documentation
   */
  [[nodiscard]] std::string normalizeUtf8(std::string_view sv) const {
    return _locManager.normalizeUtf8(sv);
  }

  /// handle to the default collation level
  Level& defaultLevel() { return _defaultLevel; }
  [[nodiscard]] const Level& defaultLevel() const { return _defaultLevel; }

 private:
  LocaleManager _locManager;
  Level _defaultLevel = Level::IDENTICAL;

  /* Split a string into its components to prepare collation.
   * SplitValType = SplitVal will transform the inner string according to the
   * locale SplitValTye = SplitValNonOwning will leave the inner string as is.
   */
  template <class SplitValType>
  [[nodiscard]] SplitValType extractComparable(
      std::string_view a, [[maybe_unused]] const Level level, bool isExternal,
      std::pmr::polymorphic_allocator<char>* allocator = nullptr) const {
    std::string_view res = a;
    const char first = a.empty() ? char(0) : a[0];
    std::string_view langtag;
    if (res.starts_with('"')) {
      // only remove the first character in case of literals that always start
      // with a quotation mark. For all other types we need this. <TODO> rework
      // the vocabulary's data type to remove ALL of those hacks
      res.remove_prefix(1);
      // In the case of prefix filters we might also have
      // Literals that do not have the closing quotation mark
      auto endPos = ad_utility::findLiteralEnd(res, "\"");
      if (endPos != string::npos) {
        // this should also be fine if there is no langtag (endPos == size()
        // according to cppreference.com
        langtag = res.substr(endPos + 1);
        res.remove_suffix(res.size() - endPos);
      } else {
        langtag = "";
      }
    }
    if constexpr (std::is_same_v<SplitValType, SplitVal>) {
      return {first, _locManager.getSortKey(res, level), std::string(langtag),
              isExternal, std::string{a}};
    } else if constexpr (std::is_same_v<SplitValType, SplitValNonOwning>) {
      return {first, res, langtag, isExternal, a};
    } else if constexpr (std::is_same_v<SplitValType,
                                        SplitValNonOwningWithSortKey>) {
      // For the non-owning sort key we allocate all the strings using the
      // `allocator`
      AD_CONTRACT_CHECK(allocator != nullptr);
      auto add =
          [allocator]<typename Char>(
              std::basic_string_view<Char> s) -> std::basic_string_view<Char> {
        auto alloc =
            std::pmr::polymorphic_allocator<Char>(allocator->resource());
        auto ptr = alloc.allocate(s.size());
        std::ranges::copy(s, ptr);
        return {ptr, ptr + s.size()};
      };
      LocaleManager::SortKeyView sortKey;
      auto writeSortKey = [&sortKey, &add](const uint8_t* begin, size_t sz) {
        sortKey = LocaleManager::SortKeyView{
            add(LocaleManager::U8StringView{begin, sz})};
      };
      _locManager.getSortKey(res, level, writeSortKey);
      return {first, sortKey, add(langtag), isExternal, add(a)};
    } else {
      static_assert(ad_utility::alwaysFalse<SplitValType>);
    }
  }
};

#endif  // QLEVER_STRINGSORTCOMPARATOR_H
