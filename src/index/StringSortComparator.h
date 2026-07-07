//  Copyright 2019, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_STRINGSORTCOMPARATOR_H
#define QLEVER_STRINGSORTCOMPARATOR_H

#include <unicode/bytestream.h>
#include <unicode/casemap.h>
#include <unicode/coleitr.h>
#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/stringpiece.h>
#include <unicode/tblcoll.h>
#include <unicode/utf8.h>
#include <unicode/utypes.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "global/Constants.h"
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

  /// Copy constructor
  LocaleManager(const LocaleManager& rhs)
      : _icuLocale(rhs._icuLocale),
        _ignorePunctuationStatus(rhs._ignorePunctuationStatus) {
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  /// Default constructor. Use the settings from "../global/Constants.h"
  LocaleManager()
      : LocaleManager(std::string{LOCALE_DEFAULT_LANG},
                      std::string{LOCALE_DEFAULT_COUNTRY},
                      LOCALE_DEFAULT_IGNORE_PUNCTUATION) {}

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
    auto res = static_cast<int>(
        _collator[idx]->compareUTF8(toStringPiece(a), toStringPiece(b), err));
    raise(err);
    return res;
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

  // Count the number of primary-weight collation elements (non-zero primary
  // order) in a UTF-8 encoded string. Returns raw ICU element weights;
  // UCOL_SHIFTED is not applied here, so variable characters (e.g.,
  // punctuation) are still counted even when ignorePunctuation is true.
  [[nodiscard]] size_t countPrimaryCollationElements(
      std::string_view text) const {
    return walkPrimaryElements(text, std::numeric_limits<size_t>::max())
        .numElements;
  }

  // Return the byte length of the shortest UTF-8 prefix of `text` that
  // contains exactly `numPrimaryElements` primary-weight collation elements.
  // If `text` has fewer than `numPrimaryElements` primary elements, returns
  // `text.size()`.
  [[nodiscard]] size_t primaryCollationPrefixLength(
      std::string_view text, size_t numPrimaryElements) const {
    return walkPrimaryElements(text, numPrimaryElements).byteOffset;
  }

 private:
  icu::Locale _icuLocale;  // the held locale
  /* One collator for each collation Level to make this class threadsafe.
   * Needed because setting the collation level and comparing strings are 2
   * different steps in icu. */
  std::unique_ptr<icu::Collator> _collator[6];
  UColAttributeValue _ignorePunctuationStatus =
      UCOL_NON_IGNORABLE;  // how to sort punctuation etc.

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

  /* This conversion is needed for "older" versions of ICU, e.g. ICU60 which is
   * contained in Ubuntu's LTS repositories */
  static icu::StringPiece toStringPiece(std::string_view s) {
    return icu::StringPiece(s.data(), s.size());
  }

  // Create a `icu::CollationElementIterator` for the given UTF-8 string.
  std::unique_ptr<icu::CollationElementIterator> makeCollationElementIterator(
      std::string_view input) const {
    auto& collator = *_collator[static_cast<uint8_t>(Level::PRIMARY)];
    icu::UnicodeString ustr =
        icu::UnicodeString::fromUTF8(toStringPiece(input));
    return std::unique_ptr<icu::CollationElementIterator>{
        dynamic_cast<icu::RuleBasedCollator&>(collator)
            .createCollationElementIterator(ustr)};
  }

  struct PrimaryWalkResult {
    size_t numElements;
    size_t byteOffset;
  };

  // Iterate through primary collation elements of `text`, stopping after
  // `targetCount` non-ignorable primary elements (or at end-of-string).
  // Returns the number of primary elements seen and the UTF-8 byte offset
  // directly after the last one (`text.size()` if the string ended first).
  [[nodiscard]] PrimaryWalkResult walkPrimaryElements(
      std::string_view text, size_t targetCount) const {
    if (targetCount == 0) {
      return {0, 0};
    }
    UErrorCode err = U_ZERO_ERROR;
    auto iter = makeCollationElementIterator(text);
    size_t count = 0;
    while (true) {
      int32_t elem = iter->next(err);
      raise(err);
      if (elem == icu::CollationElementIterator::NULLORDER) {
        break;
      }
      if (icu::CollationElementIterator::primaryOrder(elem) != 0) {
        ++count;
        if (count == targetCount) {
          return {count, utf16OffsetToUtf8ByteOffset(text, iter->getOffset())};
        }
      }
    }
    return {count, text.size()};
  }

  // Walk the UTF-8 bytes of `utf8String`, counting UTF-16 code units, and
  // return the byte offset that corresponds to `utf16Offset` UTF-16 code units
  // from the start.
  static size_t utf16OffsetToUtf8ByteOffset(std::string_view utf8String,
                                            int32_t utf16Offset) {
    const char* s = utf8String.data();
    int32_t byteIdx = 0;
    int32_t utf16Count = 0;
    int32_t len = static_cast<int32_t>(utf8String.size());
    while (byteIdx < len && utf16Count < utf16Offset) {
      UChar32 c;
      int32_t next = byteIdx;
      U8_NEXT(s, next, len, c);
      utf16Count += c > 0xFFFF ? 2 : 1;
      byteIdx = next;
    }
    return static_cast<size_t>(byteIdx);
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

  // An entry of the Vocabulary, split up into its components. Used internally
  // to implement `compare(std::string_view, std::string_view)` so that
  // datatype/language tag tiebreaking is handled consistently.
  struct SplitVal {
    // The first char of the original value, used to distinguish between
    // different datatypes
    char firstOriginalChar_;
    // The original inner value.
    std::string_view innerValue_;
    // The language tag, possibly empty.
    std::string_view langtag_;
    std::string_view fullInput_;
  };

  /**
   * \brief Compare two elements from the Vocabulary.
   * @return false iff a comes before b in the vocabulary
   */
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::QUARTERNARY) const {
    return compare(a, b, level) < 0;
  }

  // Compare two string_views from the Vocabulary. Return value according to
  // std::strcmp
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    auto splitA = extractComparable(a);
    auto splitB = extractComparable(b);
    // We have to have a total ordering of unique elements in the vocabulary,
    // so if they compare equal according to the locale, use strcmp
    return compare(splitA, splitB, level);
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

  // First compares the datatypes by the firstOriginalChar_, then the inner
  // value and then the language tags.
  [[nodiscard]] int compare(const SplitVal& a, const SplitVal& b,
                            const Level level) const {
    if (auto res =
            std::strncmp(&a.firstOriginalChar_, &b.firstOriginalChar_, 1);
        res != 0) {
      return res;  // different data types, decide on the datatype
    }

    if (int res = _locManager.compare(a.innerValue_, b.innerValue_, level);
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

 private:
  LocaleManager _locManager;

  /// Split a string into its components (datatype-indicator first char, inner
  /// value, language tag) to prepare locale-aware collation.
  [[nodiscard]] static SplitVal extractComparable(std::string_view fullInput) {
    std::string_view res = fullInput;
    const char first = fullInput.empty() ? char{0} : fullInput[0];
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
    return {first, res, langtag, fullInput};
  }
};

#endif  // QLEVER_STRINGSORTCOMPARATOR_H
