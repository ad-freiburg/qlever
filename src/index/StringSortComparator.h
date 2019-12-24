//
// Created by johannes on 18.12.19.
//

#ifndef QLEVER_STRINGSORTCOMPARATOR_H
#define QLEVER_STRINGSORTCOMPARATOR_H

#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/unistr.h>
#include <unicode/utypes.h>
#include <cstring>
#include <memory>
#include "../util/Exception.h"
#include "../util/StringUtils.h"

enum class VocabularyType { PLAIN_STRING, TRIPLE_COMPONENTS };

class LocaleManager {
 public:
  // The five collation levels supported by icu, forwarded in a typesafe manner
  enum class Level : uint8_t {
    PRIMARY = 0,
    SECONDARY = 1,
    TERTIARY = 2,
    QUARTERNARY = 3,
    IDENTICAL = 4
  };

  // Wraps a string that contains unicode collation weights for another string
  // Only needed for making interfaces explicit and less errorProne
  class WeightString {
   public:
    WeightString() = default;
    explicit WeightString(std::string_view contents)
        : _content(contents){}[[nodiscard]] const std::string & get() const {
      return _content;
    }
    std::string& get() { return _content; }

   private:
    std::string _content;
  };

  // ____________________________________________________________________________________________________________
  LocaleManager(const LocaleManager& rhs)
      : _icuLocale(rhs._icuLocale),
        _ignorePunctuationStatus(rhs._ignorePunctuationStatus) {
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  // Current usage requires default-constructibility. Default to "en_US" and
  // respecting symbols on all the levels
  LocaleManager() : LocaleManager("en", "US", false){};

  /**
   * @param lang The language of the locale, e.g. "en" or "de"
   * @param country The country of the locale, e.g. "US" or "CA"
   * @param ignorePunctuationAtFirstLevel If true then spaces/punctuation etc.
   * will only be considered for comparisons if strings match otherwise Throws
   * std::runtime_error if the locale cannot be constructed from lang and
   * country args
   *
   * \todo: make the exact punctuation level configurable.
   */
  LocaleManager(const std::string& lang, const std::string& country,
                bool ignorePunctuationAtFirstLevel) {
    _icuLocale = icu::Locale(lang.c_str(), country.c_str());
    _ignorePunctuationStatus =
        ignorePunctuationAtFirstLevel ? UCOL_SHIFTED : UCOL_NON_IGNORABLE;

    if (_icuLocale.isBogus() == TRUE) {
      throw std::runtime_error("Could not create locale with language " + lang +
                               " and Country " + country);
    }
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  // ____________________________________________________________________
  LocaleManager& operator=(const LocaleManager& other) {
    if (this == &other) return *this;
    _icuLocale = other._icuLocale;
    _ignorePunctuationStatus = other._ignorePunctuationStatus;
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
    return *this;
  }

  // _____________________________________________________________________
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level) const {
    UErrorCode err = U_ZERO_ERROR;
    auto idx = static_cast<uint8_t>(level);
    auto res = compToInd(
        _collator[idx]->compareUTF8(toStringPiece(a), toStringPiece(b), err));
    raise(err);
    return res;
  }

  // _____________________________________________________________________
  static int compare(WeightString a, WeightString b,
                     [[maybe_unused]] const Level) {
    return std::strcmp(a.get().c_str(), b.get().c_str());
  }

  // transform a UTF-8 string into a weight string that can be compared using
  // std::strcmp. needed because ICU only works on utf16 and does not create a
  // std::string.
  [[nodiscard]] WeightString getSortKey(std::string_view s,
                                        const Level level) const {
    auto utf16 = icu::UnicodeString::fromUTF8(toStringPiece(s));
    auto& col = *_collator[static_cast<uint8_t>(level)];
    auto sz = col.getSortKey(utf16, nullptr, 0);
    WeightString finalRes;
    std::string& res = finalRes.get();
    res.resize(sz);
    static_assert(sizeof(uint8_t) == sizeof(std::string::value_type));
    sz = col.getSortKey(utf16, reinterpret_cast<uint8_t*>(res.data()),
                        res.size());
    AD_CHECK(sz == static_cast<decltype(sz)>(
                       res.size()));  // this is save by the way we obtained sz
    // since this is a c-api we still have a trailing '\0'. Trimming this is
    // necessary for the prefix range to work correct.
    res.resize(res.size() - 1);
    return finalRes;
  }

      [[nodiscard]] std::string getLowercaseUtf8(const std::string& s) const {
    auto utf16 = icu::UnicodeString::fromUTF8(s);
    utf16 = utf16.toLower(_icuLocale);
    std::string res;
    utf16.toUTF8String(res);
    return res;
  }

  /// get a prefix of length prefixLength of the UTF8 string (or shorter, if the
  /// string contains less codepoints This counts utf-8 codepoints correctly and
  /// returnsa UTF-8 string referring to the Prefix and the number of utf
  /// codepoints it actually encodes ( <= prefixLength).
  static std::pair<size_t, std::string> getUTF8Prefix(std::string_view sp,
                                                      size_t prefixLength) {
    const char* s = sp.data();
    int32_t length = sp.length();
    size_t numCodepoints = 0;
    int32_t i = 0;
    for (i = 0; i < length && numCodepoints < prefixLength;) {
      UChar32 c;
      U8_NEXT(s, i, length, c);
      if (c >= 0) {
        ++numCodepoints;
      } else {
        throw std::runtime_error(
            "Illegal UTF sequence in LocaleManager::getUTF8Prefix");
      }
    }
    return {numCodepoints, std::string(sp.data(), i)};
  }

 private:
  icu::Locale _icuLocale;  /// the held locale
  /// one collator for each collation Level to make this class threadsafe
  /// needed because setting the collation level and comparing strings are 2
  /// different steps in icu.
  std::unique_ptr<icu::Collator> _collator[5];
  UColAttributeValue _ignorePunctuationStatus =
      UCOL_NON_IGNORABLE;  /// how to sort punctuations etc.

  // raise an exception if the error code holds an error.
  static void raise(const UErrorCode& err) {
    if (U_FAILURE(err)) {
      throw std::runtime_error(u_errorName(err));
    }
  }

  // create one collator for each of the possible collation levels.
  // has to be called each time the locale is changed.
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

  // convert LESS EQUAL GREATER from icu to -1, 0, +1 to make it equivalent to
  // results from std::strcmp
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

  /// This conversion is needed for "older" versions of ICU, e.g. ICU60 which is
  /// contained in Ubuntu's LTS repositories
  static icu::StringPiece toStringPiece(std::string_view s) {
    return icu::StringPiece(s.data(), s.size());
  }
};

/**
 * \brief This class compares strings according to proper Unicode collation,
 * e.g. Strings from the text index vocabulary To Compare components of RDFS
 * triples use the TripleComponentComparator defined below
 */
class SimpleStringComparator {
 public:
  using Level = LocaleManager::Level;
  using Transformed_T = LocaleManager::WeightString;
  SimpleStringComparator(const std::string& lang, const std::string& country,
                         bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}
  SimpleStringComparator() = default;

  /**
   * \brief Compare two Strings
   * @return True iff a comes before b
   */
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::IDENTICAL) const {
    return _locManager.compare(a, b, level) < 0;
  }

  // ________________________________________________________________________________________________
  bool operator()(std::string_view a, const LocaleManager::WeightString& b,
                  const Level level) const {
    auto aTrans = _locManager.getSortKey(a, level);
    auto cmp = _locManager.compare(aTrans, b, level);
    return cmp < 0;
  }

  /// transform a string s to the collation weight string of the first possible
  /// string that compares greater to s according to the held locale. Needed for
  /// Prefix search. Currently only supports the primary collation Level!!!
  /// <TODO<joka921>: Implement this on every level, either by fixing ICU or by
  /// hacking the collation strings
  [[nodiscard]] LocaleManager::WeightString transformToFirstPossibleBiggerValue(
      std::string_view s) const {
    auto transformed = _locManager.getSortKey(s, Level::PRIMARY);
    unsigned char last = transformed.get().back();
    if (last < std::numeric_limits<unsigned char>::max()) {
      transformed.get().back() += 1;
    } else {
      transformed.get().push_back('\0');
    }
    return transformed;
  }

      [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _locManager;
  }

 private:
  LocaleManager _locManager;
};

/**
 * \brief Handles the comparisons between the vocabulary's entries according to
 * their data types and proper Unicode collation.
 *
 *  General Approach: First Sort by the datatype, then by the actual value and
 * then by the language tag.
 */
class TripleComponentComparator {
 public:
  using Level = LocaleManager::Level;

  TripleComponentComparator(const std::string& lang, const std::string& country,
                            bool ignorePunctuationAtFirstLevel)
      : _locManager(lang, country, ignorePunctuationAtFirstLevel) {}
  TripleComponentComparator() = default;

  /**
   * \brief An entry of the Vocabulary, split up into its components and
   * possibly converted to a format that is easier to compare
   *
   * @tparam ST either std::string or std::string_view. Since both variants
   * differ greatly in their usage they are commented with the template
   * instantiations
   */
  template <class InnerString, class LanguageTag>
  struct SplitValBase {
    SplitValBase() = default;
    SplitValBase(char fst, InnerString trans, LanguageTag l)
        : firstOriginalChar(fst),
          transformedVal(std::move(trans)),
          langtag(std::move(l)) {}
    char firstOriginalChar =
        '\0';  // The first char of the original value, used to distinguish
               // between different datatypes
    InnerString transformedVal;  // the original inner value, possibly
                                 // transformed by a locale().
    LanguageTag langtag;         // the language tag, possibly empty
  };

  /**
   * This value owns all its contents. This is used to transform the inner value
   * and to safely pass it around.
   */
  using SplitVal = SplitValBase<LocaleManager::WeightString, std::string>;
  using Transformed_T = SplitVal;

  // only used within the class.
  using SplitValNonOwning = SplitValBase<std::string_view, std::string_view>;

  /**
   * \brief Compare two elements from the vocabulary.
   * @return false iff a comes before b in the vocabulary
   */
  bool operator()(std::string_view a, std::string_view b,
                  const Level level = Level::IDENTICAL) const {
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
    auto spA = extractAndTransformComparable(a, level);
    return compare(spA, spB, level) < 0;
  }

  bool operator()(const SplitVal& a, const SplitVal& b,
                  const Level level) const {
    return compare(a, b, level) < 0;
  }

  /// Compare to string_views. Return value according to std::strcmp
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::IDENTICAL) const {
    // The level argument is actually ignored TODO: fix this.
    auto splitA = extractComparable<SplitValNonOwning>(a, level);
    auto splitB = extractComparable<SplitValNonOwning>(b, level);
    return compare(splitA, splitB, level);
  }

      /// @brief split a literal or iri into its components and convert the
      /// inner value
      ///           according to the held locale
      [[nodiscard]] SplitVal
      extractAndTransformComparable(std::string_view a,
                                    const Level level) const {
    return extractComparable<SplitVal>(a, level);
  }

  /// @brief the inner comparison logic
  /// Comp must return 0 on equality of inner strings, <0 if a < b and >0 if  a
  /// > b
  template <class A, class B>
  [[nodiscard]] int compare(const SplitValBase<A, B>& a,
                            const SplitValBase<A, B>& b,
                            const Level level) const {
    if (auto res = std::strncmp(&a.firstOriginalChar, &b.firstOriginalChar, 1);
        res != 0) {
      return res;  // different data types, decide on the datatype
    }

    if (int res =
            _locManager.compare(a.transformedVal, b.transformedVal, level);
        res != 0) {
      return res;  // actual value differs
    }
    return a.langtag.compare(
        b.langtag);  // if everything else matches, we sort by the langtag
  }

      /// transform element s from the vocabulary to the first possible entry
      /// that compares greater to s according to the held locale and level.
      /// Needed for Prefix search. Currently only supports the primary
      /// collation Level!!! <TODO<joka921>: Implement this on every level,
      /// either by fixing ICU or by hacking the collation strings
      [[nodiscard]] SplitVal
      transformToFirstPossibleBiggerValue(std::string_view s) const {
    auto transformed = extractAndTransformComparable(s, Level::PRIMARY);
    unsigned char last = transformed.transformedVal.get().back();
    if (last < std::numeric_limits<unsigned char>::max()) {
      transformed.transformedVal.get().back() += 1;
    } else {
      transformed.transformedVal.get().push_back('\0');
    }
    return transformed;
  }

  [[nodiscard]] const LocaleManager& getLocaleManager() const {
    return _locManager;
  } private : LocaleManager _locManager;

  /// Split a string into its components to prepare collation.
  /// SplitValType = SplitVal will transform the inner string according to the
  /// locale SplitValTye = SplitValNonOwning will leave the inner string as is.
  template <class SplitValType>
  [[nodiscard]] SplitValType extractComparable(
      std::string_view a, [[maybe_unused]] const Level level) const {
    std::string_view res = a;
    const char first = a.empty() ? char(0) : a[0];
    std::string_view langtag;
    if (ad_utility::startsWith(res, "\"")) {
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
      return {first, _locManager.getSortKey(res, level), std::string(langtag)};
    } else if constexpr (std::is_same_v<SplitValType, SplitValNonOwning>) {
      return {first, res, langtag};
    } else {
      SplitValType().ThisShouldNotCompile();
    }
  }
};

#endif  // QLEVER_STRINGSORTCOMPARATOR_H
