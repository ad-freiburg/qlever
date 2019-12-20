//
// Created by johannes on 18.12.19.
//

#ifndef QLEVER_STRINGSORTCOMPARATOR_H
#define QLEVER_STRINGSORTCOMPARATOR_H

#include <unicode/locid.h>
#include <unicode/utypes.h>
#include <unicode/coll.h>
#include <unicode/unistr.h>

/**
 * \brief Handles the comparisons between the vocabulary's entries according to their data types and proper Unicode collation.
 *
 *  General Approach: First Sort by the datatype, then by the actual value and then by the language tag.
 */
class StringSortComparator {
public:

  // The five collation levels supported by icu, forwarded in a typesafe manner
  enum class Level : uint8_t {
    PRIMARY = 0, SECONDARY = 1, TERTIARY = 2, QUARTERNARY = 3, IDENTICAL = 4
  };

  // ____________________________________________________________________________________________________________
  StringSortComparator(const StringSortComparator& rhs) : _icuLocale(rhs._icuLocale), _ignorePunctuationStatus(rhs._ignorePunctuationStatus) {
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  // Current usage requires default-constructibility. Default to "en_US" and respecting symbols on all the levels
  StringSortComparator() : StringSortComparator("en", "US" , false) {};


  /**
   * @param lang The language of the locale, e.g. "en" or "de"
   * @param country The country of the locale, e.g. "US" or "CA"
   * @param ignorePunctuationAtFirstLevel If true then spaces/punctuation etc. will only be considered for comparisons if strings match otherwise
   * Throws std::runtime_error if the locale cannot be constructed from lang and country args
   *
   * \todo: make the exact punctuation level configurable.
   */
  StringSortComparator(const std::string& lang, const std::string& country, bool ignorePunctuationAtFirstLevel) {
    _icuLocale = icu::Locale(lang.c_str(), country.c_str());
    _ignorePunctuationStatus = ignorePunctuationAtFirstLevel ? UCOL_SHIFTED : UCOL_NON_IGNORABLE;

    if (_icuLocale.isBogus() == TRUE) {
      throw std::runtime_error("Could not create locale with language " + lang + " and Country " + country);
    }
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
  }

  // ____________________________________________________________________
  StringSortComparator& operator=(const StringSortComparator& other) {
    if (this == &other) return *this;
    _icuLocale = other._icuLocale;
    _ignorePunctuationStatus = other._ignorePunctuationStatus;
    setupCollators();
    setIgnorePunctuationOnFirstLevels(_ignorePunctuationStatus);
    return *this;
  }

  /**
   * \brief An entry of the Vocabulary, split up into its components and possibly converted to a
   *        format that is easier to compare
   *
   * @tparam ST either std::string or std::string_view. Since both variants differ greatly in their usage
   *            they are commented with the template instantiations
   */
  template<class ST>
  struct SplitValBase {
    SplitValBase() = default;
    SplitValBase(char fst, ST trans, ST l)
            : firstOriginalChar(fst), transformedVal(std::move(trans)), langtag(std::move(l)) {}
    char firstOriginalChar = '\0';  // The first char of the original value, used to distinguish between different datatypes
    ST transformedVal;  // the original inner value, possibly transformed by a locale().
    ST langtag;  // the language tag, possibly empty
  };

  /**
   * This value owns all its contents. This is used to transform the inner value and to safely pass it around.
   */
  using SplitVal = SplitValBase<std::string>;

  // only used within the class.
  using SplitValNonOwning = SplitValBase<std::string_view>;


  /**
   * \brief Compare two elements from the vocabulary.
   * @return false iff a comes before b in the vocabulary
   */
  bool operator()(std::string_view a, std::string_view b, const Level level=Level::IDENTICAL) const {
    return compareViews(a, b, level) < 0;
  }

  /**
   * @brief Compare a string_view from the vocabulary to a SplitVal that was previously transformed
   * @param a Element of the vocabulary
   * @param spB this splitVal must have been obtained by a call to extractAndTransformComparable
   * @param level
   * @return a comes before the originial value of spB in the vocabulary
   */
  bool operator()(std::string_view a , const SplitVal& spB, const Level level) const {
    auto spA = extractAndTransformComparable(a, level);
    return compareCStyle(spA, spB);
  }


  /// Compare to string_views. Return value according to std::strcmp
  int compareViews(std::string_view a, std::string_view b, const Level level=Level::IDENTICAL) const {
    // The level argument is actually ignored TODO: fix this.
    auto splitA = extractComparable<SplitValNonOwning>(a, level);
    auto splitB = extractComparable<SplitValNonOwning>(b, level);
    return compareByLocale(splitA, splitB, level);
  }


  /// @brief split a literal or iri into its components and convert the inner value
  ///           according to the held locale
  SplitVal extractAndTransformComparable(std::string_view a, const Level level) const {
    return extractComparable<SplitVal>(a, level);
  }


  /// Compare a and b. For the comparison of the inner values std::strcmp is used. This is correct iff
  /// a and b were obtained by calls to extractAndTransformComparable
  template<class SplitValType>
  static bool compareCStyle(const SplitValType& a, const SplitValType& b) {
    return compare(a, b, [](const std::string& a, const std::string& b){ return std::strcmp(a.c_str(), b.c_str());}) < 0;
  }

  /// Compare a and b. The inner values are treated as UTF8-Strings that are compared according to the held
  /// locale and the specified level.
  template<class SplitValType>
  int compareByLocale(const SplitValType& a, const SplitValType& b, const Level level = Level::IDENTICAL) const {
    const auto c = [this, level](const auto& a, const auto& b) {
      UErrorCode err = U_ZERO_ERROR;
      auto idx = static_cast<uint8_t>(level);
      auto res = compToInd(_collator[idx]->compareUTF8(toStringPiece(a), toStringPiece(b), err));
      raise(err);
      return res;
    };

    return compare(a, b, c);
  }

  /// @brief the inner comparison logic
  /// Comp must return 0 on equality of inner strings, <0 if a < b and >0 if  a > b
  template<class SplitValType, class Comp>
  static int compare(const SplitValType& a, const SplitValType& b, Comp comp) {
    if (auto res = std::strncmp(&a.firstOriginalChar, &b.firstOriginalChar, 1); res != 0) {
      return res; // different data types, decide on the datatype
    }

    if (int res = comp(a.transformedVal, b.transformedVal) ;res != 0) {
      return res; // actual value differs
    }
    return a.langtag.compare(b.langtag); // if everything else matches, we sort by the langtag
  }

  /// transform element s from the vocabulary to the first possible entry that compares greater to s
  /// according to the held locale and level. Needed for Prefix search.
  [[nodiscard]] SplitVal transformToFirstPossibleBiggerValue(std::string_view s, Level level) const {
    auto transformed = extractAndTransformComparable(s, level);
    unsigned char last = transformed.transformedVal.back();
    if (last < std::numeric_limits<unsigned char>::max()) {
      transformed.transformedVal.back() += 1;
    } else {
      transformed.transformedVal.push_back('\0');
    }
    return transformed;
  }
private:
  icu::Locale _icuLocale; /// the held locale
  std::unique_ptr<icu::Collator> _collator[5]; /// one collator for each collation Level to make this class threadsafe
  UColAttributeValue _ignorePunctuationStatus = UCOL_NON_IGNORABLE; /// how to sort punctuations etc.

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
    _collator[static_cast<uint8_t >(Level::PRIMARY)]->setStrength(icu::Collator::PRIMARY);
    _collator[static_cast<uint8_t >(Level::SECONDARY)]->setStrength(icu::Collator::SECONDARY);
    _collator[static_cast<uint8_t >(Level::TERTIARY)]->setStrength(icu::Collator::TERTIARY);
    _collator[static_cast<uint8_t >(Level::QUARTERNARY)]->setStrength(icu::Collator::QUATERNARY);
    _collator[static_cast<uint8_t >(Level::IDENTICAL)]->setStrength(icu::Collator::IDENTICAL);
  }


  void setIgnorePunctuationOnFirstLevels(UColAttributeValue val) {
    _ignorePunctuationStatus = val;
    UErrorCode err = U_ZERO_ERROR;
    for (auto& col : _collator) {
      col->setAttribute(UCOL_ALTERNATE_HANDLING, val, err);
      raise(err);
      // todo<johannes> : make this customizable for future versions
      col->setMaxVariable(UCOL_REORDER_CODE_SYMBOL, err);
      raise(err);
    }

  }

  template<class SplitValType>
  SplitValType extractComparable(std::string_view a, [[maybe_unused]] const Level level) const {
    std::string_view res = a;
    const char first = a.empty() ? char(0) : a[0];
    std::string_view langtag;
    if (ad_utility::startsWith(res, "\"")) {
      // In the case of prefix filters we might also have
      // Literals that do not have the closing quotation mark
      res.remove_prefix(1);
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
      return {first, getSortKey(res, level), std::string(langtag)};
    } else if constexpr (std::is_same_v<SplitValType, SplitValNonOwning>) {
      return {first, res, langtag};
    } else {
      SplitValType().ThisShouldNotCompile();
    }
  }

  static int compToInd(const UCollationResult res) {
    switch (res) {
      case UCOL_LESS :
        return -1;
      case UCOL_EQUAL :
        return 0;
      case UCOL_GREATER :
        return 1;
    }
    throw std::runtime_error("Illegal value for UCollationResult. This should never happen!");


  }

  // _________________________________________________________________
  [[nodiscard]] std::string getSortKey(std::string_view s, const Level level) const {
    auto utf16 = icu::UnicodeString::fromUTF8(toStringPiece(s));
    auto& col = *_collator[static_cast<uint8_t>(level)];
    auto sz = col.getSortKey(utf16, nullptr, 0);
    std::string res;
    res.resize(sz);
    static_assert(sizeof(uint8_t) == sizeof(std::string::value_type));
    sz = col.getSortKey(utf16, reinterpret_cast<uint8_t*>(res.data()), res.size());
    AD_CHECK(sz == static_cast<decltype(sz)>(res.size())); // this is save by the way we obtained sz
    // since this is a c-api we still have a trailing '\0'. Trimming this is necessary for the prefix range to work correct.
    res.resize(res.size()-1);
    return res;
  }

  static icu::StringPiece toStringPiece(std::string_view s) {
    return icu::StringPiece(s.data(), s.size());
  }


};


#endif //QLEVER_STRINGSORTCOMPARATOR_H
