//  Copyright 2019, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_INDEX_VOCABULARY_STRINGSORTCOMPARATOR_H
#define QLEVER_SRC_INDEX_VOCABULARY_STRINGSORTCOMPARATOR_H

#include <cstring>
#include <string>
#include <string_view>

#include "index/vocabulary/LocaleManager.h"
#include "util/StringUtils.h"

/**
 * @brief This class compares strings, e.g. strings from the text index
 * vocabulary, according to the collation of the held `LocaleManagerT`. To
 * compare components of RDFS triples use the `TripleComponentComparatorImpl`
 * defined below.
 *
 * The actual comparison logic is handled via the `LocaleManagerT` (see
 * `index/vocabulary/LocaleManager.h`), which performs proper Unicode collation
 * when built with ICU, or a plain bytewise comparison otherwise.
 */
template <typename LocaleManagerT>
class SimpleStringComparatorImpl {
 public:
  using Level = LocaleManagerBase::Level;

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
  SimpleStringComparatorImpl(const std::string& lang,
                             const std::string& country,
                             bool ignorePunctuationAtFirstLevel)
      : locManager_(lang, country, ignorePunctuationAtFirstLevel) {}

  /// Construct according to the default locale specified in
  /// ../global/Constants.h
  SimpleStringComparatorImpl() = default;

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
    auto cmpRes = locManager_.compare(a, b, level);
    if (cmpRes != 0 || level != Level::TOTAL) {
      return cmpRes;
    }
    return a.compare(b);
  }

  /// Obtain access to the held `LocaleManagerT`
  [[nodiscard]] const LocaleManagerT& getLocaleManager() const {
    return locManager_;
  }

 private:
  LocaleManagerT locManager_;
};

/**
 * @brief Handles the comparisons between RDFS triple elements according to
 * their data types and the collation of the held `LocaleManagerT`.
 *
 *  General Approach: First Sort by the datatype, then by the actual value
 * and then by the language tag.
 *
 * The actual comparison logic is handled via the `LocaleManagerT` (see
 * `index/vocabulary/LocaleManager.h`), which performs proper Unicode collation
 * when built with ICU, or a plain bytewise comparison otherwise.
 */
template <typename LocaleManagerT>
class TripleComponentComparatorImpl {
 public:
  using Level = LocaleManagerBase::Level;

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
  TripleComponentComparatorImpl(const std::string& lang,
                                const std::string& country,
                                bool ignorePunctuationAtFirstLevel)
      : locManager_(lang, country, ignorePunctuationAtFirstLevel) {}

  /// Construct according to the default locale in "../global/Constants.h"
  TripleComponentComparatorImpl() = default;

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

  /// Compare two string_views from the Vocabulary. Return value according to
  /// std::strcmp
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level = Level::QUARTERNARY) const {
    // We have to have a total ordering of unique elements in the vocabulary,
    // so if they compare equal according to the locale, use strcmp
    return compare(extractComparable(a), extractComparable(b), level);
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

  /**
   * @brief the inner comparison logic
   *
   * First compares the datatypes by the firstOriginalChar_, then the inner
   * value and then the language tags
   * @return <0 iff a<b, 0 iff a==b, >0 iff a>b
   */
  [[nodiscard]] int compare(const SplitVal& a, const SplitVal& b,
                            const Level level) const {
    if (auto res =
            std::strncmp(&a.firstOriginalChar_, &b.firstOriginalChar_, 1);
        res != 0) {
      return res;  // different data types, decide on the datatype
    }

    if (int res = locManager_.compare(a.innerValue_, b.innerValue_, level);
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

  /// obtain const access to the held `LocaleManagerT`
  [[nodiscard]] const LocaleManagerT& getLocaleManager() const {
    return locManager_;
  }

  /**
   * @brief trivialy wraps `LocaleManagerT::normalizeUtf8`, see there for
   * documentation
   */
  [[nodiscard]] std::string normalizeUtf8(std::string_view sv) const {
    return locManager_.normalizeUtf8(sv);
  }

 private:
  LocaleManagerT locManager_;

  /// Split a string into its components (datatype-indicator first char, inner
  /// value, language tag) to prepare locale-aware collation.
  [[nodiscard]] static SplitVal extractComparable(std::string_view a) {
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
    return {first, res, langtag, a};
  }
};

// The ICU-free comparators are the generic comparator templates above
// instantiated with the `LocaleManagerNoICU`. They are used by the tests (and,
// in an ICU-free build, also via `LocaleManager` below).
using SimpleStringComparatorNoICU =
    SimpleStringComparatorImpl<LocaleManagerNoICU>;
using TripleComponentComparatorNoICU =
    TripleComponentComparatorImpl<LocaleManagerNoICU>;

// The comparators used throughout QLever. `LocaleManager` already resolves to
// the right locale manager for the build configuration (see
// `index/vocabulary/LocaleManager.h`).
using SimpleStringComparator = SimpleStringComparatorImpl<LocaleManager>;
using TripleComponentComparator = TripleComponentComparatorImpl<LocaleManager>;

#endif  // QLEVER_SRC_INDEX_VOCABULARY_STRINGSORTCOMPARATOR_H
