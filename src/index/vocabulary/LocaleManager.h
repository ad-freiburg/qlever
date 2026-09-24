// Copyright 2026 The QLever Authors, in particular:
//
// 2019 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_LOCALEMANAGER_H
#define QLEVER_SRC_INDEX_VOCABULARY_LOCALEMANAGER_H

#ifndef QLEVER_NO_UNICODE
#include <unicode/bytestream.h>
#include <unicode/coleitr.h>
#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/stringpiece.h>
#include <unicode/tblcoll.h>
#include <unicode/unistr.h>
#include <unicode/unorm2.h>
#include <unicode/utypes.h>
#endif  // QLEVER_NO_UNICODE

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "global/Constants.h"
#include "util/Exception.h"
#include "util/StringUtils.h"
#include "util/TransparentFunctors.h"

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
};

#ifndef QLEVER_NO_UNICODE

// This class wraps all calls to the ICU library that are required by QLever
// when comparing strings according to a Locale.
class LocaleManagerICU : public LocaleManagerBase {
 private:
  icu::Locale icuLocale_;  // the held locale
  /* One collator for each collation Level to make this class threadsafe.
   * Needed because setting the collation level and comparing strings are 2
   * different steps in icu. */
  std::array<std::unique_ptr<icu::Collator>, 6> collators_;
  UColAttributeValue ignorePunctuationStatus_ =
      UCOL_NON_IGNORABLE;  // how to sort punctuation etc.

  // Actually locale-independent, but useful to place here since it wraps ICU.
  // Initialized by the `setupCollators()` method.
  const icu::Normalizer2* normalizer_ = nullptr;

 public:
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
                   bool ignorePunctuationAtFirstLevel)
      : icuLocale_(lang.c_str(), country.c_str()),
        ignorePunctuationStatus_(
            ignorePunctuationAtFirstLevel ? UCOL_SHIFTED : UCOL_NON_IGNORABLE) {
    if (icuLocale_.isBogus()) {
      throw std::runtime_error("Could not create locale with language " + lang +
                               " and Country " + country);
    }
    setupCollators();
    setIgnorePunctuationOnFirstLevels(ignorePunctuationStatus_);
  }

  // Copy constructor.
  LocaleManagerICU(const LocaleManagerICU& rhs)
      : LocaleManagerBase(),
        icuLocale_(rhs.icuLocale_),
        ignorePunctuationStatus_(rhs.ignorePunctuationStatus_) {
    setupCollators();
    setIgnorePunctuationOnFirstLevels(ignorePunctuationStatus_);
  }

  // Assign from another LocaleManagerICU.
  LocaleManagerICU& operator=(const LocaleManagerICU& other) {
    if (this == &other) return *this;
    icuLocale_ = other.icuLocale_;
    ignorePunctuationStatus_ = other.ignorePunctuationStatus_;
    setupCollators();
    setIgnorePunctuationOnFirstLevels(ignorePunctuationStatus_);
    return *this;
  }

  // Move operations and destructor. Moving simply transfers the owned
  // collators (unique pointers), the movable `icu::Locale`, and the non-owning
  // `normalizer_`. All of these move without throwing, so a defaulted move is
  // genuinely `noexcept`. The moved-from object is left with null collators,
  // which is a valid but unspecified state that must not be used afterwards.
  LocaleManagerICU(LocaleManagerICU&&) noexcept = default;
  LocaleManagerICU& operator=(LocaleManagerICU&&) noexcept = default;
  ~LocaleManagerICU() = default;

  // Compare two UTF-8 encoded string_views according to the held Locale.
  // Compare according to the collation Level `level`. Return <0 iff a<b, >0 iff
  // a>b, 0 iff a==b.
  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level level) const {
    UErrorCode err = U_ZERO_ERROR;
    auto idx = static_cast<uint8_t>(level);
    auto res = compToInd(
        collators_[idx]->compareUTF8(toStringPiece(a), toStringPiece(b), err));
    raise(err);
    return res;
  }

  // Convert a UTF-8 String to lowercase according to the held locale. `s` is a
  // UTF-8 encoded string; return the lowercase version of s, also encoded as
  // UTF-8.
  [[nodiscard]] std::string getLowercaseUtf8(std::string_view s) const {
    return ad_utility::utf8ToLower(s, icuLocale_.getName());
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
    normalizer_->normalizeUTF8(0, toStringPiece(input), sink, nullptr, err);
    raise(err);
    return res;
  }

  // Return true iff `text` starts with `prefix` on the `PRIMARY` level, i.e.
  // iff the primary weights of the collation elements of `prefix` are a prefix
  // of those of `text`. Elements that are ignored on the `PRIMARY` level don't
  // matter. This also works for characters that expand to several elements,
  // e.g. "groß" (g, r, o, s, s) starts with "gros".
  [[nodiscard]] bool startsWithOnPrimaryLevel(std::string_view text,
                                              std::string_view prefix) const {
    PrimaryWeightIterator textIt{*this, text};
    PrimaryWeightIterator prefixIt{*this, prefix};
    while (auto prefixWeight = prefixIt.next()) {
      if (textIt.next() != prefixWeight) {
        return false;
      }
    }
    return true;
  }

  // Return true iff the first `numElements` collation elements of `a` and `b`
  // that are relevant on the `PRIMARY` level have the same primary weights. If
  // one of the strings has fewer elements, both have to have the same elements.
  [[nodiscard]] bool haveEqualPrimaryPrefix(std::string_view a,
                                            std::string_view b,
                                            size_t numElements) const {
    PrimaryWeightIterator itA{*this, a};
    PrimaryWeightIterator itB{*this, b};
    for (size_t i = 0; i < numElements; ++i) {
      auto weightA = itA.next();
      if (weightA != itB.next()) {
        return false;
      }
      if (!weightA.has_value()) {
        break;
      }
    }
    return true;
  }

 private:
  // raise an exception if the error code holds an error.
  static void raise(const UErrorCode& err) {
    if (U_FAILURE(err)) {
      throw std::runtime_error(u_errorName(err));
    }
  }

  /* create one collator for each of the possible collation levels.
   * has to be called each time the locale is changed. */
  void setupCollators() {
    for (auto& col : collators_) {
      UErrorCode err = U_ZERO_ERROR;
      col.reset(icu::Collator::createInstance(icuLocale_, err));
      raise(err);
    }
    collators_[static_cast<uint8_t>(Level::PRIMARY)]->setStrength(
        icu::Collator::PRIMARY);
    collators_[static_cast<uint8_t>(Level::SECONDARY)]->setStrength(
        icu::Collator::SECONDARY);
    collators_[static_cast<uint8_t>(Level::TERTIARY)]->setStrength(
        icu::Collator::TERTIARY);
    collators_[static_cast<uint8_t>(Level::QUARTERNARY)]->setStrength(
        icu::Collator::QUATERNARY);
    collators_[static_cast<uint8_t>(Level::IDENTICAL)]->setStrength(
        icu::Collator::IDENTICAL);
    // as far as the locale is concerned, the total and the identical level are
    // equivalent.
    collators_[static_cast<uint8_t>(Level::TOTAL)]->setStrength(
        icu::Collator::IDENTICAL);

    // also setup the normalizer
    UErrorCode err = U_ZERO_ERROR;
    normalizer_ =
        icu::Normalizer2::getInstance(nullptr, "nfc", UNORM2_COMPOSE, err);
    raise(err);
  }

  // ______________________________________________________________________________
  void setIgnorePunctuationOnFirstLevels(UColAttributeValue val) {
    ignorePunctuationStatus_ = val;
    UErrorCode err = U_ZERO_ERROR;
    for (auto& col :
         collators_ | ql::views::transform(ad_utility::dereference)) {
      col.setAttribute(UCOL_ALTERNATE_HANDLING, val, err);
      raise(err);
      // todo<joka921> : make this customizable for future versions
      col.setMaxVariable(UCOL_REORDER_CODE_SYMBOL, err);
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
    return icu::StringPiece(s.data(), static_cast<int32_t>(s.size()));
  }

  // Yield the primary weights of the collation elements of a UTF-8 string that
  // are relevant on the `PRIMARY` level: elements with a zero primary weight
  // are skipped, and so are variable elements (punctuation, spaces, symbols)
  // if punctuation is ignored.
  class PrimaryWeightIterator {
    std::unique_ptr<icu::CollationElementIterator> iter_;
    // With `UCOL_SHIFTED`, all primary weights up to and including this value
    // are ignored on the `PRIMARY` level.
    std::optional<uint32_t> variableTop_;
    // An element that was read ahead, but not yet processed.
    std::optional<int32_t> lookahead_;

    // Return the next element or `NULLORDER` at the end.
    int32_t nextElement() {
      if (lookahead_.has_value()) {
        return std::exchange(lookahead_, std::nullopt).value();
      }
      UErrorCode err = U_ZERO_ERROR;
      int32_t elem = iter_->next(err);
      raise(err);
      return elem;
    }

   public:
    PrimaryWeightIterator(const LocaleManagerICU& locManager,
                          std::string_view text) {
      auto& collator = dynamic_cast<const icu::RuleBasedCollator&>(
          *locManager.collators_[static_cast<uint8_t>(Level::PRIMARY)]);
      iter_.reset(collator.createCollationElementIterator(
          icu::UnicodeString::fromUTF8(toStringPiece(text))));
      if (locManager.ignorePunctuationStatus_ == UCOL_SHIFTED) {
        UErrorCode err = U_ZERO_ERROR;
        variableTop_ = collator.getVariableTop(err);
        raise(err);
      }
    }

    // Return the next relevant primary weight or `std::nullopt` at the end.
    std::optional<uint32_t> next() {
      using CEI = icu::CollationElementIterator;
      while (true) {
        int32_t elem = nextElement();
        if (elem == CEI::NULLORDER) {
          return std::nullopt;
        }
        // The `CollationElementIterator` splits 32-bit primary weights into a
        // first element holding the upper 16 bits and a continuation element
        // holding the lower 16 bits. Continuations have the bits `0xC0` set in
        // their lowest byte, which never happens for a first element.
        uint32_t weight = static_cast<uint32_t>(CEI::primaryOrder(elem)) << 16;
        int32_t following = nextElement();
        if (following != CEI::NULLORDER && (following & 0xC0) == 0xC0) {
          weight |= static_cast<uint32_t>(CEI::primaryOrder(following));
        } else {
          lookahead_ = following;
        }
        if (weight == 0 ||
            (variableTop_.has_value() && weight <= *variableTop_)) {
          continue;
        }
        return weight;
      }
    }
  };
};

#endif  // QLEVER_NO_UNICODE

// A `LocaleManager` that completely ignores the locale, and only compares
// strings byte-wise (no UTF handling). Can be used if ICU is not available and
// correct unicode handling is not important.
class LocaleManagerNoICU : public LocaleManagerBase {
 public:
  LocaleManagerNoICU() = default;

  LocaleManagerNoICU(const std::string& /*lang*/,
                     const std::string& /*country*/,
                     bool /*ignorePunctuationAtFirstLevel*/) {}

  [[nodiscard]] int compare(std::string_view a, std::string_view b,
                            const Level /*level*/) const {
    return std::clamp(a.compare(b), -1, 1);
  }

  // Every byte is its own collation element.
  [[nodiscard]] bool startsWithOnPrimaryLevel(std::string_view text,
                                              std::string_view prefix) const {
    return ql::starts_with(text, prefix);
  }

  [[nodiscard]] bool haveEqualPrimaryPrefix(std::string_view a,
                                            std::string_view b,
                                            size_t numElements) const {
    return a.substr(0, numElements) == b.substr(0, numElements);
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

// Select the ICU or the NoICU locale manager depending on whether the
// `QLEVER_NO_UNICODE` macro is defined. The macro is defined by the CMake
// option of the same name. Because the two variants produce different sort
// orders, an index records which variant it was built with
// (`has-icu-support` in the index metadata), and loading an index with a
// mismatching binary fails (see `IndexImpl::applyConfiguration`).
#ifdef QLEVER_NO_UNICODE
using LocaleManager = LocaleManagerNoICU;
#else
using LocaleManager = LocaleManagerICU;
#endif

#endif  // QLEVER_SRC_INDEX_VOCABULARY_LOCALEMANAGER_H
