// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_split.h>
#include <unicode/locid.h>

#include <fstream>
#include <string>

#include "global/Id.h"
#include "index/StringSortComparator.h"
#include "util/Iterators.h"

using std::string;

// string word_, bool isEntity_, TextRecordIndex contextId_, Score score_,
// bool isLiteralEntity_
struct WordsFileLine {
  string word_;
  bool isEntity_;
  TextRecordIndex contextId_;
  Score score_;
  bool isLiteralEntity_ = false;
};

// string docContent_, DocumentIndex docId_
struct DocsFileLine {
  string docContent_;
  DocumentIndex docId_;
};

// Custom delimiter class for tokenization of literals using `absl::StrSplit`.
// The `Find` function returns the next delimiter in `text` after the given
// `pos` or an empty substring if there is no next delimiter.
struct LiteralsTokenizationDelimiter {
  absl::string_view Find(absl::string_view text, size_t pos) {
    auto isWordChar = [](char c) -> bool { return std::isalnum(c); };
    auto found = std::find_if_not(text.begin() + pos, text.end(), isWordChar);
    if (found == text.end()) return text.substr(text.size());
    return {found, found + 1};
  }
};

// This class constructs an object that can be iterated to get the normalized
// words of the text given. The text gets split into tokens using the
// LiteralsTokenizationDelimiter and those tokens get normalized using
// the localeManager. You can use the constructed object like
// obj = TokenizeAndNormalizeText{text, localeManager}
// for (auto normalizedWord : obj) { code }
// The type of the value returned when iterating is std::string
class TokenizeAndNormalizeText
    : public ad_utility::InputRangeMixin<TokenizeAndNormalizeText> {
 public:
  using StorageType = std::string;
  explicit TokenizeAndNormalizeText(std::string_view text,
                                    LocaleManager localeManager)
      : splitter_{absl::StrSplit(text, LiteralsTokenizationDelimiter{},
                                 absl::SkipEmpty{})},
        current_{splitter_.begin()},
        end_{splitter_.end()},
        localeManager_(std::move(localeManager)){};

  // Delete unsafe constructors
  TokenizeAndNormalizeText() = delete;
  TokenizeAndNormalizeText(const TokenizeAndNormalizeText&) = delete;
  TokenizeAndNormalizeText& operator=(const TokenizeAndNormalizeText&) = delete;

 private:
  using Splitter = decltype(absl::StrSplit(
      std::string_view{}, LiteralsTokenizationDelimiter{}, absl::SkipEmpty{}));
  Splitter splitter_;
  Splitter::const_iterator current_;
  Splitter::const_iterator end_;

  std::optional<StorageType> currentValue_;

  LocaleManager localeManager_;

  std::string normalizeToken(std::string_view token) {
    return localeManager_.getLowercaseUtf8(token);
  }

 public:
  void start();
  bool isFinished() const { return !currentValue_.has_value(); };
  const StorageType& get() const { return *currentValue_; };
  void next();
};

// This class is the parent class of WordsFileParser and DocsFileParser and
// it exists to reduce code duplication since the only difference between the
// child classes is the line type returned
class WordsAndDocsFileParser {
 public:
  explicit WordsAndDocsFileParser(const string& wordsOrDocsFile,
                                  LocaleManager localeManager);
  ~WordsAndDocsFileParser();
  explicit WordsAndDocsFileParser(const WordsAndDocsFileParser& other) = delete;
  WordsAndDocsFileParser& operator=(const WordsAndDocsFileParser& other) =
      delete;

 protected:
  std::ifstream in_;
  LocaleManager localeManager_;
};

// This class takes in the a pathToWordsFile and a localeManager. It then can
// be used to iterate the wordsFile while already normalizing the words using
// the localeManager. (If words are entities it doesn't normalize them)
// An object of this class can be iterated as follows:
// obj = WordsFileParser{wordsFile, localeManager}
// for (auto wordsFileLine : obj) { code }
// The type of the value returned when iterating is WordsFileLine
class WordsFileParser : public WordsAndDocsFileParser,
                        public ad_utility::InputRangeFromGet<WordsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;

 private:
#ifndef NDEBUG
  // Only used for sanity checks in debug builds
  TextRecordIndex lastCId_ = TextRecordIndex::make(0);
#endif
};
// Works similar to WordsFileParser but it instead parses a docsFile and
// doesn't normalize the text found in docsFile. To parse the returned
// docContent_ of a DocsFileLine please refer to the TokenizeAndNormalizeText
// class
class DocsFileParser : public WordsAndDocsFileParser,
                       public ad_utility::InputRangeFromGet<DocsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;
};
