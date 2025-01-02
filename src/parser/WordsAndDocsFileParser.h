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

struct WordsFileLine {
  string word_;
  bool isEntity_;
  TextRecordIndex contextId_;
  Score score_;
  bool isLiteralEntity_ = false;
};

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

class TextTokenizerAndNormalizer
    : public ad_utility::InputRangeMixin<TextTokenizerAndNormalizer> {
 public:
  using StorageType = std::string;
  explicit TextTokenizerAndNormalizer(std::string_view text,
                                      LocaleManager localeManager)
      : splitter_{absl::StrSplit(text, LiteralsTokenizationDelimiter{},
                                 absl::SkipEmpty{})},
        current_{splitter_.begin()},
        end_{splitter_.end()},
        localeManager_(localeManager){};

  // Delete unsafe constructors
  TextTokenizerAndNormalizer() = delete;
  TextTokenizerAndNormalizer(const TextTokenizerAndNormalizer&) = delete;
  TextTokenizerAndNormalizer& operator=(const TextTokenizerAndNormalizer&) =
      delete;

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

class DocsFileParser : public WordsAndDocsFileParser,
                       public ad_utility::InputRangeFromGet<DocsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;
};
