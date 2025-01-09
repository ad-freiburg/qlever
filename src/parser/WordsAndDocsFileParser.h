// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Felix Meisen (fesemeisen@outlook.de)

#pragma once

#include <absl/strings/str_split.h>
#include <unicode/locid.h>

#include <fstream>
#include <string>

#include "global/Id.h"
#include "index/StringSortComparator.h"
#include "util/Iterators.h"

using std::string;
// Represents a line from the wordsfile.tsv, which stores everything given in
// the file line and extra information with isLiteralEntity. Also used to add
// literals to the text index through emulating wordsfile lines.
//
// The Fields are ordered in the same way the values follow in a line.
// Short field overview: string word_, bool isEntity, TextRecordIndex contextId,
//                       Score score_, bool isLiteralEntity (not found in
//                       wordsfile)
//
// Fields:
// - string word_: The string of the word, if it is an entity it will be
//                 <Entity_Name>. bool isEntity_: True if the given word is an
//                 entity, false if it's a word.
// - TextRecordIndex contextId_: When creating the wordsfile docs from the
//                               docsfile get split into so called contexts.
//                               Those contexts overlap, meaning words and
//                               entities are covered multiple times. Each
//                               contextId corresponds to the next bigger or
//                               equal docId.
// - Score score_: Either 1 or 0 if isEntity is false. 0, 1, 100, 150 if
//                 isEntity is true. (this info is only constructed on the
//                 scientists.wordsfile.tsv) The score in the wordsfile is only
//                 relevant for the counting scoring metric. Because of the
//                 overlap of contexts the score is 1 if the word really has
//                 been seen for the first time and 0 if not. If a doc contains
//                 multiple mentions of a word there should be exactly as many
//                 wordsfile lines of that word with score 1 as there are
//                 mentions. The score for entities seems rather random and
//                 since no clear explanation of the creation of wordsfiles
//                 has been found yet they will stay rather random.
// - bool isLiteralEntity_: This does not directly stem from the wordsfile.
//                          When building the text index with literals, for
//                          every literal there will be WordsFileLines for all
//                          words in that literal. Additionally the whole
//                          literal itself will be added as word with isEntity
//                          being true. The need to count this comes only from
//                          a trick used in testing right now.  To be specific
//                          the method getTextRecordFromResultTable
struct WordsFileLine {
  string word_;
  bool isEntity_;
  TextRecordIndex contextId_;
  Score score_;
  bool isLiteralEntity_ = false;
};

// Represents a line from the docsfile.tsv, which stores everything given in
// the file line.
//
// The Fields are ordered in the same way the values follow in a line.
// Short field overview: DocumentIndex docId_, string docContent_
//
// Fields:
//
// - DocumentIndex docId_: The docId is needed to built inverted indices for
//                         Scoring and building of the docsDB. It is also used
//                         to return actual texts when searching for a word.
//                         The word (and entity) search returns a table with
//                         TextRecordIndex as type of one col. Those get mapped
//                         to the next bigger or equal docId which is then
//                         used to extract the text from the docsDB.
//                         TODO: check if this behaviour is consintently
//                         implemented
// - string docContent_: The whole text given after the first tab of a line of
//                       docsfile.
struct DocsFileLine {
  DocumentIndex docId_;
  string docContent_;
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
  ~WordsAndDocsFileParser() = default;
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
