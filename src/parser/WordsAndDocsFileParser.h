// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_PARSER_WORDSANDDOCSFILEPARSER_H
#define QLEVER_SRC_PARSER_WORDSANDDOCSFILEPARSER_H

#include <absl/strings/str_split.h>
#include <unicode/locid.h>
#include <unicode/uchar.h>

#include <fstream>
#include <string>

#include "global/Id.h"
#include "index/StringSortComparator.h"
#include "util/Iterators.h"
#include "util/Views.h"

/**
 * @brief Represents a line in the words file.
 *
 * This struct holds information about a word or entity as it appears in the
 * words file.
 *
 * The Fields are ordered in the same way the values follow in a line.
 * Short field overview: string word_, bool isEntity, TextRecordIndex contextId,
 *                       Score score_, bool isLiteralEntity (not found in
 *                       wordsfile)
 *
 * @details
 *
 * Fields:
 * - string word_: The string of the word, if it is an entity it will be
 *                 <Entity_Name>.
 * - bool isEntity_: True if the given word is an entity, false if it's a word.
 * - TextRecordIndex contextId_: When creating the wordsfile docs from the
 *                               docsfile get split into so called contexts.
 *                               Those contexts overlap, meaning words and
 *                               entities are covered multiple times. Each
 *                               contextId corresponds to the next bigger or
 *                               equal docId.
 * - Score score_: Either 1 or 0 if isEntity is false. 0, 1, 100, 150 if
 *                 isEntity is true. (this info is only constructed on the
 *                 scientists.wordsfile.tsv) The score in the wordsfile is only
 *                 relevant for the counting scoring metric. Because of the
 *                 overlap of contexts the score is 1 if the word really has
 *                 been seen for the first time and 0 if not. If a doc contains
 *                 multiple mentions of a word there should be exactly as many
 *                 wordsfile lines of that word with score 1 as there are
 *                 mentions. The score for entities seems rather random and
 *                 since no clear explanation of the creation of wordsfiles
 *                 has been found yet they will stay rather random.
 * - bool isLiteralEntity_: This does not directly stem from the wordsfile.
 *                          When building the text index with literals, for
 *                          every literal there will be WordsFileLines for all
 *                          words in that literal. Additionally the whole
 *                          literal itself will be added as word with isEntity
 *                          being true. The need to count this comes only from
 *                          a trick used in testing right now.  To be specific
 *                          the method getTextRecordFromResultTable
 */
struct WordsFileLine {
  std::string word_;
  bool isEntity_;
  TextRecordIndex contextId_;
  Score score_;
  bool isLiteralEntity_ = false;
};

/**
 * @brief Represents a line from the docsfile.tsv.
 *
 * This struct stores everything given in a line of the docsfile.tsv.
 *
 * The Fields are ordered in the same way the values follow in a line.
 * Short field overview: DocumentIndex docId_, string docContent_
 *
 * @details
 *
 * Fields:
 * - DocumentIndex docId_: The docId is needed to build inverted indices for
 *                         scoring and building of the docsDB. It is also used
 *                         to return actual texts when searching for a word.
 *                         The word (and entity) search returns a table with
 *                         TextRecordIndex as type of one column. Those get
 *                         mapped to the next bigger or equal docId which is
 *                         then used to extract the text from the docsDB.
 * - string docContent_: The whole text given after the first tab of a line of
 *                       docsfile.
 */
struct DocsFileLine {
  DocumentIndex docId_;
  std::string docContent_;
};

// Custom delimiter class for tokenization of literals using `absl::StrSplit`.
// The `Find` function returns the next delimiter in `text` after the given
// `pos` or an empty substring if there is no next delimiter.
// This version properly handles Unicode characters using ICU.
struct LiteralsTokenizationDelimiter {
  absl::string_view Find(absl::string_view text, size_t unsignedPos) const {
    auto pos = static_cast<int64_t>(unsignedPos);
    auto size = static_cast<int64_t>(text.size());
    // Note: If the Unicode handling ever becomes a bottleneck for ASCII only
    // words, we can integrate a fast path here that handles the ascii
    // characters. But before tackling such microoptimizations, the text index
    // builder should first be parallelized.
    while (pos < size) {
      size_t oldPos = pos;
      UChar32 codePoint;
      U8_NEXT(reinterpret_cast<const uint8_t*>(text.data()), pos, size,
              codePoint);
      AD_CONTRACT_CHECK(codePoint != U_SENTINEL, "Invalid UTF-8 in input");
      if (!u_isalnum(codePoint)) {
        return text.substr(oldPos, pos - oldPos);
      }
    }
    return {text.end(), 0};
  }
};

/**
 * @brief A function that can be used to tokenize and normalize a given text.
 * @warning Both params are const refs where the original objects have to be
 * kept alive during the usage of the returned object.
 * @param text The text to be tokenized and normalized.
 * @param localeManager The localeManager to be used for normalization.
 * @details This function can be used in the following way:
 * for (auto normalizedWord : tokenizeAndNormalizeText(text, localeManager)) {
 *  code;
 * }
 */
inline auto tokenizeAndNormalizeText(std::string_view text,
                                     const LocaleManager& localeManager) {
  std::vector<std::string_view> split{
      absl::StrSplit(text, LiteralsTokenizationDelimiter{}, absl::SkipEmpty{})};
  return ql::views::transform(ad_utility::OwningView{std::move(split)},
                              [&localeManager](const auto& str) {
                                return localeManager.getLowercaseUtf8(str);
                              });
}
/**
 * @brief This class is the parent class of WordsFileParser and DocsFileParser
 *
 * @details It exists to reduce code duplication since the only difference
 * between the child classes is the line type returned.
 */
class WordsAndDocsFileParser {
 public:
  explicit WordsAndDocsFileParser(const std::string& wordsOrDocsFile,
                                  const LocaleManager& localeManager);
  explicit WordsAndDocsFileParser(const WordsAndDocsFileParser& other) = delete;
  WordsAndDocsFileParser& operator=(const WordsAndDocsFileParser& other) =
      delete;

 protected:
  std::ifstream& getInputStream() { return in_; }
  const LocaleManager& getLocaleManager() const { return localeManager_; }

 private:
  std::ifstream in_;
  LocaleManager localeManager_;
};

/**
 * @brief This class takes in the a pathToWordsFile and a localeManager. It then
 * can be used to iterate the wordsFile while already normalizing the words
 * using the localeManager. (If words are entities it doesn't normalize them)
 *
 * @details An object of this class can be iterated as follows:
 * for (auto wordsFileLine : WordsFileParser{wordsFile, localeManager}) {
 *  code;
 * }
 * The type of the value returned when iterating is WordsFileLine
 */
class WordsFileParser : public WordsAndDocsFileParser,
                        public ad_utility::InputRangeFromGet<WordsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;

#ifndef NDEBUG
 private:
  // Only used for sanity checks in debug builds
  TextRecordIndex lastCId_ = TextRecordIndex::make(0);
#endif
};

/**
 * @brief This class takes in the a pathToDocsFile and a localeManager. It then
 * can be used to iterate over the docsFile to get the lines.
 *
 * @details An object of this class can be iterated as follows:
 * for (auto docsFileLine : DocsFileParser{docsFile, localeManager}) {
 *  code;
 * }
 * The type of the value returned when iterating is DocsFileLine
 */
class DocsFileParser : public WordsAndDocsFileParser,
                       public ad_utility::InputRangeFromGet<DocsFileLine> {
 public:
  using WordsAndDocsFileParser::WordsAndDocsFileParser;
  Storage get() override;
};

#endif  // QLEVER_SRC_PARSER_WORDSANDDOCSFILEPARSER_H
