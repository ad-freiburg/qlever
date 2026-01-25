// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_PARSER_WORDSANDDOCSFILEPARSER_H
#define QLEVER_SRC_PARSER_WORDSANDDOCSFILEPARSER_H

#include <absl/strings/str_split.h>
#include <unicode/locid.h>
#include <unicode/normalizer2.h>
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
 * @brief A range that owns a padded string and yields q-grams as string_views.
 *
 * This class stores the normalized and padded string and provides iteration
 * over all q-grams (substrings of length q).
 */
class QgramRange {
 private:
  std::string paddedText_;
  size_t q_;

 public:
  QgramRange(std::string paddedText, size_t q)
      : paddedText_(std::move(paddedText)), q_(q) {}

  class iterator {
   private:
    const std::string* text_;
    size_t pos_;
    size_t q_;

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string_view;
    using difference_type = std::ptrdiff_t;
    using pointer = const std::string_view*;
    using reference = std::string_view;

    iterator(const std::string* text, size_t pos, size_t q)
        : text_(text), pos_(pos), q_(q) {}

    std::string_view operator*() const {
      return std::string_view(*text_).substr(pos_, q_);
    }

    iterator& operator++() {
      ++pos_;
      return *this;
    }

    iterator operator++(int) {
      iterator tmp = *this;
      ++pos_;
      return tmp;
    }

    bool operator==(const iterator& other) const { return pos_ == other.pos_; }
    bool operator!=(const iterator& other) const { return pos_ != other.pos_; }
  };

  iterator begin() const { return iterator(&paddedText_, 0, q_); }

  iterator end() const {
    size_t endPos = paddedText_.size() >= q_ ? paddedText_.size() - q_ + 1 : 0;
    return iterator(&paddedText_, endPos, q_);
  }

  size_t size() const {
    return paddedText_.size() >= q_ ? paddedText_.size() - q_ + 1 : 0;
  }

  bool empty() const { return size() == 0; }
};

/**
 * @brief Normalize text and generate q-grams.
 *
 * Normalization steps:
 * 1. Remove diacritics (e.g., ö → o, é → e) using Unicode NFD decomposition
 * 2. Transform to lowercase
 * 3. Keep only characters in [a-z ] (ASCII letters and space)
 * 4. Pad with (q-1) '$' characters on both sides
 *
 * @param text The text to process.
 * @param q The size of q-grams (must be > 0).
 * @return A QgramRange that can be iterated to yield all q-grams as
 *         string_views.
 *
 * @example For text "Fei-F. Wu" and q=3:
 *   - Normalized: "feif wu"
 *   - Padded: "$$feif wu$$"
 *   - Q-grams: $$f, $fe, fei, eif, if , f w,  wu, wu$, u$$
 */
inline QgramRange qgramizeAndNormalizeText(std::string_view text, size_t q) {
  AD_CONTRACT_CHECK(q > 0, "q must be positive");

  // Get the NFD normalizer (decomposes characters into base + combining marks)
  UErrorCode err = U_ZERO_ERROR;
  const icu::Normalizer2* nfd = icu::Normalizer2::getNFDInstance(err);
  AD_CONTRACT_CHECK(U_SUCCESS(err), u_errorName(err));

  // Convert to ICU UnicodeString and normalize to NFD
  icu::UnicodeString ustr = icu::UnicodeString::fromUTF8(
      icu::StringPiece(text.data(), static_cast<int32_t>(text.size())));
  icu::UnicodeString normalized;
  nfd->normalize(ustr, normalized, err);
  AD_CONTRACT_CHECK(U_SUCCESS(err), u_errorName(err));

  // Build the result: skip combining marks, lowercase, keep only [a-z ]
  std::string padded;
  padded.reserve((q - 1) * 2 + text.size());

  // Add (q-1) padding at the start
  padded.append(q - 1, '$');

  // Process each code point
  for (int32_t i = 0; i < normalized.length();) {
    UChar32 c = normalized.char32At(i);
    i += U16_LENGTH(c);

    // Skip combining marks (diacritics)
    if (u_getCombiningClass(c) != 0) {
      continue;
    }

    // Lowercase
    UChar32 lower = u_tolower(c);

    // Keep only [a-z] and space
    if (lower >= 'a' && lower <= 'z') {
      padded.push_back(static_cast<char>(lower));
    } else if (c == ' ') {
      padded.push_back(' ');
    }
  }

  // Add (q-1) padding at the end
  padded.append(q - 1, '$');

  return QgramRange(std::move(padded), q);
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
