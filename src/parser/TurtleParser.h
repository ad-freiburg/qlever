// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#pragma once

#include <codecvt>
#include <exception>
#include <future>
#include <locale>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "global/Constants.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"
#include "parser/ParallelBuffer.h"
#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"
#include "parser/TripleComponent.h"
#include "parser/data/BlankNode.h"
#include "sys/mman.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/ParseException.h"
#include "util/TaskQueue.h"
#include "util/ThreadSafeQueue.h"

using std::string;

enum class TurtleParserIntegerOverflowBehavior {
  Error,
  OverflowingToDouble,
  AllToDouble
};

struct TurtleTriple {
  std::string subject_;
  std::string predicate_;
  TripleComponent object_;

  bool operator==(const TurtleTriple&) const = default;
};

inline std::string_view stripAngleBrackets(std::string_view input) {
  AD_CONTRACT_CHECK(input.starts_with('<') && input.ends_with('>'));
  input.remove_prefix(1);
  input.remove_suffix(1);
  return input;
}

inline std::string_view stripDoubleQuotes(std::string_view input) {
  AD_CONTRACT_CHECK(input.starts_with('"') && input.ends_with('"') &&
                    input.size() >= 2);
  input.remove_prefix(1);
  input.remove_suffix(1);
  return input;
}

// A base class for all the different turtle parsers.
class TurtleParserBase {
 private:
  // How to handle integer overflow and invalid literals (see below).
  TurtleParserIntegerOverflowBehavior integerOverflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::Error;
  bool invalidLiteralsAreSkipped_ = false;

 public:
  virtual ~TurtleParserBase() = default;
  // Wrapper to getLine that is expected by the rest of QLever
  bool getLine(TurtleTriple& triple) { return getLine(&triple); }

  virtual TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() final {
    return integerOverflowBehavior_;
  }

  // If true then triples with invalid literals (for example
  // "noNumber"^^xsd:integer) are ignored. If false an exception is thrown when
  // such literals are encountered.
  virtual bool& invalidLiteralsAreSkipped() final {
    return invalidLiteralsAreSkipped_;
  }

  virtual void printAndResetQueueStatistics() {
    // This function only does something for the parallel parser (where it is
    // overridden).
  }

  // Main access method to the parser
  // If a triple can be parsed (or has previously been parsed and stored
  // Writes the triple to the argument (format subject, object predicate)
  // returns true iff a triple can be successfully written, else the triple
  // value is invalid and the parser is at the end of the input.
  virtual bool getLine(TurtleTriple* triple) = 0;

  // Get the offset (relative to the beginning of the file) of the first byte
  // that has not yet been dealt with by the parser.
  [[nodiscard]] virtual size_t getParsePosition() const = 0;

  // Return a batch of the next 100'000 triples at once. If the parser is
  // exhausted, return `nullopt`.
  virtual std::optional<std::vector<TurtleTriple>> getBatch() {
    std::vector<TurtleTriple> result;
    result.reserve(100'000);
    for (size_t i = 0; i < 100'000; ++i) {
      result.emplace_back();
      bool success = getLine(result.back());
      if (!success) {
        result.resize(result.size() - 1);
        break;
      }
    }
    if (result.empty()) {
      return std::nullopt;
    }
    return result;
  }
};

/**
 * @brief The actual parser class
 *
 * If TokenizerCtre is used as a Tokenizer, a relaxed parsing mode is applied,
 * that does not quite fulfill the SPARQL standard. This means that:
 * - IRIS  of any kind (prefixed or not) must be limited to the ascii range
 * - Prefixed names (like prefix:suffix) may not include escape sequences
 *
 * These relaxations currently allow for fast parsing of Wikidata but might fail
 * on other knowledge bases, so this should be used with caution.
 * @tparam Tokenizer_T
 */
template <class Tokenizer_T>
class TurtleParser : public TurtleParserBase {
 public:
  using ParseException = ::ParseException;

  // The CTRE Tokenizer implies relaxed parsing.
  static constexpr bool UseRelaxedParsing =
      std::is_same_v<Tokenizer_T, TokenizerCtre>;

  // Get the result of the single rule that was parsed most recently. Used for
  // testing.
  const TripleComponent& getLastParseResult() const { return lastParseResult_; }

  // Get the currently buffered triples. Used for testing.
  const std::vector<TurtleTriple>& getTriples() const { return triples_; }

 protected:
  // Data members.

  // Stores the triples that have been parsed but not retrieved yet.
  std::vector<TurtleTriple> triples_;

  // If this is set, there is nothing else to parse and we will only
  // retrieve what is left in our tripleBuffer.
  bool isParserExhausted_ = false;

  // The tokenizer.
  Tokenizer_T tok_{std::string_view("")};

  // The result of the last successful call to a parsing function (a function
  // named after a (non-)terminal of the Turtle grammar). We are using
  // `TripleComponent` since it can hold any parsing result, not only objects.
  TripleComponent lastParseResult_;

  // Maps prefixes to their expanded form, initialized with the empty base
  // (i.e. the prefix ":" maps to the empty IRI).
  ad_utility::HashMap<std::string, std::string> prefixMap_{{"", ""}};

  // There are turtle constructs that reuse prefixes, subjects and predicates
  // so we have to save the last seen ones.
  std::string activePrefix_;
  std::string activeSubject_;
  std::string activePredicate_;
  size_t numBlankNodes_ = 0;

  bool currentTripleIgnoredBecauseOfInvalidLiteral_ = false;

  // Make sure that each blank nodes is unique, even across different parser
  // instances.
  static inline std::atomic<size_t> numParsers_ = 0;
  size_t blankNodePrefix_ = numParsers_.fetch_add(1);

 public:
  TurtleParser() = default;
  TurtleParser(TurtleParser&& rhs) noexcept = default;
  TurtleParser& operator=(TurtleParser&& rhs) noexcept = default;

 protected:
  // clear all the parser's state to the initial values.
  void clear() {
    lastParseResult_ = "";

    activeSubject_.clear();
    activePredicate_.clear();
    activePrefix_.clear();

    prefixMap_.clear();

    tok_.reset(nullptr, 0);
    triples_.clear();
    numBlankNodes_ = 0;
    isParserExhausted_ = false;
  }

  // the following functions refer to the nonterminals of the turtle grammar
  // a return value of true means that the nonterminal could be parsed and that
  // the parser's internal state has been updated accordingly.
  // A return value of false means that the nonterminal could NOT be parsed
  // but that nothing has been changed in the parser's state (the LL1 lookup has
  // failed in this case). In all other cases a ParseException is thrown because
  // the LL1 property was violated.
  void turtleDoc() {
    while (statement()) {
    }
  }

  bool statement();

  // Log error message (with parse position) and throw parse exception.
  [[noreturn]] void raise(std::string_view error_message) {
    auto d = tok_.view();
    std::stringstream errorMessage;
    errorMessage << "Parse error at byte position " << getParsePosition()
                 << ": " << error_message << '\n';
    if (!d.empty()) {
      size_t num_bytes = 500;
      auto s = std::min(size_t(num_bytes), size_t(d.size()));
      errorMessage << "The next " << num_bytes << " bytes are:\n"
                   << std::string_view(d.data(), s) << '\n';
    }
    throw ParseException{std::move(errorMessage).str()};
  }

  // Throw an exception or simply ignore the current triple, depending on the
  // setting of `invalidLiteralsAreSkipped()`.
  void raiseOrIgnoreTriple(std::string_view errorMessage) {
    if (invalidLiteralsAreSkipped()) {
      currentTripleIgnoredBecauseOfInvalidLiteral_ = true;
    } else {
      raise(errorMessage);
    }
  }

 protected:
  /* private Member Functions */

  bool directive();
  bool prefixID();
  bool base();
  bool sparqlPrefix();
  bool sparqlBase();
  bool triples();
  bool subject();
  bool predicateObjectList();
  bool blankNodePropertyList();
  bool objectList();
  bool object();
  bool verb();
  bool predicateSpecialA();
  bool predicate();

  bool iri();
  bool blankNode();
  bool collection();
  bool literal();
  bool rdfLiteral();
  bool numericLiteral();
  bool booleanLiteral();
  bool prefixedName();
  bool stringParse();

  // Terminal symbols from the grammar
  // Behavior of the functions is similar to the nonterminals (see above)
  bool iriref();
  bool integer();
  bool decimal();
  // The grammar rule is called "double" but this is a reserved name in C++.
  bool doubleParse();

  // Two helper functions for the actual conversion from strings to numbers.
  void parseDoubleConstant(const std::string& input);
  void parseIntegerConstant(const std::string& input);

  // This version only works if no escape sequences were used.
  bool pnameLnRelaxed();

  // __________________________________________________________________________
  bool pnameNS();

  // __________________________________________________________________________
  bool langtag() { return parseTerminal<TurtleTokenId::Langtag>(); }
  bool blankNodeLabel();

  bool anon() {
    if (!parseTerminal<TurtleTokenId::Anon>()) {
      return false;
    }
    lastParseResult_ = createAnonNode();
    return true;
  }

  // Skip a given regex without parsing it
  template <TurtleTokenId reg>
  bool skip() {
    tok_.skipWhitespaceAndComments();
    return tok_.template skip<reg>();
  }

  // if the prefix of the current input position matches the regex argument,
  // put the matching prefix into lastParseResult_, move the input position
  // forward by the length of the match and return true else return false and do
  // not change the parser's state
  template <TurtleTokenId terminal, bool SkipWhitespaceBefore = true>
  bool parseTerminal();

  // ______________________________________________________________________________________
  void emitTriple() {
    if (!currentTripleIgnoredBecauseOfInvalidLiteral_) {
      triples_.emplace_back(activeSubject_, activePredicate_, lastParseResult_);
    }
    currentTripleIgnoredBecauseOfInvalidLiteral_ = false;
  }

  // Enforce that the argument is true: if it is false, a parse Exception is
  // thrown this helps formulating the LL1 property in easily readable code
  bool check(bool result) {
    if (result) {
      return true;
    } else {
      raise("A check for a required element failed");
    }
  }

  // map a turtle prefix to its expanded form. Throws if the prefix was not
  // properly registered before
  string expandPrefix(const string& prefix) {
    if (!prefixMap_.count(prefix)) {
      raise("Prefix " + prefix +
            " was not previously defined using a PREFIX or @prefix "
            "declaration");
    } else {
      return prefixMap_[prefix];
    }
  }

  // create a new, unused, unique blank node string
  string createAnonNode() {
    return BlankNode{true,
                     absl::StrCat(blankNodePrefix_, "_", numBlankNodes_++)}
        .toSparql();
  }

 public:
  // To get consistent blank node labels when testing, we need to manually set
  // the prefix. This function is named `...ForTesting` so you really shouldn't
  // use it in the actual QLever code.
  void setBlankNodePrefixOnlyForTesting(size_t id) { blankNodePrefix_ = id; }

 protected:
  FRIEND_TEST(TurtleParserTest, prefixedName);
  FRIEND_TEST(TurtleParserTest, prefixID);
  FRIEND_TEST(TurtleParserTest, stringParse);
  FRIEND_TEST(TurtleParserTest, rdfLiteral);
  FRIEND_TEST(TurtleParserTest, predicateObjectList);
  FRIEND_TEST(TurtleParserTest, objectList);
  FRIEND_TEST(TurtleParserTest, object);
  FRIEND_TEST(TurtleParserTest, blankNode);
  FRIEND_TEST(TurtleParserTest, blankNodePropertyList);
  FRIEND_TEST(TurtleParserTest, numericLiteral);
  FRIEND_TEST(TurtleParserTest, booleanLiteral);
  FRIEND_TEST(TurtleParserTest, booleanLiteralLongForm);
  FRIEND_TEST(TurtleParserTest, collection);
};

/**
 * Parses turtle from std::string. Used to perform unit tests for
 * the different parser rules
 */
template <class Tokenizer_T>
class TurtleStringParser : public TurtleParser<Tokenizer_T> {
 public:
  using TurtleParser<Tokenizer_T>::prefixMap_;
  using TurtleParser<Tokenizer_T>::getLine;
  bool getLine(TurtleTriple* triple) override {
    (void)triple;
    throw std::runtime_error(
        "TurtleStringParser doesn't support calls to getLine. Only use "
        "parseUtf8String() for unit tests\n");
  }

  size_t getParsePosition() const override {
    return positionOffset_ + tmpToParse_.size() - this->tok_.data().size();
  }

  void initialize(const string& filename) {
    (void)filename;
    throw std::runtime_error(
        "TurtleStringParser doesn't support calls to initialize. Only use "
        "parseUtf8String() for unit tests\n");
  }

  // Load a string object directly to the buffer allows easier testing without a
  // file object.
  void parseUtf8String(const std::string& toParse) {
    setInputStream(toParse);
    this->turtleDoc();
  }

  // Parse all Triples (no prefix declarations etc. allowed) and return them.
  auto parseAndReturnAllTriples() {
    // Actually parse
    this->turtleDoc();
    auto d = this->tok_.view();
    if (!d.empty()) {
      this->raise(absl::StrCat(
          "Parsing failed before end of input, remaining bytes: ", d.size()));
    }
    return std::move(this->triples_);
  }

  // Parse only a single object.
  static TripleComponent parseTripleObject(std::string_view objectString) {
    TurtleStringParser parser;
    parser.parseUtf8String(absl::StrCat("<a> <b> ", objectString, "."));
    AD_CONTRACT_CHECK(parser.triples_.size() == 1);
    return std::move(parser.triples_[0].object_);
  }

  string_view getUnparsedRemainder() const { return this->tok_.view(); }

  // Parse directive and return true if a directive was found.
  bool parseDirectiveManually() { return this->directive(); }

  void raiseManually(string_view message) { this->raise(message); }

  void setPositionOffset(size_t offset) { positionOffset_ = offset; }

 private:
  // The complete input to this parser.
  ParallelBuffer::BufferType tmpToParse_;
  // Used to add a certain offset to the parsing position when using this
  // in a parallel setting.
  size_t positionOffset_ = 0;

 public:
  // testing interface for reusing a parser
  // only specifies the tokenizers input stream.
  // Does not alter the tokenizers state
  void setInputStream(const string& toParse) {
    tmpToParse_.clear();
    tmpToParse_.reserve(toParse.size());
    tmpToParse_.insert(tmpToParse_.end(), toParse.begin(), toParse.end());
    this->tok_.reset(tmpToParse_.data(), tmpToParse_.size());
  }

  void setPrefixMap(decltype(prefixMap_) m) { prefixMap_ = std::move(m); }

  const auto& getPrefixMap() const { return prefixMap_; }

  // __________________________________________________________
  void setInputStream(ParallelBuffer::BufferType&& toParse) {
    tmpToParse_ = std::move(toParse);
    this->tok_.reset(tmpToParse_.data(), tmpToParse_.size());
  }

  // testing interface, only works when parsing from an utf8-string
  // return the current position of the tokenizer in the input string
  // can be used to test if the advancing of the tokenizer works
  // as expected
  size_t getPosition() const { return this->tok_.begin() - tmpToParse_.data(); }

  FRIEND_TEST(TurtleParserTest, prefixedName);
  FRIEND_TEST(TurtleParserTest, prefixID);
  FRIEND_TEST(TurtleParserTest, stringParse);
  FRIEND_TEST(TurtleParserTest, rdfLiteral);
  FRIEND_TEST(TurtleParserTest, predicateObjectList);
  FRIEND_TEST(TurtleParserTest, objectList);
  FRIEND_TEST(TurtleParserTest, object);
  FRIEND_TEST(TurtleParserTest, blankNode);
  FRIEND_TEST(TurtleParserTest, blankNodePropertyList);
  FRIEND_TEST(TurtleParserTest, DateLiterals);
};

/**
 * This class is a TurtleParser that always assumes that
 * its input file is an uncompressed .ttl file that will be read in
 * chunks. Input file can also be a stream like stdin.
 */
template <class Tokenizer_T>
class TurtleStreamParser : public TurtleParser<Tokenizer_T> {
  // struct that can store the state of a parser
  // the previously extracted triples are not stored
  // but only the number of triples that were already present
  // before the backup
  struct TurtleParserBackupState {
    size_t numBlankNodes_ = 0;
    size_t numTriples_;
    const char* tokenizerPosition_;
    size_t tokenizerSize_;
  };

 public:
  // Default construction needed for tests
  TurtleStreamParser() = default;
  explicit TurtleStreamParser(const string& filename) {
    LOG(DEBUG) << "Initialize turtle parsing from uncompressed file or stream "
               << filename << std::endl;
    initialize(filename);
  }

  // inherit the wrapper overload
  using TurtleParser<Tokenizer_T>::getLine;

  bool getLine(TurtleTriple* triple) override;

  void initialize(const string& filename);

  size_t getParsePosition() const override {
    return numBytesBeforeCurrentBatch_ + (tok_.data().data() - byteVec_.data());
  }

 private:
  using TurtleParser<Tokenizer_T>::tok_;
  using TurtleParser<Tokenizer_T>::triples_;
  using TurtleParser<Tokenizer_T>::isParserExhausted_;
  // Backup the current state of the turtle parser to a
  // TurtleparserBackupState object
  // This can be used e.g. when parsing from a compressed input
  // and the currently uncompressed buffer is not sufficient to parse the
  // next expression
  TurtleParserBackupState backupState() const;

  // Reset the parser to the state indicated by the argument
  // Must be called on the same parser object that was used to create the backup
  // state (the actual triples are not backed up)
  bool resetStateAndRead(TurtleParserBackupState* state);

  // stores the current batch of bytes we have to parse.
  // Might end in the middle of a statement or even a multibyte utf8 character,
  // that's why we need the backupState() and resetStateAndRead() methods
  ParallelBuffer::BufferType byteVec_;

  std::unique_ptr<ParallelBuffer> fileBuffer_;
  // this many characters will be buffered at once,
  // defaults to a global constant
  size_t bufferSize_ = FILE_BUFFER_SIZE;

  // that many bytes were already parsed before dealing with the current batch
  // in member byteVec_
  size_t numBytesBeforeCurrentBatch_ = 0;
};

/**
 * This class is a TurtleParser that always assumes that
 * its input file is an uncompressed .ttl file that will be read in
 * chunks. Input file can also be a stream like stdin.
 */
template <class Tokenizer_T>
class TurtleParallelParser : public TurtleParser<Tokenizer_T> {
 public:
  using Triple = std::array<string, 3>;
  // Default construction needed for tests
  TurtleParallelParser() = default;

  // If the `sleepTimeForTesting` is set, then after the initialization the
  // parser will sleep for the specified time before parsing each batch s.t.
  // certain corner cases can be tested.
  explicit TurtleParallelParser(const string& filename,
                                std::chrono::milliseconds sleepTimeForTesting =
                                    std::chrono::milliseconds{0})
      : sleepTimeForTesting_(sleepTimeForTesting) {
    LOG(DEBUG)
        << "Initialize parallel Turtle Parsing from uncompressed file or "
           "stream "
        << filename << std::endl;
    initialize(filename);
  }

  // inherit the wrapper overload
  using TurtleParser<Tokenizer_T>::getLine;

  bool getLine(TurtleTriple* triple) override;

  std::optional<std::vector<TurtleTriple>> getBatch() override;

  void printAndResetQueueStatistics() override {
    LOG(TIMING) << parallelParser_.getTimeStatistics() << '\n';
    parallelParser_.resetTimers();
  }

  void initialize(const string& filename);

  size_t getParsePosition() const override {
    // TODO: can we really define this position here?
    return 0;
  }

  // The destructor has to clean up all the parallel structures that might be
  // still running in the background, especially when it is called before the
  // parsing has finished (e.g. in case of an exception in the code that uses
  // the parser).
  ~TurtleParallelParser() override;

 private:
  // The documentation for this is in the `.cpp` file, because it closely
  // interacts with the functions next to it.
  void finishTripleCollectorIfLastBatch();
  // Parse the single `batch` and push the result to the `triplesCollector_`.
  void parseBatch(size_t parsePosition, auto batch);
  // Read all the batches from the file and feed them to the parallel parser
  // threads. The argument is the first batch which might have been leftover
  // from the initialization phase where the prefixes are parsed.
  void feedBatchesToParser(auto remainingBatchFromInitialization);

  using TurtleParser<Tokenizer_T>::tok_;
  using TurtleParser<Tokenizer_T>::triples_;
  using TurtleParser<Tokenizer_T>::isParserExhausted_;

  // this many characters will be buffered at once,
  // defaults to a global constant
  size_t bufferSize_ = FILE_BUFFER_SIZE;

  ParallelBufferWithEndRegex fileBuffer_{bufferSize_, "\\.[\\t ]*([\\r\\n]+)"};

  ad_utility::data_structures::ThreadSafeQueue<std::function<void()>>
      tripleCollector_{QUEUE_SIZE_AFTER_PARALLEL_PARSING};
  ad_utility::TaskQueue<true> parallelParser_{
      QUEUE_SIZE_BEFORE_PARALLEL_PARSING, NUM_PARALLEL_PARSER_THREADS,
      "parallel parser"};
  std::future<void> parseFuture_;
  // The parallel parsers need to know when the last batch has been parsed, s.t.
  // the parser threads can be destroyed. The following two members are needed
  // for keeping track of this condition.
  std::atomic<size_t> batchIdx_ = 0;
  std::atomic<size_t> numBatchesTotal_ = 0;

  std::chrono::milliseconds sleepTimeForTesting_;
};
