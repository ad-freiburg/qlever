// Copyright 2018 - 2026 The QLever Authors, in particular:
//
// 2018 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_RDFPARSER_H
#define QLEVER_SRC_PARSER_RDFPARSER_H

#include <absl/strings/str_cat.h>
#include <gtest/gtest_prod.h>

#include <future>
#include <locale>
#include <stdexcept>
#include <string_view>

#include "backports/three_way_comparison.h"
#include "global/Constants.h"
#include "global/SpecialIds.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/EncodedIriManager.h"
#include "parser/AsyncBlockSource.h"
#include "parser/TripleComponent.h"
#include "parser/TurtleTokenId.h"
#include "parser/data/BlankNode.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Log.h"
#include "util/ParseException.h"
#include "util/ParsedUri.h"

enum class TurtleParserIntegerOverflowBehavior {
  Error,
  OverflowingToDouble,
  AllToDouble
};

struct TurtleTriple {
  // The subject can be IRI or BlankNode, but the IRI can also be directly
  // folded into the ID.
  TripleComponent subject_;
  // The predicate can an IRI which can also be directly folded into the ID.
  TripleComponent predicate_;
  TripleComponent object_;
  TripleComponent graphIri_ = qlever::specialIds().at(DEFAULT_GRAPH_IRI);

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(TurtleTriple, subject_,
                                              predicate_, object_, graphIri_)
};

// A base class for all the different turtle and N-Quad parsers.
//
// The class only carries the configuration that the inner parsers need
// (overflow behavior, literal handling, encoded-IRI manager). The
// asynchronous, batched interface that the index builder uses lives in
// `parser/AsyncSingleFileParser.h` and `parser/AsyncMultifileParser.h`.
class RdfParserBase {
 private:
  // How to handle integer overflow and invalid literals (see below).
  TurtleParserIntegerOverflowBehavior integerOverflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::Error;
  bool invalidLiteralsAreSkipped_ = false;

  const EncodedIriManager* encodedIriManager_;

 public:
  virtual ~RdfParserBase() = default;

  explicit RdfParserBase(const EncodedIriManager* encodedIriManager)
      : encodedIriManager_{encodedIriManager} {}

  virtual TurtleParserIntegerOverflowBehavior& integerOverflowBehavior() {
    return integerOverflowBehavior_;
  }

  // If true then triples with invalid literals (for example
  // "noNumber"^^xsd:integer) are ignored. If false an exception is thrown when
  // such literals are encountered.
  virtual bool& invalidLiteralsAreSkipped() {
    return invalidLiteralsAreSkipped_;
  }

  // Get the offset (relative to the beginning of the file) of the first byte
  // that has not yet been dealt with by the parser. Default 0; only the
  // string parser overrides this so that error messages can include a
  // position offset.
  [[nodiscard]] virtual size_t getParsePosition() const { return 0; }

 protected:
  const auto& encodedIriManager() const { return *encodedIriManager_; }
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
class TurtleParser : public RdfParserBase {
 public:
  using ParseException = ::ParseException;

  // Get the result of the single rule that was parsed most recently. Used for
  // testing.
  const TripleComponent& getLastParseResult() const { return lastParseResult_; }

  // Get the currently buffered triples. Used for testing.
  const std::vector<TurtleTriple>& getTriples() const { return triples_; }

  // Convert the content of a literal and its datatype to TripleComponent. In
  // particular this also handles the cases where the datatype indicates that
  // the value can be encoded directly into the ID (for example a `typeIri` of
  // `xsd:integer` will cause the `normalizedLiteralContent` to be parsed as an
  // integer).
  static TripleComponent literalAndDatatypeToTripleComponent(
      std::string_view normalizedLiteralContent,
      const TripleComponent::Iri& typeIri,
      const EncodedIriManager& encodedIriManager);

 private:
  // Impl of the method above, also used in rdfLiteral parsing.
  TripleComponent literalAndDatatypeToTripleComponentImpl(
      std::string_view normalizedLiteralContent,
      const TripleComponent::Iri& typeIri);

  static constexpr std::array<const char*, 12> integerDatatypes_ = {
      XSD_INT_TYPE,
      XSD_INTEGER_TYPE,
      XSD_NON_POSITIVE_INTEGER_TYPE,
      XSD_NEGATIVE_INTEGER_TYPE,
      XSD_LONG_TYPE,
      XSD_SHORT_TYPE,
      XSD_BYTE_TYPE,
      XSD_NON_NEGATIVE_INTEGER_TYPE,
      XSD_UNSIGNED_LONG_TYPE,
      XSD_UNSIGNED_INT_TYPE,
      XSD_UNSIGNED_SHORT_TYPE,
      XSD_POSITIVE_INTEGER_TYPE};

  static constexpr std::array<const char*, 3> floatDatatypes_ = {
      XSD_DECIMAL_TYPE, XSD_DOUBLE_TYPE, XSD_FLOAT_TYPE};

  // Helper function to raise an exception in response to BASE or PREFIX
  // mismatches during parallel parsing.
  [[noreturn]] void raiseDisallowedPrefixOrBaseError() const;

  // Set the prefix IRI for the given key. If `prefixAndBaseDisabled_`
  // is true, throw an error if this would change the mapping, which is illegal
  // during parallel parsing.
  void setPrefixOrThrow(const std::string& key,
                        const ad_utility::triple_component::Iri& prefix);

  // Set the base IRI for the given key. If `prefixAndBaseDisabled_`
  // is true, throw an error if this would change the mapping, which is illegal
  // during parallel parsing.
  void setBaseIriOrThrow(const ad_utility::triple_component::Iri& iri);

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

  ad_utility::HashMap<std::string, TripleComponent::Iri> prefixMap_;
  std::optional<qlever::util::ParsedUri> baseIri_;

  // There are turtle constructs that reuse prefixes, subjects and predicates
  // so we have to save the last seen ones.
  std::string activePrefix_;
  TripleComponent activeSubject_;
  TripleComponent activePredicate_;
  TripleComponent defaultGraphIri_ = qlever::specialIds().at(DEFAULT_GRAPH_IRI);
  size_t numBlankNodes_ = 0;

  bool currentTripleIgnoredBecauseOfInvalidLiteral_ = false;

  // Make sure that each blank nodes is unique, even across different parser
  // instances.
  static inline std::atomic<size_t> numParsers_ = 0;
  size_t blankNodePrefix_ = numParsers_.fetch_add(1);

  // Prefix for user-specified blank node labels. For parallel parsing of the
  // same file, this should be the same across all sub-parsers, while
  // blankNodePrefix_ should be unique per parser.
  size_t fileBlankNodePrefix_ = blankNodePrefix_;

  // Used to restrict a worker for the parallel turtle parser to a simpler
  // grammar that can be parsed in parallel. This disallows re-definitions of
  // @base and @prefix as well as usage of multiline literals.
  bool useSimplifiedGrammar_ = false;

  // Unified interface to copy/move data that has been collected by the parallel
  // parser before sharding out to parallel worker threads.
  template <typename Source, typename Target>
  static void copyHeaderFrom(Source&& source, Target& target) {
    target.prefixMap_ = AD_FWD(source).prefixMap_;
    target.baseIri_ = AD_FWD(source).baseIri_;
  }

 public:
  explicit TurtleParser(const EncodedIriManager* encodedIriManager)
      : RdfParserBase{encodedIriManager} {}
  explicit TurtleParser(const EncodedIriManager* encodedIriManager,
                        TripleComponent defaultGraphIri)
      : RdfParserBase{encodedIriManager},
        defaultGraphIri_{std::move(defaultGraphIri)} {}
  TurtleParser(TurtleParser&& rhs) noexcept = default;
  TurtleParser& operator=(TurtleParser&& rhs) noexcept = default;

 protected:
  // clear all the parser's state to the initial values.
  void clear();

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

  virtual bool statement();

  // Log error message (with parse position) and throw parse exception.
  [[noreturn]] void raise(std::string_view error_message) const;

  // Throw an exception or simply ignore the current triple, depending on the
  // setting of `invalidLiteralsAreSkipped()`.
  void raiseOrIgnoreTriple(std::string_view errorMessage);

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
  // The `Impl` indirection is for easier testing in `RdfParserTest.cpp`
  bool rdfLiteralImpl(bool allowMultilineStrings);
  bool rdfLiteral() {
    // Turtle allows for multiline strings.
    return rdfLiteralImpl(!useSimplifiedGrammar_);
  }
  bool numericLiteral();
  bool booleanLiteral();
  bool prefixedName();
  // The `Impl` indirection is for easier testing in `RdfParserTest.cpp`
  bool stringParseImpl(bool allowMultilineStrings);
  bool stringParse() { return stringParseImpl(!useSimplifiedGrammar_); }

  // Terminal symbols from the grammar
  // Behavior of the functions is similar to the nonterminals (see above)
  bool iriref();
  bool integer();
  bool decimal();
  // The grammar rule is called "double" but this is a reserved name in C++.
  bool doubleParse();

  // Two helper functions for the actual conversion from strings to numbers.
  void parseDoubleConstant(std::string_view input);
  void parseIntegerConstant(std::string_view input);

  // This version only works if no escape sequences were used.
  bool pnameLnRelaxed();

  // __________________________________________________________________________
  bool pnameNS();

  // __________________________________________________________________________
  bool langtag() { return parseTerminal<TurtleTokenId::Langtag>(); }
  bool blankNodeLabel();

  bool anon();

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
  void emitTriple();

  // Enforce that the argument is true: if it is false, a parse Exception is
  // thrown this helps formulating the LL1 property in easily readable code
  bool check(bool result) const;

  // map a turtle prefix to its expanded form. Throws if the prefix was not
  // properly registered before
  TripleComponent::Iri expandPrefix(const std::string& prefix);

  // create a new, unused, unique blank node string
  std::string createAnonNode();

 public:
  // To get consistent blank node labels when testing, we need to manually set
  // the prefix. This function is named `...ForTesting` so you really shouldn't
  // use it in the actual QLever code.
  void setBlankNodePrefixOnlyForTesting(size_t id) {
    blankNodePrefix_ = id;
    fileBlankNodePrefix_ = id;
  }

  // Set the file-level blank node prefix. This is used by parallel parsers
  // to ensure that user-specified blank node labels have the same ID across
  // all sub-parsers of the same file.
  void setFileBlankNodePrefix(size_t id) { fileBlankNodePrefix_ = id; }

 protected:
  FRIEND_TEST(RdfParserTest, prefixedName);
  FRIEND_TEST(RdfParserTest, prefixID);
  FRIEND_TEST(RdfParserTest, stringParse);
  FRIEND_TEST(RdfParserTest, rdfLiteral);
  FRIEND_TEST(RdfParserTest, predicateObjectList);
  FRIEND_TEST(RdfParserTest, objectList);
  FRIEND_TEST(RdfParserTest, object);
  FRIEND_TEST(RdfParserTest, base);
  FRIEND_TEST(RdfParserTest, sparqlBase);
  FRIEND_TEST(RdfParserTest, blankNode);
  FRIEND_TEST(RdfParserTest, blankNodesUniqueAcrossFiles);
  FRIEND_TEST(RdfParserTest, blankNodePropertyList);
  FRIEND_TEST(RdfParserTest, numericLiteral);
  FRIEND_TEST(RdfParserTest, booleanLiteral);
  FRIEND_TEST(RdfParserTest, booleanLiteralLongForm);
  FRIEND_TEST(RdfParserTest, collection);
  FRIEND_TEST(RdfParserTest, iriref);
  FRIEND_TEST(RdfParserTest, specialPredicateA);
  FRIEND_TEST(RdfParserTest, EncodedIriManagerUsage);
  FRIEND_TEST(RdfParserTest, EncodedIriManagerPrefixedNames);
};

template <class Tokenizer_T>
class NQuadParser : public TurtleParser<Tokenizer_T> {
  TripleComponent defaultGraphId_ = qlever::specialIds().at(DEFAULT_GRAPH_IRI);
  TripleComponent activeObject_;
  TripleComponent activeGraphLabel_;
  using Base = TurtleParser<Tokenizer_T>;

 public:
  explicit NQuadParser(const EncodedIriManager* ev) : Base{ev} {};
  explicit NQuadParser(const EncodedIriManager* ev,
                       TripleComponent defaultGraphId)
      : Base{ev}, defaultGraphId_{std::move(defaultGraphId)} {}

 protected:
  bool statement() override;

 private:
  bool nQuadSubject();
  bool nQuadPredicate();
  bool nQuadObject();
  bool nQuadGraphLabel();
  bool nQuadLiteral();
};

/**
 * Parses turtle from std::string. Used to perform unit tests for
 * the different parser rules
 */
CPP_template(typename Parser)(requires ql::concepts::derived_from<
                              Parser, RdfParserBase>) class RdfStringParser
    : public Parser {
 public:
  using Parser::baseIri_;
  using Parser::prefixMap_;
  explicit RdfStringParser(const EncodedIriManager* encodedIriManager)
      : Parser{encodedIriManager} {}
  explicit RdfStringParser(const EncodedIriManager* encodedIriManager,
                           TripleComponent defaultGraph)
      : Parser{encodedIriManager, std::move(defaultGraph)} {}

  size_t getParsePosition() const override {
    return positionOffset_ + tmpToParse_.size() - this->tok_.data().size();
  }

  void initialize(const std::string&, ad_utility::MemorySize) {
    throw std::runtime_error(
        "RdfStringParser doesn't support calls to initialize. Only use "
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
    // TODO<joka921> Make it possible to use an optional here.
    EncodedIriManager encodedIriManager;
    RdfStringParser parser{&encodedIriManager};
    parser.setInputStream(objectString);
    parser.object();
    AD_CONTRACT_CHECK(parser.triples_.size() == 1);
    return std::move(parser.triples_[0].object_);
  }

  std::string_view getUnparsedRemainder() const { return this->tok_.view(); }

  // Parse directive and return true if a directive was found.
  bool parseDirectiveManually() { return this->directive(); }

  void raiseManually(std::string_view message) { this->raise(message); }

  void setPositionOffset(size_t offset) { positionOffset_ = offset; }

 private:
  // The complete input to this parser.
  qlever::parser::ByteBlock tmpToParse_;
  // Used to add a certain offset to the parsing position when using this
  // in a parallel setting.
  size_t positionOffset_ = 0;

 public:
  // testing interface for reusing a parser
  // only specifies the tokenizers input stream.
  // Does not alter the tokenizers state
  void setInputStream(std::string_view toParse) {
    tmpToParse_.clear();
    tmpToParse_.reserve(toParse.size());
    tmpToParse_.insert(tmpToParse_.end(), toParse.begin(), toParse.end());
    this->tok_.reset(tmpToParse_.data(), tmpToParse_.size());
  }

  const auto& getPrefixMap() const { return prefixMap_; }

  const auto& getBaseIri() const { return baseIri_; }

  // __________________________________________________________
  void setInputStream(qlever::parser::ByteBlock&& toParse) {
    tmpToParse_ = std::move(toParse);
    this->tok_.reset(tmpToParse_.data(), tmpToParse_.size());
  }

  // testing interface, only works when parsing from an utf8-string
  // return the current position of the tokenizer in the input string
  // can be used to test if the advancing of the tokenizer works
  // as expected
  size_t getPosition() const { return this->tok_.begin() - tmpToParse_.data(); }

  // Disable use of @base, @prefix and multiline string literals for turtle
  // parsers during parallel parsing.
  void useSimplifiedGrammar() { this->useSimplifiedGrammar_ = true; }

  FRIEND_TEST(RdfParserTest, prefixedName);
  FRIEND_TEST(RdfParserTest, prefixID);
  FRIEND_TEST(RdfParserTest, stringParse);
  FRIEND_TEST(RdfParserTest, rdfLiteral);
  FRIEND_TEST(RdfParserTest, predicateObjectList);
  FRIEND_TEST(RdfParserTest, objectList);
  FRIEND_TEST(RdfParserTest, object);
  FRIEND_TEST(RdfParserTest, blankNode);
  FRIEND_TEST(RdfParserTest, blankNodePropertyList);
  FRIEND_TEST(RdfParserTest, DateLiterals);
  FRIEND_TEST(RdfParserTest, DayTimeDurationLiterals);
};

#endif  // QLEVER_SRC_PARSER_RDFPARSER_H
