// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <codecvt>
#include <exception>
#include <future>
#include <locale>
#include <string_view>
#include "../global/Constants.h"
#include "../index/ConstantsIndexCreation.h"
#include "../util/Exception.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./Tokenizer.h"
#include "ParallelBuffer.h"

using std::string;

// The actual parser class
class TurtleParser {
 public:
  class ParseException : public std::exception {
   public:
    ParseException() = default;
    ParseException(const string& msg) : _msg(msg) {}
    const char* what() const throw() { return _msg.c_str(); }

   private:
    string _msg = "Error while parsing Turtle";
  };

  // open an input file or stream
  virtual void initialize(const string& filename) = 0;
  virtual ~TurtleParser() = default;

  // Wrapper to getLine that is expected by the rest of QLever
  bool getLine(std::array<string, 3>& triple) { return getLine(&triple); }

  // Main access method to the parser
  // If a triple can be parsed (or has previously been parsed and stored
  // Writes the triple to the argument (format subject, object predicate)
  // returns true iff a triple can be successfully written, else the triple
  // value is invalid and the parser is at the end of the input.
  virtual bool getLine(std::array<string, 3>* triple) = 0;

 protected:
  // clear all the parser's state to the initial values.
  void clear() {
    _lastParseResult.clear();
    _baseIRI.clear();

    _activeSubject.clear();
    _activePredicate.clear();
    _activePrefix.clear();

    _prefixMap.clear();

    _tok.reset(nullptr, 0);
    _triples.clear();
    _numBlankNodes = 0;
    _isParserExhausted = false;
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
  /* Data Members */

  // Stores the triples that have been parsed but not retrieved yet.
  std::vector<std::array<string, 3>> _triples;

  // if this is set, there is nothing else to parse and we will only
  // retrieve what is left in our tripleBuffer;
  bool _isParserExhausted = false;

  // The tokenizer
  Tokenizer _tok{nullptr, 0};

  // the result of the last succesful call to a parsing function
  // (a function named after a (non-)terminal of the Turtle grammar
  std::string _lastParseResult;

  // the currently active base IRI
  std::string _baseIRI;

  // maps prefixes to their expanded form
  ad_utility::HashMap<std::string, std::string> _prefixMap;

  // there are turtle constructs that reuse prefixes, subjects and predicates
  // so we have to save the last seen ones
  std::string _activePrefix;
  std::string _activeSubject;
  std::string _activePredicate;
  const TurtleToken& _tokens = _tok._tokens;
  size_t _numBlankNodes = 0;

 private:
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
  bool integer() { return parseTerminal(_tokens.Integer); }
  bool decimal() { return parseTerminal(_tokens.Decimal); }
  bool doubleParse() { return parseTerminal(_tokens.Double); }
  bool pnameLN();

  // __________________________________________________________________________
  bool pnameNS();

  // __________________________________________________________________________
  bool langtag() { return parseTerminal(_tokens.Langtag); }
  bool blankNodeLabel();

  bool anon() {
    if (!parseTerminal(_tokens.Anon)) {
      return false;
    }
    _lastParseResult = createAnonNode();
    return true;
  }

  // Skip a givene regex without parsing it
  bool skip(const RE2& reg) {
    _tok.skipWhitespaceAndComments();
    return _tok.skip(reg);
  }

  // if the prefix of the current input position matches the regex argument,
  // put the matching prefix into _lastParseResult, move the input position
  // forward by the length of the match and return true else return false and do
  // not change the parser's state
  bool parseTerminal(const RE2& terminal);

  // ______________________________________________________________________________________
  void emitTriple() {
    _triples.push_back({_activeSubject, _activePredicate, _lastParseResult});
  }

  // enforce that the argument is true: if it is false, a parse Exception is
  // thrown this helps formulating the LL1 property in easily readable code
  bool check(bool result) {
    if (result) {
      return true;
    } else {
      throw ParseException();
    }
  }

  // map a turtle prefix to its expanded form. Throws if the prefix was not
  // properly registered before
  string expandPrefix(const string& prefix) {
    if (!_prefixMap.count(prefix)) {
      throw ParseException("Prefix " + prefix +
                           " was not registered using a PREFIX or @prefix "
                           "declaration before using it!\n");
    } else {
      return _prefixMap[prefix];
    }
  }

  // create a new, unused, unique blank node string
  string createAnonNode() {
    string res = ANON_NODE_PREFIX + ":" + std::to_string(_numBlankNodes);
    _numBlankNodes++;
    return res;
  }

  FRIEND_TEST(TurtleParserTest, prefixedName);
  FRIEND_TEST(TurtleParserTest, prefixID);
  FRIEND_TEST(TurtleParserTest, stringParse);
  FRIEND_TEST(TurtleParserTest, rdfLiteral);
  FRIEND_TEST(TurtleParserTest, predicateObjectList);
  FRIEND_TEST(TurtleParserTest, objectList);
  FRIEND_TEST(TurtleParserTest, object);
  FRIEND_TEST(TurtleParserTest, blankNode);
  FRIEND_TEST(TurtleParserTest, blankNodePropertyList);
};

/**
 * Parses turtle from std::string. Used to perform unit tests for
 * the different parser rules
 */
class TurtleStringParser : public TurtleParser {
 public:
  using TurtleParser::getLine;
  virtual bool getLine(std::array<string, 3>* triple) override {
    (void)triple;
    throw std::runtime_error(
        "TurtleStringParser doesn't support calls to getLine. Only use "
        "parseUtf8String() for unit tests\n");
  }

  virtual void initialize(const string& filename) override {
    (void)filename;
    throw std::runtime_error(
        "TurtleStringParser doesn't support calls to initialize. Only use "
        "parseUtf8String() for unit tests\n");
  }

  // load a string object directly to the buffer
  // allows easier testing without a file object
  void parseUtf8String(const std::string& toParse) {
    _tmpToParse = toParse;
    _tok.reset(_tmpToParse.data(), _tmpToParse.size());
    // directly parse the whole triple
    turtleDoc();
  }

 private:
  // the various ways to store the input to this parser
  // used when parsing directly from a string
  // TODO: move this to a separate class.
  std::string _tmpToParse = u8"";

  // testing interface for reusing a parser
  // only specifies the tokenizers input stream.
  // Does not alter the tokenizers state
  void setInputStream(const string& utf8String) {
    _tmpToParse = utf8String;
    _tok.reset(_tmpToParse.data(), _tmpToParse.size());
  }

  // testing interface, only works when parsing from an utf8-string
  // return the current position of the tokenizer in the input string
  // can be used to test if the advancing of the tokenizer works
  // as expected
  size_t getPosition() const {
    return _tok.data().begin() - _tmpToParse.data();
  }

  FRIEND_TEST(TurtleParserTest, prefixedName);
  FRIEND_TEST(TurtleParserTest, prefixID);
  FRIEND_TEST(TurtleParserTest, stringParse);
  FRIEND_TEST(TurtleParserTest, rdfLiteral);
  FRIEND_TEST(TurtleParserTest, predicateObjectList);
  FRIEND_TEST(TurtleParserTest, objectList);
  FRIEND_TEST(TurtleParserTest, object);
  FRIEND_TEST(TurtleParserTest, blankNode);
  FRIEND_TEST(TurtleParserTest, blankNodePropertyList);
};

/**
 * This class is a TurtleParser that always assumes that
 * its input file is an uncompressed .ttl file that will be read in
 * chunks. Input file can also be a stream like stdin.
 */
class TurtleStreamParser : public TurtleParser {
  // struct that can store the state of a parser
  // the previously extracted triples are not stored
  // but only the number of triples that were already present
  // before the backup
  struct TurtleParserBackupState {
    size_t _numBlankNodes = 0;
    size_t _numTriples;
    const char* _tokenizerPosition;
    size_t _tokenizerSize;
  };

 public:
  // Default construction needed for tests
  TurtleStreamParser() = default;
  TurtleStreamParser(const string& filename) {
    LOG(INFO) << "Initialize turtle Parsing from uncompressed file or stream "
              << filename << '\n';
    initialize(filename);
  }

  // inherit the wrapper overload
  using TurtleParser::getLine;

  virtual bool getLine(std::array<string, 3>* triple) override;

  virtual void initialize(const string& filename) override;

 private:
  // Backup the current state of the turtle parser to a
  // TurtleparserBackupState object
  // This can be used e.g. when parsing from a compressed input
  // and the currently uncompressed buffer is not sufficient to parse the
  // next expression
  TurtleParserBackupState backupState() const;

  // Reset the parser to the state indicated by the argument
  // Must be called on the same parser object that was used to create the backup
  // state (the actual triples are not backed up)
  bool resetStateAndRead(const TurtleParserBackupState state);

  // stores the current batch of bytes we have to parse.
  // Might end in the middle of a statement or even a multibyte utf8 character,
  // that's why we need the backupState() and resetStateAndRead() methods
  std::vector<char> _byteVec;

  std::unique_ptr<ParallelBuffer> _fileBuffer;
  // this many characters will be buffered at once,
  // defaults to a global constant
  size_t _bufferSize = FILE_BUFFER_SIZE;
};

/**
 * This class is a TurtleParser
 * reads from an uncompressed .ttl file, that has to lie in memory.
 * It will be loaded at once using Mmap. Thus this class does not support
 * Parsing from streams like stdin
 */
class TurtleMmapParser : public TurtleParser {
 public:
  TurtleMmapParser(const string& filename) {
    LOG(INFO) << "Initialize turtle Parsing from uncompressed file " << filename
              << '\n';
    LOG(INFO) << "This file must reside in memory since it will be loaded "
                 "using the mmap system call.\n";
    initialize(filename);
  }

  ~TurtleMmapParser() { unmapFile(); }

  // inherit the other overload
  using TurtleParser::getLine;
  // overload of the actual mmap-based parsing logic
  virtual bool getLine(std::array<string, 3>* triple) override;

  // initialize parse input from a turtle file using mmap
  virtual void initialize(const string& filename) override;

  // has to be called after finishing parsing of an mmaped .ttl file
  // if no file is currently mapped this function has no effect
  void unmapFile() {
    if (_data) {
      munmap(const_cast<char*>(_data), _dataSize);
      _data = nullptr;
      _dataSize = 0;
    }
  }

  // used when mapping a turtle file via Mmap
  const char* _data = nullptr;
  // store the size of the mapped input when using the _data ptr
  // via mmap
  size_t _dataSize = 0;
};
