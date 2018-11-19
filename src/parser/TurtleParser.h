// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <codecvt>
#include <exception>
#include <locale>
#include <string_view>
#include "../util/Exception.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./Bzip2Wrapper.h"
#include "./Tokenizer.h"

using std::string;

// struct that can store the state of a parser
// the previously extracted triples are not stored
// but only the number of triples that were already present
// before the backup
struct TurtleParserBackupState {
  ad_utility::HashMap<std::string, std::string> _blankNodeMap;
  size_t _numBlankNodes = 0;
  size_t _numTriples;
  const char* _tokenizerPosition;
  size_t _tokenizerSize;
};

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

  TurtleParser() = default;
  // Construct from the name of an uncompressed .ttl
  // or a compressed .bz2 file
  // BZ2 support is still experimental
  explicit TurtleParser(const string& filename) {
    if (ad_utility::endsWith(filename, ".ttl")) {
      mapFile(filename);
    } else if (ad_utility::endsWith(filename, ".bz2")) {
      _isBzip = true;
      _bzipWrapper.open(filename);
      _byteVec.resize(_bufferSize);
      // decompress the first block and initialize Tokenizer
      if (auto res =
              _bzipWrapper.decompressBlock(_byteVec.data(), _byteVec.size());
          res) {
        _byteVec.resize(res.value());
        _tok.reset(_byteVec.data(), _byteVec.size());
      } else {
        LOG(INFO) << "This bz2 file seems to contain no data!\n";
      }
    }
  }
  ~TurtleParser() { unmapFile(); }

  // Wrapper to getLine that is expected by the rest of QLever
  bool getLine(std::array<string, 3>& triple) { return getLine(&triple); }

  // Main access method to the parser
  // If a triple can be parsed (or has previously been parsed and stored
  // Writes the triple to the argument (format subject, object predicate)
  // returns true iff a triple can be successfully written, else the triple
  // value is invalid and the parser is at the end of the input.
  bool getLine(std::array<string, 3>* triple) {
    TurtleParserBackupState b;
    while (_triples.empty()) {
      // TODO: Backing up every time is probably slow
      if (_isBzip) {
        // if parsing the line fails because our buffer ends before the end of
        // the next statement we need to be able to recover
        b = backupState();
      }
      bool parsedStatement;
      bool exceptionThrown = false;
      ParseException ex;
      // If this is not a bzip instance, then exceptions are immediately
      // rethrown In the bzip case we can try with an extended buffer
      try {
        // variable parsedStatement will be true iff a statement can
        // successfully be parsed
        parsedStatement = statement();
      } catch (const ParseException& p) {
        if (!_isBzip) {
          throw p;
        } else {
          parsedStatement = false;
          exceptionThrown = true;
          ex = p;
        }
      }

      if (!parsedStatement) {
        if (!_isBzip) {
          auto& d = _tok.data();
          if (d.size() > 0) {
            LOG(INFO) << "Parsing of line has Failed, but parseInput is not "
                         "yet exhausted. Remaining bytes: "
                      << d.size() << '\n';
            auto s = std::min(size_t(1000), size_t(d.size()));
            LOG(INFO) << "Logging first 1000 unparsed characters\n";
            LOG(INFO) << std::string_view(d.data(), s);
          }
          return false;
        } else {
          // try to uncompress a larger buffer and repeat the reading process
          if (resetStateAndRead(b)) {
            continue;
          } else {
            if (exceptionThrown) {
              // we are at the end of a bzip2 input with an active exception
              throw ex;
            } else {
              // we are at the end of a bzip2 input without and exception
              // the input is exhausted
              return false;
            }
          }
        }
      }
    }
    // we now have at least one triple (otherwise we would have returned early
    *triple = _triples.back();
    _triples.pop_back();
    return true;
  }

  // load a string object directly to the buffer
  // allows easier testing without a file object
  void parseUtf8String(const std::string& toParse) {
    unmapFile();
    _tmpToParse = toParse;
    _tok.reset(_tmpToParse.data(), _tmpToParse.size());
    // directly parse the whole triple
    turtleDoc();
  }

  // interface needed for convenient testing

 private:
  /* Data Members */

  // the result of the last succesful call to a parsing function
  // (a function named after a (non-)terminal of the Turtle grammar
  std::string _lastParseResult;

  // the currently active base IRI
  std::string _baseIRI;

  // maps prefixes to their expanded form
  ad_utility::HashMap<std::string, std::string> _prefixMap;
  // maps blank node representations from the input to their representation
  // in the output
  ad_utility::HashMap<std::string, std::string> _blankNodeMap;

  // there are turtle constructs that reuse prefixes, subjects and predicates
  // so we have to save the last seen ones
  std::string _activePrefix;
  std::string _activeSubject;
  std::string _activePredicate;
  Tokenizer _tok{_tmpToParse.data(), _tmpToParse.size()};
  const TurtleToken& _tokens = _tok._tokens;
  size_t _numBlankNodes = 0;
  // only for testing first, stores the triples that have been parsed
  std::vector<std::array<string, 3>> _triples;
  Bzip2Wrapper _bzipWrapper;

  // the various ways to store the input to this parser
  // used when parsing directly from a string
  std::string _tmpToParse = u8"";
  // used with compressed inputs (a certain number of bytes is uncompressed at
  // once, might also be cut in the middle of a utf8 string. (bzip2 does not no
  // about encodings etc)
  std::vector<char> _byteVec;
  // used when mapping a turtle file via Mmap
  const char* _data = nullptr;
  // store the size of the mapped input when using the _data ptr
  // via mmap
  size_t _dataSize = 0;

  // remember which input variant is currently in use
  bool _isMmapped = false;
  bool _isBzip = false;

  // this many characters will be uncompressed at once, defaults to
  // roughly 100 MB
  size_t _bufferSize = 100 << 20;

  /* private Member Functions */

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
  bool iriref() {
    // manually check if the input starts with "<" and then find the next ">"
    // this might accept invalid irirefs but is faster than checking the
    // complete regexes
    _tok.skipWhitespaceAndComments();
    auto view = _tok.view();
    if (ad_utility::startsWith(view, "<")) {
      auto endPos = view.find_first_of("> \n");
      if (endPos == string::npos || view[endPos] != '>') {
        throw ParseException("Iriref");
      } else {
        _tok.data().remove_prefix(endPos + 1);
        _lastParseResult = view.substr(0, endPos + 1);
        return true;
      }
    } else {
      return false;
    }
  }
  bool integer() { return parseTerminal(_tokens.Integer); }
  bool decimal() { return parseTerminal(_tokens.Decimal); }
  bool doubleParse() { return parseTerminal(_tokens.Double); }
  bool pnameLN() {
    // relaxed parsing, only works if the greedy parsing of the ":"
    // is ok
    _tok.skipWhitespaceAndComments();
    auto view = _tok.view();
    auto pos = view.find(":");
    if (pos == string::npos) {
      return false;
    }
    // these can also be part of a collection etc.
    // find any character that can end a pnameLN
    auto posEnd = view.find_first_of(" \n,;", pos);
    if (posEnd == string::npos) {
      // make tests work
      posEnd = view.size();
    }
    // TODO: is it allowed to have no space between triples and the dots? in
    // this case we have to check something here
    _activePrefix = view.substr(0, pos);
    _lastParseResult = view.substr(pos + 1, posEnd - (pos + 1));
    // we do not remove the whitespace or the ,; since they are needed
    _tok.data().remove_prefix(posEnd);
    return true;
  }

  // __________________________________________________________________________
  bool pnameNS() {
    if (parseTerminal(_tokens.PnameNS)) {
      // this also includes a ":" which we do not need, hence the "-1"
      _activePrefix = _lastParseResult.substr(0, _lastParseResult.size() - 1);
      _lastParseResult = "";
      return true;
    } else {
      return false;
    }
  }

  // __________________________________________________________________________
  bool langtag() { return parseTerminal(_tokens.Langtag); }
  bool blankNodeLabel() {
    if (!parseTerminal(_tokens.BlankNodeLabel)) {
      return false;
    }

    if (_blankNodeMap.count(_lastParseResult)) {
      // the same blank node has been used before, reuse it
      _lastParseResult = _blankNodeMap[_lastParseResult];
    } else {
      string blank = createBlankNode();
      _blankNodeMap[_lastParseResult] = blank;
      _lastParseResult = blank;
    }
    return true;
  }

  bool anon() {
    if (!parseTerminal(_tokens.Anon)) {
      return false;
    }
    _lastParseResult = createBlankNode();
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

  // initialize parse input from a turtle file using mmap
  void mapFile(const string& filename) {
    unmapFile();
    ad_utility::File f(filename.c_str(), "r");
    size_t size = f.sizeOfFile();
    LOG(INFO) << "mapping " << size << " bytes" << std::endl;
    const int fd = f.getFileDescriptor();
    void* ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
    AD_CHECK(ptr != MAP_FAILED);
    f.close();
    _isMmapped = true;
    _isBzip = false;
    _tmpToParse = "";
    _dataSize = size;
    _data = static_cast<char*>(ptr);
    // set the tokenizers input to the complete mmap range
    _tok.reset(_data, _dataSize);
  }

  // has to be called after finishing parsing of an mmaped .ttl file
  // if no file is currently mapped this function has no effect
  void unmapFile() {
    if (_isMmapped && _data) {
      munmap(const_cast<char*>(_data), _dataSize);
      _data = nullptr;
      _dataSize = 0;
    }
    _isMmapped = false;
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
  string createBlankNode() {
    string res = "_:" + std::to_string(_numBlankNodes);
    _numBlankNodes++;
    return res;
  }

  // testing interface for reusing a parser
  void reset(const string& utf8String) {
    unmapFile();
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
