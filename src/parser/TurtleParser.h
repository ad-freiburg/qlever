// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once

#include <gtest/gtest.h>
#include <codecvt>
#include <exception>
#include <locale>
#include <string_view>
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./Tokenizer.h"

using std::string;

class ParseException : public std::exception {
 public:
  ParseException() = default;
  ParseException(const string& msg) : _msg(msg) {}
  const char* what() const throw() { return _msg.c_str(); }

 private:
  string _msg = "Error while parsing Turtle";
};

class TurtleParser {
 public:
  void parseUtf8String(const std::string& toParse) {
    _tmpToParse = toParse;
    _tok.reset(_tmpToParse.data(), _tmpToParse.size());
    turtleDoc();
  }

  // interface needed for convenient testing

 private:
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

  // Terminals
  bool iriref() { return parseTerminal(_tokens.Iriref); }
  bool integer() { return parseTerminal(_tokens.Integer); }
  bool decimal() { return parseTerminal(_tokens.Decimal); }
  bool doubleParse() { return parseTerminal(_tokens.Double); }
  bool pnameLN() {
    if (parseTerminal(_tokens.PnameLN)) {
      // TODO: better do this with subgroup matching directly
      auto pos = _lastParseResult.find(':');
      _activePrefix = _lastParseResult.substr(0, pos);
      _lastParseResult = _lastParseResult.substr(pos + 1);
      return true;
    } else {
      return false;
    }
  }
  bool pnameNS() {
    if (parseTerminal(_tokens.PnameNS)) {
      // this also includes a ":" which we do not need
      _activePrefix = _lastParseResult.substr(0, _lastParseResult.size() - 1);
      _lastParseResult = "";
      return true;
    } else {
      return false;
    }
  }
  bool langtag() { return parseTerminal(_tokens.Langtag); }
  bool blankNodeLabel() {
    if (!parseTerminal(_tokens.BlankNodeLabel)) {
      return false;
    }

    if (_blankNodeMap.count(_lastParseResult)) {
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
  }

  bool skip(const RE2& reg) {
    _tok.skipWhitespaceAndComments();
    return _tok.skip(reg);
  }
  bool parseTerminal(const RE2& terminal);

  void emitTriple() {
    LOG(INFO) << _activeSubject << " " << _activePredicate << " "
              << _lastParseResult << '\n';
    _triples.push_back({_activeSubject, _activePredicate, _lastParseResult});
  }

  std::string _lastParseResult;
  std::string _baseIRI;
  ad_utility::HashMap<std::string, std::string> _prefixMap;
  ad_utility::HashMap<std::string, std::string> _blankNodeMap;
  std::string _activePrefix;
  std::string _activeSubject;
  std::string _activePredicate;
  Tokenizer _tok{_tmpToParse.data(), _tmpToParse.size()};
  const TurtleToken& _tokens = _tok._tokens;
  size_t _numBlankNodes = 0;
  // only for testing first, stores the triples that have been parsed
  std::vector<std::array<string, 3>> _triples;

  std::string _tmpToParse = u8"";

  bool check(bool result) {
    if (result) {
      return true;
    } else {
      throw ParseException();
      return false;
    }
  }

  // _______________________________________________________________
  string expandPrefix(const string& prefix) {
    if (!_prefixMap.count(prefix)) {
      throw ParseException("Prefix " + prefix +
                           " was not registered using a PREFIX or @prefix "
                           "declaration before using it!\n");
    } else {
      return _prefixMap[prefix];
    }
  }

  // create a new, unused, unique blank node.
  string createBlankNode() {
    string res = "_:" + std::to_string(_numBlankNodes);
    _numBlankNodes++;
    return res;
  }

  // testing interface
  void reset(const string& utf8String) {
    _tmpToParse = utf8String;
    _tok.reset(_tmpToParse.data(), _tmpToParse.size());
  }

  // ___________________________________________________
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
};
