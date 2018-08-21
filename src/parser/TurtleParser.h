// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once

#include <codecvt>
#include <exception>
#include <locale>
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./Tokenizer.h"

class ParseException : public std::exception {
  const char* what() const throw() { return "Error while parsing Turtle"; }
};

class TurtleParser {
 public:
  void parseUtf8String(const std::string& toParse) {
    _tmpToParse = toParse;
    _tok.reset(_tmpToParse.data(), _tmpToParse.size());
    turtleDoc();
  }

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
  bool pnameLN() { return parseTerminal(_tokens.PnameLN); }
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
  bool blankNodeLabel() { return parseTerminal(_tokens.BlankNodeLabel); }
  bool anon() { return parseTerminal(_tokens.Anon); }

  bool skip(const RE2& reg) { return _tok.skip(reg); }
  bool parseTerminal(const RE2& terminal);

  void emitTriple() {
    LOG(INFO) << _activeSubject << " " << _activePredicate << " "
              << _lastParseResult << '\n';
  }

  std::string _lastParseResult;
  std::string _baseIRI;
  ad_utility::HashMap<std::string, std::string> _prefixMap;
  std::string _activePrefix;
  std::string _activeSubject;
  std::string _activePredicate;
  ad_utility::HashMap<std::string, std::string> _blankNodeMap;
  Tokenizer _tok{_tmpToParse.data(), _tmpToParse.size()};
  const TurtleToken& _tokens = _tok._tokens;

  std::string _tmpToParse = u8"";

  bool check(bool result) {
    if (result) {
      return true;
    } else {
      throw ParseException();
      return false;
    }
  }
};
