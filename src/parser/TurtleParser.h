// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once

#include <codecvt>
#include <locale>
#include "../util/HashMap.h"
#include "../util/Log.h"
#include "./Tokenizer.h"

class TurtleParser {
 public:
  void parseUtf8String(const std::string& toParse) {
    _tmpToParse = s2w(toParse);
    _tok.reset(_tmpToParse.cbegin(), _tmpToParse.cend());
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
  bool pnameNS() { return parseTerminal(_tokens.PnameNS); }
  bool langtag() { return parseTerminal(_tokens.Langtag); }
  bool blankNodeLabel() { return parseTerminal(_tokens.BlankNodeLabel); }
  bool anon() { return parseTerminal(_tokens.Anon); }

  bool skip(const std::wregex& reg);
  bool parseTerminal(const std::wregex& terminal);

  void emitTriple() {
    LOG(INFO) << w2s(_activeSubject) << " " << w2s(_activePredicate) << " "
              << w2s(_lastParseResult) << '\n';
}

  std::wstring _lastParseResult;
  std::wstring _baseIRI;
  ad_utility::HashMap<std::wstring, std::wstring> _prefixMap;
  std::wstring _activeSubject;
  std::wstring _activePredicate;
  ad_utility::HashMap<std::wstring, std::wstring> _blankNodeMap;
  Tokenizer<std::wstring::const_iterator> _tok{_tmpToParse.cbegin(),
                                               _tmpToParse.cend()};
  const TurtleToken& _tokens = _tok._tokens;

  mutable std::wstring_convert<std::codecvt_utf8<wchar_t>> _converter;
  std::string w2s(std::wstring w) const { return _converter.to_bytes(w); }
  std::wstring s2w(std::string s) const { return _converter.from_bytes(s); }

  std::wstring _tmpToParse = L"";
};
