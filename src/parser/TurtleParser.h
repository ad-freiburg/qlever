// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#pragma once

#include "../util/HashMap.h"
#include "./Tokenizer.h"

class TurtleParser {
 private:
  bool statement();
  bool directive();
  bool prefixID();
  bool base();
  bool sparqlPrefix();
  bool sparqlBase();
  bool triples();
  bool subject();
  bool pnameNS();
  bool iriref();
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
  bool iriref() { parseTerminal(_tokens.Iriref); }
  bool integer() { parseTerminal(_tokens.Integer); }
  bool decimal() { parseTerminal(_tokens.Decimal); }
  bool doubleParse() { parseTerminal(_tokens.Double); }
  bool pnameLN() { parseTerminal(_tokens.PnameLN); }
  bool pnameNS() { parseTerminal(_tokens.PnameNS); }

  bool skip(const std::wregex& reg);
  bool parseTerminal(const std::wregex& terminal);

  std::wstring _lastParseResult;
  std::wstring _baseIRI;
  ad_utility::HashMap<std::wstring, std::wstring> _prefixMap;
  std::wstring _activeSubject;
  std::wstring _activePredicate;
  ad_utility::HashMap<std::wstring, std::wstring> _blankNodeMap;
  Tokenizer<std::wstring::const_iterator> _tok;
  const TurtleToken& _tokens = _tok._tokens;
};
