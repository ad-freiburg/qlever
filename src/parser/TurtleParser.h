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

  bool skip(const std::wregex& reg);

  std::wstring _lastParseResult;
  std::wstring _baseIRI;
  ad_utility::HashMap<std::wstring, std::wstring> _prefixMap;
  std::wstring _activeSubject;
  std::wstring _activePredicate;
  ad_utility::HashMap<std::wstring, std::wstring> _blankNodeMap;
  Tokenizer<std::wstring::const_iterator> _tok;
  const TurtleToken& _tokens = _tok._tokens;
};
