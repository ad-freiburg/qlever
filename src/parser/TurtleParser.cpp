// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "./TurtleParser.h"

// TODO: always disambiguate if we indeed take the longest match

// _______________________________________________________________
bool TurtleParser::statement() {
  return ((directive() || triples()) && skip(_tokens.Dot));
}

// ______________________________________________________________
bool TurtleParser::directive() {
  return prefixID() || base() || sparqlPrefix() || sparqlBase();
}

// ________________________________________________________________
bool TurtleParser::prefixID() {
  if (skip(_tokens.TurtlePrefix)) {
    if (pnameNS() && iriref() && skip(_tokens.Dot)) {
      _prefixMap[_activePrefix] = _lastParseResult;
      return true;
    } else {
      throw ParseException();
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
bool TurtleParser::base() {
  if (skip(_tokens.TurtleBase)) {
    if (iriref()) {
      _baseIRI = _lastParseResult;
      return true;
    } else {
      throw ParseException();
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
bool TurtleParser::sparqlPrefix() {
  if (skip(_tokens.SparqlPrefix)) {
    if (pnameNS() && iriref()) {
      _prefixMap[_activePrefix] = _lastParseResult;
      return true;
    } else {
      throw ParseException();
    }
  } else {
    return false;
  }
}

// ________________________________________________________________
bool TurtleParser::sparqlBase() {
  if (skip(_tokens.SparqlBase)) {
    if (iriref()) {
      _baseIRI = _lastParseResult;
      return true;
    } else {
      throw ParseException();
    }
  } else {
    return false;
  }
}

// ________________________________________________
bool TurtleParser::triples() {
  if (subject()) {
    if (predicateObjectList()) {
      return true;
    } else {
      throw ParseException();
    }
  } else {
    if (blankNodePropertyList()) {
      predicateObjectList();
      return true;
    } else {
      // TODO: do we need to throw here
      return false;
    }
  }
}

// __________________________________________________
bool TurtleParser::predicateObjectList() {
  if (verb() && check(objectList())) {
    while (skip(_tokens.Semicolon)) {
      if (verb() && check(objectList())) {
        continue;
      }
    }
    return true;
  } else {
    return false;
  }
}

// _____________________________________________________
bool TurtleParser::objectList() {
  if (object()) {
    while (skip(_tokens.Comma) && check(object())) {
      continue;
    }
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________
bool TurtleParser::verb() { return predicate() || predicateSpecialA(); }

// ___________________________________________________________________
bool TurtleParser::predicateSpecialA() {
  if (auto [success, word] = _tok.getNextToken(_tokens.A); success) {
    (void)word;
    _activePredicate = u8"http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::subject() {
  if (iri() || blankNode() || collection()) {
    _activeSubject = _lastParseResult;
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::predicate() {
  if (iri()) {
    _activePredicate = _lastParseResult;
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::object() {
  if (iri() || blankNode() || collection() || blankNodePropertyList() ||
      literal()) {
    emitTriple();
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::literal() {
  return (rdfLiteral() || numericLiteral() || booleanLiteral());
}

// _____________________________________________________________________
bool TurtleParser::blankNodePropertyList() {
  // TODO: how do these have to be parsed
  return skip(_tokens.OpenSquared) &&
         check(predicateObjectList() && skip(_tokens.CloseSquared));
}

// _____________________________________________________________________
bool TurtleParser::collection() {
  if (!skip(_tokens.OpenRound)) {
    return false;
  }
  while (object()) {
  }
  return check(skip(_tokens.CloseRound));
}

// ______________________________________________________________________
bool TurtleParser::numericLiteral() {
  return integer() || decimal() || doubleParse();
}

// ______________________________________________________________________
bool TurtleParser::rdfLiteral() {
  if (!stringParse()) {
    return false;
  }
  auto s = _lastParseResult;
  if (langtag()) {
    _lastParseResult = s + _lastParseResult;
    return true;
  } else if (skip(_tokens.DoubleCircumflex) && check(iri())) {
    return true;
  } else {
    // it is okay to neither have a langtag nor a xsd datatype
    return true;
  }
}

// ______________________________________________________________________
bool TurtleParser::booleanLiteral() {
  std::vector<const RE2*> candidates;
  candidates.push_back(&(_tokens.True));
  candidates.push_back(&(_tokens.False));
  if (auto [success, index, word] = _tok.getNextToken(candidates); success) {
    (void)index;
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________________________
bool TurtleParser::stringParse() {
  std::vector<const RE2*> candidates;
  candidates.push_back(&(_tokens.StringLiteralQuote));
  candidates.push_back(&(_tokens.StringLiteralSingleQuote));
  candidates.push_back(&(_tokens.StringLiteralLongSingleQuote));
  candidates.push_back(&(_tokens.StringLiteralLongQuote));
  if (auto [success, index, word] = _tok.getNextToken(candidates); success) {
    (void)index;
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________________________
bool TurtleParser::iri() { return iriref() || prefixedName(); }

// _____________________________________________________________________
bool TurtleParser::prefixedName() {
  if (pnameLN() || pnameNS()) {
    _lastParseResult = _prefixMap[_activePrefix] + _lastParseResult;
  return true;
  } else {
    return false;
  }
}

// _____________________________________________________________________
bool TurtleParser::blankNode() { return blankNodeLabel() || anon(); }

// _______________________________________________________________________
bool TurtleParser::parseTerminal(const RE2& terminal) {
  auto [success, word] = _tok.getNextToken(terminal);
  if (success) {
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
  return false;
}
