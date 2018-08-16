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
  if (skip(_tokens.TurtlePrefix) && pnameNS() && iriref() &&
      skip(_tokens.Dot)) {
    // TODO: get values and update prefixMap
    return true;
  } else {
    return false;
  }
}

// ________________________________________________________________
bool TurtleParser::base() {
  if (skip(_tokens.TurtleBase) && iriref()) {
    _baseIRI = _lastParseResult;
    return true;
  } else {
    return false;
  }
}

// ________________________________________________________________
bool TurtleParser::sparqlPrefix() {
  if (skip(_tokens.SparqlPrefix) && pnameNS() && iriref()) {
    // TODO: get values and update prefixMap
    return true;
  } else {
    return false;
  }
}

// ________________________________________________________________
bool TurtleParser::sparqlBase() {
  if (skip(_tokens.SparqlBase) && iriref()) {
    // TODO: get values and update prefixMap
    return true;
  } else {
    return false;
  }
}

// ________________________________________________
bool TurtleParser::triples() {
  if (subject() && predicateObjectList()) {
    return true;
  } else {
    if (blankNodePropertyList()) {
      predicateObjectList();
      return true;
    } else {
      return false;
    }
  }
}

// __________________________________________________
bool TurtleParser::predicateObjectList() {
  if (verb() && objectList()) {
    while (skip(_tokens.Semicolon)) {
      if (verb()) {
        if (!objectList()) {
          // TODO: this is actually a parse error
          return false;
        }
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
    while (skip(_tokens.Comma)) {
      if (!object()) {
        // TODO parse Error
        return false;
      }
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
    _activePredicate = L"http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::subject() {
  if (iri() || BlankNode() || collection()) {
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
    _emitTriple();
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::literal() {
  return (rdfLiteral() || NumericLiteral() || BooleanLiteral());
}

// _____________________________________________________________________
bool TurtleParser::blankNodePropertyList() {
  return skip(_tokens.OpenSquared) && predicateObjectList() &&
         skip(_tokens.CloseSquared);
}

// _____________________________________________________________________
bool TurtleParser::collection() {
  if (!skip(_tokens.OpenRound)) {
    return false;
  }
  while (object()) {
  }
  // TODO: here would be a parser error
  return skip(_tokens.CloseRound);
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
  } else if (skip(_tokens.DoubleCircumflex)) {
    if
      iri() {
        _lastParseResult = s + _lastParseResult;
        return true;
      }
    else {
      // TODO: parseError here if false
      return false;
    }
  } else {
    // it is okay to neither have a langtag nor a xsd datatype
    return true;
  }
}

// ______________________________________________________________________
bool TurtleParser::booleanLiteral() {
  std::vector<std::wregex*> candidates;
  candidates.push_back(&(_tokens->True));
  candidates.push_back(&(_tokens->False));
  if (auto [success, index, word] = _tok.getNextToken(candidates); success) {
    _lastParseResult = word;
  } else {
    return false;
  }
}

// ______________________________________________________________________
bool TurtleParser::stringParse() {
  std::vector<std::wregex*> candidates;
  candidates.push_back(&(_tokens->StringLiteralQuote));
  candidates.push_back(&(_tokens->StringLiteralSingleQuote));
  candidates.push_back(&(_tokens->StringLiteralLongSingleQuote));
  candidates.push_back(&(_tokens->StringLiteralLongQuote));
  if (auto [success, index, word] = _tok.getNextToken(candidates); success) {
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
}

// ______________________________________________________________________
bool TurtleParser::iri() { return iriref() || prefixedName(); }

// _____________________________________________________________________
bool TurtleParser::prefixedName { return pnameLN() || pnameNS(); }

// _____________________________________________________________________
bool TurtleParser::blankNodePropertyListN { return blankNodeLabel() || anon(); }

// _______________________________________________________________________
bool TurtleParser::parseTerminal(const std::wregex& terminal) {
  auto [success, word] = _tok.getNextToken(terminal);
  if (success) {
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
}
