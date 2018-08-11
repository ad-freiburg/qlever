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

