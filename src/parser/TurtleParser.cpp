// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include "./TurtleParser.h"
#include <string.h>

// _______________________________________________________________
bool TurtleParser::statement() {
  _tok.skipWhitespaceAndComments();
  return directive() || (triples() && skip(_tokens.Dot));
}

// ______________________________________________________________
bool TurtleParser::directive() {
  return prefixID() || base() || sparqlPrefix() || sparqlBase();
}

// ________________________________________________________________
bool TurtleParser::prefixID() {
  if (skip(_tokens.TurtlePrefix)) {
    if (check(pnameNS()) && check(iriref()) && check(skip(_tokens.Dot))) {
      // strip  the angled brackes <bla> -> bla
      _prefixMap[_activePrefix] =
          _lastParseResult.substr(1, _lastParseResult.size() - 2);
      return true;
    } else {
      throw ParseException("prefixID");
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
      throw ParseException("base");
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
      throw ParseException("sparqlPrefix");
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
      throw ParseException("sparqlBase");
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
      throw ParseException("triples");
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
bool TurtleParser::verb() { return predicateSpecialA() || predicate(); }

// ___________________________________________________________________
bool TurtleParser::predicateSpecialA() {
  _tok.skipWhitespaceAndComments();
  if (auto [success, word] = _tok.getNextToken(_tokens.A); success) {
    (void)word;
    _activePredicate = u8"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
    return true;
  } else {
    return false;
  }
}

// ____________________________________________________________________
bool TurtleParser::subject() {
  if (blankNode() || iri() || collection()) {
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
  // these produce a single object that becomes part of a triple
  // check blank Node first because _: also could look like a prefix
  if (blankNode() || literal() || iri()) {
    emitTriple();
    return true;
  } else if (collection() || blankNodePropertyList()) {
    // these have a more complex logic and produce their own triples
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
  if (!skip(_tokens.OpenSquared)) {
    return false;
  }
  // save subject and predicate
  string savedSubject = _activeSubject;
  string savedPredicate = _activePredicate;
  // new triple with blank node as object
  string blank = createBlankNode();
  _lastParseResult = blank;
  emitTriple();
  // the following triples have the blank node as subject
  _activeSubject = blank;
  check(predicateObjectList());
  check(skip(_tokens.CloseSquared));
  // restore subject and predicate
  _activeSubject = savedSubject;
  _activePredicate = savedPredicate;
  return true;
}

// _____________________________________________________________________
bool TurtleParser::collection() {
  if (!skip(_tokens.OpenRound)) {
    return false;
  }
  throw ParseException(
      "We do not know how to handle collections in QLever yet\n");
  // TODO<joka921> understand collections
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
    // TODO<joka921> this allows spaces here since the ^^ is unique in the
    // sparql syntax. is this correct?
  } else if (skip(_tokens.DoubleCircumflex) && check(iri())) {
    _lastParseResult = s + "^^" + _lastParseResult;
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
  // manually parse strings for efficiency
  auto view = _tok.view();
  size_t startPos = 0;
  size_t endPos = 1;
  std::array<string, 4> quotes{"\"\"\"", "\'\'\'", "\"", "\'"};
  bool foundString = false;
  for (const auto& q : quotes) {
    if (ad_utility::startsWith(view, q)) {
      foundString = true;
      startPos = q.size();
      endPos = view.find(q, startPos);
      while (endPos != string::npos) {
        if (view[endPos - 1] == '\\') {
          size_t numBackslash = 1;
          auto slashPos = endPos - 2;
          while (view[slashPos] == '\\') {
            slashPos--;
            numBackslash++;
          }
          if (numBackslash % 2 == 0) {
            // even number of backslashes means that the quote we found has not
            // been escaped
            break;
          }
          endPos = view.find(q, endPos + 1);
        } else {
          // no backslash before " , the string has definitely ended
          break;
        }
      }
      break;
    }
  }
  if (!foundString) {
    return false;
  }
  if (endPos == string::npos) {
    throw ParseException("unterminated string Literal");
  }
  // also include the quotation marks in the word
  // TODO <joka921> how do we have to translate multiline strings for QLever?
  _lastParseResult = view.substr(0, endPos + startPos);
  _tok.data().remove_prefix(endPos + startPos);
  return true;
}

// ______________________________________________________________________
bool TurtleParser::iri() {
  // irirefs always start with "<", prefixedNames never, so the lookahead always
  // works
  return iriref() || prefixedName();
}

// _____________________________________________________________________
bool TurtleParser::prefixedName() {
  if (pnameLN() || pnameNS()) {
    _lastParseResult =
        '<' + expandPrefix(_activePrefix) + _lastParseResult + '>';
    return true;
  } else {
    return false;
  }
}

// _____________________________________________________________________
bool TurtleParser::blankNode() { return blankNodeLabel() || anon(); }

// _______________________________________________________________________
bool TurtleParser::parseTerminal(const RE2& terminal) {
  _tok.skipWhitespaceAndComments();
  auto [success, word] = _tok.getNextToken(terminal);
  if (success) {
    _lastParseResult = word;
    return true;
  } else {
    return false;
  }
  return false;
}

// ______________________________________________________________________
TurtleParserBackupState TurtleParser::backupState() const {
  TurtleParserBackupState b;
  b._numBlankNodes = _numBlankNodes;
  b._blankNodeMap = _blankNodeMap;
  b._numTriples = _triples.size();
  b._tokenizerPosition = _tok.data().begin();
  b._tokenizerSize = _tok.data().size();
  return b;
}

// _______________________________________________________________
bool TurtleParser::resetStateAndRead(const TurtleParserBackupState b) {
  _numBlankNodes = b._numBlankNodes;
  _blankNodeMap = b._blankNodeMap;
  AD_CHECK(_triples.size() >= b._numTriples);
  _triples.resize(b._numTriples);
  _tok.reset(b._tokenizerPosition, b._tokenizerSize);
  std::vector<char> buf;
  // TODO<joka921>: Does this work? Probably we should also take the
  // _tokenizerPosition into account here. TODO: verify BZIP2 variant and make
  // it efficient
  buf.reserve(_bufferSize + _tok.data().size());
  buf.resize(_tok.data().size());
  memcpy(buf.data(), _tok.data().begin(), _tok.data().size());
  // buf.insert(const_cast<char*>(_tok.data().begin()),
  //           const_cast<char*>(_tok.data().end()));
  auto remainderSize = buf.size();
  buf.resize(buf.size() + _bufferSize);
  LOG(INFO) << "decompressing next bytes\n";
  auto bytesRead =
      _bzipWrapper.decompressBlock(buf.data() + remainderSize, _bufferSize);
  if (!bytesRead) {
    return false;
  } else {
    buf.resize(remainderSize + bytesRead.value());
    LOG(INFO) << "Done. number of Bytes in parser Buffer: " << buf.size()
              << '\n';
    _byteVec = std::move(buf);
    _tok.reset(_byteVec.data(), _byteVec.size());
    return true;
  }
}
