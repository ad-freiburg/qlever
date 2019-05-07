// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "PropertyPathParser.h"

#include <cctype>
#include <cstring>
#include <iostream>
#include <sstream>

#include "../util/HashSet.h"
#include "../util/StringUtils.h"
#include "ParseException.h"

bool PropertyPathParser::delimiters_initialized = false;

std::array<bool, 256> PropertyPathParser::DELIMITER_CHARS;
std::array<bool, 256> PropertyPathParser::VALID_CHARS;

// _____________________________________________________________________________
PropertyPathParser::PropertyPathParser(std::string_view str) : _string(str) {}

// _____________________________________________________________________________
PropertyPath PropertyPathParser::parse() {
  _tokens = tokenize(_string);
  _position = _tokens.begin();
  _end = _tokens.end();
  PropertyPath p = path();
  // Check that we parsed the entire input
  expectNone();
  return p;
}

// _____________________________________________________________________________
std::vector<PropertyPathParser::Token> PropertyPathParser::tokenize(
    std::string_view str) {
  // runs only once to initialize the delimiter characters
  if (!delimiters_initialized) {
    initDelimiters();
  }

  std::vector<Token> tokens;

  bool inside_iri = false;

  size_t start = 0;
  size_t pos = 0;
  while (pos < str.size()) {
    char c = str[pos];
    if (!VALID_CHARS[(uint8_t)c]) {
      std::stringstream s;
      s << "Invalid character " << c << " in property path " << str;
      throw ParseException(s.str());
    }
    if (c == '<') {
      inside_iri = true;
    } else if (c == '>') {
      inside_iri = false;
    }

    if (!inside_iri && DELIMITER_CHARS[(uint8_t)str[pos]] &&
        (pos != 0 || c != '?')) {
      if (start != pos) {
        // add the string up to but not including the new token
        tokens.push_back({str.substr(start, pos - start), start});

        start = pos;
      }
      while (pos < str.size() && DELIMITER_CHARS[(uint8_t)str[pos]]) {
        pos++;
        if (c == '*' && pos < str.size() && std::isdigit(str[pos])) {
          // The * token has a number following it
          pos++;
          while (pos < str.size() && std::isdigit(str[pos])) {
            pos++;
          }
          tokens.push_back({str.substr(start, pos - start), start});
          start = pos;
        } else {
          // Add the token
          tokens.push_back({str.substr(start, pos - start), start});
          start = pos;
        }
      }
      continue;
    }
    pos++;
  }
  if (start < str.size()) {
    tokens.push_back({str.substr(start), start});
  }
  return tokens;
}

// _____________________________________________________________________________
void PropertyPathParser::initDelimiters() {
  DELIMITER_CHARS.fill(0);
  DELIMITER_CHARS['?'] = true;
  DELIMITER_CHARS['*'] = true;
  DELIMITER_CHARS['+'] = true;
  DELIMITER_CHARS['/'] = true;
  DELIMITER_CHARS['|'] = true;
  DELIMITER_CHARS['('] = true;
  DELIMITER_CHARS[')'] = true;
  DELIMITER_CHARS['^'] = true;

  // Init the valid characters with all delimiters
  VALID_CHARS = DELIMITER_CHARS;
  // Iterate all ascii characters and add the valid ones
  for (uint8_t c = 0; c < 128; c++) {
    if (std::isgraph(c)) {
      VALID_CHARS[c] = true;
    }
  }
}

// _____________________________________________________________________________
PropertyPath PropertyPathParser::path() {
  // PathSequence ( '|' PathSequence )*
  const static std::string OR = "|";

  std::vector<PropertyPath> paths;

  paths.push_back(pathSequence());
  while (accept(OR)) {
    paths.push_back(pathSequence());
  }

  if (paths.size() == 1) {
    return paths[0];
  } else {
    PropertyPath p(PropertyPath::Operation::ALTERNATIVE);
    p._children = std::move(paths);
    return p;
  }
}

// _____________________________________________________________________________
PropertyPath PropertyPathParser::pathSequence() {
  // PathEltOrInverse ( '/' PathEltOrInverse )*
  const static std::string AND = "/";
  std::vector<PropertyPath> paths;

  paths.push_back(pathEltOrInverse());

  while (accept(AND)) {
    paths.push_back(pathEltOrInverse());
  }

  if (paths.size() == 1) {
    return paths[0];
  } else {
    PropertyPath p(PropertyPath::Operation::SEQUENCE);
    p._children = std::move(paths);
    return p;
  }
}

// _____________________________________________________________________________
PropertyPath PropertyPathParser::pathEltOrInverse() {
  // PathElt | '^' PathElt
  const static std::string NEGATE = "^";
  if (accept(NEGATE)) {
    PropertyPath p(PropertyPath::Operation::INVERSE);
    p._children.push_back(pathElt());
    return p;
  } else {
    return pathElt();
  }
}

// _____________________________________________________________________________
PropertyPath PropertyPathParser::pathElt() {
  // PathPrimary ('?' | '*' | '+')?
  const static std::string OPTIONAL = "?";
  const static std::string TRANSITIVE = "*";
  const static std::string AT_LEAST_ONE = "+";

  PropertyPath child = pathPrimary();
  std::string_view transitive_count;
  if (accept(OPTIONAL)) {
    PropertyPath p(PropertyPath::Operation::TRANSITIVE_MAX);
    p._limit = 1;
    p._children.push_back(child);
    return p;
  } else if (accept(TRANSITIVE)) {
    PropertyPath p(PropertyPath::Operation::TRANSITIVE);
    p._children.push_back(child);
    return p;
  } else if (accept(AT_LEAST_ONE)) {
    PropertyPath p(PropertyPath::Operation::TRANSITIVE_MIN);
    p._limit = 1;
    p._children.push_back(child);
    return p;
  } else if (acceptPrefix(TRANSITIVE, &transitive_count)) {
    PropertyPath p(PropertyPath::Operation::TRANSITIVE_MAX);
    p._limit = std::stoi(std::string(transitive_count).substr(1));
    p._children.push_back(child);
    return p;
  } else {
    return child;
  }
}

// _____________________________________________________________________________
PropertyPath PropertyPathParser::pathPrimary() {
  // iri | 'a' | '(' Path ')'
  const static std::string OPEN = "(";
  const static std::string CLOSE = ")";
  if (accept(OPEN)) {
    PropertyPath p = this->path();
    expect(CLOSE);
    return p;
  } else {
    PropertyPath p(PropertyPath::Operation::IRI);
    std::string_view iri_view;
    expectAny(&iri_view);
    p._iri = iri_view;
    return p;
  }
}

// _____________________________________________________________________________
bool PropertyPathParser::accept(const std::string& token) {
  if (_position != _end && _position->text == token) {
    _position++;
    return true;
  }
  return false;
}

// _____________________________________________________________________________
bool PropertyPathParser::acceptPrefix(const std::string& tokenPrefix,
                                      std::string_view* token) {
  if (_position != _end &&
      ad_utility::startsWith(_position->text, tokenPrefix)) {
    if (token != nullptr) {
      *token = _position->text;
    }
    _position++;
    return true;
  }
  return false;
}

// _____________________________________________________________________________
void PropertyPathParser::expect(const std::string& token) {
  if (_position == _end || _position->text != token) {
    std::stringstream s;
    s << "Expected " << token << " but got " << _position->text
      << " while parsing " << _string << " at pos " << _position->position;
    throw ParseException(s.str());
  }
  _position++;
}

// _____________________________________________________________________________
void PropertyPathParser::expectAny(std::string_view* token) {
  if (_position == _end) {
    throw ParseException("Expected another token in input " + _string);
  }
  *token = _position->text;
  _position++;
}

// _____________________________________________________________________________
void PropertyPathParser::expectNone() {
  if (_position != _end) {
    std::stringstream s;
    s << "Expected no more tokens in input " << _string << " but got "
      << _position->text << " at pos " << _position->position;
    throw ParseException(s.str());
  }
  _position++;
}

std::string_view PropertyPathParser::currentToken() { return _position->text; }

bool PropertyPathParser::isFinished() { return _position == _tokens.end(); }
