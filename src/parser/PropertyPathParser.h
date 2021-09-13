// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "ParsedQuery.h"

class PropertyPathParser {
  struct Token {
    std::string_view text;
    size_t position;
  };

  using ParserPos = std::vector<Token>::iterator;

 public:
  PropertyPathParser(std::string_view str);
  PropertyPath parse();

 private:
  static std::vector<Token> tokenize(std::string_view str);

  /**
   * If the next token matches token returns true and consumes the token.
   * Returns false and does nothing otherwise.
   */
  bool accept(const std::string& token);

  /**
   * If the next token begins with tokenPrefix returns true and consumes the
   * token. Returns false and does nothing otherwise. If the token matches the
   * prefix it is stored in token.
   */
  bool acceptPrefix(const std::string& tokenPrefix,
                    std::string_view* token = nullptr);

  /**
   * Consumes the next token if it matches token. Throws a ParseException
   * otherwise
   */
  void expect(const std::string& token);

  /**
   * If there is still another token consumes that token, otherwise throws a
   * parser exception. If the token exists stores it in token
   */
  void expectAny(std::string_view* token = nullptr);

  /**
   * Throws an exception if there are more tokens to be processed.
   */
  void expectNone();

  std::string_view currentToken();

  /**
   * @return True if the parser reached the end of the tokens.
   */
  bool isFinished();

  // Parsing methods
  // PathSequence ( '|' PathSequence )*
  PropertyPath path();

  // PathEltOrInverse ( '/' PathEltOrInverse )*
  PropertyPath pathSequence();

  // PathElt | '^' PathElt
  PropertyPath pathEltOrInverse();

  // PathPrimary ('?' | '*' | '+')?
  PropertyPath pathElt();

  // iri | 'a' | '(' Path ')'
  PropertyPath pathPrimary();

  std::string_view _string;
  std::vector<Token> _tokens;
  ParserPos _position;
  ParserPos _end;
};
