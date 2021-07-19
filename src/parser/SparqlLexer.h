// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include <re2/re2.h>

#include <iostream>
#include <memory>
#include <string>

struct SparqlToken {
  enum class Type {
    IRI,
    WS,
    KEYWORD,
    GROUP_BY,
    ORDER_BY,
    VARIABLE,
    SYMBOL,
    AGGREGATE,
    RDFLITERAL,
    INTEGER,
    FLOAT,
    LOGICAL_OR
  };
  static const std::string TYPE_NAMES[];

  std::string raw;
  Type type = Type::IRI;
  size_t pos;

  friend std::ostream& operator<<(std::ostream& os, const SparqlToken& t) {
    os << t.raw << " : " << TYPE_NAMES[(int)t.type];
    return os;
  }
};

class SparqlLexer {
 public:
 private:
  using RegexTokenMap =
      std::vector<std::pair<std::unique_ptr<re2::RE2>, SparqlToken::Type>>;

  // contains pairs of <regex, the corresponding token type>
  // These regexes have to be checked in the correct order, because
  // this lexer currently does not perform longest matches. For this
  // reason the result is a vector with the correct order.
  static const RegexTokenMap& getRegexTokenMap();

 public:
  SparqlLexer(const std::string& sparql);

  // True if the entire input stream was consumed
  bool empty() const;

  bool accept(SparqlToken::Type type);
  bool accept(const std::string& raw, bool match_case = true);
  // Accepts any token
  void accept();

  // Adds all symbols up to the next whitespace to the next token
  void expandNextUntilWhitespace();

  void expect(SparqlToken::Type type);
  void expect(const std::string& raw, bool match_case = true);
  void expectEmpty();

  const SparqlToken& current();
  const std::string& input() const;

 private:
  void readNext();

  const std::string _sparql;
  re2::StringPiece _re_string;
  SparqlToken _current;
  SparqlToken _next;
};
