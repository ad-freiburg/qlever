// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include <re2/re2.h>
#include <iostream>
#include <string>

struct SparqlToken {
  enum class Type {
    IRI,
    WS,
    KEYWORD,
    VARIABLE,
    SYMBOL,
    AGGREGATE,
    RDFLITERAL,
    INTEGER,
    FLOAT
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
 private:
  // The rules for the lexer
  static const std::string IRIREF;
  static const std::string IRI;
  static const std::string PN_CHARS_BASE;
  static const std::string PN_CHARS_U;
  static const std::string PN_CHARS;
  static const std::string PN_PREFIX;
  static const std::string PLX;
  static const std::string PN_LOCAL;
  static const std::string VARNAME;
  static const std::string WS;
  static const std::string KEYWORD;
  static const std::string VARIABLE;
  static const std::string SYMBOL;
  static const std::string AGGREGATE;
  static const std::string ECHAR;
  static const std::string LANGTAG;
  static const std::string STRING_LITERAL;
  static const std::string RDFLITERAL;
  static const std::string PNAME_NS;
  static const std::string PNAME_LN;
  static const std::string INTEGER;
  static const std::string FLOAT;

  static const re2::RE2 RE_IRI;
  static const re2::RE2 RE_WS;
  static const re2::RE2 RE_KEYWORD;
  static const re2::RE2 RE_VARIABLE;
  static const re2::RE2 RE_SYMBOL;
  static const re2::RE2 RE_AGGREGATE;
  static const re2::RE2 RE_RDFLITERAL;
  static const re2::RE2 RE_INTEGER;
  static const re2::RE2 RE_FLOAT;

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
