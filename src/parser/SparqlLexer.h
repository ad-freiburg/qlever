// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include <re2/re2.h>

#include <iostream>
#include <memory>
#include <string>

struct SparqlToken {
  enum class Type {
    IRI, /* Internationalized Resource Identifiers */
    WS,  /* White Space */
    KEYWORD,
    GROUP_BY,
    ORDER_BY,
    VARIABLE,
    SYMBOL,
    AGGREGATE,
    RDFLITERAL,
    INTEGER,
    FLOAT,
    LOGICAL_OR,
    A_RDF_TYPE_ALIAS,
    BOOLEAN_LITERAL
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
  using RegexTokenMap =
      std::vector<std::pair<std::unique_ptr<re2::RE2>, SparqlToken::Type>>;

  // contains pairs of <regex, the corresponding token type>
  // These regexes have to be checked in the correct order, because
  // this lexer currently does not perform longest matches. For this
  // reason the result is a vector with the correct order.
  static const RegexTokenMap& getRegexTokenMap();

 public:
  explicit SparqlLexer(const std::string& sparql);

  // Copying and moving is disallowed, the default behavior is wrong,
  // and we don't need it.
  SparqlLexer(const SparqlLexer&) = delete;
  SparqlLexer& operator=(const SparqlLexer&) = delete;
  SparqlLexer(SparqlLexer&&) noexcept = delete;
  SparqlLexer& operator=(SparqlLexer&&) = delete;

  // Explicitly reset this Lexer to a new input
  void reset(std::string sparql);

  // True if the entire input stream was consumed
  [[nodiscard]] bool empty() const;

  bool accept(SparqlToken::Type type);
  bool accept(std::string_view raw, bool match_case = true);
  // Accepts any token
  void accept();

  // Checks whether the next token matches but does not consume it.
  [[nodiscard]] bool peek(SparqlToken::Type type) const;
  [[nodiscard]] bool peek(std::string_view raw, bool match_case = true) const;

  // Adds all symbols up to the next whitespace to the next token
  void expandNextUntilWhitespace();

  void expect(SparqlToken::Type type);
  void expect(const std::string& raw, bool match_case = true);
  void expectEmpty();

  const SparqlToken& current();
  [[nodiscard]] const std::string& input() const;

  // Get the part of the input that has not yet been consumed by calls to
  // `accept` or `expect`
  std::string getUnconsumedInput() {
    auto delimiter =
        _next.raw.empty() || _re_string.ToString().empty() ? "" : " ";
    return _next.raw + delimiter + _re_string.ToString();
  }

 private:
  void readNext();

  std::string _sparql;
  re2::StringPiece _re_string;
  SparqlToken _current;
  SparqlToken _next;
};
