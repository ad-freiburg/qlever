// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "SparqlLexer.h"

#include "../util/HashSet.h"
#include "../util/StringUtils.h"
#include "./RdfEscaping.h"
#include "ParseException.h"
#include "Tokenizer.h"

const std::string SparqlToken::TYPE_NAMES[] = {
    "IRI",       "WS",         "KEYWORD", "VARIABLE", "SYMBOL",
    "AGGREGATE", "RDFLITERAL", "INTEGER", "FLOAT",    "LOGICAL_OR"};

const std::string LANGTAG = "@[a-zA-Z]+(-[a-zA-Z0-9]+)*";
const std::string IRIREF = "(<[^<>\"{}|^`\\\\\\[\\]\\x00-\\x20]*>)";
const std::string PN_CHARS_BASE =
    "[A-Z]|[a-z]|[\\x{00C0}-\\x{00D6}]|[\\x{00D8}-\\x{00F6}]|"
    "[\\x{00F8}-\\x{02FF}]|[\\x{0370}-\\x{037D}]|[\\x{037F}-\\x{1FFF}]|"
    "[\\x{200C}-\\x{200D}]|[\\x{2070}-\\x{218F}]|[\\x{2C00}-\\x{2FEF}]|"
    "[\\x{3001}-\\x{D7FF}]|[\\x{F900}-\\x{FDCF}]|[\\x{FDF0}-\\x{FFFD}]|"
    "[\\x{10000}-\\x{EFFFF}]";
const std::string WS = R"((\x20|\x09|\x0D|\x0A))";
const std::string ECHAR = R"(\\[tbnrf\\"'])";
const std::string INTEGER = "(-?[0-9]+)";
const std::string FLOAT = "(-?[0-9]+\\.[0-9]+)";

const std::string PN_CHARS_U = PN_CHARS_BASE + "|_";
const std::string PN_CHARS =
    PN_CHARS_U +
    "|-|[0-9]|\\x{00B7}|[\\x{0300}-\\x{036F}]|[\\x{203F}-\\x{2040}]";
const std::string PN_PREFIX =
    "(" + PN_CHARS_BASE + ")((" + PN_CHARS + "|\\.)*(" + PN_CHARS + "))?";
const std::string PLX =
    "(%[0-9a-fA-F][0-9a-fA-F])|(\\\\(_|~|\\.|-|!|$|&|'|\\(|\\)|\\*|\\+|,|;|=|/"
    "|\\?|#|@|%))";
const std::string PN_LOCAL = "(" + PN_CHARS_U + "|:|[0-9]|" + PLX + ")((" +
                             PN_CHARS + "|\\.|:|" + PLX + ")*(" + PN_CHARS +
                             "|:|" + PLX + "))?";

const std::string PNAME_NS = "(" + PN_PREFIX + ")?:";
const std::string PNAME_LN = "(" + PNAME_NS + ")(" + PN_LOCAL + ")";

const std::string IRI = "((" + LANGTAG + "@)?((" + IRIREF + ")|(" + PNAME_LN +
                        ")|(" + PNAME_NS + ")))";
const std::string VARNAME =
    "(" + PN_CHARS_U + "|[0-9])(" + PN_CHARS_U +
    "|[0-9]|\\x{00B7}|[\\x{0300}-\\x{036F}]|[\\x{203F}-\\x{2040}])*";
const std::string GROUP_BY = "(?i)(GROUP(\\s)*BY)";
const std::string ORDER_BY = "(?i)(ORDER(\\s)*BY)";
const std::string KEYWORD =
    "(?i)(TEXTLIMIT|PREFIX|SELECT|DISTINCT|REDUCED|"
    "HAVING|WHERE|ASC|AS|LIMIT|OFFSET|DESC|FILTER|VALUES|"
    "OPTIONAL|UNION|LANGMATCHES|LANG|TEXT|SCORE|REGEX|PREFIX|SEPARATOR|STR|"
    "BIND|MINUS)";
const std::string AGGREGATE = "(?i)(SAMPLE|COUNT|MIN|MAX|AVG|SUM|GROUP_CONCAT)";
const std::string VARIABLE = "(\\?" + VARNAME + ")";
const std::string SYMBOL = "([\\.\\{\\}\\(\\)\\=\\*,;:<>!\\|/\\^\\?\\*\\+-])";

const std::string STRING_LITERAL = "(('([^\\x27\\x5C\\x0A\\x0D]|(" + ECHAR +
                                   "))*')|"
                                   "(\"([^\\x22\\x5C\\x0A\\x0D]|(" +
                                   ECHAR + "))*\"))";
const std::string RDFLITERAL =
    STRING_LITERAL + "((" + LANGTAG + ")|(\\^\\^" + IRI + "))?";

const std::string LOGICAL_OR = "(\\|\\|)";

const SparqlLexer::RegexTokenMap& SparqlLexer::getRegexTokenMap() {
  using T = SparqlToken::Type;
  static const RegexTokenMap regexTokenMap = [=]() {
    RegexTokenMap m;
    auto add = [&m](const std::string& regex, T type) {
      m.push_back(std::make_pair(std::make_unique<re2::RE2>(regex), type));
    };
    add(KEYWORD, T::KEYWORD);
    add(GROUP_BY, T::GROUP_BY);
    add(ORDER_BY, T::ORDER_BY);
    add(AGGREGATE, T::AGGREGATE);
    add(LOGICAL_OR, T::LOGICAL_OR);
    add(VARIABLE, T::VARIABLE);
    add(IRI, T::IRI);
    add("(" + RDFLITERAL + ")", T::RDFLITERAL);
    add(FLOAT, T::FLOAT);
    add(INTEGER, T::INTEGER);
    add(SYMBOL, T::SYMBOL);
    add("(" + WS + "+)", T::WS);
    add("(#.*)", T::WS);
    return m;
  }();
  return regexTokenMap;
}

SparqlLexer::SparqlLexer(const std::string& sparql)
    : _sparql(sparql), _re_string(_sparql) {
  readNext();
}

bool SparqlLexer::empty() const { return _re_string.empty(); }

void SparqlLexer::readNext() {
  _current = _next;
  _next.type = SparqlToken::Type::WS;
  std::string raw;
  // Return the first token type matched.
  static const ad_utility::HashSet<SparqlToken::Type>
      tokensThatRequireLowercasing = {
          SparqlToken::Type::KEYWORD, SparqlToken::Type::GROUP_BY,
          SparqlToken::Type::ORDER_BY, SparqlToken::Type::AGGREGATE};
  while (_next.type == SparqlToken::Type::WS && !empty()) {
    _next.pos = _sparql.size() - _re_string.size();
    bool regexMatched = false;
    for (const auto& [regexPtr, type] : getRegexTokenMap()) {
      if (re2::RE2::Consume(&_re_string, *regexPtr, &raw)) {
        regexMatched = true;
        _next.type = type;
        if (tokensThatRequireLowercasing.contains(type)) {
          raw = ad_utility::getLowercaseUtf8(raw);
        }
        if (type == SparqlToken::Type::RDFLITERAL) {
          // unescaping of RDFLiteral, only applied to the actual literal and
          // not the datatype/langtag
          auto lastQuote = raw.rfind('"');
          std::string_view quoted{raw.begin(), raw.begin() + lastQuote + 1};
          std::string_view langtagOrDatatype{raw.begin() + lastQuote + 1,
                                             raw.end()};
          raw = RdfEscaping::normalizeRDFLiteral(quoted) + langtagOrDatatype;
        }
        break;  // we check the regexes in an order that ensures that stopping
                // at the first match is indeed correct.
      }
    }
    if (!regexMatched) {
      throw ParseException("Unexpected input: " + _re_string.as_string());
    }
  }
  _next.raw = raw;
}

void SparqlLexer::expandNextUntilWhitespace() {
  std::ostringstream s;
  while (_re_string.size() > 0 && !std::isspace(*_re_string.begin())) {
    s << *_re_string.begin();
    _re_string.remove_prefix(1);
  }
  _next.raw += s.str();
}

bool SparqlLexer::accept(SparqlToken::Type type) {
  if (_next.type == type) {
    readNext();
    return true;
  }
  return false;
}

bool SparqlLexer::accept(const std::string& raw, bool match_case) {
  if (match_case && _next.raw == raw) {
    readNext();
    return true;
  } else if (!match_case && ad_utility::getLowercaseUtf8(_next.raw) ==
                                ad_utility::getLowercaseUtf8(raw)) {
    readNext();
    return true;
  }
  return false;
}

void SparqlLexer::accept() { readNext(); }

void SparqlLexer::expect(SparqlToken::Type type) {
  if (_next.type != type) {
    std::ostringstream s;
    s << "Expected a token of type " << SparqlToken::TYPE_NAMES[(int)type]
      << " but got a token of type " << SparqlToken::TYPE_NAMES[(int)_next.type]
      << " (" << _next.raw << ") in the input at pos " << _next.pos << " : "
      << _sparql.substr(_next.pos, 256);
    throw ParseException(s.str());
  }
  readNext();
}
void SparqlLexer::expect(const std::string& raw, bool match_case) {
  if (match_case && _next.raw != raw) {
    std::ostringstream s;
    s << "Expected '" << raw << "' but got '" << _next.raw
      << "' in the input at pos " << _next.pos << " : "
      << _sparql.substr(_next.pos, 256);
    throw ParseException(s.str());
  } else if (!match_case && ad_utility::getLowercaseUtf8(_next.raw) !=
                                ad_utility::getLowercaseUtf8(raw)) {
    std::ostringstream s;
    s << "Expected '" << raw << "' but got '" << _next.raw
      << "' in the input at pos " << _next.pos << " : "
      << _sparql.substr(_next.pos, 256);
    throw ParseException(s.str());
  }
  readNext();
}
void SparqlLexer::expectEmpty() {
  if (!empty()) {
    std::ostringstream s;
    s << "Expected the end of the input but found "
      << _re_string.substr(0, 256);
    throw ParseException(s.str());
  }
}

const SparqlToken& SparqlLexer::current() { return _current; }

const std::string& SparqlLexer::input() const { return _sparql; }
