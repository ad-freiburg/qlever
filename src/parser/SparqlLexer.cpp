// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "SparqlLexer.h"
#include "../util/StringUtils.h"
#include "ParseException.h"
#include "Tokenizer.h"

const std::string SparqlToken::TYPE_NAMES[] = {
    "IRI",       "WS",         "KEYWORD", "VARIABLE", "SYMBOL",
    "AGGREGATE", "RDFLITERAL", "INTEGER", "FLOAT",    "LOGICAL_OR"};

const std::string SparqlLexer::LANGTAG = "@[a-zA-Z]+(-[a-zA-Z0-9]+)*";
const std::string SparqlLexer::IRIREF =
    "(<[^<>\"{}|^`\\\\\\[\\]\\x00-\\x20]*>)";
const std::string SparqlLexer::PN_CHARS_BASE =
    "[A-Z]|[a-z]|[\\x{00C0}-\\x{00D6}]|[\\x{00D8}-\\x{00F6}]|"
    "[\\x{00F8}-\\x{02FF}]|[\\x{0370}-\\x{037D}]|[\\x{037F}-\\x{1FFF}]|"
    "[\\x{200C}-\\x{200D}]|[\\x{2070}-\\x{218F}]|[\\x{2C00}-\\x{2FEF}]|"
    "[\\x{3001}-\\x{D7FF}]|[\\x{F900}-\\x{FDCF}]|[\\x{FDF0}-\\x{FFFD}]|"
    "[\\x{10000}-\\x{EFFFF}]";
const std::string SparqlLexer::WS = R"((\x20|\x09|\x0D|\x0A))";
const std::string SparqlLexer::ECHAR = R"(\\[tbnrf\\"'])";
const std::string SparqlLexer::INTEGER = "(-?[0-9]+)";
const std::string SparqlLexer::FLOAT = "(-?[0-9]+\\.[0-9]+)";

const std::string SparqlLexer::PN_CHARS_U = PN_CHARS_BASE + "|_";
const std::string SparqlLexer::PN_CHARS =
    PN_CHARS_U +
    "|-|[0-9]|\\x{00B7}|[\\x{0300}-\\x{036F}]|[\\x{203F}-\\x{2040}]";
const std::string SparqlLexer::PN_PREFIX =
    "(" + PN_CHARS_BASE + ")((" + PN_CHARS + "|\\.)*(" + PN_CHARS + "))?";
const std::string SparqlLexer::PLX =
    "(%[0-9a-fA-F][0-9a-fA-F])|(\\\\(_|~|\\.|-|!|$|&|'|\\(|\\)|\\*|\\+|,|;|=|/"
    "|\\?|#|@|%))";
const std::string SparqlLexer::PN_LOCAL =
    "(" + PN_CHARS_U + "|:|[0-9]|" + PLX + ")((" + PN_CHARS + "|\\.|:|" + PLX +
    ")*(" + PN_CHARS + "|:|" + PLX + "))?";

const std::string SparqlLexer::PNAME_NS = "(" + PN_PREFIX + ")?:";
const std::string SparqlLexer::PNAME_LN =
    "(" + PNAME_NS + ")(" + PN_LOCAL + ")";

const std::string SparqlLexer::IRI = "((" + LANGTAG + "@)?((" + IRIREF + ")|(" +
                                     PNAME_LN + ")|(" + PNAME_NS + ")))";
const std::string SparqlLexer::VARNAME =
    "(" + PN_CHARS_U + "|[0-9])(" + PN_CHARS_U +
    "|[0-9]|\\x{00B7}|[\\x{0300}-\\x{036F}]|[\\x{203F}-\\x{2040}])*";
const std::string SparqlLexer::GROUP_BY = "(?i)(GROUP(\\s)*BY)";
const std::string SparqlLexer::ORDER_BY = "(?i)(ORDER(\\s)*BY)";
const std::string SparqlLexer::KEYWORD =
    "(?i)(TEXTLIMIT|PREFIX|SELECT|DISTINCT|REDUCED|"
    "HAVING|WHERE|ASC|AS|LIMIT|OFFSET|DESC|FILTER|VALUES|"
    "OPTIONAL|UNION|LANGMATCHES|LANG|TEXT|SCORE|REGEX|PREFIX|SEPARATOR|STR|"
    "BIND|MINUS)";
const std::string SparqlLexer::AGGREGATE =
    "(?i)(SAMPLE|COUNT|MIN|MAX|AVG|SUM|GROUP_CONCAT)";
const std::string SparqlLexer::VARIABLE = "(\\?" + VARNAME + ")";
const std::string SparqlLexer::SYMBOL =
    "([\\.\\{\\}\\(\\)\\=\\*,;:<>!\\|/\\^\\?\\*\\+])";

const std::string SparqlLexer::STRING_LITERAL =
    "(('([^\\x27\\x5C\\x0A\\x0D]|(" + ECHAR +
    "))*')|"
    "(\"([^\\x22\\x5C\\x0A\\x0D]|(" +
    ECHAR + "))*\"))";
const std::string SparqlLexer::RDFLITERAL =
    STRING_LITERAL + "((" + LANGTAG + ")|(\\^\\^" + IRI + "))?";

const std::string SparqlLexer::LOGICAL_OR = "(\\|\\|)";

const re2::RE2 SparqlLexer::RE_IRI = re2::RE2(IRI);
const re2::RE2 SparqlLexer::RE_WS = re2::RE2("(" + WS + "+)");
const re2::RE2 SparqlLexer::RE_KEYWORD = re2::RE2(KEYWORD);
const re2::RE2 SparqlLexer::RE_GROUP_BY = re2::RE2(GROUP_BY);
const re2::RE2 SparqlLexer::RE_ORDER_BY = re2::RE2(ORDER_BY);
const re2::RE2 SparqlLexer::RE_VARIABLE = re2::RE2(VARIABLE);
const re2::RE2 SparqlLexer::RE_SYMBOL = re2::RE2(SYMBOL);
const re2::RE2 SparqlLexer::RE_AGGREGATE = re2::RE2(AGGREGATE);
const re2::RE2 SparqlLexer::RE_RDFLITERAL = re2::RE2("(" + RDFLITERAL + ")");
const re2::RE2 SparqlLexer::RE_INTEGER = re2::RE2(INTEGER);
const re2::RE2 SparqlLexer::RE_FLOAT = re2::RE2(FLOAT);
const re2::RE2 SparqlLexer::RE_LOGICAL_OR = re2::RE2(LOGICAL_OR);

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
  while (_next.type == SparqlToken::Type::WS && !empty()) {
    _next.pos = _sparql.size() - _re_string.size();
    if (re2::RE2::Consume(&_re_string, RE_KEYWORD, &raw)) {
      _next.type = SparqlToken::Type::KEYWORD;
      raw = ad_utility::getLowercaseUtf8(raw);
    } else if (re2::RE2::Consume(&_re_string, RE_GROUP_BY, &raw)) {
      _next.type = SparqlToken::Type::GROUP_BY;
      raw = ad_utility::getLowercaseUtf8(raw);
    } else if (re2::RE2::Consume(&_re_string, RE_ORDER_BY, &raw)) {
      _next.type = SparqlToken::Type::ORDER_BY;
      raw = ad_utility::getLowercaseUtf8(raw);
    } else if (re2::RE2::Consume(&_re_string, RE_AGGREGATE, &raw)) {
      _next.type = SparqlToken::Type::AGGREGATE;
      raw = ad_utility::getLowercaseUtf8(raw);
    } else if (re2::RE2::Consume(&_re_string, RE_KEYWORD, &raw)) {
      _next.type = SparqlToken::Type::KEYWORD;
      raw = ad_utility::getLowercaseUtf8(raw);
    } else if (re2::RE2::Consume(&_re_string, RE_LOGICAL_OR, &raw)) {
      _next.type = SparqlToken::Type::LOGICAL_OR;
    } else if (re2::RE2::Consume(&_re_string, RE_VARIABLE, &raw)) {
      _next.type = SparqlToken::Type::VARIABLE;
    } else if (re2::RE2::Consume(&_re_string, RE_IRI, &raw)) {
      _next.type = SparqlToken::Type::IRI;
    } else if (re2::RE2::Consume(&_re_string, RE_RDFLITERAL, &raw)) {
      _next.type = SparqlToken::Type::RDFLITERAL;
      raw = TurtleToken::normalizeRDFLiteral(raw);
    } else if (re2::RE2::Consume(&_re_string, RE_FLOAT, &raw)) {
      _next.type = SparqlToken::Type::FLOAT;
    } else if (re2::RE2::Consume(&_re_string, RE_INTEGER, &raw)) {
      _next.type = SparqlToken::Type::INTEGER;
    } else if (re2::RE2::Consume(&_re_string, RE_SYMBOL, &raw)) {
      _next.type = SparqlToken::Type::SYMBOL;
    } else if (re2::RE2::Consume(&_re_string, RE_WS, &raw)) {
      _next.type = SparqlToken::Type::WS;
    } else if (_re_string[0] == '#') {
      // Start of a comment. Consume everything up to the next newline.
      while (!_re_string.empty() && _re_string[0] != '\n') {
        _re_string.remove_prefix(1);
      }
    } else {
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
