// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#pragma once

#include <ctre/ctre.h>
#include <gtest/gtest.h>
#include <re2/re2.h>
#include <regex>
#include "../util/Exception.h"
#include "../util/Log.h"

using re2::RE2;
using namespace std::string_literals;

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B - 1> operator+(
    const ctll::fixed_string<A>& a, const ctll::fixed_string<B>& b) {
  char32_t comb[A + B - 1] = {};
  for (size_t i = 0; i < A - 1;
       ++i) {  // omit the trailing 0 of the first string
    comb[i] = a.content[i];
  }
  for (size_t i = 0; i < B; ++i) {
    comb[i + A - 1] = b.content[i];
  }
  return ctll::fixed_string(comb);
}

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B - 1> operator+(
    const ctll::fixed_string<A>& a, const char (&b)[B]) {
  auto bStr = ctll::fixed_string(b);
  return a + bStr;
}

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B - 1> operator+(
    const char (&a)[A], const ctll::fixed_string<B>& b) {
  auto aStr = ctll::fixed_string(a);
  return aStr + b;
}

using ctll::fixed_string;
using namespace ctre::literals;

/// Create a regex group by putting the argument in parentheses
template <size_t N>
static constexpr auto grp(const ctll::fixed_string<N>& s) {
  return fixed_string("(") + s + fixed_string(")");
}

/// Create a regex character class by putting the argument in square brackets
template <size_t N>
static constexpr auto cls(const ctll::fixed_string<N>& s) {
  return fixed_string("[") + s + fixed_string("]");
}

/// Create a ctll::fixed string from a compile time character constant. The
/// short name helps keep the overview when assembling long regexes.
template <typename T, size_t N>
constexpr fixed_string<N> fs(const T (&input)[N]) noexcept {
  return fixed_string(input);
}

/// One entry for each Token in the Turtle Grammar. Used to create a unified
/// Interface to the two different Tokenizers
enum class TokId : int {
  TurtlePrefix,
  SparqlPrefix,
  TurtleBase,
  SparqlBase,
  Dot,
  Comma,
  Semicolon,
  OpenSquared,
  CloseSquared,
  OpenRound,
  CloseRound,
  A,
  DoubleCircumflex,
  True,
  False,
  Langtag,
  Integer,
  Decimal,
  Exponent,
  Double,
  Iriref,
  PnameNS,
  PnameLN,
  BlankNodeLabel,
  WsMultiple,
  Anon,
  Comment
};

/**
 * @brief Holds the Compile-Time-Regexes for the Turtle Grammar used by the
 * TokenizerCTRE
 *
 * Caveat: The Prefix names are currently restricted to ascii values.
 */
struct TurtleTokenCtre {
  template <size_t N>
  using STR = ctll::fixed_string<N>;

  static constexpr auto TurtlePrefix = grp(fixed_string("@prefix"));
  // TODO: this is actually case-insensitive
  static constexpr auto SparqlPrefix = grp(fixed_string("PREFIX"));
  static constexpr auto TurtleBase = grp(fixed_string("@base"));
  static constexpr auto SparqlBase = grp(fixed_string("BASE"));
  static constexpr auto Dot = grp(fixed_string("\\."));
  static constexpr auto Comma = grp(fixed_string(","));
  static constexpr auto Semicolon = grp(fixed_string(";"));
  static constexpr auto OpenSquared = grp(fixed_string("\\["));
  static constexpr auto CloseSquared = grp(fixed_string("\\]"));
  static constexpr auto OpenRound = grp(fixed_string("\\("));
  static constexpr auto CloseRound = grp(fixed_string("\\)"));
  static constexpr auto A = grp(fixed_string("a"));
  static constexpr auto DoubleCircumflex = grp(fixed_string("\\^\\^"));
  static constexpr auto True = grp(fixed_string("true"));
  static constexpr auto False = grp(fixed_string("false"));

  static constexpr auto Langtag =
      grp(fixed_string("@[a-zA-Z]+(\\-[a-zA-Z0-9]+)*"));
  static constexpr auto Integer = grp(fixed_string("[\\+\\-]?[0-9]+"));
  static constexpr auto Decimal = grp(fixed_string("[\\+\\-]?[0-9]*\\.[0-9]+"));
  static constexpr auto ExponentString = fixed_string("[eE][\\+\\-]?[0-9]+");
  static constexpr auto Exponent = grp(ExponentString);

  static constexpr auto DoubleString = fs("[\\+\\-]?([0-9]+\\.[0-9]*") +
                                       ExponentString + "|" + ExponentString +
                                       ")";

  static constexpr auto Double = grp(DoubleString);

  static constexpr auto HexString = fs("0-9A-Fa-f");
  static constexpr auto UcharString =
      fs("\\\\u[0-9a-fA-f]{4}|\\\\U[0-9a-fA-f]{8}");

  static constexpr auto EcharString = fs("\\\\[tbnrf\"\'\\\\]");

  static constexpr auto StringLiteralQuoteString =
      fs("\"([^\\x22\\x5C\\x0A\\x0D]|") + EcharString + "|" + UcharString +
      ")*\"";

  static constexpr auto StringLiteralSingleQuoteString =
      fs("'([^\\x27\\x5C\\x0A\\x0D]|") + EcharString + "|" + UcharString +
      ")*'";
  static constexpr auto StringLiteralLongSingleQuoteString =
      fs("'''((''|')?([^'\\\\]|") + EcharString + "|" + UcharString + "))*'''";

  static constexpr auto StringLiteralLongQuoteString =
      fs("\"\"\"((\"\"|\")?([^\"\\\\]|") + EcharString + "|" + UcharString +
      "))*\"\"\"";

  // TODO: fix this!
  static constexpr auto IrirefString =
      R"(<([^\x00-\x20<>"{}\x7c^`\\]|)" + UcharString + ")*>";

  static constexpr auto PercentString = "%" + cls(HexString) + "{2}";

  /*
  // TODO<joka921: Currently CTRE does not really support UTF-8. I tried to hack
  the UTF-8 byte patterns
  for this regex, see the file Utf8RegexTest.cpp, but this did increase
  compile time by an infeasible amount ( more than 20 minutes for this file
  alone which made debugging impossible) Thus we will currently only allow
  simple prefixes and emit proper warnings.
            "A-Za-z\\x{00C0}-\\x{00D6}\\x{00D8}-\\x{00F6}\\x{00F8}-"
            "\\x{02FF}"
            "\\x{0370}-"
            "\\x{037D}\\x{037F}-\\x{1FFF}\\x{200C}-\\x{200D}\\x{2070}-\\x{"
            "218F}"
            "\\x{2C00}-"
            "\\x{2FEF}"
            "\\x{3001}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFFD}"
            "\\x{00010000}-"
            "\\x{000EFFFF}");
            */
  static constexpr auto PnCharsBaseString = fixed_string("A-Za-z");

  static constexpr auto PnCharsUString = PnCharsBaseString + "_";

  // TODO<joka921>:  here we have the same issue with UTF-8 in CTRE as above
  /*
  static constexpr auto PnCharsString =
            PnCharsUString +
            "\\-0-9\\x{00B7}\\x{0300}-\\x{036F}\\x{203F}-\\x{2040}";
            */

  static constexpr auto PnCharsString = PnCharsUString + "\\-0-9";

  static constexpr auto PnPrefixString = cls(PnCharsBaseString) + "(\\." +
                                         cls(PnCharsString) + "|" +
                                         cls(PnCharsString) + ")*";

  static constexpr auto PnameNSString = PnPrefixString + ":";

  static constexpr fixed_string PnLocalEscString =
      "\\\\[_~.\\-!$&'()*+,;=/?#@%]";

  static constexpr fixed_string PlxString =
      PercentString + "|" + PnLocalEscString;

  static constexpr fixed_string TmpNoDot =
      cls(PnCharsString + ":") + "|" + PlxString;
  static constexpr fixed_string PnLocalString =
      grp(cls(PnCharsUString + ":0-9") + "|" + PlxString) +
      grp("\\.*" + grp(TmpNoDot)) + "*";

  static constexpr fixed_string PnameLNString =
      grp(PnameNSString) + grp(PnLocalString);

  static constexpr fixed_string BlankNodeLabelString =
      fs("_:") + cls(PnCharsUString + "0-9") +
      grp("\\.*" + cls(PnCharsString)) + "*";

  static constexpr fixed_string WsSingleString = "\\x20\\x09\\x0D\\x0A";

  static constexpr fixed_string WsMultipleString = cls(WsSingleString) + "*";

  static constexpr fixed_string AnonString = "\\[" + WsMultipleString + "\\]";

  static constexpr fixed_string CommentString = "#[^\\n]*\\n";

  static constexpr fixed_string Iriref = grp(IrirefString);
  static constexpr fixed_string PnameNS = grp(PnameNSString);
  static constexpr fixed_string PnameLN = grp(PnameLNString);
  static constexpr fixed_string BlankNodeLabel = grp(BlankNodeLabelString);

  static constexpr fixed_string WsMultiple = grp(WsMultipleString);
  static constexpr fixed_string Anon = grp(AnonString);
  static constexpr fixed_string Comment = grp(CommentString);
};

/** holds all the google re2 regexes that correspond to all terminals in the
 * turtle grammar cannot be static since google regexes have to be constructed
 * at runtime
 */
struct TurtleToken {
  /**
   * @brief convert a RDF Literal to a unified form that is used inside QLever
   *
   * RDFLiterals in Turtle or Sparql can have several forms: Either starting
   * with one (" or ') quotation mark and containing escape sequences like
   * "\\\t" or with three (""" or ''') quotation marks and allowing most control
   * sequences to be contained in the string directly.
   *
   * This function converts any of this forms to a literal that starts and ends
   * with a single quotation mark '"' and contains the originally escaped
   * characters directly, e.g. "al\"pha" becomes "al"pha".
   *
   * This is NOT a valid RDF form of literals, but this format is only used
   * inside QLever. By stripping the leading and trailing quotation mark and
   * possible langtags or datatype URIS one can directly obtain the actual
   * content of the literal.
   *
   * @param literal
   * @return
   */
  static std::string normalizeRDFLiteral(std::string_view literal) {
    std::string res = "\"";
    auto lastQuot = literal.find_last_of("\"\'");
    AD_CHECK(lastQuot != std::string_view::npos);
    auto langtagOrDatatype = literal.substr(lastQuot + 1);
    literal.remove_suffix(literal.size() - lastQuot - 1);
    if (ad_utility::startsWith(literal, "\"\"\"") ||
        ad_utility::startsWith(literal, "'''")) {
      AD_CHECK(ad_utility::endsWith(literal, literal.substr(0, 3)));
      literal.remove_prefix(3);
      literal.remove_suffix(3);
    } else {
      AD_CHECK(ad_utility::startsWith(literal, "\"") ||
               ad_utility::startsWith(literal, "'"));
      AD_CHECK(ad_utility::endsWith(literal, literal.substr(0, 1)));
      literal.remove_prefix(1);
      literal.remove_suffix(1);
    }
    auto pos = literal.find('\\');
    while (pos != literal.npos) {
      res.append(literal.begin(), literal.begin() + pos);
      AD_CHECK(pos + 1 <= literal.size());
      switch (literal[pos + 1]) {
        case 't':
          res.push_back('\t');
          break;
        case 'n':
          res.push_back('\n');
          break;
        case 'r':
          res.push_back('\r');
          break;
        case 'b':
          res.push_back('\b');
          break;
        case 'f':
          res.push_back('\f');
          break;
        case '"':
          res.push_back('\"');
          break;
        case '\'':
          res.push_back('\'');
          break;
        case '\\':
          res.push_back('\\');
          break;

        default:
          throw std::runtime_error(
              "Illegal escape sequence in RDF Literal. This should never "
              "happen, please report this");
      }
      literal.remove_prefix(pos + 2);
      pos = literal.find('\\');
    }
    res.append(literal);
    res.push_back('"');
    res.append(langtagOrDatatype);
    return res;
  }

  /**
   * @brief take an unescaped rdfLiteral that has single "" quotes as created by
   * normalizeRDFLiteral and escape it again
   */
  static std::string escapeRDFLiteral(std::string_view literal) {
    AD_CHECK(ad_utility::startsWith(literal, "\""));
    std::string res = "\"";
    auto lastQuot = literal.find_last_of('"');
    AD_CHECK(lastQuot != std::string_view::npos);
    auto langtagOrDatatype = literal.substr(lastQuot + 1);
    literal.remove_suffix(literal.size() - lastQuot);
    literal.remove_prefix(1);
    const string charactersToEscape = "\f\n\t\b\r\\\"\'";
    auto pos = literal.find_first_of(charactersToEscape);
    while (pos != literal.npos) {
      res.append(literal.begin(), literal.begin() + pos);
      switch (literal[pos]) {
        case '\t':
          res.append("\\t");
          break;
        case '\n':
          res.append("\\n");
          break;
        case '\r':
          res.append("\\r");
          break;
        case '\b':
          res.append("\\b");
          break;
        case '\f':
          res.append("\\f");
          break;
        case '"':
          res.append("\\\"");
          break;
        case '\'':
          res.append("\\\'");
          break;
        case '\\':
          res.append("\\\\");
          break;

        default:
          throw std::runtime_error(
              "Illegal switch value in escapeRDFLiteral. This should never "
              "happen, please report this");
      }
      literal.remove_prefix(pos + 1);
      pos = literal.find_first_of(charactersToEscape);
    }
    res.append(literal);
    res.push_back('"');
    res.append(langtagOrDatatype);
    return res;
  }

  using string = std::string;
  TurtleToken()
      // those constants are always skipped, so they don't need a group around
      // them
      : TurtlePrefix(grp("@prefix")),
        // TODO: this is actually case-insensitive
        SparqlPrefix(grp("PREFIX")),
        TurtleBase(grp("@base")),
        // TODO: this also
        SparqlBase(grp("BASE")),

        Dot(grp("\\.")),
        Comma(grp(",")),
        Semicolon(grp(";")),
        OpenSquared(grp("\\[")),
        CloseSquared(grp("\\]")),
        OpenRound(grp("\\(")),
        CloseRound(grp("\\)")),
        A(grp("a")),
        DoubleCircumflex(grp("\\^\\^")),

        True(grp("true")),
        False(grp("false")),
        Langtag(grp(LangtagString)),

        Integer(grp("[+-]?[0-9]+")),
        Decimal(grp("[+-]?[0-9]*\\.[0-9]+")),
        Exponent(grp(ExponentString)),
        Double(grp(DoubleString)),
        StringLiteralQuote(grp(StringLiteralQuoteString)),
        StringLiteralSingleQuote(grp(StringLiteralSingleQuoteString)),
        StringLiteralLongSingleQuote(grp(StringLiteralLongSingleQuoteString)),
        StringLiteralLongQuote(grp(StringLiteralLongQuoteString)),

        Iriref(grp(IrirefString)),
        PnameNS(grp(PnameNSString)),
        PnameLN(grp(PnameLNString)),
        BlankNodeLabel(grp(BlankNodeLabelString)),

        WsMultiple(grp(WsMultipleString)),
        Anon(grp(AnonString)),
        Comment(grp(CommentString)) {}

  TurtleToken(const TurtleToken& other) : TurtleToken() { (void)other; }
  TurtleToken& operator=([[maybe_unused]] const TurtleToken& other) {
    return *this;
  }

  const RE2 TurtlePrefix;
  const RE2 SparqlPrefix;
  const RE2 TurtleBase;
  const RE2 SparqlBase;

  const RE2 Dot;
  const RE2 Comma;
  const RE2 Semicolon;
  const RE2 OpenSquared;
  const RE2 CloseSquared;
  const RE2 OpenRound;
  const RE2 CloseRound;
  const RE2 A;
  const RE2 DoubleCircumflex;

  const RE2 True;
  const RE2 False;

  const string LangtagString = "@[a-zA-Z]+(\\-[a-zA-Z0-9]+)*";
  const RE2 Langtag;

  const RE2 Integer;
  const RE2 Decimal;
  const string ExponentString = "[eE][+-]?[0-9]+";
  const RE2 Exponent;
  const string DoubleString =
      "[+-]?([0-9]+\\.[0-9]*" + ExponentString + "|" + ExponentString + ")";
  const RE2 Double;

  const string HexString = "0-9A-Fa-f";
  const string UcharString = "\\\\u[0-9a-fA-f]{4}|\\\\U[0-9a-fA-f]{8}";

  const string EcharString = "\\\\[tbnrf\"\'\\\\]";

  const string StringLiteralQuoteString =
      "\"([^\\x22\\x5C\\x0A\\x0D]|" + EcharString + "|" + UcharString + ")*\"";
  const RE2 StringLiteralQuote;

  const string StringLiteralSingleQuoteString =
      "'([^\\x27\\x5C\\x0A\\x0D]|" + EcharString + "|" + UcharString + ")*'";
  const RE2 StringLiteralSingleQuote;

  const string StringLiteralLongSingleQuoteString =
      "'''((''|')?([^'\\\\]|" + EcharString + "|" + UcharString + "))*'''";
  const RE2 StringLiteralLongSingleQuote;

  const string StringLiteralLongQuoteString = "\"\"\"((\"\"|\")?([^\"\\\\]|" +
                                              EcharString + "|" + UcharString +
                                              "))*\"\"\"";
  const RE2 StringLiteralLongQuote;

  // TODO: fix this!
  const string IrirefString =
      "<([^\\x00-\\x20<>\"{}|^`\\\\]|"s + UcharString + ")*>";
  const RE2 Iriref;

  const string PercentString = "%" + cls(HexString) + "{2}";
  // const RE2 Percent;

  const string PnCharsBaseString =
      "A-Za-z\\x{00C0}-\\x{00D6}\\x{00D8}-\\x{00F6}\\x{00F8}-"
      "\\x{02FF}"
      "\\x{0370}-"
      "\\x{037D}\\x{037F}-\\x{1FFF}\\x{200C}-\\x{200D}\\x{2070}-\\x{"
      "218F}"
      "\\x{2C00}-"
      "\\x{2FEF}"
      "\\x{3001}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFFD}"
      "\\x{00010000}-"
      "\\x{000EFFFF}";

  const string PnCharsUString = PnCharsBaseString + "_";

  const string PnCharsString =
      PnCharsUString + "\\-0-9\\x{00B7}\\x{0300}-\\x{036F}\\x{203F}-\\x{2040}";

  /*
  const string PnPrefixString = grp(PnCharsBaseString) + "((" +
                                PnCharsString + "|\\.)*" + PnCharsString +
                                ")?";
                        */
  // TODO<joka921> verify that this is what is meant
  const string PnPrefixString = cls(PnCharsBaseString) + "(\\." +
                                cls(PnCharsString) + "|" + cls(PnCharsString) +
                                ")*";

  const string PnameNSString = PnPrefixString + ":";
  const RE2 PnameNS;

  const string PnLocalEscString = "\\\\[_~.\\-!$&'()*+,;=/?#@%]";

  const string PlxString = PercentString + "|" + PnLocalEscString;

  /*const string PnLocalString =
      "(" + cls(PnCharsUString + ":[0-9]" + PlxString) + ")((" +
      cls(PnCharsString + "\\.:" + PlxString) + ")*(" +
      cls(PnCharsString + ":" + PlxString) + "))?";
      */

  const string TmpNoDot = cls(PnCharsString + ":") + "|" + PlxString;
  const string PnLocalString =
      grp(cls(PnCharsUString + ":0-9") + "|" + PlxString) +
      grp("\\.*" + grp(TmpNoDot)) + "*";

  const string PnameLNString = grp(PnameNSString) + grp(PnLocalString);
  const RE2 PnameLN;

  const string BlankNodeLabelString = "_:" + cls(PnCharsUString + "0-9") +
                                      grp("\\.*" + cls(PnCharsString)) + "*";

  const RE2 BlankNodeLabel;

  const string WsSingleString = "\\x20\\x09\\x0D\\x0A";

  const string WsMultipleString = cls(WsSingleString) + "*";
  const RE2 WsMultiple;

  const string AnonString = "\\[" + WsMultipleString + "\\]";
  const RE2 Anon;

  const string CommentString = "#[^\\n]*\\n";
  const RE2 Comment;

  static string grp(const string& s) { return '(' + s + ')'; }
  static string cls(const string& s) { return '[' + s + ']'; }
};

/**
 * @brief Tokenizer that uses the Ctre library
 *
 * It has exactly the same public interface as the Tokenizer class from above,
 * so it can be used as a drop-in replacement. CAVEAT: Currently this does only
 * support prefixes that only contain ascii characters
 */
class TokenizerCtre {
  FRIEND_TEST(TokenizerTest, Compilation);

 public:
  /**
   * @brief Construct from a char ptr (the bytes to parse) and the number of
   * bytes that can be read/parsed from this ptr
   *
   * @param data the data from which we will parse. Only taken by reference
   * without ownership
   */
  explicit TokenizerCtre(std::string_view data) : _data(data) {}

  /// iterator to the next character that we have not yet consumed
  [[nodiscard]] auto begin() const { return _data.begin(); }

  /**
   * if a prefix of the input stream matches the regex argument,
   * return true and that prefix and move the input stream forward
   * by the length of the match. If no match is found,
   * false is returned and the input stream remains the same
   */
  template <TokId id>
  std::pair<bool, std::string_view> getNextToken() noexcept {
    auto res = apply<Matcher, id>();
    _data.remove_prefix(res.second.size());
    return res;
  }

  /**
   * @brief determines and matches the longest prefix match of the held _data
   * with one of the regexes.
   *
   * If such a match is found, the input stream is advanced by the longest match
   * prefix
   * @tparam fst the first of the candidate regexes
   * @tparam ids the following candidate regexes
   * @return
   *  - bool: True iff any match was found
   * - size_t: The index of the regex that was responsible for the longest match
   * - string_view: The prefix that forms the longest match
   */
  template <TokId fst, TokId... ids>
  std::tuple<bool, size_t, std::string_view> getNextTokenMultiple() {
    const char* beg = _data.begin();
    size_t dataSize = _data.size();

    auto innerResult = getNextTokenRecurse<0, fst, ids...>();

    // advance for the length of the longest match
    auto maxMatchSize = std::get<1>(innerResult);
    reset(beg + maxMatchSize, dataSize - maxMatchSize);
    return innerResult;
  }

  /// skip any whitespace or comments at the beginning of the held characters
  void skipWhitespaceAndComments() {
    skipWhitespace();
    skipComments();
    skipWhitespace();
  }

  /** If there is a prefix match with the argument, move forward the beginning
   * of _data and return true. Else return false.Can be used if we are not
   * interested in the actual value of the match
   */
  template <TokId id>
  bool skip() {
    return getNextToken<id>().first;
  }

  /// reinitialize with a new byte pointer and size pair
  void reset(const char* ptr, size_t sz) { _data = std::string_view(ptr, sz); }
  /// reinitialize with a string_view
  void reset(std::string_view s) { _data = s; }

  /// Access to input stream as std::string_view
  [[nodiscard]] std::string_view view() const { return _data; }
  /// Access to input stream as std::string_view
  [[nodiscard]] const std::string_view data() const { return _data; }
  /// remove the first n characters from our input stream (e.g. if they have
  /// been dealt with externally)
  void remove_prefix(size_t n) { _data.remove_prefix(n); }

 private:
  /// string_view of the characters we are parsing from.
  std::string_view _data;

  // ________________________________________________________________
  void skipWhitespace() {
    auto v = view();
    auto pos = v.find_first_not_of("\x20\x09\x0D\x0A");
    if (pos != string::npos) {
      _data.remove_prefix(pos);
    }
  }

  // ___________________________________________________________________________________
  void skipComments() {
    // if not successful, then there was no comment, but this does not matter to
    // us
    auto v = view();
    if (ad_utility::startsWith(v, "#")) {
      auto pos = v.find("\n");
      if (pos == string::npos) {
        // TODO: this actually should yield a n error
        LOG(INFO) << "Warning, unfinished comment found while parsing";
      } else {
        _data.remove_prefix(pos + 1);
      }
    }
  }
  FRIEND_TEST(TokenizerTest, WhitespaceAndComments);

  /*
   * Determine, which of the compile-timer regexes in the TurtleTokenCtre class
   * belongs to the specified TokId Then call the static templated process
   * Function of the class template f, so F::template process<TheRegex>(_data)
   */
  template <class F, TokId id>
  constexpr auto apply() {
    if constexpr (id == TokId::Langtag) {
      return F::template process<TurtleTokenCtre::Langtag>(_data);
    } else if constexpr (id == TokId::Double) {
      return F::template process<TurtleTokenCtre::Double>(_data);
    } else if constexpr (id == TokId::Iriref) {
      return F::template process<TurtleTokenCtre::Iriref>(_data);
    } else if constexpr (id == TokId::PnameNS) {
      return F::template process<TurtleTokenCtre::PnameNS>(_data);
    } else if constexpr (id == TokId::PnameLN) {
      return F::template process<TurtleTokenCtre::PnameLN>(_data);
    } else if constexpr (id == TokId::BlankNodeLabel) {
      return F::template process<TurtleTokenCtre::BlankNodeLabel>(_data);
    } else {
      switch (id) {
        case TokId::TurtlePrefix:
          return F::template process<TurtleTokenCtre::TurtlePrefix>(_data);
        case TokId::SparqlPrefix:
          return F::template process<TurtleTokenCtre::SparqlPrefix>(_data);
        case TokId::TurtleBase:
          return F::template process<TurtleTokenCtre::TurtleBase>(_data);
        case TokId::SparqlBase:
          return F::template process<TurtleTokenCtre::SparqlBase>(_data);
        case TokId::Dot:
          return F::template process<TurtleTokenCtre::Dot>(_data);
        case TokId::Comma:
          return F::template process<TurtleTokenCtre::Comma>(_data);
        case TokId::Semicolon:
          return F::template process<TurtleTokenCtre::Semicolon>(_data);
        case TokId::OpenSquared:
          return F::template process<TurtleTokenCtre::OpenSquared>(_data);
        case TokId::CloseSquared:
          return F::template process<TurtleTokenCtre::CloseSquared>(_data);

        case TokId::OpenRound:
          return F::template process<TurtleTokenCtre::OpenRound>(_data);
        case TokId::CloseRound:
          return F::template process<TurtleTokenCtre::CloseRound>(_data);
        case TokId::A:
          return F::template process<TurtleTokenCtre::A>(_data);
        case TokId::DoubleCircumflex:
          return F::template process<TurtleTokenCtre::DoubleCircumflex>(_data);
        case TokId::True:
          return F::template process<TurtleTokenCtre::True>(_data);
        case TokId::False:
          return F::template process<TurtleTokenCtre::False>(_data);

        case TokId::Integer:
          return F::template process<TurtleTokenCtre::Integer>(_data);
        case TokId::Decimal:
          return F::template process<TurtleTokenCtre::Decimal>(_data);
        case TokId::Exponent:
          return F::template process<TurtleTokenCtre::Exponent>(_data);
        case TokId::WsMultiple:
          return F::template process<TurtleTokenCtre::WsMultiple>(_data);
        case TokId::Anon:
          return F::template process<TurtleTokenCtre::Anon>(_data);
        case TokId::Comment:
          return F::template process<TurtleTokenCtre::Comment>(_data);
      }
    }
  }

  // base case for the recursion on longest match for multiple regexes. No regex
  // left, so there's no match
  template <size_t idx>
  std::tuple<bool, size_t, std::string_view> getNextTokenRecurse() {
    return std::tuple(false, idx, "");
  }

  /*
   * Recursion for the longest match of multiple regexes.
   * Returns the idx of the longest match and the longestMatch.
   * Does not change the internal data stream, this has to be done by the
   * external call
   *
   * @tparam idx the idx to start with (has to be 0 when called externally)
   * @tparam fst The first regex to check
   * @tparam ids The other regexes to check
   * @return <anyMatchFound?, idxOfLongestMatch, contentOfLongestMatch>
   */
  template <size_t idx, TokId fst, TokId... ids>
  std::tuple<bool, size_t, std::string_view> getNextTokenRecurse() {
    const auto [success, unusedIdx, content] =
        getNextTokenRecurse<idx + 1, ids...>();
    const char* beg = _data.begin();
    size_t dataSize = _data.size();
    const auto [currentSuccess, res] = getNextToken<fst>();
    reset(beg, dataSize);
    bool curBetter = currentSuccess && res.size() > content.size();

    return std::tuple(success || currentSuccess, curBetter ? idx : unusedIdx,
                      curBetter ? res : content);
  }

  /*
   * The helper struct used for the intenal apply function
   * Its static function process<regex>(string_view)
   * tries to match a prefix of the string_view with the regex and returns
   * <true, matchContent> on sucess and <false, emptyStringView> on failure
   */
  struct Matcher {
    template <auto& regex>
    static std::pair<bool, std::string_view> process(
        std::string_view data) noexcept {
      static constexpr auto ext = grp(regex) + ".*";
      if (auto m = ctre::match<ext>(data)) {
        return {true, m.template get<1>().to_view()};
      } else {
        return {false, std::string_view()};
      }
    }
  };
};

// The currently used hand-written tokenizer
// for this for correctness and efficiency
class Tokenizer {
  FRIEND_TEST(TokenizerTest, Compilation);

 public:
  // Construct from a std::string_view;
  Tokenizer(std::string_view input)
      : _tokens(), _data(input.data(), input.size()) {}

  // if a prefix of the input stream matches the regex argument,
  // return true and that prefix and move the input stream forward
  // by the length of the match. If no match is found,
  // false is returned and the input stream remains the same
  std::pair<bool, std::string> getNextToken(const RE2& reg);
  template <TokId id>
  std::pair<bool, std::string> getNextToken() {
    return getNextToken(idToRegex(id));
  }

  // overload that takes multiple regexes
  // determines the longest match of the input stream prefix
  // with one of the regexes. If such a match is found,
  // The input stream is advanced by the longest match prefix
  // Return value:
  //  - bool: True iff any match was found
  // - size_t: The index of the regex that was responsible for the longest match
  // - string: The prefix that forms the longest match
  std::tuple<bool, size_t, std::string> getNextToken(
      const std::vector<const RE2*>& regs);

  template <TokId fst, TokId... ids>
  std::tuple<bool, size_t, std::string_view> getNextTokenMultiple() {
    const char* beg = _data.begin();
    size_t dataSize = _data.size();

    auto innerResult = getNextTokenRecurse<0, fst, ids...>();

    // advance for the length of the longest match
    auto maxMatchSize = std::get<1>(innerResult);
    reset(beg + maxMatchSize, dataSize - maxMatchSize);
    return innerResult;
  }

  template <size_t idx>
  std::tuple<bool, size_t, std::string_view> getNextTokenRecurse() {
    return std::tuple(false, idx, "");
  }

  template <size_t idx, TokId fst, TokId... ids>
  std::tuple<bool, size_t, std::string_view> getNextTokenRecurse() {
    // TODO<joka921> : write unit tests for this Overload!!
    const auto [success, unusedIdx, content] =
        getNextTokenRecurse<idx + 1, ids...>();
    const char* beg = _data.begin();
    size_t dataSize = _data.size();
    const auto [currentSuccess, res] = getNextToken<fst>();
    reset(beg, dataSize);
    bool curBetter = currentSuccess && res.size() > content.size();

    return std::tuple(success || currentSuccess, curBetter ? idx : unusedIdx,
                      curBetter ? res : content);
  }

  // _______________________________________________________________________________
  void skipWhitespaceAndComments();

  // If there is a prefix match with the argument, move forward the input stream
  // and return true. Can be used if we are not interested in the actual value
  // of the match
  bool skip(const RE2& reg) { return RE2::Consume(&_data, reg); }
  template <TokId id>
  bool skip() {
    return skip(idToRegex(id));
  }

  // reinitialize with a new byte pointer and size pair
  void reset(const char* ptr, size_t size) {
    _data = re2::StringPiece(ptr, size);
  }

  // Access to the input stream as a  StringPiece
  const re2::StringPiece& data() const { return _data; }
  re2::StringPiece& data() { return _data; }

  // Access to input stream as std::string_view
  std::string_view view() const { return {_data.data(), _data.size()}; }

  auto begin() const { return _data.begin(); }
  void remove_prefix(size_t n) { _data.remove_prefix(n); }

  // holds all the regexes needed for Tokenization
  TurtleToken _tokens;

 private:
  // convert the (external) TokId to the internally used google-regex
  const RE2& idToRegex(const TokId reg);

  // ________________________________________________________________
  void skipWhitespace() {
    auto v = view();
    auto pos = v.find_first_not_of("\x20\x09\x0D\x0A");
    if (pos != string::npos) {
      _data.remove_prefix(pos);
    }
    // auto success = skip(_tokens.WsMultiple);
    // assert(success);
    return;
  }

  // ___________________________________________________________________________________
  void skipComments() {
    // if not successful, then there was no comment, but this does not matter to
    // us
    auto v = view();
    if (ad_utility::startsWith(v, "#")) {
      auto pos = v.find("\n");
      if (pos == string::npos) {
        // TODO: this actually should yield a n error
        LOG(INFO) << "Warning, unfinished comment found while parsing";
      } else {
        _data.remove_prefix(pos + 1);
      }
    }
  }
  FRIEND_TEST(TokenizerTest, WhitespaceAndComments);
  re2::StringPiece _data;
};
