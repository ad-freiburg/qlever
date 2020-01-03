// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#pragma once

#include <gtest/gtest.h>
#include <re2/re2.h>
#include <regex>
#include "../util/Log.h"
#include "../util/Exception.h"

using re2::RE2;
using namespace std::string_literals;

// holds all the google re2 regexes that correspond to all terminals in the
// turtle grammar cannot be static since google regexes have to be constructed
// at runtime
struct TurtleToken {
  /**
   * @brief convert a RDF Literal to a unified form that is used inside QLever
   *
   * RDFLiterals in Turtle or Sparql can have several forms: Either starting with one (" or ') quotation mark
   * and containing escape sequences like "\\\t" or with three (""" or ''') quotation marks and allowing most
   * control sequences to be contained in the string directly.
   *
   * This function converts any of this forms to a literal that starts and ends with a single quotation mark '"'
   * and contains the originally escaped characters directly, e.g. "al\"pha" becomes "al"pha".
   *
   * This is NOT a valid RDF form of literals, but this format is only used inside QLever. By stripping the leading and trailing quotation mark and possible
   * langtags or datatype URIS one can directly obtain the actual content of the literal.
   *
   * @param literal
   * @return
   */
  static std::string normalizeRDFLiteral(std::string_view literal) {
    std::string res = "\"";
    auto lastQuot = literal.find_last_of("\"\'");
    auto langtagOrDatatype = literal.substr(lastQuot + 1);
    literal.remove_suffix(literal.size() - lastQuot - 1);
    if (ad_utility::startsWith(literal, "\"\"\"") || ad_utility::startsWith(literal, "'''")) {
      AD_CHECK(ad_utility::endsWith(literal, literal.substr(0, 3)));
      literal.remove_prefix(3);
      literal.remove_suffix(3);
    } else {
      AD_CHECK(ad_utility::startsWith(literal, "\"") || ad_utility::startsWith(literal, "'"));
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
          throw std::runtime_error("Illegal escape sequence in RDF Literal. This should never happen, please report this");
      }
      literal.remove_prefix(pos + 2);
      pos = literal.find('\\');
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
      : TurtlePrefix(grp(u8"@prefix")),
        // TODO: this is actually case-insensitive
        SparqlPrefix(grp(u8"PREFIX")),
        TurtleBase(grp(u8"@base")),
        // TODO: this also
        SparqlBase(grp(u8"BASE")),

        Dot(grp(u8"\\.")),
        Comma(grp(u8",")),
        Semicolon(grp(u8";")),
        OpenSquared(grp(u8"\\[")),
        CloseSquared(grp(u8"\\]")),
        OpenRound(grp(u8"\\(")),
        CloseRound(grp(u8"\\)")),
        A(grp(u8"a")),
        DoubleCircumflex(grp(u8"\\^\\^")),

        True(grp(u8"true")),
        False(grp(u8"false")),
        Langtag(grp(LangtagString)),

        Integer(grp(u8"[+-]?[0-9]+")),
        Decimal(grp(u8"[+-]?[0-9]*\\.[0-9]+")),
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

  const string LangtagString = u8"@[a-zA-Z]+(\\-[a-zA-Z0-9]+)*";
  const RE2 Langtag;

  const RE2 Integer;
  const RE2 Decimal;
  const string ExponentString = u8"[eE][+-]?[0-9]+";
  const RE2 Exponent;
  const string DoubleString = u8"[+-]?([0-9]+\\.[0-9]*" + ExponentString +
                              u8"|" + ExponentString + u8")";
  const RE2 Double;

  const string HexString = u8"0-9A-Fa-f";
  const string UcharString = u8"\\\\u[0-9a-fA-f]{4}|\\\\U[0-9a-fA-f]{8}";

  const string EcharString = u8"\\\\[tbnrf\"\'\\\\]";

  const string StringLiteralQuoteString = u8"\"([^\\x22\\x5C\\x0A\\x0D]|" +
                                          EcharString + u8"|" + UcharString +
                                          u8")*\"";
  const RE2 StringLiteralQuote;

  const string StringLiteralSingleQuoteString = u8"'([^\\x27\\x5C\\x0A\\x0D]|" +
                                                EcharString + u8"|" +
                                                UcharString + u8")*'";
  const RE2 StringLiteralSingleQuote;

  const string StringLiteralLongSingleQuoteString = u8"'''((''|')?([^'\\\\]|" +
                                                    EcharString + u8"|" +
                                                    UcharString + u8"))*'''";
  const RE2 StringLiteralLongSingleQuote;

  const string StringLiteralLongQuoteString = u8"\"\"\"((\"\"|\")?([^\"\\\\]|" +
                                              EcharString + u8"|" +
                                              UcharString + u8"))*\"\"\"";
  const RE2 StringLiteralLongQuote;

  // TODO: fix this!
  const string IrirefString =
      "<([^\\x00-\\x20<>\"{}|^`\\\\]|"s + UcharString + u8")*>";
  const RE2 Iriref;

  const string PercentString = u8"%" + cls(HexString) + "{2}";
  // const RE2 Percent;

  const string PnCharsBaseString =
      u8"A-Za-z\\x{00C0}-\\x{00D6}\\x{00D8}-\\x{00F6}\\x{00F8}-"
      u8"\\x{02FF}"
      u8"\\x{0370}-"
      u8"\\x{037D}\\x{037F}-\\x{1FFF}\\x{200C}-\\x{200D}\\x{2070}-\\x{"
      u8"218F}"
      u8"\\x{2C00}-"
      u8"\\x{2FEF}"
      u8"\\x{3001}-\\x{D7FF}\\x{F900}-\\x{FDCF}\\x{FDF0}-\\x{FFFD}"
      u8"\\x{00010000}-"
      u8"\\x{000EFFFF}";

  const string PnCharsUString = PnCharsBaseString + u8"_";

  const string PnCharsString =
      PnCharsUString +
      u8"\\-0-9\\x{00B7}\\x{0300}-\\x{036F}\\x{203F}-\\x{2040}";

  /*
  const string PnPrefixString = grp(PnCharsBaseString) + u8"((" +
                                PnCharsString + u8"|\\.)*" + PnCharsString +
                                u8")?";
                        */
  // TODO<joka921> verify that this is what is meant
  const string PnPrefixString = cls(PnCharsBaseString) + u8"(\\." +
                                cls(PnCharsString) + u8"|" +
                                cls(PnCharsString) + ")*";

  const string PnameNSString = PnPrefixString + u8":";
  const RE2 PnameNS;

  const string PnLocalEscString = u8"\\\\[_~.\\-!$&'()*+,;=/?#@%]";

  const string PlxString = PercentString + u8"|" + PnLocalEscString;

  /*const string PnLocalString =
      u8"(" + cls(PnCharsUString + u8":[0-9]" + PlxString) + u8")((" +
      cls(PnCharsString + u8"\\.:" + PlxString) + u8")*(" +
      cls(PnCharsString + u8":" + PlxString) + u8"))?";
      */

  const string TmpNoDot = cls(PnCharsString + ":") + "|" + PlxString;
  const string PnLocalString =
      grp(cls(PnCharsUString + u8":0-9") + "|" + PlxString) +
      grp(u8"\\.*" + grp(TmpNoDot)) + "*";

  const string PnameLNString = grp(PnameNSString) + grp(PnLocalString);
  const RE2 PnameLN;

  const string BlankNodeLabelString = u8"_:" + cls(PnCharsUString + u8"0-9") +
                                      grp("\\.*" + cls(PnCharsString)) + "*";

  const RE2 BlankNodeLabel;

  const string WsSingleString = u8"\\x20\\x09\\x0D\\x0A";

  const string WsMultipleString = cls(WsSingleString) + u8"*";
  const RE2 WsMultiple;

  const string AnonString = u8"\\[" + WsMultipleString + u8"\\]";
  const RE2 Anon;

  const string CommentString = u8"#[^\\n]*\\n";
  const RE2 Comment;

  static string grp(const string& s) { return '(' + s + ')'; }
  static string cls(const string& s) { return '[' + s + ']'; }
};

// The currently used hand-written tokenizer
// TODO<joka921>: Use a automatically generated lexer (e.g. from flex)
// for this for correctness and efficiency
class Tokenizer {
  FRIEND_TEST(TokenizerTest, Compilation);

 public:
  // Construct from a char ptr (the bytes to parse) and the number of bytes that
  // can be read/parsed from this ptr
  Tokenizer(const char* ptr, size_t size) : _tokens(), _data(ptr, size) {}

  // if a prefix of the input stream matches the regex argument,
  // return true and that prefix and move the input stream forward
  // by the length of the match. If no match is found,
  // false is returned and the input stream remains the same
  std::pair<bool, std::string> getNextToken(const RE2& reg);

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

  // _______________________________________________________________________________
  void skipWhitespaceAndComments();

  // If there is a prefix match with the argument, move forward the input stream
  // and return true. Can be used if we are not interested in the actual value
  // of the match
  bool skip(const RE2& reg) { return RE2::Consume(&_data, reg); }

  // reinitialize with a new byte pointer and size pair
  void reset(const char* ptr, size_t size) {
    _data = re2::StringPiece(ptr, size);
  }

  // Access to the input stream as a  StringPiece
  const re2::StringPiece& data() const { return _data; }
  re2::StringPiece& data() { return _data; }

  // Access to input stream as std::string_view
  std::string_view view() const { return {_data.data(), _data.size()}; }

  // holds all the regexes needed for Tokenization
  const TurtleToken _tokens;

 private:
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
