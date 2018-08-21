// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#pragma once

#include <gtest/gtest.h>
#include <re2/re2.h>
#include <regex>

using re2::RE2;
using namespace std::string_literals;

struct TurtleToken {
  using string = std::string;
  TurtleToken()
      // those constants are always skipped, so they don't need a group around
      // them
      : TurtlePrefix(u8"@prefix"),
        SparqlPrefix(u8"PREFIX"),
        TurtleBase(u8"@base"),
        SparqlBase(u8"BASE"),

        Dot(u8"\\."),
        Comma(u8","),
        Semicolon(u8";"),
        OpenSquared(u8"\\["),
        CloseSquared(u8"\\]"),
        OpenRound(u8"\\("),
        CloseRound(u8"\\)"),
        A(u8"a"),
        DoubleCircumflex(u8"\\^\\^"),

        True(u8"true"),
        False(u8"false"),
        Langtag(LangtagString),

        Integer(u8"[+-]?[0-9]+"),
        Decimal(u8"[+-]?[0-9]*\\.[0-9]+"),
        Exponent(ExponentString),
        Double(DoubleString),
        StringLiteralQuote(StringLiteralQuoteString),
        StringLiteralSingleQuote(StringLiteralSingleQuoteString),
        StringLiteralLongSingleQuote(StringLiteralLongSingleQuoteString),
        StringLiteralLongQuote(StringLiteralLongQuoteString),

        Iriref(IrirefString),
        PnameNS(PnameNSString),
        PnameLN(PnameLNString),
        BlankNodeLabel(BlankNodeLabelString),

        WsMultiple(WsMultipleString),
        Anon(AnonString),
        Comment(CommentString) {}

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

  const string HexString = u8"[0-9]|[A-F]|[a-f]";
  // const RE2 Hex;

  // TODO: check precedence of "|"
  const string UcharString = u8"(\\\\u" + HexString + HexString + HexString +
                             HexString + u8")|(\\\\U" + HexString + HexString +
                             HexString + HexString + HexString + HexString +
                             HexString + HexString + u8")";
  // const RE2 Uchar;

  // const string EcharString = u8"\\\\[tbnrf\"'\\]";
  const string EcharString = u8"\\\\[tbnrf\"\'\\\\]";

  // const RE2 Echar;

  const string StringLiteralQuoteString = u8"\"([^\x22\x5C\x0A\x0D]|" +
                                          EcharString + u8"|" + UcharString +
                                          u8")*\"";
  const RE2 StringLiteralQuote;

  const string StringLiteralSingleQuoteString =
      u8"'([^\x22\x5C\x0A\x0D]|" + EcharString + u8"|" + UcharString + u8")*'";
  const RE2 StringLiteralSingleQuote;

  const string StringLiteralLongSingleQuoteString =
      u8"'''(('|'')?([^'\\]|" + EcharString + u8"|" + UcharString + u8"))*'''";
  const RE2 StringLiteralLongSingleQuote;

  const string StringLiteralLongQuoteString = u8"\"\"\"((\"|\"\")?([^\"\\]|" +
                                              EcharString + u8"|" +
                                              UcharString + u8"))*\"\"\"";
  const RE2 StringLiteralLongQuote;

  // TODO: fix this!
  const string IrirefString = "dummy";
  //  "\\<([^\\x00-\\x20<>\"{}|^`\\]|"s + UcharString + u8")*\\>";
  const RE2 Iriref;

  const string PercentString = u8"%" + HexString + HexString;
  // const RE2 Percent;

  const string PnCharsBaseString =
      u8"[A-Z]|[a-z]|[\u00C0-\u00D6]|[\u00D8-\u00F6]|[\u00F8-\u02FF]|[\u0370-"
      u8"\u037D]|[\u037F-\u1FFF]|[\u200C-\u200D]|[\u2070-\u218F]|[\u2C00-"
      u8"\u2FEF]"
      u8"|[\u3001-\uD7FF]|[\uF900-\uFDCF]|[\uFDF0-\uFFFD]|[\U00010000-"
      u8"\U000EFFFF]";

  const string PnCharsUString = PnCharsBaseString + u8"|_";

  const string PnCharsString =
      PnCharsUString + u8"|-|[0-9]|\u00B7|[\u0300-\u036F]|[\u203F-\u2040]";

  const string PnPrefixString = PnCharsBaseString + u8"((" + PnCharsString +
                                u8"|\\.)*" + PnCharsString + u8")?";

  const string PnameNSString = PnPrefixString + u8"\\:";
  const RE2 PnameNS;

  const string PnLocalEscString = u8"";  // = u8"\\\\[_~.\\-!$&'()*+,;=/?#@%";

  const string PlxString = PercentString + u8"|" + PnLocalEscString;

  const string PnLocalString = u8"(" + PnCharsUString + u8"|:|[0-9]|" +
                               PlxString + u8")((" + PnCharsString +
                               u8"|\\.|:|" + PlxString + u8")*(" +
                               PnCharsString + u8"|:|" + PlxString + u8"))?";

  const string PnameLNString = PnameNSString + PnLocalString;
  const RE2 PnameLN;

  const string BlankNodeLabelString = u8"_\\:(" + PnCharsUString +
                                      u8"|[0-9])((" + PnCharsString +
                                      u8"|\\.)*" + PnCharsString + u8")?";
  const RE2 BlankNodeLabel;

  const string WsSingleString = u8"\x20|\x09|\x0D|\x0A";

  const string WsMultipleString = u8"(" + WsSingleString + u8")*";
  const RE2 WsMultiple;

  const string AnonString = u8"\\[" + WsMultipleString + u8"\\]";
  const RE2 Anon;

  const string CommentString = u8"#[^\\n]*\\n";
  const RE2 Comment;
};

class Tokenizer {
  FRIEND_TEST(TokenizerTest, Compilation);

 public:
  Tokenizer(const char* ptr, size_t size) : _tokens(), _data(ptr, size) {}

  std::pair<bool, std::string> getNextToken(const RE2& reg);
  std::tuple<bool, size_t, std::string> getNextToken(
      const std::vector<const RE2*>& regs);

  void skipWhitespaceAndComments();
  bool skip(const RE2& reg) { return RE2::Consume(&_data, reg); }

  const TurtleToken _tokens;

  void reset(const char* ptr, size_t size) {
    _data = re2::StringPiece(ptr, size);
  }

  const re2::StringPiece& data() const { return _data; }

 private:
  void skipWhitespace() {
    auto success = skip(_tokens.WsMultiple);
    assert(success);
    return;
  }
  void skipComments() {
    // if not successful, then there was no comment, but this does not matter to
    // us
    skip(_tokens.Comment);
    return;
  }
  FRIEND_TEST(TokenizerTest, WhitespaceAndComments);
  re2::StringPiece _data;
};

// ______________________________________________________
void Tokenizer::skipWhitespaceAndComments() {
  skipWhitespace();
  skipComments();
  skipWhitespace();
}

// _______________________________________________________
std::pair<bool, std::string> Tokenizer::getNextToken(const RE2& reg) {
  std::string match = "";
  bool success = RE2::Consume(&_data, reg, &match);
  if (!success) {
    return {false, {}};
  } else {
    return {true, match};
  }
}

// _______________________________________________________
std::tuple<bool, size_t, std::string> Tokenizer::getNextToken(
    const std::vector<const RE2*>& regs) {
  size_t maxMatchSize = 0;
  size_t maxMatchIndex = 0;
  std::string maxMatch;
  bool success = false;
  for (size_t i = 0; i < regs.size(); i++) {
    auto [tmpSuccess, tmpMatch] = getNextToken(*(regs[i]));
    if (tmpSuccess && tmpMatch.size() > maxMatchSize) {
      success = true;
      maxMatchSize = tmpMatch.size();
      maxMatchIndex = i;
      maxMatch = std::move(tmpMatch);
    }
  }
  return {success, maxMatchIndex, maxMatch};
}

