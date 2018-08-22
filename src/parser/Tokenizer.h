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
      : TurtlePrefix(grp(u8"@prefix")),
        SparqlPrefix(grp(u8"PREFIX")),
        TurtleBase(grp(u8"@base")),
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

  /*
  const string BlankNodeLabelString = u8"_:(" + PnCharsUString +
                                      u8"|[0-9])((" + PnCharsString +
                                      u8"|\\.)*" + PnCharsString + u8")?";
                                      */
  const string BlankNodeLabelString = u8"_:" + cls(PnCharsUString + u8"0-9") +
                                      grp("\\.*" + cls(PnCharsString)) + "*";

  const RE2 BlankNodeLabel;

  const string WsSingleString = u8"\x20|\x09|\x0D|\x0A";

  const string WsMultipleString = u8"(" + WsSingleString + u8")*";
  const RE2 WsMultiple;

  const string AnonString = u8"\\[" + WsMultipleString + u8"\\]";
  const RE2 Anon;

  const string CommentString = u8"#[^\\n]*\\n";
  const RE2 Comment;

  static string grp(const string& s) { return '(' + s + ')'; }
  static string cls(const string& s) { return '[' + s + ']'; }
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

