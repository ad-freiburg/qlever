// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#pragma once

#include <gtest/gtest.h>
#include <re2/re2.h>
#include <regex>

using re2::RE2;

struct TurtleToken {
  using regex = std::regex;
  using wregex = std::wregex;
  using string = std::string;
  using wstring = std::wstring;
  TurtleToken()
      : TurtlePrefix(L"@prefix"),
        SparqlPrefix(L"PREFIX"),
        TurtleBase(L"@base"),
        SparqlBase(L"BASE"),
        testRegex("bla[xy]*"),

        Dot(L"\\."),
        Comma(L","),
        Semicolon(L";"),
        OpenSquared(L"\\["),
        CloseSquared(L"\\]"),
        OpenRound(L"\\("),
        CloseRound(L"\\)"),
        A(L"a"),
        DoubleCircumflex(L"\\^\\^"),

        True(L"true"),
        False(L"false"),
        Langtag(LangtagString),

        Integer(L"[+-]?[0-9]+"),
        Decimal(L"[+-]?[0-9]*\\.[0-9]+"),
        Exponent(ExponentString),
        Double(DoubleString),
        Hex(HexString),
        Uchar(UcharString),
        Echar(EcharString),
        StringLiteralQuote(StringLiteralQuoteString),
        /*
        StringLiteralSingleQuote(StringLiteralSingleQuoteString),
        StringLiteralLongSingleQuote(StringLiteralLongSingleQuoteString),
        StringLiteralLongQuote(StringLiteralLongQuoteString),
        */

        PnCharsBase(PnCharsBaseString),
        WsMultiple(WsMultipleString),
        Anon(AnonString),
        Comment(CommentString) {}

  const RE2 testRegex;
  const wregex TurtlePrefix;
  const wregex SparqlPrefix;
  const wregex TurtleBase;
  const wregex SparqlBase;

  const wregex Dot;
  const wregex Comma;
  const wregex Semicolon;
  const wregex OpenSquared;
  const wregex CloseSquared;
  const wregex OpenRound;
  const wregex CloseRound;
  const wregex A;
  const wregex DoubleCircumflex;

  const wregex True;
  const wregex False;

  const wstring LangtagString = L"@[a-zA-Z]+(\\-[a-zA-Z0-9]+)*";
  const wregex Langtag;

  const wregex Integer;
  const wregex Decimal;
  const wstring ExponentString = L"[eE][+-]?[0-9]+";
  const wregex Exponent;
  const wstring DoubleString =
      L"[+-]?([0-9]+\\.[0-9]*" + ExponentString + L"|" + ExponentString + L")";
  const wregex Double;

  const wstring HexString = L"([0-9] | [A-F] | [a-f])";
  const wregex Hex;

  // TODO: check precedence of "|"
  const wstring UcharString = L"(\\\\u" + HexString + HexString + HexString +
                              HexString + L")|(\\\\U" + HexString + HexString +
                              HexString + HexString + HexString + HexString +
                              HexString + HexString + L")";
  const wregex Uchar;

  // const wstring EcharString = L"\\\\[tbnrf\"'\\]";
  const wstring EcharString = L"\\\\[tbnrf\"\'\\\\]";

  const wregex Echar;

  const wstring StringLiteralQuoteString =
      L"\"([^\x22\x5C\x0A\x0D]|" + EcharString + L"|" + UcharString + L")*\"";
  const wregex StringLiteralQuote;

  const wstring StringLiteralSingleQuoteString =
      L"'([^\x22\x5C\x0A\x0D]|" + EcharString + L"|" + UcharString + L")*'";
  const wregex StringLiteralSingleQuote;

  const wstring StringLiteralLongSingleQuoteString =
      L"'''(('|'')?([^'\\]|" + EcharString + L"|" + UcharString + L"))*'''";
  const wregex StringLiteralLongSingleQuote;

  const wstring StringLiteralLongQuoteString = L"\"\"\"((\"|\"\")?([^\"\\]|" +
                                               EcharString + L"|" +
                                               UcharString + L"))*\"\"\"";
  const wregex StringLiteralLongQuote;

  const wstring IrirefString =
      L"\\<([^\x00-\x20<>\"{}|^`\\]|" + UcharString + L")*\\>";
  const wregex Iriref;

  const wstring PercentString = L"%" + HexString + HexString;
  const wregex Percent;

  const wstring PnCharsBaseString =
      L"[A-Z]|[a-z]|[\u00C0-\u00D6]|[\u00D8-\u00F6]|[\u00F8-\u02FF]|[\u0370-"
      L"\u037D]|[\u037F-\u1FFF]|[\u200C-\u200D]|[\u2070-\u218F]|[\u2C00-\u2FEF]"
      L"|[\u3001-\uD7FF]|[\uF900-\uFDCF]|[\uFDF0-\uFFFD]|[\U00010000-"
      L"\U000EFFFF]";
  const wregex PnCharsBase;

  const wstring PnCharsUString = PnCharsBaseString + L"|_";
  const wregex PnCharsU;

  const wstring PnCharsString =
      PnCharsUString + L"|-|[0-9]|\u00B7|[\u0300-\u036F]|[\u203F-\u2040]";
  const wregex PnChars;

  const wstring PnPrefixString = PnCharsBaseString + L"((" + PnCharsString +
                                 L"|\\.)*" + PnCharsString + L")?";
  const wregex PnPrefix;

  const wstring PnameNSString = PnPrefixString + L"\\:";
  const wregex PnameNS;

  const wstring PnLocalEscString = L"";  // = L"\\\\[_~.\\-!$&'()*+,;=/?#@%";
  const wregex PnLocalEsc;

  const wstring PlxString = PercentString + L"|" + PnLocalEscString;
  const wregex Plx;

  const wstring PnLocalString = L"(" + PnCharsUString + L"|:|[0-9]|" +
                                PlxString + L")((" + PnCharsString +
                                L"|\\.|:|" + PlxString + L")*(" +
                                PnCharsString + L"|:|" + PlxString + L"))?";
  const wregex PnLocal;

  const wstring PnameLNString = PnameNSString + PnLocalString;
  const wregex PnameLN;

  const wstring BlankNodeLabelString = L"_\\:(" + PnCharsUString +
                                       L"|[0-9])((" + PnCharsString +
                                       L"|\\.)*" + PnCharsString + L")?";
  const wregex BlankNodeLabel;

  const wstring WsSingleString = L"\x20|\x09|\x0D|\x0A";
  const wregex WsSingle;

  const wstring WsMultipleString = L"(" + WsSingleString + L")*";
  const wregex WsMultiple;

  const wstring AnonString = L"\\[" + WsMultipleString + L"\\]";
  const wregex Anon;

  const wstring CommentString = L"#[^\n]*\n";
  const wregex Comment;
};

template <class WstringIt>
class Tokenizer {
  FRIEND_TEST(TokenizerTest, Compilation);

 public:
  Tokenizer(WstringIt first, WstringIt last)
      : _tokens(), _begin(first), _current(first), _end(last) {}

  std::pair<bool, std::wstring> getNextToken(const std::wregex& reg);
  std::tuple<bool, size_t, std::wstring> getNextToken(
      const std::vector<const std::wregex*>& regs);

  void skipWhitespaceAndComments();

  const TurtleToken _tokens;

  void reset(WstringIt first, WstringIt last) {
    _begin = first;
    _current = first;
    _end = last;
  }

 private:
  void skipWhitespace() {
    auto [success, ws] = getNextToken(_tokens.WsMultiple);
    assert(success);
    _current += ws.size();
    return;
  }
  void skipComments() {
    // if not successful, then there was no comment, but this does not matter to
    // us
    auto [success, ws] = getNextToken(_tokens.Comment);
    (void)success;
    _current += ws.size();
    return;
  }
  WstringIt _begin;
  WstringIt _current;
  WstringIt _end;
};

// ______________________________________________________
template <class WstringIt>
void Tokenizer<WstringIt>::skipWhitespaceAndComments() {
  skipWhitespace();
  skipComments();
  skipWhitespace();
}

// _______________________________________________________
template <class WstringIt>
std::pair<bool, std::wstring> Tokenizer<WstringIt>::getNextToken(
    const std::wregex& reg) {
  std::wsmatch match;
  // TODO(joka921): does this enfore beginning at the first character as i
  // suppose it does
  bool success = std::regex_search(_current, _end, match, reg,
                                   std::regex_constants::match_continuous);
  if (!success) {
    return {false, {}};
  } else {
    assert(match.position() == 0);
    return {true, match.str()};
  }
}

// _______________________________________________________
template <class WstringIt>
std::tuple<bool, size_t, std::wstring> Tokenizer<WstringIt>::getNextToken(
    const std::vector<const std::wregex*>& regs) {
  size_t maxMatchSize = 0;
  size_t maxMatchIndex = 0;
  std::wstring maxMatch;
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
