// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#pragma once

#include <regex>

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
        Dot(L"\\."),
        Comma(L","),
        Semicolon(L";"),
        OpenSquared(L"\\["),
        CloseSquared(L"\\]"),
        OpenRound(L"\\("),
        CloseRound(L"\\)"),

        True(L"true"),
        False(L"false"),

        Integer(L"[+-]?[0-9]+"),
        Decimal(L"[+-]?[0-9]*\\.[0-9]+"),
	PnCharsBase(PnCharsBaseString)
  {}

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

  const wregex True;
  const wregex False;

  const wregex Integer;
  const wregex Decimal;
  const wstring ExponentString = L"[eE][+-]?[0-9]+";
  const wregex Exponent;
  const wstring DoubleString =
      L"[+-]?([0-9]+\\.[0-9]*" + ExponentString + L"|" + ExponentString + L")";
  const wregex Double;

  const wstring HexString = L"[0-9] | [A-F] | [a-f]";
  const wregex Hex;

  const wstring PercentString = L"%" + HexString + HexString;
  const wregex Percent;


  const wstring PnCharsBaseString=L"[A-Z]|[a-z]|[\u00C0-\u00D6]|[\u00D8-\u00F6]|[\u00F8-\u02FF]|[\u0370-\u037D]|[\u037F-\u1FFF]|[\u200C-\u200D]|[\u2070-\u218F]|[\u2C00-\u2FEF]|[\u3001-\uD7FF]|[\uF900-\uFDCF]|[\uFDF0-\uFFFD]|[\U00010000-\U000EFFFF]";
  const wregex PnCharsBase;
  const wstring PnCharsUString=PnCharsBaseString + L"|_";
  const wregex PnCharsU;

  const wstring PnCharsString =PnCharsUString + L"|-|[0-9]|\u00B7|[\u0300-\u036F]|[\u203F-\u2040]";
  const wregex PnChars;

  const wstring PnPrefixString=PnCharsBaseString + L"((" + PnCharsString +  L"|\\.)*" + PnCharsString + L")?";
  const wregex PnPrefix;

  const wstring PnLocalString=L"(" + PnCharsUString  + L"|:|[0-9]|" + PlxString + L")((" + PnCharsString + L"|\\.|:|" + PlxString + L")*(" + PnCharsString +  L"|:|" + PlxString + L"))?";
  const wregex PnLocal;

  const wstring PlxString = PercentString + L"|" + PnLocalEscString;
  const wregex Plx;

  const wstring PnLocalEscString = L"\\\\[_~.\\-!$&'()*+,;=/?#@%";
  const wregex PnLocalEsc;

};
