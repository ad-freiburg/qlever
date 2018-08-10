// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//
#pragma once

#include <regex>

struct TurtleToken {
  using regex = std::regex;
  TurtleToken()
      : TurtlePrefix("@prefix"),
        SparqlPrefix("PREFIX"),
        TurtleBase("@base"),
        SparqlBase("BASE"),
        Dot("."),
        Comma(","),
        Semicolon(","),
        OpenSquared("\\["),
        CloseSquared("\\]"),
        OpenRound("\\("),
        CloseRound("\\)"),

        True("true"),
        False("false"),

        Integer("[+-]?[0-9]+"),
        Decimal("[+-]?[0-9]*\\.[0-9]+"),
  {}
  const std::regex TurtlePrefix;
  const std::regex SparqlPrefix;
  const std::regex TurtleBase;
  const std::regex SparqlBase;

  const std::regex Dot;
  const regex Comma;
  const regex Semicolon;
  const regex OpenSquared;
  const regex CloseSquared;
  const regex OpenRound;
  const regex CloseRound;

  const regex True;
  const regex False;

  const regex Integer;
  const regex Decimal;
  const string ExponentString = "[eE][+-]?[0-9]+";
  const regex Exponent;
  const string DoubleString =
      "[+-]?([0-9]+\\.[0-9]*" + ExponentString + "|" + ExponentString + ")";
  const regex Double;

  const string HexString = "[0-9] | [A-F] | [a-f]";
  const string Hex = "[0-9] | [A-F] | [a-f]";

  const string PercentString = "%" + HexString + HexString;
};

