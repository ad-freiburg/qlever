// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#pragma once

#include <ctre/ctre.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <string>

#include "../util/Log.h"
#include "../util/StringUtils.h"
#include "./TurtleTokenId.h"

/// Helper function for ctre: concatenation of fixed_strings
template <size_t A, size_t B>
constexpr ctll::fixed_string<A + B> operator+(const ctll::fixed_string<A>& a,
                                              const ctll::fixed_string<B>& b) {
  char32_t comb[A + B + 1] = {};
  for (size_t i = 0; i < A; ++i) {  // omit the trailing 0 of the first string
    comb[i] = a.content[i];
  }
  for (size_t i = 0; i < B; ++i) {
    comb[i + A] = b.content[i];
  }
  // the input array must be zero-terminated
  comb[A + B] = '\0';
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

/**
 * @brief Holds the Compile-Time-Regexes for the Turtle Grammar used by the
 * TokenizerCTRE
 *
 * Caveat: The Prefix names are currently restricted to ascii values.
 */
struct TurtleTokenCtre {
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

  static constexpr auto DoubleString =
      fixed_string("[\\+\\-]?([0-9]+\\.[0-9]*") + ExponentString + "|" +
      ExponentString + ")";

  static constexpr auto Double = grp(DoubleString);

  static constexpr auto HexString = fixed_string("0-9A-Fa-f");
  static constexpr auto UcharString =
      fixed_string("\\\\u[0-9a-fA-f]{4}|\\\\U[0-9a-fA-f]{8}");

  static constexpr auto EcharString = fixed_string("\\\\[tbnrf\"\'\\\\]");

  static constexpr auto StringLiteralQuoteString =
      fixed_string("\"([^\\x22\\x5C\\x0A\\x0D]|") + EcharString + "|" +
      UcharString + ")*\"";

  static constexpr auto StringLiteralSingleQuoteString =
      fixed_string("'([^\\x27\\x5C\\x0A\\x0D]|") + EcharString + "|" +
      UcharString + ")*'";
  static constexpr auto StringLiteralLongSingleQuoteString =
      fixed_string("'''((''|')?([^'\\\\]|") + EcharString + "|" + UcharString +
      "))*'''";

  static constexpr auto StringLiteralLongQuoteString =
      fixed_string("\"\"\"((\"\"|\")?([^\"\\\\]|") + EcharString + "|" +
      UcharString + "))*\"\"\"";

  // TODO: fix this!
  static constexpr auto IrirefString =
      R"(<([^\x00-\x20<>"{}\x7c^`\\]|)" + UcharString + ")*>";

  static constexpr auto PercentString = "%" + cls(HexString) + "{2}";

  /*
  // TODO<joka921: Currently CTRE does not really support UTF-8. I tried to hack
  the UTF-8 byte patterns for this regex, see the file Utf8RegexTest.cpp, but
  this did increase compile time by an infeasible amount ( more than 20 minutes
  for this file alone which made debugging impossible) Thus we will currently
  only allow simple prefixes and emit proper warnings.

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

  // TODO<joka921>: Here we have the same issue with UTF-8 in CTRE as above
  /*
  static constexpr auto PnCharsString =
            PnCharsUString +
            "\\-0-9\\x{00B7}\\x{0300}-\\x{036F}\\x{203F}-\\x{2040}";
            */

  static constexpr auto PnCharsString = PnCharsUString + "\\-0-9";

  static constexpr auto PnPrefixString = cls(PnCharsBaseString) + "(\\." +
                                         cls(PnCharsString) + "|" +
                                         cls(PnCharsString) + ")*";

  static constexpr auto PnameNSString = grp(PnPrefixString) + "?:";

  static constexpr fixed_string PnLocalEscString =
      "\\\\[_~.\\-!$&'()*+,;=/?#@%]";

  static constexpr fixed_string PlxString =
      PercentString + "|" + PnLocalEscString;

  static constexpr fixed_string TmpNoDot =
      cls(PnCharsString + ":") + "|" + PlxString;
  static constexpr fixed_string PnLocalString =
      grp(cls(PnCharsUString + ":0-9") + "|" + PlxString) +
      grp("\\.*" + grp(TmpNoDot)) + "*";

  static constexpr fixed_string PnLocal = grp(PnLocalString);

  static constexpr fixed_string PnameLNString =
      grp(PnameNSString) + grp(PnLocalString);

  static constexpr fixed_string BlankNodeLabelString =
      fixed_string("_:") + cls(PnCharsUString + "0-9") +
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

class TokenizerCtre {
  FRIEND_TEST(TokenizerTest, Compilation);
  using string = std::string;

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
   * If a prefix of the input stream matches the regex argument, return true
   * and that prefix and move the input stream forward by the length of the
   * match. If no match is found, false is returned and the input stream
   * remains the same.
   */
  template <TurtleTokenId id>
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
  template <TurtleTokenId fst, TurtleTokenId... ids>
  std::tuple<bool, size_t, std::string_view> getNextTokenMultiple() {
    const char* beg = _data.begin();
    size_t dataSize = _data.size();

    auto innerResult = getNextTokenRecurse<0, fst, ids...>();

    // Advance by the length of the longest match.
    auto maxMatchSize = std::get<1>(innerResult);
    reset(beg + maxMatchSize, dataSize - maxMatchSize);
    return innerResult;
  }

  /// Skip any whitespace or comments at the beginning of the held characters
  void skipWhitespaceAndComments() {
    skipWhitespace();
    skipComments();
    skipWhitespace();
  }

  /**
   * If there is a prefix match with the argument, move forward the beginning
   * of _data and return true. Else return false.Can be used if we are not
   * interested in the actual value of the match
   */
  template <TurtleTokenId id>
  bool skip() {
    return getNextToken<id>().first;
  }

  /// Reinitialize with a new byte pointer and size pair.
  void reset(const char* ptr, size_t sz) { _data = std::string_view(ptr, sz); }
  /// Reinitialize with a string_view.
  void reset(std::string_view s) { _data = s; }

  /// Access to input stream as std::string_view.
  [[nodiscard]] std::string_view view() const { return _data; }
  /// Access to input stream as std::string_view.
  [[nodiscard]] const std::string_view data() const { return _data; }
  /// Remove the first n characters from our input stream (e.g. if they have
  /// been dealt with externally).
  void remove_prefix(size_t n) { _data.remove_prefix(n); }

 private:
  /// string_view of the characters we are parsing from.
  std::string_view _data;

  // ________________________________________________________________
  void skipWhitespace() {
    auto v = view();
    auto pos = v.find_first_not_of("\x20\x09\x0D\x0A");
    pos = std::min(pos, v.size());
    _data.remove_prefix(pos);
  }

  // ___________________________________________________________________________________
  void skipComments() {
    auto v = view();
    if (ad_utility::startsWith(v, "#")) {
      auto pos = v.find("\n");
      if (pos == string::npos) {
        // TODO<joka921>: This should rather yield an error.
        LOG(INFO) << "Warning, unfinished comment found while parsing";
      } else {
        _data.remove_prefix(pos + 1);
      }
    }
  }
  FRIEND_TEST(TokenizerTest, WhitespaceAndComments);

  /*
   * Determine, which of the compile-timer regexes in the TurtleTokenCtre class
   * belongs to the specified TurtleTokenId Then call the static templated
   * process Function of the class template f, so F::template
   * process<TheRegex>(_data)
   */
  template <class F, TurtleTokenId id>
  constexpr auto apply() {
    if constexpr (id == TurtleTokenId::Langtag) {
      return F::template process<TurtleTokenCtre::Langtag>(_data);
    } else if constexpr (id == TurtleTokenId::Double) {
      return F::template process<TurtleTokenCtre::Double>(_data);
    } else if constexpr (id == TurtleTokenId::Iriref) {
      return F::template process<TurtleTokenCtre::Iriref>(_data);
    } else if constexpr (id == TurtleTokenId::PnameNS) {
      return F::template process<TurtleTokenCtre::PnameNS>(_data);
    } else if constexpr (id == TurtleTokenId::PnameLN) {
      return F::template process<TurtleTokenCtre::PnameLN>(_data);
    } else if constexpr (id == TurtleTokenId::PnLocal) {
      return F::template process<TurtleTokenCtre::PnLocal>(_data);
    } else if constexpr (id == TurtleTokenId::BlankNodeLabel) {
      return F::template process<TurtleTokenCtre::BlankNodeLabel>(_data);
    } else {
      switch (id) {
        case TurtleTokenId::TurtlePrefix:
          return F::template process<TurtleTokenCtre::TurtlePrefix>(_data);
        case TurtleTokenId::SparqlPrefix:
          return F::template process<TurtleTokenCtre::SparqlPrefix>(_data);
        case TurtleTokenId::TurtleBase:
          return F::template process<TurtleTokenCtre::TurtleBase>(_data);
        case TurtleTokenId::SparqlBase:
          return F::template process<TurtleTokenCtre::SparqlBase>(_data);
        case TurtleTokenId::Dot:
          return F::template process<TurtleTokenCtre::Dot>(_data);
        case TurtleTokenId::Comma:
          return F::template process<TurtleTokenCtre::Comma>(_data);
        case TurtleTokenId::Semicolon:
          return F::template process<TurtleTokenCtre::Semicolon>(_data);
        case TurtleTokenId::OpenSquared:
          return F::template process<TurtleTokenCtre::OpenSquared>(_data);
        case TurtleTokenId::CloseSquared:
          return F::template process<TurtleTokenCtre::CloseSquared>(_data);

        case TurtleTokenId::OpenRound:
          return F::template process<TurtleTokenCtre::OpenRound>(_data);
        case TurtleTokenId::CloseRound:
          return F::template process<TurtleTokenCtre::CloseRound>(_data);
        case TurtleTokenId::A:
          return F::template process<TurtleTokenCtre::A>(_data);
        case TurtleTokenId::DoubleCircumflex:
          return F::template process<TurtleTokenCtre::DoubleCircumflex>(_data);
        case TurtleTokenId::True:
          return F::template process<TurtleTokenCtre::True>(_data);
        case TurtleTokenId::False:
          return F::template process<TurtleTokenCtre::False>(_data);

        case TurtleTokenId::Integer:
          return F::template process<TurtleTokenCtre::Integer>(_data);
        case TurtleTokenId::Decimal:
          return F::template process<TurtleTokenCtre::Decimal>(_data);
        case TurtleTokenId::Exponent:
          return F::template process<TurtleTokenCtre::Exponent>(_data);
        case TurtleTokenId::WsMultiple:
          return F::template process<TurtleTokenCtre::WsMultiple>(_data);
        case TurtleTokenId::Anon:
          return F::template process<TurtleTokenCtre::Anon>(_data);
        case TurtleTokenId::Comment:
          return F::template process<TurtleTokenCtre::Comment>(_data);
      }
    }
  }

  // Base case for the recursion on longest match for multiple regexes. No regex
  // left, so there's no match.
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
  template <size_t idx, TurtleTokenId fst, TurtleTokenId... ids>
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
