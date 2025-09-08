// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#ifndef QLEVER_SRC_PARSER_TOKENIZER_H
#define QLEVER_SRC_PARSER_TOKENIZER_H

#include <gtest/gtest_prod.h>
#include <re2/re2.h>

#include "parser/TurtleTokenId.h"
#include "util/CompilerWarnings.h"
#include "util/Log.h"

using re2::RE2;
using namespace std::string_literals;

/** holds all the google re2 regexes that correspond to all terminals in the
 * turtle grammar cannot be static since google regexes have to be constructed
 * at runtime
 */
struct TurtleToken {
  using string = std::string;
  DISABLE_STRINGOP_OVERFLOW_WARNINGS
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
        IrirefRelaxed(grp(IrirefStringRelaxed)),
        PnameNS(grp(PnameNSString)),
        PnameLN(grp(PnameLNString)),
        PnLocal(grp(PnLocalString)),
        BlankNodeLabel(grp(BlankNodeLabelString)),

        WsMultiple(grp(WsMultipleString)),
        Anon(grp(AnonString)),
        Comment(grp(CommentString)) {}

  GCC_REENABLE_WARNINGS

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
  const string DoubleString = "[+-]?([0-9]+\\.[0-9]*" + ExponentString +
                              "|\\.[0-9]+" + ExponentString + "|[0-9]+" +
                              ExponentString + ")";
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
  const string IrirefStringRelaxed =
      "<([^\\x00-\\x19<>\"\\\\]|"s + UcharString + ")*>";
  const RE2 IrirefRelaxed;

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

  const string PnameNSString = grp(PnPrefixString) + "?:";
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
  const RE2 PnLocal;

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

// A CRTP-style Mixin that factors out the common implementation of
// `skipWhitespaceAndComments` of the `Tokenizer` and `TokenizerCtre` classes.
template <typename Self>
struct SkipWhitespaceAndCommentsMixin {
 private:
  Self& self() { return static_cast<Self&>(*this); }

 public:
  /// Skip any whitespace or comments at the beginning of the held characters
  void skipWhitespaceAndComments() {
    // Call `skipWhitespace` and `skipComments` in a loop until no more input
    // was consumed. This is necessary because we might have multiple lines of
    // comments that are separated by whitespace.
    while (true) {
      bool a = skipWhitespace();
      bool b = skipComments();
      if (!(a || b)) {
        return;
      }
    }
  }

 private:
  // _________________________________________________________________________
  bool skipWhitespace() {
    auto v = self().view();
    auto numLeadingWhitespace = v.find_first_not_of("\x20\x09\x0D\x0A");
    numLeadingWhitespace = std::min(numLeadingWhitespace, v.size());
    self()._data.remove_prefix(numLeadingWhitespace);
    return numLeadingWhitespace > 0;
  }

  // _________________________________________________________________________
  bool skipComments() {
    auto v = self().view();
    if (v.starts_with('#')) {
      auto pos = v.find('\n');
      if (pos == std::string::npos) {
        // TODO<joka921>: This should rather yield an error.
        LOG(INFO) << "Warning, unfinished comment found while parsing"
                  << std::endl;
      } else {
        self()._data.remove_prefix(pos + 1);
      }
      return true;
    }
    return false;
  }
  FRIEND_TEST(TokenizerTest, WhitespaceAndComments);
};

/**
 * @brief Tokenizer that uses the Ctre library
 *
 * It has exactly the same public interface as the Tokenizer class from above,
 * so it can be used as a drop-in replacement. CAVEAT: Currently this does only
 * support prefixes that only contain ascii characters
 */

// The currently used hand-written tokenizer
// for this for correctness and efficiency
class Tokenizer : public SkipWhitespaceAndCommentsMixin<Tokenizer> {
  friend struct SkipWhitespaceAndCommentsMixin<Tokenizer>;
  FRIEND_TEST(TokenizerTest, Compilation);

 public:
  // Construct from a std::string_view;
  Tokenizer(std::string_view input)
      : _tokens(), _data(input.data(), input.size()) {}

  static constexpr bool UseRelaxedParsing = false;

  // if a prefix of the input stream matches the regex argument,
  // return true and that prefix and move the input stream forward
  // by the length of the match. If no match is found,
  // false is returned and the input stream remains the same
  std::pair<bool, std::string> getNextToken(const RE2& reg);
  template <TurtleTokenId id>
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

  template <TurtleTokenId fst, TurtleTokenId... ids>
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

  template <size_t idx, TurtleTokenId fst, TurtleTokenId... ids>
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

  // If there is a prefix match with the argument, move forward the input stream
  // and return true. Can be used if we are not interested in the actual value
  // of the match
  bool skip(const RE2& reg) { return RE2::Consume(&_data, reg); }
  template <TurtleTokenId id>
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
  // convert the (external) TurtleTokenId to the internally used google-regex
  const RE2& idToRegex(const TurtleTokenId reg);

  FRIEND_TEST(TokenizerTest, WhitespaceAndComments);
  re2::StringPiece _data;
};

#endif  // QLEVER_SRC_PARSER_TOKENIZER_H
