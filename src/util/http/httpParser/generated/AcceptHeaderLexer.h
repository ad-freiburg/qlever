
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#ifndef QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERLEXER_H
#define QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERLEXER_H

#include "antlr4-runtime.h"

class AcceptHeaderLexer : public antlr4::Lexer {
 public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    T__3 = 4,
    MediaRangeAll = 5,
    QandEqual = 6,
    DIGIT = 7,
    ALPHA = 8,
    OWS = 9,
    Minus = 10,
    Dot = 11,
    Underscore = 12,
    Tilde = 13,
    QuestionMark = 14,
    Slash = 15,
    ExclamationMark = 16,
    Colon = 17,
    At = 18,
    DollarSign = 19,
    Hashtag = 20,
    Ampersand = 21,
    Percent = 22,
    SQuote = 23,
    Star = 24,
    Plus = 25,
    Caret = 26,
    BackQuote = 27,
    VBar = 28,
    QDTEXT = 29,
    OBS_TEXT = 30,
    DQUOTE = 31,
    SP = 32,
    HTAB = 33,
    VCHAR = 34
  };

  explicit AcceptHeaderLexer(antlr4::CharStream* input);

  ~AcceptHeaderLexer() override;

  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.
};

#endif  // QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERLEXER_H
