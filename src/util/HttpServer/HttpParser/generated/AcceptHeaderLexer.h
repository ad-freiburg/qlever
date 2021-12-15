
// Generated from AcceptHeader.g4 by ANTLR 4.9.2

#pragma once

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
  ~AcceptHeaderLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames()
      const override;  // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

 private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};
