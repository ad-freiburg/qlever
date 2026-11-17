
// Generated from ConfigShorthand.g4 by ANTLR 4.13.0

#ifndef QLEVER_SRC_UTIL_CONFIGMANAGER_GENERATED_CONFIGSHORTHANDLEXER_H
#define QLEVER_SRC_UTIL_CONFIGMANAGER_GENERATED_CONFIGSHORTHANDLEXER_H

#include "antlr4-runtime.h"

class ConfigShorthandLexer : public antlr4::Lexer {
 public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    T__3 = 4,
    T__4 = 5,
    T__5 = 6,
    LITERAL = 7,
    BOOL = 8,
    INTEGER = 9,
    FLOAT = 10,
    STRING = 11,
    NAME = 12,
    WHITESPACE = 13
  };

  explicit ConfigShorthandLexer(antlr4::CharStream* input);

  ~ConfigShorthandLexer() override;

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

#endif  // QLEVER_SRC_UTIL_CONFIGMANAGER_GENERATED_CONFIGSHORTHANDLEXER_H
