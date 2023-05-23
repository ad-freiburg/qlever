
// Generated from ANTLR4Mockup.g4 by ANTLR 4.13.0

#pragma once


#include "antlr4-runtime.h"




class  ANTLR4MockupLexer : public antlr4::Lexer {
public:
  enum {
    BOOL = 1, INTEGER = 2, FLOAT = 3, STRING = 4, WHITESPACE = 5
  };

  explicit ANTLR4MockupLexer(antlr4::CharStream *input);

  ~ANTLR4MockupLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

