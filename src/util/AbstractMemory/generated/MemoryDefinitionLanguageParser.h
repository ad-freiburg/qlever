
// Generated from MemoryDefinitionLanguage.g4 by ANTLR 4.13.0

#pragma once


#include "antlr4-runtime.h"




class  MemoryDefinitionLanguageParser : public antlr4::Parser {
public:
  enum {
    MEMORY_UNIT = 1, BYTE = 2, UNSIGNED_INTEGER = 3, FLOAT = 4, WHITESPACE = 5
  };

  enum {
    RuleMemoryDefinitionString = 0, RulePureByteDefinition = 1, RuleMemoryUnitDefinition = 2
  };

  explicit MemoryDefinitionLanguageParser(antlr4::TokenStream *input);

  MemoryDefinitionLanguageParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~MemoryDefinitionLanguageParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class MemoryDefinitionStringContext;
  class PureByteDefinitionContext;
  class MemoryUnitDefinitionContext; 

  class  MemoryDefinitionStringContext : public antlr4::ParserRuleContext {
  public:
    MemoryDefinitionStringContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    PureByteDefinitionContext *pureByteDefinition();
    MemoryUnitDefinitionContext *memoryUnitDefinition();

   
  };

  MemoryDefinitionStringContext* memoryDefinitionString();

  class  PureByteDefinitionContext : public antlr4::ParserRuleContext {
  public:
    PureByteDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNSIGNED_INTEGER();
    antlr4::tree::TerminalNode *BYTE();

   
  };

  PureByteDefinitionContext* pureByteDefinition();

  class  MemoryUnitDefinitionContext : public antlr4::ParserRuleContext {
  public:
    MemoryUnitDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MEMORY_UNIT();
    antlr4::tree::TerminalNode *UNSIGNED_INTEGER();
    antlr4::tree::TerminalNode *FLOAT();

   
  };

  MemoryUnitDefinitionContext* memoryUnitDefinition();


  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

