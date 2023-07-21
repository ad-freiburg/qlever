
// Generated from MemorySizeLanguage.g4 by ANTLR 4.13.0

#pragma once

#include "antlr4-runtime.h"

class MemorySizeLanguageParser : public antlr4::Parser {
 public:
  enum {
    MEMORY_UNIT = 1,
    BYTE = 2,
    UNSIGNED_INTEGER = 3,
    FLOAT = 4,
    WHITESPACE = 5
  };

  enum {
    RuleMemorySizeString = 0,
    RulePureByteSize = 1,
    RuleMemoryUnitSize = 2
  };

  explicit MemorySizeLanguageParser(antlr4::TokenStream* input);

  MemorySizeLanguageParser(
      antlr4::TokenStream* input,
      const antlr4::atn::ParserATNSimulatorOptions& options);

  ~MemorySizeLanguageParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class MemorySizeStringContext;
  class PureByteSizeContext;
  class MemoryUnitSizeContext;

  class MemorySizeStringContext : public antlr4::ParserRuleContext {
   public:
    MemorySizeStringContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EOF();
    PureByteSizeContext* pureByteSize();
    MemoryUnitSizeContext* memoryUnitSize();
  };

  MemorySizeStringContext* memorySizeString();

  class PureByteSizeContext : public antlr4::ParserRuleContext {
   public:
    PureByteSizeContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* UNSIGNED_INTEGER();
    antlr4::tree::TerminalNode* BYTE();
  };

  PureByteSizeContext* pureByteSize();

  class MemoryUnitSizeContext : public antlr4::ParserRuleContext {
   public:
    MemoryUnitSizeContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* MEMORY_UNIT();
    antlr4::tree::TerminalNode* UNSIGNED_INTEGER();
    antlr4::tree::TerminalNode* FLOAT();
  };

  MemoryUnitSizeContext* memoryUnitSize();

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
};
