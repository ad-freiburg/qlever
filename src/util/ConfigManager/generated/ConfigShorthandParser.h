
// Generated from ConfigShorthand.g4 by ANTLR 4.13.0

#pragma once

#include "antlr4-runtime.h"

class ConfigShorthandParser : public antlr4::Parser {
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

  enum {
    RuleShortHandString = 0,
    RuleAssignments = 1,
    RuleAssignment = 2,
    RuleObject = 3,
    RuleList = 4,
    RuleContent = 5
  };

  explicit ConfigShorthandParser(antlr4::TokenStream* input);

  ConfigShorthandParser(antlr4::TokenStream* input,
                        const antlr4::atn::ParserATNSimulatorOptions& options);

  ~ConfigShorthandParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class ShortHandStringContext;
  class AssignmentsContext;
  class AssignmentContext;
  class ObjectContext;
  class ListContext;
  class ContentContext;

  class ShortHandStringContext : public antlr4::ParserRuleContext {
   public:
    ShortHandStringContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AssignmentsContext* assignments();
    antlr4::tree::TerminalNode* EOF();
  };

  ShortHandStringContext* shortHandString();

  class AssignmentsContext : public antlr4::ParserRuleContext {
   public:
    ConfigShorthandParser::AssignmentContext* assignmentContext = nullptr;
    std::vector<AssignmentContext*> listOfAssignments;
    AssignmentsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<AssignmentContext*> assignment();
    AssignmentContext* assignment(size_t i);
  };

  AssignmentsContext* assignments();

  class AssignmentContext : public antlr4::ParserRuleContext {
   public:
    AssignmentContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NAME();
    ContentContext* content();
  };

  AssignmentContext* assignment();

  class ObjectContext : public antlr4::ParserRuleContext {
   public:
    ObjectContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AssignmentsContext* assignments();
  };

  ObjectContext* object();

  class ListContext : public antlr4::ParserRuleContext {
   public:
    ConfigShorthandParser::ContentContext* contentContext = nullptr;
    std::vector<ContentContext*> listElement;
    ListContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ContentContext*> content();
    ContentContext* content(size_t i);
  };

  ListContext* list();

  class ContentContext : public antlr4::ParserRuleContext {
   public:
    ContentContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LITERAL();
    ListContext* list();
    ObjectContext* object();
  };

  ContentContext* content();

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
};
