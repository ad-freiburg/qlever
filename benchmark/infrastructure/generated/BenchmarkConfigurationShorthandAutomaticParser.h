
// Generated from BenchmarkConfigurationShorthandAutomatic.g4 by ANTLR 4.12.0

#pragma once


#include "antlr4-runtime.h"




class  BenchmarkConfigurationShorthandAutomaticParser : public antlr4::Parser {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, LITERAL = 6, BOOL = 7, 
    INTEGER = 8, FLOAT = 9, STRING = 10, NAME = 11, WHITESPACE = 12
  };

  enum {
    RuleAssignments = 0, RuleAssignment = 1, RuleList = 2
  };

  explicit BenchmarkConfigurationShorthandAutomaticParser(antlr4::TokenStream *input);

  BenchmarkConfigurationShorthandAutomaticParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~BenchmarkConfigurationShorthandAutomaticParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class AssignmentsContext;
  class AssignmentContext;
  class ListContext; 

  class  AssignmentsContext : public antlr4::ParserRuleContext {
  public:
    BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext *assignmentContext = nullptr;
    std::vector<AssignmentContext *> listOfAssignments;
    AssignmentsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    std::vector<AssignmentContext *> assignment();
    AssignmentContext* assignment(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AssignmentsContext* assignments();

  class  AssignmentContext : public antlr4::ParserRuleContext {
  public:
    AssignmentContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NAME();
    antlr4::tree::TerminalNode *LITERAL();
    ListContext *list();


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AssignmentContext* assignment();

  class  ListContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *literalToken = nullptr;
    std::vector<antlr4::Token *> listElement;
    ListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> LITERAL();
    antlr4::tree::TerminalNode* LITERAL(size_t i);


    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ListContext* list();


  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

