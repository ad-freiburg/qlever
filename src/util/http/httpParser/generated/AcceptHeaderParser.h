
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#ifndef QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERPARSER_H
#define QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERPARSER_H

#include "antlr4-runtime.h"

class AcceptHeaderParser : public antlr4::Parser {
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

  enum {
    RuleAccept = 0,
    RuleAcceptWithEof = 1,
    RuleRangeAndParams = 2,
    RuleMediaRange = 3,
    RuleType = 4,
    RuleSubtype = 5,
    RuleAcceptParams = 6,
    RuleWeight = 7,
    RuleQvalue = 8,
    RuleAcceptExt = 9,
    RuleParameter = 10,
    RuleToken = 11,
    RuleTchar = 12,
    RuleQuotedString = 13,
    RuleQuoted_pair = 14
  };

  explicit AcceptHeaderParser(antlr4::TokenStream* input);

  AcceptHeaderParser(antlr4::TokenStream* input,
                     const antlr4::atn::ParserATNSimulatorOptions& options);

  ~AcceptHeaderParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class AcceptContext;
  class AcceptWithEofContext;
  class RangeAndParamsContext;
  class MediaRangeContext;
  class TypeContext;
  class SubtypeContext;
  class AcceptParamsContext;
  class WeightContext;
  class QvalueContext;
  class AcceptExtContext;
  class ParameterContext;
  class TokenContext;
  class TcharContext;
  class QuotedStringContext;
  class Quoted_pairContext;

  class AcceptContext : public antlr4::ParserRuleContext {
   public:
    AcceptContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<RangeAndParamsContext*> rangeAndParams();
    RangeAndParamsContext* rangeAndParams(size_t i);
    std::vector<antlr4::tree::TerminalNode*> OWS();
    antlr4::tree::TerminalNode* OWS(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AcceptContext* accept();

  class AcceptWithEofContext : public antlr4::ParserRuleContext {
   public:
    AcceptWithEofContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AcceptContext* accept();
    antlr4::tree::TerminalNode* EOF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AcceptWithEofContext* acceptWithEof();

  class RangeAndParamsContext : public antlr4::ParserRuleContext {
   public:
    RangeAndParamsContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MediaRangeContext* mediaRange();
    AcceptParamsContext* acceptParams();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RangeAndParamsContext* rangeAndParams();

  class MediaRangeContext : public antlr4::ParserRuleContext {
   public:
    MediaRangeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* MediaRangeAll();
    std::vector<ParameterContext*> parameter();
    ParameterContext* parameter(size_t i);
    TypeContext* type();
    antlr4::tree::TerminalNode* Slash();
    antlr4::tree::TerminalNode* Star();
    SubtypeContext* subtype();
    std::vector<antlr4::tree::TerminalNode*> OWS();
    antlr4::tree::TerminalNode* OWS(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MediaRangeContext* mediaRange();

  class TypeContext : public antlr4::ParserRuleContext {
   public:
    TypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TokenContext* token();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TypeContext* type();

  class SubtypeContext : public antlr4::ParserRuleContext {
   public:
    SubtypeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TokenContext* token();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SubtypeContext* subtype();

  class AcceptParamsContext : public antlr4::ParserRuleContext {
   public:
    AcceptParamsContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    WeightContext* weight();
    std::vector<AcceptExtContext*> acceptExt();
    AcceptExtContext* acceptExt(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AcceptParamsContext* acceptParams();

  class WeightContext : public antlr4::ParserRuleContext {
   public:
    WeightContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* QandEqual();
    QvalueContext* qvalue();
    std::vector<antlr4::tree::TerminalNode*> OWS();
    antlr4::tree::TerminalNode* OWS(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WeightContext* weight();

  class QvalueContext : public antlr4::ParserRuleContext {
   public:
    QvalueContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode*> DIGIT();
    antlr4::tree::TerminalNode* DIGIT(size_t i);
    antlr4::tree::TerminalNode* Dot();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QvalueContext* qvalue();

  class AcceptExtContext : public antlr4::ParserRuleContext {
   public:
    AcceptExtContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TokenContext*> token();
    TokenContext* token(size_t i);
    std::vector<antlr4::tree::TerminalNode*> OWS();
    antlr4::tree::TerminalNode* OWS(size_t i);
    QuotedStringContext* quotedString();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AcceptExtContext* acceptExt();

  class ParameterContext : public antlr4::ParserRuleContext {
   public:
    ParameterContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TokenContext*> token();
    TokenContext* token(size_t i);
    QuotedStringContext* quotedString();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ParameterContext* parameter();

  class TokenContext : public antlr4::ParserRuleContext {
   public:
    TokenContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TcharContext*> tchar();
    TcharContext* tchar(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TokenContext* token();

  class TcharContext : public antlr4::ParserRuleContext {
   public:
    TcharContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ExclamationMark();
    antlr4::tree::TerminalNode* Hashtag();
    antlr4::tree::TerminalNode* DollarSign();
    antlr4::tree::TerminalNode* Percent();
    antlr4::tree::TerminalNode* Ampersand();
    antlr4::tree::TerminalNode* SQuote();
    antlr4::tree::TerminalNode* Star();
    antlr4::tree::TerminalNode* Plus();
    antlr4::tree::TerminalNode* Minus();
    antlr4::tree::TerminalNode* Dot();
    antlr4::tree::TerminalNode* Caret();
    antlr4::tree::TerminalNode* Underscore();
    antlr4::tree::TerminalNode* BackQuote();
    antlr4::tree::TerminalNode* VBar();
    antlr4::tree::TerminalNode* Tilde();
    antlr4::tree::TerminalNode* DIGIT();
    antlr4::tree::TerminalNode* ALPHA();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TcharContext* tchar();

  class QuotedStringContext : public antlr4::ParserRuleContext {
   public:
    QuotedStringContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode*> DQUOTE();
    antlr4::tree::TerminalNode* DQUOTE(size_t i);
    std::vector<antlr4::tree::TerminalNode*> QDTEXT();
    antlr4::tree::TerminalNode* QDTEXT(size_t i);
    std::vector<Quoted_pairContext*> quoted_pair();
    Quoted_pairContext* quoted_pair(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QuotedStringContext* quotedString();

  class Quoted_pairContext : public antlr4::ParserRuleContext {
   public:
    Quoted_pairContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* HTAB();
    antlr4::tree::TerminalNode* SP();
    antlr4::tree::TerminalNode* VCHAR();
    antlr4::tree::TerminalNode* OBS_TEXT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  Quoted_pairContext* quoted_pair();

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
};

#endif  // QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERPARSER_H
