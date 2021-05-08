
// Generated from Sparql.g4 by ANTLR 4.9.2


#include "SparqlListener.h"
#include "SparqlVisitor.h"

#include "SparqlParser.h"


using namespace antlrcpp;
using namespace antlr4;

SparqlParser::SparqlParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

SparqlParser::~SparqlParser() {
  delete _interpreter;
}

std::string SparqlParser::getGrammarFileName() const {
  return "Sparql.g4";
}

const std::vector<std::string>& SparqlParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& SparqlParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- QueryContext ------------------------------------------------------------------

SparqlParser::QueryContext::QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PrologueContext* SparqlParser::QueryContext::prologue() {
  return getRuleContext<SparqlParser::PrologueContext>(0);
}

SparqlParser::ValuesClauseContext* SparqlParser::QueryContext::valuesClause() {
  return getRuleContext<SparqlParser::ValuesClauseContext>(0);
}

tree::TerminalNode* SparqlParser::QueryContext::EOF() {
  return getToken(SparqlParser::EOF, 0);
}

SparqlParser::SelectQueryContext* SparqlParser::QueryContext::selectQuery() {
  return getRuleContext<SparqlParser::SelectQueryContext>(0);
}

SparqlParser::ConstructQueryContext* SparqlParser::QueryContext::constructQuery() {
  return getRuleContext<SparqlParser::ConstructQueryContext>(0);
}

SparqlParser::DescribeQueryContext* SparqlParser::QueryContext::describeQuery() {
  return getRuleContext<SparqlParser::DescribeQueryContext>(0);
}

SparqlParser::AskQueryContext* SparqlParser::QueryContext::askQuery() {
  return getRuleContext<SparqlParser::AskQueryContext>(0);
}


size_t SparqlParser::QueryContext::getRuleIndex() const {
  return SparqlParser::RuleQuery;
}

void SparqlParser::QueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterQuery(this);
}

void SparqlParser::QueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitQuery(this);
}


antlrcpp::Any SparqlParser::QueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::QueryContext* SparqlParser::query() {
  QueryContext *_localctx = _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 0, SparqlParser::RuleQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(236);
    prologue();
    setState(241);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::SELECT: {
        setState(237);
        selectQuery();
        break;
      }

      case SparqlParser::CONSTRUCT: {
        setState(238);
        constructQuery();
        break;
      }

      case SparqlParser::DESCRIBE: {
        setState(239);
        describeQuery();
        break;
      }

      case SparqlParser::ASK: {
        setState(240);
        askQuery();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(243);
    valuesClause();
    setState(244);
    match(SparqlParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrologueContext ------------------------------------------------------------------

SparqlParser::PrologueContext::PrologueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::BaseDeclContext *> SparqlParser::PrologueContext::baseDecl() {
  return getRuleContexts<SparqlParser::BaseDeclContext>();
}

SparqlParser::BaseDeclContext* SparqlParser::PrologueContext::baseDecl(size_t i) {
  return getRuleContext<SparqlParser::BaseDeclContext>(i);
}

std::vector<SparqlParser::PrefixDeclContext *> SparqlParser::PrologueContext::prefixDecl() {
  return getRuleContexts<SparqlParser::PrefixDeclContext>();
}

SparqlParser::PrefixDeclContext* SparqlParser::PrologueContext::prefixDecl(size_t i) {
  return getRuleContext<SparqlParser::PrefixDeclContext>(i);
}


size_t SparqlParser::PrologueContext::getRuleIndex() const {
  return SparqlParser::RulePrologue;
}

void SparqlParser::PrologueContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrologue(this);
}

void SparqlParser::PrologueContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrologue(this);
}


antlrcpp::Any SparqlParser::PrologueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPrologue(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrologueContext* SparqlParser::prologue() {
  PrologueContext *_localctx = _tracker.createInstance<PrologueContext>(_ctx, getState());
  enterRule(_localctx, 2, SparqlParser::RulePrologue);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(250);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::BASE

    || _la == SparqlParser::PREFIX) {
      setState(248);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlParser::BASE: {
          setState(246);
          baseDecl();
          break;
        }

        case SparqlParser::PREFIX: {
          setState(247);
          prefixDecl();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(252);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BaseDeclContext ------------------------------------------------------------------

SparqlParser::BaseDeclContext::BaseDeclContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BaseDeclContext::BASE() {
  return getToken(SparqlParser::BASE, 0);
}

SparqlParser::IrirefContext* SparqlParser::BaseDeclContext::iriref() {
  return getRuleContext<SparqlParser::IrirefContext>(0);
}


size_t SparqlParser::BaseDeclContext::getRuleIndex() const {
  return SparqlParser::RuleBaseDecl;
}

void SparqlParser::BaseDeclContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBaseDecl(this);
}

void SparqlParser::BaseDeclContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBaseDecl(this);
}


antlrcpp::Any SparqlParser::BaseDeclContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBaseDecl(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BaseDeclContext* SparqlParser::baseDecl() {
  BaseDeclContext *_localctx = _tracker.createInstance<BaseDeclContext>(_ctx, getState());
  enterRule(_localctx, 4, SparqlParser::RuleBaseDecl);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(253);
    match(SparqlParser::BASE);
    setState(254);
    iriref();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrefixDeclContext ------------------------------------------------------------------

SparqlParser::PrefixDeclContext::PrefixDeclContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::PrefixDeclContext::PREFIX() {
  return getToken(SparqlParser::PREFIX, 0);
}

tree::TerminalNode* SparqlParser::PrefixDeclContext::PNAME_NS() {
  return getToken(SparqlParser::PNAME_NS, 0);
}

SparqlParser::IrirefContext* SparqlParser::PrefixDeclContext::iriref() {
  return getRuleContext<SparqlParser::IrirefContext>(0);
}


size_t SparqlParser::PrefixDeclContext::getRuleIndex() const {
  return SparqlParser::RulePrefixDecl;
}

void SparqlParser::PrefixDeclContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrefixDecl(this);
}

void SparqlParser::PrefixDeclContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrefixDecl(this);
}


antlrcpp::Any SparqlParser::PrefixDeclContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPrefixDecl(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrefixDeclContext* SparqlParser::prefixDecl() {
  PrefixDeclContext *_localctx = _tracker.createInstance<PrefixDeclContext>(_ctx, getState());
  enterRule(_localctx, 6, SparqlParser::RulePrefixDecl);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(256);
    match(SparqlParser::PREFIX);
    setState(257);
    match(SparqlParser::PNAME_NS);
    setState(258);
    iriref();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectQueryContext ------------------------------------------------------------------

SparqlParser::SelectQueryContext::SelectQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::SelectClauseContext* SparqlParser::SelectQueryContext::selectClause() {
  return getRuleContext<SparqlParser::SelectClauseContext>(0);
}

SparqlParser::WhereClauseContext* SparqlParser::SelectQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

SparqlParser::SolutionModifierContext* SparqlParser::SelectQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::SelectQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::SelectQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}


size_t SparqlParser::SelectQueryContext::getRuleIndex() const {
  return SparqlParser::RuleSelectQuery;
}

void SparqlParser::SelectQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectQuery(this);
}

void SparqlParser::SelectQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectQuery(this);
}


antlrcpp::Any SparqlParser::SelectQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitSelectQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SelectQueryContext* SparqlParser::selectQuery() {
  SelectQueryContext *_localctx = _tracker.createInstance<SelectQueryContext>(_ctx, getState());
  enterRule(_localctx, 8, SparqlParser::RuleSelectQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(260);
    selectClause();
    setState(264);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(261);
      datasetClause();
      setState(266);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(267);
    whereClause();
    setState(268);
    solutionModifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubSelectContext ------------------------------------------------------------------

SparqlParser::SubSelectContext::SubSelectContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::SelectClauseContext* SparqlParser::SubSelectContext::selectClause() {
  return getRuleContext<SparqlParser::SelectClauseContext>(0);
}

SparqlParser::WhereClauseContext* SparqlParser::SubSelectContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

SparqlParser::SolutionModifierContext* SparqlParser::SubSelectContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

SparqlParser::ValuesClauseContext* SparqlParser::SubSelectContext::valuesClause() {
  return getRuleContext<SparqlParser::ValuesClauseContext>(0);
}


size_t SparqlParser::SubSelectContext::getRuleIndex() const {
  return SparqlParser::RuleSubSelect;
}

void SparqlParser::SubSelectContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubSelect(this);
}

void SparqlParser::SubSelectContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubSelect(this);
}


antlrcpp::Any SparqlParser::SubSelectContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitSubSelect(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SubSelectContext* SparqlParser::subSelect() {
  SubSelectContext *_localctx = _tracker.createInstance<SubSelectContext>(_ctx, getState());
  enterRule(_localctx, 10, SparqlParser::RuleSubSelect);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(270);
    selectClause();
    setState(271);
    whereClause();
    setState(272);
    solutionModifier();
    setState(273);
    valuesClause();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectClauseContext ------------------------------------------------------------------

SparqlParser::SelectClauseContext::SelectClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::SelectClauseContext::SELECT() {
  return getToken(SparqlParser::SELECT, 0);
}

tree::TerminalNode* SparqlParser::SelectClauseContext::DISTINCT() {
  return getToken(SparqlParser::DISTINCT, 0);
}

tree::TerminalNode* SparqlParser::SelectClauseContext::REDUCED() {
  return getToken(SparqlParser::REDUCED, 0);
}

std::vector<SparqlParser::VarContext *> SparqlParser::SelectClauseContext::var() {
  return getRuleContexts<SparqlParser::VarContext>();
}

SparqlParser::VarContext* SparqlParser::SelectClauseContext::var(size_t i) {
  return getRuleContext<SparqlParser::VarContext>(i);
}

std::vector<SparqlParser::AliasContext *> SparqlParser::SelectClauseContext::alias() {
  return getRuleContexts<SparqlParser::AliasContext>();
}

SparqlParser::AliasContext* SparqlParser::SelectClauseContext::alias(size_t i) {
  return getRuleContext<SparqlParser::AliasContext>(i);
}


size_t SparqlParser::SelectClauseContext::getRuleIndex() const {
  return SparqlParser::RuleSelectClause;
}

void SparqlParser::SelectClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSelectClause(this);
}

void SparqlParser::SelectClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSelectClause(this);
}


antlrcpp::Any SparqlParser::SelectClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitSelectClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SelectClauseContext* SparqlParser::selectClause() {
  SelectClauseContext *_localctx = _tracker.createInstance<SelectClauseContext>(_ctx, getState());
  enterRule(_localctx, 12, SparqlParser::RuleSelectClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(275);
    match(SparqlParser::SELECT);
    setState(277);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::DISTINCT

    || _la == SparqlParser::REDUCED) {
      setState(276);
      _la = _input->LA(1);
      if (!(_la == SparqlParser::DISTINCT

      || _la == SparqlParser::REDUCED)) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(286);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        setState(281); 
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(281);
          _errHandler->sync(this);
          switch (_input->LA(1)) {
            case SparqlParser::VAR1:
            case SparqlParser::VAR2: {
              setState(279);
              var();
              break;
            }

            case SparqlParser::T__1: {
              setState(280);
              alias();
              break;
            }

          default:
            throw NoViableAltException(this);
          }
          setState(283); 
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == SparqlParser::T__1 || _la == SparqlParser::VAR1

        || _la == SparqlParser::VAR2);
        break;
      }

      case SparqlParser::T__0: {
        setState(285);
        match(SparqlParser::T__0);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AliasContext ------------------------------------------------------------------

SparqlParser::AliasContext::AliasContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::ExpressionContext* SparqlParser::AliasContext::expression() {
  return getRuleContext<SparqlParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::AliasContext::AS() {
  return getToken(SparqlParser::AS, 0);
}

SparqlParser::VarContext* SparqlParser::AliasContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}


size_t SparqlParser::AliasContext::getRuleIndex() const {
  return SparqlParser::RuleAlias;
}

void SparqlParser::AliasContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAlias(this);
}

void SparqlParser::AliasContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAlias(this);
}


antlrcpp::Any SparqlParser::AliasContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitAlias(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::AliasContext* SparqlParser::alias() {
  AliasContext *_localctx = _tracker.createInstance<AliasContext>(_ctx, getState());
  enterRule(_localctx, 14, SparqlParser::RuleAlias);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(288);
    match(SparqlParser::T__1);
    setState(289);
    expression();
    setState(290);
    match(SparqlParser::AS);
    setState(291);
    var();
    setState(292);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructQueryContext ------------------------------------------------------------------

SparqlParser::ConstructQueryContext::ConstructQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ConstructQueryContext::CONSTRUCT() {
  return getToken(SparqlParser::CONSTRUCT, 0);
}

SparqlParser::ConstructTemplateContext* SparqlParser::ConstructQueryContext::constructTemplate() {
  return getRuleContext<SparqlParser::ConstructTemplateContext>(0);
}

SparqlParser::WhereClauseContext* SparqlParser::ConstructQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

SparqlParser::SolutionModifierContext* SparqlParser::ConstructQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

tree::TerminalNode* SparqlParser::ConstructQueryContext::WHERE() {
  return getToken(SparqlParser::WHERE, 0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::ConstructQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::ConstructQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}

SparqlParser::TriplesTemplateContext* SparqlParser::ConstructQueryContext::triplesTemplate() {
  return getRuleContext<SparqlParser::TriplesTemplateContext>(0);
}


size_t SparqlParser::ConstructQueryContext::getRuleIndex() const {
  return SparqlParser::RuleConstructQuery;
}

void SparqlParser::ConstructQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstructQuery(this);
}

void SparqlParser::ConstructQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstructQuery(this);
}


antlrcpp::Any SparqlParser::ConstructQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitConstructQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstructQueryContext* SparqlParser::constructQuery() {
  ConstructQueryContext *_localctx = _tracker.createInstance<ConstructQueryContext>(_ctx, getState());
  enterRule(_localctx, 16, SparqlParser::RuleConstructQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(294);
    match(SparqlParser::CONSTRUCT);
    setState(318);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__3: {
        setState(295);
        constructTemplate();
        setState(299);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlParser::FROM) {
          setState(296);
          datasetClause();
          setState(301);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(302);
        whereClause();
        setState(303);
        solutionModifier();
        break;
      }

      case SparqlParser::WHERE:
      case SparqlParser::FROM: {
        setState(308);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlParser::FROM) {
          setState(305);
          datasetClause();
          setState(310);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(311);
        match(SparqlParser::WHERE);
        setState(312);
        match(SparqlParser::T__3);
        setState(314);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
          | (1ULL << SparqlParser::T__16)
          | (1ULL << SparqlParser::T__29)
          | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
          | (1ULL << (SparqlParser::PNAME_NS - 139))
          | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
          | (1ULL << (SparqlParser::VAR1 - 139))
          | (1ULL << (SparqlParser::VAR2 - 139))
          | (1ULL << (SparqlParser::INTEGER - 139))
          | (1ULL << (SparqlParser::DECIMAL - 139))
          | (1ULL << (SparqlParser::DOUBLE - 139))
          | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
          | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
          | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
          | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
          | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
          | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
          | (1ULL << (SparqlParser::NIL - 139))
          | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
          setState(313);
          triplesTemplate();
        }
        setState(316);
        match(SparqlParser::T__4);
        setState(317);
        solutionModifier();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DescribeQueryContext ------------------------------------------------------------------

SparqlParser::DescribeQueryContext::DescribeQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::DescribeQueryContext::DESCRIBE() {
  return getToken(SparqlParser::DESCRIBE, 0);
}

SparqlParser::SolutionModifierContext* SparqlParser::DescribeQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::DescribeQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::DescribeQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}

SparqlParser::WhereClauseContext* SparqlParser::DescribeQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

std::vector<SparqlParser::VarOrIriContext *> SparqlParser::DescribeQueryContext::varOrIri() {
  return getRuleContexts<SparqlParser::VarOrIriContext>();
}

SparqlParser::VarOrIriContext* SparqlParser::DescribeQueryContext::varOrIri(size_t i) {
  return getRuleContext<SparqlParser::VarOrIriContext>(i);
}


size_t SparqlParser::DescribeQueryContext::getRuleIndex() const {
  return SparqlParser::RuleDescribeQuery;
}

void SparqlParser::DescribeQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDescribeQuery(this);
}

void SparqlParser::DescribeQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDescribeQuery(this);
}


antlrcpp::Any SparqlParser::DescribeQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitDescribeQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DescribeQueryContext* SparqlParser::describeQuery() {
  DescribeQueryContext *_localctx = _tracker.createInstance<DescribeQueryContext>(_ctx, getState());
  enterRule(_localctx, 18, SparqlParser::RuleDescribeQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(320);
    match(SparqlParser::DESCRIBE);
    setState(327);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        setState(322); 
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(321);
          varOrIri();
          setState(324); 
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (((((_la - 139) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
          | (1ULL << (SparqlParser::PNAME_NS - 139))
          | (1ULL << (SparqlParser::VAR1 - 139))
          | (1ULL << (SparqlParser::VAR2 - 139)))) != 0));
        break;
      }

      case SparqlParser::T__0: {
        setState(326);
        match(SparqlParser::T__0);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(332);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(329);
      datasetClause();
      setState(334);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(336);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__3

    || _la == SparqlParser::WHERE) {
      setState(335);
      whereClause();
    }
    setState(338);
    solutionModifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AskQueryContext ------------------------------------------------------------------

SparqlParser::AskQueryContext::AskQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::AskQueryContext::ASK() {
  return getToken(SparqlParser::ASK, 0);
}

SparqlParser::WhereClauseContext* SparqlParser::AskQueryContext::whereClause() {
  return getRuleContext<SparqlParser::WhereClauseContext>(0);
}

SparqlParser::SolutionModifierContext* SparqlParser::AskQueryContext::solutionModifier() {
  return getRuleContext<SparqlParser::SolutionModifierContext>(0);
}

std::vector<SparqlParser::DatasetClauseContext *> SparqlParser::AskQueryContext::datasetClause() {
  return getRuleContexts<SparqlParser::DatasetClauseContext>();
}

SparqlParser::DatasetClauseContext* SparqlParser::AskQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlParser::DatasetClauseContext>(i);
}


size_t SparqlParser::AskQueryContext::getRuleIndex() const {
  return SparqlParser::RuleAskQuery;
}

void SparqlParser::AskQueryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAskQuery(this);
}

void SparqlParser::AskQueryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAskQuery(this);
}


antlrcpp::Any SparqlParser::AskQueryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitAskQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::AskQueryContext* SparqlParser::askQuery() {
  AskQueryContext *_localctx = _tracker.createInstance<AskQueryContext>(_ctx, getState());
  enterRule(_localctx, 20, SparqlParser::RuleAskQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(340);
    match(SparqlParser::ASK);
    setState(344);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::FROM) {
      setState(341);
      datasetClause();
      setState(346);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(347);
    whereClause();
    setState(348);
    solutionModifier();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DatasetClauseContext ------------------------------------------------------------------

SparqlParser::DatasetClauseContext::DatasetClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::DatasetClauseContext::FROM() {
  return getToken(SparqlParser::FROM, 0);
}

SparqlParser::DefaultGraphClauseContext* SparqlParser::DatasetClauseContext::defaultGraphClause() {
  return getRuleContext<SparqlParser::DefaultGraphClauseContext>(0);
}

SparqlParser::NamedGraphClauseContext* SparqlParser::DatasetClauseContext::namedGraphClause() {
  return getRuleContext<SparqlParser::NamedGraphClauseContext>(0);
}


size_t SparqlParser::DatasetClauseContext::getRuleIndex() const {
  return SparqlParser::RuleDatasetClause;
}

void SparqlParser::DatasetClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDatasetClause(this);
}

void SparqlParser::DatasetClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDatasetClause(this);
}


antlrcpp::Any SparqlParser::DatasetClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitDatasetClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DatasetClauseContext* SparqlParser::datasetClause() {
  DatasetClauseContext *_localctx = _tracker.createInstance<DatasetClauseContext>(_ctx, getState());
  enterRule(_localctx, 22, SparqlParser::RuleDatasetClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(350);
    match(SparqlParser::FROM);
    setState(353);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        setState(351);
        defaultGraphClause();
        break;
      }

      case SparqlParser::NAMED: {
        setState(352);
        namedGraphClause();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DefaultGraphClauseContext ------------------------------------------------------------------

SparqlParser::DefaultGraphClauseContext::DefaultGraphClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::SourceSelectorContext* SparqlParser::DefaultGraphClauseContext::sourceSelector() {
  return getRuleContext<SparqlParser::SourceSelectorContext>(0);
}


size_t SparqlParser::DefaultGraphClauseContext::getRuleIndex() const {
  return SparqlParser::RuleDefaultGraphClause;
}

void SparqlParser::DefaultGraphClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDefaultGraphClause(this);
}

void SparqlParser::DefaultGraphClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDefaultGraphClause(this);
}


antlrcpp::Any SparqlParser::DefaultGraphClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitDefaultGraphClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DefaultGraphClauseContext* SparqlParser::defaultGraphClause() {
  DefaultGraphClauseContext *_localctx = _tracker.createInstance<DefaultGraphClauseContext>(_ctx, getState());
  enterRule(_localctx, 24, SparqlParser::RuleDefaultGraphClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(355);
    sourceSelector();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedGraphClauseContext ------------------------------------------------------------------

SparqlParser::NamedGraphClauseContext::NamedGraphClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NamedGraphClauseContext::NAMED() {
  return getToken(SparqlParser::NAMED, 0);
}

SparqlParser::SourceSelectorContext* SparqlParser::NamedGraphClauseContext::sourceSelector() {
  return getRuleContext<SparqlParser::SourceSelectorContext>(0);
}


size_t SparqlParser::NamedGraphClauseContext::getRuleIndex() const {
  return SparqlParser::RuleNamedGraphClause;
}

void SparqlParser::NamedGraphClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNamedGraphClause(this);
}

void SparqlParser::NamedGraphClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNamedGraphClause(this);
}


antlrcpp::Any SparqlParser::NamedGraphClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNamedGraphClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NamedGraphClauseContext* SparqlParser::namedGraphClause() {
  NamedGraphClauseContext *_localctx = _tracker.createInstance<NamedGraphClauseContext>(_ctx, getState());
  enterRule(_localctx, 26, SparqlParser::RuleNamedGraphClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(357);
    match(SparqlParser::NAMED);
    setState(358);
    sourceSelector();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SourceSelectorContext ------------------------------------------------------------------

SparqlParser::SourceSelectorContext::SourceSelectorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::SourceSelectorContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}


size_t SparqlParser::SourceSelectorContext::getRuleIndex() const {
  return SparqlParser::RuleSourceSelector;
}

void SparqlParser::SourceSelectorContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSourceSelector(this);
}

void SparqlParser::SourceSelectorContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSourceSelector(this);
}


antlrcpp::Any SparqlParser::SourceSelectorContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitSourceSelector(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SourceSelectorContext* SparqlParser::sourceSelector() {
  SourceSelectorContext *_localctx = _tracker.createInstance<SourceSelectorContext>(_ctx, getState());
  enterRule(_localctx, 28, SparqlParser::RuleSourceSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(360);
    iri();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhereClauseContext ------------------------------------------------------------------

SparqlParser::WhereClauseContext::WhereClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GroupGraphPatternContext* SparqlParser::WhereClauseContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}

tree::TerminalNode* SparqlParser::WhereClauseContext::WHERE() {
  return getToken(SparqlParser::WHERE, 0);
}


size_t SparqlParser::WhereClauseContext::getRuleIndex() const {
  return SparqlParser::RuleWhereClause;
}

void SparqlParser::WhereClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterWhereClause(this);
}

void SparqlParser::WhereClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitWhereClause(this);
}


antlrcpp::Any SparqlParser::WhereClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitWhereClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::WhereClauseContext* SparqlParser::whereClause() {
  WhereClauseContext *_localctx = _tracker.createInstance<WhereClauseContext>(_ctx, getState());
  enterRule(_localctx, 30, SparqlParser::RuleWhereClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(363);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::WHERE) {
      setState(362);
      match(SparqlParser::WHERE);
    }
    setState(365);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SolutionModifierContext ------------------------------------------------------------------

SparqlParser::SolutionModifierContext::SolutionModifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GroupClauseContext* SparqlParser::SolutionModifierContext::groupClause() {
  return getRuleContext<SparqlParser::GroupClauseContext>(0);
}

SparqlParser::HavingClauseContext* SparqlParser::SolutionModifierContext::havingClause() {
  return getRuleContext<SparqlParser::HavingClauseContext>(0);
}

SparqlParser::OrderClauseContext* SparqlParser::SolutionModifierContext::orderClause() {
  return getRuleContext<SparqlParser::OrderClauseContext>(0);
}

SparqlParser::LimitOffsetClausesContext* SparqlParser::SolutionModifierContext::limitOffsetClauses() {
  return getRuleContext<SparqlParser::LimitOffsetClausesContext>(0);
}


size_t SparqlParser::SolutionModifierContext::getRuleIndex() const {
  return SparqlParser::RuleSolutionModifier;
}

void SparqlParser::SolutionModifierContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSolutionModifier(this);
}

void SparqlParser::SolutionModifierContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSolutionModifier(this);
}


antlrcpp::Any SparqlParser::SolutionModifierContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitSolutionModifier(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SolutionModifierContext* SparqlParser::solutionModifier() {
  SolutionModifierContext *_localctx = _tracker.createInstance<SolutionModifierContext>(_ctx, getState());
  enterRule(_localctx, 32, SparqlParser::RuleSolutionModifier);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(368);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::GROUPBY) {
      setState(367);
      groupClause();
    }
    setState(371);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::HAVING) {
      setState(370);
      havingClause();
    }
    setState(374);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::ORDERBY) {
      setState(373);
      orderClause();
    }
    setState(377);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::LIMIT

    || _la == SparqlParser::OFFSET) {
      setState(376);
      limitOffsetClauses();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupClauseContext ------------------------------------------------------------------

SparqlParser::GroupClauseContext::GroupClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::GroupClauseContext::GROUPBY() {
  return getToken(SparqlParser::GROUPBY, 0);
}

std::vector<SparqlParser::GroupConditionContext *> SparqlParser::GroupClauseContext::groupCondition() {
  return getRuleContexts<SparqlParser::GroupConditionContext>();
}

SparqlParser::GroupConditionContext* SparqlParser::GroupClauseContext::groupCondition(size_t i) {
  return getRuleContext<SparqlParser::GroupConditionContext>(i);
}


size_t SparqlParser::GroupClauseContext::getRuleIndex() const {
  return SparqlParser::RuleGroupClause;
}

void SparqlParser::GroupClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupClause(this);
}

void SparqlParser::GroupClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupClause(this);
}


antlrcpp::Any SparqlParser::GroupClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGroupClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupClauseContext* SparqlParser::groupClause() {
  GroupClauseContext *_localctx = _tracker.createInstance<GroupClauseContext>(_ctx, getState());
  enterRule(_localctx, 34, SparqlParser::RuleGroupClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(379);
    match(SparqlParser::GROUPBY);
    setState(381); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(380);
      groupCondition();
      setState(383); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (_la == SparqlParser::T__1

    || _la == SparqlParser::GROUP_CONCAT || ((((_la - 76) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 76)) & ((1ULL << (SparqlParser::NOT - 76))
      | (1ULL << (SparqlParser::STR - 76))
      | (1ULL << (SparqlParser::LANG - 76))
      | (1ULL << (SparqlParser::LANGMATCHES - 76))
      | (1ULL << (SparqlParser::DATATYPE - 76))
      | (1ULL << (SparqlParser::BOUND - 76))
      | (1ULL << (SparqlParser::IRI - 76))
      | (1ULL << (SparqlParser::URI - 76))
      | (1ULL << (SparqlParser::BNODE - 76))
      | (1ULL << (SparqlParser::RAND - 76))
      | (1ULL << (SparqlParser::ABS - 76))
      | (1ULL << (SparqlParser::CEIL - 76))
      | (1ULL << (SparqlParser::FLOOR - 76))
      | (1ULL << (SparqlParser::ROUND - 76))
      | (1ULL << (SparqlParser::CONCAT - 76))
      | (1ULL << (SparqlParser::STRLEN - 76))
      | (1ULL << (SparqlParser::UCASE - 76))
      | (1ULL << (SparqlParser::LCASE - 76))
      | (1ULL << (SparqlParser::ENCODE - 76))
      | (1ULL << (SparqlParser::CONTAINS - 76))
      | (1ULL << (SparqlParser::STRSTARTS - 76))
      | (1ULL << (SparqlParser::STRENDS - 76))
      | (1ULL << (SparqlParser::STRBEFORE - 76))
      | (1ULL << (SparqlParser::STRAFTER - 76))
      | (1ULL << (SparqlParser::YEAR - 76))
      | (1ULL << (SparqlParser::MONTH - 76))
      | (1ULL << (SparqlParser::DAY - 76))
      | (1ULL << (SparqlParser::HOURS - 76))
      | (1ULL << (SparqlParser::MINUTES - 76))
      | (1ULL << (SparqlParser::SECONDS - 76))
      | (1ULL << (SparqlParser::TIMEZONE - 76))
      | (1ULL << (SparqlParser::TZ - 76))
      | (1ULL << (SparqlParser::NOW - 76))
      | (1ULL << (SparqlParser::UUID - 76))
      | (1ULL << (SparqlParser::STRUUID - 76))
      | (1ULL << (SparqlParser::SHA1 - 76))
      | (1ULL << (SparqlParser::SHA256 - 76))
      | (1ULL << (SparqlParser::SHA384 - 76))
      | (1ULL << (SparqlParser::SHA512 - 76))
      | (1ULL << (SparqlParser::MD5 - 76))
      | (1ULL << (SparqlParser::COALESCE - 76))
      | (1ULL << (SparqlParser::IF - 76))
      | (1ULL << (SparqlParser::STRLANG - 76))
      | (1ULL << (SparqlParser::STRDT - 76))
      | (1ULL << (SparqlParser::SAMETERM - 76))
      | (1ULL << (SparqlParser::ISIRI - 76))
      | (1ULL << (SparqlParser::ISURI - 76))
      | (1ULL << (SparqlParser::ISBLANK - 76))
      | (1ULL << (SparqlParser::ISLITERAL - 76))
      | (1ULL << (SparqlParser::ISNUMERIC - 76))
      | (1ULL << (SparqlParser::REGEX - 76))
      | (1ULL << (SparqlParser::SUBSTR - 76))
      | (1ULL << (SparqlParser::REPLACE - 76))
      | (1ULL << (SparqlParser::EXISTS - 76))
      | (1ULL << (SparqlParser::COUNT - 76))
      | (1ULL << (SparqlParser::SUM - 76))
      | (1ULL << (SparqlParser::MIN - 76))
      | (1ULL << (SparqlParser::MAX - 76))
      | (1ULL << (SparqlParser::AVG - 76))
      | (1ULL << (SparqlParser::SAMPLE - 76))
      | (1ULL << (SparqlParser::IRI_REF - 76)))) != 0) || ((((_la - 140) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 140)) & ((1ULL << (SparqlParser::PNAME_NS - 140))
      | (1ULL << (SparqlParser::VAR1 - 140))
      | (1ULL << (SparqlParser::VAR2 - 140)))) != 0));
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupConditionContext ------------------------------------------------------------------

SparqlParser::GroupConditionContext::GroupConditionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BuiltInCallContext* SparqlParser::GroupConditionContext::builtInCall() {
  return getRuleContext<SparqlParser::BuiltInCallContext>(0);
}

SparqlParser::FunctionCallContext* SparqlParser::GroupConditionContext::functionCall() {
  return getRuleContext<SparqlParser::FunctionCallContext>(0);
}

SparqlParser::ExpressionContext* SparqlParser::GroupConditionContext::expression() {
  return getRuleContext<SparqlParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::GroupConditionContext::AS() {
  return getToken(SparqlParser::AS, 0);
}

SparqlParser::VarContext* SparqlParser::GroupConditionContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}


size_t SparqlParser::GroupConditionContext::getRuleIndex() const {
  return SparqlParser::RuleGroupCondition;
}

void SparqlParser::GroupConditionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupCondition(this);
}

void SparqlParser::GroupConditionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupCondition(this);
}


antlrcpp::Any SparqlParser::GroupConditionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGroupCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupConditionContext* SparqlParser::groupCondition() {
  GroupConditionContext *_localctx = _tracker.createInstance<GroupConditionContext>(_ctx, getState());
  enterRule(_localctx, 36, SparqlParser::RuleGroupCondition);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(396);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::GROUP_CONCAT:
      case SparqlParser::NOT:
      case SparqlParser::STR:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::DATATYPE:
      case SparqlParser::BOUND:
      case SparqlParser::IRI:
      case SparqlParser::URI:
      case SparqlParser::BNODE:
      case SparqlParser::RAND:
      case SparqlParser::ABS:
      case SparqlParser::CEIL:
      case SparqlParser::FLOOR:
      case SparqlParser::ROUND:
      case SparqlParser::CONCAT:
      case SparqlParser::STRLEN:
      case SparqlParser::UCASE:
      case SparqlParser::LCASE:
      case SparqlParser::ENCODE:
      case SparqlParser::CONTAINS:
      case SparqlParser::STRSTARTS:
      case SparqlParser::STRENDS:
      case SparqlParser::STRBEFORE:
      case SparqlParser::STRAFTER:
      case SparqlParser::YEAR:
      case SparqlParser::MONTH:
      case SparqlParser::DAY:
      case SparqlParser::HOURS:
      case SparqlParser::MINUTES:
      case SparqlParser::SECONDS:
      case SparqlParser::TIMEZONE:
      case SparqlParser::TZ:
      case SparqlParser::NOW:
      case SparqlParser::UUID:
      case SparqlParser::STRUUID:
      case SparqlParser::SHA1:
      case SparqlParser::SHA256:
      case SparqlParser::SHA384:
      case SparqlParser::SHA512:
      case SparqlParser::MD5:
      case SparqlParser::COALESCE:
      case SparqlParser::IF:
      case SparqlParser::STRLANG:
      case SparqlParser::STRDT:
      case SparqlParser::SAMETERM:
      case SparqlParser::ISIRI:
      case SparqlParser::ISURI:
      case SparqlParser::ISBLANK:
      case SparqlParser::ISLITERAL:
      case SparqlParser::ISNUMERIC:
      case SparqlParser::REGEX:
      case SparqlParser::SUBSTR:
      case SparqlParser::REPLACE:
      case SparqlParser::EXISTS:
      case SparqlParser::COUNT:
      case SparqlParser::SUM:
      case SparqlParser::MIN:
      case SparqlParser::MAX:
      case SparqlParser::AVG:
      case SparqlParser::SAMPLE: {
        enterOuterAlt(_localctx, 1);
        setState(385);
        builtInCall();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 2);
        setState(386);
        functionCall();
        break;
      }

      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 3);
        setState(387);
        match(SparqlParser::T__1);
        setState(388);
        expression();
        setState(391);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::AS) {
          setState(389);
          match(SparqlParser::AS);
          setState(390);
          var();
        }
        setState(393);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 4);
        setState(395);
        var();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HavingClauseContext ------------------------------------------------------------------

SparqlParser::HavingClauseContext::HavingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::HavingClauseContext::HAVING() {
  return getToken(SparqlParser::HAVING, 0);
}

std::vector<SparqlParser::HavingConditionContext *> SparqlParser::HavingClauseContext::havingCondition() {
  return getRuleContexts<SparqlParser::HavingConditionContext>();
}

SparqlParser::HavingConditionContext* SparqlParser::HavingClauseContext::havingCondition(size_t i) {
  return getRuleContext<SparqlParser::HavingConditionContext>(i);
}


size_t SparqlParser::HavingClauseContext::getRuleIndex() const {
  return SparqlParser::RuleHavingClause;
}

void SparqlParser::HavingClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterHavingClause(this);
}

void SparqlParser::HavingClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitHavingClause(this);
}


antlrcpp::Any SparqlParser::HavingClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitHavingClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::HavingClauseContext* SparqlParser::havingClause() {
  HavingClauseContext *_localctx = _tracker.createInstance<HavingClauseContext>(_ctx, getState());
  enterRule(_localctx, 38, SparqlParser::RuleHavingClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(398);
    match(SparqlParser::HAVING);
    setState(400); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(399);
      havingCondition();
      setState(402); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (_la == SparqlParser::T__1

    || _la == SparqlParser::GROUP_CONCAT || ((((_la - 76) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 76)) & ((1ULL << (SparqlParser::NOT - 76))
      | (1ULL << (SparqlParser::STR - 76))
      | (1ULL << (SparqlParser::LANG - 76))
      | (1ULL << (SparqlParser::LANGMATCHES - 76))
      | (1ULL << (SparqlParser::DATATYPE - 76))
      | (1ULL << (SparqlParser::BOUND - 76))
      | (1ULL << (SparqlParser::IRI - 76))
      | (1ULL << (SparqlParser::URI - 76))
      | (1ULL << (SparqlParser::BNODE - 76))
      | (1ULL << (SparqlParser::RAND - 76))
      | (1ULL << (SparqlParser::ABS - 76))
      | (1ULL << (SparqlParser::CEIL - 76))
      | (1ULL << (SparqlParser::FLOOR - 76))
      | (1ULL << (SparqlParser::ROUND - 76))
      | (1ULL << (SparqlParser::CONCAT - 76))
      | (1ULL << (SparqlParser::STRLEN - 76))
      | (1ULL << (SparqlParser::UCASE - 76))
      | (1ULL << (SparqlParser::LCASE - 76))
      | (1ULL << (SparqlParser::ENCODE - 76))
      | (1ULL << (SparqlParser::CONTAINS - 76))
      | (1ULL << (SparqlParser::STRSTARTS - 76))
      | (1ULL << (SparqlParser::STRENDS - 76))
      | (1ULL << (SparqlParser::STRBEFORE - 76))
      | (1ULL << (SparqlParser::STRAFTER - 76))
      | (1ULL << (SparqlParser::YEAR - 76))
      | (1ULL << (SparqlParser::MONTH - 76))
      | (1ULL << (SparqlParser::DAY - 76))
      | (1ULL << (SparqlParser::HOURS - 76))
      | (1ULL << (SparqlParser::MINUTES - 76))
      | (1ULL << (SparqlParser::SECONDS - 76))
      | (1ULL << (SparqlParser::TIMEZONE - 76))
      | (1ULL << (SparqlParser::TZ - 76))
      | (1ULL << (SparqlParser::NOW - 76))
      | (1ULL << (SparqlParser::UUID - 76))
      | (1ULL << (SparqlParser::STRUUID - 76))
      | (1ULL << (SparqlParser::SHA1 - 76))
      | (1ULL << (SparqlParser::SHA256 - 76))
      | (1ULL << (SparqlParser::SHA384 - 76))
      | (1ULL << (SparqlParser::SHA512 - 76))
      | (1ULL << (SparqlParser::MD5 - 76))
      | (1ULL << (SparqlParser::COALESCE - 76))
      | (1ULL << (SparqlParser::IF - 76))
      | (1ULL << (SparqlParser::STRLANG - 76))
      | (1ULL << (SparqlParser::STRDT - 76))
      | (1ULL << (SparqlParser::SAMETERM - 76))
      | (1ULL << (SparqlParser::ISIRI - 76))
      | (1ULL << (SparqlParser::ISURI - 76))
      | (1ULL << (SparqlParser::ISBLANK - 76))
      | (1ULL << (SparqlParser::ISLITERAL - 76))
      | (1ULL << (SparqlParser::ISNUMERIC - 76))
      | (1ULL << (SparqlParser::REGEX - 76))
      | (1ULL << (SparqlParser::SUBSTR - 76))
      | (1ULL << (SparqlParser::REPLACE - 76))
      | (1ULL << (SparqlParser::EXISTS - 76))
      | (1ULL << (SparqlParser::COUNT - 76))
      | (1ULL << (SparqlParser::SUM - 76))
      | (1ULL << (SparqlParser::MIN - 76))
      | (1ULL << (SparqlParser::MAX - 76))
      | (1ULL << (SparqlParser::AVG - 76))
      | (1ULL << (SparqlParser::SAMPLE - 76))
      | (1ULL << (SparqlParser::IRI_REF - 76)))) != 0) || _la == SparqlParser::PNAME_NS);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HavingConditionContext ------------------------------------------------------------------

SparqlParser::HavingConditionContext::HavingConditionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::ConstraintContext* SparqlParser::HavingConditionContext::constraint() {
  return getRuleContext<SparqlParser::ConstraintContext>(0);
}


size_t SparqlParser::HavingConditionContext::getRuleIndex() const {
  return SparqlParser::RuleHavingCondition;
}

void SparqlParser::HavingConditionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterHavingCondition(this);
}

void SparqlParser::HavingConditionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitHavingCondition(this);
}


antlrcpp::Any SparqlParser::HavingConditionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitHavingCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::HavingConditionContext* SparqlParser::havingCondition() {
  HavingConditionContext *_localctx = _tracker.createInstance<HavingConditionContext>(_ctx, getState());
  enterRule(_localctx, 40, SparqlParser::RuleHavingCondition);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(404);
    constraint();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderClauseContext ------------------------------------------------------------------

SparqlParser::OrderClauseContext::OrderClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::OrderClauseContext::ORDERBY() {
  return getToken(SparqlParser::ORDERBY, 0);
}

std::vector<SparqlParser::OrderConditionContext *> SparqlParser::OrderClauseContext::orderCondition() {
  return getRuleContexts<SparqlParser::OrderConditionContext>();
}

SparqlParser::OrderConditionContext* SparqlParser::OrderClauseContext::orderCondition(size_t i) {
  return getRuleContext<SparqlParser::OrderConditionContext>(i);
}


size_t SparqlParser::OrderClauseContext::getRuleIndex() const {
  return SparqlParser::RuleOrderClause;
}

void SparqlParser::OrderClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOrderClause(this);
}

void SparqlParser::OrderClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOrderClause(this);
}


antlrcpp::Any SparqlParser::OrderClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitOrderClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OrderClauseContext* SparqlParser::orderClause() {
  OrderClauseContext *_localctx = _tracker.createInstance<OrderClauseContext>(_ctx, getState());
  enterRule(_localctx, 42, SparqlParser::RuleOrderClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(406);
    match(SparqlParser::ORDERBY);
    setState(408); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(407);
      orderCondition();
      setState(410); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
      | (1ULL << SparqlParser::GROUP_CONCAT)
      | (1ULL << SparqlParser::ASC)
      | (1ULL << SparqlParser::DESC))) != 0) || ((((_la - 76) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 76)) & ((1ULL << (SparqlParser::NOT - 76))
      | (1ULL << (SparqlParser::STR - 76))
      | (1ULL << (SparqlParser::LANG - 76))
      | (1ULL << (SparqlParser::LANGMATCHES - 76))
      | (1ULL << (SparqlParser::DATATYPE - 76))
      | (1ULL << (SparqlParser::BOUND - 76))
      | (1ULL << (SparqlParser::IRI - 76))
      | (1ULL << (SparqlParser::URI - 76))
      | (1ULL << (SparqlParser::BNODE - 76))
      | (1ULL << (SparqlParser::RAND - 76))
      | (1ULL << (SparqlParser::ABS - 76))
      | (1ULL << (SparqlParser::CEIL - 76))
      | (1ULL << (SparqlParser::FLOOR - 76))
      | (1ULL << (SparqlParser::ROUND - 76))
      | (1ULL << (SparqlParser::CONCAT - 76))
      | (1ULL << (SparqlParser::STRLEN - 76))
      | (1ULL << (SparqlParser::UCASE - 76))
      | (1ULL << (SparqlParser::LCASE - 76))
      | (1ULL << (SparqlParser::ENCODE - 76))
      | (1ULL << (SparqlParser::CONTAINS - 76))
      | (1ULL << (SparqlParser::STRSTARTS - 76))
      | (1ULL << (SparqlParser::STRENDS - 76))
      | (1ULL << (SparqlParser::STRBEFORE - 76))
      | (1ULL << (SparqlParser::STRAFTER - 76))
      | (1ULL << (SparqlParser::YEAR - 76))
      | (1ULL << (SparqlParser::MONTH - 76))
      | (1ULL << (SparqlParser::DAY - 76))
      | (1ULL << (SparqlParser::HOURS - 76))
      | (1ULL << (SparqlParser::MINUTES - 76))
      | (1ULL << (SparqlParser::SECONDS - 76))
      | (1ULL << (SparqlParser::TIMEZONE - 76))
      | (1ULL << (SparqlParser::TZ - 76))
      | (1ULL << (SparqlParser::NOW - 76))
      | (1ULL << (SparqlParser::UUID - 76))
      | (1ULL << (SparqlParser::STRUUID - 76))
      | (1ULL << (SparqlParser::SHA1 - 76))
      | (1ULL << (SparqlParser::SHA256 - 76))
      | (1ULL << (SparqlParser::SHA384 - 76))
      | (1ULL << (SparqlParser::SHA512 - 76))
      | (1ULL << (SparqlParser::MD5 - 76))
      | (1ULL << (SparqlParser::COALESCE - 76))
      | (1ULL << (SparqlParser::IF - 76))
      | (1ULL << (SparqlParser::STRLANG - 76))
      | (1ULL << (SparqlParser::STRDT - 76))
      | (1ULL << (SparqlParser::SAMETERM - 76))
      | (1ULL << (SparqlParser::ISIRI - 76))
      | (1ULL << (SparqlParser::ISURI - 76))
      | (1ULL << (SparqlParser::ISBLANK - 76))
      | (1ULL << (SparqlParser::ISLITERAL - 76))
      | (1ULL << (SparqlParser::ISNUMERIC - 76))
      | (1ULL << (SparqlParser::REGEX - 76))
      | (1ULL << (SparqlParser::SUBSTR - 76))
      | (1ULL << (SparqlParser::REPLACE - 76))
      | (1ULL << (SparqlParser::EXISTS - 76))
      | (1ULL << (SparqlParser::COUNT - 76))
      | (1ULL << (SparqlParser::SUM - 76))
      | (1ULL << (SparqlParser::MIN - 76))
      | (1ULL << (SparqlParser::MAX - 76))
      | (1ULL << (SparqlParser::AVG - 76))
      | (1ULL << (SparqlParser::SAMPLE - 76))
      | (1ULL << (SparqlParser::IRI_REF - 76)))) != 0) || ((((_la - 140) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 140)) & ((1ULL << (SparqlParser::PNAME_NS - 140))
      | (1ULL << (SparqlParser::VAR1 - 140))
      | (1ULL << (SparqlParser::VAR2 - 140)))) != 0));
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderConditionContext ------------------------------------------------------------------

SparqlParser::OrderConditionContext::OrderConditionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BrackettedExpressionContext* SparqlParser::OrderConditionContext::brackettedExpression() {
  return getRuleContext<SparqlParser::BrackettedExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::OrderConditionContext::ASC() {
  return getToken(SparqlParser::ASC, 0);
}

tree::TerminalNode* SparqlParser::OrderConditionContext::DESC() {
  return getToken(SparqlParser::DESC, 0);
}

SparqlParser::ConstraintContext* SparqlParser::OrderConditionContext::constraint() {
  return getRuleContext<SparqlParser::ConstraintContext>(0);
}

SparqlParser::VarContext* SparqlParser::OrderConditionContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}


size_t SparqlParser::OrderConditionContext::getRuleIndex() const {
  return SparqlParser::RuleOrderCondition;
}

void SparqlParser::OrderConditionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOrderCondition(this);
}

void SparqlParser::OrderConditionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOrderCondition(this);
}


antlrcpp::Any SparqlParser::OrderConditionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitOrderCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OrderConditionContext* SparqlParser::orderCondition() {
  OrderConditionContext *_localctx = _tracker.createInstance<OrderConditionContext>(_ctx, getState());
  enterRule(_localctx, 44, SparqlParser::RuleOrderCondition);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(418);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::ASC:
      case SparqlParser::DESC: {
        enterOuterAlt(_localctx, 1);
        setState(412);
        _la = _input->LA(1);
        if (!(_la == SparqlParser::ASC

        || _la == SparqlParser::DESC)) {
        _errHandler->recoverInline(this);
        }
        else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(413);
        brackettedExpression();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::GROUP_CONCAT:
      case SparqlParser::NOT:
      case SparqlParser::STR:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::DATATYPE:
      case SparqlParser::BOUND:
      case SparqlParser::IRI:
      case SparqlParser::URI:
      case SparqlParser::BNODE:
      case SparqlParser::RAND:
      case SparqlParser::ABS:
      case SparqlParser::CEIL:
      case SparqlParser::FLOOR:
      case SparqlParser::ROUND:
      case SparqlParser::CONCAT:
      case SparqlParser::STRLEN:
      case SparqlParser::UCASE:
      case SparqlParser::LCASE:
      case SparqlParser::ENCODE:
      case SparqlParser::CONTAINS:
      case SparqlParser::STRSTARTS:
      case SparqlParser::STRENDS:
      case SparqlParser::STRBEFORE:
      case SparqlParser::STRAFTER:
      case SparqlParser::YEAR:
      case SparqlParser::MONTH:
      case SparqlParser::DAY:
      case SparqlParser::HOURS:
      case SparqlParser::MINUTES:
      case SparqlParser::SECONDS:
      case SparqlParser::TIMEZONE:
      case SparqlParser::TZ:
      case SparqlParser::NOW:
      case SparqlParser::UUID:
      case SparqlParser::STRUUID:
      case SparqlParser::SHA1:
      case SparqlParser::SHA256:
      case SparqlParser::SHA384:
      case SparqlParser::SHA512:
      case SparqlParser::MD5:
      case SparqlParser::COALESCE:
      case SparqlParser::IF:
      case SparqlParser::STRLANG:
      case SparqlParser::STRDT:
      case SparqlParser::SAMETERM:
      case SparqlParser::ISIRI:
      case SparqlParser::ISURI:
      case SparqlParser::ISBLANK:
      case SparqlParser::ISLITERAL:
      case SparqlParser::ISNUMERIC:
      case SparqlParser::REGEX:
      case SparqlParser::SUBSTR:
      case SparqlParser::REPLACE:
      case SparqlParser::EXISTS:
      case SparqlParser::COUNT:
      case SparqlParser::SUM:
      case SparqlParser::MIN:
      case SparqlParser::MAX:
      case SparqlParser::AVG:
      case SparqlParser::SAMPLE:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 2);
        setState(416);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlParser::T__1:
          case SparqlParser::GROUP_CONCAT:
          case SparqlParser::NOT:
          case SparqlParser::STR:
          case SparqlParser::LANG:
          case SparqlParser::LANGMATCHES:
          case SparqlParser::DATATYPE:
          case SparqlParser::BOUND:
          case SparqlParser::IRI:
          case SparqlParser::URI:
          case SparqlParser::BNODE:
          case SparqlParser::RAND:
          case SparqlParser::ABS:
          case SparqlParser::CEIL:
          case SparqlParser::FLOOR:
          case SparqlParser::ROUND:
          case SparqlParser::CONCAT:
          case SparqlParser::STRLEN:
          case SparqlParser::UCASE:
          case SparqlParser::LCASE:
          case SparqlParser::ENCODE:
          case SparqlParser::CONTAINS:
          case SparqlParser::STRSTARTS:
          case SparqlParser::STRENDS:
          case SparqlParser::STRBEFORE:
          case SparqlParser::STRAFTER:
          case SparqlParser::YEAR:
          case SparqlParser::MONTH:
          case SparqlParser::DAY:
          case SparqlParser::HOURS:
          case SparqlParser::MINUTES:
          case SparqlParser::SECONDS:
          case SparqlParser::TIMEZONE:
          case SparqlParser::TZ:
          case SparqlParser::NOW:
          case SparqlParser::UUID:
          case SparqlParser::STRUUID:
          case SparqlParser::SHA1:
          case SparqlParser::SHA256:
          case SparqlParser::SHA384:
          case SparqlParser::SHA512:
          case SparqlParser::MD5:
          case SparqlParser::COALESCE:
          case SparqlParser::IF:
          case SparqlParser::STRLANG:
          case SparqlParser::STRDT:
          case SparqlParser::SAMETERM:
          case SparqlParser::ISIRI:
          case SparqlParser::ISURI:
          case SparqlParser::ISBLANK:
          case SparqlParser::ISLITERAL:
          case SparqlParser::ISNUMERIC:
          case SparqlParser::REGEX:
          case SparqlParser::SUBSTR:
          case SparqlParser::REPLACE:
          case SparqlParser::EXISTS:
          case SparqlParser::COUNT:
          case SparqlParser::SUM:
          case SparqlParser::MIN:
          case SparqlParser::MAX:
          case SparqlParser::AVG:
          case SparqlParser::SAMPLE:
          case SparqlParser::IRI_REF:
          case SparqlParser::PNAME_NS: {
            setState(414);
            constraint();
            break;
          }

          case SparqlParser::VAR1:
          case SparqlParser::VAR2: {
            setState(415);
            var();
            break;
          }

        default:
          throw NoViableAltException(this);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitOffsetClausesContext ------------------------------------------------------------------

SparqlParser::LimitOffsetClausesContext::LimitOffsetClausesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::LimitClauseContext* SparqlParser::LimitOffsetClausesContext::limitClause() {
  return getRuleContext<SparqlParser::LimitClauseContext>(0);
}

SparqlParser::OffsetClauseContext* SparqlParser::LimitOffsetClausesContext::offsetClause() {
  return getRuleContext<SparqlParser::OffsetClauseContext>(0);
}


size_t SparqlParser::LimitOffsetClausesContext::getRuleIndex() const {
  return SparqlParser::RuleLimitOffsetClauses;
}

void SparqlParser::LimitOffsetClausesContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLimitOffsetClauses(this);
}

void SparqlParser::LimitOffsetClausesContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLimitOffsetClauses(this);
}


antlrcpp::Any SparqlParser::LimitOffsetClausesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitLimitOffsetClauses(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::LimitOffsetClausesContext* SparqlParser::limitOffsetClauses() {
  LimitOffsetClausesContext *_localctx = _tracker.createInstance<LimitOffsetClausesContext>(_ctx, getState());
  enterRule(_localctx, 46, SparqlParser::RuleLimitOffsetClauses);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(428);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::LIMIT: {
        enterOuterAlt(_localctx, 1);
        setState(420);
        limitClause();
        setState(422);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::OFFSET) {
          setState(421);
          offsetClause();
        }
        break;
      }

      case SparqlParser::OFFSET: {
        enterOuterAlt(_localctx, 2);
        setState(424);
        offsetClause();
        setState(426);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::LIMIT) {
          setState(425);
          limitClause();
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitClauseContext ------------------------------------------------------------------

SparqlParser::LimitClauseContext::LimitClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::LimitClauseContext::LIMIT() {
  return getToken(SparqlParser::LIMIT, 0);
}

tree::TerminalNode* SparqlParser::LimitClauseContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}


size_t SparqlParser::LimitClauseContext::getRuleIndex() const {
  return SparqlParser::RuleLimitClause;
}

void SparqlParser::LimitClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterLimitClause(this);
}

void SparqlParser::LimitClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitLimitClause(this);
}


antlrcpp::Any SparqlParser::LimitClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitLimitClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::LimitClauseContext* SparqlParser::limitClause() {
  LimitClauseContext *_localctx = _tracker.createInstance<LimitClauseContext>(_ctx, getState());
  enterRule(_localctx, 48, SparqlParser::RuleLimitClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(430);
    match(SparqlParser::LIMIT);
    setState(431);
    match(SparqlParser::INTEGER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OffsetClauseContext ------------------------------------------------------------------

SparqlParser::OffsetClauseContext::OffsetClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::OffsetClauseContext::OFFSET() {
  return getToken(SparqlParser::OFFSET, 0);
}

tree::TerminalNode* SparqlParser::OffsetClauseContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}


size_t SparqlParser::OffsetClauseContext::getRuleIndex() const {
  return SparqlParser::RuleOffsetClause;
}

void SparqlParser::OffsetClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOffsetClause(this);
}

void SparqlParser::OffsetClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOffsetClause(this);
}


antlrcpp::Any SparqlParser::OffsetClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitOffsetClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OffsetClauseContext* SparqlParser::offsetClause() {
  OffsetClauseContext *_localctx = _tracker.createInstance<OffsetClauseContext>(_ctx, getState());
  enterRule(_localctx, 50, SparqlParser::RuleOffsetClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(433);
    match(SparqlParser::OFFSET);
    setState(434);
    match(SparqlParser::INTEGER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValuesClauseContext ------------------------------------------------------------------

SparqlParser::ValuesClauseContext::ValuesClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ValuesClauseContext::VALUES() {
  return getToken(SparqlParser::VALUES, 0);
}

SparqlParser::DataBlockContext* SparqlParser::ValuesClauseContext::dataBlock() {
  return getRuleContext<SparqlParser::DataBlockContext>(0);
}


size_t SparqlParser::ValuesClauseContext::getRuleIndex() const {
  return SparqlParser::RuleValuesClause;
}

void SparqlParser::ValuesClauseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterValuesClause(this);
}

void SparqlParser::ValuesClauseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitValuesClause(this);
}


antlrcpp::Any SparqlParser::ValuesClauseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitValuesClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ValuesClauseContext* SparqlParser::valuesClause() {
  ValuesClauseContext *_localctx = _tracker.createInstance<ValuesClauseContext>(_ctx, getState());
  enterRule(_localctx, 52, SparqlParser::RuleValuesClause);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(438);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::VALUES) {
      setState(436);
      match(SparqlParser::VALUES);
      setState(437);
      dataBlock();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesTemplateContext ------------------------------------------------------------------

SparqlParser::TriplesTemplateContext::TriplesTemplateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::TriplesSameSubjectContext* SparqlParser::TriplesTemplateContext::triplesSameSubject() {
  return getRuleContext<SparqlParser::TriplesSameSubjectContext>(0);
}

SparqlParser::TriplesTemplateContext* SparqlParser::TriplesTemplateContext::triplesTemplate() {
  return getRuleContext<SparqlParser::TriplesTemplateContext>(0);
}


size_t SparqlParser::TriplesTemplateContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesTemplate;
}

void SparqlParser::TriplesTemplateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesTemplate(this);
}

void SparqlParser::TriplesTemplateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesTemplate(this);
}


antlrcpp::Any SparqlParser::TriplesTemplateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitTriplesTemplate(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesTemplateContext* SparqlParser::triplesTemplate() {
  TriplesTemplateContext *_localctx = _tracker.createInstance<TriplesTemplateContext>(_ctx, getState());
  enterRule(_localctx, 54, SparqlParser::RuleTriplesTemplate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(440);
    triplesSameSubject();
    setState(445);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__5) {
      setState(441);
      match(SparqlParser::T__5);
      setState(443);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
        | (1ULL << SparqlParser::T__16)
        | (1ULL << SparqlParser::T__29)
        | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
        | (1ULL << (SparqlParser::PNAME_NS - 139))
        | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
        | (1ULL << (SparqlParser::VAR1 - 139))
        | (1ULL << (SparqlParser::VAR2 - 139))
        | (1ULL << (SparqlParser::INTEGER - 139))
        | (1ULL << (SparqlParser::DECIMAL - 139))
        | (1ULL << (SparqlParser::DOUBLE - 139))
        | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
        | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
        | (1ULL << (SparqlParser::NIL - 139))
        | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
        setState(442);
        triplesTemplate();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupGraphPatternContext ------------------------------------------------------------------

SparqlParser::GroupGraphPatternContext::GroupGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::SubSelectContext* SparqlParser::GroupGraphPatternContext::subSelect() {
  return getRuleContext<SparqlParser::SubSelectContext>(0);
}

SparqlParser::GroupGraphPatternSubContext* SparqlParser::GroupGraphPatternContext::groupGraphPatternSub() {
  return getRuleContext<SparqlParser::GroupGraphPatternSubContext>(0);
}


size_t SparqlParser::GroupGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleGroupGraphPattern;
}

void SparqlParser::GroupGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupGraphPattern(this);
}

void SparqlParser::GroupGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupGraphPattern(this);
}


antlrcpp::Any SparqlParser::GroupGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGroupGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::groupGraphPattern() {
  GroupGraphPatternContext *_localctx = _tracker.createInstance<GroupGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 56, SparqlParser::RuleGroupGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(447);
    match(SparqlParser::T__3);
    setState(450);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::SELECT: {
        setState(448);
        subSelect();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::T__3:
      case SparqlParser::T__4:
      case SparqlParser::T__16:
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::VALUES:
      case SparqlParser::GRAPH:
      case SparqlParser::OPTIONAL:
      case SparqlParser::SERVICE:
      case SparqlParser::BIND:
      case SparqlParser::MINUS:
      case SparqlParser::FILTER:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        setState(449);
        groupGraphPatternSub();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(452);
    match(SparqlParser::T__4);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupGraphPatternSubContext ------------------------------------------------------------------

SparqlParser::GroupGraphPatternSubContext::GroupGraphPatternSubContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::TriplesBlockContext *> SparqlParser::GroupGraphPatternSubContext::triplesBlock() {
  return getRuleContexts<SparqlParser::TriplesBlockContext>();
}

SparqlParser::TriplesBlockContext* SparqlParser::GroupGraphPatternSubContext::triplesBlock(size_t i) {
  return getRuleContext<SparqlParser::TriplesBlockContext>(i);
}

std::vector<SparqlParser::GraphPatternNotTriplesContext *> SparqlParser::GroupGraphPatternSubContext::graphPatternNotTriples() {
  return getRuleContexts<SparqlParser::GraphPatternNotTriplesContext>();
}

SparqlParser::GraphPatternNotTriplesContext* SparqlParser::GroupGraphPatternSubContext::graphPatternNotTriples(size_t i) {
  return getRuleContext<SparqlParser::GraphPatternNotTriplesContext>(i);
}


size_t SparqlParser::GroupGraphPatternSubContext::getRuleIndex() const {
  return SparqlParser::RuleGroupGraphPatternSub;
}

void SparqlParser::GroupGraphPatternSubContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupGraphPatternSub(this);
}

void SparqlParser::GroupGraphPatternSubContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupGraphPatternSub(this);
}


antlrcpp::Any SparqlParser::GroupGraphPatternSubContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGroupGraphPatternSub(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupGraphPatternSubContext* SparqlParser::groupGraphPatternSub() {
  GroupGraphPatternSubContext *_localctx = _tracker.createInstance<GroupGraphPatternSubContext>(_ctx, getState());
  enterRule(_localctx, 58, SparqlParser::RuleGroupGraphPatternSub);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(455);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
      | (1ULL << SparqlParser::T__16)
      | (1ULL << SparqlParser::T__29)
      | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
      | (1ULL << (SparqlParser::VAR1 - 139))
      | (1ULL << (SparqlParser::VAR2 - 139))
      | (1ULL << (SparqlParser::INTEGER - 139))
      | (1ULL << (SparqlParser::DECIMAL - 139))
      | (1ULL << (SparqlParser::DOUBLE - 139))
      | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
      | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
      | (1ULL << (SparqlParser::NIL - 139))
      | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
      setState(454);
      triplesBlock();
    }
    setState(466);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__3

    || _la == SparqlParser::VALUES || ((((_la - 67) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 67)) & ((1ULL << (SparqlParser::GRAPH - 67))
      | (1ULL << (SparqlParser::OPTIONAL - 67))
      | (1ULL << (SparqlParser::SERVICE - 67))
      | (1ULL << (SparqlParser::BIND - 67))
      | (1ULL << (SparqlParser::MINUS - 67))
      | (1ULL << (SparqlParser::FILTER - 67)))) != 0)) {
      setState(457);
      graphPatternNotTriples();
      setState(459);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SparqlParser::T__5) {
        setState(458);
        match(SparqlParser::T__5);
      }
      setState(462);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
        | (1ULL << SparqlParser::T__16)
        | (1ULL << SparqlParser::T__29)
        | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
        | (1ULL << (SparqlParser::PNAME_NS - 139))
        | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
        | (1ULL << (SparqlParser::VAR1 - 139))
        | (1ULL << (SparqlParser::VAR2 - 139))
        | (1ULL << (SparqlParser::INTEGER - 139))
        | (1ULL << (SparqlParser::DECIMAL - 139))
        | (1ULL << (SparqlParser::DOUBLE - 139))
        | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
        | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
        | (1ULL << (SparqlParser::NIL - 139))
        | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
        setState(461);
        triplesBlock();
      }
      setState(468);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesBlockContext ------------------------------------------------------------------

SparqlParser::TriplesBlockContext::TriplesBlockContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::TriplesSameSubjectPathContext* SparqlParser::TriplesBlockContext::triplesSameSubjectPath() {
  return getRuleContext<SparqlParser::TriplesSameSubjectPathContext>(0);
}

SparqlParser::TriplesBlockContext* SparqlParser::TriplesBlockContext::triplesBlock() {
  return getRuleContext<SparqlParser::TriplesBlockContext>(0);
}


size_t SparqlParser::TriplesBlockContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesBlock;
}

void SparqlParser::TriplesBlockContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesBlock(this);
}

void SparqlParser::TriplesBlockContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesBlock(this);
}


antlrcpp::Any SparqlParser::TriplesBlockContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitTriplesBlock(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesBlockContext* SparqlParser::triplesBlock() {
  TriplesBlockContext *_localctx = _tracker.createInstance<TriplesBlockContext>(_ctx, getState());
  enterRule(_localctx, 60, SparqlParser::RuleTriplesBlock);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(469);
    triplesSameSubjectPath();
    setState(474);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__5) {
      setState(470);
      match(SparqlParser::T__5);
      setState(472);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
        | (1ULL << SparqlParser::T__16)
        | (1ULL << SparqlParser::T__29)
        | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
        | (1ULL << (SparqlParser::PNAME_NS - 139))
        | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
        | (1ULL << (SparqlParser::VAR1 - 139))
        | (1ULL << (SparqlParser::VAR2 - 139))
        | (1ULL << (SparqlParser::INTEGER - 139))
        | (1ULL << (SparqlParser::DECIMAL - 139))
        | (1ULL << (SparqlParser::DOUBLE - 139))
        | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
        | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
        | (1ULL << (SparqlParser::NIL - 139))
        | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
        setState(471);
        triplesBlock();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphPatternNotTriplesContext ------------------------------------------------------------------

SparqlParser::GraphPatternNotTriplesContext::GraphPatternNotTriplesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GroupOrUnionGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::groupOrUnionGraphPattern() {
  return getRuleContext<SparqlParser::GroupOrUnionGraphPatternContext>(0);
}

SparqlParser::OptionalGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::optionalGraphPattern() {
  return getRuleContext<SparqlParser::OptionalGraphPatternContext>(0);
}

SparqlParser::MinusGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::minusGraphPattern() {
  return getRuleContext<SparqlParser::MinusGraphPatternContext>(0);
}

SparqlParser::GraphGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::graphGraphPattern() {
  return getRuleContext<SparqlParser::GraphGraphPatternContext>(0);
}

SparqlParser::ServiceGraphPatternContext* SparqlParser::GraphPatternNotTriplesContext::serviceGraphPattern() {
  return getRuleContext<SparqlParser::ServiceGraphPatternContext>(0);
}

SparqlParser::FilterRContext* SparqlParser::GraphPatternNotTriplesContext::filterR() {
  return getRuleContext<SparqlParser::FilterRContext>(0);
}

SparqlParser::BindContext* SparqlParser::GraphPatternNotTriplesContext::bind() {
  return getRuleContext<SparqlParser::BindContext>(0);
}

SparqlParser::InlineDataContext* SparqlParser::GraphPatternNotTriplesContext::inlineData() {
  return getRuleContext<SparqlParser::InlineDataContext>(0);
}


size_t SparqlParser::GraphPatternNotTriplesContext::getRuleIndex() const {
  return SparqlParser::RuleGraphPatternNotTriples;
}

void SparqlParser::GraphPatternNotTriplesContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphPatternNotTriples(this);
}

void SparqlParser::GraphPatternNotTriplesContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphPatternNotTriples(this);
}


antlrcpp::Any SparqlParser::GraphPatternNotTriplesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGraphPatternNotTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphPatternNotTriplesContext* SparqlParser::graphPatternNotTriples() {
  GraphPatternNotTriplesContext *_localctx = _tracker.createInstance<GraphPatternNotTriplesContext>(_ctx, getState());
  enterRule(_localctx, 62, SparqlParser::RuleGraphPatternNotTriples);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(484);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__3: {
        enterOuterAlt(_localctx, 1);
        setState(476);
        groupOrUnionGraphPattern();
        break;
      }

      case SparqlParser::OPTIONAL: {
        enterOuterAlt(_localctx, 2);
        setState(477);
        optionalGraphPattern();
        break;
      }

      case SparqlParser::MINUS: {
        enterOuterAlt(_localctx, 3);
        setState(478);
        minusGraphPattern();
        break;
      }

      case SparqlParser::GRAPH: {
        enterOuterAlt(_localctx, 4);
        setState(479);
        graphGraphPattern();
        break;
      }

      case SparqlParser::SERVICE: {
        enterOuterAlt(_localctx, 5);
        setState(480);
        serviceGraphPattern();
        break;
      }

      case SparqlParser::FILTER: {
        enterOuterAlt(_localctx, 6);
        setState(481);
        filterR();
        break;
      }

      case SparqlParser::BIND: {
        enterOuterAlt(_localctx, 7);
        setState(482);
        bind();
        break;
      }

      case SparqlParser::VALUES: {
        enterOuterAlt(_localctx, 8);
        setState(483);
        inlineData();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OptionalGraphPatternContext ------------------------------------------------------------------

SparqlParser::OptionalGraphPatternContext::OptionalGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::OptionalGraphPatternContext::OPTIONAL() {
  return getToken(SparqlParser::OPTIONAL, 0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::OptionalGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::OptionalGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleOptionalGraphPattern;
}

void SparqlParser::OptionalGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterOptionalGraphPattern(this);
}

void SparqlParser::OptionalGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitOptionalGraphPattern(this);
}


antlrcpp::Any SparqlParser::OptionalGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitOptionalGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::OptionalGraphPatternContext* SparqlParser::optionalGraphPattern() {
  OptionalGraphPatternContext *_localctx = _tracker.createInstance<OptionalGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 64, SparqlParser::RuleOptionalGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(486);
    match(SparqlParser::OPTIONAL);
    setState(487);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphGraphPatternContext ------------------------------------------------------------------

SparqlParser::GraphGraphPatternContext::GraphGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::GraphGraphPatternContext::GRAPH() {
  return getToken(SparqlParser::GRAPH, 0);
}

SparqlParser::VarOrIriContext* SparqlParser::GraphGraphPatternContext::varOrIri() {
  return getRuleContext<SparqlParser::VarOrIriContext>(0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::GraphGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::GraphGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleGraphGraphPattern;
}

void SparqlParser::GraphGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphGraphPattern(this);
}

void SparqlParser::GraphGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphGraphPattern(this);
}


antlrcpp::Any SparqlParser::GraphGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGraphGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphGraphPatternContext* SparqlParser::graphGraphPattern() {
  GraphGraphPatternContext *_localctx = _tracker.createInstance<GraphGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 66, SparqlParser::RuleGraphGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(489);
    match(SparqlParser::GRAPH);
    setState(490);
    varOrIri();
    setState(491);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ServiceGraphPatternContext ------------------------------------------------------------------

SparqlParser::ServiceGraphPatternContext::ServiceGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ServiceGraphPatternContext::SERVICE() {
  return getToken(SparqlParser::SERVICE, 0);
}

SparqlParser::VarOrIriContext* SparqlParser::ServiceGraphPatternContext::varOrIri() {
  return getRuleContext<SparqlParser::VarOrIriContext>(0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::ServiceGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}

tree::TerminalNode* SparqlParser::ServiceGraphPatternContext::SILENT() {
  return getToken(SparqlParser::SILENT, 0);
}


size_t SparqlParser::ServiceGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleServiceGraphPattern;
}

void SparqlParser::ServiceGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterServiceGraphPattern(this);
}

void SparqlParser::ServiceGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitServiceGraphPattern(this);
}


antlrcpp::Any SparqlParser::ServiceGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitServiceGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ServiceGraphPatternContext* SparqlParser::serviceGraphPattern() {
  ServiceGraphPatternContext *_localctx = _tracker.createInstance<ServiceGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 68, SparqlParser::RuleServiceGraphPattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(493);
    match(SparqlParser::SERVICE);
    setState(495);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::SILENT) {
      setState(494);
      match(SparqlParser::SILENT);
    }
    setState(497);
    varOrIri();
    setState(498);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BindContext ------------------------------------------------------------------

SparqlParser::BindContext::BindContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BindContext::BIND() {
  return getToken(SparqlParser::BIND, 0);
}

SparqlParser::ExpressionContext* SparqlParser::BindContext::expression() {
  return getRuleContext<SparqlParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::BindContext::AS() {
  return getToken(SparqlParser::AS, 0);
}

SparqlParser::VarContext* SparqlParser::BindContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}


size_t SparqlParser::BindContext::getRuleIndex() const {
  return SparqlParser::RuleBind;
}

void SparqlParser::BindContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBind(this);
}

void SparqlParser::BindContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBind(this);
}


antlrcpp::Any SparqlParser::BindContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBind(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BindContext* SparqlParser::bind() {
  BindContext *_localctx = _tracker.createInstance<BindContext>(_ctx, getState());
  enterRule(_localctx, 70, SparqlParser::RuleBind);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(500);
    match(SparqlParser::BIND);
    setState(501);
    match(SparqlParser::T__1);
    setState(502);
    expression();
    setState(503);
    match(SparqlParser::AS);
    setState(504);
    var();
    setState(505);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineDataContext ------------------------------------------------------------------

SparqlParser::InlineDataContext::InlineDataContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::InlineDataContext::VALUES() {
  return getToken(SparqlParser::VALUES, 0);
}

SparqlParser::DataBlockContext* SparqlParser::InlineDataContext::dataBlock() {
  return getRuleContext<SparqlParser::DataBlockContext>(0);
}


size_t SparqlParser::InlineDataContext::getRuleIndex() const {
  return SparqlParser::RuleInlineData;
}

void SparqlParser::InlineDataContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineData(this);
}

void SparqlParser::InlineDataContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineData(this);
}


antlrcpp::Any SparqlParser::InlineDataContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitInlineData(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::InlineDataContext* SparqlParser::inlineData() {
  InlineDataContext *_localctx = _tracker.createInstance<InlineDataContext>(_ctx, getState());
  enterRule(_localctx, 72, SparqlParser::RuleInlineData);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(507);
    match(SparqlParser::VALUES);
    setState(508);
    dataBlock();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataBlockContext ------------------------------------------------------------------

SparqlParser::DataBlockContext::DataBlockContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::InlineDataOneVarContext* SparqlParser::DataBlockContext::inlineDataOneVar() {
  return getRuleContext<SparqlParser::InlineDataOneVarContext>(0);
}

SparqlParser::InlineDataFullContext* SparqlParser::DataBlockContext::inlineDataFull() {
  return getRuleContext<SparqlParser::InlineDataFullContext>(0);
}


size_t SparqlParser::DataBlockContext::getRuleIndex() const {
  return SparqlParser::RuleDataBlock;
}

void SparqlParser::DataBlockContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDataBlock(this);
}

void SparqlParser::DataBlockContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDataBlock(this);
}


antlrcpp::Any SparqlParser::DataBlockContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitDataBlock(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DataBlockContext* SparqlParser::dataBlock() {
  DataBlockContext *_localctx = _tracker.createInstance<DataBlockContext>(_ctx, getState());
  enterRule(_localctx, 74, SparqlParser::RuleDataBlock);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(512);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(510);
        inlineDataOneVar();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::NIL: {
        enterOuterAlt(_localctx, 2);
        setState(511);
        inlineDataFull();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineDataOneVarContext ------------------------------------------------------------------

SparqlParser::InlineDataOneVarContext::InlineDataOneVarContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarContext* SparqlParser::InlineDataOneVarContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}

std::vector<SparqlParser::DataBlockValueContext *> SparqlParser::InlineDataOneVarContext::dataBlockValue() {
  return getRuleContexts<SparqlParser::DataBlockValueContext>();
}

SparqlParser::DataBlockValueContext* SparqlParser::InlineDataOneVarContext::dataBlockValue(size_t i) {
  return getRuleContext<SparqlParser::DataBlockValueContext>(i);
}


size_t SparqlParser::InlineDataOneVarContext::getRuleIndex() const {
  return SparqlParser::RuleInlineDataOneVar;
}

void SparqlParser::InlineDataOneVarContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineDataOneVar(this);
}

void SparqlParser::InlineDataOneVarContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineDataOneVar(this);
}


antlrcpp::Any SparqlParser::InlineDataOneVarContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitInlineDataOneVar(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::InlineDataOneVarContext* SparqlParser::inlineDataOneVar() {
  InlineDataOneVarContext *_localctx = _tracker.createInstance<InlineDataOneVarContext>(_ctx, getState());
  enterRule(_localctx, 76, SparqlParser::RuleInlineDataOneVar);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(514);
    var();
    setState(515);
    match(SparqlParser::T__3);
    setState(519);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__6)
      | (1ULL << SparqlParser::T__29)
      | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::INTEGER - 139))
      | (1ULL << (SparqlParser::DECIMAL - 139))
      | (1ULL << (SparqlParser::DOUBLE - 139))
      | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
      | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139)))) != 0)) {
      setState(516);
      dataBlockValue();
      setState(521);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(522);
    match(SparqlParser::T__4);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineDataFullContext ------------------------------------------------------------------

SparqlParser::InlineDataFullContext::InlineDataFullContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::InlineDataFullContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}

std::vector<SparqlParser::DataBlockSingleContext *> SparqlParser::InlineDataFullContext::dataBlockSingle() {
  return getRuleContexts<SparqlParser::DataBlockSingleContext>();
}

SparqlParser::DataBlockSingleContext* SparqlParser::InlineDataFullContext::dataBlockSingle(size_t i) {
  return getRuleContext<SparqlParser::DataBlockSingleContext>(i);
}

std::vector<SparqlParser::VarContext *> SparqlParser::InlineDataFullContext::var() {
  return getRuleContexts<SparqlParser::VarContext>();
}

SparqlParser::VarContext* SparqlParser::InlineDataFullContext::var(size_t i) {
  return getRuleContext<SparqlParser::VarContext>(i);
}


size_t SparqlParser::InlineDataFullContext::getRuleIndex() const {
  return SparqlParser::RuleInlineDataFull;
}

void SparqlParser::InlineDataFullContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInlineDataFull(this);
}

void SparqlParser::InlineDataFullContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInlineDataFull(this);
}


antlrcpp::Any SparqlParser::InlineDataFullContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitInlineDataFull(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::InlineDataFullContext* SparqlParser::inlineDataFull() {
  InlineDataFullContext *_localctx = _tracker.createInstance<InlineDataFullContext>(_ctx, getState());
  enterRule(_localctx, 78, SparqlParser::RuleInlineDataFull);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(533);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::NIL: {
        setState(524);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::T__1: {
        setState(525);
        match(SparqlParser::T__1);
        setState(529);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlParser::VAR1

        || _la == SparqlParser::VAR2) {
          setState(526);
          var();
          setState(531);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(532);
        match(SparqlParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
    setState(535);
    match(SparqlParser::T__3);
    setState(539);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__1 || _la == SparqlParser::NIL) {
      setState(536);
      dataBlockSingle();
      setState(541);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(542);
    match(SparqlParser::T__4);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataBlockSingleContext ------------------------------------------------------------------

SparqlParser::DataBlockSingleContext::DataBlockSingleContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::DataBlockSingleContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}

std::vector<SparqlParser::DataBlockValueContext *> SparqlParser::DataBlockSingleContext::dataBlockValue() {
  return getRuleContexts<SparqlParser::DataBlockValueContext>();
}

SparqlParser::DataBlockValueContext* SparqlParser::DataBlockSingleContext::dataBlockValue(size_t i) {
  return getRuleContext<SparqlParser::DataBlockValueContext>(i);
}


size_t SparqlParser::DataBlockSingleContext::getRuleIndex() const {
  return SparqlParser::RuleDataBlockSingle;
}

void SparqlParser::DataBlockSingleContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDataBlockSingle(this);
}

void SparqlParser::DataBlockSingleContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDataBlockSingle(this);
}


antlrcpp::Any SparqlParser::DataBlockSingleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitDataBlockSingle(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DataBlockSingleContext* SparqlParser::dataBlockSingle() {
  DataBlockSingleContext *_localctx = _tracker.createInstance<DataBlockSingleContext>(_ctx, getState());
  enterRule(_localctx, 80, SparqlParser::RuleDataBlockSingle);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(553);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1: {
        setState(544);
        match(SparqlParser::T__1);
        setState(548);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while ((((_la & ~ 0x3fULL) == 0) &&
          ((1ULL << _la) & ((1ULL << SparqlParser::T__6)
          | (1ULL << SparqlParser::T__29)
          | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
          | (1ULL << (SparqlParser::PNAME_NS - 139))
          | (1ULL << (SparqlParser::INTEGER - 139))
          | (1ULL << (SparqlParser::DECIMAL - 139))
          | (1ULL << (SparqlParser::DOUBLE - 139))
          | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
          | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
          | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
          | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
          | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
          | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
          | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139)))) != 0)) {
          setState(545);
          dataBlockValue();
          setState(550);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(551);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::NIL: {
        setState(552);
        match(SparqlParser::NIL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataBlockValueContext ------------------------------------------------------------------

SparqlParser::DataBlockValueContext::DataBlockValueContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::DataBlockValueContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}

SparqlParser::RdfLiteralContext* SparqlParser::DataBlockValueContext::rdfLiteral() {
  return getRuleContext<SparqlParser::RdfLiteralContext>(0);
}

SparqlParser::NumericLiteralContext* SparqlParser::DataBlockValueContext::numericLiteral() {
  return getRuleContext<SparqlParser::NumericLiteralContext>(0);
}

SparqlParser::BooleanLiteralContext* SparqlParser::DataBlockValueContext::booleanLiteral() {
  return getRuleContext<SparqlParser::BooleanLiteralContext>(0);
}


size_t SparqlParser::DataBlockValueContext::getRuleIndex() const {
  return SparqlParser::RuleDataBlockValue;
}

void SparqlParser::DataBlockValueContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterDataBlockValue(this);
}

void SparqlParser::DataBlockValueContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitDataBlockValue(this);
}


antlrcpp::Any SparqlParser::DataBlockValueContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitDataBlockValue(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::DataBlockValueContext* SparqlParser::dataBlockValue() {
  DataBlockValueContext *_localctx = _tracker.createInstance<DataBlockValueContext>(_ctx, getState());
  enterRule(_localctx, 82, SparqlParser::RuleDataBlockValue);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(560);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 1);
        setState(555);
        iri();
        break;
      }

      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 2);
        setState(556);
        rdfLiteral();
        break;
      }

      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(557);
        numericLiteral();
        break;
      }

      case SparqlParser::T__29:
      case SparqlParser::T__30: {
        enterOuterAlt(_localctx, 4);
        setState(558);
        booleanLiteral();
        break;
      }

      case SparqlParser::T__6: {
        enterOuterAlt(_localctx, 5);
        setState(559);
        match(SparqlParser::T__6);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MinusGraphPatternContext ------------------------------------------------------------------

SparqlParser::MinusGraphPatternContext::MinusGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::MinusGraphPatternContext::MINUS() {
  return getToken(SparqlParser::MINUS, 0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::MinusGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::MinusGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleMinusGraphPattern;
}

void SparqlParser::MinusGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMinusGraphPattern(this);
}

void SparqlParser::MinusGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMinusGraphPattern(this);
}


antlrcpp::Any SparqlParser::MinusGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitMinusGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::MinusGraphPatternContext* SparqlParser::minusGraphPattern() {
  MinusGraphPatternContext *_localctx = _tracker.createInstance<MinusGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 84, SparqlParser::RuleMinusGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(562);
    match(SparqlParser::MINUS);
    setState(563);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupOrUnionGraphPatternContext ------------------------------------------------------------------

SparqlParser::GroupOrUnionGraphPatternContext::GroupOrUnionGraphPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::GroupGraphPatternContext *> SparqlParser::GroupOrUnionGraphPatternContext::groupGraphPattern() {
  return getRuleContexts<SparqlParser::GroupGraphPatternContext>();
}

SparqlParser::GroupGraphPatternContext* SparqlParser::GroupOrUnionGraphPatternContext::groupGraphPattern(size_t i) {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(i);
}

std::vector<tree::TerminalNode *> SparqlParser::GroupOrUnionGraphPatternContext::UNION() {
  return getTokens(SparqlParser::UNION);
}

tree::TerminalNode* SparqlParser::GroupOrUnionGraphPatternContext::UNION(size_t i) {
  return getToken(SparqlParser::UNION, i);
}


size_t SparqlParser::GroupOrUnionGraphPatternContext::getRuleIndex() const {
  return SparqlParser::RuleGroupOrUnionGraphPattern;
}

void SparqlParser::GroupOrUnionGraphPatternContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupOrUnionGraphPattern(this);
}

void SparqlParser::GroupOrUnionGraphPatternContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupOrUnionGraphPattern(this);
}


antlrcpp::Any SparqlParser::GroupOrUnionGraphPatternContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGroupOrUnionGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GroupOrUnionGraphPatternContext* SparqlParser::groupOrUnionGraphPattern() {
  GroupOrUnionGraphPatternContext *_localctx = _tracker.createInstance<GroupOrUnionGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 86, SparqlParser::RuleGroupOrUnionGraphPattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(565);
    groupGraphPattern();
    setState(570);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::UNION) {
      setState(566);
      match(SparqlParser::UNION);
      setState(567);
      groupGraphPattern();
      setState(572);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FilterRContext ------------------------------------------------------------------

SparqlParser::FilterRContext::FilterRContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::FilterRContext::FILTER() {
  return getToken(SparqlParser::FILTER, 0);
}

SparqlParser::ConstraintContext* SparqlParser::FilterRContext::constraint() {
  return getRuleContext<SparqlParser::ConstraintContext>(0);
}


size_t SparqlParser::FilterRContext::getRuleIndex() const {
  return SparqlParser::RuleFilterR;
}

void SparqlParser::FilterRContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFilterR(this);
}

void SparqlParser::FilterRContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFilterR(this);
}


antlrcpp::Any SparqlParser::FilterRContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitFilterR(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::FilterRContext* SparqlParser::filterR() {
  FilterRContext *_localctx = _tracker.createInstance<FilterRContext>(_ctx, getState());
  enterRule(_localctx, 88, SparqlParser::RuleFilterR);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(573);
    match(SparqlParser::FILTER);
    setState(574);
    constraint();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintContext ------------------------------------------------------------------

SparqlParser::ConstraintContext::ConstraintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BrackettedExpressionContext* SparqlParser::ConstraintContext::brackettedExpression() {
  return getRuleContext<SparqlParser::BrackettedExpressionContext>(0);
}

SparqlParser::BuiltInCallContext* SparqlParser::ConstraintContext::builtInCall() {
  return getRuleContext<SparqlParser::BuiltInCallContext>(0);
}

SparqlParser::FunctionCallContext* SparqlParser::ConstraintContext::functionCall() {
  return getRuleContext<SparqlParser::FunctionCallContext>(0);
}


size_t SparqlParser::ConstraintContext::getRuleIndex() const {
  return SparqlParser::RuleConstraint;
}

void SparqlParser::ConstraintContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstraint(this);
}

void SparqlParser::ConstraintContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstraint(this);
}


antlrcpp::Any SparqlParser::ConstraintContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitConstraint(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstraintContext* SparqlParser::constraint() {
  ConstraintContext *_localctx = _tracker.createInstance<ConstraintContext>(_ctx, getState());
  enterRule(_localctx, 90, SparqlParser::RuleConstraint);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(579);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(576);
        brackettedExpression();
        break;
      }

      case SparqlParser::GROUP_CONCAT:
      case SparqlParser::NOT:
      case SparqlParser::STR:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::DATATYPE:
      case SparqlParser::BOUND:
      case SparqlParser::IRI:
      case SparqlParser::URI:
      case SparqlParser::BNODE:
      case SparqlParser::RAND:
      case SparqlParser::ABS:
      case SparqlParser::CEIL:
      case SparqlParser::FLOOR:
      case SparqlParser::ROUND:
      case SparqlParser::CONCAT:
      case SparqlParser::STRLEN:
      case SparqlParser::UCASE:
      case SparqlParser::LCASE:
      case SparqlParser::ENCODE:
      case SparqlParser::CONTAINS:
      case SparqlParser::STRSTARTS:
      case SparqlParser::STRENDS:
      case SparqlParser::STRBEFORE:
      case SparqlParser::STRAFTER:
      case SparqlParser::YEAR:
      case SparqlParser::MONTH:
      case SparqlParser::DAY:
      case SparqlParser::HOURS:
      case SparqlParser::MINUTES:
      case SparqlParser::SECONDS:
      case SparqlParser::TIMEZONE:
      case SparqlParser::TZ:
      case SparqlParser::NOW:
      case SparqlParser::UUID:
      case SparqlParser::STRUUID:
      case SparqlParser::SHA1:
      case SparqlParser::SHA256:
      case SparqlParser::SHA384:
      case SparqlParser::SHA512:
      case SparqlParser::MD5:
      case SparqlParser::COALESCE:
      case SparqlParser::IF:
      case SparqlParser::STRLANG:
      case SparqlParser::STRDT:
      case SparqlParser::SAMETERM:
      case SparqlParser::ISIRI:
      case SparqlParser::ISURI:
      case SparqlParser::ISBLANK:
      case SparqlParser::ISLITERAL:
      case SparqlParser::ISNUMERIC:
      case SparqlParser::REGEX:
      case SparqlParser::SUBSTR:
      case SparqlParser::REPLACE:
      case SparqlParser::EXISTS:
      case SparqlParser::COUNT:
      case SparqlParser::SUM:
      case SparqlParser::MIN:
      case SparqlParser::MAX:
      case SparqlParser::AVG:
      case SparqlParser::SAMPLE: {
        enterOuterAlt(_localctx, 2);
        setState(577);
        builtInCall();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 3);
        setState(578);
        functionCall();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionCallContext ------------------------------------------------------------------

SparqlParser::FunctionCallContext::FunctionCallContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::FunctionCallContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}

SparqlParser::ArgListContext* SparqlParser::FunctionCallContext::argList() {
  return getRuleContext<SparqlParser::ArgListContext>(0);
}


size_t SparqlParser::FunctionCallContext::getRuleIndex() const {
  return SparqlParser::RuleFunctionCall;
}

void SparqlParser::FunctionCallContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterFunctionCall(this);
}

void SparqlParser::FunctionCallContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitFunctionCall(this);
}


antlrcpp::Any SparqlParser::FunctionCallContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitFunctionCall(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::FunctionCallContext* SparqlParser::functionCall() {
  FunctionCallContext *_localctx = _tracker.createInstance<FunctionCallContext>(_ctx, getState());
  enterRule(_localctx, 92, SparqlParser::RuleFunctionCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(581);
    iri();
    setState(582);
    argList();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ArgListContext ------------------------------------------------------------------

SparqlParser::ArgListContext::ArgListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ArgListContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::ArgListContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::ArgListContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}

tree::TerminalNode* SparqlParser::ArgListContext::DISTINCT() {
  return getToken(SparqlParser::DISTINCT, 0);
}


size_t SparqlParser::ArgListContext::getRuleIndex() const {
  return SparqlParser::RuleArgList;
}

void SparqlParser::ArgListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterArgList(this);
}

void SparqlParser::ArgListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitArgList(this);
}


antlrcpp::Any SparqlParser::ArgListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitArgList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ArgListContext* SparqlParser::argList() {
  ArgListContext *_localctx = _tracker.createInstance<ArgListContext>(_ctx, getState());
  enterRule(_localctx, 94, SparqlParser::RuleArgList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(599);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(584);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(585);
        match(SparqlParser::T__1);
        setState(587);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(586);
          match(SparqlParser::DISTINCT);
        }
        setState(589);
        expression();
        setState(594);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlParser::T__7) {
          setState(590);
          match(SparqlParser::T__7);
          setState(591);
          expression();
          setState(596);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(597);
        match(SparqlParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionListContext ------------------------------------------------------------------

SparqlParser::ExpressionListContext::ExpressionListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ExpressionListContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::ExpressionListContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::ExpressionListContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}


size_t SparqlParser::ExpressionListContext::getRuleIndex() const {
  return SparqlParser::RuleExpressionList;
}

void SparqlParser::ExpressionListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpressionList(this);
}

void SparqlParser::ExpressionListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpressionList(this);
}


antlrcpp::Any SparqlParser::ExpressionListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitExpressionList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ExpressionListContext* SparqlParser::expressionList() {
  ExpressionListContext *_localctx = _tracker.createInstance<ExpressionListContext>(_ctx, getState());
  enterRule(_localctx, 96, SparqlParser::RuleExpressionList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(613);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(601);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(602);
        match(SparqlParser::T__1);
        setState(603);
        expression();
        setState(608);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlParser::T__7) {
          setState(604);
          match(SparqlParser::T__7);
          setState(605);
          expression();
          setState(610);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(611);
        match(SparqlParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructTemplateContext ------------------------------------------------------------------

SparqlParser::ConstructTemplateContext::ConstructTemplateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::ConstructTriplesContext* SparqlParser::ConstructTemplateContext::constructTriples() {
  return getRuleContext<SparqlParser::ConstructTriplesContext>(0);
}


size_t SparqlParser::ConstructTemplateContext::getRuleIndex() const {
  return SparqlParser::RuleConstructTemplate;
}

void SparqlParser::ConstructTemplateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstructTemplate(this);
}

void SparqlParser::ConstructTemplateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstructTemplate(this);
}


antlrcpp::Any SparqlParser::ConstructTemplateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitConstructTemplate(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstructTemplateContext* SparqlParser::constructTemplate() {
  ConstructTemplateContext *_localctx = _tracker.createInstance<ConstructTemplateContext>(_ctx, getState());
  enterRule(_localctx, 98, SparqlParser::RuleConstructTemplate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(615);
    match(SparqlParser::T__3);
    setState(617);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
      | (1ULL << SparqlParser::T__16)
      | (1ULL << SparqlParser::T__29)
      | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
      | (1ULL << (SparqlParser::VAR1 - 139))
      | (1ULL << (SparqlParser::VAR2 - 139))
      | (1ULL << (SparqlParser::INTEGER - 139))
      | (1ULL << (SparqlParser::DECIMAL - 139))
      | (1ULL << (SparqlParser::DOUBLE - 139))
      | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
      | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
      | (1ULL << (SparqlParser::NIL - 139))
      | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
      setState(616);
      constructTriples();
    }
    setState(619);
    match(SparqlParser::T__4);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructTriplesContext ------------------------------------------------------------------

SparqlParser::ConstructTriplesContext::ConstructTriplesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::TriplesSameSubjectContext* SparqlParser::ConstructTriplesContext::triplesSameSubject() {
  return getRuleContext<SparqlParser::TriplesSameSubjectContext>(0);
}

SparqlParser::ConstructTriplesContext* SparqlParser::ConstructTriplesContext::constructTriples() {
  return getRuleContext<SparqlParser::ConstructTriplesContext>(0);
}


size_t SparqlParser::ConstructTriplesContext::getRuleIndex() const {
  return SparqlParser::RuleConstructTriples;
}

void SparqlParser::ConstructTriplesContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConstructTriples(this);
}

void SparqlParser::ConstructTriplesContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConstructTriples(this);
}


antlrcpp::Any SparqlParser::ConstructTriplesContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitConstructTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConstructTriplesContext* SparqlParser::constructTriples() {
  ConstructTriplesContext *_localctx = _tracker.createInstance<ConstructTriplesContext>(_ctx, getState());
  enterRule(_localctx, 100, SparqlParser::RuleConstructTriples);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(621);
    triplesSameSubject();
    setState(626);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__5) {
      setState(622);
      match(SparqlParser::T__5);
      setState(624);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
        | (1ULL << SparqlParser::T__16)
        | (1ULL << SparqlParser::T__29)
        | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
        | (1ULL << (SparqlParser::PNAME_NS - 139))
        | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
        | (1ULL << (SparqlParser::VAR1 - 139))
        | (1ULL << (SparqlParser::VAR2 - 139))
        | (1ULL << (SparqlParser::INTEGER - 139))
        | (1ULL << (SparqlParser::DECIMAL - 139))
        | (1ULL << (SparqlParser::DOUBLE - 139))
        | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
        | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
        | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
        | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
        | (1ULL << (SparqlParser::NIL - 139))
        | (1ULL << (SparqlParser::ANON - 139)))) != 0)) {
        setState(623);
        constructTriples();
      }
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesSameSubjectContext ------------------------------------------------------------------

SparqlParser::TriplesSameSubjectContext::TriplesSameSubjectContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrTermContext* SparqlParser::TriplesSameSubjectContext::varOrTerm() {
  return getRuleContext<SparqlParser::VarOrTermContext>(0);
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::TriplesSameSubjectContext::propertyListNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListNotEmptyContext>(0);
}

SparqlParser::TriplesNodeContext* SparqlParser::TriplesSameSubjectContext::triplesNode() {
  return getRuleContext<SparqlParser::TriplesNodeContext>(0);
}

SparqlParser::PropertyListContext* SparqlParser::TriplesSameSubjectContext::propertyList() {
  return getRuleContext<SparqlParser::PropertyListContext>(0);
}


size_t SparqlParser::TriplesSameSubjectContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesSameSubject;
}

void SparqlParser::TriplesSameSubjectContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesSameSubject(this);
}

void SparqlParser::TriplesSameSubjectContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesSameSubject(this);
}


antlrcpp::Any SparqlParser::TriplesSameSubjectContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitTriplesSameSubject(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesSameSubjectContext* SparqlParser::triplesSameSubject() {
  TriplesSameSubjectContext *_localctx = _tracker.createInstance<TriplesSameSubjectContext>(_ctx, getState());
  enterRule(_localctx, 102, SparqlParser::RuleTriplesSameSubject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(634);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(628);
        varOrTerm();
        setState(629);
        propertyListNotEmpty();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::T__16: {
        enterOuterAlt(_localctx, 2);
        setState(631);
        triplesNode();
        setState(632);
        propertyList();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListContext ------------------------------------------------------------------

SparqlParser::PropertyListContext::PropertyListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::PropertyListContext::propertyListNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListNotEmptyContext>(0);
}


size_t SparqlParser::PropertyListContext::getRuleIndex() const {
  return SparqlParser::RulePropertyList;
}

void SparqlParser::PropertyListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyList(this);
}

void SparqlParser::PropertyListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyList(this);
}


antlrcpp::Any SparqlParser::PropertyListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPropertyList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PropertyListContext* SparqlParser::propertyList() {
  PropertyListContext *_localctx = _tracker.createInstance<PropertyListContext>(_ctx, getState());
  enterRule(_localctx, 104, SparqlParser::RulePropertyList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(637);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__9 || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::VAR1 - 139))
      | (1ULL << (SparqlParser::VAR2 - 139)))) != 0)) {
      setState(636);
      propertyListNotEmpty();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListNotEmptyContext ------------------------------------------------------------------

SparqlParser::PropertyListNotEmptyContext::PropertyListNotEmptyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::VerbContext *> SparqlParser::PropertyListNotEmptyContext::verb() {
  return getRuleContexts<SparqlParser::VerbContext>();
}

SparqlParser::VerbContext* SparqlParser::PropertyListNotEmptyContext::verb(size_t i) {
  return getRuleContext<SparqlParser::VerbContext>(i);
}

std::vector<SparqlParser::ObjectListContext *> SparqlParser::PropertyListNotEmptyContext::objectList() {
  return getRuleContexts<SparqlParser::ObjectListContext>();
}

SparqlParser::ObjectListContext* SparqlParser::PropertyListNotEmptyContext::objectList(size_t i) {
  return getRuleContext<SparqlParser::ObjectListContext>(i);
}


size_t SparqlParser::PropertyListNotEmptyContext::getRuleIndex() const {
  return SparqlParser::RulePropertyListNotEmpty;
}

void SparqlParser::PropertyListNotEmptyContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyListNotEmpty(this);
}

void SparqlParser::PropertyListNotEmptyContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyListNotEmpty(this);
}


antlrcpp::Any SparqlParser::PropertyListNotEmptyContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPropertyListNotEmpty(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::propertyListNotEmpty() {
  PropertyListNotEmptyContext *_localctx = _tracker.createInstance<PropertyListNotEmptyContext>(_ctx, getState());
  enterRule(_localctx, 106, SparqlParser::RulePropertyListNotEmpty);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(639);
    verb();
    setState(640);
    objectList();
    setState(649);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__8) {
      setState(641);
      match(SparqlParser::T__8);
      setState(645);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SparqlParser::T__9 || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
        | (1ULL << (SparqlParser::PNAME_NS - 139))
        | (1ULL << (SparqlParser::VAR1 - 139))
        | (1ULL << (SparqlParser::VAR2 - 139)))) != 0)) {
        setState(642);
        verb();
        setState(643);
        objectList();
      }
      setState(651);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbContext ------------------------------------------------------------------

SparqlParser::VerbContext::VerbContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrIriContext* SparqlParser::VerbContext::varOrIri() {
  return getRuleContext<SparqlParser::VarOrIriContext>(0);
}


size_t SparqlParser::VerbContext::getRuleIndex() const {
  return SparqlParser::RuleVerb;
}

void SparqlParser::VerbContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVerb(this);
}

void SparqlParser::VerbContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVerb(this);
}


antlrcpp::Any SparqlParser::VerbContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVerb(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VerbContext* SparqlParser::verb() {
  VerbContext *_localctx = _tracker.createInstance<VerbContext>(_ctx, getState());
  enterRule(_localctx, 108, SparqlParser::RuleVerb);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(654);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(652);
        varOrIri();
        break;
      }

      case SparqlParser::T__9: {
        enterOuterAlt(_localctx, 2);
        setState(653);
        match(SparqlParser::T__9);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectListContext ------------------------------------------------------------------

SparqlParser::ObjectListContext::ObjectListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::ObjectRContext *> SparqlParser::ObjectListContext::objectR() {
  return getRuleContexts<SparqlParser::ObjectRContext>();
}

SparqlParser::ObjectRContext* SparqlParser::ObjectListContext::objectR(size_t i) {
  return getRuleContext<SparqlParser::ObjectRContext>(i);
}


size_t SparqlParser::ObjectListContext::getRuleIndex() const {
  return SparqlParser::RuleObjectList;
}

void SparqlParser::ObjectListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterObjectList(this);
}

void SparqlParser::ObjectListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitObjectList(this);
}


antlrcpp::Any SparqlParser::ObjectListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitObjectList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ObjectListContext* SparqlParser::objectList() {
  ObjectListContext *_localctx = _tracker.createInstance<ObjectListContext>(_ctx, getState());
  enterRule(_localctx, 110, SparqlParser::RuleObjectList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(656);
    objectR();
    setState(661);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__7) {
      setState(657);
      match(SparqlParser::T__7);
      setState(658);
      objectR();
      setState(663);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectRContext ------------------------------------------------------------------

SparqlParser::ObjectRContext::ObjectRContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GraphNodeContext* SparqlParser::ObjectRContext::graphNode() {
  return getRuleContext<SparqlParser::GraphNodeContext>(0);
}


size_t SparqlParser::ObjectRContext::getRuleIndex() const {
  return SparqlParser::RuleObjectR;
}

void SparqlParser::ObjectRContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterObjectR(this);
}

void SparqlParser::ObjectRContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitObjectR(this);
}


antlrcpp::Any SparqlParser::ObjectRContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitObjectR(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ObjectRContext* SparqlParser::objectR() {
  ObjectRContext *_localctx = _tracker.createInstance<ObjectRContext>(_ctx, getState());
  enterRule(_localctx, 112, SparqlParser::RuleObjectR);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(664);
    graphNode();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesSameSubjectPathContext ------------------------------------------------------------------

SparqlParser::TriplesSameSubjectPathContext::TriplesSameSubjectPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrTermContext* SparqlParser::TriplesSameSubjectPathContext::varOrTerm() {
  return getRuleContext<SparqlParser::VarOrTermContext>(0);
}

SparqlParser::PropertyListPathNotEmptyContext* SparqlParser::TriplesSameSubjectPathContext::propertyListPathNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListPathNotEmptyContext>(0);
}

SparqlParser::TriplesNodePathContext* SparqlParser::TriplesSameSubjectPathContext::triplesNodePath() {
  return getRuleContext<SparqlParser::TriplesNodePathContext>(0);
}

SparqlParser::PropertyListPathContext* SparqlParser::TriplesSameSubjectPathContext::propertyListPath() {
  return getRuleContext<SparqlParser::PropertyListPathContext>(0);
}


size_t SparqlParser::TriplesSameSubjectPathContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesSameSubjectPath;
}

void SparqlParser::TriplesSameSubjectPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesSameSubjectPath(this);
}

void SparqlParser::TriplesSameSubjectPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesSameSubjectPath(this);
}


antlrcpp::Any SparqlParser::TriplesSameSubjectPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitTriplesSameSubjectPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesSameSubjectPathContext* SparqlParser::triplesSameSubjectPath() {
  TriplesSameSubjectPathContext *_localctx = _tracker.createInstance<TriplesSameSubjectPathContext>(_ctx, getState());
  enterRule(_localctx, 114, SparqlParser::RuleTriplesSameSubjectPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(672);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(666);
        varOrTerm();
        setState(667);
        propertyListPathNotEmpty();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::T__16: {
        enterOuterAlt(_localctx, 2);
        setState(669);
        triplesNodePath();
        setState(670);
        propertyListPath();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListPathContext ------------------------------------------------------------------

SparqlParser::PropertyListPathContext::PropertyListPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PropertyListPathNotEmptyContext* SparqlParser::PropertyListPathContext::propertyListPathNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListPathNotEmptyContext>(0);
}


size_t SparqlParser::PropertyListPathContext::getRuleIndex() const {
  return SparqlParser::RulePropertyListPath;
}

void SparqlParser::PropertyListPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyListPath(this);
}

void SparqlParser::PropertyListPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyListPath(this);
}


antlrcpp::Any SparqlParser::PropertyListPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPropertyListPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PropertyListPathContext* SparqlParser::propertyListPath() {
  PropertyListPathContext *_localctx = _tracker.createInstance<PropertyListPathContext>(_ctx, getState());
  enterRule(_localctx, 116, SparqlParser::RulePropertyListPath);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(675);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
      | (1ULL << SparqlParser::T__9)
      | (1ULL << SparqlParser::T__12)
      | (1ULL << SparqlParser::T__15))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::VAR1 - 139))
      | (1ULL << (SparqlParser::VAR2 - 139)))) != 0)) {
      setState(674);
      propertyListPathNotEmpty();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListPathNotEmptyContext ------------------------------------------------------------------

SparqlParser::PropertyListPathNotEmptyContext::PropertyListPathNotEmptyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::VerbPathOrSimpleContext *> SparqlParser::PropertyListPathNotEmptyContext::verbPathOrSimple() {
  return getRuleContexts<SparqlParser::VerbPathOrSimpleContext>();
}

SparqlParser::VerbPathOrSimpleContext* SparqlParser::PropertyListPathNotEmptyContext::verbPathOrSimple(size_t i) {
  return getRuleContext<SparqlParser::VerbPathOrSimpleContext>(i);
}

SparqlParser::ObjectListPathContext* SparqlParser::PropertyListPathNotEmptyContext::objectListPath() {
  return getRuleContext<SparqlParser::ObjectListPathContext>(0);
}

std::vector<SparqlParser::ObjectListContext *> SparqlParser::PropertyListPathNotEmptyContext::objectList() {
  return getRuleContexts<SparqlParser::ObjectListContext>();
}

SparqlParser::ObjectListContext* SparqlParser::PropertyListPathNotEmptyContext::objectList(size_t i) {
  return getRuleContext<SparqlParser::ObjectListContext>(i);
}


size_t SparqlParser::PropertyListPathNotEmptyContext::getRuleIndex() const {
  return SparqlParser::RulePropertyListPathNotEmpty;
}

void SparqlParser::PropertyListPathNotEmptyContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyListPathNotEmpty(this);
}

void SparqlParser::PropertyListPathNotEmptyContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyListPathNotEmpty(this);
}


antlrcpp::Any SparqlParser::PropertyListPathNotEmptyContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPropertyListPathNotEmpty(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PropertyListPathNotEmptyContext* SparqlParser::propertyListPathNotEmpty() {
  PropertyListPathNotEmptyContext *_localctx = _tracker.createInstance<PropertyListPathNotEmptyContext>(_ctx, getState());
  enterRule(_localctx, 118, SparqlParser::RulePropertyListPathNotEmpty);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(677);
    verbPathOrSimple();
    setState(678);
    objectListPath();
    setState(687);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__8) {
      setState(679);
      match(SparqlParser::T__8);
      setState(683);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
        | (1ULL << SparqlParser::T__9)
        | (1ULL << SparqlParser::T__12)
        | (1ULL << SparqlParser::T__15))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
        | (1ULL << (SparqlParser::PNAME_NS - 139))
        | (1ULL << (SparqlParser::VAR1 - 139))
        | (1ULL << (SparqlParser::VAR2 - 139)))) != 0)) {
        setState(680);
        verbPathOrSimple();
        setState(681);
        objectList();
      }
      setState(689);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbPathContext ------------------------------------------------------------------

SparqlParser::VerbPathContext::VerbPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PathContext* SparqlParser::VerbPathContext::path() {
  return getRuleContext<SparqlParser::PathContext>(0);
}


size_t SparqlParser::VerbPathContext::getRuleIndex() const {
  return SparqlParser::RuleVerbPath;
}

void SparqlParser::VerbPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVerbPath(this);
}

void SparqlParser::VerbPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVerbPath(this);
}


antlrcpp::Any SparqlParser::VerbPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVerbPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VerbPathContext* SparqlParser::verbPath() {
  VerbPathContext *_localctx = _tracker.createInstance<VerbPathContext>(_ctx, getState());
  enterRule(_localctx, 120, SparqlParser::RuleVerbPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(690);
    path();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbSimpleContext ------------------------------------------------------------------

SparqlParser::VerbSimpleContext::VerbSimpleContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarContext* SparqlParser::VerbSimpleContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}


size_t SparqlParser::VerbSimpleContext::getRuleIndex() const {
  return SparqlParser::RuleVerbSimple;
}

void SparqlParser::VerbSimpleContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVerbSimple(this);
}

void SparqlParser::VerbSimpleContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVerbSimple(this);
}


antlrcpp::Any SparqlParser::VerbSimpleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVerbSimple(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VerbSimpleContext* SparqlParser::verbSimple() {
  VerbSimpleContext *_localctx = _tracker.createInstance<VerbSimpleContext>(_ctx, getState());
  enterRule(_localctx, 122, SparqlParser::RuleVerbSimple);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(692);
    var();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbPathOrSimpleContext ------------------------------------------------------------------

SparqlParser::VerbPathOrSimpleContext::VerbPathOrSimpleContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VerbPathContext* SparqlParser::VerbPathOrSimpleContext::verbPath() {
  return getRuleContext<SparqlParser::VerbPathContext>(0);
}

SparqlParser::VerbSimpleContext* SparqlParser::VerbPathOrSimpleContext::verbSimple() {
  return getRuleContext<SparqlParser::VerbSimpleContext>(0);
}


size_t SparqlParser::VerbPathOrSimpleContext::getRuleIndex() const {
  return SparqlParser::RuleVerbPathOrSimple;
}

void SparqlParser::VerbPathOrSimpleContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVerbPathOrSimple(this);
}

void SparqlParser::VerbPathOrSimpleContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVerbPathOrSimple(this);
}


antlrcpp::Any SparqlParser::VerbPathOrSimpleContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVerbPathOrSimple(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VerbPathOrSimpleContext* SparqlParser::verbPathOrSimple() {
  VerbPathOrSimpleContext *_localctx = _tracker.createInstance<VerbPathOrSimpleContext>(_ctx, getState());
  enterRule(_localctx, 124, SparqlParser::RuleVerbPathOrSimple);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(696);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1:
      case SparqlParser::T__9:
      case SparqlParser::T__12:
      case SparqlParser::T__15:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        setState(694);
        verbPath();
        break;
      }

      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        setState(695);
        verbSimple();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectListPathContext ------------------------------------------------------------------

SparqlParser::ObjectListPathContext::ObjectListPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::ObjectPathContext *> SparqlParser::ObjectListPathContext::objectPath() {
  return getRuleContexts<SparqlParser::ObjectPathContext>();
}

SparqlParser::ObjectPathContext* SparqlParser::ObjectListPathContext::objectPath(size_t i) {
  return getRuleContext<SparqlParser::ObjectPathContext>(i);
}


size_t SparqlParser::ObjectListPathContext::getRuleIndex() const {
  return SparqlParser::RuleObjectListPath;
}

void SparqlParser::ObjectListPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterObjectListPath(this);
}

void SparqlParser::ObjectListPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitObjectListPath(this);
}


antlrcpp::Any SparqlParser::ObjectListPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitObjectListPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ObjectListPathContext* SparqlParser::objectListPath() {
  ObjectListPathContext *_localctx = _tracker.createInstance<ObjectListPathContext>(_ctx, getState());
  enterRule(_localctx, 126, SparqlParser::RuleObjectListPath);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(698);
    objectPath();
    setState(703);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__7) {
      setState(699);
      match(SparqlParser::T__7);
      setState(700);
      objectPath();
      setState(705);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectPathContext ------------------------------------------------------------------

SparqlParser::ObjectPathContext::ObjectPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::GraphNodePathContext* SparqlParser::ObjectPathContext::graphNodePath() {
  return getRuleContext<SparqlParser::GraphNodePathContext>(0);
}


size_t SparqlParser::ObjectPathContext::getRuleIndex() const {
  return SparqlParser::RuleObjectPath;
}

void SparqlParser::ObjectPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterObjectPath(this);
}

void SparqlParser::ObjectPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitObjectPath(this);
}


antlrcpp::Any SparqlParser::ObjectPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitObjectPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ObjectPathContext* SparqlParser::objectPath() {
  ObjectPathContext *_localctx = _tracker.createInstance<ObjectPathContext>(_ctx, getState());
  enterRule(_localctx, 128, SparqlParser::RuleObjectPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(706);
    graphNodePath();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathContext ------------------------------------------------------------------

SparqlParser::PathContext::PathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PathAlternativeContext* SparqlParser::PathContext::pathAlternative() {
  return getRuleContext<SparqlParser::PathAlternativeContext>(0);
}


size_t SparqlParser::PathContext::getRuleIndex() const {
  return SparqlParser::RulePath;
}

void SparqlParser::PathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPath(this);
}

void SparqlParser::PathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPath(this);
}


antlrcpp::Any SparqlParser::PathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathContext* SparqlParser::path() {
  PathContext *_localctx = _tracker.createInstance<PathContext>(_ctx, getState());
  enterRule(_localctx, 130, SparqlParser::RulePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(708);
    pathAlternative();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathAlternativeContext ------------------------------------------------------------------

SparqlParser::PathAlternativeContext::PathAlternativeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::PathSequenceContext *> SparqlParser::PathAlternativeContext::pathSequence() {
  return getRuleContexts<SparqlParser::PathSequenceContext>();
}

SparqlParser::PathSequenceContext* SparqlParser::PathAlternativeContext::pathSequence(size_t i) {
  return getRuleContext<SparqlParser::PathSequenceContext>(i);
}


size_t SparqlParser::PathAlternativeContext::getRuleIndex() const {
  return SparqlParser::RulePathAlternative;
}

void SparqlParser::PathAlternativeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathAlternative(this);
}

void SparqlParser::PathAlternativeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathAlternative(this);
}


antlrcpp::Any SparqlParser::PathAlternativeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathAlternative(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathAlternativeContext* SparqlParser::pathAlternative() {
  PathAlternativeContext *_localctx = _tracker.createInstance<PathAlternativeContext>(_ctx, getState());
  enterRule(_localctx, 132, SparqlParser::RulePathAlternative);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(710);
    pathSequence();
    setState(715);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__10) {
      setState(711);
      match(SparqlParser::T__10);
      setState(712);
      pathSequence();
      setState(717);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathSequenceContext ------------------------------------------------------------------

SparqlParser::PathSequenceContext::PathSequenceContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::PathEltOrInverseContext *> SparqlParser::PathSequenceContext::pathEltOrInverse() {
  return getRuleContexts<SparqlParser::PathEltOrInverseContext>();
}

SparqlParser::PathEltOrInverseContext* SparqlParser::PathSequenceContext::pathEltOrInverse(size_t i) {
  return getRuleContext<SparqlParser::PathEltOrInverseContext>(i);
}


size_t SparqlParser::PathSequenceContext::getRuleIndex() const {
  return SparqlParser::RulePathSequence;
}

void SparqlParser::PathSequenceContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathSequence(this);
}

void SparqlParser::PathSequenceContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathSequence(this);
}


antlrcpp::Any SparqlParser::PathSequenceContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathSequence(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathSequenceContext* SparqlParser::pathSequence() {
  PathSequenceContext *_localctx = _tracker.createInstance<PathSequenceContext>(_ctx, getState());
  enterRule(_localctx, 134, SparqlParser::RulePathSequence);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(718);
    pathEltOrInverse();
    setState(723);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__11) {
      setState(719);
      match(SparqlParser::T__11);
      setState(720);
      pathEltOrInverse();
      setState(725);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathEltContext ------------------------------------------------------------------

SparqlParser::PathEltContext::PathEltContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PathPrimaryContext* SparqlParser::PathEltContext::pathPrimary() {
  return getRuleContext<SparqlParser::PathPrimaryContext>(0);
}

SparqlParser::PathModContext* SparqlParser::PathEltContext::pathMod() {
  return getRuleContext<SparqlParser::PathModContext>(0);
}


size_t SparqlParser::PathEltContext::getRuleIndex() const {
  return SparqlParser::RulePathElt;
}

void SparqlParser::PathEltContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathElt(this);
}

void SparqlParser::PathEltContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathElt(this);
}


antlrcpp::Any SparqlParser::PathEltContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathElt(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathEltContext* SparqlParser::pathElt() {
  PathEltContext *_localctx = _tracker.createInstance<PathEltContext>(_ctx, getState());
  enterRule(_localctx, 136, SparqlParser::RulePathElt);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(726);
    pathPrimary();
    setState(728);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__0)
      | (1ULL << SparqlParser::T__13)
      | (1ULL << SparqlParser::T__14))) != 0)) {
      setState(727);
      pathMod();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathEltOrInverseContext ------------------------------------------------------------------

SparqlParser::PathEltOrInverseContext::PathEltOrInverseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PathEltContext* SparqlParser::PathEltOrInverseContext::pathElt() {
  return getRuleContext<SparqlParser::PathEltContext>(0);
}


size_t SparqlParser::PathEltOrInverseContext::getRuleIndex() const {
  return SparqlParser::RulePathEltOrInverse;
}

void SparqlParser::PathEltOrInverseContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathEltOrInverse(this);
}

void SparqlParser::PathEltOrInverseContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathEltOrInverse(this);
}


antlrcpp::Any SparqlParser::PathEltOrInverseContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathEltOrInverse(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathEltOrInverseContext* SparqlParser::pathEltOrInverse() {
  PathEltOrInverseContext *_localctx = _tracker.createInstance<PathEltOrInverseContext>(_ctx, getState());
  enterRule(_localctx, 138, SparqlParser::RulePathEltOrInverse);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(733);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1:
      case SparqlParser::T__9:
      case SparqlParser::T__15:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 1);
        setState(730);
        pathElt();
        break;
      }

      case SparqlParser::T__12: {
        enterOuterAlt(_localctx, 2);
        setState(731);
        match(SparqlParser::T__12);
        setState(732);
        pathElt();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathModContext ------------------------------------------------------------------

SparqlParser::PathModContext::PathModContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SparqlParser::PathModContext::getRuleIndex() const {
  return SparqlParser::RulePathMod;
}

void SparqlParser::PathModContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathMod(this);
}

void SparqlParser::PathModContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathMod(this);
}


antlrcpp::Any SparqlParser::PathModContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathMod(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathModContext* SparqlParser::pathMod() {
  PathModContext *_localctx = _tracker.createInstance<PathModContext>(_ctx, getState());
  enterRule(_localctx, 140, SparqlParser::RulePathMod);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(735);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__0)
      | (1ULL << SparqlParser::T__13)
      | (1ULL << SparqlParser::T__14))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathPrimaryContext ------------------------------------------------------------------

SparqlParser::PathPrimaryContext::PathPrimaryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::PathPrimaryContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}

SparqlParser::PathNegatedPropertySetContext* SparqlParser::PathPrimaryContext::pathNegatedPropertySet() {
  return getRuleContext<SparqlParser::PathNegatedPropertySetContext>(0);
}

SparqlParser::PathContext* SparqlParser::PathPrimaryContext::path() {
  return getRuleContext<SparqlParser::PathContext>(0);
}


size_t SparqlParser::PathPrimaryContext::getRuleIndex() const {
  return SparqlParser::RulePathPrimary;
}

void SparqlParser::PathPrimaryContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathPrimary(this);
}

void SparqlParser::PathPrimaryContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathPrimary(this);
}


antlrcpp::Any SparqlParser::PathPrimaryContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathPrimary(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathPrimaryContext* SparqlParser::pathPrimary() {
  PathPrimaryContext *_localctx = _tracker.createInstance<PathPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 142, SparqlParser::RulePathPrimary);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(745);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 1);
        setState(737);
        iri();
        break;
      }

      case SparqlParser::T__9: {
        enterOuterAlt(_localctx, 2);
        setState(738);
        match(SparqlParser::T__9);
        break;
      }

      case SparqlParser::T__15: {
        enterOuterAlt(_localctx, 3);
        setState(739);
        match(SparqlParser::T__15);
        setState(740);
        pathNegatedPropertySet();
        break;
      }

      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 4);
        setState(741);
        match(SparqlParser::T__1);
        setState(742);
        path();
        setState(743);
        match(SparqlParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathNegatedPropertySetContext ------------------------------------------------------------------

SparqlParser::PathNegatedPropertySetContext::PathNegatedPropertySetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::PathOneInPropertySetContext *> SparqlParser::PathNegatedPropertySetContext::pathOneInPropertySet() {
  return getRuleContexts<SparqlParser::PathOneInPropertySetContext>();
}

SparqlParser::PathOneInPropertySetContext* SparqlParser::PathNegatedPropertySetContext::pathOneInPropertySet(size_t i) {
  return getRuleContext<SparqlParser::PathOneInPropertySetContext>(i);
}


size_t SparqlParser::PathNegatedPropertySetContext::getRuleIndex() const {
  return SparqlParser::RulePathNegatedPropertySet;
}

void SparqlParser::PathNegatedPropertySetContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathNegatedPropertySet(this);
}

void SparqlParser::PathNegatedPropertySetContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathNegatedPropertySet(this);
}


antlrcpp::Any SparqlParser::PathNegatedPropertySetContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathNegatedPropertySet(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathNegatedPropertySetContext* SparqlParser::pathNegatedPropertySet() {
  PathNegatedPropertySetContext *_localctx = _tracker.createInstance<PathNegatedPropertySetContext>(_ctx, getState());
  enterRule(_localctx, 144, SparqlParser::RulePathNegatedPropertySet);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(760);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__9:
      case SparqlParser::T__12:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 1);
        setState(747);
        pathOneInPropertySet();
        break;
      }

      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(748);
        match(SparqlParser::T__1);
        setState(757);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::T__9

        || _la == SparqlParser::T__12 || _la == SparqlParser::IRI_REF

        || _la == SparqlParser::PNAME_NS) {
          setState(749);
          pathOneInPropertySet();
          setState(754);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == SparqlParser::T__10) {
            setState(750);
            match(SparqlParser::T__10);
            setState(751);
            pathOneInPropertySet();
            setState(756);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(759);
        match(SparqlParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathOneInPropertySetContext ------------------------------------------------------------------

SparqlParser::PathOneInPropertySetContext::PathOneInPropertySetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::PathOneInPropertySetContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}


size_t SparqlParser::PathOneInPropertySetContext::getRuleIndex() const {
  return SparqlParser::RulePathOneInPropertySet;
}

void SparqlParser::PathOneInPropertySetContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathOneInPropertySet(this);
}

void SparqlParser::PathOneInPropertySetContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathOneInPropertySet(this);
}


antlrcpp::Any SparqlParser::PathOneInPropertySetContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPathOneInPropertySet(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PathOneInPropertySetContext* SparqlParser::pathOneInPropertySet() {
  PathOneInPropertySetContext *_localctx = _tracker.createInstance<PathOneInPropertySetContext>(_ctx, getState());
  enterRule(_localctx, 146, SparqlParser::RulePathOneInPropertySet);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(769);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 1);
        setState(762);
        iri();
        break;
      }

      case SparqlParser::T__9: {
        enterOuterAlt(_localctx, 2);
        setState(763);
        match(SparqlParser::T__9);
        break;
      }

      case SparqlParser::T__12: {
        enterOuterAlt(_localctx, 3);
        setState(764);
        match(SparqlParser::T__12);
        setState(767);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlParser::IRI_REF:
          case SparqlParser::PNAME_NS: {
            setState(765);
            iri();
            break;
          }

          case SparqlParser::T__9: {
            setState(766);
            match(SparqlParser::T__9);
            break;
          }

        default:
          throw NoViableAltException(this);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IntegerContext ------------------------------------------------------------------

SparqlParser::IntegerContext::IntegerContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::IntegerContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}


size_t SparqlParser::IntegerContext::getRuleIndex() const {
  return SparqlParser::RuleInteger;
}

void SparqlParser::IntegerContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterInteger(this);
}

void SparqlParser::IntegerContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitInteger(this);
}


antlrcpp::Any SparqlParser::IntegerContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitInteger(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::IntegerContext* SparqlParser::integer() {
  IntegerContext *_localctx = _tracker.createInstance<IntegerContext>(_ctx, getState());
  enterRule(_localctx, 148, SparqlParser::RuleInteger);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(771);
    match(SparqlParser::INTEGER);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesNodeContext ------------------------------------------------------------------

SparqlParser::TriplesNodeContext::TriplesNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::CollectionContext* SparqlParser::TriplesNodeContext::collection() {
  return getRuleContext<SparqlParser::CollectionContext>(0);
}

SparqlParser::BlankNodePropertyListContext* SparqlParser::TriplesNodeContext::blankNodePropertyList() {
  return getRuleContext<SparqlParser::BlankNodePropertyListContext>(0);
}


size_t SparqlParser::TriplesNodeContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesNode;
}

void SparqlParser::TriplesNodeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesNode(this);
}

void SparqlParser::TriplesNodeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesNode(this);
}


antlrcpp::Any SparqlParser::TriplesNodeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitTriplesNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesNodeContext* SparqlParser::triplesNode() {
  TriplesNodeContext *_localctx = _tracker.createInstance<TriplesNodeContext>(_ctx, getState());
  enterRule(_localctx, 150, SparqlParser::RuleTriplesNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(775);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(773);
        collection();
        break;
      }

      case SparqlParser::T__16: {
        enterOuterAlt(_localctx, 2);
        setState(774);
        blankNodePropertyList();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BlankNodePropertyListContext ------------------------------------------------------------------

SparqlParser::BlankNodePropertyListContext::BlankNodePropertyListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PropertyListNotEmptyContext* SparqlParser::BlankNodePropertyListContext::propertyListNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListNotEmptyContext>(0);
}


size_t SparqlParser::BlankNodePropertyListContext::getRuleIndex() const {
  return SparqlParser::RuleBlankNodePropertyList;
}

void SparqlParser::BlankNodePropertyListContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNodePropertyList(this);
}

void SparqlParser::BlankNodePropertyListContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNodePropertyList(this);
}


antlrcpp::Any SparqlParser::BlankNodePropertyListContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBlankNodePropertyList(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BlankNodePropertyListContext* SparqlParser::blankNodePropertyList() {
  BlankNodePropertyListContext *_localctx = _tracker.createInstance<BlankNodePropertyListContext>(_ctx, getState());
  enterRule(_localctx, 152, SparqlParser::RuleBlankNodePropertyList);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(777);
    match(SparqlParser::T__16);
    setState(778);
    propertyListNotEmpty();
    setState(779);
    match(SparqlParser::T__17);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesNodePathContext ------------------------------------------------------------------

SparqlParser::TriplesNodePathContext::TriplesNodePathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::CollectionPathContext* SparqlParser::TriplesNodePathContext::collectionPath() {
  return getRuleContext<SparqlParser::CollectionPathContext>(0);
}

SparqlParser::BlankNodePropertyListPathContext* SparqlParser::TriplesNodePathContext::blankNodePropertyListPath() {
  return getRuleContext<SparqlParser::BlankNodePropertyListPathContext>(0);
}


size_t SparqlParser::TriplesNodePathContext::getRuleIndex() const {
  return SparqlParser::RuleTriplesNodePath;
}

void SparqlParser::TriplesNodePathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesNodePath(this);
}

void SparqlParser::TriplesNodePathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesNodePath(this);
}


antlrcpp::Any SparqlParser::TriplesNodePathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitTriplesNodePath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::TriplesNodePathContext* SparqlParser::triplesNodePath() {
  TriplesNodePathContext *_localctx = _tracker.createInstance<TriplesNodePathContext>(_ctx, getState());
  enterRule(_localctx, 154, SparqlParser::RuleTriplesNodePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(783);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(781);
        collectionPath();
        break;
      }

      case SparqlParser::T__16: {
        enterOuterAlt(_localctx, 2);
        setState(782);
        blankNodePropertyListPath();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BlankNodePropertyListPathContext ------------------------------------------------------------------

SparqlParser::BlankNodePropertyListPathContext::BlankNodePropertyListPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PropertyListPathNotEmptyContext* SparqlParser::BlankNodePropertyListPathContext::propertyListPathNotEmpty() {
  return getRuleContext<SparqlParser::PropertyListPathNotEmptyContext>(0);
}


size_t SparqlParser::BlankNodePropertyListPathContext::getRuleIndex() const {
  return SparqlParser::RuleBlankNodePropertyListPath;
}

void SparqlParser::BlankNodePropertyListPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNodePropertyListPath(this);
}

void SparqlParser::BlankNodePropertyListPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNodePropertyListPath(this);
}


antlrcpp::Any SparqlParser::BlankNodePropertyListPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBlankNodePropertyListPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BlankNodePropertyListPathContext* SparqlParser::blankNodePropertyListPath() {
  BlankNodePropertyListPathContext *_localctx = _tracker.createInstance<BlankNodePropertyListPathContext>(_ctx, getState());
  enterRule(_localctx, 156, SparqlParser::RuleBlankNodePropertyListPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(785);
    match(SparqlParser::T__16);
    setState(786);
    propertyListPathNotEmpty();
    setState(787);
    match(SparqlParser::T__17);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CollectionContext ------------------------------------------------------------------

SparqlParser::CollectionContext::CollectionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::GraphNodeContext *> SparqlParser::CollectionContext::graphNode() {
  return getRuleContexts<SparqlParser::GraphNodeContext>();
}

SparqlParser::GraphNodeContext* SparqlParser::CollectionContext::graphNode(size_t i) {
  return getRuleContext<SparqlParser::GraphNodeContext>(i);
}


size_t SparqlParser::CollectionContext::getRuleIndex() const {
  return SparqlParser::RuleCollection;
}

void SparqlParser::CollectionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCollection(this);
}

void SparqlParser::CollectionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCollection(this);
}


antlrcpp::Any SparqlParser::CollectionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitCollection(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::CollectionContext* SparqlParser::collection() {
  CollectionContext *_localctx = _tracker.createInstance<CollectionContext>(_ctx, getState());
  enterRule(_localctx, 158, SparqlParser::RuleCollection);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(789);
    match(SparqlParser::T__1);
    setState(791); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(790);
      graphNode();
      setState(793); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
      | (1ULL << SparqlParser::T__16)
      | (1ULL << SparqlParser::T__29)
      | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
      | (1ULL << (SparqlParser::VAR1 - 139))
      | (1ULL << (SparqlParser::VAR2 - 139))
      | (1ULL << (SparqlParser::INTEGER - 139))
      | (1ULL << (SparqlParser::DECIMAL - 139))
      | (1ULL << (SparqlParser::DOUBLE - 139))
      | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
      | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
      | (1ULL << (SparqlParser::NIL - 139))
      | (1ULL << (SparqlParser::ANON - 139)))) != 0));
    setState(795);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CollectionPathContext ------------------------------------------------------------------

SparqlParser::CollectionPathContext::CollectionPathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::GraphNodePathContext *> SparqlParser::CollectionPathContext::graphNodePath() {
  return getRuleContexts<SparqlParser::GraphNodePathContext>();
}

SparqlParser::GraphNodePathContext* SparqlParser::CollectionPathContext::graphNodePath(size_t i) {
  return getRuleContext<SparqlParser::GraphNodePathContext>(i);
}


size_t SparqlParser::CollectionPathContext::getRuleIndex() const {
  return SparqlParser::RuleCollectionPath;
}

void SparqlParser::CollectionPathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterCollectionPath(this);
}

void SparqlParser::CollectionPathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitCollectionPath(this);
}


antlrcpp::Any SparqlParser::CollectionPathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitCollectionPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::CollectionPathContext* SparqlParser::collectionPath() {
  CollectionPathContext *_localctx = _tracker.createInstance<CollectionPathContext>(_ctx, getState());
  enterRule(_localctx, 160, SparqlParser::RuleCollectionPath);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(797);
    match(SparqlParser::T__1);
    setState(799); 
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(798);
      graphNodePath();
      setState(801); 
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << SparqlParser::T__1)
      | (1ULL << SparqlParser::T__16)
      | (1ULL << SparqlParser::T__29)
      | (1ULL << SparqlParser::T__30))) != 0) || ((((_la - 139) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 139)) & ((1ULL << (SparqlParser::IRI_REF - 139))
      | (1ULL << (SparqlParser::PNAME_NS - 139))
      | (1ULL << (SparqlParser::BLANK_NODE_LABEL - 139))
      | (1ULL << (SparqlParser::VAR1 - 139))
      | (1ULL << (SparqlParser::VAR2 - 139))
      | (1ULL << (SparqlParser::INTEGER - 139))
      | (1ULL << (SparqlParser::DECIMAL - 139))
      | (1ULL << (SparqlParser::DOUBLE - 139))
      | (1ULL << (SparqlParser::INTEGER_POSITIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 139))
      | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 139))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL2 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 139))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 139))
      | (1ULL << (SparqlParser::NIL - 139))
      | (1ULL << (SparqlParser::ANON - 139)))) != 0));
    setState(803);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphNodeContext ------------------------------------------------------------------

SparqlParser::GraphNodeContext::GraphNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrTermContext* SparqlParser::GraphNodeContext::varOrTerm() {
  return getRuleContext<SparqlParser::VarOrTermContext>(0);
}

SparqlParser::TriplesNodeContext* SparqlParser::GraphNodeContext::triplesNode() {
  return getRuleContext<SparqlParser::TriplesNodeContext>(0);
}


size_t SparqlParser::GraphNodeContext::getRuleIndex() const {
  return SparqlParser::RuleGraphNode;
}

void SparqlParser::GraphNodeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphNode(this);
}

void SparqlParser::GraphNodeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphNode(this);
}


antlrcpp::Any SparqlParser::GraphNodeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGraphNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphNodeContext* SparqlParser::graphNode() {
  GraphNodeContext *_localctx = _tracker.createInstance<GraphNodeContext>(_ctx, getState());
  enterRule(_localctx, 162, SparqlParser::RuleGraphNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(807);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(805);
        varOrTerm();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::T__16: {
        enterOuterAlt(_localctx, 2);
        setState(806);
        triplesNode();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphNodePathContext ------------------------------------------------------------------

SparqlParser::GraphNodePathContext::GraphNodePathContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarOrTermContext* SparqlParser::GraphNodePathContext::varOrTerm() {
  return getRuleContext<SparqlParser::VarOrTermContext>(0);
}

SparqlParser::TriplesNodePathContext* SparqlParser::GraphNodePathContext::triplesNodePath() {
  return getRuleContext<SparqlParser::TriplesNodePathContext>(0);
}


size_t SparqlParser::GraphNodePathContext::getRuleIndex() const {
  return SparqlParser::RuleGraphNodePath;
}

void SparqlParser::GraphNodePathContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphNodePath(this);
}

void SparqlParser::GraphNodePathContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphNodePath(this);
}


antlrcpp::Any SparqlParser::GraphNodePathContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGraphNodePath(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphNodePathContext* SparqlParser::graphNodePath() {
  GraphNodePathContext *_localctx = _tracker.createInstance<GraphNodePathContext>(_ctx, getState());
  enterRule(_localctx, 164, SparqlParser::RuleGraphNodePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(811);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(809);
        varOrTerm();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::T__16: {
        enterOuterAlt(_localctx, 2);
        setState(810);
        triplesNodePath();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarOrTermContext ------------------------------------------------------------------

SparqlParser::VarOrTermContext::VarOrTermContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarContext* SparqlParser::VarOrTermContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}

SparqlParser::GraphTermContext* SparqlParser::VarOrTermContext::graphTerm() {
  return getRuleContext<SparqlParser::GraphTermContext>(0);
}


size_t SparqlParser::VarOrTermContext::getRuleIndex() const {
  return SparqlParser::RuleVarOrTerm;
}

void SparqlParser::VarOrTermContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVarOrTerm(this);
}

void SparqlParser::VarOrTermContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVarOrTerm(this);
}


antlrcpp::Any SparqlParser::VarOrTermContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVarOrTerm(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VarOrTermContext* SparqlParser::varOrTerm() {
  VarOrTermContext *_localctx = _tracker.createInstance<VarOrTermContext>(_ctx, getState());
  enterRule(_localctx, 166, SparqlParser::RuleVarOrTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(815);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(813);
        var();
        break;
      }

      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 2);
        setState(814);
        graphTerm();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarOrIriContext ------------------------------------------------------------------

SparqlParser::VarOrIriContext::VarOrIriContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::VarContext* SparqlParser::VarOrIriContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}

SparqlParser::IriContext* SparqlParser::VarOrIriContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}


size_t SparqlParser::VarOrIriContext::getRuleIndex() const {
  return SparqlParser::RuleVarOrIri;
}

void SparqlParser::VarOrIriContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVarOrIri(this);
}

void SparqlParser::VarOrIriContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVarOrIri(this);
}


antlrcpp::Any SparqlParser::VarOrIriContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVarOrIri(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VarOrIriContext* SparqlParser::varOrIri() {
  VarOrIriContext *_localctx = _tracker.createInstance<VarOrIriContext>(_ctx, getState());
  enterRule(_localctx, 168, SparqlParser::RuleVarOrIri);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(819);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(817);
        var();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 2);
        setState(818);
        iri();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarContext ------------------------------------------------------------------

SparqlParser::VarContext::VarContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::VarContext::VAR1() {
  return getToken(SparqlParser::VAR1, 0);
}

tree::TerminalNode* SparqlParser::VarContext::VAR2() {
  return getToken(SparqlParser::VAR2, 0);
}


size_t SparqlParser::VarContext::getRuleIndex() const {
  return SparqlParser::RuleVar;
}

void SparqlParser::VarContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterVar(this);
}

void SparqlParser::VarContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitVar(this);
}


antlrcpp::Any SparqlParser::VarContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitVar(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::VarContext* SparqlParser::var() {
  VarContext *_localctx = _tracker.createInstance<VarContext>(_ctx, getState());
  enterRule(_localctx, 170, SparqlParser::RuleVar);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(821);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::VAR1

    || _la == SparqlParser::VAR2)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphTermContext ------------------------------------------------------------------

SparqlParser::GraphTermContext::GraphTermContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::GraphTermContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}

SparqlParser::RdfLiteralContext* SparqlParser::GraphTermContext::rdfLiteral() {
  return getRuleContext<SparqlParser::RdfLiteralContext>(0);
}

SparqlParser::NumericLiteralContext* SparqlParser::GraphTermContext::numericLiteral() {
  return getRuleContext<SparqlParser::NumericLiteralContext>(0);
}

SparqlParser::BooleanLiteralContext* SparqlParser::GraphTermContext::booleanLiteral() {
  return getRuleContext<SparqlParser::BooleanLiteralContext>(0);
}

SparqlParser::BlankNodeContext* SparqlParser::GraphTermContext::blankNode() {
  return getRuleContext<SparqlParser::BlankNodeContext>(0);
}

tree::TerminalNode* SparqlParser::GraphTermContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}


size_t SparqlParser::GraphTermContext::getRuleIndex() const {
  return SparqlParser::RuleGraphTerm;
}

void SparqlParser::GraphTermContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphTerm(this);
}

void SparqlParser::GraphTermContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphTerm(this);
}


antlrcpp::Any SparqlParser::GraphTermContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitGraphTerm(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::GraphTermContext* SparqlParser::graphTerm() {
  GraphTermContext *_localctx = _tracker.createInstance<GraphTermContext>(_ctx, getState());
  enterRule(_localctx, 172, SparqlParser::RuleGraphTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(829);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 1);
        setState(823);
        iri();
        break;
      }

      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 2);
        setState(824);
        rdfLiteral();
        break;
      }

      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(825);
        numericLiteral();
        break;
      }

      case SparqlParser::T__29:
      case SparqlParser::T__30: {
        enterOuterAlt(_localctx, 4);
        setState(826);
        booleanLiteral();
        break;
      }

      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::ANON: {
        enterOuterAlt(_localctx, 5);
        setState(827);
        blankNode();
        break;
      }

      case SparqlParser::NIL: {
        enterOuterAlt(_localctx, 6);
        setState(828);
        match(SparqlParser::NIL);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext ------------------------------------------------------------------

SparqlParser::ExpressionContext::ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::ConditionalOrExpressionContext* SparqlParser::ExpressionContext::conditionalOrExpression() {
  return getRuleContext<SparqlParser::ConditionalOrExpressionContext>(0);
}


size_t SparqlParser::ExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleExpression;
}

void SparqlParser::ExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExpression(this);
}

void SparqlParser::ExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExpression(this);
}


antlrcpp::Any SparqlParser::ExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ExpressionContext* SparqlParser::expression() {
  ExpressionContext *_localctx = _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 174, SparqlParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(831);
    conditionalOrExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionalOrExpressionContext ------------------------------------------------------------------

SparqlParser::ConditionalOrExpressionContext::ConditionalOrExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::ConditionalAndExpressionContext *> SparqlParser::ConditionalOrExpressionContext::conditionalAndExpression() {
  return getRuleContexts<SparqlParser::ConditionalAndExpressionContext>();
}

SparqlParser::ConditionalAndExpressionContext* SparqlParser::ConditionalOrExpressionContext::conditionalAndExpression(size_t i) {
  return getRuleContext<SparqlParser::ConditionalAndExpressionContext>(i);
}


size_t SparqlParser::ConditionalOrExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleConditionalOrExpression;
}

void SparqlParser::ConditionalOrExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionalOrExpression(this);
}

void SparqlParser::ConditionalOrExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionalOrExpression(this);
}


antlrcpp::Any SparqlParser::ConditionalOrExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitConditionalOrExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConditionalOrExpressionContext* SparqlParser::conditionalOrExpression() {
  ConditionalOrExpressionContext *_localctx = _tracker.createInstance<ConditionalOrExpressionContext>(_ctx, getState());
  enterRule(_localctx, 176, SparqlParser::RuleConditionalOrExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(833);
    conditionalAndExpression();
    setState(838);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__18) {
      setState(834);
      match(SparqlParser::T__18);
      setState(835);
      conditionalAndExpression();
      setState(840);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionalAndExpressionContext ------------------------------------------------------------------

SparqlParser::ConditionalAndExpressionContext::ConditionalAndExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::ValueLogicalContext *> SparqlParser::ConditionalAndExpressionContext::valueLogical() {
  return getRuleContexts<SparqlParser::ValueLogicalContext>();
}

SparqlParser::ValueLogicalContext* SparqlParser::ConditionalAndExpressionContext::valueLogical(size_t i) {
  return getRuleContext<SparqlParser::ValueLogicalContext>(i);
}


size_t SparqlParser::ConditionalAndExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleConditionalAndExpression;
}

void SparqlParser::ConditionalAndExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionalAndExpression(this);
}

void SparqlParser::ConditionalAndExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionalAndExpression(this);
}


antlrcpp::Any SparqlParser::ConditionalAndExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitConditionalAndExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ConditionalAndExpressionContext* SparqlParser::conditionalAndExpression() {
  ConditionalAndExpressionContext *_localctx = _tracker.createInstance<ConditionalAndExpressionContext>(_ctx, getState());
  enterRule(_localctx, 178, SparqlParser::RuleConditionalAndExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(841);
    valueLogical();
    setState(846);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__19) {
      setState(842);
      match(SparqlParser::T__19);
      setState(843);
      valueLogical();
      setState(848);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueLogicalContext ------------------------------------------------------------------

SparqlParser::ValueLogicalContext::ValueLogicalContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::RelationalExpressionContext* SparqlParser::ValueLogicalContext::relationalExpression() {
  return getRuleContext<SparqlParser::RelationalExpressionContext>(0);
}


size_t SparqlParser::ValueLogicalContext::getRuleIndex() const {
  return SparqlParser::RuleValueLogical;
}

void SparqlParser::ValueLogicalContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterValueLogical(this);
}

void SparqlParser::ValueLogicalContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitValueLogical(this);
}


antlrcpp::Any SparqlParser::ValueLogicalContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitValueLogical(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ValueLogicalContext* SparqlParser::valueLogical() {
  ValueLogicalContext *_localctx = _tracker.createInstance<ValueLogicalContext>(_ctx, getState());
  enterRule(_localctx, 180, SparqlParser::RuleValueLogical);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(849);
    relationalExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationalExpressionContext ------------------------------------------------------------------

SparqlParser::RelationalExpressionContext::RelationalExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::NumericExpressionContext *> SparqlParser::RelationalExpressionContext::numericExpression() {
  return getRuleContexts<SparqlParser::NumericExpressionContext>();
}

SparqlParser::NumericExpressionContext* SparqlParser::RelationalExpressionContext::numericExpression(size_t i) {
  return getRuleContext<SparqlParser::NumericExpressionContext>(i);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::IN() {
  return getToken(SparqlParser::IN, 0);
}

SparqlParser::ExpressionListContext* SparqlParser::RelationalExpressionContext::expressionList() {
  return getRuleContext<SparqlParser::ExpressionListContext>(0);
}

tree::TerminalNode* SparqlParser::RelationalExpressionContext::NOT() {
  return getToken(SparqlParser::NOT, 0);
}


size_t SparqlParser::RelationalExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleRelationalExpression;
}

void SparqlParser::RelationalExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRelationalExpression(this);
}

void SparqlParser::RelationalExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRelationalExpression(this);
}


antlrcpp::Any SparqlParser::RelationalExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitRelationalExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::RelationalExpressionContext* SparqlParser::relationalExpression() {
  RelationalExpressionContext *_localctx = _tracker.createInstance<RelationalExpressionContext>(_ctx, getState());
  enterRule(_localctx, 182, SparqlParser::RuleRelationalExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(851);
    numericExpression();
    setState(869);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__20: {
        setState(852);
        match(SparqlParser::T__20);
        setState(853);
        numericExpression();
        break;
      }

      case SparqlParser::T__21: {
        setState(854);
        match(SparqlParser::T__21);
        setState(855);
        numericExpression();
        break;
      }

      case SparqlParser::T__22: {
        setState(856);
        match(SparqlParser::T__22);
        setState(857);
        numericExpression();
        break;
      }

      case SparqlParser::T__23: {
        setState(858);
        match(SparqlParser::T__23);
        setState(859);
        numericExpression();
        break;
      }

      case SparqlParser::T__24: {
        setState(860);
        match(SparqlParser::T__24);
        setState(861);
        numericExpression();
        break;
      }

      case SparqlParser::T__25: {
        setState(862);
        match(SparqlParser::T__25);
        setState(863);
        numericExpression();
        break;
      }

      case SparqlParser::IN: {
        setState(864);
        match(SparqlParser::IN);
        setState(865);
        expressionList();
        break;
      }

      case SparqlParser::NOT: {
        setState(866);
        match(SparqlParser::NOT);
        setState(867);
        match(SparqlParser::IN);
        setState(868);
        expressionList();
        break;
      }

      case SparqlParser::T__2:
      case SparqlParser::T__7:
      case SparqlParser::T__8:
      case SparqlParser::T__18:
      case SparqlParser::T__19:
      case SparqlParser::AS: {
        break;
      }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericExpressionContext ------------------------------------------------------------------

SparqlParser::NumericExpressionContext::NumericExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::AdditiveExpressionContext* SparqlParser::NumericExpressionContext::additiveExpression() {
  return getRuleContext<SparqlParser::AdditiveExpressionContext>(0);
}


size_t SparqlParser::NumericExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleNumericExpression;
}

void SparqlParser::NumericExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericExpression(this);
}

void SparqlParser::NumericExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericExpression(this);
}


antlrcpp::Any SparqlParser::NumericExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNumericExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericExpressionContext* SparqlParser::numericExpression() {
  NumericExpressionContext *_localctx = _tracker.createInstance<NumericExpressionContext>(_ctx, getState());
  enterRule(_localctx, 184, SparqlParser::RuleNumericExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(871);
    additiveExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AdditiveExpressionContext ------------------------------------------------------------------

SparqlParser::AdditiveExpressionContext::AdditiveExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::MultiplicativeExpressionContext *> SparqlParser::AdditiveExpressionContext::multiplicativeExpression() {
  return getRuleContexts<SparqlParser::MultiplicativeExpressionContext>();
}

SparqlParser::MultiplicativeExpressionContext* SparqlParser::AdditiveExpressionContext::multiplicativeExpression(size_t i) {
  return getRuleContext<SparqlParser::MultiplicativeExpressionContext>(i);
}

std::vector<SparqlParser::NumericLiteralPositiveContext *> SparqlParser::AdditiveExpressionContext::numericLiteralPositive() {
  return getRuleContexts<SparqlParser::NumericLiteralPositiveContext>();
}

SparqlParser::NumericLiteralPositiveContext* SparqlParser::AdditiveExpressionContext::numericLiteralPositive(size_t i) {
  return getRuleContext<SparqlParser::NumericLiteralPositiveContext>(i);
}

std::vector<SparqlParser::NumericLiteralNegativeContext *> SparqlParser::AdditiveExpressionContext::numericLiteralNegative() {
  return getRuleContexts<SparqlParser::NumericLiteralNegativeContext>();
}

SparqlParser::NumericLiteralNegativeContext* SparqlParser::AdditiveExpressionContext::numericLiteralNegative(size_t i) {
  return getRuleContext<SparqlParser::NumericLiteralNegativeContext>(i);
}

std::vector<SparqlParser::UnaryExpressionContext *> SparqlParser::AdditiveExpressionContext::unaryExpression() {
  return getRuleContexts<SparqlParser::UnaryExpressionContext>();
}

SparqlParser::UnaryExpressionContext* SparqlParser::AdditiveExpressionContext::unaryExpression(size_t i) {
  return getRuleContext<SparqlParser::UnaryExpressionContext>(i);
}


size_t SparqlParser::AdditiveExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleAdditiveExpression;
}

void SparqlParser::AdditiveExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAdditiveExpression(this);
}

void SparqlParser::AdditiveExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAdditiveExpression(this);
}


antlrcpp::Any SparqlParser::AdditiveExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitAdditiveExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::AdditiveExpressionContext* SparqlParser::additiveExpression() {
  AdditiveExpressionContext *_localctx = _tracker.createInstance<AdditiveExpressionContext>(_ctx, getState());
  enterRule(_localctx, 186, SparqlParser::RuleAdditiveExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(873);
    multiplicativeExpression();
    setState(893);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__13

    || _la == SparqlParser::T__26 || ((((_la - 148) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 148)) & ((1ULL << (SparqlParser::INTEGER_POSITIVE - 148))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 148))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 148))
      | (1ULL << (SparqlParser::INTEGER_NEGATIVE - 148))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 148))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 148)))) != 0)) {
      setState(891);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlParser::T__13: {
          setState(874);
          match(SparqlParser::T__13);
          setState(875);
          multiplicativeExpression();
          break;
        }

        case SparqlParser::T__26: {
          setState(876);
          match(SparqlParser::T__26);
          setState(877);
          multiplicativeExpression();
          break;
        }

        case SparqlParser::INTEGER_POSITIVE:
        case SparqlParser::DECIMAL_POSITIVE:
        case SparqlParser::DOUBLE_POSITIVE:
        case SparqlParser::INTEGER_NEGATIVE:
        case SparqlParser::DECIMAL_NEGATIVE:
        case SparqlParser::DOUBLE_NEGATIVE: {
          setState(880);
          _errHandler->sync(this);
          switch (_input->LA(1)) {
            case SparqlParser::INTEGER_POSITIVE:
            case SparqlParser::DECIMAL_POSITIVE:
            case SparqlParser::DOUBLE_POSITIVE: {
              setState(878);
              numericLiteralPositive();
              break;
            }

            case SparqlParser::INTEGER_NEGATIVE:
            case SparqlParser::DECIMAL_NEGATIVE:
            case SparqlParser::DOUBLE_NEGATIVE: {
              setState(879);
              numericLiteralNegative();
              break;
            }

          default:
            throw NoViableAltException(this);
          }
          setState(888);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == SparqlParser::T__0

          || _la == SparqlParser::T__11) {
            setState(886);
            _errHandler->sync(this);
            switch (_input->LA(1)) {
              case SparqlParser::T__0: {
                setState(882);
                match(SparqlParser::T__0);
                setState(883);
                unaryExpression();
                break;
              }

              case SparqlParser::T__11: {
                setState(884);
                match(SparqlParser::T__11);
                setState(885);
                unaryExpression();
                break;
              }

            default:
              throw NoViableAltException(this);
            }
            setState(890);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(895);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplicativeExpressionContext ------------------------------------------------------------------

SparqlParser::MultiplicativeExpressionContext::MultiplicativeExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<SparqlParser::UnaryExpressionContext *> SparqlParser::MultiplicativeExpressionContext::unaryExpression() {
  return getRuleContexts<SparqlParser::UnaryExpressionContext>();
}

SparqlParser::UnaryExpressionContext* SparqlParser::MultiplicativeExpressionContext::unaryExpression(size_t i) {
  return getRuleContext<SparqlParser::UnaryExpressionContext>(i);
}


size_t SparqlParser::MultiplicativeExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleMultiplicativeExpression;
}

void SparqlParser::MultiplicativeExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultiplicativeExpression(this);
}

void SparqlParser::MultiplicativeExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultiplicativeExpression(this);
}


antlrcpp::Any SparqlParser::MultiplicativeExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitMultiplicativeExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::MultiplicativeExpressionContext* SparqlParser::multiplicativeExpression() {
  MultiplicativeExpressionContext *_localctx = _tracker.createInstance<MultiplicativeExpressionContext>(_ctx, getState());
  enterRule(_localctx, 188, SparqlParser::RuleMultiplicativeExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(896);
    unaryExpression();
    setState(903);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlParser::T__0

    || _la == SparqlParser::T__11) {
      setState(901);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlParser::T__0: {
          setState(897);
          match(SparqlParser::T__0);
          setState(898);
          unaryExpression();
          break;
        }

        case SparqlParser::T__11: {
          setState(899);
          match(SparqlParser::T__11);
          setState(900);
          unaryExpression();
          break;
        }

      default:
        throw NoViableAltException(this);
      }
      setState(905);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UnaryExpressionContext ------------------------------------------------------------------

SparqlParser::UnaryExpressionContext::UnaryExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PrimaryExpressionContext* SparqlParser::UnaryExpressionContext::primaryExpression() {
  return getRuleContext<SparqlParser::PrimaryExpressionContext>(0);
}


size_t SparqlParser::UnaryExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleUnaryExpression;
}

void SparqlParser::UnaryExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterUnaryExpression(this);
}

void SparqlParser::UnaryExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitUnaryExpression(this);
}


antlrcpp::Any SparqlParser::UnaryExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitUnaryExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::UnaryExpressionContext* SparqlParser::unaryExpression() {
  UnaryExpressionContext *_localctx = _tracker.createInstance<UnaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 190, SparqlParser::RuleUnaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(913);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__15: {
        enterOuterAlt(_localctx, 1);
        setState(906);
        match(SparqlParser::T__15);
        setState(907);
        primaryExpression();
        break;
      }

      case SparqlParser::T__13: {
        enterOuterAlt(_localctx, 2);
        setState(908);
        match(SparqlParser::T__13);
        setState(909);
        primaryExpression();
        break;
      }

      case SparqlParser::T__26: {
        enterOuterAlt(_localctx, 3);
        setState(910);
        match(SparqlParser::T__26);
        setState(911);
        primaryExpression();
        break;
      }

      case SparqlParser::T__1:
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::GROUP_CONCAT:
      case SparqlParser::NOT:
      case SparqlParser::STR:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::DATATYPE:
      case SparqlParser::BOUND:
      case SparqlParser::IRI:
      case SparqlParser::URI:
      case SparqlParser::BNODE:
      case SparqlParser::RAND:
      case SparqlParser::ABS:
      case SparqlParser::CEIL:
      case SparqlParser::FLOOR:
      case SparqlParser::ROUND:
      case SparqlParser::CONCAT:
      case SparqlParser::STRLEN:
      case SparqlParser::UCASE:
      case SparqlParser::LCASE:
      case SparqlParser::ENCODE:
      case SparqlParser::CONTAINS:
      case SparqlParser::STRSTARTS:
      case SparqlParser::STRENDS:
      case SparqlParser::STRBEFORE:
      case SparqlParser::STRAFTER:
      case SparqlParser::YEAR:
      case SparqlParser::MONTH:
      case SparqlParser::DAY:
      case SparqlParser::HOURS:
      case SparqlParser::MINUTES:
      case SparqlParser::SECONDS:
      case SparqlParser::TIMEZONE:
      case SparqlParser::TZ:
      case SparqlParser::NOW:
      case SparqlParser::UUID:
      case SparqlParser::STRUUID:
      case SparqlParser::SHA1:
      case SparqlParser::SHA256:
      case SparqlParser::SHA384:
      case SparqlParser::SHA512:
      case SparqlParser::MD5:
      case SparqlParser::COALESCE:
      case SparqlParser::IF:
      case SparqlParser::STRLANG:
      case SparqlParser::STRDT:
      case SparqlParser::SAMETERM:
      case SparqlParser::ISIRI:
      case SparqlParser::ISURI:
      case SparqlParser::ISBLANK:
      case SparqlParser::ISLITERAL:
      case SparqlParser::ISNUMERIC:
      case SparqlParser::REGEX:
      case SparqlParser::SUBSTR:
      case SparqlParser::REPLACE:
      case SparqlParser::EXISTS:
      case SparqlParser::COUNT:
      case SparqlParser::SUM:
      case SparqlParser::MIN:
      case SparqlParser::MAX:
      case SparqlParser::AVG:
      case SparqlParser::SAMPLE:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 4);
        setState(912);
        primaryExpression();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrimaryExpressionContext ------------------------------------------------------------------

SparqlParser::PrimaryExpressionContext::PrimaryExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::BrackettedExpressionContext* SparqlParser::PrimaryExpressionContext::brackettedExpression() {
  return getRuleContext<SparqlParser::BrackettedExpressionContext>(0);
}

SparqlParser::BuiltInCallContext* SparqlParser::PrimaryExpressionContext::builtInCall() {
  return getRuleContext<SparqlParser::BuiltInCallContext>(0);
}

SparqlParser::IriOrFunctionContext* SparqlParser::PrimaryExpressionContext::iriOrFunction() {
  return getRuleContext<SparqlParser::IriOrFunctionContext>(0);
}

SparqlParser::RdfLiteralContext* SparqlParser::PrimaryExpressionContext::rdfLiteral() {
  return getRuleContext<SparqlParser::RdfLiteralContext>(0);
}

SparqlParser::NumericLiteralContext* SparqlParser::PrimaryExpressionContext::numericLiteral() {
  return getRuleContext<SparqlParser::NumericLiteralContext>(0);
}

SparqlParser::BooleanLiteralContext* SparqlParser::PrimaryExpressionContext::booleanLiteral() {
  return getRuleContext<SparqlParser::BooleanLiteralContext>(0);
}

SparqlParser::VarContext* SparqlParser::PrimaryExpressionContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}


size_t SparqlParser::PrimaryExpressionContext::getRuleIndex() const {
  return SparqlParser::RulePrimaryExpression;
}

void SparqlParser::PrimaryExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrimaryExpression(this);
}

void SparqlParser::PrimaryExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrimaryExpression(this);
}


antlrcpp::Any SparqlParser::PrimaryExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPrimaryExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrimaryExpressionContext* SparqlParser::primaryExpression() {
  PrimaryExpressionContext *_localctx = _tracker.createInstance<PrimaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 192, SparqlParser::RulePrimaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(922);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(915);
        brackettedExpression();
        break;
      }

      case SparqlParser::GROUP_CONCAT:
      case SparqlParser::NOT:
      case SparqlParser::STR:
      case SparqlParser::LANG:
      case SparqlParser::LANGMATCHES:
      case SparqlParser::DATATYPE:
      case SparqlParser::BOUND:
      case SparqlParser::IRI:
      case SparqlParser::URI:
      case SparqlParser::BNODE:
      case SparqlParser::RAND:
      case SparqlParser::ABS:
      case SparqlParser::CEIL:
      case SparqlParser::FLOOR:
      case SparqlParser::ROUND:
      case SparqlParser::CONCAT:
      case SparqlParser::STRLEN:
      case SparqlParser::UCASE:
      case SparqlParser::LCASE:
      case SparqlParser::ENCODE:
      case SparqlParser::CONTAINS:
      case SparqlParser::STRSTARTS:
      case SparqlParser::STRENDS:
      case SparqlParser::STRBEFORE:
      case SparqlParser::STRAFTER:
      case SparqlParser::YEAR:
      case SparqlParser::MONTH:
      case SparqlParser::DAY:
      case SparqlParser::HOURS:
      case SparqlParser::MINUTES:
      case SparqlParser::SECONDS:
      case SparqlParser::TIMEZONE:
      case SparqlParser::TZ:
      case SparqlParser::NOW:
      case SparqlParser::UUID:
      case SparqlParser::STRUUID:
      case SparqlParser::SHA1:
      case SparqlParser::SHA256:
      case SparqlParser::SHA384:
      case SparqlParser::SHA512:
      case SparqlParser::MD5:
      case SparqlParser::COALESCE:
      case SparqlParser::IF:
      case SparqlParser::STRLANG:
      case SparqlParser::STRDT:
      case SparqlParser::SAMETERM:
      case SparqlParser::ISIRI:
      case SparqlParser::ISURI:
      case SparqlParser::ISBLANK:
      case SparqlParser::ISLITERAL:
      case SparqlParser::ISNUMERIC:
      case SparqlParser::REGEX:
      case SparqlParser::SUBSTR:
      case SparqlParser::REPLACE:
      case SparqlParser::EXISTS:
      case SparqlParser::COUNT:
      case SparqlParser::SUM:
      case SparqlParser::MIN:
      case SparqlParser::MAX:
      case SparqlParser::AVG:
      case SparqlParser::SAMPLE: {
        enterOuterAlt(_localctx, 2);
        setState(916);
        builtInCall();
        break;
      }

      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 3);
        setState(917);
        iriOrFunction();
        break;
      }

      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 4);
        setState(918);
        rdfLiteral();
        break;
      }

      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 5);
        setState(919);
        numericLiteral();
        break;
      }

      case SparqlParser::T__29:
      case SparqlParser::T__30: {
        enterOuterAlt(_localctx, 6);
        setState(920);
        booleanLiteral();
        break;
      }

      case SparqlParser::VAR1:
      case SparqlParser::VAR2: {
        enterOuterAlt(_localctx, 7);
        setState(921);
        var();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BrackettedExpressionContext ------------------------------------------------------------------

SparqlParser::BrackettedExpressionContext::BrackettedExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::ExpressionContext* SparqlParser::BrackettedExpressionContext::expression() {
  return getRuleContext<SparqlParser::ExpressionContext>(0);
}


size_t SparqlParser::BrackettedExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleBrackettedExpression;
}

void SparqlParser::BrackettedExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBrackettedExpression(this);
}

void SparqlParser::BrackettedExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBrackettedExpression(this);
}


antlrcpp::Any SparqlParser::BrackettedExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBrackettedExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BrackettedExpressionContext* SparqlParser::brackettedExpression() {
  BrackettedExpressionContext *_localctx = _tracker.createInstance<BrackettedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 194, SparqlParser::RuleBrackettedExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(924);
    match(SparqlParser::T__1);
    setState(925);
    expression();
    setState(926);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BuiltInCallContext ------------------------------------------------------------------

SparqlParser::BuiltInCallContext::BuiltInCallContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::AggregateContext* SparqlParser::BuiltInCallContext::aggregate() {
  return getRuleContext<SparqlParser::AggregateContext>(0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STR() {
  return getToken(SparqlParser::STR, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::BuiltInCallContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::BuiltInCallContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::LANG() {
  return getToken(SparqlParser::LANG, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::LANGMATCHES() {
  return getToken(SparqlParser::LANGMATCHES, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::DATATYPE() {
  return getToken(SparqlParser::DATATYPE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::BOUND() {
  return getToken(SparqlParser::BOUND, 0);
}

SparqlParser::VarContext* SparqlParser::BuiltInCallContext::var() {
  return getRuleContext<SparqlParser::VarContext>(0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::IRI() {
  return getToken(SparqlParser::IRI, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::URI() {
  return getToken(SparqlParser::URI, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::BNODE() {
  return getToken(SparqlParser::BNODE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::NIL() {
  return getToken(SparqlParser::NIL, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::RAND() {
  return getToken(SparqlParser::RAND, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ABS() {
  return getToken(SparqlParser::ABS, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::CEIL() {
  return getToken(SparqlParser::CEIL, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::FLOOR() {
  return getToken(SparqlParser::FLOOR, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ROUND() {
  return getToken(SparqlParser::ROUND, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::CONCAT() {
  return getToken(SparqlParser::CONCAT, 0);
}

SparqlParser::ExpressionListContext* SparqlParser::BuiltInCallContext::expressionList() {
  return getRuleContext<SparqlParser::ExpressionListContext>(0);
}

SparqlParser::SubstringExpressionContext* SparqlParser::BuiltInCallContext::substringExpression() {
  return getRuleContext<SparqlParser::SubstringExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRLEN() {
  return getToken(SparqlParser::STRLEN, 0);
}

SparqlParser::StrReplaceExpressionContext* SparqlParser::BuiltInCallContext::strReplaceExpression() {
  return getRuleContext<SparqlParser::StrReplaceExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::UCASE() {
  return getToken(SparqlParser::UCASE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::LCASE() {
  return getToken(SparqlParser::LCASE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ENCODE() {
  return getToken(SparqlParser::ENCODE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::FOR() {
  return getToken(SparqlParser::FOR, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::CONTAINS() {
  return getToken(SparqlParser::CONTAINS, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRSTARTS() {
  return getToken(SparqlParser::STRSTARTS, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRENDS() {
  return getToken(SparqlParser::STRENDS, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRBEFORE() {
  return getToken(SparqlParser::STRBEFORE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRAFTER() {
  return getToken(SparqlParser::STRAFTER, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::YEAR() {
  return getToken(SparqlParser::YEAR, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::MONTH() {
  return getToken(SparqlParser::MONTH, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::DAY() {
  return getToken(SparqlParser::DAY, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::HOURS() {
  return getToken(SparqlParser::HOURS, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::MINUTES() {
  return getToken(SparqlParser::MINUTES, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SECONDS() {
  return getToken(SparqlParser::SECONDS, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::TIMEZONE() {
  return getToken(SparqlParser::TIMEZONE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::TZ() {
  return getToken(SparqlParser::TZ, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::NOW() {
  return getToken(SparqlParser::NOW, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::UUID() {
  return getToken(SparqlParser::UUID, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRUUID() {
  return getToken(SparqlParser::STRUUID, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::MD5() {
  return getToken(SparqlParser::MD5, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SHA1() {
  return getToken(SparqlParser::SHA1, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SHA256() {
  return getToken(SparqlParser::SHA256, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SHA384() {
  return getToken(SparqlParser::SHA384, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SHA512() {
  return getToken(SparqlParser::SHA512, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::COALESCE() {
  return getToken(SparqlParser::COALESCE, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::IF() {
  return getToken(SparqlParser::IF, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRLANG() {
  return getToken(SparqlParser::STRLANG, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::STRDT() {
  return getToken(SparqlParser::STRDT, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::SAMETERM() {
  return getToken(SparqlParser::SAMETERM, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ISIRI() {
  return getToken(SparqlParser::ISIRI, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ISURI() {
  return getToken(SparqlParser::ISURI, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ISBLANK() {
  return getToken(SparqlParser::ISBLANK, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ISLITERAL() {
  return getToken(SparqlParser::ISLITERAL, 0);
}

tree::TerminalNode* SparqlParser::BuiltInCallContext::ISNUMERIC() {
  return getToken(SparqlParser::ISNUMERIC, 0);
}

SparqlParser::RegexExpressionContext* SparqlParser::BuiltInCallContext::regexExpression() {
  return getRuleContext<SparqlParser::RegexExpressionContext>(0);
}

SparqlParser::ExistsFuncContext* SparqlParser::BuiltInCallContext::existsFunc() {
  return getRuleContext<SparqlParser::ExistsFuncContext>(0);
}

SparqlParser::NotExistsFuncContext* SparqlParser::BuiltInCallContext::notExistsFunc() {
  return getRuleContext<SparqlParser::NotExistsFuncContext>(0);
}


size_t SparqlParser::BuiltInCallContext::getRuleIndex() const {
  return SparqlParser::RuleBuiltInCall;
}

void SparqlParser::BuiltInCallContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBuiltInCall(this);
}

void SparqlParser::BuiltInCallContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBuiltInCall(this);
}


antlrcpp::Any SparqlParser::BuiltInCallContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBuiltInCall(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BuiltInCallContext* SparqlParser::builtInCall() {
  BuiltInCallContext *_localctx = _tracker.createInstance<BuiltInCallContext>(_ctx, getState());
  enterRule(_localctx, 196, SparqlParser::RuleBuiltInCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1190);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::GROUP_CONCAT:
      case SparqlParser::COUNT:
      case SparqlParser::SUM:
      case SparqlParser::MIN:
      case SparqlParser::MAX:
      case SparqlParser::AVG:
      case SparqlParser::SAMPLE: {
        enterOuterAlt(_localctx, 1);
        setState(928);
        aggregate();
        break;
      }

      case SparqlParser::STR: {
        enterOuterAlt(_localctx, 2);
        setState(929);
        match(SparqlParser::STR);
        setState(930);
        match(SparqlParser::T__1);
        setState(931);
        expression();
        setState(932);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::LANG: {
        enterOuterAlt(_localctx, 3);
        setState(934);
        match(SparqlParser::LANG);
        setState(935);
        match(SparqlParser::T__1);
        setState(936);
        expression();
        setState(937);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::LANGMATCHES: {
        enterOuterAlt(_localctx, 4);
        setState(939);
        match(SparqlParser::LANGMATCHES);
        setState(940);
        match(SparqlParser::T__1);
        setState(941);
        expression();
        setState(942);
        match(SparqlParser::T__7);
        setState(943);
        expression();
        setState(944);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::DATATYPE: {
        enterOuterAlt(_localctx, 5);
        setState(946);
        match(SparqlParser::DATATYPE);
        setState(947);
        match(SparqlParser::T__1);
        setState(948);
        expression();
        setState(949);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::BOUND: {
        enterOuterAlt(_localctx, 6);
        setState(951);
        match(SparqlParser::BOUND);
        setState(952);
        match(SparqlParser::T__1);
        setState(953);
        var();
        setState(954);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::IRI: {
        enterOuterAlt(_localctx, 7);
        setState(956);
        match(SparqlParser::IRI);
        setState(957);
        match(SparqlParser::T__1);
        setState(958);
        expression();
        setState(959);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::URI: {
        enterOuterAlt(_localctx, 8);
        setState(961);
        match(SparqlParser::URI);
        setState(962);
        match(SparqlParser::T__1);
        setState(963);
        expression();
        setState(964);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::BNODE: {
        enterOuterAlt(_localctx, 9);
        setState(966);
        match(SparqlParser::BNODE);
        setState(972);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlParser::T__1: {
            setState(967);
            match(SparqlParser::T__1);
            setState(968);
            expression();
            setState(969);
            match(SparqlParser::T__2);
            break;
          }

          case SparqlParser::NIL: {
            setState(971);
            match(SparqlParser::NIL);
            break;
          }

        default:
          throw NoViableAltException(this);
        }
        break;
      }

      case SparqlParser::RAND: {
        enterOuterAlt(_localctx, 10);
        setState(974);
        match(SparqlParser::RAND);
        setState(975);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::ABS: {
        enterOuterAlt(_localctx, 11);
        setState(976);
        match(SparqlParser::ABS);
        setState(977);
        match(SparqlParser::T__1);
        setState(978);
        expression();
        setState(979);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::CEIL: {
        enterOuterAlt(_localctx, 12);
        setState(981);
        match(SparqlParser::CEIL);
        setState(982);
        match(SparqlParser::T__1);
        setState(983);
        expression();
        setState(984);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::FLOOR: {
        enterOuterAlt(_localctx, 13);
        setState(986);
        match(SparqlParser::FLOOR);
        setState(987);
        match(SparqlParser::T__1);
        setState(988);
        expression();
        setState(989);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ROUND: {
        enterOuterAlt(_localctx, 14);
        setState(991);
        match(SparqlParser::ROUND);
        setState(992);
        match(SparqlParser::T__1);
        setState(993);
        expression();
        setState(994);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::CONCAT: {
        enterOuterAlt(_localctx, 15);
        setState(996);
        match(SparqlParser::CONCAT);
        setState(997);
        expressionList();
        break;
      }

      case SparqlParser::SUBSTR: {
        enterOuterAlt(_localctx, 16);
        setState(998);
        substringExpression();
        break;
      }

      case SparqlParser::STRLEN: {
        enterOuterAlt(_localctx, 17);
        setState(999);
        match(SparqlParser::STRLEN);
        setState(1000);
        match(SparqlParser::T__1);
        setState(1001);
        expression();
        setState(1002);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::REPLACE: {
        enterOuterAlt(_localctx, 18);
        setState(1004);
        strReplaceExpression();
        break;
      }

      case SparqlParser::UCASE: {
        enterOuterAlt(_localctx, 19);
        setState(1005);
        match(SparqlParser::UCASE);
        setState(1006);
        match(SparqlParser::T__1);
        setState(1007);
        expression();
        setState(1008);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::LCASE: {
        enterOuterAlt(_localctx, 20);
        setState(1010);
        match(SparqlParser::LCASE);
        setState(1011);
        match(SparqlParser::T__1);
        setState(1012);
        expression();
        setState(1013);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ENCODE: {
        enterOuterAlt(_localctx, 21);
        setState(1015);
        match(SparqlParser::ENCODE);
        setState(1016);
        match(SparqlParser::T__27);
        setState(1017);
        match(SparqlParser::FOR);
        setState(1018);
        match(SparqlParser::T__27);
        setState(1019);
        match(SparqlParser::URI);
        setState(1020);
        match(SparqlParser::T__1);
        setState(1021);
        expression();
        setState(1022);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::CONTAINS: {
        enterOuterAlt(_localctx, 22);
        setState(1024);
        match(SparqlParser::CONTAINS);
        setState(1025);
        match(SparqlParser::T__1);
        setState(1026);
        expression();
        setState(1027);
        match(SparqlParser::T__7);
        setState(1028);
        expression();
        setState(1029);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::STRSTARTS: {
        enterOuterAlt(_localctx, 23);
        setState(1031);
        match(SparqlParser::STRSTARTS);
        setState(1032);
        match(SparqlParser::T__1);
        setState(1033);
        expression();
        setState(1034);
        match(SparqlParser::T__7);
        setState(1035);
        expression();
        setState(1036);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::STRENDS: {
        enterOuterAlt(_localctx, 24);
        setState(1038);
        match(SparqlParser::STRENDS);
        setState(1039);
        match(SparqlParser::T__1);
        setState(1040);
        expression();
        setState(1041);
        match(SparqlParser::T__7);
        setState(1042);
        expression();
        setState(1043);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::STRBEFORE: {
        enterOuterAlt(_localctx, 25);
        setState(1045);
        match(SparqlParser::STRBEFORE);
        setState(1046);
        match(SparqlParser::T__1);
        setState(1047);
        expression();
        setState(1048);
        match(SparqlParser::T__7);
        setState(1049);
        expression();
        setState(1050);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::STRAFTER: {
        enterOuterAlt(_localctx, 26);
        setState(1052);
        match(SparqlParser::STRAFTER);
        setState(1053);
        match(SparqlParser::T__1);
        setState(1054);
        expression();
        setState(1055);
        match(SparqlParser::T__7);
        setState(1056);
        expression();
        setState(1057);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::YEAR: {
        enterOuterAlt(_localctx, 27);
        setState(1059);
        match(SparqlParser::YEAR);
        setState(1060);
        match(SparqlParser::T__1);
        setState(1061);
        expression();
        setState(1062);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::MONTH: {
        enterOuterAlt(_localctx, 28);
        setState(1064);
        match(SparqlParser::MONTH);
        setState(1065);
        match(SparqlParser::T__1);
        setState(1066);
        expression();
        setState(1067);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::DAY: {
        enterOuterAlt(_localctx, 29);
        setState(1069);
        match(SparqlParser::DAY);
        setState(1070);
        match(SparqlParser::T__1);
        setState(1071);
        expression();
        setState(1072);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::HOURS: {
        enterOuterAlt(_localctx, 30);
        setState(1074);
        match(SparqlParser::HOURS);
        setState(1075);
        match(SparqlParser::T__1);
        setState(1076);
        expression();
        setState(1077);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::MINUTES: {
        enterOuterAlt(_localctx, 31);
        setState(1079);
        match(SparqlParser::MINUTES);
        setState(1080);
        match(SparqlParser::T__1);
        setState(1081);
        expression();
        setState(1082);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SECONDS: {
        enterOuterAlt(_localctx, 32);
        setState(1084);
        match(SparqlParser::SECONDS);
        setState(1085);
        match(SparqlParser::T__1);
        setState(1086);
        expression();
        setState(1087);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::TIMEZONE: {
        enterOuterAlt(_localctx, 33);
        setState(1089);
        match(SparqlParser::TIMEZONE);
        setState(1090);
        match(SparqlParser::T__1);
        setState(1091);
        expression();
        setState(1092);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::TZ: {
        enterOuterAlt(_localctx, 34);
        setState(1094);
        match(SparqlParser::TZ);
        setState(1095);
        match(SparqlParser::T__1);
        setState(1096);
        expression();
        setState(1097);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::NOW: {
        enterOuterAlt(_localctx, 35);
        setState(1099);
        match(SparqlParser::NOW);
        setState(1100);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::UUID: {
        enterOuterAlt(_localctx, 36);
        setState(1101);
        match(SparqlParser::UUID);
        setState(1102);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::STRUUID: {
        enterOuterAlt(_localctx, 37);
        setState(1103);
        match(SparqlParser::STRUUID);
        setState(1104);
        match(SparqlParser::NIL);
        break;
      }

      case SparqlParser::MD5: {
        enterOuterAlt(_localctx, 38);
        setState(1105);
        match(SparqlParser::MD5);
        setState(1106);
        match(SparqlParser::T__1);
        setState(1107);
        expression();
        setState(1108);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SHA1: {
        enterOuterAlt(_localctx, 39);
        setState(1110);
        match(SparqlParser::SHA1);
        setState(1111);
        match(SparqlParser::T__1);
        setState(1112);
        expression();
        setState(1113);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SHA256: {
        enterOuterAlt(_localctx, 40);
        setState(1115);
        match(SparqlParser::SHA256);
        setState(1116);
        match(SparqlParser::T__1);
        setState(1117);
        expression();
        setState(1118);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SHA384: {
        enterOuterAlt(_localctx, 41);
        setState(1120);
        match(SparqlParser::SHA384);
        setState(1121);
        match(SparqlParser::T__1);
        setState(1122);
        expression();
        setState(1123);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SHA512: {
        enterOuterAlt(_localctx, 42);
        setState(1125);
        match(SparqlParser::SHA512);
        setState(1126);
        match(SparqlParser::T__1);
        setState(1127);
        expression();
        setState(1128);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::COALESCE: {
        enterOuterAlt(_localctx, 43);
        setState(1130);
        match(SparqlParser::COALESCE);
        setState(1131);
        expressionList();
        break;
      }

      case SparqlParser::IF: {
        enterOuterAlt(_localctx, 44);
        setState(1132);
        match(SparqlParser::IF);
        setState(1133);
        match(SparqlParser::T__1);
        setState(1134);
        expression();
        setState(1135);
        match(SparqlParser::T__7);
        setState(1136);
        expression();
        setState(1137);
        match(SparqlParser::T__7);
        setState(1138);
        expression();
        setState(1139);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::STRLANG: {
        enterOuterAlt(_localctx, 45);
        setState(1141);
        match(SparqlParser::STRLANG);
        setState(1142);
        match(SparqlParser::T__1);
        setState(1143);
        expression();
        setState(1144);
        match(SparqlParser::T__7);
        setState(1145);
        expression();
        setState(1146);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::STRDT: {
        enterOuterAlt(_localctx, 46);
        setState(1148);
        match(SparqlParser::STRDT);
        setState(1149);
        match(SparqlParser::T__1);
        setState(1150);
        expression();
        setState(1151);
        match(SparqlParser::T__7);
        setState(1152);
        expression();
        setState(1153);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SAMETERM: {
        enterOuterAlt(_localctx, 47);
        setState(1155);
        match(SparqlParser::SAMETERM);
        setState(1156);
        match(SparqlParser::T__1);
        setState(1157);
        expression();
        setState(1158);
        match(SparqlParser::T__7);
        setState(1159);
        expression();
        setState(1160);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ISIRI: {
        enterOuterAlt(_localctx, 48);
        setState(1162);
        match(SparqlParser::ISIRI);
        setState(1163);
        match(SparqlParser::T__1);
        setState(1164);
        expression();
        setState(1165);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ISURI: {
        enterOuterAlt(_localctx, 49);
        setState(1167);
        match(SparqlParser::ISURI);
        setState(1168);
        match(SparqlParser::T__1);
        setState(1169);
        expression();
        setState(1170);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ISBLANK: {
        enterOuterAlt(_localctx, 50);
        setState(1172);
        match(SparqlParser::ISBLANK);
        setState(1173);
        match(SparqlParser::T__1);
        setState(1174);
        expression();
        setState(1175);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ISLITERAL: {
        enterOuterAlt(_localctx, 51);
        setState(1177);
        match(SparqlParser::ISLITERAL);
        setState(1178);
        match(SparqlParser::T__1);
        setState(1179);
        expression();
        setState(1180);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::ISNUMERIC: {
        enterOuterAlt(_localctx, 52);
        setState(1182);
        match(SparqlParser::ISNUMERIC);
        setState(1183);
        match(SparqlParser::T__1);
        setState(1184);
        expression();
        setState(1185);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::REGEX: {
        enterOuterAlt(_localctx, 53);
        setState(1187);
        regexExpression();
        break;
      }

      case SparqlParser::EXISTS: {
        enterOuterAlt(_localctx, 54);
        setState(1188);
        existsFunc();
        break;
      }

      case SparqlParser::NOT: {
        enterOuterAlt(_localctx, 55);
        setState(1189);
        notExistsFunc();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RegexExpressionContext ------------------------------------------------------------------

SparqlParser::RegexExpressionContext::RegexExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::RegexExpressionContext::REGEX() {
  return getToken(SparqlParser::REGEX, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::RegexExpressionContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::RegexExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}


size_t SparqlParser::RegexExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleRegexExpression;
}

void SparqlParser::RegexExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRegexExpression(this);
}

void SparqlParser::RegexExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRegexExpression(this);
}


antlrcpp::Any SparqlParser::RegexExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitRegexExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::RegexExpressionContext* SparqlParser::regexExpression() {
  RegexExpressionContext *_localctx = _tracker.createInstance<RegexExpressionContext>(_ctx, getState());
  enterRule(_localctx, 198, SparqlParser::RuleRegexExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1192);
    match(SparqlParser::REGEX);
    setState(1193);
    match(SparqlParser::T__1);
    setState(1194);
    expression();
    setState(1195);
    match(SparqlParser::T__7);
    setState(1196);
    expression();
    setState(1199);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__7) {
      setState(1197);
      match(SparqlParser::T__7);
      setState(1198);
      expression();
    }
    setState(1201);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubstringExpressionContext ------------------------------------------------------------------

SparqlParser::SubstringExpressionContext::SubstringExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::SubstringExpressionContext::SUBSTR() {
  return getToken(SparqlParser::SUBSTR, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::SubstringExpressionContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::SubstringExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}


size_t SparqlParser::SubstringExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleSubstringExpression;
}

void SparqlParser::SubstringExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterSubstringExpression(this);
}

void SparqlParser::SubstringExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitSubstringExpression(this);
}


antlrcpp::Any SparqlParser::SubstringExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitSubstringExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::SubstringExpressionContext* SparqlParser::substringExpression() {
  SubstringExpressionContext *_localctx = _tracker.createInstance<SubstringExpressionContext>(_ctx, getState());
  enterRule(_localctx, 200, SparqlParser::RuleSubstringExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1203);
    match(SparqlParser::SUBSTR);
    setState(1204);
    match(SparqlParser::T__1);
    setState(1205);
    expression();
    setState(1206);
    match(SparqlParser::T__7);
    setState(1207);
    expression();
    setState(1210);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__7) {
      setState(1208);
      match(SparqlParser::T__7);
      setState(1209);
      expression();
    }
    setState(1212);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StrReplaceExpressionContext ------------------------------------------------------------------

SparqlParser::StrReplaceExpressionContext::StrReplaceExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::StrReplaceExpressionContext::REPLACE() {
  return getToken(SparqlParser::REPLACE, 0);
}

std::vector<SparqlParser::ExpressionContext *> SparqlParser::StrReplaceExpressionContext::expression() {
  return getRuleContexts<SparqlParser::ExpressionContext>();
}

SparqlParser::ExpressionContext* SparqlParser::StrReplaceExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlParser::ExpressionContext>(i);
}


size_t SparqlParser::StrReplaceExpressionContext::getRuleIndex() const {
  return SparqlParser::RuleStrReplaceExpression;
}

void SparqlParser::StrReplaceExpressionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterStrReplaceExpression(this);
}

void SparqlParser::StrReplaceExpressionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitStrReplaceExpression(this);
}


antlrcpp::Any SparqlParser::StrReplaceExpressionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitStrReplaceExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::StrReplaceExpressionContext* SparqlParser::strReplaceExpression() {
  StrReplaceExpressionContext *_localctx = _tracker.createInstance<StrReplaceExpressionContext>(_ctx, getState());
  enterRule(_localctx, 202, SparqlParser::RuleStrReplaceExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1214);
    match(SparqlParser::REPLACE);
    setState(1215);
    match(SparqlParser::T__1);
    setState(1216);
    expression();
    setState(1217);
    match(SparqlParser::T__7);
    setState(1218);
    expression();
    setState(1219);
    match(SparqlParser::T__7);
    setState(1220);
    expression();
    setState(1223);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__7) {
      setState(1221);
      match(SparqlParser::T__7);
      setState(1222);
      expression();
    }
    setState(1225);
    match(SparqlParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExistsFuncContext ------------------------------------------------------------------

SparqlParser::ExistsFuncContext::ExistsFuncContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::ExistsFuncContext::EXISTS() {
  return getToken(SparqlParser::EXISTS, 0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::ExistsFuncContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::ExistsFuncContext::getRuleIndex() const {
  return SparqlParser::RuleExistsFunc;
}

void SparqlParser::ExistsFuncContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterExistsFunc(this);
}

void SparqlParser::ExistsFuncContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitExistsFunc(this);
}


antlrcpp::Any SparqlParser::ExistsFuncContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitExistsFunc(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::ExistsFuncContext* SparqlParser::existsFunc() {
  ExistsFuncContext *_localctx = _tracker.createInstance<ExistsFuncContext>(_ctx, getState());
  enterRule(_localctx, 204, SparqlParser::RuleExistsFunc);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1227);
    match(SparqlParser::EXISTS);
    setState(1228);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NotExistsFuncContext ------------------------------------------------------------------

SparqlParser::NotExistsFuncContext::NotExistsFuncContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NotExistsFuncContext::NOT() {
  return getToken(SparqlParser::NOT, 0);
}

tree::TerminalNode* SparqlParser::NotExistsFuncContext::EXISTS() {
  return getToken(SparqlParser::EXISTS, 0);
}

SparqlParser::GroupGraphPatternContext* SparqlParser::NotExistsFuncContext::groupGraphPattern() {
  return getRuleContext<SparqlParser::GroupGraphPatternContext>(0);
}


size_t SparqlParser::NotExistsFuncContext::getRuleIndex() const {
  return SparqlParser::RuleNotExistsFunc;
}

void SparqlParser::NotExistsFuncContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNotExistsFunc(this);
}

void SparqlParser::NotExistsFuncContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNotExistsFunc(this);
}


antlrcpp::Any SparqlParser::NotExistsFuncContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNotExistsFunc(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NotExistsFuncContext* SparqlParser::notExistsFunc() {
  NotExistsFuncContext *_localctx = _tracker.createInstance<NotExistsFuncContext>(_ctx, getState());
  enterRule(_localctx, 206, SparqlParser::RuleNotExistsFunc);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1230);
    match(SparqlParser::NOT);
    setState(1231);
    match(SparqlParser::EXISTS);
    setState(1232);
    groupGraphPattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AggregateContext ------------------------------------------------------------------

SparqlParser::AggregateContext::AggregateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::AggregateContext::COUNT() {
  return getToken(SparqlParser::COUNT, 0);
}

SparqlParser::ExpressionContext* SparqlParser::AggregateContext::expression() {
  return getRuleContext<SparqlParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlParser::AggregateContext::DISTINCT() {
  return getToken(SparqlParser::DISTINCT, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::SUM() {
  return getToken(SparqlParser::SUM, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::MIN() {
  return getToken(SparqlParser::MIN, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::MAX() {
  return getToken(SparqlParser::MAX, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::AVG() {
  return getToken(SparqlParser::AVG, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::SAMPLE() {
  return getToken(SparqlParser::SAMPLE, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::GROUP_CONCAT() {
  return getToken(SparqlParser::GROUP_CONCAT, 0);
}

tree::TerminalNode* SparqlParser::AggregateContext::SEPARATOR() {
  return getToken(SparqlParser::SEPARATOR, 0);
}

SparqlParser::StringContext* SparqlParser::AggregateContext::string() {
  return getRuleContext<SparqlParser::StringContext>(0);
}


size_t SparqlParser::AggregateContext::getRuleIndex() const {
  return SparqlParser::RuleAggregate;
}

void SparqlParser::AggregateContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterAggregate(this);
}

void SparqlParser::AggregateContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitAggregate(this);
}


antlrcpp::Any SparqlParser::AggregateContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitAggregate(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::AggregateContext* SparqlParser::aggregate() {
  AggregateContext *_localctx = _tracker.createInstance<AggregateContext>(_ctx, getState());
  enterRule(_localctx, 208, SparqlParser::RuleAggregate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1298);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::COUNT: {
        enterOuterAlt(_localctx, 1);
        setState(1234);
        match(SparqlParser::COUNT);
        setState(1235);
        match(SparqlParser::T__1);
        setState(1237);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1236);
          match(SparqlParser::DISTINCT);
        }
        setState(1241);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlParser::T__0: {
            setState(1239);
            match(SparqlParser::T__0);
            break;
          }

          case SparqlParser::T__1:
          case SparqlParser::T__13:
          case SparqlParser::T__15:
          case SparqlParser::T__26:
          case SparqlParser::T__29:
          case SparqlParser::T__30:
          case SparqlParser::GROUP_CONCAT:
          case SparqlParser::NOT:
          case SparqlParser::STR:
          case SparqlParser::LANG:
          case SparqlParser::LANGMATCHES:
          case SparqlParser::DATATYPE:
          case SparqlParser::BOUND:
          case SparqlParser::IRI:
          case SparqlParser::URI:
          case SparqlParser::BNODE:
          case SparqlParser::RAND:
          case SparqlParser::ABS:
          case SparqlParser::CEIL:
          case SparqlParser::FLOOR:
          case SparqlParser::ROUND:
          case SparqlParser::CONCAT:
          case SparqlParser::STRLEN:
          case SparqlParser::UCASE:
          case SparqlParser::LCASE:
          case SparqlParser::ENCODE:
          case SparqlParser::CONTAINS:
          case SparqlParser::STRSTARTS:
          case SparqlParser::STRENDS:
          case SparqlParser::STRBEFORE:
          case SparqlParser::STRAFTER:
          case SparqlParser::YEAR:
          case SparqlParser::MONTH:
          case SparqlParser::DAY:
          case SparqlParser::HOURS:
          case SparqlParser::MINUTES:
          case SparqlParser::SECONDS:
          case SparqlParser::TIMEZONE:
          case SparqlParser::TZ:
          case SparqlParser::NOW:
          case SparqlParser::UUID:
          case SparqlParser::STRUUID:
          case SparqlParser::SHA1:
          case SparqlParser::SHA256:
          case SparqlParser::SHA384:
          case SparqlParser::SHA512:
          case SparqlParser::MD5:
          case SparqlParser::COALESCE:
          case SparqlParser::IF:
          case SparqlParser::STRLANG:
          case SparqlParser::STRDT:
          case SparqlParser::SAMETERM:
          case SparqlParser::ISIRI:
          case SparqlParser::ISURI:
          case SparqlParser::ISBLANK:
          case SparqlParser::ISLITERAL:
          case SparqlParser::ISNUMERIC:
          case SparqlParser::REGEX:
          case SparqlParser::SUBSTR:
          case SparqlParser::REPLACE:
          case SparqlParser::EXISTS:
          case SparqlParser::COUNT:
          case SparqlParser::SUM:
          case SparqlParser::MIN:
          case SparqlParser::MAX:
          case SparqlParser::AVG:
          case SparqlParser::SAMPLE:
          case SparqlParser::IRI_REF:
          case SparqlParser::PNAME_NS:
          case SparqlParser::VAR1:
          case SparqlParser::VAR2:
          case SparqlParser::INTEGER:
          case SparqlParser::DECIMAL:
          case SparqlParser::DOUBLE:
          case SparqlParser::INTEGER_POSITIVE:
          case SparqlParser::DECIMAL_POSITIVE:
          case SparqlParser::DOUBLE_POSITIVE:
          case SparqlParser::INTEGER_NEGATIVE:
          case SparqlParser::DECIMAL_NEGATIVE:
          case SparqlParser::DOUBLE_NEGATIVE:
          case SparqlParser::STRING_LITERAL1:
          case SparqlParser::STRING_LITERAL2:
          case SparqlParser::STRING_LITERAL_LONG1:
          case SparqlParser::STRING_LITERAL_LONG2: {
            setState(1240);
            expression();
            break;
          }

        default:
          throw NoViableAltException(this);
        }
        setState(1243);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SUM: {
        enterOuterAlt(_localctx, 2);
        setState(1244);
        match(SparqlParser::SUM);
        setState(1245);
        match(SparqlParser::T__1);
        setState(1247);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1246);
          match(SparqlParser::DISTINCT);
        }
        setState(1249);
        expression();
        setState(1250);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::MIN: {
        enterOuterAlt(_localctx, 3);
        setState(1252);
        match(SparqlParser::MIN);
        setState(1253);
        match(SparqlParser::T__1);
        setState(1255);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1254);
          match(SparqlParser::DISTINCT);
        }
        setState(1257);
        expression();
        setState(1258);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::MAX: {
        enterOuterAlt(_localctx, 4);
        setState(1260);
        match(SparqlParser::MAX);
        setState(1261);
        match(SparqlParser::T__1);
        setState(1263);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1262);
          match(SparqlParser::DISTINCT);
        }
        setState(1265);
        expression();
        setState(1266);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::AVG: {
        enterOuterAlt(_localctx, 5);
        setState(1268);
        match(SparqlParser::AVG);
        setState(1269);
        match(SparqlParser::T__1);
        setState(1271);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1270);
          match(SparqlParser::DISTINCT);
        }
        setState(1273);
        expression();
        setState(1274);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::SAMPLE: {
        enterOuterAlt(_localctx, 6);
        setState(1276);
        match(SparqlParser::SAMPLE);
        setState(1277);
        match(SparqlParser::T__1);
        setState(1279);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1278);
          match(SparqlParser::DISTINCT);
        }
        setState(1281);
        expression();
        setState(1282);
        match(SparqlParser::T__2);
        break;
      }

      case SparqlParser::GROUP_CONCAT: {
        enterOuterAlt(_localctx, 7);
        setState(1284);
        match(SparqlParser::GROUP_CONCAT);
        setState(1285);
        match(SparqlParser::T__1);
        setState(1287);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::DISTINCT) {
          setState(1286);
          match(SparqlParser::DISTINCT);
        }
        setState(1289);
        expression();
        setState(1294);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlParser::T__8) {
          setState(1290);
          match(SparqlParser::T__8);
          setState(1291);
          match(SparqlParser::SEPARATOR);
          setState(1292);
          match(SparqlParser::T__20);
          setState(1293);
          string();
        }
        setState(1296);
        match(SparqlParser::T__2);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IriOrFunctionContext ------------------------------------------------------------------

SparqlParser::IriOrFunctionContext::IriOrFunctionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IriContext* SparqlParser::IriOrFunctionContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}

SparqlParser::ArgListContext* SparqlParser::IriOrFunctionContext::argList() {
  return getRuleContext<SparqlParser::ArgListContext>(0);
}


size_t SparqlParser::IriOrFunctionContext::getRuleIndex() const {
  return SparqlParser::RuleIriOrFunction;
}

void SparqlParser::IriOrFunctionContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIriOrFunction(this);
}

void SparqlParser::IriOrFunctionContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIriOrFunction(this);
}


antlrcpp::Any SparqlParser::IriOrFunctionContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitIriOrFunction(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::IriOrFunctionContext* SparqlParser::iriOrFunction() {
  IriOrFunctionContext *_localctx = _tracker.createInstance<IriOrFunctionContext>(_ctx, getState());
  enterRule(_localctx, 210, SparqlParser::RuleIriOrFunction);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1300);
    iri();
    setState(1302);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlParser::T__1 || _la == SparqlParser::NIL) {
      setState(1301);
      argList();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RdfLiteralContext ------------------------------------------------------------------

SparqlParser::RdfLiteralContext::RdfLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::StringContext* SparqlParser::RdfLiteralContext::string() {
  return getRuleContext<SparqlParser::StringContext>(0);
}

tree::TerminalNode* SparqlParser::RdfLiteralContext::LANGTAG() {
  return getToken(SparqlParser::LANGTAG, 0);
}

SparqlParser::IriContext* SparqlParser::RdfLiteralContext::iri() {
  return getRuleContext<SparqlParser::IriContext>(0);
}


size_t SparqlParser::RdfLiteralContext::getRuleIndex() const {
  return SparqlParser::RuleRdfLiteral;
}

void SparqlParser::RdfLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterRdfLiteral(this);
}

void SparqlParser::RdfLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitRdfLiteral(this);
}


antlrcpp::Any SparqlParser::RdfLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitRdfLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::RdfLiteralContext* SparqlParser::rdfLiteral() {
  RdfLiteralContext *_localctx = _tracker.createInstance<RdfLiteralContext>(_ctx, getState());
  enterRule(_localctx, 212, SparqlParser::RuleRdfLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1304);
    string();
    setState(1308);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::LANGTAG: {
        setState(1305);
        match(SparqlParser::LANGTAG);
        break;
      }

      case SparqlParser::T__28: {
        setState(1306);
        match(SparqlParser::T__28);
        setState(1307);
        iri();
        break;
      }

      case SparqlParser::T__0:
      case SparqlParser::T__1:
      case SparqlParser::T__2:
      case SparqlParser::T__3:
      case SparqlParser::T__4:
      case SparqlParser::T__5:
      case SparqlParser::T__6:
      case SparqlParser::T__7:
      case SparqlParser::T__8:
      case SparqlParser::T__9:
      case SparqlParser::T__11:
      case SparqlParser::T__12:
      case SparqlParser::T__13:
      case SparqlParser::T__15:
      case SparqlParser::T__16:
      case SparqlParser::T__17:
      case SparqlParser::T__18:
      case SparqlParser::T__19:
      case SparqlParser::T__20:
      case SparqlParser::T__21:
      case SparqlParser::T__22:
      case SparqlParser::T__23:
      case SparqlParser::T__24:
      case SparqlParser::T__25:
      case SparqlParser::T__26:
      case SparqlParser::T__29:
      case SparqlParser::T__30:
      case SparqlParser::AS:
      case SparqlParser::VALUES:
      case SparqlParser::GRAPH:
      case SparqlParser::OPTIONAL:
      case SparqlParser::SERVICE:
      case SparqlParser::BIND:
      case SparqlParser::MINUS:
      case SparqlParser::FILTER:
      case SparqlParser::NOT:
      case SparqlParser::IN:
      case SparqlParser::IRI_REF:
      case SparqlParser::PNAME_NS:
      case SparqlParser::BLANK_NODE_LABEL:
      case SparqlParser::VAR1:
      case SparqlParser::VAR2:
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE:
      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE:
      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE:
      case SparqlParser::STRING_LITERAL1:
      case SparqlParser::STRING_LITERAL2:
      case SparqlParser::STRING_LITERAL_LONG1:
      case SparqlParser::STRING_LITERAL_LONG2:
      case SparqlParser::NIL:
      case SparqlParser::ANON: {
        break;
      }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralContext ------------------------------------------------------------------

SparqlParser::NumericLiteralContext::NumericLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::NumericLiteralUnsignedContext* SparqlParser::NumericLiteralContext::numericLiteralUnsigned() {
  return getRuleContext<SparqlParser::NumericLiteralUnsignedContext>(0);
}

SparqlParser::NumericLiteralPositiveContext* SparqlParser::NumericLiteralContext::numericLiteralPositive() {
  return getRuleContext<SparqlParser::NumericLiteralPositiveContext>(0);
}

SparqlParser::NumericLiteralNegativeContext* SparqlParser::NumericLiteralContext::numericLiteralNegative() {
  return getRuleContext<SparqlParser::NumericLiteralNegativeContext>(0);
}


size_t SparqlParser::NumericLiteralContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteral;
}

void SparqlParser::NumericLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteral(this);
}

void SparqlParser::NumericLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteral(this);
}


antlrcpp::Any SparqlParser::NumericLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNumericLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralContext* SparqlParser::numericLiteral() {
  NumericLiteralContext *_localctx = _tracker.createInstance<NumericLiteralContext>(_ctx, getState());
  enterRule(_localctx, 214, SparqlParser::RuleNumericLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1313);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::INTEGER:
      case SparqlParser::DECIMAL:
      case SparqlParser::DOUBLE: {
        enterOuterAlt(_localctx, 1);
        setState(1310);
        numericLiteralUnsigned();
        break;
      }

      case SparqlParser::INTEGER_POSITIVE:
      case SparqlParser::DECIMAL_POSITIVE:
      case SparqlParser::DOUBLE_POSITIVE: {
        enterOuterAlt(_localctx, 2);
        setState(1311);
        numericLiteralPositive();
        break;
      }

      case SparqlParser::INTEGER_NEGATIVE:
      case SparqlParser::DECIMAL_NEGATIVE:
      case SparqlParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(1312);
        numericLiteralNegative();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralUnsignedContext ------------------------------------------------------------------

SparqlParser::NumericLiteralUnsignedContext::NumericLiteralUnsignedContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NumericLiteralUnsignedContext::INTEGER() {
  return getToken(SparqlParser::INTEGER, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralUnsignedContext::DECIMAL() {
  return getToken(SparqlParser::DECIMAL, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralUnsignedContext::DOUBLE() {
  return getToken(SparqlParser::DOUBLE, 0);
}


size_t SparqlParser::NumericLiteralUnsignedContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteralUnsigned;
}

void SparqlParser::NumericLiteralUnsignedContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralUnsigned(this);
}

void SparqlParser::NumericLiteralUnsignedContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralUnsigned(this);
}


antlrcpp::Any SparqlParser::NumericLiteralUnsignedContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralUnsigned(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralUnsignedContext* SparqlParser::numericLiteralUnsigned() {
  NumericLiteralUnsignedContext *_localctx = _tracker.createInstance<NumericLiteralUnsignedContext>(_ctx, getState());
  enterRule(_localctx, 216, SparqlParser::RuleNumericLiteralUnsigned);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1315);
    _la = _input->LA(1);
    if (!(((((_la - 145) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 145)) & ((1ULL << (SparqlParser::INTEGER - 145))
      | (1ULL << (SparqlParser::DECIMAL - 145))
      | (1ULL << (SparqlParser::DOUBLE - 145)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralPositiveContext ------------------------------------------------------------------

SparqlParser::NumericLiteralPositiveContext::NumericLiteralPositiveContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NumericLiteralPositiveContext::INTEGER_POSITIVE() {
  return getToken(SparqlParser::INTEGER_POSITIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralPositiveContext::DECIMAL_POSITIVE() {
  return getToken(SparqlParser::DECIMAL_POSITIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralPositiveContext::DOUBLE_POSITIVE() {
  return getToken(SparqlParser::DOUBLE_POSITIVE, 0);
}


size_t SparqlParser::NumericLiteralPositiveContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteralPositive;
}

void SparqlParser::NumericLiteralPositiveContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralPositive(this);
}

void SparqlParser::NumericLiteralPositiveContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralPositive(this);
}


antlrcpp::Any SparqlParser::NumericLiteralPositiveContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralPositive(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralPositiveContext* SparqlParser::numericLiteralPositive() {
  NumericLiteralPositiveContext *_localctx = _tracker.createInstance<NumericLiteralPositiveContext>(_ctx, getState());
  enterRule(_localctx, 218, SparqlParser::RuleNumericLiteralPositive);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1317);
    _la = _input->LA(1);
    if (!(((((_la - 148) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 148)) & ((1ULL << (SparqlParser::INTEGER_POSITIVE - 148))
      | (1ULL << (SparqlParser::DECIMAL_POSITIVE - 148))
      | (1ULL << (SparqlParser::DOUBLE_POSITIVE - 148)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralNegativeContext ------------------------------------------------------------------

SparqlParser::NumericLiteralNegativeContext::NumericLiteralNegativeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::NumericLiteralNegativeContext::INTEGER_NEGATIVE() {
  return getToken(SparqlParser::INTEGER_NEGATIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralNegativeContext::DECIMAL_NEGATIVE() {
  return getToken(SparqlParser::DECIMAL_NEGATIVE, 0);
}

tree::TerminalNode* SparqlParser::NumericLiteralNegativeContext::DOUBLE_NEGATIVE() {
  return getToken(SparqlParser::DOUBLE_NEGATIVE, 0);
}


size_t SparqlParser::NumericLiteralNegativeContext::getRuleIndex() const {
  return SparqlParser::RuleNumericLiteralNegative;
}

void SparqlParser::NumericLiteralNegativeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralNegative(this);
}

void SparqlParser::NumericLiteralNegativeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralNegative(this);
}


antlrcpp::Any SparqlParser::NumericLiteralNegativeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralNegative(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::NumericLiteralNegativeContext* SparqlParser::numericLiteralNegative() {
  NumericLiteralNegativeContext *_localctx = _tracker.createInstance<NumericLiteralNegativeContext>(_ctx, getState());
  enterRule(_localctx, 220, SparqlParser::RuleNumericLiteralNegative);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1319);
    _la = _input->LA(1);
    if (!(((((_la - 151) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 151)) & ((1ULL << (SparqlParser::INTEGER_NEGATIVE - 151))
      | (1ULL << (SparqlParser::DECIMAL_NEGATIVE - 151))
      | (1ULL << (SparqlParser::DOUBLE_NEGATIVE - 151)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanLiteralContext ------------------------------------------------------------------

SparqlParser::BooleanLiteralContext::BooleanLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t SparqlParser::BooleanLiteralContext::getRuleIndex() const {
  return SparqlParser::RuleBooleanLiteral;
}

void SparqlParser::BooleanLiteralContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBooleanLiteral(this);
}

void SparqlParser::BooleanLiteralContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBooleanLiteral(this);
}


antlrcpp::Any SparqlParser::BooleanLiteralContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBooleanLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BooleanLiteralContext* SparqlParser::booleanLiteral() {
  BooleanLiteralContext *_localctx = _tracker.createInstance<BooleanLiteralContext>(_ctx, getState());
  enterRule(_localctx, 222, SparqlParser::RuleBooleanLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1321);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::T__29

    || _la == SparqlParser::T__30)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StringContext ------------------------------------------------------------------

SparqlParser::StringContext::StringContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::StringContext::STRING_LITERAL1() {
  return getToken(SparqlParser::STRING_LITERAL1, 0);
}

tree::TerminalNode* SparqlParser::StringContext::STRING_LITERAL2() {
  return getToken(SparqlParser::STRING_LITERAL2, 0);
}

tree::TerminalNode* SparqlParser::StringContext::STRING_LITERAL_LONG1() {
  return getToken(SparqlParser::STRING_LITERAL_LONG1, 0);
}

tree::TerminalNode* SparqlParser::StringContext::STRING_LITERAL_LONG2() {
  return getToken(SparqlParser::STRING_LITERAL_LONG2, 0);
}


size_t SparqlParser::StringContext::getRuleIndex() const {
  return SparqlParser::RuleString;
}

void SparqlParser::StringContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterString(this);
}

void SparqlParser::StringContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitString(this);
}


antlrcpp::Any SparqlParser::StringContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitString(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::StringContext* SparqlParser::string() {
  StringContext *_localctx = _tracker.createInstance<StringContext>(_ctx, getState());
  enterRule(_localctx, 224, SparqlParser::RuleString);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1323);
    _la = _input->LA(1);
    if (!(((((_la - 155) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 155)) & ((1ULL << (SparqlParser::STRING_LITERAL1 - 155))
      | (1ULL << (SparqlParser::STRING_LITERAL2 - 155))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG1 - 155))
      | (1ULL << (SparqlParser::STRING_LITERAL_LONG2 - 155)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IriContext ------------------------------------------------------------------

SparqlParser::IriContext::IriContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::IrirefContext* SparqlParser::IriContext::iriref() {
  return getRuleContext<SparqlParser::IrirefContext>(0);
}

SparqlParser::PrefixedNameContext* SparqlParser::IriContext::prefixedName() {
  return getRuleContext<SparqlParser::PrefixedNameContext>(0);
}


size_t SparqlParser::IriContext::getRuleIndex() const {
  return SparqlParser::RuleIri;
}

void SparqlParser::IriContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIri(this);
}

void SparqlParser::IriContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIri(this);
}


antlrcpp::Any SparqlParser::IriContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitIri(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::IriContext* SparqlParser::iri() {
  IriContext *_localctx = _tracker.createInstance<IriContext>(_ctx, getState());
  enterRule(_localctx, 226, SparqlParser::RuleIri);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1327);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlParser::IRI_REF: {
        enterOuterAlt(_localctx, 1);
        setState(1325);
        iriref();
        break;
      }

      case SparqlParser::PNAME_NS: {
        enterOuterAlt(_localctx, 2);
        setState(1326);
        prefixedName();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrefixedNameContext ------------------------------------------------------------------

SparqlParser::PrefixedNameContext::PrefixedNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

SparqlParser::PnameLnContext* SparqlParser::PrefixedNameContext::pnameLn() {
  return getRuleContext<SparqlParser::PnameLnContext>(0);
}

tree::TerminalNode* SparqlParser::PrefixedNameContext::PNAME_NS() {
  return getToken(SparqlParser::PNAME_NS, 0);
}


size_t SparqlParser::PrefixedNameContext::getRuleIndex() const {
  return SparqlParser::RulePrefixedName;
}

void SparqlParser::PrefixedNameContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPrefixedName(this);
}

void SparqlParser::PrefixedNameContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPrefixedName(this);
}


antlrcpp::Any SparqlParser::PrefixedNameContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPrefixedName(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PrefixedNameContext* SparqlParser::prefixedName() {
  PrefixedNameContext *_localctx = _tracker.createInstance<PrefixedNameContext>(_ctx, getState());
  enterRule(_localctx, 228, SparqlParser::RulePrefixedName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1331);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 125, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1329);
      pnameLn();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1330);
      match(SparqlParser::PNAME_NS);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BlankNodeContext ------------------------------------------------------------------

SparqlParser::BlankNodeContext::BlankNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::BlankNodeContext::BLANK_NODE_LABEL() {
  return getToken(SparqlParser::BLANK_NODE_LABEL, 0);
}

tree::TerminalNode* SparqlParser::BlankNodeContext::ANON() {
  return getToken(SparqlParser::ANON, 0);
}


size_t SparqlParser::BlankNodeContext::getRuleIndex() const {
  return SparqlParser::RuleBlankNode;
}

void SparqlParser::BlankNodeContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNode(this);
}

void SparqlParser::BlankNodeContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNode(this);
}


antlrcpp::Any SparqlParser::BlankNodeContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitBlankNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::BlankNodeContext* SparqlParser::blankNode() {
  BlankNodeContext *_localctx = _tracker.createInstance<BlankNodeContext>(_ctx, getState());
  enterRule(_localctx, 230, SparqlParser::RuleBlankNode);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1333);
    _la = _input->LA(1);
    if (!(_la == SparqlParser::BLANK_NODE_LABEL

    || _la == SparqlParser::ANON)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IrirefContext ------------------------------------------------------------------

SparqlParser::IrirefContext::IrirefContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::IrirefContext::IRI_REF() {
  return getToken(SparqlParser::IRI_REF, 0);
}


size_t SparqlParser::IrirefContext::getRuleIndex() const {
  return SparqlParser::RuleIriref;
}

void SparqlParser::IrirefContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterIriref(this);
}

void SparqlParser::IrirefContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitIriref(this);
}


antlrcpp::Any SparqlParser::IrirefContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitIriref(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::IrirefContext* SparqlParser::iriref() {
  IrirefContext *_localctx = _tracker.createInstance<IrirefContext>(_ctx, getState());
  enterRule(_localctx, 232, SparqlParser::RuleIriref);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1335);
    match(SparqlParser::IRI_REF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PnameLnContext ------------------------------------------------------------------

SparqlParser::PnameLnContext::PnameLnContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* SparqlParser::PnameLnContext::PNAME_NS() {
  return getToken(SparqlParser::PNAME_NS, 0);
}

tree::TerminalNode* SparqlParser::PnameLnContext::PN_LOCAL() {
  return getToken(SparqlParser::PN_LOCAL, 0);
}


size_t SparqlParser::PnameLnContext::getRuleIndex() const {
  return SparqlParser::RulePnameLn;
}

void SparqlParser::PnameLnContext::enterRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->enterPnameLn(this);
}

void SparqlParser::PnameLnContext::exitRule(tree::ParseTreeListener *listener) {
  auto parserListener = dynamic_cast<SparqlListener *>(listener);
  if (parserListener != nullptr)
    parserListener->exitPnameLn(this);
}


antlrcpp::Any SparqlParser::PnameLnContext::accept(tree::ParseTreeVisitor *visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlVisitor*>(visitor))
    return parserVisitor->visitPnameLn(this);
  else
    return visitor->visitChildren(this);
}

SparqlParser::PnameLnContext* SparqlParser::pnameLn() {
  PnameLnContext *_localctx = _tracker.createInstance<PnameLnContext>(_ctx, getState());
  enterRule(_localctx, 234, SparqlParser::RulePnameLn);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1337);
    match(SparqlParser::PNAME_NS);
    setState(1338);
    match(SparqlParser::PN_LOCAL);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> SparqlParser::_decisionToDFA;
atn::PredictionContextCache SparqlParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN SparqlParser::_atn;
std::vector<uint16_t> SparqlParser::_serializedATN;

std::vector<std::string> SparqlParser::_ruleNames = {
  "query", "prologue", "baseDecl", "prefixDecl", "selectQuery", "subSelect", 
  "selectClause", "alias", "constructQuery", "describeQuery", "askQuery", 
  "datasetClause", "defaultGraphClause", "namedGraphClause", "sourceSelector", 
  "whereClause", "solutionModifier", "groupClause", "groupCondition", "havingClause", 
  "havingCondition", "orderClause", "orderCondition", "limitOffsetClauses", 
  "limitClause", "offsetClause", "valuesClause", "triplesTemplate", "groupGraphPattern", 
  "groupGraphPatternSub", "triplesBlock", "graphPatternNotTriples", "optionalGraphPattern", 
  "graphGraphPattern", "serviceGraphPattern", "bind", "inlineData", "dataBlock", 
  "inlineDataOneVar", "inlineDataFull", "dataBlockSingle", "dataBlockValue", 
  "minusGraphPattern", "groupOrUnionGraphPattern", "filterR", "constraint", 
  "functionCall", "argList", "expressionList", "constructTemplate", "constructTriples", 
  "triplesSameSubject", "propertyList", "propertyListNotEmpty", "verb", 
  "objectList", "objectR", "triplesSameSubjectPath", "propertyListPath", 
  "propertyListPathNotEmpty", "verbPath", "verbSimple", "verbPathOrSimple", 
  "objectListPath", "objectPath", "path", "pathAlternative", "pathSequence", 
  "pathElt", "pathEltOrInverse", "pathMod", "pathPrimary", "pathNegatedPropertySet", 
  "pathOneInPropertySet", "integer", "triplesNode", "blankNodePropertyList", 
  "triplesNodePath", "blankNodePropertyListPath", "collection", "collectionPath", 
  "graphNode", "graphNodePath", "varOrTerm", "varOrIri", "var", "graphTerm", 
  "expression", "conditionalOrExpression", "conditionalAndExpression", "valueLogical", 
  "relationalExpression", "numericExpression", "additiveExpression", "multiplicativeExpression", 
  "unaryExpression", "primaryExpression", "brackettedExpression", "builtInCall", 
  "regexExpression", "substringExpression", "strReplaceExpression", "existsFunc", 
  "notExistsFunc", "aggregate", "iriOrFunction", "rdfLiteral", "numericLiteral", 
  "numericLiteralUnsigned", "numericLiteralPositive", "numericLiteralNegative", 
  "booleanLiteral", "string", "iri", "prefixedName", "blankNode", "iriref", 
  "pnameLn"
};

std::vector<std::string> SparqlParser::_literalNames = {
  "", "'*'", "'('", "')'", "'{'", "'}'", "'.'", "'UNDEF'", "','", "';'", 
  "'a'", "'|'", "'/'", "'^'", "'+'", "'\u003F'", "'!'", "'['", "']'", "'||'", 
  "'&&'", "'='", "'!='", "'<'", "'>'", "'<='", "'>='", "'-'", "'_'", "'^^'", 
  "'true'", "'false'"
};

std::vector<std::string> SparqlParser::_symbolicNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "BASE", "PREFIX", 
  "SELECT", "DISTINCT", "REDUCED", "AS", "CONSTRUCT", "WHERE", "DESCRIBE", 
  "ASK", "FROM", "NAMED", "GROUPBY", "GROUP_CONCAT", "HAVING", "ORDERBY", 
  "ASC", "DESC", "LIMIT", "OFFSET", "VALUES", "LOAD", "SILENT", "CLEAR", 
  "DROP", "CREATE", "ADD", "DATA", "MOVE", "COPY", "INSERT", "DELETE", "WITH", 
  "USING", "DEFAULT", "GRAPH", "ALL", "OPTIONAL", "SERVICE", "BIND", "UNDEF", 
  "MINUS", "UNION", "FILTER", "NOT", "IN", "STR", "LANG", "LANGMATCHES", 
  "DATATYPE", "BOUND", "IRI", "URI", "BNODE", "RAND", "ABS", "CEIL", "FLOOR", 
  "ROUND", "CONCAT", "STRLEN", "UCASE", "LCASE", "ENCODE", "FOR", "CONTAINS", 
  "STRSTARTS", "STRENDS", "STRBEFORE", "STRAFTER", "YEAR", "MONTH", "DAY", 
  "HOURS", "MINUTES", "SECONDS", "TIMEZONE", "TZ", "NOW", "UUID", "STRUUID", 
  "SHA1", "SHA256", "SHA384", "SHA512", "MD5", "COALESCE", "IF", "STRLANG", 
  "STRDT", "SAMETERM", "ISIRI", "ISURI", "ISBLANK", "ISLITERAL", "ISNUMERIC", 
  "REGEX", "SUBSTR", "REPLACE", "EXISTS", "COUNT", "SUM", "MIN", "MAX", 
  "AVG", "SAMPLE", "SEPARATOR", "IRI_REF", "PNAME_NS", "BLANK_NODE_LABEL", 
  "VAR1", "VAR2", "LANGTAG", "INTEGER", "DECIMAL", "DOUBLE", "INTEGER_POSITIVE", 
  "DECIMAL_POSITIVE", "DOUBLE_POSITIVE", "INTEGER_NEGATIVE", "DECIMAL_NEGATIVE", 
  "DOUBLE_NEGATIVE", "EXPONENT", "STRING_LITERAL1", "STRING_LITERAL2", "STRING_LITERAL_LONG1", 
  "STRING_LITERAL_LONG2", "ECHAR", "NIL", "ANON", "PN_CHARS_U", "VARNAME", 
  "PN_PREFIX", "PN_LOCAL", "PLX", "PERCENT", "HEX", "PN_LOCAL_ESC", "WS", 
  "COMMENTS"
};

dfa::Vocabulary SparqlParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> SparqlParser::_tokenNames;

SparqlParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  static const uint16_t serializedATNSegment0[] = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
       0x3, 0xad, 0x53f, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
       0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 
       0x7, 0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 
       0x4, 0xb, 0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 
       0xe, 0x9, 0xe, 0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 
       0x9, 0x11, 0x4, 0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 
       0x9, 0x14, 0x4, 0x15, 0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 
       0x9, 0x17, 0x4, 0x18, 0x9, 0x18, 0x4, 0x19, 0x9, 0x19, 0x4, 0x1a, 
       0x9, 0x1a, 0x4, 0x1b, 0x9, 0x1b, 0x4, 0x1c, 0x9, 0x1c, 0x4, 0x1d, 
       0x9, 0x1d, 0x4, 0x1e, 0x9, 0x1e, 0x4, 0x1f, 0x9, 0x1f, 0x4, 0x20, 
       0x9, 0x20, 0x4, 0x21, 0x9, 0x21, 0x4, 0x22, 0x9, 0x22, 0x4, 0x23, 
       0x9, 0x23, 0x4, 0x24, 0x9, 0x24, 0x4, 0x25, 0x9, 0x25, 0x4, 0x26, 
       0x9, 0x26, 0x4, 0x27, 0x9, 0x27, 0x4, 0x28, 0x9, 0x28, 0x4, 0x29, 
       0x9, 0x29, 0x4, 0x2a, 0x9, 0x2a, 0x4, 0x2b, 0x9, 0x2b, 0x4, 0x2c, 
       0x9, 0x2c, 0x4, 0x2d, 0x9, 0x2d, 0x4, 0x2e, 0x9, 0x2e, 0x4, 0x2f, 
       0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x4, 0x31, 0x9, 0x31, 0x4, 0x32, 
       0x9, 0x32, 0x4, 0x33, 0x9, 0x33, 0x4, 0x34, 0x9, 0x34, 0x4, 0x35, 
       0x9, 0x35, 0x4, 0x36, 0x9, 0x36, 0x4, 0x37, 0x9, 0x37, 0x4, 0x38, 
       0x9, 0x38, 0x4, 0x39, 0x9, 0x39, 0x4, 0x3a, 0x9, 0x3a, 0x4, 0x3b, 
       0x9, 0x3b, 0x4, 0x3c, 0x9, 0x3c, 0x4, 0x3d, 0x9, 0x3d, 0x4, 0x3e, 
       0x9, 0x3e, 0x4, 0x3f, 0x9, 0x3f, 0x4, 0x40, 0x9, 0x40, 0x4, 0x41, 
       0x9, 0x41, 0x4, 0x42, 0x9, 0x42, 0x4, 0x43, 0x9, 0x43, 0x4, 0x44, 
       0x9, 0x44, 0x4, 0x45, 0x9, 0x45, 0x4, 0x46, 0x9, 0x46, 0x4, 0x47, 
       0x9, 0x47, 0x4, 0x48, 0x9, 0x48, 0x4, 0x49, 0x9, 0x49, 0x4, 0x4a, 
       0x9, 0x4a, 0x4, 0x4b, 0x9, 0x4b, 0x4, 0x4c, 0x9, 0x4c, 0x4, 0x4d, 
       0x9, 0x4d, 0x4, 0x4e, 0x9, 0x4e, 0x4, 0x4f, 0x9, 0x4f, 0x4, 0x50, 
       0x9, 0x50, 0x4, 0x51, 0x9, 0x51, 0x4, 0x52, 0x9, 0x52, 0x4, 0x53, 
       0x9, 0x53, 0x4, 0x54, 0x9, 0x54, 0x4, 0x55, 0x9, 0x55, 0x4, 0x56, 
       0x9, 0x56, 0x4, 0x57, 0x9, 0x57, 0x4, 0x58, 0x9, 0x58, 0x4, 0x59, 
       0x9, 0x59, 0x4, 0x5a, 0x9, 0x5a, 0x4, 0x5b, 0x9, 0x5b, 0x4, 0x5c, 
       0x9, 0x5c, 0x4, 0x5d, 0x9, 0x5d, 0x4, 0x5e, 0x9, 0x5e, 0x4, 0x5f, 
       0x9, 0x5f, 0x4, 0x60, 0x9, 0x60, 0x4, 0x61, 0x9, 0x61, 0x4, 0x62, 
       0x9, 0x62, 0x4, 0x63, 0x9, 0x63, 0x4, 0x64, 0x9, 0x64, 0x4, 0x65, 
       0x9, 0x65, 0x4, 0x66, 0x9, 0x66, 0x4, 0x67, 0x9, 0x67, 0x4, 0x68, 
       0x9, 0x68, 0x4, 0x69, 0x9, 0x69, 0x4, 0x6a, 0x9, 0x6a, 0x4, 0x6b, 
       0x9, 0x6b, 0x4, 0x6c, 0x9, 0x6c, 0x4, 0x6d, 0x9, 0x6d, 0x4, 0x6e, 
       0x9, 0x6e, 0x4, 0x6f, 0x9, 0x6f, 0x4, 0x70, 0x9, 0x70, 0x4, 0x71, 
       0x9, 0x71, 0x4, 0x72, 0x9, 0x72, 0x4, 0x73, 0x9, 0x73, 0x4, 0x74, 
       0x9, 0x74, 0x4, 0x75, 0x9, 0x75, 0x4, 0x76, 0x9, 0x76, 0x4, 0x77, 
       0x9, 0x77, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x5, 
       0x2, 0xf4, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 
       0x3, 0x7, 0x3, 0xfb, 0xa, 0x3, 0xc, 0x3, 0xe, 0x3, 0xfe, 0xb, 0x3, 
       0x3, 0x4, 0x3, 0x4, 0x3, 0x4, 0x3, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 
       0x5, 0x3, 0x6, 0x3, 0x6, 0x7, 0x6, 0x109, 0xa, 0x6, 0xc, 0x6, 0xe, 
       0x6, 0x10c, 0xb, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x6, 0x3, 0x7, 0x3, 
       0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 
       0x118, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x6, 0x8, 0x11c, 0xa, 0x8, 0xd, 
       0x8, 0xe, 0x8, 0x11d, 0x3, 0x8, 0x5, 0x8, 0x121, 0xa, 0x8, 0x3, 0x9, 
       0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0xa, 0x3, 
       0xa, 0x3, 0xa, 0x7, 0xa, 0x12c, 0xa, 0xa, 0xc, 0xa, 0xe, 0xa, 0x12f, 
       0xb, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x7, 0xa, 0x135, 
       0xa, 0xa, 0xc, 0xa, 0xe, 0xa, 0x138, 0xb, 0xa, 0x3, 0xa, 0x3, 0xa, 
       0x3, 0xa, 0x5, 0xa, 0x13d, 0xa, 0xa, 0x3, 0xa, 0x3, 0xa, 0x5, 0xa, 
       0x141, 0xa, 0xa, 0x3, 0xb, 0x3, 0xb, 0x6, 0xb, 0x145, 0xa, 0xb, 0xd, 
       0xb, 0xe, 0xb, 0x146, 0x3, 0xb, 0x5, 0xb, 0x14a, 0xa, 0xb, 0x3, 0xb, 
       0x7, 0xb, 0x14d, 0xa, 0xb, 0xc, 0xb, 0xe, 0xb, 0x150, 0xb, 0xb, 0x3, 
       0xb, 0x5, 0xb, 0x153, 0xa, 0xb, 0x3, 0xb, 0x3, 0xb, 0x3, 0xc, 0x3, 
       0xc, 0x7, 0xc, 0x159, 0xa, 0xc, 0xc, 0xc, 0xe, 0xc, 0x15c, 0xb, 0xc, 
       0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x5, 
       0xd, 0x164, 0xa, 0xd, 0x3, 0xe, 0x3, 0xe, 0x3, 0xf, 0x3, 0xf, 0x3, 
       0xf, 0x3, 0x10, 0x3, 0x10, 0x3, 0x11, 0x5, 0x11, 0x16e, 0xa, 0x11, 
       0x3, 0x11, 0x3, 0x11, 0x3, 0x12, 0x5, 0x12, 0x173, 0xa, 0x12, 0x3, 
       0x12, 0x5, 0x12, 0x176, 0xa, 0x12, 0x3, 0x12, 0x5, 0x12, 0x179, 0xa, 
       0x12, 0x3, 0x12, 0x5, 0x12, 0x17c, 0xa, 0x12, 0x3, 0x13, 0x3, 0x13, 
       0x6, 0x13, 0x180, 0xa, 0x13, 0xd, 0x13, 0xe, 0x13, 0x181, 0x3, 0x14, 
       0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 
       0x18a, 0xa, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x5, 0x14, 0x18f, 
       0xa, 0x14, 0x3, 0x15, 0x3, 0x15, 0x6, 0x15, 0x193, 0xa, 0x15, 0xd, 
       0x15, 0xe, 0x15, 0x194, 0x3, 0x16, 0x3, 0x16, 0x3, 0x17, 0x3, 0x17, 
       0x6, 0x17, 0x19b, 0xa, 0x17, 0xd, 0x17, 0xe, 0x17, 0x19c, 0x3, 0x18, 
       0x3, 0x18, 0x3, 0x18, 0x3, 0x18, 0x5, 0x18, 0x1a3, 0xa, 0x18, 0x5, 
       0x18, 0x1a5, 0xa, 0x18, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1a9, 0xa, 
       0x19, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x1ad, 0xa, 0x19, 0x5, 0x19, 
       0x1af, 0xa, 0x19, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1b, 0x3, 
       0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x5, 0x1c, 0x1b9, 0xa, 0x1c, 
       0x3, 0x1d, 0x3, 0x1d, 0x3, 0x1d, 0x5, 0x1d, 0x1be, 0xa, 0x1d, 0x5, 
       0x1d, 0x1c0, 0xa, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 
       0x1c5, 0xa, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x3, 0x1f, 0x5, 0x1f, 0x1ca, 
       0xa, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x5, 0x1f, 0x1ce, 0xa, 0x1f, 0x3, 
       0x1f, 0x5, 0x1f, 0x1d1, 0xa, 0x1f, 0x7, 0x1f, 0x1d3, 0xa, 0x1f, 0xc, 
       0x1f, 0xe, 0x1f, 0x1d6, 0xb, 0x1f, 0x3, 0x20, 0x3, 0x20, 0x3, 0x20, 
       0x5, 0x20, 0x1db, 0xa, 0x20, 0x5, 0x20, 0x1dd, 0xa, 0x20, 0x3, 0x21, 
       0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 
       0x3, 0x21, 0x5, 0x21, 0x1e7, 0xa, 0x21, 0x3, 0x22, 0x3, 0x22, 0x3, 
       0x22, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x23, 0x3, 0x24, 0x3, 
       0x24, 0x5, 0x24, 0x1f2, 0xa, 0x24, 0x3, 0x24, 0x3, 0x24, 0x3, 0x24, 
       0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 0x3, 0x25, 
       0x3, 0x25, 0x3, 0x26, 0x3, 0x26, 0x3, 0x26, 0x3, 0x27, 0x3, 0x27, 
       0x5, 0x27, 0x203, 0xa, 0x27, 0x3, 0x28, 0x3, 0x28, 0x3, 0x28, 0x7, 
       0x28, 0x208, 0xa, 0x28, 0xc, 0x28, 0xe, 0x28, 0x20b, 0xb, 0x28, 0x3, 
       0x28, 0x3, 0x28, 0x3, 0x29, 0x3, 0x29, 0x3, 0x29, 0x7, 0x29, 0x212, 
       0xa, 0x29, 0xc, 0x29, 0xe, 0x29, 0x215, 0xb, 0x29, 0x3, 0x29, 0x5, 
       0x29, 0x218, 0xa, 0x29, 0x3, 0x29, 0x3, 0x29, 0x7, 0x29, 0x21c, 0xa, 
       0x29, 0xc, 0x29, 0xe, 0x29, 0x21f, 0xb, 0x29, 0x3, 0x29, 0x3, 0x29, 
       0x3, 0x2a, 0x3, 0x2a, 0x7, 0x2a, 0x225, 0xa, 0x2a, 0xc, 0x2a, 0xe, 
       0x2a, 0x228, 0xb, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 0x22c, 0xa, 
       0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 
       0x2b, 0x233, 0xa, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2d, 
       0x3, 0x2d, 0x3, 0x2d, 0x7, 0x2d, 0x23b, 0xa, 0x2d, 0xc, 0x2d, 0xe, 
       0x2d, 0x23e, 0xb, 0x2d, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2f, 
       0x3, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x246, 0xa, 0x2f, 0x3, 0x30, 0x3, 
       0x30, 0x3, 0x30, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x5, 0x31, 0x24e, 
       0xa, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x7, 0x31, 0x253, 0xa, 
       0x31, 0xc, 0x31, 0xe, 0x31, 0x256, 0xb, 0x31, 0x3, 0x31, 0x3, 0x31, 
       0x5, 0x31, 0x25a, 0xa, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 0x3, 
       0x32, 0x3, 0x32, 0x7, 0x32, 0x261, 0xa, 0x32, 0xc, 0x32, 0xe, 0x32, 
       0x264, 0xb, 0x32, 0x3, 0x32, 0x3, 0x32, 0x5, 0x32, 0x268, 0xa, 0x32, 
       0x3, 0x33, 0x3, 0x33, 0x5, 0x33, 0x26c, 0xa, 0x33, 0x3, 0x33, 0x3, 
       0x33, 0x3, 0x34, 0x3, 0x34, 0x3, 0x34, 0x5, 0x34, 0x273, 0xa, 0x34, 
       0x5, 0x34, 0x275, 0xa, 0x34, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 
       0x35, 0x3, 0x35, 0x3, 0x35, 0x5, 0x35, 0x27d, 0xa, 0x35, 0x3, 0x36, 
       0x5, 0x36, 0x280, 0xa, 0x36, 0x3, 0x37, 0x3, 0x37, 0x3, 0x37, 0x3, 
       0x37, 0x3, 0x37, 0x3, 0x37, 0x5, 0x37, 0x288, 0xa, 0x37, 0x7, 0x37, 
       0x28a, 0xa, 0x37, 0xc, 0x37, 0xe, 0x37, 0x28d, 0xb, 0x37, 0x3, 0x38, 
       0x3, 0x38, 0x5, 0x38, 0x291, 0xa, 0x38, 0x3, 0x39, 0x3, 0x39, 0x3, 
       0x39, 0x7, 0x39, 0x296, 0xa, 0x39, 0xc, 0x39, 0xe, 0x39, 0x299, 0xb, 
       0x39, 0x3, 0x3a, 0x3, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 
       0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 0x2a3, 0xa, 0x3b, 0x3, 0x3c, 
       0x5, 0x3c, 0x2a6, 0xa, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x3, 
       0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x2ae, 0xa, 0x3d, 0x7, 0x3d, 
       0x2b0, 0xa, 0x3d, 0xc, 0x3d, 0xe, 0x3d, 0x2b3, 0xb, 0x3d, 0x3, 0x3e, 
       0x3, 0x3e, 0x3, 0x3f, 0x3, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x5, 0x40, 
       0x2bb, 0xa, 0x40, 0x3, 0x41, 0x3, 0x41, 0x3, 0x41, 0x7, 0x41, 0x2c0, 
       0xa, 0x41, 0xc, 0x41, 0xe, 0x41, 0x2c3, 0xb, 0x41, 0x3, 0x42, 0x3, 
       0x42, 0x3, 0x43, 0x3, 0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x44, 0x7, 
       0x44, 0x2cc, 0xa, 0x44, 0xc, 0x44, 0xe, 0x44, 0x2cf, 0xb, 0x44, 0x3, 
       0x45, 0x3, 0x45, 0x3, 0x45, 0x7, 0x45, 0x2d4, 0xa, 0x45, 0xc, 0x45, 
       0xe, 0x45, 0x2d7, 0xb, 0x45, 0x3, 0x46, 0x3, 0x46, 0x5, 0x46, 0x2db, 
       0xa, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 0x5, 0x47, 0x2e0, 0xa, 
       0x47, 0x3, 0x48, 0x3, 0x48, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 
       0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x2ec, 
       0xa, 0x49, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 
       0x7, 0x4a, 0x2f3, 0xa, 0x4a, 0xc, 0x4a, 0xe, 0x4a, 0x2f6, 0xb, 0x4a, 
       0x5, 0x4a, 0x2f8, 0xa, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x2fb, 0xa, 0x4a, 
       0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4b, 0x5, 0x4b, 
       0x302, 0xa, 0x4b, 0x5, 0x4b, 0x304, 0xa, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 
       0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 0x30a, 0xa, 0x4d, 0x3, 0x4e, 0x3, 
       0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x3, 0x4f, 0x3, 0x4f, 0x5, 0x4f, 0x312, 
       0xa, 0x4f, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x50, 0x3, 0x51, 
       0x3, 0x51, 0x6, 0x51, 0x31a, 0xa, 0x51, 0xd, 0x51, 0xe, 0x51, 0x31b, 
       0x3, 0x51, 0x3, 0x51, 0x3, 0x52, 0x3, 0x52, 0x6, 0x52, 0x322, 0xa, 
       0x52, 0xd, 0x52, 0xe, 0x52, 0x323, 0x3, 0x52, 0x3, 0x52, 0x3, 0x53, 
       0x3, 0x53, 0x5, 0x53, 0x32a, 0xa, 0x53, 0x3, 0x54, 0x3, 0x54, 0x5, 
       0x54, 0x32e, 0xa, 0x54, 0x3, 0x55, 0x3, 0x55, 0x5, 0x55, 0x332, 0xa, 
       0x55, 0x3, 0x56, 0x3, 0x56, 0x5, 0x56, 0x336, 0xa, 0x56, 0x3, 0x57, 
       0x3, 0x57, 0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 0x3, 0x58, 
       0x3, 0x58, 0x5, 0x58, 0x340, 0xa, 0x58, 0x3, 0x59, 0x3, 0x59, 0x3, 
       0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x7, 0x5a, 0x347, 0xa, 0x5a, 0xc, 0x5a, 
       0xe, 0x5a, 0x34a, 0xb, 0x5a, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x7, 
       0x5b, 0x34f, 0xa, 0x5b, 0xc, 0x5b, 0xe, 0x5b, 0x352, 0xb, 0x5b, 0x3, 
       0x5c, 0x3, 0x5c, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 
       0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 
       0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 
       0x5d, 0x3, 0x5d, 0x5, 0x5d, 0x368, 0xa, 0x5d, 0x3, 0x5e, 0x3, 0x5e, 
       0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 
       0x3, 0x5f, 0x5, 0x5f, 0x373, 0xa, 0x5f, 0x3, 0x5f, 0x3, 0x5f, 0x3, 
       0x5f, 0x3, 0x5f, 0x7, 0x5f, 0x379, 0xa, 0x5f, 0xc, 0x5f, 0xe, 0x5f, 
       0x37c, 0xb, 0x5f, 0x7, 0x5f, 0x37e, 0xa, 0x5f, 0xc, 0x5f, 0xe, 0x5f, 
       0x381, 0xb, 0x5f, 0x3, 0x60, 0x3, 0x60, 0x3, 0x60, 0x3, 0x60, 0x3, 
       0x60, 0x7, 0x60, 0x388, 0xa, 0x60, 0xc, 0x60, 0xe, 0x60, 0x38b, 0xb, 
       0x60, 0x3, 0x61, 0x3, 0x61, 0x3, 0x61, 0x3, 0x61, 0x3, 0x61, 0x3, 
       0x61, 0x3, 0x61, 0x5, 0x61, 0x394, 0xa, 0x61, 0x3, 0x62, 0x3, 0x62, 
       0x3, 0x62, 0x3, 0x62, 0x3, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 
       0x39d, 0xa, 0x62, 0x3, 0x63, 0x3, 0x63, 0x3, 0x63, 0x3, 0x63, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 
       0x64, 0x3, 0x64, 0x5, 0x64, 0x3cf, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 
       0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x4a9, 0xa, 
       0x64, 0x3, 0x65, 0x3, 0x65, 0x3, 0x65, 0x3, 0x65, 0x3, 0x65, 0x3, 
       0x65, 0x3, 0x65, 0x5, 0x65, 0x4b2, 0xa, 0x65, 0x3, 0x65, 0x3, 0x65, 
       0x3, 0x66, 0x3, 0x66, 0x3, 0x66, 0x3, 0x66, 0x3, 0x66, 0x3, 0x66, 
       0x3, 0x66, 0x5, 0x66, 0x4bd, 0xa, 0x66, 0x3, 0x66, 0x3, 0x66, 0x3, 
       0x67, 0x3, 0x67, 0x3, 0x67, 0x3, 0x67, 0x3, 0x67, 0x3, 0x67, 0x3, 
       0x67, 0x3, 0x67, 0x3, 0x67, 0x5, 0x67, 0x4ca, 0xa, 0x67, 0x3, 0x67, 
       0x3, 0x67, 0x3, 0x68, 0x3, 0x68, 0x3, 0x68, 0x3, 0x69, 0x3, 0x69, 
       0x3, 0x69, 0x3, 0x69, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 
       0x4d8, 0xa, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 0x4dc, 0xa, 0x6a, 
       0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 0x4e2, 0xa, 
       0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 
       0x6a, 0x5, 0x6a, 0x4ea, 0xa, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 
       0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 0x4f2, 0xa, 0x6a, 0x3, 
       0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 
       0x6a, 0x4fa, 0xa, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 
       0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 0x502, 0xa, 0x6a, 0x3, 0x6a, 0x3, 
       0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 0x50a, 
       0xa, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 
       0x5, 0x6a, 0x511, 0xa, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 0x5, 0x6a, 0x515, 
       0xa, 0x6a, 0x3, 0x6b, 0x3, 0x6b, 0x5, 0x6b, 0x519, 0xa, 0x6b, 0x3, 
       0x6c, 0x3, 0x6c, 0x3, 0x6c, 0x3, 0x6c, 0x5, 0x6c, 0x51f, 0xa, 0x6c, 
       0x3, 0x6d, 0x3, 0x6d, 0x3, 0x6d, 0x5, 0x6d, 0x524, 0xa, 0x6d, 0x3, 
       0x6e, 0x3, 0x6e, 0x3, 0x6f, 0x3, 0x6f, 0x3, 0x70, 0x3, 0x70, 0x3, 
       0x71, 0x3, 0x71, 0x3, 0x72, 0x3, 0x72, 0x3, 0x73, 0x3, 0x73, 0x5, 
       0x73, 0x532, 0xa, 0x73, 0x3, 0x74, 0x3, 0x74, 0x5, 0x74, 0x536, 0xa, 
       0x74, 0x3, 0x75, 0x3, 0x75, 0x3, 0x76, 0x3, 0x76, 0x3, 0x77, 0x3, 
       0x77, 0x3, 0x77, 0x3, 0x77, 0x2, 0x2, 0x78, 0x2, 0x4, 0x6, 0x8, 0xa, 
       0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 0x1c, 0x1e, 0x20, 0x22, 
       0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 0x34, 0x36, 0x38, 
       0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 0x4c, 0x4e, 
       0x50, 0x52, 0x54, 0x56, 0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 0x64, 
       0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 
       0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 0x88, 0x8a, 0x8c, 0x8e, 0x90, 
       0x92, 0x94, 0x96, 0x98, 0x9a, 0x9c, 0x9e, 0xa0, 0xa2, 0xa4, 0xa6, 
       0xa8, 0xaa, 0xac, 0xae, 0xb0, 0xb2, 0xb4, 0xb6, 0xb8, 0xba, 0xbc, 
       0xbe, 0xc0, 0xc2, 0xc4, 0xc6, 0xc8, 0xca, 0xcc, 0xce, 0xd0, 0xd2, 
       0xd4, 0xd6, 0xd8, 0xda, 0xdc, 0xde, 0xe0, 0xe2, 0xe4, 0xe6, 0xe8, 
       0xea, 0xec, 0x2, 0xc, 0x3, 0x2, 0x25, 0x26, 0x3, 0x2, 0x32, 0x33, 
       0x4, 0x2, 0x3, 0x3, 0x10, 0x11, 0x3, 0x2, 0x90, 0x91, 0x3, 0x2, 0x93, 
       0x95, 0x3, 0x2, 0x96, 0x98, 0x3, 0x2, 0x99, 0x9b, 0x3, 0x2, 0x20, 
       0x21, 0x3, 0x2, 0x9d, 0xa0, 0x4, 0x2, 0x8f, 0x8f, 0xa3, 0xa3, 0x2, 
       0x5a6, 0x2, 0xee, 0x3, 0x2, 0x2, 0x2, 0x4, 0xfc, 0x3, 0x2, 0x2, 0x2, 
       0x6, 0xff, 0x3, 0x2, 0x2, 0x2, 0x8, 0x102, 0x3, 0x2, 0x2, 0x2, 0xa, 
       0x106, 0x3, 0x2, 0x2, 0x2, 0xc, 0x110, 0x3, 0x2, 0x2, 0x2, 0xe, 0x115, 
       0x3, 0x2, 0x2, 0x2, 0x10, 0x122, 0x3, 0x2, 0x2, 0x2, 0x12, 0x128, 
       0x3, 0x2, 0x2, 0x2, 0x14, 0x142, 0x3, 0x2, 0x2, 0x2, 0x16, 0x156, 
       0x3, 0x2, 0x2, 0x2, 0x18, 0x160, 0x3, 0x2, 0x2, 0x2, 0x1a, 0x165, 
       0x3, 0x2, 0x2, 0x2, 0x1c, 0x167, 0x3, 0x2, 0x2, 0x2, 0x1e, 0x16a, 
       0x3, 0x2, 0x2, 0x2, 0x20, 0x16d, 0x3, 0x2, 0x2, 0x2, 0x22, 0x172, 
       0x3, 0x2, 0x2, 0x2, 0x24, 0x17d, 0x3, 0x2, 0x2, 0x2, 0x26, 0x18e, 
       0x3, 0x2, 0x2, 0x2, 0x28, 0x190, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x196, 
       0x3, 0x2, 0x2, 0x2, 0x2c, 0x198, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x1a4, 
       0x3, 0x2, 0x2, 0x2, 0x30, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x32, 0x1b0, 
       0x3, 0x2, 0x2, 0x2, 0x34, 0x1b3, 0x3, 0x2, 0x2, 0x2, 0x36, 0x1b8, 
       0x3, 0x2, 0x2, 0x2, 0x38, 0x1ba, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x1c1, 
       0x3, 0x2, 0x2, 0x2, 0x3c, 0x1c9, 0x3, 0x2, 0x2, 0x2, 0x3e, 0x1d7, 
       0x3, 0x2, 0x2, 0x2, 0x40, 0x1e6, 0x3, 0x2, 0x2, 0x2, 0x42, 0x1e8, 
       0x3, 0x2, 0x2, 0x2, 0x44, 0x1eb, 0x3, 0x2, 0x2, 0x2, 0x46, 0x1ef, 
       0x3, 0x2, 0x2, 0x2, 0x48, 0x1f6, 0x3, 0x2, 0x2, 0x2, 0x4a, 0x1fd, 
       0x3, 0x2, 0x2, 0x2, 0x4c, 0x202, 0x3, 0x2, 0x2, 0x2, 0x4e, 0x204, 
       0x3, 0x2, 0x2, 0x2, 0x50, 0x217, 0x3, 0x2, 0x2, 0x2, 0x52, 0x22b, 
       0x3, 0x2, 0x2, 0x2, 0x54, 0x232, 0x3, 0x2, 0x2, 0x2, 0x56, 0x234, 
       0x3, 0x2, 0x2, 0x2, 0x58, 0x237, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x23f, 
       0x3, 0x2, 0x2, 0x2, 0x5c, 0x245, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x247, 
       0x3, 0x2, 0x2, 0x2, 0x60, 0x259, 0x3, 0x2, 0x2, 0x2, 0x62, 0x267, 
       0x3, 0x2, 0x2, 0x2, 0x64, 0x269, 0x3, 0x2, 0x2, 0x2, 0x66, 0x26f, 
       0x3, 0x2, 0x2, 0x2, 0x68, 0x27c, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x27f, 
       0x3, 0x2, 0x2, 0x2, 0x6c, 0x281, 0x3, 0x2, 0x2, 0x2, 0x6e, 0x290, 
       0x3, 0x2, 0x2, 0x2, 0x70, 0x292, 0x3, 0x2, 0x2, 0x2, 0x72, 0x29a, 
       0x3, 0x2, 0x2, 0x2, 0x74, 0x2a2, 0x3, 0x2, 0x2, 0x2, 0x76, 0x2a5, 
       0x3, 0x2, 0x2, 0x2, 0x78, 0x2a7, 0x3, 0x2, 0x2, 0x2, 0x7a, 0x2b4, 
       0x3, 0x2, 0x2, 0x2, 0x7c, 0x2b6, 0x3, 0x2, 0x2, 0x2, 0x7e, 0x2ba, 
       0x3, 0x2, 0x2, 0x2, 0x80, 0x2bc, 0x3, 0x2, 0x2, 0x2, 0x82, 0x2c4, 
       0x3, 0x2, 0x2, 0x2, 0x84, 0x2c6, 0x3, 0x2, 0x2, 0x2, 0x86, 0x2c8, 
       0x3, 0x2, 0x2, 0x2, 0x88, 0x2d0, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x2d8, 
       0x3, 0x2, 0x2, 0x2, 0x8c, 0x2df, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x2e1, 
       0x3, 0x2, 0x2, 0x2, 0x90, 0x2eb, 0x3, 0x2, 0x2, 0x2, 0x92, 0x2fa, 
       0x3, 0x2, 0x2, 0x2, 0x94, 0x303, 0x3, 0x2, 0x2, 0x2, 0x96, 0x305, 
       0x3, 0x2, 0x2, 0x2, 0x98, 0x309, 0x3, 0x2, 0x2, 0x2, 0x9a, 0x30b, 
       0x3, 0x2, 0x2, 0x2, 0x9c, 0x311, 0x3, 0x2, 0x2, 0x2, 0x9e, 0x313, 
       0x3, 0x2, 0x2, 0x2, 0xa0, 0x317, 0x3, 0x2, 0x2, 0x2, 0xa2, 0x31f, 
       0x3, 0x2, 0x2, 0x2, 0xa4, 0x329, 0x3, 0x2, 0x2, 0x2, 0xa6, 0x32d, 
       0x3, 0x2, 0x2, 0x2, 0xa8, 0x331, 0x3, 0x2, 0x2, 0x2, 0xaa, 0x335, 
       0x3, 0x2, 0x2, 0x2, 0xac, 0x337, 0x3, 0x2, 0x2, 0x2, 0xae, 0x33f, 
       0x3, 0x2, 0x2, 0x2, 0xb0, 0x341, 0x3, 0x2, 0x2, 0x2, 0xb2, 0x343, 
       0x3, 0x2, 0x2, 0x2, 0xb4, 0x34b, 0x3, 0x2, 0x2, 0x2, 0xb6, 0x353, 
       0x3, 0x2, 0x2, 0x2, 0xb8, 0x355, 0x3, 0x2, 0x2, 0x2, 0xba, 0x369, 
       0x3, 0x2, 0x2, 0x2, 0xbc, 0x36b, 0x3, 0x2, 0x2, 0x2, 0xbe, 0x382, 
       0x3, 0x2, 0x2, 0x2, 0xc0, 0x393, 0x3, 0x2, 0x2, 0x2, 0xc2, 0x39c, 
       0x3, 0x2, 0x2, 0x2, 0xc4, 0x39e, 0x3, 0x2, 0x2, 0x2, 0xc6, 0x4a8, 
       0x3, 0x2, 0x2, 0x2, 0xc8, 0x4aa, 0x3, 0x2, 0x2, 0x2, 0xca, 0x4b5, 
       0x3, 0x2, 0x2, 0x2, 0xcc, 0x4c0, 0x3, 0x2, 0x2, 0x2, 0xce, 0x4cd, 
       0x3, 0x2, 0x2, 0x2, 0xd0, 0x4d0, 0x3, 0x2, 0x2, 0x2, 0xd2, 0x514, 
       0x3, 0x2, 0x2, 0x2, 0xd4, 0x516, 0x3, 0x2, 0x2, 0x2, 0xd6, 0x51a, 
       0x3, 0x2, 0x2, 0x2, 0xd8, 0x523, 0x3, 0x2, 0x2, 0x2, 0xda, 0x525, 
       0x3, 0x2, 0x2, 0x2, 0xdc, 0x527, 0x3, 0x2, 0x2, 0x2, 0xde, 0x529, 
       0x3, 0x2, 0x2, 0x2, 0xe0, 0x52b, 0x3, 0x2, 0x2, 0x2, 0xe2, 0x52d, 
       0x3, 0x2, 0x2, 0x2, 0xe4, 0x531, 0x3, 0x2, 0x2, 0x2, 0xe6, 0x535, 
       0x3, 0x2, 0x2, 0x2, 0xe8, 0x537, 0x3, 0x2, 0x2, 0x2, 0xea, 0x539, 
       0x3, 0x2, 0x2, 0x2, 0xec, 0x53b, 0x3, 0x2, 0x2, 0x2, 0xee, 0xf3, 
       0x5, 0x4, 0x3, 0x2, 0xef, 0xf4, 0x5, 0xa, 0x6, 0x2, 0xf0, 0xf4, 0x5, 
       0x12, 0xa, 0x2, 0xf1, 0xf4, 0x5, 0x14, 0xb, 0x2, 0xf2, 0xf4, 0x5, 
       0x16, 0xc, 0x2, 0xf3, 0xef, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf0, 0x3, 
       0x2, 0x2, 0x2, 0xf3, 0xf1, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf2, 0x3, 0x2, 
       0x2, 0x2, 0xf4, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf5, 0xf6, 0x5, 0x36, 
       0x1c, 0x2, 0xf6, 0xf7, 0x7, 0x2, 0x2, 0x3, 0xf7, 0x3, 0x3, 0x2, 0x2, 
       0x2, 0xf8, 0xfb, 0x5, 0x6, 0x4, 0x2, 0xf9, 0xfb, 0x5, 0x8, 0x5, 0x2, 
       0xfa, 0xf8, 0x3, 0x2, 0x2, 0x2, 0xfa, 0xf9, 0x3, 0x2, 0x2, 0x2, 0xfb, 
       0xfe, 0x3, 0x2, 0x2, 0x2, 0xfc, 0xfa, 0x3, 0x2, 0x2, 0x2, 0xfc, 0xfd, 
       0x3, 0x2, 0x2, 0x2, 0xfd, 0x5, 0x3, 0x2, 0x2, 0x2, 0xfe, 0xfc, 0x3, 
       0x2, 0x2, 0x2, 0xff, 0x100, 0x7, 0x22, 0x2, 0x2, 0x100, 0x101, 0x5, 
       0xea, 0x76, 0x2, 0x101, 0x7, 0x3, 0x2, 0x2, 0x2, 0x102, 0x103, 0x7, 
       0x23, 0x2, 0x2, 0x103, 0x104, 0x7, 0x8e, 0x2, 0x2, 0x104, 0x105, 
       0x5, 0xea, 0x76, 0x2, 0x105, 0x9, 0x3, 0x2, 0x2, 0x2, 0x106, 0x10a, 
       0x5, 0xe, 0x8, 0x2, 0x107, 0x109, 0x5, 0x18, 0xd, 0x2, 0x108, 0x107, 
       0x3, 0x2, 0x2, 0x2, 0x109, 0x10c, 0x3, 0x2, 0x2, 0x2, 0x10a, 0x108, 
       0x3, 0x2, 0x2, 0x2, 0x10a, 0x10b, 0x3, 0x2, 0x2, 0x2, 0x10b, 0x10d, 
       0x3, 0x2, 0x2, 0x2, 0x10c, 0x10a, 0x3, 0x2, 0x2, 0x2, 0x10d, 0x10e, 
       0x5, 0x20, 0x11, 0x2, 0x10e, 0x10f, 0x5, 0x22, 0x12, 0x2, 0x10f, 
       0xb, 0x3, 0x2, 0x2, 0x2, 0x110, 0x111, 0x5, 0xe, 0x8, 0x2, 0x111, 
       0x112, 0x5, 0x20, 0x11, 0x2, 0x112, 0x113, 0x5, 0x22, 0x12, 0x2, 
       0x113, 0x114, 0x5, 0x36, 0x1c, 0x2, 0x114, 0xd, 0x3, 0x2, 0x2, 0x2, 
       0x115, 0x117, 0x7, 0x24, 0x2, 0x2, 0x116, 0x118, 0x9, 0x2, 0x2, 0x2, 
       0x117, 0x116, 0x3, 0x2, 0x2, 0x2, 0x117, 0x118, 0x3, 0x2, 0x2, 0x2, 
       0x118, 0x120, 0x3, 0x2, 0x2, 0x2, 0x119, 0x11c, 0x5, 0xac, 0x57, 
       0x2, 0x11a, 0x11c, 0x5, 0x10, 0x9, 0x2, 0x11b, 0x119, 0x3, 0x2, 0x2, 
       0x2, 0x11b, 0x11a, 0x3, 0x2, 0x2, 0x2, 0x11c, 0x11d, 0x3, 0x2, 0x2, 
       0x2, 0x11d, 0x11b, 0x3, 0x2, 0x2, 0x2, 0x11d, 0x11e, 0x3, 0x2, 0x2, 
       0x2, 0x11e, 0x121, 0x3, 0x2, 0x2, 0x2, 0x11f, 0x121, 0x7, 0x3, 0x2, 
       0x2, 0x120, 0x11b, 0x3, 0x2, 0x2, 0x2, 0x120, 0x11f, 0x3, 0x2, 0x2, 
       0x2, 0x121, 0xf, 0x3, 0x2, 0x2, 0x2, 0x122, 0x123, 0x7, 0x4, 0x2, 
       0x2, 0x123, 0x124, 0x5, 0xb0, 0x59, 0x2, 0x124, 0x125, 0x7, 0x27, 
       0x2, 0x2, 0x125, 0x126, 0x5, 0xac, 0x57, 0x2, 0x126, 0x127, 0x7, 
       0x5, 0x2, 0x2, 0x127, 0x11, 0x3, 0x2, 0x2, 0x2, 0x128, 0x140, 0x7, 
       0x28, 0x2, 0x2, 0x129, 0x12d, 0x5, 0x64, 0x33, 0x2, 0x12a, 0x12c, 
       0x5, 0x18, 0xd, 0x2, 0x12b, 0x12a, 0x3, 0x2, 0x2, 0x2, 0x12c, 0x12f, 
       0x3, 0x2, 0x2, 0x2, 0x12d, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x12d, 0x12e, 
       0x3, 0x2, 0x2, 0x2, 0x12e, 0x130, 0x3, 0x2, 0x2, 0x2, 0x12f, 0x12d, 
       0x3, 0x2, 0x2, 0x2, 0x130, 0x131, 0x5, 0x20, 0x11, 0x2, 0x131, 0x132, 
       0x5, 0x22, 0x12, 0x2, 0x132, 0x141, 0x3, 0x2, 0x2, 0x2, 0x133, 0x135, 
       0x5, 0x18, 0xd, 0x2, 0x134, 0x133, 0x3, 0x2, 0x2, 0x2, 0x135, 0x138, 
       0x3, 0x2, 0x2, 0x2, 0x136, 0x134, 0x3, 0x2, 0x2, 0x2, 0x136, 0x137, 
       0x3, 0x2, 0x2, 0x2, 0x137, 0x139, 0x3, 0x2, 0x2, 0x2, 0x138, 0x136, 
       0x3, 0x2, 0x2, 0x2, 0x139, 0x13a, 0x7, 0x29, 0x2, 0x2, 0x13a, 0x13c, 
       0x7, 0x6, 0x2, 0x2, 0x13b, 0x13d, 0x5, 0x38, 0x1d, 0x2, 0x13c, 0x13b, 
       0x3, 0x2, 0x2, 0x2, 0x13c, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13d, 0x13e, 
       0x3, 0x2, 0x2, 0x2, 0x13e, 0x13f, 0x7, 0x7, 0x2, 0x2, 0x13f, 0x141, 
       0x5, 0x22, 0x12, 0x2, 0x140, 0x129, 0x3, 0x2, 0x2, 0x2, 0x140, 0x136, 
       0x3, 0x2, 0x2, 0x2, 0x141, 0x13, 0x3, 0x2, 0x2, 0x2, 0x142, 0x149, 
       0x7, 0x2a, 0x2, 0x2, 0x143, 0x145, 0x5, 0xaa, 0x56, 0x2, 0x144, 0x143, 
       0x3, 0x2, 0x2, 0x2, 0x145, 0x146, 0x3, 0x2, 0x2, 0x2, 0x146, 0x144, 
       0x3, 0x2, 0x2, 0x2, 0x146, 0x147, 0x3, 0x2, 0x2, 0x2, 0x147, 0x14a, 
       0x3, 0x2, 0x2, 0x2, 0x148, 0x14a, 0x7, 0x3, 0x2, 0x2, 0x149, 0x144, 
       0x3, 0x2, 0x2, 0x2, 0x149, 0x148, 0x3, 0x2, 0x2, 0x2, 0x14a, 0x14e, 
       0x3, 0x2, 0x2, 0x2, 0x14b, 0x14d, 0x5, 0x18, 0xd, 0x2, 0x14c, 0x14b, 
       0x3, 0x2, 0x2, 0x2, 0x14d, 0x150, 0x3, 0x2, 0x2, 0x2, 0x14e, 0x14c, 
       0x3, 0x2, 0x2, 0x2, 0x14e, 0x14f, 0x3, 0x2, 0x2, 0x2, 0x14f, 0x152, 
       0x3, 0x2, 0x2, 0x2, 0x150, 0x14e, 0x3, 0x2, 0x2, 0x2, 0x151, 0x153, 
       0x5, 0x20, 0x11, 0x2, 0x152, 0x151, 0x3, 0x2, 0x2, 0x2, 0x152, 0x153, 
       0x3, 0x2, 0x2, 0x2, 0x153, 0x154, 0x3, 0x2, 0x2, 0x2, 0x154, 0x155, 
       0x5, 0x22, 0x12, 0x2, 0x155, 0x15, 0x3, 0x2, 0x2, 0x2, 0x156, 0x15a, 
       0x7, 0x2b, 0x2, 0x2, 0x157, 0x159, 0x5, 0x18, 0xd, 0x2, 0x158, 0x157, 
       0x3, 0x2, 0x2, 0x2, 0x159, 0x15c, 0x3, 0x2, 0x2, 0x2, 0x15a, 0x158, 
       0x3, 0x2, 0x2, 0x2, 0x15a, 0x15b, 0x3, 0x2, 0x2, 0x2, 0x15b, 0x15d, 
       0x3, 0x2, 0x2, 0x2, 0x15c, 0x15a, 0x3, 0x2, 0x2, 0x2, 0x15d, 0x15e, 
       0x5, 0x20, 0x11, 0x2, 0x15e, 0x15f, 0x5, 0x22, 0x12, 0x2, 0x15f, 
       0x17, 0x3, 0x2, 0x2, 0x2, 0x160, 0x163, 0x7, 0x2c, 0x2, 0x2, 0x161, 
       0x164, 0x5, 0x1a, 0xe, 0x2, 0x162, 0x164, 0x5, 0x1c, 0xf, 0x2, 0x163, 
       0x161, 0x3, 0x2, 0x2, 0x2, 0x163, 0x162, 0x3, 0x2, 0x2, 0x2, 0x164, 
       0x19, 0x3, 0x2, 0x2, 0x2, 0x165, 0x166, 0x5, 0x1e, 0x10, 0x2, 0x166, 
       0x1b, 0x3, 0x2, 0x2, 0x2, 0x167, 0x168, 0x7, 0x2d, 0x2, 0x2, 0x168, 
       0x169, 0x5, 0x1e, 0x10, 0x2, 0x169, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x16a, 
       0x16b, 0x5, 0xe4, 0x73, 0x2, 0x16b, 0x1f, 0x3, 0x2, 0x2, 0x2, 0x16c, 
       0x16e, 0x7, 0x29, 0x2, 0x2, 0x16d, 0x16c, 0x3, 0x2, 0x2, 0x2, 0x16d, 
       0x16e, 0x3, 0x2, 0x2, 0x2, 0x16e, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x16f, 
       0x170, 0x5, 0x3a, 0x1e, 0x2, 0x170, 0x21, 0x3, 0x2, 0x2, 0x2, 0x171, 
       0x173, 0x5, 0x24, 0x13, 0x2, 0x172, 0x171, 0x3, 0x2, 0x2, 0x2, 0x172, 
       0x173, 0x3, 0x2, 0x2, 0x2, 0x173, 0x175, 0x3, 0x2, 0x2, 0x2, 0x174, 
       0x176, 0x5, 0x28, 0x15, 0x2, 0x175, 0x174, 0x3, 0x2, 0x2, 0x2, 0x175, 
       0x176, 0x3, 0x2, 0x2, 0x2, 0x176, 0x178, 0x3, 0x2, 0x2, 0x2, 0x177, 
       0x179, 0x5, 0x2c, 0x17, 0x2, 0x178, 0x177, 0x3, 0x2, 0x2, 0x2, 0x178, 
       0x179, 0x3, 0x2, 0x2, 0x2, 0x179, 0x17b, 0x3, 0x2, 0x2, 0x2, 0x17a, 
       0x17c, 0x5, 0x30, 0x19, 0x2, 0x17b, 0x17a, 0x3, 0x2, 0x2, 0x2, 0x17b, 
       0x17c, 0x3, 0x2, 0x2, 0x2, 0x17c, 0x23, 0x3, 0x2, 0x2, 0x2, 0x17d, 
       0x17f, 0x7, 0x2e, 0x2, 0x2, 0x17e, 0x180, 0x5, 0x26, 0x14, 0x2, 0x17f, 
       0x17e, 0x3, 0x2, 0x2, 0x2, 0x180, 0x181, 0x3, 0x2, 0x2, 0x2, 0x181, 
       0x17f, 0x3, 0x2, 0x2, 0x2, 0x181, 0x182, 0x3, 0x2, 0x2, 0x2, 0x182, 
       0x25, 0x3, 0x2, 0x2, 0x2, 0x183, 0x18f, 0x5, 0xc6, 0x64, 0x2, 0x184, 
       0x18f, 0x5, 0x5e, 0x30, 0x2, 0x185, 0x186, 0x7, 0x4, 0x2, 0x2, 0x186, 
       0x189, 0x5, 0xb0, 0x59, 0x2, 0x187, 0x188, 0x7, 0x27, 0x2, 0x2, 0x188, 
       0x18a, 0x5, 0xac, 0x57, 0x2, 0x189, 0x187, 0x3, 0x2, 0x2, 0x2, 0x189, 
       0x18a, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x18b, 0x3, 0x2, 0x2, 0x2, 0x18b, 
       0x18c, 0x7, 0x5, 0x2, 0x2, 0x18c, 0x18f, 0x3, 0x2, 0x2, 0x2, 0x18d, 
       0x18f, 0x5, 0xac, 0x57, 0x2, 0x18e, 0x183, 0x3, 0x2, 0x2, 0x2, 0x18e, 
       0x184, 0x3, 0x2, 0x2, 0x2, 0x18e, 0x185, 0x3, 0x2, 0x2, 0x2, 0x18e, 
       0x18d, 0x3, 0x2, 0x2, 0x2, 0x18f, 0x27, 0x3, 0x2, 0x2, 0x2, 0x190, 
       0x192, 0x7, 0x30, 0x2, 0x2, 0x191, 0x193, 0x5, 0x2a, 0x16, 0x2, 0x192, 
       0x191, 0x3, 0x2, 0x2, 0x2, 0x193, 0x194, 0x3, 0x2, 0x2, 0x2, 0x194, 
       0x192, 0x3, 0x2, 0x2, 0x2, 0x194, 0x195, 0x3, 0x2, 0x2, 0x2, 0x195, 
       0x29, 0x3, 0x2, 0x2, 0x2, 0x196, 0x197, 0x5, 0x5c, 0x2f, 0x2, 0x197, 
       0x2b, 0x3, 0x2, 0x2, 0x2, 0x198, 0x19a, 0x7, 0x31, 0x2, 0x2, 0x199, 
       0x19b, 0x5, 0x2e, 0x18, 0x2, 0x19a, 0x199, 0x3, 0x2, 0x2, 0x2, 0x19b, 
       0x19c, 0x3, 0x2, 0x2, 0x2, 0x19c, 0x19a, 0x3, 0x2, 0x2, 0x2, 0x19c, 
       0x19d, 0x3, 0x2, 0x2, 0x2, 0x19d, 0x2d, 0x3, 0x2, 0x2, 0x2, 0x19e, 
       0x19f, 0x9, 0x3, 0x2, 0x2, 0x19f, 0x1a5, 0x5, 0xc4, 0x63, 0x2, 0x1a0, 
       0x1a3, 0x5, 0x5c, 0x2f, 0x2, 0x1a1, 0x1a3, 0x5, 0xac, 0x57, 0x2, 
       0x1a2, 0x1a0, 0x3, 0x2, 0x2, 0x2, 0x1a2, 0x1a1, 0x3, 0x2, 0x2, 0x2, 
       0x1a3, 0x1a5, 0x3, 0x2, 0x2, 0x2, 0x1a4, 0x19e, 0x3, 0x2, 0x2, 0x2, 
       0x1a4, 0x1a2, 0x3, 0x2, 0x2, 0x2, 0x1a5, 0x2f, 0x3, 0x2, 0x2, 0x2, 
       0x1a6, 0x1a8, 0x5, 0x32, 0x1a, 0x2, 0x1a7, 0x1a9, 0x5, 0x34, 0x1b, 
       0x2, 0x1a8, 0x1a7, 0x3, 0x2, 0x2, 0x2, 0x1a8, 0x1a9, 0x3, 0x2, 0x2, 
       0x2, 0x1a9, 0x1af, 0x3, 0x2, 0x2, 0x2, 0x1aa, 0x1ac, 0x5, 0x34, 0x1b, 
       0x2, 0x1ab, 0x1ad, 0x5, 0x32, 0x1a, 0x2, 0x1ac, 0x1ab, 0x3, 0x2, 
       0x2, 0x2, 0x1ac, 0x1ad, 0x3, 0x2, 0x2, 0x2, 0x1ad, 0x1af, 0x3, 0x2, 
       0x2, 0x2, 0x1ae, 0x1a6, 0x3, 0x2, 0x2, 0x2, 0x1ae, 0x1aa, 0x3, 0x2, 
       0x2, 0x2, 0x1af, 0x31, 0x3, 0x2, 0x2, 0x2, 0x1b0, 0x1b1, 0x7, 0x34, 
       0x2, 0x2, 0x1b1, 0x1b2, 0x7, 0x93, 0x2, 0x2, 0x1b2, 0x33, 0x3, 0x2, 
       0x2, 0x2, 0x1b3, 0x1b4, 0x7, 0x35, 0x2, 0x2, 0x1b4, 0x1b5, 0x7, 0x93, 
       0x2, 0x2, 0x1b5, 0x35, 0x3, 0x2, 0x2, 0x2, 0x1b6, 0x1b7, 0x7, 0x36, 
       0x2, 0x2, 0x1b7, 0x1b9, 0x5, 0x4c, 0x27, 0x2, 0x1b8, 0x1b6, 0x3, 
       0x2, 0x2, 0x2, 0x1b8, 0x1b9, 0x3, 0x2, 0x2, 0x2, 0x1b9, 0x37, 0x3, 
       0x2, 0x2, 0x2, 0x1ba, 0x1bf, 0x5, 0x68, 0x35, 0x2, 0x1bb, 0x1bd, 
       0x7, 0x8, 0x2, 0x2, 0x1bc, 0x1be, 0x5, 0x38, 0x1d, 0x2, 0x1bd, 0x1bc, 
       0x3, 0x2, 0x2, 0x2, 0x1bd, 0x1be, 0x3, 0x2, 0x2, 0x2, 0x1be, 0x1c0, 
       0x3, 0x2, 0x2, 0x2, 0x1bf, 0x1bb, 0x3, 0x2, 0x2, 0x2, 0x1bf, 0x1c0, 
       0x3, 0x2, 0x2, 0x2, 0x1c0, 0x39, 0x3, 0x2, 0x2, 0x2, 0x1c1, 0x1c4, 
       0x7, 0x6, 0x2, 0x2, 0x1c2, 0x1c5, 0x5, 0xc, 0x7, 0x2, 0x1c3, 0x1c5, 
       0x5, 0x3c, 0x1f, 0x2, 0x1c4, 0x1c2, 0x3, 0x2, 0x2, 0x2, 0x1c4, 0x1c3, 
       0x3, 0x2, 0x2, 0x2, 0x1c5, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1c6, 0x1c7, 
       0x7, 0x7, 0x2, 0x2, 0x1c7, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x1c8, 0x1ca, 
       0x5, 0x3e, 0x20, 0x2, 0x1c9, 0x1c8, 0x3, 0x2, 0x2, 0x2, 0x1c9, 0x1ca, 
       0x3, 0x2, 0x2, 0x2, 0x1ca, 0x1d4, 0x3, 0x2, 0x2, 0x2, 0x1cb, 0x1cd, 
       0x5, 0x40, 0x21, 0x2, 0x1cc, 0x1ce, 0x7, 0x8, 0x2, 0x2, 0x1cd, 0x1cc, 
       0x3, 0x2, 0x2, 0x2, 0x1cd, 0x1ce, 0x3, 0x2, 0x2, 0x2, 0x1ce, 0x1d0, 
       0x3, 0x2, 0x2, 0x2, 0x1cf, 0x1d1, 0x5, 0x3e, 0x20, 0x2, 0x1d0, 0x1cf, 
       0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 0x3, 0x2, 0x2, 0x2, 0x1d1, 0x1d3, 
       0x3, 0x2, 0x2, 0x2, 0x1d2, 0x1cb, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1d6, 
       0x3, 0x2, 0x2, 0x2, 0x1d4, 0x1d2, 0x3, 0x2, 0x2, 0x2, 0x1d4, 0x1d5, 
       0x3, 0x2, 0x2, 0x2, 0x1d5, 0x3d, 0x3, 0x2, 0x2, 0x2, 0x1d6, 0x1d4, 
       0x3, 0x2, 0x2, 0x2, 0x1d7, 0x1dc, 0x5, 0x74, 0x3b, 0x2, 0x1d8, 0x1da, 
       0x7, 0x8, 0x2, 0x2, 0x1d9, 0x1db, 0x5, 0x3e, 0x20, 0x2, 0x1da, 0x1d9, 
       0x3, 0x2, 0x2, 0x2, 0x1da, 0x1db, 0x3, 0x2, 0x2, 0x2, 0x1db, 0x1dd, 
       0x3, 0x2, 0x2, 0x2, 0x1dc, 0x1d8, 0x3, 0x2, 0x2, 0x2, 0x1dc, 0x1dd, 
       0x3, 0x2, 0x2, 0x2, 0x1dd, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x1de, 0x1e7, 
       0x5, 0x58, 0x2d, 0x2, 0x1df, 0x1e7, 0x5, 0x42, 0x22, 0x2, 0x1e0, 
       0x1e7, 0x5, 0x56, 0x2c, 0x2, 0x1e1, 0x1e7, 0x5, 0x44, 0x23, 0x2, 
       0x1e2, 0x1e7, 0x5, 0x46, 0x24, 0x2, 0x1e3, 0x1e7, 0x5, 0x5a, 0x2e, 
       0x2, 0x1e4, 0x1e7, 0x5, 0x48, 0x25, 0x2, 0x1e5, 0x1e7, 0x5, 0x4a, 
       0x26, 0x2, 0x1e6, 0x1de, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1df, 0x3, 0x2, 
       0x2, 0x2, 0x1e6, 0x1e0, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1e1, 0x3, 0x2, 
       0x2, 0x2, 0x1e6, 0x1e2, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1e3, 0x3, 0x2, 
       0x2, 0x2, 0x1e6, 0x1e4, 0x3, 0x2, 0x2, 0x2, 0x1e6, 0x1e5, 0x3, 0x2, 
       0x2, 0x2, 0x1e7, 0x41, 0x3, 0x2, 0x2, 0x2, 0x1e8, 0x1e9, 0x7, 0x47, 
       0x2, 0x2, 0x1e9, 0x1ea, 0x5, 0x3a, 0x1e, 0x2, 0x1ea, 0x43, 0x3, 0x2, 
       0x2, 0x2, 0x1eb, 0x1ec, 0x7, 0x45, 0x2, 0x2, 0x1ec, 0x1ed, 0x5, 0xaa, 
       0x56, 0x2, 0x1ed, 0x1ee, 0x5, 0x3a, 0x1e, 0x2, 0x1ee, 0x45, 0x3, 
       0x2, 0x2, 0x2, 0x1ef, 0x1f1, 0x7, 0x48, 0x2, 0x2, 0x1f0, 0x1f2, 0x7, 
       0x38, 0x2, 0x2, 0x1f1, 0x1f0, 0x3, 0x2, 0x2, 0x2, 0x1f1, 0x1f2, 0x3, 
       0x2, 0x2, 0x2, 0x1f2, 0x1f3, 0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1f4, 0x5, 
       0xaa, 0x56, 0x2, 0x1f4, 0x1f5, 0x5, 0x3a, 0x1e, 0x2, 0x1f5, 0x47, 
       0x3, 0x2, 0x2, 0x2, 0x1f6, 0x1f7, 0x7, 0x49, 0x2, 0x2, 0x1f7, 0x1f8, 
       0x7, 0x4, 0x2, 0x2, 0x1f8, 0x1f9, 0x5, 0xb0, 0x59, 0x2, 0x1f9, 0x1fa, 
       0x7, 0x27, 0x2, 0x2, 0x1fa, 0x1fb, 0x5, 0xac, 0x57, 0x2, 0x1fb, 0x1fc, 
       0x7, 0x5, 0x2, 0x2, 0x1fc, 0x49, 0x3, 0x2, 0x2, 0x2, 0x1fd, 0x1fe, 
       0x7, 0x36, 0x2, 0x2, 0x1fe, 0x1ff, 0x5, 0x4c, 0x27, 0x2, 0x1ff, 0x4b, 
       0x3, 0x2, 0x2, 0x2, 0x200, 0x203, 0x5, 0x4e, 0x28, 0x2, 0x201, 0x203, 
       0x5, 0x50, 0x29, 0x2, 0x202, 0x200, 0x3, 0x2, 0x2, 0x2, 0x202, 0x201, 
       0x3, 0x2, 0x2, 0x2, 0x203, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x204, 0x205, 
       0x5, 0xac, 0x57, 0x2, 0x205, 0x209, 0x7, 0x6, 0x2, 0x2, 0x206, 0x208, 
       0x5, 0x54, 0x2b, 0x2, 0x207, 0x206, 0x3, 0x2, 0x2, 0x2, 0x208, 0x20b, 
       0x3, 0x2, 0x2, 0x2, 0x209, 0x207, 0x3, 0x2, 0x2, 0x2, 0x209, 0x20a, 
       0x3, 0x2, 0x2, 0x2, 0x20a, 0x20c, 0x3, 0x2, 0x2, 0x2, 0x20b, 0x209, 
       0x3, 0x2, 0x2, 0x2, 0x20c, 0x20d, 0x7, 0x7, 0x2, 0x2, 0x20d, 0x4f, 
       0x3, 0x2, 0x2, 0x2, 0x20e, 0x218, 0x7, 0xa2, 0x2, 0x2, 0x20f, 0x213, 
       0x7, 0x4, 0x2, 0x2, 0x210, 0x212, 0x5, 0xac, 0x57, 0x2, 0x211, 0x210, 
       0x3, 0x2, 0x2, 0x2, 0x212, 0x215, 0x3, 0x2, 0x2, 0x2, 0x213, 0x211, 
       0x3, 0x2, 0x2, 0x2, 0x213, 0x214, 0x3, 0x2, 0x2, 0x2, 0x214, 0x216, 
       0x3, 0x2, 0x2, 0x2, 0x215, 0x213, 0x3, 0x2, 0x2, 0x2, 0x216, 0x218, 
       0x7, 0x5, 0x2, 0x2, 0x217, 0x20e, 0x3, 0x2, 0x2, 0x2, 0x217, 0x20f, 
       0x3, 0x2, 0x2, 0x2, 0x218, 0x219, 0x3, 0x2, 0x2, 0x2, 0x219, 0x21d, 
       0x7, 0x6, 0x2, 0x2, 0x21a, 0x21c, 0x5, 0x52, 0x2a, 0x2, 0x21b, 0x21a, 
       0x3, 0x2, 0x2, 0x2, 0x21c, 0x21f, 0x3, 0x2, 0x2, 0x2, 0x21d, 0x21b, 
       0x3, 0x2, 0x2, 0x2, 0x21d, 0x21e, 0x3, 0x2, 0x2, 0x2, 0x21e, 0x220, 
       0x3, 0x2, 0x2, 0x2, 0x21f, 0x21d, 0x3, 0x2, 0x2, 0x2, 0x220, 0x221, 
       0x7, 0x7, 0x2, 0x2, 0x221, 0x51, 0x3, 0x2, 0x2, 0x2, 0x222, 0x226, 
       0x7, 0x4, 0x2, 0x2, 0x223, 0x225, 0x5, 0x54, 0x2b, 0x2, 0x224, 0x223, 
       0x3, 0x2, 0x2, 0x2, 0x225, 0x228, 0x3, 0x2, 0x2, 0x2, 0x226, 0x224, 
       0x3, 0x2, 0x2, 0x2, 0x226, 0x227, 0x3, 0x2, 0x2, 0x2, 0x227, 0x229, 
       0x3, 0x2, 0x2, 0x2, 0x228, 0x226, 0x3, 0x2, 0x2, 0x2, 0x229, 0x22c, 
       0x7, 0x5, 0x2, 0x2, 0x22a, 0x22c, 0x7, 0xa2, 0x2, 0x2, 0x22b, 0x222, 
       0x3, 0x2, 0x2, 0x2, 0x22b, 0x22a, 0x3, 0x2, 0x2, 0x2, 0x22c, 0x53, 
       0x3, 0x2, 0x2, 0x2, 0x22d, 0x233, 0x5, 0xe4, 0x73, 0x2, 0x22e, 0x233, 
       0x5, 0xd6, 0x6c, 0x2, 0x22f, 0x233, 0x5, 0xd8, 0x6d, 0x2, 0x230, 
       0x233, 0x5, 0xe0, 0x71, 0x2, 0x231, 0x233, 0x7, 0x9, 0x2, 0x2, 0x232, 
       0x22d, 0x3, 0x2, 0x2, 0x2, 0x232, 0x22e, 0x3, 0x2, 0x2, 0x2, 0x232, 
       0x22f, 0x3, 0x2, 0x2, 0x2, 0x232, 0x230, 0x3, 0x2, 0x2, 0x2, 0x232, 
       0x231, 0x3, 0x2, 0x2, 0x2, 0x233, 0x55, 0x3, 0x2, 0x2, 0x2, 0x234, 
       0x235, 0x7, 0x4b, 0x2, 0x2, 0x235, 0x236, 0x5, 0x3a, 0x1e, 0x2, 0x236, 
       0x57, 0x3, 0x2, 0x2, 0x2, 0x237, 0x23c, 0x5, 0x3a, 0x1e, 0x2, 0x238, 
       0x239, 0x7, 0x4c, 0x2, 0x2, 0x239, 0x23b, 0x5, 0x3a, 0x1e, 0x2, 0x23a, 
       0x238, 0x3, 0x2, 0x2, 0x2, 0x23b, 0x23e, 0x3, 0x2, 0x2, 0x2, 0x23c, 
       0x23a, 0x3, 0x2, 0x2, 0x2, 0x23c, 0x23d, 0x3, 0x2, 0x2, 0x2, 0x23d, 
       0x59, 0x3, 0x2, 0x2, 0x2, 0x23e, 0x23c, 0x3, 0x2, 0x2, 0x2, 0x23f, 
       0x240, 0x7, 0x4d, 0x2, 0x2, 0x240, 0x241, 0x5, 0x5c, 0x2f, 0x2, 0x241, 
       0x5b, 0x3, 0x2, 0x2, 0x2, 0x242, 0x246, 0x5, 0xc4, 0x63, 0x2, 0x243, 
       0x246, 0x5, 0xc6, 0x64, 0x2, 0x244, 0x246, 0x5, 0x5e, 0x30, 0x2, 
       0x245, 0x242, 0x3, 0x2, 0x2, 0x2, 0x245, 0x243, 0x3, 0x2, 0x2, 0x2, 
       0x245, 0x244, 0x3, 0x2, 0x2, 0x2, 0x246, 0x5d, 0x3, 0x2, 0x2, 0x2, 
       0x247, 0x248, 0x5, 0xe4, 0x73, 0x2, 0x248, 0x249, 0x5, 0x60, 0x31, 
       0x2, 0x249, 0x5f, 0x3, 0x2, 0x2, 0x2, 0x24a, 0x25a, 0x7, 0xa2, 0x2, 
       0x2, 0x24b, 0x24d, 0x7, 0x4, 0x2, 0x2, 0x24c, 0x24e, 0x7, 0x25, 0x2, 
       0x2, 0x24d, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x24d, 0x24e, 0x3, 0x2, 0x2, 
       0x2, 0x24e, 0x24f, 0x3, 0x2, 0x2, 0x2, 0x24f, 0x254, 0x5, 0xb0, 0x59, 
       0x2, 0x250, 0x251, 0x7, 0xa, 0x2, 0x2, 0x251, 0x253, 0x5, 0xb0, 0x59, 
       0x2, 0x252, 0x250, 0x3, 0x2, 0x2, 0x2, 0x253, 0x256, 0x3, 0x2, 0x2, 
       0x2, 0x254, 0x252, 0x3, 0x2, 0x2, 0x2, 0x254, 0x255, 0x3, 0x2, 0x2, 
       0x2, 0x255, 0x257, 0x3, 0x2, 0x2, 0x2, 0x256, 0x254, 0x3, 0x2, 0x2, 
       0x2, 0x257, 0x258, 0x7, 0x5, 0x2, 0x2, 0x258, 0x25a, 0x3, 0x2, 0x2, 
       0x2, 0x259, 0x24a, 0x3, 0x2, 0x2, 0x2, 0x259, 0x24b, 0x3, 0x2, 0x2, 
       0x2, 0x25a, 0x61, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x268, 0x7, 0xa2, 0x2, 
       0x2, 0x25c, 0x25d, 0x7, 0x4, 0x2, 0x2, 0x25d, 0x262, 0x5, 0xb0, 0x59, 
       0x2, 0x25e, 0x25f, 0x7, 0xa, 0x2, 0x2, 0x25f, 0x261, 0x5, 0xb0, 0x59, 
       0x2, 0x260, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x261, 0x264, 0x3, 0x2, 0x2, 
       0x2, 0x262, 0x260, 0x3, 0x2, 0x2, 0x2, 0x262, 0x263, 0x3, 0x2, 0x2, 
       0x2, 0x263, 0x265, 0x3, 0x2, 0x2, 0x2, 0x264, 0x262, 0x3, 0x2, 0x2, 
       0x2, 0x265, 0x266, 0x7, 0x5, 0x2, 0x2, 0x266, 0x268, 0x3, 0x2, 0x2, 
       0x2, 0x267, 0x25b, 0x3, 0x2, 0x2, 0x2, 0x267, 0x25c, 0x3, 0x2, 0x2, 
       0x2, 0x268, 0x63, 0x3, 0x2, 0x2, 0x2, 0x269, 0x26b, 0x7, 0x6, 0x2, 
       0x2, 0x26a, 0x26c, 0x5, 0x66, 0x34, 0x2, 0x26b, 0x26a, 0x3, 0x2, 
       0x2, 0x2, 0x26b, 0x26c, 0x3, 0x2, 0x2, 0x2, 0x26c, 0x26d, 0x3, 0x2, 
       0x2, 0x2, 0x26d, 0x26e, 0x7, 0x7, 0x2, 0x2, 0x26e, 0x65, 0x3, 0x2, 
       0x2, 0x2, 0x26f, 0x274, 0x5, 0x68, 0x35, 0x2, 0x270, 0x272, 0x7, 
       0x8, 0x2, 0x2, 0x271, 0x273, 0x5, 0x66, 0x34, 0x2, 0x272, 0x271, 
       0x3, 0x2, 0x2, 0x2, 0x272, 0x273, 0x3, 0x2, 0x2, 0x2, 0x273, 0x275, 
       0x3, 0x2, 0x2, 0x2, 0x274, 0x270, 0x3, 0x2, 0x2, 0x2, 0x274, 0x275, 
       0x3, 0x2, 0x2, 0x2, 0x275, 0x67, 0x3, 0x2, 0x2, 0x2, 0x276, 0x277, 
       0x5, 0xa8, 0x55, 0x2, 0x277, 0x278, 0x5, 0x6c, 0x37, 0x2, 0x278, 
       0x27d, 0x3, 0x2, 0x2, 0x2, 0x279, 0x27a, 0x5, 0x98, 0x4d, 0x2, 0x27a, 
       0x27b, 0x5, 0x6a, 0x36, 0x2, 0x27b, 0x27d, 0x3, 0x2, 0x2, 0x2, 0x27c, 
       0x276, 0x3, 0x2, 0x2, 0x2, 0x27c, 0x279, 0x3, 0x2, 0x2, 0x2, 0x27d, 
       0x69, 0x3, 0x2, 0x2, 0x2, 0x27e, 0x280, 0x5, 0x6c, 0x37, 0x2, 0x27f, 
       0x27e, 0x3, 0x2, 0x2, 0x2, 0x27f, 0x280, 0x3, 0x2, 0x2, 0x2, 0x280, 
       0x6b, 0x3, 0x2, 0x2, 0x2, 0x281, 0x282, 0x5, 0x6e, 0x38, 0x2, 0x282, 
       0x28b, 0x5, 0x70, 0x39, 0x2, 0x283, 0x287, 0x7, 0xb, 0x2, 0x2, 0x284, 
       0x285, 0x5, 0x6e, 0x38, 0x2, 0x285, 0x286, 0x5, 0x70, 0x39, 0x2, 
       0x286, 0x288, 0x3, 0x2, 0x2, 0x2, 0x287, 0x284, 0x3, 0x2, 0x2, 0x2, 
       0x287, 0x288, 0x3, 0x2, 0x2, 0x2, 0x288, 0x28a, 0x3, 0x2, 0x2, 0x2, 
       0x289, 0x283, 0x3, 0x2, 0x2, 0x2, 0x28a, 0x28d, 0x3, 0x2, 0x2, 0x2, 
       0x28b, 0x289, 0x3, 0x2, 0x2, 0x2, 0x28b, 0x28c, 0x3, 0x2, 0x2, 0x2, 
       0x28c, 0x6d, 0x3, 0x2, 0x2, 0x2, 0x28d, 0x28b, 0x3, 0x2, 0x2, 0x2, 
       0x28e, 0x291, 0x5, 0xaa, 0x56, 0x2, 0x28f, 0x291, 0x7, 0xc, 0x2, 
       0x2, 0x290, 0x28e, 0x3, 0x2, 0x2, 0x2, 0x290, 0x28f, 0x3, 0x2, 0x2, 
       0x2, 0x291, 0x6f, 0x3, 0x2, 0x2, 0x2, 0x292, 0x297, 0x5, 0x72, 0x3a, 
       0x2, 0x293, 0x294, 0x7, 0xa, 0x2, 0x2, 0x294, 0x296, 0x5, 0x72, 0x3a, 
       0x2, 0x295, 0x293, 0x3, 0x2, 0x2, 0x2, 0x296, 0x299, 0x3, 0x2, 0x2, 
       0x2, 0x297, 0x295, 0x3, 0x2, 0x2, 0x2, 0x297, 0x298, 0x3, 0x2, 0x2, 
       0x2, 0x298, 0x71, 0x3, 0x2, 0x2, 0x2, 0x299, 0x297, 0x3, 0x2, 0x2, 
       0x2, 0x29a, 0x29b, 0x5, 0xa4, 0x53, 0x2, 0x29b, 0x73, 0x3, 0x2, 0x2, 
       0x2, 0x29c, 0x29d, 0x5, 0xa8, 0x55, 0x2, 0x29d, 0x29e, 0x5, 0x78, 
       0x3d, 0x2, 0x29e, 0x2a3, 0x3, 0x2, 0x2, 0x2, 0x29f, 0x2a0, 0x5, 0x9c, 
       0x4f, 0x2, 0x2a0, 0x2a1, 0x5, 0x76, 0x3c, 0x2, 0x2a1, 0x2a3, 0x3, 
       0x2, 0x2, 0x2, 0x2a2, 0x29c, 0x3, 0x2, 0x2, 0x2, 0x2a2, 0x29f, 0x3, 
       0x2, 0x2, 0x2, 0x2a3, 0x75, 0x3, 0x2, 0x2, 0x2, 0x2a4, 0x2a6, 0x5, 
       0x78, 0x3d, 0x2, 0x2a5, 0x2a4, 0x3, 0x2, 0x2, 0x2, 0x2a5, 0x2a6, 
       0x3, 0x2, 0x2, 0x2, 0x2a6, 0x77, 0x3, 0x2, 0x2, 0x2, 0x2a7, 0x2a8, 
       0x5, 0x7e, 0x40, 0x2, 0x2a8, 0x2b1, 0x5, 0x80, 0x41, 0x2, 0x2a9, 
       0x2ad, 0x7, 0xb, 0x2, 0x2, 0x2aa, 0x2ab, 0x5, 0x7e, 0x40, 0x2, 0x2ab, 
       0x2ac, 0x5, 0x70, 0x39, 0x2, 0x2ac, 0x2ae, 0x3, 0x2, 0x2, 0x2, 0x2ad, 
       0x2aa, 0x3, 0x2, 0x2, 0x2, 0x2ad, 0x2ae, 0x3, 0x2, 0x2, 0x2, 0x2ae, 
       0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2af, 0x2a9, 0x3, 0x2, 0x2, 0x2, 0x2b0, 
       0x2b3, 0x3, 0x2, 0x2, 0x2, 0x2b1, 0x2af, 0x3, 0x2, 0x2, 0x2, 0x2b1, 
       0x2b2, 0x3, 0x2, 0x2, 0x2, 0x2b2, 0x79, 0x3, 0x2, 0x2, 0x2, 0x2b3, 
       0x2b1, 0x3, 0x2, 0x2, 0x2, 0x2b4, 0x2b5, 0x5, 0x84, 0x43, 0x2, 0x2b5, 
       0x7b, 0x3, 0x2, 0x2, 0x2, 0x2b6, 0x2b7, 0x5, 0xac, 0x57, 0x2, 0x2b7, 
       0x7d, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2bb, 0x5, 0x7a, 0x3e, 0x2, 0x2b9, 
       0x2bb, 0x5, 0x7c, 0x3f, 0x2, 0x2ba, 0x2b8, 0x3, 0x2, 0x2, 0x2, 0x2ba, 
       0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2bb, 0x7f, 0x3, 0x2, 0x2, 0x2, 0x2bc, 
       0x2c1, 0x5, 0x82, 0x42, 0x2, 0x2bd, 0x2be, 0x7, 0xa, 0x2, 0x2, 0x2be, 
       0x2c0, 0x5, 0x82, 0x42, 0x2, 0x2bf, 0x2bd, 0x3, 0x2, 0x2, 0x2, 0x2c0, 
       0x2c3, 0x3, 0x2, 0x2, 0x2, 0x2c1, 0x2bf, 0x3, 0x2, 0x2, 0x2, 0x2c1, 
       0x2c2, 0x3, 0x2, 0x2, 0x2, 0x2c2, 0x81, 0x3, 0x2, 0x2, 0x2, 0x2c3, 
       0x2c1, 0x3, 0x2, 0x2, 0x2, 0x2c4, 0x2c5, 0x5, 0xa6, 0x54, 0x2, 0x2c5, 
       0x83, 0x3, 0x2, 0x2, 0x2, 0x2c6, 0x2c7, 0x5, 0x86, 0x44, 0x2, 0x2c7, 
       0x85, 0x3, 0x2, 0x2, 0x2, 0x2c8, 0x2cd, 0x5, 0x88, 0x45, 0x2, 0x2c9, 
       0x2ca, 0x7, 0xd, 0x2, 0x2, 0x2ca, 0x2cc, 0x5, 0x88, 0x45, 0x2, 0x2cb, 
       0x2c9, 0x3, 0x2, 0x2, 0x2, 0x2cc, 0x2cf, 0x3, 0x2, 0x2, 0x2, 0x2cd, 
       0x2cb, 0x3, 0x2, 0x2, 0x2, 0x2cd, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x2ce, 
       0x87, 0x3, 0x2, 0x2, 0x2, 0x2cf, 0x2cd, 0x3, 0x2, 0x2, 0x2, 0x2d0, 
       0x2d5, 0x5, 0x8c, 0x47, 0x2, 0x2d1, 0x2d2, 0x7, 0xe, 0x2, 0x2, 0x2d2, 
       0x2d4, 0x5, 0x8c, 0x47, 0x2, 0x2d3, 0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2d4, 
       0x2d7, 0x3, 0x2, 0x2, 0x2, 0x2d5, 0x2d3, 0x3, 0x2, 0x2, 0x2, 0x2d5, 
       0x2d6, 0x3, 0x2, 0x2, 0x2, 0x2d6, 0x89, 0x3, 0x2, 0x2, 0x2, 0x2d7, 
       0x2d5, 0x3, 0x2, 0x2, 0x2, 0x2d8, 0x2da, 0x5, 0x90, 0x49, 0x2, 0x2d9, 
       0x2db, 0x5, 0x8e, 0x48, 0x2, 0x2da, 0x2d9, 0x3, 0x2, 0x2, 0x2, 0x2da, 
       0x2db, 0x3, 0x2, 0x2, 0x2, 0x2db, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x2dc, 
       0x2e0, 0x5, 0x8a, 0x46, 0x2, 0x2dd, 0x2de, 0x7, 0xf, 0x2, 0x2, 0x2de, 
       0x2e0, 0x5, 0x8a, 0x46, 0x2, 0x2df, 0x2dc, 0x3, 0x2, 0x2, 0x2, 0x2df, 
       0x2dd, 0x3, 0x2, 0x2, 0x2, 0x2e0, 0x8d, 0x3, 0x2, 0x2, 0x2, 0x2e1, 
       0x2e2, 0x9, 0x4, 0x2, 0x2, 0x2e2, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x2e3, 
       0x2ec, 0x5, 0xe4, 0x73, 0x2, 0x2e4, 0x2ec, 0x7, 0xc, 0x2, 0x2, 0x2e5, 
       0x2e6, 0x7, 0x12, 0x2, 0x2, 0x2e6, 0x2ec, 0x5, 0x92, 0x4a, 0x2, 0x2e7, 
       0x2e8, 0x7, 0x4, 0x2, 0x2, 0x2e8, 0x2e9, 0x5, 0x84, 0x43, 0x2, 0x2e9, 
       0x2ea, 0x7, 0x5, 0x2, 0x2, 0x2ea, 0x2ec, 0x3, 0x2, 0x2, 0x2, 0x2eb, 
       0x2e3, 0x3, 0x2, 0x2, 0x2, 0x2eb, 0x2e4, 0x3, 0x2, 0x2, 0x2, 0x2eb, 
       0x2e5, 0x3, 0x2, 0x2, 0x2, 0x2eb, 0x2e7, 0x3, 0x2, 0x2, 0x2, 0x2ec, 
       0x91, 0x3, 0x2, 0x2, 0x2, 0x2ed, 0x2fb, 0x5, 0x94, 0x4b, 0x2, 0x2ee, 
       0x2f7, 0x7, 0x4, 0x2, 0x2, 0x2ef, 0x2f4, 0x5, 0x94, 0x4b, 0x2, 0x2f0, 
       0x2f1, 0x7, 0xd, 0x2, 0x2, 0x2f1, 0x2f3, 0x5, 0x94, 0x4b, 0x2, 0x2f2, 
       0x2f0, 0x3, 0x2, 0x2, 0x2, 0x2f3, 0x2f6, 0x3, 0x2, 0x2, 0x2, 0x2f4, 
       0x2f2, 0x3, 0x2, 0x2, 0x2, 0x2f4, 0x2f5, 0x3, 0x2, 0x2, 0x2, 0x2f5, 
       0x2f8, 0x3, 0x2, 0x2, 0x2, 0x2f6, 0x2f4, 0x3, 0x2, 0x2, 0x2, 0x2f7, 
       0x2ef, 0x3, 0x2, 0x2, 0x2, 0x2f7, 0x2f8, 0x3, 0x2, 0x2, 0x2, 0x2f8, 
       0x2f9, 0x3, 0x2, 0x2, 0x2, 0x2f9, 0x2fb, 0x7, 0x5, 0x2, 0x2, 0x2fa, 
       0x2ed, 0x3, 0x2, 0x2, 0x2, 0x2fa, 0x2ee, 0x3, 0x2, 0x2, 0x2, 0x2fb, 
       0x93, 0x3, 0x2, 0x2, 0x2, 0x2fc, 0x304, 0x5, 0xe4, 0x73, 0x2, 0x2fd, 
       0x304, 0x7, 0xc, 0x2, 0x2, 0x2fe, 0x301, 0x7, 0xf, 0x2, 0x2, 0x2ff, 
       0x302, 0x5, 0xe4, 0x73, 0x2, 0x300, 0x302, 0x7, 0xc, 0x2, 0x2, 0x301, 
       0x2ff, 0x3, 0x2, 0x2, 0x2, 0x301, 0x300, 0x3, 0x2, 0x2, 0x2, 0x302, 
       0x304, 0x3, 0x2, 0x2, 0x2, 0x303, 0x2fc, 0x3, 0x2, 0x2, 0x2, 0x303, 
       0x2fd, 0x3, 0x2, 0x2, 0x2, 0x303, 0x2fe, 0x3, 0x2, 0x2, 0x2, 0x304, 
       0x95, 0x3, 0x2, 0x2, 0x2, 0x305, 0x306, 0x7, 0x93, 0x2, 0x2, 0x306, 
       0x97, 0x3, 0x2, 0x2, 0x2, 0x307, 0x30a, 0x5, 0xa0, 0x51, 0x2, 0x308, 
       0x30a, 0x5, 0x9a, 0x4e, 0x2, 0x309, 0x307, 0x3, 0x2, 0x2, 0x2, 0x309, 
       0x308, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x99, 0x3, 0x2, 0x2, 0x2, 0x30b, 
       0x30c, 0x7, 0x13, 0x2, 0x2, 0x30c, 0x30d, 0x5, 0x6c, 0x37, 0x2, 0x30d, 
       0x30e, 0x7, 0x14, 0x2, 0x2, 0x30e, 0x9b, 0x3, 0x2, 0x2, 0x2, 0x30f, 
       0x312, 0x5, 0xa2, 0x52, 0x2, 0x310, 0x312, 0x5, 0x9e, 0x50, 0x2, 
       0x311, 0x30f, 0x3, 0x2, 0x2, 0x2, 0x311, 0x310, 0x3, 0x2, 0x2, 0x2, 
       0x312, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x313, 0x314, 0x7, 0x13, 0x2, 0x2, 
       0x314, 0x315, 0x5, 0x78, 0x3d, 0x2, 0x315, 0x316, 0x7, 0x14, 0x2, 
       0x2, 0x316, 0x9f, 0x3, 0x2, 0x2, 0x2, 0x317, 0x319, 0x7, 0x4, 0x2, 
       0x2, 0x318, 0x31a, 0x5, 0xa4, 0x53, 0x2, 0x319, 0x318, 0x3, 0x2, 
       0x2, 0x2, 0x31a, 0x31b, 0x3, 0x2, 0x2, 0x2, 0x31b, 0x319, 0x3, 0x2, 
       0x2, 0x2, 0x31b, 0x31c, 0x3, 0x2, 0x2, 0x2, 0x31c, 0x31d, 0x3, 0x2, 
       0x2, 0x2, 0x31d, 0x31e, 0x7, 0x5, 0x2, 0x2, 0x31e, 0xa1, 0x3, 0x2, 
       0x2, 0x2, 0x31f, 0x321, 0x7, 0x4, 0x2, 0x2, 0x320, 0x322, 0x5, 0xa6, 
       0x54, 0x2, 0x321, 0x320, 0x3, 0x2, 0x2, 0x2, 0x322, 0x323, 0x3, 0x2, 
       0x2, 0x2, 0x323, 0x321, 0x3, 0x2, 0x2, 0x2, 0x323, 0x324, 0x3, 0x2, 
       0x2, 0x2, 0x324, 0x325, 0x3, 0x2, 0x2, 0x2, 0x325, 0x326, 0x7, 0x5, 
       0x2, 0x2, 0x326, 0xa3, 0x3, 0x2, 0x2, 0x2, 0x327, 0x32a, 0x5, 0xa8, 
       0x55, 0x2, 0x328, 0x32a, 0x5, 0x98, 0x4d, 0x2, 0x329, 0x327, 0x3, 
       0x2, 0x2, 0x2, 0x329, 0x328, 0x3, 0x2, 0x2, 0x2, 0x32a, 0xa5, 0x3, 
       0x2, 0x2, 0x2, 0x32b, 0x32e, 0x5, 0xa8, 0x55, 0x2, 0x32c, 0x32e, 
       0x5, 0x9c, 0x4f, 0x2, 0x32d, 0x32b, 0x3, 0x2, 0x2, 0x2, 0x32d, 0x32c, 
       0x3, 0x2, 0x2, 0x2, 0x32e, 0xa7, 0x3, 0x2, 0x2, 0x2, 0x32f, 0x332, 
       0x5, 0xac, 0x57, 0x2, 0x330, 0x332, 0x5, 0xae, 0x58, 0x2, 0x331, 
       0x32f, 0x3, 0x2, 0x2, 0x2, 0x331, 0x330, 0x3, 0x2, 0x2, 0x2, 0x332, 
       0xa9, 0x3, 0x2, 0x2, 0x2, 0x333, 0x336, 0x5, 0xac, 0x57, 0x2, 0x334, 
       0x336, 0x5, 0xe4, 0x73, 0x2, 0x335, 0x333, 0x3, 0x2, 0x2, 0x2, 0x335, 
       0x334, 0x3, 0x2, 0x2, 0x2, 0x336, 0xab, 0x3, 0x2, 0x2, 0x2, 0x337, 
       0x338, 0x9, 0x5, 0x2, 0x2, 0x338, 0xad, 0x3, 0x2, 0x2, 0x2, 0x339, 
       0x340, 0x5, 0xe4, 0x73, 0x2, 0x33a, 0x340, 0x5, 0xd6, 0x6c, 0x2, 
       0x33b, 0x340, 0x5, 0xd8, 0x6d, 0x2, 0x33c, 0x340, 0x5, 0xe0, 0x71, 
       0x2, 0x33d, 0x340, 0x5, 0xe8, 0x75, 0x2, 0x33e, 0x340, 0x7, 0xa2, 
       0x2, 0x2, 0x33f, 0x339, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x33a, 0x3, 0x2, 
       0x2, 0x2, 0x33f, 0x33b, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x33c, 0x3, 0x2, 
       0x2, 0x2, 0x33f, 0x33d, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x33e, 0x3, 0x2, 
       0x2, 0x2, 0x340, 0xaf, 0x3, 0x2, 0x2, 0x2, 0x341, 0x342, 0x5, 0xb2, 
       0x5a, 0x2, 0x342, 0xb1, 0x3, 0x2, 0x2, 0x2, 0x343, 0x348, 0x5, 0xb4, 
       0x5b, 0x2, 0x344, 0x345, 0x7, 0x15, 0x2, 0x2, 0x345, 0x347, 0x5, 
       0xb4, 0x5b, 0x2, 0x346, 0x344, 0x3, 0x2, 0x2, 0x2, 0x347, 0x34a, 
       0x3, 0x2, 0x2, 0x2, 0x348, 0x346, 0x3, 0x2, 0x2, 0x2, 0x348, 0x349, 
       0x3, 0x2, 0x2, 0x2, 0x349, 0xb3, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x348, 
       0x3, 0x2, 0x2, 0x2, 0x34b, 0x350, 0x5, 0xb6, 0x5c, 0x2, 0x34c, 0x34d, 
       0x7, 0x16, 0x2, 0x2, 0x34d, 0x34f, 0x5, 0xb6, 0x5c, 0x2, 0x34e, 0x34c, 
       0x3, 0x2, 0x2, 0x2, 0x34f, 0x352, 0x3, 0x2, 0x2, 0x2, 0x350, 0x34e, 
       0x3, 0x2, 0x2, 0x2, 0x350, 0x351, 0x3, 0x2, 0x2, 0x2, 0x351, 0xb5, 
       0x3, 0x2, 0x2, 0x2, 0x352, 0x350, 0x3, 0x2, 0x2, 0x2, 0x353, 0x354, 
       0x5, 0xb8, 0x5d, 0x2, 0x354, 0xb7, 0x3, 0x2, 0x2, 0x2, 0x355, 0x367, 
       0x5, 0xba, 0x5e, 0x2, 0x356, 0x357, 0x7, 0x17, 0x2, 0x2, 0x357, 0x368, 
       0x5, 0xba, 0x5e, 0x2, 0x358, 0x359, 0x7, 0x18, 0x2, 0x2, 0x359, 0x368, 
       0x5, 0xba, 0x5e, 0x2, 0x35a, 0x35b, 0x7, 0x19, 0x2, 0x2, 0x35b, 0x368, 
       0x5, 0xba, 0x5e, 0x2, 0x35c, 0x35d, 0x7, 0x1a, 0x2, 0x2, 0x35d, 0x368, 
       0x5, 0xba, 0x5e, 0x2, 0x35e, 0x35f, 0x7, 0x1b, 0x2, 0x2, 0x35f, 0x368, 
       0x5, 0xba, 0x5e, 0x2, 0x360, 0x361, 0x7, 0x1c, 0x2, 0x2, 0x361, 0x368, 
       0x5, 0xba, 0x5e, 0x2, 0x362, 0x363, 0x7, 0x4f, 0x2, 0x2, 0x363, 0x368, 
       0x5, 0x62, 0x32, 0x2, 0x364, 0x365, 0x7, 0x4e, 0x2, 0x2, 0x365, 0x366, 
       0x7, 0x4f, 0x2, 0x2, 0x366, 0x368, 0x5, 0x62, 0x32, 0x2, 0x367, 0x356, 
       0x3, 0x2, 0x2, 0x2, 0x367, 0x358, 0x3, 0x2, 0x2, 0x2, 0x367, 0x35a, 
       0x3, 0x2, 0x2, 0x2, 0x367, 0x35c, 0x3, 0x2, 0x2, 0x2, 0x367, 0x35e, 
       0x3, 0x2, 0x2, 0x2, 0x367, 0x360, 0x3, 0x2, 0x2, 0x2, 0x367, 0x362, 
       0x3, 0x2, 0x2, 0x2, 0x367, 0x364, 0x3, 0x2, 0x2, 0x2, 0x367, 0x368, 
       0x3, 0x2, 0x2, 0x2, 0x368, 0xb9, 0x3, 0x2, 0x2, 0x2, 0x369, 0x36a, 
       0x5, 0xbc, 0x5f, 0x2, 0x36a, 0xbb, 0x3, 0x2, 0x2, 0x2, 0x36b, 0x37f, 
       0x5, 0xbe, 0x60, 0x2, 0x36c, 0x36d, 0x7, 0x10, 0x2, 0x2, 0x36d, 0x37e, 
       0x5, 0xbe, 0x60, 0x2, 0x36e, 0x36f, 0x7, 0x1d, 0x2, 0x2, 0x36f, 0x37e, 
       0x5, 0xbe, 0x60, 0x2, 0x370, 0x373, 0x5, 0xdc, 0x6f, 0x2, 0x371, 
       0x373, 0x5, 0xde, 0x70, 0x2, 0x372, 0x370, 0x3, 0x2, 0x2, 0x2, 0x372, 
       0x371, 0x3, 0x2, 0x2, 0x2, 0x373, 0x37a, 0x3, 0x2, 0x2, 0x2, 0x374, 
       0x375, 0x7, 0x3, 0x2, 0x2, 0x375, 0x379, 0x5, 0xc0, 0x61, 0x2, 0x376, 
       0x377, 0x7, 0xe, 0x2, 0x2, 0x377, 0x379, 0x5, 0xc0, 0x61, 0x2, 0x378, 
       0x374, 0x3, 0x2, 0x2, 0x2, 0x378, 0x376, 0x3, 0x2, 0x2, 0x2, 0x379, 
       0x37c, 0x3, 0x2, 0x2, 0x2, 0x37a, 0x378, 0x3, 0x2, 0x2, 0x2, 0x37a, 
       0x37b, 0x3, 0x2, 0x2, 0x2, 0x37b, 0x37e, 0x3, 0x2, 0x2, 0x2, 0x37c, 
       0x37a, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x36c, 0x3, 0x2, 0x2, 0x2, 0x37d, 
       0x36e, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x372, 0x3, 0x2, 0x2, 0x2, 0x37e, 
       0x381, 0x3, 0x2, 0x2, 0x2, 0x37f, 0x37d, 0x3, 0x2, 0x2, 0x2, 0x37f, 
       0x380, 0x3, 0x2, 0x2, 0x2, 0x380, 0xbd, 0x3, 0x2, 0x2, 0x2, 0x381, 
       0x37f, 0x3, 0x2, 0x2, 0x2, 0x382, 0x389, 0x5, 0xc0, 0x61, 0x2, 0x383, 
       0x384, 0x7, 0x3, 0x2, 0x2, 0x384, 0x388, 0x5, 0xc0, 0x61, 0x2, 0x385, 
       0x386, 0x7, 0xe, 0x2, 0x2, 0x386, 0x388, 0x5, 0xc0, 0x61, 0x2, 0x387, 
       0x383, 0x3, 0x2, 0x2, 0x2, 0x387, 0x385, 0x3, 0x2, 0x2, 0x2, 0x388, 
       0x38b, 0x3, 0x2, 0x2, 0x2, 0x389, 0x387, 0x3, 0x2, 0x2, 0x2, 0x389, 
       0x38a, 0x3, 0x2, 0x2, 0x2, 0x38a, 0xbf, 0x3, 0x2, 0x2, 0x2, 0x38b, 
       0x389, 0x3, 0x2, 0x2, 0x2, 0x38c, 0x38d, 0x7, 0x12, 0x2, 0x2, 0x38d, 
       0x394, 0x5, 0xc2, 0x62, 0x2, 0x38e, 0x38f, 0x7, 0x10, 0x2, 0x2, 0x38f, 
       0x394, 0x5, 0xc2, 0x62, 0x2, 0x390, 0x391, 0x7, 0x1d, 0x2, 0x2, 0x391, 
       0x394, 0x5, 0xc2, 0x62, 0x2, 0x392, 0x394, 0x5, 0xc2, 0x62, 0x2, 
       0x393, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x393, 0x38e, 0x3, 0x2, 0x2, 0x2, 
       0x393, 0x390, 0x3, 0x2, 0x2, 0x2, 0x393, 0x392, 0x3, 0x2, 0x2, 0x2, 
       0x394, 0xc1, 0x3, 0x2, 0x2, 0x2, 0x395, 0x39d, 0x5, 0xc4, 0x63, 0x2, 
       0x396, 0x39d, 0x5, 0xc6, 0x64, 0x2, 0x397, 0x39d, 0x5, 0xd4, 0x6b, 
       0x2, 0x398, 0x39d, 0x5, 0xd6, 0x6c, 0x2, 0x399, 0x39d, 0x5, 0xd8, 
       0x6d, 0x2, 0x39a, 0x39d, 0x5, 0xe0, 0x71, 0x2, 0x39b, 0x39d, 0x5, 
       0xac, 0x57, 0x2, 0x39c, 0x395, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x396, 
       0x3, 0x2, 0x2, 0x2, 0x39c, 0x397, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x398, 
       0x3, 0x2, 0x2, 0x2, 0x39c, 0x399, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x39a, 
       0x3, 0x2, 0x2, 0x2, 0x39c, 0x39b, 0x3, 0x2, 0x2, 0x2, 0x39d, 0xc3, 
       0x3, 0x2, 0x2, 0x2, 0x39e, 0x39f, 0x7, 0x4, 0x2, 0x2, 0x39f, 0x3a0, 
       0x5, 0xb0, 0x59, 0x2, 0x3a0, 0x3a1, 0x7, 0x5, 0x2, 0x2, 0x3a1, 0xc5, 
       0x3, 0x2, 0x2, 0x2, 0x3a2, 0x4a9, 0x5, 0xd2, 0x6a, 0x2, 0x3a3, 0x3a4, 
       0x7, 0x50, 0x2, 0x2, 0x3a4, 0x3a5, 0x7, 0x4, 0x2, 0x2, 0x3a5, 0x3a6, 
       0x5, 0xb0, 0x59, 0x2, 0x3a6, 0x3a7, 0x7, 0x5, 0x2, 0x2, 0x3a7, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3a8, 0x3a9, 0x7, 0x51, 0x2, 0x2, 0x3a9, 0x3aa, 
       0x7, 0x4, 0x2, 0x2, 0x3aa, 0x3ab, 0x5, 0xb0, 0x59, 0x2, 0x3ab, 0x3ac, 
       0x7, 0x5, 0x2, 0x2, 0x3ac, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3ad, 0x3ae, 
       0x7, 0x52, 0x2, 0x2, 0x3ae, 0x3af, 0x7, 0x4, 0x2, 0x2, 0x3af, 0x3b0, 
       0x5, 0xb0, 0x59, 0x2, 0x3b0, 0x3b1, 0x7, 0xa, 0x2, 0x2, 0x3b1, 0x3b2, 
       0x5, 0xb0, 0x59, 0x2, 0x3b2, 0x3b3, 0x7, 0x5, 0x2, 0x2, 0x3b3, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3b5, 0x7, 0x53, 0x2, 0x2, 0x3b5, 0x3b6, 
       0x7, 0x4, 0x2, 0x2, 0x3b6, 0x3b7, 0x5, 0xb0, 0x59, 0x2, 0x3b7, 0x3b8, 
       0x7, 0x5, 0x2, 0x2, 0x3b8, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3b9, 0x3ba, 
       0x7, 0x54, 0x2, 0x2, 0x3ba, 0x3bb, 0x7, 0x4, 0x2, 0x2, 0x3bb, 0x3bc, 
       0x5, 0xac, 0x57, 0x2, 0x3bc, 0x3bd, 0x7, 0x5, 0x2, 0x2, 0x3bd, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3be, 0x3bf, 0x7, 0x55, 0x2, 0x2, 0x3bf, 0x3c0, 
       0x7, 0x4, 0x2, 0x2, 0x3c0, 0x3c1, 0x5, 0xb0, 0x59, 0x2, 0x3c1, 0x3c2, 
       0x7, 0x5, 0x2, 0x2, 0x3c2, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3c3, 0x3c4, 
       0x7, 0x56, 0x2, 0x2, 0x3c4, 0x3c5, 0x7, 0x4, 0x2, 0x2, 0x3c5, 0x3c6, 
       0x5, 0xb0, 0x59, 0x2, 0x3c6, 0x3c7, 0x7, 0x5, 0x2, 0x2, 0x3c7, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3c8, 0x3ce, 0x7, 0x57, 0x2, 0x2, 0x3c9, 0x3ca, 
       0x7, 0x4, 0x2, 0x2, 0x3ca, 0x3cb, 0x5, 0xb0, 0x59, 0x2, 0x3cb, 0x3cc, 
       0x7, 0x5, 0x2, 0x2, 0x3cc, 0x3cf, 0x3, 0x2, 0x2, 0x2, 0x3cd, 0x3cf, 
       0x7, 0xa2, 0x2, 0x2, 0x3ce, 0x3c9, 0x3, 0x2, 0x2, 0x2, 0x3ce, 0x3cd, 
       0x3, 0x2, 0x2, 0x2, 0x3cf, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3d0, 0x3d1, 
       0x7, 0x58, 0x2, 0x2, 0x3d1, 0x4a9, 0x7, 0xa2, 0x2, 0x2, 0x3d2, 0x3d3, 
       0x7, 0x59, 0x2, 0x2, 0x3d3, 0x3d4, 0x7, 0x4, 0x2, 0x2, 0x3d4, 0x3d5, 
       0x5, 0xb0, 0x59, 0x2, 0x3d5, 0x3d6, 0x7, 0x5, 0x2, 0x2, 0x3d6, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3d7, 0x3d8, 0x7, 0x5a, 0x2, 0x2, 0x3d8, 0x3d9, 
       0x7, 0x4, 0x2, 0x2, 0x3d9, 0x3da, 0x5, 0xb0, 0x59, 0x2, 0x3da, 0x3db, 
       0x7, 0x5, 0x2, 0x2, 0x3db, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3dc, 0x3dd, 
       0x7, 0x5b, 0x2, 0x2, 0x3dd, 0x3de, 0x7, 0x4, 0x2, 0x2, 0x3de, 0x3df, 
       0x5, 0xb0, 0x59, 0x2, 0x3df, 0x3e0, 0x7, 0x5, 0x2, 0x2, 0x3e0, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3e1, 0x3e2, 0x7, 0x5c, 0x2, 0x2, 0x3e2, 0x3e3, 
       0x7, 0x4, 0x2, 0x2, 0x3e3, 0x3e4, 0x5, 0xb0, 0x59, 0x2, 0x3e4, 0x3e5, 
       0x7, 0x5, 0x2, 0x2, 0x3e5, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3e6, 0x3e7, 
       0x7, 0x5d, 0x2, 0x2, 0x3e7, 0x4a9, 0x5, 0x62, 0x32, 0x2, 0x3e8, 0x4a9, 
       0x5, 0xca, 0x66, 0x2, 0x3e9, 0x3ea, 0x7, 0x5e, 0x2, 0x2, 0x3ea, 0x3eb, 
       0x7, 0x4, 0x2, 0x2, 0x3eb, 0x3ec, 0x5, 0xb0, 0x59, 0x2, 0x3ec, 0x3ed, 
       0x7, 0x5, 0x2, 0x2, 0x3ed, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3ee, 0x4a9, 
       0x5, 0xcc, 0x67, 0x2, 0x3ef, 0x3f0, 0x7, 0x5f, 0x2, 0x2, 0x3f0, 0x3f1, 
       0x7, 0x4, 0x2, 0x2, 0x3f1, 0x3f2, 0x5, 0xb0, 0x59, 0x2, 0x3f2, 0x3f3, 
       0x7, 0x5, 0x2, 0x2, 0x3f3, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3f5, 
       0x7, 0x60, 0x2, 0x2, 0x3f5, 0x3f6, 0x7, 0x4, 0x2, 0x2, 0x3f6, 0x3f7, 
       0x5, 0xb0, 0x59, 0x2, 0x3f7, 0x3f8, 0x7, 0x5, 0x2, 0x2, 0x3f8, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x3f9, 0x3fa, 0x7, 0x61, 0x2, 0x2, 0x3fa, 0x3fb, 
       0x7, 0x1e, 0x2, 0x2, 0x3fb, 0x3fc, 0x7, 0x62, 0x2, 0x2, 0x3fc, 0x3fd, 
       0x7, 0x1e, 0x2, 0x2, 0x3fd, 0x3fe, 0x7, 0x56, 0x2, 0x2, 0x3fe, 0x3ff, 
       0x7, 0x4, 0x2, 0x2, 0x3ff, 0x400, 0x5, 0xb0, 0x59, 0x2, 0x400, 0x401, 
       0x7, 0x5, 0x2, 0x2, 0x401, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x402, 0x403, 
       0x7, 0x63, 0x2, 0x2, 0x403, 0x404, 0x7, 0x4, 0x2, 0x2, 0x404, 0x405, 
       0x5, 0xb0, 0x59, 0x2, 0x405, 0x406, 0x7, 0xa, 0x2, 0x2, 0x406, 0x407, 
       0x5, 0xb0, 0x59, 0x2, 0x407, 0x408, 0x7, 0x5, 0x2, 0x2, 0x408, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x409, 0x40a, 0x7, 0x64, 0x2, 0x2, 0x40a, 0x40b, 
       0x7, 0x4, 0x2, 0x2, 0x40b, 0x40c, 0x5, 0xb0, 0x59, 0x2, 0x40c, 0x40d, 
       0x7, 0xa, 0x2, 0x2, 0x40d, 0x40e, 0x5, 0xb0, 0x59, 0x2, 0x40e, 0x40f, 
       0x7, 0x5, 0x2, 0x2, 0x40f, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x410, 0x411, 
       0x7, 0x65, 0x2, 0x2, 0x411, 0x412, 0x7, 0x4, 0x2, 0x2, 0x412, 0x413, 
       0x5, 0xb0, 0x59, 0x2, 0x413, 0x414, 0x7, 0xa, 0x2, 0x2, 0x414, 0x415, 
       0x5, 0xb0, 0x59, 0x2, 0x415, 0x416, 0x7, 0x5, 0x2, 0x2, 0x416, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x417, 0x418, 0x7, 0x66, 0x2, 0x2, 0x418, 0x419, 
       0x7, 0x4, 0x2, 0x2, 0x419, 0x41a, 0x5, 0xb0, 0x59, 0x2, 0x41a, 0x41b, 
       0x7, 0xa, 0x2, 0x2, 0x41b, 0x41c, 0x5, 0xb0, 0x59, 0x2, 0x41c, 0x41d, 
       0x7, 0x5, 0x2, 0x2, 0x41d, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x41e, 0x41f, 
       0x7, 0x67, 0x2, 0x2, 0x41f, 0x420, 0x7, 0x4, 0x2, 0x2, 0x420, 0x421, 
       0x5, 0xb0, 0x59, 0x2, 0x421, 0x422, 0x7, 0xa, 0x2, 0x2, 0x422, 0x423, 
       0x5, 0xb0, 0x59, 0x2, 0x423, 0x424, 0x7, 0x5, 0x2, 0x2, 0x424, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x425, 0x426, 0x7, 0x68, 0x2, 0x2, 0x426, 0x427, 
       0x7, 0x4, 0x2, 0x2, 0x427, 0x428, 0x5, 0xb0, 0x59, 0x2, 0x428, 0x429, 
       0x7, 0x5, 0x2, 0x2, 0x429, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x42a, 0x42b, 
       0x7, 0x69, 0x2, 0x2, 0x42b, 0x42c, 0x7, 0x4, 0x2, 0x2, 0x42c, 0x42d, 
       0x5, 0xb0, 0x59, 0x2, 0x42d, 0x42e, 0x7, 0x5, 0x2, 0x2, 0x42e, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x42f, 0x430, 0x7, 0x6a, 0x2, 0x2, 0x430, 0x431, 
       0x7, 0x4, 0x2, 0x2, 0x431, 0x432, 0x5, 0xb0, 0x59, 0x2, 0x432, 0x433, 
       0x7, 0x5, 0x2, 0x2, 0x433, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x434, 0x435, 
       0x7, 0x6b, 0x2, 0x2, 0x435, 0x436, 0x7, 0x4, 0x2, 0x2, 0x436, 0x437, 
       0x5, 0xb0, 0x59, 0x2, 0x437, 0x438, 0x7, 0x5, 0x2, 0x2, 0x438, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x439, 0x43a, 0x7, 0x6c, 0x2, 0x2, 0x43a, 0x43b, 
       0x7, 0x4, 0x2, 0x2, 0x43b, 0x43c, 0x5, 0xb0, 0x59, 0x2, 0x43c, 0x43d, 
       0x7, 0x5, 0x2, 0x2, 0x43d, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x43e, 0x43f, 
       0x7, 0x6d, 0x2, 0x2, 0x43f, 0x440, 0x7, 0x4, 0x2, 0x2, 0x440, 0x441, 
       0x5, 0xb0, 0x59, 0x2, 0x441, 0x442, 0x7, 0x5, 0x2, 0x2, 0x442, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x443, 0x444, 0x7, 0x6e, 0x2, 0x2, 0x444, 0x445, 
       0x7, 0x4, 0x2, 0x2, 0x445, 0x446, 0x5, 0xb0, 0x59, 0x2, 0x446, 0x447, 
       0x7, 0x5, 0x2, 0x2, 0x447, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x448, 0x449, 
       0x7, 0x6f, 0x2, 0x2, 0x449, 0x44a, 0x7, 0x4, 0x2, 0x2, 0x44a, 0x44b, 
       0x5, 0xb0, 0x59, 0x2, 0x44b, 0x44c, 0x7, 0x5, 0x2, 0x2, 0x44c, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x44d, 0x44e, 0x7, 0x70, 0x2, 0x2, 0x44e, 0x4a9, 
       0x7, 0xa2, 0x2, 0x2, 0x44f, 0x450, 0x7, 0x71, 0x2, 0x2, 0x450, 0x4a9, 
       0x7, 0xa2, 0x2, 0x2, 0x451, 0x452, 0x7, 0x72, 0x2, 0x2, 0x452, 0x4a9, 
       0x7, 0xa2, 0x2, 0x2, 0x453, 0x454, 0x7, 0x77, 0x2, 0x2, 0x454, 0x455, 
       0x7, 0x4, 0x2, 0x2, 0x455, 0x456, 0x5, 0xb0, 0x59, 0x2, 0x456, 0x457, 
       0x7, 0x5, 0x2, 0x2, 0x457, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x458, 0x459, 
       0x7, 0x73, 0x2, 0x2, 0x459, 0x45a, 0x7, 0x4, 0x2, 0x2, 0x45a, 0x45b, 
       0x5, 0xb0, 0x59, 0x2, 0x45b, 0x45c, 0x7, 0x5, 0x2, 0x2, 0x45c, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x45d, 0x45e, 0x7, 0x74, 0x2, 0x2, 0x45e, 0x45f, 
       0x7, 0x4, 0x2, 0x2, 0x45f, 0x460, 0x5, 0xb0, 0x59, 0x2, 0x460, 0x461, 
       0x7, 0x5, 0x2, 0x2, 0x461, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x462, 0x463, 
       0x7, 0x75, 0x2, 0x2, 0x463, 0x464, 0x7, 0x4, 0x2, 0x2, 0x464, 0x465, 
       0x5, 0xb0, 0x59, 0x2, 0x465, 0x466, 0x7, 0x5, 0x2, 0x2, 0x466, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x467, 0x468, 0x7, 0x76, 0x2, 0x2, 0x468, 0x469, 
       0x7, 0x4, 0x2, 0x2, 0x469, 0x46a, 0x5, 0xb0, 0x59, 0x2, 0x46a, 0x46b, 
       0x7, 0x5, 0x2, 0x2, 0x46b, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x46c, 0x46d, 
       0x7, 0x78, 0x2, 0x2, 0x46d, 0x4a9, 0x5, 0x62, 0x32, 0x2, 0x46e, 0x46f, 
       0x7, 0x79, 0x2, 0x2, 0x46f, 0x470, 0x7, 0x4, 0x2, 0x2, 0x470, 0x471, 
       0x5, 0xb0, 0x59, 0x2, 0x471, 0x472, 0x7, 0xa, 0x2, 0x2, 0x472, 0x473, 
       0x5, 0xb0, 0x59, 0x2, 0x473, 0x474, 0x7, 0xa, 0x2, 0x2, 0x474, 0x475, 
       0x5, 0xb0, 0x59, 0x2, 0x475, 0x476, 0x7, 0x5, 0x2, 0x2, 0x476, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x477, 0x478, 0x7, 0x7a, 0x2, 0x2, 0x478, 0x479, 
       0x7, 0x4, 0x2, 0x2, 0x479, 0x47a, 0x5, 0xb0, 0x59, 0x2, 0x47a, 0x47b, 
       0x7, 0xa, 0x2, 0x2, 0x47b, 0x47c, 0x5, 0xb0, 0x59, 0x2, 0x47c, 0x47d, 
       0x7, 0x5, 0x2, 0x2, 0x47d, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x47e, 0x47f, 
       0x7, 0x7b, 0x2, 0x2, 0x47f, 0x480, 0x7, 0x4, 0x2, 0x2, 0x480, 0x481, 
       0x5, 0xb0, 0x59, 0x2, 0x481, 0x482, 0x7, 0xa, 0x2, 0x2, 0x482, 0x483, 
       0x5, 0xb0, 0x59, 0x2, 0x483, 0x484, 0x7, 0x5, 0x2, 0x2, 0x484, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x485, 0x486, 0x7, 0x7c, 0x2, 0x2, 0x486, 0x487, 
       0x7, 0x4, 0x2, 0x2, 0x487, 0x488, 0x5, 0xb0, 0x59, 0x2, 0x488, 0x489, 
       0x7, 0xa, 0x2, 0x2, 0x489, 0x48a, 0x5, 0xb0, 0x59, 0x2, 0x48a, 0x48b, 
       0x7, 0x5, 0x2, 0x2, 0x48b, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x48c, 0x48d, 
       0x7, 0x7d, 0x2, 0x2, 0x48d, 0x48e, 0x7, 0x4, 0x2, 0x2, 0x48e, 0x48f, 
       0x5, 0xb0, 0x59, 0x2, 0x48f, 0x490, 0x7, 0x5, 0x2, 0x2, 0x490, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x491, 0x492, 0x7, 0x7e, 0x2, 0x2, 0x492, 0x493, 
       0x7, 0x4, 0x2, 0x2, 0x493, 0x494, 0x5, 0xb0, 0x59, 0x2, 0x494, 0x495, 
       0x7, 0x5, 0x2, 0x2, 0x495, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x496, 0x497, 
       0x7, 0x7f, 0x2, 0x2, 0x497, 0x498, 0x7, 0x4, 0x2, 0x2, 0x498, 0x499, 
       0x5, 0xb0, 0x59, 0x2, 0x499, 0x49a, 0x7, 0x5, 0x2, 0x2, 0x49a, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x49b, 0x49c, 0x7, 0x80, 0x2, 0x2, 0x49c, 0x49d, 
       0x7, 0x4, 0x2, 0x2, 0x49d, 0x49e, 0x5, 0xb0, 0x59, 0x2, 0x49e, 0x49f, 
       0x7, 0x5, 0x2, 0x2, 0x49f, 0x4a9, 0x3, 0x2, 0x2, 0x2, 0x4a0, 0x4a1, 
       0x7, 0x81, 0x2, 0x2, 0x4a1, 0x4a2, 0x7, 0x4, 0x2, 0x2, 0x4a2, 0x4a3, 
       0x5, 0xb0, 0x59, 0x2, 0x4a3, 0x4a4, 0x7, 0x5, 0x2, 0x2, 0x4a4, 0x4a9, 
       0x3, 0x2, 0x2, 0x2, 0x4a5, 0x4a9, 0x5, 0xc8, 0x65, 0x2, 0x4a6, 0x4a9, 
       0x5, 0xce, 0x68, 0x2, 0x4a7, 0x4a9, 0x5, 0xd0, 0x69, 0x2, 0x4a8, 
       0x3a2, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3a3, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3a8, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3ad, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3b4, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3b9, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3be, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3c3, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3c8, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3d0, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3d2, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3d7, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3dc, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3e1, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3e6, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3e8, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3e9, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3ee, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3ef, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x3f4, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x3f9, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x402, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x409, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x410, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x417, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x41e, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x425, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x42a, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x42f, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x434, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x439, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x43e, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x443, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x448, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x44d, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x44f, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x451, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x453, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x458, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x45d, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x462, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x467, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x46c, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x46e, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x477, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x47e, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x485, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x48c, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x491, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x496, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x49b, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x4a0, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x4a5, 0x3, 0x2, 0x2, 0x2, 0x4a8, 0x4a6, 0x3, 0x2, 0x2, 0x2, 0x4a8, 
       0x4a7, 0x3, 0x2, 0x2, 0x2, 0x4a9, 0xc7, 0x3, 0x2, 0x2, 0x2, 0x4aa, 
       0x4ab, 0x7, 0x82, 0x2, 0x2, 0x4ab, 0x4ac, 0x7, 0x4, 0x2, 0x2, 0x4ac, 
       0x4ad, 0x5, 0xb0, 0x59, 0x2, 0x4ad, 0x4ae, 0x7, 0xa, 0x2, 0x2, 0x4ae, 
       0x4b1, 0x5, 0xb0, 0x59, 0x2, 0x4af, 0x4b0, 0x7, 0xa, 0x2, 0x2, 0x4b0, 
       0x4b2, 0x5, 0xb0, 0x59, 0x2, 0x4b1, 0x4af, 0x3, 0x2, 0x2, 0x2, 0x4b1, 
       0x4b2, 0x3, 0x2, 0x2, 0x2, 0x4b2, 0x4b3, 0x3, 0x2, 0x2, 0x2, 0x4b3, 
       0x4b4, 0x7, 0x5, 0x2, 0x2, 0x4b4, 0xc9, 0x3, 0x2, 0x2, 0x2, 0x4b5, 
       0x4b6, 0x7, 0x83, 0x2, 0x2, 0x4b6, 0x4b7, 0x7, 0x4, 0x2, 0x2, 0x4b7, 
       0x4b8, 0x5, 0xb0, 0x59, 0x2, 0x4b8, 0x4b9, 0x7, 0xa, 0x2, 0x2, 0x4b9, 
       0x4bc, 0x5, 0xb0, 0x59, 0x2, 0x4ba, 0x4bb, 0x7, 0xa, 0x2, 0x2, 0x4bb, 
       0x4bd, 0x5, 0xb0, 0x59, 0x2, 0x4bc, 0x4ba, 0x3, 0x2, 0x2, 0x2, 0x4bc, 
       0x4bd, 0x3, 0x2, 0x2, 0x2, 0x4bd, 0x4be, 0x3, 0x2, 0x2, 0x2, 0x4be, 
       0x4bf, 0x7, 0x5, 0x2, 0x2, 0x4bf, 0xcb, 0x3, 0x2, 0x2, 0x2, 0x4c0, 
       0x4c1, 0x7, 0x84, 0x2, 0x2, 0x4c1, 0x4c2, 0x7, 0x4, 0x2, 0x2, 0x4c2, 
       0x4c3, 0x5, 0xb0, 0x59, 0x2, 0x4c3, 0x4c4, 0x7, 0xa, 0x2, 0x2, 0x4c4, 
       0x4c5, 0x5, 0xb0, 0x59, 0x2, 0x4c5, 0x4c6, 0x7, 0xa, 0x2, 0x2, 0x4c6, 
       0x4c9, 0x5, 0xb0, 0x59, 0x2, 0x4c7, 0x4c8, 0x7, 0xa, 0x2, 0x2, 0x4c8, 
       0x4ca, 0x5, 0xb0, 0x59, 0x2, 0x4c9, 0x4c7, 0x3, 0x2, 0x2, 0x2, 0x4c9, 
       0x4ca, 0x3, 0x2, 0x2, 0x2, 0x4ca, 0x4cb, 0x3, 0x2, 0x2, 0x2, 0x4cb, 
       0x4cc, 0x7, 0x5, 0x2, 0x2, 0x4cc, 0xcd, 0x3, 0x2, 0x2, 0x2, 0x4cd, 
       0x4ce, 0x7, 0x85, 0x2, 0x2, 0x4ce, 0x4cf, 0x5, 0x3a, 0x1e, 0x2, 0x4cf, 
       0xcf, 0x3, 0x2, 0x2, 0x2, 0x4d0, 0x4d1, 0x7, 0x4e, 0x2, 0x2, 0x4d1, 
       0x4d2, 0x7, 0x85, 0x2, 0x2, 0x4d2, 0x4d3, 0x5, 0x3a, 0x1e, 0x2, 0x4d3, 
       0xd1, 0x3, 0x2, 0x2, 0x2, 0x4d4, 0x4d5, 0x7, 0x86, 0x2, 0x2, 0x4d5, 
       0x4d7, 0x7, 0x4, 0x2, 0x2, 0x4d6, 0x4d8, 0x7, 0x25, 0x2, 0x2, 0x4d7, 
       0x4d6, 0x3, 0x2, 0x2, 0x2, 0x4d7, 0x4d8, 0x3, 0x2, 0x2, 0x2, 0x4d8, 
       0x4db, 0x3, 0x2, 0x2, 0x2, 0x4d9, 0x4dc, 0x7, 0x3, 0x2, 0x2, 0x4da, 
       0x4dc, 0x5, 0xb0, 0x59, 0x2, 0x4db, 0x4d9, 0x3, 0x2, 0x2, 0x2, 0x4db, 
       0x4da, 0x3, 0x2, 0x2, 0x2, 0x4dc, 0x4dd, 0x3, 0x2, 0x2, 0x2, 0x4dd, 
       0x515, 0x7, 0x5, 0x2, 0x2, 0x4de, 0x4df, 0x7, 0x87, 0x2, 0x2, 0x4df, 
       0x4e1, 0x7, 0x4, 0x2, 0x2, 0x4e0, 0x4e2, 0x7, 0x25, 0x2, 0x2, 0x4e1, 
       0x4e0, 0x3, 0x2, 0x2, 0x2, 0x4e1, 0x4e2, 0x3, 0x2, 0x2, 0x2, 0x4e2, 
       0x4e3, 0x3, 0x2, 0x2, 0x2, 0x4e3, 0x4e4, 0x5, 0xb0, 0x59, 0x2, 0x4e4, 
       0x4e5, 0x7, 0x5, 0x2, 0x2, 0x4e5, 0x515, 0x3, 0x2, 0x2, 0x2, 0x4e6, 
       0x4e7, 0x7, 0x88, 0x2, 0x2, 0x4e7, 0x4e9, 0x7, 0x4, 0x2, 0x2, 0x4e8, 
       0x4ea, 0x7, 0x25, 0x2, 0x2, 0x4e9, 0x4e8, 0x3, 0x2, 0x2, 0x2, 0x4e9, 
       0x4ea, 0x3, 0x2, 0x2, 0x2, 0x4ea, 0x4eb, 0x3, 0x2, 0x2, 0x2, 0x4eb, 
       0x4ec, 0x5, 0xb0, 0x59, 0x2, 0x4ec, 0x4ed, 0x7, 0x5, 0x2, 0x2, 0x4ed, 
       0x515, 0x3, 0x2, 0x2, 0x2, 0x4ee, 0x4ef, 0x7, 0x89, 0x2, 0x2, 0x4ef, 
       0x4f1, 0x7, 0x4, 0x2, 0x2, 0x4f0, 0x4f2, 0x7, 0x25, 0x2, 0x2, 0x4f1, 
       0x4f0, 0x3, 0x2, 0x2, 0x2, 0x4f1, 0x4f2, 0x3, 0x2, 0x2, 0x2, 0x4f2, 
       0x4f3, 0x3, 0x2, 0x2, 0x2, 0x4f3, 0x4f4, 0x5, 0xb0, 0x59, 0x2, 0x4f4, 
       0x4f5, 0x7, 0x5, 0x2, 0x2, 0x4f5, 0x515, 0x3, 0x2, 0x2, 0x2, 0x4f6, 
       0x4f7, 0x7, 0x8a, 0x2, 0x2, 0x4f7, 0x4f9, 0x7, 0x4, 0x2, 0x2, 0x4f8, 
       0x4fa, 0x7, 0x25, 0x2, 0x2, 0x4f9, 0x4f8, 0x3, 0x2, 0x2, 0x2, 0x4f9, 
       0x4fa, 0x3, 0x2, 0x2, 0x2, 0x4fa, 0x4fb, 0x3, 0x2, 0x2, 0x2, 0x4fb, 
       0x4fc, 0x5, 0xb0, 0x59, 0x2, 0x4fc, 0x4fd, 0x7, 0x5, 0x2, 0x2, 0x4fd, 
       0x515, 0x3, 0x2, 0x2, 0x2, 0x4fe, 0x4ff, 0x7, 0x8b, 0x2, 0x2, 0x4ff, 
       0x501, 0x7, 0x4, 0x2, 0x2, 0x500, 0x502, 0x7, 0x25, 0x2, 0x2, 0x501, 
       0x500, 0x3, 0x2, 0x2, 0x2, 0x501, 0x502, 0x3, 0x2, 0x2, 0x2, 0x502, 
       0x503, 0x3, 0x2, 0x2, 0x2, 0x503, 0x504, 0x5, 0xb0, 0x59, 0x2, 0x504, 
       0x505, 0x7, 0x5, 0x2, 0x2, 0x505, 0x515, 0x3, 0x2, 0x2, 0x2, 0x506, 
       0x507, 0x7, 0x2f, 0x2, 0x2, 0x507, 0x509, 0x7, 0x4, 0x2, 0x2, 0x508, 
       0x50a, 0x7, 0x25, 0x2, 0x2, 0x509, 0x508, 0x3, 0x2, 0x2, 0x2, 0x509, 
       0x50a, 0x3, 0x2, 0x2, 0x2, 0x50a, 0x50b, 0x3, 0x2, 0x2, 0x2, 0x50b, 
       0x510, 0x5, 0xb0, 0x59, 0x2, 0x50c, 0x50d, 0x7, 0xb, 0x2, 0x2, 0x50d, 
       0x50e, 0x7, 0x8c, 0x2, 0x2, 0x50e, 0x50f, 0x7, 0x17, 0x2, 0x2, 0x50f, 
       0x511, 0x5, 0xe2, 0x72, 0x2, 0x510, 0x50c, 0x3, 0x2, 0x2, 0x2, 0x510, 
       0x511, 0x3, 0x2, 0x2, 0x2, 0x511, 0x512, 0x3, 0x2, 0x2, 0x2, 0x512, 
       0x513, 0x7, 0x5, 0x2, 0x2, 0x513, 0x515, 0x3, 0x2, 0x2, 0x2, 0x514, 
       0x4d4, 0x3, 0x2, 0x2, 0x2, 0x514, 0x4de, 0x3, 0x2, 0x2, 0x2, 0x514, 
       0x4e6, 0x3, 0x2, 0x2, 0x2, 0x514, 0x4ee, 0x3, 0x2, 0x2, 0x2, 0x514, 
       0x4f6, 0x3, 0x2, 0x2, 0x2, 0x514, 0x4fe, 0x3, 0x2, 0x2, 0x2, 0x514, 
       0x506, 0x3, 0x2, 0x2, 0x2, 0x515, 0xd3, 0x3, 0x2, 0x2, 0x2, 0x516, 
       0x518, 0x5, 0xe4, 0x73, 0x2, 0x517, 0x519, 0x5, 0x60, 0x31, 0x2, 
       0x518, 0x517, 0x3, 0x2, 0x2, 0x2, 0x518, 0x519, 0x3, 0x2, 0x2, 0x2, 
       0x519, 0xd5, 0x3, 0x2, 0x2, 0x2, 0x51a, 0x51e, 0x5, 0xe2, 0x72, 0x2, 
       0x51b, 0x51f, 0x7, 0x92, 0x2, 0x2, 0x51c, 0x51d, 0x7, 0x1f, 0x2, 
       0x2, 0x51d, 0x51f, 0x5, 0xe4, 0x73, 0x2, 0x51e, 0x51b, 0x3, 0x2, 
       0x2, 0x2, 0x51e, 0x51c, 0x3, 0x2, 0x2, 0x2, 0x51e, 0x51f, 0x3, 0x2, 
       0x2, 0x2, 0x51f, 0xd7, 0x3, 0x2, 0x2, 0x2, 0x520, 0x524, 0x5, 0xda, 
       0x6e, 0x2, 0x521, 0x524, 0x5, 0xdc, 0x6f, 0x2, 0x522, 0x524, 0x5, 
       0xde, 0x70, 0x2, 0x523, 0x520, 0x3, 0x2, 0x2, 0x2, 0x523, 0x521, 
       0x3, 0x2, 0x2, 0x2, 0x523, 0x522, 0x3, 0x2, 0x2, 0x2, 0x524, 0xd9, 
       0x3, 0x2, 0x2, 0x2, 0x525, 0x526, 0x9, 0x6, 0x2, 0x2, 0x526, 0xdb, 
       0x3, 0x2, 0x2, 0x2, 0x527, 0x528, 0x9, 0x7, 0x2, 0x2, 0x528, 0xdd, 
       0x3, 0x2, 0x2, 0x2, 0x529, 0x52a, 0x9, 0x8, 0x2, 0x2, 0x52a, 0xdf, 
       0x3, 0x2, 0x2, 0x2, 0x52b, 0x52c, 0x9, 0x9, 0x2, 0x2, 0x52c, 0xe1, 
       0x3, 0x2, 0x2, 0x2, 0x52d, 0x52e, 0x9, 0xa, 0x2, 0x2, 0x52e, 0xe3, 
       0x3, 0x2, 0x2, 0x2, 0x52f, 0x532, 0x5, 0xea, 0x76, 0x2, 0x530, 0x532, 
       0x5, 0xe6, 0x74, 0x2, 0x531, 0x52f, 0x3, 0x2, 0x2, 0x2, 0x531, 0x530, 
       0x3, 0x2, 0x2, 0x2, 0x532, 0xe5, 0x3, 0x2, 0x2, 0x2, 0x533, 0x536, 
       0x5, 0xec, 0x77, 0x2, 0x534, 0x536, 0x7, 0x8e, 0x2, 0x2, 0x535, 0x533, 
       0x3, 0x2, 0x2, 0x2, 0x535, 0x534, 0x3, 0x2, 0x2, 0x2, 0x536, 0xe7, 
       0x3, 0x2, 0x2, 0x2, 0x537, 0x538, 0x9, 0xb, 0x2, 0x2, 0x538, 0xe9, 
       0x3, 0x2, 0x2, 0x2, 0x539, 0x53a, 0x7, 0x8d, 0x2, 0x2, 0x53a, 0xeb, 
       0x3, 0x2, 0x2, 0x2, 0x53b, 0x53c, 0x7, 0x8e, 0x2, 0x2, 0x53c, 0x53d, 
       0x7, 0xa7, 0x2, 0x2, 0x53d, 0xed, 0x3, 0x2, 0x2, 0x2, 0x80, 0xf3, 
       0xfa, 0xfc, 0x10a, 0x117, 0x11b, 0x11d, 0x120, 0x12d, 0x136, 0x13c, 
       0x140, 0x146, 0x149, 0x14e, 0x152, 0x15a, 0x163, 0x16d, 0x172, 0x175, 
       0x178, 0x17b, 0x181, 0x189, 0x18e, 0x194, 0x19c, 0x1a2, 0x1a4, 0x1a8, 
       0x1ac, 0x1ae, 0x1b8, 0x1bd, 0x1bf, 0x1c4, 0x1c9, 0x1cd, 0x1d0, 0x1d4, 
       0x1da, 0x1dc, 0x1e6, 0x1f1, 0x202, 0x209, 0x213, 0x217, 0x21d, 0x226, 
       0x22b, 0x232, 0x23c, 0x245, 0x24d, 0x254, 0x259, 0x262, 0x267, 0x26b, 
       0x272, 0x274, 0x27c, 0x27f, 0x287, 0x28b, 0x290, 0x297, 0x2a2, 0x2a5, 
       0x2ad, 0x2b1, 0x2ba, 0x2c1, 0x2cd, 0x2d5, 0x2da, 0x2df, 0x2eb, 0x2f4, 
       0x2f7, 0x2fa, 0x301, 0x303, 0x309, 0x311, 0x31b, 0x323, 0x329, 0x32d, 
       0x331, 0x335, 0x33f, 0x348, 0x350, 0x367, 0x372, 0x378, 0x37a, 0x37d, 
       0x37f, 0x387, 0x389, 0x393, 0x39c, 0x3ce, 0x4a8, 0x4b1, 0x4bc, 0x4c9, 
       0x4d7, 0x4db, 0x4e1, 0x4e9, 0x4f1, 0x4f9, 0x501, 0x509, 0x510, 0x514, 
       0x518, 0x51e, 0x523, 0x531, 0x535, 
  };

  _serializedATN.insert(_serializedATN.end(), serializedATNSegment0,
    serializedATNSegment0 + sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));


  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

SparqlParser::Initializer SparqlParser::_init;
