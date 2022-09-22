
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#include "SparqlAutomaticParser.h"

#include "SparqlAutomaticListener.h"
#include "SparqlAutomaticVisitor.h"

using namespace antlrcpp;
using namespace antlr4;

SparqlAutomaticParser::SparqlAutomaticParser(TokenStream* input)
    : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA,
                                             _sharedContextCache);
}

SparqlAutomaticParser::~SparqlAutomaticParser() { delete _interpreter; }

std::string SparqlAutomaticParser::getGrammarFileName() const {
  return "SparqlAutomatic.g4";
}

const std::vector<std::string>& SparqlAutomaticParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& SparqlAutomaticParser::getVocabulary() const {
  return _vocabulary;
}

//----------------- QueryContext
//------------------------------------------------------------------

SparqlAutomaticParser::QueryContext::QueryContext(ParserRuleContext* parent,
                                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PrologueContext*
SparqlAutomaticParser::QueryContext::prologue() {
  return getRuleContext<SparqlAutomaticParser::PrologueContext>(0);
}

SparqlAutomaticParser::ValuesClauseContext*
SparqlAutomaticParser::QueryContext::valuesClause() {
  return getRuleContext<SparqlAutomaticParser::ValuesClauseContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::QueryContext::EOF() {
  return getToken(SparqlAutomaticParser::EOF, 0);
}

SparqlAutomaticParser::SelectQueryContext*
SparqlAutomaticParser::QueryContext::selectQuery() {
  return getRuleContext<SparqlAutomaticParser::SelectQueryContext>(0);
}

SparqlAutomaticParser::ConstructQueryContext*
SparqlAutomaticParser::QueryContext::constructQuery() {
  return getRuleContext<SparqlAutomaticParser::ConstructQueryContext>(0);
}

SparqlAutomaticParser::DescribeQueryContext*
SparqlAutomaticParser::QueryContext::describeQuery() {
  return getRuleContext<SparqlAutomaticParser::DescribeQueryContext>(0);
}

SparqlAutomaticParser::AskQueryContext*
SparqlAutomaticParser::QueryContext::askQuery() {
  return getRuleContext<SparqlAutomaticParser::AskQueryContext>(0);
}

size_t SparqlAutomaticParser::QueryContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleQuery;
}

void SparqlAutomaticParser::QueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterQuery(this);
}

void SparqlAutomaticParser::QueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitQuery(this);
}

antlrcpp::Any SparqlAutomaticParser::QueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::QueryContext* SparqlAutomaticParser::query() {
  QueryContext* _localctx =
      _tracker.createInstance<QueryContext>(_ctx, getState());
  enterRule(_localctx, 0, SparqlAutomaticParser::RuleQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(264);
    prologue();
    setState(269);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::SELECT: {
        setState(265);
        selectQuery();
        break;
      }

      case SparqlAutomaticParser::CONSTRUCT: {
        setState(266);
        constructQuery();
        break;
      }

      case SparqlAutomaticParser::DESCRIBE: {
        setState(267);
        describeQuery();
        break;
      }

      case SparqlAutomaticParser::ASK: {
        setState(268);
        askQuery();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(271);
    valuesClause();
    setState(272);
    match(SparqlAutomaticParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrologueContext
//------------------------------------------------------------------

SparqlAutomaticParser::PrologueContext::PrologueContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::BaseDeclContext*>
SparqlAutomaticParser::PrologueContext::baseDecl() {
  return getRuleContexts<SparqlAutomaticParser::BaseDeclContext>();
}

SparqlAutomaticParser::BaseDeclContext*
SparqlAutomaticParser::PrologueContext::baseDecl(size_t i) {
  return getRuleContext<SparqlAutomaticParser::BaseDeclContext>(i);
}

std::vector<SparqlAutomaticParser::PrefixDeclContext*>
SparqlAutomaticParser::PrologueContext::prefixDecl() {
  return getRuleContexts<SparqlAutomaticParser::PrefixDeclContext>();
}

SparqlAutomaticParser::PrefixDeclContext*
SparqlAutomaticParser::PrologueContext::prefixDecl(size_t i) {
  return getRuleContext<SparqlAutomaticParser::PrefixDeclContext>(i);
}

size_t SparqlAutomaticParser::PrologueContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePrologue;
}

void SparqlAutomaticParser::PrologueContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPrologue(this);
}

void SparqlAutomaticParser::PrologueContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPrologue(this);
}

antlrcpp::Any SparqlAutomaticParser::PrologueContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPrologue(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PrologueContext* SparqlAutomaticParser::prologue() {
  PrologueContext* _localctx =
      _tracker.createInstance<PrologueContext>(_ctx, getState());
  enterRule(_localctx, 2, SparqlAutomaticParser::RulePrologue);
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
    setState(278);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::BASE

           || _la == SparqlAutomaticParser::PREFIX) {
      setState(276);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlAutomaticParser::BASE: {
          setState(274);
          baseDecl();
          break;
        }

        case SparqlAutomaticParser::PREFIX: {
          setState(275);
          prefixDecl();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(280);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BaseDeclContext
//------------------------------------------------------------------

SparqlAutomaticParser::BaseDeclContext::BaseDeclContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::BaseDeclContext::BASE() {
  return getToken(SparqlAutomaticParser::BASE, 0);
}

SparqlAutomaticParser::IrirefContext*
SparqlAutomaticParser::BaseDeclContext::iriref() {
  return getRuleContext<SparqlAutomaticParser::IrirefContext>(0);
}

size_t SparqlAutomaticParser::BaseDeclContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleBaseDecl;
}

void SparqlAutomaticParser::BaseDeclContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterBaseDecl(this);
}

void SparqlAutomaticParser::BaseDeclContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitBaseDecl(this);
}

antlrcpp::Any SparqlAutomaticParser::BaseDeclContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBaseDecl(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BaseDeclContext* SparqlAutomaticParser::baseDecl() {
  BaseDeclContext* _localctx =
      _tracker.createInstance<BaseDeclContext>(_ctx, getState());
  enterRule(_localctx, 4, SparqlAutomaticParser::RuleBaseDecl);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(281);
    match(SparqlAutomaticParser::BASE);
    setState(282);
    iriref();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrefixDeclContext
//------------------------------------------------------------------

SparqlAutomaticParser::PrefixDeclContext::PrefixDeclContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::PrefixDeclContext::PREFIX() {
  return getToken(SparqlAutomaticParser::PREFIX, 0);
}

tree::TerminalNode* SparqlAutomaticParser::PrefixDeclContext::PNAME_NS() {
  return getToken(SparqlAutomaticParser::PNAME_NS, 0);
}

SparqlAutomaticParser::IrirefContext*
SparqlAutomaticParser::PrefixDeclContext::iriref() {
  return getRuleContext<SparqlAutomaticParser::IrirefContext>(0);
}

size_t SparqlAutomaticParser::PrefixDeclContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePrefixDecl;
}

void SparqlAutomaticParser::PrefixDeclContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPrefixDecl(this);
}

void SparqlAutomaticParser::PrefixDeclContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPrefixDecl(this);
}

antlrcpp::Any SparqlAutomaticParser::PrefixDeclContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPrefixDecl(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PrefixDeclContext* SparqlAutomaticParser::prefixDecl() {
  PrefixDeclContext* _localctx =
      _tracker.createInstance<PrefixDeclContext>(_ctx, getState());
  enterRule(_localctx, 6, SparqlAutomaticParser::RulePrefixDecl);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(284);
    match(SparqlAutomaticParser::PREFIX);
    setState(285);
    match(SparqlAutomaticParser::PNAME_NS);
    setState(286);
    iriref();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectQueryContext
//------------------------------------------------------------------

SparqlAutomaticParser::SelectQueryContext::SelectQueryContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::SelectClauseContext*
SparqlAutomaticParser::SelectQueryContext::selectClause() {
  return getRuleContext<SparqlAutomaticParser::SelectClauseContext>(0);
}

SparqlAutomaticParser::WhereClauseContext*
SparqlAutomaticParser::SelectQueryContext::whereClause() {
  return getRuleContext<SparqlAutomaticParser::WhereClauseContext>(0);
}

SparqlAutomaticParser::SolutionModifierContext*
SparqlAutomaticParser::SelectQueryContext::solutionModifier() {
  return getRuleContext<SparqlAutomaticParser::SolutionModifierContext>(0);
}

std::vector<SparqlAutomaticParser::DatasetClauseContext*>
SparqlAutomaticParser::SelectQueryContext::datasetClause() {
  return getRuleContexts<SparqlAutomaticParser::DatasetClauseContext>();
}

SparqlAutomaticParser::DatasetClauseContext*
SparqlAutomaticParser::SelectQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DatasetClauseContext>(i);
}

size_t SparqlAutomaticParser::SelectQueryContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleSelectQuery;
}

void SparqlAutomaticParser::SelectQueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSelectQuery(this);
}

void SparqlAutomaticParser::SelectQueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSelectQuery(this);
}

antlrcpp::Any SparqlAutomaticParser::SelectQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitSelectQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::SelectQueryContext*
SparqlAutomaticParser::selectQuery() {
  SelectQueryContext* _localctx =
      _tracker.createInstance<SelectQueryContext>(_ctx, getState());
  enterRule(_localctx, 8, SparqlAutomaticParser::RuleSelectQuery);
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
    setState(288);
    selectClause();
    setState(292);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::FROM) {
      setState(289);
      datasetClause();
      setState(294);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(295);
    whereClause();
    setState(296);
    solutionModifier();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubSelectContext
//------------------------------------------------------------------

SparqlAutomaticParser::SubSelectContext::SubSelectContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::SelectClauseContext*
SparqlAutomaticParser::SubSelectContext::selectClause() {
  return getRuleContext<SparqlAutomaticParser::SelectClauseContext>(0);
}

SparqlAutomaticParser::WhereClauseContext*
SparqlAutomaticParser::SubSelectContext::whereClause() {
  return getRuleContext<SparqlAutomaticParser::WhereClauseContext>(0);
}

SparqlAutomaticParser::SolutionModifierContext*
SparqlAutomaticParser::SubSelectContext::solutionModifier() {
  return getRuleContext<SparqlAutomaticParser::SolutionModifierContext>(0);
}

SparqlAutomaticParser::ValuesClauseContext*
SparqlAutomaticParser::SubSelectContext::valuesClause() {
  return getRuleContext<SparqlAutomaticParser::ValuesClauseContext>(0);
}

size_t SparqlAutomaticParser::SubSelectContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleSubSelect;
}

void SparqlAutomaticParser::SubSelectContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSubSelect(this);
}

void SparqlAutomaticParser::SubSelectContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSubSelect(this);
}

antlrcpp::Any SparqlAutomaticParser::SubSelectContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitSubSelect(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::SubSelectContext* SparqlAutomaticParser::subSelect() {
  SubSelectContext* _localctx =
      _tracker.createInstance<SubSelectContext>(_ctx, getState());
  enterRule(_localctx, 10, SparqlAutomaticParser::RuleSubSelect);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(298);
    selectClause();
    setState(299);
    whereClause();
    setState(300);
    solutionModifier();
    setState(301);
    valuesClause();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SelectClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::SelectClauseContext::SelectClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::SelectClauseContext::SELECT() {
  return getToken(SparqlAutomaticParser::SELECT, 0);
}

tree::TerminalNode* SparqlAutomaticParser::SelectClauseContext::DISTINCT() {
  return getToken(SparqlAutomaticParser::DISTINCT, 0);
}

tree::TerminalNode* SparqlAutomaticParser::SelectClauseContext::REDUCED() {
  return getToken(SparqlAutomaticParser::REDUCED, 0);
}

std::vector<SparqlAutomaticParser::VarOrAliasContext*>
SparqlAutomaticParser::SelectClauseContext::varOrAlias() {
  return getRuleContexts<SparqlAutomaticParser::VarOrAliasContext>();
}

SparqlAutomaticParser::VarOrAliasContext*
SparqlAutomaticParser::SelectClauseContext::varOrAlias(size_t i) {
  return getRuleContext<SparqlAutomaticParser::VarOrAliasContext>(i);
}

size_t SparqlAutomaticParser::SelectClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleSelectClause;
}

void SparqlAutomaticParser::SelectClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSelectClause(this);
}

void SparqlAutomaticParser::SelectClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSelectClause(this);
}

antlrcpp::Any SparqlAutomaticParser::SelectClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitSelectClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::SelectClauseContext*
SparqlAutomaticParser::selectClause() {
  SelectClauseContext* _localctx =
      _tracker.createInstance<SelectClauseContext>(_ctx, getState());
  enterRule(_localctx, 12, SparqlAutomaticParser::RuleSelectClause);
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
    setState(303);
    match(SparqlAutomaticParser::SELECT);
    setState(305);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::DISTINCT

        || _la == SparqlAutomaticParser::REDUCED) {
      setState(304);
      _la = _input->LA(1);
      if (!(_la == SparqlAutomaticParser::DISTINCT

            || _la == SparqlAutomaticParser::REDUCED)) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(313);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        setState(308);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(307);
          varOrAlias();
          setState(310);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == SparqlAutomaticParser::T__1 ||
                 _la == SparqlAutomaticParser::VAR1

                 || _la == SparqlAutomaticParser::VAR2);
        break;
      }

      case SparqlAutomaticParser::T__0: {
        setState(312);
        dynamic_cast<SelectClauseContext*>(_localctx)->asterisk =
            match(SparqlAutomaticParser::T__0);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarOrAliasContext
//------------------------------------------------------------------

SparqlAutomaticParser::VarOrAliasContext::VarOrAliasContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::VarOrAliasContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

SparqlAutomaticParser::AliasContext*
SparqlAutomaticParser::VarOrAliasContext::alias() {
  return getRuleContext<SparqlAutomaticParser::AliasContext>(0);
}

size_t SparqlAutomaticParser::VarOrAliasContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVarOrAlias;
}

void SparqlAutomaticParser::VarOrAliasContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVarOrAlias(this);
}

void SparqlAutomaticParser::VarOrAliasContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVarOrAlias(this);
}

antlrcpp::Any SparqlAutomaticParser::VarOrAliasContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVarOrAlias(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VarOrAliasContext* SparqlAutomaticParser::varOrAlias() {
  VarOrAliasContext* _localctx =
      _tracker.createInstance<VarOrAliasContext>(_ctx, getState());
  enterRule(_localctx, 14, SparqlAutomaticParser::RuleVarOrAlias);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(317);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(315);
        var();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(316);
        alias();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AliasContext
//------------------------------------------------------------------

SparqlAutomaticParser::AliasContext::AliasContext(ParserRuleContext* parent,
                                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::AliasWithoutBracketsContext*
SparqlAutomaticParser::AliasContext::aliasWithoutBrackets() {
  return getRuleContext<SparqlAutomaticParser::AliasWithoutBracketsContext>(0);
}

size_t SparqlAutomaticParser::AliasContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleAlias;
}

void SparqlAutomaticParser::AliasContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAlias(this);
}

void SparqlAutomaticParser::AliasContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAlias(this);
}

antlrcpp::Any SparqlAutomaticParser::AliasContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitAlias(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::AliasContext* SparqlAutomaticParser::alias() {
  AliasContext* _localctx =
      _tracker.createInstance<AliasContext>(_ctx, getState());
  enterRule(_localctx, 16, SparqlAutomaticParser::RuleAlias);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(319);
    match(SparqlAutomaticParser::T__1);
    setState(320);
    aliasWithoutBrackets();
    setState(321);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AliasWithoutBracketsContext
//------------------------------------------------------------------

SparqlAutomaticParser::AliasWithoutBracketsContext::AliasWithoutBracketsContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::AliasWithoutBracketsContext::expression() {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::AliasWithoutBracketsContext::AS() {
  return getToken(SparqlAutomaticParser::AS, 0);
}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::AliasWithoutBracketsContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

size_t SparqlAutomaticParser::AliasWithoutBracketsContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleAliasWithoutBrackets;
}

void SparqlAutomaticParser::AliasWithoutBracketsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterAliasWithoutBrackets(this);
}

void SparqlAutomaticParser::AliasWithoutBracketsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAliasWithoutBrackets(this);
}

antlrcpp::Any SparqlAutomaticParser::AliasWithoutBracketsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitAliasWithoutBrackets(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::AliasWithoutBracketsContext*
SparqlAutomaticParser::aliasWithoutBrackets() {
  AliasWithoutBracketsContext* _localctx =
      _tracker.createInstance<AliasWithoutBracketsContext>(_ctx, getState());
  enterRule(_localctx, 18, SparqlAutomaticParser::RuleAliasWithoutBrackets);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(323);
    expression();
    setState(324);
    match(SparqlAutomaticParser::AS);
    setState(325);
    var();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructQueryContext
//------------------------------------------------------------------

SparqlAutomaticParser::ConstructQueryContext::ConstructQueryContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::ConstructQueryContext::CONSTRUCT() {
  return getToken(SparqlAutomaticParser::CONSTRUCT, 0);
}

SparqlAutomaticParser::ConstructTemplateContext*
SparqlAutomaticParser::ConstructQueryContext::constructTemplate() {
  return getRuleContext<SparqlAutomaticParser::ConstructTemplateContext>(0);
}

SparqlAutomaticParser::WhereClauseContext*
SparqlAutomaticParser::ConstructQueryContext::whereClause() {
  return getRuleContext<SparqlAutomaticParser::WhereClauseContext>(0);
}

SparqlAutomaticParser::SolutionModifierContext*
SparqlAutomaticParser::ConstructQueryContext::solutionModifier() {
  return getRuleContext<SparqlAutomaticParser::SolutionModifierContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::ConstructQueryContext::WHERE() {
  return getToken(SparqlAutomaticParser::WHERE, 0);
}

std::vector<SparqlAutomaticParser::DatasetClauseContext*>
SparqlAutomaticParser::ConstructQueryContext::datasetClause() {
  return getRuleContexts<SparqlAutomaticParser::DatasetClauseContext>();
}

SparqlAutomaticParser::DatasetClauseContext*
SparqlAutomaticParser::ConstructQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DatasetClauseContext>(i);
}

SparqlAutomaticParser::TriplesTemplateContext*
SparqlAutomaticParser::ConstructQueryContext::triplesTemplate() {
  return getRuleContext<SparqlAutomaticParser::TriplesTemplateContext>(0);
}

size_t SparqlAutomaticParser::ConstructQueryContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleConstructQuery;
}

void SparqlAutomaticParser::ConstructQueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterConstructQuery(this);
}

void SparqlAutomaticParser::ConstructQueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitConstructQuery(this);
}

antlrcpp::Any SparqlAutomaticParser::ConstructQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitConstructQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ConstructQueryContext*
SparqlAutomaticParser::constructQuery() {
  ConstructQueryContext* _localctx =
      _tracker.createInstance<ConstructQueryContext>(_ctx, getState());
  enterRule(_localctx, 20, SparqlAutomaticParser::RuleConstructQuery);
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
    setState(327);
    match(SparqlAutomaticParser::CONSTRUCT);
    setState(351);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__3: {
        setState(328);
        constructTemplate();
        setState(332);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::FROM) {
          setState(329);
          datasetClause();
          setState(334);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(335);
        whereClause();
        setState(336);
        solutionModifier();
        break;
      }

      case SparqlAutomaticParser::WHERE:
      case SparqlAutomaticParser::FROM: {
        setState(341);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::FROM) {
          setState(338);
          datasetClause();
          setState(343);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(344);
        match(SparqlAutomaticParser::WHERE);
        setState(345);
        match(SparqlAutomaticParser::T__3);
        setState(347);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                               (1ULL << SparqlAutomaticParser::T__15) |
                               (1ULL << SparqlAutomaticParser::T__28) |
                               (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
               (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
               (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
               (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
               (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
               (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
               (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
               (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
               (1ULL << (SparqlAutomaticParser::NIL - 139)) |
               (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
          setState(346);
          triplesTemplate();
        }
        setState(349);
        match(SparqlAutomaticParser::T__4);
        setState(350);
        solutionModifier();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DescribeQueryContext
//------------------------------------------------------------------

SparqlAutomaticParser::DescribeQueryContext::DescribeQueryContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::DescribeQueryContext::DESCRIBE() {
  return getToken(SparqlAutomaticParser::DESCRIBE, 0);
}

SparqlAutomaticParser::SolutionModifierContext*
SparqlAutomaticParser::DescribeQueryContext::solutionModifier() {
  return getRuleContext<SparqlAutomaticParser::SolutionModifierContext>(0);
}

std::vector<SparqlAutomaticParser::DatasetClauseContext*>
SparqlAutomaticParser::DescribeQueryContext::datasetClause() {
  return getRuleContexts<SparqlAutomaticParser::DatasetClauseContext>();
}

SparqlAutomaticParser::DatasetClauseContext*
SparqlAutomaticParser::DescribeQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DatasetClauseContext>(i);
}

SparqlAutomaticParser::WhereClauseContext*
SparqlAutomaticParser::DescribeQueryContext::whereClause() {
  return getRuleContext<SparqlAutomaticParser::WhereClauseContext>(0);
}

std::vector<SparqlAutomaticParser::VarOrIriContext*>
SparqlAutomaticParser::DescribeQueryContext::varOrIri() {
  return getRuleContexts<SparqlAutomaticParser::VarOrIriContext>();
}

SparqlAutomaticParser::VarOrIriContext*
SparqlAutomaticParser::DescribeQueryContext::varOrIri(size_t i) {
  return getRuleContext<SparqlAutomaticParser::VarOrIriContext>(i);
}

size_t SparqlAutomaticParser::DescribeQueryContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDescribeQuery;
}

void SparqlAutomaticParser::DescribeQueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDescribeQuery(this);
}

void SparqlAutomaticParser::DescribeQueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDescribeQuery(this);
}

antlrcpp::Any SparqlAutomaticParser::DescribeQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDescribeQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DescribeQueryContext*
SparqlAutomaticParser::describeQuery() {
  DescribeQueryContext* _localctx =
      _tracker.createInstance<DescribeQueryContext>(_ctx, getState());
  enterRule(_localctx, 22, SparqlAutomaticParser::RuleDescribeQuery);
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
    setState(353);
    match(SparqlAutomaticParser::DESCRIBE);
    setState(360);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        setState(355);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(354);
          varOrIri();
          setState(357);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
               (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
               (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)))) != 0));
        break;
      }

      case SparqlAutomaticParser::T__0: {
        setState(359);
        match(SparqlAutomaticParser::T__0);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(365);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::FROM) {
      setState(362);
      datasetClause();
      setState(367);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(369);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__3

        || _la == SparqlAutomaticParser::WHERE) {
      setState(368);
      whereClause();
    }
    setState(371);
    solutionModifier();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AskQueryContext
//------------------------------------------------------------------

SparqlAutomaticParser::AskQueryContext::AskQueryContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::AskQueryContext::ASK() {
  return getToken(SparqlAutomaticParser::ASK, 0);
}

SparqlAutomaticParser::WhereClauseContext*
SparqlAutomaticParser::AskQueryContext::whereClause() {
  return getRuleContext<SparqlAutomaticParser::WhereClauseContext>(0);
}

SparqlAutomaticParser::SolutionModifierContext*
SparqlAutomaticParser::AskQueryContext::solutionModifier() {
  return getRuleContext<SparqlAutomaticParser::SolutionModifierContext>(0);
}

std::vector<SparqlAutomaticParser::DatasetClauseContext*>
SparqlAutomaticParser::AskQueryContext::datasetClause() {
  return getRuleContexts<SparqlAutomaticParser::DatasetClauseContext>();
}

SparqlAutomaticParser::DatasetClauseContext*
SparqlAutomaticParser::AskQueryContext::datasetClause(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DatasetClauseContext>(i);
}

size_t SparqlAutomaticParser::AskQueryContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleAskQuery;
}

void SparqlAutomaticParser::AskQueryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAskQuery(this);
}

void SparqlAutomaticParser::AskQueryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAskQuery(this);
}

antlrcpp::Any SparqlAutomaticParser::AskQueryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitAskQuery(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::AskQueryContext* SparqlAutomaticParser::askQuery() {
  AskQueryContext* _localctx =
      _tracker.createInstance<AskQueryContext>(_ctx, getState());
  enterRule(_localctx, 24, SparqlAutomaticParser::RuleAskQuery);
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
    setState(373);
    match(SparqlAutomaticParser::ASK);
    setState(377);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::FROM) {
      setState(374);
      datasetClause();
      setState(379);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(380);
    whereClause();
    setState(381);
    solutionModifier();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DatasetClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::DatasetClauseContext::DatasetClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::DatasetClauseContext::FROM() {
  return getToken(SparqlAutomaticParser::FROM, 0);
}

SparqlAutomaticParser::DefaultGraphClauseContext*
SparqlAutomaticParser::DatasetClauseContext::defaultGraphClause() {
  return getRuleContext<SparqlAutomaticParser::DefaultGraphClauseContext>(0);
}

SparqlAutomaticParser::NamedGraphClauseContext*
SparqlAutomaticParser::DatasetClauseContext::namedGraphClause() {
  return getRuleContext<SparqlAutomaticParser::NamedGraphClauseContext>(0);
}

size_t SparqlAutomaticParser::DatasetClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDatasetClause;
}

void SparqlAutomaticParser::DatasetClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDatasetClause(this);
}

void SparqlAutomaticParser::DatasetClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDatasetClause(this);
}

antlrcpp::Any SparqlAutomaticParser::DatasetClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDatasetClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DatasetClauseContext*
SparqlAutomaticParser::datasetClause() {
  DatasetClauseContext* _localctx =
      _tracker.createInstance<DatasetClauseContext>(_ctx, getState());
  enterRule(_localctx, 26, SparqlAutomaticParser::RuleDatasetClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(383);
    match(SparqlAutomaticParser::FROM);
    setState(386);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        setState(384);
        defaultGraphClause();
        break;
      }

      case SparqlAutomaticParser::NAMED: {
        setState(385);
        namedGraphClause();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DefaultGraphClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::DefaultGraphClauseContext::DefaultGraphClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::SourceSelectorContext*
SparqlAutomaticParser::DefaultGraphClauseContext::sourceSelector() {
  return getRuleContext<SparqlAutomaticParser::SourceSelectorContext>(0);
}

size_t SparqlAutomaticParser::DefaultGraphClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDefaultGraphClause;
}

void SparqlAutomaticParser::DefaultGraphClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDefaultGraphClause(this);
}

void SparqlAutomaticParser::DefaultGraphClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDefaultGraphClause(this);
}

antlrcpp::Any SparqlAutomaticParser::DefaultGraphClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDefaultGraphClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DefaultGraphClauseContext*
SparqlAutomaticParser::defaultGraphClause() {
  DefaultGraphClauseContext* _localctx =
      _tracker.createInstance<DefaultGraphClauseContext>(_ctx, getState());
  enterRule(_localctx, 28, SparqlAutomaticParser::RuleDefaultGraphClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(388);
    sourceSelector();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NamedGraphClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::NamedGraphClauseContext::NamedGraphClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::NamedGraphClauseContext::NAMED() {
  return getToken(SparqlAutomaticParser::NAMED, 0);
}

SparqlAutomaticParser::SourceSelectorContext*
SparqlAutomaticParser::NamedGraphClauseContext::sourceSelector() {
  return getRuleContext<SparqlAutomaticParser::SourceSelectorContext>(0);
}

size_t SparqlAutomaticParser::NamedGraphClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleNamedGraphClause;
}

void SparqlAutomaticParser::NamedGraphClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterNamedGraphClause(this);
}

void SparqlAutomaticParser::NamedGraphClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitNamedGraphClause(this);
}

antlrcpp::Any SparqlAutomaticParser::NamedGraphClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNamedGraphClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NamedGraphClauseContext*
SparqlAutomaticParser::namedGraphClause() {
  NamedGraphClauseContext* _localctx =
      _tracker.createInstance<NamedGraphClauseContext>(_ctx, getState());
  enterRule(_localctx, 30, SparqlAutomaticParser::RuleNamedGraphClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(390);
    match(SparqlAutomaticParser::NAMED);
    setState(391);
    sourceSelector();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SourceSelectorContext
//------------------------------------------------------------------

SparqlAutomaticParser::SourceSelectorContext::SourceSelectorContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::SourceSelectorContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

size_t SparqlAutomaticParser::SourceSelectorContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleSourceSelector;
}

void SparqlAutomaticParser::SourceSelectorContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSourceSelector(this);
}

void SparqlAutomaticParser::SourceSelectorContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSourceSelector(this);
}

antlrcpp::Any SparqlAutomaticParser::SourceSelectorContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitSourceSelector(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::SourceSelectorContext*
SparqlAutomaticParser::sourceSelector() {
  SourceSelectorContext* _localctx =
      _tracker.createInstance<SourceSelectorContext>(_ctx, getState());
  enterRule(_localctx, 32, SparqlAutomaticParser::RuleSourceSelector);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(393);
    iri();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WhereClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::WhereClauseContext::WhereClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::WhereClauseContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::WhereClauseContext::WHERE() {
  return getToken(SparqlAutomaticParser::WHERE, 0);
}

size_t SparqlAutomaticParser::WhereClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleWhereClause;
}

void SparqlAutomaticParser::WhereClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterWhereClause(this);
}

void SparqlAutomaticParser::WhereClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitWhereClause(this);
}

antlrcpp::Any SparqlAutomaticParser::WhereClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitWhereClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::WhereClauseContext*
SparqlAutomaticParser::whereClause() {
  WhereClauseContext* _localctx =
      _tracker.createInstance<WhereClauseContext>(_ctx, getState());
  enterRule(_localctx, 34, SparqlAutomaticParser::RuleWhereClause);
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
    setState(396);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::WHERE) {
      setState(395);
      match(SparqlAutomaticParser::WHERE);
    }
    setState(398);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SolutionModifierContext
//------------------------------------------------------------------

SparqlAutomaticParser::SolutionModifierContext::SolutionModifierContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::GroupClauseContext*
SparqlAutomaticParser::SolutionModifierContext::groupClause() {
  return getRuleContext<SparqlAutomaticParser::GroupClauseContext>(0);
}

SparqlAutomaticParser::HavingClauseContext*
SparqlAutomaticParser::SolutionModifierContext::havingClause() {
  return getRuleContext<SparqlAutomaticParser::HavingClauseContext>(0);
}

SparqlAutomaticParser::OrderClauseContext*
SparqlAutomaticParser::SolutionModifierContext::orderClause() {
  return getRuleContext<SparqlAutomaticParser::OrderClauseContext>(0);
}

SparqlAutomaticParser::LimitOffsetClausesContext*
SparqlAutomaticParser::SolutionModifierContext::limitOffsetClauses() {
  return getRuleContext<SparqlAutomaticParser::LimitOffsetClausesContext>(0);
}

size_t SparqlAutomaticParser::SolutionModifierContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleSolutionModifier;
}

void SparqlAutomaticParser::SolutionModifierContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSolutionModifier(this);
}

void SparqlAutomaticParser::SolutionModifierContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSolutionModifier(this);
}

antlrcpp::Any SparqlAutomaticParser::SolutionModifierContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitSolutionModifier(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::SolutionModifierContext*
SparqlAutomaticParser::solutionModifier() {
  SolutionModifierContext* _localctx =
      _tracker.createInstance<SolutionModifierContext>(_ctx, getState());
  enterRule(_localctx, 36, SparqlAutomaticParser::RuleSolutionModifier);
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
    setState(401);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::GROUPBY) {
      setState(400);
      groupClause();
    }
    setState(404);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::HAVING) {
      setState(403);
      havingClause();
    }
    setState(407);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::ORDERBY) {
      setState(406);
      orderClause();
    }
    setState(410);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::LIMIT) |
                           (1ULL << SparqlAutomaticParser::OFFSET) |
                           (1ULL << SparqlAutomaticParser::TEXTLIMIT))) != 0)) {
      setState(409);
      limitOffsetClauses();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::GroupClauseContext::GroupClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::GroupClauseContext::GROUPBY() {
  return getToken(SparqlAutomaticParser::GROUPBY, 0);
}

std::vector<SparqlAutomaticParser::GroupConditionContext*>
SparqlAutomaticParser::GroupClauseContext::groupCondition() {
  return getRuleContexts<SparqlAutomaticParser::GroupConditionContext>();
}

SparqlAutomaticParser::GroupConditionContext*
SparqlAutomaticParser::GroupClauseContext::groupCondition(size_t i) {
  return getRuleContext<SparqlAutomaticParser::GroupConditionContext>(i);
}

size_t SparqlAutomaticParser::GroupClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGroupClause;
}

void SparqlAutomaticParser::GroupClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGroupClause(this);
}

void SparqlAutomaticParser::GroupClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGroupClause(this);
}

antlrcpp::Any SparqlAutomaticParser::GroupClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGroupClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GroupClauseContext*
SparqlAutomaticParser::groupClause() {
  GroupClauseContext* _localctx =
      _tracker.createInstance<GroupClauseContext>(_ctx, getState());
  enterRule(_localctx, 38, SparqlAutomaticParser::RuleGroupClause);
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
    setState(412);
    match(SparqlAutomaticParser::GROUPBY);
    setState(414);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(413);
      groupCondition();
      setState(416);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        _la == SparqlAutomaticParser::T__1

        || _la == SparqlAutomaticParser::GROUP_CONCAT ||
        ((((_la - 76) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 76)) &
          ((1ULL << (SparqlAutomaticParser::NOT - 76)) |
           (1ULL << (SparqlAutomaticParser::STR - 76)) |
           (1ULL << (SparqlAutomaticParser::LANG - 76)) |
           (1ULL << (SparqlAutomaticParser::LANGMATCHES - 76)) |
           (1ULL << (SparqlAutomaticParser::DATATYPE - 76)) |
           (1ULL << (SparqlAutomaticParser::BOUND - 76)) |
           (1ULL << (SparqlAutomaticParser::IRI - 76)) |
           (1ULL << (SparqlAutomaticParser::URI - 76)) |
           (1ULL << (SparqlAutomaticParser::BNODE - 76)) |
           (1ULL << (SparqlAutomaticParser::RAND - 76)) |
           (1ULL << (SparqlAutomaticParser::ABS - 76)) |
           (1ULL << (SparqlAutomaticParser::CEIL - 76)) |
           (1ULL << (SparqlAutomaticParser::FLOOR - 76)) |
           (1ULL << (SparqlAutomaticParser::ROUND - 76)) |
           (1ULL << (SparqlAutomaticParser::CONCAT - 76)) |
           (1ULL << (SparqlAutomaticParser::STRLEN - 76)) |
           (1ULL << (SparqlAutomaticParser::UCASE - 76)) |
           (1ULL << (SparqlAutomaticParser::LCASE - 76)) |
           (1ULL << (SparqlAutomaticParser::ENCODE - 76)) |
           (1ULL << (SparqlAutomaticParser::CONTAINS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRSTARTS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRENDS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRBEFORE - 76)) |
           (1ULL << (SparqlAutomaticParser::STRAFTER - 76)) |
           (1ULL << (SparqlAutomaticParser::YEAR - 76)) |
           (1ULL << (SparqlAutomaticParser::MONTH - 76)) |
           (1ULL << (SparqlAutomaticParser::DAY - 76)) |
           (1ULL << (SparqlAutomaticParser::HOURS - 76)) |
           (1ULL << (SparqlAutomaticParser::MINUTES - 76)) |
           (1ULL << (SparqlAutomaticParser::SECONDS - 76)) |
           (1ULL << (SparqlAutomaticParser::TIMEZONE - 76)) |
           (1ULL << (SparqlAutomaticParser::TZ - 76)) |
           (1ULL << (SparqlAutomaticParser::NOW - 76)) |
           (1ULL << (SparqlAutomaticParser::UUID - 76)) |
           (1ULL << (SparqlAutomaticParser::STRUUID - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA1 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA256 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA384 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA512 - 76)) |
           (1ULL << (SparqlAutomaticParser::MD5 - 76)) |
           (1ULL << (SparqlAutomaticParser::COALESCE - 76)) |
           (1ULL << (SparqlAutomaticParser::IF - 76)) |
           (1ULL << (SparqlAutomaticParser::STRLANG - 76)) |
           (1ULL << (SparqlAutomaticParser::STRDT - 76)) |
           (1ULL << (SparqlAutomaticParser::SAMETERM - 76)) |
           (1ULL << (SparqlAutomaticParser::ISIRI - 76)) |
           (1ULL << (SparqlAutomaticParser::ISURI - 76)) |
           (1ULL << (SparqlAutomaticParser::ISBLANK - 76)) |
           (1ULL << (SparqlAutomaticParser::ISLITERAL - 76)) |
           (1ULL << (SparqlAutomaticParser::ISNUMERIC - 76)) |
           (1ULL << (SparqlAutomaticParser::REGEX - 76)) |
           (1ULL << (SparqlAutomaticParser::SUBSTR - 76)) |
           (1ULL << (SparqlAutomaticParser::REPLACE - 76)) |
           (1ULL << (SparqlAutomaticParser::EXISTS - 76)) |
           (1ULL << (SparqlAutomaticParser::COUNT - 76)) |
           (1ULL << (SparqlAutomaticParser::SUM - 76)) |
           (1ULL << (SparqlAutomaticParser::MIN - 76)) |
           (1ULL << (SparqlAutomaticParser::MAX - 76)) |
           (1ULL << (SparqlAutomaticParser::AVG - 76)) |
           (1ULL << (SparqlAutomaticParser::SAMPLE - 76)) |
           (1ULL << (SparqlAutomaticParser::IRI_REF - 76)))) != 0) ||
        ((((_la - 140) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 140)) &
          ((1ULL << (SparqlAutomaticParser::PNAME_NS - 140)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 140)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 140)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 140)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 140)))) != 0));

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupConditionContext
//------------------------------------------------------------------

SparqlAutomaticParser::GroupConditionContext::GroupConditionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::BuiltInCallContext*
SparqlAutomaticParser::GroupConditionContext::builtInCall() {
  return getRuleContext<SparqlAutomaticParser::BuiltInCallContext>(0);
}

SparqlAutomaticParser::FunctionCallContext*
SparqlAutomaticParser::GroupConditionContext::functionCall() {
  return getRuleContext<SparqlAutomaticParser::FunctionCallContext>(0);
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::GroupConditionContext::expression() {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::GroupConditionContext::AS() {
  return getToken(SparqlAutomaticParser::AS, 0);
}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::GroupConditionContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

size_t SparqlAutomaticParser::GroupConditionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGroupCondition;
}

void SparqlAutomaticParser::GroupConditionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGroupCondition(this);
}

void SparqlAutomaticParser::GroupConditionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGroupCondition(this);
}

antlrcpp::Any SparqlAutomaticParser::GroupConditionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGroupCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GroupConditionContext*
SparqlAutomaticParser::groupCondition() {
  GroupConditionContext* _localctx =
      _tracker.createInstance<GroupConditionContext>(_ctx, getState());
  enterRule(_localctx, 40, SparqlAutomaticParser::RuleGroupCondition);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(429);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::GROUP_CONCAT:
      case SparqlAutomaticParser::NOT:
      case SparqlAutomaticParser::STR:
      case SparqlAutomaticParser::LANG:
      case SparqlAutomaticParser::LANGMATCHES:
      case SparqlAutomaticParser::DATATYPE:
      case SparqlAutomaticParser::BOUND:
      case SparqlAutomaticParser::IRI:
      case SparqlAutomaticParser::URI:
      case SparqlAutomaticParser::BNODE:
      case SparqlAutomaticParser::RAND:
      case SparqlAutomaticParser::ABS:
      case SparqlAutomaticParser::CEIL:
      case SparqlAutomaticParser::FLOOR:
      case SparqlAutomaticParser::ROUND:
      case SparqlAutomaticParser::CONCAT:
      case SparqlAutomaticParser::STRLEN:
      case SparqlAutomaticParser::UCASE:
      case SparqlAutomaticParser::LCASE:
      case SparqlAutomaticParser::ENCODE:
      case SparqlAutomaticParser::CONTAINS:
      case SparqlAutomaticParser::STRSTARTS:
      case SparqlAutomaticParser::STRENDS:
      case SparqlAutomaticParser::STRBEFORE:
      case SparqlAutomaticParser::STRAFTER:
      case SparqlAutomaticParser::YEAR:
      case SparqlAutomaticParser::MONTH:
      case SparqlAutomaticParser::DAY:
      case SparqlAutomaticParser::HOURS:
      case SparqlAutomaticParser::MINUTES:
      case SparqlAutomaticParser::SECONDS:
      case SparqlAutomaticParser::TIMEZONE:
      case SparqlAutomaticParser::TZ:
      case SparqlAutomaticParser::NOW:
      case SparqlAutomaticParser::UUID:
      case SparqlAutomaticParser::STRUUID:
      case SparqlAutomaticParser::SHA1:
      case SparqlAutomaticParser::SHA256:
      case SparqlAutomaticParser::SHA384:
      case SparqlAutomaticParser::SHA512:
      case SparqlAutomaticParser::MD5:
      case SparqlAutomaticParser::COALESCE:
      case SparqlAutomaticParser::IF:
      case SparqlAutomaticParser::STRLANG:
      case SparqlAutomaticParser::STRDT:
      case SparqlAutomaticParser::SAMETERM:
      case SparqlAutomaticParser::ISIRI:
      case SparqlAutomaticParser::ISURI:
      case SparqlAutomaticParser::ISBLANK:
      case SparqlAutomaticParser::ISLITERAL:
      case SparqlAutomaticParser::ISNUMERIC:
      case SparqlAutomaticParser::REGEX:
      case SparqlAutomaticParser::SUBSTR:
      case SparqlAutomaticParser::REPLACE:
      case SparqlAutomaticParser::EXISTS:
      case SparqlAutomaticParser::COUNT:
      case SparqlAutomaticParser::SUM:
      case SparqlAutomaticParser::MIN:
      case SparqlAutomaticParser::MAX:
      case SparqlAutomaticParser::AVG:
      case SparqlAutomaticParser::SAMPLE: {
        enterOuterAlt(_localctx, 1);
        setState(418);
        builtInCall();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 2);
        setState(419);
        functionCall();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 3);
        setState(420);
        match(SparqlAutomaticParser::T__1);
        setState(421);
        expression();
        setState(424);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::AS) {
          setState(422);
          match(SparqlAutomaticParser::AS);
          setState(423);
          var();
        }
        setState(426);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 4);
        setState(428);
        var();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HavingClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::HavingClauseContext::HavingClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::HavingClauseContext::HAVING() {
  return getToken(SparqlAutomaticParser::HAVING, 0);
}

std::vector<SparqlAutomaticParser::HavingConditionContext*>
SparqlAutomaticParser::HavingClauseContext::havingCondition() {
  return getRuleContexts<SparqlAutomaticParser::HavingConditionContext>();
}

SparqlAutomaticParser::HavingConditionContext*
SparqlAutomaticParser::HavingClauseContext::havingCondition(size_t i) {
  return getRuleContext<SparqlAutomaticParser::HavingConditionContext>(i);
}

size_t SparqlAutomaticParser::HavingClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleHavingClause;
}

void SparqlAutomaticParser::HavingClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterHavingClause(this);
}

void SparqlAutomaticParser::HavingClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitHavingClause(this);
}

antlrcpp::Any SparqlAutomaticParser::HavingClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitHavingClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::HavingClauseContext*
SparqlAutomaticParser::havingClause() {
  HavingClauseContext* _localctx =
      _tracker.createInstance<HavingClauseContext>(_ctx, getState());
  enterRule(_localctx, 42, SparqlAutomaticParser::RuleHavingClause);
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
    setState(431);
    match(SparqlAutomaticParser::HAVING);
    setState(433);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(432);
      havingCondition();
      setState(435);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        _la == SparqlAutomaticParser::T__1

        || _la == SparqlAutomaticParser::GROUP_CONCAT ||
        ((((_la - 76) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 76)) &
          ((1ULL << (SparqlAutomaticParser::NOT - 76)) |
           (1ULL << (SparqlAutomaticParser::STR - 76)) |
           (1ULL << (SparqlAutomaticParser::LANG - 76)) |
           (1ULL << (SparqlAutomaticParser::LANGMATCHES - 76)) |
           (1ULL << (SparqlAutomaticParser::DATATYPE - 76)) |
           (1ULL << (SparqlAutomaticParser::BOUND - 76)) |
           (1ULL << (SparqlAutomaticParser::IRI - 76)) |
           (1ULL << (SparqlAutomaticParser::URI - 76)) |
           (1ULL << (SparqlAutomaticParser::BNODE - 76)) |
           (1ULL << (SparqlAutomaticParser::RAND - 76)) |
           (1ULL << (SparqlAutomaticParser::ABS - 76)) |
           (1ULL << (SparqlAutomaticParser::CEIL - 76)) |
           (1ULL << (SparqlAutomaticParser::FLOOR - 76)) |
           (1ULL << (SparqlAutomaticParser::ROUND - 76)) |
           (1ULL << (SparqlAutomaticParser::CONCAT - 76)) |
           (1ULL << (SparqlAutomaticParser::STRLEN - 76)) |
           (1ULL << (SparqlAutomaticParser::UCASE - 76)) |
           (1ULL << (SparqlAutomaticParser::LCASE - 76)) |
           (1ULL << (SparqlAutomaticParser::ENCODE - 76)) |
           (1ULL << (SparqlAutomaticParser::CONTAINS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRSTARTS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRENDS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRBEFORE - 76)) |
           (1ULL << (SparqlAutomaticParser::STRAFTER - 76)) |
           (1ULL << (SparqlAutomaticParser::YEAR - 76)) |
           (1ULL << (SparqlAutomaticParser::MONTH - 76)) |
           (1ULL << (SparqlAutomaticParser::DAY - 76)) |
           (1ULL << (SparqlAutomaticParser::HOURS - 76)) |
           (1ULL << (SparqlAutomaticParser::MINUTES - 76)) |
           (1ULL << (SparqlAutomaticParser::SECONDS - 76)) |
           (1ULL << (SparqlAutomaticParser::TIMEZONE - 76)) |
           (1ULL << (SparqlAutomaticParser::TZ - 76)) |
           (1ULL << (SparqlAutomaticParser::NOW - 76)) |
           (1ULL << (SparqlAutomaticParser::UUID - 76)) |
           (1ULL << (SparqlAutomaticParser::STRUUID - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA1 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA256 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA384 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA512 - 76)) |
           (1ULL << (SparqlAutomaticParser::MD5 - 76)) |
           (1ULL << (SparqlAutomaticParser::COALESCE - 76)) |
           (1ULL << (SparqlAutomaticParser::IF - 76)) |
           (1ULL << (SparqlAutomaticParser::STRLANG - 76)) |
           (1ULL << (SparqlAutomaticParser::STRDT - 76)) |
           (1ULL << (SparqlAutomaticParser::SAMETERM - 76)) |
           (1ULL << (SparqlAutomaticParser::ISIRI - 76)) |
           (1ULL << (SparqlAutomaticParser::ISURI - 76)) |
           (1ULL << (SparqlAutomaticParser::ISBLANK - 76)) |
           (1ULL << (SparqlAutomaticParser::ISLITERAL - 76)) |
           (1ULL << (SparqlAutomaticParser::ISNUMERIC - 76)) |
           (1ULL << (SparqlAutomaticParser::REGEX - 76)) |
           (1ULL << (SparqlAutomaticParser::SUBSTR - 76)) |
           (1ULL << (SparqlAutomaticParser::REPLACE - 76)) |
           (1ULL << (SparqlAutomaticParser::EXISTS - 76)) |
           (1ULL << (SparqlAutomaticParser::COUNT - 76)) |
           (1ULL << (SparqlAutomaticParser::SUM - 76)) |
           (1ULL << (SparqlAutomaticParser::MIN - 76)) |
           (1ULL << (SparqlAutomaticParser::MAX - 76)) |
           (1ULL << (SparqlAutomaticParser::AVG - 76)) |
           (1ULL << (SparqlAutomaticParser::SAMPLE - 76)) |
           (1ULL << (SparqlAutomaticParser::IRI_REF - 76)))) != 0) ||
        ((((_la - 140) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 140)) &
          ((1ULL << (SparqlAutomaticParser::PNAME_NS - 140)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 140)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 140)))) != 0));

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- HavingConditionContext
//------------------------------------------------------------------

SparqlAutomaticParser::HavingConditionContext::HavingConditionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::ConstraintContext*
SparqlAutomaticParser::HavingConditionContext::constraint() {
  return getRuleContext<SparqlAutomaticParser::ConstraintContext>(0);
}

size_t SparqlAutomaticParser::HavingConditionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleHavingCondition;
}

void SparqlAutomaticParser::HavingConditionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterHavingCondition(this);
}

void SparqlAutomaticParser::HavingConditionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitHavingCondition(this);
}

antlrcpp::Any SparqlAutomaticParser::HavingConditionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitHavingCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::HavingConditionContext*
SparqlAutomaticParser::havingCondition() {
  HavingConditionContext* _localctx =
      _tracker.createInstance<HavingConditionContext>(_ctx, getState());
  enterRule(_localctx, 44, SparqlAutomaticParser::RuleHavingCondition);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(437);
    constraint();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::OrderClauseContext::OrderClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::OrderClauseContext::ORDERBY() {
  return getToken(SparqlAutomaticParser::ORDERBY, 0);
}

std::vector<SparqlAutomaticParser::OrderConditionContext*>
SparqlAutomaticParser::OrderClauseContext::orderCondition() {
  return getRuleContexts<SparqlAutomaticParser::OrderConditionContext>();
}

SparqlAutomaticParser::OrderConditionContext*
SparqlAutomaticParser::OrderClauseContext::orderCondition(size_t i) {
  return getRuleContext<SparqlAutomaticParser::OrderConditionContext>(i);
}

size_t SparqlAutomaticParser::OrderClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleOrderClause;
}

void SparqlAutomaticParser::OrderClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterOrderClause(this);
}

void SparqlAutomaticParser::OrderClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitOrderClause(this);
}

antlrcpp::Any SparqlAutomaticParser::OrderClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitOrderClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::OrderClauseContext*
SparqlAutomaticParser::orderClause() {
  OrderClauseContext* _localctx =
      _tracker.createInstance<OrderClauseContext>(_ctx, getState());
  enterRule(_localctx, 46, SparqlAutomaticParser::RuleOrderClause);
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
    setState(439);
    match(SparqlAutomaticParser::ORDERBY);
    setState(441);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(440);
      orderCondition();
      setState(443);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        (((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::GROUP_CONCAT) |
                           (1ULL << SparqlAutomaticParser::ASC) |
                           (1ULL << SparqlAutomaticParser::DESC))) != 0) ||
        ((((_la - 76) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 76)) &
          ((1ULL << (SparqlAutomaticParser::NOT - 76)) |
           (1ULL << (SparqlAutomaticParser::STR - 76)) |
           (1ULL << (SparqlAutomaticParser::LANG - 76)) |
           (1ULL << (SparqlAutomaticParser::LANGMATCHES - 76)) |
           (1ULL << (SparqlAutomaticParser::DATATYPE - 76)) |
           (1ULL << (SparqlAutomaticParser::BOUND - 76)) |
           (1ULL << (SparqlAutomaticParser::IRI - 76)) |
           (1ULL << (SparqlAutomaticParser::URI - 76)) |
           (1ULL << (SparqlAutomaticParser::BNODE - 76)) |
           (1ULL << (SparqlAutomaticParser::RAND - 76)) |
           (1ULL << (SparqlAutomaticParser::ABS - 76)) |
           (1ULL << (SparqlAutomaticParser::CEIL - 76)) |
           (1ULL << (SparqlAutomaticParser::FLOOR - 76)) |
           (1ULL << (SparqlAutomaticParser::ROUND - 76)) |
           (1ULL << (SparqlAutomaticParser::CONCAT - 76)) |
           (1ULL << (SparqlAutomaticParser::STRLEN - 76)) |
           (1ULL << (SparqlAutomaticParser::UCASE - 76)) |
           (1ULL << (SparqlAutomaticParser::LCASE - 76)) |
           (1ULL << (SparqlAutomaticParser::ENCODE - 76)) |
           (1ULL << (SparqlAutomaticParser::CONTAINS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRSTARTS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRENDS - 76)) |
           (1ULL << (SparqlAutomaticParser::STRBEFORE - 76)) |
           (1ULL << (SparqlAutomaticParser::STRAFTER - 76)) |
           (1ULL << (SparqlAutomaticParser::YEAR - 76)) |
           (1ULL << (SparqlAutomaticParser::MONTH - 76)) |
           (1ULL << (SparqlAutomaticParser::DAY - 76)) |
           (1ULL << (SparqlAutomaticParser::HOURS - 76)) |
           (1ULL << (SparqlAutomaticParser::MINUTES - 76)) |
           (1ULL << (SparqlAutomaticParser::SECONDS - 76)) |
           (1ULL << (SparqlAutomaticParser::TIMEZONE - 76)) |
           (1ULL << (SparqlAutomaticParser::TZ - 76)) |
           (1ULL << (SparqlAutomaticParser::NOW - 76)) |
           (1ULL << (SparqlAutomaticParser::UUID - 76)) |
           (1ULL << (SparqlAutomaticParser::STRUUID - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA1 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA256 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA384 - 76)) |
           (1ULL << (SparqlAutomaticParser::SHA512 - 76)) |
           (1ULL << (SparqlAutomaticParser::MD5 - 76)) |
           (1ULL << (SparqlAutomaticParser::COALESCE - 76)) |
           (1ULL << (SparqlAutomaticParser::IF - 76)) |
           (1ULL << (SparqlAutomaticParser::STRLANG - 76)) |
           (1ULL << (SparqlAutomaticParser::STRDT - 76)) |
           (1ULL << (SparqlAutomaticParser::SAMETERM - 76)) |
           (1ULL << (SparqlAutomaticParser::ISIRI - 76)) |
           (1ULL << (SparqlAutomaticParser::ISURI - 76)) |
           (1ULL << (SparqlAutomaticParser::ISBLANK - 76)) |
           (1ULL << (SparqlAutomaticParser::ISLITERAL - 76)) |
           (1ULL << (SparqlAutomaticParser::ISNUMERIC - 76)) |
           (1ULL << (SparqlAutomaticParser::REGEX - 76)) |
           (1ULL << (SparqlAutomaticParser::SUBSTR - 76)) |
           (1ULL << (SparqlAutomaticParser::REPLACE - 76)) |
           (1ULL << (SparqlAutomaticParser::EXISTS - 76)) |
           (1ULL << (SparqlAutomaticParser::COUNT - 76)) |
           (1ULL << (SparqlAutomaticParser::SUM - 76)) |
           (1ULL << (SparqlAutomaticParser::MIN - 76)) |
           (1ULL << (SparqlAutomaticParser::MAX - 76)) |
           (1ULL << (SparqlAutomaticParser::AVG - 76)) |
           (1ULL << (SparqlAutomaticParser::SAMPLE - 76)) |
           (1ULL << (SparqlAutomaticParser::IRI_REF - 76)))) != 0) ||
        ((((_la - 140) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 140)) &
          ((1ULL << (SparqlAutomaticParser::PNAME_NS - 140)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 140)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 140)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 140)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 140)))) != 0));

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OrderConditionContext
//------------------------------------------------------------------

SparqlAutomaticParser::OrderConditionContext::OrderConditionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::BrackettedExpressionContext*
SparqlAutomaticParser::OrderConditionContext::brackettedExpression() {
  return getRuleContext<SparqlAutomaticParser::BrackettedExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::OrderConditionContext::ASC() {
  return getToken(SparqlAutomaticParser::ASC, 0);
}

tree::TerminalNode* SparqlAutomaticParser::OrderConditionContext::DESC() {
  return getToken(SparqlAutomaticParser::DESC, 0);
}

SparqlAutomaticParser::ConstraintContext*
SparqlAutomaticParser::OrderConditionContext::constraint() {
  return getRuleContext<SparqlAutomaticParser::ConstraintContext>(0);
}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::OrderConditionContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

size_t SparqlAutomaticParser::OrderConditionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleOrderCondition;
}

void SparqlAutomaticParser::OrderConditionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterOrderCondition(this);
}

void SparqlAutomaticParser::OrderConditionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitOrderCondition(this);
}

antlrcpp::Any SparqlAutomaticParser::OrderConditionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitOrderCondition(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::OrderConditionContext*
SparqlAutomaticParser::orderCondition() {
  OrderConditionContext* _localctx =
      _tracker.createInstance<OrderConditionContext>(_ctx, getState());
  enterRule(_localctx, 48, SparqlAutomaticParser::RuleOrderCondition);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(451);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::ASC:
      case SparqlAutomaticParser::DESC: {
        enterOuterAlt(_localctx, 1);
        setState(445);
        _la = _input->LA(1);
        if (!(_la == SparqlAutomaticParser::ASC

              || _la == SparqlAutomaticParser::DESC)) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(446);
        brackettedExpression();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::GROUP_CONCAT:
      case SparqlAutomaticParser::NOT:
      case SparqlAutomaticParser::STR:
      case SparqlAutomaticParser::LANG:
      case SparqlAutomaticParser::LANGMATCHES:
      case SparqlAutomaticParser::DATATYPE:
      case SparqlAutomaticParser::BOUND:
      case SparqlAutomaticParser::IRI:
      case SparqlAutomaticParser::URI:
      case SparqlAutomaticParser::BNODE:
      case SparqlAutomaticParser::RAND:
      case SparqlAutomaticParser::ABS:
      case SparqlAutomaticParser::CEIL:
      case SparqlAutomaticParser::FLOOR:
      case SparqlAutomaticParser::ROUND:
      case SparqlAutomaticParser::CONCAT:
      case SparqlAutomaticParser::STRLEN:
      case SparqlAutomaticParser::UCASE:
      case SparqlAutomaticParser::LCASE:
      case SparqlAutomaticParser::ENCODE:
      case SparqlAutomaticParser::CONTAINS:
      case SparqlAutomaticParser::STRSTARTS:
      case SparqlAutomaticParser::STRENDS:
      case SparqlAutomaticParser::STRBEFORE:
      case SparqlAutomaticParser::STRAFTER:
      case SparqlAutomaticParser::YEAR:
      case SparqlAutomaticParser::MONTH:
      case SparqlAutomaticParser::DAY:
      case SparqlAutomaticParser::HOURS:
      case SparqlAutomaticParser::MINUTES:
      case SparqlAutomaticParser::SECONDS:
      case SparqlAutomaticParser::TIMEZONE:
      case SparqlAutomaticParser::TZ:
      case SparqlAutomaticParser::NOW:
      case SparqlAutomaticParser::UUID:
      case SparqlAutomaticParser::STRUUID:
      case SparqlAutomaticParser::SHA1:
      case SparqlAutomaticParser::SHA256:
      case SparqlAutomaticParser::SHA384:
      case SparqlAutomaticParser::SHA512:
      case SparqlAutomaticParser::MD5:
      case SparqlAutomaticParser::COALESCE:
      case SparqlAutomaticParser::IF:
      case SparqlAutomaticParser::STRLANG:
      case SparqlAutomaticParser::STRDT:
      case SparqlAutomaticParser::SAMETERM:
      case SparqlAutomaticParser::ISIRI:
      case SparqlAutomaticParser::ISURI:
      case SparqlAutomaticParser::ISBLANK:
      case SparqlAutomaticParser::ISLITERAL:
      case SparqlAutomaticParser::ISNUMERIC:
      case SparqlAutomaticParser::REGEX:
      case SparqlAutomaticParser::SUBSTR:
      case SparqlAutomaticParser::REPLACE:
      case SparqlAutomaticParser::EXISTS:
      case SparqlAutomaticParser::COUNT:
      case SparqlAutomaticParser::SUM:
      case SparqlAutomaticParser::MIN:
      case SparqlAutomaticParser::MAX:
      case SparqlAutomaticParser::AVG:
      case SparqlAutomaticParser::SAMPLE:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 2);
        setState(449);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::T__1:
          case SparqlAutomaticParser::GROUP_CONCAT:
          case SparqlAutomaticParser::NOT:
          case SparqlAutomaticParser::STR:
          case SparqlAutomaticParser::LANG:
          case SparqlAutomaticParser::LANGMATCHES:
          case SparqlAutomaticParser::DATATYPE:
          case SparqlAutomaticParser::BOUND:
          case SparqlAutomaticParser::IRI:
          case SparqlAutomaticParser::URI:
          case SparqlAutomaticParser::BNODE:
          case SparqlAutomaticParser::RAND:
          case SparqlAutomaticParser::ABS:
          case SparqlAutomaticParser::CEIL:
          case SparqlAutomaticParser::FLOOR:
          case SparqlAutomaticParser::ROUND:
          case SparqlAutomaticParser::CONCAT:
          case SparqlAutomaticParser::STRLEN:
          case SparqlAutomaticParser::UCASE:
          case SparqlAutomaticParser::LCASE:
          case SparqlAutomaticParser::ENCODE:
          case SparqlAutomaticParser::CONTAINS:
          case SparqlAutomaticParser::STRSTARTS:
          case SparqlAutomaticParser::STRENDS:
          case SparqlAutomaticParser::STRBEFORE:
          case SparqlAutomaticParser::STRAFTER:
          case SparqlAutomaticParser::YEAR:
          case SparqlAutomaticParser::MONTH:
          case SparqlAutomaticParser::DAY:
          case SparqlAutomaticParser::HOURS:
          case SparqlAutomaticParser::MINUTES:
          case SparqlAutomaticParser::SECONDS:
          case SparqlAutomaticParser::TIMEZONE:
          case SparqlAutomaticParser::TZ:
          case SparqlAutomaticParser::NOW:
          case SparqlAutomaticParser::UUID:
          case SparqlAutomaticParser::STRUUID:
          case SparqlAutomaticParser::SHA1:
          case SparqlAutomaticParser::SHA256:
          case SparqlAutomaticParser::SHA384:
          case SparqlAutomaticParser::SHA512:
          case SparqlAutomaticParser::MD5:
          case SparqlAutomaticParser::COALESCE:
          case SparqlAutomaticParser::IF:
          case SparqlAutomaticParser::STRLANG:
          case SparqlAutomaticParser::STRDT:
          case SparqlAutomaticParser::SAMETERM:
          case SparqlAutomaticParser::ISIRI:
          case SparqlAutomaticParser::ISURI:
          case SparqlAutomaticParser::ISBLANK:
          case SparqlAutomaticParser::ISLITERAL:
          case SparqlAutomaticParser::ISNUMERIC:
          case SparqlAutomaticParser::REGEX:
          case SparqlAutomaticParser::SUBSTR:
          case SparqlAutomaticParser::REPLACE:
          case SparqlAutomaticParser::EXISTS:
          case SparqlAutomaticParser::COUNT:
          case SparqlAutomaticParser::SUM:
          case SparqlAutomaticParser::MIN:
          case SparqlAutomaticParser::MAX:
          case SparqlAutomaticParser::AVG:
          case SparqlAutomaticParser::SAMPLE:
          case SparqlAutomaticParser::IRI_REF:
          case SparqlAutomaticParser::PNAME_NS:
          case SparqlAutomaticParser::PNAME_LN:
          case SparqlAutomaticParser::PREFIX_LANGTAG: {
            setState(447);
            constraint();
            break;
          }

          case SparqlAutomaticParser::VAR1:
          case SparqlAutomaticParser::VAR2: {
            setState(448);
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

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitOffsetClausesContext
//------------------------------------------------------------------

SparqlAutomaticParser::LimitOffsetClausesContext::LimitOffsetClausesContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::LimitClauseContext*
SparqlAutomaticParser::LimitOffsetClausesContext::limitClause() {
  return getRuleContext<SparqlAutomaticParser::LimitClauseContext>(0);
}

SparqlAutomaticParser::OffsetClauseContext*
SparqlAutomaticParser::LimitOffsetClausesContext::offsetClause() {
  return getRuleContext<SparqlAutomaticParser::OffsetClauseContext>(0);
}

SparqlAutomaticParser::TextLimitClauseContext*
SparqlAutomaticParser::LimitOffsetClausesContext::textLimitClause() {
  return getRuleContext<SparqlAutomaticParser::TextLimitClauseContext>(0);
}

size_t SparqlAutomaticParser::LimitOffsetClausesContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleLimitOffsetClauses;
}

void SparqlAutomaticParser::LimitOffsetClausesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterLimitOffsetClauses(this);
}

void SparqlAutomaticParser::LimitOffsetClausesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitLimitOffsetClauses(this);
}

antlrcpp::Any SparqlAutomaticParser::LimitOffsetClausesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitLimitOffsetClauses(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::LimitOffsetClausesContext*
SparqlAutomaticParser::limitOffsetClauses() {
  LimitOffsetClausesContext* _localctx =
      _tracker.createInstance<LimitOffsetClausesContext>(_ctx, getState());
  enterRule(_localctx, 50, SparqlAutomaticParser::RuleLimitOffsetClauses);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(495);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 42, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(453);
        limitClause();
        setState(455);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(454);
          offsetClause();
        }
        setState(458);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(457);
          textLimitClause();
        }
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(460);
        limitClause();
        setState(462);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(461);
          textLimitClause();
        }
        setState(465);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(464);
          offsetClause();
        }
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(467);
        offsetClause();
        setState(469);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(468);
          limitClause();
        }
        setState(472);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(471);
          textLimitClause();
        }
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(474);
        offsetClause();
        setState(476);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(475);
          textLimitClause();
        }
        setState(479);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(478);
          limitClause();
        }
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(481);
        textLimitClause();
        setState(483);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(482);
          offsetClause();
        }
        setState(486);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(485);
          limitClause();
        }
        break;
      }

      case 6: {
        enterOuterAlt(_localctx, 6);
        setState(488);
        textLimitClause();
        setState(490);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(489);
          limitClause();
        }
        setState(493);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(492);
          offsetClause();
        }
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- LimitClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::LimitClauseContext::LimitClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::LimitClauseContext::LIMIT() {
  return getToken(SparqlAutomaticParser::LIMIT, 0);
}

SparqlAutomaticParser::IntegerContext*
SparqlAutomaticParser::LimitClauseContext::integer() {
  return getRuleContext<SparqlAutomaticParser::IntegerContext>(0);
}

size_t SparqlAutomaticParser::LimitClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleLimitClause;
}

void SparqlAutomaticParser::LimitClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterLimitClause(this);
}

void SparqlAutomaticParser::LimitClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitLimitClause(this);
}

antlrcpp::Any SparqlAutomaticParser::LimitClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitLimitClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::LimitClauseContext*
SparqlAutomaticParser::limitClause() {
  LimitClauseContext* _localctx =
      _tracker.createInstance<LimitClauseContext>(_ctx, getState());
  enterRule(_localctx, 52, SparqlAutomaticParser::RuleLimitClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(497);
    match(SparqlAutomaticParser::LIMIT);
    setState(498);
    integer();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OffsetClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::OffsetClauseContext::OffsetClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::OffsetClauseContext::OFFSET() {
  return getToken(SparqlAutomaticParser::OFFSET, 0);
}

SparqlAutomaticParser::IntegerContext*
SparqlAutomaticParser::OffsetClauseContext::integer() {
  return getRuleContext<SparqlAutomaticParser::IntegerContext>(0);
}

size_t SparqlAutomaticParser::OffsetClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleOffsetClause;
}

void SparqlAutomaticParser::OffsetClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterOffsetClause(this);
}

void SparqlAutomaticParser::OffsetClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitOffsetClause(this);
}

antlrcpp::Any SparqlAutomaticParser::OffsetClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitOffsetClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::OffsetClauseContext*
SparqlAutomaticParser::offsetClause() {
  OffsetClauseContext* _localctx =
      _tracker.createInstance<OffsetClauseContext>(_ctx, getState());
  enterRule(_localctx, 54, SparqlAutomaticParser::RuleOffsetClause);

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
    match(SparqlAutomaticParser::OFFSET);
    setState(501);
    integer();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TextLimitClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::TextLimitClauseContext::TextLimitClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::TextLimitClauseContext::TEXTLIMIT() {
  return getToken(SparqlAutomaticParser::TEXTLIMIT, 0);
}

SparqlAutomaticParser::IntegerContext*
SparqlAutomaticParser::TextLimitClauseContext::integer() {
  return getRuleContext<SparqlAutomaticParser::IntegerContext>(0);
}

size_t SparqlAutomaticParser::TextLimitClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTextLimitClause;
}

void SparqlAutomaticParser::TextLimitClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTextLimitClause(this);
}

void SparqlAutomaticParser::TextLimitClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTextLimitClause(this);
}

antlrcpp::Any SparqlAutomaticParser::TextLimitClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTextLimitClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TextLimitClauseContext*
SparqlAutomaticParser::textLimitClause() {
  TextLimitClauseContext* _localctx =
      _tracker.createInstance<TextLimitClauseContext>(_ctx, getState());
  enterRule(_localctx, 56, SparqlAutomaticParser::RuleTextLimitClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(503);
    match(SparqlAutomaticParser::TEXTLIMIT);
    setState(504);
    integer();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValuesClauseContext
//------------------------------------------------------------------

SparqlAutomaticParser::ValuesClauseContext::ValuesClauseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::ValuesClauseContext::VALUES() {
  return getToken(SparqlAutomaticParser::VALUES, 0);
}

SparqlAutomaticParser::DataBlockContext*
SparqlAutomaticParser::ValuesClauseContext::dataBlock() {
  return getRuleContext<SparqlAutomaticParser::DataBlockContext>(0);
}

size_t SparqlAutomaticParser::ValuesClauseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleValuesClause;
}

void SparqlAutomaticParser::ValuesClauseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterValuesClause(this);
}

void SparqlAutomaticParser::ValuesClauseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitValuesClause(this);
}

antlrcpp::Any SparqlAutomaticParser::ValuesClauseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitValuesClause(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ValuesClauseContext*
SparqlAutomaticParser::valuesClause() {
  ValuesClauseContext* _localctx =
      _tracker.createInstance<ValuesClauseContext>(_ctx, getState());
  enterRule(_localctx, 58, SparqlAutomaticParser::RuleValuesClause);
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
    setState(508);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::VALUES) {
      setState(506);
      match(SparqlAutomaticParser::VALUES);
      setState(507);
      dataBlock();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesTemplateContext
//------------------------------------------------------------------

SparqlAutomaticParser::TriplesTemplateContext::TriplesTemplateContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::TriplesSameSubjectContext*
SparqlAutomaticParser::TriplesTemplateContext::triplesSameSubject() {
  return getRuleContext<SparqlAutomaticParser::TriplesSameSubjectContext>(0);
}

SparqlAutomaticParser::TriplesTemplateContext*
SparqlAutomaticParser::TriplesTemplateContext::triplesTemplate() {
  return getRuleContext<SparqlAutomaticParser::TriplesTemplateContext>(0);
}

size_t SparqlAutomaticParser::TriplesTemplateContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTriplesTemplate;
}

void SparqlAutomaticParser::TriplesTemplateContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTriplesTemplate(this);
}

void SparqlAutomaticParser::TriplesTemplateContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTriplesTemplate(this);
}

antlrcpp::Any SparqlAutomaticParser::TriplesTemplateContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTriplesTemplate(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TriplesTemplateContext*
SparqlAutomaticParser::triplesTemplate() {
  TriplesTemplateContext* _localctx =
      _tracker.createInstance<TriplesTemplateContext>(_ctx, getState());
  enterRule(_localctx, 60, SparqlAutomaticParser::RuleTriplesTemplate);
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
    setState(510);
    triplesSameSubject();
    setState(515);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(511);
      match(SparqlAutomaticParser::T__5);
      setState(513);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__28) |
                             (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
             (1ULL << (SparqlAutomaticParser::NIL - 139)) |
             (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
        setState(512);
        triplesTemplate();
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupGraphPatternContext
//------------------------------------------------------------------

SparqlAutomaticParser::GroupGraphPatternContext::GroupGraphPatternContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::SubSelectContext*
SparqlAutomaticParser::GroupGraphPatternContext::subSelect() {
  return getRuleContext<SparqlAutomaticParser::SubSelectContext>(0);
}

SparqlAutomaticParser::GroupGraphPatternSubContext*
SparqlAutomaticParser::GroupGraphPatternContext::groupGraphPatternSub() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternSubContext>(0);
}

size_t SparqlAutomaticParser::GroupGraphPatternContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGroupGraphPattern;
}

void SparqlAutomaticParser::GroupGraphPatternContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGroupGraphPattern(this);
}

void SparqlAutomaticParser::GroupGraphPatternContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGroupGraphPattern(this);
}

antlrcpp::Any SparqlAutomaticParser::GroupGraphPatternContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGroupGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::groupGraphPattern() {
  GroupGraphPatternContext* _localctx =
      _tracker.createInstance<GroupGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 62, SparqlAutomaticParser::RuleGroupGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(517);
    match(SparqlAutomaticParser::T__3);
    setState(520);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::SELECT: {
        setState(518);
        subSelect();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__3:
      case SparqlAutomaticParser::T__4:
      case SparqlAutomaticParser::T__15:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::VALUES:
      case SparqlAutomaticParser::GRAPH:
      case SparqlAutomaticParser::OPTIONAL:
      case SparqlAutomaticParser::SERVICE:
      case SparqlAutomaticParser::BIND:
      case SparqlAutomaticParser::MINUS:
      case SparqlAutomaticParser::FILTER:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        setState(519);
        groupGraphPatternSub();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(522);
    match(SparqlAutomaticParser::T__4);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupGraphPatternSubContext
//------------------------------------------------------------------

SparqlAutomaticParser::GroupGraphPatternSubContext::GroupGraphPatternSubContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::TriplesBlockContext*
SparqlAutomaticParser::GroupGraphPatternSubContext::triplesBlock() {
  return getRuleContext<SparqlAutomaticParser::TriplesBlockContext>(0);
}

std::vector<
    SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext*>
SparqlAutomaticParser::GroupGraphPatternSubContext::
    graphPatternNotTriplesAndMaybeTriples() {
  return getRuleContexts<
      SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext>();
}

SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext*
SparqlAutomaticParser::GroupGraphPatternSubContext::
    graphPatternNotTriplesAndMaybeTriples(size_t i) {
  return getRuleContext<
      SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext>(i);
}

size_t SparqlAutomaticParser::GroupGraphPatternSubContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleGroupGraphPatternSub;
}

void SparqlAutomaticParser::GroupGraphPatternSubContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupGraphPatternSub(this);
}

void SparqlAutomaticParser::GroupGraphPatternSubContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGroupGraphPatternSub(this);
}

antlrcpp::Any SparqlAutomaticParser::GroupGraphPatternSubContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGroupGraphPatternSub(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GroupGraphPatternSubContext*
SparqlAutomaticParser::groupGraphPatternSub() {
  GroupGraphPatternSubContext* _localctx =
      _tracker.createInstance<GroupGraphPatternSubContext>(_ctx, getState());
  enterRule(_localctx, 64, SparqlAutomaticParser::RuleGroupGraphPatternSub);
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
    setState(525);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__28) |
                           (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
           (1ULL << (SparqlAutomaticParser::NIL - 139)) |
           (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
      setState(524);
      triplesBlock();
    }
    setState(530);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__3

           || _la == SparqlAutomaticParser::VALUES ||
           ((((_la - 67) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 67)) &
             ((1ULL << (SparqlAutomaticParser::GRAPH - 67)) |
              (1ULL << (SparqlAutomaticParser::OPTIONAL - 67)) |
              (1ULL << (SparqlAutomaticParser::SERVICE - 67)) |
              (1ULL << (SparqlAutomaticParser::BIND - 67)) |
              (1ULL << (SparqlAutomaticParser::MINUS - 67)) |
              (1ULL << (SparqlAutomaticParser::FILTER - 67)))) != 0)) {
      setState(527);
      graphPatternNotTriplesAndMaybeTriples();
      setState(532);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphPatternNotTriplesAndMaybeTriplesContext
//------------------------------------------------------------------

SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext::
    GraphPatternNotTriplesAndMaybeTriplesContext(ParserRuleContext* parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::GraphPatternNotTriplesContext* SparqlAutomaticParser::
    GraphPatternNotTriplesAndMaybeTriplesContext::graphPatternNotTriples() {
  return getRuleContext<SparqlAutomaticParser::GraphPatternNotTriplesContext>(
      0);
}

SparqlAutomaticParser::TriplesBlockContext* SparqlAutomaticParser::
    GraphPatternNotTriplesAndMaybeTriplesContext::triplesBlock() {
  return getRuleContext<SparqlAutomaticParser::TriplesBlockContext>(0);
}

size_t SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext::
    getRuleIndex() const {
  return SparqlAutomaticParser::RuleGraphPatternNotTriplesAndMaybeTriples;
}

void SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext::
    enterRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphPatternNotTriplesAndMaybeTriples(this);
}

void SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext::
    exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphPatternNotTriplesAndMaybeTriples(this);
}

antlrcpp::Any
SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGraphPatternNotTriplesAndMaybeTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GraphPatternNotTriplesAndMaybeTriplesContext*
SparqlAutomaticParser::graphPatternNotTriplesAndMaybeTriples() {
  GraphPatternNotTriplesAndMaybeTriplesContext* _localctx =
      _tracker.createInstance<GraphPatternNotTriplesAndMaybeTriplesContext>(
          _ctx, getState());
  enterRule(_localctx, 66,
            SparqlAutomaticParser::RuleGraphPatternNotTriplesAndMaybeTriples);
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
    graphPatternNotTriples();
    setState(535);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(534);
      match(SparqlAutomaticParser::T__5);
    }
    setState(538);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__28) |
                           (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
           (1ULL << (SparqlAutomaticParser::NIL - 139)) |
           (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
      setState(537);
      triplesBlock();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesBlockContext
//------------------------------------------------------------------

SparqlAutomaticParser::TriplesBlockContext::TriplesBlockContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::TriplesSameSubjectPathContext*
SparqlAutomaticParser::TriplesBlockContext::triplesSameSubjectPath() {
  return getRuleContext<SparqlAutomaticParser::TriplesSameSubjectPathContext>(
      0);
}

SparqlAutomaticParser::TriplesBlockContext*
SparqlAutomaticParser::TriplesBlockContext::triplesBlock() {
  return getRuleContext<SparqlAutomaticParser::TriplesBlockContext>(0);
}

size_t SparqlAutomaticParser::TriplesBlockContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTriplesBlock;
}

void SparqlAutomaticParser::TriplesBlockContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTriplesBlock(this);
}

void SparqlAutomaticParser::TriplesBlockContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTriplesBlock(this);
}

antlrcpp::Any SparqlAutomaticParser::TriplesBlockContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTriplesBlock(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TriplesBlockContext*
SparqlAutomaticParser::triplesBlock() {
  TriplesBlockContext* _localctx =
      _tracker.createInstance<TriplesBlockContext>(_ctx, getState());
  enterRule(_localctx, 68, SparqlAutomaticParser::RuleTriplesBlock);
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
    setState(540);
    triplesSameSubjectPath();
    setState(545);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(541);
      match(SparqlAutomaticParser::T__5);
      setState(543);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__28) |
                             (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
             (1ULL << (SparqlAutomaticParser::NIL - 139)) |
             (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
        setState(542);
        triplesBlock();
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphPatternNotTriplesContext
//------------------------------------------------------------------

SparqlAutomaticParser::GraphPatternNotTriplesContext::
    GraphPatternNotTriplesContext(ParserRuleContext* parent,
                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::GroupOrUnionGraphPatternContext* SparqlAutomaticParser::
    GraphPatternNotTriplesContext::groupOrUnionGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupOrUnionGraphPatternContext>(
      0);
}

SparqlAutomaticParser::OptionalGraphPatternContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::optionalGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::OptionalGraphPatternContext>(0);
}

SparqlAutomaticParser::MinusGraphPatternContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::minusGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::MinusGraphPatternContext>(0);
}

SparqlAutomaticParser::GraphGraphPatternContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::graphGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GraphGraphPatternContext>(0);
}

SparqlAutomaticParser::ServiceGraphPatternContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::serviceGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::ServiceGraphPatternContext>(0);
}

SparqlAutomaticParser::FilterRContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::filterR() {
  return getRuleContext<SparqlAutomaticParser::FilterRContext>(0);
}

SparqlAutomaticParser::BindContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::bind() {
  return getRuleContext<SparqlAutomaticParser::BindContext>(0);
}

SparqlAutomaticParser::InlineDataContext*
SparqlAutomaticParser::GraphPatternNotTriplesContext::inlineData() {
  return getRuleContext<SparqlAutomaticParser::InlineDataContext>(0);
}

size_t SparqlAutomaticParser::GraphPatternNotTriplesContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleGraphPatternNotTriples;
}

void SparqlAutomaticParser::GraphPatternNotTriplesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGraphPatternNotTriples(this);
}

void SparqlAutomaticParser::GraphPatternNotTriplesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGraphPatternNotTriples(this);
}

antlrcpp::Any SparqlAutomaticParser::GraphPatternNotTriplesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGraphPatternNotTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GraphPatternNotTriplesContext*
SparqlAutomaticParser::graphPatternNotTriples() {
  GraphPatternNotTriplesContext* _localctx =
      _tracker.createInstance<GraphPatternNotTriplesContext>(_ctx, getState());
  enterRule(_localctx, 70, SparqlAutomaticParser::RuleGraphPatternNotTriples);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(555);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__3: {
        enterOuterAlt(_localctx, 1);
        setState(547);
        groupOrUnionGraphPattern();
        break;
      }

      case SparqlAutomaticParser::OPTIONAL: {
        enterOuterAlt(_localctx, 2);
        setState(548);
        optionalGraphPattern();
        break;
      }

      case SparqlAutomaticParser::MINUS: {
        enterOuterAlt(_localctx, 3);
        setState(549);
        minusGraphPattern();
        break;
      }

      case SparqlAutomaticParser::GRAPH: {
        enterOuterAlt(_localctx, 4);
        setState(550);
        graphGraphPattern();
        break;
      }

      case SparqlAutomaticParser::SERVICE: {
        enterOuterAlt(_localctx, 5);
        setState(551);
        serviceGraphPattern();
        break;
      }

      case SparqlAutomaticParser::FILTER: {
        enterOuterAlt(_localctx, 6);
        setState(552);
        filterR();
        break;
      }

      case SparqlAutomaticParser::BIND: {
        enterOuterAlt(_localctx, 7);
        setState(553);
        bind();
        break;
      }

      case SparqlAutomaticParser::VALUES: {
        enterOuterAlt(_localctx, 8);
        setState(554);
        inlineData();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OptionalGraphPatternContext
//------------------------------------------------------------------

SparqlAutomaticParser::OptionalGraphPatternContext::OptionalGraphPatternContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::OptionalGraphPatternContext::OPTIONAL() {
  return getToken(SparqlAutomaticParser::OPTIONAL, 0);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::OptionalGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

size_t SparqlAutomaticParser::OptionalGraphPatternContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleOptionalGraphPattern;
}

void SparqlAutomaticParser::OptionalGraphPatternContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterOptionalGraphPattern(this);
}

void SparqlAutomaticParser::OptionalGraphPatternContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitOptionalGraphPattern(this);
}

antlrcpp::Any SparqlAutomaticParser::OptionalGraphPatternContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitOptionalGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::OptionalGraphPatternContext*
SparqlAutomaticParser::optionalGraphPattern() {
  OptionalGraphPatternContext* _localctx =
      _tracker.createInstance<OptionalGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 72, SparqlAutomaticParser::RuleOptionalGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(557);
    match(SparqlAutomaticParser::OPTIONAL);
    setState(558);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphGraphPatternContext
//------------------------------------------------------------------

SparqlAutomaticParser::GraphGraphPatternContext::GraphGraphPatternContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::GraphGraphPatternContext::GRAPH() {
  return getToken(SparqlAutomaticParser::GRAPH, 0);
}

SparqlAutomaticParser::VarOrIriContext*
SparqlAutomaticParser::GraphGraphPatternContext::varOrIri() {
  return getRuleContext<SparqlAutomaticParser::VarOrIriContext>(0);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::GraphGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

size_t SparqlAutomaticParser::GraphGraphPatternContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGraphGraphPattern;
}

void SparqlAutomaticParser::GraphGraphPatternContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGraphGraphPattern(this);
}

void SparqlAutomaticParser::GraphGraphPatternContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGraphGraphPattern(this);
}

antlrcpp::Any SparqlAutomaticParser::GraphGraphPatternContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGraphGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GraphGraphPatternContext*
SparqlAutomaticParser::graphGraphPattern() {
  GraphGraphPatternContext* _localctx =
      _tracker.createInstance<GraphGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 74, SparqlAutomaticParser::RuleGraphGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(560);
    match(SparqlAutomaticParser::GRAPH);
    setState(561);
    varOrIri();
    setState(562);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ServiceGraphPatternContext
//------------------------------------------------------------------

SparqlAutomaticParser::ServiceGraphPatternContext::ServiceGraphPatternContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::ServiceGraphPatternContext::SERVICE() {
  return getToken(SparqlAutomaticParser::SERVICE, 0);
}

SparqlAutomaticParser::VarOrIriContext*
SparqlAutomaticParser::ServiceGraphPatternContext::varOrIri() {
  return getRuleContext<SparqlAutomaticParser::VarOrIriContext>(0);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::ServiceGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

tree::TerminalNode*
SparqlAutomaticParser::ServiceGraphPatternContext::SILENT() {
  return getToken(SparqlAutomaticParser::SILENT, 0);
}

size_t SparqlAutomaticParser::ServiceGraphPatternContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleServiceGraphPattern;
}

void SparqlAutomaticParser::ServiceGraphPatternContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterServiceGraphPattern(this);
}

void SparqlAutomaticParser::ServiceGraphPatternContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitServiceGraphPattern(this);
}

antlrcpp::Any SparqlAutomaticParser::ServiceGraphPatternContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitServiceGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ServiceGraphPatternContext*
SparqlAutomaticParser::serviceGraphPattern() {
  ServiceGraphPatternContext* _localctx =
      _tracker.createInstance<ServiceGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 76, SparqlAutomaticParser::RuleServiceGraphPattern);
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
    setState(564);
    match(SparqlAutomaticParser::SERVICE);
    setState(566);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::SILENT) {
      setState(565);
      match(SparqlAutomaticParser::SILENT);
    }
    setState(568);
    varOrIri();
    setState(569);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BindContext
//------------------------------------------------------------------

SparqlAutomaticParser::BindContext::BindContext(ParserRuleContext* parent,
                                                size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::BindContext::BIND() {
  return getToken(SparqlAutomaticParser::BIND, 0);
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::BindContext::expression() {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::BindContext::AS() {
  return getToken(SparqlAutomaticParser::AS, 0);
}

SparqlAutomaticParser::VarContext* SparqlAutomaticParser::BindContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

size_t SparqlAutomaticParser::BindContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleBind;
}

void SparqlAutomaticParser::BindContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterBind(this);
}

void SparqlAutomaticParser::BindContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitBind(this);
}

antlrcpp::Any SparqlAutomaticParser::BindContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBind(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BindContext* SparqlAutomaticParser::bind() {
  BindContext* _localctx =
      _tracker.createInstance<BindContext>(_ctx, getState());
  enterRule(_localctx, 78, SparqlAutomaticParser::RuleBind);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(571);
    match(SparqlAutomaticParser::BIND);
    setState(572);
    match(SparqlAutomaticParser::T__1);
    setState(573);
    expression();
    setState(574);
    match(SparqlAutomaticParser::AS);
    setState(575);
    var();
    setState(576);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineDataContext
//------------------------------------------------------------------

SparqlAutomaticParser::InlineDataContext::InlineDataContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::InlineDataContext::VALUES() {
  return getToken(SparqlAutomaticParser::VALUES, 0);
}

SparqlAutomaticParser::DataBlockContext*
SparqlAutomaticParser::InlineDataContext::dataBlock() {
  return getRuleContext<SparqlAutomaticParser::DataBlockContext>(0);
}

size_t SparqlAutomaticParser::InlineDataContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleInlineData;
}

void SparqlAutomaticParser::InlineDataContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterInlineData(this);
}

void SparqlAutomaticParser::InlineDataContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitInlineData(this);
}

antlrcpp::Any SparqlAutomaticParser::InlineDataContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitInlineData(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::InlineDataContext* SparqlAutomaticParser::inlineData() {
  InlineDataContext* _localctx =
      _tracker.createInstance<InlineDataContext>(_ctx, getState());
  enterRule(_localctx, 80, SparqlAutomaticParser::RuleInlineData);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(578);
    match(SparqlAutomaticParser::VALUES);
    setState(579);
    dataBlock();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataBlockContext
//------------------------------------------------------------------

SparqlAutomaticParser::DataBlockContext::DataBlockContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::InlineDataOneVarContext*
SparqlAutomaticParser::DataBlockContext::inlineDataOneVar() {
  return getRuleContext<SparqlAutomaticParser::InlineDataOneVarContext>(0);
}

SparqlAutomaticParser::InlineDataFullContext*
SparqlAutomaticParser::DataBlockContext::inlineDataFull() {
  return getRuleContext<SparqlAutomaticParser::InlineDataFullContext>(0);
}

size_t SparqlAutomaticParser::DataBlockContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDataBlock;
}

void SparqlAutomaticParser::DataBlockContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDataBlock(this);
}

void SparqlAutomaticParser::DataBlockContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDataBlock(this);
}

antlrcpp::Any SparqlAutomaticParser::DataBlockContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDataBlock(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DataBlockContext* SparqlAutomaticParser::dataBlock() {
  DataBlockContext* _localctx =
      _tracker.createInstance<DataBlockContext>(_ctx, getState());
  enterRule(_localctx, 82, SparqlAutomaticParser::RuleDataBlock);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(583);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(581);
        inlineDataOneVar();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 2);
        setState(582);
        inlineDataFull();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineDataOneVarContext
//------------------------------------------------------------------

SparqlAutomaticParser::InlineDataOneVarContext::InlineDataOneVarContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::InlineDataOneVarContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

std::vector<SparqlAutomaticParser::DataBlockValueContext*>
SparqlAutomaticParser::InlineDataOneVarContext::dataBlockValue() {
  return getRuleContexts<SparqlAutomaticParser::DataBlockValueContext>();
}

SparqlAutomaticParser::DataBlockValueContext*
SparqlAutomaticParser::InlineDataOneVarContext::dataBlockValue(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DataBlockValueContext>(i);
}

size_t SparqlAutomaticParser::InlineDataOneVarContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleInlineDataOneVar;
}

void SparqlAutomaticParser::InlineDataOneVarContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterInlineDataOneVar(this);
}

void SparqlAutomaticParser::InlineDataOneVarContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitInlineDataOneVar(this);
}

antlrcpp::Any SparqlAutomaticParser::InlineDataOneVarContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitInlineDataOneVar(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::InlineDataOneVarContext*
SparqlAutomaticParser::inlineDataOneVar() {
  InlineDataOneVarContext* _localctx =
      _tracker.createInstance<InlineDataOneVarContext>(_ctx, getState());
  enterRule(_localctx, 84, SparqlAutomaticParser::RuleInlineDataOneVar);
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
    setState(585);
    var();
    setState(586);
    match(SparqlAutomaticParser::T__3);
    setState(590);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 29) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 29)) &
             ((1ULL << (SparqlAutomaticParser::T__28 - 29)) |
              (1ULL << (SparqlAutomaticParser::T__29 - 29)) |
              (1ULL << (SparqlAutomaticParser::UNDEF - 29)))) != 0) ||
           ((((_la - 139) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 139)) &
             ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
              (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
              (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
              (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
              (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
              (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
              (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
              (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
              (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
              (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
              (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
              (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
              (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
              (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
              (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
              (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
              (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)))) !=
                0)) {
      setState(587);
      dataBlockValue();
      setState(592);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(593);
    match(SparqlAutomaticParser::T__4);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- InlineDataFullContext
//------------------------------------------------------------------

SparqlAutomaticParser::InlineDataFullContext::InlineDataFullContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::InlineDataFullContext::NIL() {
  return getToken(SparqlAutomaticParser::NIL, 0);
}

std::vector<SparqlAutomaticParser::DataBlockSingleContext*>
SparqlAutomaticParser::InlineDataFullContext::dataBlockSingle() {
  return getRuleContexts<SparqlAutomaticParser::DataBlockSingleContext>();
}

SparqlAutomaticParser::DataBlockSingleContext*
SparqlAutomaticParser::InlineDataFullContext::dataBlockSingle(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DataBlockSingleContext>(i);
}

std::vector<SparqlAutomaticParser::VarContext*>
SparqlAutomaticParser::InlineDataFullContext::var() {
  return getRuleContexts<SparqlAutomaticParser::VarContext>();
}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::InlineDataFullContext::var(size_t i) {
  return getRuleContext<SparqlAutomaticParser::VarContext>(i);
}

size_t SparqlAutomaticParser::InlineDataFullContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleInlineDataFull;
}

void SparqlAutomaticParser::InlineDataFullContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterInlineDataFull(this);
}

void SparqlAutomaticParser::InlineDataFullContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitInlineDataFull(this);
}

antlrcpp::Any SparqlAutomaticParser::InlineDataFullContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitInlineDataFull(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::InlineDataFullContext*
SparqlAutomaticParser::inlineDataFull() {
  InlineDataFullContext* _localctx =
      _tracker.createInstance<InlineDataFullContext>(_ctx, getState());
  enterRule(_localctx, 86, SparqlAutomaticParser::RuleInlineDataFull);
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
    setState(604);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::NIL: {
        setState(595);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::T__1: {
        setState(596);
        match(SparqlAutomaticParser::T__1);
        setState(600);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::VAR1

               || _la == SparqlAutomaticParser::VAR2) {
          setState(597);
          var();
          setState(602);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(603);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(606);
    match(SparqlAutomaticParser::T__3);
    setState(610);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__1 ||
           _la == SparqlAutomaticParser::NIL) {
      setState(607);
      dataBlockSingle();
      setState(612);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(613);
    match(SparqlAutomaticParser::T__4);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataBlockSingleContext
//------------------------------------------------------------------

SparqlAutomaticParser::DataBlockSingleContext::DataBlockSingleContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::DataBlockSingleContext::NIL() {
  return getToken(SparqlAutomaticParser::NIL, 0);
}

std::vector<SparqlAutomaticParser::DataBlockValueContext*>
SparqlAutomaticParser::DataBlockSingleContext::dataBlockValue() {
  return getRuleContexts<SparqlAutomaticParser::DataBlockValueContext>();
}

SparqlAutomaticParser::DataBlockValueContext*
SparqlAutomaticParser::DataBlockSingleContext::dataBlockValue(size_t i) {
  return getRuleContext<SparqlAutomaticParser::DataBlockValueContext>(i);
}

size_t SparqlAutomaticParser::DataBlockSingleContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDataBlockSingle;
}

void SparqlAutomaticParser::DataBlockSingleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDataBlockSingle(this);
}

void SparqlAutomaticParser::DataBlockSingleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDataBlockSingle(this);
}

antlrcpp::Any SparqlAutomaticParser::DataBlockSingleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDataBlockSingle(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DataBlockSingleContext*
SparqlAutomaticParser::dataBlockSingle() {
  DataBlockSingleContext* _localctx =
      _tracker.createInstance<DataBlockSingleContext>(_ctx, getState());
  enterRule(_localctx, 88, SparqlAutomaticParser::RuleDataBlockSingle);
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
    setState(624);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        setState(615);
        match(SparqlAutomaticParser::T__1);
        setState(619);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (
            ((((_la - 29) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 29)) &
              ((1ULL << (SparqlAutomaticParser::T__28 - 29)) |
               (1ULL << (SparqlAutomaticParser::T__29 - 29)) |
               (1ULL << (SparqlAutomaticParser::UNDEF - 29)))) != 0) ||
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
               (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
               (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
               (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
               (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
               (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 -
                         139)))) != 0)) {
          setState(616);
          dataBlockValue();
          setState(621);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(622);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::NIL: {
        setState(623);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DataBlockValueContext
//------------------------------------------------------------------

SparqlAutomaticParser::DataBlockValueContext::DataBlockValueContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::DataBlockValueContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

SparqlAutomaticParser::RdfLiteralContext*
SparqlAutomaticParser::DataBlockValueContext::rdfLiteral() {
  return getRuleContext<SparqlAutomaticParser::RdfLiteralContext>(0);
}

SparqlAutomaticParser::NumericLiteralContext*
SparqlAutomaticParser::DataBlockValueContext::numericLiteral() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralContext>(0);
}

SparqlAutomaticParser::BooleanLiteralContext*
SparqlAutomaticParser::DataBlockValueContext::booleanLiteral() {
  return getRuleContext<SparqlAutomaticParser::BooleanLiteralContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::DataBlockValueContext::UNDEF() {
  return getToken(SparqlAutomaticParser::UNDEF, 0);
}

size_t SparqlAutomaticParser::DataBlockValueContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDataBlockValue;
}

void SparqlAutomaticParser::DataBlockValueContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDataBlockValue(this);
}

void SparqlAutomaticParser::DataBlockValueContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDataBlockValue(this);
}

antlrcpp::Any SparqlAutomaticParser::DataBlockValueContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDataBlockValue(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DataBlockValueContext*
SparqlAutomaticParser::dataBlockValue() {
  DataBlockValueContext* _localctx =
      _tracker.createInstance<DataBlockValueContext>(_ctx, getState());
  enterRule(_localctx, 90, SparqlAutomaticParser::RuleDataBlockValue);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(631);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(626);
        iri();
        break;
      }

      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 2);
        setState(627);
        rdfLiteral();
        break;
      }

      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(628);
        numericLiteral();
        break;
      }

      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29: {
        enterOuterAlt(_localctx, 4);
        setState(629);
        booleanLiteral();
        break;
      }

      case SparqlAutomaticParser::UNDEF: {
        enterOuterAlt(_localctx, 5);
        setState(630);
        match(SparqlAutomaticParser::UNDEF);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MinusGraphPatternContext
//------------------------------------------------------------------

SparqlAutomaticParser::MinusGraphPatternContext::MinusGraphPatternContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::MinusGraphPatternContext::MINUS() {
  return getToken(SparqlAutomaticParser::MINUS, 0);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::MinusGraphPatternContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

size_t SparqlAutomaticParser::MinusGraphPatternContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleMinusGraphPattern;
}

void SparqlAutomaticParser::MinusGraphPatternContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterMinusGraphPattern(this);
}

void SparqlAutomaticParser::MinusGraphPatternContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitMinusGraphPattern(this);
}

antlrcpp::Any SparqlAutomaticParser::MinusGraphPatternContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitMinusGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MinusGraphPatternContext*
SparqlAutomaticParser::minusGraphPattern() {
  MinusGraphPatternContext* _localctx =
      _tracker.createInstance<MinusGraphPatternContext>(_ctx, getState());
  enterRule(_localctx, 92, SparqlAutomaticParser::RuleMinusGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(633);
    match(SparqlAutomaticParser::MINUS);
    setState(634);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GroupOrUnionGraphPatternContext
//------------------------------------------------------------------

SparqlAutomaticParser::GroupOrUnionGraphPatternContext::
    GroupOrUnionGraphPatternContext(ParserRuleContext* parent,
                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::GroupGraphPatternContext*>
SparqlAutomaticParser::GroupOrUnionGraphPatternContext::groupGraphPattern() {
  return getRuleContexts<SparqlAutomaticParser::GroupGraphPatternContext>();
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::GroupOrUnionGraphPatternContext::groupGraphPattern(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(i);
}

std::vector<tree::TerminalNode*>
SparqlAutomaticParser::GroupOrUnionGraphPatternContext::UNION() {
  return getTokens(SparqlAutomaticParser::UNION);
}

tree::TerminalNode*
SparqlAutomaticParser::GroupOrUnionGraphPatternContext::UNION(size_t i) {
  return getToken(SparqlAutomaticParser::UNION, i);
}

size_t SparqlAutomaticParser::GroupOrUnionGraphPatternContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleGroupOrUnionGraphPattern;
}

void SparqlAutomaticParser::GroupOrUnionGraphPatternContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterGroupOrUnionGraphPattern(this);
}

void SparqlAutomaticParser::GroupOrUnionGraphPatternContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitGroupOrUnionGraphPattern(this);
}

antlrcpp::Any SparqlAutomaticParser::GroupOrUnionGraphPatternContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGroupOrUnionGraphPattern(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GroupOrUnionGraphPatternContext*
SparqlAutomaticParser::groupOrUnionGraphPattern() {
  GroupOrUnionGraphPatternContext* _localctx =
      _tracker.createInstance<GroupOrUnionGraphPatternContext>(_ctx,
                                                               getState());
  enterRule(_localctx, 94, SparqlAutomaticParser::RuleGroupOrUnionGraphPattern);
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
    setState(636);
    groupGraphPattern();
    setState(641);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::UNION) {
      setState(637);
      match(SparqlAutomaticParser::UNION);
      setState(638);
      groupGraphPattern();
      setState(643);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FilterRContext
//------------------------------------------------------------------

SparqlAutomaticParser::FilterRContext::FilterRContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::FilterRContext::FILTER() {
  return getToken(SparqlAutomaticParser::FILTER, 0);
}

SparqlAutomaticParser::ConstraintContext*
SparqlAutomaticParser::FilterRContext::constraint() {
  return getRuleContext<SparqlAutomaticParser::ConstraintContext>(0);
}

size_t SparqlAutomaticParser::FilterRContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleFilterR;
}

void SparqlAutomaticParser::FilterRContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterFilterR(this);
}

void SparqlAutomaticParser::FilterRContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitFilterR(this);
}

antlrcpp::Any SparqlAutomaticParser::FilterRContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitFilterR(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::FilterRContext* SparqlAutomaticParser::filterR() {
  FilterRContext* _localctx =
      _tracker.createInstance<FilterRContext>(_ctx, getState());
  enterRule(_localctx, 96, SparqlAutomaticParser::RuleFilterR);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(644);
    match(SparqlAutomaticParser::FILTER);
    setState(645);
    constraint();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstraintContext
//------------------------------------------------------------------

SparqlAutomaticParser::ConstraintContext::ConstraintContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::BrackettedExpressionContext*
SparqlAutomaticParser::ConstraintContext::brackettedExpression() {
  return getRuleContext<SparqlAutomaticParser::BrackettedExpressionContext>(0);
}

SparqlAutomaticParser::BuiltInCallContext*
SparqlAutomaticParser::ConstraintContext::builtInCall() {
  return getRuleContext<SparqlAutomaticParser::BuiltInCallContext>(0);
}

SparqlAutomaticParser::FunctionCallContext*
SparqlAutomaticParser::ConstraintContext::functionCall() {
  return getRuleContext<SparqlAutomaticParser::FunctionCallContext>(0);
}

size_t SparqlAutomaticParser::ConstraintContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleConstraint;
}

void SparqlAutomaticParser::ConstraintContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterConstraint(this);
}

void SparqlAutomaticParser::ConstraintContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitConstraint(this);
}

antlrcpp::Any SparqlAutomaticParser::ConstraintContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitConstraint(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ConstraintContext* SparqlAutomaticParser::constraint() {
  ConstraintContext* _localctx =
      _tracker.createInstance<ConstraintContext>(_ctx, getState());
  enterRule(_localctx, 98, SparqlAutomaticParser::RuleConstraint);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(650);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(647);
        brackettedExpression();
        break;
      }

      case SparqlAutomaticParser::GROUP_CONCAT:
      case SparqlAutomaticParser::NOT:
      case SparqlAutomaticParser::STR:
      case SparqlAutomaticParser::LANG:
      case SparqlAutomaticParser::LANGMATCHES:
      case SparqlAutomaticParser::DATATYPE:
      case SparqlAutomaticParser::BOUND:
      case SparqlAutomaticParser::IRI:
      case SparqlAutomaticParser::URI:
      case SparqlAutomaticParser::BNODE:
      case SparqlAutomaticParser::RAND:
      case SparqlAutomaticParser::ABS:
      case SparqlAutomaticParser::CEIL:
      case SparqlAutomaticParser::FLOOR:
      case SparqlAutomaticParser::ROUND:
      case SparqlAutomaticParser::CONCAT:
      case SparqlAutomaticParser::STRLEN:
      case SparqlAutomaticParser::UCASE:
      case SparqlAutomaticParser::LCASE:
      case SparqlAutomaticParser::ENCODE:
      case SparqlAutomaticParser::CONTAINS:
      case SparqlAutomaticParser::STRSTARTS:
      case SparqlAutomaticParser::STRENDS:
      case SparqlAutomaticParser::STRBEFORE:
      case SparqlAutomaticParser::STRAFTER:
      case SparqlAutomaticParser::YEAR:
      case SparqlAutomaticParser::MONTH:
      case SparqlAutomaticParser::DAY:
      case SparqlAutomaticParser::HOURS:
      case SparqlAutomaticParser::MINUTES:
      case SparqlAutomaticParser::SECONDS:
      case SparqlAutomaticParser::TIMEZONE:
      case SparqlAutomaticParser::TZ:
      case SparqlAutomaticParser::NOW:
      case SparqlAutomaticParser::UUID:
      case SparqlAutomaticParser::STRUUID:
      case SparqlAutomaticParser::SHA1:
      case SparqlAutomaticParser::SHA256:
      case SparqlAutomaticParser::SHA384:
      case SparqlAutomaticParser::SHA512:
      case SparqlAutomaticParser::MD5:
      case SparqlAutomaticParser::COALESCE:
      case SparqlAutomaticParser::IF:
      case SparqlAutomaticParser::STRLANG:
      case SparqlAutomaticParser::STRDT:
      case SparqlAutomaticParser::SAMETERM:
      case SparqlAutomaticParser::ISIRI:
      case SparqlAutomaticParser::ISURI:
      case SparqlAutomaticParser::ISBLANK:
      case SparqlAutomaticParser::ISLITERAL:
      case SparqlAutomaticParser::ISNUMERIC:
      case SparqlAutomaticParser::REGEX:
      case SparqlAutomaticParser::SUBSTR:
      case SparqlAutomaticParser::REPLACE:
      case SparqlAutomaticParser::EXISTS:
      case SparqlAutomaticParser::COUNT:
      case SparqlAutomaticParser::SUM:
      case SparqlAutomaticParser::MIN:
      case SparqlAutomaticParser::MAX:
      case SparqlAutomaticParser::AVG:
      case SparqlAutomaticParser::SAMPLE: {
        enterOuterAlt(_localctx, 2);
        setState(648);
        builtInCall();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 3);
        setState(649);
        functionCall();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- FunctionCallContext
//------------------------------------------------------------------

SparqlAutomaticParser::FunctionCallContext::FunctionCallContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::FunctionCallContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

SparqlAutomaticParser::ArgListContext*
SparqlAutomaticParser::FunctionCallContext::argList() {
  return getRuleContext<SparqlAutomaticParser::ArgListContext>(0);
}

size_t SparqlAutomaticParser::FunctionCallContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleFunctionCall;
}

void SparqlAutomaticParser::FunctionCallContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterFunctionCall(this);
}

void SparqlAutomaticParser::FunctionCallContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitFunctionCall(this);
}

antlrcpp::Any SparqlAutomaticParser::FunctionCallContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitFunctionCall(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::FunctionCallContext*
SparqlAutomaticParser::functionCall() {
  FunctionCallContext* _localctx =
      _tracker.createInstance<FunctionCallContext>(_ctx, getState());
  enterRule(_localctx, 100, SparqlAutomaticParser::RuleFunctionCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(652);
    iri();
    setState(653);
    argList();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ArgListContext
//------------------------------------------------------------------

SparqlAutomaticParser::ArgListContext::ArgListContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::ArgListContext::NIL() {
  return getToken(SparqlAutomaticParser::NIL, 0);
}

std::vector<SparqlAutomaticParser::ExpressionContext*>
SparqlAutomaticParser::ArgListContext::expression() {
  return getRuleContexts<SparqlAutomaticParser::ExpressionContext>();
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::ArgListContext::expression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(i);
}

tree::TerminalNode* SparqlAutomaticParser::ArgListContext::DISTINCT() {
  return getToken(SparqlAutomaticParser::DISTINCT, 0);
}

size_t SparqlAutomaticParser::ArgListContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleArgList;
}

void SparqlAutomaticParser::ArgListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterArgList(this);
}

void SparqlAutomaticParser::ArgListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitArgList(this);
}

antlrcpp::Any SparqlAutomaticParser::ArgListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitArgList(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ArgListContext* SparqlAutomaticParser::argList() {
  ArgListContext* _localctx =
      _tracker.createInstance<ArgListContext>(_ctx, getState());
  enterRule(_localctx, 102, SparqlAutomaticParser::RuleArgList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(670);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(655);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(656);
        match(SparqlAutomaticParser::T__1);
        setState(658);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(657);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(660);
        expression();
        setState(665);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::T__6) {
          setState(661);
          match(SparqlAutomaticParser::T__6);
          setState(662);
          expression();
          setState(667);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(668);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionListContext
//------------------------------------------------------------------

SparqlAutomaticParser::ExpressionListContext::ExpressionListContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::ExpressionListContext::NIL() {
  return getToken(SparqlAutomaticParser::NIL, 0);
}

std::vector<SparqlAutomaticParser::ExpressionContext*>
SparqlAutomaticParser::ExpressionListContext::expression() {
  return getRuleContexts<SparqlAutomaticParser::ExpressionContext>();
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::ExpressionListContext::expression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(i);
}

size_t SparqlAutomaticParser::ExpressionListContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleExpressionList;
}

void SparqlAutomaticParser::ExpressionListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterExpressionList(this);
}

void SparqlAutomaticParser::ExpressionListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitExpressionList(this);
}

antlrcpp::Any SparqlAutomaticParser::ExpressionListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitExpressionList(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ExpressionListContext*
SparqlAutomaticParser::expressionList() {
  ExpressionListContext* _localctx =
      _tracker.createInstance<ExpressionListContext>(_ctx, getState());
  enterRule(_localctx, 104, SparqlAutomaticParser::RuleExpressionList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(684);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(672);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(673);
        match(SparqlAutomaticParser::T__1);
        setState(674);
        expression();
        setState(679);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::T__6) {
          setState(675);
          match(SparqlAutomaticParser::T__6);
          setState(676);
          expression();
          setState(681);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(682);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructTemplateContext
//------------------------------------------------------------------

SparqlAutomaticParser::ConstructTemplateContext::ConstructTemplateContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::ConstructTriplesContext*
SparqlAutomaticParser::ConstructTemplateContext::constructTriples() {
  return getRuleContext<SparqlAutomaticParser::ConstructTriplesContext>(0);
}

size_t SparqlAutomaticParser::ConstructTemplateContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleConstructTemplate;
}

void SparqlAutomaticParser::ConstructTemplateContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterConstructTemplate(this);
}

void SparqlAutomaticParser::ConstructTemplateContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitConstructTemplate(this);
}

antlrcpp::Any SparqlAutomaticParser::ConstructTemplateContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitConstructTemplate(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ConstructTemplateContext*
SparqlAutomaticParser::constructTemplate() {
  ConstructTemplateContext* _localctx =
      _tracker.createInstance<ConstructTemplateContext>(_ctx, getState());
  enterRule(_localctx, 106, SparqlAutomaticParser::RuleConstructTemplate);
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
    setState(686);
    match(SparqlAutomaticParser::T__3);
    setState(688);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__28) |
                           (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
           (1ULL << (SparqlAutomaticParser::NIL - 139)) |
           (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
      setState(687);
      constructTriples();
    }
    setState(690);
    match(SparqlAutomaticParser::T__4);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConstructTriplesContext
//------------------------------------------------------------------

SparqlAutomaticParser::ConstructTriplesContext::ConstructTriplesContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::TriplesSameSubjectContext*
SparqlAutomaticParser::ConstructTriplesContext::triplesSameSubject() {
  return getRuleContext<SparqlAutomaticParser::TriplesSameSubjectContext>(0);
}

SparqlAutomaticParser::ConstructTriplesContext*
SparqlAutomaticParser::ConstructTriplesContext::constructTriples() {
  return getRuleContext<SparqlAutomaticParser::ConstructTriplesContext>(0);
}

size_t SparqlAutomaticParser::ConstructTriplesContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleConstructTriples;
}

void SparqlAutomaticParser::ConstructTriplesContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterConstructTriples(this);
}

void SparqlAutomaticParser::ConstructTriplesContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitConstructTriples(this);
}

antlrcpp::Any SparqlAutomaticParser::ConstructTriplesContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitConstructTriples(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ConstructTriplesContext*
SparqlAutomaticParser::constructTriples() {
  ConstructTriplesContext* _localctx =
      _tracker.createInstance<ConstructTriplesContext>(_ctx, getState());
  enterRule(_localctx, 108, SparqlAutomaticParser::RuleConstructTriples);
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
    setState(692);
    triplesSameSubject();
    setState(697);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(693);
      match(SparqlAutomaticParser::T__5);
      setState(695);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__28) |
                             (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
             (1ULL << (SparqlAutomaticParser::NIL - 139)) |
             (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0)) {
        setState(694);
        constructTriples();
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesSameSubjectContext
//------------------------------------------------------------------

SparqlAutomaticParser::TriplesSameSubjectContext::TriplesSameSubjectContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarOrTermContext*
SparqlAutomaticParser::TriplesSameSubjectContext::varOrTerm() {
  return getRuleContext<SparqlAutomaticParser::VarOrTermContext>(0);
}

SparqlAutomaticParser::PropertyListNotEmptyContext*
SparqlAutomaticParser::TriplesSameSubjectContext::propertyListNotEmpty() {
  return getRuleContext<SparqlAutomaticParser::PropertyListNotEmptyContext>(0);
}

SparqlAutomaticParser::TriplesNodeContext*
SparqlAutomaticParser::TriplesSameSubjectContext::triplesNode() {
  return getRuleContext<SparqlAutomaticParser::TriplesNodeContext>(0);
}

SparqlAutomaticParser::PropertyListContext*
SparqlAutomaticParser::TriplesSameSubjectContext::propertyList() {
  return getRuleContext<SparqlAutomaticParser::PropertyListContext>(0);
}

size_t SparqlAutomaticParser::TriplesSameSubjectContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTriplesSameSubject;
}

void SparqlAutomaticParser::TriplesSameSubjectContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTriplesSameSubject(this);
}

void SparqlAutomaticParser::TriplesSameSubjectContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTriplesSameSubject(this);
}

antlrcpp::Any SparqlAutomaticParser::TriplesSameSubjectContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTriplesSameSubject(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TriplesSameSubjectContext*
SparqlAutomaticParser::triplesSameSubject() {
  TriplesSameSubjectContext* _localctx =
      _tracker.createInstance<TriplesSameSubjectContext>(_ctx, getState());
  enterRule(_localctx, 110, SparqlAutomaticParser::RuleTriplesSameSubject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(705);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(699);
        varOrTerm();
        setState(700);
        propertyListNotEmpty();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(702);
        triplesNode();
        setState(703);
        propertyList();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListContext
//------------------------------------------------------------------

SparqlAutomaticParser::PropertyListContext::PropertyListContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PropertyListNotEmptyContext*
SparqlAutomaticParser::PropertyListContext::propertyListNotEmpty() {
  return getRuleContext<SparqlAutomaticParser::PropertyListNotEmptyContext>(0);
}

size_t SparqlAutomaticParser::PropertyListContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePropertyList;
}

void SparqlAutomaticParser::PropertyListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPropertyList(this);
}

void SparqlAutomaticParser::PropertyListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPropertyList(this);
}

antlrcpp::Any SparqlAutomaticParser::PropertyListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPropertyList(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PropertyListContext*
SparqlAutomaticParser::propertyList() {
  PropertyListContext* _localctx =
      _tracker.createInstance<PropertyListContext>(_ctx, getState());
  enterRule(_localctx, 112, SparqlAutomaticParser::RulePropertyList);
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
    setState(708);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__8 ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)))) != 0)) {
      setState(707);
      propertyListNotEmpty();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListNotEmptyContext
//------------------------------------------------------------------

SparqlAutomaticParser::PropertyListNotEmptyContext::PropertyListNotEmptyContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::VerbContext*>
SparqlAutomaticParser::PropertyListNotEmptyContext::verb() {
  return getRuleContexts<SparqlAutomaticParser::VerbContext>();
}

SparqlAutomaticParser::VerbContext*
SparqlAutomaticParser::PropertyListNotEmptyContext::verb(size_t i) {
  return getRuleContext<SparqlAutomaticParser::VerbContext>(i);
}

std::vector<SparqlAutomaticParser::ObjectListContext*>
SparqlAutomaticParser::PropertyListNotEmptyContext::objectList() {
  return getRuleContexts<SparqlAutomaticParser::ObjectListContext>();
}

SparqlAutomaticParser::ObjectListContext*
SparqlAutomaticParser::PropertyListNotEmptyContext::objectList(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ObjectListContext>(i);
}

size_t SparqlAutomaticParser::PropertyListNotEmptyContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RulePropertyListNotEmpty;
}

void SparqlAutomaticParser::PropertyListNotEmptyContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyListNotEmpty(this);
}

void SparqlAutomaticParser::PropertyListNotEmptyContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPropertyListNotEmpty(this);
}

antlrcpp::Any SparqlAutomaticParser::PropertyListNotEmptyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPropertyListNotEmpty(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PropertyListNotEmptyContext*
SparqlAutomaticParser::propertyListNotEmpty() {
  PropertyListNotEmptyContext* _localctx =
      _tracker.createInstance<PropertyListNotEmptyContext>(_ctx, getState());
  enterRule(_localctx, 114, SparqlAutomaticParser::RulePropertyListNotEmpty);
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
    verb();
    setState(711);
    objectList();
    setState(720);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__7) {
      setState(712);
      match(SparqlAutomaticParser::T__7);
      setState(716);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SparqlAutomaticParser::T__8 ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)))) != 0)) {
        setState(713);
        verb();
        setState(714);
        objectList();
      }
      setState(722);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbContext
//------------------------------------------------------------------

SparqlAutomaticParser::VerbContext::VerbContext(ParserRuleContext* parent,
                                                size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarOrIriContext*
SparqlAutomaticParser::VerbContext::varOrIri() {
  return getRuleContext<SparqlAutomaticParser::VarOrIriContext>(0);
}

size_t SparqlAutomaticParser::VerbContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVerb;
}

void SparqlAutomaticParser::VerbContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVerb(this);
}

void SparqlAutomaticParser::VerbContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVerb(this);
}

antlrcpp::Any SparqlAutomaticParser::VerbContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVerb(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VerbContext* SparqlAutomaticParser::verb() {
  VerbContext* _localctx =
      _tracker.createInstance<VerbContext>(_ctx, getState());
  enterRule(_localctx, 116, SparqlAutomaticParser::RuleVerb);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(725);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(723);
        varOrIri();
        break;
      }

      case SparqlAutomaticParser::T__8: {
        enterOuterAlt(_localctx, 2);
        setState(724);
        match(SparqlAutomaticParser::T__8);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectListContext
//------------------------------------------------------------------

SparqlAutomaticParser::ObjectListContext::ObjectListContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::ObjectRContext*>
SparqlAutomaticParser::ObjectListContext::objectR() {
  return getRuleContexts<SparqlAutomaticParser::ObjectRContext>();
}

SparqlAutomaticParser::ObjectRContext*
SparqlAutomaticParser::ObjectListContext::objectR(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ObjectRContext>(i);
}

size_t SparqlAutomaticParser::ObjectListContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleObjectList;
}

void SparqlAutomaticParser::ObjectListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterObjectList(this);
}

void SparqlAutomaticParser::ObjectListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitObjectList(this);
}

antlrcpp::Any SparqlAutomaticParser::ObjectListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitObjectList(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ObjectListContext* SparqlAutomaticParser::objectList() {
  ObjectListContext* _localctx =
      _tracker.createInstance<ObjectListContext>(_ctx, getState());
  enterRule(_localctx, 118, SparqlAutomaticParser::RuleObjectList);
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
    setState(727);
    objectR();
    setState(732);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__6) {
      setState(728);
      match(SparqlAutomaticParser::T__6);
      setState(729);
      objectR();
      setState(734);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectRContext
//------------------------------------------------------------------

SparqlAutomaticParser::ObjectRContext::ObjectRContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::GraphNodeContext*
SparqlAutomaticParser::ObjectRContext::graphNode() {
  return getRuleContext<SparqlAutomaticParser::GraphNodeContext>(0);
}

size_t SparqlAutomaticParser::ObjectRContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleObjectR;
}

void SparqlAutomaticParser::ObjectRContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterObjectR(this);
}

void SparqlAutomaticParser::ObjectRContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitObjectR(this);
}

antlrcpp::Any SparqlAutomaticParser::ObjectRContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitObjectR(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ObjectRContext* SparqlAutomaticParser::objectR() {
  ObjectRContext* _localctx =
      _tracker.createInstance<ObjectRContext>(_ctx, getState());
  enterRule(_localctx, 120, SparqlAutomaticParser::RuleObjectR);

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
    graphNode();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesSameSubjectPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::TriplesSameSubjectPathContext::
    TriplesSameSubjectPathContext(ParserRuleContext* parent,
                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarOrTermContext*
SparqlAutomaticParser::TriplesSameSubjectPathContext::varOrTerm() {
  return getRuleContext<SparqlAutomaticParser::VarOrTermContext>(0);
}

SparqlAutomaticParser::PropertyListPathNotEmptyContext* SparqlAutomaticParser::
    TriplesSameSubjectPathContext::propertyListPathNotEmpty() {
  return getRuleContext<SparqlAutomaticParser::PropertyListPathNotEmptyContext>(
      0);
}

SparqlAutomaticParser::TriplesNodePathContext*
SparqlAutomaticParser::TriplesSameSubjectPathContext::triplesNodePath() {
  return getRuleContext<SparqlAutomaticParser::TriplesNodePathContext>(0);
}

SparqlAutomaticParser::PropertyListPathContext*
SparqlAutomaticParser::TriplesSameSubjectPathContext::propertyListPath() {
  return getRuleContext<SparqlAutomaticParser::PropertyListPathContext>(0);
}

size_t SparqlAutomaticParser::TriplesSameSubjectPathContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleTriplesSameSubjectPath;
}

void SparqlAutomaticParser::TriplesSameSubjectPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterTriplesSameSubjectPath(this);
}

void SparqlAutomaticParser::TriplesSameSubjectPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitTriplesSameSubjectPath(this);
}

antlrcpp::Any SparqlAutomaticParser::TriplesSameSubjectPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTriplesSameSubjectPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TriplesSameSubjectPathContext*
SparqlAutomaticParser::triplesSameSubjectPath() {
  TriplesSameSubjectPathContext* _localctx =
      _tracker.createInstance<TriplesSameSubjectPathContext>(_ctx, getState());
  enterRule(_localctx, 122, SparqlAutomaticParser::RuleTriplesSameSubjectPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(743);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(737);
        varOrTerm();
        setState(738);
        propertyListPathNotEmpty();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(740);
        triplesNodePath();
        setState(741);
        propertyListPath();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::PropertyListPathContext::PropertyListPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PropertyListPathNotEmptyContext*
SparqlAutomaticParser::PropertyListPathContext::propertyListPathNotEmpty() {
  return getRuleContext<SparqlAutomaticParser::PropertyListPathNotEmptyContext>(
      0);
}

size_t SparqlAutomaticParser::PropertyListPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePropertyListPath;
}

void SparqlAutomaticParser::PropertyListPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPropertyListPath(this);
}

void SparqlAutomaticParser::PropertyListPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPropertyListPath(this);
}

antlrcpp::Any SparqlAutomaticParser::PropertyListPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPropertyListPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PropertyListPathContext*
SparqlAutomaticParser::propertyListPath() {
  PropertyListPathContext* _localctx =
      _tracker.createInstance<PropertyListPathContext>(_ctx, getState());
  enterRule(_localctx, 124, SparqlAutomaticParser::RulePropertyListPath);
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
    setState(746);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__8) |
                           (1ULL << SparqlAutomaticParser::T__11) |
                           (1ULL << SparqlAutomaticParser::T__14))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)))) != 0)) {
      setState(745);
      propertyListPathNotEmpty();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PropertyListPathNotEmptyContext
//------------------------------------------------------------------

SparqlAutomaticParser::PropertyListPathNotEmptyContext::
    PropertyListPathNotEmptyContext(ParserRuleContext* parent,
                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::TupleWithPathContext*
SparqlAutomaticParser::PropertyListPathNotEmptyContext::tupleWithPath() {
  return getRuleContext<SparqlAutomaticParser::TupleWithPathContext>(0);
}

std::vector<SparqlAutomaticParser::TupleWithoutPathContext*>
SparqlAutomaticParser::PropertyListPathNotEmptyContext::tupleWithoutPath() {
  return getRuleContexts<SparqlAutomaticParser::TupleWithoutPathContext>();
}

SparqlAutomaticParser::TupleWithoutPathContext*
SparqlAutomaticParser::PropertyListPathNotEmptyContext::tupleWithoutPath(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::TupleWithoutPathContext>(i);
}

size_t SparqlAutomaticParser::PropertyListPathNotEmptyContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RulePropertyListPathNotEmpty;
}

void SparqlAutomaticParser::PropertyListPathNotEmptyContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPropertyListPathNotEmpty(this);
}

void SparqlAutomaticParser::PropertyListPathNotEmptyContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPropertyListPathNotEmpty(this);
}

antlrcpp::Any SparqlAutomaticParser::PropertyListPathNotEmptyContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPropertyListPathNotEmpty(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PropertyListPathNotEmptyContext*
SparqlAutomaticParser::propertyListPathNotEmpty() {
  PropertyListPathNotEmptyContext* _localctx =
      _tracker.createInstance<PropertyListPathNotEmptyContext>(_ctx,
                                                               getState());
  enterRule(_localctx, 126,
            SparqlAutomaticParser::RulePropertyListPathNotEmpty);
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
    setState(748);
    tupleWithPath();
    setState(755);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__7) {
      setState(749);
      match(SparqlAutomaticParser::T__7);
      setState(751);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__8) |
                             (1ULL << SparqlAutomaticParser::T__11) |
                             (1ULL << SparqlAutomaticParser::T__14))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)))) != 0)) {
        setState(750);
        tupleWithoutPath();
      }
      setState(757);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::VerbPathContext::VerbPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PathContext*
SparqlAutomaticParser::VerbPathContext::path() {
  return getRuleContext<SparqlAutomaticParser::PathContext>(0);
}

size_t SparqlAutomaticParser::VerbPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVerbPath;
}

void SparqlAutomaticParser::VerbPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVerbPath(this);
}

void SparqlAutomaticParser::VerbPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVerbPath(this);
}

antlrcpp::Any SparqlAutomaticParser::VerbPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVerbPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VerbPathContext* SparqlAutomaticParser::verbPath() {
  VerbPathContext* _localctx =
      _tracker.createInstance<VerbPathContext>(_ctx, getState());
  enterRule(_localctx, 128, SparqlAutomaticParser::RuleVerbPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(758);
    path();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbSimpleContext
//------------------------------------------------------------------

SparqlAutomaticParser::VerbSimpleContext::VerbSimpleContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::VerbSimpleContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

size_t SparqlAutomaticParser::VerbSimpleContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVerbSimple;
}

void SparqlAutomaticParser::VerbSimpleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVerbSimple(this);
}

void SparqlAutomaticParser::VerbSimpleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVerbSimple(this);
}

antlrcpp::Any SparqlAutomaticParser::VerbSimpleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVerbSimple(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VerbSimpleContext* SparqlAutomaticParser::verbSimple() {
  VerbSimpleContext* _localctx =
      _tracker.createInstance<VerbSimpleContext>(_ctx, getState());
  enterRule(_localctx, 130, SparqlAutomaticParser::RuleVerbSimple);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(760);
    var();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TupleWithoutPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::TupleWithoutPathContext::TupleWithoutPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VerbPathOrSimpleContext*
SparqlAutomaticParser::TupleWithoutPathContext::verbPathOrSimple() {
  return getRuleContext<SparqlAutomaticParser::VerbPathOrSimpleContext>(0);
}

SparqlAutomaticParser::ObjectListContext*
SparqlAutomaticParser::TupleWithoutPathContext::objectList() {
  return getRuleContext<SparqlAutomaticParser::ObjectListContext>(0);
}

size_t SparqlAutomaticParser::TupleWithoutPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTupleWithoutPath;
}

void SparqlAutomaticParser::TupleWithoutPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTupleWithoutPath(this);
}

void SparqlAutomaticParser::TupleWithoutPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTupleWithoutPath(this);
}

antlrcpp::Any SparqlAutomaticParser::TupleWithoutPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTupleWithoutPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TupleWithoutPathContext*
SparqlAutomaticParser::tupleWithoutPath() {
  TupleWithoutPathContext* _localctx =
      _tracker.createInstance<TupleWithoutPathContext>(_ctx, getState());
  enterRule(_localctx, 132, SparqlAutomaticParser::RuleTupleWithoutPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(762);
    verbPathOrSimple();
    setState(763);
    objectList();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TupleWithPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::TupleWithPathContext::TupleWithPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VerbPathOrSimpleContext*
SparqlAutomaticParser::TupleWithPathContext::verbPathOrSimple() {
  return getRuleContext<SparqlAutomaticParser::VerbPathOrSimpleContext>(0);
}

SparqlAutomaticParser::ObjectListPathContext*
SparqlAutomaticParser::TupleWithPathContext::objectListPath() {
  return getRuleContext<SparqlAutomaticParser::ObjectListPathContext>(0);
}

size_t SparqlAutomaticParser::TupleWithPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTupleWithPath;
}

void SparqlAutomaticParser::TupleWithPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTupleWithPath(this);
}

void SparqlAutomaticParser::TupleWithPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTupleWithPath(this);
}

antlrcpp::Any SparqlAutomaticParser::TupleWithPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTupleWithPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TupleWithPathContext*
SparqlAutomaticParser::tupleWithPath() {
  TupleWithPathContext* _localctx =
      _tracker.createInstance<TupleWithPathContext>(_ctx, getState());
  enterRule(_localctx, 134, SparqlAutomaticParser::RuleTupleWithPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(765);
    verbPathOrSimple();
    setState(766);
    objectListPath();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VerbPathOrSimpleContext
//------------------------------------------------------------------

SparqlAutomaticParser::VerbPathOrSimpleContext::VerbPathOrSimpleContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VerbPathContext*
SparqlAutomaticParser::VerbPathOrSimpleContext::verbPath() {
  return getRuleContext<SparqlAutomaticParser::VerbPathContext>(0);
}

SparqlAutomaticParser::VerbSimpleContext*
SparqlAutomaticParser::VerbPathOrSimpleContext::verbSimple() {
  return getRuleContext<SparqlAutomaticParser::VerbSimpleContext>(0);
}

size_t SparqlAutomaticParser::VerbPathOrSimpleContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVerbPathOrSimple;
}

void SparqlAutomaticParser::VerbPathOrSimpleContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVerbPathOrSimple(this);
}

void SparqlAutomaticParser::VerbPathOrSimpleContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVerbPathOrSimple(this);
}

antlrcpp::Any SparqlAutomaticParser::VerbPathOrSimpleContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVerbPathOrSimple(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VerbPathOrSimpleContext*
SparqlAutomaticParser::verbPathOrSimple() {
  VerbPathOrSimpleContext* _localctx =
      _tracker.createInstance<VerbPathOrSimpleContext>(_ctx, getState());
  enterRule(_localctx, 136, SparqlAutomaticParser::RuleVerbPathOrSimple);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(770);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__11:
      case SparqlAutomaticParser::T__14:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        setState(768);
        verbPath();
        break;
      }

      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        setState(769);
        verbSimple();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectListPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::ObjectListPathContext::ObjectListPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::ObjectPathContext*>
SparqlAutomaticParser::ObjectListPathContext::objectPath() {
  return getRuleContexts<SparqlAutomaticParser::ObjectPathContext>();
}

SparqlAutomaticParser::ObjectPathContext*
SparqlAutomaticParser::ObjectListPathContext::objectPath(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ObjectPathContext>(i);
}

size_t SparqlAutomaticParser::ObjectListPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleObjectListPath;
}

void SparqlAutomaticParser::ObjectListPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterObjectListPath(this);
}

void SparqlAutomaticParser::ObjectListPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitObjectListPath(this);
}

antlrcpp::Any SparqlAutomaticParser::ObjectListPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitObjectListPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ObjectListPathContext*
SparqlAutomaticParser::objectListPath() {
  ObjectListPathContext* _localctx =
      _tracker.createInstance<ObjectListPathContext>(_ctx, getState());
  enterRule(_localctx, 138, SparqlAutomaticParser::RuleObjectListPath);
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
    setState(772);
    objectPath();
    setState(777);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__6) {
      setState(773);
      match(SparqlAutomaticParser::T__6);
      setState(774);
      objectPath();
      setState(779);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::ObjectPathContext::ObjectPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::GraphNodePathContext*
SparqlAutomaticParser::ObjectPathContext::graphNodePath() {
  return getRuleContext<SparqlAutomaticParser::GraphNodePathContext>(0);
}

size_t SparqlAutomaticParser::ObjectPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleObjectPath;
}

void SparqlAutomaticParser::ObjectPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterObjectPath(this);
}

void SparqlAutomaticParser::ObjectPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitObjectPath(this);
}

antlrcpp::Any SparqlAutomaticParser::ObjectPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitObjectPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ObjectPathContext* SparqlAutomaticParser::objectPath() {
  ObjectPathContext* _localctx =
      _tracker.createInstance<ObjectPathContext>(_ctx, getState());
  enterRule(_localctx, 140, SparqlAutomaticParser::RuleObjectPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(780);
    graphNodePath();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathContext::PathContext(ParserRuleContext* parent,
                                                size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PathAlternativeContext*
SparqlAutomaticParser::PathContext::pathAlternative() {
  return getRuleContext<SparqlAutomaticParser::PathAlternativeContext>(0);
}

size_t SparqlAutomaticParser::PathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePath;
}

void SparqlAutomaticParser::PathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPath(this);
}

void SparqlAutomaticParser::PathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPath(this);
}

antlrcpp::Any SparqlAutomaticParser::PathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathContext* SparqlAutomaticParser::path() {
  PathContext* _localctx =
      _tracker.createInstance<PathContext>(_ctx, getState());
  enterRule(_localctx, 142, SparqlAutomaticParser::RulePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(782);
    pathAlternative();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathAlternativeContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathAlternativeContext::PathAlternativeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::PathSequenceContext*>
SparqlAutomaticParser::PathAlternativeContext::pathSequence() {
  return getRuleContexts<SparqlAutomaticParser::PathSequenceContext>();
}

SparqlAutomaticParser::PathSequenceContext*
SparqlAutomaticParser::PathAlternativeContext::pathSequence(size_t i) {
  return getRuleContext<SparqlAutomaticParser::PathSequenceContext>(i);
}

size_t SparqlAutomaticParser::PathAlternativeContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePathAlternative;
}

void SparqlAutomaticParser::PathAlternativeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPathAlternative(this);
}

void SparqlAutomaticParser::PathAlternativeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathAlternative(this);
}

antlrcpp::Any SparqlAutomaticParser::PathAlternativeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathAlternative(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathAlternativeContext*
SparqlAutomaticParser::pathAlternative() {
  PathAlternativeContext* _localctx =
      _tracker.createInstance<PathAlternativeContext>(_ctx, getState());
  enterRule(_localctx, 144, SparqlAutomaticParser::RulePathAlternative);
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
    setState(784);
    pathSequence();
    setState(789);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__9) {
      setState(785);
      match(SparqlAutomaticParser::T__9);
      setState(786);
      pathSequence();
      setState(791);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathSequenceContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathSequenceContext::PathSequenceContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::PathEltOrInverseContext*>
SparqlAutomaticParser::PathSequenceContext::pathEltOrInverse() {
  return getRuleContexts<SparqlAutomaticParser::PathEltOrInverseContext>();
}

SparqlAutomaticParser::PathEltOrInverseContext*
SparqlAutomaticParser::PathSequenceContext::pathEltOrInverse(size_t i) {
  return getRuleContext<SparqlAutomaticParser::PathEltOrInverseContext>(i);
}

size_t SparqlAutomaticParser::PathSequenceContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePathSequence;
}

void SparqlAutomaticParser::PathSequenceContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPathSequence(this);
}

void SparqlAutomaticParser::PathSequenceContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathSequence(this);
}

antlrcpp::Any SparqlAutomaticParser::PathSequenceContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathSequence(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathSequenceContext*
SparqlAutomaticParser::pathSequence() {
  PathSequenceContext* _localctx =
      _tracker.createInstance<PathSequenceContext>(_ctx, getState());
  enterRule(_localctx, 146, SparqlAutomaticParser::RulePathSequence);
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
    setState(792);
    pathEltOrInverse();
    setState(797);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__10) {
      setState(793);
      match(SparqlAutomaticParser::T__10);
      setState(794);
      pathEltOrInverse();
      setState(799);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathEltContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathEltContext::PathEltContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PathPrimaryContext*
SparqlAutomaticParser::PathEltContext::pathPrimary() {
  return getRuleContext<SparqlAutomaticParser::PathPrimaryContext>(0);
}

SparqlAutomaticParser::PathModContext*
SparqlAutomaticParser::PathEltContext::pathMod() {
  return getRuleContext<SparqlAutomaticParser::PathModContext>(0);
}

size_t SparqlAutomaticParser::PathEltContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePathElt;
}

void SparqlAutomaticParser::PathEltContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPathElt(this);
}

void SparqlAutomaticParser::PathEltContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathElt(this);
}

antlrcpp::Any SparqlAutomaticParser::PathEltContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathElt(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathEltContext* SparqlAutomaticParser::pathElt() {
  PathEltContext* _localctx =
      _tracker.createInstance<PathEltContext>(_ctx, getState());
  enterRule(_localctx, 148, SparqlAutomaticParser::RulePathElt);
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
    setState(800);
    pathPrimary();
    setState(802);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__0) |
                           (1ULL << SparqlAutomaticParser::T__12) |
                           (1ULL << SparqlAutomaticParser::T__13))) != 0)) {
      setState(801);
      pathMod();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathEltOrInverseContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathEltOrInverseContext::PathEltOrInverseContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PathEltContext*
SparqlAutomaticParser::PathEltOrInverseContext::pathElt() {
  return getRuleContext<SparqlAutomaticParser::PathEltContext>(0);
}

size_t SparqlAutomaticParser::PathEltOrInverseContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePathEltOrInverse;
}

void SparqlAutomaticParser::PathEltOrInverseContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPathEltOrInverse(this);
}

void SparqlAutomaticParser::PathEltOrInverseContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathEltOrInverse(this);
}

antlrcpp::Any SparqlAutomaticParser::PathEltOrInverseContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathEltOrInverse(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathEltOrInverseContext*
SparqlAutomaticParser::pathEltOrInverse() {
  PathEltOrInverseContext* _localctx =
      _tracker.createInstance<PathEltOrInverseContext>(_ctx, getState());
  enterRule(_localctx, 150, SparqlAutomaticParser::RulePathEltOrInverse);

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
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__14:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(804);
        pathElt();
        break;
      }

      case SparqlAutomaticParser::T__11: {
        enterOuterAlt(_localctx, 2);
        setState(805);
        dynamic_cast<PathEltOrInverseContext*>(_localctx)->negationOperator =
            match(SparqlAutomaticParser::T__11);
        setState(806);
        pathElt();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathModContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathModContext::PathModContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t SparqlAutomaticParser::PathModContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePathMod;
}

void SparqlAutomaticParser::PathModContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPathMod(this);
}

void SparqlAutomaticParser::PathModContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathMod(this);
}

antlrcpp::Any SparqlAutomaticParser::PathModContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathMod(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathModContext* SparqlAutomaticParser::pathMod() {
  PathModContext* _localctx =
      _tracker.createInstance<PathModContext>(_ctx, getState());
  enterRule(_localctx, 152, SparqlAutomaticParser::RulePathMod);
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
    setState(809);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__0) |
                             (1ULL << SparqlAutomaticParser::T__12) |
                             (1ULL << SparqlAutomaticParser::T__13))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathPrimaryContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathPrimaryContext::PathPrimaryContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::PathPrimaryContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

SparqlAutomaticParser::PathNegatedPropertySetContext*
SparqlAutomaticParser::PathPrimaryContext::pathNegatedPropertySet() {
  return getRuleContext<SparqlAutomaticParser::PathNegatedPropertySetContext>(
      0);
}

SparqlAutomaticParser::PathContext*
SparqlAutomaticParser::PathPrimaryContext::path() {
  return getRuleContext<SparqlAutomaticParser::PathContext>(0);
}

size_t SparqlAutomaticParser::PathPrimaryContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePathPrimary;
}

void SparqlAutomaticParser::PathPrimaryContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPathPrimary(this);
}

void SparqlAutomaticParser::PathPrimaryContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathPrimary(this);
}

antlrcpp::Any SparqlAutomaticParser::PathPrimaryContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathPrimary(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathPrimaryContext*
SparqlAutomaticParser::pathPrimary() {
  PathPrimaryContext* _localctx =
      _tracker.createInstance<PathPrimaryContext>(_ctx, getState());
  enterRule(_localctx, 154, SparqlAutomaticParser::RulePathPrimary);

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
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(811);
        iri();
        break;
      }

      case SparqlAutomaticParser::T__8: {
        enterOuterAlt(_localctx, 2);
        setState(812);
        match(SparqlAutomaticParser::T__8);
        break;
      }

      case SparqlAutomaticParser::T__14: {
        enterOuterAlt(_localctx, 3);
        setState(813);
        match(SparqlAutomaticParser::T__14);
        setState(814);
        pathNegatedPropertySet();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 4);
        setState(815);
        match(SparqlAutomaticParser::T__1);
        setState(816);
        path();
        setState(817);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathNegatedPropertySetContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathNegatedPropertySetContext::
    PathNegatedPropertySetContext(ParserRuleContext* parent,
                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::PathOneInPropertySetContext*>
SparqlAutomaticParser::PathNegatedPropertySetContext::pathOneInPropertySet() {
  return getRuleContexts<SparqlAutomaticParser::PathOneInPropertySetContext>();
}

SparqlAutomaticParser::PathOneInPropertySetContext*
SparqlAutomaticParser::PathNegatedPropertySetContext::pathOneInPropertySet(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::PathOneInPropertySetContext>(i);
}

size_t SparqlAutomaticParser::PathNegatedPropertySetContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RulePathNegatedPropertySet;
}

void SparqlAutomaticParser::PathNegatedPropertySetContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathNegatedPropertySet(this);
}

void SparqlAutomaticParser::PathNegatedPropertySetContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitPathNegatedPropertySet(this);
}

antlrcpp::Any SparqlAutomaticParser::PathNegatedPropertySetContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathNegatedPropertySet(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathNegatedPropertySetContext*
SparqlAutomaticParser::pathNegatedPropertySet() {
  PathNegatedPropertySetContext* _localctx =
      _tracker.createInstance<PathNegatedPropertySetContext>(_ctx, getState());
  enterRule(_localctx, 156, SparqlAutomaticParser::RulePathNegatedPropertySet);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(834);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__11:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(821);
        pathOneInPropertySet();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(822);
        match(SparqlAutomaticParser::T__1);
        setState(831);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::T__8

            || _la == SparqlAutomaticParser::T__11 ||
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)))) !=
                 0)) {
          setState(823);
          pathOneInPropertySet();
          setState(828);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == SparqlAutomaticParser::T__9) {
            setState(824);
            match(SparqlAutomaticParser::T__9);
            setState(825);
            pathOneInPropertySet();
            setState(830);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(833);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PathOneInPropertySetContext
//------------------------------------------------------------------

SparqlAutomaticParser::PathOneInPropertySetContext::PathOneInPropertySetContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::PathOneInPropertySetContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

size_t SparqlAutomaticParser::PathOneInPropertySetContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RulePathOneInPropertySet;
}

void SparqlAutomaticParser::PathOneInPropertySetContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterPathOneInPropertySet(this);
}

void SparqlAutomaticParser::PathOneInPropertySetContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPathOneInPropertySet(this);
}

antlrcpp::Any SparqlAutomaticParser::PathOneInPropertySetContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPathOneInPropertySet(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PathOneInPropertySetContext*
SparqlAutomaticParser::pathOneInPropertySet() {
  PathOneInPropertySetContext* _localctx =
      _tracker.createInstance<PathOneInPropertySetContext>(_ctx, getState());
  enterRule(_localctx, 158, SparqlAutomaticParser::RulePathOneInPropertySet);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(843);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(836);
        iri();
        break;
      }

      case SparqlAutomaticParser::T__8: {
        enterOuterAlt(_localctx, 2);
        setState(837);
        match(SparqlAutomaticParser::T__8);
        break;
      }

      case SparqlAutomaticParser::T__11: {
        enterOuterAlt(_localctx, 3);
        setState(838);
        match(SparqlAutomaticParser::T__11);
        setState(841);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::IRI_REF:
          case SparqlAutomaticParser::PNAME_NS:
          case SparqlAutomaticParser::PNAME_LN:
          case SparqlAutomaticParser::PREFIX_LANGTAG: {
            setState(839);
            iri();
            break;
          }

          case SparqlAutomaticParser::T__8: {
            setState(840);
            match(SparqlAutomaticParser::T__8);
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

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IntegerContext
//------------------------------------------------------------------

SparqlAutomaticParser::IntegerContext::IntegerContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::IntegerContext::INTEGER() {
  return getToken(SparqlAutomaticParser::INTEGER, 0);
}

size_t SparqlAutomaticParser::IntegerContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleInteger;
}

void SparqlAutomaticParser::IntegerContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterInteger(this);
}

void SparqlAutomaticParser::IntegerContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitInteger(this);
}

antlrcpp::Any SparqlAutomaticParser::IntegerContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitInteger(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::IntegerContext* SparqlAutomaticParser::integer() {
  IntegerContext* _localctx =
      _tracker.createInstance<IntegerContext>(_ctx, getState());
  enterRule(_localctx, 160, SparqlAutomaticParser::RuleInteger);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(845);
    match(SparqlAutomaticParser::INTEGER);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesNodeContext
//------------------------------------------------------------------

SparqlAutomaticParser::TriplesNodeContext::TriplesNodeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::CollectionContext*
SparqlAutomaticParser::TriplesNodeContext::collection() {
  return getRuleContext<SparqlAutomaticParser::CollectionContext>(0);
}

SparqlAutomaticParser::BlankNodePropertyListContext*
SparqlAutomaticParser::TriplesNodeContext::blankNodePropertyList() {
  return getRuleContext<SparqlAutomaticParser::BlankNodePropertyListContext>(0);
}

size_t SparqlAutomaticParser::TriplesNodeContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTriplesNode;
}

void SparqlAutomaticParser::TriplesNodeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTriplesNode(this);
}

void SparqlAutomaticParser::TriplesNodeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTriplesNode(this);
}

antlrcpp::Any SparqlAutomaticParser::TriplesNodeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTriplesNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TriplesNodeContext*
SparqlAutomaticParser::triplesNode() {
  TriplesNodeContext* _localctx =
      _tracker.createInstance<TriplesNodeContext>(_ctx, getState());
  enterRule(_localctx, 162, SparqlAutomaticParser::RuleTriplesNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(849);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(847);
        collection();
        break;
      }

      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(848);
        blankNodePropertyList();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BlankNodePropertyListContext
//------------------------------------------------------------------

SparqlAutomaticParser::BlankNodePropertyListContext::
    BlankNodePropertyListContext(ParserRuleContext* parent,
                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PropertyListNotEmptyContext*
SparqlAutomaticParser::BlankNodePropertyListContext::propertyListNotEmpty() {
  return getRuleContext<SparqlAutomaticParser::PropertyListNotEmptyContext>(0);
}

size_t SparqlAutomaticParser::BlankNodePropertyListContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleBlankNodePropertyList;
}

void SparqlAutomaticParser::BlankNodePropertyListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNodePropertyList(this);
}

void SparqlAutomaticParser::BlankNodePropertyListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNodePropertyList(this);
}

antlrcpp::Any SparqlAutomaticParser::BlankNodePropertyListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBlankNodePropertyList(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BlankNodePropertyListContext*
SparqlAutomaticParser::blankNodePropertyList() {
  BlankNodePropertyListContext* _localctx =
      _tracker.createInstance<BlankNodePropertyListContext>(_ctx, getState());
  enterRule(_localctx, 164, SparqlAutomaticParser::RuleBlankNodePropertyList);

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
    match(SparqlAutomaticParser::T__15);
    setState(852);
    propertyListNotEmpty();
    setState(853);
    match(SparqlAutomaticParser::T__16);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TriplesNodePathContext
//------------------------------------------------------------------

SparqlAutomaticParser::TriplesNodePathContext::TriplesNodePathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::CollectionPathContext*
SparqlAutomaticParser::TriplesNodePathContext::collectionPath() {
  return getRuleContext<SparqlAutomaticParser::CollectionPathContext>(0);
}

SparqlAutomaticParser::BlankNodePropertyListPathContext*
SparqlAutomaticParser::TriplesNodePathContext::blankNodePropertyListPath() {
  return getRuleContext<
      SparqlAutomaticParser::BlankNodePropertyListPathContext>(0);
}

size_t SparqlAutomaticParser::TriplesNodePathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleTriplesNodePath;
}

void SparqlAutomaticParser::TriplesNodePathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTriplesNodePath(this);
}

void SparqlAutomaticParser::TriplesNodePathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTriplesNodePath(this);
}

antlrcpp::Any SparqlAutomaticParser::TriplesNodePathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitTriplesNodePath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::TriplesNodePathContext*
SparqlAutomaticParser::triplesNodePath() {
  TriplesNodePathContext* _localctx =
      _tracker.createInstance<TriplesNodePathContext>(_ctx, getState());
  enterRule(_localctx, 166, SparqlAutomaticParser::RuleTriplesNodePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(857);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(855);
        collectionPath();
        break;
      }

      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(856);
        blankNodePropertyListPath();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BlankNodePropertyListPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::BlankNodePropertyListPathContext::
    BlankNodePropertyListPathContext(ParserRuleContext* parent,
                                     size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PropertyListPathNotEmptyContext* SparqlAutomaticParser::
    BlankNodePropertyListPathContext::propertyListPathNotEmpty() {
  return getRuleContext<SparqlAutomaticParser::PropertyListPathNotEmptyContext>(
      0);
}

size_t SparqlAutomaticParser::BlankNodePropertyListPathContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleBlankNodePropertyListPath;
}

void SparqlAutomaticParser::BlankNodePropertyListPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBlankNodePropertyListPath(this);
}

void SparqlAutomaticParser::BlankNodePropertyListPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitBlankNodePropertyListPath(this);
}

antlrcpp::Any SparqlAutomaticParser::BlankNodePropertyListPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBlankNodePropertyListPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BlankNodePropertyListPathContext*
SparqlAutomaticParser::blankNodePropertyListPath() {
  BlankNodePropertyListPathContext* _localctx =
      _tracker.createInstance<BlankNodePropertyListPathContext>(_ctx,
                                                                getState());
  enterRule(_localctx, 168,
            SparqlAutomaticParser::RuleBlankNodePropertyListPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(859);
    match(SparqlAutomaticParser::T__15);
    setState(860);
    propertyListPathNotEmpty();
    setState(861);
    match(SparqlAutomaticParser::T__16);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CollectionContext
//------------------------------------------------------------------

SparqlAutomaticParser::CollectionContext::CollectionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::GraphNodeContext*>
SparqlAutomaticParser::CollectionContext::graphNode() {
  return getRuleContexts<SparqlAutomaticParser::GraphNodeContext>();
}

SparqlAutomaticParser::GraphNodeContext*
SparqlAutomaticParser::CollectionContext::graphNode(size_t i) {
  return getRuleContext<SparqlAutomaticParser::GraphNodeContext>(i);
}

size_t SparqlAutomaticParser::CollectionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleCollection;
}

void SparqlAutomaticParser::CollectionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterCollection(this);
}

void SparqlAutomaticParser::CollectionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitCollection(this);
}

antlrcpp::Any SparqlAutomaticParser::CollectionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitCollection(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::CollectionContext* SparqlAutomaticParser::collection() {
  CollectionContext* _localctx =
      _tracker.createInstance<CollectionContext>(_ctx, getState());
  enterRule(_localctx, 170, SparqlAutomaticParser::RuleCollection);
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
    setState(863);
    match(SparqlAutomaticParser::T__1);
    setState(865);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(864);
      graphNode();
      setState(867);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        (((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__28) |
                           (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
           (1ULL << (SparqlAutomaticParser::NIL - 139)) |
           (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0));
    setState(869);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- CollectionPathContext
//------------------------------------------------------------------

SparqlAutomaticParser::CollectionPathContext::CollectionPathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::GraphNodePathContext*>
SparqlAutomaticParser::CollectionPathContext::graphNodePath() {
  return getRuleContexts<SparqlAutomaticParser::GraphNodePathContext>();
}

SparqlAutomaticParser::GraphNodePathContext*
SparqlAutomaticParser::CollectionPathContext::graphNodePath(size_t i) {
  return getRuleContext<SparqlAutomaticParser::GraphNodePathContext>(i);
}

size_t SparqlAutomaticParser::CollectionPathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleCollectionPath;
}

void SparqlAutomaticParser::CollectionPathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterCollectionPath(this);
}

void SparqlAutomaticParser::CollectionPathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitCollectionPath(this);
}

antlrcpp::Any SparqlAutomaticParser::CollectionPathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitCollectionPath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::CollectionPathContext*
SparqlAutomaticParser::collectionPath() {
  CollectionPathContext* _localctx =
      _tracker.createInstance<CollectionPathContext>(_ctx, getState());
  enterRule(_localctx, 172, SparqlAutomaticParser::RuleCollectionPath);
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
    setState(871);
    match(SparqlAutomaticParser::T__1);
    setState(873);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(872);
      graphNodePath();
      setState(875);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        (((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__28) |
                           (1ULL << SparqlAutomaticParser::T__29))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::PREFIX_LANGTAG - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 139)) |
           (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 139)) |
           (1ULL << (SparqlAutomaticParser::NIL - 139)) |
           (1ULL << (SparqlAutomaticParser::ANON - 139)))) != 0));
    setState(877);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphNodeContext
//------------------------------------------------------------------

SparqlAutomaticParser::GraphNodeContext::GraphNodeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarOrTermContext*
SparqlAutomaticParser::GraphNodeContext::varOrTerm() {
  return getRuleContext<SparqlAutomaticParser::VarOrTermContext>(0);
}

SparqlAutomaticParser::TriplesNodeContext*
SparqlAutomaticParser::GraphNodeContext::triplesNode() {
  return getRuleContext<SparqlAutomaticParser::TriplesNodeContext>(0);
}

size_t SparqlAutomaticParser::GraphNodeContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGraphNode;
}

void SparqlAutomaticParser::GraphNodeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGraphNode(this);
}

void SparqlAutomaticParser::GraphNodeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGraphNode(this);
}

antlrcpp::Any SparqlAutomaticParser::GraphNodeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGraphNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GraphNodeContext* SparqlAutomaticParser::graphNode() {
  GraphNodeContext* _localctx =
      _tracker.createInstance<GraphNodeContext>(_ctx, getState());
  enterRule(_localctx, 174, SparqlAutomaticParser::RuleGraphNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(881);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(879);
        varOrTerm();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(880);
        triplesNode();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphNodePathContext
//------------------------------------------------------------------

SparqlAutomaticParser::GraphNodePathContext::GraphNodePathContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarOrTermContext*
SparqlAutomaticParser::GraphNodePathContext::varOrTerm() {
  return getRuleContext<SparqlAutomaticParser::VarOrTermContext>(0);
}

SparqlAutomaticParser::TriplesNodePathContext*
SparqlAutomaticParser::GraphNodePathContext::triplesNodePath() {
  return getRuleContext<SparqlAutomaticParser::TriplesNodePathContext>(0);
}

size_t SparqlAutomaticParser::GraphNodePathContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGraphNodePath;
}

void SparqlAutomaticParser::GraphNodePathContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGraphNodePath(this);
}

void SparqlAutomaticParser::GraphNodePathContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGraphNodePath(this);
}

antlrcpp::Any SparqlAutomaticParser::GraphNodePathContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGraphNodePath(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GraphNodePathContext*
SparqlAutomaticParser::graphNodePath() {
  GraphNodePathContext* _localctx =
      _tracker.createInstance<GraphNodePathContext>(_ctx, getState());
  enterRule(_localctx, 176, SparqlAutomaticParser::RuleGraphNodePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(885);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 1);
        setState(883);
        varOrTerm();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(884);
        triplesNodePath();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarOrTermContext
//------------------------------------------------------------------

SparqlAutomaticParser::VarOrTermContext::VarOrTermContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::VarOrTermContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

SparqlAutomaticParser::GraphTermContext*
SparqlAutomaticParser::VarOrTermContext::graphTerm() {
  return getRuleContext<SparqlAutomaticParser::GraphTermContext>(0);
}

size_t SparqlAutomaticParser::VarOrTermContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVarOrTerm;
}

void SparqlAutomaticParser::VarOrTermContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVarOrTerm(this);
}

void SparqlAutomaticParser::VarOrTermContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVarOrTerm(this);
}

antlrcpp::Any SparqlAutomaticParser::VarOrTermContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVarOrTerm(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VarOrTermContext* SparqlAutomaticParser::varOrTerm() {
  VarOrTermContext* _localctx =
      _tracker.createInstance<VarOrTermContext>(_ctx, getState());
  enterRule(_localctx, 178, SparqlAutomaticParser::RuleVarOrTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(889);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(887);
        var();
        break;
      }

      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 2);
        setState(888);
        graphTerm();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarOrIriContext
//------------------------------------------------------------------

SparqlAutomaticParser::VarOrIriContext::VarOrIriContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::VarOrIriContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::VarOrIriContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

size_t SparqlAutomaticParser::VarOrIriContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVarOrIri;
}

void SparqlAutomaticParser::VarOrIriContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVarOrIri(this);
}

void SparqlAutomaticParser::VarOrIriContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVarOrIri(this);
}

antlrcpp::Any SparqlAutomaticParser::VarOrIriContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVarOrIri(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VarOrIriContext* SparqlAutomaticParser::varOrIri() {
  VarOrIriContext* _localctx =
      _tracker.createInstance<VarOrIriContext>(_ctx, getState());
  enterRule(_localctx, 180, SparqlAutomaticParser::RuleVarOrIri);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(893);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(891);
        var();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 2);
        setState(892);
        iri();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- VarContext
//------------------------------------------------------------------

SparqlAutomaticParser::VarContext::VarContext(ParserRuleContext* parent,
                                              size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::VarContext::VAR1() {
  return getToken(SparqlAutomaticParser::VAR1, 0);
}

tree::TerminalNode* SparqlAutomaticParser::VarContext::VAR2() {
  return getToken(SparqlAutomaticParser::VAR2, 0);
}

size_t SparqlAutomaticParser::VarContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleVar;
}

void SparqlAutomaticParser::VarContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterVar(this);
}

void SparqlAutomaticParser::VarContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitVar(this);
}

antlrcpp::Any SparqlAutomaticParser::VarContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitVar(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::VarContext* SparqlAutomaticParser::var() {
  VarContext* _localctx = _tracker.createInstance<VarContext>(_ctx, getState());
  enterRule(_localctx, 182, SparqlAutomaticParser::RuleVar);
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
    setState(895);
    _la = _input->LA(1);
    if (!(_la == SparqlAutomaticParser::VAR1

          || _la == SparqlAutomaticParser::VAR2)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- GraphTermContext
//------------------------------------------------------------------

SparqlAutomaticParser::GraphTermContext::GraphTermContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::GraphTermContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

SparqlAutomaticParser::RdfLiteralContext*
SparqlAutomaticParser::GraphTermContext::rdfLiteral() {
  return getRuleContext<SparqlAutomaticParser::RdfLiteralContext>(0);
}

SparqlAutomaticParser::NumericLiteralContext*
SparqlAutomaticParser::GraphTermContext::numericLiteral() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralContext>(0);
}

SparqlAutomaticParser::BooleanLiteralContext*
SparqlAutomaticParser::GraphTermContext::booleanLiteral() {
  return getRuleContext<SparqlAutomaticParser::BooleanLiteralContext>(0);
}

SparqlAutomaticParser::BlankNodeContext*
SparqlAutomaticParser::GraphTermContext::blankNode() {
  return getRuleContext<SparqlAutomaticParser::BlankNodeContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::GraphTermContext::NIL() {
  return getToken(SparqlAutomaticParser::NIL, 0);
}

size_t SparqlAutomaticParser::GraphTermContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleGraphTerm;
}

void SparqlAutomaticParser::GraphTermContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterGraphTerm(this);
}

void SparqlAutomaticParser::GraphTermContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitGraphTerm(this);
}

antlrcpp::Any SparqlAutomaticParser::GraphTermContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitGraphTerm(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::GraphTermContext* SparqlAutomaticParser::graphTerm() {
  GraphTermContext* _localctx =
      _tracker.createInstance<GraphTermContext>(_ctx, getState());
  enterRule(_localctx, 184, SparqlAutomaticParser::RuleGraphTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(903);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(897);
        iri();
        break;
      }

      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 2);
        setState(898);
        rdfLiteral();
        break;
      }

      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(899);
        numericLiteral();
        break;
      }

      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29: {
        enterOuterAlt(_localctx, 4);
        setState(900);
        booleanLiteral();
        break;
      }

      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 5);
        setState(901);
        blankNode();
        break;
      }

      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 6);
        setState(902);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::ExpressionContext::ExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::ConditionalOrExpressionContext*
SparqlAutomaticParser::ExpressionContext::conditionalOrExpression() {
  return getRuleContext<SparqlAutomaticParser::ConditionalOrExpressionContext>(
      0);
}

size_t SparqlAutomaticParser::ExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleExpression;
}

void SparqlAutomaticParser::ExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterExpression(this);
}

void SparqlAutomaticParser::ExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::ExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ExpressionContext* SparqlAutomaticParser::expression() {
  ExpressionContext* _localctx =
      _tracker.createInstance<ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 186, SparqlAutomaticParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(905);
    conditionalOrExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionalOrExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::ConditionalOrExpressionContext::
    ConditionalOrExpressionContext(ParserRuleContext* parent,
                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::ConditionalAndExpressionContext*>
SparqlAutomaticParser::ConditionalOrExpressionContext::
    conditionalAndExpression() {
  return getRuleContexts<
      SparqlAutomaticParser::ConditionalAndExpressionContext>();
}

SparqlAutomaticParser::ConditionalAndExpressionContext*
SparqlAutomaticParser::ConditionalOrExpressionContext::conditionalAndExpression(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::ConditionalAndExpressionContext>(
      i);
}

size_t SparqlAutomaticParser::ConditionalOrExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleConditionalOrExpression;
}

void SparqlAutomaticParser::ConditionalOrExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionalOrExpression(this);
}

void SparqlAutomaticParser::ConditionalOrExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionalOrExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::ConditionalOrExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitConditionalOrExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ConditionalOrExpressionContext*
SparqlAutomaticParser::conditionalOrExpression() {
  ConditionalOrExpressionContext* _localctx =
      _tracker.createInstance<ConditionalOrExpressionContext>(_ctx, getState());
  enterRule(_localctx, 188, SparqlAutomaticParser::RuleConditionalOrExpression);
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
    setState(907);
    conditionalAndExpression();
    setState(912);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__17) {
      setState(908);
      match(SparqlAutomaticParser::T__17);
      setState(909);
      conditionalAndExpression();
      setState(914);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ConditionalAndExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::ConditionalAndExpressionContext::
    ConditionalAndExpressionContext(ParserRuleContext* parent,
                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::ValueLogicalContext*>
SparqlAutomaticParser::ConditionalAndExpressionContext::valueLogical() {
  return getRuleContexts<SparqlAutomaticParser::ValueLogicalContext>();
}

SparqlAutomaticParser::ValueLogicalContext*
SparqlAutomaticParser::ConditionalAndExpressionContext::valueLogical(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ValueLogicalContext>(i);
}

size_t SparqlAutomaticParser::ConditionalAndExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleConditionalAndExpression;
}

void SparqlAutomaticParser::ConditionalAndExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterConditionalAndExpression(this);
}

void SparqlAutomaticParser::ConditionalAndExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitConditionalAndExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::ConditionalAndExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitConditionalAndExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ConditionalAndExpressionContext*
SparqlAutomaticParser::conditionalAndExpression() {
  ConditionalAndExpressionContext* _localctx =
      _tracker.createInstance<ConditionalAndExpressionContext>(_ctx,
                                                               getState());
  enterRule(_localctx, 190,
            SparqlAutomaticParser::RuleConditionalAndExpression);
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
    setState(915);
    valueLogical();
    setState(920);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__18) {
      setState(916);
      match(SparqlAutomaticParser::T__18);
      setState(917);
      valueLogical();
      setState(922);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ValueLogicalContext
//------------------------------------------------------------------

SparqlAutomaticParser::ValueLogicalContext::ValueLogicalContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::RelationalExpressionContext*
SparqlAutomaticParser::ValueLogicalContext::relationalExpression() {
  return getRuleContext<SparqlAutomaticParser::RelationalExpressionContext>(0);
}

size_t SparqlAutomaticParser::ValueLogicalContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleValueLogical;
}

void SparqlAutomaticParser::ValueLogicalContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterValueLogical(this);
}

void SparqlAutomaticParser::ValueLogicalContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitValueLogical(this);
}

antlrcpp::Any SparqlAutomaticParser::ValueLogicalContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitValueLogical(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ValueLogicalContext*
SparqlAutomaticParser::valueLogical() {
  ValueLogicalContext* _localctx =
      _tracker.createInstance<ValueLogicalContext>(_ctx, getState());
  enterRule(_localctx, 192, SparqlAutomaticParser::RuleValueLogical);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(923);
    relationalExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RelationalExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::RelationalExpressionContext::RelationalExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<SparqlAutomaticParser::NumericExpressionContext*>
SparqlAutomaticParser::RelationalExpressionContext::numericExpression() {
  return getRuleContexts<SparqlAutomaticParser::NumericExpressionContext>();
}

SparqlAutomaticParser::NumericExpressionContext*
SparqlAutomaticParser::RelationalExpressionContext::numericExpression(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::NumericExpressionContext>(i);
}

tree::TerminalNode* SparqlAutomaticParser::RelationalExpressionContext::IN() {
  return getToken(SparqlAutomaticParser::IN, 0);
}

SparqlAutomaticParser::ExpressionListContext*
SparqlAutomaticParser::RelationalExpressionContext::expressionList() {
  return getRuleContext<SparqlAutomaticParser::ExpressionListContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::RelationalExpressionContext::NOT() {
  return getToken(SparqlAutomaticParser::NOT, 0);
}

size_t SparqlAutomaticParser::RelationalExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleRelationalExpression;
}

void SparqlAutomaticParser::RelationalExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterRelationalExpression(this);
}

void SparqlAutomaticParser::RelationalExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitRelationalExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::RelationalExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitRelationalExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::RelationalExpressionContext*
SparqlAutomaticParser::relationalExpression() {
  RelationalExpressionContext* _localctx =
      _tracker.createInstance<RelationalExpressionContext>(_ctx, getState());
  enterRule(_localctx, 194, SparqlAutomaticParser::RuleRelationalExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(925);
    numericExpression();
    setState(943);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__19: {
        setState(926);
        match(SparqlAutomaticParser::T__19);
        setState(927);
        numericExpression();
        break;
      }

      case SparqlAutomaticParser::T__20: {
        setState(928);
        match(SparqlAutomaticParser::T__20);
        setState(929);
        numericExpression();
        break;
      }

      case SparqlAutomaticParser::T__21: {
        setState(930);
        match(SparqlAutomaticParser::T__21);
        setState(931);
        numericExpression();
        break;
      }

      case SparqlAutomaticParser::T__22: {
        setState(932);
        match(SparqlAutomaticParser::T__22);
        setState(933);
        numericExpression();
        break;
      }

      case SparqlAutomaticParser::T__23: {
        setState(934);
        match(SparqlAutomaticParser::T__23);
        setState(935);
        numericExpression();
        break;
      }

      case SparqlAutomaticParser::T__24: {
        setState(936);
        match(SparqlAutomaticParser::T__24);
        setState(937);
        numericExpression();
        break;
      }

      case SparqlAutomaticParser::IN: {
        setState(938);
        match(SparqlAutomaticParser::IN);
        setState(939);
        expressionList();
        break;
      }

      case SparqlAutomaticParser::NOT: {
        setState(940);
        match(SparqlAutomaticParser::NOT);
        setState(941);
        match(SparqlAutomaticParser::IN);
        setState(942);
        expressionList();
        break;
      }

      case SparqlAutomaticParser::T__2:
      case SparqlAutomaticParser::T__6:
      case SparqlAutomaticParser::T__7:
      case SparqlAutomaticParser::T__17:
      case SparqlAutomaticParser::T__18:
      case SparqlAutomaticParser::AS: {
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::NumericExpressionContext::NumericExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::AdditiveExpressionContext*
SparqlAutomaticParser::NumericExpressionContext::additiveExpression() {
  return getRuleContext<SparqlAutomaticParser::AdditiveExpressionContext>(0);
}

size_t SparqlAutomaticParser::NumericExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleNumericExpression;
}

void SparqlAutomaticParser::NumericExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterNumericExpression(this);
}

void SparqlAutomaticParser::NumericExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitNumericExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::NumericExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNumericExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NumericExpressionContext*
SparqlAutomaticParser::numericExpression() {
  NumericExpressionContext* _localctx =
      _tracker.createInstance<NumericExpressionContext>(_ctx, getState());
  enterRule(_localctx, 196, SparqlAutomaticParser::RuleNumericExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(945);
    additiveExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AdditiveExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::AdditiveExpressionContext::AdditiveExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::MultiplicativeExpressionContext*
SparqlAutomaticParser::AdditiveExpressionContext::multiplicativeExpression() {
  return getRuleContext<SparqlAutomaticParser::MultiplicativeExpressionContext>(
      0);
}

std::vector<SparqlAutomaticParser::MultiplicativeExpressionWithSignContext*>
SparqlAutomaticParser::AdditiveExpressionContext::
    multiplicativeExpressionWithSign() {
  return getRuleContexts<
      SparqlAutomaticParser::MultiplicativeExpressionWithSignContext>();
}

SparqlAutomaticParser::MultiplicativeExpressionWithSignContext*
SparqlAutomaticParser::AdditiveExpressionContext::
    multiplicativeExpressionWithSign(size_t i) {
  return getRuleContext<
      SparqlAutomaticParser::MultiplicativeExpressionWithSignContext>(i);
}

size_t SparqlAutomaticParser::AdditiveExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleAdditiveExpression;
}

void SparqlAutomaticParser::AdditiveExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAdditiveExpression(this);
}

void SparqlAutomaticParser::AdditiveExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAdditiveExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::AdditiveExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitAdditiveExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::AdditiveExpressionContext*
SparqlAutomaticParser::additiveExpression() {
  AdditiveExpressionContext* _localctx =
      _tracker.createInstance<AdditiveExpressionContext>(_ctx, getState());
  enterRule(_localctx, 198, SparqlAutomaticParser::RuleAdditiveExpression);
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
    setState(947);
    multiplicativeExpression();
    setState(951);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (
        _la == SparqlAutomaticParser::T__12

        || _la == SparqlAutomaticParser::T__25 ||
        ((((_la - 150) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 150)) &
          ((1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 150)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 150)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 150)) |
           (1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 150)) |
           (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 150)) |
           (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 150)))) != 0)) {
      setState(948);
      multiplicativeExpressionWithSign();
      setState(953);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplicativeExpressionWithSignContext
//------------------------------------------------------------------

SparqlAutomaticParser::MultiplicativeExpressionWithSignContext::
    MultiplicativeExpressionWithSignContext(ParserRuleContext* parent,
                                            size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PlusSubexpressionContext* SparqlAutomaticParser::
    MultiplicativeExpressionWithSignContext::plusSubexpression() {
  return getRuleContext<SparqlAutomaticParser::PlusSubexpressionContext>(0);
}

SparqlAutomaticParser::MinusSubexpressionContext* SparqlAutomaticParser::
    MultiplicativeExpressionWithSignContext::minusSubexpression() {
  return getRuleContext<SparqlAutomaticParser::MinusSubexpressionContext>(0);
}

SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext*
SparqlAutomaticParser::MultiplicativeExpressionWithSignContext::
    multiplicativeExpressionWithLeadingSignButNoSpace() {
  return getRuleContext<
      SparqlAutomaticParser::
          MultiplicativeExpressionWithLeadingSignButNoSpaceContext>(0);
}

size_t
SparqlAutomaticParser::MultiplicativeExpressionWithSignContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleMultiplicativeExpressionWithSign;
}

void SparqlAutomaticParser::MultiplicativeExpressionWithSignContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultiplicativeExpressionWithSign(this);
}

void SparqlAutomaticParser::MultiplicativeExpressionWithSignContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultiplicativeExpressionWithSign(this);
}

antlrcpp::Any
SparqlAutomaticParser::MultiplicativeExpressionWithSignContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitMultiplicativeExpressionWithSign(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MultiplicativeExpressionWithSignContext*
SparqlAutomaticParser::multiplicativeExpressionWithSign() {
  MultiplicativeExpressionWithSignContext* _localctx =
      _tracker.createInstance<MultiplicativeExpressionWithSignContext>(
          _ctx, getState());
  enterRule(_localctx, 200,
            SparqlAutomaticParser::RuleMultiplicativeExpressionWithSign);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(959);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__12: {
        enterOuterAlt(_localctx, 1);
        setState(954);
        match(SparqlAutomaticParser::T__12);
        setState(955);
        plusSubexpression();
        break;
      }

      case SparqlAutomaticParser::T__25: {
        enterOuterAlt(_localctx, 2);
        setState(956);
        match(SparqlAutomaticParser::T__25);
        setState(957);
        minusSubexpression();
        break;
      }

      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(958);
        multiplicativeExpressionWithLeadingSignButNoSpace();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PlusSubexpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::PlusSubexpressionContext::PlusSubexpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::MultiplicativeExpressionContext*
SparqlAutomaticParser::PlusSubexpressionContext::multiplicativeExpression() {
  return getRuleContext<SparqlAutomaticParser::MultiplicativeExpressionContext>(
      0);
}

size_t SparqlAutomaticParser::PlusSubexpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePlusSubexpression;
}

void SparqlAutomaticParser::PlusSubexpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPlusSubexpression(this);
}

void SparqlAutomaticParser::PlusSubexpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPlusSubexpression(this);
}

antlrcpp::Any SparqlAutomaticParser::PlusSubexpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPlusSubexpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PlusSubexpressionContext*
SparqlAutomaticParser::plusSubexpression() {
  PlusSubexpressionContext* _localctx =
      _tracker.createInstance<PlusSubexpressionContext>(_ctx, getState());
  enterRule(_localctx, 202, SparqlAutomaticParser::RulePlusSubexpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(961);
    multiplicativeExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MinusSubexpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::MinusSubexpressionContext::MinusSubexpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::MultiplicativeExpressionContext*
SparqlAutomaticParser::MinusSubexpressionContext::multiplicativeExpression() {
  return getRuleContext<SparqlAutomaticParser::MultiplicativeExpressionContext>(
      0);
}

size_t SparqlAutomaticParser::MinusSubexpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleMinusSubexpression;
}

void SparqlAutomaticParser::MinusSubexpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterMinusSubexpression(this);
}

void SparqlAutomaticParser::MinusSubexpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitMinusSubexpression(this);
}

antlrcpp::Any SparqlAutomaticParser::MinusSubexpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitMinusSubexpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MinusSubexpressionContext*
SparqlAutomaticParser::minusSubexpression() {
  MinusSubexpressionContext* _localctx =
      _tracker.createInstance<MinusSubexpressionContext>(_ctx, getState());
  enterRule(_localctx, 204, SparqlAutomaticParser::RuleMinusSubexpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(963);
    multiplicativeExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplicativeExpressionWithLeadingSignButNoSpaceContext
//------------------------------------------------------------------

SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::
        MultiplicativeExpressionWithLeadingSignButNoSpaceContext(
            ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::NumericLiteralPositiveContext* SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::
        numericLiteralPositive() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralPositiveContext>(
      0);
}

SparqlAutomaticParser::NumericLiteralNegativeContext* SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::
        numericLiteralNegative() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralNegativeContext>(
      0);
}

std::vector<SparqlAutomaticParser::MultiplyOrDivideExpressionContext*>
SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::
        multiplyOrDivideExpression() {
  return getRuleContexts<
      SparqlAutomaticParser::MultiplyOrDivideExpressionContext>();
}

SparqlAutomaticParser::MultiplyOrDivideExpressionContext*
SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::
        multiplyOrDivideExpression(size_t i) {
  return getRuleContext<
      SparqlAutomaticParser::MultiplyOrDivideExpressionContext>(i);
}

size_t SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::getRuleIndex()
        const {
  return SparqlAutomaticParser::
      RuleMultiplicativeExpressionWithLeadingSignButNoSpace;
}

void SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::enterRule(
        tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultiplicativeExpressionWithLeadingSignButNoSpace(
        this);
}

void SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::exitRule(
        tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultiplicativeExpressionWithLeadingSignButNoSpace(this);
}

antlrcpp::Any SparqlAutomaticParser::
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext::accept(
        tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor
        ->visitMultiplicativeExpressionWithLeadingSignButNoSpace(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MultiplicativeExpressionWithLeadingSignButNoSpaceContext*
SparqlAutomaticParser::multiplicativeExpressionWithLeadingSignButNoSpace() {
  MultiplicativeExpressionWithLeadingSignButNoSpaceContext* _localctx =
      _tracker.createInstance<
          MultiplicativeExpressionWithLeadingSignButNoSpaceContext>(_ctx,
                                                                    getState());
  enterRule(_localctx, 206,
            SparqlAutomaticParser::
                RuleMultiplicativeExpressionWithLeadingSignButNoSpace);
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
    setState(967);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE: {
        setState(965);
        numericLiteralPositive();
        break;
      }

      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        setState(966);
        numericLiteralNegative();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(972);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__0

           || _la == SparqlAutomaticParser::T__10) {
      setState(969);
      multiplyOrDivideExpression();
      setState(974);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplicativeExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::MultiplicativeExpressionContext::
    MultiplicativeExpressionContext(ParserRuleContext* parent,
                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::UnaryExpressionContext*
SparqlAutomaticParser::MultiplicativeExpressionContext::unaryExpression() {
  return getRuleContext<SparqlAutomaticParser::UnaryExpressionContext>(0);
}

std::vector<SparqlAutomaticParser::MultiplyOrDivideExpressionContext*>
SparqlAutomaticParser::MultiplicativeExpressionContext::
    multiplyOrDivideExpression() {
  return getRuleContexts<
      SparqlAutomaticParser::MultiplyOrDivideExpressionContext>();
}

SparqlAutomaticParser::MultiplyOrDivideExpressionContext*
SparqlAutomaticParser::MultiplicativeExpressionContext::
    multiplyOrDivideExpression(size_t i) {
  return getRuleContext<
      SparqlAutomaticParser::MultiplyOrDivideExpressionContext>(i);
}

size_t SparqlAutomaticParser::MultiplicativeExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleMultiplicativeExpression;
}

void SparqlAutomaticParser::MultiplicativeExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultiplicativeExpression(this);
}

void SparqlAutomaticParser::MultiplicativeExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultiplicativeExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::MultiplicativeExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitMultiplicativeExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MultiplicativeExpressionContext*
SparqlAutomaticParser::multiplicativeExpression() {
  MultiplicativeExpressionContext* _localctx =
      _tracker.createInstance<MultiplicativeExpressionContext>(_ctx,
                                                               getState());
  enterRule(_localctx, 208,
            SparqlAutomaticParser::RuleMultiplicativeExpression);
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
    setState(975);
    unaryExpression();
    setState(979);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__0

           || _la == SparqlAutomaticParser::T__10) {
      setState(976);
      multiplyOrDivideExpression();
      setState(981);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplyOrDivideExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::MultiplyOrDivideExpressionContext::
    MultiplyOrDivideExpressionContext(ParserRuleContext* parent,
                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::MultiplyExpressionContext*
SparqlAutomaticParser::MultiplyOrDivideExpressionContext::multiplyExpression() {
  return getRuleContext<SparqlAutomaticParser::MultiplyExpressionContext>(0);
}

SparqlAutomaticParser::DivideExpressionContext*
SparqlAutomaticParser::MultiplyOrDivideExpressionContext::divideExpression() {
  return getRuleContext<SparqlAutomaticParser::DivideExpressionContext>(0);
}

size_t SparqlAutomaticParser::MultiplyOrDivideExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleMultiplyOrDivideExpression;
}

void SparqlAutomaticParser::MultiplyOrDivideExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterMultiplyOrDivideExpression(this);
}

void SparqlAutomaticParser::MultiplyOrDivideExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitMultiplyOrDivideExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::MultiplyOrDivideExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitMultiplyOrDivideExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MultiplyOrDivideExpressionContext*
SparqlAutomaticParser::multiplyOrDivideExpression() {
  MultiplyOrDivideExpressionContext* _localctx =
      _tracker.createInstance<MultiplyOrDivideExpressionContext>(_ctx,
                                                                 getState());
  enterRule(_localctx, 210,
            SparqlAutomaticParser::RuleMultiplyOrDivideExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(984);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__0: {
        enterOuterAlt(_localctx, 1);
        setState(982);
        multiplyExpression();
        break;
      }

      case SparqlAutomaticParser::T__10: {
        enterOuterAlt(_localctx, 2);
        setState(983);
        divideExpression();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MultiplyExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::MultiplyExpressionContext::MultiplyExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::UnaryExpressionContext*
SparqlAutomaticParser::MultiplyExpressionContext::unaryExpression() {
  return getRuleContext<SparqlAutomaticParser::UnaryExpressionContext>(0);
}

size_t SparqlAutomaticParser::MultiplyExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleMultiplyExpression;
}

void SparqlAutomaticParser::MultiplyExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterMultiplyExpression(this);
}

void SparqlAutomaticParser::MultiplyExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitMultiplyExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::MultiplyExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitMultiplyExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::MultiplyExpressionContext*
SparqlAutomaticParser::multiplyExpression() {
  MultiplyExpressionContext* _localctx =
      _tracker.createInstance<MultiplyExpressionContext>(_ctx, getState());
  enterRule(_localctx, 212, SparqlAutomaticParser::RuleMultiplyExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(986);
    match(SparqlAutomaticParser::T__0);
    setState(987);
    unaryExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- DivideExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::DivideExpressionContext::DivideExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::UnaryExpressionContext*
SparqlAutomaticParser::DivideExpressionContext::unaryExpression() {
  return getRuleContext<SparqlAutomaticParser::UnaryExpressionContext>(0);
}

size_t SparqlAutomaticParser::DivideExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleDivideExpression;
}

void SparqlAutomaticParser::DivideExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterDivideExpression(this);
}

void SparqlAutomaticParser::DivideExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitDivideExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::DivideExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitDivideExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::DivideExpressionContext*
SparqlAutomaticParser::divideExpression() {
  DivideExpressionContext* _localctx =
      _tracker.createInstance<DivideExpressionContext>(_ctx, getState());
  enterRule(_localctx, 214, SparqlAutomaticParser::RuleDivideExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(989);
    match(SparqlAutomaticParser::T__10);
    setState(990);
    unaryExpression();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- UnaryExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::UnaryExpressionContext::UnaryExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PrimaryExpressionContext*
SparqlAutomaticParser::UnaryExpressionContext::primaryExpression() {
  return getRuleContext<SparqlAutomaticParser::PrimaryExpressionContext>(0);
}

size_t SparqlAutomaticParser::UnaryExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleUnaryExpression;
}

void SparqlAutomaticParser::UnaryExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterUnaryExpression(this);
}

void SparqlAutomaticParser::UnaryExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitUnaryExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::UnaryExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitUnaryExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::UnaryExpressionContext*
SparqlAutomaticParser::unaryExpression() {
  UnaryExpressionContext* _localctx =
      _tracker.createInstance<UnaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 216, SparqlAutomaticParser::RuleUnaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(999);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__14: {
        enterOuterAlt(_localctx, 1);
        setState(992);
        match(SparqlAutomaticParser::T__14);
        setState(993);
        primaryExpression();
        break;
      }

      case SparqlAutomaticParser::T__12: {
        enterOuterAlt(_localctx, 2);
        setState(994);
        match(SparqlAutomaticParser::T__12);
        setState(995);
        primaryExpression();
        break;
      }

      case SparqlAutomaticParser::T__25: {
        enterOuterAlt(_localctx, 3);
        setState(996);
        match(SparqlAutomaticParser::T__25);
        setState(997);
        primaryExpression();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::GROUP_CONCAT:
      case SparqlAutomaticParser::NOT:
      case SparqlAutomaticParser::STR:
      case SparqlAutomaticParser::LANG:
      case SparqlAutomaticParser::LANGMATCHES:
      case SparqlAutomaticParser::DATATYPE:
      case SparqlAutomaticParser::BOUND:
      case SparqlAutomaticParser::IRI:
      case SparqlAutomaticParser::URI:
      case SparqlAutomaticParser::BNODE:
      case SparqlAutomaticParser::RAND:
      case SparqlAutomaticParser::ABS:
      case SparqlAutomaticParser::CEIL:
      case SparqlAutomaticParser::FLOOR:
      case SparqlAutomaticParser::ROUND:
      case SparqlAutomaticParser::CONCAT:
      case SparqlAutomaticParser::STRLEN:
      case SparqlAutomaticParser::UCASE:
      case SparqlAutomaticParser::LCASE:
      case SparqlAutomaticParser::ENCODE:
      case SparqlAutomaticParser::CONTAINS:
      case SparqlAutomaticParser::STRSTARTS:
      case SparqlAutomaticParser::STRENDS:
      case SparqlAutomaticParser::STRBEFORE:
      case SparqlAutomaticParser::STRAFTER:
      case SparqlAutomaticParser::YEAR:
      case SparqlAutomaticParser::MONTH:
      case SparqlAutomaticParser::DAY:
      case SparqlAutomaticParser::HOURS:
      case SparqlAutomaticParser::MINUTES:
      case SparqlAutomaticParser::SECONDS:
      case SparqlAutomaticParser::TIMEZONE:
      case SparqlAutomaticParser::TZ:
      case SparqlAutomaticParser::NOW:
      case SparqlAutomaticParser::UUID:
      case SparqlAutomaticParser::STRUUID:
      case SparqlAutomaticParser::SHA1:
      case SparqlAutomaticParser::SHA256:
      case SparqlAutomaticParser::SHA384:
      case SparqlAutomaticParser::SHA512:
      case SparqlAutomaticParser::MD5:
      case SparqlAutomaticParser::COALESCE:
      case SparqlAutomaticParser::IF:
      case SparqlAutomaticParser::STRLANG:
      case SparqlAutomaticParser::STRDT:
      case SparqlAutomaticParser::SAMETERM:
      case SparqlAutomaticParser::ISIRI:
      case SparqlAutomaticParser::ISURI:
      case SparqlAutomaticParser::ISBLANK:
      case SparqlAutomaticParser::ISLITERAL:
      case SparqlAutomaticParser::ISNUMERIC:
      case SparqlAutomaticParser::REGEX:
      case SparqlAutomaticParser::SUBSTR:
      case SparqlAutomaticParser::REPLACE:
      case SparqlAutomaticParser::EXISTS:
      case SparqlAutomaticParser::COUNT:
      case SparqlAutomaticParser::SUM:
      case SparqlAutomaticParser::MIN:
      case SparqlAutomaticParser::MAX:
      case SparqlAutomaticParser::AVG:
      case SparqlAutomaticParser::SAMPLE:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 4);
        setState(998);
        primaryExpression();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrimaryExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::PrimaryExpressionContext::PrimaryExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::BrackettedExpressionContext*
SparqlAutomaticParser::PrimaryExpressionContext::brackettedExpression() {
  return getRuleContext<SparqlAutomaticParser::BrackettedExpressionContext>(0);
}

SparqlAutomaticParser::BuiltInCallContext*
SparqlAutomaticParser::PrimaryExpressionContext::builtInCall() {
  return getRuleContext<SparqlAutomaticParser::BuiltInCallContext>(0);
}

SparqlAutomaticParser::IriOrFunctionContext*
SparqlAutomaticParser::PrimaryExpressionContext::iriOrFunction() {
  return getRuleContext<SparqlAutomaticParser::IriOrFunctionContext>(0);
}

SparqlAutomaticParser::RdfLiteralContext*
SparqlAutomaticParser::PrimaryExpressionContext::rdfLiteral() {
  return getRuleContext<SparqlAutomaticParser::RdfLiteralContext>(0);
}

SparqlAutomaticParser::NumericLiteralContext*
SparqlAutomaticParser::PrimaryExpressionContext::numericLiteral() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralContext>(0);
}

SparqlAutomaticParser::BooleanLiteralContext*
SparqlAutomaticParser::PrimaryExpressionContext::booleanLiteral() {
  return getRuleContext<SparqlAutomaticParser::BooleanLiteralContext>(0);
}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::PrimaryExpressionContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

size_t SparqlAutomaticParser::PrimaryExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePrimaryExpression;
}

void SparqlAutomaticParser::PrimaryExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPrimaryExpression(this);
}

void SparqlAutomaticParser::PrimaryExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPrimaryExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::PrimaryExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPrimaryExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PrimaryExpressionContext*
SparqlAutomaticParser::primaryExpression() {
  PrimaryExpressionContext* _localctx =
      _tracker.createInstance<PrimaryExpressionContext>(_ctx, getState());
  enterRule(_localctx, 218, SparqlAutomaticParser::RulePrimaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1008);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(1001);
        brackettedExpression();
        break;
      }

      case SparqlAutomaticParser::GROUP_CONCAT:
      case SparqlAutomaticParser::NOT:
      case SparqlAutomaticParser::STR:
      case SparqlAutomaticParser::LANG:
      case SparqlAutomaticParser::LANGMATCHES:
      case SparqlAutomaticParser::DATATYPE:
      case SparqlAutomaticParser::BOUND:
      case SparqlAutomaticParser::IRI:
      case SparqlAutomaticParser::URI:
      case SparqlAutomaticParser::BNODE:
      case SparqlAutomaticParser::RAND:
      case SparqlAutomaticParser::ABS:
      case SparqlAutomaticParser::CEIL:
      case SparqlAutomaticParser::FLOOR:
      case SparqlAutomaticParser::ROUND:
      case SparqlAutomaticParser::CONCAT:
      case SparqlAutomaticParser::STRLEN:
      case SparqlAutomaticParser::UCASE:
      case SparqlAutomaticParser::LCASE:
      case SparqlAutomaticParser::ENCODE:
      case SparqlAutomaticParser::CONTAINS:
      case SparqlAutomaticParser::STRSTARTS:
      case SparqlAutomaticParser::STRENDS:
      case SparqlAutomaticParser::STRBEFORE:
      case SparqlAutomaticParser::STRAFTER:
      case SparqlAutomaticParser::YEAR:
      case SparqlAutomaticParser::MONTH:
      case SparqlAutomaticParser::DAY:
      case SparqlAutomaticParser::HOURS:
      case SparqlAutomaticParser::MINUTES:
      case SparqlAutomaticParser::SECONDS:
      case SparqlAutomaticParser::TIMEZONE:
      case SparqlAutomaticParser::TZ:
      case SparqlAutomaticParser::NOW:
      case SparqlAutomaticParser::UUID:
      case SparqlAutomaticParser::STRUUID:
      case SparqlAutomaticParser::SHA1:
      case SparqlAutomaticParser::SHA256:
      case SparqlAutomaticParser::SHA384:
      case SparqlAutomaticParser::SHA512:
      case SparqlAutomaticParser::MD5:
      case SparqlAutomaticParser::COALESCE:
      case SparqlAutomaticParser::IF:
      case SparqlAutomaticParser::STRLANG:
      case SparqlAutomaticParser::STRDT:
      case SparqlAutomaticParser::SAMETERM:
      case SparqlAutomaticParser::ISIRI:
      case SparqlAutomaticParser::ISURI:
      case SparqlAutomaticParser::ISBLANK:
      case SparqlAutomaticParser::ISLITERAL:
      case SparqlAutomaticParser::ISNUMERIC:
      case SparqlAutomaticParser::REGEX:
      case SparqlAutomaticParser::SUBSTR:
      case SparqlAutomaticParser::REPLACE:
      case SparqlAutomaticParser::EXISTS:
      case SparqlAutomaticParser::COUNT:
      case SparqlAutomaticParser::SUM:
      case SparqlAutomaticParser::MIN:
      case SparqlAutomaticParser::MAX:
      case SparqlAutomaticParser::AVG:
      case SparqlAutomaticParser::SAMPLE: {
        enterOuterAlt(_localctx, 2);
        setState(1002);
        builtInCall();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::PREFIX_LANGTAG: {
        enterOuterAlt(_localctx, 3);
        setState(1003);
        iriOrFunction();
        break;
      }

      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 4);
        setState(1004);
        rdfLiteral();
        break;
      }

      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 5);
        setState(1005);
        numericLiteral();
        break;
      }

      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29: {
        enterOuterAlt(_localctx, 6);
        setState(1006);
        booleanLiteral();
        break;
      }

      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 7);
        setState(1007);
        var();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BrackettedExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::BrackettedExpressionContext::BrackettedExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::BrackettedExpressionContext::expression() {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(0);
}

size_t SparqlAutomaticParser::BrackettedExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleBrackettedExpression;
}

void SparqlAutomaticParser::BrackettedExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterBrackettedExpression(this);
}

void SparqlAutomaticParser::BrackettedExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitBrackettedExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::BrackettedExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBrackettedExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BrackettedExpressionContext*
SparqlAutomaticParser::brackettedExpression() {
  BrackettedExpressionContext* _localctx =
      _tracker.createInstance<BrackettedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 220, SparqlAutomaticParser::RuleBrackettedExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1010);
    match(SparqlAutomaticParser::T__1);
    setState(1011);
    expression();
    setState(1012);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BuiltInCallContext
//------------------------------------------------------------------

SparqlAutomaticParser::BuiltInCallContext::BuiltInCallContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::AggregateContext*
SparqlAutomaticParser::BuiltInCallContext::aggregate() {
  return getRuleContext<SparqlAutomaticParser::AggregateContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STR() {
  return getToken(SparqlAutomaticParser::STR, 0);
}

std::vector<SparqlAutomaticParser::ExpressionContext*>
SparqlAutomaticParser::BuiltInCallContext::expression() {
  return getRuleContexts<SparqlAutomaticParser::ExpressionContext>();
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::BuiltInCallContext::expression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(i);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::LANG() {
  return getToken(SparqlAutomaticParser::LANG, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::LANGMATCHES() {
  return getToken(SparqlAutomaticParser::LANGMATCHES, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::DATATYPE() {
  return getToken(SparqlAutomaticParser::DATATYPE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::BOUND() {
  return getToken(SparqlAutomaticParser::BOUND, 0);
}

SparqlAutomaticParser::VarContext*
SparqlAutomaticParser::BuiltInCallContext::var() {
  return getRuleContext<SparqlAutomaticParser::VarContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::IRI() {
  return getToken(SparqlAutomaticParser::IRI, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::URI() {
  return getToken(SparqlAutomaticParser::URI, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::BNODE() {
  return getToken(SparqlAutomaticParser::BNODE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::NIL() {
  return getToken(SparqlAutomaticParser::NIL, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::RAND() {
  return getToken(SparqlAutomaticParser::RAND, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ABS() {
  return getToken(SparqlAutomaticParser::ABS, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::CEIL() {
  return getToken(SparqlAutomaticParser::CEIL, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::FLOOR() {
  return getToken(SparqlAutomaticParser::FLOOR, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ROUND() {
  return getToken(SparqlAutomaticParser::ROUND, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::CONCAT() {
  return getToken(SparqlAutomaticParser::CONCAT, 0);
}

SparqlAutomaticParser::ExpressionListContext*
SparqlAutomaticParser::BuiltInCallContext::expressionList() {
  return getRuleContext<SparqlAutomaticParser::ExpressionListContext>(0);
}

SparqlAutomaticParser::SubstringExpressionContext*
SparqlAutomaticParser::BuiltInCallContext::substringExpression() {
  return getRuleContext<SparqlAutomaticParser::SubstringExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRLEN() {
  return getToken(SparqlAutomaticParser::STRLEN, 0);
}

SparqlAutomaticParser::StrReplaceExpressionContext*
SparqlAutomaticParser::BuiltInCallContext::strReplaceExpression() {
  return getRuleContext<SparqlAutomaticParser::StrReplaceExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::UCASE() {
  return getToken(SparqlAutomaticParser::UCASE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::LCASE() {
  return getToken(SparqlAutomaticParser::LCASE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ENCODE() {
  return getToken(SparqlAutomaticParser::ENCODE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::FOR() {
  return getToken(SparqlAutomaticParser::FOR, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::CONTAINS() {
  return getToken(SparqlAutomaticParser::CONTAINS, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRSTARTS() {
  return getToken(SparqlAutomaticParser::STRSTARTS, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRENDS() {
  return getToken(SparqlAutomaticParser::STRENDS, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRBEFORE() {
  return getToken(SparqlAutomaticParser::STRBEFORE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRAFTER() {
  return getToken(SparqlAutomaticParser::STRAFTER, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::YEAR() {
  return getToken(SparqlAutomaticParser::YEAR, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::MONTH() {
  return getToken(SparqlAutomaticParser::MONTH, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::DAY() {
  return getToken(SparqlAutomaticParser::DAY, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::HOURS() {
  return getToken(SparqlAutomaticParser::HOURS, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::MINUTES() {
  return getToken(SparqlAutomaticParser::MINUTES, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::SECONDS() {
  return getToken(SparqlAutomaticParser::SECONDS, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::TIMEZONE() {
  return getToken(SparqlAutomaticParser::TIMEZONE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::TZ() {
  return getToken(SparqlAutomaticParser::TZ, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::NOW() {
  return getToken(SparqlAutomaticParser::NOW, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::UUID() {
  return getToken(SparqlAutomaticParser::UUID, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRUUID() {
  return getToken(SparqlAutomaticParser::STRUUID, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::MD5() {
  return getToken(SparqlAutomaticParser::MD5, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::SHA1() {
  return getToken(SparqlAutomaticParser::SHA1, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::SHA256() {
  return getToken(SparqlAutomaticParser::SHA256, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::SHA384() {
  return getToken(SparqlAutomaticParser::SHA384, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::SHA512() {
  return getToken(SparqlAutomaticParser::SHA512, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::COALESCE() {
  return getToken(SparqlAutomaticParser::COALESCE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::IF() {
  return getToken(SparqlAutomaticParser::IF, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRLANG() {
  return getToken(SparqlAutomaticParser::STRLANG, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::STRDT() {
  return getToken(SparqlAutomaticParser::STRDT, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::SAMETERM() {
  return getToken(SparqlAutomaticParser::SAMETERM, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ISIRI() {
  return getToken(SparqlAutomaticParser::ISIRI, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ISURI() {
  return getToken(SparqlAutomaticParser::ISURI, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ISBLANK() {
  return getToken(SparqlAutomaticParser::ISBLANK, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ISLITERAL() {
  return getToken(SparqlAutomaticParser::ISLITERAL, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BuiltInCallContext::ISNUMERIC() {
  return getToken(SparqlAutomaticParser::ISNUMERIC, 0);
}

SparqlAutomaticParser::RegexExpressionContext*
SparqlAutomaticParser::BuiltInCallContext::regexExpression() {
  return getRuleContext<SparqlAutomaticParser::RegexExpressionContext>(0);
}

SparqlAutomaticParser::ExistsFuncContext*
SparqlAutomaticParser::BuiltInCallContext::existsFunc() {
  return getRuleContext<SparqlAutomaticParser::ExistsFuncContext>(0);
}

SparqlAutomaticParser::NotExistsFuncContext*
SparqlAutomaticParser::BuiltInCallContext::notExistsFunc() {
  return getRuleContext<SparqlAutomaticParser::NotExistsFuncContext>(0);
}

size_t SparqlAutomaticParser::BuiltInCallContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleBuiltInCall;
}

void SparqlAutomaticParser::BuiltInCallContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterBuiltInCall(this);
}

void SparqlAutomaticParser::BuiltInCallContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitBuiltInCall(this);
}

antlrcpp::Any SparqlAutomaticParser::BuiltInCallContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBuiltInCall(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BuiltInCallContext*
SparqlAutomaticParser::builtInCall() {
  BuiltInCallContext* _localctx =
      _tracker.createInstance<BuiltInCallContext>(_ctx, getState());
  enterRule(_localctx, 222, SparqlAutomaticParser::RuleBuiltInCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1276);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::GROUP_CONCAT:
      case SparqlAutomaticParser::COUNT:
      case SparqlAutomaticParser::SUM:
      case SparqlAutomaticParser::MIN:
      case SparqlAutomaticParser::MAX:
      case SparqlAutomaticParser::AVG:
      case SparqlAutomaticParser::SAMPLE: {
        enterOuterAlt(_localctx, 1);
        setState(1014);
        aggregate();
        break;
      }

      case SparqlAutomaticParser::STR: {
        enterOuterAlt(_localctx, 2);
        setState(1015);
        match(SparqlAutomaticParser::STR);
        setState(1016);
        match(SparqlAutomaticParser::T__1);
        setState(1017);
        expression();
        setState(1018);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::LANG: {
        enterOuterAlt(_localctx, 3);
        setState(1020);
        match(SparqlAutomaticParser::LANG);
        setState(1021);
        match(SparqlAutomaticParser::T__1);
        setState(1022);
        expression();
        setState(1023);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::LANGMATCHES: {
        enterOuterAlt(_localctx, 4);
        setState(1025);
        match(SparqlAutomaticParser::LANGMATCHES);
        setState(1026);
        match(SparqlAutomaticParser::T__1);
        setState(1027);
        expression();
        setState(1028);
        match(SparqlAutomaticParser::T__6);
        setState(1029);
        expression();
        setState(1030);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::DATATYPE: {
        enterOuterAlt(_localctx, 5);
        setState(1032);
        match(SparqlAutomaticParser::DATATYPE);
        setState(1033);
        match(SparqlAutomaticParser::T__1);
        setState(1034);
        expression();
        setState(1035);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::BOUND: {
        enterOuterAlt(_localctx, 6);
        setState(1037);
        match(SparqlAutomaticParser::BOUND);
        setState(1038);
        match(SparqlAutomaticParser::T__1);
        setState(1039);
        var();
        setState(1040);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::IRI: {
        enterOuterAlt(_localctx, 7);
        setState(1042);
        match(SparqlAutomaticParser::IRI);
        setState(1043);
        match(SparqlAutomaticParser::T__1);
        setState(1044);
        expression();
        setState(1045);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::URI: {
        enterOuterAlt(_localctx, 8);
        setState(1047);
        match(SparqlAutomaticParser::URI);
        setState(1048);
        match(SparqlAutomaticParser::T__1);
        setState(1049);
        expression();
        setState(1050);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::BNODE: {
        enterOuterAlt(_localctx, 9);
        setState(1052);
        match(SparqlAutomaticParser::BNODE);
        setState(1058);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::T__1: {
            setState(1053);
            match(SparqlAutomaticParser::T__1);
            setState(1054);
            expression();
            setState(1055);
            match(SparqlAutomaticParser::T__2);
            break;
          }

          case SparqlAutomaticParser::NIL: {
            setState(1057);
            match(SparqlAutomaticParser::NIL);
            break;
          }

          default:
            throw NoViableAltException(this);
        }
        break;
      }

      case SparqlAutomaticParser::RAND: {
        enterOuterAlt(_localctx, 10);
        setState(1060);
        match(SparqlAutomaticParser::RAND);
        setState(1061);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::ABS: {
        enterOuterAlt(_localctx, 11);
        setState(1062);
        match(SparqlAutomaticParser::ABS);
        setState(1063);
        match(SparqlAutomaticParser::T__1);
        setState(1064);
        expression();
        setState(1065);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::CEIL: {
        enterOuterAlt(_localctx, 12);
        setState(1067);
        match(SparqlAutomaticParser::CEIL);
        setState(1068);
        match(SparqlAutomaticParser::T__1);
        setState(1069);
        expression();
        setState(1070);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::FLOOR: {
        enterOuterAlt(_localctx, 13);
        setState(1072);
        match(SparqlAutomaticParser::FLOOR);
        setState(1073);
        match(SparqlAutomaticParser::T__1);
        setState(1074);
        expression();
        setState(1075);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ROUND: {
        enterOuterAlt(_localctx, 14);
        setState(1077);
        match(SparqlAutomaticParser::ROUND);
        setState(1078);
        match(SparqlAutomaticParser::T__1);
        setState(1079);
        expression();
        setState(1080);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::CONCAT: {
        enterOuterAlt(_localctx, 15);
        setState(1082);
        match(SparqlAutomaticParser::CONCAT);
        setState(1083);
        expressionList();
        break;
      }

      case SparqlAutomaticParser::SUBSTR: {
        enterOuterAlt(_localctx, 16);
        setState(1084);
        substringExpression();
        break;
      }

      case SparqlAutomaticParser::STRLEN: {
        enterOuterAlt(_localctx, 17);
        setState(1085);
        match(SparqlAutomaticParser::STRLEN);
        setState(1086);
        match(SparqlAutomaticParser::T__1);
        setState(1087);
        expression();
        setState(1088);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::REPLACE: {
        enterOuterAlt(_localctx, 18);
        setState(1090);
        strReplaceExpression();
        break;
      }

      case SparqlAutomaticParser::UCASE: {
        enterOuterAlt(_localctx, 19);
        setState(1091);
        match(SparqlAutomaticParser::UCASE);
        setState(1092);
        match(SparqlAutomaticParser::T__1);
        setState(1093);
        expression();
        setState(1094);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::LCASE: {
        enterOuterAlt(_localctx, 20);
        setState(1096);
        match(SparqlAutomaticParser::LCASE);
        setState(1097);
        match(SparqlAutomaticParser::T__1);
        setState(1098);
        expression();
        setState(1099);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ENCODE: {
        enterOuterAlt(_localctx, 21);
        setState(1101);
        match(SparqlAutomaticParser::ENCODE);
        setState(1102);
        match(SparqlAutomaticParser::T__26);
        setState(1103);
        match(SparqlAutomaticParser::FOR);
        setState(1104);
        match(SparqlAutomaticParser::T__26);
        setState(1105);
        match(SparqlAutomaticParser::URI);
        setState(1106);
        match(SparqlAutomaticParser::T__1);
        setState(1107);
        expression();
        setState(1108);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::CONTAINS: {
        enterOuterAlt(_localctx, 22);
        setState(1110);
        match(SparqlAutomaticParser::CONTAINS);
        setState(1111);
        match(SparqlAutomaticParser::T__1);
        setState(1112);
        expression();
        setState(1113);
        match(SparqlAutomaticParser::T__6);
        setState(1114);
        expression();
        setState(1115);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRSTARTS: {
        enterOuterAlt(_localctx, 23);
        setState(1117);
        match(SparqlAutomaticParser::STRSTARTS);
        setState(1118);
        match(SparqlAutomaticParser::T__1);
        setState(1119);
        expression();
        setState(1120);
        match(SparqlAutomaticParser::T__6);
        setState(1121);
        expression();
        setState(1122);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRENDS: {
        enterOuterAlt(_localctx, 24);
        setState(1124);
        match(SparqlAutomaticParser::STRENDS);
        setState(1125);
        match(SparqlAutomaticParser::T__1);
        setState(1126);
        expression();
        setState(1127);
        match(SparqlAutomaticParser::T__6);
        setState(1128);
        expression();
        setState(1129);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRBEFORE: {
        enterOuterAlt(_localctx, 25);
        setState(1131);
        match(SparqlAutomaticParser::STRBEFORE);
        setState(1132);
        match(SparqlAutomaticParser::T__1);
        setState(1133);
        expression();
        setState(1134);
        match(SparqlAutomaticParser::T__6);
        setState(1135);
        expression();
        setState(1136);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRAFTER: {
        enterOuterAlt(_localctx, 26);
        setState(1138);
        match(SparqlAutomaticParser::STRAFTER);
        setState(1139);
        match(SparqlAutomaticParser::T__1);
        setState(1140);
        expression();
        setState(1141);
        match(SparqlAutomaticParser::T__6);
        setState(1142);
        expression();
        setState(1143);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::YEAR: {
        enterOuterAlt(_localctx, 27);
        setState(1145);
        match(SparqlAutomaticParser::YEAR);
        setState(1146);
        match(SparqlAutomaticParser::T__1);
        setState(1147);
        expression();
        setState(1148);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MONTH: {
        enterOuterAlt(_localctx, 28);
        setState(1150);
        match(SparqlAutomaticParser::MONTH);
        setState(1151);
        match(SparqlAutomaticParser::T__1);
        setState(1152);
        expression();
        setState(1153);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::DAY: {
        enterOuterAlt(_localctx, 29);
        setState(1155);
        match(SparqlAutomaticParser::DAY);
        setState(1156);
        match(SparqlAutomaticParser::T__1);
        setState(1157);
        expression();
        setState(1158);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::HOURS: {
        enterOuterAlt(_localctx, 30);
        setState(1160);
        match(SparqlAutomaticParser::HOURS);
        setState(1161);
        match(SparqlAutomaticParser::T__1);
        setState(1162);
        expression();
        setState(1163);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MINUTES: {
        enterOuterAlt(_localctx, 31);
        setState(1165);
        match(SparqlAutomaticParser::MINUTES);
        setState(1166);
        match(SparqlAutomaticParser::T__1);
        setState(1167);
        expression();
        setState(1168);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SECONDS: {
        enterOuterAlt(_localctx, 32);
        setState(1170);
        match(SparqlAutomaticParser::SECONDS);
        setState(1171);
        match(SparqlAutomaticParser::T__1);
        setState(1172);
        expression();
        setState(1173);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::TIMEZONE: {
        enterOuterAlt(_localctx, 33);
        setState(1175);
        match(SparqlAutomaticParser::TIMEZONE);
        setState(1176);
        match(SparqlAutomaticParser::T__1);
        setState(1177);
        expression();
        setState(1178);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::TZ: {
        enterOuterAlt(_localctx, 34);
        setState(1180);
        match(SparqlAutomaticParser::TZ);
        setState(1181);
        match(SparqlAutomaticParser::T__1);
        setState(1182);
        expression();
        setState(1183);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::NOW: {
        enterOuterAlt(_localctx, 35);
        setState(1185);
        match(SparqlAutomaticParser::NOW);
        setState(1186);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::UUID: {
        enterOuterAlt(_localctx, 36);
        setState(1187);
        match(SparqlAutomaticParser::UUID);
        setState(1188);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::STRUUID: {
        enterOuterAlt(_localctx, 37);
        setState(1189);
        match(SparqlAutomaticParser::STRUUID);
        setState(1190);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::MD5: {
        enterOuterAlt(_localctx, 38);
        setState(1191);
        match(SparqlAutomaticParser::MD5);
        setState(1192);
        match(SparqlAutomaticParser::T__1);
        setState(1193);
        expression();
        setState(1194);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA1: {
        enterOuterAlt(_localctx, 39);
        setState(1196);
        match(SparqlAutomaticParser::SHA1);
        setState(1197);
        match(SparqlAutomaticParser::T__1);
        setState(1198);
        expression();
        setState(1199);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA256: {
        enterOuterAlt(_localctx, 40);
        setState(1201);
        match(SparqlAutomaticParser::SHA256);
        setState(1202);
        match(SparqlAutomaticParser::T__1);
        setState(1203);
        expression();
        setState(1204);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA384: {
        enterOuterAlt(_localctx, 41);
        setState(1206);
        match(SparqlAutomaticParser::SHA384);
        setState(1207);
        match(SparqlAutomaticParser::T__1);
        setState(1208);
        expression();
        setState(1209);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA512: {
        enterOuterAlt(_localctx, 42);
        setState(1211);
        match(SparqlAutomaticParser::SHA512);
        setState(1212);
        match(SparqlAutomaticParser::T__1);
        setState(1213);
        expression();
        setState(1214);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::COALESCE: {
        enterOuterAlt(_localctx, 43);
        setState(1216);
        match(SparqlAutomaticParser::COALESCE);
        setState(1217);
        expressionList();
        break;
      }

      case SparqlAutomaticParser::IF: {
        enterOuterAlt(_localctx, 44);
        setState(1218);
        match(SparqlAutomaticParser::IF);
        setState(1219);
        match(SparqlAutomaticParser::T__1);
        setState(1220);
        expression();
        setState(1221);
        match(SparqlAutomaticParser::T__6);
        setState(1222);
        expression();
        setState(1223);
        match(SparqlAutomaticParser::T__6);
        setState(1224);
        expression();
        setState(1225);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRLANG: {
        enterOuterAlt(_localctx, 45);
        setState(1227);
        match(SparqlAutomaticParser::STRLANG);
        setState(1228);
        match(SparqlAutomaticParser::T__1);
        setState(1229);
        expression();
        setState(1230);
        match(SparqlAutomaticParser::T__6);
        setState(1231);
        expression();
        setState(1232);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRDT: {
        enterOuterAlt(_localctx, 46);
        setState(1234);
        match(SparqlAutomaticParser::STRDT);
        setState(1235);
        match(SparqlAutomaticParser::T__1);
        setState(1236);
        expression();
        setState(1237);
        match(SparqlAutomaticParser::T__6);
        setState(1238);
        expression();
        setState(1239);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SAMETERM: {
        enterOuterAlt(_localctx, 47);
        setState(1241);
        match(SparqlAutomaticParser::SAMETERM);
        setState(1242);
        match(SparqlAutomaticParser::T__1);
        setState(1243);
        expression();
        setState(1244);
        match(SparqlAutomaticParser::T__6);
        setState(1245);
        expression();
        setState(1246);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISIRI: {
        enterOuterAlt(_localctx, 48);
        setState(1248);
        match(SparqlAutomaticParser::ISIRI);
        setState(1249);
        match(SparqlAutomaticParser::T__1);
        setState(1250);
        expression();
        setState(1251);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISURI: {
        enterOuterAlt(_localctx, 49);
        setState(1253);
        match(SparqlAutomaticParser::ISURI);
        setState(1254);
        match(SparqlAutomaticParser::T__1);
        setState(1255);
        expression();
        setState(1256);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISBLANK: {
        enterOuterAlt(_localctx, 50);
        setState(1258);
        match(SparqlAutomaticParser::ISBLANK);
        setState(1259);
        match(SparqlAutomaticParser::T__1);
        setState(1260);
        expression();
        setState(1261);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISLITERAL: {
        enterOuterAlt(_localctx, 51);
        setState(1263);
        match(SparqlAutomaticParser::ISLITERAL);
        setState(1264);
        match(SparqlAutomaticParser::T__1);
        setState(1265);
        expression();
        setState(1266);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISNUMERIC: {
        enterOuterAlt(_localctx, 52);
        setState(1268);
        match(SparqlAutomaticParser::ISNUMERIC);
        setState(1269);
        match(SparqlAutomaticParser::T__1);
        setState(1270);
        expression();
        setState(1271);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::REGEX: {
        enterOuterAlt(_localctx, 53);
        setState(1273);
        regexExpression();
        break;
      }

      case SparqlAutomaticParser::EXISTS: {
        enterOuterAlt(_localctx, 54);
        setState(1274);
        existsFunc();
        break;
      }

      case SparqlAutomaticParser::NOT: {
        enterOuterAlt(_localctx, 55);
        setState(1275);
        notExistsFunc();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RegexExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::RegexExpressionContext::RegexExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::RegexExpressionContext::REGEX() {
  return getToken(SparqlAutomaticParser::REGEX, 0);
}

std::vector<SparqlAutomaticParser::ExpressionContext*>
SparqlAutomaticParser::RegexExpressionContext::expression() {
  return getRuleContexts<SparqlAutomaticParser::ExpressionContext>();
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::RegexExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(i);
}

size_t SparqlAutomaticParser::RegexExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleRegexExpression;
}

void SparqlAutomaticParser::RegexExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterRegexExpression(this);
}

void SparqlAutomaticParser::RegexExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitRegexExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::RegexExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitRegexExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::RegexExpressionContext*
SparqlAutomaticParser::regexExpression() {
  RegexExpressionContext* _localctx =
      _tracker.createInstance<RegexExpressionContext>(_ctx, getState());
  enterRule(_localctx, 224, SparqlAutomaticParser::RuleRegexExpression);
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
    setState(1278);
    match(SparqlAutomaticParser::REGEX);
    setState(1279);
    match(SparqlAutomaticParser::T__1);
    setState(1280);
    expression();
    setState(1281);
    match(SparqlAutomaticParser::T__6);
    setState(1282);
    expression();
    setState(1285);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__6) {
      setState(1283);
      match(SparqlAutomaticParser::T__6);
      setState(1284);
      expression();
    }
    setState(1287);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubstringExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::SubstringExpressionContext::SubstringExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::SubstringExpressionContext::SUBSTR() {
  return getToken(SparqlAutomaticParser::SUBSTR, 0);
}

std::vector<SparqlAutomaticParser::ExpressionContext*>
SparqlAutomaticParser::SubstringExpressionContext::expression() {
  return getRuleContexts<SparqlAutomaticParser::ExpressionContext>();
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::SubstringExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(i);
}

size_t SparqlAutomaticParser::SubstringExpressionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleSubstringExpression;
}

void SparqlAutomaticParser::SubstringExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSubstringExpression(this);
}

void SparqlAutomaticParser::SubstringExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSubstringExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::SubstringExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitSubstringExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::SubstringExpressionContext*
SparqlAutomaticParser::substringExpression() {
  SubstringExpressionContext* _localctx =
      _tracker.createInstance<SubstringExpressionContext>(_ctx, getState());
  enterRule(_localctx, 226, SparqlAutomaticParser::RuleSubstringExpression);
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
    setState(1289);
    match(SparqlAutomaticParser::SUBSTR);
    setState(1290);
    match(SparqlAutomaticParser::T__1);
    setState(1291);
    expression();
    setState(1292);
    match(SparqlAutomaticParser::T__6);
    setState(1293);
    expression();
    setState(1296);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__6) {
      setState(1294);
      match(SparqlAutomaticParser::T__6);
      setState(1295);
      expression();
    }
    setState(1298);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StrReplaceExpressionContext
//------------------------------------------------------------------

SparqlAutomaticParser::StrReplaceExpressionContext::StrReplaceExpressionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::StrReplaceExpressionContext::REPLACE() {
  return getToken(SparqlAutomaticParser::REPLACE, 0);
}

std::vector<SparqlAutomaticParser::ExpressionContext*>
SparqlAutomaticParser::StrReplaceExpressionContext::expression() {
  return getRuleContexts<SparqlAutomaticParser::ExpressionContext>();
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::StrReplaceExpressionContext::expression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(i);
}

size_t SparqlAutomaticParser::StrReplaceExpressionContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleStrReplaceExpression;
}

void SparqlAutomaticParser::StrReplaceExpressionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStrReplaceExpression(this);
}

void SparqlAutomaticParser::StrReplaceExpressionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitStrReplaceExpression(this);
}

antlrcpp::Any SparqlAutomaticParser::StrReplaceExpressionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitStrReplaceExpression(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::StrReplaceExpressionContext*
SparqlAutomaticParser::strReplaceExpression() {
  StrReplaceExpressionContext* _localctx =
      _tracker.createInstance<StrReplaceExpressionContext>(_ctx, getState());
  enterRule(_localctx, 228, SparqlAutomaticParser::RuleStrReplaceExpression);
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
    match(SparqlAutomaticParser::REPLACE);
    setState(1301);
    match(SparqlAutomaticParser::T__1);
    setState(1302);
    expression();
    setState(1303);
    match(SparqlAutomaticParser::T__6);
    setState(1304);
    expression();
    setState(1305);
    match(SparqlAutomaticParser::T__6);
    setState(1306);
    expression();
    setState(1309);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__6) {
      setState(1307);
      match(SparqlAutomaticParser::T__6);
      setState(1308);
      expression();
    }
    setState(1311);
    match(SparqlAutomaticParser::T__2);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ExistsFuncContext
//------------------------------------------------------------------

SparqlAutomaticParser::ExistsFuncContext::ExistsFuncContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::ExistsFuncContext::EXISTS() {
  return getToken(SparqlAutomaticParser::EXISTS, 0);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::ExistsFuncContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

size_t SparqlAutomaticParser::ExistsFuncContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleExistsFunc;
}

void SparqlAutomaticParser::ExistsFuncContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterExistsFunc(this);
}

void SparqlAutomaticParser::ExistsFuncContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitExistsFunc(this);
}

antlrcpp::Any SparqlAutomaticParser::ExistsFuncContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitExistsFunc(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::ExistsFuncContext* SparqlAutomaticParser::existsFunc() {
  ExistsFuncContext* _localctx =
      _tracker.createInstance<ExistsFuncContext>(_ctx, getState());
  enterRule(_localctx, 230, SparqlAutomaticParser::RuleExistsFunc);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1313);
    match(SparqlAutomaticParser::EXISTS);
    setState(1314);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NotExistsFuncContext
//------------------------------------------------------------------

SparqlAutomaticParser::NotExistsFuncContext::NotExistsFuncContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::NotExistsFuncContext::NOT() {
  return getToken(SparqlAutomaticParser::NOT, 0);
}

tree::TerminalNode* SparqlAutomaticParser::NotExistsFuncContext::EXISTS() {
  return getToken(SparqlAutomaticParser::EXISTS, 0);
}

SparqlAutomaticParser::GroupGraphPatternContext*
SparqlAutomaticParser::NotExistsFuncContext::groupGraphPattern() {
  return getRuleContext<SparqlAutomaticParser::GroupGraphPatternContext>(0);
}

size_t SparqlAutomaticParser::NotExistsFuncContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleNotExistsFunc;
}

void SparqlAutomaticParser::NotExistsFuncContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterNotExistsFunc(this);
}

void SparqlAutomaticParser::NotExistsFuncContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitNotExistsFunc(this);
}

antlrcpp::Any SparqlAutomaticParser::NotExistsFuncContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNotExistsFunc(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NotExistsFuncContext*
SparqlAutomaticParser::notExistsFunc() {
  NotExistsFuncContext* _localctx =
      _tracker.createInstance<NotExistsFuncContext>(_ctx, getState());
  enterRule(_localctx, 232, SparqlAutomaticParser::RuleNotExistsFunc);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1316);
    match(SparqlAutomaticParser::NOT);
    setState(1317);
    match(SparqlAutomaticParser::EXISTS);
    setState(1318);
    groupGraphPattern();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AggregateContext
//------------------------------------------------------------------

SparqlAutomaticParser::AggregateContext::AggregateContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::COUNT() {
  return getToken(SparqlAutomaticParser::COUNT, 0);
}

SparqlAutomaticParser::ExpressionContext*
SparqlAutomaticParser::AggregateContext::expression() {
  return getRuleContext<SparqlAutomaticParser::ExpressionContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::DISTINCT() {
  return getToken(SparqlAutomaticParser::DISTINCT, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::SUM() {
  return getToken(SparqlAutomaticParser::SUM, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::MIN() {
  return getToken(SparqlAutomaticParser::MIN, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::MAX() {
  return getToken(SparqlAutomaticParser::MAX, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::AVG() {
  return getToken(SparqlAutomaticParser::AVG, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::SAMPLE() {
  return getToken(SparqlAutomaticParser::SAMPLE, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::GROUP_CONCAT() {
  return getToken(SparqlAutomaticParser::GROUP_CONCAT, 0);
}

tree::TerminalNode* SparqlAutomaticParser::AggregateContext::SEPARATOR() {
  return getToken(SparqlAutomaticParser::SEPARATOR, 0);
}

SparqlAutomaticParser::StringContext*
SparqlAutomaticParser::AggregateContext::string() {
  return getRuleContext<SparqlAutomaticParser::StringContext>(0);
}

size_t SparqlAutomaticParser::AggregateContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleAggregate;
}

void SparqlAutomaticParser::AggregateContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAggregate(this);
}

void SparqlAutomaticParser::AggregateContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAggregate(this);
}

antlrcpp::Any SparqlAutomaticParser::AggregateContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitAggregate(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::AggregateContext* SparqlAutomaticParser::aggregate() {
  AggregateContext* _localctx =
      _tracker.createInstance<AggregateContext>(_ctx, getState());
  enterRule(_localctx, 234, SparqlAutomaticParser::RuleAggregate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1384);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::COUNT: {
        enterOuterAlt(_localctx, 1);
        setState(1320);
        match(SparqlAutomaticParser::COUNT);
        setState(1321);
        match(SparqlAutomaticParser::T__1);
        setState(1323);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1322);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1327);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::T__0: {
            setState(1325);
            match(SparqlAutomaticParser::T__0);
            break;
          }

          case SparqlAutomaticParser::T__1:
          case SparqlAutomaticParser::T__12:
          case SparqlAutomaticParser::T__14:
          case SparqlAutomaticParser::T__25:
          case SparqlAutomaticParser::T__28:
          case SparqlAutomaticParser::T__29:
          case SparqlAutomaticParser::GROUP_CONCAT:
          case SparqlAutomaticParser::NOT:
          case SparqlAutomaticParser::STR:
          case SparqlAutomaticParser::LANG:
          case SparqlAutomaticParser::LANGMATCHES:
          case SparqlAutomaticParser::DATATYPE:
          case SparqlAutomaticParser::BOUND:
          case SparqlAutomaticParser::IRI:
          case SparqlAutomaticParser::URI:
          case SparqlAutomaticParser::BNODE:
          case SparqlAutomaticParser::RAND:
          case SparqlAutomaticParser::ABS:
          case SparqlAutomaticParser::CEIL:
          case SparqlAutomaticParser::FLOOR:
          case SparqlAutomaticParser::ROUND:
          case SparqlAutomaticParser::CONCAT:
          case SparqlAutomaticParser::STRLEN:
          case SparqlAutomaticParser::UCASE:
          case SparqlAutomaticParser::LCASE:
          case SparqlAutomaticParser::ENCODE:
          case SparqlAutomaticParser::CONTAINS:
          case SparqlAutomaticParser::STRSTARTS:
          case SparqlAutomaticParser::STRENDS:
          case SparqlAutomaticParser::STRBEFORE:
          case SparqlAutomaticParser::STRAFTER:
          case SparqlAutomaticParser::YEAR:
          case SparqlAutomaticParser::MONTH:
          case SparqlAutomaticParser::DAY:
          case SparqlAutomaticParser::HOURS:
          case SparqlAutomaticParser::MINUTES:
          case SparqlAutomaticParser::SECONDS:
          case SparqlAutomaticParser::TIMEZONE:
          case SparqlAutomaticParser::TZ:
          case SparqlAutomaticParser::NOW:
          case SparqlAutomaticParser::UUID:
          case SparqlAutomaticParser::STRUUID:
          case SparqlAutomaticParser::SHA1:
          case SparqlAutomaticParser::SHA256:
          case SparqlAutomaticParser::SHA384:
          case SparqlAutomaticParser::SHA512:
          case SparqlAutomaticParser::MD5:
          case SparqlAutomaticParser::COALESCE:
          case SparqlAutomaticParser::IF:
          case SparqlAutomaticParser::STRLANG:
          case SparqlAutomaticParser::STRDT:
          case SparqlAutomaticParser::SAMETERM:
          case SparqlAutomaticParser::ISIRI:
          case SparqlAutomaticParser::ISURI:
          case SparqlAutomaticParser::ISBLANK:
          case SparqlAutomaticParser::ISLITERAL:
          case SparqlAutomaticParser::ISNUMERIC:
          case SparqlAutomaticParser::REGEX:
          case SparqlAutomaticParser::SUBSTR:
          case SparqlAutomaticParser::REPLACE:
          case SparqlAutomaticParser::EXISTS:
          case SparqlAutomaticParser::COUNT:
          case SparqlAutomaticParser::SUM:
          case SparqlAutomaticParser::MIN:
          case SparqlAutomaticParser::MAX:
          case SparqlAutomaticParser::AVG:
          case SparqlAutomaticParser::SAMPLE:
          case SparqlAutomaticParser::IRI_REF:
          case SparqlAutomaticParser::PNAME_NS:
          case SparqlAutomaticParser::PNAME_LN:
          case SparqlAutomaticParser::VAR1:
          case SparqlAutomaticParser::VAR2:
          case SparqlAutomaticParser::PREFIX_LANGTAG:
          case SparqlAutomaticParser::INTEGER:
          case SparqlAutomaticParser::DECIMAL:
          case SparqlAutomaticParser::DOUBLE:
          case SparqlAutomaticParser::INTEGER_POSITIVE:
          case SparqlAutomaticParser::DECIMAL_POSITIVE:
          case SparqlAutomaticParser::DOUBLE_POSITIVE:
          case SparqlAutomaticParser::INTEGER_NEGATIVE:
          case SparqlAutomaticParser::DECIMAL_NEGATIVE:
          case SparqlAutomaticParser::DOUBLE_NEGATIVE:
          case SparqlAutomaticParser::STRING_LITERAL1:
          case SparqlAutomaticParser::STRING_LITERAL2:
          case SparqlAutomaticParser::STRING_LITERAL_LONG1:
          case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
            setState(1326);
            expression();
            break;
          }

          default:
            throw NoViableAltException(this);
        }
        setState(1329);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SUM: {
        enterOuterAlt(_localctx, 2);
        setState(1330);
        match(SparqlAutomaticParser::SUM);
        setState(1331);
        match(SparqlAutomaticParser::T__1);
        setState(1333);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1332);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1335);
        expression();
        setState(1336);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MIN: {
        enterOuterAlt(_localctx, 3);
        setState(1338);
        match(SparqlAutomaticParser::MIN);
        setState(1339);
        match(SparqlAutomaticParser::T__1);
        setState(1341);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1340);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1343);
        expression();
        setState(1344);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MAX: {
        enterOuterAlt(_localctx, 4);
        setState(1346);
        match(SparqlAutomaticParser::MAX);
        setState(1347);
        match(SparqlAutomaticParser::T__1);
        setState(1349);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1348);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1351);
        expression();
        setState(1352);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::AVG: {
        enterOuterAlt(_localctx, 5);
        setState(1354);
        match(SparqlAutomaticParser::AVG);
        setState(1355);
        match(SparqlAutomaticParser::T__1);
        setState(1357);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1356);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1359);
        expression();
        setState(1360);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SAMPLE: {
        enterOuterAlt(_localctx, 6);
        setState(1362);
        match(SparqlAutomaticParser::SAMPLE);
        setState(1363);
        match(SparqlAutomaticParser::T__1);
        setState(1365);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1364);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1367);
        expression();
        setState(1368);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::GROUP_CONCAT: {
        enterOuterAlt(_localctx, 7);
        setState(1370);
        match(SparqlAutomaticParser::GROUP_CONCAT);
        setState(1371);
        match(SparqlAutomaticParser::T__1);
        setState(1373);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1372);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1375);
        expression();
        setState(1380);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::T__7) {
          setState(1376);
          match(SparqlAutomaticParser::T__7);
          setState(1377);
          match(SparqlAutomaticParser::SEPARATOR);
          setState(1378);
          match(SparqlAutomaticParser::T__19);
          setState(1379);
          string();
        }
        setState(1382);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IriOrFunctionContext
//------------------------------------------------------------------

SparqlAutomaticParser::IriOrFunctionContext::IriOrFunctionContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::IriOrFunctionContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

SparqlAutomaticParser::ArgListContext*
SparqlAutomaticParser::IriOrFunctionContext::argList() {
  return getRuleContext<SparqlAutomaticParser::ArgListContext>(0);
}

size_t SparqlAutomaticParser::IriOrFunctionContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleIriOrFunction;
}

void SparqlAutomaticParser::IriOrFunctionContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterIriOrFunction(this);
}

void SparqlAutomaticParser::IriOrFunctionContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitIriOrFunction(this);
}

antlrcpp::Any SparqlAutomaticParser::IriOrFunctionContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitIriOrFunction(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::IriOrFunctionContext*
SparqlAutomaticParser::iriOrFunction() {
  IriOrFunctionContext* _localctx =
      _tracker.createInstance<IriOrFunctionContext>(_ctx, getState());
  enterRule(_localctx, 236, SparqlAutomaticParser::RuleIriOrFunction);
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
    setState(1386);
    iri();
    setState(1388);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__1 ||
        _la == SparqlAutomaticParser::NIL) {
      setState(1387);
      argList();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RdfLiteralContext
//------------------------------------------------------------------

SparqlAutomaticParser::RdfLiteralContext::RdfLiteralContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::StringContext*
SparqlAutomaticParser::RdfLiteralContext::string() {
  return getRuleContext<SparqlAutomaticParser::StringContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::RdfLiteralContext::LANGTAG() {
  return getToken(SparqlAutomaticParser::LANGTAG, 0);
}

SparqlAutomaticParser::IriContext*
SparqlAutomaticParser::RdfLiteralContext::iri() {
  return getRuleContext<SparqlAutomaticParser::IriContext>(0);
}

size_t SparqlAutomaticParser::RdfLiteralContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleRdfLiteral;
}

void SparqlAutomaticParser::RdfLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterRdfLiteral(this);
}

void SparqlAutomaticParser::RdfLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitRdfLiteral(this);
}

antlrcpp::Any SparqlAutomaticParser::RdfLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitRdfLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::RdfLiteralContext* SparqlAutomaticParser::rdfLiteral() {
  RdfLiteralContext* _localctx =
      _tracker.createInstance<RdfLiteralContext>(_ctx, getState());
  enterRule(_localctx, 238, SparqlAutomaticParser::RuleRdfLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1390);
    string();
    setState(1394);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::LANGTAG: {
        setState(1391);
        match(SparqlAutomaticParser::LANGTAG);
        break;
      }

      case SparqlAutomaticParser::T__27: {
        setState(1392);
        match(SparqlAutomaticParser::T__27);
        setState(1393);
        iri();
        break;
      }

      case SparqlAutomaticParser::T__0:
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__2:
      case SparqlAutomaticParser::T__3:
      case SparqlAutomaticParser::T__4:
      case SparqlAutomaticParser::T__5:
      case SparqlAutomaticParser::T__6:
      case SparqlAutomaticParser::T__7:
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__10:
      case SparqlAutomaticParser::T__11:
      case SparqlAutomaticParser::T__12:
      case SparqlAutomaticParser::T__14:
      case SparqlAutomaticParser::T__15:
      case SparqlAutomaticParser::T__16:
      case SparqlAutomaticParser::T__17:
      case SparqlAutomaticParser::T__18:
      case SparqlAutomaticParser::T__19:
      case SparqlAutomaticParser::T__20:
      case SparqlAutomaticParser::T__21:
      case SparqlAutomaticParser::T__22:
      case SparqlAutomaticParser::T__23:
      case SparqlAutomaticParser::T__24:
      case SparqlAutomaticParser::T__25:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::T__29:
      case SparqlAutomaticParser::AS:
      case SparqlAutomaticParser::VALUES:
      case SparqlAutomaticParser::GRAPH:
      case SparqlAutomaticParser::OPTIONAL:
      case SparqlAutomaticParser::SERVICE:
      case SparqlAutomaticParser::BIND:
      case SparqlAutomaticParser::UNDEF:
      case SparqlAutomaticParser::MINUS:
      case SparqlAutomaticParser::FILTER:
      case SparqlAutomaticParser::NOT:
      case SparqlAutomaticParser::IN:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::PREFIX_LANGTAG:
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE:
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE:
      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE:
      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2:
      case SparqlAutomaticParser::NIL:
      case SparqlAutomaticParser::ANON: {
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralContext
//------------------------------------------------------------------

SparqlAutomaticParser::NumericLiteralContext::NumericLiteralContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::NumericLiteralUnsignedContext*
SparqlAutomaticParser::NumericLiteralContext::numericLiteralUnsigned() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralUnsignedContext>(
      0);
}

SparqlAutomaticParser::NumericLiteralPositiveContext*
SparqlAutomaticParser::NumericLiteralContext::numericLiteralPositive() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralPositiveContext>(
      0);
}

SparqlAutomaticParser::NumericLiteralNegativeContext*
SparqlAutomaticParser::NumericLiteralContext::numericLiteralNegative() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralNegativeContext>(
      0);
}

size_t SparqlAutomaticParser::NumericLiteralContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleNumericLiteral;
}

void SparqlAutomaticParser::NumericLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterNumericLiteral(this);
}

void SparqlAutomaticParser::NumericLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitNumericLiteral(this);
}

antlrcpp::Any SparqlAutomaticParser::NumericLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNumericLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NumericLiteralContext*
SparqlAutomaticParser::numericLiteral() {
  NumericLiteralContext* _localctx =
      _tracker.createInstance<NumericLiteralContext>(_ctx, getState());
  enterRule(_localctx, 240, SparqlAutomaticParser::RuleNumericLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1399);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE: {
        enterOuterAlt(_localctx, 1);
        setState(1396);
        numericLiteralUnsigned();
        break;
      }

      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE: {
        enterOuterAlt(_localctx, 2);
        setState(1397);
        numericLiteralPositive();
        break;
      }

      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(1398);
        numericLiteralNegative();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralUnsignedContext
//------------------------------------------------------------------

SparqlAutomaticParser::NumericLiteralUnsignedContext::
    NumericLiteralUnsignedContext(ParserRuleContext* parent,
                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralUnsignedContext::INTEGER() {
  return getToken(SparqlAutomaticParser::INTEGER, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralUnsignedContext::DECIMAL() {
  return getToken(SparqlAutomaticParser::DECIMAL, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralUnsignedContext::DOUBLE() {
  return getToken(SparqlAutomaticParser::DOUBLE, 0);
}

size_t SparqlAutomaticParser::NumericLiteralUnsignedContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleNumericLiteralUnsigned;
}

void SparqlAutomaticParser::NumericLiteralUnsignedContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralUnsigned(this);
}

void SparqlAutomaticParser::NumericLiteralUnsignedContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralUnsigned(this);
}

antlrcpp::Any SparqlAutomaticParser::NumericLiteralUnsignedContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralUnsigned(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NumericLiteralUnsignedContext*
SparqlAutomaticParser::numericLiteralUnsigned() {
  NumericLiteralUnsignedContext* _localctx =
      _tracker.createInstance<NumericLiteralUnsignedContext>(_ctx, getState());
  enterRule(_localctx, 242, SparqlAutomaticParser::RuleNumericLiteralUnsigned);
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
    setState(1401);
    _la = _input->LA(1);
    if (!(((((_la - 147) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 147)) &
            ((1ULL << (SparqlAutomaticParser::INTEGER - 147)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL - 147)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE - 147)))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralPositiveContext
//------------------------------------------------------------------

SparqlAutomaticParser::NumericLiteralPositiveContext::
    NumericLiteralPositiveContext(ParserRuleContext* parent,
                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralPositiveContext::INTEGER_POSITIVE() {
  return getToken(SparqlAutomaticParser::INTEGER_POSITIVE, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralPositiveContext::DECIMAL_POSITIVE() {
  return getToken(SparqlAutomaticParser::DECIMAL_POSITIVE, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralPositiveContext::DOUBLE_POSITIVE() {
  return getToken(SparqlAutomaticParser::DOUBLE_POSITIVE, 0);
}

size_t SparqlAutomaticParser::NumericLiteralPositiveContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleNumericLiteralPositive;
}

void SparqlAutomaticParser::NumericLiteralPositiveContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralPositive(this);
}

void SparqlAutomaticParser::NumericLiteralPositiveContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralPositive(this);
}

antlrcpp::Any SparqlAutomaticParser::NumericLiteralPositiveContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralPositive(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NumericLiteralPositiveContext*
SparqlAutomaticParser::numericLiteralPositive() {
  NumericLiteralPositiveContext* _localctx =
      _tracker.createInstance<NumericLiteralPositiveContext>(_ctx, getState());
  enterRule(_localctx, 244, SparqlAutomaticParser::RuleNumericLiteralPositive);
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
    setState(1403);
    _la = _input->LA(1);
    if (!(((((_la - 150) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 150)) &
            ((1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 150)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 150)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 150)))) !=
               0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- NumericLiteralNegativeContext
//------------------------------------------------------------------

SparqlAutomaticParser::NumericLiteralNegativeContext::
    NumericLiteralNegativeContext(ParserRuleContext* parent,
                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralNegativeContext::INTEGER_NEGATIVE() {
  return getToken(SparqlAutomaticParser::INTEGER_NEGATIVE, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralNegativeContext::DECIMAL_NEGATIVE() {
  return getToken(SparqlAutomaticParser::DECIMAL_NEGATIVE, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::NumericLiteralNegativeContext::DOUBLE_NEGATIVE() {
  return getToken(SparqlAutomaticParser::DOUBLE_NEGATIVE, 0);
}

size_t SparqlAutomaticParser::NumericLiteralNegativeContext::getRuleIndex()
    const {
  return SparqlAutomaticParser::RuleNumericLiteralNegative;
}

void SparqlAutomaticParser::NumericLiteralNegativeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterNumericLiteralNegative(this);
}

void SparqlAutomaticParser::NumericLiteralNegativeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitNumericLiteralNegative(this);
}

antlrcpp::Any SparqlAutomaticParser::NumericLiteralNegativeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitNumericLiteralNegative(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::NumericLiteralNegativeContext*
SparqlAutomaticParser::numericLiteralNegative() {
  NumericLiteralNegativeContext* _localctx =
      _tracker.createInstance<NumericLiteralNegativeContext>(_ctx, getState());
  enterRule(_localctx, 246, SparqlAutomaticParser::RuleNumericLiteralNegative);
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
    setState(1405);
    _la = _input->LA(1);
    if (!(((((_la - 153) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 153)) &
            ((1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 153)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 153)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 153)))) !=
               0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BooleanLiteralContext
//------------------------------------------------------------------

SparqlAutomaticParser::BooleanLiteralContext::BooleanLiteralContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

size_t SparqlAutomaticParser::BooleanLiteralContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleBooleanLiteral;
}

void SparqlAutomaticParser::BooleanLiteralContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterBooleanLiteral(this);
}

void SparqlAutomaticParser::BooleanLiteralContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitBooleanLiteral(this);
}

antlrcpp::Any SparqlAutomaticParser::BooleanLiteralContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBooleanLiteral(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BooleanLiteralContext*
SparqlAutomaticParser::booleanLiteral() {
  BooleanLiteralContext* _localctx =
      _tracker.createInstance<BooleanLiteralContext>(_ctx, getState());
  enterRule(_localctx, 248, SparqlAutomaticParser::RuleBooleanLiteral);
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
    setState(1407);
    _la = _input->LA(1);
    if (!(_la == SparqlAutomaticParser::T__28

          || _la == SparqlAutomaticParser::T__29)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StringContext
//------------------------------------------------------------------

SparqlAutomaticParser::StringContext::StringContext(ParserRuleContext* parent,
                                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::StringContext::STRING_LITERAL1() {
  return getToken(SparqlAutomaticParser::STRING_LITERAL1, 0);
}

tree::TerminalNode* SparqlAutomaticParser::StringContext::STRING_LITERAL2() {
  return getToken(SparqlAutomaticParser::STRING_LITERAL2, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::StringContext::STRING_LITERAL_LONG1() {
  return getToken(SparqlAutomaticParser::STRING_LITERAL_LONG1, 0);
}

tree::TerminalNode*
SparqlAutomaticParser::StringContext::STRING_LITERAL_LONG2() {
  return getToken(SparqlAutomaticParser::STRING_LITERAL_LONG2, 0);
}

size_t SparqlAutomaticParser::StringContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleString;
}

void SparqlAutomaticParser::StringContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterString(this);
}

void SparqlAutomaticParser::StringContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitString(this);
}

antlrcpp::Any SparqlAutomaticParser::StringContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitString(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::StringContext* SparqlAutomaticParser::string() {
  StringContext* _localctx =
      _tracker.createInstance<StringContext>(_ctx, getState());
  enterRule(_localctx, 250, SparqlAutomaticParser::RuleString);
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
    setState(1409);
    _la = _input->LA(1);
    if (!(((((_la - 157) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 157)) &
            ((1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 157)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 157)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 157)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 157)))) !=
               0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IriContext
//------------------------------------------------------------------

SparqlAutomaticParser::IriContext::IriContext(ParserRuleContext* parent,
                                              size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::IrirefContext*
SparqlAutomaticParser::IriContext::iriref() {
  return getRuleContext<SparqlAutomaticParser::IrirefContext>(0);
}

SparqlAutomaticParser::PrefixedNameContext*
SparqlAutomaticParser::IriContext::prefixedName() {
  return getRuleContext<SparqlAutomaticParser::PrefixedNameContext>(0);
}

tree::TerminalNode* SparqlAutomaticParser::IriContext::PREFIX_LANGTAG() {
  return getToken(SparqlAutomaticParser::PREFIX_LANGTAG, 0);
}

size_t SparqlAutomaticParser::IriContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleIri;
}

void SparqlAutomaticParser::IriContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterIri(this);
}

void SparqlAutomaticParser::IriContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitIri(this);
}

antlrcpp::Any SparqlAutomaticParser::IriContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitIri(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::IriContext* SparqlAutomaticParser::iri() {
  IriContext* _localctx = _tracker.createInstance<IriContext>(_ctx, getState());
  enterRule(_localctx, 252, SparqlAutomaticParser::RuleIri);
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
    setState(1412);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::PREFIX_LANGTAG) {
      setState(1411);
      match(SparqlAutomaticParser::PREFIX_LANGTAG);
    }
    setState(1416);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF: {
        setState(1414);
        iriref();
        break;
      }

      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN: {
        setState(1415);
        prefixedName();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PrefixedNameContext
//------------------------------------------------------------------

SparqlAutomaticParser::PrefixedNameContext::PrefixedNameContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::PnameLnContext*
SparqlAutomaticParser::PrefixedNameContext::pnameLn() {
  return getRuleContext<SparqlAutomaticParser::PnameLnContext>(0);
}

SparqlAutomaticParser::PnameNsContext*
SparqlAutomaticParser::PrefixedNameContext::pnameNs() {
  return getRuleContext<SparqlAutomaticParser::PnameNsContext>(0);
}

size_t SparqlAutomaticParser::PrefixedNameContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePrefixedName;
}

void SparqlAutomaticParser::PrefixedNameContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPrefixedName(this);
}

void SparqlAutomaticParser::PrefixedNameContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPrefixedName(this);
}

antlrcpp::Any SparqlAutomaticParser::PrefixedNameContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPrefixedName(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PrefixedNameContext*
SparqlAutomaticParser::prefixedName() {
  PrefixedNameContext* _localctx =
      _tracker.createInstance<PrefixedNameContext>(_ctx, getState());
  enterRule(_localctx, 254, SparqlAutomaticParser::RulePrefixedName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1420);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::PNAME_LN: {
        enterOuterAlt(_localctx, 1);
        setState(1418);
        pnameLn();
        break;
      }

      case SparqlAutomaticParser::PNAME_NS: {
        enterOuterAlt(_localctx, 2);
        setState(1419);
        pnameNs();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- BlankNodeContext
//------------------------------------------------------------------

SparqlAutomaticParser::BlankNodeContext::BlankNodeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
SparqlAutomaticParser::BlankNodeContext::BLANK_NODE_LABEL() {
  return getToken(SparqlAutomaticParser::BLANK_NODE_LABEL, 0);
}

tree::TerminalNode* SparqlAutomaticParser::BlankNodeContext::ANON() {
  return getToken(SparqlAutomaticParser::ANON, 0);
}

size_t SparqlAutomaticParser::BlankNodeContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleBlankNode;
}

void SparqlAutomaticParser::BlankNodeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterBlankNode(this);
}

void SparqlAutomaticParser::BlankNodeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitBlankNode(this);
}

antlrcpp::Any SparqlAutomaticParser::BlankNodeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitBlankNode(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::BlankNodeContext* SparqlAutomaticParser::blankNode() {
  BlankNodeContext* _localctx =
      _tracker.createInstance<BlankNodeContext>(_ctx, getState());
  enterRule(_localctx, 256, SparqlAutomaticParser::RuleBlankNode);
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
    setState(1422);
    _la = _input->LA(1);
    if (!(_la == SparqlAutomaticParser::BLANK_NODE_LABEL

          || _la == SparqlAutomaticParser::ANON)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- IrirefContext
//------------------------------------------------------------------

SparqlAutomaticParser::IrirefContext::IrirefContext(ParserRuleContext* parent,
                                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::IrirefContext::IRI_REF() {
  return getToken(SparqlAutomaticParser::IRI_REF, 0);
}

size_t SparqlAutomaticParser::IrirefContext::getRuleIndex() const {
  return SparqlAutomaticParser::RuleIriref;
}

void SparqlAutomaticParser::IrirefContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterIriref(this);
}

void SparqlAutomaticParser::IrirefContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitIriref(this);
}

antlrcpp::Any SparqlAutomaticParser::IrirefContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitIriref(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::IrirefContext* SparqlAutomaticParser::iriref() {
  IrirefContext* _localctx =
      _tracker.createInstance<IrirefContext>(_ctx, getState());
  enterRule(_localctx, 258, SparqlAutomaticParser::RuleIriref);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1424);
    match(SparqlAutomaticParser::IRI_REF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PnameLnContext
//------------------------------------------------------------------

SparqlAutomaticParser::PnameLnContext::PnameLnContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::PnameLnContext::PNAME_LN() {
  return getToken(SparqlAutomaticParser::PNAME_LN, 0);
}

size_t SparqlAutomaticParser::PnameLnContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePnameLn;
}

void SparqlAutomaticParser::PnameLnContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPnameLn(this);
}

void SparqlAutomaticParser::PnameLnContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPnameLn(this);
}

antlrcpp::Any SparqlAutomaticParser::PnameLnContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPnameLn(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PnameLnContext* SparqlAutomaticParser::pnameLn() {
  PnameLnContext* _localctx =
      _tracker.createInstance<PnameLnContext>(_ctx, getState());
  enterRule(_localctx, 260, SparqlAutomaticParser::RulePnameLn);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1426);
    match(SparqlAutomaticParser::PNAME_LN);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PnameNsContext
//------------------------------------------------------------------

SparqlAutomaticParser::PnameNsContext::PnameNsContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* SparqlAutomaticParser::PnameNsContext::PNAME_NS() {
  return getToken(SparqlAutomaticParser::PNAME_NS, 0);
}

size_t SparqlAutomaticParser::PnameNsContext::getRuleIndex() const {
  return SparqlAutomaticParser::RulePnameNs;
}

void SparqlAutomaticParser::PnameNsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->enterPnameNs(this);
}

void SparqlAutomaticParser::PnameNsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr) parserListener->exitPnameNs(this);
}

antlrcpp::Any SparqlAutomaticParser::PnameNsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitPnameNs(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::PnameNsContext* SparqlAutomaticParser::pnameNs() {
  PnameNsContext* _localctx =
      _tracker.createInstance<PnameNsContext>(_ctx, getState());
  enterRule(_localctx, 262, SparqlAutomaticParser::RulePnameNs);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1428);
    match(SparqlAutomaticParser::PNAME_NS);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> SparqlAutomaticParser::_decisionToDFA;
atn::PredictionContextCache SparqlAutomaticParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN SparqlAutomaticParser::_atn;
std::vector<uint16_t> SparqlAutomaticParser::_serializedATN;

std::vector<std::string> SparqlAutomaticParser::_ruleNames = {
    "query",
    "prologue",
    "baseDecl",
    "prefixDecl",
    "selectQuery",
    "subSelect",
    "selectClause",
    "varOrAlias",
    "alias",
    "aliasWithoutBrackets",
    "constructQuery",
    "describeQuery",
    "askQuery",
    "datasetClause",
    "defaultGraphClause",
    "namedGraphClause",
    "sourceSelector",
    "whereClause",
    "solutionModifier",
    "groupClause",
    "groupCondition",
    "havingClause",
    "havingCondition",
    "orderClause",
    "orderCondition",
    "limitOffsetClauses",
    "limitClause",
    "offsetClause",
    "textLimitClause",
    "valuesClause",
    "triplesTemplate",
    "groupGraphPattern",
    "groupGraphPatternSub",
    "graphPatternNotTriplesAndMaybeTriples",
    "triplesBlock",
    "graphPatternNotTriples",
    "optionalGraphPattern",
    "graphGraphPattern",
    "serviceGraphPattern",
    "bind",
    "inlineData",
    "dataBlock",
    "inlineDataOneVar",
    "inlineDataFull",
    "dataBlockSingle",
    "dataBlockValue",
    "minusGraphPattern",
    "groupOrUnionGraphPattern",
    "filterR",
    "constraint",
    "functionCall",
    "argList",
    "expressionList",
    "constructTemplate",
    "constructTriples",
    "triplesSameSubject",
    "propertyList",
    "propertyListNotEmpty",
    "verb",
    "objectList",
    "objectR",
    "triplesSameSubjectPath",
    "propertyListPath",
    "propertyListPathNotEmpty",
    "verbPath",
    "verbSimple",
    "tupleWithoutPath",
    "tupleWithPath",
    "verbPathOrSimple",
    "objectListPath",
    "objectPath",
    "path",
    "pathAlternative",
    "pathSequence",
    "pathElt",
    "pathEltOrInverse",
    "pathMod",
    "pathPrimary",
    "pathNegatedPropertySet",
    "pathOneInPropertySet",
    "integer",
    "triplesNode",
    "blankNodePropertyList",
    "triplesNodePath",
    "blankNodePropertyListPath",
    "collection",
    "collectionPath",
    "graphNode",
    "graphNodePath",
    "varOrTerm",
    "varOrIri",
    "var",
    "graphTerm",
    "expression",
    "conditionalOrExpression",
    "conditionalAndExpression",
    "valueLogical",
    "relationalExpression",
    "numericExpression",
    "additiveExpression",
    "multiplicativeExpressionWithSign",
    "plusSubexpression",
    "minusSubexpression",
    "multiplicativeExpressionWithLeadingSignButNoSpace",
    "multiplicativeExpression",
    "multiplyOrDivideExpression",
    "multiplyExpression",
    "divideExpression",
    "unaryExpression",
    "primaryExpression",
    "brackettedExpression",
    "builtInCall",
    "regexExpression",
    "substringExpression",
    "strReplaceExpression",
    "existsFunc",
    "notExistsFunc",
    "aggregate",
    "iriOrFunction",
    "rdfLiteral",
    "numericLiteral",
    "numericLiteralUnsigned",
    "numericLiteralPositive",
    "numericLiteralNegative",
    "booleanLiteral",
    "string",
    "iri",
    "prefixedName",
    "blankNode",
    "iriref",
    "pnameLn",
    "pnameNs"};

std::vector<std::string> SparqlAutomaticParser::_literalNames = {
    "",     "'*'",  "'('",  "')'",  "'{'",  "'}'",    "'.'",      "','",
    "';'",  "'a'",  "'|'",  "'/'",  "'^'",  "'+'",    "'\u003F'", "'!'",
    "'['",  "']'",  "'||'", "'&&'", "'='",  "'!='",   "'<'",      "'>'",
    "'<='", "'>='", "'-'",  "'_'",  "'^^'", "'true'", "'false'"};

std::vector<std::string> SparqlAutomaticParser::_symbolicNames = {
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "BASE",
    "PREFIX",
    "SELECT",
    "DISTINCT",
    "REDUCED",
    "AS",
    "CONSTRUCT",
    "WHERE",
    "DESCRIBE",
    "ASK",
    "FROM",
    "NAMED",
    "GROUPBY",
    "GROUP_CONCAT",
    "HAVING",
    "ORDERBY",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "TEXTLIMIT",
    "VALUES",
    "LOAD",
    "SILENT",
    "CLEAR",
    "DROP",
    "CREATE",
    "ADD",
    "DATA",
    "MOVE",
    "COPY",
    "INSERT",
    "DELETE",
    "WITH",
    "USING",
    "DEFAULT",
    "GRAPH",
    "ALL",
    "OPTIONAL",
    "SERVICE",
    "BIND",
    "UNDEF",
    "MINUS",
    "UNION",
    "FILTER",
    "NOT",
    "IN",
    "STR",
    "LANG",
    "LANGMATCHES",
    "DATATYPE",
    "BOUND",
    "IRI",
    "URI",
    "BNODE",
    "RAND",
    "ABS",
    "CEIL",
    "FLOOR",
    "ROUND",
    "CONCAT",
    "STRLEN",
    "UCASE",
    "LCASE",
    "ENCODE",
    "FOR",
    "CONTAINS",
    "STRSTARTS",
    "STRENDS",
    "STRBEFORE",
    "STRAFTER",
    "YEAR",
    "MONTH",
    "DAY",
    "HOURS",
    "MINUTES",
    "SECONDS",
    "TIMEZONE",
    "TZ",
    "NOW",
    "UUID",
    "STRUUID",
    "SHA1",
    "SHA256",
    "SHA384",
    "SHA512",
    "MD5",
    "COALESCE",
    "IF",
    "STRLANG",
    "STRDT",
    "SAMETERM",
    "ISIRI",
    "ISURI",
    "ISBLANK",
    "ISLITERAL",
    "ISNUMERIC",
    "REGEX",
    "SUBSTR",
    "REPLACE",
    "EXISTS",
    "COUNT",
    "SUM",
    "MIN",
    "MAX",
    "AVG",
    "SAMPLE",
    "SEPARATOR",
    "IRI_REF",
    "PNAME_NS",
    "PNAME_LN",
    "BLANK_NODE_LABEL",
    "VAR1",
    "VAR2",
    "LANGTAG",
    "PREFIX_LANGTAG",
    "INTEGER",
    "DECIMAL",
    "DOUBLE",
    "INTEGER_POSITIVE",
    "DECIMAL_POSITIVE",
    "DOUBLE_POSITIVE",
    "INTEGER_NEGATIVE",
    "DECIMAL_NEGATIVE",
    "DOUBLE_NEGATIVE",
    "EXPONENT",
    "STRING_LITERAL1",
    "STRING_LITERAL2",
    "STRING_LITERAL_LONG1",
    "STRING_LITERAL_LONG2",
    "ECHAR",
    "NIL",
    "ANON",
    "PN_CHARS_U",
    "VARNAME",
    "PN_PREFIX",
    "PN_LOCAL",
    "PLX",
    "PERCENT",
    "HEX",
    "PN_LOCAL_ESC",
    "WS",
    "COMMENTS"};

dfa::Vocabulary SparqlAutomaticParser::_vocabulary(_literalNames,
                                                   _symbolicNames);

std::vector<std::string> SparqlAutomaticParser::_tokenNames;

SparqlAutomaticParser::Initializer::Initializer() {
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
      0x3,   0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964,
      0x3,   0xaf,   0x599,  0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
      0x9,   0x3,    0x4,    0x4,    0x9,    0x4,    0x4,    0x5,    0x9,
      0x5,   0x4,    0x6,    0x9,    0x6,    0x4,    0x7,    0x9,    0x7,
      0x4,   0x8,    0x9,    0x8,    0x4,    0x9,    0x9,    0x9,    0x4,
      0xa,   0x9,    0xa,    0x4,    0xb,    0x9,    0xb,    0x4,    0xc,
      0x9,   0xc,    0x4,    0xd,    0x9,    0xd,    0x4,    0xe,    0x9,
      0xe,   0x4,    0xf,    0x9,    0xf,    0x4,    0x10,   0x9,    0x10,
      0x4,   0x11,   0x9,    0x11,   0x4,    0x12,   0x9,    0x12,   0x4,
      0x13,  0x9,    0x13,   0x4,    0x14,   0x9,    0x14,   0x4,    0x15,
      0x9,   0x15,   0x4,    0x16,   0x9,    0x16,   0x4,    0x17,   0x9,
      0x17,  0x4,    0x18,   0x9,    0x18,   0x4,    0x19,   0x9,    0x19,
      0x4,   0x1a,   0x9,    0x1a,   0x4,    0x1b,   0x9,    0x1b,   0x4,
      0x1c,  0x9,    0x1c,   0x4,    0x1d,   0x9,    0x1d,   0x4,    0x1e,
      0x9,   0x1e,   0x4,    0x1f,   0x9,    0x1f,   0x4,    0x20,   0x9,
      0x20,  0x4,    0x21,   0x9,    0x21,   0x4,    0x22,   0x9,    0x22,
      0x4,   0x23,   0x9,    0x23,   0x4,    0x24,   0x9,    0x24,   0x4,
      0x25,  0x9,    0x25,   0x4,    0x26,   0x9,    0x26,   0x4,    0x27,
      0x9,   0x27,   0x4,    0x28,   0x9,    0x28,   0x4,    0x29,   0x9,
      0x29,  0x4,    0x2a,   0x9,    0x2a,   0x4,    0x2b,   0x9,    0x2b,
      0x4,   0x2c,   0x9,    0x2c,   0x4,    0x2d,   0x9,    0x2d,   0x4,
      0x2e,  0x9,    0x2e,   0x4,    0x2f,   0x9,    0x2f,   0x4,    0x30,
      0x9,   0x30,   0x4,    0x31,   0x9,    0x31,   0x4,    0x32,   0x9,
      0x32,  0x4,    0x33,   0x9,    0x33,   0x4,    0x34,   0x9,    0x34,
      0x4,   0x35,   0x9,    0x35,   0x4,    0x36,   0x9,    0x36,   0x4,
      0x37,  0x9,    0x37,   0x4,    0x38,   0x9,    0x38,   0x4,    0x39,
      0x9,   0x39,   0x4,    0x3a,   0x9,    0x3a,   0x4,    0x3b,   0x9,
      0x3b,  0x4,    0x3c,   0x9,    0x3c,   0x4,    0x3d,   0x9,    0x3d,
      0x4,   0x3e,   0x9,    0x3e,   0x4,    0x3f,   0x9,    0x3f,   0x4,
      0x40,  0x9,    0x40,   0x4,    0x41,   0x9,    0x41,   0x4,    0x42,
      0x9,   0x42,   0x4,    0x43,   0x9,    0x43,   0x4,    0x44,   0x9,
      0x44,  0x4,    0x45,   0x9,    0x45,   0x4,    0x46,   0x9,    0x46,
      0x4,   0x47,   0x9,    0x47,   0x4,    0x48,   0x9,    0x48,   0x4,
      0x49,  0x9,    0x49,   0x4,    0x4a,   0x9,    0x4a,   0x4,    0x4b,
      0x9,   0x4b,   0x4,    0x4c,   0x9,    0x4c,   0x4,    0x4d,   0x9,
      0x4d,  0x4,    0x4e,   0x9,    0x4e,   0x4,    0x4f,   0x9,    0x4f,
      0x4,   0x50,   0x9,    0x50,   0x4,    0x51,   0x9,    0x51,   0x4,
      0x52,  0x9,    0x52,   0x4,    0x53,   0x9,    0x53,   0x4,    0x54,
      0x9,   0x54,   0x4,    0x55,   0x9,    0x55,   0x4,    0x56,   0x9,
      0x56,  0x4,    0x57,   0x9,    0x57,   0x4,    0x58,   0x9,    0x58,
      0x4,   0x59,   0x9,    0x59,   0x4,    0x5a,   0x9,    0x5a,   0x4,
      0x5b,  0x9,    0x5b,   0x4,    0x5c,   0x9,    0x5c,   0x4,    0x5d,
      0x9,   0x5d,   0x4,    0x5e,   0x9,    0x5e,   0x4,    0x5f,   0x9,
      0x5f,  0x4,    0x60,   0x9,    0x60,   0x4,    0x61,   0x9,    0x61,
      0x4,   0x62,   0x9,    0x62,   0x4,    0x63,   0x9,    0x63,   0x4,
      0x64,  0x9,    0x64,   0x4,    0x65,   0x9,    0x65,   0x4,    0x66,
      0x9,   0x66,   0x4,    0x67,   0x9,    0x67,   0x4,    0x68,   0x9,
      0x68,  0x4,    0x69,   0x9,    0x69,   0x4,    0x6a,   0x9,    0x6a,
      0x4,   0x6b,   0x9,    0x6b,   0x4,    0x6c,   0x9,    0x6c,   0x4,
      0x6d,  0x9,    0x6d,   0x4,    0x6e,   0x9,    0x6e,   0x4,    0x6f,
      0x9,   0x6f,   0x4,    0x70,   0x9,    0x70,   0x4,    0x71,   0x9,
      0x71,  0x4,    0x72,   0x9,    0x72,   0x4,    0x73,   0x9,    0x73,
      0x4,   0x74,   0x9,    0x74,   0x4,    0x75,   0x9,    0x75,   0x4,
      0x76,  0x9,    0x76,   0x4,    0x77,   0x9,    0x77,   0x4,    0x78,
      0x9,   0x78,   0x4,    0x79,   0x9,    0x79,   0x4,    0x7a,   0x9,
      0x7a,  0x4,    0x7b,   0x9,    0x7b,   0x4,    0x7c,   0x9,    0x7c,
      0x4,   0x7d,   0x9,    0x7d,   0x4,    0x7e,   0x9,    0x7e,   0x4,
      0x7f,  0x9,    0x7f,   0x4,    0x80,   0x9,    0x80,   0x4,    0x81,
      0x9,   0x81,   0x4,    0x82,   0x9,    0x82,   0x4,    0x83,   0x9,
      0x83,  0x4,    0x84,   0x9,    0x84,   0x4,    0x85,   0x9,    0x85,
      0x3,   0x2,    0x3,    0x2,    0x3,    0x2,    0x3,    0x2,    0x3,
      0x2,   0x5,    0x2,    0x110,  0xa,    0x2,    0x3,    0x2,    0x3,
      0x2,   0x3,    0x2,    0x3,    0x3,    0x3,    0x3,    0x7,    0x3,
      0x117, 0xa,    0x3,    0xc,    0x3,    0xe,    0x3,    0x11a,  0xb,
      0x3,   0x3,    0x4,    0x3,    0x4,    0x3,    0x4,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x6,    0x3,
      0x6,   0x7,    0x6,    0x125,  0xa,    0x6,    0xc,    0x6,    0xe,
      0x6,   0x128,  0xb,    0x6,    0x3,    0x6,    0x3,    0x6,    0x3,
      0x6,   0x3,    0x7,    0x3,    0x7,    0x3,    0x7,    0x3,    0x7,
      0x3,   0x7,    0x3,    0x8,    0x3,    0x8,    0x5,    0x8,    0x134,
      0xa,   0x8,    0x3,    0x8,    0x6,    0x8,    0x137,  0xa,    0x8,
      0xd,   0x8,    0xe,    0x8,    0x138,  0x3,    0x8,    0x5,    0x8,
      0x13c, 0xa,    0x8,    0x3,    0x9,    0x3,    0x9,    0x5,    0x9,
      0x140, 0xa,    0x9,    0x3,    0xa,    0x3,    0xa,    0x3,    0xa,
      0x3,   0xa,    0x3,    0xb,    0x3,    0xb,    0x3,    0xb,    0x3,
      0xb,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x7,    0xc,
      0x14d, 0xa,    0xc,    0xc,    0xc,    0xe,    0xc,    0x150,  0xb,
      0xc,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x7,   0xc,    0x156,  0xa,    0xc,    0xc,    0xc,    0xe,    0xc,
      0x159, 0xb,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x5,   0xc,    0x15e,  0xa,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x5,   0xc,    0x162,  0xa,    0xc,    0x3,    0xd,    0x3,    0xd,
      0x6,   0xd,    0x166,  0xa,    0xd,    0xd,    0xd,    0xe,    0xd,
      0x167, 0x3,    0xd,    0x5,    0xd,    0x16b,  0xa,    0xd,    0x3,
      0xd,   0x7,    0xd,    0x16e,  0xa,    0xd,    0xc,    0xd,    0xe,
      0xd,   0x171,  0xb,    0xd,    0x3,    0xd,    0x5,    0xd,    0x174,
      0xa,   0xd,    0x3,    0xd,    0x3,    0xd,    0x3,    0xe,    0x3,
      0xe,   0x7,    0xe,    0x17a,  0xa,    0xe,    0xc,    0xe,    0xe,
      0xe,   0x17d,  0xb,    0xe,    0x3,    0xe,    0x3,    0xe,    0x3,
      0xe,   0x3,    0xf,    0x3,    0xf,    0x3,    0xf,    0x5,    0xf,
      0x185, 0xa,    0xf,    0x3,    0x10,   0x3,    0x10,   0x3,    0x11,
      0x3,   0x11,   0x3,    0x11,   0x3,    0x12,   0x3,    0x12,   0x3,
      0x13,  0x5,    0x13,   0x18f,  0xa,    0x13,   0x3,    0x13,   0x3,
      0x13,  0x3,    0x14,   0x5,    0x14,   0x194,  0xa,    0x14,   0x3,
      0x14,  0x5,    0x14,   0x197,  0xa,    0x14,   0x3,    0x14,   0x5,
      0x14,  0x19a,  0xa,    0x14,   0x3,    0x14,   0x5,    0x14,   0x19d,
      0xa,   0x14,   0x3,    0x15,   0x3,    0x15,   0x6,    0x15,   0x1a1,
      0xa,   0x15,   0xd,    0x15,   0xe,    0x15,   0x1a2,  0x3,    0x16,
      0x3,   0x16,   0x3,    0x16,   0x3,    0x16,   0x3,    0x16,   0x3,
      0x16,  0x5,    0x16,   0x1ab,  0xa,    0x16,   0x3,    0x16,   0x3,
      0x16,  0x3,    0x16,   0x5,    0x16,   0x1b0,  0xa,    0x16,   0x3,
      0x17,  0x3,    0x17,   0x6,    0x17,   0x1b4,  0xa,    0x17,   0xd,
      0x17,  0xe,    0x17,   0x1b5,  0x3,    0x18,   0x3,    0x18,   0x3,
      0x19,  0x3,    0x19,   0x6,    0x19,   0x1bc,  0xa,    0x19,   0xd,
      0x19,  0xe,    0x19,   0x1bd,  0x3,    0x1a,   0x3,    0x1a,   0x3,
      0x1a,  0x3,    0x1a,   0x5,    0x1a,   0x1c4,  0xa,    0x1a,   0x5,
      0x1a,  0x1c6,  0xa,    0x1a,   0x3,    0x1b,   0x3,    0x1b,   0x5,
      0x1b,  0x1ca,  0xa,    0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1cd,
      0xa,   0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1d1,
      0xa,   0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1d4,  0xa,    0x1b,
      0x3,   0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1d8,  0xa,    0x1b,
      0x3,   0x1b,   0x5,    0x1b,   0x1db,  0xa,    0x1b,   0x3,    0x1b,
      0x3,   0x1b,   0x5,    0x1b,   0x1df,  0xa,    0x1b,   0x3,    0x1b,
      0x5,   0x1b,   0x1e2,  0xa,    0x1b,   0x3,    0x1b,   0x3,    0x1b,
      0x5,   0x1b,   0x1e6,  0xa,    0x1b,   0x3,    0x1b,   0x5,    0x1b,
      0x1e9, 0xa,    0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x5,    0x1b,
      0x1ed, 0xa,    0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1f0,  0xa,
      0x1b,  0x5,    0x1b,   0x1f2,  0xa,    0x1b,   0x3,    0x1c,   0x3,
      0x1c,  0x3,    0x1c,   0x3,    0x1d,   0x3,    0x1d,   0x3,    0x1d,
      0x3,   0x1e,   0x3,    0x1e,   0x3,    0x1e,   0x3,    0x1f,   0x3,
      0x1f,  0x5,    0x1f,   0x1ff,  0xa,    0x1f,   0x3,    0x20,   0x3,
      0x20,  0x3,    0x20,   0x5,    0x20,   0x204,  0xa,    0x20,   0x5,
      0x20,  0x206,  0xa,    0x20,   0x3,    0x21,   0x3,    0x21,   0x3,
      0x21,  0x5,    0x21,   0x20b,  0xa,    0x21,   0x3,    0x21,   0x3,
      0x21,  0x3,    0x22,   0x5,    0x22,   0x210,  0xa,    0x22,   0x3,
      0x22,  0x7,    0x22,   0x213,  0xa,    0x22,   0xc,    0x22,   0xe,
      0x22,  0x216,  0xb,    0x22,   0x3,    0x23,   0x3,    0x23,   0x5,
      0x23,  0x21a,  0xa,    0x23,   0x3,    0x23,   0x5,    0x23,   0x21d,
      0xa,   0x23,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x5,
      0x24,  0x222,  0xa,    0x24,   0x5,    0x24,   0x224,  0xa,    0x24,
      0x3,   0x25,   0x3,    0x25,   0x3,    0x25,   0x3,    0x25,   0x3,
      0x25,  0x3,    0x25,   0x3,    0x25,   0x3,    0x25,   0x5,    0x25,
      0x22e, 0xa,    0x25,   0x3,    0x26,   0x3,    0x26,   0x3,    0x26,
      0x3,   0x27,   0x3,    0x27,   0x3,    0x27,   0x3,    0x27,   0x3,
      0x28,  0x3,    0x28,   0x5,    0x28,   0x239,  0xa,    0x28,   0x3,
      0x28,  0x3,    0x28,   0x3,    0x28,   0x3,    0x29,   0x3,    0x29,
      0x3,   0x29,   0x3,    0x29,   0x3,    0x29,   0x3,    0x29,   0x3,
      0x29,  0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2a,   0x3,    0x2b,
      0x3,   0x2b,   0x5,    0x2b,   0x24a,  0xa,    0x2b,   0x3,    0x2c,
      0x3,   0x2c,   0x3,    0x2c,   0x7,    0x2c,   0x24f,  0xa,    0x2c,
      0xc,   0x2c,   0xe,    0x2c,   0x252,  0xb,    0x2c,   0x3,    0x2c,
      0x3,   0x2c,   0x3,    0x2d,   0x3,    0x2d,   0x3,    0x2d,   0x7,
      0x2d,  0x259,  0xa,    0x2d,   0xc,    0x2d,   0xe,    0x2d,   0x25c,
      0xb,   0x2d,   0x3,    0x2d,   0x5,    0x2d,   0x25f,  0xa,    0x2d,
      0x3,   0x2d,   0x3,    0x2d,   0x7,    0x2d,   0x263,  0xa,    0x2d,
      0xc,   0x2d,   0xe,    0x2d,   0x266,  0xb,    0x2d,   0x3,    0x2d,
      0x3,   0x2d,   0x3,    0x2e,   0x3,    0x2e,   0x7,    0x2e,   0x26c,
      0xa,   0x2e,   0xc,    0x2e,   0xe,    0x2e,   0x26f,  0xb,    0x2e,
      0x3,   0x2e,   0x3,    0x2e,   0x5,    0x2e,   0x273,  0xa,    0x2e,
      0x3,   0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x3,    0x2f,   0x3,
      0x2f,  0x5,    0x2f,   0x27a,  0xa,    0x2f,   0x3,    0x30,   0x3,
      0x30,  0x3,    0x30,   0x3,    0x31,   0x3,    0x31,   0x3,    0x31,
      0x7,   0x31,   0x282,  0xa,    0x31,   0xc,    0x31,   0xe,    0x31,
      0x285, 0xb,    0x31,   0x3,    0x32,   0x3,    0x32,   0x3,    0x32,
      0x3,   0x33,   0x3,    0x33,   0x3,    0x33,   0x5,    0x33,   0x28d,
      0xa,   0x33,   0x3,    0x34,   0x3,    0x34,   0x3,    0x34,   0x3,
      0x35,  0x3,    0x35,   0x3,    0x35,   0x5,    0x35,   0x295,  0xa,
      0x35,  0x3,    0x35,   0x3,    0x35,   0x3,    0x35,   0x7,    0x35,
      0x29a, 0xa,    0x35,   0xc,    0x35,   0xe,    0x35,   0x29d,  0xb,
      0x35,  0x3,    0x35,   0x3,    0x35,   0x5,    0x35,   0x2a1,  0xa,
      0x35,  0x3,    0x36,   0x3,    0x36,   0x3,    0x36,   0x3,    0x36,
      0x3,   0x36,   0x7,    0x36,   0x2a8,  0xa,    0x36,   0xc,    0x36,
      0xe,   0x36,   0x2ab,  0xb,    0x36,   0x3,    0x36,   0x3,    0x36,
      0x5,   0x36,   0x2af,  0xa,    0x36,   0x3,    0x37,   0x3,    0x37,
      0x5,   0x37,   0x2b3,  0xa,    0x37,   0x3,    0x37,   0x3,    0x37,
      0x3,   0x38,   0x3,    0x38,   0x3,    0x38,   0x5,    0x38,   0x2ba,
      0xa,   0x38,   0x5,    0x38,   0x2bc,  0xa,    0x38,   0x3,    0x39,
      0x3,   0x39,   0x3,    0x39,   0x3,    0x39,   0x3,    0x39,   0x3,
      0x39,  0x5,    0x39,   0x2c4,  0xa,    0x39,   0x3,    0x3a,   0x5,
      0x3a,  0x2c7,  0xa,    0x3a,   0x3,    0x3b,   0x3,    0x3b,   0x3,
      0x3b,  0x3,    0x3b,   0x3,    0x3b,   0x3,    0x3b,   0x5,    0x3b,
      0x2cf, 0xa,    0x3b,   0x7,    0x3b,   0x2d1,  0xa,    0x3b,   0xc,
      0x3b,  0xe,    0x3b,   0x2d4,  0xb,    0x3b,   0x3,    0x3c,   0x3,
      0x3c,  0x5,    0x3c,   0x2d8,  0xa,    0x3c,   0x3,    0x3d,   0x3,
      0x3d,  0x3,    0x3d,   0x7,    0x3d,   0x2dd,  0xa,    0x3d,   0xc,
      0x3d,  0xe,    0x3d,   0x2e0,  0xb,    0x3d,   0x3,    0x3e,   0x3,
      0x3e,  0x3,    0x3f,   0x3,    0x3f,   0x3,    0x3f,   0x3,    0x3f,
      0x3,   0x3f,   0x3,    0x3f,   0x5,    0x3f,   0x2ea,  0xa,    0x3f,
      0x3,   0x40,   0x5,    0x40,   0x2ed,  0xa,    0x40,   0x3,    0x41,
      0x3,   0x41,   0x3,    0x41,   0x5,    0x41,   0x2f2,  0xa,    0x41,
      0x7,   0x41,   0x2f4,  0xa,    0x41,   0xc,    0x41,   0xe,    0x41,
      0x2f7, 0xb,    0x41,   0x3,    0x42,   0x3,    0x42,   0x3,    0x43,
      0x3,   0x43,   0x3,    0x44,   0x3,    0x44,   0x3,    0x44,   0x3,
      0x45,  0x3,    0x45,   0x3,    0x45,   0x3,    0x46,   0x3,    0x46,
      0x5,   0x46,   0x305,  0xa,    0x46,   0x3,    0x47,   0x3,    0x47,
      0x3,   0x47,   0x7,    0x47,   0x30a,  0xa,    0x47,   0xc,    0x47,
      0xe,   0x47,   0x30d,  0xb,    0x47,   0x3,    0x48,   0x3,    0x48,
      0x3,   0x49,   0x3,    0x49,   0x3,    0x4a,   0x3,    0x4a,   0x3,
      0x4a,  0x7,    0x4a,   0x316,  0xa,    0x4a,   0xc,    0x4a,   0xe,
      0x4a,  0x319,  0xb,    0x4a,   0x3,    0x4b,   0x3,    0x4b,   0x3,
      0x4b,  0x7,    0x4b,   0x31e,  0xa,    0x4b,   0xc,    0x4b,   0xe,
      0x4b,  0x321,  0xb,    0x4b,   0x3,    0x4c,   0x3,    0x4c,   0x5,
      0x4c,  0x325,  0xa,    0x4c,   0x3,    0x4d,   0x3,    0x4d,   0x3,
      0x4d,  0x5,    0x4d,   0x32a,  0xa,    0x4d,   0x3,    0x4e,   0x3,
      0x4e,  0x3,    0x4f,   0x3,    0x4f,   0x3,    0x4f,   0x3,    0x4f,
      0x3,   0x4f,   0x3,    0x4f,   0x3,    0x4f,   0x3,    0x4f,   0x5,
      0x4f,  0x336,  0xa,    0x4f,   0x3,    0x50,   0x3,    0x50,   0x3,
      0x50,  0x3,    0x50,   0x3,    0x50,   0x7,    0x50,   0x33d,  0xa,
      0x50,  0xc,    0x50,   0xe,    0x50,   0x340,  0xb,    0x50,   0x5,
      0x50,  0x342,  0xa,    0x50,   0x3,    0x50,   0x5,    0x50,   0x345,
      0xa,   0x50,   0x3,    0x51,   0x3,    0x51,   0x3,    0x51,   0x3,
      0x51,  0x3,    0x51,   0x5,    0x51,   0x34c,  0xa,    0x51,   0x5,
      0x51,  0x34e,  0xa,    0x51,   0x3,    0x52,   0x3,    0x52,   0x3,
      0x53,  0x3,    0x53,   0x5,    0x53,   0x354,  0xa,    0x53,   0x3,
      0x54,  0x3,    0x54,   0x3,    0x54,   0x3,    0x54,   0x3,    0x55,
      0x3,   0x55,   0x5,    0x55,   0x35c,  0xa,    0x55,   0x3,    0x56,
      0x3,   0x56,   0x3,    0x56,   0x3,    0x56,   0x3,    0x57,   0x3,
      0x57,  0x6,    0x57,   0x364,  0xa,    0x57,   0xd,    0x57,   0xe,
      0x57,  0x365,  0x3,    0x57,   0x3,    0x57,   0x3,    0x58,   0x3,
      0x58,  0x6,    0x58,   0x36c,  0xa,    0x58,   0xd,    0x58,   0xe,
      0x58,  0x36d,  0x3,    0x58,   0x3,    0x58,   0x3,    0x59,   0x3,
      0x59,  0x5,    0x59,   0x374,  0xa,    0x59,   0x3,    0x5a,   0x3,
      0x5a,  0x5,    0x5a,   0x378,  0xa,    0x5a,   0x3,    0x5b,   0x3,
      0x5b,  0x5,    0x5b,   0x37c,  0xa,    0x5b,   0x3,    0x5c,   0x3,
      0x5c,  0x5,    0x5c,   0x380,  0xa,    0x5c,   0x3,    0x5d,   0x3,
      0x5d,  0x3,    0x5e,   0x3,    0x5e,   0x3,    0x5e,   0x3,    0x5e,
      0x3,   0x5e,   0x3,    0x5e,   0x5,    0x5e,   0x38a,  0xa,    0x5e,
      0x3,   0x5f,   0x3,    0x5f,   0x3,    0x60,   0x3,    0x60,   0x3,
      0x60,  0x7,    0x60,   0x391,  0xa,    0x60,   0xc,    0x60,   0xe,
      0x60,  0x394,  0xb,    0x60,   0x3,    0x61,   0x3,    0x61,   0x3,
      0x61,  0x7,    0x61,   0x399,  0xa,    0x61,   0xc,    0x61,   0xe,
      0x61,  0x39c,  0xb,    0x61,   0x3,    0x62,   0x3,    0x62,   0x3,
      0x63,  0x3,    0x63,   0x3,    0x63,   0x3,    0x63,   0x3,    0x63,
      0x3,   0x63,   0x3,    0x63,   0x3,    0x63,   0x3,    0x63,   0x3,
      0x63,  0x3,    0x63,   0x3,    0x63,   0x3,    0x63,   0x3,    0x63,
      0x3,   0x63,   0x3,    0x63,   0x3,    0x63,   0x3,    0x63,   0x5,
      0x63,  0x3b2,  0xa,    0x63,   0x3,    0x64,   0x3,    0x64,   0x3,
      0x65,  0x3,    0x65,   0x7,    0x65,   0x3b8,  0xa,    0x65,   0xc,
      0x65,  0xe,    0x65,   0x3bb,  0xb,    0x65,   0x3,    0x66,   0x3,
      0x66,  0x3,    0x66,   0x3,    0x66,   0x3,    0x66,   0x5,    0x66,
      0x3c2, 0xa,    0x66,   0x3,    0x67,   0x3,    0x67,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x69,   0x3,    0x69,   0x5,    0x69,   0x3ca,
      0xa,   0x69,   0x3,    0x69,   0x7,    0x69,   0x3cd,  0xa,    0x69,
      0xc,   0x69,   0xe,    0x69,   0x3d0,  0xb,    0x69,   0x3,    0x6a,
      0x3,   0x6a,   0x7,    0x6a,   0x3d4,  0xa,    0x6a,   0xc,    0x6a,
      0xe,   0x6a,   0x3d7,  0xb,    0x6a,   0x3,    0x6b,   0x3,    0x6b,
      0x5,   0x6b,   0x3db,  0xa,    0x6b,   0x3,    0x6c,   0x3,    0x6c,
      0x3,   0x6c,   0x3,    0x6d,   0x3,    0x6d,   0x3,    0x6d,   0x3,
      0x6e,  0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x5,    0x6e,   0x3ea,  0xa,    0x6e,
      0x3,   0x6f,   0x3,    0x6f,   0x3,    0x6f,   0x3,    0x6f,   0x3,
      0x6f,  0x3,    0x6f,   0x3,    0x6f,   0x5,    0x6f,   0x3f3,  0xa,
      0x6f,  0x3,    0x70,   0x3,    0x70,   0x3,    0x70,   0x3,    0x70,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x5,    0x71,
      0x425, 0xa,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x3,    0x71,   0x3,
      0x71,  0x3,    0x71,   0x5,    0x71,   0x4ff,  0xa,    0x71,   0x3,
      0x72,  0x3,    0x72,   0x3,    0x72,   0x3,    0x72,   0x3,    0x72,
      0x3,   0x72,   0x3,    0x72,   0x5,    0x72,   0x508,  0xa,    0x72,
      0x3,   0x72,   0x3,    0x72,   0x3,    0x73,   0x3,    0x73,   0x3,
      0x73,  0x3,    0x73,   0x3,    0x73,   0x3,    0x73,   0x3,    0x73,
      0x5,   0x73,   0x513,  0xa,    0x73,   0x3,    0x73,   0x3,    0x73,
      0x3,   0x74,   0x3,    0x74,   0x3,    0x74,   0x3,    0x74,   0x3,
      0x74,  0x3,    0x74,   0x3,    0x74,   0x3,    0x74,   0x3,    0x74,
      0x5,   0x74,   0x520,  0xa,    0x74,   0x3,    0x74,   0x3,    0x74,
      0x3,   0x75,   0x3,    0x75,   0x3,    0x75,   0x3,    0x76,   0x3,
      0x76,  0x3,    0x76,   0x3,    0x76,   0x3,    0x77,   0x3,    0x77,
      0x3,   0x77,   0x5,    0x77,   0x52e,  0xa,    0x77,   0x3,    0x77,
      0x3,   0x77,   0x5,    0x77,   0x532,  0xa,    0x77,   0x3,    0x77,
      0x3,   0x77,   0x3,    0x77,   0x3,    0x77,   0x5,    0x77,   0x538,
      0xa,   0x77,   0x3,    0x77,   0x3,    0x77,   0x3,    0x77,   0x3,
      0x77,  0x3,    0x77,   0x3,    0x77,   0x5,    0x77,   0x540,  0xa,
      0x77,  0x3,    0x77,   0x3,    0x77,   0x3,    0x77,   0x3,    0x77,
      0x3,   0x77,   0x3,    0x77,   0x5,    0x77,   0x548,  0xa,    0x77,
      0x3,   0x77,   0x3,    0x77,   0x3,    0x77,   0x3,    0x77,   0x3,
      0x77,  0x3,    0x77,   0x5,    0x77,   0x550,  0xa,    0x77,   0x3,
      0x77,  0x3,    0x77,   0x3,    0x77,   0x3,    0x77,   0x3,    0x77,
      0x3,   0x77,   0x5,    0x77,   0x558,  0xa,    0x77,   0x3,    0x77,
      0x3,   0x77,   0x3,    0x77,   0x3,    0x77,   0x3,    0x77,   0x3,
      0x77,  0x5,    0x77,   0x560,  0xa,    0x77,   0x3,    0x77,   0x3,
      0x77,  0x3,    0x77,   0x3,    0x77,   0x3,    0x77,   0x5,    0x77,
      0x567, 0xa,    0x77,   0x3,    0x77,   0x3,    0x77,   0x5,    0x77,
      0x56b, 0xa,    0x77,   0x3,    0x78,   0x3,    0x78,   0x5,    0x78,
      0x56f, 0xa,    0x78,   0x3,    0x79,   0x3,    0x79,   0x3,    0x79,
      0x3,   0x79,   0x5,    0x79,   0x575,  0xa,    0x79,   0x3,    0x7a,
      0x3,   0x7a,   0x3,    0x7a,   0x5,    0x7a,   0x57a,  0xa,    0x7a,
      0x3,   0x7b,   0x3,    0x7b,   0x3,    0x7c,   0x3,    0x7c,   0x3,
      0x7d,  0x3,    0x7d,   0x3,    0x7e,   0x3,    0x7e,   0x3,    0x7f,
      0x3,   0x7f,   0x3,    0x80,   0x5,    0x80,   0x587,  0xa,    0x80,
      0x3,   0x80,   0x3,    0x80,   0x5,    0x80,   0x58b,  0xa,    0x80,
      0x3,   0x81,   0x3,    0x81,   0x5,    0x81,   0x58f,  0xa,    0x81,
      0x3,   0x82,   0x3,    0x82,   0x3,    0x83,   0x3,    0x83,   0x3,
      0x84,  0x3,    0x84,   0x3,    0x85,   0x3,    0x85,   0x3,    0x85,
      0x2,   0x2,    0x86,   0x2,    0x4,    0x6,    0x8,    0xa,    0xc,
      0xe,   0x10,   0x12,   0x14,   0x16,   0x18,   0x1a,   0x1c,   0x1e,
      0x20,  0x22,   0x24,   0x26,   0x28,   0x2a,   0x2c,   0x2e,   0x30,
      0x32,  0x34,   0x36,   0x38,   0x3a,   0x3c,   0x3e,   0x40,   0x42,
      0x44,  0x46,   0x48,   0x4a,   0x4c,   0x4e,   0x50,   0x52,   0x54,
      0x56,  0x58,   0x5a,   0x5c,   0x5e,   0x60,   0x62,   0x64,   0x66,
      0x68,  0x6a,   0x6c,   0x6e,   0x70,   0x72,   0x74,   0x76,   0x78,
      0x7a,  0x7c,   0x7e,   0x80,   0x82,   0x84,   0x86,   0x88,   0x8a,
      0x8c,  0x8e,   0x90,   0x92,   0x94,   0x96,   0x98,   0x9a,   0x9c,
      0x9e,  0xa0,   0xa2,   0xa4,   0xa6,   0xa8,   0xaa,   0xac,   0xae,
      0xb0,  0xb2,   0xb4,   0xb6,   0xb8,   0xba,   0xbc,   0xbe,   0xc0,
      0xc2,  0xc4,   0xc6,   0xc8,   0xca,   0xcc,   0xce,   0xd0,   0xd2,
      0xd4,  0xd6,   0xd8,   0xda,   0xdc,   0xde,   0xe0,   0xe2,   0xe4,
      0xe6,  0xe8,   0xea,   0xec,   0xee,   0xf0,   0xf2,   0xf4,   0xf6,
      0xf8,  0xfa,   0xfc,   0xfe,   0x100,  0x102,  0x104,  0x106,  0x108,
      0x2,   0xc,    0x3,    0x2,    0x24,   0x25,   0x3,    0x2,    0x31,
      0x32,  0x4,    0x2,    0x3,    0x3,    0xf,    0x10,   0x3,    0x2,
      0x91,  0x92,   0x3,    0x2,    0x95,   0x97,   0x3,    0x2,    0x98,
      0x9a,  0x3,    0x2,    0x9b,   0x9d,   0x3,    0x2,    0x1f,   0x20,
      0x3,   0x2,    0x9f,   0xa2,   0x4,    0x2,    0x90,   0x90,   0xa5,
      0xa5,  0x2,    0x600,  0x2,    0x10a,  0x3,    0x2,    0x2,    0x2,
      0x4,   0x118,  0x3,    0x2,    0x2,    0x2,    0x6,    0x11b,  0x3,
      0x2,   0x2,    0x2,    0x8,    0x11e,  0x3,    0x2,    0x2,    0x2,
      0xa,   0x122,  0x3,    0x2,    0x2,    0x2,    0xc,    0x12c,  0x3,
      0x2,   0x2,    0x2,    0xe,    0x131,  0x3,    0x2,    0x2,    0x2,
      0x10,  0x13f,  0x3,    0x2,    0x2,    0x2,    0x12,   0x141,  0x3,
      0x2,   0x2,    0x2,    0x14,   0x145,  0x3,    0x2,    0x2,    0x2,
      0x16,  0x149,  0x3,    0x2,    0x2,    0x2,    0x18,   0x163,  0x3,
      0x2,   0x2,    0x2,    0x1a,   0x177,  0x3,    0x2,    0x2,    0x2,
      0x1c,  0x181,  0x3,    0x2,    0x2,    0x2,    0x1e,   0x186,  0x3,
      0x2,   0x2,    0x2,    0x20,   0x188,  0x3,    0x2,    0x2,    0x2,
      0x22,  0x18b,  0x3,    0x2,    0x2,    0x2,    0x24,   0x18e,  0x3,
      0x2,   0x2,    0x2,    0x26,   0x193,  0x3,    0x2,    0x2,    0x2,
      0x28,  0x19e,  0x3,    0x2,    0x2,    0x2,    0x2a,   0x1af,  0x3,
      0x2,   0x2,    0x2,    0x2c,   0x1b1,  0x3,    0x2,    0x2,    0x2,
      0x2e,  0x1b7,  0x3,    0x2,    0x2,    0x2,    0x30,   0x1b9,  0x3,
      0x2,   0x2,    0x2,    0x32,   0x1c5,  0x3,    0x2,    0x2,    0x2,
      0x34,  0x1f1,  0x3,    0x2,    0x2,    0x2,    0x36,   0x1f3,  0x3,
      0x2,   0x2,    0x2,    0x38,   0x1f6,  0x3,    0x2,    0x2,    0x2,
      0x3a,  0x1f9,  0x3,    0x2,    0x2,    0x2,    0x3c,   0x1fe,  0x3,
      0x2,   0x2,    0x2,    0x3e,   0x200,  0x3,    0x2,    0x2,    0x2,
      0x40,  0x207,  0x3,    0x2,    0x2,    0x2,    0x42,   0x20f,  0x3,
      0x2,   0x2,    0x2,    0x44,   0x217,  0x3,    0x2,    0x2,    0x2,
      0x46,  0x21e,  0x3,    0x2,    0x2,    0x2,    0x48,   0x22d,  0x3,
      0x2,   0x2,    0x2,    0x4a,   0x22f,  0x3,    0x2,    0x2,    0x2,
      0x4c,  0x232,  0x3,    0x2,    0x2,    0x2,    0x4e,   0x236,  0x3,
      0x2,   0x2,    0x2,    0x50,   0x23d,  0x3,    0x2,    0x2,    0x2,
      0x52,  0x244,  0x3,    0x2,    0x2,    0x2,    0x54,   0x249,  0x3,
      0x2,   0x2,    0x2,    0x56,   0x24b,  0x3,    0x2,    0x2,    0x2,
      0x58,  0x25e,  0x3,    0x2,    0x2,    0x2,    0x5a,   0x272,  0x3,
      0x2,   0x2,    0x2,    0x5c,   0x279,  0x3,    0x2,    0x2,    0x2,
      0x5e,  0x27b,  0x3,    0x2,    0x2,    0x2,    0x60,   0x27e,  0x3,
      0x2,   0x2,    0x2,    0x62,   0x286,  0x3,    0x2,    0x2,    0x2,
      0x64,  0x28c,  0x3,    0x2,    0x2,    0x2,    0x66,   0x28e,  0x3,
      0x2,   0x2,    0x2,    0x68,   0x2a0,  0x3,    0x2,    0x2,    0x2,
      0x6a,  0x2ae,  0x3,    0x2,    0x2,    0x2,    0x6c,   0x2b0,  0x3,
      0x2,   0x2,    0x2,    0x6e,   0x2b6,  0x3,    0x2,    0x2,    0x2,
      0x70,  0x2c3,  0x3,    0x2,    0x2,    0x2,    0x72,   0x2c6,  0x3,
      0x2,   0x2,    0x2,    0x74,   0x2c8,  0x3,    0x2,    0x2,    0x2,
      0x76,  0x2d7,  0x3,    0x2,    0x2,    0x2,    0x78,   0x2d9,  0x3,
      0x2,   0x2,    0x2,    0x7a,   0x2e1,  0x3,    0x2,    0x2,    0x2,
      0x7c,  0x2e9,  0x3,    0x2,    0x2,    0x2,    0x7e,   0x2ec,  0x3,
      0x2,   0x2,    0x2,    0x80,   0x2ee,  0x3,    0x2,    0x2,    0x2,
      0x82,  0x2f8,  0x3,    0x2,    0x2,    0x2,    0x84,   0x2fa,  0x3,
      0x2,   0x2,    0x2,    0x86,   0x2fc,  0x3,    0x2,    0x2,    0x2,
      0x88,  0x2ff,  0x3,    0x2,    0x2,    0x2,    0x8a,   0x304,  0x3,
      0x2,   0x2,    0x2,    0x8c,   0x306,  0x3,    0x2,    0x2,    0x2,
      0x8e,  0x30e,  0x3,    0x2,    0x2,    0x2,    0x90,   0x310,  0x3,
      0x2,   0x2,    0x2,    0x92,   0x312,  0x3,    0x2,    0x2,    0x2,
      0x94,  0x31a,  0x3,    0x2,    0x2,    0x2,    0x96,   0x322,  0x3,
      0x2,   0x2,    0x2,    0x98,   0x329,  0x3,    0x2,    0x2,    0x2,
      0x9a,  0x32b,  0x3,    0x2,    0x2,    0x2,    0x9c,   0x335,  0x3,
      0x2,   0x2,    0x2,    0x9e,   0x344,  0x3,    0x2,    0x2,    0x2,
      0xa0,  0x34d,  0x3,    0x2,    0x2,    0x2,    0xa2,   0x34f,  0x3,
      0x2,   0x2,    0x2,    0xa4,   0x353,  0x3,    0x2,    0x2,    0x2,
      0xa6,  0x355,  0x3,    0x2,    0x2,    0x2,    0xa8,   0x35b,  0x3,
      0x2,   0x2,    0x2,    0xaa,   0x35d,  0x3,    0x2,    0x2,    0x2,
      0xac,  0x361,  0x3,    0x2,    0x2,    0x2,    0xae,   0x369,  0x3,
      0x2,   0x2,    0x2,    0xb0,   0x373,  0x3,    0x2,    0x2,    0x2,
      0xb2,  0x377,  0x3,    0x2,    0x2,    0x2,    0xb4,   0x37b,  0x3,
      0x2,   0x2,    0x2,    0xb6,   0x37f,  0x3,    0x2,    0x2,    0x2,
      0xb8,  0x381,  0x3,    0x2,    0x2,    0x2,    0xba,   0x389,  0x3,
      0x2,   0x2,    0x2,    0xbc,   0x38b,  0x3,    0x2,    0x2,    0x2,
      0xbe,  0x38d,  0x3,    0x2,    0x2,    0x2,    0xc0,   0x395,  0x3,
      0x2,   0x2,    0x2,    0xc2,   0x39d,  0x3,    0x2,    0x2,    0x2,
      0xc4,  0x39f,  0x3,    0x2,    0x2,    0x2,    0xc6,   0x3b3,  0x3,
      0x2,   0x2,    0x2,    0xc8,   0x3b5,  0x3,    0x2,    0x2,    0x2,
      0xca,  0x3c1,  0x3,    0x2,    0x2,    0x2,    0xcc,   0x3c3,  0x3,
      0x2,   0x2,    0x2,    0xce,   0x3c5,  0x3,    0x2,    0x2,    0x2,
      0xd0,  0x3c9,  0x3,    0x2,    0x2,    0x2,    0xd2,   0x3d1,  0x3,
      0x2,   0x2,    0x2,    0xd4,   0x3da,  0x3,    0x2,    0x2,    0x2,
      0xd6,  0x3dc,  0x3,    0x2,    0x2,    0x2,    0xd8,   0x3df,  0x3,
      0x2,   0x2,    0x2,    0xda,   0x3e9,  0x3,    0x2,    0x2,    0x2,
      0xdc,  0x3f2,  0x3,    0x2,    0x2,    0x2,    0xde,   0x3f4,  0x3,
      0x2,   0x2,    0x2,    0xe0,   0x4fe,  0x3,    0x2,    0x2,    0x2,
      0xe2,  0x500,  0x3,    0x2,    0x2,    0x2,    0xe4,   0x50b,  0x3,
      0x2,   0x2,    0x2,    0xe6,   0x516,  0x3,    0x2,    0x2,    0x2,
      0xe8,  0x523,  0x3,    0x2,    0x2,    0x2,    0xea,   0x526,  0x3,
      0x2,   0x2,    0x2,    0xec,   0x56a,  0x3,    0x2,    0x2,    0x2,
      0xee,  0x56c,  0x3,    0x2,    0x2,    0x2,    0xf0,   0x570,  0x3,
      0x2,   0x2,    0x2,    0xf2,   0x579,  0x3,    0x2,    0x2,    0x2,
      0xf4,  0x57b,  0x3,    0x2,    0x2,    0x2,    0xf6,   0x57d,  0x3,
      0x2,   0x2,    0x2,    0xf8,   0x57f,  0x3,    0x2,    0x2,    0x2,
      0xfa,  0x581,  0x3,    0x2,    0x2,    0x2,    0xfc,   0x583,  0x3,
      0x2,   0x2,    0x2,    0xfe,   0x586,  0x3,    0x2,    0x2,    0x2,
      0x100, 0x58e,  0x3,    0x2,    0x2,    0x2,    0x102,  0x590,  0x3,
      0x2,   0x2,    0x2,    0x104,  0x592,  0x3,    0x2,    0x2,    0x2,
      0x106, 0x594,  0x3,    0x2,    0x2,    0x2,    0x108,  0x596,  0x3,
      0x2,   0x2,    0x2,    0x10a,  0x10f,  0x5,    0x4,    0x3,    0x2,
      0x10b, 0x110,  0x5,    0xa,    0x6,    0x2,    0x10c,  0x110,  0x5,
      0x16,  0xc,    0x2,    0x10d,  0x110,  0x5,    0x18,   0xd,    0x2,
      0x10e, 0x110,  0x5,    0x1a,   0xe,    0x2,    0x10f,  0x10b,  0x3,
      0x2,   0x2,    0x2,    0x10f,  0x10c,  0x3,    0x2,    0x2,    0x2,
      0x10f, 0x10d,  0x3,    0x2,    0x2,    0x2,    0x10f,  0x10e,  0x3,
      0x2,   0x2,    0x2,    0x110,  0x111,  0x3,    0x2,    0x2,    0x2,
      0x111, 0x112,  0x5,    0x3c,   0x1f,   0x2,    0x112,  0x113,  0x7,
      0x2,   0x2,    0x3,    0x113,  0x3,    0x3,    0x2,    0x2,    0x2,
      0x114, 0x117,  0x5,    0x6,    0x4,    0x2,    0x115,  0x117,  0x5,
      0x8,   0x5,    0x2,    0x116,  0x114,  0x3,    0x2,    0x2,    0x2,
      0x116, 0x115,  0x3,    0x2,    0x2,    0x2,    0x117,  0x11a,  0x3,
      0x2,   0x2,    0x2,    0x118,  0x116,  0x3,    0x2,    0x2,    0x2,
      0x118, 0x119,  0x3,    0x2,    0x2,    0x2,    0x119,  0x5,    0x3,
      0x2,   0x2,    0x2,    0x11a,  0x118,  0x3,    0x2,    0x2,    0x2,
      0x11b, 0x11c,  0x7,    0x21,   0x2,    0x2,    0x11c,  0x11d,  0x5,
      0x104, 0x83,   0x2,    0x11d,  0x7,    0x3,    0x2,    0x2,    0x2,
      0x11e, 0x11f,  0x7,    0x22,   0x2,    0x2,    0x11f,  0x120,  0x7,
      0x8e,  0x2,    0x2,    0x120,  0x121,  0x5,    0x104,  0x83,   0x2,
      0x121, 0x9,    0x3,    0x2,    0x2,    0x2,    0x122,  0x126,  0x5,
      0xe,   0x8,    0x2,    0x123,  0x125,  0x5,    0x1c,   0xf,    0x2,
      0x124, 0x123,  0x3,    0x2,    0x2,    0x2,    0x125,  0x128,  0x3,
      0x2,   0x2,    0x2,    0x126,  0x124,  0x3,    0x2,    0x2,    0x2,
      0x126, 0x127,  0x3,    0x2,    0x2,    0x2,    0x127,  0x129,  0x3,
      0x2,   0x2,    0x2,    0x128,  0x126,  0x3,    0x2,    0x2,    0x2,
      0x129, 0x12a,  0x5,    0x24,   0x13,   0x2,    0x12a,  0x12b,  0x5,
      0x26,  0x14,   0x2,    0x12b,  0xb,    0x3,    0x2,    0x2,    0x2,
      0x12c, 0x12d,  0x5,    0xe,    0x8,    0x2,    0x12d,  0x12e,  0x5,
      0x24,  0x13,   0x2,    0x12e,  0x12f,  0x5,    0x26,   0x14,   0x2,
      0x12f, 0x130,  0x5,    0x3c,   0x1f,   0x2,    0x130,  0xd,    0x3,
      0x2,   0x2,    0x2,    0x131,  0x133,  0x7,    0x23,   0x2,    0x2,
      0x132, 0x134,  0x9,    0x2,    0x2,    0x2,    0x133,  0x132,  0x3,
      0x2,   0x2,    0x2,    0x133,  0x134,  0x3,    0x2,    0x2,    0x2,
      0x134, 0x13b,  0x3,    0x2,    0x2,    0x2,    0x135,  0x137,  0x5,
      0x10,  0x9,    0x2,    0x136,  0x135,  0x3,    0x2,    0x2,    0x2,
      0x137, 0x138,  0x3,    0x2,    0x2,    0x2,    0x138,  0x136,  0x3,
      0x2,   0x2,    0x2,    0x138,  0x139,  0x3,    0x2,    0x2,    0x2,
      0x139, 0x13c,  0x3,    0x2,    0x2,    0x2,    0x13a,  0x13c,  0x7,
      0x3,   0x2,    0x2,    0x13b,  0x136,  0x3,    0x2,    0x2,    0x2,
      0x13b, 0x13a,  0x3,    0x2,    0x2,    0x2,    0x13c,  0xf,    0x3,
      0x2,   0x2,    0x2,    0x13d,  0x140,  0x5,    0xb8,   0x5d,   0x2,
      0x13e, 0x140,  0x5,    0x12,   0xa,    0x2,    0x13f,  0x13d,  0x3,
      0x2,   0x2,    0x2,    0x13f,  0x13e,  0x3,    0x2,    0x2,    0x2,
      0x140, 0x11,   0x3,    0x2,    0x2,    0x2,    0x141,  0x142,  0x7,
      0x4,   0x2,    0x2,    0x142,  0x143,  0x5,    0x14,   0xb,    0x2,
      0x143, 0x144,  0x7,    0x5,    0x2,    0x2,    0x144,  0x13,   0x3,
      0x2,   0x2,    0x2,    0x145,  0x146,  0x5,    0xbc,   0x5f,   0x2,
      0x146, 0x147,  0x7,    0x26,   0x2,    0x2,    0x147,  0x148,  0x5,
      0xb8,  0x5d,   0x2,    0x148,  0x15,   0x3,    0x2,    0x2,    0x2,
      0x149, 0x161,  0x7,    0x27,   0x2,    0x2,    0x14a,  0x14e,  0x5,
      0x6c,  0x37,   0x2,    0x14b,  0x14d,  0x5,    0x1c,   0xf,    0x2,
      0x14c, 0x14b,  0x3,    0x2,    0x2,    0x2,    0x14d,  0x150,  0x3,
      0x2,   0x2,    0x2,    0x14e,  0x14c,  0x3,    0x2,    0x2,    0x2,
      0x14e, 0x14f,  0x3,    0x2,    0x2,    0x2,    0x14f,  0x151,  0x3,
      0x2,   0x2,    0x2,    0x150,  0x14e,  0x3,    0x2,    0x2,    0x2,
      0x151, 0x152,  0x5,    0x24,   0x13,   0x2,    0x152,  0x153,  0x5,
      0x26,  0x14,   0x2,    0x153,  0x162,  0x3,    0x2,    0x2,    0x2,
      0x154, 0x156,  0x5,    0x1c,   0xf,    0x2,    0x155,  0x154,  0x3,
      0x2,   0x2,    0x2,    0x156,  0x159,  0x3,    0x2,    0x2,    0x2,
      0x157, 0x155,  0x3,    0x2,    0x2,    0x2,    0x157,  0x158,  0x3,
      0x2,   0x2,    0x2,    0x158,  0x15a,  0x3,    0x2,    0x2,    0x2,
      0x159, 0x157,  0x3,    0x2,    0x2,    0x2,    0x15a,  0x15b,  0x7,
      0x28,  0x2,    0x2,    0x15b,  0x15d,  0x7,    0x6,    0x2,    0x2,
      0x15c, 0x15e,  0x5,    0x3e,   0x20,   0x2,    0x15d,  0x15c,  0x3,
      0x2,   0x2,    0x2,    0x15d,  0x15e,  0x3,    0x2,    0x2,    0x2,
      0x15e, 0x15f,  0x3,    0x2,    0x2,    0x2,    0x15f,  0x160,  0x7,
      0x7,   0x2,    0x2,    0x160,  0x162,  0x5,    0x26,   0x14,   0x2,
      0x161, 0x14a,  0x3,    0x2,    0x2,    0x2,    0x161,  0x157,  0x3,
      0x2,   0x2,    0x2,    0x162,  0x17,   0x3,    0x2,    0x2,    0x2,
      0x163, 0x16a,  0x7,    0x29,   0x2,    0x2,    0x164,  0x166,  0x5,
      0xb6,  0x5c,   0x2,    0x165,  0x164,  0x3,    0x2,    0x2,    0x2,
      0x166, 0x167,  0x3,    0x2,    0x2,    0x2,    0x167,  0x165,  0x3,
      0x2,   0x2,    0x2,    0x167,  0x168,  0x3,    0x2,    0x2,    0x2,
      0x168, 0x16b,  0x3,    0x2,    0x2,    0x2,    0x169,  0x16b,  0x7,
      0x3,   0x2,    0x2,    0x16a,  0x165,  0x3,    0x2,    0x2,    0x2,
      0x16a, 0x169,  0x3,    0x2,    0x2,    0x2,    0x16b,  0x16f,  0x3,
      0x2,   0x2,    0x2,    0x16c,  0x16e,  0x5,    0x1c,   0xf,    0x2,
      0x16d, 0x16c,  0x3,    0x2,    0x2,    0x2,    0x16e,  0x171,  0x3,
      0x2,   0x2,    0x2,    0x16f,  0x16d,  0x3,    0x2,    0x2,    0x2,
      0x16f, 0x170,  0x3,    0x2,    0x2,    0x2,    0x170,  0x173,  0x3,
      0x2,   0x2,    0x2,    0x171,  0x16f,  0x3,    0x2,    0x2,    0x2,
      0x172, 0x174,  0x5,    0x24,   0x13,   0x2,    0x173,  0x172,  0x3,
      0x2,   0x2,    0x2,    0x173,  0x174,  0x3,    0x2,    0x2,    0x2,
      0x174, 0x175,  0x3,    0x2,    0x2,    0x2,    0x175,  0x176,  0x5,
      0x26,  0x14,   0x2,    0x176,  0x19,   0x3,    0x2,    0x2,    0x2,
      0x177, 0x17b,  0x7,    0x2a,   0x2,    0x2,    0x178,  0x17a,  0x5,
      0x1c,  0xf,    0x2,    0x179,  0x178,  0x3,    0x2,    0x2,    0x2,
      0x17a, 0x17d,  0x3,    0x2,    0x2,    0x2,    0x17b,  0x179,  0x3,
      0x2,   0x2,    0x2,    0x17b,  0x17c,  0x3,    0x2,    0x2,    0x2,
      0x17c, 0x17e,  0x3,    0x2,    0x2,    0x2,    0x17d,  0x17b,  0x3,
      0x2,   0x2,    0x2,    0x17e,  0x17f,  0x5,    0x24,   0x13,   0x2,
      0x17f, 0x180,  0x5,    0x26,   0x14,   0x2,    0x180,  0x1b,   0x3,
      0x2,   0x2,    0x2,    0x181,  0x184,  0x7,    0x2b,   0x2,    0x2,
      0x182, 0x185,  0x5,    0x1e,   0x10,   0x2,    0x183,  0x185,  0x5,
      0x20,  0x11,   0x2,    0x184,  0x182,  0x3,    0x2,    0x2,    0x2,
      0x184, 0x183,  0x3,    0x2,    0x2,    0x2,    0x185,  0x1d,   0x3,
      0x2,   0x2,    0x2,    0x186,  0x187,  0x5,    0x22,   0x12,   0x2,
      0x187, 0x1f,   0x3,    0x2,    0x2,    0x2,    0x188,  0x189,  0x7,
      0x2c,  0x2,    0x2,    0x189,  0x18a,  0x5,    0x22,   0x12,   0x2,
      0x18a, 0x21,   0x3,    0x2,    0x2,    0x2,    0x18b,  0x18c,  0x5,
      0xfe,  0x80,   0x2,    0x18c,  0x23,   0x3,    0x2,    0x2,    0x2,
      0x18d, 0x18f,  0x7,    0x28,   0x2,    0x2,    0x18e,  0x18d,  0x3,
      0x2,   0x2,    0x2,    0x18e,  0x18f,  0x3,    0x2,    0x2,    0x2,
      0x18f, 0x190,  0x3,    0x2,    0x2,    0x2,    0x190,  0x191,  0x5,
      0x40,  0x21,   0x2,    0x191,  0x25,   0x3,    0x2,    0x2,    0x2,
      0x192, 0x194,  0x5,    0x28,   0x15,   0x2,    0x193,  0x192,  0x3,
      0x2,   0x2,    0x2,    0x193,  0x194,  0x3,    0x2,    0x2,    0x2,
      0x194, 0x196,  0x3,    0x2,    0x2,    0x2,    0x195,  0x197,  0x5,
      0x2c,  0x17,   0x2,    0x196,  0x195,  0x3,    0x2,    0x2,    0x2,
      0x196, 0x197,  0x3,    0x2,    0x2,    0x2,    0x197,  0x199,  0x3,
      0x2,   0x2,    0x2,    0x198,  0x19a,  0x5,    0x30,   0x19,   0x2,
      0x199, 0x198,  0x3,    0x2,    0x2,    0x2,    0x199,  0x19a,  0x3,
      0x2,   0x2,    0x2,    0x19a,  0x19c,  0x3,    0x2,    0x2,    0x2,
      0x19b, 0x19d,  0x5,    0x34,   0x1b,   0x2,    0x19c,  0x19b,  0x3,
      0x2,   0x2,    0x2,    0x19c,  0x19d,  0x3,    0x2,    0x2,    0x2,
      0x19d, 0x27,   0x3,    0x2,    0x2,    0x2,    0x19e,  0x1a0,  0x7,
      0x2d,  0x2,    0x2,    0x19f,  0x1a1,  0x5,    0x2a,   0x16,   0x2,
      0x1a0, 0x19f,  0x3,    0x2,    0x2,    0x2,    0x1a1,  0x1a2,  0x3,
      0x2,   0x2,    0x2,    0x1a2,  0x1a0,  0x3,    0x2,    0x2,    0x2,
      0x1a2, 0x1a3,  0x3,    0x2,    0x2,    0x2,    0x1a3,  0x29,   0x3,
      0x2,   0x2,    0x2,    0x1a4,  0x1b0,  0x5,    0xe0,   0x71,   0x2,
      0x1a5, 0x1b0,  0x5,    0x66,   0x34,   0x2,    0x1a6,  0x1a7,  0x7,
      0x4,   0x2,    0x2,    0x1a7,  0x1aa,  0x5,    0xbc,   0x5f,   0x2,
      0x1a8, 0x1a9,  0x7,    0x26,   0x2,    0x2,    0x1a9,  0x1ab,  0x5,
      0xb8,  0x5d,   0x2,    0x1aa,  0x1a8,  0x3,    0x2,    0x2,    0x2,
      0x1aa, 0x1ab,  0x3,    0x2,    0x2,    0x2,    0x1ab,  0x1ac,  0x3,
      0x2,   0x2,    0x2,    0x1ac,  0x1ad,  0x7,    0x5,    0x2,    0x2,
      0x1ad, 0x1b0,  0x3,    0x2,    0x2,    0x2,    0x1ae,  0x1b0,  0x5,
      0xb8,  0x5d,   0x2,    0x1af,  0x1a4,  0x3,    0x2,    0x2,    0x2,
      0x1af, 0x1a5,  0x3,    0x2,    0x2,    0x2,    0x1af,  0x1a6,  0x3,
      0x2,   0x2,    0x2,    0x1af,  0x1ae,  0x3,    0x2,    0x2,    0x2,
      0x1b0, 0x2b,   0x3,    0x2,    0x2,    0x2,    0x1b1,  0x1b3,  0x7,
      0x2f,  0x2,    0x2,    0x1b2,  0x1b4,  0x5,    0x2e,   0x18,   0x2,
      0x1b3, 0x1b2,  0x3,    0x2,    0x2,    0x2,    0x1b4,  0x1b5,  0x3,
      0x2,   0x2,    0x2,    0x1b5,  0x1b3,  0x3,    0x2,    0x2,    0x2,
      0x1b5, 0x1b6,  0x3,    0x2,    0x2,    0x2,    0x1b6,  0x2d,   0x3,
      0x2,   0x2,    0x2,    0x1b7,  0x1b8,  0x5,    0x64,   0x33,   0x2,
      0x1b8, 0x2f,   0x3,    0x2,    0x2,    0x2,    0x1b9,  0x1bb,  0x7,
      0x30,  0x2,    0x2,    0x1ba,  0x1bc,  0x5,    0x32,   0x1a,   0x2,
      0x1bb, 0x1ba,  0x3,    0x2,    0x2,    0x2,    0x1bc,  0x1bd,  0x3,
      0x2,   0x2,    0x2,    0x1bd,  0x1bb,  0x3,    0x2,    0x2,    0x2,
      0x1bd, 0x1be,  0x3,    0x2,    0x2,    0x2,    0x1be,  0x31,   0x3,
      0x2,   0x2,    0x2,    0x1bf,  0x1c0,  0x9,    0x3,    0x2,    0x2,
      0x1c0, 0x1c6,  0x5,    0xde,   0x70,   0x2,    0x1c1,  0x1c4,  0x5,
      0x64,  0x33,   0x2,    0x1c2,  0x1c4,  0x5,    0xb8,   0x5d,   0x2,
      0x1c3, 0x1c1,  0x3,    0x2,    0x2,    0x2,    0x1c3,  0x1c2,  0x3,
      0x2,   0x2,    0x2,    0x1c4,  0x1c6,  0x3,    0x2,    0x2,    0x2,
      0x1c5, 0x1bf,  0x3,    0x2,    0x2,    0x2,    0x1c5,  0x1c3,  0x3,
      0x2,   0x2,    0x2,    0x1c6,  0x33,   0x3,    0x2,    0x2,    0x2,
      0x1c7, 0x1c9,  0x5,    0x36,   0x1c,   0x2,    0x1c8,  0x1ca,  0x5,
      0x38,  0x1d,   0x2,    0x1c9,  0x1c8,  0x3,    0x2,    0x2,    0x2,
      0x1c9, 0x1ca,  0x3,    0x2,    0x2,    0x2,    0x1ca,  0x1cc,  0x3,
      0x2,   0x2,    0x2,    0x1cb,  0x1cd,  0x5,    0x3a,   0x1e,   0x2,
      0x1cc, 0x1cb,  0x3,    0x2,    0x2,    0x2,    0x1cc,  0x1cd,  0x3,
      0x2,   0x2,    0x2,    0x1cd,  0x1f2,  0x3,    0x2,    0x2,    0x2,
      0x1ce, 0x1d0,  0x5,    0x36,   0x1c,   0x2,    0x1cf,  0x1d1,  0x5,
      0x3a,  0x1e,   0x2,    0x1d0,  0x1cf,  0x3,    0x2,    0x2,    0x2,
      0x1d0, 0x1d1,  0x3,    0x2,    0x2,    0x2,    0x1d1,  0x1d3,  0x3,
      0x2,   0x2,    0x2,    0x1d2,  0x1d4,  0x5,    0x38,   0x1d,   0x2,
      0x1d3, 0x1d2,  0x3,    0x2,    0x2,    0x2,    0x1d3,  0x1d4,  0x3,
      0x2,   0x2,    0x2,    0x1d4,  0x1f2,  0x3,    0x2,    0x2,    0x2,
      0x1d5, 0x1d7,  0x5,    0x38,   0x1d,   0x2,    0x1d6,  0x1d8,  0x5,
      0x36,  0x1c,   0x2,    0x1d7,  0x1d6,  0x3,    0x2,    0x2,    0x2,
      0x1d7, 0x1d8,  0x3,    0x2,    0x2,    0x2,    0x1d8,  0x1da,  0x3,
      0x2,   0x2,    0x2,    0x1d9,  0x1db,  0x5,    0x3a,   0x1e,   0x2,
      0x1da, 0x1d9,  0x3,    0x2,    0x2,    0x2,    0x1da,  0x1db,  0x3,
      0x2,   0x2,    0x2,    0x1db,  0x1f2,  0x3,    0x2,    0x2,    0x2,
      0x1dc, 0x1de,  0x5,    0x38,   0x1d,   0x2,    0x1dd,  0x1df,  0x5,
      0x3a,  0x1e,   0x2,    0x1de,  0x1dd,  0x3,    0x2,    0x2,    0x2,
      0x1de, 0x1df,  0x3,    0x2,    0x2,    0x2,    0x1df,  0x1e1,  0x3,
      0x2,   0x2,    0x2,    0x1e0,  0x1e2,  0x5,    0x36,   0x1c,   0x2,
      0x1e1, 0x1e0,  0x3,    0x2,    0x2,    0x2,    0x1e1,  0x1e2,  0x3,
      0x2,   0x2,    0x2,    0x1e2,  0x1f2,  0x3,    0x2,    0x2,    0x2,
      0x1e3, 0x1e5,  0x5,    0x3a,   0x1e,   0x2,    0x1e4,  0x1e6,  0x5,
      0x38,  0x1d,   0x2,    0x1e5,  0x1e4,  0x3,    0x2,    0x2,    0x2,
      0x1e5, 0x1e6,  0x3,    0x2,    0x2,    0x2,    0x1e6,  0x1e8,  0x3,
      0x2,   0x2,    0x2,    0x1e7,  0x1e9,  0x5,    0x36,   0x1c,   0x2,
      0x1e8, 0x1e7,  0x3,    0x2,    0x2,    0x2,    0x1e8,  0x1e9,  0x3,
      0x2,   0x2,    0x2,    0x1e9,  0x1f2,  0x3,    0x2,    0x2,    0x2,
      0x1ea, 0x1ec,  0x5,    0x3a,   0x1e,   0x2,    0x1eb,  0x1ed,  0x5,
      0x36,  0x1c,   0x2,    0x1ec,  0x1eb,  0x3,    0x2,    0x2,    0x2,
      0x1ec, 0x1ed,  0x3,    0x2,    0x2,    0x2,    0x1ed,  0x1ef,  0x3,
      0x2,   0x2,    0x2,    0x1ee,  0x1f0,  0x5,    0x38,   0x1d,   0x2,
      0x1ef, 0x1ee,  0x3,    0x2,    0x2,    0x2,    0x1ef,  0x1f0,  0x3,
      0x2,   0x2,    0x2,    0x1f0,  0x1f2,  0x3,    0x2,    0x2,    0x2,
      0x1f1, 0x1c7,  0x3,    0x2,    0x2,    0x2,    0x1f1,  0x1ce,  0x3,
      0x2,   0x2,    0x2,    0x1f1,  0x1d5,  0x3,    0x2,    0x2,    0x2,
      0x1f1, 0x1dc,  0x3,    0x2,    0x2,    0x2,    0x1f1,  0x1e3,  0x3,
      0x2,   0x2,    0x2,    0x1f1,  0x1ea,  0x3,    0x2,    0x2,    0x2,
      0x1f2, 0x35,   0x3,    0x2,    0x2,    0x2,    0x1f3,  0x1f4,  0x7,
      0x33,  0x2,    0x2,    0x1f4,  0x1f5,  0x5,    0xa2,   0x52,   0x2,
      0x1f5, 0x37,   0x3,    0x2,    0x2,    0x2,    0x1f6,  0x1f7,  0x7,
      0x34,  0x2,    0x2,    0x1f7,  0x1f8,  0x5,    0xa2,   0x52,   0x2,
      0x1f8, 0x39,   0x3,    0x2,    0x2,    0x2,    0x1f9,  0x1fa,  0x7,
      0x35,  0x2,    0x2,    0x1fa,  0x1fb,  0x5,    0xa2,   0x52,   0x2,
      0x1fb, 0x3b,   0x3,    0x2,    0x2,    0x2,    0x1fc,  0x1fd,  0x7,
      0x36,  0x2,    0x2,    0x1fd,  0x1ff,  0x5,    0x54,   0x2b,   0x2,
      0x1fe, 0x1fc,  0x3,    0x2,    0x2,    0x2,    0x1fe,  0x1ff,  0x3,
      0x2,   0x2,    0x2,    0x1ff,  0x3d,   0x3,    0x2,    0x2,    0x2,
      0x200, 0x205,  0x5,    0x70,   0x39,   0x2,    0x201,  0x203,  0x7,
      0x8,   0x2,    0x2,    0x202,  0x204,  0x5,    0x3e,   0x20,   0x2,
      0x203, 0x202,  0x3,    0x2,    0x2,    0x2,    0x203,  0x204,  0x3,
      0x2,   0x2,    0x2,    0x204,  0x206,  0x3,    0x2,    0x2,    0x2,
      0x205, 0x201,  0x3,    0x2,    0x2,    0x2,    0x205,  0x206,  0x3,
      0x2,   0x2,    0x2,    0x206,  0x3f,   0x3,    0x2,    0x2,    0x2,
      0x207, 0x20a,  0x7,    0x6,    0x2,    0x2,    0x208,  0x20b,  0x5,
      0xc,   0x7,    0x2,    0x209,  0x20b,  0x5,    0x42,   0x22,   0x2,
      0x20a, 0x208,  0x3,    0x2,    0x2,    0x2,    0x20a,  0x209,  0x3,
      0x2,   0x2,    0x2,    0x20b,  0x20c,  0x3,    0x2,    0x2,    0x2,
      0x20c, 0x20d,  0x7,    0x7,    0x2,    0x2,    0x20d,  0x41,   0x3,
      0x2,   0x2,    0x2,    0x20e,  0x210,  0x5,    0x46,   0x24,   0x2,
      0x20f, 0x20e,  0x3,    0x2,    0x2,    0x2,    0x20f,  0x210,  0x3,
      0x2,   0x2,    0x2,    0x210,  0x214,  0x3,    0x2,    0x2,    0x2,
      0x211, 0x213,  0x5,    0x44,   0x23,   0x2,    0x212,  0x211,  0x3,
      0x2,   0x2,    0x2,    0x213,  0x216,  0x3,    0x2,    0x2,    0x2,
      0x214, 0x212,  0x3,    0x2,    0x2,    0x2,    0x214,  0x215,  0x3,
      0x2,   0x2,    0x2,    0x215,  0x43,   0x3,    0x2,    0x2,    0x2,
      0x216, 0x214,  0x3,    0x2,    0x2,    0x2,    0x217,  0x219,  0x5,
      0x48,  0x25,   0x2,    0x218,  0x21a,  0x7,    0x8,    0x2,    0x2,
      0x219, 0x218,  0x3,    0x2,    0x2,    0x2,    0x219,  0x21a,  0x3,
      0x2,   0x2,    0x2,    0x21a,  0x21c,  0x3,    0x2,    0x2,    0x2,
      0x21b, 0x21d,  0x5,    0x46,   0x24,   0x2,    0x21c,  0x21b,  0x3,
      0x2,   0x2,    0x2,    0x21c,  0x21d,  0x3,    0x2,    0x2,    0x2,
      0x21d, 0x45,   0x3,    0x2,    0x2,    0x2,    0x21e,  0x223,  0x5,
      0x7c,  0x3f,   0x2,    0x21f,  0x221,  0x7,    0x8,    0x2,    0x2,
      0x220, 0x222,  0x5,    0x46,   0x24,   0x2,    0x221,  0x220,  0x3,
      0x2,   0x2,    0x2,    0x221,  0x222,  0x3,    0x2,    0x2,    0x2,
      0x222, 0x224,  0x3,    0x2,    0x2,    0x2,    0x223,  0x21f,  0x3,
      0x2,   0x2,    0x2,    0x223,  0x224,  0x3,    0x2,    0x2,    0x2,
      0x224, 0x47,   0x3,    0x2,    0x2,    0x2,    0x225,  0x22e,  0x5,
      0x60,  0x31,   0x2,    0x226,  0x22e,  0x5,    0x4a,   0x26,   0x2,
      0x227, 0x22e,  0x5,    0x5e,   0x30,   0x2,    0x228,  0x22e,  0x5,
      0x4c,  0x27,   0x2,    0x229,  0x22e,  0x5,    0x4e,   0x28,   0x2,
      0x22a, 0x22e,  0x5,    0x62,   0x32,   0x2,    0x22b,  0x22e,  0x5,
      0x50,  0x29,   0x2,    0x22c,  0x22e,  0x5,    0x52,   0x2a,   0x2,
      0x22d, 0x225,  0x3,    0x2,    0x2,    0x2,    0x22d,  0x226,  0x3,
      0x2,   0x2,    0x2,    0x22d,  0x227,  0x3,    0x2,    0x2,    0x2,
      0x22d, 0x228,  0x3,    0x2,    0x2,    0x2,    0x22d,  0x229,  0x3,
      0x2,   0x2,    0x2,    0x22d,  0x22a,  0x3,    0x2,    0x2,    0x2,
      0x22d, 0x22b,  0x3,    0x2,    0x2,    0x2,    0x22d,  0x22c,  0x3,
      0x2,   0x2,    0x2,    0x22e,  0x49,   0x3,    0x2,    0x2,    0x2,
      0x22f, 0x230,  0x7,    0x47,   0x2,    0x2,    0x230,  0x231,  0x5,
      0x40,  0x21,   0x2,    0x231,  0x4b,   0x3,    0x2,    0x2,    0x2,
      0x232, 0x233,  0x7,    0x45,   0x2,    0x2,    0x233,  0x234,  0x5,
      0xb6,  0x5c,   0x2,    0x234,  0x235,  0x5,    0x40,   0x21,   0x2,
      0x235, 0x4d,   0x3,    0x2,    0x2,    0x2,    0x236,  0x238,  0x7,
      0x48,  0x2,    0x2,    0x237,  0x239,  0x7,    0x38,   0x2,    0x2,
      0x238, 0x237,  0x3,    0x2,    0x2,    0x2,    0x238,  0x239,  0x3,
      0x2,   0x2,    0x2,    0x239,  0x23a,  0x3,    0x2,    0x2,    0x2,
      0x23a, 0x23b,  0x5,    0xb6,   0x5c,   0x2,    0x23b,  0x23c,  0x5,
      0x40,  0x21,   0x2,    0x23c,  0x4f,   0x3,    0x2,    0x2,    0x2,
      0x23d, 0x23e,  0x7,    0x49,   0x2,    0x2,    0x23e,  0x23f,  0x7,
      0x4,   0x2,    0x2,    0x23f,  0x240,  0x5,    0xbc,   0x5f,   0x2,
      0x240, 0x241,  0x7,    0x26,   0x2,    0x2,    0x241,  0x242,  0x5,
      0xb8,  0x5d,   0x2,    0x242,  0x243,  0x7,    0x5,    0x2,    0x2,
      0x243, 0x51,   0x3,    0x2,    0x2,    0x2,    0x244,  0x245,  0x7,
      0x36,  0x2,    0x2,    0x245,  0x246,  0x5,    0x54,   0x2b,   0x2,
      0x246, 0x53,   0x3,    0x2,    0x2,    0x2,    0x247,  0x24a,  0x5,
      0x56,  0x2c,   0x2,    0x248,  0x24a,  0x5,    0x58,   0x2d,   0x2,
      0x249, 0x247,  0x3,    0x2,    0x2,    0x2,    0x249,  0x248,  0x3,
      0x2,   0x2,    0x2,    0x24a,  0x55,   0x3,    0x2,    0x2,    0x2,
      0x24b, 0x24c,  0x5,    0xb8,   0x5d,   0x2,    0x24c,  0x250,  0x7,
      0x6,   0x2,    0x2,    0x24d,  0x24f,  0x5,    0x5c,   0x2f,   0x2,
      0x24e, 0x24d,  0x3,    0x2,    0x2,    0x2,    0x24f,  0x252,  0x3,
      0x2,   0x2,    0x2,    0x250,  0x24e,  0x3,    0x2,    0x2,    0x2,
      0x250, 0x251,  0x3,    0x2,    0x2,    0x2,    0x251,  0x253,  0x3,
      0x2,   0x2,    0x2,    0x252,  0x250,  0x3,    0x2,    0x2,    0x2,
      0x253, 0x254,  0x7,    0x7,    0x2,    0x2,    0x254,  0x57,   0x3,
      0x2,   0x2,    0x2,    0x255,  0x25f,  0x7,    0xa4,   0x2,    0x2,
      0x256, 0x25a,  0x7,    0x4,    0x2,    0x2,    0x257,  0x259,  0x5,
      0xb8,  0x5d,   0x2,    0x258,  0x257,  0x3,    0x2,    0x2,    0x2,
      0x259, 0x25c,  0x3,    0x2,    0x2,    0x2,    0x25a,  0x258,  0x3,
      0x2,   0x2,    0x2,    0x25a,  0x25b,  0x3,    0x2,    0x2,    0x2,
      0x25b, 0x25d,  0x3,    0x2,    0x2,    0x2,    0x25c,  0x25a,  0x3,
      0x2,   0x2,    0x2,    0x25d,  0x25f,  0x7,    0x5,    0x2,    0x2,
      0x25e, 0x255,  0x3,    0x2,    0x2,    0x2,    0x25e,  0x256,  0x3,
      0x2,   0x2,    0x2,    0x25f,  0x260,  0x3,    0x2,    0x2,    0x2,
      0x260, 0x264,  0x7,    0x6,    0x2,    0x2,    0x261,  0x263,  0x5,
      0x5a,  0x2e,   0x2,    0x262,  0x261,  0x3,    0x2,    0x2,    0x2,
      0x263, 0x266,  0x3,    0x2,    0x2,    0x2,    0x264,  0x262,  0x3,
      0x2,   0x2,    0x2,    0x264,  0x265,  0x3,    0x2,    0x2,    0x2,
      0x265, 0x267,  0x3,    0x2,    0x2,    0x2,    0x266,  0x264,  0x3,
      0x2,   0x2,    0x2,    0x267,  0x268,  0x7,    0x7,    0x2,    0x2,
      0x268, 0x59,   0x3,    0x2,    0x2,    0x2,    0x269,  0x26d,  0x7,
      0x4,   0x2,    0x2,    0x26a,  0x26c,  0x5,    0x5c,   0x2f,   0x2,
      0x26b, 0x26a,  0x3,    0x2,    0x2,    0x2,    0x26c,  0x26f,  0x3,
      0x2,   0x2,    0x2,    0x26d,  0x26b,  0x3,    0x2,    0x2,    0x2,
      0x26d, 0x26e,  0x3,    0x2,    0x2,    0x2,    0x26e,  0x270,  0x3,
      0x2,   0x2,    0x2,    0x26f,  0x26d,  0x3,    0x2,    0x2,    0x2,
      0x270, 0x273,  0x7,    0x5,    0x2,    0x2,    0x271,  0x273,  0x7,
      0xa4,  0x2,    0x2,    0x272,  0x269,  0x3,    0x2,    0x2,    0x2,
      0x272, 0x271,  0x3,    0x2,    0x2,    0x2,    0x273,  0x5b,   0x3,
      0x2,   0x2,    0x2,    0x274,  0x27a,  0x5,    0xfe,   0x80,   0x2,
      0x275, 0x27a,  0x5,    0xf0,   0x79,   0x2,    0x276,  0x27a,  0x5,
      0xf2,  0x7a,   0x2,    0x277,  0x27a,  0x5,    0xfa,   0x7e,   0x2,
      0x278, 0x27a,  0x7,    0x4a,   0x2,    0x2,    0x279,  0x274,  0x3,
      0x2,   0x2,    0x2,    0x279,  0x275,  0x3,    0x2,    0x2,    0x2,
      0x279, 0x276,  0x3,    0x2,    0x2,    0x2,    0x279,  0x277,  0x3,
      0x2,   0x2,    0x2,    0x279,  0x278,  0x3,    0x2,    0x2,    0x2,
      0x27a, 0x5d,   0x3,    0x2,    0x2,    0x2,    0x27b,  0x27c,  0x7,
      0x4b,  0x2,    0x2,    0x27c,  0x27d,  0x5,    0x40,   0x21,   0x2,
      0x27d, 0x5f,   0x3,    0x2,    0x2,    0x2,    0x27e,  0x283,  0x5,
      0x40,  0x21,   0x2,    0x27f,  0x280,  0x7,    0x4c,   0x2,    0x2,
      0x280, 0x282,  0x5,    0x40,   0x21,   0x2,    0x281,  0x27f,  0x3,
      0x2,   0x2,    0x2,    0x282,  0x285,  0x3,    0x2,    0x2,    0x2,
      0x283, 0x281,  0x3,    0x2,    0x2,    0x2,    0x283,  0x284,  0x3,
      0x2,   0x2,    0x2,    0x284,  0x61,   0x3,    0x2,    0x2,    0x2,
      0x285, 0x283,  0x3,    0x2,    0x2,    0x2,    0x286,  0x287,  0x7,
      0x4d,  0x2,    0x2,    0x287,  0x288,  0x5,    0x64,   0x33,   0x2,
      0x288, 0x63,   0x3,    0x2,    0x2,    0x2,    0x289,  0x28d,  0x5,
      0xde,  0x70,   0x2,    0x28a,  0x28d,  0x5,    0xe0,   0x71,   0x2,
      0x28b, 0x28d,  0x5,    0x66,   0x34,   0x2,    0x28c,  0x289,  0x3,
      0x2,   0x2,    0x2,    0x28c,  0x28a,  0x3,    0x2,    0x2,    0x2,
      0x28c, 0x28b,  0x3,    0x2,    0x2,    0x2,    0x28d,  0x65,   0x3,
      0x2,   0x2,    0x2,    0x28e,  0x28f,  0x5,    0xfe,   0x80,   0x2,
      0x28f, 0x290,  0x5,    0x68,   0x35,   0x2,    0x290,  0x67,   0x3,
      0x2,   0x2,    0x2,    0x291,  0x2a1,  0x7,    0xa4,   0x2,    0x2,
      0x292, 0x294,  0x7,    0x4,    0x2,    0x2,    0x293,  0x295,  0x7,
      0x24,  0x2,    0x2,    0x294,  0x293,  0x3,    0x2,    0x2,    0x2,
      0x294, 0x295,  0x3,    0x2,    0x2,    0x2,    0x295,  0x296,  0x3,
      0x2,   0x2,    0x2,    0x296,  0x29b,  0x5,    0xbc,   0x5f,   0x2,
      0x297, 0x298,  0x7,    0x9,    0x2,    0x2,    0x298,  0x29a,  0x5,
      0xbc,  0x5f,   0x2,    0x299,  0x297,  0x3,    0x2,    0x2,    0x2,
      0x29a, 0x29d,  0x3,    0x2,    0x2,    0x2,    0x29b,  0x299,  0x3,
      0x2,   0x2,    0x2,    0x29b,  0x29c,  0x3,    0x2,    0x2,    0x2,
      0x29c, 0x29e,  0x3,    0x2,    0x2,    0x2,    0x29d,  0x29b,  0x3,
      0x2,   0x2,    0x2,    0x29e,  0x29f,  0x7,    0x5,    0x2,    0x2,
      0x29f, 0x2a1,  0x3,    0x2,    0x2,    0x2,    0x2a0,  0x291,  0x3,
      0x2,   0x2,    0x2,    0x2a0,  0x292,  0x3,    0x2,    0x2,    0x2,
      0x2a1, 0x69,   0x3,    0x2,    0x2,    0x2,    0x2a2,  0x2af,  0x7,
      0xa4,  0x2,    0x2,    0x2a3,  0x2a4,  0x7,    0x4,    0x2,    0x2,
      0x2a4, 0x2a9,  0x5,    0xbc,   0x5f,   0x2,    0x2a5,  0x2a6,  0x7,
      0x9,   0x2,    0x2,    0x2a6,  0x2a8,  0x5,    0xbc,   0x5f,   0x2,
      0x2a7, 0x2a5,  0x3,    0x2,    0x2,    0x2,    0x2a8,  0x2ab,  0x3,
      0x2,   0x2,    0x2,    0x2a9,  0x2a7,  0x3,    0x2,    0x2,    0x2,
      0x2a9, 0x2aa,  0x3,    0x2,    0x2,    0x2,    0x2aa,  0x2ac,  0x3,
      0x2,   0x2,    0x2,    0x2ab,  0x2a9,  0x3,    0x2,    0x2,    0x2,
      0x2ac, 0x2ad,  0x7,    0x5,    0x2,    0x2,    0x2ad,  0x2af,  0x3,
      0x2,   0x2,    0x2,    0x2ae,  0x2a2,  0x3,    0x2,    0x2,    0x2,
      0x2ae, 0x2a3,  0x3,    0x2,    0x2,    0x2,    0x2af,  0x6b,   0x3,
      0x2,   0x2,    0x2,    0x2b0,  0x2b2,  0x7,    0x6,    0x2,    0x2,
      0x2b1, 0x2b3,  0x5,    0x6e,   0x38,   0x2,    0x2b2,  0x2b1,  0x3,
      0x2,   0x2,    0x2,    0x2b2,  0x2b3,  0x3,    0x2,    0x2,    0x2,
      0x2b3, 0x2b4,  0x3,    0x2,    0x2,    0x2,    0x2b4,  0x2b5,  0x7,
      0x7,   0x2,    0x2,    0x2b5,  0x6d,   0x3,    0x2,    0x2,    0x2,
      0x2b6, 0x2bb,  0x5,    0x70,   0x39,   0x2,    0x2b7,  0x2b9,  0x7,
      0x8,   0x2,    0x2,    0x2b8,  0x2ba,  0x5,    0x6e,   0x38,   0x2,
      0x2b9, 0x2b8,  0x3,    0x2,    0x2,    0x2,    0x2b9,  0x2ba,  0x3,
      0x2,   0x2,    0x2,    0x2ba,  0x2bc,  0x3,    0x2,    0x2,    0x2,
      0x2bb, 0x2b7,  0x3,    0x2,    0x2,    0x2,    0x2bb,  0x2bc,  0x3,
      0x2,   0x2,    0x2,    0x2bc,  0x6f,   0x3,    0x2,    0x2,    0x2,
      0x2bd, 0x2be,  0x5,    0xb4,   0x5b,   0x2,    0x2be,  0x2bf,  0x5,
      0x74,  0x3b,   0x2,    0x2bf,  0x2c4,  0x3,    0x2,    0x2,    0x2,
      0x2c0, 0x2c1,  0x5,    0xa4,   0x53,   0x2,    0x2c1,  0x2c2,  0x5,
      0x72,  0x3a,   0x2,    0x2c2,  0x2c4,  0x3,    0x2,    0x2,    0x2,
      0x2c3, 0x2bd,  0x3,    0x2,    0x2,    0x2,    0x2c3,  0x2c0,  0x3,
      0x2,   0x2,    0x2,    0x2c4,  0x71,   0x3,    0x2,    0x2,    0x2,
      0x2c5, 0x2c7,  0x5,    0x74,   0x3b,   0x2,    0x2c6,  0x2c5,  0x3,
      0x2,   0x2,    0x2,    0x2c6,  0x2c7,  0x3,    0x2,    0x2,    0x2,
      0x2c7, 0x73,   0x3,    0x2,    0x2,    0x2,    0x2c8,  0x2c9,  0x5,
      0x76,  0x3c,   0x2,    0x2c9,  0x2d2,  0x5,    0x78,   0x3d,   0x2,
      0x2ca, 0x2ce,  0x7,    0xa,    0x2,    0x2,    0x2cb,  0x2cc,  0x5,
      0x76,  0x3c,   0x2,    0x2cc,  0x2cd,  0x5,    0x78,   0x3d,   0x2,
      0x2cd, 0x2cf,  0x3,    0x2,    0x2,    0x2,    0x2ce,  0x2cb,  0x3,
      0x2,   0x2,    0x2,    0x2ce,  0x2cf,  0x3,    0x2,    0x2,    0x2,
      0x2cf, 0x2d1,  0x3,    0x2,    0x2,    0x2,    0x2d0,  0x2ca,  0x3,
      0x2,   0x2,    0x2,    0x2d1,  0x2d4,  0x3,    0x2,    0x2,    0x2,
      0x2d2, 0x2d0,  0x3,    0x2,    0x2,    0x2,    0x2d2,  0x2d3,  0x3,
      0x2,   0x2,    0x2,    0x2d3,  0x75,   0x3,    0x2,    0x2,    0x2,
      0x2d4, 0x2d2,  0x3,    0x2,    0x2,    0x2,    0x2d5,  0x2d8,  0x5,
      0xb6,  0x5c,   0x2,    0x2d6,  0x2d8,  0x7,    0xb,    0x2,    0x2,
      0x2d7, 0x2d5,  0x3,    0x2,    0x2,    0x2,    0x2d7,  0x2d6,  0x3,
      0x2,   0x2,    0x2,    0x2d8,  0x77,   0x3,    0x2,    0x2,    0x2,
      0x2d9, 0x2de,  0x5,    0x7a,   0x3e,   0x2,    0x2da,  0x2db,  0x7,
      0x9,   0x2,    0x2,    0x2db,  0x2dd,  0x5,    0x7a,   0x3e,   0x2,
      0x2dc, 0x2da,  0x3,    0x2,    0x2,    0x2,    0x2dd,  0x2e0,  0x3,
      0x2,   0x2,    0x2,    0x2de,  0x2dc,  0x3,    0x2,    0x2,    0x2,
      0x2de, 0x2df,  0x3,    0x2,    0x2,    0x2,    0x2df,  0x79,   0x3,
      0x2,   0x2,    0x2,    0x2e0,  0x2de,  0x3,    0x2,    0x2,    0x2,
      0x2e1, 0x2e2,  0x5,    0xb0,   0x59,   0x2,    0x2e2,  0x7b,   0x3,
      0x2,   0x2,    0x2,    0x2e3,  0x2e4,  0x5,    0xb4,   0x5b,   0x2,
      0x2e4, 0x2e5,  0x5,    0x80,   0x41,   0x2,    0x2e5,  0x2ea,  0x3,
      0x2,   0x2,    0x2,    0x2e6,  0x2e7,  0x5,    0xa8,   0x55,   0x2,
      0x2e7, 0x2e8,  0x5,    0x7e,   0x40,   0x2,    0x2e8,  0x2ea,  0x3,
      0x2,   0x2,    0x2,    0x2e9,  0x2e3,  0x3,    0x2,    0x2,    0x2,
      0x2e9, 0x2e6,  0x3,    0x2,    0x2,    0x2,    0x2ea,  0x7d,   0x3,
      0x2,   0x2,    0x2,    0x2eb,  0x2ed,  0x5,    0x80,   0x41,   0x2,
      0x2ec, 0x2eb,  0x3,    0x2,    0x2,    0x2,    0x2ec,  0x2ed,  0x3,
      0x2,   0x2,    0x2,    0x2ed,  0x7f,   0x3,    0x2,    0x2,    0x2,
      0x2ee, 0x2f5,  0x5,    0x88,   0x45,   0x2,    0x2ef,  0x2f1,  0x7,
      0xa,   0x2,    0x2,    0x2f0,  0x2f2,  0x5,    0x86,   0x44,   0x2,
      0x2f1, 0x2f0,  0x3,    0x2,    0x2,    0x2,    0x2f1,  0x2f2,  0x3,
      0x2,   0x2,    0x2,    0x2f2,  0x2f4,  0x3,    0x2,    0x2,    0x2,
      0x2f3, 0x2ef,  0x3,    0x2,    0x2,    0x2,    0x2f4,  0x2f7,  0x3,
      0x2,   0x2,    0x2,    0x2f5,  0x2f3,  0x3,    0x2,    0x2,    0x2,
      0x2f5, 0x2f6,  0x3,    0x2,    0x2,    0x2,    0x2f6,  0x81,   0x3,
      0x2,   0x2,    0x2,    0x2f7,  0x2f5,  0x3,    0x2,    0x2,    0x2,
      0x2f8, 0x2f9,  0x5,    0x90,   0x49,   0x2,    0x2f9,  0x83,   0x3,
      0x2,   0x2,    0x2,    0x2fa,  0x2fb,  0x5,    0xb8,   0x5d,   0x2,
      0x2fb, 0x85,   0x3,    0x2,    0x2,    0x2,    0x2fc,  0x2fd,  0x5,
      0x8a,  0x46,   0x2,    0x2fd,  0x2fe,  0x5,    0x78,   0x3d,   0x2,
      0x2fe, 0x87,   0x3,    0x2,    0x2,    0x2,    0x2ff,  0x300,  0x5,
      0x8a,  0x46,   0x2,    0x300,  0x301,  0x5,    0x8c,   0x47,   0x2,
      0x301, 0x89,   0x3,    0x2,    0x2,    0x2,    0x302,  0x305,  0x5,
      0x82,  0x42,   0x2,    0x303,  0x305,  0x5,    0x84,   0x43,   0x2,
      0x304, 0x302,  0x3,    0x2,    0x2,    0x2,    0x304,  0x303,  0x3,
      0x2,   0x2,    0x2,    0x305,  0x8b,   0x3,    0x2,    0x2,    0x2,
      0x306, 0x30b,  0x5,    0x8e,   0x48,   0x2,    0x307,  0x308,  0x7,
      0x9,   0x2,    0x2,    0x308,  0x30a,  0x5,    0x8e,   0x48,   0x2,
      0x309, 0x307,  0x3,    0x2,    0x2,    0x2,    0x30a,  0x30d,  0x3,
      0x2,   0x2,    0x2,    0x30b,  0x309,  0x3,    0x2,    0x2,    0x2,
      0x30b, 0x30c,  0x3,    0x2,    0x2,    0x2,    0x30c,  0x8d,   0x3,
      0x2,   0x2,    0x2,    0x30d,  0x30b,  0x3,    0x2,    0x2,    0x2,
      0x30e, 0x30f,  0x5,    0xb2,   0x5a,   0x2,    0x30f,  0x8f,   0x3,
      0x2,   0x2,    0x2,    0x310,  0x311,  0x5,    0x92,   0x4a,   0x2,
      0x311, 0x91,   0x3,    0x2,    0x2,    0x2,    0x312,  0x317,  0x5,
      0x94,  0x4b,   0x2,    0x313,  0x314,  0x7,    0xc,    0x2,    0x2,
      0x314, 0x316,  0x5,    0x94,   0x4b,   0x2,    0x315,  0x313,  0x3,
      0x2,   0x2,    0x2,    0x316,  0x319,  0x3,    0x2,    0x2,    0x2,
      0x317, 0x315,  0x3,    0x2,    0x2,    0x2,    0x317,  0x318,  0x3,
      0x2,   0x2,    0x2,    0x318,  0x93,   0x3,    0x2,    0x2,    0x2,
      0x319, 0x317,  0x3,    0x2,    0x2,    0x2,    0x31a,  0x31f,  0x5,
      0x98,  0x4d,   0x2,    0x31b,  0x31c,  0x7,    0xd,    0x2,    0x2,
      0x31c, 0x31e,  0x5,    0x98,   0x4d,   0x2,    0x31d,  0x31b,  0x3,
      0x2,   0x2,    0x2,    0x31e,  0x321,  0x3,    0x2,    0x2,    0x2,
      0x31f, 0x31d,  0x3,    0x2,    0x2,    0x2,    0x31f,  0x320,  0x3,
      0x2,   0x2,    0x2,    0x320,  0x95,   0x3,    0x2,    0x2,    0x2,
      0x321, 0x31f,  0x3,    0x2,    0x2,    0x2,    0x322,  0x324,  0x5,
      0x9c,  0x4f,   0x2,    0x323,  0x325,  0x5,    0x9a,   0x4e,   0x2,
      0x324, 0x323,  0x3,    0x2,    0x2,    0x2,    0x324,  0x325,  0x3,
      0x2,   0x2,    0x2,    0x325,  0x97,   0x3,    0x2,    0x2,    0x2,
      0x326, 0x32a,  0x5,    0x96,   0x4c,   0x2,    0x327,  0x328,  0x7,
      0xe,   0x2,    0x2,    0x328,  0x32a,  0x5,    0x96,   0x4c,   0x2,
      0x329, 0x326,  0x3,    0x2,    0x2,    0x2,    0x329,  0x327,  0x3,
      0x2,   0x2,    0x2,    0x32a,  0x99,   0x3,    0x2,    0x2,    0x2,
      0x32b, 0x32c,  0x9,    0x4,    0x2,    0x2,    0x32c,  0x9b,   0x3,
      0x2,   0x2,    0x2,    0x32d,  0x336,  0x5,    0xfe,   0x80,   0x2,
      0x32e, 0x336,  0x7,    0xb,    0x2,    0x2,    0x32f,  0x330,  0x7,
      0x11,  0x2,    0x2,    0x330,  0x336,  0x5,    0x9e,   0x50,   0x2,
      0x331, 0x332,  0x7,    0x4,    0x2,    0x2,    0x332,  0x333,  0x5,
      0x90,  0x49,   0x2,    0x333,  0x334,  0x7,    0x5,    0x2,    0x2,
      0x334, 0x336,  0x3,    0x2,    0x2,    0x2,    0x335,  0x32d,  0x3,
      0x2,   0x2,    0x2,    0x335,  0x32e,  0x3,    0x2,    0x2,    0x2,
      0x335, 0x32f,  0x3,    0x2,    0x2,    0x2,    0x335,  0x331,  0x3,
      0x2,   0x2,    0x2,    0x336,  0x9d,   0x3,    0x2,    0x2,    0x2,
      0x337, 0x345,  0x5,    0xa0,   0x51,   0x2,    0x338,  0x341,  0x7,
      0x4,   0x2,    0x2,    0x339,  0x33e,  0x5,    0xa0,   0x51,   0x2,
      0x33a, 0x33b,  0x7,    0xc,    0x2,    0x2,    0x33b,  0x33d,  0x5,
      0xa0,  0x51,   0x2,    0x33c,  0x33a,  0x3,    0x2,    0x2,    0x2,
      0x33d, 0x340,  0x3,    0x2,    0x2,    0x2,    0x33e,  0x33c,  0x3,
      0x2,   0x2,    0x2,    0x33e,  0x33f,  0x3,    0x2,    0x2,    0x2,
      0x33f, 0x342,  0x3,    0x2,    0x2,    0x2,    0x340,  0x33e,  0x3,
      0x2,   0x2,    0x2,    0x341,  0x339,  0x3,    0x2,    0x2,    0x2,
      0x341, 0x342,  0x3,    0x2,    0x2,    0x2,    0x342,  0x343,  0x3,
      0x2,   0x2,    0x2,    0x343,  0x345,  0x7,    0x5,    0x2,    0x2,
      0x344, 0x337,  0x3,    0x2,    0x2,    0x2,    0x344,  0x338,  0x3,
      0x2,   0x2,    0x2,    0x345,  0x9f,   0x3,    0x2,    0x2,    0x2,
      0x346, 0x34e,  0x5,    0xfe,   0x80,   0x2,    0x347,  0x34e,  0x7,
      0xb,   0x2,    0x2,    0x348,  0x34b,  0x7,    0xe,    0x2,    0x2,
      0x349, 0x34c,  0x5,    0xfe,   0x80,   0x2,    0x34a,  0x34c,  0x7,
      0xb,   0x2,    0x2,    0x34b,  0x349,  0x3,    0x2,    0x2,    0x2,
      0x34b, 0x34a,  0x3,    0x2,    0x2,    0x2,    0x34c,  0x34e,  0x3,
      0x2,   0x2,    0x2,    0x34d,  0x346,  0x3,    0x2,    0x2,    0x2,
      0x34d, 0x347,  0x3,    0x2,    0x2,    0x2,    0x34d,  0x348,  0x3,
      0x2,   0x2,    0x2,    0x34e,  0xa1,   0x3,    0x2,    0x2,    0x2,
      0x34f, 0x350,  0x7,    0x95,   0x2,    0x2,    0x350,  0xa3,   0x3,
      0x2,   0x2,    0x2,    0x351,  0x354,  0x5,    0xac,   0x57,   0x2,
      0x352, 0x354,  0x5,    0xa6,   0x54,   0x2,    0x353,  0x351,  0x3,
      0x2,   0x2,    0x2,    0x353,  0x352,  0x3,    0x2,    0x2,    0x2,
      0x354, 0xa5,   0x3,    0x2,    0x2,    0x2,    0x355,  0x356,  0x7,
      0x12,  0x2,    0x2,    0x356,  0x357,  0x5,    0x74,   0x3b,   0x2,
      0x357, 0x358,  0x7,    0x13,   0x2,    0x2,    0x358,  0xa7,   0x3,
      0x2,   0x2,    0x2,    0x359,  0x35c,  0x5,    0xae,   0x58,   0x2,
      0x35a, 0x35c,  0x5,    0xaa,   0x56,   0x2,    0x35b,  0x359,  0x3,
      0x2,   0x2,    0x2,    0x35b,  0x35a,  0x3,    0x2,    0x2,    0x2,
      0x35c, 0xa9,   0x3,    0x2,    0x2,    0x2,    0x35d,  0x35e,  0x7,
      0x12,  0x2,    0x2,    0x35e,  0x35f,  0x5,    0x80,   0x41,   0x2,
      0x35f, 0x360,  0x7,    0x13,   0x2,    0x2,    0x360,  0xab,   0x3,
      0x2,   0x2,    0x2,    0x361,  0x363,  0x7,    0x4,    0x2,    0x2,
      0x362, 0x364,  0x5,    0xb0,   0x59,   0x2,    0x363,  0x362,  0x3,
      0x2,   0x2,    0x2,    0x364,  0x365,  0x3,    0x2,    0x2,    0x2,
      0x365, 0x363,  0x3,    0x2,    0x2,    0x2,    0x365,  0x366,  0x3,
      0x2,   0x2,    0x2,    0x366,  0x367,  0x3,    0x2,    0x2,    0x2,
      0x367, 0x368,  0x7,    0x5,    0x2,    0x2,    0x368,  0xad,   0x3,
      0x2,   0x2,    0x2,    0x369,  0x36b,  0x7,    0x4,    0x2,    0x2,
      0x36a, 0x36c,  0x5,    0xb2,   0x5a,   0x2,    0x36b,  0x36a,  0x3,
      0x2,   0x2,    0x2,    0x36c,  0x36d,  0x3,    0x2,    0x2,    0x2,
      0x36d, 0x36b,  0x3,    0x2,    0x2,    0x2,    0x36d,  0x36e,  0x3,
      0x2,   0x2,    0x2,    0x36e,  0x36f,  0x3,    0x2,    0x2,    0x2,
      0x36f, 0x370,  0x7,    0x5,    0x2,    0x2,    0x370,  0xaf,   0x3,
      0x2,   0x2,    0x2,    0x371,  0x374,  0x5,    0xb4,   0x5b,   0x2,
      0x372, 0x374,  0x5,    0xa4,   0x53,   0x2,    0x373,  0x371,  0x3,
      0x2,   0x2,    0x2,    0x373,  0x372,  0x3,    0x2,    0x2,    0x2,
      0x374, 0xb1,   0x3,    0x2,    0x2,    0x2,    0x375,  0x378,  0x5,
      0xb4,  0x5b,   0x2,    0x376,  0x378,  0x5,    0xa8,   0x55,   0x2,
      0x377, 0x375,  0x3,    0x2,    0x2,    0x2,    0x377,  0x376,  0x3,
      0x2,   0x2,    0x2,    0x378,  0xb3,   0x3,    0x2,    0x2,    0x2,
      0x379, 0x37c,  0x5,    0xb8,   0x5d,   0x2,    0x37a,  0x37c,  0x5,
      0xba,  0x5e,   0x2,    0x37b,  0x379,  0x3,    0x2,    0x2,    0x2,
      0x37b, 0x37a,  0x3,    0x2,    0x2,    0x2,    0x37c,  0xb5,   0x3,
      0x2,   0x2,    0x2,    0x37d,  0x380,  0x5,    0xb8,   0x5d,   0x2,
      0x37e, 0x380,  0x5,    0xfe,   0x80,   0x2,    0x37f,  0x37d,  0x3,
      0x2,   0x2,    0x2,    0x37f,  0x37e,  0x3,    0x2,    0x2,    0x2,
      0x380, 0xb7,   0x3,    0x2,    0x2,    0x2,    0x381,  0x382,  0x9,
      0x5,   0x2,    0x2,    0x382,  0xb9,   0x3,    0x2,    0x2,    0x2,
      0x383, 0x38a,  0x5,    0xfe,   0x80,   0x2,    0x384,  0x38a,  0x5,
      0xf0,  0x79,   0x2,    0x385,  0x38a,  0x5,    0xf2,   0x7a,   0x2,
      0x386, 0x38a,  0x5,    0xfa,   0x7e,   0x2,    0x387,  0x38a,  0x5,
      0x102, 0x82,   0x2,    0x388,  0x38a,  0x7,    0xa4,   0x2,    0x2,
      0x389, 0x383,  0x3,    0x2,    0x2,    0x2,    0x389,  0x384,  0x3,
      0x2,   0x2,    0x2,    0x389,  0x385,  0x3,    0x2,    0x2,    0x2,
      0x389, 0x386,  0x3,    0x2,    0x2,    0x2,    0x389,  0x387,  0x3,
      0x2,   0x2,    0x2,    0x389,  0x388,  0x3,    0x2,    0x2,    0x2,
      0x38a, 0xbb,   0x3,    0x2,    0x2,    0x2,    0x38b,  0x38c,  0x5,
      0xbe,  0x60,   0x2,    0x38c,  0xbd,   0x3,    0x2,    0x2,    0x2,
      0x38d, 0x392,  0x5,    0xc0,   0x61,   0x2,    0x38e,  0x38f,  0x7,
      0x14,  0x2,    0x2,    0x38f,  0x391,  0x5,    0xc0,   0x61,   0x2,
      0x390, 0x38e,  0x3,    0x2,    0x2,    0x2,    0x391,  0x394,  0x3,
      0x2,   0x2,    0x2,    0x392,  0x390,  0x3,    0x2,    0x2,    0x2,
      0x392, 0x393,  0x3,    0x2,    0x2,    0x2,    0x393,  0xbf,   0x3,
      0x2,   0x2,    0x2,    0x394,  0x392,  0x3,    0x2,    0x2,    0x2,
      0x395, 0x39a,  0x5,    0xc2,   0x62,   0x2,    0x396,  0x397,  0x7,
      0x15,  0x2,    0x2,    0x397,  0x399,  0x5,    0xc2,   0x62,   0x2,
      0x398, 0x396,  0x3,    0x2,    0x2,    0x2,    0x399,  0x39c,  0x3,
      0x2,   0x2,    0x2,    0x39a,  0x398,  0x3,    0x2,    0x2,    0x2,
      0x39a, 0x39b,  0x3,    0x2,    0x2,    0x2,    0x39b,  0xc1,   0x3,
      0x2,   0x2,    0x2,    0x39c,  0x39a,  0x3,    0x2,    0x2,    0x2,
      0x39d, 0x39e,  0x5,    0xc4,   0x63,   0x2,    0x39e,  0xc3,   0x3,
      0x2,   0x2,    0x2,    0x39f,  0x3b1,  0x5,    0xc6,   0x64,   0x2,
      0x3a0, 0x3a1,  0x7,    0x16,   0x2,    0x2,    0x3a1,  0x3b2,  0x5,
      0xc6,  0x64,   0x2,    0x3a2,  0x3a3,  0x7,    0x17,   0x2,    0x2,
      0x3a3, 0x3b2,  0x5,    0xc6,   0x64,   0x2,    0x3a4,  0x3a5,  0x7,
      0x18,  0x2,    0x2,    0x3a5,  0x3b2,  0x5,    0xc6,   0x64,   0x2,
      0x3a6, 0x3a7,  0x7,    0x19,   0x2,    0x2,    0x3a7,  0x3b2,  0x5,
      0xc6,  0x64,   0x2,    0x3a8,  0x3a9,  0x7,    0x1a,   0x2,    0x2,
      0x3a9, 0x3b2,  0x5,    0xc6,   0x64,   0x2,    0x3aa,  0x3ab,  0x7,
      0x1b,  0x2,    0x2,    0x3ab,  0x3b2,  0x5,    0xc6,   0x64,   0x2,
      0x3ac, 0x3ad,  0x7,    0x4f,   0x2,    0x2,    0x3ad,  0x3b2,  0x5,
      0x6a,  0x36,   0x2,    0x3ae,  0x3af,  0x7,    0x4e,   0x2,    0x2,
      0x3af, 0x3b0,  0x7,    0x4f,   0x2,    0x2,    0x3b0,  0x3b2,  0x5,
      0x6a,  0x36,   0x2,    0x3b1,  0x3a0,  0x3,    0x2,    0x2,    0x2,
      0x3b1, 0x3a2,  0x3,    0x2,    0x2,    0x2,    0x3b1,  0x3a4,  0x3,
      0x2,   0x2,    0x2,    0x3b1,  0x3a6,  0x3,    0x2,    0x2,    0x2,
      0x3b1, 0x3a8,  0x3,    0x2,    0x2,    0x2,    0x3b1,  0x3aa,  0x3,
      0x2,   0x2,    0x2,    0x3b1,  0x3ac,  0x3,    0x2,    0x2,    0x2,
      0x3b1, 0x3ae,  0x3,    0x2,    0x2,    0x2,    0x3b1,  0x3b2,  0x3,
      0x2,   0x2,    0x2,    0x3b2,  0xc5,   0x3,    0x2,    0x2,    0x2,
      0x3b3, 0x3b4,  0x5,    0xc8,   0x65,   0x2,    0x3b4,  0xc7,   0x3,
      0x2,   0x2,    0x2,    0x3b5,  0x3b9,  0x5,    0xd2,   0x6a,   0x2,
      0x3b6, 0x3b8,  0x5,    0xca,   0x66,   0x2,    0x3b7,  0x3b6,  0x3,
      0x2,   0x2,    0x2,    0x3b8,  0x3bb,  0x3,    0x2,    0x2,    0x2,
      0x3b9, 0x3b7,  0x3,    0x2,    0x2,    0x2,    0x3b9,  0x3ba,  0x3,
      0x2,   0x2,    0x2,    0x3ba,  0xc9,   0x3,    0x2,    0x2,    0x2,
      0x3bb, 0x3b9,  0x3,    0x2,    0x2,    0x2,    0x3bc,  0x3bd,  0x7,
      0xf,   0x2,    0x2,    0x3bd,  0x3c2,  0x5,    0xcc,   0x67,   0x2,
      0x3be, 0x3bf,  0x7,    0x1c,   0x2,    0x2,    0x3bf,  0x3c2,  0x5,
      0xce,  0x68,   0x2,    0x3c0,  0x3c2,  0x5,    0xd0,   0x69,   0x2,
      0x3c1, 0x3bc,  0x3,    0x2,    0x2,    0x2,    0x3c1,  0x3be,  0x3,
      0x2,   0x2,    0x2,    0x3c1,  0x3c0,  0x3,    0x2,    0x2,    0x2,
      0x3c2, 0xcb,   0x3,    0x2,    0x2,    0x2,    0x3c3,  0x3c4,  0x5,
      0xd2,  0x6a,   0x2,    0x3c4,  0xcd,   0x3,    0x2,    0x2,    0x2,
      0x3c5, 0x3c6,  0x5,    0xd2,   0x6a,   0x2,    0x3c6,  0xcf,   0x3,
      0x2,   0x2,    0x2,    0x3c7,  0x3ca,  0x5,    0xf6,   0x7c,   0x2,
      0x3c8, 0x3ca,  0x5,    0xf8,   0x7d,   0x2,    0x3c9,  0x3c7,  0x3,
      0x2,   0x2,    0x2,    0x3c9,  0x3c8,  0x3,    0x2,    0x2,    0x2,
      0x3ca, 0x3ce,  0x3,    0x2,    0x2,    0x2,    0x3cb,  0x3cd,  0x5,
      0xd4,  0x6b,   0x2,    0x3cc,  0x3cb,  0x3,    0x2,    0x2,    0x2,
      0x3cd, 0x3d0,  0x3,    0x2,    0x2,    0x2,    0x3ce,  0x3cc,  0x3,
      0x2,   0x2,    0x2,    0x3ce,  0x3cf,  0x3,    0x2,    0x2,    0x2,
      0x3cf, 0xd1,   0x3,    0x2,    0x2,    0x2,    0x3d0,  0x3ce,  0x3,
      0x2,   0x2,    0x2,    0x3d1,  0x3d5,  0x5,    0xda,   0x6e,   0x2,
      0x3d2, 0x3d4,  0x5,    0xd4,   0x6b,   0x2,    0x3d3,  0x3d2,  0x3,
      0x2,   0x2,    0x2,    0x3d4,  0x3d7,  0x3,    0x2,    0x2,    0x2,
      0x3d5, 0x3d3,  0x3,    0x2,    0x2,    0x2,    0x3d5,  0x3d6,  0x3,
      0x2,   0x2,    0x2,    0x3d6,  0xd3,   0x3,    0x2,    0x2,    0x2,
      0x3d7, 0x3d5,  0x3,    0x2,    0x2,    0x2,    0x3d8,  0x3db,  0x5,
      0xd6,  0x6c,   0x2,    0x3d9,  0x3db,  0x5,    0xd8,   0x6d,   0x2,
      0x3da, 0x3d8,  0x3,    0x2,    0x2,    0x2,    0x3da,  0x3d9,  0x3,
      0x2,   0x2,    0x2,    0x3db,  0xd5,   0x3,    0x2,    0x2,    0x2,
      0x3dc, 0x3dd,  0x7,    0x3,    0x2,    0x2,    0x3dd,  0x3de,  0x5,
      0xda,  0x6e,   0x2,    0x3de,  0xd7,   0x3,    0x2,    0x2,    0x2,
      0x3df, 0x3e0,  0x7,    0xd,    0x2,    0x2,    0x3e0,  0x3e1,  0x5,
      0xda,  0x6e,   0x2,    0x3e1,  0xd9,   0x3,    0x2,    0x2,    0x2,
      0x3e2, 0x3e3,  0x7,    0x11,   0x2,    0x2,    0x3e3,  0x3ea,  0x5,
      0xdc,  0x6f,   0x2,    0x3e4,  0x3e5,  0x7,    0xf,    0x2,    0x2,
      0x3e5, 0x3ea,  0x5,    0xdc,   0x6f,   0x2,    0x3e6,  0x3e7,  0x7,
      0x1c,  0x2,    0x2,    0x3e7,  0x3ea,  0x5,    0xdc,   0x6f,   0x2,
      0x3e8, 0x3ea,  0x5,    0xdc,   0x6f,   0x2,    0x3e9,  0x3e2,  0x3,
      0x2,   0x2,    0x2,    0x3e9,  0x3e4,  0x3,    0x2,    0x2,    0x2,
      0x3e9, 0x3e6,  0x3,    0x2,    0x2,    0x2,    0x3e9,  0x3e8,  0x3,
      0x2,   0x2,    0x2,    0x3ea,  0xdb,   0x3,    0x2,    0x2,    0x2,
      0x3eb, 0x3f3,  0x5,    0xde,   0x70,   0x2,    0x3ec,  0x3f3,  0x5,
      0xe0,  0x71,   0x2,    0x3ed,  0x3f3,  0x5,    0xee,   0x78,   0x2,
      0x3ee, 0x3f3,  0x5,    0xf0,   0x79,   0x2,    0x3ef,  0x3f3,  0x5,
      0xf2,  0x7a,   0x2,    0x3f0,  0x3f3,  0x5,    0xfa,   0x7e,   0x2,
      0x3f1, 0x3f3,  0x5,    0xb8,   0x5d,   0x2,    0x3f2,  0x3eb,  0x3,
      0x2,   0x2,    0x2,    0x3f2,  0x3ec,  0x3,    0x2,    0x2,    0x2,
      0x3f2, 0x3ed,  0x3,    0x2,    0x2,    0x2,    0x3f2,  0x3ee,  0x3,
      0x2,   0x2,    0x2,    0x3f2,  0x3ef,  0x3,    0x2,    0x2,    0x2,
      0x3f2, 0x3f0,  0x3,    0x2,    0x2,    0x2,    0x3f2,  0x3f1,  0x3,
      0x2,   0x2,    0x2,    0x3f3,  0xdd,   0x3,    0x2,    0x2,    0x2,
      0x3f4, 0x3f5,  0x7,    0x4,    0x2,    0x2,    0x3f5,  0x3f6,  0x5,
      0xbc,  0x5f,   0x2,    0x3f6,  0x3f7,  0x7,    0x5,    0x2,    0x2,
      0x3f7, 0xdf,   0x3,    0x2,    0x2,    0x2,    0x3f8,  0x4ff,  0x5,
      0xec,  0x77,   0x2,    0x3f9,  0x3fa,  0x7,    0x50,   0x2,    0x2,
      0x3fa, 0x3fb,  0x7,    0x4,    0x2,    0x2,    0x3fb,  0x3fc,  0x5,
      0xbc,  0x5f,   0x2,    0x3fc,  0x3fd,  0x7,    0x5,    0x2,    0x2,
      0x3fd, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x3fe,  0x3ff,  0x7,
      0x51,  0x2,    0x2,    0x3ff,  0x400,  0x7,    0x4,    0x2,    0x2,
      0x400, 0x401,  0x5,    0xbc,   0x5f,   0x2,    0x401,  0x402,  0x7,
      0x5,   0x2,    0x2,    0x402,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x403, 0x404,  0x7,    0x52,   0x2,    0x2,    0x404,  0x405,  0x7,
      0x4,   0x2,    0x2,    0x405,  0x406,  0x5,    0xbc,   0x5f,   0x2,
      0x406, 0x407,  0x7,    0x9,    0x2,    0x2,    0x407,  0x408,  0x5,
      0xbc,  0x5f,   0x2,    0x408,  0x409,  0x7,    0x5,    0x2,    0x2,
      0x409, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x40a,  0x40b,  0x7,
      0x53,  0x2,    0x2,    0x40b,  0x40c,  0x7,    0x4,    0x2,    0x2,
      0x40c, 0x40d,  0x5,    0xbc,   0x5f,   0x2,    0x40d,  0x40e,  0x7,
      0x5,   0x2,    0x2,    0x40e,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x40f, 0x410,  0x7,    0x54,   0x2,    0x2,    0x410,  0x411,  0x7,
      0x4,   0x2,    0x2,    0x411,  0x412,  0x5,    0xb8,   0x5d,   0x2,
      0x412, 0x413,  0x7,    0x5,    0x2,    0x2,    0x413,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x414,  0x415,  0x7,    0x55,   0x2,    0x2,
      0x415, 0x416,  0x7,    0x4,    0x2,    0x2,    0x416,  0x417,  0x5,
      0xbc,  0x5f,   0x2,    0x417,  0x418,  0x7,    0x5,    0x2,    0x2,
      0x418, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x419,  0x41a,  0x7,
      0x56,  0x2,    0x2,    0x41a,  0x41b,  0x7,    0x4,    0x2,    0x2,
      0x41b, 0x41c,  0x5,    0xbc,   0x5f,   0x2,    0x41c,  0x41d,  0x7,
      0x5,   0x2,    0x2,    0x41d,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x41e, 0x424,  0x7,    0x57,   0x2,    0x2,    0x41f,  0x420,  0x7,
      0x4,   0x2,    0x2,    0x420,  0x421,  0x5,    0xbc,   0x5f,   0x2,
      0x421, 0x422,  0x7,    0x5,    0x2,    0x2,    0x422,  0x425,  0x3,
      0x2,   0x2,    0x2,    0x423,  0x425,  0x7,    0xa4,   0x2,    0x2,
      0x424, 0x41f,  0x3,    0x2,    0x2,    0x2,    0x424,  0x423,  0x3,
      0x2,   0x2,    0x2,    0x425,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x426, 0x427,  0x7,    0x58,   0x2,    0x2,    0x427,  0x4ff,  0x7,
      0xa4,  0x2,    0x2,    0x428,  0x429,  0x7,    0x59,   0x2,    0x2,
      0x429, 0x42a,  0x7,    0x4,    0x2,    0x2,    0x42a,  0x42b,  0x5,
      0xbc,  0x5f,   0x2,    0x42b,  0x42c,  0x7,    0x5,    0x2,    0x2,
      0x42c, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x42d,  0x42e,  0x7,
      0x5a,  0x2,    0x2,    0x42e,  0x42f,  0x7,    0x4,    0x2,    0x2,
      0x42f, 0x430,  0x5,    0xbc,   0x5f,   0x2,    0x430,  0x431,  0x7,
      0x5,   0x2,    0x2,    0x431,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x432, 0x433,  0x7,    0x5b,   0x2,    0x2,    0x433,  0x434,  0x7,
      0x4,   0x2,    0x2,    0x434,  0x435,  0x5,    0xbc,   0x5f,   0x2,
      0x435, 0x436,  0x7,    0x5,    0x2,    0x2,    0x436,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x437,  0x438,  0x7,    0x5c,   0x2,    0x2,
      0x438, 0x439,  0x7,    0x4,    0x2,    0x2,    0x439,  0x43a,  0x5,
      0xbc,  0x5f,   0x2,    0x43a,  0x43b,  0x7,    0x5,    0x2,    0x2,
      0x43b, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x43c,  0x43d,  0x7,
      0x5d,  0x2,    0x2,    0x43d,  0x4ff,  0x5,    0x6a,   0x36,   0x2,
      0x43e, 0x4ff,  0x5,    0xe4,   0x73,   0x2,    0x43f,  0x440,  0x7,
      0x5e,  0x2,    0x2,    0x440,  0x441,  0x7,    0x4,    0x2,    0x2,
      0x441, 0x442,  0x5,    0xbc,   0x5f,   0x2,    0x442,  0x443,  0x7,
      0x5,   0x2,    0x2,    0x443,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x444, 0x4ff,  0x5,    0xe6,   0x74,   0x2,    0x445,  0x446,  0x7,
      0x5f,  0x2,    0x2,    0x446,  0x447,  0x7,    0x4,    0x2,    0x2,
      0x447, 0x448,  0x5,    0xbc,   0x5f,   0x2,    0x448,  0x449,  0x7,
      0x5,   0x2,    0x2,    0x449,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x44a, 0x44b,  0x7,    0x60,   0x2,    0x2,    0x44b,  0x44c,  0x7,
      0x4,   0x2,    0x2,    0x44c,  0x44d,  0x5,    0xbc,   0x5f,   0x2,
      0x44d, 0x44e,  0x7,    0x5,    0x2,    0x2,    0x44e,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x44f,  0x450,  0x7,    0x61,   0x2,    0x2,
      0x450, 0x451,  0x7,    0x1d,   0x2,    0x2,    0x451,  0x452,  0x7,
      0x62,  0x2,    0x2,    0x452,  0x453,  0x7,    0x1d,   0x2,    0x2,
      0x453, 0x454,  0x7,    0x56,   0x2,    0x2,    0x454,  0x455,  0x7,
      0x4,   0x2,    0x2,    0x455,  0x456,  0x5,    0xbc,   0x5f,   0x2,
      0x456, 0x457,  0x7,    0x5,    0x2,    0x2,    0x457,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x458,  0x459,  0x7,    0x63,   0x2,    0x2,
      0x459, 0x45a,  0x7,    0x4,    0x2,    0x2,    0x45a,  0x45b,  0x5,
      0xbc,  0x5f,   0x2,    0x45b,  0x45c,  0x7,    0x9,    0x2,    0x2,
      0x45c, 0x45d,  0x5,    0xbc,   0x5f,   0x2,    0x45d,  0x45e,  0x7,
      0x5,   0x2,    0x2,    0x45e,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x45f, 0x460,  0x7,    0x64,   0x2,    0x2,    0x460,  0x461,  0x7,
      0x4,   0x2,    0x2,    0x461,  0x462,  0x5,    0xbc,   0x5f,   0x2,
      0x462, 0x463,  0x7,    0x9,    0x2,    0x2,    0x463,  0x464,  0x5,
      0xbc,  0x5f,   0x2,    0x464,  0x465,  0x7,    0x5,    0x2,    0x2,
      0x465, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x466,  0x467,  0x7,
      0x65,  0x2,    0x2,    0x467,  0x468,  0x7,    0x4,    0x2,    0x2,
      0x468, 0x469,  0x5,    0xbc,   0x5f,   0x2,    0x469,  0x46a,  0x7,
      0x9,   0x2,    0x2,    0x46a,  0x46b,  0x5,    0xbc,   0x5f,   0x2,
      0x46b, 0x46c,  0x7,    0x5,    0x2,    0x2,    0x46c,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x46d,  0x46e,  0x7,    0x66,   0x2,    0x2,
      0x46e, 0x46f,  0x7,    0x4,    0x2,    0x2,    0x46f,  0x470,  0x5,
      0xbc,  0x5f,   0x2,    0x470,  0x471,  0x7,    0x9,    0x2,    0x2,
      0x471, 0x472,  0x5,    0xbc,   0x5f,   0x2,    0x472,  0x473,  0x7,
      0x5,   0x2,    0x2,    0x473,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x474, 0x475,  0x7,    0x67,   0x2,    0x2,    0x475,  0x476,  0x7,
      0x4,   0x2,    0x2,    0x476,  0x477,  0x5,    0xbc,   0x5f,   0x2,
      0x477, 0x478,  0x7,    0x9,    0x2,    0x2,    0x478,  0x479,  0x5,
      0xbc,  0x5f,   0x2,    0x479,  0x47a,  0x7,    0x5,    0x2,    0x2,
      0x47a, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x47b,  0x47c,  0x7,
      0x68,  0x2,    0x2,    0x47c,  0x47d,  0x7,    0x4,    0x2,    0x2,
      0x47d, 0x47e,  0x5,    0xbc,   0x5f,   0x2,    0x47e,  0x47f,  0x7,
      0x5,   0x2,    0x2,    0x47f,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x480, 0x481,  0x7,    0x69,   0x2,    0x2,    0x481,  0x482,  0x7,
      0x4,   0x2,    0x2,    0x482,  0x483,  0x5,    0xbc,   0x5f,   0x2,
      0x483, 0x484,  0x7,    0x5,    0x2,    0x2,    0x484,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x485,  0x486,  0x7,    0x6a,   0x2,    0x2,
      0x486, 0x487,  0x7,    0x4,    0x2,    0x2,    0x487,  0x488,  0x5,
      0xbc,  0x5f,   0x2,    0x488,  0x489,  0x7,    0x5,    0x2,    0x2,
      0x489, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x48a,  0x48b,  0x7,
      0x6b,  0x2,    0x2,    0x48b,  0x48c,  0x7,    0x4,    0x2,    0x2,
      0x48c, 0x48d,  0x5,    0xbc,   0x5f,   0x2,    0x48d,  0x48e,  0x7,
      0x5,   0x2,    0x2,    0x48e,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x48f, 0x490,  0x7,    0x6c,   0x2,    0x2,    0x490,  0x491,  0x7,
      0x4,   0x2,    0x2,    0x491,  0x492,  0x5,    0xbc,   0x5f,   0x2,
      0x492, 0x493,  0x7,    0x5,    0x2,    0x2,    0x493,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x494,  0x495,  0x7,    0x6d,   0x2,    0x2,
      0x495, 0x496,  0x7,    0x4,    0x2,    0x2,    0x496,  0x497,  0x5,
      0xbc,  0x5f,   0x2,    0x497,  0x498,  0x7,    0x5,    0x2,    0x2,
      0x498, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x499,  0x49a,  0x7,
      0x6e,  0x2,    0x2,    0x49a,  0x49b,  0x7,    0x4,    0x2,    0x2,
      0x49b, 0x49c,  0x5,    0xbc,   0x5f,   0x2,    0x49c,  0x49d,  0x7,
      0x5,   0x2,    0x2,    0x49d,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x49e, 0x49f,  0x7,    0x6f,   0x2,    0x2,    0x49f,  0x4a0,  0x7,
      0x4,   0x2,    0x2,    0x4a0,  0x4a1,  0x5,    0xbc,   0x5f,   0x2,
      0x4a1, 0x4a2,  0x7,    0x5,    0x2,    0x2,    0x4a2,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x4a3,  0x4a4,  0x7,    0x70,   0x2,    0x2,
      0x4a4, 0x4ff,  0x7,    0xa4,   0x2,    0x2,    0x4a5,  0x4a6,  0x7,
      0x71,  0x2,    0x2,    0x4a6,  0x4ff,  0x7,    0xa4,   0x2,    0x2,
      0x4a7, 0x4a8,  0x7,    0x72,   0x2,    0x2,    0x4a8,  0x4ff,  0x7,
      0xa4,  0x2,    0x2,    0x4a9,  0x4aa,  0x7,    0x77,   0x2,    0x2,
      0x4aa, 0x4ab,  0x7,    0x4,    0x2,    0x2,    0x4ab,  0x4ac,  0x5,
      0xbc,  0x5f,   0x2,    0x4ac,  0x4ad,  0x7,    0x5,    0x2,    0x2,
      0x4ad, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x4ae,  0x4af,  0x7,
      0x73,  0x2,    0x2,    0x4af,  0x4b0,  0x7,    0x4,    0x2,    0x2,
      0x4b0, 0x4b1,  0x5,    0xbc,   0x5f,   0x2,    0x4b1,  0x4b2,  0x7,
      0x5,   0x2,    0x2,    0x4b2,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x4b3, 0x4b4,  0x7,    0x74,   0x2,    0x2,    0x4b4,  0x4b5,  0x7,
      0x4,   0x2,    0x2,    0x4b5,  0x4b6,  0x5,    0xbc,   0x5f,   0x2,
      0x4b6, 0x4b7,  0x7,    0x5,    0x2,    0x2,    0x4b7,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x4b8,  0x4b9,  0x7,    0x75,   0x2,    0x2,
      0x4b9, 0x4ba,  0x7,    0x4,    0x2,    0x2,    0x4ba,  0x4bb,  0x5,
      0xbc,  0x5f,   0x2,    0x4bb,  0x4bc,  0x7,    0x5,    0x2,    0x2,
      0x4bc, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x4bd,  0x4be,  0x7,
      0x76,  0x2,    0x2,    0x4be,  0x4bf,  0x7,    0x4,    0x2,    0x2,
      0x4bf, 0x4c0,  0x5,    0xbc,   0x5f,   0x2,    0x4c0,  0x4c1,  0x7,
      0x5,   0x2,    0x2,    0x4c1,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x4c2, 0x4c3,  0x7,    0x78,   0x2,    0x2,    0x4c3,  0x4ff,  0x5,
      0x6a,  0x36,   0x2,    0x4c4,  0x4c5,  0x7,    0x79,   0x2,    0x2,
      0x4c5, 0x4c6,  0x7,    0x4,    0x2,    0x2,    0x4c6,  0x4c7,  0x5,
      0xbc,  0x5f,   0x2,    0x4c7,  0x4c8,  0x7,    0x9,    0x2,    0x2,
      0x4c8, 0x4c9,  0x5,    0xbc,   0x5f,   0x2,    0x4c9,  0x4ca,  0x7,
      0x9,   0x2,    0x2,    0x4ca,  0x4cb,  0x5,    0xbc,   0x5f,   0x2,
      0x4cb, 0x4cc,  0x7,    0x5,    0x2,    0x2,    0x4cc,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x4cd,  0x4ce,  0x7,    0x7a,   0x2,    0x2,
      0x4ce, 0x4cf,  0x7,    0x4,    0x2,    0x2,    0x4cf,  0x4d0,  0x5,
      0xbc,  0x5f,   0x2,    0x4d0,  0x4d1,  0x7,    0x9,    0x2,    0x2,
      0x4d1, 0x4d2,  0x5,    0xbc,   0x5f,   0x2,    0x4d2,  0x4d3,  0x7,
      0x5,   0x2,    0x2,    0x4d3,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x4d4, 0x4d5,  0x7,    0x7b,   0x2,    0x2,    0x4d5,  0x4d6,  0x7,
      0x4,   0x2,    0x2,    0x4d6,  0x4d7,  0x5,    0xbc,   0x5f,   0x2,
      0x4d7, 0x4d8,  0x7,    0x9,    0x2,    0x2,    0x4d8,  0x4d9,  0x5,
      0xbc,  0x5f,   0x2,    0x4d9,  0x4da,  0x7,    0x5,    0x2,    0x2,
      0x4da, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x4db,  0x4dc,  0x7,
      0x7c,  0x2,    0x2,    0x4dc,  0x4dd,  0x7,    0x4,    0x2,    0x2,
      0x4dd, 0x4de,  0x5,    0xbc,   0x5f,   0x2,    0x4de,  0x4df,  0x7,
      0x9,   0x2,    0x2,    0x4df,  0x4e0,  0x5,    0xbc,   0x5f,   0x2,
      0x4e0, 0x4e1,  0x7,    0x5,    0x2,    0x2,    0x4e1,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x4e2,  0x4e3,  0x7,    0x7d,   0x2,    0x2,
      0x4e3, 0x4e4,  0x7,    0x4,    0x2,    0x2,    0x4e4,  0x4e5,  0x5,
      0xbc,  0x5f,   0x2,    0x4e5,  0x4e6,  0x7,    0x5,    0x2,    0x2,
      0x4e6, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x4e7,  0x4e8,  0x7,
      0x7e,  0x2,    0x2,    0x4e8,  0x4e9,  0x7,    0x4,    0x2,    0x2,
      0x4e9, 0x4ea,  0x5,    0xbc,   0x5f,   0x2,    0x4ea,  0x4eb,  0x7,
      0x5,   0x2,    0x2,    0x4eb,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x4ec, 0x4ed,  0x7,    0x7f,   0x2,    0x2,    0x4ed,  0x4ee,  0x7,
      0x4,   0x2,    0x2,    0x4ee,  0x4ef,  0x5,    0xbc,   0x5f,   0x2,
      0x4ef, 0x4f0,  0x7,    0x5,    0x2,    0x2,    0x4f0,  0x4ff,  0x3,
      0x2,   0x2,    0x2,    0x4f1,  0x4f2,  0x7,    0x80,   0x2,    0x2,
      0x4f2, 0x4f3,  0x7,    0x4,    0x2,    0x2,    0x4f3,  0x4f4,  0x5,
      0xbc,  0x5f,   0x2,    0x4f4,  0x4f5,  0x7,    0x5,    0x2,    0x2,
      0x4f5, 0x4ff,  0x3,    0x2,    0x2,    0x2,    0x4f6,  0x4f7,  0x7,
      0x81,  0x2,    0x2,    0x4f7,  0x4f8,  0x7,    0x4,    0x2,    0x2,
      0x4f8, 0x4f9,  0x5,    0xbc,   0x5f,   0x2,    0x4f9,  0x4fa,  0x7,
      0x5,   0x2,    0x2,    0x4fa,  0x4ff,  0x3,    0x2,    0x2,    0x2,
      0x4fb, 0x4ff,  0x5,    0xe2,   0x72,   0x2,    0x4fc,  0x4ff,  0x5,
      0xe8,  0x75,   0x2,    0x4fd,  0x4ff,  0x5,    0xea,   0x76,   0x2,
      0x4fe, 0x3f8,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x3f9,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x3fe,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x403,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x40a,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x40f,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x414,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x419,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x41e,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x426,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x428,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x42d,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x432,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x437,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x43c,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x43e,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x43f,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x444,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x445,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x44a,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x44f,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x458,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x45f,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x466,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x46d,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x474,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x47b,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x480,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x485,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x48a,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x48f,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x494,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x499,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x49e,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4a3,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4a5,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4a7,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4a9,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4ae,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4b3,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4b8,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4bd,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4c2,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4c4,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4cd,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4d4,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4db,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4e2,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4e7,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4ec,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4f1,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4f6,  0x3,    0x2,    0x2,    0x2,    0x4fe,  0x4fb,  0x3,
      0x2,   0x2,    0x2,    0x4fe,  0x4fc,  0x3,    0x2,    0x2,    0x2,
      0x4fe, 0x4fd,  0x3,    0x2,    0x2,    0x2,    0x4ff,  0xe1,   0x3,
      0x2,   0x2,    0x2,    0x500,  0x501,  0x7,    0x82,   0x2,    0x2,
      0x501, 0x502,  0x7,    0x4,    0x2,    0x2,    0x502,  0x503,  0x5,
      0xbc,  0x5f,   0x2,    0x503,  0x504,  0x7,    0x9,    0x2,    0x2,
      0x504, 0x507,  0x5,    0xbc,   0x5f,   0x2,    0x505,  0x506,  0x7,
      0x9,   0x2,    0x2,    0x506,  0x508,  0x5,    0xbc,   0x5f,   0x2,
      0x507, 0x505,  0x3,    0x2,    0x2,    0x2,    0x507,  0x508,  0x3,
      0x2,   0x2,    0x2,    0x508,  0x509,  0x3,    0x2,    0x2,    0x2,
      0x509, 0x50a,  0x7,    0x5,    0x2,    0x2,    0x50a,  0xe3,   0x3,
      0x2,   0x2,    0x2,    0x50b,  0x50c,  0x7,    0x83,   0x2,    0x2,
      0x50c, 0x50d,  0x7,    0x4,    0x2,    0x2,    0x50d,  0x50e,  0x5,
      0xbc,  0x5f,   0x2,    0x50e,  0x50f,  0x7,    0x9,    0x2,    0x2,
      0x50f, 0x512,  0x5,    0xbc,   0x5f,   0x2,    0x510,  0x511,  0x7,
      0x9,   0x2,    0x2,    0x511,  0x513,  0x5,    0xbc,   0x5f,   0x2,
      0x512, 0x510,  0x3,    0x2,    0x2,    0x2,    0x512,  0x513,  0x3,
      0x2,   0x2,    0x2,    0x513,  0x514,  0x3,    0x2,    0x2,    0x2,
      0x514, 0x515,  0x7,    0x5,    0x2,    0x2,    0x515,  0xe5,   0x3,
      0x2,   0x2,    0x2,    0x516,  0x517,  0x7,    0x84,   0x2,    0x2,
      0x517, 0x518,  0x7,    0x4,    0x2,    0x2,    0x518,  0x519,  0x5,
      0xbc,  0x5f,   0x2,    0x519,  0x51a,  0x7,    0x9,    0x2,    0x2,
      0x51a, 0x51b,  0x5,    0xbc,   0x5f,   0x2,    0x51b,  0x51c,  0x7,
      0x9,   0x2,    0x2,    0x51c,  0x51f,  0x5,    0xbc,   0x5f,   0x2,
      0x51d, 0x51e,  0x7,    0x9,    0x2,    0x2,    0x51e,  0x520,  0x5,
      0xbc,  0x5f,   0x2,    0x51f,  0x51d,  0x3,    0x2,    0x2,    0x2,
      0x51f, 0x520,  0x3,    0x2,    0x2,    0x2,    0x520,  0x521,  0x3,
      0x2,   0x2,    0x2,    0x521,  0x522,  0x7,    0x5,    0x2,    0x2,
      0x522, 0xe7,   0x3,    0x2,    0x2,    0x2,    0x523,  0x524,  0x7,
      0x85,  0x2,    0x2,    0x524,  0x525,  0x5,    0x40,   0x21,   0x2,
      0x525, 0xe9,   0x3,    0x2,    0x2,    0x2,    0x526,  0x527,  0x7,
      0x4e,  0x2,    0x2,    0x527,  0x528,  0x7,    0x85,   0x2,    0x2,
      0x528, 0x529,  0x5,    0x40,   0x21,   0x2,    0x529,  0xeb,   0x3,
      0x2,   0x2,    0x2,    0x52a,  0x52b,  0x7,    0x86,   0x2,    0x2,
      0x52b, 0x52d,  0x7,    0x4,    0x2,    0x2,    0x52c,  0x52e,  0x7,
      0x24,  0x2,    0x2,    0x52d,  0x52c,  0x3,    0x2,    0x2,    0x2,
      0x52d, 0x52e,  0x3,    0x2,    0x2,    0x2,    0x52e,  0x531,  0x3,
      0x2,   0x2,    0x2,    0x52f,  0x532,  0x7,    0x3,    0x2,    0x2,
      0x530, 0x532,  0x5,    0xbc,   0x5f,   0x2,    0x531,  0x52f,  0x3,
      0x2,   0x2,    0x2,    0x531,  0x530,  0x3,    0x2,    0x2,    0x2,
      0x532, 0x533,  0x3,    0x2,    0x2,    0x2,    0x533,  0x56b,  0x7,
      0x5,   0x2,    0x2,    0x534,  0x535,  0x7,    0x87,   0x2,    0x2,
      0x535, 0x537,  0x7,    0x4,    0x2,    0x2,    0x536,  0x538,  0x7,
      0x24,  0x2,    0x2,    0x537,  0x536,  0x3,    0x2,    0x2,    0x2,
      0x537, 0x538,  0x3,    0x2,    0x2,    0x2,    0x538,  0x539,  0x3,
      0x2,   0x2,    0x2,    0x539,  0x53a,  0x5,    0xbc,   0x5f,   0x2,
      0x53a, 0x53b,  0x7,    0x5,    0x2,    0x2,    0x53b,  0x56b,  0x3,
      0x2,   0x2,    0x2,    0x53c,  0x53d,  0x7,    0x88,   0x2,    0x2,
      0x53d, 0x53f,  0x7,    0x4,    0x2,    0x2,    0x53e,  0x540,  0x7,
      0x24,  0x2,    0x2,    0x53f,  0x53e,  0x3,    0x2,    0x2,    0x2,
      0x53f, 0x540,  0x3,    0x2,    0x2,    0x2,    0x540,  0x541,  0x3,
      0x2,   0x2,    0x2,    0x541,  0x542,  0x5,    0xbc,   0x5f,   0x2,
      0x542, 0x543,  0x7,    0x5,    0x2,    0x2,    0x543,  0x56b,  0x3,
      0x2,   0x2,    0x2,    0x544,  0x545,  0x7,    0x89,   0x2,    0x2,
      0x545, 0x547,  0x7,    0x4,    0x2,    0x2,    0x546,  0x548,  0x7,
      0x24,  0x2,    0x2,    0x547,  0x546,  0x3,    0x2,    0x2,    0x2,
      0x547, 0x548,  0x3,    0x2,    0x2,    0x2,    0x548,  0x549,  0x3,
      0x2,   0x2,    0x2,    0x549,  0x54a,  0x5,    0xbc,   0x5f,   0x2,
      0x54a, 0x54b,  0x7,    0x5,    0x2,    0x2,    0x54b,  0x56b,  0x3,
      0x2,   0x2,    0x2,    0x54c,  0x54d,  0x7,    0x8a,   0x2,    0x2,
      0x54d, 0x54f,  0x7,    0x4,    0x2,    0x2,    0x54e,  0x550,  0x7,
      0x24,  0x2,    0x2,    0x54f,  0x54e,  0x3,    0x2,    0x2,    0x2,
      0x54f, 0x550,  0x3,    0x2,    0x2,    0x2,    0x550,  0x551,  0x3,
      0x2,   0x2,    0x2,    0x551,  0x552,  0x5,    0xbc,   0x5f,   0x2,
      0x552, 0x553,  0x7,    0x5,    0x2,    0x2,    0x553,  0x56b,  0x3,
      0x2,   0x2,    0x2,    0x554,  0x555,  0x7,    0x8b,   0x2,    0x2,
      0x555, 0x557,  0x7,    0x4,    0x2,    0x2,    0x556,  0x558,  0x7,
      0x24,  0x2,    0x2,    0x557,  0x556,  0x3,    0x2,    0x2,    0x2,
      0x557, 0x558,  0x3,    0x2,    0x2,    0x2,    0x558,  0x559,  0x3,
      0x2,   0x2,    0x2,    0x559,  0x55a,  0x5,    0xbc,   0x5f,   0x2,
      0x55a, 0x55b,  0x7,    0x5,    0x2,    0x2,    0x55b,  0x56b,  0x3,
      0x2,   0x2,    0x2,    0x55c,  0x55d,  0x7,    0x2e,   0x2,    0x2,
      0x55d, 0x55f,  0x7,    0x4,    0x2,    0x2,    0x55e,  0x560,  0x7,
      0x24,  0x2,    0x2,    0x55f,  0x55e,  0x3,    0x2,    0x2,    0x2,
      0x55f, 0x560,  0x3,    0x2,    0x2,    0x2,    0x560,  0x561,  0x3,
      0x2,   0x2,    0x2,    0x561,  0x566,  0x5,    0xbc,   0x5f,   0x2,
      0x562, 0x563,  0x7,    0xa,    0x2,    0x2,    0x563,  0x564,  0x7,
      0x8c,  0x2,    0x2,    0x564,  0x565,  0x7,    0x16,   0x2,    0x2,
      0x565, 0x567,  0x5,    0xfc,   0x7f,   0x2,    0x566,  0x562,  0x3,
      0x2,   0x2,    0x2,    0x566,  0x567,  0x3,    0x2,    0x2,    0x2,
      0x567, 0x568,  0x3,    0x2,    0x2,    0x2,    0x568,  0x569,  0x7,
      0x5,   0x2,    0x2,    0x569,  0x56b,  0x3,    0x2,    0x2,    0x2,
      0x56a, 0x52a,  0x3,    0x2,    0x2,    0x2,    0x56a,  0x534,  0x3,
      0x2,   0x2,    0x2,    0x56a,  0x53c,  0x3,    0x2,    0x2,    0x2,
      0x56a, 0x544,  0x3,    0x2,    0x2,    0x2,    0x56a,  0x54c,  0x3,
      0x2,   0x2,    0x2,    0x56a,  0x554,  0x3,    0x2,    0x2,    0x2,
      0x56a, 0x55c,  0x3,    0x2,    0x2,    0x2,    0x56b,  0xed,   0x3,
      0x2,   0x2,    0x2,    0x56c,  0x56e,  0x5,    0xfe,   0x80,   0x2,
      0x56d, 0x56f,  0x5,    0x68,   0x35,   0x2,    0x56e,  0x56d,  0x3,
      0x2,   0x2,    0x2,    0x56e,  0x56f,  0x3,    0x2,    0x2,    0x2,
      0x56f, 0xef,   0x3,    0x2,    0x2,    0x2,    0x570,  0x574,  0x5,
      0xfc,  0x7f,   0x2,    0x571,  0x575,  0x7,    0x93,   0x2,    0x2,
      0x572, 0x573,  0x7,    0x1e,   0x2,    0x2,    0x573,  0x575,  0x5,
      0xfe,  0x80,   0x2,    0x574,  0x571,  0x3,    0x2,    0x2,    0x2,
      0x574, 0x572,  0x3,    0x2,    0x2,    0x2,    0x574,  0x575,  0x3,
      0x2,   0x2,    0x2,    0x575,  0xf1,   0x3,    0x2,    0x2,    0x2,
      0x576, 0x57a,  0x5,    0xf4,   0x7b,   0x2,    0x577,  0x57a,  0x5,
      0xf6,  0x7c,   0x2,    0x578,  0x57a,  0x5,    0xf8,   0x7d,   0x2,
      0x579, 0x576,  0x3,    0x2,    0x2,    0x2,    0x579,  0x577,  0x3,
      0x2,   0x2,    0x2,    0x579,  0x578,  0x3,    0x2,    0x2,    0x2,
      0x57a, 0xf3,   0x3,    0x2,    0x2,    0x2,    0x57b,  0x57c,  0x9,
      0x6,   0x2,    0x2,    0x57c,  0xf5,   0x3,    0x2,    0x2,    0x2,
      0x57d, 0x57e,  0x9,    0x7,    0x2,    0x2,    0x57e,  0xf7,   0x3,
      0x2,   0x2,    0x2,    0x57f,  0x580,  0x9,    0x8,    0x2,    0x2,
      0x580, 0xf9,   0x3,    0x2,    0x2,    0x2,    0x581,  0x582,  0x9,
      0x9,   0x2,    0x2,    0x582,  0xfb,   0x3,    0x2,    0x2,    0x2,
      0x583, 0x584,  0x9,    0xa,    0x2,    0x2,    0x584,  0xfd,   0x3,
      0x2,   0x2,    0x2,    0x585,  0x587,  0x7,    0x94,   0x2,    0x2,
      0x586, 0x585,  0x3,    0x2,    0x2,    0x2,    0x586,  0x587,  0x3,
      0x2,   0x2,    0x2,    0x587,  0x58a,  0x3,    0x2,    0x2,    0x2,
      0x588, 0x58b,  0x5,    0x104,  0x83,   0x2,    0x589,  0x58b,  0x5,
      0x100, 0x81,   0x2,    0x58a,  0x588,  0x3,    0x2,    0x2,    0x2,
      0x58a, 0x589,  0x3,    0x2,    0x2,    0x2,    0x58b,  0xff,   0x3,
      0x2,   0x2,    0x2,    0x58c,  0x58f,  0x5,    0x106,  0x84,   0x2,
      0x58d, 0x58f,  0x5,    0x108,  0x85,   0x2,    0x58e,  0x58c,  0x3,
      0x2,   0x2,    0x2,    0x58e,  0x58d,  0x3,    0x2,    0x2,    0x2,
      0x58f, 0x101,  0x3,    0x2,    0x2,    0x2,    0x590,  0x591,  0x9,
      0xb,   0x2,    0x2,    0x591,  0x103,  0x3,    0x2,    0x2,    0x2,
      0x592, 0x593,  0x7,    0x8d,   0x2,    0x2,    0x593,  0x105,  0x3,
      0x2,   0x2,    0x2,    0x594,  0x595,  0x7,    0x8f,   0x2,    0x2,
      0x595, 0x107,  0x3,    0x2,    0x2,    0x2,    0x596,  0x597,  0x7,
      0x8e,  0x2,    0x2,    0x597,  0x109,  0x3,    0x2,    0x2,    0x2,
      0x8a,  0x10f,  0x116,  0x118,  0x126,  0x133,  0x138,  0x13b,  0x13f,
      0x14e, 0x157,  0x15d,  0x161,  0x167,  0x16a,  0x16f,  0x173,  0x17b,
      0x184, 0x18e,  0x193,  0x196,  0x199,  0x19c,  0x1a2,  0x1aa,  0x1af,
      0x1b5, 0x1bd,  0x1c3,  0x1c5,  0x1c9,  0x1cc,  0x1d0,  0x1d3,  0x1d7,
      0x1da, 0x1de,  0x1e1,  0x1e5,  0x1e8,  0x1ec,  0x1ef,  0x1f1,  0x1fe,
      0x203, 0x205,  0x20a,  0x20f,  0x214,  0x219,  0x21c,  0x221,  0x223,
      0x22d, 0x238,  0x249,  0x250,  0x25a,  0x25e,  0x264,  0x26d,  0x272,
      0x279, 0x283,  0x28c,  0x294,  0x29b,  0x2a0,  0x2a9,  0x2ae,  0x2b2,
      0x2b9, 0x2bb,  0x2c3,  0x2c6,  0x2ce,  0x2d2,  0x2d7,  0x2de,  0x2e9,
      0x2ec, 0x2f1,  0x2f5,  0x304,  0x30b,  0x317,  0x31f,  0x324,  0x329,
      0x335, 0x33e,  0x341,  0x344,  0x34b,  0x34d,  0x353,  0x35b,  0x365,
      0x36d, 0x373,  0x377,  0x37b,  0x37f,  0x389,  0x392,  0x39a,  0x3b1,
      0x3b9, 0x3c1,  0x3c9,  0x3ce,  0x3d5,  0x3da,  0x3e9,  0x3f2,  0x424,
      0x4fe, 0x507,  0x512,  0x51f,  0x52d,  0x531,  0x537,  0x53f,  0x547,
      0x54f, 0x557,  0x55f,  0x566,  0x56a,  0x56e,  0x574,  0x579,  0x586,
      0x58a, 0x58e,
  };

  _serializedATN.insert(
      _serializedATN.end(), serializedATNSegment0,
      serializedATNSegment0 +
          sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

SparqlAutomaticParser::Initializer SparqlAutomaticParser::_init;
