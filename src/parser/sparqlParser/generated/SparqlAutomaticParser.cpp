
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
    setState(246);
    prologue();
    setState(251);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::SELECT: {
        setState(247);
        selectQuery();
        break;
      }

      case SparqlAutomaticParser::CONSTRUCT: {
        setState(248);
        constructQuery();
        break;
      }

      case SparqlAutomaticParser::DESCRIBE: {
        setState(249);
        describeQuery();
        break;
      }

      case SparqlAutomaticParser::ASK: {
        setState(250);
        askQuery();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(253);
    valuesClause();
    setState(254);
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
    setState(260);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::BASE

           || _la == SparqlAutomaticParser::PREFIX) {
      setState(258);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlAutomaticParser::BASE: {
          setState(256);
          baseDecl();
          break;
        }

        case SparqlAutomaticParser::PREFIX: {
          setState(257);
          prefixDecl();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(262);
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
    setState(263);
    match(SparqlAutomaticParser::BASE);
    setState(264);
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
    setState(266);
    match(SparqlAutomaticParser::PREFIX);
    setState(267);
    match(SparqlAutomaticParser::PNAME_NS);
    setState(268);
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
    setState(270);
    selectClause();
    setState(274);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::FROM) {
      setState(271);
      datasetClause();
      setState(276);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(277);
    whereClause();
    setState(278);
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
    setState(280);
    selectClause();
    setState(281);
    whereClause();
    setState(282);
    solutionModifier();
    setState(283);
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
    setState(285);
    match(SparqlAutomaticParser::SELECT);
    setState(287);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::DISTINCT

        || _la == SparqlAutomaticParser::REDUCED) {
      setState(286);
      _la = _input->LA(1);
      if (!(_la == SparqlAutomaticParser::DISTINCT

            || _la == SparqlAutomaticParser::REDUCED)) {
        _errHandler->recoverInline(this);
      } else {
        _errHandler->reportMatch(this);
        consume();
      }
    }
    setState(295);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        setState(290);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(289);
          varOrAlias();
          setState(292);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (_la == SparqlAutomaticParser::T__1 ||
                 _la == SparqlAutomaticParser::VAR1

                 || _la == SparqlAutomaticParser::VAR2);
        break;
      }

      case SparqlAutomaticParser::T__0: {
        setState(294);
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
    setState(299);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(297);
        var();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(298);
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
    setState(301);
    match(SparqlAutomaticParser::T__1);
    setState(302);
    aliasWithoutBrackets();
    setState(303);
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
    setState(305);
    expression();
    setState(306);
    match(SparqlAutomaticParser::AS);
    setState(307);
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
    setState(309);
    match(SparqlAutomaticParser::CONSTRUCT);
    setState(333);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__3: {
        setState(310);
        constructTemplate();
        setState(314);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::FROM) {
          setState(311);
          datasetClause();
          setState(316);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(317);
        whereClause();
        setState(318);
        solutionModifier();
        break;
      }

      case SparqlAutomaticParser::WHERE:
      case SparqlAutomaticParser::FROM: {
        setState(323);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::FROM) {
          setState(320);
          datasetClause();
          setState(325);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(326);
        match(SparqlAutomaticParser::WHERE);
        setState(327);
        match(SparqlAutomaticParser::T__3);
        setState(329);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if ((((_la & ~0x3fULL) == 0) &&
             ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                               (1ULL << SparqlAutomaticParser::T__15) |
                               (1ULL << SparqlAutomaticParser::T__27) |
                               (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
               (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
               (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
               (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
          setState(328);
          triplesTemplate();
        }
        setState(331);
        match(SparqlAutomaticParser::T__4);
        setState(332);
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
    setState(335);
    match(SparqlAutomaticParser::DESCRIBE);
    setState(342);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::LANGTAG: {
        setState(337);
        _errHandler->sync(this);
        _la = _input->LA(1);
        do {
          setState(336);
          varOrIri();
          setState(339);
          _errHandler->sync(this);
          _la = _input->LA(1);
        } while (((((_la - 139) & ~0x3fULL) == 0) &&
                  ((1ULL << (_la - 139)) &
                   ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
                    (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
                    (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
                    (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
                    (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
                    (1ULL << (SparqlAutomaticParser::LANGTAG - 139)))) != 0));
        break;
      }

      case SparqlAutomaticParser::T__0: {
        setState(341);
        match(SparqlAutomaticParser::T__0);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(347);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::FROM) {
      setState(344);
      datasetClause();
      setState(349);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(351);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__3

        || _la == SparqlAutomaticParser::WHERE) {
      setState(350);
      whereClause();
    }
    setState(353);
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
    setState(355);
    match(SparqlAutomaticParser::ASK);
    setState(359);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::FROM) {
      setState(356);
      datasetClause();
      setState(361);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(362);
    whereClause();
    setState(363);
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
    setState(365);
    match(SparqlAutomaticParser::FROM);
    setState(368);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        setState(366);
        defaultGraphClause();
        break;
      }

      case SparqlAutomaticParser::NAMED: {
        setState(367);
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
    setState(370);
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
    setState(372);
    match(SparqlAutomaticParser::NAMED);
    setState(373);
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
    setState(375);
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
    setState(378);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::WHERE) {
      setState(377);
      match(SparqlAutomaticParser::WHERE);
    }
    setState(380);
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
    setState(383);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::GROUPBY) {
      setState(382);
      groupClause();
    }
    setState(386);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::HAVING) {
      setState(385);
      havingClause();
    }
    setState(389);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::ORDERBY) {
      setState(388);
      orderClause();
    }
    setState(392);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::LIMIT) |
                           (1ULL << SparqlAutomaticParser::OFFSET) |
                           (1ULL << SparqlAutomaticParser::TEXTLIMIT))) != 0)) {
      setState(391);
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
    setState(394);
    match(SparqlAutomaticParser::GROUPBY);
    setState(396);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(395);
      groupCondition();
      setState(398);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (_la == SparqlAutomaticParser::T__1

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
                (1ULL << (SparqlAutomaticParser::LANGTAG - 140)))) != 0));

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
    setState(411);
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
        setState(400);
        builtInCall();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 2);
        setState(401);
        functionCall();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 3);
        setState(402);
        match(SparqlAutomaticParser::T__1);
        setState(403);
        expression();
        setState(406);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::AS) {
          setState(404);
          match(SparqlAutomaticParser::AS);
          setState(405);
          var();
        }
        setState(408);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 4);
        setState(410);
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
    setState(413);
    match(SparqlAutomaticParser::HAVING);
    setState(415);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(414);
      havingCondition();
      setState(417);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (_la == SparqlAutomaticParser::T__1

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
                (1ULL << (SparqlAutomaticParser::LANGTAG - 140)))) != 0));

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
    setState(419);
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
    setState(421);
    match(SparqlAutomaticParser::ORDERBY);
    setState(423);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(422);
      orderCondition();
      setState(425);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while ((((_la & ~0x3fULL) == 0) &&
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
                (1ULL << (SparqlAutomaticParser::LANGTAG - 140)))) != 0));

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
    setState(433);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::ASC:
      case SparqlAutomaticParser::DESC: {
        enterOuterAlt(_localctx, 1);
        setState(427);
        _la = _input->LA(1);
        if (!(_la == SparqlAutomaticParser::ASC

              || _la == SparqlAutomaticParser::DESC)) {
          _errHandler->recoverInline(this);
        } else {
          _errHandler->reportMatch(this);
          consume();
        }
        setState(428);
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
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 2);
        setState(431);
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
          case SparqlAutomaticParser::LANGTAG: {
            setState(429);
            constraint();
            break;
          }

          case SparqlAutomaticParser::VAR1:
          case SparqlAutomaticParser::VAR2: {
            setState(430);
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
    setState(477);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 42, _ctx)) {
      case 1: {
        enterOuterAlt(_localctx, 1);
        setState(435);
        limitClause();
        setState(437);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(436);
          offsetClause();
        }
        setState(440);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(439);
          textLimitClause();
        }
        break;
      }

      case 2: {
        enterOuterAlt(_localctx, 2);
        setState(442);
        limitClause();
        setState(444);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(443);
          textLimitClause();
        }
        setState(447);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(446);
          offsetClause();
        }
        break;
      }

      case 3: {
        enterOuterAlt(_localctx, 3);
        setState(449);
        offsetClause();
        setState(451);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(450);
          limitClause();
        }
        setState(454);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(453);
          textLimitClause();
        }
        break;
      }

      case 4: {
        enterOuterAlt(_localctx, 4);
        setState(456);
        offsetClause();
        setState(458);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::TEXTLIMIT) {
          setState(457);
          textLimitClause();
        }
        setState(461);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(460);
          limitClause();
        }
        break;
      }

      case 5: {
        enterOuterAlt(_localctx, 5);
        setState(463);
        textLimitClause();
        setState(465);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(464);
          offsetClause();
        }
        setState(468);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(467);
          limitClause();
        }
        break;
      }

      case 6: {
        enterOuterAlt(_localctx, 6);
        setState(470);
        textLimitClause();
        setState(472);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::LIMIT) {
          setState(471);
          limitClause();
        }
        setState(475);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::OFFSET) {
          setState(474);
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
    setState(479);
    match(SparqlAutomaticParser::LIMIT);
    setState(480);
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
    setState(482);
    match(SparqlAutomaticParser::OFFSET);
    setState(483);
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
    setState(485);
    match(SparqlAutomaticParser::TEXTLIMIT);
    setState(486);
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
    setState(490);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::VALUES) {
      setState(488);
      match(SparqlAutomaticParser::VALUES);
      setState(489);
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
    setState(492);
    triplesSameSubject();
    setState(497);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(493);
      match(SparqlAutomaticParser::T__5);
      setState(495);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__27) |
                             (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
        setState(494);
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
    setState(499);
    match(SparqlAutomaticParser::T__3);
    setState(502);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::SELECT: {
        setState(500);
        subSelect();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__3:
      case SparqlAutomaticParser::T__4:
      case SparqlAutomaticParser::T__15:
      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
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
      case SparqlAutomaticParser::LANGTAG:
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
        setState(501);
        groupGraphPatternSub();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(504);
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

std::vector<SparqlAutomaticParser::TriplesBlockContext*>
SparqlAutomaticParser::GroupGraphPatternSubContext::triplesBlock() {
  return getRuleContexts<SparqlAutomaticParser::TriplesBlockContext>();
}

SparqlAutomaticParser::TriplesBlockContext*
SparqlAutomaticParser::GroupGraphPatternSubContext::triplesBlock(size_t i) {
  return getRuleContext<SparqlAutomaticParser::TriplesBlockContext>(i);
}

std::vector<SparqlAutomaticParser::GraphPatternNotTriplesContext*>
SparqlAutomaticParser::GroupGraphPatternSubContext::graphPatternNotTriples() {
  return getRuleContexts<
      SparqlAutomaticParser::GraphPatternNotTriplesContext>();
}

SparqlAutomaticParser::GraphPatternNotTriplesContext*
SparqlAutomaticParser::GroupGraphPatternSubContext::graphPatternNotTriples(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::GraphPatternNotTriplesContext>(
      i);
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
    setState(507);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__27) |
                           (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
      setState(506);
      triplesBlock();
    }
    setState(518);
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
      setState(509);
      graphPatternNotTriples();
      setState(511);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == SparqlAutomaticParser::T__5) {
        setState(510);
        match(SparqlAutomaticParser::T__5);
      }
      setState(514);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__27) |
                             (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
        setState(513);
        triplesBlock();
      }
      setState(520);
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
  enterRule(_localctx, 66, SparqlAutomaticParser::RuleTriplesBlock);
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
    setState(521);
    triplesSameSubjectPath();
    setState(526);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(522);
      match(SparqlAutomaticParser::T__5);
      setState(524);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__27) |
                             (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
        setState(523);
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
  enterRule(_localctx, 68, SparqlAutomaticParser::RuleGraphPatternNotTriples);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(536);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__3: {
        enterOuterAlt(_localctx, 1);
        setState(528);
        groupOrUnionGraphPattern();
        break;
      }

      case SparqlAutomaticParser::OPTIONAL: {
        enterOuterAlt(_localctx, 2);
        setState(529);
        optionalGraphPattern();
        break;
      }

      case SparqlAutomaticParser::MINUS: {
        enterOuterAlt(_localctx, 3);
        setState(530);
        minusGraphPattern();
        break;
      }

      case SparqlAutomaticParser::GRAPH: {
        enterOuterAlt(_localctx, 4);
        setState(531);
        graphGraphPattern();
        break;
      }

      case SparqlAutomaticParser::SERVICE: {
        enterOuterAlt(_localctx, 5);
        setState(532);
        serviceGraphPattern();
        break;
      }

      case SparqlAutomaticParser::FILTER: {
        enterOuterAlt(_localctx, 6);
        setState(533);
        filterR();
        break;
      }

      case SparqlAutomaticParser::BIND: {
        enterOuterAlt(_localctx, 7);
        setState(534);
        bind();
        break;
      }

      case SparqlAutomaticParser::VALUES: {
        enterOuterAlt(_localctx, 8);
        setState(535);
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
  enterRule(_localctx, 70, SparqlAutomaticParser::RuleOptionalGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(538);
    match(SparqlAutomaticParser::OPTIONAL);
    setState(539);
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
  enterRule(_localctx, 72, SparqlAutomaticParser::RuleGraphGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(541);
    match(SparqlAutomaticParser::GRAPH);
    setState(542);
    varOrIri();
    setState(543);
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
  enterRule(_localctx, 74, SparqlAutomaticParser::RuleServiceGraphPattern);
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
    setState(545);
    match(SparqlAutomaticParser::SERVICE);
    setState(547);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::SILENT) {
      setState(546);
      match(SparqlAutomaticParser::SILENT);
    }
    setState(549);
    varOrIri();
    setState(550);
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
  enterRule(_localctx, 76, SparqlAutomaticParser::RuleBind);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(552);
    match(SparqlAutomaticParser::BIND);
    setState(553);
    match(SparqlAutomaticParser::T__1);
    setState(554);
    expression();
    setState(555);
    match(SparqlAutomaticParser::AS);
    setState(556);
    var();
    setState(557);
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
  enterRule(_localctx, 78, SparqlAutomaticParser::RuleInlineData);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(559);
    match(SparqlAutomaticParser::VALUES);
    setState(560);
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
  enterRule(_localctx, 80, SparqlAutomaticParser::RuleDataBlock);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(564);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(562);
        inlineDataOneVar();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 2);
        setState(563);
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
  enterRule(_localctx, 82, SparqlAutomaticParser::RuleInlineDataOneVar);
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
    setState(566);
    var();
    setState(567);
    match(SparqlAutomaticParser::T__3);
    setState(571);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 28) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 28)) &
             ((1ULL << (SparqlAutomaticParser::T__27 - 28)) |
              (1ULL << (SparqlAutomaticParser::T__28 - 28)) |
              (1ULL << (SparqlAutomaticParser::UNDEF - 28)))) != 0) ||
           ((((_la - 139) & ~0x3fULL) == 0) &&
            ((1ULL << (_la - 139)) &
             ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
              (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
              (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
              (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
      setState(568);
      dataBlockValue();
      setState(573);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(574);
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
  enterRule(_localctx, 84, SparqlAutomaticParser::RuleInlineDataFull);
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
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::NIL: {
        setState(576);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::T__1: {
        setState(577);
        match(SparqlAutomaticParser::T__1);
        setState(581);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::VAR1

               || _la == SparqlAutomaticParser::VAR2) {
          setState(578);
          var();
          setState(583);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(584);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(587);
    match(SparqlAutomaticParser::T__3);
    setState(591);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__1 ||
           _la == SparqlAutomaticParser::NIL) {
      setState(588);
      dataBlockSingle();
      setState(593);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(594);
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
  enterRule(_localctx, 86, SparqlAutomaticParser::RuleDataBlockSingle);
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
    setState(605);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        setState(596);
        match(SparqlAutomaticParser::T__1);
        setState(600);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (
            ((((_la - 28) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 28)) &
              ((1ULL << (SparqlAutomaticParser::T__27 - 28)) |
               (1ULL << (SparqlAutomaticParser::T__28 - 28)) |
               (1ULL << (SparqlAutomaticParser::UNDEF - 28)))) != 0) ||
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
          setState(597);
          dataBlockValue();
          setState(602);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(603);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::NIL: {
        setState(604);
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
  enterRule(_localctx, 88, SparqlAutomaticParser::RuleDataBlockValue);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(612);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(607);
        iri();
        break;
      }

      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 2);
        setState(608);
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
        setState(609);
        numericLiteral();
        break;
      }

      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28: {
        enterOuterAlt(_localctx, 4);
        setState(610);
        booleanLiteral();
        break;
      }

      case SparqlAutomaticParser::UNDEF: {
        enterOuterAlt(_localctx, 5);
        setState(611);
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
  enterRule(_localctx, 90, SparqlAutomaticParser::RuleMinusGraphPattern);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(614);
    match(SparqlAutomaticParser::MINUS);
    setState(615);
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
  enterRule(_localctx, 92, SparqlAutomaticParser::RuleGroupOrUnionGraphPattern);
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
    setState(617);
    groupGraphPattern();
    setState(622);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::UNION) {
      setState(618);
      match(SparqlAutomaticParser::UNION);
      setState(619);
      groupGraphPattern();
      setState(624);
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
  enterRule(_localctx, 94, SparqlAutomaticParser::RuleFilterR);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(625);
    match(SparqlAutomaticParser::FILTER);
    setState(626);
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
  enterRule(_localctx, 96, SparqlAutomaticParser::RuleConstraint);

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
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(628);
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
        setState(629);
        builtInCall();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 3);
        setState(630);
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
  enterRule(_localctx, 98, SparqlAutomaticParser::RuleFunctionCall);

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
    iri();
    setState(634);
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
  enterRule(_localctx, 100, SparqlAutomaticParser::RuleArgList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(651);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(636);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(637);
        match(SparqlAutomaticParser::T__1);
        setState(639);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(638);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(641);
        expression();
        setState(646);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::T__6) {
          setState(642);
          match(SparqlAutomaticParser::T__6);
          setState(643);
          expression();
          setState(648);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(649);
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
  enterRule(_localctx, 102, SparqlAutomaticParser::RuleExpressionList);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(665);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 1);
        setState(653);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(654);
        match(SparqlAutomaticParser::T__1);
        setState(655);
        expression();
        setState(660);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == SparqlAutomaticParser::T__6) {
          setState(656);
          match(SparqlAutomaticParser::T__6);
          setState(657);
          expression();
          setState(662);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(663);
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
  enterRule(_localctx, 104, SparqlAutomaticParser::RuleConstructTemplate);
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
    setState(667);
    match(SparqlAutomaticParser::T__3);
    setState(669);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__27) |
                           (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
      setState(668);
      constructTriples();
    }
    setState(671);
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
  enterRule(_localctx, 106, SparqlAutomaticParser::RuleConstructTriples);
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
    setState(673);
    triplesSameSubject();
    setState(678);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__5) {
      setState(674);
      match(SparqlAutomaticParser::T__5);
      setState(676);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                             (1ULL << SparqlAutomaticParser::T__15) |
                             (1ULL << SparqlAutomaticParser::T__27) |
                             (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
          ((((_la - 139) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 139)) &
            ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
             (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
             (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
             (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
             (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
        setState(675);
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
  enterRule(_localctx, 108, SparqlAutomaticParser::RuleTriplesSameSubject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(686);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::LANGTAG:
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
        setState(680);
        varOrTerm();
        setState(681);
        propertyListNotEmpty();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(683);
        triplesNode();
        setState(684);
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
  enterRule(_localctx, 110, SparqlAutomaticParser::RulePropertyList);
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
    setState(689);
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
           (1ULL << (SparqlAutomaticParser::LANGTAG - 139)))) != 0)) {
      setState(688);
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
  enterRule(_localctx, 112, SparqlAutomaticParser::RulePropertyListNotEmpty);
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
    setState(691);
    verb();
    setState(692);
    objectList();
    setState(701);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__7) {
      setState(693);
      match(SparqlAutomaticParser::T__7);
      setState(697);
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
             (1ULL << (SparqlAutomaticParser::LANGTAG - 139)))) != 0)) {
        setState(694);
        verb();
        setState(695);
        objectList();
      }
      setState(703);
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
  enterRule(_localctx, 114, SparqlAutomaticParser::RuleVerb);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(706);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(704);
        varOrIri();
        break;
      }

      case SparqlAutomaticParser::T__8: {
        enterOuterAlt(_localctx, 2);
        setState(705);
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
  enterRule(_localctx, 116, SparqlAutomaticParser::RuleObjectList);
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
    objectR();
    setState(713);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__6) {
      setState(709);
      match(SparqlAutomaticParser::T__6);
      setState(710);
      objectR();
      setState(715);
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
  enterRule(_localctx, 118, SparqlAutomaticParser::RuleObjectR);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(716);
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
  enterRule(_localctx, 120, SparqlAutomaticParser::RuleTriplesSameSubjectPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(724);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::LANGTAG:
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
        setState(718);
        varOrTerm();
        setState(719);
        propertyListPathNotEmpty();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(721);
        triplesNodePath();
        setState(722);
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
  enterRule(_localctx, 122, SparqlAutomaticParser::RulePropertyListPath);
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
           (1ULL << (SparqlAutomaticParser::LANGTAG - 139)))) != 0)) {
      setState(726);
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

std::vector<SparqlAutomaticParser::VerbPathOrSimpleContext*>
SparqlAutomaticParser::PropertyListPathNotEmptyContext::verbPathOrSimple() {
  return getRuleContexts<SparqlAutomaticParser::VerbPathOrSimpleContext>();
}

SparqlAutomaticParser::VerbPathOrSimpleContext*
SparqlAutomaticParser::PropertyListPathNotEmptyContext::verbPathOrSimple(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::VerbPathOrSimpleContext>(i);
}

SparqlAutomaticParser::ObjectListPathContext*
SparqlAutomaticParser::PropertyListPathNotEmptyContext::objectListPath() {
  return getRuleContext<SparqlAutomaticParser::ObjectListPathContext>(0);
}

std::vector<SparqlAutomaticParser::ObjectListContext*>
SparqlAutomaticParser::PropertyListPathNotEmptyContext::objectList() {
  return getRuleContexts<SparqlAutomaticParser::ObjectListContext>();
}

SparqlAutomaticParser::ObjectListContext*
SparqlAutomaticParser::PropertyListPathNotEmptyContext::objectList(size_t i) {
  return getRuleContext<SparqlAutomaticParser::ObjectListContext>(i);
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
  enterRule(_localctx, 124,
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
    setState(729);
    verbPathOrSimple();
    setState(730);
    objectListPath();
    setState(739);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__7) {
      setState(731);
      match(SparqlAutomaticParser::T__7);
      setState(735);
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
             (1ULL << (SparqlAutomaticParser::LANGTAG - 139)))) != 0)) {
        setState(732);
        verbPathOrSimple();
        setState(733);
        objectList();
      }
      setState(741);
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
  enterRule(_localctx, 126, SparqlAutomaticParser::RuleVerbPath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(742);
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
  enterRule(_localctx, 128, SparqlAutomaticParser::RuleVerbSimple);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(744);
    var();

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
  enterRule(_localctx, 130, SparqlAutomaticParser::RuleVerbPathOrSimple);

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
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__11:
      case SparqlAutomaticParser::T__14:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        setState(746);
        verbPath();
        break;
      }

      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        setState(747);
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
  enterRule(_localctx, 132, SparqlAutomaticParser::RuleObjectListPath);
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
    setState(750);
    objectPath();
    setState(755);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__6) {
      setState(751);
      match(SparqlAutomaticParser::T__6);
      setState(752);
      objectPath();
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
  enterRule(_localctx, 134, SparqlAutomaticParser::RuleObjectPath);

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
  enterRule(_localctx, 136, SparqlAutomaticParser::RulePath);

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
  enterRule(_localctx, 138, SparqlAutomaticParser::RulePathAlternative);
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
    setState(762);
    pathSequence();
    setState(767);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__9) {
      setState(763);
      match(SparqlAutomaticParser::T__9);
      setState(764);
      pathSequence();
      setState(769);
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
  enterRule(_localctx, 140, SparqlAutomaticParser::RulePathSequence);
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
    setState(770);
    pathEltOrInverse();
    setState(775);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__10) {
      setState(771);
      match(SparqlAutomaticParser::T__10);
      setState(772);
      pathEltOrInverse();
      setState(777);
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
  enterRule(_localctx, 142, SparqlAutomaticParser::RulePathElt);
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
    setState(778);
    pathPrimary();
    setState(780);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__0) |
                           (1ULL << SparqlAutomaticParser::T__12) |
                           (1ULL << SparqlAutomaticParser::T__13))) != 0)) {
      setState(779);
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
  enterRule(_localctx, 144, SparqlAutomaticParser::RulePathEltOrInverse);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(785);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__14:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(782);
        pathElt();
        break;
      }

      case SparqlAutomaticParser::T__11: {
        enterOuterAlt(_localctx, 2);
        setState(783);
        dynamic_cast<PathEltOrInverseContext*>(_localctx)->negationOperator =
            match(SparqlAutomaticParser::T__11);
        setState(784);
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
  enterRule(_localctx, 146, SparqlAutomaticParser::RulePathMod);
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
    setState(787);
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
  enterRule(_localctx, 148, SparqlAutomaticParser::RulePathPrimary);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(797);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(789);
        iri();
        break;
      }

      case SparqlAutomaticParser::T__8: {
        enterOuterAlt(_localctx, 2);
        setState(790);
        match(SparqlAutomaticParser::T__8);
        break;
      }

      case SparqlAutomaticParser::T__14: {
        enterOuterAlt(_localctx, 3);
        setState(791);
        match(SparqlAutomaticParser::T__14);
        setState(792);
        pathNegatedPropertySet();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 4);
        setState(793);
        match(SparqlAutomaticParser::T__1);
        setState(794);
        path();
        setState(795);
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
  enterRule(_localctx, 150, SparqlAutomaticParser::RulePathNegatedPropertySet);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(812);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__8:
      case SparqlAutomaticParser::T__11:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(799);
        pathOneInPropertySet();
        break;
      }

      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 2);
        setState(800);
        match(SparqlAutomaticParser::T__1);
        setState(809);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::T__8

            || _la == SparqlAutomaticParser::T__11 ||
            ((((_la - 139) & ~0x3fULL) == 0) &&
             ((1ULL << (_la - 139)) &
              ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
               (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
               (1ULL << (SparqlAutomaticParser::LANGTAG - 139)))) != 0)) {
          setState(801);
          pathOneInPropertySet();
          setState(806);
          _errHandler->sync(this);
          _la = _input->LA(1);
          while (_la == SparqlAutomaticParser::T__9) {
            setState(802);
            match(SparqlAutomaticParser::T__9);
            setState(803);
            pathOneInPropertySet();
            setState(808);
            _errHandler->sync(this);
            _la = _input->LA(1);
          }
        }
        setState(811);
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
  enterRule(_localctx, 152, SparqlAutomaticParser::RulePathOneInPropertySet);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(821);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(814);
        iri();
        break;
      }

      case SparqlAutomaticParser::T__8: {
        enterOuterAlt(_localctx, 2);
        setState(815);
        match(SparqlAutomaticParser::T__8);
        break;
      }

      case SparqlAutomaticParser::T__11: {
        enterOuterAlt(_localctx, 3);
        setState(816);
        match(SparqlAutomaticParser::T__11);
        setState(819);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::IRI_REF:
          case SparqlAutomaticParser::PNAME_NS:
          case SparqlAutomaticParser::PNAME_LN:
          case SparqlAutomaticParser::LANGTAG: {
            setState(817);
            iri();
            break;
          }

          case SparqlAutomaticParser::T__8: {
            setState(818);
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
  enterRule(_localctx, 154, SparqlAutomaticParser::RuleInteger);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(823);
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
  enterRule(_localctx, 156, SparqlAutomaticParser::RuleTriplesNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(827);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(825);
        collection();
        break;
      }

      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(826);
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
  enterRule(_localctx, 158, SparqlAutomaticParser::RuleBlankNodePropertyList);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(829);
    match(SparqlAutomaticParser::T__15);
    setState(830);
    propertyListNotEmpty();
    setState(831);
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
  enterRule(_localctx, 160, SparqlAutomaticParser::RuleTriplesNodePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(835);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(833);
        collectionPath();
        break;
      }

      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(834);
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
  enterRule(_localctx, 162,
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
    setState(837);
    match(SparqlAutomaticParser::T__15);
    setState(838);
    propertyListPathNotEmpty();
    setState(839);
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
  enterRule(_localctx, 164, SparqlAutomaticParser::RuleCollection);
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
    match(SparqlAutomaticParser::T__1);
    setState(843);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(842);
      graphNode();
      setState(845);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        (((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__27) |
                           (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
    setState(847);
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
  enterRule(_localctx, 166, SparqlAutomaticParser::RuleCollectionPath);
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
    setState(849);
    match(SparqlAutomaticParser::T__1);
    setState(851);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(850);
      graphNodePath();
      setState(853);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while (
        (((_la & ~0x3fULL) == 0) &&
         ((1ULL << _la) & ((1ULL << SparqlAutomaticParser::T__1) |
                           (1ULL << SparqlAutomaticParser::T__15) |
                           (1ULL << SparqlAutomaticParser::T__27) |
                           (1ULL << SparqlAutomaticParser::T__28))) != 0) ||
        ((((_la - 139) & ~0x3fULL) == 0) &&
         ((1ULL << (_la - 139)) &
          ((1ULL << (SparqlAutomaticParser::IRI_REF - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_NS - 139)) |
           (1ULL << (SparqlAutomaticParser::PNAME_LN - 139)) |
           (1ULL << (SparqlAutomaticParser::BLANK_NODE_LABEL - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR1 - 139)) |
           (1ULL << (SparqlAutomaticParser::VAR2 - 139)) |
           (1ULL << (SparqlAutomaticParser::LANGTAG - 139)) |
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
    setState(855);
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
  enterRule(_localctx, 168, SparqlAutomaticParser::RuleGraphNode);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(859);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::LANGTAG:
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
        setState(857);
        varOrTerm();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(858);
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
  enterRule(_localctx, 170, SparqlAutomaticParser::RuleGraphNodePath);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(863);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2:
      case SparqlAutomaticParser::LANGTAG:
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
        setState(861);
        varOrTerm();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__15: {
        enterOuterAlt(_localctx, 2);
        setState(862);
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
  enterRule(_localctx, 172, SparqlAutomaticParser::RuleVarOrTerm);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(867);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(865);
        var();
        break;
      }

      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::LANGTAG:
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
        setState(866);
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
  enterRule(_localctx, 174, SparqlAutomaticParser::RuleVarOrIri);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(871);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 1);
        setState(869);
        var();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 2);
        setState(870);
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
  enterRule(_localctx, 176, SparqlAutomaticParser::RuleVar);
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
  enterRule(_localctx, 178, SparqlAutomaticParser::RuleGraphTerm);

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
      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 1);
        setState(875);
        iri();
        break;
      }

      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 2);
        setState(876);
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
        setState(877);
        numericLiteral();
        break;
      }

      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28: {
        enterOuterAlt(_localctx, 4);
        setState(878);
        booleanLiteral();
        break;
      }

      case SparqlAutomaticParser::BLANK_NODE_LABEL:
      case SparqlAutomaticParser::ANON: {
        enterOuterAlt(_localctx, 5);
        setState(879);
        blankNode();
        break;
      }

      case SparqlAutomaticParser::NIL: {
        enterOuterAlt(_localctx, 6);
        setState(880);
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
  enterRule(_localctx, 180, SparqlAutomaticParser::RuleExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(883);
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
  enterRule(_localctx, 182, SparqlAutomaticParser::RuleConditionalOrExpression);
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
    setState(885);
    conditionalAndExpression();
    setState(890);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__17) {
      setState(886);
      match(SparqlAutomaticParser::T__17);
      setState(887);
      conditionalAndExpression();
      setState(892);
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
  enterRule(_localctx, 184,
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
    setState(893);
    valueLogical();
    setState(898);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__18) {
      setState(894);
      match(SparqlAutomaticParser::T__18);
      setState(895);
      valueLogical();
      setState(900);
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
  enterRule(_localctx, 186, SparqlAutomaticParser::RuleValueLogical);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(901);
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
  enterRule(_localctx, 188, SparqlAutomaticParser::RuleRelationalExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(903);
    numericExpression();
    setState(920);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 106, _ctx)) {
      case 1: {
        setState(904);
        match(SparqlAutomaticParser::T__19);
        setState(905);
        numericExpression();
        break;
      }

      case 2: {
        setState(906);
        match(SparqlAutomaticParser::T__20);
        setState(907);
        numericExpression();
        break;
      }

      case 3: {
        setState(908);
        numericExpression();
        break;
      }

      case 4: {
        setState(909);
        match(SparqlAutomaticParser::T__21);
        setState(910);
        numericExpression();
        break;
      }

      case 5: {
        setState(911);
        match(SparqlAutomaticParser::T__22);
        setState(912);
        numericExpression();
        break;
      }

      case 6: {
        setState(913);
        match(SparqlAutomaticParser::T__23);
        setState(914);
        numericExpression();
        break;
      }

      case 7: {
        setState(915);
        match(SparqlAutomaticParser::IN);
        setState(916);
        expressionList();
        break;
      }

      case 8: {
        setState(917);
        match(SparqlAutomaticParser::NOT);
        setState(918);
        match(SparqlAutomaticParser::IN);
        setState(919);
        expressionList();
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
  enterRule(_localctx, 190, SparqlAutomaticParser::RuleNumericExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(922);
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

std::vector<SparqlAutomaticParser::MultiplicativeExpressionContext*>
SparqlAutomaticParser::AdditiveExpressionContext::multiplicativeExpression() {
  return getRuleContexts<
      SparqlAutomaticParser::MultiplicativeExpressionContext>();
}

SparqlAutomaticParser::MultiplicativeExpressionContext*
SparqlAutomaticParser::AdditiveExpressionContext::multiplicativeExpression(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::MultiplicativeExpressionContext>(
      i);
}

std::vector<
    SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext*>
SparqlAutomaticParser::AdditiveExpressionContext::
    strangeMultiplicativeSubexprOfAdditive() {
  return getRuleContexts<
      SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext>();
}

SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext*
SparqlAutomaticParser::AdditiveExpressionContext::
    strangeMultiplicativeSubexprOfAdditive(size_t i) {
  return getRuleContext<
      SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext>(i);
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
  enterRule(_localctx, 192, SparqlAutomaticParser::RuleAdditiveExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(924);
    multiplicativeExpression();
    setState(932);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                     108, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(930);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::T__12: {
            setState(925);
            match(SparqlAutomaticParser::T__12);
            setState(926);
            multiplicativeExpression();
            break;
          }

          case SparqlAutomaticParser::T__24: {
            setState(927);
            match(SparqlAutomaticParser::T__24);
            setState(928);
            multiplicativeExpression();
            break;
          }

          case SparqlAutomaticParser::INTEGER_POSITIVE:
          case SparqlAutomaticParser::DECIMAL_POSITIVE:
          case SparqlAutomaticParser::DOUBLE_POSITIVE:
          case SparqlAutomaticParser::INTEGER_NEGATIVE:
          case SparqlAutomaticParser::DECIMAL_NEGATIVE:
          case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
            setState(929);
            strangeMultiplicativeSubexprOfAdditive();
            break;
          }

          default:
            throw NoViableAltException(this);
        }
      }
      setState(934);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
          _input, 108, _ctx);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- StrangeMultiplicativeSubexprOfAdditiveContext
//------------------------------------------------------------------

SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext::
    StrangeMultiplicativeSubexprOfAdditiveContext(ParserRuleContext* parent,
                                                  size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

SparqlAutomaticParser::NumericLiteralPositiveContext* SparqlAutomaticParser::
    StrangeMultiplicativeSubexprOfAdditiveContext::numericLiteralPositive() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralPositiveContext>(
      0);
}

SparqlAutomaticParser::NumericLiteralNegativeContext* SparqlAutomaticParser::
    StrangeMultiplicativeSubexprOfAdditiveContext::numericLiteralNegative() {
  return getRuleContext<SparqlAutomaticParser::NumericLiteralNegativeContext>(
      0);
}

std::vector<SparqlAutomaticParser::UnaryExpressionContext*>
SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext::
    unaryExpression() {
  return getRuleContexts<SparqlAutomaticParser::UnaryExpressionContext>();
}

SparqlAutomaticParser::UnaryExpressionContext* SparqlAutomaticParser::
    StrangeMultiplicativeSubexprOfAdditiveContext::unaryExpression(size_t i) {
  return getRuleContext<SparqlAutomaticParser::UnaryExpressionContext>(i);
}

size_t SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext::
    getRuleIndex() const {
  return SparqlAutomaticParser::RuleStrangeMultiplicativeSubexprOfAdditive;
}

void SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext::
    enterRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->enterStrangeMultiplicativeSubexprOfAdditive(this);
}

void SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext::
    exitRule(tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<SparqlAutomaticListener*>(listener);
  if (parserListener != nullptr)
    parserListener->exitStrangeMultiplicativeSubexprOfAdditive(this);
}

antlrcpp::Any
SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<SparqlAutomaticVisitor*>(visitor))
    return parserVisitor->visitStrangeMultiplicativeSubexprOfAdditive(this);
  else
    return visitor->visitChildren(this);
}

SparqlAutomaticParser::StrangeMultiplicativeSubexprOfAdditiveContext*
SparqlAutomaticParser::strangeMultiplicativeSubexprOfAdditive() {
  StrangeMultiplicativeSubexprOfAdditiveContext* _localctx =
      _tracker.createInstance<StrangeMultiplicativeSubexprOfAdditiveContext>(
          _ctx, getState());
  enterRule(_localctx, 194,
            SparqlAutomaticParser::RuleStrangeMultiplicativeSubexprOfAdditive);
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
    setState(937);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE: {
        setState(935);
        numericLiteralPositive();
        break;
      }

      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        setState(936);
        numericLiteralNegative();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(945);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__0

           || _la == SparqlAutomaticParser::T__10) {
      setState(943);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlAutomaticParser::T__0: {
          setState(939);
          match(SparqlAutomaticParser::T__0);
          setState(940);
          unaryExpression();
          break;
        }

        case SparqlAutomaticParser::T__10: {
          setState(941);
          match(SparqlAutomaticParser::T__10);
          setState(942);
          unaryExpression();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(947);
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

std::vector<SparqlAutomaticParser::UnaryExpressionContext*>
SparqlAutomaticParser::MultiplicativeExpressionContext::unaryExpression() {
  return getRuleContexts<SparqlAutomaticParser::UnaryExpressionContext>();
}

SparqlAutomaticParser::UnaryExpressionContext*
SparqlAutomaticParser::MultiplicativeExpressionContext::unaryExpression(
    size_t i) {
  return getRuleContext<SparqlAutomaticParser::UnaryExpressionContext>(i);
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
  enterRule(_localctx, 196,
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
    setState(948);
    unaryExpression();
    setState(955);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == SparqlAutomaticParser::T__0

           || _la == SparqlAutomaticParser::T__10) {
      setState(953);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case SparqlAutomaticParser::T__0: {
          setState(949);
          match(SparqlAutomaticParser::T__0);
          setState(950);
          unaryExpression();
          break;
        }

        case SparqlAutomaticParser::T__10: {
          setState(951);
          match(SparqlAutomaticParser::T__10);
          setState(952);
          unaryExpression();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(957);
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
  enterRule(_localctx, 198, SparqlAutomaticParser::RuleUnaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(965);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__14: {
        enterOuterAlt(_localctx, 1);
        setState(958);
        match(SparqlAutomaticParser::T__14);
        setState(959);
        primaryExpression();
        break;
      }

      case SparqlAutomaticParser::T__12: {
        enterOuterAlt(_localctx, 2);
        setState(960);
        match(SparqlAutomaticParser::T__12);
        setState(961);
        primaryExpression();
        break;
      }

      case SparqlAutomaticParser::T__24: {
        enterOuterAlt(_localctx, 3);
        setState(962);
        match(SparqlAutomaticParser::T__24);
        setState(963);
        primaryExpression();
        break;
      }

      case SparqlAutomaticParser::T__1:
      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28:
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
      case SparqlAutomaticParser::LANGTAG:
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
        setState(964);
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
  enterRule(_localctx, 200, SparqlAutomaticParser::RulePrimaryExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(974);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(967);
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
        setState(968);
        builtInCall();
        break;
      }

      case SparqlAutomaticParser::IRI_REF:
      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN:
      case SparqlAutomaticParser::LANGTAG: {
        enterOuterAlt(_localctx, 3);
        setState(969);
        iriOrFunction();
        break;
      }

      case SparqlAutomaticParser::STRING_LITERAL1:
      case SparqlAutomaticParser::STRING_LITERAL2:
      case SparqlAutomaticParser::STRING_LITERAL_LONG1:
      case SparqlAutomaticParser::STRING_LITERAL_LONG2: {
        enterOuterAlt(_localctx, 4);
        setState(970);
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
        setState(971);
        numericLiteral();
        break;
      }

      case SparqlAutomaticParser::T__27:
      case SparqlAutomaticParser::T__28: {
        enterOuterAlt(_localctx, 6);
        setState(972);
        booleanLiteral();
        break;
      }

      case SparqlAutomaticParser::VAR1:
      case SparqlAutomaticParser::VAR2: {
        enterOuterAlt(_localctx, 7);
        setState(973);
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
  enterRule(_localctx, 202, SparqlAutomaticParser::RuleBrackettedExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(976);
    match(SparqlAutomaticParser::T__1);
    setState(977);
    expression();
    setState(978);
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
  enterRule(_localctx, 204, SparqlAutomaticParser::RuleBuiltInCall);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1242);
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
        setState(980);
        aggregate();
        break;
      }

      case SparqlAutomaticParser::STR: {
        enterOuterAlt(_localctx, 2);
        setState(981);
        match(SparqlAutomaticParser::STR);
        setState(982);
        match(SparqlAutomaticParser::T__1);
        setState(983);
        expression();
        setState(984);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::LANG: {
        enterOuterAlt(_localctx, 3);
        setState(986);
        match(SparqlAutomaticParser::LANG);
        setState(987);
        match(SparqlAutomaticParser::T__1);
        setState(988);
        expression();
        setState(989);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::LANGMATCHES: {
        enterOuterAlt(_localctx, 4);
        setState(991);
        match(SparqlAutomaticParser::LANGMATCHES);
        setState(992);
        match(SparqlAutomaticParser::T__1);
        setState(993);
        expression();
        setState(994);
        match(SparqlAutomaticParser::T__6);
        setState(995);
        expression();
        setState(996);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::DATATYPE: {
        enterOuterAlt(_localctx, 5);
        setState(998);
        match(SparqlAutomaticParser::DATATYPE);
        setState(999);
        match(SparqlAutomaticParser::T__1);
        setState(1000);
        expression();
        setState(1001);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::BOUND: {
        enterOuterAlt(_localctx, 6);
        setState(1003);
        match(SparqlAutomaticParser::BOUND);
        setState(1004);
        match(SparqlAutomaticParser::T__1);
        setState(1005);
        var();
        setState(1006);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::IRI: {
        enterOuterAlt(_localctx, 7);
        setState(1008);
        match(SparqlAutomaticParser::IRI);
        setState(1009);
        match(SparqlAutomaticParser::T__1);
        setState(1010);
        expression();
        setState(1011);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::URI: {
        enterOuterAlt(_localctx, 8);
        setState(1013);
        match(SparqlAutomaticParser::URI);
        setState(1014);
        match(SparqlAutomaticParser::T__1);
        setState(1015);
        expression();
        setState(1016);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::BNODE: {
        enterOuterAlt(_localctx, 9);
        setState(1018);
        match(SparqlAutomaticParser::BNODE);
        setState(1024);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::T__1: {
            setState(1019);
            match(SparqlAutomaticParser::T__1);
            setState(1020);
            expression();
            setState(1021);
            match(SparqlAutomaticParser::T__2);
            break;
          }

          case SparqlAutomaticParser::NIL: {
            setState(1023);
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
        setState(1026);
        match(SparqlAutomaticParser::RAND);
        setState(1027);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::ABS: {
        enterOuterAlt(_localctx, 11);
        setState(1028);
        match(SparqlAutomaticParser::ABS);
        setState(1029);
        match(SparqlAutomaticParser::T__1);
        setState(1030);
        expression();
        setState(1031);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::CEIL: {
        enterOuterAlt(_localctx, 12);
        setState(1033);
        match(SparqlAutomaticParser::CEIL);
        setState(1034);
        match(SparqlAutomaticParser::T__1);
        setState(1035);
        expression();
        setState(1036);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::FLOOR: {
        enterOuterAlt(_localctx, 13);
        setState(1038);
        match(SparqlAutomaticParser::FLOOR);
        setState(1039);
        match(SparqlAutomaticParser::T__1);
        setState(1040);
        expression();
        setState(1041);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ROUND: {
        enterOuterAlt(_localctx, 14);
        setState(1043);
        match(SparqlAutomaticParser::ROUND);
        setState(1044);
        match(SparqlAutomaticParser::T__1);
        setState(1045);
        expression();
        setState(1046);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::CONCAT: {
        enterOuterAlt(_localctx, 15);
        setState(1048);
        match(SparqlAutomaticParser::CONCAT);
        setState(1049);
        expressionList();
        break;
      }

      case SparqlAutomaticParser::SUBSTR: {
        enterOuterAlt(_localctx, 16);
        setState(1050);
        substringExpression();
        break;
      }

      case SparqlAutomaticParser::STRLEN: {
        enterOuterAlt(_localctx, 17);
        setState(1051);
        match(SparqlAutomaticParser::STRLEN);
        setState(1052);
        match(SparqlAutomaticParser::T__1);
        setState(1053);
        expression();
        setState(1054);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::REPLACE: {
        enterOuterAlt(_localctx, 18);
        setState(1056);
        strReplaceExpression();
        break;
      }

      case SparqlAutomaticParser::UCASE: {
        enterOuterAlt(_localctx, 19);
        setState(1057);
        match(SparqlAutomaticParser::UCASE);
        setState(1058);
        match(SparqlAutomaticParser::T__1);
        setState(1059);
        expression();
        setState(1060);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::LCASE: {
        enterOuterAlt(_localctx, 20);
        setState(1062);
        match(SparqlAutomaticParser::LCASE);
        setState(1063);
        match(SparqlAutomaticParser::T__1);
        setState(1064);
        expression();
        setState(1065);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ENCODE: {
        enterOuterAlt(_localctx, 21);
        setState(1067);
        match(SparqlAutomaticParser::ENCODE);
        setState(1068);
        match(SparqlAutomaticParser::T__25);
        setState(1069);
        match(SparqlAutomaticParser::FOR);
        setState(1070);
        match(SparqlAutomaticParser::T__25);
        setState(1071);
        match(SparqlAutomaticParser::URI);
        setState(1072);
        match(SparqlAutomaticParser::T__1);
        setState(1073);
        expression();
        setState(1074);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::CONTAINS: {
        enterOuterAlt(_localctx, 22);
        setState(1076);
        match(SparqlAutomaticParser::CONTAINS);
        setState(1077);
        match(SparqlAutomaticParser::T__1);
        setState(1078);
        expression();
        setState(1079);
        match(SparqlAutomaticParser::T__6);
        setState(1080);
        expression();
        setState(1081);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRSTARTS: {
        enterOuterAlt(_localctx, 23);
        setState(1083);
        match(SparqlAutomaticParser::STRSTARTS);
        setState(1084);
        match(SparqlAutomaticParser::T__1);
        setState(1085);
        expression();
        setState(1086);
        match(SparqlAutomaticParser::T__6);
        setState(1087);
        expression();
        setState(1088);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRENDS: {
        enterOuterAlt(_localctx, 24);
        setState(1090);
        match(SparqlAutomaticParser::STRENDS);
        setState(1091);
        match(SparqlAutomaticParser::T__1);
        setState(1092);
        expression();
        setState(1093);
        match(SparqlAutomaticParser::T__6);
        setState(1094);
        expression();
        setState(1095);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRBEFORE: {
        enterOuterAlt(_localctx, 25);
        setState(1097);
        match(SparqlAutomaticParser::STRBEFORE);
        setState(1098);
        match(SparqlAutomaticParser::T__1);
        setState(1099);
        expression();
        setState(1100);
        match(SparqlAutomaticParser::T__6);
        setState(1101);
        expression();
        setState(1102);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRAFTER: {
        enterOuterAlt(_localctx, 26);
        setState(1104);
        match(SparqlAutomaticParser::STRAFTER);
        setState(1105);
        match(SparqlAutomaticParser::T__1);
        setState(1106);
        expression();
        setState(1107);
        match(SparqlAutomaticParser::T__6);
        setState(1108);
        expression();
        setState(1109);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::YEAR: {
        enterOuterAlt(_localctx, 27);
        setState(1111);
        match(SparqlAutomaticParser::YEAR);
        setState(1112);
        match(SparqlAutomaticParser::T__1);
        setState(1113);
        expression();
        setState(1114);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MONTH: {
        enterOuterAlt(_localctx, 28);
        setState(1116);
        match(SparqlAutomaticParser::MONTH);
        setState(1117);
        match(SparqlAutomaticParser::T__1);
        setState(1118);
        expression();
        setState(1119);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::DAY: {
        enterOuterAlt(_localctx, 29);
        setState(1121);
        match(SparqlAutomaticParser::DAY);
        setState(1122);
        match(SparqlAutomaticParser::T__1);
        setState(1123);
        expression();
        setState(1124);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::HOURS: {
        enterOuterAlt(_localctx, 30);
        setState(1126);
        match(SparqlAutomaticParser::HOURS);
        setState(1127);
        match(SparqlAutomaticParser::T__1);
        setState(1128);
        expression();
        setState(1129);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MINUTES: {
        enterOuterAlt(_localctx, 31);
        setState(1131);
        match(SparqlAutomaticParser::MINUTES);
        setState(1132);
        match(SparqlAutomaticParser::T__1);
        setState(1133);
        expression();
        setState(1134);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SECONDS: {
        enterOuterAlt(_localctx, 32);
        setState(1136);
        match(SparqlAutomaticParser::SECONDS);
        setState(1137);
        match(SparqlAutomaticParser::T__1);
        setState(1138);
        expression();
        setState(1139);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::TIMEZONE: {
        enterOuterAlt(_localctx, 33);
        setState(1141);
        match(SparqlAutomaticParser::TIMEZONE);
        setState(1142);
        match(SparqlAutomaticParser::T__1);
        setState(1143);
        expression();
        setState(1144);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::TZ: {
        enterOuterAlt(_localctx, 34);
        setState(1146);
        match(SparqlAutomaticParser::TZ);
        setState(1147);
        match(SparqlAutomaticParser::T__1);
        setState(1148);
        expression();
        setState(1149);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::NOW: {
        enterOuterAlt(_localctx, 35);
        setState(1151);
        match(SparqlAutomaticParser::NOW);
        setState(1152);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::UUID: {
        enterOuterAlt(_localctx, 36);
        setState(1153);
        match(SparqlAutomaticParser::UUID);
        setState(1154);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::STRUUID: {
        enterOuterAlt(_localctx, 37);
        setState(1155);
        match(SparqlAutomaticParser::STRUUID);
        setState(1156);
        match(SparqlAutomaticParser::NIL);
        break;
      }

      case SparqlAutomaticParser::MD5: {
        enterOuterAlt(_localctx, 38);
        setState(1157);
        match(SparqlAutomaticParser::MD5);
        setState(1158);
        match(SparqlAutomaticParser::T__1);
        setState(1159);
        expression();
        setState(1160);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA1: {
        enterOuterAlt(_localctx, 39);
        setState(1162);
        match(SparqlAutomaticParser::SHA1);
        setState(1163);
        match(SparqlAutomaticParser::T__1);
        setState(1164);
        expression();
        setState(1165);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA256: {
        enterOuterAlt(_localctx, 40);
        setState(1167);
        match(SparqlAutomaticParser::SHA256);
        setState(1168);
        match(SparqlAutomaticParser::T__1);
        setState(1169);
        expression();
        setState(1170);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA384: {
        enterOuterAlt(_localctx, 41);
        setState(1172);
        match(SparqlAutomaticParser::SHA384);
        setState(1173);
        match(SparqlAutomaticParser::T__1);
        setState(1174);
        expression();
        setState(1175);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SHA512: {
        enterOuterAlt(_localctx, 42);
        setState(1177);
        match(SparqlAutomaticParser::SHA512);
        setState(1178);
        match(SparqlAutomaticParser::T__1);
        setState(1179);
        expression();
        setState(1180);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::COALESCE: {
        enterOuterAlt(_localctx, 43);
        setState(1182);
        match(SparqlAutomaticParser::COALESCE);
        setState(1183);
        expressionList();
        break;
      }

      case SparqlAutomaticParser::IF: {
        enterOuterAlt(_localctx, 44);
        setState(1184);
        match(SparqlAutomaticParser::IF);
        setState(1185);
        match(SparqlAutomaticParser::T__1);
        setState(1186);
        expression();
        setState(1187);
        match(SparqlAutomaticParser::T__6);
        setState(1188);
        expression();
        setState(1189);
        match(SparqlAutomaticParser::T__6);
        setState(1190);
        expression();
        setState(1191);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRLANG: {
        enterOuterAlt(_localctx, 45);
        setState(1193);
        match(SparqlAutomaticParser::STRLANG);
        setState(1194);
        match(SparqlAutomaticParser::T__1);
        setState(1195);
        expression();
        setState(1196);
        match(SparqlAutomaticParser::T__6);
        setState(1197);
        expression();
        setState(1198);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::STRDT: {
        enterOuterAlt(_localctx, 46);
        setState(1200);
        match(SparqlAutomaticParser::STRDT);
        setState(1201);
        match(SparqlAutomaticParser::T__1);
        setState(1202);
        expression();
        setState(1203);
        match(SparqlAutomaticParser::T__6);
        setState(1204);
        expression();
        setState(1205);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SAMETERM: {
        enterOuterAlt(_localctx, 47);
        setState(1207);
        match(SparqlAutomaticParser::SAMETERM);
        setState(1208);
        match(SparqlAutomaticParser::T__1);
        setState(1209);
        expression();
        setState(1210);
        match(SparqlAutomaticParser::T__6);
        setState(1211);
        expression();
        setState(1212);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISIRI: {
        enterOuterAlt(_localctx, 48);
        setState(1214);
        match(SparqlAutomaticParser::ISIRI);
        setState(1215);
        match(SparqlAutomaticParser::T__1);
        setState(1216);
        expression();
        setState(1217);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISURI: {
        enterOuterAlt(_localctx, 49);
        setState(1219);
        match(SparqlAutomaticParser::ISURI);
        setState(1220);
        match(SparqlAutomaticParser::T__1);
        setState(1221);
        expression();
        setState(1222);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISBLANK: {
        enterOuterAlt(_localctx, 50);
        setState(1224);
        match(SparqlAutomaticParser::ISBLANK);
        setState(1225);
        match(SparqlAutomaticParser::T__1);
        setState(1226);
        expression();
        setState(1227);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISLITERAL: {
        enterOuterAlt(_localctx, 51);
        setState(1229);
        match(SparqlAutomaticParser::ISLITERAL);
        setState(1230);
        match(SparqlAutomaticParser::T__1);
        setState(1231);
        expression();
        setState(1232);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::ISNUMERIC: {
        enterOuterAlt(_localctx, 52);
        setState(1234);
        match(SparqlAutomaticParser::ISNUMERIC);
        setState(1235);
        match(SparqlAutomaticParser::T__1);
        setState(1236);
        expression();
        setState(1237);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::REGEX: {
        enterOuterAlt(_localctx, 53);
        setState(1239);
        regexExpression();
        break;
      }

      case SparqlAutomaticParser::EXISTS: {
        enterOuterAlt(_localctx, 54);
        setState(1240);
        existsFunc();
        break;
      }

      case SparqlAutomaticParser::NOT: {
        enterOuterAlt(_localctx, 55);
        setState(1241);
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
  enterRule(_localctx, 206, SparqlAutomaticParser::RuleRegexExpression);
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
    setState(1244);
    match(SparqlAutomaticParser::REGEX);
    setState(1245);
    match(SparqlAutomaticParser::T__1);
    setState(1246);
    expression();
    setState(1247);
    match(SparqlAutomaticParser::T__6);
    setState(1248);
    expression();
    setState(1251);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__6) {
      setState(1249);
      match(SparqlAutomaticParser::T__6);
      setState(1250);
      expression();
    }
    setState(1253);
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
  enterRule(_localctx, 208, SparqlAutomaticParser::RuleSubstringExpression);
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
    setState(1255);
    match(SparqlAutomaticParser::SUBSTR);
    setState(1256);
    match(SparqlAutomaticParser::T__1);
    setState(1257);
    expression();
    setState(1258);
    match(SparqlAutomaticParser::T__6);
    setState(1259);
    expression();
    setState(1262);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__6) {
      setState(1260);
      match(SparqlAutomaticParser::T__6);
      setState(1261);
      expression();
    }
    setState(1264);
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
  enterRule(_localctx, 210, SparqlAutomaticParser::RuleStrReplaceExpression);
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
    setState(1266);
    match(SparqlAutomaticParser::REPLACE);
    setState(1267);
    match(SparqlAutomaticParser::T__1);
    setState(1268);
    expression();
    setState(1269);
    match(SparqlAutomaticParser::T__6);
    setState(1270);
    expression();
    setState(1271);
    match(SparqlAutomaticParser::T__6);
    setState(1272);
    expression();
    setState(1275);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::T__6) {
      setState(1273);
      match(SparqlAutomaticParser::T__6);
      setState(1274);
      expression();
    }
    setState(1277);
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
  enterRule(_localctx, 212, SparqlAutomaticParser::RuleExistsFunc);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1279);
    match(SparqlAutomaticParser::EXISTS);
    setState(1280);
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
  enterRule(_localctx, 214, SparqlAutomaticParser::RuleNotExistsFunc);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1282);
    match(SparqlAutomaticParser::NOT);
    setState(1283);
    match(SparqlAutomaticParser::EXISTS);
    setState(1284);
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
  enterRule(_localctx, 216, SparqlAutomaticParser::RuleAggregate);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1350);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::COUNT: {
        enterOuterAlt(_localctx, 1);
        setState(1286);
        match(SparqlAutomaticParser::COUNT);
        setState(1287);
        match(SparqlAutomaticParser::T__1);
        setState(1289);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1288);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1293);
        _errHandler->sync(this);
        switch (_input->LA(1)) {
          case SparqlAutomaticParser::T__0: {
            setState(1291);
            match(SparqlAutomaticParser::T__0);
            break;
          }

          case SparqlAutomaticParser::T__1:
          case SparqlAutomaticParser::T__12:
          case SparqlAutomaticParser::T__14:
          case SparqlAutomaticParser::T__24:
          case SparqlAutomaticParser::T__27:
          case SparqlAutomaticParser::T__28:
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
          case SparqlAutomaticParser::LANGTAG:
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
            setState(1292);
            expression();
            break;
          }

          default:
            throw NoViableAltException(this);
        }
        setState(1295);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SUM: {
        enterOuterAlt(_localctx, 2);
        setState(1296);
        match(SparqlAutomaticParser::SUM);
        setState(1297);
        match(SparqlAutomaticParser::T__1);
        setState(1299);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1298);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1301);
        expression();
        setState(1302);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MIN: {
        enterOuterAlt(_localctx, 3);
        setState(1304);
        match(SparqlAutomaticParser::MIN);
        setState(1305);
        match(SparqlAutomaticParser::T__1);
        setState(1307);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1306);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1309);
        expression();
        setState(1310);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::MAX: {
        enterOuterAlt(_localctx, 4);
        setState(1312);
        match(SparqlAutomaticParser::MAX);
        setState(1313);
        match(SparqlAutomaticParser::T__1);
        setState(1315);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1314);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1317);
        expression();
        setState(1318);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::AVG: {
        enterOuterAlt(_localctx, 5);
        setState(1320);
        match(SparqlAutomaticParser::AVG);
        setState(1321);
        match(SparqlAutomaticParser::T__1);
        setState(1323);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1322);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1325);
        expression();
        setState(1326);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::SAMPLE: {
        enterOuterAlt(_localctx, 6);
        setState(1328);
        match(SparqlAutomaticParser::SAMPLE);
        setState(1329);
        match(SparqlAutomaticParser::T__1);
        setState(1331);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1330);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1333);
        expression();
        setState(1334);
        match(SparqlAutomaticParser::T__2);
        break;
      }

      case SparqlAutomaticParser::GROUP_CONCAT: {
        enterOuterAlt(_localctx, 7);
        setState(1336);
        match(SparqlAutomaticParser::GROUP_CONCAT);
        setState(1337);
        match(SparqlAutomaticParser::T__1);
        setState(1339);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::DISTINCT) {
          setState(1338);
          match(SparqlAutomaticParser::DISTINCT);
        }
        setState(1341);
        expression();
        setState(1346);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == SparqlAutomaticParser::T__7) {
          setState(1342);
          match(SparqlAutomaticParser::T__7);
          setState(1343);
          match(SparqlAutomaticParser::SEPARATOR);
          setState(1344);
          match(SparqlAutomaticParser::T__19);
          setState(1345);
          string();
        }
        setState(1348);
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
  enterRule(_localctx, 218, SparqlAutomaticParser::RuleIriOrFunction);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1352);
    iri();
    setState(1354);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 131, _ctx)) {
      case 1: {
        setState(1353);
        argList();
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
  enterRule(_localctx, 220, SparqlAutomaticParser::RuleRdfLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1356);
    string();
    setState(1360);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 132, _ctx)) {
      case 1: {
        setState(1357);
        match(SparqlAutomaticParser::LANGTAG);
        break;
      }

      case 2: {
        setState(1358);
        match(SparqlAutomaticParser::T__26);
        setState(1359);
        iri();
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
  enterRule(_localctx, 222, SparqlAutomaticParser::RuleNumericLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1365);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::INTEGER:
      case SparqlAutomaticParser::DECIMAL:
      case SparqlAutomaticParser::DOUBLE: {
        enterOuterAlt(_localctx, 1);
        setState(1362);
        numericLiteralUnsigned();
        break;
      }

      case SparqlAutomaticParser::INTEGER_POSITIVE:
      case SparqlAutomaticParser::DECIMAL_POSITIVE:
      case SparqlAutomaticParser::DOUBLE_POSITIVE: {
        enterOuterAlt(_localctx, 2);
        setState(1363);
        numericLiteralPositive();
        break;
      }

      case SparqlAutomaticParser::INTEGER_NEGATIVE:
      case SparqlAutomaticParser::DECIMAL_NEGATIVE:
      case SparqlAutomaticParser::DOUBLE_NEGATIVE: {
        enterOuterAlt(_localctx, 3);
        setState(1364);
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
  enterRule(_localctx, 224, SparqlAutomaticParser::RuleNumericLiteralUnsigned);
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
    setState(1367);
    _la = _input->LA(1);
    if (!(((((_la - 146) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 146)) &
            ((1ULL << (SparqlAutomaticParser::INTEGER - 146)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL - 146)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE - 146)))) != 0))) {
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
  enterRule(_localctx, 226, SparqlAutomaticParser::RuleNumericLiteralPositive);
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
    setState(1369);
    _la = _input->LA(1);
    if (!(((((_la - 149) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 149)) &
            ((1ULL << (SparqlAutomaticParser::INTEGER_POSITIVE - 149)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_POSITIVE - 149)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_POSITIVE - 149)))) !=
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
  enterRule(_localctx, 228, SparqlAutomaticParser::RuleNumericLiteralNegative);
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
    setState(1371);
    _la = _input->LA(1);
    if (!(((((_la - 152) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 152)) &
            ((1ULL << (SparqlAutomaticParser::INTEGER_NEGATIVE - 152)) |
             (1ULL << (SparqlAutomaticParser::DECIMAL_NEGATIVE - 152)) |
             (1ULL << (SparqlAutomaticParser::DOUBLE_NEGATIVE - 152)))) !=
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
  enterRule(_localctx, 230, SparqlAutomaticParser::RuleBooleanLiteral);
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
    setState(1373);
    _la = _input->LA(1);
    if (!(_la == SparqlAutomaticParser::T__27

          || _la == SparqlAutomaticParser::T__28)) {
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
  enterRule(_localctx, 232, SparqlAutomaticParser::RuleString);
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
    setState(1375);
    _la = _input->LA(1);
    if (!(((((_la - 156) & ~0x3fULL) == 0) &&
           ((1ULL << (_la - 156)) &
            ((1ULL << (SparqlAutomaticParser::STRING_LITERAL1 - 156)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL2 - 156)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG1 - 156)) |
             (1ULL << (SparqlAutomaticParser::STRING_LITERAL_LONG2 - 156)))) !=
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

tree::TerminalNode* SparqlAutomaticParser::IriContext::LANGTAG() {
  return getToken(SparqlAutomaticParser::LANGTAG, 0);
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
  enterRule(_localctx, 234, SparqlAutomaticParser::RuleIri);
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
    setState(1379);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == SparqlAutomaticParser::LANGTAG) {
      setState(1377);
      match(SparqlAutomaticParser::LANGTAG);
      setState(1378);
      match(SparqlAutomaticParser::T__29);
    }
    setState(1383);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::IRI_REF: {
        setState(1381);
        iriref();
        break;
      }

      case SparqlAutomaticParser::PNAME_NS:
      case SparqlAutomaticParser::PNAME_LN: {
        setState(1382);
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
  enterRule(_localctx, 236, SparqlAutomaticParser::RulePrefixedName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1387);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case SparqlAutomaticParser::PNAME_LN: {
        enterOuterAlt(_localctx, 1);
        setState(1385);
        pnameLn();
        break;
      }

      case SparqlAutomaticParser::PNAME_NS: {
        enterOuterAlt(_localctx, 2);
        setState(1386);
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
  enterRule(_localctx, 238, SparqlAutomaticParser::RuleBlankNode);
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
    setState(1389);
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
  enterRule(_localctx, 240, SparqlAutomaticParser::RuleIriref);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1391);
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
  enterRule(_localctx, 242, SparqlAutomaticParser::RulePnameLn);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1393);
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
  enterRule(_localctx, 244, SparqlAutomaticParser::RulePnameNs);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1395);
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
    "strangeMultiplicativeSubexprOfAdditive",
    "multiplicativeExpression",
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
    "",     "'*'", "'('",  "')'",  "'{'",    "'}'",     "'.'",      "','",
    "';'",  "'a'", "'|'",  "'/'",  "'^'",    "'+'",     "'\u003F'", "'!'",
    "'['",  "']'", "'||'", "'&&'", "'='",    "'!='",    "'>'",      "'<='",
    "'>='", "'-'", "'_'",  "'^^'", "'true'", "'false'", "'@'"};

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
      0x3,   0xae,   0x578,  0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
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
      0x3,   0x2,    0x3,    0x2,    0x3,    0x2,    0x3,    0x2,    0x3,
      0x2,   0x5,    0x2,    0xfe,   0xa,    0x2,    0x3,    0x2,    0x3,
      0x2,   0x3,    0x2,    0x3,    0x3,    0x3,    0x3,    0x7,    0x3,
      0x105, 0xa,    0x3,    0xc,    0x3,    0xe,    0x3,    0x108,  0xb,
      0x3,   0x3,    0x4,    0x3,    0x4,    0x3,    0x4,    0x3,    0x5,
      0x3,   0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x6,    0x3,
      0x6,   0x7,    0x6,    0x113,  0xa,    0x6,    0xc,    0x6,    0xe,
      0x6,   0x116,  0xb,    0x6,    0x3,    0x6,    0x3,    0x6,    0x3,
      0x6,   0x3,    0x7,    0x3,    0x7,    0x3,    0x7,    0x3,    0x7,
      0x3,   0x7,    0x3,    0x8,    0x3,    0x8,    0x5,    0x8,    0x122,
      0xa,   0x8,    0x3,    0x8,    0x6,    0x8,    0x125,  0xa,    0x8,
      0xd,   0x8,    0xe,    0x8,    0x126,  0x3,    0x8,    0x5,    0x8,
      0x12a, 0xa,    0x8,    0x3,    0x9,    0x3,    0x9,    0x5,    0x9,
      0x12e, 0xa,    0x9,    0x3,    0xa,    0x3,    0xa,    0x3,    0xa,
      0x3,   0xa,    0x3,    0xb,    0x3,    0xb,    0x3,    0xb,    0x3,
      0xb,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x7,    0xc,
      0x13b, 0xa,    0xc,    0xc,    0xc,    0xe,    0xc,    0x13e,  0xb,
      0xc,   0x3,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x7,   0xc,    0x144,  0xa,    0xc,    0xc,    0xc,    0xe,    0xc,
      0x147, 0xb,    0xc,    0x3,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x5,   0xc,    0x14c,  0xa,    0xc,    0x3,    0xc,    0x3,    0xc,
      0x5,   0xc,    0x150,  0xa,    0xc,    0x3,    0xd,    0x3,    0xd,
      0x6,   0xd,    0x154,  0xa,    0xd,    0xd,    0xd,    0xe,    0xd,
      0x155, 0x3,    0xd,    0x5,    0xd,    0x159,  0xa,    0xd,    0x3,
      0xd,   0x7,    0xd,    0x15c,  0xa,    0xd,    0xc,    0xd,    0xe,
      0xd,   0x15f,  0xb,    0xd,    0x3,    0xd,    0x5,    0xd,    0x162,
      0xa,   0xd,    0x3,    0xd,    0x3,    0xd,    0x3,    0xe,    0x3,
      0xe,   0x7,    0xe,    0x168,  0xa,    0xe,    0xc,    0xe,    0xe,
      0xe,   0x16b,  0xb,    0xe,    0x3,    0xe,    0x3,    0xe,    0x3,
      0xe,   0x3,    0xf,    0x3,    0xf,    0x3,    0xf,    0x5,    0xf,
      0x173, 0xa,    0xf,    0x3,    0x10,   0x3,    0x10,   0x3,    0x11,
      0x3,   0x11,   0x3,    0x11,   0x3,    0x12,   0x3,    0x12,   0x3,
      0x13,  0x5,    0x13,   0x17d,  0xa,    0x13,   0x3,    0x13,   0x3,
      0x13,  0x3,    0x14,   0x5,    0x14,   0x182,  0xa,    0x14,   0x3,
      0x14,  0x5,    0x14,   0x185,  0xa,    0x14,   0x3,    0x14,   0x5,
      0x14,  0x188,  0xa,    0x14,   0x3,    0x14,   0x5,    0x14,   0x18b,
      0xa,   0x14,   0x3,    0x15,   0x3,    0x15,   0x6,    0x15,   0x18f,
      0xa,   0x15,   0xd,    0x15,   0xe,    0x15,   0x190,  0x3,    0x16,
      0x3,   0x16,   0x3,    0x16,   0x3,    0x16,   0x3,    0x16,   0x3,
      0x16,  0x5,    0x16,   0x199,  0xa,    0x16,   0x3,    0x16,   0x3,
      0x16,  0x3,    0x16,   0x5,    0x16,   0x19e,  0xa,    0x16,   0x3,
      0x17,  0x3,    0x17,   0x6,    0x17,   0x1a2,  0xa,    0x17,   0xd,
      0x17,  0xe,    0x17,   0x1a3,  0x3,    0x18,   0x3,    0x18,   0x3,
      0x19,  0x3,    0x19,   0x6,    0x19,   0x1aa,  0xa,    0x19,   0xd,
      0x19,  0xe,    0x19,   0x1ab,  0x3,    0x1a,   0x3,    0x1a,   0x3,
      0x1a,  0x3,    0x1a,   0x5,    0x1a,   0x1b2,  0xa,    0x1a,   0x5,
      0x1a,  0x1b4,  0xa,    0x1a,   0x3,    0x1b,   0x3,    0x1b,   0x5,
      0x1b,  0x1b8,  0xa,    0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1bb,
      0xa,   0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1bf,
      0xa,   0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1c2,  0xa,    0x1b,
      0x3,   0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1c6,  0xa,    0x1b,
      0x3,   0x1b,   0x5,    0x1b,   0x1c9,  0xa,    0x1b,   0x3,    0x1b,
      0x3,   0x1b,   0x5,    0x1b,   0x1cd,  0xa,    0x1b,   0x3,    0x1b,
      0x5,   0x1b,   0x1d0,  0xa,    0x1b,   0x3,    0x1b,   0x3,    0x1b,
      0x5,   0x1b,   0x1d4,  0xa,    0x1b,   0x3,    0x1b,   0x5,    0x1b,
      0x1d7, 0xa,    0x1b,   0x3,    0x1b,   0x3,    0x1b,   0x5,    0x1b,
      0x1db, 0xa,    0x1b,   0x3,    0x1b,   0x5,    0x1b,   0x1de,  0xa,
      0x1b,  0x5,    0x1b,   0x1e0,  0xa,    0x1b,   0x3,    0x1c,   0x3,
      0x1c,  0x3,    0x1c,   0x3,    0x1d,   0x3,    0x1d,   0x3,    0x1d,
      0x3,   0x1e,   0x3,    0x1e,   0x3,    0x1e,   0x3,    0x1f,   0x3,
      0x1f,  0x5,    0x1f,   0x1ed,  0xa,    0x1f,   0x3,    0x20,   0x3,
      0x20,  0x3,    0x20,   0x5,    0x20,   0x1f2,  0xa,    0x20,   0x5,
      0x20,  0x1f4,  0xa,    0x20,   0x3,    0x21,   0x3,    0x21,   0x3,
      0x21,  0x5,    0x21,   0x1f9,  0xa,    0x21,   0x3,    0x21,   0x3,
      0x21,  0x3,    0x22,   0x5,    0x22,   0x1fe,  0xa,    0x22,   0x3,
      0x22,  0x3,    0x22,   0x5,    0x22,   0x202,  0xa,    0x22,   0x3,
      0x22,  0x5,    0x22,   0x205,  0xa,    0x22,   0x7,    0x22,   0x207,
      0xa,   0x22,   0xc,    0x22,   0xe,    0x22,   0x20a,  0xb,    0x22,
      0x3,   0x23,   0x3,    0x23,   0x3,    0x23,   0x5,    0x23,   0x20f,
      0xa,   0x23,   0x5,    0x23,   0x211,  0xa,    0x23,   0x3,    0x24,
      0x3,   0x24,   0x3,    0x24,   0x3,    0x24,   0x3,    0x24,   0x3,
      0x24,  0x3,    0x24,   0x3,    0x24,   0x5,    0x24,   0x21b,  0xa,
      0x24,  0x3,    0x25,   0x3,    0x25,   0x3,    0x25,   0x3,    0x26,
      0x3,   0x26,   0x3,    0x26,   0x3,    0x26,   0x3,    0x27,   0x3,
      0x27,  0x5,    0x27,   0x226,  0xa,    0x27,   0x3,    0x27,   0x3,
      0x27,  0x3,    0x27,   0x3,    0x28,   0x3,    0x28,   0x3,    0x28,
      0x3,   0x28,   0x3,    0x28,   0x3,    0x28,   0x3,    0x28,   0x3,
      0x29,  0x3,    0x29,   0x3,    0x29,   0x3,    0x2a,   0x3,    0x2a,
      0x5,   0x2a,   0x237,  0xa,    0x2a,   0x3,    0x2b,   0x3,    0x2b,
      0x3,   0x2b,   0x7,    0x2b,   0x23c,  0xa,    0x2b,   0xc,    0x2b,
      0xe,   0x2b,   0x23f,  0xb,    0x2b,   0x3,    0x2b,   0x3,    0x2b,
      0x3,   0x2c,   0x3,    0x2c,   0x3,    0x2c,   0x7,    0x2c,   0x246,
      0xa,   0x2c,   0xc,    0x2c,   0xe,    0x2c,   0x249,  0xb,    0x2c,
      0x3,   0x2c,   0x5,    0x2c,   0x24c,  0xa,    0x2c,   0x3,    0x2c,
      0x3,   0x2c,   0x7,    0x2c,   0x250,  0xa,    0x2c,   0xc,    0x2c,
      0xe,   0x2c,   0x253,  0xb,    0x2c,   0x3,    0x2c,   0x3,    0x2c,
      0x3,   0x2d,   0x3,    0x2d,   0x7,    0x2d,   0x259,  0xa,    0x2d,
      0xc,   0x2d,   0xe,    0x2d,   0x25c,  0xb,    0x2d,   0x3,    0x2d,
      0x3,   0x2d,   0x5,    0x2d,   0x260,  0xa,    0x2d,   0x3,    0x2e,
      0x3,   0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x3,    0x2e,   0x5,
      0x2e,  0x267,  0xa,    0x2e,   0x3,    0x2f,   0x3,    0x2f,   0x3,
      0x2f,  0x3,    0x30,   0x3,    0x30,   0x3,    0x30,   0x7,    0x30,
      0x26f, 0xa,    0x30,   0xc,    0x30,   0xe,    0x30,   0x272,  0xb,
      0x30,  0x3,    0x31,   0x3,    0x31,   0x3,    0x31,   0x3,    0x32,
      0x3,   0x32,   0x3,    0x32,   0x5,    0x32,   0x27a,  0xa,    0x32,
      0x3,   0x33,   0x3,    0x33,   0x3,    0x33,   0x3,    0x34,   0x3,
      0x34,  0x3,    0x34,   0x5,    0x34,   0x282,  0xa,    0x34,   0x3,
      0x34,  0x3,    0x34,   0x3,    0x34,   0x7,    0x34,   0x287,  0xa,
      0x34,  0xc,    0x34,   0xe,    0x34,   0x28a,  0xb,    0x34,   0x3,
      0x34,  0x3,    0x34,   0x5,    0x34,   0x28e,  0xa,    0x34,   0x3,
      0x35,  0x3,    0x35,   0x3,    0x35,   0x3,    0x35,   0x3,    0x35,
      0x7,   0x35,   0x295,  0xa,    0x35,   0xc,    0x35,   0xe,    0x35,
      0x298, 0xb,    0x35,   0x3,    0x35,   0x3,    0x35,   0x5,    0x35,
      0x29c, 0xa,    0x35,   0x3,    0x36,   0x3,    0x36,   0x5,    0x36,
      0x2a0, 0xa,    0x36,   0x3,    0x36,   0x3,    0x36,   0x3,    0x37,
      0x3,   0x37,   0x3,    0x37,   0x5,    0x37,   0x2a7,  0xa,    0x37,
      0x5,   0x37,   0x2a9,  0xa,    0x37,   0x3,    0x38,   0x3,    0x38,
      0x3,   0x38,   0x3,    0x38,   0x3,    0x38,   0x3,    0x38,   0x5,
      0x38,  0x2b1,  0xa,    0x38,   0x3,    0x39,   0x5,    0x39,   0x2b4,
      0xa,   0x39,   0x3,    0x3a,   0x3,    0x3a,   0x3,    0x3a,   0x3,
      0x3a,  0x3,    0x3a,   0x3,    0x3a,   0x5,    0x3a,   0x2bc,  0xa,
      0x3a,  0x7,    0x3a,   0x2be,  0xa,    0x3a,   0xc,    0x3a,   0xe,
      0x3a,  0x2c1,  0xb,    0x3a,   0x3,    0x3b,   0x3,    0x3b,   0x5,
      0x3b,  0x2c5,  0xa,    0x3b,   0x3,    0x3c,   0x3,    0x3c,   0x3,
      0x3c,  0x7,    0x3c,   0x2ca,  0xa,    0x3c,   0xc,    0x3c,   0xe,
      0x3c,  0x2cd,  0xb,    0x3c,   0x3,    0x3d,   0x3,    0x3d,   0x3,
      0x3e,  0x3,    0x3e,   0x3,    0x3e,   0x3,    0x3e,   0x3,    0x3e,
      0x3,   0x3e,   0x5,    0x3e,   0x2d7,  0xa,    0x3e,   0x3,    0x3f,
      0x5,   0x3f,   0x2da,  0xa,    0x3f,   0x3,    0x40,   0x3,    0x40,
      0x3,   0x40,   0x3,    0x40,   0x3,    0x40,   0x3,    0x40,   0x5,
      0x40,  0x2e2,  0xa,    0x40,   0x7,    0x40,   0x2e4,  0xa,    0x40,
      0xc,   0x40,   0xe,    0x40,   0x2e7,  0xb,    0x40,   0x3,    0x41,
      0x3,   0x41,   0x3,    0x42,   0x3,    0x42,   0x3,    0x43,   0x3,
      0x43,  0x5,    0x43,   0x2ef,  0xa,    0x43,   0x3,    0x44,   0x3,
      0x44,  0x3,    0x44,   0x7,    0x44,   0x2f4,  0xa,    0x44,   0xc,
      0x44,  0xe,    0x44,   0x2f7,  0xb,    0x44,   0x3,    0x45,   0x3,
      0x45,  0x3,    0x46,   0x3,    0x46,   0x3,    0x47,   0x3,    0x47,
      0x3,   0x47,   0x7,    0x47,   0x300,  0xa,    0x47,   0xc,    0x47,
      0xe,   0x47,   0x303,  0xb,    0x47,   0x3,    0x48,   0x3,    0x48,
      0x3,   0x48,   0x7,    0x48,   0x308,  0xa,    0x48,   0xc,    0x48,
      0xe,   0x48,   0x30b,  0xb,    0x48,   0x3,    0x49,   0x3,    0x49,
      0x5,   0x49,   0x30f,  0xa,    0x49,   0x3,    0x4a,   0x3,    0x4a,
      0x3,   0x4a,   0x5,    0x4a,   0x314,  0xa,    0x4a,   0x3,    0x4b,
      0x3,   0x4b,   0x3,    0x4c,   0x3,    0x4c,   0x3,    0x4c,   0x3,
      0x4c,  0x3,    0x4c,   0x3,    0x4c,   0x3,    0x4c,   0x3,    0x4c,
      0x5,   0x4c,   0x320,  0xa,    0x4c,   0x3,    0x4d,   0x3,    0x4d,
      0x3,   0x4d,   0x3,    0x4d,   0x3,    0x4d,   0x7,    0x4d,   0x327,
      0xa,   0x4d,   0xc,    0x4d,   0xe,    0x4d,   0x32a,  0xb,    0x4d,
      0x5,   0x4d,   0x32c,  0xa,    0x4d,   0x3,    0x4d,   0x5,    0x4d,
      0x32f, 0xa,    0x4d,   0x3,    0x4e,   0x3,    0x4e,   0x3,    0x4e,
      0x3,   0x4e,   0x3,    0x4e,   0x5,    0x4e,   0x336,  0xa,    0x4e,
      0x5,   0x4e,   0x338,  0xa,    0x4e,   0x3,    0x4f,   0x3,    0x4f,
      0x3,   0x50,   0x3,    0x50,   0x5,    0x50,   0x33e,  0xa,    0x50,
      0x3,   0x51,   0x3,    0x51,   0x3,    0x51,   0x3,    0x51,   0x3,
      0x52,  0x3,    0x52,   0x5,    0x52,   0x346,  0xa,    0x52,   0x3,
      0x53,  0x3,    0x53,   0x3,    0x53,   0x3,    0x53,   0x3,    0x54,
      0x3,   0x54,   0x6,    0x54,   0x34e,  0xa,    0x54,   0xd,    0x54,
      0xe,   0x54,   0x34f,  0x3,    0x54,   0x3,    0x54,   0x3,    0x55,
      0x3,   0x55,   0x6,    0x55,   0x356,  0xa,    0x55,   0xd,    0x55,
      0xe,   0x55,   0x357,  0x3,    0x55,   0x3,    0x55,   0x3,    0x56,
      0x3,   0x56,   0x5,    0x56,   0x35e,  0xa,    0x56,   0x3,    0x57,
      0x3,   0x57,   0x5,    0x57,   0x362,  0xa,    0x57,   0x3,    0x58,
      0x3,   0x58,   0x5,    0x58,   0x366,  0xa,    0x58,   0x3,    0x59,
      0x3,   0x59,   0x5,    0x59,   0x36a,  0xa,    0x59,   0x3,    0x5a,
      0x3,   0x5a,   0x3,    0x5b,   0x3,    0x5b,   0x3,    0x5b,   0x3,
      0x5b,  0x3,    0x5b,   0x3,    0x5b,   0x5,    0x5b,   0x374,  0xa,
      0x5b,  0x3,    0x5c,   0x3,    0x5c,   0x3,    0x5d,   0x3,    0x5d,
      0x3,   0x5d,   0x7,    0x5d,   0x37b,  0xa,    0x5d,   0xc,    0x5d,
      0xe,   0x5d,   0x37e,  0xb,    0x5d,   0x3,    0x5e,   0x3,    0x5e,
      0x3,   0x5e,   0x7,    0x5e,   0x383,  0xa,    0x5e,   0xc,    0x5e,
      0xe,   0x5e,   0x386,  0xb,    0x5e,   0x3,    0x5f,   0x3,    0x5f,
      0x3,   0x60,   0x3,    0x60,   0x3,    0x60,   0x3,    0x60,   0x3,
      0x60,  0x3,    0x60,   0x3,    0x60,   0x3,    0x60,   0x3,    0x60,
      0x3,   0x60,   0x3,    0x60,   0x3,    0x60,   0x3,    0x60,   0x3,
      0x60,  0x3,    0x60,   0x3,    0x60,   0x3,    0x60,   0x5,    0x60,
      0x39b, 0xa,    0x60,   0x3,    0x61,   0x3,    0x61,   0x3,    0x62,
      0x3,   0x62,   0x3,    0x62,   0x3,    0x62,   0x3,    0x62,   0x3,
      0x62,  0x7,    0x62,   0x3a5,  0xa,    0x62,   0xc,    0x62,   0xe,
      0x62,  0x3a8,  0xb,    0x62,   0x3,    0x63,   0x3,    0x63,   0x5,
      0x63,  0x3ac,  0xa,    0x63,   0x3,    0x63,   0x3,    0x63,   0x3,
      0x63,  0x3,    0x63,   0x7,    0x63,   0x3b2,  0xa,    0x63,   0xc,
      0x63,  0xe,    0x63,   0x3b5,  0xb,    0x63,   0x3,    0x64,   0x3,
      0x64,  0x3,    0x64,   0x3,    0x64,   0x3,    0x64,   0x7,    0x64,
      0x3bc, 0xa,    0x64,   0xc,    0x64,   0xe,    0x64,   0x3bf,  0xb,
      0x64,  0x3,    0x65,   0x3,    0x65,   0x3,    0x65,   0x3,    0x65,
      0x3,   0x65,   0x3,    0x65,   0x3,    0x65,   0x5,    0x65,   0x3c8,
      0xa,   0x65,   0x3,    0x66,   0x3,    0x66,   0x3,    0x66,   0x3,
      0x66,  0x3,    0x66,   0x3,    0x66,   0x3,    0x66,   0x5,    0x66,
      0x3d1, 0xa,    0x66,   0x3,    0x67,   0x3,    0x67,   0x3,    0x67,
      0x3,   0x67,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x5,   0x68,   0x403,  0xa,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,
      0x3,   0x68,   0x3,    0x68,   0x3,    0x68,   0x3,    0x68,   0x3,
      0x68,  0x3,    0x68,   0x3,    0x68,   0x5,    0x68,   0x4dd,  0xa,
      0x68,  0x3,    0x69,   0x3,    0x69,   0x3,    0x69,   0x3,    0x69,
      0x3,   0x69,   0x3,    0x69,   0x3,    0x69,   0x5,    0x69,   0x4e6,
      0xa,   0x69,   0x3,    0x69,   0x3,    0x69,   0x3,    0x6a,   0x3,
      0x6a,  0x3,    0x6a,   0x3,    0x6a,   0x3,    0x6a,   0x3,    0x6a,
      0x3,   0x6a,   0x5,    0x6a,   0x4f1,  0xa,    0x6a,   0x3,    0x6a,
      0x3,   0x6a,   0x3,    0x6b,   0x3,    0x6b,   0x3,    0x6b,   0x3,
      0x6b,  0x3,    0x6b,   0x3,    0x6b,   0x3,    0x6b,   0x3,    0x6b,
      0x3,   0x6b,   0x5,    0x6b,   0x4fe,  0xa,    0x6b,   0x3,    0x6b,
      0x3,   0x6b,   0x3,    0x6c,   0x3,    0x6c,   0x3,    0x6c,   0x3,
      0x6d,  0x3,    0x6d,   0x3,    0x6d,   0x3,    0x6d,   0x3,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x5,    0x6e,   0x50c,  0xa,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x5,    0x6e,   0x510,  0xa,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x5,
      0x6e,  0x516,  0xa,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,
      0x6e,  0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x5,    0x6e,
      0x51e, 0xa,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x5,    0x6e,   0x526,
      0xa,   0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,
      0x6e,  0x3,    0x6e,   0x3,    0x6e,   0x5,    0x6e,   0x52e,  0xa,
      0x6e,  0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x5,    0x6e,   0x536,  0xa,    0x6e,
      0x3,   0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,
      0x6e,  0x3,    0x6e,   0x5,    0x6e,   0x53e,  0xa,    0x6e,   0x3,
      0x6e,  0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,   0x3,    0x6e,
      0x5,   0x6e,   0x545,  0xa,    0x6e,   0x3,    0x6e,   0x3,    0x6e,
      0x5,   0x6e,   0x549,  0xa,    0x6e,   0x3,    0x6f,   0x3,    0x6f,
      0x5,   0x6f,   0x54d,  0xa,    0x6f,   0x3,    0x70,   0x3,    0x70,
      0x3,   0x70,   0x3,    0x70,   0x5,    0x70,   0x553,  0xa,    0x70,
      0x3,   0x71,   0x3,    0x71,   0x3,    0x71,   0x5,    0x71,   0x558,
      0xa,   0x71,   0x3,    0x72,   0x3,    0x72,   0x3,    0x73,   0x3,
      0x73,  0x3,    0x74,   0x3,    0x74,   0x3,    0x75,   0x3,    0x75,
      0x3,   0x76,   0x3,    0x76,   0x3,    0x77,   0x3,    0x77,   0x5,
      0x77,  0x566,  0xa,    0x77,   0x3,    0x77,   0x3,    0x77,   0x5,
      0x77,  0x56a,  0xa,    0x77,   0x3,    0x78,   0x3,    0x78,   0x5,
      0x78,  0x56e,  0xa,    0x78,   0x3,    0x79,   0x3,    0x79,   0x3,
      0x7a,  0x3,    0x7a,   0x3,    0x7b,   0x3,    0x7b,   0x3,    0x7c,
      0x3,   0x7c,   0x3,    0x7c,   0x2,    0x2,    0x7d,   0x2,    0x4,
      0x6,   0x8,    0xa,    0xc,    0xe,    0x10,   0x12,   0x14,   0x16,
      0x18,  0x1a,   0x1c,   0x1e,   0x20,   0x22,   0x24,   0x26,   0x28,
      0x2a,  0x2c,   0x2e,   0x30,   0x32,   0x34,   0x36,   0x38,   0x3a,
      0x3c,  0x3e,   0x40,   0x42,   0x44,   0x46,   0x48,   0x4a,   0x4c,
      0x4e,  0x50,   0x52,   0x54,   0x56,   0x58,   0x5a,   0x5c,   0x5e,
      0x60,  0x62,   0x64,   0x66,   0x68,   0x6a,   0x6c,   0x6e,   0x70,
      0x72,  0x74,   0x76,   0x78,   0x7a,   0x7c,   0x7e,   0x80,   0x82,
      0x84,  0x86,   0x88,   0x8a,   0x8c,   0x8e,   0x90,   0x92,   0x94,
      0x96,  0x98,   0x9a,   0x9c,   0x9e,   0xa0,   0xa2,   0xa4,   0xa6,
      0xa8,  0xaa,   0xac,   0xae,   0xb0,   0xb2,   0xb4,   0xb6,   0xb8,
      0xba,  0xbc,   0xbe,   0xc0,   0xc2,   0xc4,   0xc6,   0xc8,   0xca,
      0xcc,  0xce,   0xd0,   0xd2,   0xd4,   0xd6,   0xd8,   0xda,   0xdc,
      0xde,  0xe0,   0xe2,   0xe4,   0xe6,   0xe8,   0xea,   0xec,   0xee,
      0xf0,  0xf2,   0xf4,   0xf6,   0x2,    0xc,    0x3,    0x2,    0x24,
      0x25,  0x3,    0x2,    0x31,   0x32,   0x4,    0x2,    0x3,    0x3,
      0xf,   0x10,   0x3,    0x2,    0x91,   0x92,   0x3,    0x2,    0x94,
      0x96,  0x3,    0x2,    0x97,   0x99,   0x3,    0x2,    0x9a,   0x9c,
      0x3,   0x2,    0x1e,   0x1f,   0x3,    0x2,    0x9e,   0xa1,   0x4,
      0x2,   0x90,   0x90,   0xa4,   0xa4,   0x2,    0x5e9,  0x2,    0xf8,
      0x3,   0x2,    0x2,    0x2,    0x4,    0x106,  0x3,    0x2,    0x2,
      0x2,   0x6,    0x109,  0x3,    0x2,    0x2,    0x2,    0x8,    0x10c,
      0x3,   0x2,    0x2,    0x2,    0xa,    0x110,  0x3,    0x2,    0x2,
      0x2,   0xc,    0x11a,  0x3,    0x2,    0x2,    0x2,    0xe,    0x11f,
      0x3,   0x2,    0x2,    0x2,    0x10,   0x12d,  0x3,    0x2,    0x2,
      0x2,   0x12,   0x12f,  0x3,    0x2,    0x2,    0x2,    0x14,   0x133,
      0x3,   0x2,    0x2,    0x2,    0x16,   0x137,  0x3,    0x2,    0x2,
      0x2,   0x18,   0x151,  0x3,    0x2,    0x2,    0x2,    0x1a,   0x165,
      0x3,   0x2,    0x2,    0x2,    0x1c,   0x16f,  0x3,    0x2,    0x2,
      0x2,   0x1e,   0x174,  0x3,    0x2,    0x2,    0x2,    0x20,   0x176,
      0x3,   0x2,    0x2,    0x2,    0x22,   0x179,  0x3,    0x2,    0x2,
      0x2,   0x24,   0x17c,  0x3,    0x2,    0x2,    0x2,    0x26,   0x181,
      0x3,   0x2,    0x2,    0x2,    0x28,   0x18c,  0x3,    0x2,    0x2,
      0x2,   0x2a,   0x19d,  0x3,    0x2,    0x2,    0x2,    0x2c,   0x19f,
      0x3,   0x2,    0x2,    0x2,    0x2e,   0x1a5,  0x3,    0x2,    0x2,
      0x2,   0x30,   0x1a7,  0x3,    0x2,    0x2,    0x2,    0x32,   0x1b3,
      0x3,   0x2,    0x2,    0x2,    0x34,   0x1df,  0x3,    0x2,    0x2,
      0x2,   0x36,   0x1e1,  0x3,    0x2,    0x2,    0x2,    0x38,   0x1e4,
      0x3,   0x2,    0x2,    0x2,    0x3a,   0x1e7,  0x3,    0x2,    0x2,
      0x2,   0x3c,   0x1ec,  0x3,    0x2,    0x2,    0x2,    0x3e,   0x1ee,
      0x3,   0x2,    0x2,    0x2,    0x40,   0x1f5,  0x3,    0x2,    0x2,
      0x2,   0x42,   0x1fd,  0x3,    0x2,    0x2,    0x2,    0x44,   0x20b,
      0x3,   0x2,    0x2,    0x2,    0x46,   0x21a,  0x3,    0x2,    0x2,
      0x2,   0x48,   0x21c,  0x3,    0x2,    0x2,    0x2,    0x4a,   0x21f,
      0x3,   0x2,    0x2,    0x2,    0x4c,   0x223,  0x3,    0x2,    0x2,
      0x2,   0x4e,   0x22a,  0x3,    0x2,    0x2,    0x2,    0x50,   0x231,
      0x3,   0x2,    0x2,    0x2,    0x52,   0x236,  0x3,    0x2,    0x2,
      0x2,   0x54,   0x238,  0x3,    0x2,    0x2,    0x2,    0x56,   0x24b,
      0x3,   0x2,    0x2,    0x2,    0x58,   0x25f,  0x3,    0x2,    0x2,
      0x2,   0x5a,   0x266,  0x3,    0x2,    0x2,    0x2,    0x5c,   0x268,
      0x3,   0x2,    0x2,    0x2,    0x5e,   0x26b,  0x3,    0x2,    0x2,
      0x2,   0x60,   0x273,  0x3,    0x2,    0x2,    0x2,    0x62,   0x279,
      0x3,   0x2,    0x2,    0x2,    0x64,   0x27b,  0x3,    0x2,    0x2,
      0x2,   0x66,   0x28d,  0x3,    0x2,    0x2,    0x2,    0x68,   0x29b,
      0x3,   0x2,    0x2,    0x2,    0x6a,   0x29d,  0x3,    0x2,    0x2,
      0x2,   0x6c,   0x2a3,  0x3,    0x2,    0x2,    0x2,    0x6e,   0x2b0,
      0x3,   0x2,    0x2,    0x2,    0x70,   0x2b3,  0x3,    0x2,    0x2,
      0x2,   0x72,   0x2b5,  0x3,    0x2,    0x2,    0x2,    0x74,   0x2c4,
      0x3,   0x2,    0x2,    0x2,    0x76,   0x2c6,  0x3,    0x2,    0x2,
      0x2,   0x78,   0x2ce,  0x3,    0x2,    0x2,    0x2,    0x7a,   0x2d6,
      0x3,   0x2,    0x2,    0x2,    0x7c,   0x2d9,  0x3,    0x2,    0x2,
      0x2,   0x7e,   0x2db,  0x3,    0x2,    0x2,    0x2,    0x80,   0x2e8,
      0x3,   0x2,    0x2,    0x2,    0x82,   0x2ea,  0x3,    0x2,    0x2,
      0x2,   0x84,   0x2ee,  0x3,    0x2,    0x2,    0x2,    0x86,   0x2f0,
      0x3,   0x2,    0x2,    0x2,    0x88,   0x2f8,  0x3,    0x2,    0x2,
      0x2,   0x8a,   0x2fa,  0x3,    0x2,    0x2,    0x2,    0x8c,   0x2fc,
      0x3,   0x2,    0x2,    0x2,    0x8e,   0x304,  0x3,    0x2,    0x2,
      0x2,   0x90,   0x30c,  0x3,    0x2,    0x2,    0x2,    0x92,   0x313,
      0x3,   0x2,    0x2,    0x2,    0x94,   0x315,  0x3,    0x2,    0x2,
      0x2,   0x96,   0x31f,  0x3,    0x2,    0x2,    0x2,    0x98,   0x32e,
      0x3,   0x2,    0x2,    0x2,    0x9a,   0x337,  0x3,    0x2,    0x2,
      0x2,   0x9c,   0x339,  0x3,    0x2,    0x2,    0x2,    0x9e,   0x33d,
      0x3,   0x2,    0x2,    0x2,    0xa0,   0x33f,  0x3,    0x2,    0x2,
      0x2,   0xa2,   0x345,  0x3,    0x2,    0x2,    0x2,    0xa4,   0x347,
      0x3,   0x2,    0x2,    0x2,    0xa6,   0x34b,  0x3,    0x2,    0x2,
      0x2,   0xa8,   0x353,  0x3,    0x2,    0x2,    0x2,    0xaa,   0x35d,
      0x3,   0x2,    0x2,    0x2,    0xac,   0x361,  0x3,    0x2,    0x2,
      0x2,   0xae,   0x365,  0x3,    0x2,    0x2,    0x2,    0xb0,   0x369,
      0x3,   0x2,    0x2,    0x2,    0xb2,   0x36b,  0x3,    0x2,    0x2,
      0x2,   0xb4,   0x373,  0x3,    0x2,    0x2,    0x2,    0xb6,   0x375,
      0x3,   0x2,    0x2,    0x2,    0xb8,   0x377,  0x3,    0x2,    0x2,
      0x2,   0xba,   0x37f,  0x3,    0x2,    0x2,    0x2,    0xbc,   0x387,
      0x3,   0x2,    0x2,    0x2,    0xbe,   0x389,  0x3,    0x2,    0x2,
      0x2,   0xc0,   0x39c,  0x3,    0x2,    0x2,    0x2,    0xc2,   0x39e,
      0x3,   0x2,    0x2,    0x2,    0xc4,   0x3ab,  0x3,    0x2,    0x2,
      0x2,   0xc6,   0x3b6,  0x3,    0x2,    0x2,    0x2,    0xc8,   0x3c7,
      0x3,   0x2,    0x2,    0x2,    0xca,   0x3d0,  0x3,    0x2,    0x2,
      0x2,   0xcc,   0x3d2,  0x3,    0x2,    0x2,    0x2,    0xce,   0x4dc,
      0x3,   0x2,    0x2,    0x2,    0xd0,   0x4de,  0x3,    0x2,    0x2,
      0x2,   0xd2,   0x4e9,  0x3,    0x2,    0x2,    0x2,    0xd4,   0x4f4,
      0x3,   0x2,    0x2,    0x2,    0xd6,   0x501,  0x3,    0x2,    0x2,
      0x2,   0xd8,   0x504,  0x3,    0x2,    0x2,    0x2,    0xda,   0x548,
      0x3,   0x2,    0x2,    0x2,    0xdc,   0x54a,  0x3,    0x2,    0x2,
      0x2,   0xde,   0x54e,  0x3,    0x2,    0x2,    0x2,    0xe0,   0x557,
      0x3,   0x2,    0x2,    0x2,    0xe2,   0x559,  0x3,    0x2,    0x2,
      0x2,   0xe4,   0x55b,  0x3,    0x2,    0x2,    0x2,    0xe6,   0x55d,
      0x3,   0x2,    0x2,    0x2,    0xe8,   0x55f,  0x3,    0x2,    0x2,
      0x2,   0xea,   0x561,  0x3,    0x2,    0x2,    0x2,    0xec,   0x565,
      0x3,   0x2,    0x2,    0x2,    0xee,   0x56d,  0x3,    0x2,    0x2,
      0x2,   0xf0,   0x56f,  0x3,    0x2,    0x2,    0x2,    0xf2,   0x571,
      0x3,   0x2,    0x2,    0x2,    0xf4,   0x573,  0x3,    0x2,    0x2,
      0x2,   0xf6,   0x575,  0x3,    0x2,    0x2,    0x2,    0xf8,   0xfd,
      0x5,   0x4,    0x3,    0x2,    0xf9,   0xfe,   0x5,    0xa,    0x6,
      0x2,   0xfa,   0xfe,   0x5,    0x16,   0xc,    0x2,    0xfb,   0xfe,
      0x5,   0x18,   0xd,    0x2,    0xfc,   0xfe,   0x5,    0x1a,   0xe,
      0x2,   0xfd,   0xf9,   0x3,    0x2,    0x2,    0x2,    0xfd,   0xfa,
      0x3,   0x2,    0x2,    0x2,    0xfd,   0xfb,   0x3,    0x2,    0x2,
      0x2,   0xfd,   0xfc,   0x3,    0x2,    0x2,    0x2,    0xfe,   0xff,
      0x3,   0x2,    0x2,    0x2,    0xff,   0x100,  0x5,    0x3c,   0x1f,
      0x2,   0x100,  0x101,  0x7,    0x2,    0x2,    0x3,    0x101,  0x3,
      0x3,   0x2,    0x2,    0x2,    0x102,  0x105,  0x5,    0x6,    0x4,
      0x2,   0x103,  0x105,  0x5,    0x8,    0x5,    0x2,    0x104,  0x102,
      0x3,   0x2,    0x2,    0x2,    0x104,  0x103,  0x3,    0x2,    0x2,
      0x2,   0x105,  0x108,  0x3,    0x2,    0x2,    0x2,    0x106,  0x104,
      0x3,   0x2,    0x2,    0x2,    0x106,  0x107,  0x3,    0x2,    0x2,
      0x2,   0x107,  0x5,    0x3,    0x2,    0x2,    0x2,    0x108,  0x106,
      0x3,   0x2,    0x2,    0x2,    0x109,  0x10a,  0x7,    0x21,   0x2,
      0x2,   0x10a,  0x10b,  0x5,    0xf2,   0x7a,   0x2,    0x10b,  0x7,
      0x3,   0x2,    0x2,    0x2,    0x10c,  0x10d,  0x7,    0x22,   0x2,
      0x2,   0x10d,  0x10e,  0x7,    0x8e,   0x2,    0x2,    0x10e,  0x10f,
      0x5,   0xf2,   0x7a,   0x2,    0x10f,  0x9,    0x3,    0x2,    0x2,
      0x2,   0x110,  0x114,  0x5,    0xe,    0x8,    0x2,    0x111,  0x113,
      0x5,   0x1c,   0xf,    0x2,    0x112,  0x111,  0x3,    0x2,    0x2,
      0x2,   0x113,  0x116,  0x3,    0x2,    0x2,    0x2,    0x114,  0x112,
      0x3,   0x2,    0x2,    0x2,    0x114,  0x115,  0x3,    0x2,    0x2,
      0x2,   0x115,  0x117,  0x3,    0x2,    0x2,    0x2,    0x116,  0x114,
      0x3,   0x2,    0x2,    0x2,    0x117,  0x118,  0x5,    0x24,   0x13,
      0x2,   0x118,  0x119,  0x5,    0x26,   0x14,   0x2,    0x119,  0xb,
      0x3,   0x2,    0x2,    0x2,    0x11a,  0x11b,  0x5,    0xe,    0x8,
      0x2,   0x11b,  0x11c,  0x5,    0x24,   0x13,   0x2,    0x11c,  0x11d,
      0x5,   0x26,   0x14,   0x2,    0x11d,  0x11e,  0x5,    0x3c,   0x1f,
      0x2,   0x11e,  0xd,    0x3,    0x2,    0x2,    0x2,    0x11f,  0x121,
      0x7,   0x23,   0x2,    0x2,    0x120,  0x122,  0x9,    0x2,    0x2,
      0x2,   0x121,  0x120,  0x3,    0x2,    0x2,    0x2,    0x121,  0x122,
      0x3,   0x2,    0x2,    0x2,    0x122,  0x129,  0x3,    0x2,    0x2,
      0x2,   0x123,  0x125,  0x5,    0x10,   0x9,    0x2,    0x124,  0x123,
      0x3,   0x2,    0x2,    0x2,    0x125,  0x126,  0x3,    0x2,    0x2,
      0x2,   0x126,  0x124,  0x3,    0x2,    0x2,    0x2,    0x126,  0x127,
      0x3,   0x2,    0x2,    0x2,    0x127,  0x12a,  0x3,    0x2,    0x2,
      0x2,   0x128,  0x12a,  0x7,    0x3,    0x2,    0x2,    0x129,  0x124,
      0x3,   0x2,    0x2,    0x2,    0x129,  0x128,  0x3,    0x2,    0x2,
      0x2,   0x12a,  0xf,    0x3,    0x2,    0x2,    0x2,    0x12b,  0x12e,
      0x5,   0xb2,   0x5a,   0x2,    0x12c,  0x12e,  0x5,    0x12,   0xa,
      0x2,   0x12d,  0x12b,  0x3,    0x2,    0x2,    0x2,    0x12d,  0x12c,
      0x3,   0x2,    0x2,    0x2,    0x12e,  0x11,   0x3,    0x2,    0x2,
      0x2,   0x12f,  0x130,  0x7,    0x4,    0x2,    0x2,    0x130,  0x131,
      0x5,   0x14,   0xb,    0x2,    0x131,  0x132,  0x7,    0x5,    0x2,
      0x2,   0x132,  0x13,   0x3,    0x2,    0x2,    0x2,    0x133,  0x134,
      0x5,   0xb6,   0x5c,   0x2,    0x134,  0x135,  0x7,    0x26,   0x2,
      0x2,   0x135,  0x136,  0x5,    0xb2,   0x5a,   0x2,    0x136,  0x15,
      0x3,   0x2,    0x2,    0x2,    0x137,  0x14f,  0x7,    0x27,   0x2,
      0x2,   0x138,  0x13c,  0x5,    0x6a,   0x36,   0x2,    0x139,  0x13b,
      0x5,   0x1c,   0xf,    0x2,    0x13a,  0x139,  0x3,    0x2,    0x2,
      0x2,   0x13b,  0x13e,  0x3,    0x2,    0x2,    0x2,    0x13c,  0x13a,
      0x3,   0x2,    0x2,    0x2,    0x13c,  0x13d,  0x3,    0x2,    0x2,
      0x2,   0x13d,  0x13f,  0x3,    0x2,    0x2,    0x2,    0x13e,  0x13c,
      0x3,   0x2,    0x2,    0x2,    0x13f,  0x140,  0x5,    0x24,   0x13,
      0x2,   0x140,  0x141,  0x5,    0x26,   0x14,   0x2,    0x141,  0x150,
      0x3,   0x2,    0x2,    0x2,    0x142,  0x144,  0x5,    0x1c,   0xf,
      0x2,   0x143,  0x142,  0x3,    0x2,    0x2,    0x2,    0x144,  0x147,
      0x3,   0x2,    0x2,    0x2,    0x145,  0x143,  0x3,    0x2,    0x2,
      0x2,   0x145,  0x146,  0x3,    0x2,    0x2,    0x2,    0x146,  0x148,
      0x3,   0x2,    0x2,    0x2,    0x147,  0x145,  0x3,    0x2,    0x2,
      0x2,   0x148,  0x149,  0x7,    0x28,   0x2,    0x2,    0x149,  0x14b,
      0x7,   0x6,    0x2,    0x2,    0x14a,  0x14c,  0x5,    0x3e,   0x20,
      0x2,   0x14b,  0x14a,  0x3,    0x2,    0x2,    0x2,    0x14b,  0x14c,
      0x3,   0x2,    0x2,    0x2,    0x14c,  0x14d,  0x3,    0x2,    0x2,
      0x2,   0x14d,  0x14e,  0x7,    0x7,    0x2,    0x2,    0x14e,  0x150,
      0x5,   0x26,   0x14,   0x2,    0x14f,  0x138,  0x3,    0x2,    0x2,
      0x2,   0x14f,  0x145,  0x3,    0x2,    0x2,    0x2,    0x150,  0x17,
      0x3,   0x2,    0x2,    0x2,    0x151,  0x158,  0x7,    0x29,   0x2,
      0x2,   0x152,  0x154,  0x5,    0xb0,   0x59,   0x2,    0x153,  0x152,
      0x3,   0x2,    0x2,    0x2,    0x154,  0x155,  0x3,    0x2,    0x2,
      0x2,   0x155,  0x153,  0x3,    0x2,    0x2,    0x2,    0x155,  0x156,
      0x3,   0x2,    0x2,    0x2,    0x156,  0x159,  0x3,    0x2,    0x2,
      0x2,   0x157,  0x159,  0x7,    0x3,    0x2,    0x2,    0x158,  0x153,
      0x3,   0x2,    0x2,    0x2,    0x158,  0x157,  0x3,    0x2,    0x2,
      0x2,   0x159,  0x15d,  0x3,    0x2,    0x2,    0x2,    0x15a,  0x15c,
      0x5,   0x1c,   0xf,    0x2,    0x15b,  0x15a,  0x3,    0x2,    0x2,
      0x2,   0x15c,  0x15f,  0x3,    0x2,    0x2,    0x2,    0x15d,  0x15b,
      0x3,   0x2,    0x2,    0x2,    0x15d,  0x15e,  0x3,    0x2,    0x2,
      0x2,   0x15e,  0x161,  0x3,    0x2,    0x2,    0x2,    0x15f,  0x15d,
      0x3,   0x2,    0x2,    0x2,    0x160,  0x162,  0x5,    0x24,   0x13,
      0x2,   0x161,  0x160,  0x3,    0x2,    0x2,    0x2,    0x161,  0x162,
      0x3,   0x2,    0x2,    0x2,    0x162,  0x163,  0x3,    0x2,    0x2,
      0x2,   0x163,  0x164,  0x5,    0x26,   0x14,   0x2,    0x164,  0x19,
      0x3,   0x2,    0x2,    0x2,    0x165,  0x169,  0x7,    0x2a,   0x2,
      0x2,   0x166,  0x168,  0x5,    0x1c,   0xf,    0x2,    0x167,  0x166,
      0x3,   0x2,    0x2,    0x2,    0x168,  0x16b,  0x3,    0x2,    0x2,
      0x2,   0x169,  0x167,  0x3,    0x2,    0x2,    0x2,    0x169,  0x16a,
      0x3,   0x2,    0x2,    0x2,    0x16a,  0x16c,  0x3,    0x2,    0x2,
      0x2,   0x16b,  0x169,  0x3,    0x2,    0x2,    0x2,    0x16c,  0x16d,
      0x5,   0x24,   0x13,   0x2,    0x16d,  0x16e,  0x5,    0x26,   0x14,
      0x2,   0x16e,  0x1b,   0x3,    0x2,    0x2,    0x2,    0x16f,  0x172,
      0x7,   0x2b,   0x2,    0x2,    0x170,  0x173,  0x5,    0x1e,   0x10,
      0x2,   0x171,  0x173,  0x5,    0x20,   0x11,   0x2,    0x172,  0x170,
      0x3,   0x2,    0x2,    0x2,    0x172,  0x171,  0x3,    0x2,    0x2,
      0x2,   0x173,  0x1d,   0x3,    0x2,    0x2,    0x2,    0x174,  0x175,
      0x5,   0x22,   0x12,   0x2,    0x175,  0x1f,   0x3,    0x2,    0x2,
      0x2,   0x176,  0x177,  0x7,    0x2c,   0x2,    0x2,    0x177,  0x178,
      0x5,   0x22,   0x12,   0x2,    0x178,  0x21,   0x3,    0x2,    0x2,
      0x2,   0x179,  0x17a,  0x5,    0xec,   0x77,   0x2,    0x17a,  0x23,
      0x3,   0x2,    0x2,    0x2,    0x17b,  0x17d,  0x7,    0x28,   0x2,
      0x2,   0x17c,  0x17b,  0x3,    0x2,    0x2,    0x2,    0x17c,  0x17d,
      0x3,   0x2,    0x2,    0x2,    0x17d,  0x17e,  0x3,    0x2,    0x2,
      0x2,   0x17e,  0x17f,  0x5,    0x40,   0x21,   0x2,    0x17f,  0x25,
      0x3,   0x2,    0x2,    0x2,    0x180,  0x182,  0x5,    0x28,   0x15,
      0x2,   0x181,  0x180,  0x3,    0x2,    0x2,    0x2,    0x181,  0x182,
      0x3,   0x2,    0x2,    0x2,    0x182,  0x184,  0x3,    0x2,    0x2,
      0x2,   0x183,  0x185,  0x5,    0x2c,   0x17,   0x2,    0x184,  0x183,
      0x3,   0x2,    0x2,    0x2,    0x184,  0x185,  0x3,    0x2,    0x2,
      0x2,   0x185,  0x187,  0x3,    0x2,    0x2,    0x2,    0x186,  0x188,
      0x5,   0x30,   0x19,   0x2,    0x187,  0x186,  0x3,    0x2,    0x2,
      0x2,   0x187,  0x188,  0x3,    0x2,    0x2,    0x2,    0x188,  0x18a,
      0x3,   0x2,    0x2,    0x2,    0x189,  0x18b,  0x5,    0x34,   0x1b,
      0x2,   0x18a,  0x189,  0x3,    0x2,    0x2,    0x2,    0x18a,  0x18b,
      0x3,   0x2,    0x2,    0x2,    0x18b,  0x27,   0x3,    0x2,    0x2,
      0x2,   0x18c,  0x18e,  0x7,    0x2d,   0x2,    0x2,    0x18d,  0x18f,
      0x5,   0x2a,   0x16,   0x2,    0x18e,  0x18d,  0x3,    0x2,    0x2,
      0x2,   0x18f,  0x190,  0x3,    0x2,    0x2,    0x2,    0x190,  0x18e,
      0x3,   0x2,    0x2,    0x2,    0x190,  0x191,  0x3,    0x2,    0x2,
      0x2,   0x191,  0x29,   0x3,    0x2,    0x2,    0x2,    0x192,  0x19e,
      0x5,   0xce,   0x68,   0x2,    0x193,  0x19e,  0x5,    0x64,   0x33,
      0x2,   0x194,  0x195,  0x7,    0x4,    0x2,    0x2,    0x195,  0x198,
      0x5,   0xb6,   0x5c,   0x2,    0x196,  0x197,  0x7,    0x26,   0x2,
      0x2,   0x197,  0x199,  0x5,    0xb2,   0x5a,   0x2,    0x198,  0x196,
      0x3,   0x2,    0x2,    0x2,    0x198,  0x199,  0x3,    0x2,    0x2,
      0x2,   0x199,  0x19a,  0x3,    0x2,    0x2,    0x2,    0x19a,  0x19b,
      0x7,   0x5,    0x2,    0x2,    0x19b,  0x19e,  0x3,    0x2,    0x2,
      0x2,   0x19c,  0x19e,  0x5,    0xb2,   0x5a,   0x2,    0x19d,  0x192,
      0x3,   0x2,    0x2,    0x2,    0x19d,  0x193,  0x3,    0x2,    0x2,
      0x2,   0x19d,  0x194,  0x3,    0x2,    0x2,    0x2,    0x19d,  0x19c,
      0x3,   0x2,    0x2,    0x2,    0x19e,  0x2b,   0x3,    0x2,    0x2,
      0x2,   0x19f,  0x1a1,  0x7,    0x2f,   0x2,    0x2,    0x1a0,  0x1a2,
      0x5,   0x2e,   0x18,   0x2,    0x1a1,  0x1a0,  0x3,    0x2,    0x2,
      0x2,   0x1a2,  0x1a3,  0x3,    0x2,    0x2,    0x2,    0x1a3,  0x1a1,
      0x3,   0x2,    0x2,    0x2,    0x1a3,  0x1a4,  0x3,    0x2,    0x2,
      0x2,   0x1a4,  0x2d,   0x3,    0x2,    0x2,    0x2,    0x1a5,  0x1a6,
      0x5,   0x62,   0x32,   0x2,    0x1a6,  0x2f,   0x3,    0x2,    0x2,
      0x2,   0x1a7,  0x1a9,  0x7,    0x30,   0x2,    0x2,    0x1a8,  0x1aa,
      0x5,   0x32,   0x1a,   0x2,    0x1a9,  0x1a8,  0x3,    0x2,    0x2,
      0x2,   0x1aa,  0x1ab,  0x3,    0x2,    0x2,    0x2,    0x1ab,  0x1a9,
      0x3,   0x2,    0x2,    0x2,    0x1ab,  0x1ac,  0x3,    0x2,    0x2,
      0x2,   0x1ac,  0x31,   0x3,    0x2,    0x2,    0x2,    0x1ad,  0x1ae,
      0x9,   0x3,    0x2,    0x2,    0x1ae,  0x1b4,  0x5,    0xcc,   0x67,
      0x2,   0x1af,  0x1b2,  0x5,    0x62,   0x32,   0x2,    0x1b0,  0x1b2,
      0x5,   0xb2,   0x5a,   0x2,    0x1b1,  0x1af,  0x3,    0x2,    0x2,
      0x2,   0x1b1,  0x1b0,  0x3,    0x2,    0x2,    0x2,    0x1b2,  0x1b4,
      0x3,   0x2,    0x2,    0x2,    0x1b3,  0x1ad,  0x3,    0x2,    0x2,
      0x2,   0x1b3,  0x1b1,  0x3,    0x2,    0x2,    0x2,    0x1b4,  0x33,
      0x3,   0x2,    0x2,    0x2,    0x1b5,  0x1b7,  0x5,    0x36,   0x1c,
      0x2,   0x1b6,  0x1b8,  0x5,    0x38,   0x1d,   0x2,    0x1b7,  0x1b6,
      0x3,   0x2,    0x2,    0x2,    0x1b7,  0x1b8,  0x3,    0x2,    0x2,
      0x2,   0x1b8,  0x1ba,  0x3,    0x2,    0x2,    0x2,    0x1b9,  0x1bb,
      0x5,   0x3a,   0x1e,   0x2,    0x1ba,  0x1b9,  0x3,    0x2,    0x2,
      0x2,   0x1ba,  0x1bb,  0x3,    0x2,    0x2,    0x2,    0x1bb,  0x1e0,
      0x3,   0x2,    0x2,    0x2,    0x1bc,  0x1be,  0x5,    0x36,   0x1c,
      0x2,   0x1bd,  0x1bf,  0x5,    0x3a,   0x1e,   0x2,    0x1be,  0x1bd,
      0x3,   0x2,    0x2,    0x2,    0x1be,  0x1bf,  0x3,    0x2,    0x2,
      0x2,   0x1bf,  0x1c1,  0x3,    0x2,    0x2,    0x2,    0x1c0,  0x1c2,
      0x5,   0x38,   0x1d,   0x2,    0x1c1,  0x1c0,  0x3,    0x2,    0x2,
      0x2,   0x1c1,  0x1c2,  0x3,    0x2,    0x2,    0x2,    0x1c2,  0x1e0,
      0x3,   0x2,    0x2,    0x2,    0x1c3,  0x1c5,  0x5,    0x38,   0x1d,
      0x2,   0x1c4,  0x1c6,  0x5,    0x36,   0x1c,   0x2,    0x1c5,  0x1c4,
      0x3,   0x2,    0x2,    0x2,    0x1c5,  0x1c6,  0x3,    0x2,    0x2,
      0x2,   0x1c6,  0x1c8,  0x3,    0x2,    0x2,    0x2,    0x1c7,  0x1c9,
      0x5,   0x3a,   0x1e,   0x2,    0x1c8,  0x1c7,  0x3,    0x2,    0x2,
      0x2,   0x1c8,  0x1c9,  0x3,    0x2,    0x2,    0x2,    0x1c9,  0x1e0,
      0x3,   0x2,    0x2,    0x2,    0x1ca,  0x1cc,  0x5,    0x38,   0x1d,
      0x2,   0x1cb,  0x1cd,  0x5,    0x3a,   0x1e,   0x2,    0x1cc,  0x1cb,
      0x3,   0x2,    0x2,    0x2,    0x1cc,  0x1cd,  0x3,    0x2,    0x2,
      0x2,   0x1cd,  0x1cf,  0x3,    0x2,    0x2,    0x2,    0x1ce,  0x1d0,
      0x5,   0x36,   0x1c,   0x2,    0x1cf,  0x1ce,  0x3,    0x2,    0x2,
      0x2,   0x1cf,  0x1d0,  0x3,    0x2,    0x2,    0x2,    0x1d0,  0x1e0,
      0x3,   0x2,    0x2,    0x2,    0x1d1,  0x1d3,  0x5,    0x3a,   0x1e,
      0x2,   0x1d2,  0x1d4,  0x5,    0x38,   0x1d,   0x2,    0x1d3,  0x1d2,
      0x3,   0x2,    0x2,    0x2,    0x1d3,  0x1d4,  0x3,    0x2,    0x2,
      0x2,   0x1d4,  0x1d6,  0x3,    0x2,    0x2,    0x2,    0x1d5,  0x1d7,
      0x5,   0x36,   0x1c,   0x2,    0x1d6,  0x1d5,  0x3,    0x2,    0x2,
      0x2,   0x1d6,  0x1d7,  0x3,    0x2,    0x2,    0x2,    0x1d7,  0x1e0,
      0x3,   0x2,    0x2,    0x2,    0x1d8,  0x1da,  0x5,    0x3a,   0x1e,
      0x2,   0x1d9,  0x1db,  0x5,    0x36,   0x1c,   0x2,    0x1da,  0x1d9,
      0x3,   0x2,    0x2,    0x2,    0x1da,  0x1db,  0x3,    0x2,    0x2,
      0x2,   0x1db,  0x1dd,  0x3,    0x2,    0x2,    0x2,    0x1dc,  0x1de,
      0x5,   0x38,   0x1d,   0x2,    0x1dd,  0x1dc,  0x3,    0x2,    0x2,
      0x2,   0x1dd,  0x1de,  0x3,    0x2,    0x2,    0x2,    0x1de,  0x1e0,
      0x3,   0x2,    0x2,    0x2,    0x1df,  0x1b5,  0x3,    0x2,    0x2,
      0x2,   0x1df,  0x1bc,  0x3,    0x2,    0x2,    0x2,    0x1df,  0x1c3,
      0x3,   0x2,    0x2,    0x2,    0x1df,  0x1ca,  0x3,    0x2,    0x2,
      0x2,   0x1df,  0x1d1,  0x3,    0x2,    0x2,    0x2,    0x1df,  0x1d8,
      0x3,   0x2,    0x2,    0x2,    0x1e0,  0x35,   0x3,    0x2,    0x2,
      0x2,   0x1e1,  0x1e2,  0x7,    0x33,   0x2,    0x2,    0x1e2,  0x1e3,
      0x5,   0x9c,   0x4f,   0x2,    0x1e3,  0x37,   0x3,    0x2,    0x2,
      0x2,   0x1e4,  0x1e5,  0x7,    0x34,   0x2,    0x2,    0x1e5,  0x1e6,
      0x5,   0x9c,   0x4f,   0x2,    0x1e6,  0x39,   0x3,    0x2,    0x2,
      0x2,   0x1e7,  0x1e8,  0x7,    0x35,   0x2,    0x2,    0x1e8,  0x1e9,
      0x5,   0x9c,   0x4f,   0x2,    0x1e9,  0x3b,   0x3,    0x2,    0x2,
      0x2,   0x1ea,  0x1eb,  0x7,    0x36,   0x2,    0x2,    0x1eb,  0x1ed,
      0x5,   0x52,   0x2a,   0x2,    0x1ec,  0x1ea,  0x3,    0x2,    0x2,
      0x2,   0x1ec,  0x1ed,  0x3,    0x2,    0x2,    0x2,    0x1ed,  0x3d,
      0x3,   0x2,    0x2,    0x2,    0x1ee,  0x1f3,  0x5,    0x6e,   0x38,
      0x2,   0x1ef,  0x1f1,  0x7,    0x8,    0x2,    0x2,    0x1f0,  0x1f2,
      0x5,   0x3e,   0x20,   0x2,    0x1f1,  0x1f0,  0x3,    0x2,    0x2,
      0x2,   0x1f1,  0x1f2,  0x3,    0x2,    0x2,    0x2,    0x1f2,  0x1f4,
      0x3,   0x2,    0x2,    0x2,    0x1f3,  0x1ef,  0x3,    0x2,    0x2,
      0x2,   0x1f3,  0x1f4,  0x3,    0x2,    0x2,    0x2,    0x1f4,  0x3f,
      0x3,   0x2,    0x2,    0x2,    0x1f5,  0x1f8,  0x7,    0x6,    0x2,
      0x2,   0x1f6,  0x1f9,  0x5,    0xc,    0x7,    0x2,    0x1f7,  0x1f9,
      0x5,   0x42,   0x22,   0x2,    0x1f8,  0x1f6,  0x3,    0x2,    0x2,
      0x2,   0x1f8,  0x1f7,  0x3,    0x2,    0x2,    0x2,    0x1f9,  0x1fa,
      0x3,   0x2,    0x2,    0x2,    0x1fa,  0x1fb,  0x7,    0x7,    0x2,
      0x2,   0x1fb,  0x41,   0x3,    0x2,    0x2,    0x2,    0x1fc,  0x1fe,
      0x5,   0x44,   0x23,   0x2,    0x1fd,  0x1fc,  0x3,    0x2,    0x2,
      0x2,   0x1fd,  0x1fe,  0x3,    0x2,    0x2,    0x2,    0x1fe,  0x208,
      0x3,   0x2,    0x2,    0x2,    0x1ff,  0x201,  0x5,    0x46,   0x24,
      0x2,   0x200,  0x202,  0x7,    0x8,    0x2,    0x2,    0x201,  0x200,
      0x3,   0x2,    0x2,    0x2,    0x201,  0x202,  0x3,    0x2,    0x2,
      0x2,   0x202,  0x204,  0x3,    0x2,    0x2,    0x2,    0x203,  0x205,
      0x5,   0x44,   0x23,   0x2,    0x204,  0x203,  0x3,    0x2,    0x2,
      0x2,   0x204,  0x205,  0x3,    0x2,    0x2,    0x2,    0x205,  0x207,
      0x3,   0x2,    0x2,    0x2,    0x206,  0x1ff,  0x3,    0x2,    0x2,
      0x2,   0x207,  0x20a,  0x3,    0x2,    0x2,    0x2,    0x208,  0x206,
      0x3,   0x2,    0x2,    0x2,    0x208,  0x209,  0x3,    0x2,    0x2,
      0x2,   0x209,  0x43,   0x3,    0x2,    0x2,    0x2,    0x20a,  0x208,
      0x3,   0x2,    0x2,    0x2,    0x20b,  0x210,  0x5,    0x7a,   0x3e,
      0x2,   0x20c,  0x20e,  0x7,    0x8,    0x2,    0x2,    0x20d,  0x20f,
      0x5,   0x44,   0x23,   0x2,    0x20e,  0x20d,  0x3,    0x2,    0x2,
      0x2,   0x20e,  0x20f,  0x3,    0x2,    0x2,    0x2,    0x20f,  0x211,
      0x3,   0x2,    0x2,    0x2,    0x210,  0x20c,  0x3,    0x2,    0x2,
      0x2,   0x210,  0x211,  0x3,    0x2,    0x2,    0x2,    0x211,  0x45,
      0x3,   0x2,    0x2,    0x2,    0x212,  0x21b,  0x5,    0x5e,   0x30,
      0x2,   0x213,  0x21b,  0x5,    0x48,   0x25,   0x2,    0x214,  0x21b,
      0x5,   0x5c,   0x2f,   0x2,    0x215,  0x21b,  0x5,    0x4a,   0x26,
      0x2,   0x216,  0x21b,  0x5,    0x4c,   0x27,   0x2,    0x217,  0x21b,
      0x5,   0x60,   0x31,   0x2,    0x218,  0x21b,  0x5,    0x4e,   0x28,
      0x2,   0x219,  0x21b,  0x5,    0x50,   0x29,   0x2,    0x21a,  0x212,
      0x3,   0x2,    0x2,    0x2,    0x21a,  0x213,  0x3,    0x2,    0x2,
      0x2,   0x21a,  0x214,  0x3,    0x2,    0x2,    0x2,    0x21a,  0x215,
      0x3,   0x2,    0x2,    0x2,    0x21a,  0x216,  0x3,    0x2,    0x2,
      0x2,   0x21a,  0x217,  0x3,    0x2,    0x2,    0x2,    0x21a,  0x218,
      0x3,   0x2,    0x2,    0x2,    0x21a,  0x219,  0x3,    0x2,    0x2,
      0x2,   0x21b,  0x47,   0x3,    0x2,    0x2,    0x2,    0x21c,  0x21d,
      0x7,   0x47,   0x2,    0x2,    0x21d,  0x21e,  0x5,    0x40,   0x21,
      0x2,   0x21e,  0x49,   0x3,    0x2,    0x2,    0x2,    0x21f,  0x220,
      0x7,   0x45,   0x2,    0x2,    0x220,  0x221,  0x5,    0xb0,   0x59,
      0x2,   0x221,  0x222,  0x5,    0x40,   0x21,   0x2,    0x222,  0x4b,
      0x3,   0x2,    0x2,    0x2,    0x223,  0x225,  0x7,    0x48,   0x2,
      0x2,   0x224,  0x226,  0x7,    0x38,   0x2,    0x2,    0x225,  0x224,
      0x3,   0x2,    0x2,    0x2,    0x225,  0x226,  0x3,    0x2,    0x2,
      0x2,   0x226,  0x227,  0x3,    0x2,    0x2,    0x2,    0x227,  0x228,
      0x5,   0xb0,   0x59,   0x2,    0x228,  0x229,  0x5,    0x40,   0x21,
      0x2,   0x229,  0x4d,   0x3,    0x2,    0x2,    0x2,    0x22a,  0x22b,
      0x7,   0x49,   0x2,    0x2,    0x22b,  0x22c,  0x7,    0x4,    0x2,
      0x2,   0x22c,  0x22d,  0x5,    0xb6,   0x5c,   0x2,    0x22d,  0x22e,
      0x7,   0x26,   0x2,    0x2,    0x22e,  0x22f,  0x5,    0xb2,   0x5a,
      0x2,   0x22f,  0x230,  0x7,    0x5,    0x2,    0x2,    0x230,  0x4f,
      0x3,   0x2,    0x2,    0x2,    0x231,  0x232,  0x7,    0x36,   0x2,
      0x2,   0x232,  0x233,  0x5,    0x52,   0x2a,   0x2,    0x233,  0x51,
      0x3,   0x2,    0x2,    0x2,    0x234,  0x237,  0x5,    0x54,   0x2b,
      0x2,   0x235,  0x237,  0x5,    0x56,   0x2c,   0x2,    0x236,  0x234,
      0x3,   0x2,    0x2,    0x2,    0x236,  0x235,  0x3,    0x2,    0x2,
      0x2,   0x237,  0x53,   0x3,    0x2,    0x2,    0x2,    0x238,  0x239,
      0x5,   0xb2,   0x5a,   0x2,    0x239,  0x23d,  0x7,    0x6,    0x2,
      0x2,   0x23a,  0x23c,  0x5,    0x5a,   0x2e,   0x2,    0x23b,  0x23a,
      0x3,   0x2,    0x2,    0x2,    0x23c,  0x23f,  0x3,    0x2,    0x2,
      0x2,   0x23d,  0x23b,  0x3,    0x2,    0x2,    0x2,    0x23d,  0x23e,
      0x3,   0x2,    0x2,    0x2,    0x23e,  0x240,  0x3,    0x2,    0x2,
      0x2,   0x23f,  0x23d,  0x3,    0x2,    0x2,    0x2,    0x240,  0x241,
      0x7,   0x7,    0x2,    0x2,    0x241,  0x55,   0x3,    0x2,    0x2,
      0x2,   0x242,  0x24c,  0x7,    0xa3,   0x2,    0x2,    0x243,  0x247,
      0x7,   0x4,    0x2,    0x2,    0x244,  0x246,  0x5,    0xb2,   0x5a,
      0x2,   0x245,  0x244,  0x3,    0x2,    0x2,    0x2,    0x246,  0x249,
      0x3,   0x2,    0x2,    0x2,    0x247,  0x245,  0x3,    0x2,    0x2,
      0x2,   0x247,  0x248,  0x3,    0x2,    0x2,    0x2,    0x248,  0x24a,
      0x3,   0x2,    0x2,    0x2,    0x249,  0x247,  0x3,    0x2,    0x2,
      0x2,   0x24a,  0x24c,  0x7,    0x5,    0x2,    0x2,    0x24b,  0x242,
      0x3,   0x2,    0x2,    0x2,    0x24b,  0x243,  0x3,    0x2,    0x2,
      0x2,   0x24c,  0x24d,  0x3,    0x2,    0x2,    0x2,    0x24d,  0x251,
      0x7,   0x6,    0x2,    0x2,    0x24e,  0x250,  0x5,    0x58,   0x2d,
      0x2,   0x24f,  0x24e,  0x3,    0x2,    0x2,    0x2,    0x250,  0x253,
      0x3,   0x2,    0x2,    0x2,    0x251,  0x24f,  0x3,    0x2,    0x2,
      0x2,   0x251,  0x252,  0x3,    0x2,    0x2,    0x2,    0x252,  0x254,
      0x3,   0x2,    0x2,    0x2,    0x253,  0x251,  0x3,    0x2,    0x2,
      0x2,   0x254,  0x255,  0x7,    0x7,    0x2,    0x2,    0x255,  0x57,
      0x3,   0x2,    0x2,    0x2,    0x256,  0x25a,  0x7,    0x4,    0x2,
      0x2,   0x257,  0x259,  0x5,    0x5a,   0x2e,   0x2,    0x258,  0x257,
      0x3,   0x2,    0x2,    0x2,    0x259,  0x25c,  0x3,    0x2,    0x2,
      0x2,   0x25a,  0x258,  0x3,    0x2,    0x2,    0x2,    0x25a,  0x25b,
      0x3,   0x2,    0x2,    0x2,    0x25b,  0x25d,  0x3,    0x2,    0x2,
      0x2,   0x25c,  0x25a,  0x3,    0x2,    0x2,    0x2,    0x25d,  0x260,
      0x7,   0x5,    0x2,    0x2,    0x25e,  0x260,  0x7,    0xa3,   0x2,
      0x2,   0x25f,  0x256,  0x3,    0x2,    0x2,    0x2,    0x25f,  0x25e,
      0x3,   0x2,    0x2,    0x2,    0x260,  0x59,   0x3,    0x2,    0x2,
      0x2,   0x261,  0x267,  0x5,    0xec,   0x77,   0x2,    0x262,  0x267,
      0x5,   0xde,   0x70,   0x2,    0x263,  0x267,  0x5,    0xe0,   0x71,
      0x2,   0x264,  0x267,  0x5,    0xe8,   0x75,   0x2,    0x265,  0x267,
      0x7,   0x4a,   0x2,    0x2,    0x266,  0x261,  0x3,    0x2,    0x2,
      0x2,   0x266,  0x262,  0x3,    0x2,    0x2,    0x2,    0x266,  0x263,
      0x3,   0x2,    0x2,    0x2,    0x266,  0x264,  0x3,    0x2,    0x2,
      0x2,   0x266,  0x265,  0x3,    0x2,    0x2,    0x2,    0x267,  0x5b,
      0x3,   0x2,    0x2,    0x2,    0x268,  0x269,  0x7,    0x4b,   0x2,
      0x2,   0x269,  0x26a,  0x5,    0x40,   0x21,   0x2,    0x26a,  0x5d,
      0x3,   0x2,    0x2,    0x2,    0x26b,  0x270,  0x5,    0x40,   0x21,
      0x2,   0x26c,  0x26d,  0x7,    0x4c,   0x2,    0x2,    0x26d,  0x26f,
      0x5,   0x40,   0x21,   0x2,    0x26e,  0x26c,  0x3,    0x2,    0x2,
      0x2,   0x26f,  0x272,  0x3,    0x2,    0x2,    0x2,    0x270,  0x26e,
      0x3,   0x2,    0x2,    0x2,    0x270,  0x271,  0x3,    0x2,    0x2,
      0x2,   0x271,  0x5f,   0x3,    0x2,    0x2,    0x2,    0x272,  0x270,
      0x3,   0x2,    0x2,    0x2,    0x273,  0x274,  0x7,    0x4d,   0x2,
      0x2,   0x274,  0x275,  0x5,    0x62,   0x32,   0x2,    0x275,  0x61,
      0x3,   0x2,    0x2,    0x2,    0x276,  0x27a,  0x5,    0xcc,   0x67,
      0x2,   0x277,  0x27a,  0x5,    0xce,   0x68,   0x2,    0x278,  0x27a,
      0x5,   0x64,   0x33,   0x2,    0x279,  0x276,  0x3,    0x2,    0x2,
      0x2,   0x279,  0x277,  0x3,    0x2,    0x2,    0x2,    0x279,  0x278,
      0x3,   0x2,    0x2,    0x2,    0x27a,  0x63,   0x3,    0x2,    0x2,
      0x2,   0x27b,  0x27c,  0x5,    0xec,   0x77,   0x2,    0x27c,  0x27d,
      0x5,   0x66,   0x34,   0x2,    0x27d,  0x65,   0x3,    0x2,    0x2,
      0x2,   0x27e,  0x28e,  0x7,    0xa3,   0x2,    0x2,    0x27f,  0x281,
      0x7,   0x4,    0x2,    0x2,    0x280,  0x282,  0x7,    0x24,   0x2,
      0x2,   0x281,  0x280,  0x3,    0x2,    0x2,    0x2,    0x281,  0x282,
      0x3,   0x2,    0x2,    0x2,    0x282,  0x283,  0x3,    0x2,    0x2,
      0x2,   0x283,  0x288,  0x5,    0xb6,   0x5c,   0x2,    0x284,  0x285,
      0x7,   0x9,    0x2,    0x2,    0x285,  0x287,  0x5,    0xb6,   0x5c,
      0x2,   0x286,  0x284,  0x3,    0x2,    0x2,    0x2,    0x287,  0x28a,
      0x3,   0x2,    0x2,    0x2,    0x288,  0x286,  0x3,    0x2,    0x2,
      0x2,   0x288,  0x289,  0x3,    0x2,    0x2,    0x2,    0x289,  0x28b,
      0x3,   0x2,    0x2,    0x2,    0x28a,  0x288,  0x3,    0x2,    0x2,
      0x2,   0x28b,  0x28c,  0x7,    0x5,    0x2,    0x2,    0x28c,  0x28e,
      0x3,   0x2,    0x2,    0x2,    0x28d,  0x27e,  0x3,    0x2,    0x2,
      0x2,   0x28d,  0x27f,  0x3,    0x2,    0x2,    0x2,    0x28e,  0x67,
      0x3,   0x2,    0x2,    0x2,    0x28f,  0x29c,  0x7,    0xa3,   0x2,
      0x2,   0x290,  0x291,  0x7,    0x4,    0x2,    0x2,    0x291,  0x296,
      0x5,   0xb6,   0x5c,   0x2,    0x292,  0x293,  0x7,    0x9,    0x2,
      0x2,   0x293,  0x295,  0x5,    0xb6,   0x5c,   0x2,    0x294,  0x292,
      0x3,   0x2,    0x2,    0x2,    0x295,  0x298,  0x3,    0x2,    0x2,
      0x2,   0x296,  0x294,  0x3,    0x2,    0x2,    0x2,    0x296,  0x297,
      0x3,   0x2,    0x2,    0x2,    0x297,  0x299,  0x3,    0x2,    0x2,
      0x2,   0x298,  0x296,  0x3,    0x2,    0x2,    0x2,    0x299,  0x29a,
      0x7,   0x5,    0x2,    0x2,    0x29a,  0x29c,  0x3,    0x2,    0x2,
      0x2,   0x29b,  0x28f,  0x3,    0x2,    0x2,    0x2,    0x29b,  0x290,
      0x3,   0x2,    0x2,    0x2,    0x29c,  0x69,   0x3,    0x2,    0x2,
      0x2,   0x29d,  0x29f,  0x7,    0x6,    0x2,    0x2,    0x29e,  0x2a0,
      0x5,   0x6c,   0x37,   0x2,    0x29f,  0x29e,  0x3,    0x2,    0x2,
      0x2,   0x29f,  0x2a0,  0x3,    0x2,    0x2,    0x2,    0x2a0,  0x2a1,
      0x3,   0x2,    0x2,    0x2,    0x2a1,  0x2a2,  0x7,    0x7,    0x2,
      0x2,   0x2a2,  0x6b,   0x3,    0x2,    0x2,    0x2,    0x2a3,  0x2a8,
      0x5,   0x6e,   0x38,   0x2,    0x2a4,  0x2a6,  0x7,    0x8,    0x2,
      0x2,   0x2a5,  0x2a7,  0x5,    0x6c,   0x37,   0x2,    0x2a6,  0x2a5,
      0x3,   0x2,    0x2,    0x2,    0x2a6,  0x2a7,  0x3,    0x2,    0x2,
      0x2,   0x2a7,  0x2a9,  0x3,    0x2,    0x2,    0x2,    0x2a8,  0x2a4,
      0x3,   0x2,    0x2,    0x2,    0x2a8,  0x2a9,  0x3,    0x2,    0x2,
      0x2,   0x2a9,  0x6d,   0x3,    0x2,    0x2,    0x2,    0x2aa,  0x2ab,
      0x5,   0xae,   0x58,   0x2,    0x2ab,  0x2ac,  0x5,    0x72,   0x3a,
      0x2,   0x2ac,  0x2b1,  0x3,    0x2,    0x2,    0x2,    0x2ad,  0x2ae,
      0x5,   0x9e,   0x50,   0x2,    0x2ae,  0x2af,  0x5,    0x70,   0x39,
      0x2,   0x2af,  0x2b1,  0x3,    0x2,    0x2,    0x2,    0x2b0,  0x2aa,
      0x3,   0x2,    0x2,    0x2,    0x2b0,  0x2ad,  0x3,    0x2,    0x2,
      0x2,   0x2b1,  0x6f,   0x3,    0x2,    0x2,    0x2,    0x2b2,  0x2b4,
      0x5,   0x72,   0x3a,   0x2,    0x2b3,  0x2b2,  0x3,    0x2,    0x2,
      0x2,   0x2b3,  0x2b4,  0x3,    0x2,    0x2,    0x2,    0x2b4,  0x71,
      0x3,   0x2,    0x2,    0x2,    0x2b5,  0x2b6,  0x5,    0x74,   0x3b,
      0x2,   0x2b6,  0x2bf,  0x5,    0x76,   0x3c,   0x2,    0x2b7,  0x2bb,
      0x7,   0xa,    0x2,    0x2,    0x2b8,  0x2b9,  0x5,    0x74,   0x3b,
      0x2,   0x2b9,  0x2ba,  0x5,    0x76,   0x3c,   0x2,    0x2ba,  0x2bc,
      0x3,   0x2,    0x2,    0x2,    0x2bb,  0x2b8,  0x3,    0x2,    0x2,
      0x2,   0x2bb,  0x2bc,  0x3,    0x2,    0x2,    0x2,    0x2bc,  0x2be,
      0x3,   0x2,    0x2,    0x2,    0x2bd,  0x2b7,  0x3,    0x2,    0x2,
      0x2,   0x2be,  0x2c1,  0x3,    0x2,    0x2,    0x2,    0x2bf,  0x2bd,
      0x3,   0x2,    0x2,    0x2,    0x2bf,  0x2c0,  0x3,    0x2,    0x2,
      0x2,   0x2c0,  0x73,   0x3,    0x2,    0x2,    0x2,    0x2c1,  0x2bf,
      0x3,   0x2,    0x2,    0x2,    0x2c2,  0x2c5,  0x5,    0xb0,   0x59,
      0x2,   0x2c3,  0x2c5,  0x7,    0xb,    0x2,    0x2,    0x2c4,  0x2c2,
      0x3,   0x2,    0x2,    0x2,    0x2c4,  0x2c3,  0x3,    0x2,    0x2,
      0x2,   0x2c5,  0x75,   0x3,    0x2,    0x2,    0x2,    0x2c6,  0x2cb,
      0x5,   0x78,   0x3d,   0x2,    0x2c7,  0x2c8,  0x7,    0x9,    0x2,
      0x2,   0x2c8,  0x2ca,  0x5,    0x78,   0x3d,   0x2,    0x2c9,  0x2c7,
      0x3,   0x2,    0x2,    0x2,    0x2ca,  0x2cd,  0x3,    0x2,    0x2,
      0x2,   0x2cb,  0x2c9,  0x3,    0x2,    0x2,    0x2,    0x2cb,  0x2cc,
      0x3,   0x2,    0x2,    0x2,    0x2cc,  0x77,   0x3,    0x2,    0x2,
      0x2,   0x2cd,  0x2cb,  0x3,    0x2,    0x2,    0x2,    0x2ce,  0x2cf,
      0x5,   0xaa,   0x56,   0x2,    0x2cf,  0x79,   0x3,    0x2,    0x2,
      0x2,   0x2d0,  0x2d1,  0x5,    0xae,   0x58,   0x2,    0x2d1,  0x2d2,
      0x5,   0x7e,   0x40,   0x2,    0x2d2,  0x2d7,  0x3,    0x2,    0x2,
      0x2,   0x2d3,  0x2d4,  0x5,    0xa2,   0x52,   0x2,    0x2d4,  0x2d5,
      0x5,   0x7c,   0x3f,   0x2,    0x2d5,  0x2d7,  0x3,    0x2,    0x2,
      0x2,   0x2d6,  0x2d0,  0x3,    0x2,    0x2,    0x2,    0x2d6,  0x2d3,
      0x3,   0x2,    0x2,    0x2,    0x2d7,  0x7b,   0x3,    0x2,    0x2,
      0x2,   0x2d8,  0x2da,  0x5,    0x7e,   0x40,   0x2,    0x2d9,  0x2d8,
      0x3,   0x2,    0x2,    0x2,    0x2d9,  0x2da,  0x3,    0x2,    0x2,
      0x2,   0x2da,  0x7d,   0x3,    0x2,    0x2,    0x2,    0x2db,  0x2dc,
      0x5,   0x84,   0x43,   0x2,    0x2dc,  0x2e5,  0x5,    0x86,   0x44,
      0x2,   0x2dd,  0x2e1,  0x7,    0xa,    0x2,    0x2,    0x2de,  0x2df,
      0x5,   0x84,   0x43,   0x2,    0x2df,  0x2e0,  0x5,    0x76,   0x3c,
      0x2,   0x2e0,  0x2e2,  0x3,    0x2,    0x2,    0x2,    0x2e1,  0x2de,
      0x3,   0x2,    0x2,    0x2,    0x2e1,  0x2e2,  0x3,    0x2,    0x2,
      0x2,   0x2e2,  0x2e4,  0x3,    0x2,    0x2,    0x2,    0x2e3,  0x2dd,
      0x3,   0x2,    0x2,    0x2,    0x2e4,  0x2e7,  0x3,    0x2,    0x2,
      0x2,   0x2e5,  0x2e3,  0x3,    0x2,    0x2,    0x2,    0x2e5,  0x2e6,
      0x3,   0x2,    0x2,    0x2,    0x2e6,  0x7f,   0x3,    0x2,    0x2,
      0x2,   0x2e7,  0x2e5,  0x3,    0x2,    0x2,    0x2,    0x2e8,  0x2e9,
      0x5,   0x8a,   0x46,   0x2,    0x2e9,  0x81,   0x3,    0x2,    0x2,
      0x2,   0x2ea,  0x2eb,  0x5,    0xb2,   0x5a,   0x2,    0x2eb,  0x83,
      0x3,   0x2,    0x2,    0x2,    0x2ec,  0x2ef,  0x5,    0x80,   0x41,
      0x2,   0x2ed,  0x2ef,  0x5,    0x82,   0x42,   0x2,    0x2ee,  0x2ec,
      0x3,   0x2,    0x2,    0x2,    0x2ee,  0x2ed,  0x3,    0x2,    0x2,
      0x2,   0x2ef,  0x85,   0x3,    0x2,    0x2,    0x2,    0x2f0,  0x2f5,
      0x5,   0x88,   0x45,   0x2,    0x2f1,  0x2f2,  0x7,    0x9,    0x2,
      0x2,   0x2f2,  0x2f4,  0x5,    0x88,   0x45,   0x2,    0x2f3,  0x2f1,
      0x3,   0x2,    0x2,    0x2,    0x2f4,  0x2f7,  0x3,    0x2,    0x2,
      0x2,   0x2f5,  0x2f3,  0x3,    0x2,    0x2,    0x2,    0x2f5,  0x2f6,
      0x3,   0x2,    0x2,    0x2,    0x2f6,  0x87,   0x3,    0x2,    0x2,
      0x2,   0x2f7,  0x2f5,  0x3,    0x2,    0x2,    0x2,    0x2f8,  0x2f9,
      0x5,   0xac,   0x57,   0x2,    0x2f9,  0x89,   0x3,    0x2,    0x2,
      0x2,   0x2fa,  0x2fb,  0x5,    0x8c,   0x47,   0x2,    0x2fb,  0x8b,
      0x3,   0x2,    0x2,    0x2,    0x2fc,  0x301,  0x5,    0x8e,   0x48,
      0x2,   0x2fd,  0x2fe,  0x7,    0xc,    0x2,    0x2,    0x2fe,  0x300,
      0x5,   0x8e,   0x48,   0x2,    0x2ff,  0x2fd,  0x3,    0x2,    0x2,
      0x2,   0x300,  0x303,  0x3,    0x2,    0x2,    0x2,    0x301,  0x2ff,
      0x3,   0x2,    0x2,    0x2,    0x301,  0x302,  0x3,    0x2,    0x2,
      0x2,   0x302,  0x8d,   0x3,    0x2,    0x2,    0x2,    0x303,  0x301,
      0x3,   0x2,    0x2,    0x2,    0x304,  0x309,  0x5,    0x92,   0x4a,
      0x2,   0x305,  0x306,  0x7,    0xd,    0x2,    0x2,    0x306,  0x308,
      0x5,   0x92,   0x4a,   0x2,    0x307,  0x305,  0x3,    0x2,    0x2,
      0x2,   0x308,  0x30b,  0x3,    0x2,    0x2,    0x2,    0x309,  0x307,
      0x3,   0x2,    0x2,    0x2,    0x309,  0x30a,  0x3,    0x2,    0x2,
      0x2,   0x30a,  0x8f,   0x3,    0x2,    0x2,    0x2,    0x30b,  0x309,
      0x3,   0x2,    0x2,    0x2,    0x30c,  0x30e,  0x5,    0x96,   0x4c,
      0x2,   0x30d,  0x30f,  0x5,    0x94,   0x4b,   0x2,    0x30e,  0x30d,
      0x3,   0x2,    0x2,    0x2,    0x30e,  0x30f,  0x3,    0x2,    0x2,
      0x2,   0x30f,  0x91,   0x3,    0x2,    0x2,    0x2,    0x310,  0x314,
      0x5,   0x90,   0x49,   0x2,    0x311,  0x312,  0x7,    0xe,    0x2,
      0x2,   0x312,  0x314,  0x5,    0x90,   0x49,   0x2,    0x313,  0x310,
      0x3,   0x2,    0x2,    0x2,    0x313,  0x311,  0x3,    0x2,    0x2,
      0x2,   0x314,  0x93,   0x3,    0x2,    0x2,    0x2,    0x315,  0x316,
      0x9,   0x4,    0x2,    0x2,    0x316,  0x95,   0x3,    0x2,    0x2,
      0x2,   0x317,  0x320,  0x5,    0xec,   0x77,   0x2,    0x318,  0x320,
      0x7,   0xb,    0x2,    0x2,    0x319,  0x31a,  0x7,    0x11,   0x2,
      0x2,   0x31a,  0x320,  0x5,    0x98,   0x4d,   0x2,    0x31b,  0x31c,
      0x7,   0x4,    0x2,    0x2,    0x31c,  0x31d,  0x5,    0x8a,   0x46,
      0x2,   0x31d,  0x31e,  0x7,    0x5,    0x2,    0x2,    0x31e,  0x320,
      0x3,   0x2,    0x2,    0x2,    0x31f,  0x317,  0x3,    0x2,    0x2,
      0x2,   0x31f,  0x318,  0x3,    0x2,    0x2,    0x2,    0x31f,  0x319,
      0x3,   0x2,    0x2,    0x2,    0x31f,  0x31b,  0x3,    0x2,    0x2,
      0x2,   0x320,  0x97,   0x3,    0x2,    0x2,    0x2,    0x321,  0x32f,
      0x5,   0x9a,   0x4e,   0x2,    0x322,  0x32b,  0x7,    0x4,    0x2,
      0x2,   0x323,  0x328,  0x5,    0x9a,   0x4e,   0x2,    0x324,  0x325,
      0x7,   0xc,    0x2,    0x2,    0x325,  0x327,  0x5,    0x9a,   0x4e,
      0x2,   0x326,  0x324,  0x3,    0x2,    0x2,    0x2,    0x327,  0x32a,
      0x3,   0x2,    0x2,    0x2,    0x328,  0x326,  0x3,    0x2,    0x2,
      0x2,   0x328,  0x329,  0x3,    0x2,    0x2,    0x2,    0x329,  0x32c,
      0x3,   0x2,    0x2,    0x2,    0x32a,  0x328,  0x3,    0x2,    0x2,
      0x2,   0x32b,  0x323,  0x3,    0x2,    0x2,    0x2,    0x32b,  0x32c,
      0x3,   0x2,    0x2,    0x2,    0x32c,  0x32d,  0x3,    0x2,    0x2,
      0x2,   0x32d,  0x32f,  0x7,    0x5,    0x2,    0x2,    0x32e,  0x321,
      0x3,   0x2,    0x2,    0x2,    0x32e,  0x322,  0x3,    0x2,    0x2,
      0x2,   0x32f,  0x99,   0x3,    0x2,    0x2,    0x2,    0x330,  0x338,
      0x5,   0xec,   0x77,   0x2,    0x331,  0x338,  0x7,    0xb,    0x2,
      0x2,   0x332,  0x335,  0x7,    0xe,    0x2,    0x2,    0x333,  0x336,
      0x5,   0xec,   0x77,   0x2,    0x334,  0x336,  0x7,    0xb,    0x2,
      0x2,   0x335,  0x333,  0x3,    0x2,    0x2,    0x2,    0x335,  0x334,
      0x3,   0x2,    0x2,    0x2,    0x336,  0x338,  0x3,    0x2,    0x2,
      0x2,   0x337,  0x330,  0x3,    0x2,    0x2,    0x2,    0x337,  0x331,
      0x3,   0x2,    0x2,    0x2,    0x337,  0x332,  0x3,    0x2,    0x2,
      0x2,   0x338,  0x9b,   0x3,    0x2,    0x2,    0x2,    0x339,  0x33a,
      0x7,   0x94,   0x2,    0x2,    0x33a,  0x9d,   0x3,    0x2,    0x2,
      0x2,   0x33b,  0x33e,  0x5,    0xa6,   0x54,   0x2,    0x33c,  0x33e,
      0x5,   0xa0,   0x51,   0x2,    0x33d,  0x33b,  0x3,    0x2,    0x2,
      0x2,   0x33d,  0x33c,  0x3,    0x2,    0x2,    0x2,    0x33e,  0x9f,
      0x3,   0x2,    0x2,    0x2,    0x33f,  0x340,  0x7,    0x12,   0x2,
      0x2,   0x340,  0x341,  0x5,    0x72,   0x3a,   0x2,    0x341,  0x342,
      0x7,   0x13,   0x2,    0x2,    0x342,  0xa1,   0x3,    0x2,    0x2,
      0x2,   0x343,  0x346,  0x5,    0xa8,   0x55,   0x2,    0x344,  0x346,
      0x5,   0xa4,   0x53,   0x2,    0x345,  0x343,  0x3,    0x2,    0x2,
      0x2,   0x345,  0x344,  0x3,    0x2,    0x2,    0x2,    0x346,  0xa3,
      0x3,   0x2,    0x2,    0x2,    0x347,  0x348,  0x7,    0x12,   0x2,
      0x2,   0x348,  0x349,  0x5,    0x7e,   0x40,   0x2,    0x349,  0x34a,
      0x7,   0x13,   0x2,    0x2,    0x34a,  0xa5,   0x3,    0x2,    0x2,
      0x2,   0x34b,  0x34d,  0x7,    0x4,    0x2,    0x2,    0x34c,  0x34e,
      0x5,   0xaa,   0x56,   0x2,    0x34d,  0x34c,  0x3,    0x2,    0x2,
      0x2,   0x34e,  0x34f,  0x3,    0x2,    0x2,    0x2,    0x34f,  0x34d,
      0x3,   0x2,    0x2,    0x2,    0x34f,  0x350,  0x3,    0x2,    0x2,
      0x2,   0x350,  0x351,  0x3,    0x2,    0x2,    0x2,    0x351,  0x352,
      0x7,   0x5,    0x2,    0x2,    0x352,  0xa7,   0x3,    0x2,    0x2,
      0x2,   0x353,  0x355,  0x7,    0x4,    0x2,    0x2,    0x354,  0x356,
      0x5,   0xac,   0x57,   0x2,    0x355,  0x354,  0x3,    0x2,    0x2,
      0x2,   0x356,  0x357,  0x3,    0x2,    0x2,    0x2,    0x357,  0x355,
      0x3,   0x2,    0x2,    0x2,    0x357,  0x358,  0x3,    0x2,    0x2,
      0x2,   0x358,  0x359,  0x3,    0x2,    0x2,    0x2,    0x359,  0x35a,
      0x7,   0x5,    0x2,    0x2,    0x35a,  0xa9,   0x3,    0x2,    0x2,
      0x2,   0x35b,  0x35e,  0x5,    0xae,   0x58,   0x2,    0x35c,  0x35e,
      0x5,   0x9e,   0x50,   0x2,    0x35d,  0x35b,  0x3,    0x2,    0x2,
      0x2,   0x35d,  0x35c,  0x3,    0x2,    0x2,    0x2,    0x35e,  0xab,
      0x3,   0x2,    0x2,    0x2,    0x35f,  0x362,  0x5,    0xae,   0x58,
      0x2,   0x360,  0x362,  0x5,    0xa2,   0x52,   0x2,    0x361,  0x35f,
      0x3,   0x2,    0x2,    0x2,    0x361,  0x360,  0x3,    0x2,    0x2,
      0x2,   0x362,  0xad,   0x3,    0x2,    0x2,    0x2,    0x363,  0x366,
      0x5,   0xb2,   0x5a,   0x2,    0x364,  0x366,  0x5,    0xb4,   0x5b,
      0x2,   0x365,  0x363,  0x3,    0x2,    0x2,    0x2,    0x365,  0x364,
      0x3,   0x2,    0x2,    0x2,    0x366,  0xaf,   0x3,    0x2,    0x2,
      0x2,   0x367,  0x36a,  0x5,    0xb2,   0x5a,   0x2,    0x368,  0x36a,
      0x5,   0xec,   0x77,   0x2,    0x369,  0x367,  0x3,    0x2,    0x2,
      0x2,   0x369,  0x368,  0x3,    0x2,    0x2,    0x2,    0x36a,  0xb1,
      0x3,   0x2,    0x2,    0x2,    0x36b,  0x36c,  0x9,    0x5,    0x2,
      0x2,   0x36c,  0xb3,   0x3,    0x2,    0x2,    0x2,    0x36d,  0x374,
      0x5,   0xec,   0x77,   0x2,    0x36e,  0x374,  0x5,    0xde,   0x70,
      0x2,   0x36f,  0x374,  0x5,    0xe0,   0x71,   0x2,    0x370,  0x374,
      0x5,   0xe8,   0x75,   0x2,    0x371,  0x374,  0x5,    0xf0,   0x79,
      0x2,   0x372,  0x374,  0x7,    0xa3,   0x2,    0x2,    0x373,  0x36d,
      0x3,   0x2,    0x2,    0x2,    0x373,  0x36e,  0x3,    0x2,    0x2,
      0x2,   0x373,  0x36f,  0x3,    0x2,    0x2,    0x2,    0x373,  0x370,
      0x3,   0x2,    0x2,    0x2,    0x373,  0x371,  0x3,    0x2,    0x2,
      0x2,   0x373,  0x372,  0x3,    0x2,    0x2,    0x2,    0x374,  0xb5,
      0x3,   0x2,    0x2,    0x2,    0x375,  0x376,  0x5,    0xb8,   0x5d,
      0x2,   0x376,  0xb7,   0x3,    0x2,    0x2,    0x2,    0x377,  0x37c,
      0x5,   0xba,   0x5e,   0x2,    0x378,  0x379,  0x7,    0x14,   0x2,
      0x2,   0x379,  0x37b,  0x5,    0xba,   0x5e,   0x2,    0x37a,  0x378,
      0x3,   0x2,    0x2,    0x2,    0x37b,  0x37e,  0x3,    0x2,    0x2,
      0x2,   0x37c,  0x37a,  0x3,    0x2,    0x2,    0x2,    0x37c,  0x37d,
      0x3,   0x2,    0x2,    0x2,    0x37d,  0xb9,   0x3,    0x2,    0x2,
      0x2,   0x37e,  0x37c,  0x3,    0x2,    0x2,    0x2,    0x37f,  0x384,
      0x5,   0xbc,   0x5f,   0x2,    0x380,  0x381,  0x7,    0x15,   0x2,
      0x2,   0x381,  0x383,  0x5,    0xbc,   0x5f,   0x2,    0x382,  0x380,
      0x3,   0x2,    0x2,    0x2,    0x383,  0x386,  0x3,    0x2,    0x2,
      0x2,   0x384,  0x382,  0x3,    0x2,    0x2,    0x2,    0x384,  0x385,
      0x3,   0x2,    0x2,    0x2,    0x385,  0xbb,   0x3,    0x2,    0x2,
      0x2,   0x386,  0x384,  0x3,    0x2,    0x2,    0x2,    0x387,  0x388,
      0x5,   0xbe,   0x60,   0x2,    0x388,  0xbd,   0x3,    0x2,    0x2,
      0x2,   0x389,  0x39a,  0x5,    0xc0,   0x61,   0x2,    0x38a,  0x38b,
      0x7,   0x16,   0x2,    0x2,    0x38b,  0x39b,  0x5,    0xc0,   0x61,
      0x2,   0x38c,  0x38d,  0x7,    0x17,   0x2,    0x2,    0x38d,  0x39b,
      0x5,   0xc0,   0x61,   0x2,    0x38e,  0x39b,  0x5,    0xc0,   0x61,
      0x2,   0x38f,  0x390,  0x7,    0x18,   0x2,    0x2,    0x390,  0x39b,
      0x5,   0xc0,   0x61,   0x2,    0x391,  0x392,  0x7,    0x19,   0x2,
      0x2,   0x392,  0x39b,  0x5,    0xc0,   0x61,   0x2,    0x393,  0x394,
      0x7,   0x1a,   0x2,    0x2,    0x394,  0x39b,  0x5,    0xc0,   0x61,
      0x2,   0x395,  0x396,  0x7,    0x4f,   0x2,    0x2,    0x396,  0x39b,
      0x5,   0x68,   0x35,   0x2,    0x397,  0x398,  0x7,    0x4e,   0x2,
      0x2,   0x398,  0x399,  0x7,    0x4f,   0x2,    0x2,    0x399,  0x39b,
      0x5,   0x68,   0x35,   0x2,    0x39a,  0x38a,  0x3,    0x2,    0x2,
      0x2,   0x39a,  0x38c,  0x3,    0x2,    0x2,    0x2,    0x39a,  0x38e,
      0x3,   0x2,    0x2,    0x2,    0x39a,  0x38f,  0x3,    0x2,    0x2,
      0x2,   0x39a,  0x391,  0x3,    0x2,    0x2,    0x2,    0x39a,  0x393,
      0x3,   0x2,    0x2,    0x2,    0x39a,  0x395,  0x3,    0x2,    0x2,
      0x2,   0x39a,  0x397,  0x3,    0x2,    0x2,    0x2,    0x39a,  0x39b,
      0x3,   0x2,    0x2,    0x2,    0x39b,  0xbf,   0x3,    0x2,    0x2,
      0x2,   0x39c,  0x39d,  0x5,    0xc2,   0x62,   0x2,    0x39d,  0xc1,
      0x3,   0x2,    0x2,    0x2,    0x39e,  0x3a6,  0x5,    0xc6,   0x64,
      0x2,   0x39f,  0x3a0,  0x7,    0xf,    0x2,    0x2,    0x3a0,  0x3a5,
      0x5,   0xc6,   0x64,   0x2,    0x3a1,  0x3a2,  0x7,    0x1b,   0x2,
      0x2,   0x3a2,  0x3a5,  0x5,    0xc6,   0x64,   0x2,    0x3a3,  0x3a5,
      0x5,   0xc4,   0x63,   0x2,    0x3a4,  0x39f,  0x3,    0x2,    0x2,
      0x2,   0x3a4,  0x3a1,  0x3,    0x2,    0x2,    0x2,    0x3a4,  0x3a3,
      0x3,   0x2,    0x2,    0x2,    0x3a5,  0x3a8,  0x3,    0x2,    0x2,
      0x2,   0x3a6,  0x3a4,  0x3,    0x2,    0x2,    0x2,    0x3a6,  0x3a7,
      0x3,   0x2,    0x2,    0x2,    0x3a7,  0xc3,   0x3,    0x2,    0x2,
      0x2,   0x3a8,  0x3a6,  0x3,    0x2,    0x2,    0x2,    0x3a9,  0x3ac,
      0x5,   0xe4,   0x73,   0x2,    0x3aa,  0x3ac,  0x5,    0xe6,   0x74,
      0x2,   0x3ab,  0x3a9,  0x3,    0x2,    0x2,    0x2,    0x3ab,  0x3aa,
      0x3,   0x2,    0x2,    0x2,    0x3ac,  0x3b3,  0x3,    0x2,    0x2,
      0x2,   0x3ad,  0x3ae,  0x7,    0x3,    0x2,    0x2,    0x3ae,  0x3b2,
      0x5,   0xc8,   0x65,   0x2,    0x3af,  0x3b0,  0x7,    0xd,    0x2,
      0x2,   0x3b0,  0x3b2,  0x5,    0xc8,   0x65,   0x2,    0x3b1,  0x3ad,
      0x3,   0x2,    0x2,    0x2,    0x3b1,  0x3af,  0x3,    0x2,    0x2,
      0x2,   0x3b2,  0x3b5,  0x3,    0x2,    0x2,    0x2,    0x3b3,  0x3b1,
      0x3,   0x2,    0x2,    0x2,    0x3b3,  0x3b4,  0x3,    0x2,    0x2,
      0x2,   0x3b4,  0xc5,   0x3,    0x2,    0x2,    0x2,    0x3b5,  0x3b3,
      0x3,   0x2,    0x2,    0x2,    0x3b6,  0x3bd,  0x5,    0xc8,   0x65,
      0x2,   0x3b7,  0x3b8,  0x7,    0x3,    0x2,    0x2,    0x3b8,  0x3bc,
      0x5,   0xc8,   0x65,   0x2,    0x3b9,  0x3ba,  0x7,    0xd,    0x2,
      0x2,   0x3ba,  0x3bc,  0x5,    0xc8,   0x65,   0x2,    0x3bb,  0x3b7,
      0x3,   0x2,    0x2,    0x2,    0x3bb,  0x3b9,  0x3,    0x2,    0x2,
      0x2,   0x3bc,  0x3bf,  0x3,    0x2,    0x2,    0x2,    0x3bd,  0x3bb,
      0x3,   0x2,    0x2,    0x2,    0x3bd,  0x3be,  0x3,    0x2,    0x2,
      0x2,   0x3be,  0xc7,   0x3,    0x2,    0x2,    0x2,    0x3bf,  0x3bd,
      0x3,   0x2,    0x2,    0x2,    0x3c0,  0x3c1,  0x7,    0x11,   0x2,
      0x2,   0x3c1,  0x3c8,  0x5,    0xca,   0x66,   0x2,    0x3c2,  0x3c3,
      0x7,   0xf,    0x2,    0x2,    0x3c3,  0x3c8,  0x5,    0xca,   0x66,
      0x2,   0x3c4,  0x3c5,  0x7,    0x1b,   0x2,    0x2,    0x3c5,  0x3c8,
      0x5,   0xca,   0x66,   0x2,    0x3c6,  0x3c8,  0x5,    0xca,   0x66,
      0x2,   0x3c7,  0x3c0,  0x3,    0x2,    0x2,    0x2,    0x3c7,  0x3c2,
      0x3,   0x2,    0x2,    0x2,    0x3c7,  0x3c4,  0x3,    0x2,    0x2,
      0x2,   0x3c7,  0x3c6,  0x3,    0x2,    0x2,    0x2,    0x3c8,  0xc9,
      0x3,   0x2,    0x2,    0x2,    0x3c9,  0x3d1,  0x5,    0xcc,   0x67,
      0x2,   0x3ca,  0x3d1,  0x5,    0xce,   0x68,   0x2,    0x3cb,  0x3d1,
      0x5,   0xdc,   0x6f,   0x2,    0x3cc,  0x3d1,  0x5,    0xde,   0x70,
      0x2,   0x3cd,  0x3d1,  0x5,    0xe0,   0x71,   0x2,    0x3ce,  0x3d1,
      0x5,   0xe8,   0x75,   0x2,    0x3cf,  0x3d1,  0x5,    0xb2,   0x5a,
      0x2,   0x3d0,  0x3c9,  0x3,    0x2,    0x2,    0x2,    0x3d0,  0x3ca,
      0x3,   0x2,    0x2,    0x2,    0x3d0,  0x3cb,  0x3,    0x2,    0x2,
      0x2,   0x3d0,  0x3cc,  0x3,    0x2,    0x2,    0x2,    0x3d0,  0x3cd,
      0x3,   0x2,    0x2,    0x2,    0x3d0,  0x3ce,  0x3,    0x2,    0x2,
      0x2,   0x3d0,  0x3cf,  0x3,    0x2,    0x2,    0x2,    0x3d1,  0xcb,
      0x3,   0x2,    0x2,    0x2,    0x3d2,  0x3d3,  0x7,    0x4,    0x2,
      0x2,   0x3d3,  0x3d4,  0x5,    0xb6,   0x5c,   0x2,    0x3d4,  0x3d5,
      0x7,   0x5,    0x2,    0x2,    0x3d5,  0xcd,   0x3,    0x2,    0x2,
      0x2,   0x3d6,  0x4dd,  0x5,    0xda,   0x6e,   0x2,    0x3d7,  0x3d8,
      0x7,   0x50,   0x2,    0x2,    0x3d8,  0x3d9,  0x7,    0x4,    0x2,
      0x2,   0x3d9,  0x3da,  0x5,    0xb6,   0x5c,   0x2,    0x3da,  0x3db,
      0x7,   0x5,    0x2,    0x2,    0x3db,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x3dc,  0x3dd,  0x7,    0x51,   0x2,    0x2,    0x3dd,  0x3de,
      0x7,   0x4,    0x2,    0x2,    0x3de,  0x3df,  0x5,    0xb6,   0x5c,
      0x2,   0x3df,  0x3e0,  0x7,    0x5,    0x2,    0x2,    0x3e0,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x3e1,  0x3e2,  0x7,    0x52,   0x2,
      0x2,   0x3e2,  0x3e3,  0x7,    0x4,    0x2,    0x2,    0x3e3,  0x3e4,
      0x5,   0xb6,   0x5c,   0x2,    0x3e4,  0x3e5,  0x7,    0x9,    0x2,
      0x2,   0x3e5,  0x3e6,  0x5,    0xb6,   0x5c,   0x2,    0x3e6,  0x3e7,
      0x7,   0x5,    0x2,    0x2,    0x3e7,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x3e8,  0x3e9,  0x7,    0x53,   0x2,    0x2,    0x3e9,  0x3ea,
      0x7,   0x4,    0x2,    0x2,    0x3ea,  0x3eb,  0x5,    0xb6,   0x5c,
      0x2,   0x3eb,  0x3ec,  0x7,    0x5,    0x2,    0x2,    0x3ec,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x3ed,  0x3ee,  0x7,    0x54,   0x2,
      0x2,   0x3ee,  0x3ef,  0x7,    0x4,    0x2,    0x2,    0x3ef,  0x3f0,
      0x5,   0xb2,   0x5a,   0x2,    0x3f0,  0x3f1,  0x7,    0x5,    0x2,
      0x2,   0x3f1,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x3f2,  0x3f3,
      0x7,   0x55,   0x2,    0x2,    0x3f3,  0x3f4,  0x7,    0x4,    0x2,
      0x2,   0x3f4,  0x3f5,  0x5,    0xb6,   0x5c,   0x2,    0x3f5,  0x3f6,
      0x7,   0x5,    0x2,    0x2,    0x3f6,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x3f7,  0x3f8,  0x7,    0x56,   0x2,    0x2,    0x3f8,  0x3f9,
      0x7,   0x4,    0x2,    0x2,    0x3f9,  0x3fa,  0x5,    0xb6,   0x5c,
      0x2,   0x3fa,  0x3fb,  0x7,    0x5,    0x2,    0x2,    0x3fb,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x3fc,  0x402,  0x7,    0x57,   0x2,
      0x2,   0x3fd,  0x3fe,  0x7,    0x4,    0x2,    0x2,    0x3fe,  0x3ff,
      0x5,   0xb6,   0x5c,   0x2,    0x3ff,  0x400,  0x7,    0x5,    0x2,
      0x2,   0x400,  0x403,  0x3,    0x2,    0x2,    0x2,    0x401,  0x403,
      0x7,   0xa3,   0x2,    0x2,    0x402,  0x3fd,  0x3,    0x2,    0x2,
      0x2,   0x402,  0x401,  0x3,    0x2,    0x2,    0x2,    0x403,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x404,  0x405,  0x7,    0x58,   0x2,
      0x2,   0x405,  0x4dd,  0x7,    0xa3,   0x2,    0x2,    0x406,  0x407,
      0x7,   0x59,   0x2,    0x2,    0x407,  0x408,  0x7,    0x4,    0x2,
      0x2,   0x408,  0x409,  0x5,    0xb6,   0x5c,   0x2,    0x409,  0x40a,
      0x7,   0x5,    0x2,    0x2,    0x40a,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x40b,  0x40c,  0x7,    0x5a,   0x2,    0x2,    0x40c,  0x40d,
      0x7,   0x4,    0x2,    0x2,    0x40d,  0x40e,  0x5,    0xb6,   0x5c,
      0x2,   0x40e,  0x40f,  0x7,    0x5,    0x2,    0x2,    0x40f,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x410,  0x411,  0x7,    0x5b,   0x2,
      0x2,   0x411,  0x412,  0x7,    0x4,    0x2,    0x2,    0x412,  0x413,
      0x5,   0xb6,   0x5c,   0x2,    0x413,  0x414,  0x7,    0x5,    0x2,
      0x2,   0x414,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x415,  0x416,
      0x7,   0x5c,   0x2,    0x2,    0x416,  0x417,  0x7,    0x4,    0x2,
      0x2,   0x417,  0x418,  0x5,    0xb6,   0x5c,   0x2,    0x418,  0x419,
      0x7,   0x5,    0x2,    0x2,    0x419,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x41a,  0x41b,  0x7,    0x5d,   0x2,    0x2,    0x41b,  0x4dd,
      0x5,   0x68,   0x35,   0x2,    0x41c,  0x4dd,  0x5,    0xd2,   0x6a,
      0x2,   0x41d,  0x41e,  0x7,    0x5e,   0x2,    0x2,    0x41e,  0x41f,
      0x7,   0x4,    0x2,    0x2,    0x41f,  0x420,  0x5,    0xb6,   0x5c,
      0x2,   0x420,  0x421,  0x7,    0x5,    0x2,    0x2,    0x421,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x422,  0x4dd,  0x5,    0xd4,   0x6b,
      0x2,   0x423,  0x424,  0x7,    0x5f,   0x2,    0x2,    0x424,  0x425,
      0x7,   0x4,    0x2,    0x2,    0x425,  0x426,  0x5,    0xb6,   0x5c,
      0x2,   0x426,  0x427,  0x7,    0x5,    0x2,    0x2,    0x427,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x428,  0x429,  0x7,    0x60,   0x2,
      0x2,   0x429,  0x42a,  0x7,    0x4,    0x2,    0x2,    0x42a,  0x42b,
      0x5,   0xb6,   0x5c,   0x2,    0x42b,  0x42c,  0x7,    0x5,    0x2,
      0x2,   0x42c,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x42d,  0x42e,
      0x7,   0x61,   0x2,    0x2,    0x42e,  0x42f,  0x7,    0x1c,   0x2,
      0x2,   0x42f,  0x430,  0x7,    0x62,   0x2,    0x2,    0x430,  0x431,
      0x7,   0x1c,   0x2,    0x2,    0x431,  0x432,  0x7,    0x56,   0x2,
      0x2,   0x432,  0x433,  0x7,    0x4,    0x2,    0x2,    0x433,  0x434,
      0x5,   0xb6,   0x5c,   0x2,    0x434,  0x435,  0x7,    0x5,    0x2,
      0x2,   0x435,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x436,  0x437,
      0x7,   0x63,   0x2,    0x2,    0x437,  0x438,  0x7,    0x4,    0x2,
      0x2,   0x438,  0x439,  0x5,    0xb6,   0x5c,   0x2,    0x439,  0x43a,
      0x7,   0x9,    0x2,    0x2,    0x43a,  0x43b,  0x5,    0xb6,   0x5c,
      0x2,   0x43b,  0x43c,  0x7,    0x5,    0x2,    0x2,    0x43c,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x43d,  0x43e,  0x7,    0x64,   0x2,
      0x2,   0x43e,  0x43f,  0x7,    0x4,    0x2,    0x2,    0x43f,  0x440,
      0x5,   0xb6,   0x5c,   0x2,    0x440,  0x441,  0x7,    0x9,    0x2,
      0x2,   0x441,  0x442,  0x5,    0xb6,   0x5c,   0x2,    0x442,  0x443,
      0x7,   0x5,    0x2,    0x2,    0x443,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x444,  0x445,  0x7,    0x65,   0x2,    0x2,    0x445,  0x446,
      0x7,   0x4,    0x2,    0x2,    0x446,  0x447,  0x5,    0xb6,   0x5c,
      0x2,   0x447,  0x448,  0x7,    0x9,    0x2,    0x2,    0x448,  0x449,
      0x5,   0xb6,   0x5c,   0x2,    0x449,  0x44a,  0x7,    0x5,    0x2,
      0x2,   0x44a,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x44b,  0x44c,
      0x7,   0x66,   0x2,    0x2,    0x44c,  0x44d,  0x7,    0x4,    0x2,
      0x2,   0x44d,  0x44e,  0x5,    0xb6,   0x5c,   0x2,    0x44e,  0x44f,
      0x7,   0x9,    0x2,    0x2,    0x44f,  0x450,  0x5,    0xb6,   0x5c,
      0x2,   0x450,  0x451,  0x7,    0x5,    0x2,    0x2,    0x451,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x452,  0x453,  0x7,    0x67,   0x2,
      0x2,   0x453,  0x454,  0x7,    0x4,    0x2,    0x2,    0x454,  0x455,
      0x5,   0xb6,   0x5c,   0x2,    0x455,  0x456,  0x7,    0x9,    0x2,
      0x2,   0x456,  0x457,  0x5,    0xb6,   0x5c,   0x2,    0x457,  0x458,
      0x7,   0x5,    0x2,    0x2,    0x458,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x459,  0x45a,  0x7,    0x68,   0x2,    0x2,    0x45a,  0x45b,
      0x7,   0x4,    0x2,    0x2,    0x45b,  0x45c,  0x5,    0xb6,   0x5c,
      0x2,   0x45c,  0x45d,  0x7,    0x5,    0x2,    0x2,    0x45d,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x45e,  0x45f,  0x7,    0x69,   0x2,
      0x2,   0x45f,  0x460,  0x7,    0x4,    0x2,    0x2,    0x460,  0x461,
      0x5,   0xb6,   0x5c,   0x2,    0x461,  0x462,  0x7,    0x5,    0x2,
      0x2,   0x462,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x463,  0x464,
      0x7,   0x6a,   0x2,    0x2,    0x464,  0x465,  0x7,    0x4,    0x2,
      0x2,   0x465,  0x466,  0x5,    0xb6,   0x5c,   0x2,    0x466,  0x467,
      0x7,   0x5,    0x2,    0x2,    0x467,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x468,  0x469,  0x7,    0x6b,   0x2,    0x2,    0x469,  0x46a,
      0x7,   0x4,    0x2,    0x2,    0x46a,  0x46b,  0x5,    0xb6,   0x5c,
      0x2,   0x46b,  0x46c,  0x7,    0x5,    0x2,    0x2,    0x46c,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x46d,  0x46e,  0x7,    0x6c,   0x2,
      0x2,   0x46e,  0x46f,  0x7,    0x4,    0x2,    0x2,    0x46f,  0x470,
      0x5,   0xb6,   0x5c,   0x2,    0x470,  0x471,  0x7,    0x5,    0x2,
      0x2,   0x471,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x472,  0x473,
      0x7,   0x6d,   0x2,    0x2,    0x473,  0x474,  0x7,    0x4,    0x2,
      0x2,   0x474,  0x475,  0x5,    0xb6,   0x5c,   0x2,    0x475,  0x476,
      0x7,   0x5,    0x2,    0x2,    0x476,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x477,  0x478,  0x7,    0x6e,   0x2,    0x2,    0x478,  0x479,
      0x7,   0x4,    0x2,    0x2,    0x479,  0x47a,  0x5,    0xb6,   0x5c,
      0x2,   0x47a,  0x47b,  0x7,    0x5,    0x2,    0x2,    0x47b,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x47c,  0x47d,  0x7,    0x6f,   0x2,
      0x2,   0x47d,  0x47e,  0x7,    0x4,    0x2,    0x2,    0x47e,  0x47f,
      0x5,   0xb6,   0x5c,   0x2,    0x47f,  0x480,  0x7,    0x5,    0x2,
      0x2,   0x480,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x481,  0x482,
      0x7,   0x70,   0x2,    0x2,    0x482,  0x4dd,  0x7,    0xa3,   0x2,
      0x2,   0x483,  0x484,  0x7,    0x71,   0x2,    0x2,    0x484,  0x4dd,
      0x7,   0xa3,   0x2,    0x2,    0x485,  0x486,  0x7,    0x72,   0x2,
      0x2,   0x486,  0x4dd,  0x7,    0xa3,   0x2,    0x2,    0x487,  0x488,
      0x7,   0x77,   0x2,    0x2,    0x488,  0x489,  0x7,    0x4,    0x2,
      0x2,   0x489,  0x48a,  0x5,    0xb6,   0x5c,   0x2,    0x48a,  0x48b,
      0x7,   0x5,    0x2,    0x2,    0x48b,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x48c,  0x48d,  0x7,    0x73,   0x2,    0x2,    0x48d,  0x48e,
      0x7,   0x4,    0x2,    0x2,    0x48e,  0x48f,  0x5,    0xb6,   0x5c,
      0x2,   0x48f,  0x490,  0x7,    0x5,    0x2,    0x2,    0x490,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x491,  0x492,  0x7,    0x74,   0x2,
      0x2,   0x492,  0x493,  0x7,    0x4,    0x2,    0x2,    0x493,  0x494,
      0x5,   0xb6,   0x5c,   0x2,    0x494,  0x495,  0x7,    0x5,    0x2,
      0x2,   0x495,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x496,  0x497,
      0x7,   0x75,   0x2,    0x2,    0x497,  0x498,  0x7,    0x4,    0x2,
      0x2,   0x498,  0x499,  0x5,    0xb6,   0x5c,   0x2,    0x499,  0x49a,
      0x7,   0x5,    0x2,    0x2,    0x49a,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x49b,  0x49c,  0x7,    0x76,   0x2,    0x2,    0x49c,  0x49d,
      0x7,   0x4,    0x2,    0x2,    0x49d,  0x49e,  0x5,    0xb6,   0x5c,
      0x2,   0x49e,  0x49f,  0x7,    0x5,    0x2,    0x2,    0x49f,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x4a0,  0x4a1,  0x7,    0x78,   0x2,
      0x2,   0x4a1,  0x4dd,  0x5,    0x68,   0x35,   0x2,    0x4a2,  0x4a3,
      0x7,   0x79,   0x2,    0x2,    0x4a3,  0x4a4,  0x7,    0x4,    0x2,
      0x2,   0x4a4,  0x4a5,  0x5,    0xb6,   0x5c,   0x2,    0x4a5,  0x4a6,
      0x7,   0x9,    0x2,    0x2,    0x4a6,  0x4a7,  0x5,    0xb6,   0x5c,
      0x2,   0x4a7,  0x4a8,  0x7,    0x9,    0x2,    0x2,    0x4a8,  0x4a9,
      0x5,   0xb6,   0x5c,   0x2,    0x4a9,  0x4aa,  0x7,    0x5,    0x2,
      0x2,   0x4aa,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x4ab,  0x4ac,
      0x7,   0x7a,   0x2,    0x2,    0x4ac,  0x4ad,  0x7,    0x4,    0x2,
      0x2,   0x4ad,  0x4ae,  0x5,    0xb6,   0x5c,   0x2,    0x4ae,  0x4af,
      0x7,   0x9,    0x2,    0x2,    0x4af,  0x4b0,  0x5,    0xb6,   0x5c,
      0x2,   0x4b0,  0x4b1,  0x7,    0x5,    0x2,    0x2,    0x4b1,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x4b2,  0x4b3,  0x7,    0x7b,   0x2,
      0x2,   0x4b3,  0x4b4,  0x7,    0x4,    0x2,    0x2,    0x4b4,  0x4b5,
      0x5,   0xb6,   0x5c,   0x2,    0x4b5,  0x4b6,  0x7,    0x9,    0x2,
      0x2,   0x4b6,  0x4b7,  0x5,    0xb6,   0x5c,   0x2,    0x4b7,  0x4b8,
      0x7,   0x5,    0x2,    0x2,    0x4b8,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x4b9,  0x4ba,  0x7,    0x7c,   0x2,    0x2,    0x4ba,  0x4bb,
      0x7,   0x4,    0x2,    0x2,    0x4bb,  0x4bc,  0x5,    0xb6,   0x5c,
      0x2,   0x4bc,  0x4bd,  0x7,    0x9,    0x2,    0x2,    0x4bd,  0x4be,
      0x5,   0xb6,   0x5c,   0x2,    0x4be,  0x4bf,  0x7,    0x5,    0x2,
      0x2,   0x4bf,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x4c0,  0x4c1,
      0x7,   0x7d,   0x2,    0x2,    0x4c1,  0x4c2,  0x7,    0x4,    0x2,
      0x2,   0x4c2,  0x4c3,  0x5,    0xb6,   0x5c,   0x2,    0x4c3,  0x4c4,
      0x7,   0x5,    0x2,    0x2,    0x4c4,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x4c5,  0x4c6,  0x7,    0x7e,   0x2,    0x2,    0x4c6,  0x4c7,
      0x7,   0x4,    0x2,    0x2,    0x4c7,  0x4c8,  0x5,    0xb6,   0x5c,
      0x2,   0x4c8,  0x4c9,  0x7,    0x5,    0x2,    0x2,    0x4c9,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x4ca,  0x4cb,  0x7,    0x7f,   0x2,
      0x2,   0x4cb,  0x4cc,  0x7,    0x4,    0x2,    0x2,    0x4cc,  0x4cd,
      0x5,   0xb6,   0x5c,   0x2,    0x4cd,  0x4ce,  0x7,    0x5,    0x2,
      0x2,   0x4ce,  0x4dd,  0x3,    0x2,    0x2,    0x2,    0x4cf,  0x4d0,
      0x7,   0x80,   0x2,    0x2,    0x4d0,  0x4d1,  0x7,    0x4,    0x2,
      0x2,   0x4d1,  0x4d2,  0x5,    0xb6,   0x5c,   0x2,    0x4d2,  0x4d3,
      0x7,   0x5,    0x2,    0x2,    0x4d3,  0x4dd,  0x3,    0x2,    0x2,
      0x2,   0x4d4,  0x4d5,  0x7,    0x81,   0x2,    0x2,    0x4d5,  0x4d6,
      0x7,   0x4,    0x2,    0x2,    0x4d6,  0x4d7,  0x5,    0xb6,   0x5c,
      0x2,   0x4d7,  0x4d8,  0x7,    0x5,    0x2,    0x2,    0x4d8,  0x4dd,
      0x3,   0x2,    0x2,    0x2,    0x4d9,  0x4dd,  0x5,    0xd0,   0x69,
      0x2,   0x4da,  0x4dd,  0x5,    0xd6,   0x6c,   0x2,    0x4db,  0x4dd,
      0x5,   0xd8,   0x6d,   0x2,    0x4dc,  0x3d6,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x3d7,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x3dc,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x3e1,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x3e8,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x3ed,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x3f2,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x3f7,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x3fc,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x404,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x406,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x40b,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x410,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x415,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x41a,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x41c,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x41d,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x422,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x423,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x428,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x42d,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x436,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x43d,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x444,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x44b,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x452,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x459,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x45e,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x463,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x468,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x46d,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x472,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x477,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x47c,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x481,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x483,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x485,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x487,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x48c,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x491,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x496,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x49b,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x4a0,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x4a2,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x4ab,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x4b2,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x4b9,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x4c0,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x4c5,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x4ca,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x4cf,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x4d4,  0x3,    0x2,    0x2,
      0x2,   0x4dc,  0x4d9,  0x3,    0x2,    0x2,    0x2,    0x4dc,  0x4da,
      0x3,   0x2,    0x2,    0x2,    0x4dc,  0x4db,  0x3,    0x2,    0x2,
      0x2,   0x4dd,  0xcf,   0x3,    0x2,    0x2,    0x2,    0x4de,  0x4df,
      0x7,   0x82,   0x2,    0x2,    0x4df,  0x4e0,  0x7,    0x4,    0x2,
      0x2,   0x4e0,  0x4e1,  0x5,    0xb6,   0x5c,   0x2,    0x4e1,  0x4e2,
      0x7,   0x9,    0x2,    0x2,    0x4e2,  0x4e5,  0x5,    0xb6,   0x5c,
      0x2,   0x4e3,  0x4e4,  0x7,    0x9,    0x2,    0x2,    0x4e4,  0x4e6,
      0x5,   0xb6,   0x5c,   0x2,    0x4e5,  0x4e3,  0x3,    0x2,    0x2,
      0x2,   0x4e5,  0x4e6,  0x3,    0x2,    0x2,    0x2,    0x4e6,  0x4e7,
      0x3,   0x2,    0x2,    0x2,    0x4e7,  0x4e8,  0x7,    0x5,    0x2,
      0x2,   0x4e8,  0xd1,   0x3,    0x2,    0x2,    0x2,    0x4e9,  0x4ea,
      0x7,   0x83,   0x2,    0x2,    0x4ea,  0x4eb,  0x7,    0x4,    0x2,
      0x2,   0x4eb,  0x4ec,  0x5,    0xb6,   0x5c,   0x2,    0x4ec,  0x4ed,
      0x7,   0x9,    0x2,    0x2,    0x4ed,  0x4f0,  0x5,    0xb6,   0x5c,
      0x2,   0x4ee,  0x4ef,  0x7,    0x9,    0x2,    0x2,    0x4ef,  0x4f1,
      0x5,   0xb6,   0x5c,   0x2,    0x4f0,  0x4ee,  0x3,    0x2,    0x2,
      0x2,   0x4f0,  0x4f1,  0x3,    0x2,    0x2,    0x2,    0x4f1,  0x4f2,
      0x3,   0x2,    0x2,    0x2,    0x4f2,  0x4f3,  0x7,    0x5,    0x2,
      0x2,   0x4f3,  0xd3,   0x3,    0x2,    0x2,    0x2,    0x4f4,  0x4f5,
      0x7,   0x84,   0x2,    0x2,    0x4f5,  0x4f6,  0x7,    0x4,    0x2,
      0x2,   0x4f6,  0x4f7,  0x5,    0xb6,   0x5c,   0x2,    0x4f7,  0x4f8,
      0x7,   0x9,    0x2,    0x2,    0x4f8,  0x4f9,  0x5,    0xb6,   0x5c,
      0x2,   0x4f9,  0x4fa,  0x7,    0x9,    0x2,    0x2,    0x4fa,  0x4fd,
      0x5,   0xb6,   0x5c,   0x2,    0x4fb,  0x4fc,  0x7,    0x9,    0x2,
      0x2,   0x4fc,  0x4fe,  0x5,    0xb6,   0x5c,   0x2,    0x4fd,  0x4fb,
      0x3,   0x2,    0x2,    0x2,    0x4fd,  0x4fe,  0x3,    0x2,    0x2,
      0x2,   0x4fe,  0x4ff,  0x3,    0x2,    0x2,    0x2,    0x4ff,  0x500,
      0x7,   0x5,    0x2,    0x2,    0x500,  0xd5,   0x3,    0x2,    0x2,
      0x2,   0x501,  0x502,  0x7,    0x85,   0x2,    0x2,    0x502,  0x503,
      0x5,   0x40,   0x21,   0x2,    0x503,  0xd7,   0x3,    0x2,    0x2,
      0x2,   0x504,  0x505,  0x7,    0x4e,   0x2,    0x2,    0x505,  0x506,
      0x7,   0x85,   0x2,    0x2,    0x506,  0x507,  0x5,    0x40,   0x21,
      0x2,   0x507,  0xd9,   0x3,    0x2,    0x2,    0x2,    0x508,  0x509,
      0x7,   0x86,   0x2,    0x2,    0x509,  0x50b,  0x7,    0x4,    0x2,
      0x2,   0x50a,  0x50c,  0x7,    0x24,   0x2,    0x2,    0x50b,  0x50a,
      0x3,   0x2,    0x2,    0x2,    0x50b,  0x50c,  0x3,    0x2,    0x2,
      0x2,   0x50c,  0x50f,  0x3,    0x2,    0x2,    0x2,    0x50d,  0x510,
      0x7,   0x3,    0x2,    0x2,    0x50e,  0x510,  0x5,    0xb6,   0x5c,
      0x2,   0x50f,  0x50d,  0x3,    0x2,    0x2,    0x2,    0x50f,  0x50e,
      0x3,   0x2,    0x2,    0x2,    0x510,  0x511,  0x3,    0x2,    0x2,
      0x2,   0x511,  0x549,  0x7,    0x5,    0x2,    0x2,    0x512,  0x513,
      0x7,   0x87,   0x2,    0x2,    0x513,  0x515,  0x7,    0x4,    0x2,
      0x2,   0x514,  0x516,  0x7,    0x24,   0x2,    0x2,    0x515,  0x514,
      0x3,   0x2,    0x2,    0x2,    0x515,  0x516,  0x3,    0x2,    0x2,
      0x2,   0x516,  0x517,  0x3,    0x2,    0x2,    0x2,    0x517,  0x518,
      0x5,   0xb6,   0x5c,   0x2,    0x518,  0x519,  0x7,    0x5,    0x2,
      0x2,   0x519,  0x549,  0x3,    0x2,    0x2,    0x2,    0x51a,  0x51b,
      0x7,   0x88,   0x2,    0x2,    0x51b,  0x51d,  0x7,    0x4,    0x2,
      0x2,   0x51c,  0x51e,  0x7,    0x24,   0x2,    0x2,    0x51d,  0x51c,
      0x3,   0x2,    0x2,    0x2,    0x51d,  0x51e,  0x3,    0x2,    0x2,
      0x2,   0x51e,  0x51f,  0x3,    0x2,    0x2,    0x2,    0x51f,  0x520,
      0x5,   0xb6,   0x5c,   0x2,    0x520,  0x521,  0x7,    0x5,    0x2,
      0x2,   0x521,  0x549,  0x3,    0x2,    0x2,    0x2,    0x522,  0x523,
      0x7,   0x89,   0x2,    0x2,    0x523,  0x525,  0x7,    0x4,    0x2,
      0x2,   0x524,  0x526,  0x7,    0x24,   0x2,    0x2,    0x525,  0x524,
      0x3,   0x2,    0x2,    0x2,    0x525,  0x526,  0x3,    0x2,    0x2,
      0x2,   0x526,  0x527,  0x3,    0x2,    0x2,    0x2,    0x527,  0x528,
      0x5,   0xb6,   0x5c,   0x2,    0x528,  0x529,  0x7,    0x5,    0x2,
      0x2,   0x529,  0x549,  0x3,    0x2,    0x2,    0x2,    0x52a,  0x52b,
      0x7,   0x8a,   0x2,    0x2,    0x52b,  0x52d,  0x7,    0x4,    0x2,
      0x2,   0x52c,  0x52e,  0x7,    0x24,   0x2,    0x2,    0x52d,  0x52c,
      0x3,   0x2,    0x2,    0x2,    0x52d,  0x52e,  0x3,    0x2,    0x2,
      0x2,   0x52e,  0x52f,  0x3,    0x2,    0x2,    0x2,    0x52f,  0x530,
      0x5,   0xb6,   0x5c,   0x2,    0x530,  0x531,  0x7,    0x5,    0x2,
      0x2,   0x531,  0x549,  0x3,    0x2,    0x2,    0x2,    0x532,  0x533,
      0x7,   0x8b,   0x2,    0x2,    0x533,  0x535,  0x7,    0x4,    0x2,
      0x2,   0x534,  0x536,  0x7,    0x24,   0x2,    0x2,    0x535,  0x534,
      0x3,   0x2,    0x2,    0x2,    0x535,  0x536,  0x3,    0x2,    0x2,
      0x2,   0x536,  0x537,  0x3,    0x2,    0x2,    0x2,    0x537,  0x538,
      0x5,   0xb6,   0x5c,   0x2,    0x538,  0x539,  0x7,    0x5,    0x2,
      0x2,   0x539,  0x549,  0x3,    0x2,    0x2,    0x2,    0x53a,  0x53b,
      0x7,   0x2e,   0x2,    0x2,    0x53b,  0x53d,  0x7,    0x4,    0x2,
      0x2,   0x53c,  0x53e,  0x7,    0x24,   0x2,    0x2,    0x53d,  0x53c,
      0x3,   0x2,    0x2,    0x2,    0x53d,  0x53e,  0x3,    0x2,    0x2,
      0x2,   0x53e,  0x53f,  0x3,    0x2,    0x2,    0x2,    0x53f,  0x544,
      0x5,   0xb6,   0x5c,   0x2,    0x540,  0x541,  0x7,    0xa,    0x2,
      0x2,   0x541,  0x542,  0x7,    0x8c,   0x2,    0x2,    0x542,  0x543,
      0x7,   0x16,   0x2,    0x2,    0x543,  0x545,  0x5,    0xea,   0x76,
      0x2,   0x544,  0x540,  0x3,    0x2,    0x2,    0x2,    0x544,  0x545,
      0x3,   0x2,    0x2,    0x2,    0x545,  0x546,  0x3,    0x2,    0x2,
      0x2,   0x546,  0x547,  0x7,    0x5,    0x2,    0x2,    0x547,  0x549,
      0x3,   0x2,    0x2,    0x2,    0x548,  0x508,  0x3,    0x2,    0x2,
      0x2,   0x548,  0x512,  0x3,    0x2,    0x2,    0x2,    0x548,  0x51a,
      0x3,   0x2,    0x2,    0x2,    0x548,  0x522,  0x3,    0x2,    0x2,
      0x2,   0x548,  0x52a,  0x3,    0x2,    0x2,    0x2,    0x548,  0x532,
      0x3,   0x2,    0x2,    0x2,    0x548,  0x53a,  0x3,    0x2,    0x2,
      0x2,   0x549,  0xdb,   0x3,    0x2,    0x2,    0x2,    0x54a,  0x54c,
      0x5,   0xec,   0x77,   0x2,    0x54b,  0x54d,  0x5,    0x66,   0x34,
      0x2,   0x54c,  0x54b,  0x3,    0x2,    0x2,    0x2,    0x54c,  0x54d,
      0x3,   0x2,    0x2,    0x2,    0x54d,  0xdd,   0x3,    0x2,    0x2,
      0x2,   0x54e,  0x552,  0x5,    0xea,   0x76,   0x2,    0x54f,  0x553,
      0x7,   0x93,   0x2,    0x2,    0x550,  0x551,  0x7,    0x1d,   0x2,
      0x2,   0x551,  0x553,  0x5,    0xec,   0x77,   0x2,    0x552,  0x54f,
      0x3,   0x2,    0x2,    0x2,    0x552,  0x550,  0x3,    0x2,    0x2,
      0x2,   0x552,  0x553,  0x3,    0x2,    0x2,    0x2,    0x553,  0xdf,
      0x3,   0x2,    0x2,    0x2,    0x554,  0x558,  0x5,    0xe2,   0x72,
      0x2,   0x555,  0x558,  0x5,    0xe4,   0x73,   0x2,    0x556,  0x558,
      0x5,   0xe6,   0x74,   0x2,    0x557,  0x554,  0x3,    0x2,    0x2,
      0x2,   0x557,  0x555,  0x3,    0x2,    0x2,    0x2,    0x557,  0x556,
      0x3,   0x2,    0x2,    0x2,    0x558,  0xe1,   0x3,    0x2,    0x2,
      0x2,   0x559,  0x55a,  0x9,    0x6,    0x2,    0x2,    0x55a,  0xe3,
      0x3,   0x2,    0x2,    0x2,    0x55b,  0x55c,  0x9,    0x7,    0x2,
      0x2,   0x55c,  0xe5,   0x3,    0x2,    0x2,    0x2,    0x55d,  0x55e,
      0x9,   0x8,    0x2,    0x2,    0x55e,  0xe7,   0x3,    0x2,    0x2,
      0x2,   0x55f,  0x560,  0x9,    0x9,    0x2,    0x2,    0x560,  0xe9,
      0x3,   0x2,    0x2,    0x2,    0x561,  0x562,  0x9,    0xa,    0x2,
      0x2,   0x562,  0xeb,   0x3,    0x2,    0x2,    0x2,    0x563,  0x564,
      0x7,   0x93,   0x2,    0x2,    0x564,  0x566,  0x7,    0x20,   0x2,
      0x2,   0x565,  0x563,  0x3,    0x2,    0x2,    0x2,    0x565,  0x566,
      0x3,   0x2,    0x2,    0x2,    0x566,  0x569,  0x3,    0x2,    0x2,
      0x2,   0x567,  0x56a,  0x5,    0xf2,   0x7a,   0x2,    0x568,  0x56a,
      0x5,   0xee,   0x78,   0x2,    0x569,  0x567,  0x3,    0x2,    0x2,
      0x2,   0x569,  0x568,  0x3,    0x2,    0x2,    0x2,    0x56a,  0xed,
      0x3,   0x2,    0x2,    0x2,    0x56b,  0x56e,  0x5,    0xf4,   0x7b,
      0x2,   0x56c,  0x56e,  0x5,    0xf6,   0x7c,   0x2,    0x56d,  0x56b,
      0x3,   0x2,    0x2,    0x2,    0x56d,  0x56c,  0x3,    0x2,    0x2,
      0x2,   0x56e,  0xef,   0x3,    0x2,    0x2,    0x2,    0x56f,  0x570,
      0x9,   0xb,    0x2,    0x2,    0x570,  0xf1,   0x3,    0x2,    0x2,
      0x2,   0x571,  0x572,  0x7,    0x8d,   0x2,    0x2,    0x572,  0xf3,
      0x3,   0x2,    0x2,    0x2,    0x573,  0x574,  0x7,    0x8f,   0x2,
      0x2,   0x574,  0xf5,   0x3,    0x2,    0x2,    0x2,    0x575,  0x576,
      0x7,   0x8e,   0x2,    0x2,    0x576,  0xf7,   0x3,    0x2,    0x2,
      0x2,   0x8b,   0xfd,   0x104,  0x106,  0x114,  0x121,  0x126,  0x129,
      0x12d, 0x13c,  0x145,  0x14b,  0x14f,  0x155,  0x158,  0x15d,  0x161,
      0x169, 0x172,  0x17c,  0x181,  0x184,  0x187,  0x18a,  0x190,  0x198,
      0x19d, 0x1a3,  0x1ab,  0x1b1,  0x1b3,  0x1b7,  0x1ba,  0x1be,  0x1c1,
      0x1c5, 0x1c8,  0x1cc,  0x1cf,  0x1d3,  0x1d6,  0x1da,  0x1dd,  0x1df,
      0x1ec, 0x1f1,  0x1f3,  0x1f8,  0x1fd,  0x201,  0x204,  0x208,  0x20e,
      0x210, 0x21a,  0x225,  0x236,  0x23d,  0x247,  0x24b,  0x251,  0x25a,
      0x25f, 0x266,  0x270,  0x279,  0x281,  0x288,  0x28d,  0x296,  0x29b,
      0x29f, 0x2a6,  0x2a8,  0x2b0,  0x2b3,  0x2bb,  0x2bf,  0x2c4,  0x2cb,
      0x2d6, 0x2d9,  0x2e1,  0x2e5,  0x2ee,  0x2f5,  0x301,  0x309,  0x30e,
      0x313, 0x31f,  0x328,  0x32b,  0x32e,  0x335,  0x337,  0x33d,  0x345,
      0x34f, 0x357,  0x35d,  0x361,  0x365,  0x369,  0x373,  0x37c,  0x384,
      0x39a, 0x3a4,  0x3a6,  0x3ab,  0x3b1,  0x3b3,  0x3bb,  0x3bd,  0x3c7,
      0x3d0, 0x402,  0x4dc,  0x4e5,  0x4f0,  0x4fd,  0x50b,  0x50f,  0x515,
      0x51d, 0x525,  0x52d,  0x535,  0x53d,  0x544,  0x548,  0x54c,  0x552,
      0x557, 0x565,  0x569,  0x56d,
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
