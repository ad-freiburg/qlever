
// Generated from ConfigShorthand.g4 by ANTLR 4.13.0

#include "ConfigShorthandParser.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct ConfigShorthandParserStaticData final {
  ConfigShorthandParserStaticData(std::vector<std::string> ruleNames,
                                  std::vector<std::string> literalNames,
                                  std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  ConfigShorthandParserStaticData(const ConfigShorthandParserStaticData&) =
      delete;
  ConfigShorthandParserStaticData(ConfigShorthandParserStaticData&&) = delete;
  ConfigShorthandParserStaticData& operator=(
      const ConfigShorthandParserStaticData&) = delete;
  ConfigShorthandParserStaticData& operator=(
      ConfigShorthandParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag configshorthandParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
    ConfigShorthandParserStaticData* configshorthandParserStaticData = nullptr;

void configshorthandParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (configshorthandParserStaticData != nullptr) {
    return;
  }
#else
  assert(configshorthandParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<ConfigShorthandParserStaticData>(
      std::vector<std::string>{"shortHandString", "assignments", "assignment",
                               "object", "list", "content"},
      std::vector<std::string>{"", "','", "':'", "'{'", "'}'", "'['", "']'"},
      std::vector<std::string>{"", "", "", "", "", "", "", "LITERAL", "BOOL",
                               "INTEGER", "FLOAT", "STRING", "NAME",
                               "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4, 1, 13, 51, 2,  0, 7,  0, 2, 1,  7,  1,  2,  2,  7,  2,  2,  3,
      7, 3, 2,  4,  7,  4, 2,  5, 7, 5,  1,  0,  1,  0,  1,  0,  1,  1,
      1, 1, 1,  1,  5,  1, 19, 8, 1, 10, 1,  12, 1,  22, 9,  1,  1,  1,
      1, 1, 1,  2,  1,  2, 1,  2, 1, 2,  1,  3,  1,  3,  1,  3,  1,  3,
      1, 4, 1,  4,  1,  4, 1,  4, 5, 4,  38, 8,  4,  10, 4,  12, 4,  41,
      9, 4, 1,  4,  1,  4, 1,  4, 1, 5,  1,  5,  1,  5,  3,  5,  49, 8,
      5, 1, 5,  0,  0,  6, 0,  2, 4, 6,  8,  10, 0,  0,  48, 0,  12, 1,
      0, 0, 0,  2,  20, 1, 0,  0, 0, 4,  25, 1,  0,  0,  0,  6,  29, 1,
      0, 0, 0,  8,  33, 1, 0,  0, 0, 10, 48, 1,  0,  0,  0,  12, 13, 3,
      2, 1, 0,  13, 14, 5, 0,  0, 1, 14, 1,  1,  0,  0,  0,  15, 16, 3,
      4, 2, 0,  16, 17, 5, 1,  0, 0, 17, 19, 1,  0,  0,  0,  18, 15, 1,
      0, 0, 0,  19, 22, 1, 0,  0, 0, 20, 18, 1,  0,  0,  0,  20, 21, 1,
      0, 0, 0,  21, 23, 1, 0,  0, 0, 22, 20, 1,  0,  0,  0,  23, 24, 3,
      4, 2, 0,  24, 3,  1, 0,  0, 0, 25, 26, 5,  12, 0,  0,  26, 27, 5,
      2, 0, 0,  27, 28, 3, 10, 5, 0, 28, 5,  1,  0,  0,  0,  29, 30, 5,
      3, 0, 0,  30, 31, 3, 2,  1, 0, 31, 32, 5,  4,  0,  0,  32, 7,  1,
      0, 0, 0,  33, 39, 5, 5,  0, 0, 34, 35, 3,  10, 5,  0,  35, 36, 5,
      1, 0, 0,  36, 38, 1, 0,  0, 0, 37, 34, 1,  0,  0,  0,  38, 41, 1,
      0, 0, 0,  39, 37, 1, 0,  0, 0, 39, 40, 1,  0,  0,  0,  40, 42, 1,
      0, 0, 0,  41, 39, 1, 0,  0, 0, 42, 43, 3,  10, 5,  0,  43, 44, 5,
      6, 0, 0,  44, 9,  1, 0,  0, 0, 45, 49, 5,  7,  0,  0,  46, 49, 3,
      8, 4, 0,  47, 49, 3, 6,  3, 0, 48, 45, 1,  0,  0,  0,  48, 46, 1,
      0, 0, 0,  48, 47, 1, 0,  0, 0, 49, 11, 1,  0,  0,  0,  3,  20, 39,
      48};
  staticData->serializedATN = antlr4::atn::SerializedATNView(
      serializedATNSegment,
      sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i),
                                           i);
  }
  configshorthandParserStaticData = staticData.release();
}

}  // namespace

ConfigShorthandParser::ConfigShorthandParser(TokenStream* input)
    : ConfigShorthandParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

ConfigShorthandParser::ConfigShorthandParser(
    TokenStream* input, const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  ConfigShorthandParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *configshorthandParserStaticData->atn,
      configshorthandParserStaticData->decisionToDFA,
      configshorthandParserStaticData->sharedContextCache, options);
}

ConfigShorthandParser::~ConfigShorthandParser() { delete _interpreter; }

const atn::ATN& ConfigShorthandParser::getATN() const {
  return *configshorthandParserStaticData->atn;
}

std::string ConfigShorthandParser::getGrammarFileName() const {
  return "ConfigShorthand.g4";
}

const std::vector<std::string>& ConfigShorthandParser::getRuleNames() const {
  return configshorthandParserStaticData->ruleNames;
}

const dfa::Vocabulary& ConfigShorthandParser::getVocabulary() const {
  return configshorthandParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView ConfigShorthandParser::getSerializedATN() const {
  return configshorthandParserStaticData->serializedATN;
}

//----------------- ShortHandStringContext
//------------------------------------------------------------------

ConfigShorthandParser::ShortHandStringContext::ShortHandStringContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

ConfigShorthandParser::AssignmentsContext*
ConfigShorthandParser::ShortHandStringContext::assignments() {
  return getRuleContext<ConfigShorthandParser::AssignmentsContext>(0);
}

tree::TerminalNode* ConfigShorthandParser::ShortHandStringContext::EOF() {
  return getToken(ConfigShorthandParser::EOF, 0);
}

size_t ConfigShorthandParser::ShortHandStringContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleShortHandString;
}

ConfigShorthandParser::ShortHandStringContext*
ConfigShorthandParser::shortHandString() {
  ShortHandStringContext* _localctx =
      _tracker.createInstance<ShortHandStringContext>(_ctx, getState());
  enterRule(_localctx, 0, ConfigShorthandParser::RuleShortHandString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(12);
    assignments();
    setState(13);
    match(ConfigShorthandParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentsContext
//------------------------------------------------------------------

ConfigShorthandParser::AssignmentsContext::AssignmentsContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<ConfigShorthandParser::AssignmentContext*>
ConfigShorthandParser::AssignmentsContext::assignment() {
  return getRuleContexts<ConfigShorthandParser::AssignmentContext>();
}

ConfigShorthandParser::AssignmentContext*
ConfigShorthandParser::AssignmentsContext::assignment(size_t i) {
  return getRuleContext<ConfigShorthandParser::AssignmentContext>(i);
}

size_t ConfigShorthandParser::AssignmentsContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleAssignments;
}

ConfigShorthandParser::AssignmentsContext*
ConfigShorthandParser::assignments() {
  AssignmentsContext* _localctx =
      _tracker.createInstance<AssignmentsContext>(_ctx, getState());
  enterRule(_localctx, 2, ConfigShorthandParser::RuleAssignments);

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
    setState(20);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(15);
        antlrcpp::downCast<AssignmentsContext*>(_localctx)->assignmentContext =
            assignment();
        antlrcpp::downCast<AssignmentsContext*>(_localctx)
            ->listOfAssignments.push_back(
                antlrcpp::downCast<AssignmentsContext*>(_localctx)
                    ->assignmentContext);
        setState(16);
        match(ConfigShorthandParser::T__0);
      }
      setState(22);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       0, _ctx);
    }
    setState(23);
    antlrcpp::downCast<AssignmentsContext*>(_localctx)->assignmentContext =
        assignment();
    antlrcpp::downCast<AssignmentsContext*>(_localctx)
        ->listOfAssignments.push_back(
            antlrcpp::downCast<AssignmentsContext*>(_localctx)
                ->assignmentContext);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentContext
//------------------------------------------------------------------

ConfigShorthandParser::AssignmentContext::AssignmentContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* ConfigShorthandParser::AssignmentContext::NAME() {
  return getToken(ConfigShorthandParser::NAME, 0);
}

ConfigShorthandParser::ContentContext*
ConfigShorthandParser::AssignmentContext::content() {
  return getRuleContext<ConfigShorthandParser::ContentContext>(0);
}

size_t ConfigShorthandParser::AssignmentContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleAssignment;
}

ConfigShorthandParser::AssignmentContext* ConfigShorthandParser::assignment() {
  AssignmentContext* _localctx =
      _tracker.createInstance<AssignmentContext>(_ctx, getState());
  enterRule(_localctx, 4, ConfigShorthandParser::RuleAssignment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(25);
    match(ConfigShorthandParser::NAME);
    setState(26);
    match(ConfigShorthandParser::T__1);
    setState(27);
    content();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectContext
//------------------------------------------------------------------

ConfigShorthandParser::ObjectContext::ObjectContext(ParserRuleContext* parent,
                                                    size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

ConfigShorthandParser::AssignmentsContext*
ConfigShorthandParser::ObjectContext::assignments() {
  return getRuleContext<ConfigShorthandParser::AssignmentsContext>(0);
}

size_t ConfigShorthandParser::ObjectContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleObject;
}

ConfigShorthandParser::ObjectContext* ConfigShorthandParser::object() {
  ObjectContext* _localctx =
      _tracker.createInstance<ObjectContext>(_ctx, getState());
  enterRule(_localctx, 6, ConfigShorthandParser::RuleObject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(29);
    match(ConfigShorthandParser::T__2);
    setState(30);
    assignments();
    setState(31);
    match(ConfigShorthandParser::T__3);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ListContext
//------------------------------------------------------------------

ConfigShorthandParser::ListContext::ListContext(ParserRuleContext* parent,
                                                size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<ConfigShorthandParser::ContentContext*>
ConfigShorthandParser::ListContext::content() {
  return getRuleContexts<ConfigShorthandParser::ContentContext>();
}

ConfigShorthandParser::ContentContext*
ConfigShorthandParser::ListContext::content(size_t i) {
  return getRuleContext<ConfigShorthandParser::ContentContext>(i);
}

size_t ConfigShorthandParser::ListContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleList;
}

ConfigShorthandParser::ListContext* ConfigShorthandParser::list() {
  ListContext* _localctx =
      _tracker.createInstance<ListContext>(_ctx, getState());
  enterRule(_localctx, 8, ConfigShorthandParser::RuleList);

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
    setState(33);
    match(ConfigShorthandParser::T__4);
    setState(39);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(34);
        antlrcpp::downCast<ListContext*>(_localctx)->contentContext = content();
        antlrcpp::downCast<ListContext*>(_localctx)->listElement.push_back(
            antlrcpp::downCast<ListContext*>(_localctx)->contentContext);
        setState(35);
        match(ConfigShorthandParser::T__0);
      }
      setState(41);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       1, _ctx);
    }
    setState(42);
    antlrcpp::downCast<ListContext*>(_localctx)->contentContext = content();
    antlrcpp::downCast<ListContext*>(_localctx)->listElement.push_back(
        antlrcpp::downCast<ListContext*>(_localctx)->contentContext);
    setState(43);
    match(ConfigShorthandParser::T__5);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ContentContext
//------------------------------------------------------------------

ConfigShorthandParser::ContentContext::ContentContext(ParserRuleContext* parent,
                                                      size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* ConfigShorthandParser::ContentContext::LITERAL() {
  return getToken(ConfigShorthandParser::LITERAL, 0);
}

ConfigShorthandParser::ListContext*
ConfigShorthandParser::ContentContext::list() {
  return getRuleContext<ConfigShorthandParser::ListContext>(0);
}

ConfigShorthandParser::ObjectContext*
ConfigShorthandParser::ContentContext::object() {
  return getRuleContext<ConfigShorthandParser::ObjectContext>(0);
}

size_t ConfigShorthandParser::ContentContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleContent;
}

ConfigShorthandParser::ContentContext* ConfigShorthandParser::content() {
  ContentContext* _localctx =
      _tracker.createInstance<ContentContext>(_ctx, getState());
  enterRule(_localctx, 10, ConfigShorthandParser::RuleContent);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(48);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ConfigShorthandParser::LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(45);
        match(ConfigShorthandParser::LITERAL);
        break;
      }

      case ConfigShorthandParser::T__4: {
        enterOuterAlt(_localctx, 2);
        setState(46);
        list();
        break;
      }

      case ConfigShorthandParser::T__2: {
        enterOuterAlt(_localctx, 3);
        setState(47);
        object();
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

void ConfigShorthandParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  configshorthandParserInitialize();
#else
  ::antlr4::internal::call_once(configshorthandParserOnceFlag,
                                configshorthandParserInitialize);
#endif
}
