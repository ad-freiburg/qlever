
// Generated from ConfigShorthand.g4 by ANTLR 4.13.0



#include "ConfigShorthandParser.h"


using namespace antlrcpp;

using namespace antlr4;

namespace {

struct ConfigShorthandParserStaticData final {
  ConfigShorthandParserStaticData(std::vector<std::string> ruleNames,
                        std::vector<std::string> literalNames,
                        std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  ConfigShorthandParserStaticData(const ConfigShorthandParserStaticData&) = delete;
  ConfigShorthandParserStaticData(ConfigShorthandParserStaticData&&) = delete;
  ConfigShorthandParserStaticData& operator=(const ConfigShorthandParserStaticData&) = delete;
  ConfigShorthandParserStaticData& operator=(ConfigShorthandParserStaticData&&) = delete;

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
ConfigShorthandParserStaticData *configshorthandParserStaticData = nullptr;

void configshorthandParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (configshorthandParserStaticData != nullptr) {
    return;
  }
#else
  assert(configshorthandParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<ConfigShorthandParserStaticData>(
    std::vector<std::string>{
      "isName", "shortHandString", "assignments", "assignment", "object", 
      "list", "content"
    },
    std::vector<std::string>{
      "", "','", "':'", "'{'", "'}'", "'['", "']'"
    },
    std::vector<std::string>{
      "", "", "", "", "", "", "", "LITERAL", "BOOL", "INTEGER", "FLOAT", 
      "STRING", "NAME", "WHITESPACE"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,13,56,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,6,1,0,
  	1,0,1,0,1,1,1,1,1,1,1,2,1,2,1,2,5,2,24,8,2,10,2,12,2,27,9,2,1,2,1,2,1,
  	3,1,3,1,3,1,3,1,4,1,4,1,4,1,4,1,5,1,5,1,5,1,5,5,5,43,8,5,10,5,12,5,46,
  	9,5,1,5,1,5,1,5,1,6,1,6,1,6,3,6,54,8,6,1,6,0,0,7,0,2,4,6,8,10,12,0,0,
  	52,0,14,1,0,0,0,2,17,1,0,0,0,4,25,1,0,0,0,6,30,1,0,0,0,8,34,1,0,0,0,10,
  	38,1,0,0,0,12,53,1,0,0,0,14,15,5,12,0,0,15,16,5,0,0,1,16,1,1,0,0,0,17,
  	18,3,4,2,0,18,19,5,0,0,1,19,3,1,0,0,0,20,21,3,6,3,0,21,22,5,1,0,0,22,
  	24,1,0,0,0,23,20,1,0,0,0,24,27,1,0,0,0,25,23,1,0,0,0,25,26,1,0,0,0,26,
  	28,1,0,0,0,27,25,1,0,0,0,28,29,3,6,3,0,29,5,1,0,0,0,30,31,5,12,0,0,31,
  	32,5,2,0,0,32,33,3,12,6,0,33,7,1,0,0,0,34,35,5,3,0,0,35,36,3,4,2,0,36,
  	37,5,4,0,0,37,9,1,0,0,0,38,44,5,5,0,0,39,40,3,12,6,0,40,41,5,1,0,0,41,
  	43,1,0,0,0,42,39,1,0,0,0,43,46,1,0,0,0,44,42,1,0,0,0,44,45,1,0,0,0,45,
  	47,1,0,0,0,46,44,1,0,0,0,47,48,3,12,6,0,48,49,5,6,0,0,49,11,1,0,0,0,50,
  	54,5,7,0,0,51,54,3,10,5,0,52,54,3,8,4,0,53,50,1,0,0,0,53,51,1,0,0,0,53,
  	52,1,0,0,0,54,13,1,0,0,0,3,25,44,53
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  configshorthandParserStaticData = staticData.release();
}

}

ConfigShorthandParser::ConfigShorthandParser(TokenStream *input) : ConfigShorthandParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

ConfigShorthandParser::ConfigShorthandParser(TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options) : Parser(input) {
  ConfigShorthandParser::initialize();
  _interpreter = new atn::ParserATNSimulator(this, *configshorthandParserStaticData->atn, configshorthandParserStaticData->decisionToDFA, configshorthandParserStaticData->sharedContextCache, options);
}

ConfigShorthandParser::~ConfigShorthandParser() {
  delete _interpreter;
}

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


//----------------- IsNameContext ------------------------------------------------------------------

ConfigShorthandParser::IsNameContext::IsNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ConfigShorthandParser::IsNameContext::NAME() {
  return getToken(ConfigShorthandParser::NAME, 0);
}

tree::TerminalNode* ConfigShorthandParser::IsNameContext::EOF() {
  return getToken(ConfigShorthandParser::EOF, 0);
}


size_t ConfigShorthandParser::IsNameContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleIsName;
}


ConfigShorthandParser::IsNameContext* ConfigShorthandParser::isName() {
  IsNameContext *_localctx = _tracker.createInstance<IsNameContext>(_ctx, getState());
  enterRule(_localctx, 0, ConfigShorthandParser::RuleIsName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(14);
    match(ConfigShorthandParser::NAME);
    setState(15);
    match(ConfigShorthandParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ShortHandStringContext ------------------------------------------------------------------

ConfigShorthandParser::ShortHandStringContext::ShortHandStringContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ConfigShorthandParser::AssignmentsContext* ConfigShorthandParser::ShortHandStringContext::assignments() {
  return getRuleContext<ConfigShorthandParser::AssignmentsContext>(0);
}

tree::TerminalNode* ConfigShorthandParser::ShortHandStringContext::EOF() {
  return getToken(ConfigShorthandParser::EOF, 0);
}


size_t ConfigShorthandParser::ShortHandStringContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleShortHandString;
}


ConfigShorthandParser::ShortHandStringContext* ConfigShorthandParser::shortHandString() {
  ShortHandStringContext *_localctx = _tracker.createInstance<ShortHandStringContext>(_ctx, getState());
  enterRule(_localctx, 2, ConfigShorthandParser::RuleShortHandString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(17);
    assignments();
    setState(18);
    match(ConfigShorthandParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentsContext ------------------------------------------------------------------

ConfigShorthandParser::AssignmentsContext::AssignmentsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ConfigShorthandParser::AssignmentContext *> ConfigShorthandParser::AssignmentsContext::assignment() {
  return getRuleContexts<ConfigShorthandParser::AssignmentContext>();
}

ConfigShorthandParser::AssignmentContext* ConfigShorthandParser::AssignmentsContext::assignment(size_t i) {
  return getRuleContext<ConfigShorthandParser::AssignmentContext>(i);
}


size_t ConfigShorthandParser::AssignmentsContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleAssignments;
}


ConfigShorthandParser::AssignmentsContext* ConfigShorthandParser::assignments() {
  AssignmentsContext *_localctx = _tracker.createInstance<AssignmentsContext>(_ctx, getState());
  enterRule(_localctx, 4, ConfigShorthandParser::RuleAssignments);

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
    setState(25);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(20);
        antlrcpp::downCast<AssignmentsContext *>(_localctx)->assignmentContext = assignment();
        antlrcpp::downCast<AssignmentsContext *>(_localctx)->listOfAssignments.push_back(antlrcpp::downCast<AssignmentsContext *>(_localctx)->assignmentContext);
        setState(21);
        match(ConfigShorthandParser::T__0); 
      }
      setState(27);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx);
    }
    setState(28);
    antlrcpp::downCast<AssignmentsContext *>(_localctx)->assignmentContext = assignment();
    antlrcpp::downCast<AssignmentsContext *>(_localctx)->listOfAssignments.push_back(antlrcpp::downCast<AssignmentsContext *>(_localctx)->assignmentContext);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentContext ------------------------------------------------------------------

ConfigShorthandParser::AssignmentContext::AssignmentContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ConfigShorthandParser::AssignmentContext::NAME() {
  return getToken(ConfigShorthandParser::NAME, 0);
}

ConfigShorthandParser::ContentContext* ConfigShorthandParser::AssignmentContext::content() {
  return getRuleContext<ConfigShorthandParser::ContentContext>(0);
}


size_t ConfigShorthandParser::AssignmentContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleAssignment;
}


ConfigShorthandParser::AssignmentContext* ConfigShorthandParser::assignment() {
  AssignmentContext *_localctx = _tracker.createInstance<AssignmentContext>(_ctx, getState());
  enterRule(_localctx, 6, ConfigShorthandParser::RuleAssignment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(30);
    match(ConfigShorthandParser::NAME);
    setState(31);
    match(ConfigShorthandParser::T__1);
    setState(32);
    content();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectContext ------------------------------------------------------------------

ConfigShorthandParser::ObjectContext::ObjectContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

ConfigShorthandParser::AssignmentsContext* ConfigShorthandParser::ObjectContext::assignments() {
  return getRuleContext<ConfigShorthandParser::AssignmentsContext>(0);
}


size_t ConfigShorthandParser::ObjectContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleObject;
}


ConfigShorthandParser::ObjectContext* ConfigShorthandParser::object() {
  ObjectContext *_localctx = _tracker.createInstance<ObjectContext>(_ctx, getState());
  enterRule(_localctx, 8, ConfigShorthandParser::RuleObject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(34);
    match(ConfigShorthandParser::T__2);
    setState(35);
    assignments();
    setState(36);
    match(ConfigShorthandParser::T__3);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ListContext ------------------------------------------------------------------

ConfigShorthandParser::ListContext::ListContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<ConfigShorthandParser::ContentContext *> ConfigShorthandParser::ListContext::content() {
  return getRuleContexts<ConfigShorthandParser::ContentContext>();
}

ConfigShorthandParser::ContentContext* ConfigShorthandParser::ListContext::content(size_t i) {
  return getRuleContext<ConfigShorthandParser::ContentContext>(i);
}


size_t ConfigShorthandParser::ListContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleList;
}


ConfigShorthandParser::ListContext* ConfigShorthandParser::list() {
  ListContext *_localctx = _tracker.createInstance<ListContext>(_ctx, getState());
  enterRule(_localctx, 10, ConfigShorthandParser::RuleList);

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
    setState(38);
    match(ConfigShorthandParser::T__4);
    setState(44);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(39);
        antlrcpp::downCast<ListContext *>(_localctx)->contentContext = content();
        antlrcpp::downCast<ListContext *>(_localctx)->listElement.push_back(antlrcpp::downCast<ListContext *>(_localctx)->contentContext);
        setState(40);
        match(ConfigShorthandParser::T__0); 
      }
      setState(46);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1, _ctx);
    }
    setState(47);
    antlrcpp::downCast<ListContext *>(_localctx)->contentContext = content();
    antlrcpp::downCast<ListContext *>(_localctx)->listElement.push_back(antlrcpp::downCast<ListContext *>(_localctx)->contentContext);
    setState(48);
    match(ConfigShorthandParser::T__5);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ContentContext ------------------------------------------------------------------

ConfigShorthandParser::ContentContext::ContentContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* ConfigShorthandParser::ContentContext::LITERAL() {
  return getToken(ConfigShorthandParser::LITERAL, 0);
}

ConfigShorthandParser::ListContext* ConfigShorthandParser::ContentContext::list() {
  return getRuleContext<ConfigShorthandParser::ListContext>(0);
}

ConfigShorthandParser::ObjectContext* ConfigShorthandParser::ContentContext::object() {
  return getRuleContext<ConfigShorthandParser::ObjectContext>(0);
}


size_t ConfigShorthandParser::ContentContext::getRuleIndex() const {
  return ConfigShorthandParser::RuleContent;
}


ConfigShorthandParser::ContentContext* ConfigShorthandParser::content() {
  ContentContext *_localctx = _tracker.createInstance<ContentContext>(_ctx, getState());
  enterRule(_localctx, 12, ConfigShorthandParser::RuleContent);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(53);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case ConfigShorthandParser::LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(50);
        match(ConfigShorthandParser::LITERAL);
        break;
      }

      case ConfigShorthandParser::T__4: {
        enterOuterAlt(_localctx, 2);
        setState(51);
        list();
        break;
      }

      case ConfigShorthandParser::T__2: {
        enterOuterAlt(_localctx, 3);
        setState(52);
        object();
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

void ConfigShorthandParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  configshorthandParserInitialize();
#else
  ::antlr4::internal::call_once(configshorthandParserOnceFlag, configshorthandParserInitialize);
#endif
}
