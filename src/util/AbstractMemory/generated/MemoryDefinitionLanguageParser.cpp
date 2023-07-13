
// Generated from MemoryDefinitionLanguage.g4 by ANTLR 4.13.0



#include "MemoryDefinitionLanguageParser.h"


using namespace antlrcpp;

using namespace antlr4;

namespace {

struct MemoryDefinitionLanguageParserStaticData final {
  MemoryDefinitionLanguageParserStaticData(std::vector<std::string> ruleNames,
                        std::vector<std::string> literalNames,
                        std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  MemoryDefinitionLanguageParserStaticData(const MemoryDefinitionLanguageParserStaticData&) = delete;
  MemoryDefinitionLanguageParserStaticData(MemoryDefinitionLanguageParserStaticData&&) = delete;
  MemoryDefinitionLanguageParserStaticData& operator=(const MemoryDefinitionLanguageParserStaticData&) = delete;
  MemoryDefinitionLanguageParserStaticData& operator=(MemoryDefinitionLanguageParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag memorydefinitionlanguageParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
MemoryDefinitionLanguageParserStaticData *memorydefinitionlanguageParserStaticData = nullptr;

void memorydefinitionlanguageParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (memorydefinitionlanguageParserStaticData != nullptr) {
    return;
  }
#else
  assert(memorydefinitionlanguageParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<MemoryDefinitionLanguageParserStaticData>(
    std::vector<std::string>{
      "memoryDefinitionString", "pureByteDefinition", "memoryUnitDefinition"
    },
    std::vector<std::string>{
    },
    std::vector<std::string>{
      "", "MEMORY_UNIT", "BYTE", "UNSIGNED_INTEGER", "FLOAT", "WHITESPACE"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,1,5,19,2,0,7,0,2,1,7,1,2,2,7,2,1,0,1,0,3,0,9,8,0,1,0,1,0,1,1,1,1,1,
  	1,1,2,1,2,1,2,1,2,0,0,3,0,2,4,0,1,1,0,3,4,16,0,8,1,0,0,0,2,12,1,0,0,0,
  	4,15,1,0,0,0,6,9,3,2,1,0,7,9,3,4,2,0,8,6,1,0,0,0,8,7,1,0,0,0,9,10,1,0,
  	0,0,10,11,5,0,0,1,11,1,1,0,0,0,12,13,5,3,0,0,13,14,5,2,0,0,14,3,1,0,0,
  	0,15,16,7,0,0,0,16,17,5,1,0,0,17,5,1,0,0,0,1,8
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  memorydefinitionlanguageParserStaticData = staticData.release();
}

}

MemoryDefinitionLanguageParser::MemoryDefinitionLanguageParser(TokenStream *input) : MemoryDefinitionLanguageParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

MemoryDefinitionLanguageParser::MemoryDefinitionLanguageParser(TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options) : Parser(input) {
  MemoryDefinitionLanguageParser::initialize();
  _interpreter = new atn::ParserATNSimulator(this, *memorydefinitionlanguageParserStaticData->atn, memorydefinitionlanguageParserStaticData->decisionToDFA, memorydefinitionlanguageParserStaticData->sharedContextCache, options);
}

MemoryDefinitionLanguageParser::~MemoryDefinitionLanguageParser() {
  delete _interpreter;
}

const atn::ATN& MemoryDefinitionLanguageParser::getATN() const {
  return *memorydefinitionlanguageParserStaticData->atn;
}

std::string MemoryDefinitionLanguageParser::getGrammarFileName() const {
  return "MemoryDefinitionLanguage.g4";
}

const std::vector<std::string>& MemoryDefinitionLanguageParser::getRuleNames() const {
  return memorydefinitionlanguageParserStaticData->ruleNames;
}

const dfa::Vocabulary& MemoryDefinitionLanguageParser::getVocabulary() const {
  return memorydefinitionlanguageParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView MemoryDefinitionLanguageParser::getSerializedATN() const {
  return memorydefinitionlanguageParserStaticData->serializedATN;
}


//----------------- MemoryDefinitionStringContext ------------------------------------------------------------------

MemoryDefinitionLanguageParser::MemoryDefinitionStringContext::MemoryDefinitionStringContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* MemoryDefinitionLanguageParser::MemoryDefinitionStringContext::EOF() {
  return getToken(MemoryDefinitionLanguageParser::EOF, 0);
}

MemoryDefinitionLanguageParser::PureByteDefinitionContext* MemoryDefinitionLanguageParser::MemoryDefinitionStringContext::pureByteDefinition() {
  return getRuleContext<MemoryDefinitionLanguageParser::PureByteDefinitionContext>(0);
}

MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext* MemoryDefinitionLanguageParser::MemoryDefinitionStringContext::memoryUnitDefinition() {
  return getRuleContext<MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext>(0);
}


size_t MemoryDefinitionLanguageParser::MemoryDefinitionStringContext::getRuleIndex() const {
  return MemoryDefinitionLanguageParser::RuleMemoryDefinitionString;
}


MemoryDefinitionLanguageParser::MemoryDefinitionStringContext* MemoryDefinitionLanguageParser::memoryDefinitionString() {
  MemoryDefinitionStringContext *_localctx = _tracker.createInstance<MemoryDefinitionStringContext>(_ctx, getState());
  enterRule(_localctx, 0, MemoryDefinitionLanguageParser::RuleMemoryDefinitionString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(8);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx)) {
    case 1: {
      setState(6);
      pureByteDefinition();
      break;
    }

    case 2: {
      setState(7);
      memoryUnitDefinition();
      break;
    }

    default:
      break;
    }
    setState(10);
    match(MemoryDefinitionLanguageParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PureByteDefinitionContext ------------------------------------------------------------------

MemoryDefinitionLanguageParser::PureByteDefinitionContext::PureByteDefinitionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* MemoryDefinitionLanguageParser::PureByteDefinitionContext::UNSIGNED_INTEGER() {
  return getToken(MemoryDefinitionLanguageParser::UNSIGNED_INTEGER, 0);
}

tree::TerminalNode* MemoryDefinitionLanguageParser::PureByteDefinitionContext::BYTE() {
  return getToken(MemoryDefinitionLanguageParser::BYTE, 0);
}


size_t MemoryDefinitionLanguageParser::PureByteDefinitionContext::getRuleIndex() const {
  return MemoryDefinitionLanguageParser::RulePureByteDefinition;
}


MemoryDefinitionLanguageParser::PureByteDefinitionContext* MemoryDefinitionLanguageParser::pureByteDefinition() {
  PureByteDefinitionContext *_localctx = _tracker.createInstance<PureByteDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 2, MemoryDefinitionLanguageParser::RulePureByteDefinition);

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
    match(MemoryDefinitionLanguageParser::UNSIGNED_INTEGER);
    setState(13);
    match(MemoryDefinitionLanguageParser::BYTE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MemoryUnitDefinitionContext ------------------------------------------------------------------

MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext::MemoryUnitDefinitionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext::MEMORY_UNIT() {
  return getToken(MemoryDefinitionLanguageParser::MEMORY_UNIT, 0);
}

tree::TerminalNode* MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext::UNSIGNED_INTEGER() {
  return getToken(MemoryDefinitionLanguageParser::UNSIGNED_INTEGER, 0);
}

tree::TerminalNode* MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext::FLOAT() {
  return getToken(MemoryDefinitionLanguageParser::FLOAT, 0);
}


size_t MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext::getRuleIndex() const {
  return MemoryDefinitionLanguageParser::RuleMemoryUnitDefinition;
}


MemoryDefinitionLanguageParser::MemoryUnitDefinitionContext* MemoryDefinitionLanguageParser::memoryUnitDefinition() {
  MemoryUnitDefinitionContext *_localctx = _tracker.createInstance<MemoryUnitDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 4, MemoryDefinitionLanguageParser::RuleMemoryUnitDefinition);
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
    setState(15);
    _la = _input->LA(1);
    if (!(_la == MemoryDefinitionLanguageParser::UNSIGNED_INTEGER

    || _la == MemoryDefinitionLanguageParser::FLOAT)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(16);
    match(MemoryDefinitionLanguageParser::MEMORY_UNIT);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

void MemoryDefinitionLanguageParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  memorydefinitionlanguageParserInitialize();
#else
  ::antlr4::internal::call_once(memorydefinitionlanguageParserOnceFlag, memorydefinitionlanguageParserInitialize);
#endif
}
