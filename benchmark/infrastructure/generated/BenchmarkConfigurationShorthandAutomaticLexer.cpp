
// Generated from BenchmarkConfigurationShorthandAutomatic.g4 by ANTLR 4.12.0


#include "BenchmarkConfigurationShorthandAutomaticLexer.h"


using namespace antlr4;



using namespace antlr4;

namespace {

struct BenchmarkConfigurationShorthandAutomaticLexerStaticData final {
  BenchmarkConfigurationShorthandAutomaticLexerStaticData(std::vector<std::string> ruleNames,
                          std::vector<std::string> channelNames,
                          std::vector<std::string> modeNames,
                          std::vector<std::string> literalNames,
                          std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)), channelNames(std::move(channelNames)),
        modeNames(std::move(modeNames)), literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  BenchmarkConfigurationShorthandAutomaticLexerStaticData(const BenchmarkConfigurationShorthandAutomaticLexerStaticData&) = delete;
  BenchmarkConfigurationShorthandAutomaticLexerStaticData(BenchmarkConfigurationShorthandAutomaticLexerStaticData&&) = delete;
  BenchmarkConfigurationShorthandAutomaticLexerStaticData& operator=(const BenchmarkConfigurationShorthandAutomaticLexerStaticData&) = delete;
  BenchmarkConfigurationShorthandAutomaticLexerStaticData& operator=(BenchmarkConfigurationShorthandAutomaticLexerStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> channelNames;
  const std::vector<std::string> modeNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag benchmarkconfigurationshorthandautomaticlexerLexerOnceFlag;
BenchmarkConfigurationShorthandAutomaticLexerStaticData *benchmarkconfigurationshorthandautomaticlexerLexerStaticData = nullptr;

void benchmarkconfigurationshorthandautomaticlexerLexerInitialize() {
  assert(benchmarkconfigurationshorthandautomaticlexerLexerStaticData == nullptr);
  auto staticData = std::make_unique<BenchmarkConfigurationShorthandAutomaticLexerStaticData>(
    std::vector<std::string>{
      "T__0", "T__1", "T__2", "T__3", "T__4", "LITERAL", "BOOL", "INTEGER", 
      "FLOAT", "STRING", "NAME", "WHITESPACE"
    },
    std::vector<std::string>{
      "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    },
    std::vector<std::string>{
      "DEFAULT_MODE"
    },
    std::vector<std::string>{
      "", "'='", "';'", "'['", "','", "']'"
    },
    std::vector<std::string>{
      "", "", "", "", "", "", "LITERAL", "BOOL", "INTEGER", "FLOAT", "STRING", 
      "NAME", "WHITESPACE"
    }
  );
  static const int32_t serializedATNSegment[] = {
  	4,0,12,85,6,-1,2,0,7,0,2,1,7,1,2,2,7,2,2,3,7,3,2,4,7,4,2,5,7,5,2,6,7,
  	6,2,7,7,7,2,8,7,8,2,9,7,9,2,10,7,10,2,11,7,11,1,0,1,0,1,1,1,1,1,2,1,2,
  	1,3,1,3,1,4,1,4,1,5,1,5,1,5,1,5,3,5,40,8,5,1,6,1,6,1,6,1,6,1,6,1,6,1,
  	6,1,6,1,6,3,6,51,8,6,1,7,3,7,54,8,7,1,7,4,7,57,8,7,11,7,12,7,58,1,8,1,
  	8,1,8,4,8,64,8,8,11,8,12,8,65,1,9,1,9,5,9,70,8,9,10,9,12,9,73,9,9,1,9,
  	1,9,1,10,4,10,78,8,10,11,10,12,10,79,1,11,1,11,1,11,1,11,1,71,0,12,1,
  	1,3,2,5,3,7,4,9,5,11,6,13,7,15,8,17,9,19,10,21,11,23,12,1,0,3,1,0,48,
  	57,5,0,45,45,48,57,65,90,95,95,97,122,2,0,9,9,32,32,93,0,1,1,0,0,0,0,
  	3,1,0,0,0,0,5,1,0,0,0,0,7,1,0,0,0,0,9,1,0,0,0,0,11,1,0,0,0,0,13,1,0,0,
  	0,0,15,1,0,0,0,0,17,1,0,0,0,0,19,1,0,0,0,0,21,1,0,0,0,0,23,1,0,0,0,1,
  	25,1,0,0,0,3,27,1,0,0,0,5,29,1,0,0,0,7,31,1,0,0,0,9,33,1,0,0,0,11,39,
  	1,0,0,0,13,50,1,0,0,0,15,53,1,0,0,0,17,60,1,0,0,0,19,67,1,0,0,0,21,77,
  	1,0,0,0,23,81,1,0,0,0,25,26,5,61,0,0,26,2,1,0,0,0,27,28,5,59,0,0,28,4,
  	1,0,0,0,29,30,5,91,0,0,30,6,1,0,0,0,31,32,5,44,0,0,32,8,1,0,0,0,33,34,
  	5,93,0,0,34,10,1,0,0,0,35,40,3,13,6,0,36,40,3,15,7,0,37,40,3,17,8,0,38,
  	40,3,19,9,0,39,35,1,0,0,0,39,36,1,0,0,0,39,37,1,0,0,0,39,38,1,0,0,0,40,
  	12,1,0,0,0,41,42,5,116,0,0,42,43,5,114,0,0,43,44,5,117,0,0,44,51,5,101,
  	0,0,45,46,5,102,0,0,46,47,5,97,0,0,47,48,5,108,0,0,48,49,5,115,0,0,49,
  	51,5,101,0,0,50,41,1,0,0,0,50,45,1,0,0,0,51,14,1,0,0,0,52,54,5,45,0,0,
  	53,52,1,0,0,0,53,54,1,0,0,0,54,56,1,0,0,0,55,57,7,0,0,0,56,55,1,0,0,0,
  	57,58,1,0,0,0,58,56,1,0,0,0,58,59,1,0,0,0,59,16,1,0,0,0,60,61,3,15,7,
  	0,61,63,5,46,0,0,62,64,7,0,0,0,63,62,1,0,0,0,64,65,1,0,0,0,65,63,1,0,
  	0,0,65,66,1,0,0,0,66,18,1,0,0,0,67,71,5,34,0,0,68,70,9,0,0,0,69,68,1,
  	0,0,0,70,73,1,0,0,0,71,72,1,0,0,0,71,69,1,0,0,0,72,74,1,0,0,0,73,71,1,
  	0,0,0,74,75,5,34,0,0,75,20,1,0,0,0,76,78,7,1,0,0,77,76,1,0,0,0,78,79,
  	1,0,0,0,79,77,1,0,0,0,79,80,1,0,0,0,80,22,1,0,0,0,81,82,7,2,0,0,82,83,
  	1,0,0,0,83,84,6,11,0,0,84,24,1,0,0,0,8,0,39,50,53,58,65,71,79,1,6,0,0
  };
  staticData->serializedATN = antlr4::atn::SerializedATNView(serializedATNSegment, sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i), i);
  }
  benchmarkconfigurationshorthandautomaticlexerLexerStaticData = staticData.release();
}

}

BenchmarkConfigurationShorthandAutomaticLexer::BenchmarkConfigurationShorthandAutomaticLexer(CharStream *input) : Lexer(input) {
  BenchmarkConfigurationShorthandAutomaticLexer::initialize();
  _interpreter = new atn::LexerATNSimulator(this, *benchmarkconfigurationshorthandautomaticlexerLexerStaticData->atn, benchmarkconfigurationshorthandautomaticlexerLexerStaticData->decisionToDFA, benchmarkconfigurationshorthandautomaticlexerLexerStaticData->sharedContextCache);
}

BenchmarkConfigurationShorthandAutomaticLexer::~BenchmarkConfigurationShorthandAutomaticLexer() {
  delete _interpreter;
}

std::string BenchmarkConfigurationShorthandAutomaticLexer::getGrammarFileName() const {
  return "BenchmarkConfigurationShorthandAutomatic.g4";
}

const std::vector<std::string>& BenchmarkConfigurationShorthandAutomaticLexer::getRuleNames() const {
  return benchmarkconfigurationshorthandautomaticlexerLexerStaticData->ruleNames;
}

const std::vector<std::string>& BenchmarkConfigurationShorthandAutomaticLexer::getChannelNames() const {
  return benchmarkconfigurationshorthandautomaticlexerLexerStaticData->channelNames;
}

const std::vector<std::string>& BenchmarkConfigurationShorthandAutomaticLexer::getModeNames() const {
  return benchmarkconfigurationshorthandautomaticlexerLexerStaticData->modeNames;
}

const dfa::Vocabulary& BenchmarkConfigurationShorthandAutomaticLexer::getVocabulary() const {
  return benchmarkconfigurationshorthandautomaticlexerLexerStaticData->vocabulary;
}

antlr4::atn::SerializedATNView BenchmarkConfigurationShorthandAutomaticLexer::getSerializedATN() const {
  return benchmarkconfigurationshorthandautomaticlexerLexerStaticData->serializedATN;
}

const atn::ATN& BenchmarkConfigurationShorthandAutomaticLexer::getATN() const {
  return *benchmarkconfigurationshorthandautomaticlexerLexerStaticData->atn;
}




void BenchmarkConfigurationShorthandAutomaticLexer::initialize() {
  ::antlr4::internal::call_once(benchmarkconfigurationshorthandautomaticlexerLexerOnceFlag, benchmarkconfigurationshorthandautomaticlexerLexerInitialize);
}
