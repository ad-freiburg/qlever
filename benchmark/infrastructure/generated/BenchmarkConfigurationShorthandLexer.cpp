
// Generated from BenchmarkConfigurationShorthand.g4 by ANTLR 4.12.0

#include "BenchmarkConfigurationShorthandLexer.h"

using namespace antlr4;

using namespace antlr4;

namespace {

struct BenchmarkConfigurationShorthandLexerStaticData final {
  BenchmarkConfigurationShorthandLexerStaticData(
      std::vector<std::string> ruleNames, std::vector<std::string> channelNames,
      std::vector<std::string> modeNames, std::vector<std::string> literalNames,
      std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        channelNames(std::move(channelNames)),
        modeNames(std::move(modeNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  BenchmarkConfigurationShorthandLexerStaticData(
      const BenchmarkConfigurationShorthandLexerStaticData&) = delete;
  BenchmarkConfigurationShorthandLexerStaticData(
      BenchmarkConfigurationShorthandLexerStaticData&&) = delete;
  BenchmarkConfigurationShorthandLexerStaticData& operator=(
      const BenchmarkConfigurationShorthandLexerStaticData&) = delete;
  BenchmarkConfigurationShorthandLexerStaticData& operator=(
      BenchmarkConfigurationShorthandLexerStaticData&&) = delete;

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

::antlr4::internal::OnceFlag benchmarkconfigurationshorthandlexerLexerOnceFlag;
BenchmarkConfigurationShorthandLexerStaticData*
    benchmarkconfigurationshorthandlexerLexerStaticData = nullptr;

void benchmarkconfigurationshorthandlexerLexerInitialize() {
  assert(benchmarkconfigurationshorthandlexerLexerStaticData == nullptr);
  auto staticData =
      std::make_unique<BenchmarkConfigurationShorthandLexerStaticData>(
          std::vector<std::string>{"T__0", "T__1", "T__2", "T__3", "T__4",
                                   "T__5", "LITERAL", "BOOL", "INTEGER",
                                   "FLOAT", "STRING", "NAME", "WHITESPACE"},
          std::vector<std::string>{"DEFAULT_TOKEN_CHANNEL", "HIDDEN"},
          std::vector<std::string>{"DEFAULT_MODE"},
          std::vector<std::string>{"", "','", "':'", "'{'", "'}'", "'['",
                                   "']'"},
          std::vector<std::string>{"", "", "", "", "", "", "", "LITERAL",
                                   "BOOL", "INTEGER", "FLOAT", "STRING", "NAME",
                                   "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4,   0,   13,  89, 6,  -1, 2,   0,  7,   0,  2,   1,  7,   1,  2,   2,
      7,   2,   2,   3,  7,  3,  2,   4,  7,   4,  2,   5,  7,   5,  2,   6,
      7,   6,   2,   7,  7,  7,  2,   8,  7,   8,  2,   9,  7,   9,  2,   10,
      7,   10,  2,   11, 7,  11, 2,   12, 7,   12, 1,   0,  1,   0,  1,   1,
      1,   1,   1,   2,  1,  2,  1,   3,  1,   3,  1,   4,  1,   4,  1,   5,
      1,   5,   1,   6,  1,  6,  1,   6,  1,   6,  3,   6,  44,  8,  6,   1,
      7,   1,   7,   1,  7,  1,  7,   1,  7,   1,  7,   1,  7,   1,  7,   1,
      7,   3,   7,   55, 8,  7,  1,   8,  3,   8,  58,  8,  8,   1,  8,   4,
      8,   61,  8,   8,  11, 8,  12,  8,  62,  1,  9,   1,  9,   1,  9,   4,
      9,   68,  8,   9,  11, 9,  12,  9,  69,  1,  10,  1,  10,  5,  10,  74,
      8,   10,  10,  10, 12, 10, 77,  9,  10,  1,  10,  1,  10,  1,  11,  4,
      11,  82,  8,   11, 11, 11, 12,  11, 83,  1,  12,  1,  12,  1,  12,  1,
      12,  1,   75,  0,  13, 1,  1,   3,  2,   5,  3,   7,  4,   9,  5,   11,
      6,   13,  7,   15, 8,  17, 9,   19, 10,  21, 11,  23, 12,  25, 13,  1,
      0,   3,   1,   0,  48, 57, 5,   0,  45,  45, 48,  57, 65,  90, 95,  95,
      97,  122, 2,   0,  9,  9,  32,  32, 97,  0,  1,   1,  0,   0,  0,   0,
      3,   1,   0,   0,  0,  0,  5,   1,  0,   0,  0,   0,  7,   1,  0,   0,
      0,   0,   9,   1,  0,  0,  0,   0,  11,  1,  0,   0,  0,   0,  13,  1,
      0,   0,   0,   0,  15, 1,  0,   0,  0,   0,  17,  1,  0,   0,  0,   0,
      19,  1,   0,   0,  0,  0,  21,  1,  0,   0,  0,   0,  23,  1,  0,   0,
      0,   0,   25,  1,  0,  0,  0,   1,  27,  1,  0,   0,  0,   3,  29,  1,
      0,   0,   0,   5,  31, 1,  0,   0,  0,   7,  33,  1,  0,   0,  0,   9,
      35,  1,   0,   0,  0,  11, 37,  1,  0,   0,  0,   13, 43,  1,  0,   0,
      0,   15,  54,  1,  0,  0,  0,   17, 57,  1,  0,   0,  0,   19, 64,  1,
      0,   0,   0,   21, 71, 1,  0,   0,  0,   23, 81,  1,  0,   0,  0,   25,
      85,  1,   0,   0,  0,  27, 28,  5,  44,  0,  0,   28, 2,   1,  0,   0,
      0,   29,  30,  5,  58, 0,  0,   30, 4,   1,  0,   0,  0,   31, 32,  5,
      123, 0,   0,   32, 6,  1,  0,   0,  0,   33, 34,  5,  125, 0,  0,   34,
      8,   1,   0,   0,  0,  35, 36,  5,  91,  0,  0,   36, 10,  1,  0,   0,
      0,   37,  38,  5,  93, 0,  0,   38, 12,  1,  0,   0,  0,   39, 44,  3,
      15,  7,   0,   40, 44, 3,  17,  8,  0,   41, 44,  3,  19,  9,  0,   42,
      44,  3,   21,  10, 0,  43, 39,  1,  0,   0,  0,   43, 40,  1,  0,   0,
      0,   43,  41,  1,  0,  0,  0,   43, 42,  1,  0,   0,  0,   44, 14,  1,
      0,   0,   0,   45, 46, 5,  116, 0,  0,   46, 47,  5,  114, 0,  0,   47,
      48,  5,   117, 0,  0,  48, 55,  5,  101, 0,  0,   49, 50,  5,  102, 0,
      0,   50,  51,  5,  97, 0,  0,   51, 52,  5,  108, 0,  0,   52, 53,  5,
      115, 0,   0,   53, 55, 5,  101, 0,  0,   54, 45,  1,  0,   0,  0,   54,
      49,  1,   0,   0,  0,  55, 16,  1,  0,   0,  0,   56, 58,  5,  45,  0,
      0,   57,  56,  1,  0,  0,  0,   57, 58,  1,  0,   0,  0,   58, 60,  1,
      0,   0,   0,   59, 61, 7,  0,   0,  0,   60, 59,  1,  0,   0,  0,   61,
      62,  1,   0,   0,  0,  62, 60,  1,  0,   0,  0,   62, 63,  1,  0,   0,
      0,   63,  18,  1,  0,  0,  0,   64, 65,  3,  17,  8,  0,   65, 67,  5,
      46,  0,   0,   66, 68, 7,  0,   0,  0,   67, 66,  1,  0,   0,  0,   68,
      69,  1,   0,   0,  0,  69, 67,  1,  0,   0,  0,   69, 70,  1,  0,   0,
      0,   70,  20,  1,  0,  0,  0,   71, 75,  5,  34,  0,  0,   72, 74,  9,
      0,   0,   0,   73, 72, 1,  0,   0,  0,   74, 77,  1,  0,   0,  0,   75,
      76,  1,   0,   0,  0,  75, 73,  1,  0,   0,  0,   76, 78,  1,  0,   0,
      0,   77,  75,  1,  0,  0,  0,   78, 79,  5,  34,  0,  0,   79, 22,  1,
      0,   0,   0,   80, 82, 7,  1,   0,  0,   81, 80,  1,  0,   0,  0,   82,
      83,  1,   0,   0,  0,  83, 81,  1,  0,   0,  0,   83, 84,  1,  0,   0,
      0,   84,  24,  1,  0,  0,  0,   85, 86,  7,  2,   0,  0,   86, 87,  1,
      0,   0,   0,   87, 88, 6,  12,  0,  0,   88, 26,  1,  0,   0,  0,   8,
      0,   43,  54,  57, 62, 69, 75,  83, 1,   6,  0,   0};
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
  benchmarkconfigurationshorthandlexerLexerStaticData = staticData.release();
}

}  // namespace

BenchmarkConfigurationShorthandLexer::BenchmarkConfigurationShorthandLexer(
    CharStream* input)
    : Lexer(input) {
  BenchmarkConfigurationShorthandLexer::initialize();
  _interpreter = new atn::LexerATNSimulator(
      this, *benchmarkconfigurationshorthandlexerLexerStaticData->atn,
      benchmarkconfigurationshorthandlexerLexerStaticData->decisionToDFA,
      benchmarkconfigurationshorthandlexerLexerStaticData->sharedContextCache);
}

BenchmarkConfigurationShorthandLexer::~BenchmarkConfigurationShorthandLexer() {
  delete _interpreter;
}

std::string BenchmarkConfigurationShorthandLexer::getGrammarFileName() const {
  return "BenchmarkConfigurationShorthand.g4";
}

const std::vector<std::string>&
BenchmarkConfigurationShorthandLexer::getRuleNames() const {
  return benchmarkconfigurationshorthandlexerLexerStaticData->ruleNames;
}

const std::vector<std::string>&
BenchmarkConfigurationShorthandLexer::getChannelNames() const {
  return benchmarkconfigurationshorthandlexerLexerStaticData->channelNames;
}

const std::vector<std::string>&
BenchmarkConfigurationShorthandLexer::getModeNames() const {
  return benchmarkconfigurationshorthandlexerLexerStaticData->modeNames;
}

const dfa::Vocabulary& BenchmarkConfigurationShorthandLexer::getVocabulary()
    const {
  return benchmarkconfigurationshorthandlexerLexerStaticData->vocabulary;
}

antlr4::atn::SerializedATNView
BenchmarkConfigurationShorthandLexer::getSerializedATN() const {
  return benchmarkconfigurationshorthandlexerLexerStaticData->serializedATN;
}

const atn::ATN& BenchmarkConfigurationShorthandLexer::getATN() const {
  return *benchmarkconfigurationshorthandlexerLexerStaticData->atn;
}

void BenchmarkConfigurationShorthandLexer::initialize() {
  ::antlr4::internal::call_once(
      benchmarkconfigurationshorthandlexerLexerOnceFlag,
      benchmarkconfigurationshorthandlexerLexerInitialize);
}
