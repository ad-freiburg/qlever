
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include "antlr4-runtime.h"

class SparqlAutomaticLexer : public antlr4::Lexer {
 public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    T__3 = 4,
    T__4 = 5,
    T__5 = 6,
    T__6 = 7,
    T__7 = 8,
    T__8 = 9,
    T__9 = 10,
    T__10 = 11,
    T__11 = 12,
    T__12 = 13,
    T__13 = 14,
    T__14 = 15,
    T__15 = 16,
    T__16 = 17,
    T__17 = 18,
    T__18 = 19,
    T__19 = 20,
    T__20 = 21,
    T__21 = 22,
    T__22 = 23,
    T__23 = 24,
    T__24 = 25,
    T__25 = 26,
    T__26 = 27,
    T__27 = 28,
    T__28 = 29,
    T__29 = 30,
    T__30 = 31,
    DIST = 32,
    SQR = 33,
    BASE = 34,
    PREFIX = 35,
    SELECT = 36,
    DISTINCT = 37,
    REDUCED = 38,
    AS = 39,
    CONSTRUCT = 40,
    WHERE = 41,
    DESCRIBE = 42,
    ASK = 43,
    FROM = 44,
    NAMED = 45,
    GROUPBY = 46,
    GROUP_CONCAT = 47,
    HAVING = 48,
    ORDERBY = 49,
    ASC = 50,
    DESC = 51,
    LIMIT = 52,
    OFFSET = 53,
    VALUES = 54,
    LOAD = 55,
    SILENT = 56,
    CLEAR = 57,
    DROP = 58,
    CREATE = 59,
    ADD = 60,
    DATA = 61,
    MOVE = 62,
    COPY = 63,
    INSERT = 64,
    DELETE = 65,
    WITH = 66,
    USING = 67,
    DEFAULT = 68,
    GRAPH = 69,
    ALL = 70,
    OPTIONAL = 71,
    SERVICE = 72,
    BIND = 73,
    UNDEF = 74,
    MINUS = 75,
    UNION = 76,
    FILTER = 77,
    NOT = 78,
    IN = 79,
    STR = 80,
    LANG = 81,
    LANGMATCHES = 82,
    DATATYPE = 83,
    BOUND = 84,
    IRI = 85,
    URI = 86,
    BNODE = 87,
    RAND = 88,
    ABS = 89,
    CEIL = 90,
    FLOOR = 91,
    ROUND = 92,
    CONCAT = 93,
    STRLEN = 94,
    UCASE = 95,
    LCASE = 96,
    ENCODE = 97,
    FOR = 98,
    CONTAINS = 99,
    STRSTARTS = 100,
    STRENDS = 101,
    STRBEFORE = 102,
    STRAFTER = 103,
    YEAR = 104,
    MONTH = 105,
    DAY = 106,
    HOURS = 107,
    MINUTES = 108,
    SECONDS = 109,
    TIMEZONE = 110,
    TZ = 111,
    NOW = 112,
    UUID = 113,
    STRUUID = 114,
    SHA1 = 115,
    SHA256 = 116,
    SHA384 = 117,
    SHA512 = 118,
    MD5 = 119,
    COALESCE = 120,
    IF = 121,
    STRLANG = 122,
    STRDT = 123,
    SAMETERM = 124,
    ISIRI = 125,
    ISURI = 126,
    ISBLANK = 127,
    ISLITERAL = 128,
    ISNUMERIC = 129,
    REGEX = 130,
    SUBSTR = 131,
    REPLACE = 132,
    EXISTS = 133,
    COUNT = 134,
    SUM = 135,
    MIN = 136,
    MAX = 137,
    AVG = 138,
    SAMPLE = 139,
    SEPARATOR = 140,
    IRI_REF = 141,
    PNAME_NS = 142,
    PNAME_LN = 143,
    BLANK_NODE_LABEL = 144,
    VAR1 = 145,
    VAR2 = 146,
    LANGTAG = 147,
    INTEGER = 148,
    DECIMAL = 149,
    DOUBLE = 150,
    INTEGER_POSITIVE = 151,
    DECIMAL_POSITIVE = 152,
    DOUBLE_POSITIVE = 153,
    INTEGER_NEGATIVE = 154,
    DECIMAL_NEGATIVE = 155,
    DOUBLE_NEGATIVE = 156,
    EXPONENT = 157,
    STRING_LITERAL1 = 158,
    STRING_LITERAL2 = 159,
    STRING_LITERAL_LONG1 = 160,
    STRING_LITERAL_LONG2 = 161,
    ECHAR = 162,
    NIL = 163,
    ANON = 164,
    PN_CHARS_U = 165,
    VARNAME = 166,
    PN_PREFIX = 167,
    PN_LOCAL = 168,
    PLX = 169,
    PERCENT = 170,
    HEX = 171,
    PN_LOCAL_ESC = 172,
    WS = 173,
    COMMENTS = 174
  };

  explicit SparqlAutomaticLexer(antlr4::CharStream* input);
  ~SparqlAutomaticLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames()
      const override;  // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

 private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};
