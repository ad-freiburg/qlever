
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
    BASE = 30,
    PREFIX = 31,
    SELECT = 32,
    DISTINCT = 33,
    REDUCED = 34,
    AS = 35,
    CONSTRUCT = 36,
    WHERE = 37,
    DESCRIBE = 38,
    ASK = 39,
    FROM = 40,
    NAMED = 41,
    GROUPBY = 42,
    GROUP_CONCAT = 43,
    HAVING = 44,
    ORDERBY = 45,
    ASC = 46,
    DESC = 47,
    LIMIT = 48,
    OFFSET = 49,
    TEXTLIMIT = 50,
    VALUES = 51,
    LOAD = 52,
    SILENT = 53,
    CLEAR = 54,
    DROP = 55,
    CREATE = 56,
    ADD = 57,
    DATA = 58,
    MOVE = 59,
    COPY = 60,
    INSERT = 61,
    DELETE = 62,
    WITH = 63,
    USING = 64,
    DEFAULT = 65,
    GRAPH = 66,
    ALL = 67,
    OPTIONAL = 68,
    SERVICE = 69,
    BIND = 70,
    UNDEF = 71,
    MINUS = 72,
    UNION = 73,
    FILTER = 74,
    NOT = 75,
    IN = 76,
    STR = 77,
    LANG = 78,
    LANGMATCHES = 79,
    DATATYPE = 80,
    BOUND = 81,
    IRI = 82,
    URI = 83,
    BNODE = 84,
    RAND = 85,
    ABS = 86,
    CEIL = 87,
    FLOOR = 88,
    ROUND = 89,
    CONCAT = 90,
    STRLEN = 91,
    UCASE = 92,
    LCASE = 93,
    ENCODE = 94,
    FOR = 95,
    CONTAINS = 96,
    STRSTARTS = 97,
    STRENDS = 98,
    STRBEFORE = 99,
    STRAFTER = 100,
    YEAR = 101,
    MONTH = 102,
    DAY = 103,
    HOURS = 104,
    MINUTES = 105,
    SECONDS = 106,
    TIMEZONE = 107,
    TZ = 108,
    NOW = 109,
    UUID = 110,
    STRUUID = 111,
    SHA1 = 112,
    SHA256 = 113,
    SHA384 = 114,
    SHA512 = 115,
    MD5 = 116,
    COALESCE = 117,
    IF = 118,
    STRLANG = 119,
    STRDT = 120,
    SAMETERM = 121,
    ISIRI = 122,
    ISURI = 123,
    ISBLANK = 124,
    ISLITERAL = 125,
    ISNUMERIC = 126,
    REGEX = 127,
    SUBSTR = 128,
    REPLACE = 129,
    EXISTS = 130,
    COUNT = 131,
    SUM = 132,
    MIN = 133,
    MAX = 134,
    AVG = 135,
    SAMPLE = 136,
    SEPARATOR = 137,
    IRI_REF = 138,
    PNAME_NS = 139,
    PNAME_LN = 140,
    BLANK_NODE_LABEL = 141,
    VAR1 = 142,
    VAR2 = 143,
    LANGTAG = 144,
    PREFIX_LANGTAG = 145,
    INTEGER = 146,
    DECIMAL = 147,
    DOUBLE = 148,
    INTEGER_POSITIVE = 149,
    DECIMAL_POSITIVE = 150,
    DOUBLE_POSITIVE = 151,
    INTEGER_NEGATIVE = 152,
    DECIMAL_NEGATIVE = 153,
    DOUBLE_NEGATIVE = 154,
    EXPONENT = 155,
    STRING_LITERAL1 = 156,
    STRING_LITERAL2 = 157,
    STRING_LITERAL_LONG1 = 158,
    STRING_LITERAL_LONG2 = 159,
    ECHAR = 160,
    NIL = 161,
    ANON = 162,
    PN_CHARS_U = 163,
    VARNAME = 164,
    PN_PREFIX = 165,
    PN_LOCAL = 166,
    PLX = 167,
    PERCENT = 168,
    HEX = 169,
    PN_LOCAL_ESC = 170,
    WS = 171,
    COMMENTS = 172
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
