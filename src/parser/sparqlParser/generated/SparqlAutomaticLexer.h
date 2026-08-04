
// Generated from SparqlAutomatic.g4 by ANTLR 4.13.2

#ifndef QLEVER_SRC_PARSER_SPARQLPARSER_GENERATED_SPARQLAUTOMATICLEXER_H
#define QLEVER_SRC_PARSER_SPARQLPARSER_GENERATED_SPARQLAUTOMATICLEXER_H

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
    BASE = 28,
    PREFIX = 29,
    SELECT = 30,
    DISTINCT = 31,
    REDUCED = 32,
    AS = 33,
    CONSTRUCT = 34,
    WHERE = 35,
    DESCRIBE = 36,
    ASK = 37,
    FROM = 38,
    NAMED = 39,
    GROUPBY = 40,
    GROUP_CONCAT = 41,
    HAVING = 42,
    ORDERBY = 43,
    INTERNALSORTBY = 44,
    ASC = 45,
    DESC = 46,
    LIMIT = 47,
    OFFSET = 48,
    TEXTLIMIT = 49,
    VALUES = 50,
    LOAD = 51,
    SILENT = 52,
    INTO = 53,
    CLEAR = 54,
    DROP = 55,
    CREATE = 56,
    ADD = 57,
    TO = 58,
    DATA = 59,
    MOVE = 60,
    COPY = 61,
    INSERT = 62,
    DELETE = 63,
    WITH = 64,
    USING = 65,
    DEFAULT = 66,
    GRAPH = 67,
    ALL = 68,
    OPTIONAL = 69,
    SERVICE = 70,
    BIND = 71,
    UNDEF = 72,
    MINUS = 73,
    UNION = 74,
    FILTER = 75,
    NOT = 76,
    IN = 77,
    STR = 78,
    LANG = 79,
    LANGMATCHES = 80,
    DATATYPE = 81,
    BOUND = 82,
    IRI = 83,
    URI = 84,
    BNODE = 85,
    RAND = 86,
    ABS = 87,
    CEIL = 88,
    FLOOR = 89,
    ROUND = 90,
    CONCAT = 91,
    STRLEN = 92,
    UCASE = 93,
    LCASE = 94,
    ENCODE_FOR_URI = 95,
    FOR = 96,
    CONTAINS = 97,
    STRSTARTS = 98,
    STRENDS = 99,
    STRBEFORE = 100,
    STRAFTER = 101,
    YEAR = 102,
    MONTH = 103,
    DAY = 104,
    HOURS = 105,
    MINUTES = 106,
    SECONDS = 107,
    TIMEZONE = 108,
    TZ = 109,
    NOW = 110,
    UUID = 111,
    STRUUID = 112,
    SHA1 = 113,
    SHA256 = 114,
    SHA384 = 115,
    SHA512 = 116,
    MD5 = 117,
    COALESCE = 118,
    IF = 119,
    STRLANG = 120,
    STRDT = 121,
    SAMETERM = 122,
    ISIRI = 123,
    ISURI = 124,
    ISBLANK = 125,
    ISLITERAL = 126,
    ISNUMERIC = 127,
    REGEX = 128,
    SUBSTR = 129,
    REPLACE = 130,
    EXISTS = 131,
    COUNT = 132,
    SUM = 133,
    MIN = 134,
    MAX = 135,
    AVG = 136,
    STDEV = 137,
    SAMPLE = 138,
    SEPARATOR = 139,
    TRUE = 140,
    FALSE = 141,
    IRI_REF = 142,
    PNAME_NS = 143,
    PNAME_LN = 144,
    BLANK_NODE_LABEL = 145,
    VAR1 = 146,
    VAR2 = 147,
    LANGTAG = 148,
    PREFIX_LANGTAG = 149,
    INTEGER = 150,
    DECIMAL = 151,
    DOUBLE = 152,
    INTEGER_POSITIVE = 153,
    DECIMAL_POSITIVE = 154,
    DOUBLE_POSITIVE = 155,
    INTEGER_NEGATIVE = 156,
    DECIMAL_NEGATIVE = 157,
    DOUBLE_NEGATIVE = 158,
    EXPONENT = 159,
    STRING_LITERAL1 = 160,
    STRING_LITERAL2 = 161,
    STRING_LITERAL_LONG1 = 162,
    STRING_LITERAL_LONG2 = 163,
    ECHAR = 164,
    NIL = 165,
    ANON = 166,
    PN_CHARS_U = 167,
    VARNAME = 168,
    PN_PREFIX = 169,
    PN_LOCAL = 170,
    PLX = 171,
    PERCENT = 172,
    HEX = 173,
    PN_LOCAL_ESC = 174,
    WS = 175,
    COMMENTS = 176
  };

  explicit SparqlAutomaticLexer(antlr4::CharStream* input);

  ~SparqlAutomaticLexer() override;

  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.
};

#endif  // QLEVER_SRC_PARSER_SPARQLPARSER_GENERATED_SPARQLAUTOMATICLEXER_H
