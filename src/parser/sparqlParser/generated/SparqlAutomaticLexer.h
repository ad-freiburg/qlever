
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
    INTERNALSORTBY = 46,
    ASC = 47,
    DESC = 48,
    LIMIT = 49,
    OFFSET = 50,
    TEXTLIMIT = 51,
    VALUES = 52,
    LOAD = 53,
    SILENT = 54,
    INTO = 55,
    CLEAR = 56,
    DROP = 57,
    CREATE = 58,
    ADD = 59,
    TO = 60,
    DATA = 61,
    MOVE = 62,
    COPY = 63,
    INSERT = 64,
    DELETE = 65,
    WITH = 66,
    INCLUDE = 67,
    USING = 68,
    DEFAULT = 69,
    GRAPH = 70,
    ALL = 71,
    OPTIONAL = 72,
    SERVICE = 73,
    BIND = 74,
    UNDEF = 75,
    MINUS = 76,
    UNION = 77,
    FILTER = 78,
    NOT = 79,
    IN = 80,
    STR = 81,
    LANG = 82,
    LANGMATCHES = 83,
    DATATYPE = 84,
    BOUND = 85,
    IRI = 86,
    URI = 87,
    BNODE = 88,
    RAND = 89,
    ABS = 90,
    CEIL = 91,
    FLOOR = 92,
    ROUND = 93,
    CONCAT = 94,
    STRLEN = 95,
    UCASE = 96,
    LCASE = 97,
    ENCODE_FOR_URI = 98,
    FOR = 99,
    CONTAINS = 100,
    STRSTARTS = 101,
    STRENDS = 102,
    STRBEFORE = 103,
    STRAFTER = 104,
    YEAR = 105,
    MONTH = 106,
    DAY = 107,
    HOURS = 108,
    MINUTES = 109,
    SECONDS = 110,
    TIMEZONE = 111,
    TZ = 112,
    NOW = 113,
    UUID = 114,
    STRUUID = 115,
    SHA1 = 116,
    SHA256 = 117,
    SHA384 = 118,
    SHA512 = 119,
    MD5 = 120,
    COALESCE = 121,
    IF = 122,
    STRLANG = 123,
    STRDT = 124,
    SAMETERM = 125,
    ISIRI = 126,
    ISURI = 127,
    ISBLANK = 128,
    ISLITERAL = 129,
    ISNUMERIC = 130,
    REGEX = 131,
    SUBSTR = 132,
    REPLACE = 133,
    EXISTS = 134,
    COUNT = 135,
    SUM = 136,
    MIN = 137,
    MAX = 138,
    AVG = 139,
    STDEV = 140,
    SAMPLE = 141,
    SEPARATOR = 142,
    IRI_REF = 143,
    PNAME_NS = 144,
    PNAME_LN = 145,
    BLANK_NODE_LABEL = 146,
    VAR1 = 147,
    VAR2 = 148,
    LANGTAG = 149,
    PREFIX_LANGTAG = 150,
    INTEGER = 151,
    DECIMAL = 152,
    DOUBLE = 153,
    INTEGER_POSITIVE = 154,
    DECIMAL_POSITIVE = 155,
    DOUBLE_POSITIVE = 156,
    INTEGER_NEGATIVE = 157,
    DECIMAL_NEGATIVE = 158,
    DOUBLE_NEGATIVE = 159,
    EXPONENT = 160,
    STRING_LITERAL1 = 161,
    STRING_LITERAL2 = 162,
    STRING_LITERAL_LONG1 = 163,
    STRING_LITERAL_LONG2 = 164,
    ECHAR = 165,
    NIL = 166,
    ANON = 167,
    PN_CHARS_U = 168,
    VARNAME = 169,
    NAMED_SUBQUERY_NAME = 170,
    PN_PREFIX = 171,
    PN_LOCAL = 172,
    PLX = 173,
    PERCENT = 174,
    HEX = 175,
    PN_LOCAL_ESC = 176,
    WS = 177,
    COMMENTS = 178
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
