
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
    INCLUDE = 65,
    USING = 66,
    DEFAULT = 67,
    GRAPH = 68,
    ALL = 69,
    OPTIONAL = 70,
    SERVICE = 71,
    BIND = 72,
    UNDEF = 73,
    MINUS = 74,
    UNION = 75,
    FILTER = 76,
    NOT = 77,
    IN = 78,
    STR = 79,
    LANG = 80,
    LANGMATCHES = 81,
    DATATYPE = 82,
    BOUND = 83,
    IRI = 84,
    URI = 85,
    BNODE = 86,
    RAND = 87,
    ABS = 88,
    CEIL = 89,
    FLOOR = 90,
    ROUND = 91,
    CONCAT = 92,
    STRLEN = 93,
    UCASE = 94,
    LCASE = 95,
    ENCODE_FOR_URI = 96,
    FOR = 97,
    CONTAINS = 98,
    STRSTARTS = 99,
    STRENDS = 100,
    STRBEFORE = 101,
    STRAFTER = 102,
    YEAR = 103,
    MONTH = 104,
    DAY = 105,
    HOURS = 106,
    MINUTES = 107,
    SECONDS = 108,
    TIMEZONE = 109,
    TZ = 110,
    NOW = 111,
    UUID = 112,
    STRUUID = 113,
    SHA1 = 114,
    SHA256 = 115,
    SHA384 = 116,
    SHA512 = 117,
    MD5 = 118,
    COALESCE = 119,
    IF = 120,
    STRLANG = 121,
    STRDT = 122,
    SAMETERM = 123,
    ISIRI = 124,
    ISURI = 125,
    ISBLANK = 126,
    ISLITERAL = 127,
    ISNUMERIC = 128,
    REGEX = 129,
    SUBSTR = 130,
    REPLACE = 131,
    EXISTS = 132,
    COUNT = 133,
    SUM = 134,
    MIN = 135,
    MAX = 136,
    AVG = 137,
    STDEV = 138,
    SAMPLE = 139,
    SEPARATOR = 140,
    TRUE = 141,
    FALSE = 142,
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
