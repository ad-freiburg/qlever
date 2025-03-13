// Generated from /home/sven/7/Projects/Open/qlever-code/src/parser/sparqlParser/generated/SparqlAutomatic.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class SparqlAutomaticParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, BASE=30, PREFIX=31, 
		SELECT=32, DISTINCT=33, REDUCED=34, AS=35, CONSTRUCT=36, WHERE=37, DESCRIBE=38, 
		ASK=39, FROM=40, NAMED=41, GROUPBY=42, GROUP_CONCAT=43, HAVING=44, ORDERBY=45, 
		INTERNALSORTBY=46, ASC=47, DESC=48, LIMIT=49, OFFSET=50, TEXTLIMIT=51, 
		VALUES=52, LOAD=53, SILENT=54, INTO=55, CLEAR=56, DROP=57, CREATE=58, 
		ADD=59, TO=60, DATA=61, MOVE=62, COPY=63, INSERT=64, DELETE=65, WITH=66, 
		USING=67, DEFAULT=68, GRAPH=69, ALL=70, OPTIONAL=71, SERVICE=72, BIND=73, 
		UNDEF=74, MINUS=75, UNION=76, FILTER=77, NOT=78, IN=79, STR=80, LANG=81, 
		LANGMATCHES=82, DATATYPE=83, BOUND=84, IRI=85, URI=86, BNODE=87, RAND=88, 
		ABS=89, CEIL=90, FLOOR=91, ROUND=92, CONCAT=93, STRLEN=94, UCASE=95, LCASE=96, 
		ENCODE_FOR_URI=97, FOR=98, CONTAINS=99, STRSTARTS=100, STRENDS=101, STRBEFORE=102, 
		STRAFTER=103, YEAR=104, MONTH=105, DAY=106, HOURS=107, MINUTES=108, SECONDS=109, 
		TIMEZONE=110, TZ=111, NOW=112, UUID=113, STRUUID=114, SHA1=115, SHA256=116, 
		SHA384=117, SHA512=118, MD5=119, COALESCE=120, IF=121, STRLANG=122, STRDT=123, 
		SAMETERM=124, ISIRI=125, ISURI=126, ISBLANK=127, ISLITERAL=128, ISNUMERIC=129, 
		REGEX=130, SUBSTR=131, REPLACE=132, EXISTS=133, COUNT=134, SUM=135, MIN=136, 
		MAX=137, AVG=138, STDEV=139, SAMPLE=140, SEPARATOR=141, IRI_REF=142, PNAME_NS=143, 
		PNAME_LN=144, BLANK_NODE_LABEL=145, VAR1=146, VAR2=147, LANGTAG=148, PREFIX_LANGTAG=149, 
		INTEGER=150, DECIMAL=151, DOUBLE=152, INTEGER_POSITIVE=153, DECIMAL_POSITIVE=154, 
		DOUBLE_POSITIVE=155, INTEGER_NEGATIVE=156, DECIMAL_NEGATIVE=157, DOUBLE_NEGATIVE=158, 
		EXPONENT=159, STRING_LITERAL1=160, STRING_LITERAL2=161, STRING_LITERAL_LONG1=162, 
		STRING_LITERAL_LONG2=163, ECHAR=164, NIL=165, ANON=166, PN_CHARS_U=167, 
		VARNAME=168, PN_PREFIX=169, PN_LOCAL=170, PLX=171, PERCENT=172, HEX=173, 
		PN_LOCAL_ESC=174, WS=175, COMMENTS=176;
	public static final int
		RULE_queryOrUpdate = 0, RULE_query = 1, RULE_prologue = 2, RULE_baseDecl = 3, 
		RULE_prefixDecl = 4, RULE_selectQuery = 5, RULE_subSelect = 6, RULE_selectClause = 7, 
		RULE_varOrAlias = 8, RULE_alias = 9, RULE_aliasWithoutBrackets = 10, RULE_constructQuery = 11, 
		RULE_describeQuery = 12, RULE_askQuery = 13, RULE_datasetClause = 14, 
		RULE_defaultGraphClause = 15, RULE_namedGraphClause = 16, RULE_sourceSelector = 17, 
		RULE_whereClause = 18, RULE_solutionModifier = 19, RULE_groupClause = 20, 
		RULE_groupCondition = 21, RULE_havingClause = 22, RULE_havingCondition = 23, 
		RULE_orderClause = 24, RULE_orderCondition = 25, RULE_limitOffsetClauses = 26, 
		RULE_limitClause = 27, RULE_offsetClause = 28, RULE_textLimitClause = 29, 
		RULE_valuesClause = 30, RULE_update = 31, RULE_update1 = 32, RULE_load = 33, 
		RULE_clear = 34, RULE_drop = 35, RULE_create = 36, RULE_add = 37, RULE_move = 38, 
		RULE_copy = 39, RULE_insertData = 40, RULE_deleteData = 41, RULE_deleteWhere = 42, 
		RULE_modify = 43, RULE_deleteClause = 44, RULE_insertClause = 45, RULE_usingClause = 46, 
		RULE_graphOrDefault = 47, RULE_graphRef = 48, RULE_graphRefAll = 49, RULE_quadPattern = 50, 
		RULE_quadData = 51, RULE_quads = 52, RULE_quadsNotTriples = 53, RULE_triplesTemplate = 54, 
		RULE_groupGraphPattern = 55, RULE_groupGraphPatternSub = 56, RULE_graphPatternNotTriplesAndMaybeTriples = 57, 
		RULE_triplesBlock = 58, RULE_graphPatternNotTriples = 59, RULE_optionalGraphPattern = 60, 
		RULE_graphGraphPattern = 61, RULE_serviceGraphPattern = 62, RULE_bind = 63, 
		RULE_inlineData = 64, RULE_dataBlock = 65, RULE_inlineDataOneVar = 66, 
		RULE_inlineDataFull = 67, RULE_dataBlockSingle = 68, RULE_dataBlockValue = 69, 
		RULE_minusGraphPattern = 70, RULE_groupOrUnionGraphPattern = 71, RULE_filterR = 72, 
		RULE_constraint = 73, RULE_functionCall = 74, RULE_argList = 75, RULE_expressionList = 76, 
		RULE_constructTemplate = 77, RULE_constructTriples = 78, RULE_triplesSameSubject = 79, 
		RULE_propertyList = 80, RULE_propertyListNotEmpty = 81, RULE_verb = 82, 
		RULE_objectList = 83, RULE_objectR = 84, RULE_triplesSameSubjectPath = 85, 
		RULE_propertyListPath = 86, RULE_propertyListPathNotEmpty = 87, RULE_verbPath = 88, 
		RULE_verbSimple = 89, RULE_tupleWithoutPath = 90, RULE_tupleWithPath = 91, 
		RULE_verbPathOrSimple = 92, RULE_objectListPath = 93, RULE_objectPath = 94, 
		RULE_path = 95, RULE_pathAlternative = 96, RULE_pathSequence = 97, RULE_pathElt = 98, 
		RULE_pathEltOrInverse = 99, RULE_pathMod = 100, RULE_minMax = 101, RULE_stepsMin = 102, 
		RULE_stepsMax = 103, RULE_stepsExact = 104, RULE_pathPrimary = 105, RULE_pathNegatedPropertySet = 106, 
		RULE_pathOneInPropertySet = 107, RULE_integer = 108, RULE_triplesNode = 109, 
		RULE_blankNodePropertyList = 110, RULE_triplesNodePath = 111, RULE_blankNodePropertyListPath = 112, 
		RULE_collection = 113, RULE_collectionPath = 114, RULE_graphNode = 115, 
		RULE_graphNodePath = 116, RULE_varOrTerm = 117, RULE_varOrIri = 118, RULE_var = 119, 
		RULE_graphTerm = 120, RULE_expression = 121, RULE_conditionalOrExpression = 122, 
		RULE_conditionalAndExpression = 123, RULE_valueLogical = 124, RULE_relationalExpression = 125, 
		RULE_numericExpression = 126, RULE_additiveExpression = 127, RULE_multiplicativeExpressionWithSign = 128, 
		RULE_plusSubexpression = 129, RULE_minusSubexpression = 130, RULE_multiplicativeExpressionWithLeadingSignButNoSpace = 131, 
		RULE_multiplicativeExpression = 132, RULE_multiplyOrDivideExpression = 133, 
		RULE_multiplyExpression = 134, RULE_divideExpression = 135, RULE_unaryExpression = 136, 
		RULE_primaryExpression = 137, RULE_brackettedExpression = 138, RULE_builtInCall = 139, 
		RULE_regexExpression = 140, RULE_langExpression = 141, RULE_substringExpression = 142, 
		RULE_strReplaceExpression = 143, RULE_existsFunc = 144, RULE_notExistsFunc = 145, 
		RULE_aggregate = 146, RULE_iriOrFunction = 147, RULE_rdfLiteral = 148, 
		RULE_numericLiteral = 149, RULE_numericLiteralUnsigned = 150, RULE_numericLiteralPositive = 151, 
		RULE_numericLiteralNegative = 152, RULE_booleanLiteral = 153, RULE_string = 154, 
		RULE_iri = 155, RULE_prefixedName = 156, RULE_blankNode = 157, RULE_iriref = 158, 
		RULE_pnameLn = 159, RULE_pnameNs = 160;
	private static String[] makeRuleNames() {
		return new String[] {
			"queryOrUpdate", "query", "prologue", "baseDecl", "prefixDecl", "selectQuery", 
			"subSelect", "selectClause", "varOrAlias", "alias", "aliasWithoutBrackets", 
			"constructQuery", "describeQuery", "askQuery", "datasetClause", "defaultGraphClause", 
			"namedGraphClause", "sourceSelector", "whereClause", "solutionModifier", 
			"groupClause", "groupCondition", "havingClause", "havingCondition", "orderClause", 
			"orderCondition", "limitOffsetClauses", "limitClause", "offsetClause", 
			"textLimitClause", "valuesClause", "update", "update1", "load", "clear", 
			"drop", "create", "add", "move", "copy", "insertData", "deleteData", 
			"deleteWhere", "modify", "deleteClause", "insertClause", "usingClause", 
			"graphOrDefault", "graphRef", "graphRefAll", "quadPattern", "quadData", 
			"quads", "quadsNotTriples", "triplesTemplate", "groupGraphPattern", "groupGraphPatternSub", 
			"graphPatternNotTriplesAndMaybeTriples", "triplesBlock", "graphPatternNotTriples", 
			"optionalGraphPattern", "graphGraphPattern", "serviceGraphPattern", "bind", 
			"inlineData", "dataBlock", "inlineDataOneVar", "inlineDataFull", "dataBlockSingle", 
			"dataBlockValue", "minusGraphPattern", "groupOrUnionGraphPattern", "filterR", 
			"constraint", "functionCall", "argList", "expressionList", "constructTemplate", 
			"constructTriples", "triplesSameSubject", "propertyList", "propertyListNotEmpty", 
			"verb", "objectList", "objectR", "triplesSameSubjectPath", "propertyListPath", 
			"propertyListPathNotEmpty", "verbPath", "verbSimple", "tupleWithoutPath", 
			"tupleWithPath", "verbPathOrSimple", "objectListPath", "objectPath", 
			"path", "pathAlternative", "pathSequence", "pathElt", "pathEltOrInverse", 
			"pathMod", "minMax", "stepsMin", "stepsMax", "stepsExact", "pathPrimary", 
			"pathNegatedPropertySet", "pathOneInPropertySet", "integer", "triplesNode", 
			"blankNodePropertyList", "triplesNodePath", "blankNodePropertyListPath", 
			"collection", "collectionPath", "graphNode", "graphNodePath", "varOrTerm", 
			"varOrIri", "var", "graphTerm", "expression", "conditionalOrExpression", 
			"conditionalAndExpression", "valueLogical", "relationalExpression", "numericExpression", 
			"additiveExpression", "multiplicativeExpressionWithSign", "plusSubexpression", 
			"minusSubexpression", "multiplicativeExpressionWithLeadingSignButNoSpace", 
			"multiplicativeExpression", "multiplyOrDivideExpression", "multiplyExpression", 
			"divideExpression", "unaryExpression", "primaryExpression", "brackettedExpression", 
			"builtInCall", "regexExpression", "langExpression", "substringExpression", 
			"strReplaceExpression", "existsFunc", "notExistsFunc", "aggregate", "iriOrFunction", 
			"rdfLiteral", "numericLiteral", "numericLiteralUnsigned", "numericLiteralPositive", 
			"numericLiteralNegative", "booleanLiteral", "string", "iri", "prefixedName", 
			"blankNode", "iriref", "pnameLn", "pnameNs"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'*'", "'('", "')'", "'{'", "'}'", "';'", "'.'", "','", "'a'", 
			"'|'", "'/'", "'^'", "'+'", "'?'", "'!'", "'['", "']'", "'||'", "'&&'", 
			"'='", "'!='", "'<'", "'>'", "'<='", "'>='", "'-'", "'^^'", "'true'", 
			"'false'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, "BASE", "PREFIX", "SELECT", "DISTINCT", 
			"REDUCED", "AS", "CONSTRUCT", "WHERE", "DESCRIBE", "ASK", "FROM", "NAMED", 
			"GROUPBY", "GROUP_CONCAT", "HAVING", "ORDERBY", "INTERNALSORTBY", "ASC", 
			"DESC", "LIMIT", "OFFSET", "TEXTLIMIT", "VALUES", "LOAD", "SILENT", "INTO", 
			"CLEAR", "DROP", "CREATE", "ADD", "TO", "DATA", "MOVE", "COPY", "INSERT", 
			"DELETE", "WITH", "USING", "DEFAULT", "GRAPH", "ALL", "OPTIONAL", "SERVICE", 
			"BIND", "UNDEF", "MINUS", "UNION", "FILTER", "NOT", "IN", "STR", "LANG", 
			"LANGMATCHES", "DATATYPE", "BOUND", "IRI", "URI", "BNODE", "RAND", "ABS", 
			"CEIL", "FLOOR", "ROUND", "CONCAT", "STRLEN", "UCASE", "LCASE", "ENCODE_FOR_URI", 
			"FOR", "CONTAINS", "STRSTARTS", "STRENDS", "STRBEFORE", "STRAFTER", "YEAR", 
			"MONTH", "DAY", "HOURS", "MINUTES", "SECONDS", "TIMEZONE", "TZ", "NOW", 
			"UUID", "STRUUID", "SHA1", "SHA256", "SHA384", "SHA512", "MD5", "COALESCE", 
			"IF", "STRLANG", "STRDT", "SAMETERM", "ISIRI", "ISURI", "ISBLANK", "ISLITERAL", 
			"ISNUMERIC", "REGEX", "SUBSTR", "REPLACE", "EXISTS", "COUNT", "SUM", 
			"MIN", "MAX", "AVG", "STDEV", "SAMPLE", "SEPARATOR", "IRI_REF", "PNAME_NS", 
			"PNAME_LN", "BLANK_NODE_LABEL", "VAR1", "VAR2", "LANGTAG", "PREFIX_LANGTAG", 
			"INTEGER", "DECIMAL", "DOUBLE", "INTEGER_POSITIVE", "DECIMAL_POSITIVE", 
			"DOUBLE_POSITIVE", "INTEGER_NEGATIVE", "DECIMAL_NEGATIVE", "DOUBLE_NEGATIVE", 
			"EXPONENT", "STRING_LITERAL1", "STRING_LITERAL2", "STRING_LITERAL_LONG1", 
			"STRING_LITERAL_LONG2", "ECHAR", "NIL", "ANON", "PN_CHARS_U", "VARNAME", 
			"PN_PREFIX", "PN_LOCAL", "PLX", "PERCENT", "HEX", "PN_LOCAL_ESC", "WS", 
			"COMMENTS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "SparqlAutomatic.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public SparqlAutomaticParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryOrUpdateContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(SparqlAutomaticParser.EOF, 0); }
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public UpdateContext update() {
			return getRuleContext(UpdateContext.class,0);
		}
		public QueryOrUpdateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryOrUpdate; }
	}

	public final QueryOrUpdateContext queryOrUpdate() throws RecognitionException {
		QueryOrUpdateContext _localctx = new QueryOrUpdateContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_queryOrUpdate);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(324);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				{
				setState(322);
				query();
				}
				break;
			case 2:
				{
				setState(323);
				update();
				}
				break;
			}
			setState(326);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public PrologueContext prologue() {
			return getRuleContext(PrologueContext.class,0);
		}
		public ValuesClauseContext valuesClause() {
			return getRuleContext(ValuesClauseContext.class,0);
		}
		public SelectQueryContext selectQuery() {
			return getRuleContext(SelectQueryContext.class,0);
		}
		public ConstructQueryContext constructQuery() {
			return getRuleContext(ConstructQueryContext.class,0);
		}
		public DescribeQueryContext describeQuery() {
			return getRuleContext(DescribeQueryContext.class,0);
		}
		public AskQueryContext askQuery() {
			return getRuleContext(AskQueryContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_query);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(328);
			prologue();
			setState(333);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				{
				setState(329);
				selectQuery();
				}
				break;
			case CONSTRUCT:
				{
				setState(330);
				constructQuery();
				}
				break;
			case DESCRIBE:
				{
				setState(331);
				describeQuery();
				}
				break;
			case ASK:
				{
				setState(332);
				askQuery();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(335);
			valuesClause();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrologueContext extends ParserRuleContext {
		public List<BaseDeclContext> baseDecl() {
			return getRuleContexts(BaseDeclContext.class);
		}
		public BaseDeclContext baseDecl(int i) {
			return getRuleContext(BaseDeclContext.class,i);
		}
		public List<PrefixDeclContext> prefixDecl() {
			return getRuleContexts(PrefixDeclContext.class);
		}
		public PrefixDeclContext prefixDecl(int i) {
			return getRuleContext(PrefixDeclContext.class,i);
		}
		public PrologueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prologue; }
	}

	public final PrologueContext prologue() throws RecognitionException {
		PrologueContext _localctx = new PrologueContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_prologue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(341);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==BASE || _la==PREFIX) {
				{
				setState(339);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case BASE:
					{
					setState(337);
					baseDecl();
					}
					break;
				case PREFIX:
					{
					setState(338);
					prefixDecl();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(343);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BaseDeclContext extends ParserRuleContext {
		public TerminalNode BASE() { return getToken(SparqlAutomaticParser.BASE, 0); }
		public IrirefContext iriref() {
			return getRuleContext(IrirefContext.class,0);
		}
		public BaseDeclContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_baseDecl; }
	}

	public final BaseDeclContext baseDecl() throws RecognitionException {
		BaseDeclContext _localctx = new BaseDeclContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_baseDecl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(344);
			match(BASE);
			setState(345);
			iriref();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrefixDeclContext extends ParserRuleContext {
		public TerminalNode PREFIX() { return getToken(SparqlAutomaticParser.PREFIX, 0); }
		public TerminalNode PNAME_NS() { return getToken(SparqlAutomaticParser.PNAME_NS, 0); }
		public IrirefContext iriref() {
			return getRuleContext(IrirefContext.class,0);
		}
		public PrefixDeclContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prefixDecl; }
	}

	public final PrefixDeclContext prefixDecl() throws RecognitionException {
		PrefixDeclContext _localctx = new PrefixDeclContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_prefixDecl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(347);
			match(PREFIX);
			setState(348);
			match(PNAME_NS);
			setState(349);
			iriref();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectQueryContext extends ParserRuleContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SolutionModifierContext solutionModifier() {
			return getRuleContext(SolutionModifierContext.class,0);
		}
		public List<DatasetClauseContext> datasetClause() {
			return getRuleContexts(DatasetClauseContext.class);
		}
		public DatasetClauseContext datasetClause(int i) {
			return getRuleContext(DatasetClauseContext.class,i);
		}
		public SelectQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectQuery; }
	}

	public final SelectQueryContext selectQuery() throws RecognitionException {
		SelectQueryContext _localctx = new SelectQueryContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_selectQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(351);
			selectClause();
			setState(355);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==FROM) {
				{
				{
				setState(352);
				datasetClause();
				}
				}
				setState(357);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(358);
			whereClause();
			setState(359);
			solutionModifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SubSelectContext extends ParserRuleContext {
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SolutionModifierContext solutionModifier() {
			return getRuleContext(SolutionModifierContext.class,0);
		}
		public ValuesClauseContext valuesClause() {
			return getRuleContext(ValuesClauseContext.class,0);
		}
		public SubSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subSelect; }
	}

	public final SubSelectContext subSelect() throws RecognitionException {
		SubSelectContext _localctx = new SubSelectContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_subSelect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(361);
			selectClause();
			setState(362);
			whereClause();
			setState(363);
			solutionModifier();
			setState(364);
			valuesClause();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SelectClauseContext extends ParserRuleContext {
		public Token asterisk;
		public TerminalNode SELECT() { return getToken(SparqlAutomaticParser.SELECT, 0); }
		public TerminalNode DISTINCT() { return getToken(SparqlAutomaticParser.DISTINCT, 0); }
		public TerminalNode REDUCED() { return getToken(SparqlAutomaticParser.REDUCED, 0); }
		public List<VarOrAliasContext> varOrAlias() {
			return getRuleContexts(VarOrAliasContext.class);
		}
		public VarOrAliasContext varOrAlias(int i) {
			return getRuleContext(VarOrAliasContext.class,i);
		}
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_selectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(366);
			match(SELECT);
			setState(368);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTINCT || _la==REDUCED) {
				{
				setState(367);
				_la = _input.LA(1);
				if ( !(_la==DISTINCT || _la==REDUCED) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(376);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case VAR1:
			case VAR2:
				{
				setState(371); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(370);
					varOrAlias();
					}
					}
					setState(373); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==T__1 || _la==VAR1 || _la==VAR2 );
				}
				break;
			case T__0:
				{
				setState(375);
				((SelectClauseContext)_localctx).asterisk = match(T__0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VarOrAliasContext extends ParserRuleContext {
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public AliasContext alias() {
			return getRuleContext(AliasContext.class,0);
		}
		public VarOrAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varOrAlias; }
	}

	public final VarOrAliasContext varOrAlias() throws RecognitionException {
		VarOrAliasContext _localctx = new VarOrAliasContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_varOrAlias);
		try {
			setState(380);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case VAR1:
			case VAR2:
				enterOuterAlt(_localctx, 1);
				{
				setState(378);
				var();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				setState(379);
				alias();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AliasContext extends ParserRuleContext {
		public AliasWithoutBracketsContext aliasWithoutBrackets() {
			return getRuleContext(AliasWithoutBracketsContext.class,0);
		}
		public AliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alias; }
	}

	public final AliasContext alias() throws RecognitionException {
		AliasContext _localctx = new AliasContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(382);
			match(T__1);
			setState(383);
			aliasWithoutBrackets();
			setState(384);
			match(T__2);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AliasWithoutBracketsContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SparqlAutomaticParser.AS, 0); }
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public AliasWithoutBracketsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliasWithoutBrackets; }
	}

	public final AliasWithoutBracketsContext aliasWithoutBrackets() throws RecognitionException {
		AliasWithoutBracketsContext _localctx = new AliasWithoutBracketsContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_aliasWithoutBrackets);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(386);
			expression();
			setState(387);
			match(AS);
			setState(388);
			var();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstructQueryContext extends ParserRuleContext {
		public TerminalNode CONSTRUCT() { return getToken(SparqlAutomaticParser.CONSTRUCT, 0); }
		public ConstructTemplateContext constructTemplate() {
			return getRuleContext(ConstructTemplateContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SolutionModifierContext solutionModifier() {
			return getRuleContext(SolutionModifierContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SparqlAutomaticParser.WHERE, 0); }
		public List<DatasetClauseContext> datasetClause() {
			return getRuleContexts(DatasetClauseContext.class);
		}
		public DatasetClauseContext datasetClause(int i) {
			return getRuleContext(DatasetClauseContext.class,i);
		}
		public TriplesTemplateContext triplesTemplate() {
			return getRuleContext(TriplesTemplateContext.class,0);
		}
		public ConstructQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constructQuery; }
	}

	public final ConstructQueryContext constructQuery() throws RecognitionException {
		ConstructQueryContext _localctx = new ConstructQueryContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_constructQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(390);
			match(CONSTRUCT);
			setState(414);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__3:
				{
				setState(391);
				constructTemplate();
				setState(395);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==FROM) {
					{
					{
					setState(392);
					datasetClause();
					}
					}
					setState(397);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(398);
				whereClause();
				setState(399);
				solutionModifier();
				}
				break;
			case WHERE:
			case FROM:
				{
				setState(404);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==FROM) {
					{
					{
					setState(401);
					datasetClause();
					}
					}
					setState(406);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(407);
				match(WHERE);
				setState(408);
				match(T__3);
				setState(410);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
					{
					setState(409);
					triplesTemplate();
					}
				}

				setState(412);
				match(T__4);
				setState(413);
				solutionModifier();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DescribeQueryContext extends ParserRuleContext {
		public TerminalNode DESCRIBE() { return getToken(SparqlAutomaticParser.DESCRIBE, 0); }
		public SolutionModifierContext solutionModifier() {
			return getRuleContext(SolutionModifierContext.class,0);
		}
		public List<DatasetClauseContext> datasetClause() {
			return getRuleContexts(DatasetClauseContext.class);
		}
		public DatasetClauseContext datasetClause(int i) {
			return getRuleContext(DatasetClauseContext.class,i);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public List<VarOrIriContext> varOrIri() {
			return getRuleContexts(VarOrIriContext.class);
		}
		public VarOrIriContext varOrIri(int i) {
			return getRuleContext(VarOrIriContext.class,i);
		}
		public DescribeQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describeQuery; }
	}

	public final DescribeQueryContext describeQuery() throws RecognitionException {
		DescribeQueryContext _localctx = new DescribeQueryContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_describeQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(416);
			match(DESCRIBE);
			setState(423);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
				{
				setState(418); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(417);
					varOrIri();
					}
					}
					setState(420); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0) );
				}
				break;
			case T__0:
				{
				setState(422);
				match(T__0);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(428);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==FROM) {
				{
				{
				setState(425);
				datasetClause();
				}
				}
				setState(430);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(432);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__3 || _la==WHERE) {
				{
				setState(431);
				whereClause();
				}
			}

			setState(434);
			solutionModifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AskQueryContext extends ParserRuleContext {
		public TerminalNode ASK() { return getToken(SparqlAutomaticParser.ASK, 0); }
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public SolutionModifierContext solutionModifier() {
			return getRuleContext(SolutionModifierContext.class,0);
		}
		public List<DatasetClauseContext> datasetClause() {
			return getRuleContexts(DatasetClauseContext.class);
		}
		public DatasetClauseContext datasetClause(int i) {
			return getRuleContext(DatasetClauseContext.class,i);
		}
		public AskQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_askQuery; }
	}

	public final AskQueryContext askQuery() throws RecognitionException {
		AskQueryContext _localctx = new AskQueryContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_askQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(436);
			match(ASK);
			setState(440);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==FROM) {
				{
				{
				setState(437);
				datasetClause();
				}
				}
				setState(442);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(443);
			whereClause();
			setState(444);
			solutionModifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DatasetClauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(SparqlAutomaticParser.FROM, 0); }
		public DefaultGraphClauseContext defaultGraphClause() {
			return getRuleContext(DefaultGraphClauseContext.class,0);
		}
		public NamedGraphClauseContext namedGraphClause() {
			return getRuleContext(NamedGraphClauseContext.class,0);
		}
		public DatasetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasetClause; }
	}

	public final DatasetClauseContext datasetClause() throws RecognitionException {
		DatasetClauseContext _localctx = new DatasetClauseContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_datasetClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(446);
			match(FROM);
			setState(449);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				{
				setState(447);
				defaultGraphClause();
				}
				break;
			case NAMED:
				{
				setState(448);
				namedGraphClause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DefaultGraphClauseContext extends ParserRuleContext {
		public SourceSelectorContext sourceSelector() {
			return getRuleContext(SourceSelectorContext.class,0);
		}
		public DefaultGraphClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_defaultGraphClause; }
	}

	public final DefaultGraphClauseContext defaultGraphClause() throws RecognitionException {
		DefaultGraphClauseContext _localctx = new DefaultGraphClauseContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_defaultGraphClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(451);
			sourceSelector();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamedGraphClauseContext extends ParserRuleContext {
		public TerminalNode NAMED() { return getToken(SparqlAutomaticParser.NAMED, 0); }
		public SourceSelectorContext sourceSelector() {
			return getRuleContext(SourceSelectorContext.class,0);
		}
		public NamedGraphClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namedGraphClause; }
	}

	public final NamedGraphClauseContext namedGraphClause() throws RecognitionException {
		NamedGraphClauseContext _localctx = new NamedGraphClauseContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_namedGraphClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(453);
			match(NAMED);
			setState(454);
			sourceSelector();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SourceSelectorContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public SourceSelectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceSelector; }
	}

	public final SourceSelectorContext sourceSelector() throws RecognitionException {
		SourceSelectorContext _localctx = new SourceSelectorContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_sourceSelector);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(456);
			iri();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class WhereClauseContext extends ParserRuleContext {
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(SparqlAutomaticParser.WHERE, 0); }
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_whereClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(459);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(458);
				match(WHERE);
				}
			}

			setState(461);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SolutionModifierContext extends ParserRuleContext {
		public GroupClauseContext groupClause() {
			return getRuleContext(GroupClauseContext.class,0);
		}
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public OrderClauseContext orderClause() {
			return getRuleContext(OrderClauseContext.class,0);
		}
		public LimitOffsetClausesContext limitOffsetClauses() {
			return getRuleContext(LimitOffsetClausesContext.class,0);
		}
		public SolutionModifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_solutionModifier; }
	}

	public final SolutionModifierContext solutionModifier() throws RecognitionException {
		SolutionModifierContext _localctx = new SolutionModifierContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_solutionModifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(464);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUPBY) {
				{
				setState(463);
				groupClause();
				}
			}

			setState(467);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==HAVING) {
				{
				setState(466);
				havingClause();
				}
			}

			setState(470);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDERBY || _la==INTERNALSORTBY) {
				{
				setState(469);
				orderClause();
				}
			}

			setState(473);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 3940649673949184L) != 0)) {
				{
				setState(472);
				limitOffsetClauses();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupClauseContext extends ParserRuleContext {
		public TerminalNode GROUPBY() { return getToken(SparqlAutomaticParser.GROUPBY, 0); }
		public List<GroupConditionContext> groupCondition() {
			return getRuleContexts(GroupConditionContext.class);
		}
		public GroupConditionContext groupCondition(int i) {
			return getRuleContext(GroupConditionContext.class,i);
		}
		public GroupClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupClause; }
	}

	public final GroupClauseContext groupClause() throws RecognitionException {
		GroupClauseContext _localctx = new GroupClauseContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_groupClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(475);
			match(GROUPBY);
			setState(477); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(476);
				groupCondition();
				}
				}
				setState(479); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==T__1 || _la==GROUP_CONCAT || ((((_la - 78)) & ~0x3f) == 0 && ((1L << (_la - 78)) & 9223372036853727229L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0) );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupConditionContext extends ParserRuleContext {
		public BuiltInCallContext builtInCall() {
			return getRuleContext(BuiltInCallContext.class,0);
		}
		public FunctionCallContext functionCall() {
			return getRuleContext(FunctionCallContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SparqlAutomaticParser.AS, 0); }
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public GroupConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupCondition; }
	}

	public final GroupConditionContext groupCondition() throws RecognitionException {
		GroupConditionContext _localctx = new GroupConditionContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_groupCondition);
		int _la;
		try {
			setState(492);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case GROUP_CONCAT:
			case NOT:
			case STR:
			case LANG:
			case LANGMATCHES:
			case DATATYPE:
			case BOUND:
			case IRI:
			case URI:
			case BNODE:
			case RAND:
			case ABS:
			case CEIL:
			case FLOOR:
			case ROUND:
			case CONCAT:
			case STRLEN:
			case UCASE:
			case LCASE:
			case ENCODE_FOR_URI:
			case CONTAINS:
			case STRSTARTS:
			case STRENDS:
			case STRBEFORE:
			case STRAFTER:
			case YEAR:
			case MONTH:
			case DAY:
			case HOURS:
			case MINUTES:
			case SECONDS:
			case TIMEZONE:
			case TZ:
			case NOW:
			case UUID:
			case STRUUID:
			case SHA1:
			case SHA256:
			case SHA384:
			case SHA512:
			case MD5:
			case COALESCE:
			case IF:
			case STRLANG:
			case STRDT:
			case SAMETERM:
			case ISIRI:
			case ISURI:
			case ISBLANK:
			case ISLITERAL:
			case ISNUMERIC:
			case REGEX:
			case SUBSTR:
			case REPLACE:
			case EXISTS:
			case COUNT:
			case SUM:
			case MIN:
			case MAX:
			case AVG:
			case STDEV:
			case SAMPLE:
				enterOuterAlt(_localctx, 1);
				{
				setState(481);
				builtInCall();
				}
				break;
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 2);
				{
				setState(482);
				functionCall();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 3);
				{
				setState(483);
				match(T__1);
				setState(484);
				expression();
				setState(487);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(485);
					match(AS);
					setState(486);
					var();
					}
				}

				setState(489);
				match(T__2);
				}
				break;
			case VAR1:
			case VAR2:
				enterOuterAlt(_localctx, 4);
				{
				setState(491);
				var();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HavingClauseContext extends ParserRuleContext {
		public TerminalNode HAVING() { return getToken(SparqlAutomaticParser.HAVING, 0); }
		public List<HavingConditionContext> havingCondition() {
			return getRuleContexts(HavingConditionContext.class);
		}
		public HavingConditionContext havingCondition(int i) {
			return getRuleContext(HavingConditionContext.class,i);
		}
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_havingClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(494);
			match(HAVING);
			setState(496); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(495);
				havingCondition();
				}
				}
				setState(498); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==T__1 || _la==GROUP_CONCAT || ((((_la - 78)) & ~0x3f) == 0 && ((1L << (_la - 78)) & 9223372036853727229L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 135L) != 0) );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HavingConditionContext extends ParserRuleContext {
		public ConstraintContext constraint() {
			return getRuleContext(ConstraintContext.class,0);
		}
		public HavingConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingCondition; }
	}

	public final HavingConditionContext havingCondition() throws RecognitionException {
		HavingConditionContext _localctx = new HavingConditionContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_havingCondition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(500);
			constraint();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderClauseContext extends ParserRuleContext {
		public Token orderBy;
		public Token internalSortBy;
		public TerminalNode ORDERBY() { return getToken(SparqlAutomaticParser.ORDERBY, 0); }
		public TerminalNode INTERNALSORTBY() { return getToken(SparqlAutomaticParser.INTERNALSORTBY, 0); }
		public List<OrderConditionContext> orderCondition() {
			return getRuleContexts(OrderConditionContext.class);
		}
		public OrderConditionContext orderCondition(int i) {
			return getRuleContext(OrderConditionContext.class,i);
		}
		public OrderClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderClause; }
	}

	public final OrderClauseContext orderClause() throws RecognitionException {
		OrderClauseContext _localctx = new OrderClauseContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_orderClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(504);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ORDERBY:
				{
				setState(502);
				((OrderClauseContext)_localctx).orderBy = match(ORDERBY);
				}
				break;
			case INTERNALSORTBY:
				{
				setState(503);
				((OrderClauseContext)_localctx).internalSortBy = match(INTERNALSORTBY);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(507); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(506);
				orderCondition();
				}
				}
				setState(509); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 431008558088196L) != 0) || ((((_la - 78)) & ~0x3f) == 0 && ((1L << (_la - 78)) & 9223372036853727229L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0) );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OrderConditionContext extends ParserRuleContext {
		public BrackettedExpressionContext brackettedExpression() {
			return getRuleContext(BrackettedExpressionContext.class,0);
		}
		public TerminalNode ASC() { return getToken(SparqlAutomaticParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(SparqlAutomaticParser.DESC, 0); }
		public ConstraintContext constraint() {
			return getRuleContext(ConstraintContext.class,0);
		}
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public OrderConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderCondition; }
	}

	public final OrderConditionContext orderCondition() throws RecognitionException {
		OrderConditionContext _localctx = new OrderConditionContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_orderCondition);
		int _la;
		try {
			setState(517);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ASC:
			case DESC:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(511);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(512);
				brackettedExpression();
				}
				}
				break;
			case T__1:
			case GROUP_CONCAT:
			case NOT:
			case STR:
			case LANG:
			case LANGMATCHES:
			case DATATYPE:
			case BOUND:
			case IRI:
			case URI:
			case BNODE:
			case RAND:
			case ABS:
			case CEIL:
			case FLOOR:
			case ROUND:
			case CONCAT:
			case STRLEN:
			case UCASE:
			case LCASE:
			case ENCODE_FOR_URI:
			case CONTAINS:
			case STRSTARTS:
			case STRENDS:
			case STRBEFORE:
			case STRAFTER:
			case YEAR:
			case MONTH:
			case DAY:
			case HOURS:
			case MINUTES:
			case SECONDS:
			case TIMEZONE:
			case TZ:
			case NOW:
			case UUID:
			case STRUUID:
			case SHA1:
			case SHA256:
			case SHA384:
			case SHA512:
			case MD5:
			case COALESCE:
			case IF:
			case STRLANG:
			case STRDT:
			case SAMETERM:
			case ISIRI:
			case ISURI:
			case ISBLANK:
			case ISLITERAL:
			case ISNUMERIC:
			case REGEX:
			case SUBSTR:
			case REPLACE:
			case EXISTS:
			case COUNT:
			case SUM:
			case MIN:
			case MAX:
			case AVG:
			case STDEV:
			case SAMPLE:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 2);
				{
				setState(515);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case T__1:
				case GROUP_CONCAT:
				case NOT:
				case STR:
				case LANG:
				case LANGMATCHES:
				case DATATYPE:
				case BOUND:
				case IRI:
				case URI:
				case BNODE:
				case RAND:
				case ABS:
				case CEIL:
				case FLOOR:
				case ROUND:
				case CONCAT:
				case STRLEN:
				case UCASE:
				case LCASE:
				case ENCODE_FOR_URI:
				case CONTAINS:
				case STRSTARTS:
				case STRENDS:
				case STRBEFORE:
				case STRAFTER:
				case YEAR:
				case MONTH:
				case DAY:
				case HOURS:
				case MINUTES:
				case SECONDS:
				case TIMEZONE:
				case TZ:
				case NOW:
				case UUID:
				case STRUUID:
				case SHA1:
				case SHA256:
				case SHA384:
				case SHA512:
				case MD5:
				case COALESCE:
				case IF:
				case STRLANG:
				case STRDT:
				case SAMETERM:
				case ISIRI:
				case ISURI:
				case ISBLANK:
				case ISLITERAL:
				case ISNUMERIC:
				case REGEX:
				case SUBSTR:
				case REPLACE:
				case EXISTS:
				case COUNT:
				case SUM:
				case MIN:
				case MAX:
				case AVG:
				case STDEV:
				case SAMPLE:
				case IRI_REF:
				case PNAME_NS:
				case PNAME_LN:
				case PREFIX_LANGTAG:
					{
					setState(513);
					constraint();
					}
					break;
				case VAR1:
				case VAR2:
					{
					setState(514);
					var();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LimitOffsetClausesContext extends ParserRuleContext {
		public LimitClauseContext limitClause() {
			return getRuleContext(LimitClauseContext.class,0);
		}
		public OffsetClauseContext offsetClause() {
			return getRuleContext(OffsetClauseContext.class,0);
		}
		public TextLimitClauseContext textLimitClause() {
			return getRuleContext(TextLimitClauseContext.class,0);
		}
		public LimitOffsetClausesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitOffsetClauses; }
	}

	public final LimitOffsetClausesContext limitOffsetClauses() throws RecognitionException {
		LimitOffsetClausesContext _localctx = new LimitOffsetClausesContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_limitOffsetClauses);
		int _la;
		try {
			setState(561);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(519);
				limitClause();
				setState(521);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OFFSET) {
					{
					setState(520);
					offsetClause();
					}
				}

				setState(524);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEXTLIMIT) {
					{
					setState(523);
					textLimitClause();
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(526);
				limitClause();
				setState(528);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEXTLIMIT) {
					{
					setState(527);
					textLimitClause();
					}
				}

				setState(531);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OFFSET) {
					{
					setState(530);
					offsetClause();
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(533);
				offsetClause();
				setState(535);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT) {
					{
					setState(534);
					limitClause();
					}
				}

				setState(538);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEXTLIMIT) {
					{
					setState(537);
					textLimitClause();
					}
				}

				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(540);
				offsetClause();
				setState(542);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==TEXTLIMIT) {
					{
					setState(541);
					textLimitClause();
					}
				}

				setState(545);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT) {
					{
					setState(544);
					limitClause();
					}
				}

				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(547);
				textLimitClause();
				setState(549);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OFFSET) {
					{
					setState(548);
					offsetClause();
					}
				}

				setState(552);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT) {
					{
					setState(551);
					limitClause();
					}
				}

				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(554);
				textLimitClause();
				setState(556);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LIMIT) {
					{
					setState(555);
					limitClause();
					}
				}

				setState(559);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==OFFSET) {
					{
					setState(558);
					offsetClause();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LimitClauseContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(SparqlAutomaticParser.LIMIT, 0); }
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public LimitClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitClause; }
	}

	public final LimitClauseContext limitClause() throws RecognitionException {
		LimitClauseContext _localctx = new LimitClauseContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_limitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(563);
			match(LIMIT);
			setState(564);
			integer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OffsetClauseContext extends ParserRuleContext {
		public TerminalNode OFFSET() { return getToken(SparqlAutomaticParser.OFFSET, 0); }
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public OffsetClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offsetClause; }
	}

	public final OffsetClauseContext offsetClause() throws RecognitionException {
		OffsetClauseContext _localctx = new OffsetClauseContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_offsetClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(566);
			match(OFFSET);
			setState(567);
			integer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TextLimitClauseContext extends ParserRuleContext {
		public TerminalNode TEXTLIMIT() { return getToken(SparqlAutomaticParser.TEXTLIMIT, 0); }
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public TextLimitClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_textLimitClause; }
	}

	public final TextLimitClauseContext textLimitClause() throws RecognitionException {
		TextLimitClauseContext _localctx = new TextLimitClauseContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_textLimitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(569);
			match(TEXTLIMIT);
			setState(570);
			integer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValuesClauseContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SparqlAutomaticParser.VALUES, 0); }
		public DataBlockContext dataBlock() {
			return getRuleContext(DataBlockContext.class,0);
		}
		public ValuesClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valuesClause; }
	}

	public final ValuesClauseContext valuesClause() throws RecognitionException {
		ValuesClauseContext _localctx = new ValuesClauseContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_valuesClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(574);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==VALUES) {
				{
				setState(572);
				match(VALUES);
				setState(573);
				dataBlock();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UpdateContext extends ParserRuleContext {
		public PrologueContext prologue() {
			return getRuleContext(PrologueContext.class,0);
		}
		public Update1Context update1() {
			return getRuleContext(Update1Context.class,0);
		}
		public UpdateContext update() {
			return getRuleContext(UpdateContext.class,0);
		}
		public UpdateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update; }
	}

	public final UpdateContext update() throws RecognitionException {
		UpdateContext _localctx = new UpdateContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_update);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(576);
			prologue();
			setState(582);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 53)) & ~0x3f) == 0 && ((1L << (_la - 53)) & 15993L) != 0)) {
				{
				setState(577);
				update1();
				setState(580);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__5) {
					{
					setState(578);
					match(T__5);
					setState(579);
					update();
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Update1Context extends ParserRuleContext {
		public LoadContext load() {
			return getRuleContext(LoadContext.class,0);
		}
		public ClearContext clear() {
			return getRuleContext(ClearContext.class,0);
		}
		public DropContext drop() {
			return getRuleContext(DropContext.class,0);
		}
		public AddContext add() {
			return getRuleContext(AddContext.class,0);
		}
		public MoveContext move() {
			return getRuleContext(MoveContext.class,0);
		}
		public CopyContext copy() {
			return getRuleContext(CopyContext.class,0);
		}
		public CreateContext create() {
			return getRuleContext(CreateContext.class,0);
		}
		public InsertDataContext insertData() {
			return getRuleContext(InsertDataContext.class,0);
		}
		public DeleteDataContext deleteData() {
			return getRuleContext(DeleteDataContext.class,0);
		}
		public DeleteWhereContext deleteWhere() {
			return getRuleContext(DeleteWhereContext.class,0);
		}
		public ModifyContext modify() {
			return getRuleContext(ModifyContext.class,0);
		}
		public Update1Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update1; }
	}

	public final Update1Context update1() throws RecognitionException {
		Update1Context _localctx = new Update1Context(_ctx, getState());
		enterRule(_localctx, 64, RULE_update1);
		try {
			setState(595);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(584);
				load();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(585);
				clear();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(586);
				drop();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(587);
				add();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(588);
				move();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(589);
				copy();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(590);
				create();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(591);
				insertData();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(592);
				deleteData();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(593);
				deleteWhere();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(594);
				modify();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LoadContext extends ParserRuleContext {
		public TerminalNode LOAD() { return getToken(SparqlAutomaticParser.LOAD, 0); }
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public TerminalNode INTO() { return getToken(SparqlAutomaticParser.INTO, 0); }
		public GraphRefContext graphRef() {
			return getRuleContext(GraphRefContext.class,0);
		}
		public LoadContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_load; }
	}

	public final LoadContext load() throws RecognitionException {
		LoadContext _localctx = new LoadContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_load);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(597);
			match(LOAD);
			setState(599);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(598);
				match(SILENT);
				}
			}

			setState(601);
			iri();
			setState(604);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INTO) {
				{
				setState(602);
				match(INTO);
				setState(603);
				graphRef();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ClearContext extends ParserRuleContext {
		public TerminalNode CLEAR() { return getToken(SparqlAutomaticParser.CLEAR, 0); }
		public GraphRefAllContext graphRefAll() {
			return getRuleContext(GraphRefAllContext.class,0);
		}
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public ClearContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clear; }
	}

	public final ClearContext clear() throws RecognitionException {
		ClearContext _localctx = new ClearContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_clear);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(606);
			match(CLEAR);
			setState(608);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(607);
				match(SILENT);
				}
			}

			setState(610);
			graphRefAll();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DropContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(SparqlAutomaticParser.DROP, 0); }
		public GraphRefAllContext graphRefAll() {
			return getRuleContext(GraphRefAllContext.class,0);
		}
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public DropContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop; }
	}

	public final DropContext drop() throws RecognitionException {
		DropContext _localctx = new DropContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_drop);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(612);
			match(DROP);
			setState(614);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(613);
				match(SILENT);
				}
			}

			setState(616);
			graphRefAll();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CreateContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(SparqlAutomaticParser.CREATE, 0); }
		public GraphRefContext graphRef() {
			return getRuleContext(GraphRefContext.class,0);
		}
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public CreateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create; }
	}

	public final CreateContext create() throws RecognitionException {
		CreateContext _localctx = new CreateContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_create);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(618);
			match(CREATE);
			setState(620);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(619);
				match(SILENT);
				}
			}

			setState(622);
			graphRef();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AddContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(SparqlAutomaticParser.ADD, 0); }
		public List<GraphOrDefaultContext> graphOrDefault() {
			return getRuleContexts(GraphOrDefaultContext.class);
		}
		public GraphOrDefaultContext graphOrDefault(int i) {
			return getRuleContext(GraphOrDefaultContext.class,i);
		}
		public TerminalNode TO() { return getToken(SparqlAutomaticParser.TO, 0); }
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public AddContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add; }
	}

	public final AddContext add() throws RecognitionException {
		AddContext _localctx = new AddContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_add);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(624);
			match(ADD);
			setState(626);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(625);
				match(SILENT);
				}
			}

			setState(628);
			graphOrDefault();
			setState(629);
			match(TO);
			setState(630);
			graphOrDefault();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MoveContext extends ParserRuleContext {
		public TerminalNode MOVE() { return getToken(SparqlAutomaticParser.MOVE, 0); }
		public List<GraphOrDefaultContext> graphOrDefault() {
			return getRuleContexts(GraphOrDefaultContext.class);
		}
		public GraphOrDefaultContext graphOrDefault(int i) {
			return getRuleContext(GraphOrDefaultContext.class,i);
		}
		public TerminalNode TO() { return getToken(SparqlAutomaticParser.TO, 0); }
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public MoveContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_move; }
	}

	public final MoveContext move() throws RecognitionException {
		MoveContext _localctx = new MoveContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_move);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(632);
			match(MOVE);
			setState(634);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(633);
				match(SILENT);
				}
			}

			setState(636);
			graphOrDefault();
			setState(637);
			match(TO);
			setState(638);
			graphOrDefault();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CopyContext extends ParserRuleContext {
		public TerminalNode COPY() { return getToken(SparqlAutomaticParser.COPY, 0); }
		public List<GraphOrDefaultContext> graphOrDefault() {
			return getRuleContexts(GraphOrDefaultContext.class);
		}
		public GraphOrDefaultContext graphOrDefault(int i) {
			return getRuleContext(GraphOrDefaultContext.class,i);
		}
		public TerminalNode TO() { return getToken(SparqlAutomaticParser.TO, 0); }
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public CopyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_copy; }
	}

	public final CopyContext copy() throws RecognitionException {
		CopyContext _localctx = new CopyContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_copy);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(640);
			match(COPY);
			setState(642);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(641);
				match(SILENT);
				}
			}

			setState(644);
			graphOrDefault();
			setState(645);
			match(TO);
			setState(646);
			graphOrDefault();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InsertDataContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(SparqlAutomaticParser.INSERT, 0); }
		public TerminalNode DATA() { return getToken(SparqlAutomaticParser.DATA, 0); }
		public QuadDataContext quadData() {
			return getRuleContext(QuadDataContext.class,0);
		}
		public InsertDataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertData; }
	}

	public final InsertDataContext insertData() throws RecognitionException {
		InsertDataContext _localctx = new InsertDataContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_insertData);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(648);
			match(INSERT);
			setState(649);
			match(DATA);
			setState(650);
			quadData();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DeleteDataContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(SparqlAutomaticParser.DELETE, 0); }
		public TerminalNode DATA() { return getToken(SparqlAutomaticParser.DATA, 0); }
		public QuadDataContext quadData() {
			return getRuleContext(QuadDataContext.class,0);
		}
		public DeleteDataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteData; }
	}

	public final DeleteDataContext deleteData() throws RecognitionException {
		DeleteDataContext _localctx = new DeleteDataContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_deleteData);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(652);
			match(DELETE);
			setState(653);
			match(DATA);
			setState(654);
			quadData();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DeleteWhereContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(SparqlAutomaticParser.DELETE, 0); }
		public TerminalNode WHERE() { return getToken(SparqlAutomaticParser.WHERE, 0); }
		public QuadPatternContext quadPattern() {
			return getRuleContext(QuadPatternContext.class,0);
		}
		public DeleteWhereContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteWhere; }
	}

	public final DeleteWhereContext deleteWhere() throws RecognitionException {
		DeleteWhereContext _localctx = new DeleteWhereContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_deleteWhere);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(656);
			match(DELETE);
			setState(657);
			match(WHERE);
			setState(658);
			quadPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ModifyContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(SparqlAutomaticParser.WHERE, 0); }
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public DeleteClauseContext deleteClause() {
			return getRuleContext(DeleteClauseContext.class,0);
		}
		public InsertClauseContext insertClause() {
			return getRuleContext(InsertClauseContext.class,0);
		}
		public TerminalNode WITH() { return getToken(SparqlAutomaticParser.WITH, 0); }
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public List<UsingClauseContext> usingClause() {
			return getRuleContexts(UsingClauseContext.class);
		}
		public UsingClauseContext usingClause(int i) {
			return getRuleContext(UsingClauseContext.class,i);
		}
		public ModifyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_modify; }
	}

	public final ModifyContext modify() throws RecognitionException {
		ModifyContext _localctx = new ModifyContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_modify);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(662);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITH) {
				{
				setState(660);
				match(WITH);
				setState(661);
				iri();
				}
			}

			setState(669);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DELETE:
				{
				setState(664);
				deleteClause();
				setState(666);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==INSERT) {
					{
					setState(665);
					insertClause();
					}
				}

				}
				break;
			case INSERT:
				{
				setState(668);
				insertClause();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(674);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==USING) {
				{
				{
				setState(671);
				usingClause();
				}
				}
				setState(676);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(677);
			match(WHERE);
			setState(678);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DeleteClauseContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(SparqlAutomaticParser.DELETE, 0); }
		public QuadPatternContext quadPattern() {
			return getRuleContext(QuadPatternContext.class,0);
		}
		public DeleteClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteClause; }
	}

	public final DeleteClauseContext deleteClause() throws RecognitionException {
		DeleteClauseContext _localctx = new DeleteClauseContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_deleteClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(680);
			match(DELETE);
			setState(681);
			quadPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InsertClauseContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(SparqlAutomaticParser.INSERT, 0); }
		public QuadPatternContext quadPattern() {
			return getRuleContext(QuadPatternContext.class,0);
		}
		public InsertClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertClause; }
	}

	public final InsertClauseContext insertClause() throws RecognitionException {
		InsertClauseContext _localctx = new InsertClauseContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_insertClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(683);
			match(INSERT);
			setState(684);
			quadPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UsingClauseContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(SparqlAutomaticParser.USING, 0); }
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public TerminalNode NAMED() { return getToken(SparqlAutomaticParser.NAMED, 0); }
		public UsingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_usingClause; }
	}

	public final UsingClauseContext usingClause() throws RecognitionException {
		UsingClauseContext _localctx = new UsingClauseContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_usingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(686);
			match(USING);
			setState(690);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				{
				setState(687);
				iri();
				}
				break;
			case NAMED:
				{
				setState(688);
				match(NAMED);
				setState(689);
				iri();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphOrDefaultContext extends ParserRuleContext {
		public TerminalNode DEFAULT() { return getToken(SparqlAutomaticParser.DEFAULT, 0); }
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public TerminalNode GRAPH() { return getToken(SparqlAutomaticParser.GRAPH, 0); }
		public GraphOrDefaultContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphOrDefault; }
	}

	public final GraphOrDefaultContext graphOrDefault() throws RecognitionException {
		GraphOrDefaultContext _localctx = new GraphOrDefaultContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_graphOrDefault);
		int _la;
		try {
			setState(697);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEFAULT:
				enterOuterAlt(_localctx, 1);
				{
				setState(692);
				match(DEFAULT);
				}
				break;
			case GRAPH:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 2);
				{
				setState(694);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==GRAPH) {
					{
					setState(693);
					match(GRAPH);
					}
				}

				setState(696);
				iri();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphRefContext extends ParserRuleContext {
		public TerminalNode GRAPH() { return getToken(SparqlAutomaticParser.GRAPH, 0); }
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public GraphRefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphRef; }
	}

	public final GraphRefContext graphRef() throws RecognitionException {
		GraphRefContext _localctx = new GraphRefContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_graphRef);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(699);
			match(GRAPH);
			setState(700);
			iri();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphRefAllContext extends ParserRuleContext {
		public GraphRefContext graphRef() {
			return getRuleContext(GraphRefContext.class,0);
		}
		public TerminalNode DEFAULT() { return getToken(SparqlAutomaticParser.DEFAULT, 0); }
		public TerminalNode NAMED() { return getToken(SparqlAutomaticParser.NAMED, 0); }
		public TerminalNode ALL() { return getToken(SparqlAutomaticParser.ALL, 0); }
		public GraphRefAllContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphRefAll; }
	}

	public final GraphRefAllContext graphRefAll() throws RecognitionException {
		GraphRefAllContext _localctx = new GraphRefAllContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_graphRefAll);
		try {
			setState(706);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case GRAPH:
				enterOuterAlt(_localctx, 1);
				{
				setState(702);
				graphRef();
				}
				break;
			case DEFAULT:
				enterOuterAlt(_localctx, 2);
				{
				setState(703);
				match(DEFAULT);
				}
				break;
			case NAMED:
				enterOuterAlt(_localctx, 3);
				{
				setState(704);
				match(NAMED);
				}
				break;
			case ALL:
				enterOuterAlt(_localctx, 4);
				{
				setState(705);
				match(ALL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuadPatternContext extends ParserRuleContext {
		public QuadsContext quads() {
			return getRuleContext(QuadsContext.class,0);
		}
		public QuadPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quadPattern; }
	}

	public final QuadPatternContext quadPattern() throws RecognitionException {
		QuadPatternContext _localctx = new QuadPatternContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_quadPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(708);
			match(T__3);
			setState(709);
			quads();
			setState(710);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuadDataContext extends ParserRuleContext {
		public QuadsContext quads() {
			return getRuleContext(QuadsContext.class,0);
		}
		public QuadDataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quadData; }
	}

	public final QuadDataContext quadData() throws RecognitionException {
		QuadDataContext _localctx = new QuadDataContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_quadData);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(712);
			match(T__3);
			setState(713);
			quads();
			setState(714);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuadsContext extends ParserRuleContext {
		public List<TriplesTemplateContext> triplesTemplate() {
			return getRuleContexts(TriplesTemplateContext.class);
		}
		public TriplesTemplateContext triplesTemplate(int i) {
			return getRuleContext(TriplesTemplateContext.class,i);
		}
		public List<QuadsNotTriplesContext> quadsNotTriples() {
			return getRuleContexts(QuadsNotTriplesContext.class);
		}
		public QuadsNotTriplesContext quadsNotTriples(int i) {
			return getRuleContext(QuadsNotTriplesContext.class,i);
		}
		public QuadsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quads; }
	}

	public final QuadsContext quads() throws RecognitionException {
		QuadsContext _localctx = new QuadsContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_quads);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(717);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
				{
				setState(716);
				triplesTemplate();
				}
			}

			setState(728);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==GRAPH) {
				{
				{
				setState(719);
				quadsNotTriples();
				setState(721);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__6) {
					{
					setState(720);
					match(T__6);
					}
				}

				setState(724);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
					{
					setState(723);
					triplesTemplate();
					}
				}

				}
				}
				setState(730);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QuadsNotTriplesContext extends ParserRuleContext {
		public TerminalNode GRAPH() { return getToken(SparqlAutomaticParser.GRAPH, 0); }
		public VarOrIriContext varOrIri() {
			return getRuleContext(VarOrIriContext.class,0);
		}
		public TriplesTemplateContext triplesTemplate() {
			return getRuleContext(TriplesTemplateContext.class,0);
		}
		public QuadsNotTriplesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quadsNotTriples; }
	}

	public final QuadsNotTriplesContext quadsNotTriples() throws RecognitionException {
		QuadsNotTriplesContext _localctx = new QuadsNotTriplesContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_quadsNotTriples);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(731);
			match(GRAPH);
			setState(732);
			varOrIri();
			setState(733);
			match(T__3);
			setState(735);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
				{
				setState(734);
				triplesTemplate();
				}
			}

			setState(737);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TriplesTemplateContext extends ParserRuleContext {
		public TriplesSameSubjectContext triplesSameSubject() {
			return getRuleContext(TriplesSameSubjectContext.class,0);
		}
		public TriplesTemplateContext triplesTemplate() {
			return getRuleContext(TriplesTemplateContext.class,0);
		}
		public TriplesTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triplesTemplate; }
	}

	public final TriplesTemplateContext triplesTemplate() throws RecognitionException {
		TriplesTemplateContext _localctx = new TriplesTemplateContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_triplesTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(739);
			triplesSameSubject();
			setState(744);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__6) {
				{
				setState(740);
				match(T__6);
				setState(742);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
					{
					setState(741);
					triplesTemplate();
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupGraphPatternContext extends ParserRuleContext {
		public SubSelectContext subSelect() {
			return getRuleContext(SubSelectContext.class,0);
		}
		public GroupGraphPatternSubContext groupGraphPatternSub() {
			return getRuleContext(GroupGraphPatternSubContext.class,0);
		}
		public GroupGraphPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupGraphPattern; }
	}

	public final GroupGraphPatternContext groupGraphPattern() throws RecognitionException {
		GroupGraphPatternContext _localctx = new GroupGraphPatternContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_groupGraphPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(746);
			match(T__3);
			setState(749);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SELECT:
				{
				setState(747);
				subSelect();
				}
				break;
			case T__1:
			case T__3:
			case T__4:
			case T__15:
			case T__27:
			case T__28:
			case VALUES:
			case GRAPH:
			case OPTIONAL:
			case SERVICE:
			case BIND:
			case MINUS:
			case FILTER:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				{
				setState(748);
				groupGraphPatternSub();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(751);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupGraphPatternSubContext extends ParserRuleContext {
		public TriplesBlockContext triplesBlock() {
			return getRuleContext(TriplesBlockContext.class,0);
		}
		public List<GraphPatternNotTriplesAndMaybeTriplesContext> graphPatternNotTriplesAndMaybeTriples() {
			return getRuleContexts(GraphPatternNotTriplesAndMaybeTriplesContext.class);
		}
		public GraphPatternNotTriplesAndMaybeTriplesContext graphPatternNotTriplesAndMaybeTriples(int i) {
			return getRuleContext(GraphPatternNotTriplesAndMaybeTriplesContext.class,i);
		}
		public GroupGraphPatternSubContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupGraphPatternSub; }
	}

	public final GroupGraphPatternSubContext groupGraphPatternSub() throws RecognitionException {
		GroupGraphPatternSubContext _localctx = new GroupGraphPatternSubContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_groupGraphPatternSub);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(754);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
				{
				setState(753);
				triplesBlock();
				}
			}

			setState(759);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3 || _la==VALUES || ((((_la - 69)) & ~0x3f) == 0 && ((1L << (_la - 69)) & 349L) != 0)) {
				{
				{
				setState(756);
				graphPatternNotTriplesAndMaybeTriples();
				}
				}
				setState(761);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphPatternNotTriplesAndMaybeTriplesContext extends ParserRuleContext {
		public GraphPatternNotTriplesContext graphPatternNotTriples() {
			return getRuleContext(GraphPatternNotTriplesContext.class,0);
		}
		public TriplesBlockContext triplesBlock() {
			return getRuleContext(TriplesBlockContext.class,0);
		}
		public GraphPatternNotTriplesAndMaybeTriplesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphPatternNotTriplesAndMaybeTriples; }
	}

	public final GraphPatternNotTriplesAndMaybeTriplesContext graphPatternNotTriplesAndMaybeTriples() throws RecognitionException {
		GraphPatternNotTriplesAndMaybeTriplesContext _localctx = new GraphPatternNotTriplesAndMaybeTriplesContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_graphPatternNotTriplesAndMaybeTriples);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(762);
			graphPatternNotTriples();
			setState(764);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__6) {
				{
				setState(763);
				match(T__6);
				}
			}

			setState(767);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
				{
				setState(766);
				triplesBlock();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TriplesBlockContext extends ParserRuleContext {
		public TriplesSameSubjectPathContext triplesSameSubjectPath() {
			return getRuleContext(TriplesSameSubjectPathContext.class,0);
		}
		public TriplesBlockContext triplesBlock() {
			return getRuleContext(TriplesBlockContext.class,0);
		}
		public TriplesBlockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triplesBlock; }
	}

	public final TriplesBlockContext triplesBlock() throws RecognitionException {
		TriplesBlockContext _localctx = new TriplesBlockContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_triplesBlock);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(769);
			triplesSameSubjectPath();
			setState(774);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__6) {
				{
				setState(770);
				match(T__6);
				setState(772);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
					{
					setState(771);
					triplesBlock();
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphPatternNotTriplesContext extends ParserRuleContext {
		public GroupOrUnionGraphPatternContext groupOrUnionGraphPattern() {
			return getRuleContext(GroupOrUnionGraphPatternContext.class,0);
		}
		public OptionalGraphPatternContext optionalGraphPattern() {
			return getRuleContext(OptionalGraphPatternContext.class,0);
		}
		public MinusGraphPatternContext minusGraphPattern() {
			return getRuleContext(MinusGraphPatternContext.class,0);
		}
		public GraphGraphPatternContext graphGraphPattern() {
			return getRuleContext(GraphGraphPatternContext.class,0);
		}
		public ServiceGraphPatternContext serviceGraphPattern() {
			return getRuleContext(ServiceGraphPatternContext.class,0);
		}
		public FilterRContext filterR() {
			return getRuleContext(FilterRContext.class,0);
		}
		public BindContext bind() {
			return getRuleContext(BindContext.class,0);
		}
		public InlineDataContext inlineData() {
			return getRuleContext(InlineDataContext.class,0);
		}
		public GraphPatternNotTriplesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphPatternNotTriples; }
	}

	public final GraphPatternNotTriplesContext graphPatternNotTriples() throws RecognitionException {
		GraphPatternNotTriplesContext _localctx = new GraphPatternNotTriplesContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_graphPatternNotTriples);
		try {
			setState(784);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__3:
				enterOuterAlt(_localctx, 1);
				{
				setState(776);
				groupOrUnionGraphPattern();
				}
				break;
			case OPTIONAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(777);
				optionalGraphPattern();
				}
				break;
			case MINUS:
				enterOuterAlt(_localctx, 3);
				{
				setState(778);
				minusGraphPattern();
				}
				break;
			case GRAPH:
				enterOuterAlt(_localctx, 4);
				{
				setState(779);
				graphGraphPattern();
				}
				break;
			case SERVICE:
				enterOuterAlt(_localctx, 5);
				{
				setState(780);
				serviceGraphPattern();
				}
				break;
			case FILTER:
				enterOuterAlt(_localctx, 6);
				{
				setState(781);
				filterR();
				}
				break;
			case BIND:
				enterOuterAlt(_localctx, 7);
				{
				setState(782);
				bind();
				}
				break;
			case VALUES:
				enterOuterAlt(_localctx, 8);
				{
				setState(783);
				inlineData();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class OptionalGraphPatternContext extends ParserRuleContext {
		public TerminalNode OPTIONAL() { return getToken(SparqlAutomaticParser.OPTIONAL, 0); }
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public OptionalGraphPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_optionalGraphPattern; }
	}

	public final OptionalGraphPatternContext optionalGraphPattern() throws RecognitionException {
		OptionalGraphPatternContext _localctx = new OptionalGraphPatternContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_optionalGraphPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(786);
			match(OPTIONAL);
			setState(787);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphGraphPatternContext extends ParserRuleContext {
		public TerminalNode GRAPH() { return getToken(SparqlAutomaticParser.GRAPH, 0); }
		public VarOrIriContext varOrIri() {
			return getRuleContext(VarOrIriContext.class,0);
		}
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public GraphGraphPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphGraphPattern; }
	}

	public final GraphGraphPatternContext graphGraphPattern() throws RecognitionException {
		GraphGraphPatternContext _localctx = new GraphGraphPatternContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_graphGraphPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(789);
			match(GRAPH);
			setState(790);
			varOrIri();
			setState(791);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ServiceGraphPatternContext extends ParserRuleContext {
		public TerminalNode SERVICE() { return getToken(SparqlAutomaticParser.SERVICE, 0); }
		public VarOrIriContext varOrIri() {
			return getRuleContext(VarOrIriContext.class,0);
		}
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public TerminalNode SILENT() { return getToken(SparqlAutomaticParser.SILENT, 0); }
		public ServiceGraphPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_serviceGraphPattern; }
	}

	public final ServiceGraphPatternContext serviceGraphPattern() throws RecognitionException {
		ServiceGraphPatternContext _localctx = new ServiceGraphPatternContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_serviceGraphPattern);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(793);
			match(SERVICE);
			setState(795);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SILENT) {
				{
				setState(794);
				match(SILENT);
				}
			}

			setState(797);
			varOrIri();
			setState(798);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BindContext extends ParserRuleContext {
		public TerminalNode BIND() { return getToken(SparqlAutomaticParser.BIND, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode AS() { return getToken(SparqlAutomaticParser.AS, 0); }
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public BindContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bind; }
	}

	public final BindContext bind() throws RecognitionException {
		BindContext _localctx = new BindContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_bind);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(800);
			match(BIND);
			setState(801);
			match(T__1);
			setState(802);
			expression();
			setState(803);
			match(AS);
			setState(804);
			var();
			setState(805);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InlineDataContext extends ParserRuleContext {
		public TerminalNode VALUES() { return getToken(SparqlAutomaticParser.VALUES, 0); }
		public DataBlockContext dataBlock() {
			return getRuleContext(DataBlockContext.class,0);
		}
		public InlineDataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineData; }
	}

	public final InlineDataContext inlineData() throws RecognitionException {
		InlineDataContext _localctx = new InlineDataContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_inlineData);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(807);
			match(VALUES);
			setState(808);
			dataBlock();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DataBlockContext extends ParserRuleContext {
		public InlineDataOneVarContext inlineDataOneVar() {
			return getRuleContext(InlineDataOneVarContext.class,0);
		}
		public InlineDataFullContext inlineDataFull() {
			return getRuleContext(InlineDataFullContext.class,0);
		}
		public DataBlockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataBlock; }
	}

	public final DataBlockContext dataBlock() throws RecognitionException {
		DataBlockContext _localctx = new DataBlockContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_dataBlock);
		try {
			setState(812);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case VAR1:
			case VAR2:
				enterOuterAlt(_localctx, 1);
				{
				setState(810);
				inlineDataOneVar();
				}
				break;
			case T__1:
			case NIL:
				enterOuterAlt(_localctx, 2);
				{
				setState(811);
				inlineDataFull();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InlineDataOneVarContext extends ParserRuleContext {
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public List<DataBlockValueContext> dataBlockValue() {
			return getRuleContexts(DataBlockValueContext.class);
		}
		public DataBlockValueContext dataBlockValue(int i) {
			return getRuleContext(DataBlockValueContext.class,i);
		}
		public InlineDataOneVarContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineDataOneVar; }
	}

	public final InlineDataOneVarContext inlineDataOneVar() throws RecognitionException {
		InlineDataOneVarContext _localctx = new InlineDataOneVarContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_inlineDataOneVar);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(814);
			var();
			setState(815);
			match(T__3);
			setState(819);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 28)) & ~0x3f) == 0 && ((1L << (_la - 28)) & 70368744177667L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 4063111L) != 0)) {
				{
				{
				setState(816);
				dataBlockValue();
				}
				}
				setState(821);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(822);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class InlineDataFullContext extends ParserRuleContext {
		public TerminalNode NIL() { return getToken(SparqlAutomaticParser.NIL, 0); }
		public List<DataBlockSingleContext> dataBlockSingle() {
			return getRuleContexts(DataBlockSingleContext.class);
		}
		public DataBlockSingleContext dataBlockSingle(int i) {
			return getRuleContext(DataBlockSingleContext.class,i);
		}
		public List<VarContext> var() {
			return getRuleContexts(VarContext.class);
		}
		public VarContext var(int i) {
			return getRuleContext(VarContext.class,i);
		}
		public InlineDataFullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inlineDataFull; }
	}

	public final InlineDataFullContext inlineDataFull() throws RecognitionException {
		InlineDataFullContext _localctx = new InlineDataFullContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_inlineDataFull);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(833);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NIL:
				{
				setState(824);
				match(NIL);
				}
				break;
			case T__1:
				{
				setState(825);
				match(T__1);
				setState(829);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==VAR1 || _la==VAR2) {
					{
					{
					setState(826);
					var();
					}
					}
					setState(831);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(832);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(835);
			match(T__3);
			setState(839);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1 || _la==NIL) {
				{
				{
				setState(836);
				dataBlockSingle();
				}
				}
				setState(841);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(842);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DataBlockSingleContext extends ParserRuleContext {
		public TerminalNode NIL() { return getToken(SparqlAutomaticParser.NIL, 0); }
		public List<DataBlockValueContext> dataBlockValue() {
			return getRuleContexts(DataBlockValueContext.class);
		}
		public DataBlockValueContext dataBlockValue(int i) {
			return getRuleContext(DataBlockValueContext.class,i);
		}
		public DataBlockSingleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataBlockSingle; }
	}

	public final DataBlockSingleContext dataBlockSingle() throws RecognitionException {
		DataBlockSingleContext _localctx = new DataBlockSingleContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_dataBlockSingle);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(853);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
				{
				setState(844);
				match(T__1);
				setState(848);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (((((_la - 28)) & ~0x3f) == 0 && ((1L << (_la - 28)) & 70368744177667L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 4063111L) != 0)) {
					{
					{
					setState(845);
					dataBlockValue();
					}
					}
					setState(850);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(851);
				match(T__2);
				}
				break;
			case NIL:
				{
				setState(852);
				match(NIL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DataBlockValueContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public RdfLiteralContext rdfLiteral() {
			return getRuleContext(RdfLiteralContext.class,0);
		}
		public NumericLiteralContext numericLiteral() {
			return getRuleContext(NumericLiteralContext.class,0);
		}
		public BooleanLiteralContext booleanLiteral() {
			return getRuleContext(BooleanLiteralContext.class,0);
		}
		public TerminalNode UNDEF() { return getToken(SparqlAutomaticParser.UNDEF, 0); }
		public DataBlockValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataBlockValue; }
	}

	public final DataBlockValueContext dataBlockValue() throws RecognitionException {
		DataBlockValueContext _localctx = new DataBlockValueContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_dataBlockValue);
		try {
			setState(860);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(855);
				iri();
				}
				break;
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
				enterOuterAlt(_localctx, 2);
				{
				setState(856);
				rdfLiteral();
				}
				break;
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
				enterOuterAlt(_localctx, 3);
				{
				setState(857);
				numericLiteral();
				}
				break;
			case T__27:
			case T__28:
				enterOuterAlt(_localctx, 4);
				{
				setState(858);
				booleanLiteral();
				}
				break;
			case UNDEF:
				enterOuterAlt(_localctx, 5);
				{
				setState(859);
				match(UNDEF);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MinusGraphPatternContext extends ParserRuleContext {
		public TerminalNode MINUS() { return getToken(SparqlAutomaticParser.MINUS, 0); }
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public MinusGraphPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_minusGraphPattern; }
	}

	public final MinusGraphPatternContext minusGraphPattern() throws RecognitionException {
		MinusGraphPatternContext _localctx = new MinusGraphPatternContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_minusGraphPattern);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(862);
			match(MINUS);
			setState(863);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GroupOrUnionGraphPatternContext extends ParserRuleContext {
		public List<GroupGraphPatternContext> groupGraphPattern() {
			return getRuleContexts(GroupGraphPatternContext.class);
		}
		public GroupGraphPatternContext groupGraphPattern(int i) {
			return getRuleContext(GroupGraphPatternContext.class,i);
		}
		public List<TerminalNode> UNION() { return getTokens(SparqlAutomaticParser.UNION); }
		public TerminalNode UNION(int i) {
			return getToken(SparqlAutomaticParser.UNION, i);
		}
		public GroupOrUnionGraphPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupOrUnionGraphPattern; }
	}

	public final GroupOrUnionGraphPatternContext groupOrUnionGraphPattern() throws RecognitionException {
		GroupOrUnionGraphPatternContext _localctx = new GroupOrUnionGraphPatternContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_groupOrUnionGraphPattern);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(865);
			groupGraphPattern();
			setState(870);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==UNION) {
				{
				{
				setState(866);
				match(UNION);
				setState(867);
				groupGraphPattern();
				}
				}
				setState(872);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FilterRContext extends ParserRuleContext {
		public TerminalNode FILTER() { return getToken(SparqlAutomaticParser.FILTER, 0); }
		public ConstraintContext constraint() {
			return getRuleContext(ConstraintContext.class,0);
		}
		public FilterRContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filterR; }
	}

	public final FilterRContext filterR() throws RecognitionException {
		FilterRContext _localctx = new FilterRContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_filterR);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(873);
			match(FILTER);
			setState(874);
			constraint();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstraintContext extends ParserRuleContext {
		public BrackettedExpressionContext brackettedExpression() {
			return getRuleContext(BrackettedExpressionContext.class,0);
		}
		public BuiltInCallContext builtInCall() {
			return getRuleContext(BuiltInCallContext.class,0);
		}
		public FunctionCallContext functionCall() {
			return getRuleContext(FunctionCallContext.class,0);
		}
		public ConstraintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constraint; }
	}

	public final ConstraintContext constraint() throws RecognitionException {
		ConstraintContext _localctx = new ConstraintContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_constraint);
		try {
			setState(879);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
				enterOuterAlt(_localctx, 1);
				{
				setState(876);
				brackettedExpression();
				}
				break;
			case GROUP_CONCAT:
			case NOT:
			case STR:
			case LANG:
			case LANGMATCHES:
			case DATATYPE:
			case BOUND:
			case IRI:
			case URI:
			case BNODE:
			case RAND:
			case ABS:
			case CEIL:
			case FLOOR:
			case ROUND:
			case CONCAT:
			case STRLEN:
			case UCASE:
			case LCASE:
			case ENCODE_FOR_URI:
			case CONTAINS:
			case STRSTARTS:
			case STRENDS:
			case STRBEFORE:
			case STRAFTER:
			case YEAR:
			case MONTH:
			case DAY:
			case HOURS:
			case MINUTES:
			case SECONDS:
			case TIMEZONE:
			case TZ:
			case NOW:
			case UUID:
			case STRUUID:
			case SHA1:
			case SHA256:
			case SHA384:
			case SHA512:
			case MD5:
			case COALESCE:
			case IF:
			case STRLANG:
			case STRDT:
			case SAMETERM:
			case ISIRI:
			case ISURI:
			case ISBLANK:
			case ISLITERAL:
			case ISNUMERIC:
			case REGEX:
			case SUBSTR:
			case REPLACE:
			case EXISTS:
			case COUNT:
			case SUM:
			case MIN:
			case MAX:
			case AVG:
			case STDEV:
			case SAMPLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(877);
				builtInCall();
				}
				break;
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 3);
				{
				setState(878);
				functionCall();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class FunctionCallContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public ArgListContext argList() {
			return getRuleContext(ArgListContext.class,0);
		}
		public FunctionCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionCall; }
	}

	public final FunctionCallContext functionCall() throws RecognitionException {
		FunctionCallContext _localctx = new FunctionCallContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_functionCall);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(881);
			iri();
			setState(882);
			argList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ArgListContext extends ParserRuleContext {
		public TerminalNode NIL() { return getToken(SparqlAutomaticParser.NIL, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode DISTINCT() { return getToken(SparqlAutomaticParser.DISTINCT, 0); }
		public ArgListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_argList; }
	}

	public final ArgListContext argList() throws RecognitionException {
		ArgListContext _localctx = new ArgListContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_argList);
		int _la;
		try {
			setState(899);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NIL:
				enterOuterAlt(_localctx, 1);
				{
				setState(884);
				match(NIL);
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				setState(885);
				match(T__1);
				setState(887);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(886);
					match(DISTINCT);
					}
				}

				setState(889);
				expression();
				setState(894);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__7) {
					{
					{
					setState(890);
					match(T__7);
					setState(891);
					expression();
					}
					}
					setState(896);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(897);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionListContext extends ParserRuleContext {
		public TerminalNode NIL() { return getToken(SparqlAutomaticParser.NIL, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public ExpressionListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionList; }
	}

	public final ExpressionListContext expressionList() throws RecognitionException {
		ExpressionListContext _localctx = new ExpressionListContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_expressionList);
		int _la;
		try {
			setState(913);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case NIL:
				enterOuterAlt(_localctx, 1);
				{
				setState(901);
				match(NIL);
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				setState(902);
				match(T__1);
				setState(903);
				expression();
				setState(908);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__7) {
					{
					{
					setState(904);
					match(T__7);
					setState(905);
					expression();
					}
					}
					setState(910);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(911);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstructTemplateContext extends ParserRuleContext {
		public ConstructTriplesContext constructTriples() {
			return getRuleContext(ConstructTriplesContext.class,0);
		}
		public ConstructTemplateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constructTemplate; }
	}

	public final ConstructTemplateContext constructTemplate() throws RecognitionException {
		ConstructTemplateContext _localctx = new ConstructTemplateContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_constructTemplate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(915);
			match(T__3);
			setState(917);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
				{
				setState(916);
				constructTriples();
				}
			}

			setState(919);
			match(T__4);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConstructTriplesContext extends ParserRuleContext {
		public TriplesSameSubjectContext triplesSameSubject() {
			return getRuleContext(TriplesSameSubjectContext.class,0);
		}
		public ConstructTriplesContext constructTriples() {
			return getRuleContext(ConstructTriplesContext.class,0);
		}
		public ConstructTriplesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constructTriples; }
	}

	public final ConstructTriplesContext constructTriples() throws RecognitionException {
		ConstructTriplesContext _localctx = new ConstructTriplesContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_constructTriples);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(921);
			triplesSameSubject();
			setState(926);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__6) {
				{
				setState(922);
				match(T__6);
				setState(924);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0)) {
					{
					setState(923);
					constructTriples();
					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TriplesSameSubjectContext extends ParserRuleContext {
		public VarOrTermContext varOrTerm() {
			return getRuleContext(VarOrTermContext.class,0);
		}
		public PropertyListNotEmptyContext propertyListNotEmpty() {
			return getRuleContext(PropertyListNotEmptyContext.class,0);
		}
		public TriplesNodeContext triplesNode() {
			return getRuleContext(TriplesNodeContext.class,0);
		}
		public PropertyListContext propertyList() {
			return getRuleContext(PropertyListContext.class,0);
		}
		public TriplesSameSubjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triplesSameSubject; }
	}

	public final TriplesSameSubjectContext triplesSameSubject() throws RecognitionException {
		TriplesSameSubjectContext _localctx = new TriplesSameSubjectContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_triplesSameSubject);
		try {
			setState(934);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__27:
			case T__28:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				enterOuterAlt(_localctx, 1);
				{
				setState(928);
				varOrTerm();
				setState(929);
				propertyListNotEmpty();
				}
				break;
			case T__1:
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(931);
				triplesNode();
				setState(932);
				propertyList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyListContext extends ParserRuleContext {
		public PropertyListNotEmptyContext propertyListNotEmpty() {
			return getRuleContext(PropertyListNotEmptyContext.class,0);
		}
		public PropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyList; }
	}

	public final PropertyListContext propertyList() throws RecognitionException {
		PropertyListContext _localctx = new PropertyListContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_propertyList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(937);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__8 || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0)) {
				{
				setState(936);
				propertyListNotEmpty();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyListNotEmptyContext extends ParserRuleContext {
		public List<VerbContext> verb() {
			return getRuleContexts(VerbContext.class);
		}
		public VerbContext verb(int i) {
			return getRuleContext(VerbContext.class,i);
		}
		public List<ObjectListContext> objectList() {
			return getRuleContexts(ObjectListContext.class);
		}
		public ObjectListContext objectList(int i) {
			return getRuleContext(ObjectListContext.class,i);
		}
		public PropertyListNotEmptyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyListNotEmpty; }
	}

	public final PropertyListNotEmptyContext propertyListNotEmpty() throws RecognitionException {
		PropertyListNotEmptyContext _localctx = new PropertyListNotEmptyContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_propertyListNotEmpty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(939);
			verb();
			setState(940);
			objectList();
			setState(949);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__5) {
				{
				{
				setState(941);
				match(T__5);
				setState(945);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__8 || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0)) {
					{
					setState(942);
					verb();
					setState(943);
					objectList();
					}
				}

				}
				}
				setState(951);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VerbContext extends ParserRuleContext {
		public VarOrIriContext varOrIri() {
			return getRuleContext(VarOrIriContext.class,0);
		}
		public VerbContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_verb; }
	}

	public final VerbContext verb() throws RecognitionException {
		VerbContext _localctx = new VerbContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_verb);
		try {
			setState(954);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(952);
				varOrIri();
				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 2);
				{
				setState(953);
				match(T__8);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObjectListContext extends ParserRuleContext {
		public List<ObjectRContext> objectR() {
			return getRuleContexts(ObjectRContext.class);
		}
		public ObjectRContext objectR(int i) {
			return getRuleContext(ObjectRContext.class,i);
		}
		public ObjectListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_objectList; }
	}

	public final ObjectListContext objectList() throws RecognitionException {
		ObjectListContext _localctx = new ObjectListContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_objectList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(956);
			objectR();
			setState(961);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(957);
				match(T__7);
				setState(958);
				objectR();
				}
				}
				setState(963);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObjectRContext extends ParserRuleContext {
		public GraphNodeContext graphNode() {
			return getRuleContext(GraphNodeContext.class,0);
		}
		public ObjectRContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_objectR; }
	}

	public final ObjectRContext objectR() throws RecognitionException {
		ObjectRContext _localctx = new ObjectRContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_objectR);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(964);
			graphNode();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TriplesSameSubjectPathContext extends ParserRuleContext {
		public VarOrTermContext varOrTerm() {
			return getRuleContext(VarOrTermContext.class,0);
		}
		public PropertyListPathNotEmptyContext propertyListPathNotEmpty() {
			return getRuleContext(PropertyListPathNotEmptyContext.class,0);
		}
		public TriplesNodePathContext triplesNodePath() {
			return getRuleContext(TriplesNodePathContext.class,0);
		}
		public PropertyListPathContext propertyListPath() {
			return getRuleContext(PropertyListPathContext.class,0);
		}
		public TriplesSameSubjectPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triplesSameSubjectPath; }
	}

	public final TriplesSameSubjectPathContext triplesSameSubjectPath() throws RecognitionException {
		TriplesSameSubjectPathContext _localctx = new TriplesSameSubjectPathContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_triplesSameSubjectPath);
		try {
			setState(972);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__27:
			case T__28:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				enterOuterAlt(_localctx, 1);
				{
				setState(966);
				varOrTerm();
				setState(967);
				propertyListPathNotEmpty();
				}
				break;
			case T__1:
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(969);
				triplesNodePath();
				setState(970);
				propertyListPath();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyListPathContext extends ParserRuleContext {
		public PropertyListPathNotEmptyContext propertyListPathNotEmpty() {
			return getRuleContext(PropertyListPathNotEmptyContext.class,0);
		}
		public PropertyListPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyListPath; }
	}

	public final PropertyListPathContext propertyListPath() throws RecognitionException {
		PropertyListPathContext _localctx = new PropertyListPathContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_propertyListPath);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(975);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 37380L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0)) {
				{
				setState(974);
				propertyListPathNotEmpty();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PropertyListPathNotEmptyContext extends ParserRuleContext {
		public TupleWithPathContext tupleWithPath() {
			return getRuleContext(TupleWithPathContext.class,0);
		}
		public List<TupleWithoutPathContext> tupleWithoutPath() {
			return getRuleContexts(TupleWithoutPathContext.class);
		}
		public TupleWithoutPathContext tupleWithoutPath(int i) {
			return getRuleContext(TupleWithoutPathContext.class,i);
		}
		public PropertyListPathNotEmptyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyListPathNotEmpty; }
	}

	public final PropertyListPathNotEmptyContext propertyListPathNotEmpty() throws RecognitionException {
		PropertyListPathNotEmptyContext _localctx = new PropertyListPathNotEmptyContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_propertyListPathNotEmpty);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(977);
			tupleWithPath();
			setState(984);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__5) {
				{
				{
				setState(978);
				match(T__5);
				setState(980);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 37380L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 183L) != 0)) {
					{
					setState(979);
					tupleWithoutPath();
					}
				}

				}
				}
				setState(986);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VerbPathContext extends ParserRuleContext {
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public VerbPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_verbPath; }
	}

	public final VerbPathContext verbPath() throws RecognitionException {
		VerbPathContext _localctx = new VerbPathContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_verbPath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(987);
			path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VerbSimpleContext extends ParserRuleContext {
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public VerbSimpleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_verbSimple; }
	}

	public final VerbSimpleContext verbSimple() throws RecognitionException {
		VerbSimpleContext _localctx = new VerbSimpleContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_verbSimple);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(989);
			var();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TupleWithoutPathContext extends ParserRuleContext {
		public VerbPathOrSimpleContext verbPathOrSimple() {
			return getRuleContext(VerbPathOrSimpleContext.class,0);
		}
		public ObjectListContext objectList() {
			return getRuleContext(ObjectListContext.class,0);
		}
		public TupleWithoutPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tupleWithoutPath; }
	}

	public final TupleWithoutPathContext tupleWithoutPath() throws RecognitionException {
		TupleWithoutPathContext _localctx = new TupleWithoutPathContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_tupleWithoutPath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(991);
			verbPathOrSimple();
			setState(992);
			objectList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TupleWithPathContext extends ParserRuleContext {
		public VerbPathOrSimpleContext verbPathOrSimple() {
			return getRuleContext(VerbPathOrSimpleContext.class,0);
		}
		public ObjectListPathContext objectListPath() {
			return getRuleContext(ObjectListPathContext.class,0);
		}
		public TupleWithPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tupleWithPath; }
	}

	public final TupleWithPathContext tupleWithPath() throws RecognitionException {
		TupleWithPathContext _localctx = new TupleWithPathContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_tupleWithPath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(994);
			verbPathOrSimple();
			setState(995);
			objectListPath();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VerbPathOrSimpleContext extends ParserRuleContext {
		public VerbPathContext verbPath() {
			return getRuleContext(VerbPathContext.class,0);
		}
		public VerbSimpleContext verbSimple() {
			return getRuleContext(VerbSimpleContext.class,0);
		}
		public VerbPathOrSimpleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_verbPathOrSimple; }
	}

	public final VerbPathOrSimpleContext verbPathOrSimple() throws RecognitionException {
		VerbPathOrSimpleContext _localctx = new VerbPathOrSimpleContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_verbPathOrSimple);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(999);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case T__8:
			case T__11:
			case T__14:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				{
				setState(997);
				verbPath();
				}
				break;
			case VAR1:
			case VAR2:
				{
				setState(998);
				verbSimple();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObjectListPathContext extends ParserRuleContext {
		public List<ObjectPathContext> objectPath() {
			return getRuleContexts(ObjectPathContext.class);
		}
		public ObjectPathContext objectPath(int i) {
			return getRuleContext(ObjectPathContext.class,i);
		}
		public ObjectListPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_objectListPath; }
	}

	public final ObjectListPathContext objectListPath() throws RecognitionException {
		ObjectListPathContext _localctx = new ObjectListPathContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_objectListPath);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1001);
			objectPath();
			setState(1006);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__7) {
				{
				{
				setState(1002);
				match(T__7);
				setState(1003);
				objectPath();
				}
				}
				setState(1008);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObjectPathContext extends ParserRuleContext {
		public GraphNodePathContext graphNodePath() {
			return getRuleContext(GraphNodePathContext.class,0);
		}
		public ObjectPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_objectPath; }
	}

	public final ObjectPathContext objectPath() throws RecognitionException {
		ObjectPathContext _localctx = new ObjectPathContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_objectPath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1009);
			graphNodePath();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathContext extends ParserRuleContext {
		public PathAlternativeContext pathAlternative() {
			return getRuleContext(PathAlternativeContext.class,0);
		}
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_path);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1011);
			pathAlternative();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathAlternativeContext extends ParserRuleContext {
		public List<PathSequenceContext> pathSequence() {
			return getRuleContexts(PathSequenceContext.class);
		}
		public PathSequenceContext pathSequence(int i) {
			return getRuleContext(PathSequenceContext.class,i);
		}
		public PathAlternativeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathAlternative; }
	}

	public final PathAlternativeContext pathAlternative() throws RecognitionException {
		PathAlternativeContext _localctx = new PathAlternativeContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_pathAlternative);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1013);
			pathSequence();
			setState(1018);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__9) {
				{
				{
				setState(1014);
				match(T__9);
				setState(1015);
				pathSequence();
				}
				}
				setState(1020);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathSequenceContext extends ParserRuleContext {
		public List<PathEltOrInverseContext> pathEltOrInverse() {
			return getRuleContexts(PathEltOrInverseContext.class);
		}
		public PathEltOrInverseContext pathEltOrInverse(int i) {
			return getRuleContext(PathEltOrInverseContext.class,i);
		}
		public PathSequenceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathSequence; }
	}

	public final PathSequenceContext pathSequence() throws RecognitionException {
		PathSequenceContext _localctx = new PathSequenceContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_pathSequence);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1021);
			pathEltOrInverse();
			setState(1026);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__10) {
				{
				{
				setState(1022);
				match(T__10);
				setState(1023);
				pathEltOrInverse();
				}
				}
				setState(1028);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathEltContext extends ParserRuleContext {
		public PathPrimaryContext pathPrimary() {
			return getRuleContext(PathPrimaryContext.class,0);
		}
		public PathModContext pathMod() {
			return getRuleContext(PathModContext.class,0);
		}
		public PathEltContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathElt; }
	}

	public final PathEltContext pathElt() throws RecognitionException {
		PathEltContext _localctx = new PathEltContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_pathElt);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1029);
			pathPrimary();
			setState(1031);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & 24594L) != 0)) {
				{
				setState(1030);
				pathMod();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathEltOrInverseContext extends ParserRuleContext {
		public Token negationOperator;
		public PathEltContext pathElt() {
			return getRuleContext(PathEltContext.class,0);
		}
		public PathEltOrInverseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathEltOrInverse; }
	}

	public final PathEltOrInverseContext pathEltOrInverse() throws RecognitionException {
		PathEltOrInverseContext _localctx = new PathEltOrInverseContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_pathEltOrInverse);
		try {
			setState(1036);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
			case T__8:
			case T__14:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(1033);
				pathElt();
				}
				break;
			case T__11:
				enterOuterAlt(_localctx, 2);
				{
				setState(1034);
				((PathEltOrInverseContext)_localctx).negationOperator = match(T__11);
				setState(1035);
				pathElt();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathModContext extends ParserRuleContext {
		public MinMaxContext minMax() {
			return getRuleContext(MinMaxContext.class,0);
		}
		public PathModContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathMod; }
	}

	public final PathModContext pathMod() throws RecognitionException {
		PathModContext _localctx = new PathModContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_pathMod);
		try {
			setState(1045);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__12:
				enterOuterAlt(_localctx, 1);
				{
				setState(1038);
				match(T__12);
				}
				break;
			case T__0:
				enterOuterAlt(_localctx, 2);
				{
				setState(1039);
				match(T__0);
				}
				break;
			case T__13:
				enterOuterAlt(_localctx, 3);
				{
				setState(1040);
				match(T__13);
				}
				break;
			case T__3:
				enterOuterAlt(_localctx, 4);
				{
				setState(1041);
				match(T__3);
				setState(1042);
				minMax();
				setState(1043);
				match(T__4);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MinMaxContext extends ParserRuleContext {
		public StepsMinContext stepsMin() {
			return getRuleContext(StepsMinContext.class,0);
		}
		public StepsMaxContext stepsMax() {
			return getRuleContext(StepsMaxContext.class,0);
		}
		public StepsExactContext stepsExact() {
			return getRuleContext(StepsExactContext.class,0);
		}
		public MinMaxContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_minMax; }
	}

	public final MinMaxContext minMax() throws RecognitionException {
		MinMaxContext _localctx = new MinMaxContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_minMax);
		try {
			setState(1057);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1047);
				stepsMin();
				setState(1048);
				match(T__7);
				setState(1049);
				stepsMax();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1051);
				stepsExact();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1052);
				stepsMin();
				setState(1053);
				match(T__7);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(1055);
				match(T__7);
				setState(1056);
				stepsMax();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StepsMinContext extends ParserRuleContext {
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public StepsMinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stepsMin; }
	}

	public final StepsMinContext stepsMin() throws RecognitionException {
		StepsMinContext _localctx = new StepsMinContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_stepsMin);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1059);
			integer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StepsMaxContext extends ParserRuleContext {
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public StepsMaxContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stepsMax; }
	}

	public final StepsMaxContext stepsMax() throws RecognitionException {
		StepsMaxContext _localctx = new StepsMaxContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_stepsMax);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1061);
			integer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StepsExactContext extends ParserRuleContext {
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public StepsExactContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stepsExact; }
	}

	public final StepsExactContext stepsExact() throws RecognitionException {
		StepsExactContext _localctx = new StepsExactContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_stepsExact);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1063);
			integer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathPrimaryContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public PathNegatedPropertySetContext pathNegatedPropertySet() {
			return getRuleContext(PathNegatedPropertySetContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public PathPrimaryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathPrimary; }
	}

	public final PathPrimaryContext pathPrimary() throws RecognitionException {
		PathPrimaryContext _localctx = new PathPrimaryContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_pathPrimary);
		try {
			setState(1073);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(1065);
				iri();
				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 2);
				{
				setState(1066);
				match(T__8);
				}
				break;
			case T__14:
				enterOuterAlt(_localctx, 3);
				{
				setState(1067);
				match(T__14);
				setState(1068);
				pathNegatedPropertySet();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 4);
				{
				setState(1069);
				match(T__1);
				setState(1070);
				path();
				setState(1071);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathNegatedPropertySetContext extends ParserRuleContext {
		public List<PathOneInPropertySetContext> pathOneInPropertySet() {
			return getRuleContexts(PathOneInPropertySetContext.class);
		}
		public PathOneInPropertySetContext pathOneInPropertySet(int i) {
			return getRuleContext(PathOneInPropertySetContext.class,i);
		}
		public PathNegatedPropertySetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathNegatedPropertySet; }
	}

	public final PathNegatedPropertySetContext pathNegatedPropertySet() throws RecognitionException {
		PathNegatedPropertySetContext _localctx = new PathNegatedPropertySetContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_pathNegatedPropertySet);
		int _la;
		try {
			setState(1088);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__8:
			case T__11:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(1075);
				pathOneInPropertySet();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				setState(1076);
				match(T__1);
				setState(1085);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__8 || _la==T__11 || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 135L) != 0)) {
					{
					setState(1077);
					pathOneInPropertySet();
					setState(1082);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__9) {
						{
						{
						setState(1078);
						match(T__9);
						setState(1079);
						pathOneInPropertySet();
						}
						}
						setState(1084);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					}
				}

				setState(1087);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PathOneInPropertySetContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public PathOneInPropertySetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathOneInPropertySet; }
	}

	public final PathOneInPropertySetContext pathOneInPropertySet() throws RecognitionException {
		PathOneInPropertySetContext _localctx = new PathOneInPropertySetContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_pathOneInPropertySet);
		try {
			setState(1097);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(1090);
				iri();
				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 2);
				{
				setState(1091);
				match(T__8);
				}
				break;
			case T__11:
				enterOuterAlt(_localctx, 3);
				{
				setState(1092);
				match(T__11);
				setState(1095);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case IRI_REF:
				case PNAME_NS:
				case PNAME_LN:
				case PREFIX_LANGTAG:
					{
					setState(1093);
					iri();
					}
					break;
				case T__8:
					{
					setState(1094);
					match(T__8);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IntegerContext extends ParserRuleContext {
		public TerminalNode INTEGER() { return getToken(SparqlAutomaticParser.INTEGER, 0); }
		public IntegerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_integer; }
	}

	public final IntegerContext integer() throws RecognitionException {
		IntegerContext _localctx = new IntegerContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_integer);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1099);
			match(INTEGER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TriplesNodeContext extends ParserRuleContext {
		public CollectionContext collection() {
			return getRuleContext(CollectionContext.class,0);
		}
		public BlankNodePropertyListContext blankNodePropertyList() {
			return getRuleContext(BlankNodePropertyListContext.class,0);
		}
		public TriplesNodeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triplesNode; }
	}

	public final TriplesNodeContext triplesNode() throws RecognitionException {
		TriplesNodeContext _localctx = new TriplesNodeContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_triplesNode);
		try {
			setState(1103);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1101);
				collection();
				}
				break;
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(1102);
				blankNodePropertyList();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BlankNodePropertyListContext extends ParserRuleContext {
		public PropertyListNotEmptyContext propertyListNotEmpty() {
			return getRuleContext(PropertyListNotEmptyContext.class,0);
		}
		public BlankNodePropertyListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_blankNodePropertyList; }
	}

	public final BlankNodePropertyListContext blankNodePropertyList() throws RecognitionException {
		BlankNodePropertyListContext _localctx = new BlankNodePropertyListContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_blankNodePropertyList);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1105);
			match(T__15);
			setState(1106);
			propertyListNotEmpty();
			setState(1107);
			match(T__16);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class TriplesNodePathContext extends ParserRuleContext {
		public CollectionPathContext collectionPath() {
			return getRuleContext(CollectionPathContext.class,0);
		}
		public BlankNodePropertyListPathContext blankNodePropertyListPath() {
			return getRuleContext(BlankNodePropertyListPathContext.class,0);
		}
		public TriplesNodePathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triplesNodePath; }
	}

	public final TriplesNodePathContext triplesNodePath() throws RecognitionException {
		TriplesNodePathContext _localctx = new TriplesNodePathContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_triplesNodePath);
		try {
			setState(1111);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1109);
				collectionPath();
				}
				break;
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(1110);
				blankNodePropertyListPath();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BlankNodePropertyListPathContext extends ParserRuleContext {
		public PropertyListPathNotEmptyContext propertyListPathNotEmpty() {
			return getRuleContext(PropertyListPathNotEmptyContext.class,0);
		}
		public BlankNodePropertyListPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_blankNodePropertyListPath; }
	}

	public final BlankNodePropertyListPathContext blankNodePropertyListPath() throws RecognitionException {
		BlankNodePropertyListPathContext _localctx = new BlankNodePropertyListPathContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_blankNodePropertyListPath);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1113);
			match(T__15);
			setState(1114);
			propertyListPathNotEmpty();
			setState(1115);
			match(T__16);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CollectionContext extends ParserRuleContext {
		public List<GraphNodeContext> graphNode() {
			return getRuleContexts(GraphNodeContext.class);
		}
		public GraphNodeContext graphNode(int i) {
			return getRuleContext(GraphNodeContext.class,i);
		}
		public CollectionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_collection; }
	}

	public final CollectionContext collection() throws RecognitionException {
		CollectionContext _localctx = new CollectionContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_collection);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1117);
			match(T__1);
			setState(1119); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(1118);
				graphNode();
				}
				}
				setState(1121); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0) );
			setState(1123);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CollectionPathContext extends ParserRuleContext {
		public List<GraphNodePathContext> graphNodePath() {
			return getRuleContexts(GraphNodePathContext.class);
		}
		public GraphNodePathContext graphNodePath(int i) {
			return getRuleContext(GraphNodePathContext.class,i);
		}
		public CollectionPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_collectionPath; }
	}

	public final CollectionPathContext collectionPath() throws RecognitionException {
		CollectionPathContext _localctx = new CollectionPathContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_collectionPath);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1125);
			match(T__1);
			setState(1127); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(1126);
				graphNodePath();
				}
				}
				setState(1129); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & 805371908L) != 0) || ((((_la - 142)) & ~0x3f) == 0 && ((1L << (_la - 142)) & 29228991L) != 0) );
			setState(1131);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphNodeContext extends ParserRuleContext {
		public VarOrTermContext varOrTerm() {
			return getRuleContext(VarOrTermContext.class,0);
		}
		public TriplesNodeContext triplesNode() {
			return getRuleContext(TriplesNodeContext.class,0);
		}
		public GraphNodeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphNode; }
	}

	public final GraphNodeContext graphNode() throws RecognitionException {
		GraphNodeContext _localctx = new GraphNodeContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_graphNode);
		try {
			setState(1135);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__27:
			case T__28:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1133);
				varOrTerm();
				}
				break;
			case T__1:
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(1134);
				triplesNode();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphNodePathContext extends ParserRuleContext {
		public VarOrTermContext varOrTerm() {
			return getRuleContext(VarOrTermContext.class,0);
		}
		public TriplesNodePathContext triplesNodePath() {
			return getRuleContext(TriplesNodePathContext.class,0);
		}
		public GraphNodePathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphNodePath; }
	}

	public final GraphNodePathContext graphNodePath() throws RecognitionException {
		GraphNodePathContext _localctx = new GraphNodePathContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_graphNodePath);
		try {
			setState(1139);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__27:
			case T__28:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				enterOuterAlt(_localctx, 1);
				{
				setState(1137);
				varOrTerm();
				}
				break;
			case T__1:
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(1138);
				triplesNodePath();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VarOrTermContext extends ParserRuleContext {
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public GraphTermContext graphTerm() {
			return getRuleContext(GraphTermContext.class,0);
		}
		public VarOrTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varOrTerm; }
	}

	public final VarOrTermContext varOrTerm() throws RecognitionException {
		VarOrTermContext _localctx = new VarOrTermContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_varOrTerm);
		try {
			setState(1143);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case VAR1:
			case VAR2:
				enterOuterAlt(_localctx, 1);
				{
				setState(1141);
				var();
				}
				break;
			case T__27:
			case T__28:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				enterOuterAlt(_localctx, 2);
				{
				setState(1142);
				graphTerm();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VarOrIriContext extends ParserRuleContext {
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public VarOrIriContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_varOrIri; }
	}

	public final VarOrIriContext varOrIri() throws RecognitionException {
		VarOrIriContext _localctx = new VarOrIriContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_varOrIri);
		try {
			setState(1147);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case VAR1:
			case VAR2:
				enterOuterAlt(_localctx, 1);
				{
				setState(1145);
				var();
				}
				break;
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 2);
				{
				setState(1146);
				iri();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class VarContext extends ParserRuleContext {
		public TerminalNode VAR1() { return getToken(SparqlAutomaticParser.VAR1, 0); }
		public TerminalNode VAR2() { return getToken(SparqlAutomaticParser.VAR2, 0); }
		public VarContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_var; }
	}

	public final VarContext var() throws RecognitionException {
		VarContext _localctx = new VarContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_var);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1149);
			_la = _input.LA(1);
			if ( !(_la==VAR1 || _la==VAR2) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class GraphTermContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public RdfLiteralContext rdfLiteral() {
			return getRuleContext(RdfLiteralContext.class,0);
		}
		public NumericLiteralContext numericLiteral() {
			return getRuleContext(NumericLiteralContext.class,0);
		}
		public BooleanLiteralContext booleanLiteral() {
			return getRuleContext(BooleanLiteralContext.class,0);
		}
		public BlankNodeContext blankNode() {
			return getRuleContext(BlankNodeContext.class,0);
		}
		public TerminalNode NIL() { return getToken(SparqlAutomaticParser.NIL, 0); }
		public GraphTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_graphTerm; }
	}

	public final GraphTermContext graphTerm() throws RecognitionException {
		GraphTermContext _localctx = new GraphTermContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_graphTerm);
		try {
			setState(1157);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 1);
				{
				setState(1151);
				iri();
				}
				break;
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1152);
				rdfLiteral();
				}
				break;
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1153);
				numericLiteral();
				}
				break;
			case T__27:
			case T__28:
				enterOuterAlt(_localctx, 4);
				{
				setState(1154);
				booleanLiteral();
				}
				break;
			case BLANK_NODE_LABEL:
			case ANON:
				enterOuterAlt(_localctx, 5);
				{
				setState(1155);
				blankNode();
				}
				break;
			case NIL:
				enterOuterAlt(_localctx, 6);
				{
				setState(1156);
				match(NIL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExpressionContext extends ParserRuleContext {
		public ConditionalOrExpressionContext conditionalOrExpression() {
			return getRuleContext(ConditionalOrExpressionContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1159);
			conditionalOrExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConditionalOrExpressionContext extends ParserRuleContext {
		public List<ConditionalAndExpressionContext> conditionalAndExpression() {
			return getRuleContexts(ConditionalAndExpressionContext.class);
		}
		public ConditionalAndExpressionContext conditionalAndExpression(int i) {
			return getRuleContext(ConditionalAndExpressionContext.class,i);
		}
		public ConditionalOrExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conditionalOrExpression; }
	}

	public final ConditionalOrExpressionContext conditionalOrExpression() throws RecognitionException {
		ConditionalOrExpressionContext _localctx = new ConditionalOrExpressionContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_conditionalOrExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1161);
			conditionalAndExpression();
			setState(1166);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__17) {
				{
				{
				setState(1162);
				match(T__17);
				setState(1163);
				conditionalAndExpression();
				}
				}
				setState(1168);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ConditionalAndExpressionContext extends ParserRuleContext {
		public List<ValueLogicalContext> valueLogical() {
			return getRuleContexts(ValueLogicalContext.class);
		}
		public ValueLogicalContext valueLogical(int i) {
			return getRuleContext(ValueLogicalContext.class,i);
		}
		public ConditionalAndExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_conditionalAndExpression; }
	}

	public final ConditionalAndExpressionContext conditionalAndExpression() throws RecognitionException {
		ConditionalAndExpressionContext _localctx = new ConditionalAndExpressionContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_conditionalAndExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1169);
			valueLogical();
			setState(1174);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__18) {
				{
				{
				setState(1170);
				match(T__18);
				setState(1171);
				valueLogical();
				}
				}
				setState(1176);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ValueLogicalContext extends ParserRuleContext {
		public RelationalExpressionContext relationalExpression() {
			return getRuleContext(RelationalExpressionContext.class,0);
		}
		public ValueLogicalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_valueLogical; }
	}

	public final ValueLogicalContext valueLogical() throws RecognitionException {
		ValueLogicalContext _localctx = new ValueLogicalContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_valueLogical);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1177);
			relationalExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RelationalExpressionContext extends ParserRuleContext {
		public Token notToken;
		public List<NumericExpressionContext> numericExpression() {
			return getRuleContexts(NumericExpressionContext.class);
		}
		public NumericExpressionContext numericExpression(int i) {
			return getRuleContext(NumericExpressionContext.class,i);
		}
		public TerminalNode IN() { return getToken(SparqlAutomaticParser.IN, 0); }
		public ExpressionListContext expressionList() {
			return getRuleContext(ExpressionListContext.class,0);
		}
		public TerminalNode NOT() { return getToken(SparqlAutomaticParser.NOT, 0); }
		public RelationalExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationalExpression; }
	}

	public final RelationalExpressionContext relationalExpression() throws RecognitionException {
		RelationalExpressionContext _localctx = new RelationalExpressionContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_relationalExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1179);
			numericExpression();
			setState(1197);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__19:
				{
				setState(1180);
				match(T__19);
				setState(1181);
				numericExpression();
				}
				break;
			case T__20:
				{
				setState(1182);
				match(T__20);
				setState(1183);
				numericExpression();
				}
				break;
			case T__21:
				{
				setState(1184);
				match(T__21);
				setState(1185);
				numericExpression();
				}
				break;
			case T__22:
				{
				setState(1186);
				match(T__22);
				setState(1187);
				numericExpression();
				}
				break;
			case T__23:
				{
				setState(1188);
				match(T__23);
				setState(1189);
				numericExpression();
				}
				break;
			case T__24:
				{
				setState(1190);
				match(T__24);
				setState(1191);
				numericExpression();
				}
				break;
			case IN:
				{
				setState(1192);
				match(IN);
				setState(1193);
				expressionList();
				}
				break;
			case NOT:
				{
				{
				setState(1194);
				((RelationalExpressionContext)_localctx).notToken = match(NOT);
				}
				setState(1195);
				match(IN);
				setState(1196);
				expressionList();
				}
				break;
			case T__2:
			case T__5:
			case T__7:
			case T__17:
			case T__18:
			case AS:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericExpressionContext extends ParserRuleContext {
		public AdditiveExpressionContext additiveExpression() {
			return getRuleContext(AdditiveExpressionContext.class,0);
		}
		public NumericExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericExpression; }
	}

	public final NumericExpressionContext numericExpression() throws RecognitionException {
		NumericExpressionContext _localctx = new NumericExpressionContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_numericExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1199);
			additiveExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AdditiveExpressionContext extends ParserRuleContext {
		public MultiplicativeExpressionContext multiplicativeExpression() {
			return getRuleContext(MultiplicativeExpressionContext.class,0);
		}
		public List<MultiplicativeExpressionWithSignContext> multiplicativeExpressionWithSign() {
			return getRuleContexts(MultiplicativeExpressionWithSignContext.class);
		}
		public MultiplicativeExpressionWithSignContext multiplicativeExpressionWithSign(int i) {
			return getRuleContext(MultiplicativeExpressionWithSignContext.class,i);
		}
		public AdditiveExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_additiveExpression; }
	}

	public final AdditiveExpressionContext additiveExpression() throws RecognitionException {
		AdditiveExpressionContext _localctx = new AdditiveExpressionContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_additiveExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1201);
			multiplicativeExpression();
			setState(1205);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__12 || _la==T__25 || ((((_la - 153)) & ~0x3f) == 0 && ((1L << (_la - 153)) & 63L) != 0)) {
				{
				{
				setState(1202);
				multiplicativeExpressionWithSign();
				}
				}
				setState(1207);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiplicativeExpressionWithSignContext extends ParserRuleContext {
		public PlusSubexpressionContext plusSubexpression() {
			return getRuleContext(PlusSubexpressionContext.class,0);
		}
		public MinusSubexpressionContext minusSubexpression() {
			return getRuleContext(MinusSubexpressionContext.class,0);
		}
		public MultiplicativeExpressionWithLeadingSignButNoSpaceContext multiplicativeExpressionWithLeadingSignButNoSpace() {
			return getRuleContext(MultiplicativeExpressionWithLeadingSignButNoSpaceContext.class,0);
		}
		public MultiplicativeExpressionWithSignContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplicativeExpressionWithSign; }
	}

	public final MultiplicativeExpressionWithSignContext multiplicativeExpressionWithSign() throws RecognitionException {
		MultiplicativeExpressionWithSignContext _localctx = new MultiplicativeExpressionWithSignContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_multiplicativeExpressionWithSign);
		try {
			setState(1213);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__12:
				enterOuterAlt(_localctx, 1);
				{
				setState(1208);
				match(T__12);
				setState(1209);
				plusSubexpression();
				}
				break;
			case T__25:
				enterOuterAlt(_localctx, 2);
				{
				setState(1210);
				match(T__25);
				setState(1211);
				minusSubexpression();
				}
				break;
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1212);
				multiplicativeExpressionWithLeadingSignButNoSpace();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PlusSubexpressionContext extends ParserRuleContext {
		public MultiplicativeExpressionContext multiplicativeExpression() {
			return getRuleContext(MultiplicativeExpressionContext.class,0);
		}
		public PlusSubexpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_plusSubexpression; }
	}

	public final PlusSubexpressionContext plusSubexpression() throws RecognitionException {
		PlusSubexpressionContext _localctx = new PlusSubexpressionContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_plusSubexpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1215);
			multiplicativeExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MinusSubexpressionContext extends ParserRuleContext {
		public MultiplicativeExpressionContext multiplicativeExpression() {
			return getRuleContext(MultiplicativeExpressionContext.class,0);
		}
		public MinusSubexpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_minusSubexpression; }
	}

	public final MinusSubexpressionContext minusSubexpression() throws RecognitionException {
		MinusSubexpressionContext _localctx = new MinusSubexpressionContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_minusSubexpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1217);
			multiplicativeExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiplicativeExpressionWithLeadingSignButNoSpaceContext extends ParserRuleContext {
		public NumericLiteralPositiveContext numericLiteralPositive() {
			return getRuleContext(NumericLiteralPositiveContext.class,0);
		}
		public NumericLiteralNegativeContext numericLiteralNegative() {
			return getRuleContext(NumericLiteralNegativeContext.class,0);
		}
		public List<MultiplyOrDivideExpressionContext> multiplyOrDivideExpression() {
			return getRuleContexts(MultiplyOrDivideExpressionContext.class);
		}
		public MultiplyOrDivideExpressionContext multiplyOrDivideExpression(int i) {
			return getRuleContext(MultiplyOrDivideExpressionContext.class,i);
		}
		public MultiplicativeExpressionWithLeadingSignButNoSpaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplicativeExpressionWithLeadingSignButNoSpace; }
	}

	public final MultiplicativeExpressionWithLeadingSignButNoSpaceContext multiplicativeExpressionWithLeadingSignButNoSpace() throws RecognitionException {
		MultiplicativeExpressionWithLeadingSignButNoSpaceContext _localctx = new MultiplicativeExpressionWithLeadingSignButNoSpaceContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_multiplicativeExpressionWithLeadingSignButNoSpace);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1221);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
				{
				setState(1219);
				numericLiteralPositive();
				}
				break;
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
				{
				setState(1220);
				numericLiteralNegative();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1226);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0 || _la==T__10) {
				{
				{
				setState(1223);
				multiplyOrDivideExpression();
				}
				}
				setState(1228);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiplicativeExpressionContext extends ParserRuleContext {
		public UnaryExpressionContext unaryExpression() {
			return getRuleContext(UnaryExpressionContext.class,0);
		}
		public List<MultiplyOrDivideExpressionContext> multiplyOrDivideExpression() {
			return getRuleContexts(MultiplyOrDivideExpressionContext.class);
		}
		public MultiplyOrDivideExpressionContext multiplyOrDivideExpression(int i) {
			return getRuleContext(MultiplyOrDivideExpressionContext.class,i);
		}
		public MultiplicativeExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplicativeExpression; }
	}

	public final MultiplicativeExpressionContext multiplicativeExpression() throws RecognitionException {
		MultiplicativeExpressionContext _localctx = new MultiplicativeExpressionContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_multiplicativeExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1229);
			unaryExpression();
			setState(1233);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0 || _la==T__10) {
				{
				{
				setState(1230);
				multiplyOrDivideExpression();
				}
				}
				setState(1235);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiplyOrDivideExpressionContext extends ParserRuleContext {
		public MultiplyExpressionContext multiplyExpression() {
			return getRuleContext(MultiplyExpressionContext.class,0);
		}
		public DivideExpressionContext divideExpression() {
			return getRuleContext(DivideExpressionContext.class,0);
		}
		public MultiplyOrDivideExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplyOrDivideExpression; }
	}

	public final MultiplyOrDivideExpressionContext multiplyOrDivideExpression() throws RecognitionException {
		MultiplyOrDivideExpressionContext _localctx = new MultiplyOrDivideExpressionContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_multiplyOrDivideExpression);
		try {
			setState(1238);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
				enterOuterAlt(_localctx, 1);
				{
				setState(1236);
				multiplyExpression();
				}
				break;
			case T__10:
				enterOuterAlt(_localctx, 2);
				{
				setState(1237);
				divideExpression();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class MultiplyExpressionContext extends ParserRuleContext {
		public UnaryExpressionContext unaryExpression() {
			return getRuleContext(UnaryExpressionContext.class,0);
		}
		public MultiplyExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplyExpression; }
	}

	public final MultiplyExpressionContext multiplyExpression() throws RecognitionException {
		MultiplyExpressionContext _localctx = new MultiplyExpressionContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_multiplyExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1240);
			match(T__0);
			setState(1241);
			unaryExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DivideExpressionContext extends ParserRuleContext {
		public UnaryExpressionContext unaryExpression() {
			return getRuleContext(UnaryExpressionContext.class,0);
		}
		public DivideExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_divideExpression; }
	}

	public final DivideExpressionContext divideExpression() throws RecognitionException {
		DivideExpressionContext _localctx = new DivideExpressionContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_divideExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1243);
			match(T__10);
			setState(1244);
			unaryExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class UnaryExpressionContext extends ParserRuleContext {
		public PrimaryExpressionContext primaryExpression() {
			return getRuleContext(PrimaryExpressionContext.class,0);
		}
		public UnaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unaryExpression; }
	}

	public final UnaryExpressionContext unaryExpression() throws RecognitionException {
		UnaryExpressionContext _localctx = new UnaryExpressionContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_unaryExpression);
		try {
			setState(1253);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__14:
				enterOuterAlt(_localctx, 1);
				{
				setState(1246);
				match(T__14);
				setState(1247);
				primaryExpression();
				}
				break;
			case T__12:
				enterOuterAlt(_localctx, 2);
				{
				setState(1248);
				match(T__12);
				setState(1249);
				primaryExpression();
				}
				break;
			case T__25:
				enterOuterAlt(_localctx, 3);
				{
				setState(1250);
				match(T__25);
				setState(1251);
				primaryExpression();
				}
				break;
			case T__1:
			case T__27:
			case T__28:
			case GROUP_CONCAT:
			case NOT:
			case STR:
			case LANG:
			case LANGMATCHES:
			case DATATYPE:
			case BOUND:
			case IRI:
			case URI:
			case BNODE:
			case RAND:
			case ABS:
			case CEIL:
			case FLOOR:
			case ROUND:
			case CONCAT:
			case STRLEN:
			case UCASE:
			case LCASE:
			case ENCODE_FOR_URI:
			case CONTAINS:
			case STRSTARTS:
			case STRENDS:
			case STRBEFORE:
			case STRAFTER:
			case YEAR:
			case MONTH:
			case DAY:
			case HOURS:
			case MINUTES:
			case SECONDS:
			case TIMEZONE:
			case TZ:
			case NOW:
			case UUID:
			case STRUUID:
			case SHA1:
			case SHA256:
			case SHA384:
			case SHA512:
			case MD5:
			case COALESCE:
			case IF:
			case STRLANG:
			case STRDT:
			case SAMETERM:
			case ISIRI:
			case ISURI:
			case ISBLANK:
			case ISLITERAL:
			case ISNUMERIC:
			case REGEX:
			case SUBSTR:
			case REPLACE:
			case EXISTS:
			case COUNT:
			case SUM:
			case MIN:
			case MAX:
			case AVG:
			case STDEV:
			case SAMPLE:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
				enterOuterAlt(_localctx, 4);
				{
				setState(1252);
				primaryExpression();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrimaryExpressionContext extends ParserRuleContext {
		public BrackettedExpressionContext brackettedExpression() {
			return getRuleContext(BrackettedExpressionContext.class,0);
		}
		public BuiltInCallContext builtInCall() {
			return getRuleContext(BuiltInCallContext.class,0);
		}
		public IriOrFunctionContext iriOrFunction() {
			return getRuleContext(IriOrFunctionContext.class,0);
		}
		public RdfLiteralContext rdfLiteral() {
			return getRuleContext(RdfLiteralContext.class,0);
		}
		public NumericLiteralContext numericLiteral() {
			return getRuleContext(NumericLiteralContext.class,0);
		}
		public BooleanLiteralContext booleanLiteral() {
			return getRuleContext(BooleanLiteralContext.class,0);
		}
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primaryExpression; }
	}

	public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
		PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, getState());
		enterRule(_localctx, 274, RULE_primaryExpression);
		try {
			setState(1262);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1255);
				brackettedExpression();
				}
				break;
			case GROUP_CONCAT:
			case NOT:
			case STR:
			case LANG:
			case LANGMATCHES:
			case DATATYPE:
			case BOUND:
			case IRI:
			case URI:
			case BNODE:
			case RAND:
			case ABS:
			case CEIL:
			case FLOOR:
			case ROUND:
			case CONCAT:
			case STRLEN:
			case UCASE:
			case LCASE:
			case ENCODE_FOR_URI:
			case CONTAINS:
			case STRSTARTS:
			case STRENDS:
			case STRBEFORE:
			case STRAFTER:
			case YEAR:
			case MONTH:
			case DAY:
			case HOURS:
			case MINUTES:
			case SECONDS:
			case TIMEZONE:
			case TZ:
			case NOW:
			case UUID:
			case STRUUID:
			case SHA1:
			case SHA256:
			case SHA384:
			case SHA512:
			case MD5:
			case COALESCE:
			case IF:
			case STRLANG:
			case STRDT:
			case SAMETERM:
			case ISIRI:
			case ISURI:
			case ISBLANK:
			case ISLITERAL:
			case ISNUMERIC:
			case REGEX:
			case SUBSTR:
			case REPLACE:
			case EXISTS:
			case COUNT:
			case SUM:
			case MIN:
			case MAX:
			case AVG:
			case STDEV:
			case SAMPLE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1256);
				builtInCall();
				}
				break;
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case PREFIX_LANGTAG:
				enterOuterAlt(_localctx, 3);
				{
				setState(1257);
				iriOrFunction();
				}
				break;
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
				enterOuterAlt(_localctx, 4);
				{
				setState(1258);
				rdfLiteral();
				}
				break;
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
				enterOuterAlt(_localctx, 5);
				{
				setState(1259);
				numericLiteral();
				}
				break;
			case T__27:
			case T__28:
				enterOuterAlt(_localctx, 6);
				{
				setState(1260);
				booleanLiteral();
				}
				break;
			case VAR1:
			case VAR2:
				enterOuterAlt(_localctx, 7);
				{
				setState(1261);
				var();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BrackettedExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public BrackettedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_brackettedExpression; }
	}

	public final BrackettedExpressionContext brackettedExpression() throws RecognitionException {
		BrackettedExpressionContext _localctx = new BrackettedExpressionContext(_ctx, getState());
		enterRule(_localctx, 276, RULE_brackettedExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1264);
			match(T__1);
			setState(1265);
			expression();
			setState(1266);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BuiltInCallContext extends ParserRuleContext {
		public AggregateContext aggregate() {
			return getRuleContext(AggregateContext.class,0);
		}
		public TerminalNode STR() { return getToken(SparqlAutomaticParser.STR, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public LangExpressionContext langExpression() {
			return getRuleContext(LangExpressionContext.class,0);
		}
		public TerminalNode LANGMATCHES() { return getToken(SparqlAutomaticParser.LANGMATCHES, 0); }
		public TerminalNode DATATYPE() { return getToken(SparqlAutomaticParser.DATATYPE, 0); }
		public TerminalNode BOUND() { return getToken(SparqlAutomaticParser.BOUND, 0); }
		public VarContext var() {
			return getRuleContext(VarContext.class,0);
		}
		public TerminalNode IRI() { return getToken(SparqlAutomaticParser.IRI, 0); }
		public TerminalNode URI() { return getToken(SparqlAutomaticParser.URI, 0); }
		public TerminalNode BNODE() { return getToken(SparqlAutomaticParser.BNODE, 0); }
		public TerminalNode NIL() { return getToken(SparqlAutomaticParser.NIL, 0); }
		public TerminalNode RAND() { return getToken(SparqlAutomaticParser.RAND, 0); }
		public TerminalNode ABS() { return getToken(SparqlAutomaticParser.ABS, 0); }
		public TerminalNode CEIL() { return getToken(SparqlAutomaticParser.CEIL, 0); }
		public TerminalNode FLOOR() { return getToken(SparqlAutomaticParser.FLOOR, 0); }
		public TerminalNode ROUND() { return getToken(SparqlAutomaticParser.ROUND, 0); }
		public TerminalNode CONCAT() { return getToken(SparqlAutomaticParser.CONCAT, 0); }
		public ExpressionListContext expressionList() {
			return getRuleContext(ExpressionListContext.class,0);
		}
		public SubstringExpressionContext substringExpression() {
			return getRuleContext(SubstringExpressionContext.class,0);
		}
		public TerminalNode STRLEN() { return getToken(SparqlAutomaticParser.STRLEN, 0); }
		public StrReplaceExpressionContext strReplaceExpression() {
			return getRuleContext(StrReplaceExpressionContext.class,0);
		}
		public TerminalNode UCASE() { return getToken(SparqlAutomaticParser.UCASE, 0); }
		public TerminalNode LCASE() { return getToken(SparqlAutomaticParser.LCASE, 0); }
		public TerminalNode ENCODE_FOR_URI() { return getToken(SparqlAutomaticParser.ENCODE_FOR_URI, 0); }
		public TerminalNode CONTAINS() { return getToken(SparqlAutomaticParser.CONTAINS, 0); }
		public TerminalNode STRSTARTS() { return getToken(SparqlAutomaticParser.STRSTARTS, 0); }
		public TerminalNode STRENDS() { return getToken(SparqlAutomaticParser.STRENDS, 0); }
		public TerminalNode STRBEFORE() { return getToken(SparqlAutomaticParser.STRBEFORE, 0); }
		public TerminalNode STRAFTER() { return getToken(SparqlAutomaticParser.STRAFTER, 0); }
		public TerminalNode YEAR() { return getToken(SparqlAutomaticParser.YEAR, 0); }
		public TerminalNode MONTH() { return getToken(SparqlAutomaticParser.MONTH, 0); }
		public TerminalNode DAY() { return getToken(SparqlAutomaticParser.DAY, 0); }
		public TerminalNode HOURS() { return getToken(SparqlAutomaticParser.HOURS, 0); }
		public TerminalNode MINUTES() { return getToken(SparqlAutomaticParser.MINUTES, 0); }
		public TerminalNode SECONDS() { return getToken(SparqlAutomaticParser.SECONDS, 0); }
		public TerminalNode TIMEZONE() { return getToken(SparqlAutomaticParser.TIMEZONE, 0); }
		public TerminalNode TZ() { return getToken(SparqlAutomaticParser.TZ, 0); }
		public TerminalNode NOW() { return getToken(SparqlAutomaticParser.NOW, 0); }
		public TerminalNode UUID() { return getToken(SparqlAutomaticParser.UUID, 0); }
		public TerminalNode STRUUID() { return getToken(SparqlAutomaticParser.STRUUID, 0); }
		public TerminalNode MD5() { return getToken(SparqlAutomaticParser.MD5, 0); }
		public TerminalNode SHA1() { return getToken(SparqlAutomaticParser.SHA1, 0); }
		public TerminalNode SHA256() { return getToken(SparqlAutomaticParser.SHA256, 0); }
		public TerminalNode SHA384() { return getToken(SparqlAutomaticParser.SHA384, 0); }
		public TerminalNode SHA512() { return getToken(SparqlAutomaticParser.SHA512, 0); }
		public TerminalNode COALESCE() { return getToken(SparqlAutomaticParser.COALESCE, 0); }
		public TerminalNode IF() { return getToken(SparqlAutomaticParser.IF, 0); }
		public TerminalNode STRLANG() { return getToken(SparqlAutomaticParser.STRLANG, 0); }
		public TerminalNode STRDT() { return getToken(SparqlAutomaticParser.STRDT, 0); }
		public TerminalNode SAMETERM() { return getToken(SparqlAutomaticParser.SAMETERM, 0); }
		public TerminalNode ISIRI() { return getToken(SparqlAutomaticParser.ISIRI, 0); }
		public TerminalNode ISURI() { return getToken(SparqlAutomaticParser.ISURI, 0); }
		public TerminalNode ISBLANK() { return getToken(SparqlAutomaticParser.ISBLANK, 0); }
		public TerminalNode ISLITERAL() { return getToken(SparqlAutomaticParser.ISLITERAL, 0); }
		public TerminalNode ISNUMERIC() { return getToken(SparqlAutomaticParser.ISNUMERIC, 0); }
		public RegexExpressionContext regexExpression() {
			return getRuleContext(RegexExpressionContext.class,0);
		}
		public ExistsFuncContext existsFunc() {
			return getRuleContext(ExistsFuncContext.class,0);
		}
		public NotExistsFuncContext notExistsFunc() {
			return getRuleContext(NotExistsFuncContext.class,0);
		}
		public BuiltInCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_builtInCall; }
	}

	public final BuiltInCallContext builtInCall() throws RecognitionException {
		BuiltInCallContext _localctx = new BuiltInCallContext(_ctx, getState());
		enterRule(_localctx, 278, RULE_builtInCall);
		try {
			setState(1522);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case GROUP_CONCAT:
			case COUNT:
			case SUM:
			case MIN:
			case MAX:
			case AVG:
			case STDEV:
			case SAMPLE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1268);
				aggregate();
				}
				break;
			case STR:
				enterOuterAlt(_localctx, 2);
				{
				setState(1269);
				match(STR);
				setState(1270);
				match(T__1);
				setState(1271);
				expression();
				setState(1272);
				match(T__2);
				}
				break;
			case LANG:
				enterOuterAlt(_localctx, 3);
				{
				setState(1274);
				langExpression();
				}
				break;
			case LANGMATCHES:
				enterOuterAlt(_localctx, 4);
				{
				setState(1275);
				match(LANGMATCHES);
				setState(1276);
				match(T__1);
				setState(1277);
				expression();
				setState(1278);
				match(T__7);
				setState(1279);
				expression();
				setState(1280);
				match(T__2);
				}
				break;
			case DATATYPE:
				enterOuterAlt(_localctx, 5);
				{
				setState(1282);
				match(DATATYPE);
				setState(1283);
				match(T__1);
				setState(1284);
				expression();
				setState(1285);
				match(T__2);
				}
				break;
			case BOUND:
				enterOuterAlt(_localctx, 6);
				{
				setState(1287);
				match(BOUND);
				setState(1288);
				match(T__1);
				setState(1289);
				var();
				setState(1290);
				match(T__2);
				}
				break;
			case IRI:
				enterOuterAlt(_localctx, 7);
				{
				setState(1292);
				match(IRI);
				setState(1293);
				match(T__1);
				setState(1294);
				expression();
				setState(1295);
				match(T__2);
				}
				break;
			case URI:
				enterOuterAlt(_localctx, 8);
				{
				setState(1297);
				match(URI);
				setState(1298);
				match(T__1);
				setState(1299);
				expression();
				setState(1300);
				match(T__2);
				}
				break;
			case BNODE:
				enterOuterAlt(_localctx, 9);
				{
				setState(1302);
				match(BNODE);
				setState(1308);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case T__1:
					{
					setState(1303);
					match(T__1);
					setState(1304);
					expression();
					setState(1305);
					match(T__2);
					}
					break;
				case NIL:
					{
					setState(1307);
					match(NIL);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case RAND:
				enterOuterAlt(_localctx, 10);
				{
				setState(1310);
				match(RAND);
				setState(1311);
				match(NIL);
				}
				break;
			case ABS:
				enterOuterAlt(_localctx, 11);
				{
				setState(1312);
				match(ABS);
				setState(1313);
				match(T__1);
				setState(1314);
				expression();
				setState(1315);
				match(T__2);
				}
				break;
			case CEIL:
				enterOuterAlt(_localctx, 12);
				{
				setState(1317);
				match(CEIL);
				setState(1318);
				match(T__1);
				setState(1319);
				expression();
				setState(1320);
				match(T__2);
				}
				break;
			case FLOOR:
				enterOuterAlt(_localctx, 13);
				{
				setState(1322);
				match(FLOOR);
				setState(1323);
				match(T__1);
				setState(1324);
				expression();
				setState(1325);
				match(T__2);
				}
				break;
			case ROUND:
				enterOuterAlt(_localctx, 14);
				{
				setState(1327);
				match(ROUND);
				setState(1328);
				match(T__1);
				setState(1329);
				expression();
				setState(1330);
				match(T__2);
				}
				break;
			case CONCAT:
				enterOuterAlt(_localctx, 15);
				{
				setState(1332);
				match(CONCAT);
				setState(1333);
				expressionList();
				}
				break;
			case SUBSTR:
				enterOuterAlt(_localctx, 16);
				{
				setState(1334);
				substringExpression();
				}
				break;
			case STRLEN:
				enterOuterAlt(_localctx, 17);
				{
				setState(1335);
				match(STRLEN);
				setState(1336);
				match(T__1);
				setState(1337);
				expression();
				setState(1338);
				match(T__2);
				}
				break;
			case REPLACE:
				enterOuterAlt(_localctx, 18);
				{
				setState(1340);
				strReplaceExpression();
				}
				break;
			case UCASE:
				enterOuterAlt(_localctx, 19);
				{
				setState(1341);
				match(UCASE);
				setState(1342);
				match(T__1);
				setState(1343);
				expression();
				setState(1344);
				match(T__2);
				}
				break;
			case LCASE:
				enterOuterAlt(_localctx, 20);
				{
				setState(1346);
				match(LCASE);
				setState(1347);
				match(T__1);
				setState(1348);
				expression();
				setState(1349);
				match(T__2);
				}
				break;
			case ENCODE_FOR_URI:
				enterOuterAlt(_localctx, 21);
				{
				setState(1351);
				match(ENCODE_FOR_URI);
				setState(1352);
				match(T__1);
				setState(1353);
				expression();
				setState(1354);
				match(T__2);
				}
				break;
			case CONTAINS:
				enterOuterAlt(_localctx, 22);
				{
				setState(1356);
				match(CONTAINS);
				setState(1357);
				match(T__1);
				setState(1358);
				expression();
				setState(1359);
				match(T__7);
				setState(1360);
				expression();
				setState(1361);
				match(T__2);
				}
				break;
			case STRSTARTS:
				enterOuterAlt(_localctx, 23);
				{
				setState(1363);
				match(STRSTARTS);
				setState(1364);
				match(T__1);
				setState(1365);
				expression();
				setState(1366);
				match(T__7);
				setState(1367);
				expression();
				setState(1368);
				match(T__2);
				}
				break;
			case STRENDS:
				enterOuterAlt(_localctx, 24);
				{
				setState(1370);
				match(STRENDS);
				setState(1371);
				match(T__1);
				setState(1372);
				expression();
				setState(1373);
				match(T__7);
				setState(1374);
				expression();
				setState(1375);
				match(T__2);
				}
				break;
			case STRBEFORE:
				enterOuterAlt(_localctx, 25);
				{
				setState(1377);
				match(STRBEFORE);
				setState(1378);
				match(T__1);
				setState(1379);
				expression();
				setState(1380);
				match(T__7);
				setState(1381);
				expression();
				setState(1382);
				match(T__2);
				}
				break;
			case STRAFTER:
				enterOuterAlt(_localctx, 26);
				{
				setState(1384);
				match(STRAFTER);
				setState(1385);
				match(T__1);
				setState(1386);
				expression();
				setState(1387);
				match(T__7);
				setState(1388);
				expression();
				setState(1389);
				match(T__2);
				}
				break;
			case YEAR:
				enterOuterAlt(_localctx, 27);
				{
				setState(1391);
				match(YEAR);
				setState(1392);
				match(T__1);
				setState(1393);
				expression();
				setState(1394);
				match(T__2);
				}
				break;
			case MONTH:
				enterOuterAlt(_localctx, 28);
				{
				setState(1396);
				match(MONTH);
				setState(1397);
				match(T__1);
				setState(1398);
				expression();
				setState(1399);
				match(T__2);
				}
				break;
			case DAY:
				enterOuterAlt(_localctx, 29);
				{
				setState(1401);
				match(DAY);
				setState(1402);
				match(T__1);
				setState(1403);
				expression();
				setState(1404);
				match(T__2);
				}
				break;
			case HOURS:
				enterOuterAlt(_localctx, 30);
				{
				setState(1406);
				match(HOURS);
				setState(1407);
				match(T__1);
				setState(1408);
				expression();
				setState(1409);
				match(T__2);
				}
				break;
			case MINUTES:
				enterOuterAlt(_localctx, 31);
				{
				setState(1411);
				match(MINUTES);
				setState(1412);
				match(T__1);
				setState(1413);
				expression();
				setState(1414);
				match(T__2);
				}
				break;
			case SECONDS:
				enterOuterAlt(_localctx, 32);
				{
				setState(1416);
				match(SECONDS);
				setState(1417);
				match(T__1);
				setState(1418);
				expression();
				setState(1419);
				match(T__2);
				}
				break;
			case TIMEZONE:
				enterOuterAlt(_localctx, 33);
				{
				setState(1421);
				match(TIMEZONE);
				setState(1422);
				match(T__1);
				setState(1423);
				expression();
				setState(1424);
				match(T__2);
				}
				break;
			case TZ:
				enterOuterAlt(_localctx, 34);
				{
				setState(1426);
				match(TZ);
				setState(1427);
				match(T__1);
				setState(1428);
				expression();
				setState(1429);
				match(T__2);
				}
				break;
			case NOW:
				enterOuterAlt(_localctx, 35);
				{
				setState(1431);
				match(NOW);
				setState(1432);
				match(NIL);
				}
				break;
			case UUID:
				enterOuterAlt(_localctx, 36);
				{
				setState(1433);
				match(UUID);
				setState(1434);
				match(NIL);
				}
				break;
			case STRUUID:
				enterOuterAlt(_localctx, 37);
				{
				setState(1435);
				match(STRUUID);
				setState(1436);
				match(NIL);
				}
				break;
			case MD5:
				enterOuterAlt(_localctx, 38);
				{
				setState(1437);
				match(MD5);
				setState(1438);
				match(T__1);
				setState(1439);
				expression();
				setState(1440);
				match(T__2);
				}
				break;
			case SHA1:
				enterOuterAlt(_localctx, 39);
				{
				setState(1442);
				match(SHA1);
				setState(1443);
				match(T__1);
				setState(1444);
				expression();
				setState(1445);
				match(T__2);
				}
				break;
			case SHA256:
				enterOuterAlt(_localctx, 40);
				{
				setState(1447);
				match(SHA256);
				setState(1448);
				match(T__1);
				setState(1449);
				expression();
				setState(1450);
				match(T__2);
				}
				break;
			case SHA384:
				enterOuterAlt(_localctx, 41);
				{
				setState(1452);
				match(SHA384);
				setState(1453);
				match(T__1);
				setState(1454);
				expression();
				setState(1455);
				match(T__2);
				}
				break;
			case SHA512:
				enterOuterAlt(_localctx, 42);
				{
				setState(1457);
				match(SHA512);
				setState(1458);
				match(T__1);
				setState(1459);
				expression();
				setState(1460);
				match(T__2);
				}
				break;
			case COALESCE:
				enterOuterAlt(_localctx, 43);
				{
				setState(1462);
				match(COALESCE);
				setState(1463);
				expressionList();
				}
				break;
			case IF:
				enterOuterAlt(_localctx, 44);
				{
				setState(1464);
				match(IF);
				setState(1465);
				match(T__1);
				setState(1466);
				expression();
				setState(1467);
				match(T__7);
				setState(1468);
				expression();
				setState(1469);
				match(T__7);
				setState(1470);
				expression();
				setState(1471);
				match(T__2);
				}
				break;
			case STRLANG:
				enterOuterAlt(_localctx, 45);
				{
				setState(1473);
				match(STRLANG);
				setState(1474);
				match(T__1);
				setState(1475);
				expression();
				setState(1476);
				match(T__7);
				setState(1477);
				expression();
				setState(1478);
				match(T__2);
				}
				break;
			case STRDT:
				enterOuterAlt(_localctx, 46);
				{
				setState(1480);
				match(STRDT);
				setState(1481);
				match(T__1);
				setState(1482);
				expression();
				setState(1483);
				match(T__7);
				setState(1484);
				expression();
				setState(1485);
				match(T__2);
				}
				break;
			case SAMETERM:
				enterOuterAlt(_localctx, 47);
				{
				setState(1487);
				match(SAMETERM);
				setState(1488);
				match(T__1);
				setState(1489);
				expression();
				setState(1490);
				match(T__7);
				setState(1491);
				expression();
				setState(1492);
				match(T__2);
				}
				break;
			case ISIRI:
				enterOuterAlt(_localctx, 48);
				{
				setState(1494);
				match(ISIRI);
				setState(1495);
				match(T__1);
				setState(1496);
				expression();
				setState(1497);
				match(T__2);
				}
				break;
			case ISURI:
				enterOuterAlt(_localctx, 49);
				{
				setState(1499);
				match(ISURI);
				setState(1500);
				match(T__1);
				setState(1501);
				expression();
				setState(1502);
				match(T__2);
				}
				break;
			case ISBLANK:
				enterOuterAlt(_localctx, 50);
				{
				setState(1504);
				match(ISBLANK);
				setState(1505);
				match(T__1);
				setState(1506);
				expression();
				setState(1507);
				match(T__2);
				}
				break;
			case ISLITERAL:
				enterOuterAlt(_localctx, 51);
				{
				setState(1509);
				match(ISLITERAL);
				setState(1510);
				match(T__1);
				setState(1511);
				expression();
				setState(1512);
				match(T__2);
				}
				break;
			case ISNUMERIC:
				enterOuterAlt(_localctx, 52);
				{
				setState(1514);
				match(ISNUMERIC);
				setState(1515);
				match(T__1);
				setState(1516);
				expression();
				setState(1517);
				match(T__2);
				}
				break;
			case REGEX:
				enterOuterAlt(_localctx, 53);
				{
				setState(1519);
				regexExpression();
				}
				break;
			case EXISTS:
				enterOuterAlt(_localctx, 54);
				{
				setState(1520);
				existsFunc();
				}
				break;
			case NOT:
				enterOuterAlt(_localctx, 55);
				{
				setState(1521);
				notExistsFunc();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RegexExpressionContext extends ParserRuleContext {
		public TerminalNode REGEX() { return getToken(SparqlAutomaticParser.REGEX, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public RegexExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_regexExpression; }
	}

	public final RegexExpressionContext regexExpression() throws RecognitionException {
		RegexExpressionContext _localctx = new RegexExpressionContext(_ctx, getState());
		enterRule(_localctx, 280, RULE_regexExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1524);
			match(REGEX);
			setState(1525);
			match(T__1);
			setState(1526);
			expression();
			setState(1527);
			match(T__7);
			setState(1528);
			expression();
			setState(1531);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__7) {
				{
				setState(1529);
				match(T__7);
				setState(1530);
				expression();
				}
			}

			setState(1533);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class LangExpressionContext extends ParserRuleContext {
		public TerminalNode LANG() { return getToken(SparqlAutomaticParser.LANG, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public LangExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_langExpression; }
	}

	public final LangExpressionContext langExpression() throws RecognitionException {
		LangExpressionContext _localctx = new LangExpressionContext(_ctx, getState());
		enterRule(_localctx, 282, RULE_langExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1535);
			match(LANG);
			setState(1536);
			match(T__1);
			setState(1537);
			expression();
			setState(1538);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class SubstringExpressionContext extends ParserRuleContext {
		public TerminalNode SUBSTR() { return getToken(SparqlAutomaticParser.SUBSTR, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public SubstringExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_substringExpression; }
	}

	public final SubstringExpressionContext substringExpression() throws RecognitionException {
		SubstringExpressionContext _localctx = new SubstringExpressionContext(_ctx, getState());
		enterRule(_localctx, 284, RULE_substringExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1540);
			match(SUBSTR);
			setState(1541);
			match(T__1);
			setState(1542);
			expression();
			setState(1543);
			match(T__7);
			setState(1544);
			expression();
			setState(1547);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__7) {
				{
				setState(1545);
				match(T__7);
				setState(1546);
				expression();
				}
			}

			setState(1549);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StrReplaceExpressionContext extends ParserRuleContext {
		public TerminalNode REPLACE() { return getToken(SparqlAutomaticParser.REPLACE, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public StrReplaceExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strReplaceExpression; }
	}

	public final StrReplaceExpressionContext strReplaceExpression() throws RecognitionException {
		StrReplaceExpressionContext _localctx = new StrReplaceExpressionContext(_ctx, getState());
		enterRule(_localctx, 286, RULE_strReplaceExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1551);
			match(REPLACE);
			setState(1552);
			match(T__1);
			setState(1553);
			expression();
			setState(1554);
			match(T__7);
			setState(1555);
			expression();
			setState(1556);
			match(T__7);
			setState(1557);
			expression();
			setState(1560);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__7) {
				{
				setState(1558);
				match(T__7);
				setState(1559);
				expression();
				}
			}

			setState(1562);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExistsFuncContext extends ParserRuleContext {
		public TerminalNode EXISTS() { return getToken(SparqlAutomaticParser.EXISTS, 0); }
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public ExistsFuncContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_existsFunc; }
	}

	public final ExistsFuncContext existsFunc() throws RecognitionException {
		ExistsFuncContext _localctx = new ExistsFuncContext(_ctx, getState());
		enterRule(_localctx, 288, RULE_existsFunc);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1564);
			match(EXISTS);
			setState(1565);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NotExistsFuncContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(SparqlAutomaticParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(SparqlAutomaticParser.EXISTS, 0); }
		public GroupGraphPatternContext groupGraphPattern() {
			return getRuleContext(GroupGraphPatternContext.class,0);
		}
		public NotExistsFuncContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_notExistsFunc; }
	}

	public final NotExistsFuncContext notExistsFunc() throws RecognitionException {
		NotExistsFuncContext _localctx = new NotExistsFuncContext(_ctx, getState());
		enterRule(_localctx, 290, RULE_notExistsFunc);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1567);
			match(NOT);
			setState(1568);
			match(EXISTS);
			setState(1569);
			groupGraphPattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AggregateContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(SparqlAutomaticParser.COUNT, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode DISTINCT() { return getToken(SparqlAutomaticParser.DISTINCT, 0); }
		public TerminalNode SUM() { return getToken(SparqlAutomaticParser.SUM, 0); }
		public TerminalNode MIN() { return getToken(SparqlAutomaticParser.MIN, 0); }
		public TerminalNode MAX() { return getToken(SparqlAutomaticParser.MAX, 0); }
		public TerminalNode AVG() { return getToken(SparqlAutomaticParser.AVG, 0); }
		public TerminalNode STDEV() { return getToken(SparqlAutomaticParser.STDEV, 0); }
		public TerminalNode SAMPLE() { return getToken(SparqlAutomaticParser.SAMPLE, 0); }
		public TerminalNode GROUP_CONCAT() { return getToken(SparqlAutomaticParser.GROUP_CONCAT, 0); }
		public TerminalNode SEPARATOR() { return getToken(SparqlAutomaticParser.SEPARATOR, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public AggregateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aggregate; }
	}

	public final AggregateContext aggregate() throws RecognitionException {
		AggregateContext _localctx = new AggregateContext(_ctx, getState());
		enterRule(_localctx, 292, RULE_aggregate);
		int _la;
		try {
			setState(1643);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COUNT:
				enterOuterAlt(_localctx, 1);
				{
				setState(1571);
				match(COUNT);
				setState(1572);
				match(T__1);
				setState(1574);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1573);
					match(DISTINCT);
					}
				}

				setState(1578);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case T__0:
					{
					setState(1576);
					match(T__0);
					}
					break;
				case T__1:
				case T__12:
				case T__14:
				case T__25:
				case T__27:
				case T__28:
				case GROUP_CONCAT:
				case NOT:
				case STR:
				case LANG:
				case LANGMATCHES:
				case DATATYPE:
				case BOUND:
				case IRI:
				case URI:
				case BNODE:
				case RAND:
				case ABS:
				case CEIL:
				case FLOOR:
				case ROUND:
				case CONCAT:
				case STRLEN:
				case UCASE:
				case LCASE:
				case ENCODE_FOR_URI:
				case CONTAINS:
				case STRSTARTS:
				case STRENDS:
				case STRBEFORE:
				case STRAFTER:
				case YEAR:
				case MONTH:
				case DAY:
				case HOURS:
				case MINUTES:
				case SECONDS:
				case TIMEZONE:
				case TZ:
				case NOW:
				case UUID:
				case STRUUID:
				case SHA1:
				case SHA256:
				case SHA384:
				case SHA512:
				case MD5:
				case COALESCE:
				case IF:
				case STRLANG:
				case STRDT:
				case SAMETERM:
				case ISIRI:
				case ISURI:
				case ISBLANK:
				case ISLITERAL:
				case ISNUMERIC:
				case REGEX:
				case SUBSTR:
				case REPLACE:
				case EXISTS:
				case COUNT:
				case SUM:
				case MIN:
				case MAX:
				case AVG:
				case STDEV:
				case SAMPLE:
				case IRI_REF:
				case PNAME_NS:
				case PNAME_LN:
				case VAR1:
				case VAR2:
				case PREFIX_LANGTAG:
				case INTEGER:
				case DECIMAL:
				case DOUBLE:
				case INTEGER_POSITIVE:
				case DECIMAL_POSITIVE:
				case DOUBLE_POSITIVE:
				case INTEGER_NEGATIVE:
				case DECIMAL_NEGATIVE:
				case DOUBLE_NEGATIVE:
				case STRING_LITERAL1:
				case STRING_LITERAL2:
				case STRING_LITERAL_LONG1:
				case STRING_LITERAL_LONG2:
					{
					setState(1577);
					expression();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1580);
				match(T__2);
				}
				break;
			case SUM:
				enterOuterAlt(_localctx, 2);
				{
				setState(1581);
				match(SUM);
				setState(1582);
				match(T__1);
				setState(1584);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1583);
					match(DISTINCT);
					}
				}

				setState(1586);
				expression();
				setState(1587);
				match(T__2);
				}
				break;
			case MIN:
				enterOuterAlt(_localctx, 3);
				{
				setState(1589);
				match(MIN);
				setState(1590);
				match(T__1);
				setState(1592);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1591);
					match(DISTINCT);
					}
				}

				setState(1594);
				expression();
				setState(1595);
				match(T__2);
				}
				break;
			case MAX:
				enterOuterAlt(_localctx, 4);
				{
				setState(1597);
				match(MAX);
				setState(1598);
				match(T__1);
				setState(1600);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1599);
					match(DISTINCT);
					}
				}

				setState(1602);
				expression();
				setState(1603);
				match(T__2);
				}
				break;
			case AVG:
				enterOuterAlt(_localctx, 5);
				{
				setState(1605);
				match(AVG);
				setState(1606);
				match(T__1);
				setState(1608);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1607);
					match(DISTINCT);
					}
				}

				setState(1610);
				expression();
				setState(1611);
				match(T__2);
				}
				break;
			case STDEV:
				enterOuterAlt(_localctx, 6);
				{
				setState(1613);
				match(STDEV);
				setState(1614);
				match(T__1);
				setState(1616);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1615);
					match(DISTINCT);
					}
				}

				setState(1618);
				expression();
				setState(1619);
				match(T__2);
				}
				break;
			case SAMPLE:
				enterOuterAlt(_localctx, 7);
				{
				setState(1621);
				match(SAMPLE);
				setState(1622);
				match(T__1);
				setState(1624);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1623);
					match(DISTINCT);
					}
				}

				setState(1626);
				expression();
				setState(1627);
				match(T__2);
				}
				break;
			case GROUP_CONCAT:
				enterOuterAlt(_localctx, 8);
				{
				setState(1629);
				match(GROUP_CONCAT);
				setState(1630);
				match(T__1);
				setState(1632);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DISTINCT) {
					{
					setState(1631);
					match(DISTINCT);
					}
				}

				setState(1634);
				expression();
				setState(1639);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__5) {
					{
					setState(1635);
					match(T__5);
					setState(1636);
					match(SEPARATOR);
					setState(1637);
					match(T__19);
					setState(1638);
					string();
					}
				}

				setState(1641);
				match(T__2);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IriOrFunctionContext extends ParserRuleContext {
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public ArgListContext argList() {
			return getRuleContext(ArgListContext.class,0);
		}
		public IriOrFunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_iriOrFunction; }
	}

	public final IriOrFunctionContext iriOrFunction() throws RecognitionException {
		IriOrFunctionContext _localctx = new IriOrFunctionContext(_ctx, getState());
		enterRule(_localctx, 294, RULE_iriOrFunction);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1645);
			iri();
			setState(1647);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__1 || _la==NIL) {
				{
				setState(1646);
				argList();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class RdfLiteralContext extends ParserRuleContext {
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode LANGTAG() { return getToken(SparqlAutomaticParser.LANGTAG, 0); }
		public IriContext iri() {
			return getRuleContext(IriContext.class,0);
		}
		public RdfLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rdfLiteral; }
	}

	public final RdfLiteralContext rdfLiteral() throws RecognitionException {
		RdfLiteralContext _localctx = new RdfLiteralContext(_ctx, getState());
		enterRule(_localctx, 296, RULE_rdfLiteral);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1649);
			string();
			setState(1653);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LANGTAG:
				{
				setState(1650);
				match(LANGTAG);
				}
				break;
			case T__26:
				{
				{
				setState(1651);
				match(T__26);
				setState(1652);
				iri();
				}
				}
				break;
			case T__0:
			case T__1:
			case T__2:
			case T__3:
			case T__4:
			case T__5:
			case T__6:
			case T__7:
			case T__8:
			case T__10:
			case T__11:
			case T__12:
			case T__14:
			case T__15:
			case T__16:
			case T__17:
			case T__18:
			case T__19:
			case T__20:
			case T__21:
			case T__22:
			case T__23:
			case T__24:
			case T__25:
			case T__27:
			case T__28:
			case AS:
			case VALUES:
			case GRAPH:
			case OPTIONAL:
			case SERVICE:
			case BIND:
			case UNDEF:
			case MINUS:
			case FILTER:
			case NOT:
			case IN:
			case IRI_REF:
			case PNAME_NS:
			case PNAME_LN:
			case BLANK_NODE_LABEL:
			case VAR1:
			case VAR2:
			case PREFIX_LANGTAG:
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
			case STRING_LITERAL1:
			case STRING_LITERAL2:
			case STRING_LITERAL_LONG1:
			case STRING_LITERAL_LONG2:
			case NIL:
			case ANON:
				break;
			default:
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralContext extends ParserRuleContext {
		public NumericLiteralUnsignedContext numericLiteralUnsigned() {
			return getRuleContext(NumericLiteralUnsignedContext.class,0);
		}
		public NumericLiteralPositiveContext numericLiteralPositive() {
			return getRuleContext(NumericLiteralPositiveContext.class,0);
		}
		public NumericLiteralNegativeContext numericLiteralNegative() {
			return getRuleContext(NumericLiteralNegativeContext.class,0);
		}
		public NumericLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericLiteral; }
	}

	public final NumericLiteralContext numericLiteral() throws RecognitionException {
		NumericLiteralContext _localctx = new NumericLiteralContext(_ctx, getState());
		enterRule(_localctx, 298, RULE_numericLiteral);
		try {
			setState(1658);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER:
			case DECIMAL:
			case DOUBLE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1655);
				numericLiteralUnsigned();
				}
				break;
			case INTEGER_POSITIVE:
			case DECIMAL_POSITIVE:
			case DOUBLE_POSITIVE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1656);
				numericLiteralPositive();
				}
				break;
			case INTEGER_NEGATIVE:
			case DECIMAL_NEGATIVE:
			case DOUBLE_NEGATIVE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1657);
				numericLiteralNegative();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralUnsignedContext extends ParserRuleContext {
		public TerminalNode INTEGER() { return getToken(SparqlAutomaticParser.INTEGER, 0); }
		public TerminalNode DECIMAL() { return getToken(SparqlAutomaticParser.DECIMAL, 0); }
		public TerminalNode DOUBLE() { return getToken(SparqlAutomaticParser.DOUBLE, 0); }
		public NumericLiteralUnsignedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericLiteralUnsigned; }
	}

	public final NumericLiteralUnsignedContext numericLiteralUnsigned() throws RecognitionException {
		NumericLiteralUnsignedContext _localctx = new NumericLiteralUnsignedContext(_ctx, getState());
		enterRule(_localctx, 300, RULE_numericLiteralUnsigned);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1660);
			_la = _input.LA(1);
			if ( !(((((_la - 150)) & ~0x3f) == 0 && ((1L << (_la - 150)) & 7L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralPositiveContext extends ParserRuleContext {
		public TerminalNode INTEGER_POSITIVE() { return getToken(SparqlAutomaticParser.INTEGER_POSITIVE, 0); }
		public TerminalNode DECIMAL_POSITIVE() { return getToken(SparqlAutomaticParser.DECIMAL_POSITIVE, 0); }
		public TerminalNode DOUBLE_POSITIVE() { return getToken(SparqlAutomaticParser.DOUBLE_POSITIVE, 0); }
		public NumericLiteralPositiveContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericLiteralPositive; }
	}

	public final NumericLiteralPositiveContext numericLiteralPositive() throws RecognitionException {
		NumericLiteralPositiveContext _localctx = new NumericLiteralPositiveContext(_ctx, getState());
		enterRule(_localctx, 302, RULE_numericLiteralPositive);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1662);
			_la = _input.LA(1);
			if ( !(((((_la - 153)) & ~0x3f) == 0 && ((1L << (_la - 153)) & 7L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumericLiteralNegativeContext extends ParserRuleContext {
		public TerminalNode INTEGER_NEGATIVE() { return getToken(SparqlAutomaticParser.INTEGER_NEGATIVE, 0); }
		public TerminalNode DECIMAL_NEGATIVE() { return getToken(SparqlAutomaticParser.DECIMAL_NEGATIVE, 0); }
		public TerminalNode DOUBLE_NEGATIVE() { return getToken(SparqlAutomaticParser.DOUBLE_NEGATIVE, 0); }
		public NumericLiteralNegativeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_numericLiteralNegative; }
	}

	public final NumericLiteralNegativeContext numericLiteralNegative() throws RecognitionException {
		NumericLiteralNegativeContext _localctx = new NumericLiteralNegativeContext(_ctx, getState());
		enterRule(_localctx, 304, RULE_numericLiteralNegative);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1664);
			_la = _input.LA(1);
			if ( !(((((_la - 156)) & ~0x3f) == 0 && ((1L << (_la - 156)) & 7L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BooleanLiteralContext extends ParserRuleContext {
		public BooleanLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanLiteral; }
	}

	public final BooleanLiteralContext booleanLiteral() throws RecognitionException {
		BooleanLiteralContext _localctx = new BooleanLiteralContext(_ctx, getState());
		enterRule(_localctx, 306, RULE_booleanLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1666);
			_la = _input.LA(1);
			if ( !(_la==T__27 || _la==T__28) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StringContext extends ParserRuleContext {
		public TerminalNode STRING_LITERAL1() { return getToken(SparqlAutomaticParser.STRING_LITERAL1, 0); }
		public TerminalNode STRING_LITERAL2() { return getToken(SparqlAutomaticParser.STRING_LITERAL2, 0); }
		public TerminalNode STRING_LITERAL_LONG1() { return getToken(SparqlAutomaticParser.STRING_LITERAL_LONG1, 0); }
		public TerminalNode STRING_LITERAL_LONG2() { return getToken(SparqlAutomaticParser.STRING_LITERAL_LONG2, 0); }
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 308, RULE_string);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1668);
			_la = _input.LA(1);
			if ( !(((((_la - 160)) & ~0x3f) == 0 && ((1L << (_la - 160)) & 15L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IriContext extends ParserRuleContext {
		public IrirefContext iriref() {
			return getRuleContext(IrirefContext.class,0);
		}
		public PrefixedNameContext prefixedName() {
			return getRuleContext(PrefixedNameContext.class,0);
		}
		public TerminalNode PREFIX_LANGTAG() { return getToken(SparqlAutomaticParser.PREFIX_LANGTAG, 0); }
		public IriContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_iri; }
	}

	public final IriContext iri() throws RecognitionException {
		IriContext _localctx = new IriContext(_ctx, getState());
		enterRule(_localctx, 310, RULE_iri);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1671);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PREFIX_LANGTAG) {
				{
				setState(1670);
				match(PREFIX_LANGTAG);
				}
			}

			setState(1675);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case IRI_REF:
				{
				setState(1673);
				iriref();
				}
				break;
			case PNAME_NS:
			case PNAME_LN:
				{
				setState(1674);
				prefixedName();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrefixedNameContext extends ParserRuleContext {
		public PnameLnContext pnameLn() {
			return getRuleContext(PnameLnContext.class,0);
		}
		public PnameNsContext pnameNs() {
			return getRuleContext(PnameNsContext.class,0);
		}
		public PrefixedNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prefixedName; }
	}

	public final PrefixedNameContext prefixedName() throws RecognitionException {
		PrefixedNameContext _localctx = new PrefixedNameContext(_ctx, getState());
		enterRule(_localctx, 312, RULE_prefixedName);
		try {
			setState(1679);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PNAME_LN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1677);
				pnameLn();
				}
				break;
			case PNAME_NS:
				enterOuterAlt(_localctx, 2);
				{
				setState(1678);
				pnameNs();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class BlankNodeContext extends ParserRuleContext {
		public TerminalNode BLANK_NODE_LABEL() { return getToken(SparqlAutomaticParser.BLANK_NODE_LABEL, 0); }
		public TerminalNode ANON() { return getToken(SparqlAutomaticParser.ANON, 0); }
		public BlankNodeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_blankNode; }
	}

	public final BlankNodeContext blankNode() throws RecognitionException {
		BlankNodeContext _localctx = new BlankNodeContext(_ctx, getState());
		enterRule(_localctx, 314, RULE_blankNode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1681);
			_la = _input.LA(1);
			if ( !(_la==BLANK_NODE_LABEL || _la==ANON) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IrirefContext extends ParserRuleContext {
		public TerminalNode IRI_REF() { return getToken(SparqlAutomaticParser.IRI_REF, 0); }
		public IrirefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_iriref; }
	}

	public final IrirefContext iriref() throws RecognitionException {
		IrirefContext _localctx = new IrirefContext(_ctx, getState());
		enterRule(_localctx, 316, RULE_iriref);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1683);
			match(IRI_REF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PnameLnContext extends ParserRuleContext {
		public TerminalNode PNAME_LN() { return getToken(SparqlAutomaticParser.PNAME_LN, 0); }
		public PnameLnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pnameLn; }
	}

	public final PnameLnContext pnameLn() throws RecognitionException {
		PnameLnContext _localctx = new PnameLnContext(_ctx, getState());
		enterRule(_localctx, 318, RULE_pnameLn);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1685);
			match(PNAME_LN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PnameNsContext extends ParserRuleContext {
		public TerminalNode PNAME_NS() { return getToken(SparqlAutomaticParser.PNAME_NS, 0); }
		public PnameNsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pnameNs; }
	}

	public final PnameNsContext pnameNs() throws RecognitionException {
		PnameNsContext _localctx = new PnameNsContext(_ctx, getState());
		enterRule(_localctx, 320, RULE_pnameNs);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1687);
			match(PNAME_NS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u0001\u00b0\u069a\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007"+
		"r\u0002s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0002w\u0007"+
		"w\u0002x\u0007x\u0002y\u0007y\u0002z\u0007z\u0002{\u0007{\u0002|\u0007"+
		"|\u0002}\u0007}\u0002~\u0007~\u0002\u007f\u0007\u007f\u0002\u0080\u0007"+
		"\u0080\u0002\u0081\u0007\u0081\u0002\u0082\u0007\u0082\u0002\u0083\u0007"+
		"\u0083\u0002\u0084\u0007\u0084\u0002\u0085\u0007\u0085\u0002\u0086\u0007"+
		"\u0086\u0002\u0087\u0007\u0087\u0002\u0088\u0007\u0088\u0002\u0089\u0007"+
		"\u0089\u0002\u008a\u0007\u008a\u0002\u008b\u0007\u008b\u0002\u008c\u0007"+
		"\u008c\u0002\u008d\u0007\u008d\u0002\u008e\u0007\u008e\u0002\u008f\u0007"+
		"\u008f\u0002\u0090\u0007\u0090\u0002\u0091\u0007\u0091\u0002\u0092\u0007"+
		"\u0092\u0002\u0093\u0007\u0093\u0002\u0094\u0007\u0094\u0002\u0095\u0007"+
		"\u0095\u0002\u0096\u0007\u0096\u0002\u0097\u0007\u0097\u0002\u0098\u0007"+
		"\u0098\u0002\u0099\u0007\u0099\u0002\u009a\u0007\u009a\u0002\u009b\u0007"+
		"\u009b\u0002\u009c\u0007\u009c\u0002\u009d\u0007\u009d\u0002\u009e\u0007"+
		"\u009e\u0002\u009f\u0007\u009f\u0002\u00a0\u0007\u00a0\u0001\u0000\u0001"+
		"\u0000\u0003\u0000\u0145\b\u0000\u0001\u0000\u0001\u0000\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0003\u0001\u014e\b\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0005\u0002\u0154\b\u0002\n"+
		"\u0002\f\u0002\u0157\t\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0004\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0005"+
		"\u0005\u0162\b\u0005\n\u0005\f\u0005\u0165\t\u0005\u0001\u0005\u0001\u0005"+
		"\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0006"+
		"\u0001\u0007\u0001\u0007\u0003\u0007\u0171\b\u0007\u0001\u0007\u0004\u0007"+
		"\u0174\b\u0007\u000b\u0007\f\u0007\u0175\u0001\u0007\u0003\u0007\u0179"+
		"\b\u0007\u0001\b\u0001\b\u0003\b\u017d\b\b\u0001\t\u0001\t\u0001\t\u0001"+
		"\t\u0001\n\u0001\n\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001\u000b"+
		"\u0005\u000b\u018a\b\u000b\n\u000b\f\u000b\u018d\t\u000b\u0001\u000b\u0001"+
		"\u000b\u0001\u000b\u0001\u000b\u0005\u000b\u0193\b\u000b\n\u000b\f\u000b"+
		"\u0196\t\u000b\u0001\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u019b\b"+
		"\u000b\u0001\u000b\u0001\u000b\u0003\u000b\u019f\b\u000b\u0001\f\u0001"+
		"\f\u0004\f\u01a3\b\f\u000b\f\f\f\u01a4\u0001\f\u0003\f\u01a8\b\f\u0001"+
		"\f\u0005\f\u01ab\b\f\n\f\f\f\u01ae\t\f\u0001\f\u0003\f\u01b1\b\f\u0001"+
		"\f\u0001\f\u0001\r\u0001\r\u0005\r\u01b7\b\r\n\r\f\r\u01ba\t\r\u0001\r"+
		"\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000e\u0003\u000e\u01c2"+
		"\b\u000e\u0001\u000f\u0001\u000f\u0001\u0010\u0001\u0010\u0001\u0010\u0001"+
		"\u0011\u0001\u0011\u0001\u0012\u0003\u0012\u01cc\b\u0012\u0001\u0012\u0001"+
		"\u0012\u0001\u0013\u0003\u0013\u01d1\b\u0013\u0001\u0013\u0003\u0013\u01d4"+
		"\b\u0013\u0001\u0013\u0003\u0013\u01d7\b\u0013\u0001\u0013\u0003\u0013"+
		"\u01da\b\u0013\u0001\u0014\u0001\u0014\u0004\u0014\u01de\b\u0014\u000b"+
		"\u0014\f\u0014\u01df\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001"+
		"\u0015\u0001\u0015\u0003\u0015\u01e8\b\u0015\u0001\u0015\u0001\u0015\u0001"+
		"\u0015\u0003\u0015\u01ed\b\u0015\u0001\u0016\u0001\u0016\u0004\u0016\u01f1"+
		"\b\u0016\u000b\u0016\f\u0016\u01f2\u0001\u0017\u0001\u0017\u0001\u0018"+
		"\u0001\u0018\u0003\u0018\u01f9\b\u0018\u0001\u0018\u0004\u0018\u01fc\b"+
		"\u0018\u000b\u0018\f\u0018\u01fd\u0001\u0019\u0001\u0019\u0001\u0019\u0001"+
		"\u0019\u0003\u0019\u0204\b\u0019\u0003\u0019\u0206\b\u0019\u0001\u001a"+
		"\u0001\u001a\u0003\u001a\u020a\b\u001a\u0001\u001a\u0003\u001a\u020d\b"+
		"\u001a\u0001\u001a\u0001\u001a\u0003\u001a\u0211\b\u001a\u0001\u001a\u0003"+
		"\u001a\u0214\b\u001a\u0001\u001a\u0001\u001a\u0003\u001a\u0218\b\u001a"+
		"\u0001\u001a\u0003\u001a\u021b\b\u001a\u0001\u001a\u0001\u001a\u0003\u001a"+
		"\u021f\b\u001a\u0001\u001a\u0003\u001a\u0222\b\u001a\u0001\u001a\u0001"+
		"\u001a\u0003\u001a\u0226\b\u001a\u0001\u001a\u0003\u001a\u0229\b\u001a"+
		"\u0001\u001a\u0001\u001a\u0003\u001a\u022d\b\u001a\u0001\u001a\u0003\u001a"+
		"\u0230\b\u001a\u0003\u001a\u0232\b\u001a\u0001\u001b\u0001\u001b\u0001"+
		"\u001b\u0001\u001c\u0001\u001c\u0001\u001c\u0001\u001d\u0001\u001d\u0001"+
		"\u001d\u0001\u001e\u0001\u001e\u0003\u001e\u023f\b\u001e\u0001\u001f\u0001"+
		"\u001f\u0001\u001f\u0001\u001f\u0003\u001f\u0245\b\u001f\u0003\u001f\u0247"+
		"\b\u001f\u0001 \u0001 \u0001 \u0001 \u0001 \u0001 \u0001 \u0001 \u0001"+
		" \u0001 \u0001 \u0003 \u0254\b \u0001!\u0001!\u0003!\u0258\b!\u0001!\u0001"+
		"!\u0001!\u0003!\u025d\b!\u0001\"\u0001\"\u0003\"\u0261\b\"\u0001\"\u0001"+
		"\"\u0001#\u0001#\u0003#\u0267\b#\u0001#\u0001#\u0001$\u0001$\u0003$\u026d"+
		"\b$\u0001$\u0001$\u0001%\u0001%\u0003%\u0273\b%\u0001%\u0001%\u0001%\u0001"+
		"%\u0001&\u0001&\u0003&\u027b\b&\u0001&\u0001&\u0001&\u0001&\u0001\'\u0001"+
		"\'\u0003\'\u0283\b\'\u0001\'\u0001\'\u0001\'\u0001\'\u0001(\u0001(\u0001"+
		"(\u0001(\u0001)\u0001)\u0001)\u0001)\u0001*\u0001*\u0001*\u0001*\u0001"+
		"+\u0001+\u0003+\u0297\b+\u0001+\u0001+\u0003+\u029b\b+\u0001+\u0003+\u029e"+
		"\b+\u0001+\u0005+\u02a1\b+\n+\f+\u02a4\t+\u0001+\u0001+\u0001+\u0001,"+
		"\u0001,\u0001,\u0001-\u0001-\u0001-\u0001.\u0001.\u0001.\u0001.\u0003"+
		".\u02b3\b.\u0001/\u0001/\u0003/\u02b7\b/\u0001/\u0003/\u02ba\b/\u0001"+
		"0\u00010\u00010\u00011\u00011\u00011\u00011\u00031\u02c3\b1\u00012\u0001"+
		"2\u00012\u00012\u00013\u00013\u00013\u00013\u00014\u00034\u02ce\b4\u0001"+
		"4\u00014\u00034\u02d2\b4\u00014\u00034\u02d5\b4\u00054\u02d7\b4\n4\f4"+
		"\u02da\t4\u00015\u00015\u00015\u00015\u00035\u02e0\b5\u00015\u00015\u0001"+
		"6\u00016\u00016\u00036\u02e7\b6\u00036\u02e9\b6\u00017\u00017\u00017\u0003"+
		"7\u02ee\b7\u00017\u00017\u00018\u00038\u02f3\b8\u00018\u00058\u02f6\b"+
		"8\n8\f8\u02f9\t8\u00019\u00019\u00039\u02fd\b9\u00019\u00039\u0300\b9"+
		"\u0001:\u0001:\u0001:\u0003:\u0305\b:\u0003:\u0307\b:\u0001;\u0001;\u0001"+
		";\u0001;\u0001;\u0001;\u0001;\u0001;\u0003;\u0311\b;\u0001<\u0001<\u0001"+
		"<\u0001=\u0001=\u0001=\u0001=\u0001>\u0001>\u0003>\u031c\b>\u0001>\u0001"+
		">\u0001>\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001?\u0001@\u0001"+
		"@\u0001@\u0001A\u0001A\u0003A\u032d\bA\u0001B\u0001B\u0001B\u0005B\u0332"+
		"\bB\nB\fB\u0335\tB\u0001B\u0001B\u0001C\u0001C\u0001C\u0005C\u033c\bC"+
		"\nC\fC\u033f\tC\u0001C\u0003C\u0342\bC\u0001C\u0001C\u0005C\u0346\bC\n"+
		"C\fC\u0349\tC\u0001C\u0001C\u0001D\u0001D\u0005D\u034f\bD\nD\fD\u0352"+
		"\tD\u0001D\u0001D\u0003D\u0356\bD\u0001E\u0001E\u0001E\u0001E\u0001E\u0003"+
		"E\u035d\bE\u0001F\u0001F\u0001F\u0001G\u0001G\u0001G\u0005G\u0365\bG\n"+
		"G\fG\u0368\tG\u0001H\u0001H\u0001H\u0001I\u0001I\u0001I\u0003I\u0370\b"+
		"I\u0001J\u0001J\u0001J\u0001K\u0001K\u0001K\u0003K\u0378\bK\u0001K\u0001"+
		"K\u0001K\u0005K\u037d\bK\nK\fK\u0380\tK\u0001K\u0001K\u0003K\u0384\bK"+
		"\u0001L\u0001L\u0001L\u0001L\u0001L\u0005L\u038b\bL\nL\fL\u038e\tL\u0001"+
		"L\u0001L\u0003L\u0392\bL\u0001M\u0001M\u0003M\u0396\bM\u0001M\u0001M\u0001"+
		"N\u0001N\u0001N\u0003N\u039d\bN\u0003N\u039f\bN\u0001O\u0001O\u0001O\u0001"+
		"O\u0001O\u0001O\u0003O\u03a7\bO\u0001P\u0003P\u03aa\bP\u0001Q\u0001Q\u0001"+
		"Q\u0001Q\u0001Q\u0001Q\u0003Q\u03b2\bQ\u0005Q\u03b4\bQ\nQ\fQ\u03b7\tQ"+
		"\u0001R\u0001R\u0003R\u03bb\bR\u0001S\u0001S\u0001S\u0005S\u03c0\bS\n"+
		"S\fS\u03c3\tS\u0001T\u0001T\u0001U\u0001U\u0001U\u0001U\u0001U\u0001U"+
		"\u0003U\u03cd\bU\u0001V\u0003V\u03d0\bV\u0001W\u0001W\u0001W\u0003W\u03d5"+
		"\bW\u0005W\u03d7\bW\nW\fW\u03da\tW\u0001X\u0001X\u0001Y\u0001Y\u0001Z"+
		"\u0001Z\u0001Z\u0001[\u0001[\u0001[\u0001\\\u0001\\\u0003\\\u03e8\b\\"+
		"\u0001]\u0001]\u0001]\u0005]\u03ed\b]\n]\f]\u03f0\t]\u0001^\u0001^\u0001"+
		"_\u0001_\u0001`\u0001`\u0001`\u0005`\u03f9\b`\n`\f`\u03fc\t`\u0001a\u0001"+
		"a\u0001a\u0005a\u0401\ba\na\fa\u0404\ta\u0001b\u0001b\u0003b\u0408\bb"+
		"\u0001c\u0001c\u0001c\u0003c\u040d\bc\u0001d\u0001d\u0001d\u0001d\u0001"+
		"d\u0001d\u0001d\u0003d\u0416\bd\u0001e\u0001e\u0001e\u0001e\u0001e\u0001"+
		"e\u0001e\u0001e\u0001e\u0001e\u0003e\u0422\be\u0001f\u0001f\u0001g\u0001"+
		"g\u0001h\u0001h\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001i\u0001"+
		"i\u0003i\u0432\bi\u0001j\u0001j\u0001j\u0001j\u0001j\u0005j\u0439\bj\n"+
		"j\fj\u043c\tj\u0003j\u043e\bj\u0001j\u0003j\u0441\bj\u0001k\u0001k\u0001"+
		"k\u0001k\u0001k\u0003k\u0448\bk\u0003k\u044a\bk\u0001l\u0001l\u0001m\u0001"+
		"m\u0003m\u0450\bm\u0001n\u0001n\u0001n\u0001n\u0001o\u0001o\u0003o\u0458"+
		"\bo\u0001p\u0001p\u0001p\u0001p\u0001q\u0001q\u0004q\u0460\bq\u000bq\f"+
		"q\u0461\u0001q\u0001q\u0001r\u0001r\u0004r\u0468\br\u000br\fr\u0469\u0001"+
		"r\u0001r\u0001s\u0001s\u0003s\u0470\bs\u0001t\u0001t\u0003t\u0474\bt\u0001"+
		"u\u0001u\u0003u\u0478\bu\u0001v\u0001v\u0003v\u047c\bv\u0001w\u0001w\u0001"+
		"x\u0001x\u0001x\u0001x\u0001x\u0001x\u0003x\u0486\bx\u0001y\u0001y\u0001"+
		"z\u0001z\u0001z\u0005z\u048d\bz\nz\fz\u0490\tz\u0001{\u0001{\u0001{\u0005"+
		"{\u0495\b{\n{\f{\u0498\t{\u0001|\u0001|\u0001}\u0001}\u0001}\u0001}\u0001"+
		"}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001}\u0001"+
		"}\u0001}\u0001}\u0001}\u0003}\u04ae\b}\u0001~\u0001~\u0001\u007f\u0001"+
		"\u007f\u0005\u007f\u04b4\b\u007f\n\u007f\f\u007f\u04b7\t\u007f\u0001\u0080"+
		"\u0001\u0080\u0001\u0080\u0001\u0080\u0001\u0080\u0003\u0080\u04be\b\u0080"+
		"\u0001\u0081\u0001\u0081\u0001\u0082\u0001\u0082\u0001\u0083\u0001\u0083"+
		"\u0003\u0083\u04c6\b\u0083\u0001\u0083\u0005\u0083\u04c9\b\u0083\n\u0083"+
		"\f\u0083\u04cc\t\u0083\u0001\u0084\u0001\u0084\u0005\u0084\u04d0\b\u0084"+
		"\n\u0084\f\u0084\u04d3\t\u0084\u0001\u0085\u0001\u0085\u0003\u0085\u04d7"+
		"\b\u0085\u0001\u0086\u0001\u0086\u0001\u0086\u0001\u0087\u0001\u0087\u0001"+
		"\u0087\u0001\u0088\u0001\u0088\u0001\u0088\u0001\u0088\u0001\u0088\u0001"+
		"\u0088\u0001\u0088\u0003\u0088\u04e6\b\u0088\u0001\u0089\u0001\u0089\u0001"+
		"\u0089\u0001\u0089\u0001\u0089\u0001\u0089\u0001\u0089\u0003\u0089\u04ef"+
		"\b\u0089\u0001\u008a\u0001\u008a\u0001\u008a\u0001\u008a\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0003\u008b\u051d\b\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001\u008b\u0001"+
		"\u008b\u0003\u008b\u05f3\b\u008b\u0001\u008c\u0001\u008c\u0001\u008c\u0001"+
		"\u008c\u0001\u008c\u0001\u008c\u0001\u008c\u0003\u008c\u05fc\b\u008c\u0001"+
		"\u008c\u0001\u008c\u0001\u008d\u0001\u008d\u0001\u008d\u0001\u008d\u0001"+
		"\u008d\u0001\u008e\u0001\u008e\u0001\u008e\u0001\u008e\u0001\u008e\u0001"+
		"\u008e\u0001\u008e\u0003\u008e\u060c\b\u008e\u0001\u008e\u0001\u008e\u0001"+
		"\u008f\u0001\u008f\u0001\u008f\u0001\u008f\u0001\u008f\u0001\u008f\u0001"+
		"\u008f\u0001\u008f\u0001\u008f\u0003\u008f\u0619\b\u008f\u0001\u008f\u0001"+
		"\u008f\u0001\u0090\u0001\u0090\u0001\u0090\u0001\u0091\u0001\u0091\u0001"+
		"\u0091\u0001\u0091\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092\u0627"+
		"\b\u0092\u0001\u0092\u0001\u0092\u0003\u0092\u062b\b\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092\u0631\b\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092"+
		"\u0639\b\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092"+
		"\u0001\u0092\u0003\u0092\u0641\b\u0092\u0001\u0092\u0001\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092\u0649\b\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092"+
		"\u0651\b\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092"+
		"\u0001\u0092\u0003\u0092\u0659\b\u0092\u0001\u0092\u0001\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092\u0661\b\u0092\u0001\u0092"+
		"\u0001\u0092\u0001\u0092\u0001\u0092\u0001\u0092\u0003\u0092\u0668\b\u0092"+
		"\u0001\u0092\u0001\u0092\u0003\u0092\u066c\b\u0092\u0001\u0093\u0001\u0093"+
		"\u0003\u0093\u0670\b\u0093\u0001\u0094\u0001\u0094\u0001\u0094\u0001\u0094"+
		"\u0003\u0094\u0676\b\u0094\u0001\u0095\u0001\u0095\u0001\u0095\u0003\u0095"+
		"\u067b\b\u0095\u0001\u0096\u0001\u0096\u0001\u0097\u0001\u0097\u0001\u0098"+
		"\u0001\u0098\u0001\u0099\u0001\u0099\u0001\u009a\u0001\u009a\u0001\u009b"+
		"\u0003\u009b\u0688\b\u009b\u0001\u009b\u0001\u009b\u0003\u009b\u068c\b"+
		"\u009b\u0001\u009c\u0001\u009c\u0003\u009c\u0690\b\u009c\u0001\u009d\u0001"+
		"\u009d\u0001\u009e\u0001\u009e\u0001\u009f\u0001\u009f\u0001\u00a0\u0001"+
		"\u00a0\u0001\u00a0\u0000\u0000\u00a1\u0000\u0002\u0004\u0006\b\n\f\u000e"+
		"\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDF"+
		"HJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c"+
		"\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4"+
		"\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc"+
		"\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4"+
		"\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec"+
		"\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\u0102\u0104"+
		"\u0106\u0108\u010a\u010c\u010e\u0110\u0112\u0114\u0116\u0118\u011a\u011c"+
		"\u011e\u0120\u0122\u0124\u0126\u0128\u012a\u012c\u012e\u0130\u0132\u0134"+
		"\u0136\u0138\u013a\u013c\u013e\u0140\u0000\t\u0001\u0000!\"\u0001\u0000"+
		"/0\u0001\u0000\u0092\u0093\u0001\u0000\u0096\u0098\u0001\u0000\u0099\u009b"+
		"\u0001\u0000\u009c\u009e\u0001\u0000\u001c\u001d\u0001\u0000\u00a0\u00a3"+
		"\u0002\u0000\u0091\u0091\u00a6\u00a6\u0711\u0000\u0144\u0001\u0000\u0000"+
		"\u0000\u0002\u0148\u0001\u0000\u0000\u0000\u0004\u0155\u0001\u0000\u0000"+
		"\u0000\u0006\u0158\u0001\u0000\u0000\u0000\b\u015b\u0001\u0000\u0000\u0000"+
		"\n\u015f\u0001\u0000\u0000\u0000\f\u0169\u0001\u0000\u0000\u0000\u000e"+
		"\u016e\u0001\u0000\u0000\u0000\u0010\u017c\u0001\u0000\u0000\u0000\u0012"+
		"\u017e\u0001\u0000\u0000\u0000\u0014\u0182\u0001\u0000\u0000\u0000\u0016"+
		"\u0186\u0001\u0000\u0000\u0000\u0018\u01a0\u0001\u0000\u0000\u0000\u001a"+
		"\u01b4\u0001\u0000\u0000\u0000\u001c\u01be\u0001\u0000\u0000\u0000\u001e"+
		"\u01c3\u0001\u0000\u0000\u0000 \u01c5\u0001\u0000\u0000\u0000\"\u01c8"+
		"\u0001\u0000\u0000\u0000$\u01cb\u0001\u0000\u0000\u0000&\u01d0\u0001\u0000"+
		"\u0000\u0000(\u01db\u0001\u0000\u0000\u0000*\u01ec\u0001\u0000\u0000\u0000"+
		",\u01ee\u0001\u0000\u0000\u0000.\u01f4\u0001\u0000\u0000\u00000\u01f8"+
		"\u0001\u0000\u0000\u00002\u0205\u0001\u0000\u0000\u00004\u0231\u0001\u0000"+
		"\u0000\u00006\u0233\u0001\u0000\u0000\u00008\u0236\u0001\u0000\u0000\u0000"+
		":\u0239\u0001\u0000\u0000\u0000<\u023e\u0001\u0000\u0000\u0000>\u0240"+
		"\u0001\u0000\u0000\u0000@\u0253\u0001\u0000\u0000\u0000B\u0255\u0001\u0000"+
		"\u0000\u0000D\u025e\u0001\u0000\u0000\u0000F\u0264\u0001\u0000\u0000\u0000"+
		"H\u026a\u0001\u0000\u0000\u0000J\u0270\u0001\u0000\u0000\u0000L\u0278"+
		"\u0001\u0000\u0000\u0000N\u0280\u0001\u0000\u0000\u0000P\u0288\u0001\u0000"+
		"\u0000\u0000R\u028c\u0001\u0000\u0000\u0000T\u0290\u0001\u0000\u0000\u0000"+
		"V\u0296\u0001\u0000\u0000\u0000X\u02a8\u0001\u0000\u0000\u0000Z\u02ab"+
		"\u0001\u0000\u0000\u0000\\\u02ae\u0001\u0000\u0000\u0000^\u02b9\u0001"+
		"\u0000\u0000\u0000`\u02bb\u0001\u0000\u0000\u0000b\u02c2\u0001\u0000\u0000"+
		"\u0000d\u02c4\u0001\u0000\u0000\u0000f\u02c8\u0001\u0000\u0000\u0000h"+
		"\u02cd\u0001\u0000\u0000\u0000j\u02db\u0001\u0000\u0000\u0000l\u02e3\u0001"+
		"\u0000\u0000\u0000n\u02ea\u0001\u0000\u0000\u0000p\u02f2\u0001\u0000\u0000"+
		"\u0000r\u02fa\u0001\u0000\u0000\u0000t\u0301\u0001\u0000\u0000\u0000v"+
		"\u0310\u0001\u0000\u0000\u0000x\u0312\u0001\u0000\u0000\u0000z\u0315\u0001"+
		"\u0000\u0000\u0000|\u0319\u0001\u0000\u0000\u0000~\u0320\u0001\u0000\u0000"+
		"\u0000\u0080\u0327\u0001\u0000\u0000\u0000\u0082\u032c\u0001\u0000\u0000"+
		"\u0000\u0084\u032e\u0001\u0000\u0000\u0000\u0086\u0341\u0001\u0000\u0000"+
		"\u0000\u0088\u0355\u0001\u0000\u0000\u0000\u008a\u035c\u0001\u0000\u0000"+
		"\u0000\u008c\u035e\u0001\u0000\u0000\u0000\u008e\u0361\u0001\u0000\u0000"+
		"\u0000\u0090\u0369\u0001\u0000\u0000\u0000\u0092\u036f\u0001\u0000\u0000"+
		"\u0000\u0094\u0371\u0001\u0000\u0000\u0000\u0096\u0383\u0001\u0000\u0000"+
		"\u0000\u0098\u0391\u0001\u0000\u0000\u0000\u009a\u0393\u0001\u0000\u0000"+
		"\u0000\u009c\u0399\u0001\u0000\u0000\u0000\u009e\u03a6\u0001\u0000\u0000"+
		"\u0000\u00a0\u03a9\u0001\u0000\u0000\u0000\u00a2\u03ab\u0001\u0000\u0000"+
		"\u0000\u00a4\u03ba\u0001\u0000\u0000\u0000\u00a6\u03bc\u0001\u0000\u0000"+
		"\u0000\u00a8\u03c4\u0001\u0000\u0000\u0000\u00aa\u03cc\u0001\u0000\u0000"+
		"\u0000\u00ac\u03cf\u0001\u0000\u0000\u0000\u00ae\u03d1\u0001\u0000\u0000"+
		"\u0000\u00b0\u03db\u0001\u0000\u0000\u0000\u00b2\u03dd\u0001\u0000\u0000"+
		"\u0000\u00b4\u03df\u0001\u0000\u0000\u0000\u00b6\u03e2\u0001\u0000\u0000"+
		"\u0000\u00b8\u03e7\u0001\u0000\u0000\u0000\u00ba\u03e9\u0001\u0000\u0000"+
		"\u0000\u00bc\u03f1\u0001\u0000\u0000\u0000\u00be\u03f3\u0001\u0000\u0000"+
		"\u0000\u00c0\u03f5\u0001\u0000\u0000\u0000\u00c2\u03fd\u0001\u0000\u0000"+
		"\u0000\u00c4\u0405\u0001\u0000\u0000\u0000\u00c6\u040c\u0001\u0000\u0000"+
		"\u0000\u00c8\u0415\u0001\u0000\u0000\u0000\u00ca\u0421\u0001\u0000\u0000"+
		"\u0000\u00cc\u0423\u0001\u0000\u0000\u0000\u00ce\u0425\u0001\u0000\u0000"+
		"\u0000\u00d0\u0427\u0001\u0000\u0000\u0000\u00d2\u0431\u0001\u0000\u0000"+
		"\u0000\u00d4\u0440\u0001\u0000\u0000\u0000\u00d6\u0449\u0001\u0000\u0000"+
		"\u0000\u00d8\u044b\u0001\u0000\u0000\u0000\u00da\u044f\u0001\u0000\u0000"+
		"\u0000\u00dc\u0451\u0001\u0000\u0000\u0000\u00de\u0457\u0001\u0000\u0000"+
		"\u0000\u00e0\u0459\u0001\u0000\u0000\u0000\u00e2\u045d\u0001\u0000\u0000"+
		"\u0000\u00e4\u0465\u0001\u0000\u0000\u0000\u00e6\u046f\u0001\u0000\u0000"+
		"\u0000\u00e8\u0473\u0001\u0000\u0000\u0000\u00ea\u0477\u0001\u0000\u0000"+
		"\u0000\u00ec\u047b\u0001\u0000\u0000\u0000\u00ee\u047d\u0001\u0000\u0000"+
		"\u0000\u00f0\u0485\u0001\u0000\u0000\u0000\u00f2\u0487\u0001\u0000\u0000"+
		"\u0000\u00f4\u0489\u0001\u0000\u0000\u0000\u00f6\u0491\u0001\u0000\u0000"+
		"\u0000\u00f8\u0499\u0001\u0000\u0000\u0000\u00fa\u049b\u0001\u0000\u0000"+
		"\u0000\u00fc\u04af\u0001\u0000\u0000\u0000\u00fe\u04b1\u0001\u0000\u0000"+
		"\u0000\u0100\u04bd\u0001\u0000\u0000\u0000\u0102\u04bf\u0001\u0000\u0000"+
		"\u0000\u0104\u04c1\u0001\u0000\u0000\u0000\u0106\u04c5\u0001\u0000\u0000"+
		"\u0000\u0108\u04cd\u0001\u0000\u0000\u0000\u010a\u04d6\u0001\u0000\u0000"+
		"\u0000\u010c\u04d8\u0001\u0000\u0000\u0000\u010e\u04db\u0001\u0000\u0000"+
		"\u0000\u0110\u04e5\u0001\u0000\u0000\u0000\u0112\u04ee\u0001\u0000\u0000"+
		"\u0000\u0114\u04f0\u0001\u0000\u0000\u0000\u0116\u05f2\u0001\u0000\u0000"+
		"\u0000\u0118\u05f4\u0001\u0000\u0000\u0000\u011a\u05ff\u0001\u0000\u0000"+
		"\u0000\u011c\u0604\u0001\u0000\u0000\u0000\u011e\u060f\u0001\u0000\u0000"+
		"\u0000\u0120\u061c\u0001\u0000\u0000\u0000\u0122\u061f\u0001\u0000\u0000"+
		"\u0000\u0124\u066b\u0001\u0000\u0000\u0000\u0126\u066d\u0001\u0000\u0000"+
		"\u0000\u0128\u0671\u0001\u0000\u0000\u0000\u012a\u067a\u0001\u0000\u0000"+
		"\u0000\u012c\u067c\u0001\u0000\u0000\u0000\u012e\u067e\u0001\u0000\u0000"+
		"\u0000\u0130\u0680\u0001\u0000\u0000\u0000\u0132\u0682\u0001\u0000\u0000"+
		"\u0000\u0134\u0684\u0001\u0000\u0000\u0000\u0136\u0687\u0001\u0000\u0000"+
		"\u0000\u0138\u068f\u0001\u0000\u0000\u0000\u013a\u0691\u0001\u0000\u0000"+
		"\u0000\u013c\u0693\u0001\u0000\u0000\u0000\u013e\u0695\u0001\u0000\u0000"+
		"\u0000\u0140\u0697\u0001\u0000\u0000\u0000\u0142\u0145\u0003\u0002\u0001"+
		"\u0000\u0143\u0145\u0003>\u001f\u0000\u0144\u0142\u0001\u0000\u0000\u0000"+
		"\u0144\u0143\u0001\u0000\u0000\u0000\u0145\u0146\u0001\u0000\u0000\u0000"+
		"\u0146\u0147\u0005\u0000\u0000\u0001\u0147\u0001\u0001\u0000\u0000\u0000"+
		"\u0148\u014d\u0003\u0004\u0002\u0000\u0149\u014e\u0003\n\u0005\u0000\u014a"+
		"\u014e\u0003\u0016\u000b\u0000\u014b\u014e\u0003\u0018\f\u0000\u014c\u014e"+
		"\u0003\u001a\r\u0000\u014d\u0149\u0001\u0000\u0000\u0000\u014d\u014a\u0001"+
		"\u0000\u0000\u0000\u014d\u014b\u0001\u0000\u0000\u0000\u014d\u014c\u0001"+
		"\u0000\u0000\u0000\u014e\u014f\u0001\u0000\u0000\u0000\u014f\u0150\u0003"+
		"<\u001e\u0000\u0150\u0003\u0001\u0000\u0000\u0000\u0151\u0154\u0003\u0006"+
		"\u0003\u0000\u0152\u0154\u0003\b\u0004\u0000\u0153\u0151\u0001\u0000\u0000"+
		"\u0000\u0153\u0152\u0001\u0000\u0000\u0000\u0154\u0157\u0001\u0000\u0000"+
		"\u0000\u0155\u0153\u0001\u0000\u0000\u0000\u0155\u0156\u0001\u0000\u0000"+
		"\u0000\u0156\u0005\u0001\u0000\u0000\u0000\u0157\u0155\u0001\u0000\u0000"+
		"\u0000\u0158\u0159\u0005\u001e\u0000\u0000\u0159\u015a\u0003\u013c\u009e"+
		"\u0000\u015a\u0007\u0001\u0000\u0000\u0000\u015b\u015c\u0005\u001f\u0000"+
		"\u0000\u015c\u015d\u0005\u008f\u0000\u0000\u015d\u015e\u0003\u013c\u009e"+
		"\u0000\u015e\t\u0001\u0000\u0000\u0000\u015f\u0163\u0003\u000e\u0007\u0000"+
		"\u0160\u0162\u0003\u001c\u000e\u0000\u0161\u0160\u0001\u0000\u0000\u0000"+
		"\u0162\u0165\u0001\u0000\u0000\u0000\u0163\u0161\u0001\u0000\u0000\u0000"+
		"\u0163\u0164\u0001\u0000\u0000\u0000\u0164\u0166\u0001\u0000\u0000\u0000"+
		"\u0165\u0163\u0001\u0000\u0000\u0000\u0166\u0167\u0003$\u0012\u0000\u0167"+
		"\u0168\u0003&\u0013\u0000\u0168\u000b\u0001\u0000\u0000\u0000\u0169\u016a"+
		"\u0003\u000e\u0007\u0000\u016a\u016b\u0003$\u0012\u0000\u016b\u016c\u0003"+
		"&\u0013\u0000\u016c\u016d\u0003<\u001e\u0000\u016d\r\u0001\u0000\u0000"+
		"\u0000\u016e\u0170\u0005 \u0000\u0000\u016f\u0171\u0007\u0000\u0000\u0000"+
		"\u0170\u016f\u0001\u0000\u0000\u0000\u0170\u0171\u0001\u0000\u0000\u0000"+
		"\u0171\u0178\u0001\u0000\u0000\u0000\u0172\u0174\u0003\u0010\b\u0000\u0173"+
		"\u0172\u0001\u0000\u0000\u0000\u0174\u0175\u0001\u0000\u0000\u0000\u0175"+
		"\u0173\u0001\u0000\u0000\u0000\u0175\u0176\u0001\u0000\u0000\u0000\u0176"+
		"\u0179\u0001\u0000\u0000\u0000\u0177\u0179\u0005\u0001\u0000\u0000\u0178"+
		"\u0173\u0001\u0000\u0000\u0000\u0178\u0177\u0001\u0000\u0000\u0000\u0179"+
		"\u000f\u0001\u0000\u0000\u0000\u017a\u017d\u0003\u00eew\u0000\u017b\u017d"+
		"\u0003\u0012\t\u0000\u017c\u017a\u0001\u0000\u0000\u0000\u017c\u017b\u0001"+
		"\u0000\u0000\u0000\u017d\u0011\u0001\u0000\u0000\u0000\u017e\u017f\u0005"+
		"\u0002\u0000\u0000\u017f\u0180\u0003\u0014\n\u0000\u0180\u0181\u0005\u0003"+
		"\u0000\u0000\u0181\u0013\u0001\u0000\u0000\u0000\u0182\u0183\u0003\u00f2"+
		"y\u0000\u0183\u0184\u0005#\u0000\u0000\u0184\u0185\u0003\u00eew\u0000"+
		"\u0185\u0015\u0001\u0000\u0000\u0000\u0186\u019e\u0005$\u0000\u0000\u0187"+
		"\u018b\u0003\u009aM\u0000\u0188\u018a\u0003\u001c\u000e\u0000\u0189\u0188"+
		"\u0001\u0000\u0000\u0000\u018a\u018d\u0001\u0000\u0000\u0000\u018b\u0189"+
		"\u0001\u0000\u0000\u0000\u018b\u018c\u0001\u0000\u0000\u0000\u018c\u018e"+
		"\u0001\u0000\u0000\u0000\u018d\u018b\u0001\u0000\u0000\u0000\u018e\u018f"+
		"\u0003$\u0012\u0000\u018f\u0190\u0003&\u0013\u0000\u0190\u019f\u0001\u0000"+
		"\u0000\u0000\u0191\u0193\u0003\u001c\u000e\u0000\u0192\u0191\u0001\u0000"+
		"\u0000\u0000\u0193\u0196\u0001\u0000\u0000\u0000\u0194\u0192\u0001\u0000"+
		"\u0000\u0000\u0194\u0195\u0001\u0000\u0000\u0000\u0195\u0197\u0001\u0000"+
		"\u0000\u0000\u0196\u0194\u0001\u0000\u0000\u0000\u0197\u0198\u0005%\u0000"+
		"\u0000\u0198\u019a\u0005\u0004\u0000\u0000\u0199\u019b\u0003l6\u0000\u019a"+
		"\u0199\u0001\u0000\u0000\u0000\u019a\u019b\u0001\u0000\u0000\u0000\u019b"+
		"\u019c\u0001\u0000\u0000\u0000\u019c\u019d\u0005\u0005\u0000\u0000\u019d"+
		"\u019f\u0003&\u0013\u0000\u019e\u0187\u0001\u0000\u0000\u0000\u019e\u0194"+
		"\u0001\u0000\u0000\u0000\u019f\u0017\u0001\u0000\u0000\u0000\u01a0\u01a7"+
		"\u0005&\u0000\u0000\u01a1\u01a3\u0003\u00ecv\u0000\u01a2\u01a1\u0001\u0000"+
		"\u0000\u0000\u01a3\u01a4\u0001\u0000\u0000\u0000\u01a4\u01a2\u0001\u0000"+
		"\u0000\u0000\u01a4\u01a5\u0001\u0000\u0000\u0000\u01a5\u01a8\u0001\u0000"+
		"\u0000\u0000\u01a6\u01a8\u0005\u0001\u0000\u0000\u01a7\u01a2\u0001\u0000"+
		"\u0000\u0000\u01a7\u01a6\u0001\u0000\u0000\u0000\u01a8\u01ac\u0001\u0000"+
		"\u0000\u0000\u01a9\u01ab\u0003\u001c\u000e\u0000\u01aa\u01a9\u0001\u0000"+
		"\u0000\u0000\u01ab\u01ae\u0001\u0000\u0000\u0000\u01ac\u01aa\u0001\u0000"+
		"\u0000\u0000\u01ac\u01ad\u0001\u0000\u0000\u0000\u01ad\u01b0\u0001\u0000"+
		"\u0000\u0000\u01ae\u01ac\u0001\u0000\u0000\u0000\u01af\u01b1\u0003$\u0012"+
		"\u0000\u01b0\u01af\u0001\u0000\u0000\u0000\u01b0\u01b1\u0001\u0000\u0000"+
		"\u0000\u01b1\u01b2\u0001\u0000\u0000\u0000\u01b2\u01b3\u0003&\u0013\u0000"+
		"\u01b3\u0019\u0001\u0000\u0000\u0000\u01b4\u01b8\u0005\'\u0000\u0000\u01b5"+
		"\u01b7\u0003\u001c\u000e\u0000\u01b6\u01b5\u0001\u0000\u0000\u0000\u01b7"+
		"\u01ba\u0001\u0000\u0000\u0000\u01b8\u01b6\u0001\u0000\u0000\u0000\u01b8"+
		"\u01b9\u0001\u0000\u0000\u0000\u01b9\u01bb\u0001\u0000\u0000\u0000\u01ba"+
		"\u01b8\u0001\u0000\u0000\u0000\u01bb\u01bc\u0003$\u0012\u0000\u01bc\u01bd"+
		"\u0003&\u0013\u0000\u01bd\u001b\u0001\u0000\u0000\u0000\u01be\u01c1\u0005"+
		"(\u0000\u0000\u01bf\u01c2\u0003\u001e\u000f\u0000\u01c0\u01c2\u0003 \u0010"+
		"\u0000\u01c1\u01bf\u0001\u0000\u0000\u0000\u01c1\u01c0\u0001\u0000\u0000"+
		"\u0000\u01c2\u001d\u0001\u0000\u0000\u0000\u01c3\u01c4\u0003\"\u0011\u0000"+
		"\u01c4\u001f\u0001\u0000\u0000\u0000\u01c5\u01c6\u0005)\u0000\u0000\u01c6"+
		"\u01c7\u0003\"\u0011\u0000\u01c7!\u0001\u0000\u0000\u0000\u01c8\u01c9"+
		"\u0003\u0136\u009b\u0000\u01c9#\u0001\u0000\u0000\u0000\u01ca\u01cc\u0005"+
		"%\u0000\u0000\u01cb\u01ca\u0001\u0000\u0000\u0000\u01cb\u01cc\u0001\u0000"+
		"\u0000\u0000\u01cc\u01cd\u0001\u0000\u0000\u0000\u01cd\u01ce\u0003n7\u0000"+
		"\u01ce%\u0001\u0000\u0000\u0000\u01cf\u01d1\u0003(\u0014\u0000\u01d0\u01cf"+
		"\u0001\u0000\u0000\u0000\u01d0\u01d1\u0001\u0000\u0000\u0000\u01d1\u01d3"+
		"\u0001\u0000\u0000\u0000\u01d2\u01d4\u0003,\u0016\u0000\u01d3\u01d2\u0001"+
		"\u0000\u0000\u0000\u01d3\u01d4\u0001\u0000\u0000\u0000\u01d4\u01d6\u0001"+
		"\u0000\u0000\u0000\u01d5\u01d7\u00030\u0018\u0000\u01d6\u01d5\u0001\u0000"+
		"\u0000\u0000\u01d6\u01d7\u0001\u0000\u0000\u0000\u01d7\u01d9\u0001\u0000"+
		"\u0000\u0000\u01d8\u01da\u00034\u001a\u0000\u01d9\u01d8\u0001\u0000\u0000"+
		"\u0000\u01d9\u01da\u0001\u0000\u0000\u0000\u01da\'\u0001\u0000\u0000\u0000"+
		"\u01db\u01dd\u0005*\u0000\u0000\u01dc\u01de\u0003*\u0015\u0000\u01dd\u01dc"+
		"\u0001\u0000\u0000\u0000\u01de\u01df\u0001\u0000\u0000\u0000\u01df\u01dd"+
		"\u0001\u0000\u0000\u0000\u01df\u01e0\u0001\u0000\u0000\u0000\u01e0)\u0001"+
		"\u0000\u0000\u0000\u01e1\u01ed\u0003\u0116\u008b\u0000\u01e2\u01ed\u0003"+
		"\u0094J\u0000\u01e3\u01e4\u0005\u0002\u0000\u0000\u01e4\u01e7\u0003\u00f2"+
		"y\u0000\u01e5\u01e6\u0005#\u0000\u0000\u01e6\u01e8\u0003\u00eew\u0000"+
		"\u01e7\u01e5\u0001\u0000\u0000\u0000\u01e7\u01e8\u0001\u0000\u0000\u0000"+
		"\u01e8\u01e9\u0001\u0000\u0000\u0000\u01e9\u01ea\u0005\u0003\u0000\u0000"+
		"\u01ea\u01ed\u0001\u0000\u0000\u0000\u01eb\u01ed\u0003\u00eew\u0000\u01ec"+
		"\u01e1\u0001\u0000\u0000\u0000\u01ec\u01e2\u0001\u0000\u0000\u0000\u01ec"+
		"\u01e3\u0001\u0000\u0000\u0000\u01ec\u01eb\u0001\u0000\u0000\u0000\u01ed"+
		"+\u0001\u0000\u0000\u0000\u01ee\u01f0\u0005,\u0000\u0000\u01ef\u01f1\u0003"+
		".\u0017\u0000\u01f0\u01ef\u0001\u0000\u0000\u0000\u01f1\u01f2\u0001\u0000"+
		"\u0000\u0000\u01f2\u01f0\u0001\u0000\u0000\u0000\u01f2\u01f3\u0001\u0000"+
		"\u0000\u0000\u01f3-\u0001\u0000\u0000\u0000\u01f4\u01f5\u0003\u0092I\u0000"+
		"\u01f5/\u0001\u0000\u0000\u0000\u01f6\u01f9\u0005-\u0000\u0000\u01f7\u01f9"+
		"\u0005.\u0000\u0000\u01f8\u01f6\u0001\u0000\u0000\u0000\u01f8\u01f7\u0001"+
		"\u0000\u0000\u0000\u01f9\u01fb\u0001\u0000\u0000\u0000\u01fa\u01fc\u0003"+
		"2\u0019\u0000\u01fb\u01fa\u0001\u0000\u0000\u0000\u01fc\u01fd\u0001\u0000"+
		"\u0000\u0000\u01fd\u01fb\u0001\u0000\u0000\u0000\u01fd\u01fe\u0001\u0000"+
		"\u0000\u0000\u01fe1\u0001\u0000\u0000\u0000\u01ff\u0200\u0007\u0001\u0000"+
		"\u0000\u0200\u0206\u0003\u0114\u008a\u0000\u0201\u0204\u0003\u0092I\u0000"+
		"\u0202\u0204\u0003\u00eew\u0000\u0203\u0201\u0001\u0000\u0000\u0000\u0203"+
		"\u0202\u0001\u0000\u0000\u0000\u0204\u0206\u0001\u0000\u0000\u0000\u0205"+
		"\u01ff\u0001\u0000\u0000\u0000\u0205\u0203\u0001\u0000\u0000\u0000\u0206"+
		"3\u0001\u0000\u0000\u0000\u0207\u0209\u00036\u001b\u0000\u0208\u020a\u0003"+
		"8\u001c\u0000\u0209\u0208\u0001\u0000\u0000\u0000\u0209\u020a\u0001\u0000"+
		"\u0000\u0000\u020a\u020c\u0001\u0000\u0000\u0000\u020b\u020d\u0003:\u001d"+
		"\u0000\u020c\u020b\u0001\u0000\u0000\u0000\u020c\u020d\u0001\u0000\u0000"+
		"\u0000\u020d\u0232\u0001\u0000\u0000\u0000\u020e\u0210\u00036\u001b\u0000"+
		"\u020f\u0211\u0003:\u001d\u0000\u0210\u020f\u0001\u0000\u0000\u0000\u0210"+
		"\u0211\u0001\u0000\u0000\u0000\u0211\u0213\u0001\u0000\u0000\u0000\u0212"+
		"\u0214\u00038\u001c\u0000\u0213\u0212\u0001\u0000\u0000\u0000\u0213\u0214"+
		"\u0001\u0000\u0000\u0000\u0214\u0232\u0001\u0000\u0000\u0000\u0215\u0217"+
		"\u00038\u001c\u0000\u0216\u0218\u00036\u001b\u0000\u0217\u0216\u0001\u0000"+
		"\u0000\u0000\u0217\u0218\u0001\u0000\u0000\u0000\u0218\u021a\u0001\u0000"+
		"\u0000\u0000\u0219\u021b\u0003:\u001d\u0000\u021a\u0219\u0001\u0000\u0000"+
		"\u0000\u021a\u021b\u0001\u0000\u0000\u0000\u021b\u0232\u0001\u0000\u0000"+
		"\u0000\u021c\u021e\u00038\u001c\u0000\u021d\u021f\u0003:\u001d\u0000\u021e"+
		"\u021d\u0001\u0000\u0000\u0000\u021e\u021f\u0001\u0000\u0000\u0000\u021f"+
		"\u0221\u0001\u0000\u0000\u0000\u0220\u0222\u00036\u001b\u0000\u0221\u0220"+
		"\u0001\u0000\u0000\u0000\u0221\u0222\u0001\u0000\u0000\u0000\u0222\u0232"+
		"\u0001\u0000\u0000\u0000\u0223\u0225\u0003:\u001d\u0000\u0224\u0226\u0003"+
		"8\u001c\u0000\u0225\u0224\u0001\u0000\u0000\u0000\u0225\u0226\u0001\u0000"+
		"\u0000\u0000\u0226\u0228\u0001\u0000\u0000\u0000\u0227\u0229\u00036\u001b"+
		"\u0000\u0228\u0227\u0001\u0000\u0000\u0000\u0228\u0229\u0001\u0000\u0000"+
		"\u0000\u0229\u0232\u0001\u0000\u0000\u0000\u022a\u022c\u0003:\u001d\u0000"+
		"\u022b\u022d\u00036\u001b\u0000\u022c\u022b\u0001\u0000\u0000\u0000\u022c"+
		"\u022d\u0001\u0000\u0000\u0000\u022d\u022f\u0001\u0000\u0000\u0000\u022e"+
		"\u0230\u00038\u001c\u0000\u022f\u022e\u0001\u0000\u0000\u0000\u022f\u0230"+
		"\u0001\u0000\u0000\u0000\u0230\u0232\u0001\u0000\u0000\u0000\u0231\u0207"+
		"\u0001\u0000\u0000\u0000\u0231\u020e\u0001\u0000\u0000\u0000\u0231\u0215"+
		"\u0001\u0000\u0000\u0000\u0231\u021c\u0001\u0000\u0000\u0000\u0231\u0223"+
		"\u0001\u0000\u0000\u0000\u0231\u022a\u0001\u0000\u0000\u0000\u02325\u0001"+
		"\u0000\u0000\u0000\u0233\u0234\u00051\u0000\u0000\u0234\u0235\u0003\u00d8"+
		"l\u0000\u02357\u0001\u0000\u0000\u0000\u0236\u0237\u00052\u0000\u0000"+
		"\u0237\u0238\u0003\u00d8l\u0000\u02389\u0001\u0000\u0000\u0000\u0239\u023a"+
		"\u00053\u0000\u0000\u023a\u023b\u0003\u00d8l\u0000\u023b;\u0001\u0000"+
		"\u0000\u0000\u023c\u023d\u00054\u0000\u0000\u023d\u023f\u0003\u0082A\u0000"+
		"\u023e\u023c\u0001\u0000\u0000\u0000\u023e\u023f\u0001\u0000\u0000\u0000"+
		"\u023f=\u0001\u0000\u0000\u0000\u0240\u0246\u0003\u0004\u0002\u0000\u0241"+
		"\u0244\u0003@ \u0000\u0242\u0243\u0005\u0006\u0000\u0000\u0243\u0245\u0003"+
		">\u001f\u0000\u0244\u0242\u0001\u0000\u0000\u0000\u0244\u0245\u0001\u0000"+
		"\u0000\u0000\u0245\u0247\u0001\u0000\u0000\u0000\u0246\u0241\u0001\u0000"+
		"\u0000\u0000\u0246\u0247\u0001\u0000\u0000\u0000\u0247?\u0001\u0000\u0000"+
		"\u0000\u0248\u0254\u0003B!\u0000\u0249\u0254\u0003D\"\u0000\u024a\u0254"+
		"\u0003F#\u0000\u024b\u0254\u0003J%\u0000\u024c\u0254\u0003L&\u0000\u024d"+
		"\u0254\u0003N\'\u0000\u024e\u0254\u0003H$\u0000\u024f\u0254\u0003P(\u0000"+
		"\u0250\u0254\u0003R)\u0000\u0251\u0254\u0003T*\u0000\u0252\u0254\u0003"+
		"V+\u0000\u0253\u0248\u0001\u0000\u0000\u0000\u0253\u0249\u0001\u0000\u0000"+
		"\u0000\u0253\u024a\u0001\u0000\u0000\u0000\u0253\u024b\u0001\u0000\u0000"+
		"\u0000\u0253\u024c\u0001\u0000\u0000\u0000\u0253\u024d\u0001\u0000\u0000"+
		"\u0000\u0253\u024e\u0001\u0000\u0000\u0000\u0253\u024f\u0001\u0000\u0000"+
		"\u0000\u0253\u0250\u0001\u0000\u0000\u0000\u0253\u0251\u0001\u0000\u0000"+
		"\u0000\u0253\u0252\u0001\u0000\u0000\u0000\u0254A\u0001\u0000\u0000\u0000"+
		"\u0255\u0257\u00055\u0000\u0000\u0256\u0258\u00056\u0000\u0000\u0257\u0256"+
		"\u0001\u0000\u0000\u0000\u0257\u0258\u0001\u0000\u0000\u0000\u0258\u0259"+
		"\u0001\u0000\u0000\u0000\u0259\u025c\u0003\u0136\u009b\u0000\u025a\u025b"+
		"\u00057\u0000\u0000\u025b\u025d\u0003`0\u0000\u025c\u025a\u0001\u0000"+
		"\u0000\u0000\u025c\u025d\u0001\u0000\u0000\u0000\u025dC\u0001\u0000\u0000"+
		"\u0000\u025e\u0260\u00058\u0000\u0000\u025f\u0261\u00056\u0000\u0000\u0260"+
		"\u025f\u0001\u0000\u0000\u0000\u0260\u0261\u0001\u0000\u0000\u0000\u0261"+
		"\u0262\u0001\u0000\u0000\u0000\u0262\u0263\u0003b1\u0000\u0263E\u0001"+
		"\u0000\u0000\u0000\u0264\u0266\u00059\u0000\u0000\u0265\u0267\u00056\u0000"+
		"\u0000\u0266\u0265\u0001\u0000\u0000\u0000\u0266\u0267\u0001\u0000\u0000"+
		"\u0000\u0267\u0268\u0001\u0000\u0000\u0000\u0268\u0269\u0003b1\u0000\u0269"+
		"G\u0001\u0000\u0000\u0000\u026a\u026c\u0005:\u0000\u0000\u026b\u026d\u0005"+
		"6\u0000\u0000\u026c\u026b\u0001\u0000\u0000\u0000\u026c\u026d\u0001\u0000"+
		"\u0000\u0000\u026d\u026e\u0001\u0000\u0000\u0000\u026e\u026f\u0003`0\u0000"+
		"\u026fI\u0001\u0000\u0000\u0000\u0270\u0272\u0005;\u0000\u0000\u0271\u0273"+
		"\u00056\u0000\u0000\u0272\u0271\u0001\u0000\u0000\u0000\u0272\u0273\u0001"+
		"\u0000\u0000\u0000\u0273\u0274\u0001\u0000\u0000\u0000\u0274\u0275\u0003"+
		"^/\u0000\u0275\u0276\u0005<\u0000\u0000\u0276\u0277\u0003^/\u0000\u0277"+
		"K\u0001\u0000\u0000\u0000\u0278\u027a\u0005>\u0000\u0000\u0279\u027b\u0005"+
		"6\u0000\u0000\u027a\u0279\u0001\u0000\u0000\u0000\u027a\u027b\u0001\u0000"+
		"\u0000\u0000\u027b\u027c\u0001\u0000\u0000\u0000\u027c\u027d\u0003^/\u0000"+
		"\u027d\u027e\u0005<\u0000\u0000\u027e\u027f\u0003^/\u0000\u027fM\u0001"+
		"\u0000\u0000\u0000\u0280\u0282\u0005?\u0000\u0000\u0281\u0283\u00056\u0000"+
		"\u0000\u0282\u0281\u0001\u0000\u0000\u0000\u0282\u0283\u0001\u0000\u0000"+
		"\u0000\u0283\u0284\u0001\u0000\u0000\u0000\u0284\u0285\u0003^/\u0000\u0285"+
		"\u0286\u0005<\u0000\u0000\u0286\u0287\u0003^/\u0000\u0287O\u0001\u0000"+
		"\u0000\u0000\u0288\u0289\u0005@\u0000\u0000\u0289\u028a\u0005=\u0000\u0000"+
		"\u028a\u028b\u0003f3\u0000\u028bQ\u0001\u0000\u0000\u0000\u028c\u028d"+
		"\u0005A\u0000\u0000\u028d\u028e\u0005=\u0000\u0000\u028e\u028f\u0003f"+
		"3\u0000\u028fS\u0001\u0000\u0000\u0000\u0290\u0291\u0005A\u0000\u0000"+
		"\u0291\u0292\u0005%\u0000\u0000\u0292\u0293\u0003d2\u0000\u0293U\u0001"+
		"\u0000\u0000\u0000\u0294\u0295\u0005B\u0000\u0000\u0295\u0297\u0003\u0136"+
		"\u009b\u0000\u0296\u0294\u0001\u0000\u0000\u0000\u0296\u0297\u0001\u0000"+
		"\u0000\u0000\u0297\u029d\u0001\u0000\u0000\u0000\u0298\u029a\u0003X,\u0000"+
		"\u0299\u029b\u0003Z-\u0000\u029a\u0299\u0001\u0000\u0000\u0000\u029a\u029b"+
		"\u0001\u0000\u0000\u0000\u029b\u029e\u0001\u0000\u0000\u0000\u029c\u029e"+
		"\u0003Z-\u0000\u029d\u0298\u0001\u0000\u0000\u0000\u029d\u029c\u0001\u0000"+
		"\u0000\u0000\u029e\u02a2\u0001\u0000\u0000\u0000\u029f\u02a1\u0003\\."+
		"\u0000\u02a0\u029f\u0001\u0000\u0000\u0000\u02a1\u02a4\u0001\u0000\u0000"+
		"\u0000\u02a2\u02a0\u0001\u0000\u0000\u0000\u02a2\u02a3\u0001\u0000\u0000"+
		"\u0000\u02a3\u02a5\u0001\u0000\u0000\u0000\u02a4\u02a2\u0001\u0000\u0000"+
		"\u0000\u02a5\u02a6\u0005%\u0000\u0000\u02a6\u02a7\u0003n7\u0000\u02a7"+
		"W\u0001\u0000\u0000\u0000\u02a8\u02a9\u0005A\u0000\u0000\u02a9\u02aa\u0003"+
		"d2\u0000\u02aaY\u0001\u0000\u0000\u0000\u02ab\u02ac\u0005@\u0000\u0000"+
		"\u02ac\u02ad\u0003d2\u0000\u02ad[\u0001\u0000\u0000\u0000\u02ae\u02b2"+
		"\u0005C\u0000\u0000\u02af\u02b3\u0003\u0136\u009b\u0000\u02b0\u02b1\u0005"+
		")\u0000\u0000\u02b1\u02b3\u0003\u0136\u009b\u0000\u02b2\u02af\u0001\u0000"+
		"\u0000\u0000\u02b2\u02b0\u0001\u0000\u0000\u0000\u02b3]\u0001\u0000\u0000"+
		"\u0000\u02b4\u02ba\u0005D\u0000\u0000\u02b5\u02b7\u0005E\u0000\u0000\u02b6"+
		"\u02b5\u0001\u0000\u0000\u0000\u02b6\u02b7\u0001\u0000\u0000\u0000\u02b7"+
		"\u02b8\u0001\u0000\u0000\u0000\u02b8\u02ba\u0003\u0136\u009b\u0000\u02b9"+
		"\u02b4\u0001\u0000\u0000\u0000\u02b9\u02b6\u0001\u0000\u0000\u0000\u02ba"+
		"_\u0001\u0000\u0000\u0000\u02bb\u02bc\u0005E\u0000\u0000\u02bc\u02bd\u0003"+
		"\u0136\u009b\u0000\u02bda\u0001\u0000\u0000\u0000\u02be\u02c3\u0003`0"+
		"\u0000\u02bf\u02c3\u0005D\u0000\u0000\u02c0\u02c3\u0005)\u0000\u0000\u02c1"+
		"\u02c3\u0005F\u0000\u0000\u02c2\u02be\u0001\u0000\u0000\u0000\u02c2\u02bf"+
		"\u0001\u0000\u0000\u0000\u02c2\u02c0\u0001\u0000\u0000\u0000\u02c2\u02c1"+
		"\u0001\u0000\u0000\u0000\u02c3c\u0001\u0000\u0000\u0000\u02c4\u02c5\u0005"+
		"\u0004\u0000\u0000\u02c5\u02c6\u0003h4\u0000\u02c6\u02c7\u0005\u0005\u0000"+
		"\u0000\u02c7e\u0001\u0000\u0000\u0000\u02c8\u02c9\u0005\u0004\u0000\u0000"+
		"\u02c9\u02ca\u0003h4\u0000\u02ca\u02cb\u0005\u0005\u0000\u0000\u02cbg"+
		"\u0001\u0000\u0000\u0000\u02cc\u02ce\u0003l6\u0000\u02cd\u02cc\u0001\u0000"+
		"\u0000\u0000\u02cd\u02ce\u0001\u0000\u0000\u0000\u02ce\u02d8\u0001\u0000"+
		"\u0000\u0000\u02cf\u02d1\u0003j5\u0000\u02d0\u02d2\u0005\u0007\u0000\u0000"+
		"\u02d1\u02d0\u0001\u0000\u0000\u0000\u02d1\u02d2\u0001\u0000\u0000\u0000"+
		"\u02d2\u02d4\u0001\u0000\u0000\u0000\u02d3\u02d5\u0003l6\u0000\u02d4\u02d3"+
		"\u0001\u0000\u0000\u0000\u02d4\u02d5\u0001\u0000\u0000\u0000\u02d5\u02d7"+
		"\u0001\u0000\u0000\u0000\u02d6\u02cf\u0001\u0000\u0000\u0000\u02d7\u02da"+
		"\u0001\u0000\u0000\u0000\u02d8\u02d6\u0001\u0000\u0000\u0000\u02d8\u02d9"+
		"\u0001\u0000\u0000\u0000\u02d9i\u0001\u0000\u0000\u0000\u02da\u02d8\u0001"+
		"\u0000\u0000\u0000\u02db\u02dc\u0005E\u0000\u0000\u02dc\u02dd\u0003\u00ec"+
		"v\u0000\u02dd\u02df\u0005\u0004\u0000\u0000\u02de\u02e0\u0003l6\u0000"+
		"\u02df\u02de\u0001\u0000\u0000\u0000\u02df\u02e0\u0001\u0000\u0000\u0000"+
		"\u02e0\u02e1\u0001\u0000\u0000\u0000\u02e1\u02e2\u0005\u0005\u0000\u0000"+
		"\u02e2k\u0001\u0000\u0000\u0000\u02e3\u02e8\u0003\u009eO\u0000\u02e4\u02e6"+
		"\u0005\u0007\u0000\u0000\u02e5\u02e7\u0003l6\u0000\u02e6\u02e5\u0001\u0000"+
		"\u0000\u0000\u02e6\u02e7\u0001\u0000\u0000\u0000\u02e7\u02e9\u0001\u0000"+
		"\u0000\u0000\u02e8\u02e4\u0001\u0000\u0000\u0000\u02e8\u02e9\u0001\u0000"+
		"\u0000\u0000\u02e9m\u0001\u0000\u0000\u0000\u02ea\u02ed\u0005\u0004\u0000"+
		"\u0000\u02eb\u02ee\u0003\f\u0006\u0000\u02ec\u02ee\u0003p8\u0000\u02ed"+
		"\u02eb\u0001\u0000\u0000\u0000\u02ed\u02ec\u0001\u0000\u0000\u0000\u02ee"+
		"\u02ef\u0001\u0000\u0000\u0000\u02ef\u02f0\u0005\u0005\u0000\u0000\u02f0"+
		"o\u0001\u0000\u0000\u0000\u02f1\u02f3\u0003t:\u0000\u02f2\u02f1\u0001"+
		"\u0000\u0000\u0000\u02f2\u02f3\u0001\u0000\u0000\u0000\u02f3\u02f7\u0001"+
		"\u0000\u0000\u0000\u02f4\u02f6\u0003r9\u0000\u02f5\u02f4\u0001\u0000\u0000"+
		"\u0000\u02f6\u02f9\u0001\u0000\u0000\u0000\u02f7\u02f5\u0001\u0000\u0000"+
		"\u0000\u02f7\u02f8\u0001\u0000\u0000\u0000\u02f8q\u0001\u0000\u0000\u0000"+
		"\u02f9\u02f7\u0001\u0000\u0000\u0000\u02fa\u02fc\u0003v;\u0000\u02fb\u02fd"+
		"\u0005\u0007\u0000\u0000\u02fc\u02fb\u0001\u0000\u0000\u0000\u02fc\u02fd"+
		"\u0001\u0000\u0000\u0000\u02fd\u02ff\u0001\u0000\u0000\u0000\u02fe\u0300"+
		"\u0003t:\u0000\u02ff\u02fe\u0001\u0000\u0000\u0000\u02ff\u0300\u0001\u0000"+
		"\u0000\u0000\u0300s\u0001\u0000\u0000\u0000\u0301\u0306\u0003\u00aaU\u0000"+
		"\u0302\u0304\u0005\u0007\u0000\u0000\u0303\u0305\u0003t:\u0000\u0304\u0303"+
		"\u0001\u0000\u0000\u0000\u0304\u0305\u0001\u0000\u0000\u0000\u0305\u0307"+
		"\u0001\u0000\u0000\u0000\u0306\u0302\u0001\u0000\u0000\u0000\u0306\u0307"+
		"\u0001\u0000\u0000\u0000\u0307u\u0001\u0000\u0000\u0000\u0308\u0311\u0003"+
		"\u008eG\u0000\u0309\u0311\u0003x<\u0000\u030a\u0311\u0003\u008cF\u0000"+
		"\u030b\u0311\u0003z=\u0000\u030c\u0311\u0003|>\u0000\u030d\u0311\u0003"+
		"\u0090H\u0000\u030e\u0311\u0003~?\u0000\u030f\u0311\u0003\u0080@\u0000"+
		"\u0310\u0308\u0001\u0000\u0000\u0000\u0310\u0309\u0001\u0000\u0000\u0000"+
		"\u0310\u030a\u0001\u0000\u0000\u0000\u0310\u030b\u0001\u0000\u0000\u0000"+
		"\u0310\u030c\u0001\u0000\u0000\u0000\u0310\u030d\u0001\u0000\u0000\u0000"+
		"\u0310\u030e\u0001\u0000\u0000\u0000\u0310\u030f\u0001\u0000\u0000\u0000"+
		"\u0311w\u0001\u0000\u0000\u0000\u0312\u0313\u0005G\u0000\u0000\u0313\u0314"+
		"\u0003n7\u0000\u0314y\u0001\u0000\u0000\u0000\u0315\u0316\u0005E\u0000"+
		"\u0000\u0316\u0317\u0003\u00ecv\u0000\u0317\u0318\u0003n7\u0000\u0318"+
		"{\u0001\u0000\u0000\u0000\u0319\u031b\u0005H\u0000\u0000\u031a\u031c\u0005"+
		"6\u0000\u0000\u031b\u031a\u0001\u0000\u0000\u0000\u031b\u031c\u0001\u0000"+
		"\u0000\u0000\u031c\u031d\u0001\u0000\u0000\u0000\u031d\u031e\u0003\u00ec"+
		"v\u0000\u031e\u031f\u0003n7\u0000\u031f}\u0001\u0000\u0000\u0000\u0320"+
		"\u0321\u0005I\u0000\u0000\u0321\u0322\u0005\u0002\u0000\u0000\u0322\u0323"+
		"\u0003\u00f2y\u0000\u0323\u0324\u0005#\u0000\u0000\u0324\u0325\u0003\u00ee"+
		"w\u0000\u0325\u0326\u0005\u0003\u0000\u0000\u0326\u007f\u0001\u0000\u0000"+
		"\u0000\u0327\u0328\u00054\u0000\u0000\u0328\u0329\u0003\u0082A\u0000\u0329"+
		"\u0081\u0001\u0000\u0000\u0000\u032a\u032d\u0003\u0084B\u0000\u032b\u032d"+
		"\u0003\u0086C\u0000\u032c\u032a\u0001\u0000\u0000\u0000\u032c\u032b\u0001"+
		"\u0000\u0000\u0000\u032d\u0083\u0001\u0000\u0000\u0000\u032e\u032f\u0003"+
		"\u00eew\u0000\u032f\u0333\u0005\u0004\u0000\u0000\u0330\u0332\u0003\u008a"+
		"E\u0000\u0331\u0330\u0001\u0000\u0000\u0000\u0332\u0335\u0001\u0000\u0000"+
		"\u0000\u0333\u0331\u0001\u0000\u0000\u0000\u0333\u0334\u0001\u0000\u0000"+
		"\u0000\u0334\u0336\u0001\u0000\u0000\u0000\u0335\u0333\u0001\u0000\u0000"+
		"\u0000\u0336\u0337\u0005\u0005\u0000\u0000\u0337\u0085\u0001\u0000\u0000"+
		"\u0000\u0338\u0342\u0005\u00a5\u0000\u0000\u0339\u033d\u0005\u0002\u0000"+
		"\u0000\u033a\u033c\u0003\u00eew\u0000\u033b\u033a\u0001\u0000\u0000\u0000"+
		"\u033c\u033f\u0001\u0000\u0000\u0000\u033d\u033b\u0001\u0000\u0000\u0000"+
		"\u033d\u033e\u0001\u0000\u0000\u0000\u033e\u0340\u0001\u0000\u0000\u0000"+
		"\u033f\u033d\u0001\u0000\u0000\u0000\u0340\u0342\u0005\u0003\u0000\u0000"+
		"\u0341\u0338\u0001\u0000\u0000\u0000\u0341\u0339\u0001\u0000\u0000\u0000"+
		"\u0342\u0343\u0001\u0000\u0000\u0000\u0343\u0347\u0005\u0004\u0000\u0000"+
		"\u0344\u0346\u0003\u0088D\u0000\u0345\u0344\u0001\u0000\u0000\u0000\u0346"+
		"\u0349\u0001\u0000\u0000\u0000\u0347\u0345\u0001\u0000\u0000\u0000\u0347"+
		"\u0348\u0001\u0000\u0000\u0000\u0348\u034a\u0001\u0000\u0000\u0000\u0349"+
		"\u0347\u0001\u0000\u0000\u0000\u034a\u034b\u0005\u0005\u0000\u0000\u034b"+
		"\u0087\u0001\u0000\u0000\u0000\u034c\u0350\u0005\u0002\u0000\u0000\u034d"+
		"\u034f\u0003\u008aE\u0000\u034e\u034d\u0001\u0000\u0000\u0000\u034f\u0352"+
		"\u0001\u0000\u0000\u0000\u0350\u034e\u0001\u0000\u0000\u0000\u0350\u0351"+
		"\u0001\u0000\u0000\u0000\u0351\u0353\u0001\u0000\u0000\u0000\u0352\u0350"+
		"\u0001\u0000\u0000\u0000\u0353\u0356\u0005\u0003\u0000\u0000\u0354\u0356"+
		"\u0005\u00a5\u0000\u0000\u0355\u034c\u0001\u0000\u0000\u0000\u0355\u0354"+
		"\u0001\u0000\u0000\u0000\u0356\u0089\u0001\u0000\u0000\u0000\u0357\u035d"+
		"\u0003\u0136\u009b\u0000\u0358\u035d\u0003\u0128\u0094\u0000\u0359\u035d"+
		"\u0003\u012a\u0095\u0000\u035a\u035d\u0003\u0132\u0099\u0000\u035b\u035d"+
		"\u0005J\u0000\u0000\u035c\u0357\u0001\u0000\u0000\u0000\u035c\u0358\u0001"+
		"\u0000\u0000\u0000\u035c\u0359\u0001\u0000\u0000\u0000\u035c\u035a\u0001"+
		"\u0000\u0000\u0000\u035c\u035b\u0001\u0000\u0000\u0000\u035d\u008b\u0001"+
		"\u0000\u0000\u0000\u035e\u035f\u0005K\u0000\u0000\u035f\u0360\u0003n7"+
		"\u0000\u0360\u008d\u0001\u0000\u0000\u0000\u0361\u0366\u0003n7\u0000\u0362"+
		"\u0363\u0005L\u0000\u0000\u0363\u0365\u0003n7\u0000\u0364\u0362\u0001"+
		"\u0000\u0000\u0000\u0365\u0368\u0001\u0000\u0000\u0000\u0366\u0364\u0001"+
		"\u0000\u0000\u0000\u0366\u0367\u0001\u0000\u0000\u0000\u0367\u008f\u0001"+
		"\u0000\u0000\u0000\u0368\u0366\u0001\u0000\u0000\u0000\u0369\u036a\u0005"+
		"M\u0000\u0000\u036a\u036b\u0003\u0092I\u0000\u036b\u0091\u0001\u0000\u0000"+
		"\u0000\u036c\u0370\u0003\u0114\u008a\u0000\u036d\u0370\u0003\u0116\u008b"+
		"\u0000\u036e\u0370\u0003\u0094J\u0000\u036f\u036c\u0001\u0000\u0000\u0000"+
		"\u036f\u036d\u0001\u0000\u0000\u0000\u036f\u036e\u0001\u0000\u0000\u0000"+
		"\u0370\u0093\u0001\u0000\u0000\u0000\u0371\u0372\u0003\u0136\u009b\u0000"+
		"\u0372\u0373\u0003\u0096K\u0000\u0373\u0095\u0001\u0000\u0000\u0000\u0374"+
		"\u0384\u0005\u00a5\u0000\u0000\u0375\u0377\u0005\u0002\u0000\u0000\u0376"+
		"\u0378\u0005!\u0000\u0000\u0377\u0376\u0001\u0000\u0000\u0000\u0377\u0378"+
		"\u0001\u0000\u0000\u0000\u0378\u0379\u0001\u0000\u0000\u0000\u0379\u037e"+
		"\u0003\u00f2y\u0000\u037a\u037b\u0005\b\u0000\u0000\u037b\u037d\u0003"+
		"\u00f2y\u0000\u037c\u037a\u0001\u0000\u0000\u0000\u037d\u0380\u0001\u0000"+
		"\u0000\u0000\u037e\u037c\u0001\u0000\u0000\u0000\u037e\u037f\u0001\u0000"+
		"\u0000\u0000\u037f\u0381\u0001\u0000\u0000\u0000\u0380\u037e\u0001\u0000"+
		"\u0000\u0000\u0381\u0382\u0005\u0003\u0000\u0000\u0382\u0384\u0001\u0000"+
		"\u0000\u0000\u0383\u0374\u0001\u0000\u0000\u0000\u0383\u0375\u0001\u0000"+
		"\u0000\u0000\u0384\u0097\u0001\u0000\u0000\u0000\u0385\u0392\u0005\u00a5"+
		"\u0000\u0000\u0386\u0387\u0005\u0002\u0000\u0000\u0387\u038c\u0003\u00f2"+
		"y\u0000\u0388\u0389\u0005\b\u0000\u0000\u0389\u038b\u0003\u00f2y\u0000"+
		"\u038a\u0388\u0001\u0000\u0000\u0000\u038b\u038e\u0001\u0000\u0000\u0000"+
		"\u038c\u038a\u0001\u0000\u0000\u0000\u038c\u038d\u0001\u0000\u0000\u0000"+
		"\u038d\u038f\u0001\u0000\u0000\u0000\u038e\u038c\u0001\u0000\u0000\u0000"+
		"\u038f\u0390\u0005\u0003\u0000\u0000\u0390\u0392\u0001\u0000\u0000\u0000"+
		"\u0391\u0385\u0001\u0000\u0000\u0000\u0391\u0386\u0001\u0000\u0000\u0000"+
		"\u0392\u0099\u0001\u0000\u0000\u0000\u0393\u0395\u0005\u0004\u0000\u0000"+
		"\u0394\u0396\u0003\u009cN\u0000\u0395\u0394\u0001\u0000\u0000\u0000\u0395"+
		"\u0396\u0001\u0000\u0000\u0000\u0396\u0397\u0001\u0000\u0000\u0000\u0397"+
		"\u0398\u0005\u0005\u0000\u0000\u0398\u009b\u0001\u0000\u0000\u0000\u0399"+
		"\u039e\u0003\u009eO\u0000\u039a\u039c\u0005\u0007\u0000\u0000\u039b\u039d"+
		"\u0003\u009cN\u0000\u039c\u039b\u0001\u0000\u0000\u0000\u039c\u039d\u0001"+
		"\u0000\u0000\u0000\u039d\u039f\u0001\u0000\u0000\u0000\u039e\u039a\u0001"+
		"\u0000\u0000\u0000\u039e\u039f\u0001\u0000\u0000\u0000\u039f\u009d\u0001"+
		"\u0000\u0000\u0000\u03a0\u03a1\u0003\u00eau\u0000\u03a1\u03a2\u0003\u00a2"+
		"Q\u0000\u03a2\u03a7\u0001\u0000\u0000\u0000\u03a3\u03a4\u0003\u00dam\u0000"+
		"\u03a4\u03a5\u0003\u00a0P\u0000\u03a5\u03a7\u0001\u0000\u0000\u0000\u03a6"+
		"\u03a0\u0001\u0000\u0000\u0000\u03a6\u03a3\u0001\u0000\u0000\u0000\u03a7"+
		"\u009f\u0001\u0000\u0000\u0000\u03a8\u03aa\u0003\u00a2Q\u0000\u03a9\u03a8"+
		"\u0001\u0000\u0000\u0000\u03a9\u03aa\u0001\u0000\u0000\u0000\u03aa\u00a1"+
		"\u0001\u0000\u0000\u0000\u03ab\u03ac\u0003\u00a4R\u0000\u03ac\u03b5\u0003"+
		"\u00a6S\u0000\u03ad\u03b1\u0005\u0006\u0000\u0000\u03ae\u03af\u0003\u00a4"+
		"R\u0000\u03af\u03b0\u0003\u00a6S\u0000\u03b0\u03b2\u0001\u0000\u0000\u0000"+
		"\u03b1\u03ae\u0001\u0000\u0000\u0000\u03b1\u03b2\u0001\u0000\u0000\u0000"+
		"\u03b2\u03b4\u0001\u0000\u0000\u0000\u03b3\u03ad\u0001\u0000\u0000\u0000"+
		"\u03b4\u03b7\u0001\u0000\u0000\u0000\u03b5\u03b3\u0001\u0000\u0000\u0000"+
		"\u03b5\u03b6\u0001\u0000\u0000\u0000\u03b6\u00a3\u0001\u0000\u0000\u0000"+
		"\u03b7\u03b5\u0001\u0000\u0000\u0000\u03b8\u03bb\u0003\u00ecv\u0000\u03b9"+
		"\u03bb\u0005\t\u0000\u0000\u03ba\u03b8\u0001\u0000\u0000\u0000\u03ba\u03b9"+
		"\u0001\u0000\u0000\u0000\u03bb\u00a5\u0001\u0000\u0000\u0000\u03bc\u03c1"+
		"\u0003\u00a8T\u0000\u03bd\u03be\u0005\b\u0000\u0000\u03be\u03c0\u0003"+
		"\u00a8T\u0000\u03bf\u03bd\u0001\u0000\u0000\u0000\u03c0\u03c3\u0001\u0000"+
		"\u0000\u0000\u03c1\u03bf\u0001\u0000\u0000\u0000\u03c1\u03c2\u0001\u0000"+
		"\u0000\u0000\u03c2\u00a7\u0001\u0000\u0000\u0000\u03c3\u03c1\u0001\u0000"+
		"\u0000\u0000\u03c4\u03c5\u0003\u00e6s\u0000\u03c5\u00a9\u0001\u0000\u0000"+
		"\u0000\u03c6\u03c7\u0003\u00eau\u0000\u03c7\u03c8\u0003\u00aeW\u0000\u03c8"+
		"\u03cd\u0001\u0000\u0000\u0000\u03c9\u03ca\u0003\u00deo\u0000\u03ca\u03cb"+
		"\u0003\u00acV\u0000\u03cb\u03cd\u0001\u0000\u0000\u0000\u03cc\u03c6\u0001"+
		"\u0000\u0000\u0000\u03cc\u03c9\u0001\u0000\u0000\u0000\u03cd\u00ab\u0001"+
		"\u0000\u0000\u0000\u03ce\u03d0\u0003\u00aeW\u0000\u03cf\u03ce\u0001\u0000"+
		"\u0000\u0000\u03cf\u03d0\u0001\u0000\u0000\u0000\u03d0\u00ad\u0001\u0000"+
		"\u0000\u0000\u03d1\u03d8\u0003\u00b6[\u0000\u03d2\u03d4\u0005\u0006\u0000"+
		"\u0000\u03d3\u03d5\u0003\u00b4Z\u0000\u03d4\u03d3\u0001\u0000\u0000\u0000"+
		"\u03d4\u03d5\u0001\u0000\u0000\u0000\u03d5\u03d7\u0001\u0000\u0000\u0000"+
		"\u03d6\u03d2\u0001\u0000\u0000\u0000\u03d7\u03da\u0001\u0000\u0000\u0000"+
		"\u03d8\u03d6\u0001\u0000\u0000\u0000\u03d8\u03d9\u0001\u0000\u0000\u0000"+
		"\u03d9\u00af\u0001\u0000\u0000\u0000\u03da\u03d8\u0001\u0000\u0000\u0000"+
		"\u03db\u03dc\u0003\u00be_\u0000\u03dc\u00b1\u0001\u0000\u0000\u0000\u03dd"+
		"\u03de\u0003\u00eew\u0000\u03de\u00b3\u0001\u0000\u0000\u0000\u03df\u03e0"+
		"\u0003\u00b8\\\u0000\u03e0\u03e1\u0003\u00a6S\u0000\u03e1\u00b5\u0001"+
		"\u0000\u0000\u0000\u03e2\u03e3\u0003\u00b8\\\u0000\u03e3\u03e4\u0003\u00ba"+
		"]\u0000\u03e4\u00b7\u0001\u0000\u0000\u0000\u03e5\u03e8\u0003\u00b0X\u0000"+
		"\u03e6\u03e8\u0003\u00b2Y\u0000\u03e7\u03e5\u0001\u0000\u0000\u0000\u03e7"+
		"\u03e6\u0001\u0000\u0000\u0000\u03e8\u00b9\u0001\u0000\u0000\u0000\u03e9"+
		"\u03ee\u0003\u00bc^\u0000\u03ea\u03eb\u0005\b\u0000\u0000\u03eb\u03ed"+
		"\u0003\u00bc^\u0000\u03ec\u03ea\u0001\u0000\u0000\u0000\u03ed\u03f0\u0001"+
		"\u0000\u0000\u0000\u03ee\u03ec\u0001\u0000\u0000\u0000\u03ee\u03ef\u0001"+
		"\u0000\u0000\u0000\u03ef\u00bb\u0001\u0000\u0000\u0000\u03f0\u03ee\u0001"+
		"\u0000\u0000\u0000\u03f1\u03f2\u0003\u00e8t\u0000\u03f2\u00bd\u0001\u0000"+
		"\u0000\u0000\u03f3\u03f4\u0003\u00c0`\u0000\u03f4\u00bf\u0001\u0000\u0000"+
		"\u0000\u03f5\u03fa\u0003\u00c2a\u0000\u03f6\u03f7\u0005\n\u0000\u0000"+
		"\u03f7\u03f9\u0003\u00c2a\u0000\u03f8\u03f6\u0001\u0000\u0000\u0000\u03f9"+
		"\u03fc\u0001\u0000\u0000\u0000\u03fa\u03f8\u0001\u0000\u0000\u0000\u03fa"+
		"\u03fb\u0001\u0000\u0000\u0000\u03fb\u00c1\u0001\u0000\u0000\u0000\u03fc"+
		"\u03fa\u0001\u0000\u0000\u0000\u03fd\u0402\u0003\u00c6c\u0000\u03fe\u03ff"+
		"\u0005\u000b\u0000\u0000\u03ff\u0401\u0003\u00c6c\u0000\u0400\u03fe\u0001"+
		"\u0000\u0000\u0000\u0401\u0404\u0001\u0000\u0000\u0000\u0402\u0400\u0001"+
		"\u0000\u0000\u0000\u0402\u0403\u0001\u0000\u0000\u0000\u0403\u00c3\u0001"+
		"\u0000\u0000\u0000\u0404\u0402\u0001\u0000\u0000\u0000\u0405\u0407\u0003"+
		"\u00d2i\u0000\u0406\u0408\u0003\u00c8d\u0000\u0407\u0406\u0001\u0000\u0000"+
		"\u0000\u0407\u0408\u0001\u0000\u0000\u0000\u0408\u00c5\u0001\u0000\u0000"+
		"\u0000\u0409\u040d\u0003\u00c4b\u0000\u040a\u040b\u0005\f\u0000\u0000"+
		"\u040b\u040d\u0003\u00c4b\u0000\u040c\u0409\u0001\u0000\u0000\u0000\u040c"+
		"\u040a\u0001\u0000\u0000\u0000\u040d\u00c7\u0001\u0000\u0000\u0000\u040e"+
		"\u0416\u0005\r\u0000\u0000\u040f\u0416\u0005\u0001\u0000\u0000\u0410\u0416"+
		"\u0005\u000e\u0000\u0000\u0411\u0412\u0005\u0004\u0000\u0000\u0412\u0413"+
		"\u0003\u00cae\u0000\u0413\u0414\u0005\u0005\u0000\u0000\u0414\u0416\u0001"+
		"\u0000\u0000\u0000\u0415\u040e\u0001\u0000\u0000\u0000\u0415\u040f\u0001"+
		"\u0000\u0000\u0000\u0415\u0410\u0001\u0000\u0000\u0000\u0415\u0411\u0001"+
		"\u0000\u0000\u0000\u0416\u00c9\u0001\u0000\u0000\u0000\u0417\u0418\u0003"+
		"\u00ccf\u0000\u0418\u0419\u0005\b\u0000\u0000\u0419\u041a\u0003\u00ce"+
		"g\u0000\u041a\u0422\u0001\u0000\u0000\u0000\u041b\u0422\u0003\u00d0h\u0000"+
		"\u041c\u041d\u0003\u00ccf\u0000\u041d\u041e\u0005\b\u0000\u0000\u041e"+
		"\u0422\u0001\u0000\u0000\u0000\u041f\u0420\u0005\b\u0000\u0000\u0420\u0422"+
		"\u0003\u00ceg\u0000\u0421\u0417\u0001\u0000\u0000\u0000\u0421\u041b\u0001"+
		"\u0000\u0000\u0000\u0421\u041c\u0001\u0000\u0000\u0000\u0421\u041f\u0001"+
		"\u0000\u0000\u0000\u0422\u00cb\u0001\u0000\u0000\u0000\u0423\u0424\u0003"+
		"\u00d8l\u0000\u0424\u00cd\u0001\u0000\u0000\u0000\u0425\u0426\u0003\u00d8"+
		"l\u0000\u0426\u00cf\u0001\u0000\u0000\u0000\u0427\u0428\u0003\u00d8l\u0000"+
		"\u0428\u00d1\u0001\u0000\u0000\u0000\u0429\u0432\u0003\u0136\u009b\u0000"+
		"\u042a\u0432\u0005\t\u0000\u0000\u042b\u042c\u0005\u000f\u0000\u0000\u042c"+
		"\u0432\u0003\u00d4j\u0000\u042d\u042e\u0005\u0002\u0000\u0000\u042e\u042f"+
		"\u0003\u00be_\u0000\u042f\u0430\u0005\u0003\u0000\u0000\u0430\u0432\u0001"+
		"\u0000\u0000\u0000\u0431\u0429\u0001\u0000\u0000\u0000\u0431\u042a\u0001"+
		"\u0000\u0000\u0000\u0431\u042b\u0001\u0000\u0000\u0000\u0431\u042d\u0001"+
		"\u0000\u0000\u0000\u0432\u00d3\u0001\u0000\u0000\u0000\u0433\u0441\u0003"+
		"\u00d6k\u0000\u0434\u043d\u0005\u0002\u0000\u0000\u0435\u043a\u0003\u00d6"+
		"k\u0000\u0436\u0437\u0005\n\u0000\u0000\u0437\u0439\u0003\u00d6k\u0000"+
		"\u0438\u0436\u0001\u0000\u0000\u0000\u0439\u043c\u0001\u0000\u0000\u0000"+
		"\u043a\u0438\u0001\u0000\u0000\u0000\u043a\u043b\u0001\u0000\u0000\u0000"+
		"\u043b\u043e\u0001\u0000\u0000\u0000\u043c\u043a\u0001\u0000\u0000\u0000"+
		"\u043d\u0435\u0001\u0000\u0000\u0000\u043d\u043e\u0001\u0000\u0000\u0000"+
		"\u043e\u043f\u0001\u0000\u0000\u0000\u043f\u0441\u0005\u0003\u0000\u0000"+
		"\u0440\u0433\u0001\u0000\u0000\u0000\u0440\u0434\u0001\u0000\u0000\u0000"+
		"\u0441\u00d5\u0001\u0000\u0000\u0000\u0442\u044a\u0003\u0136\u009b\u0000"+
		"\u0443\u044a\u0005\t\u0000\u0000\u0444\u0447\u0005\f\u0000\u0000\u0445"+
		"\u0448\u0003\u0136\u009b\u0000\u0446\u0448\u0005\t\u0000\u0000\u0447\u0445"+
		"\u0001\u0000\u0000\u0000\u0447\u0446\u0001\u0000\u0000\u0000\u0448\u044a"+
		"\u0001\u0000\u0000\u0000\u0449\u0442\u0001\u0000\u0000\u0000\u0449\u0443"+
		"\u0001\u0000\u0000\u0000\u0449\u0444\u0001\u0000\u0000\u0000\u044a\u00d7"+
		"\u0001\u0000\u0000\u0000\u044b\u044c\u0005\u0096\u0000\u0000\u044c\u00d9"+
		"\u0001\u0000\u0000\u0000\u044d\u0450\u0003\u00e2q\u0000\u044e\u0450\u0003"+
		"\u00dcn\u0000\u044f\u044d\u0001\u0000\u0000\u0000\u044f\u044e\u0001\u0000"+
		"\u0000\u0000\u0450\u00db\u0001\u0000\u0000\u0000\u0451\u0452\u0005\u0010"+
		"\u0000\u0000\u0452\u0453\u0003\u00a2Q\u0000\u0453\u0454\u0005\u0011\u0000"+
		"\u0000\u0454\u00dd\u0001\u0000\u0000\u0000\u0455\u0458\u0003\u00e4r\u0000"+
		"\u0456\u0458\u0003\u00e0p\u0000\u0457\u0455\u0001\u0000\u0000\u0000\u0457"+
		"\u0456\u0001\u0000\u0000\u0000\u0458\u00df\u0001\u0000\u0000\u0000\u0459"+
		"\u045a\u0005\u0010\u0000\u0000\u045a\u045b\u0003\u00aeW\u0000\u045b\u045c"+
		"\u0005\u0011\u0000\u0000\u045c\u00e1\u0001\u0000\u0000\u0000\u045d\u045f"+
		"\u0005\u0002\u0000\u0000\u045e\u0460\u0003\u00e6s\u0000\u045f\u045e\u0001"+
		"\u0000\u0000\u0000\u0460\u0461\u0001\u0000\u0000\u0000\u0461\u045f\u0001"+
		"\u0000\u0000\u0000\u0461\u0462\u0001\u0000\u0000\u0000\u0462\u0463\u0001"+
		"\u0000\u0000\u0000\u0463\u0464\u0005\u0003\u0000\u0000\u0464\u00e3\u0001"+
		"\u0000\u0000\u0000\u0465\u0467\u0005\u0002\u0000\u0000\u0466\u0468\u0003"+
		"\u00e8t\u0000\u0467\u0466\u0001\u0000\u0000\u0000\u0468\u0469\u0001\u0000"+
		"\u0000\u0000\u0469\u0467\u0001\u0000\u0000\u0000\u0469\u046a\u0001\u0000"+
		"\u0000\u0000\u046a\u046b\u0001\u0000\u0000\u0000\u046b\u046c\u0005\u0003"+
		"\u0000\u0000\u046c\u00e5\u0001\u0000\u0000\u0000\u046d\u0470\u0003\u00ea"+
		"u\u0000\u046e\u0470\u0003\u00dam\u0000\u046f\u046d\u0001\u0000\u0000\u0000"+
		"\u046f\u046e\u0001\u0000\u0000\u0000\u0470\u00e7\u0001\u0000\u0000\u0000"+
		"\u0471\u0474\u0003\u00eau\u0000\u0472\u0474\u0003\u00deo\u0000\u0473\u0471"+
		"\u0001\u0000\u0000\u0000\u0473\u0472\u0001\u0000\u0000\u0000\u0474\u00e9"+
		"\u0001\u0000\u0000\u0000\u0475\u0478\u0003\u00eew\u0000\u0476\u0478\u0003"+
		"\u00f0x\u0000\u0477\u0475\u0001\u0000\u0000\u0000\u0477\u0476\u0001\u0000"+
		"\u0000\u0000\u0478\u00eb\u0001\u0000\u0000\u0000\u0479\u047c\u0003\u00ee"+
		"w\u0000\u047a\u047c\u0003\u0136\u009b\u0000\u047b\u0479\u0001\u0000\u0000"+
		"\u0000\u047b\u047a\u0001\u0000\u0000\u0000\u047c\u00ed\u0001\u0000\u0000"+
		"\u0000\u047d\u047e\u0007\u0002\u0000\u0000\u047e\u00ef\u0001\u0000\u0000"+
		"\u0000\u047f\u0486\u0003\u0136\u009b\u0000\u0480\u0486\u0003\u0128\u0094"+
		"\u0000\u0481\u0486\u0003\u012a\u0095\u0000\u0482\u0486\u0003\u0132\u0099"+
		"\u0000\u0483\u0486\u0003\u013a\u009d\u0000\u0484\u0486\u0005\u00a5\u0000"+
		"\u0000\u0485\u047f\u0001\u0000\u0000\u0000\u0485\u0480\u0001\u0000\u0000"+
		"\u0000\u0485\u0481\u0001\u0000\u0000\u0000\u0485\u0482\u0001\u0000\u0000"+
		"\u0000\u0485\u0483\u0001\u0000\u0000\u0000\u0485\u0484\u0001\u0000\u0000"+
		"\u0000\u0486\u00f1\u0001\u0000\u0000\u0000\u0487\u0488\u0003\u00f4z\u0000"+
		"\u0488\u00f3\u0001\u0000\u0000\u0000\u0489\u048e\u0003\u00f6{\u0000\u048a"+
		"\u048b\u0005\u0012\u0000\u0000\u048b\u048d\u0003\u00f6{\u0000\u048c\u048a"+
		"\u0001\u0000\u0000\u0000\u048d\u0490\u0001\u0000\u0000\u0000\u048e\u048c"+
		"\u0001\u0000\u0000\u0000\u048e\u048f\u0001\u0000\u0000\u0000\u048f\u00f5"+
		"\u0001\u0000\u0000\u0000\u0490\u048e\u0001\u0000\u0000\u0000\u0491\u0496"+
		"\u0003\u00f8|\u0000\u0492\u0493\u0005\u0013\u0000\u0000\u0493\u0495\u0003"+
		"\u00f8|\u0000\u0494\u0492\u0001\u0000\u0000\u0000\u0495\u0498\u0001\u0000"+
		"\u0000\u0000\u0496\u0494\u0001\u0000\u0000\u0000\u0496\u0497\u0001\u0000"+
		"\u0000\u0000\u0497\u00f7\u0001\u0000\u0000\u0000\u0498\u0496\u0001\u0000"+
		"\u0000\u0000\u0499\u049a\u0003\u00fa}\u0000\u049a\u00f9\u0001\u0000\u0000"+
		"\u0000\u049b\u04ad\u0003\u00fc~\u0000\u049c\u049d\u0005\u0014\u0000\u0000"+
		"\u049d\u04ae\u0003\u00fc~\u0000\u049e\u049f\u0005\u0015\u0000\u0000\u049f"+
		"\u04ae\u0003\u00fc~\u0000\u04a0\u04a1\u0005\u0016\u0000\u0000\u04a1\u04ae"+
		"\u0003\u00fc~\u0000\u04a2\u04a3\u0005\u0017\u0000\u0000\u04a3\u04ae\u0003"+
		"\u00fc~\u0000\u04a4\u04a5\u0005\u0018\u0000\u0000\u04a5\u04ae\u0003\u00fc"+
		"~\u0000\u04a6\u04a7\u0005\u0019\u0000\u0000\u04a7\u04ae\u0003\u00fc~\u0000"+
		"\u04a8\u04a9\u0005O\u0000\u0000\u04a9\u04ae\u0003\u0098L\u0000\u04aa\u04ab"+
		"\u0005N\u0000\u0000\u04ab\u04ac\u0005O\u0000\u0000\u04ac\u04ae\u0003\u0098"+
		"L\u0000\u04ad\u049c\u0001\u0000\u0000\u0000\u04ad\u049e\u0001\u0000\u0000"+
		"\u0000\u04ad\u04a0\u0001\u0000\u0000\u0000\u04ad\u04a2\u0001\u0000\u0000"+
		"\u0000\u04ad\u04a4\u0001\u0000\u0000\u0000\u04ad\u04a6\u0001\u0000\u0000"+
		"\u0000\u04ad\u04a8\u0001\u0000\u0000\u0000\u04ad\u04aa\u0001\u0000\u0000"+
		"\u0000\u04ad\u04ae\u0001\u0000\u0000\u0000\u04ae\u00fb\u0001\u0000\u0000"+
		"\u0000\u04af\u04b0\u0003\u00fe\u007f\u0000\u04b0\u00fd\u0001\u0000\u0000"+
		"\u0000\u04b1\u04b5\u0003\u0108\u0084\u0000\u04b2\u04b4\u0003\u0100\u0080"+
		"\u0000\u04b3\u04b2\u0001\u0000\u0000\u0000\u04b4\u04b7\u0001\u0000\u0000"+
		"\u0000\u04b5\u04b3\u0001\u0000\u0000\u0000\u04b5\u04b6\u0001\u0000\u0000"+
		"\u0000\u04b6\u00ff\u0001\u0000\u0000\u0000\u04b7\u04b5\u0001\u0000\u0000"+
		"\u0000\u04b8\u04b9\u0005\r\u0000\u0000\u04b9\u04be\u0003\u0102\u0081\u0000"+
		"\u04ba\u04bb\u0005\u001a\u0000\u0000\u04bb\u04be\u0003\u0104\u0082\u0000"+
		"\u04bc\u04be\u0003\u0106\u0083\u0000\u04bd\u04b8\u0001\u0000\u0000\u0000"+
		"\u04bd\u04ba\u0001\u0000\u0000\u0000\u04bd\u04bc\u0001\u0000\u0000\u0000"+
		"\u04be\u0101\u0001\u0000\u0000\u0000\u04bf\u04c0\u0003\u0108\u0084\u0000"+
		"\u04c0\u0103\u0001\u0000\u0000\u0000\u04c1\u04c2\u0003\u0108\u0084\u0000"+
		"\u04c2\u0105\u0001\u0000\u0000\u0000\u04c3\u04c6\u0003\u012e\u0097\u0000"+
		"\u04c4\u04c6\u0003\u0130\u0098\u0000\u04c5\u04c3\u0001\u0000\u0000\u0000"+
		"\u04c5\u04c4\u0001\u0000\u0000\u0000\u04c6\u04ca\u0001\u0000\u0000\u0000"+
		"\u04c7\u04c9\u0003\u010a\u0085\u0000\u04c8\u04c7\u0001\u0000\u0000\u0000"+
		"\u04c9\u04cc\u0001\u0000\u0000\u0000\u04ca\u04c8\u0001\u0000\u0000\u0000"+
		"\u04ca\u04cb\u0001\u0000\u0000\u0000\u04cb\u0107\u0001\u0000\u0000\u0000"+
		"\u04cc\u04ca\u0001\u0000\u0000\u0000\u04cd\u04d1\u0003\u0110\u0088\u0000"+
		"\u04ce\u04d0\u0003\u010a\u0085\u0000\u04cf\u04ce\u0001\u0000\u0000\u0000"+
		"\u04d0\u04d3\u0001\u0000\u0000\u0000\u04d1\u04cf\u0001\u0000\u0000\u0000"+
		"\u04d1\u04d2\u0001\u0000\u0000\u0000\u04d2\u0109\u0001\u0000\u0000\u0000"+
		"\u04d3\u04d1\u0001\u0000\u0000\u0000\u04d4\u04d7\u0003\u010c\u0086\u0000"+
		"\u04d5\u04d7\u0003\u010e\u0087\u0000\u04d6\u04d4\u0001\u0000\u0000\u0000"+
		"\u04d6\u04d5\u0001\u0000\u0000\u0000\u04d7\u010b\u0001\u0000\u0000\u0000"+
		"\u04d8\u04d9\u0005\u0001\u0000\u0000\u04d9\u04da\u0003\u0110\u0088\u0000"+
		"\u04da\u010d\u0001\u0000\u0000\u0000\u04db\u04dc\u0005\u000b\u0000\u0000"+
		"\u04dc\u04dd\u0003\u0110\u0088\u0000\u04dd\u010f\u0001\u0000\u0000\u0000"+
		"\u04de\u04df\u0005\u000f\u0000\u0000\u04df\u04e6\u0003\u0112\u0089\u0000"+
		"\u04e0\u04e1\u0005\r\u0000\u0000\u04e1\u04e6\u0003\u0112\u0089\u0000\u04e2"+
		"\u04e3\u0005\u001a\u0000\u0000\u04e3\u04e6\u0003\u0112\u0089\u0000\u04e4"+
		"\u04e6\u0003\u0112\u0089\u0000\u04e5\u04de\u0001\u0000\u0000\u0000\u04e5"+
		"\u04e0\u0001\u0000\u0000\u0000\u04e5\u04e2\u0001\u0000\u0000\u0000\u04e5"+
		"\u04e4\u0001\u0000\u0000\u0000\u04e6\u0111\u0001\u0000\u0000\u0000\u04e7"+
		"\u04ef\u0003\u0114\u008a\u0000\u04e8\u04ef\u0003\u0116\u008b\u0000\u04e9"+
		"\u04ef\u0003\u0126\u0093\u0000\u04ea\u04ef\u0003\u0128\u0094\u0000\u04eb"+
		"\u04ef\u0003\u012a\u0095\u0000\u04ec\u04ef\u0003\u0132\u0099\u0000\u04ed"+
		"\u04ef\u0003\u00eew\u0000\u04ee\u04e7\u0001\u0000\u0000\u0000\u04ee\u04e8"+
		"\u0001\u0000\u0000\u0000\u04ee\u04e9\u0001\u0000\u0000\u0000\u04ee\u04ea"+
		"\u0001\u0000\u0000\u0000\u04ee\u04eb\u0001\u0000\u0000\u0000\u04ee\u04ec"+
		"\u0001\u0000\u0000\u0000\u04ee\u04ed\u0001\u0000\u0000\u0000\u04ef\u0113"+
		"\u0001\u0000\u0000\u0000\u04f0\u04f1\u0005\u0002\u0000\u0000\u04f1\u04f2"+
		"\u0003\u00f2y\u0000\u04f2\u04f3\u0005\u0003\u0000\u0000\u04f3\u0115\u0001"+
		"\u0000\u0000\u0000\u04f4\u05f3\u0003\u0124\u0092\u0000\u04f5\u04f6\u0005"+
		"P\u0000\u0000\u04f6\u04f7\u0005\u0002\u0000\u0000\u04f7\u04f8\u0003\u00f2"+
		"y\u0000\u04f8\u04f9\u0005\u0003\u0000\u0000\u04f9\u05f3\u0001\u0000\u0000"+
		"\u0000\u04fa\u05f3\u0003\u011a\u008d\u0000\u04fb\u04fc\u0005R\u0000\u0000"+
		"\u04fc\u04fd\u0005\u0002\u0000\u0000\u04fd\u04fe\u0003\u00f2y\u0000\u04fe"+
		"\u04ff\u0005\b\u0000\u0000\u04ff\u0500\u0003\u00f2y\u0000\u0500\u0501"+
		"\u0005\u0003\u0000\u0000\u0501\u05f3\u0001\u0000\u0000\u0000\u0502\u0503"+
		"\u0005S\u0000\u0000\u0503\u0504\u0005\u0002\u0000\u0000\u0504\u0505\u0003"+
		"\u00f2y\u0000\u0505\u0506\u0005\u0003\u0000\u0000\u0506\u05f3\u0001\u0000"+
		"\u0000\u0000\u0507\u0508\u0005T\u0000\u0000\u0508\u0509\u0005\u0002\u0000"+
		"\u0000\u0509\u050a\u0003\u00eew\u0000\u050a\u050b\u0005\u0003\u0000\u0000"+
		"\u050b\u05f3\u0001\u0000\u0000\u0000\u050c\u050d\u0005U\u0000\u0000\u050d"+
		"\u050e\u0005\u0002\u0000\u0000\u050e\u050f\u0003\u00f2y\u0000\u050f\u0510"+
		"\u0005\u0003\u0000\u0000\u0510\u05f3\u0001\u0000\u0000\u0000\u0511\u0512"+
		"\u0005V\u0000\u0000\u0512\u0513\u0005\u0002\u0000\u0000\u0513\u0514\u0003"+
		"\u00f2y\u0000\u0514\u0515\u0005\u0003\u0000\u0000\u0515\u05f3\u0001\u0000"+
		"\u0000\u0000\u0516\u051c\u0005W\u0000\u0000\u0517\u0518\u0005\u0002\u0000"+
		"\u0000\u0518\u0519\u0003\u00f2y\u0000\u0519\u051a\u0005\u0003\u0000\u0000"+
		"\u051a\u051d\u0001\u0000\u0000\u0000\u051b\u051d\u0005\u00a5\u0000\u0000"+
		"\u051c\u0517\u0001\u0000\u0000\u0000\u051c\u051b\u0001\u0000\u0000\u0000"+
		"\u051d\u05f3\u0001\u0000\u0000\u0000\u051e\u051f\u0005X\u0000\u0000\u051f"+
		"\u05f3\u0005\u00a5\u0000\u0000\u0520\u0521\u0005Y\u0000\u0000\u0521\u0522"+
		"\u0005\u0002\u0000\u0000\u0522\u0523\u0003\u00f2y\u0000\u0523\u0524\u0005"+
		"\u0003\u0000\u0000\u0524\u05f3\u0001\u0000\u0000\u0000\u0525\u0526\u0005"+
		"Z\u0000\u0000\u0526\u0527\u0005\u0002\u0000\u0000\u0527\u0528\u0003\u00f2"+
		"y\u0000\u0528\u0529\u0005\u0003\u0000\u0000\u0529\u05f3\u0001\u0000\u0000"+
		"\u0000\u052a\u052b\u0005[\u0000\u0000\u052b\u052c\u0005\u0002\u0000\u0000"+
		"\u052c\u052d\u0003\u00f2y\u0000\u052d\u052e\u0005\u0003\u0000\u0000\u052e"+
		"\u05f3\u0001\u0000\u0000\u0000\u052f\u0530\u0005\\\u0000\u0000\u0530\u0531"+
		"\u0005\u0002\u0000\u0000\u0531\u0532\u0003\u00f2y\u0000\u0532\u0533\u0005"+
		"\u0003\u0000\u0000\u0533\u05f3\u0001\u0000\u0000\u0000\u0534\u0535\u0005"+
		"]\u0000\u0000\u0535\u05f3\u0003\u0098L\u0000\u0536\u05f3\u0003\u011c\u008e"+
		"\u0000\u0537\u0538\u0005^\u0000\u0000\u0538\u0539\u0005\u0002\u0000\u0000"+
		"\u0539\u053a\u0003\u00f2y\u0000\u053a\u053b\u0005\u0003\u0000\u0000\u053b"+
		"\u05f3\u0001\u0000\u0000\u0000\u053c\u05f3\u0003\u011e\u008f\u0000\u053d"+
		"\u053e\u0005_\u0000\u0000\u053e\u053f\u0005\u0002\u0000\u0000\u053f\u0540"+
		"\u0003\u00f2y\u0000\u0540\u0541\u0005\u0003\u0000\u0000\u0541\u05f3\u0001"+
		"\u0000\u0000\u0000\u0542\u0543\u0005`\u0000\u0000\u0543\u0544\u0005\u0002"+
		"\u0000\u0000\u0544\u0545\u0003\u00f2y\u0000\u0545\u0546\u0005\u0003\u0000"+
		"\u0000\u0546\u05f3\u0001\u0000\u0000\u0000\u0547\u0548\u0005a\u0000\u0000"+
		"\u0548\u0549\u0005\u0002\u0000\u0000\u0549\u054a\u0003\u00f2y\u0000\u054a"+
		"\u054b\u0005\u0003\u0000\u0000\u054b\u05f3\u0001\u0000\u0000\u0000\u054c"+
		"\u054d\u0005c\u0000\u0000\u054d\u054e\u0005\u0002\u0000\u0000\u054e\u054f"+
		"\u0003\u00f2y\u0000\u054f\u0550\u0005\b\u0000\u0000\u0550\u0551\u0003"+
		"\u00f2y\u0000\u0551\u0552\u0005\u0003\u0000\u0000\u0552\u05f3\u0001\u0000"+
		"\u0000\u0000\u0553\u0554\u0005d\u0000\u0000\u0554\u0555\u0005\u0002\u0000"+
		"\u0000\u0555\u0556\u0003\u00f2y\u0000\u0556\u0557\u0005\b\u0000\u0000"+
		"\u0557\u0558\u0003\u00f2y\u0000\u0558\u0559\u0005\u0003\u0000\u0000\u0559"+
		"\u05f3\u0001\u0000\u0000\u0000\u055a\u055b\u0005e\u0000\u0000\u055b\u055c"+
		"\u0005\u0002\u0000\u0000\u055c\u055d\u0003\u00f2y\u0000\u055d\u055e\u0005"+
		"\b\u0000\u0000\u055e\u055f\u0003\u00f2y\u0000\u055f\u0560\u0005\u0003"+
		"\u0000\u0000\u0560\u05f3\u0001\u0000\u0000\u0000\u0561\u0562\u0005f\u0000"+
		"\u0000\u0562\u0563\u0005\u0002\u0000\u0000\u0563\u0564\u0003\u00f2y\u0000"+
		"\u0564\u0565\u0005\b\u0000\u0000\u0565\u0566\u0003\u00f2y\u0000\u0566"+
		"\u0567\u0005\u0003\u0000\u0000\u0567\u05f3\u0001\u0000\u0000\u0000\u0568"+
		"\u0569\u0005g\u0000\u0000\u0569\u056a\u0005\u0002\u0000\u0000\u056a\u056b"+
		"\u0003\u00f2y\u0000\u056b\u056c\u0005\b\u0000\u0000\u056c\u056d\u0003"+
		"\u00f2y\u0000\u056d\u056e\u0005\u0003\u0000\u0000\u056e\u05f3\u0001\u0000"+
		"\u0000\u0000\u056f\u0570\u0005h\u0000\u0000\u0570\u0571\u0005\u0002\u0000"+
		"\u0000\u0571\u0572\u0003\u00f2y\u0000\u0572\u0573\u0005\u0003\u0000\u0000"+
		"\u0573\u05f3\u0001\u0000\u0000\u0000\u0574\u0575\u0005i\u0000\u0000\u0575"+
		"\u0576\u0005\u0002\u0000\u0000\u0576\u0577\u0003\u00f2y\u0000\u0577\u0578"+
		"\u0005\u0003\u0000\u0000\u0578\u05f3\u0001\u0000\u0000\u0000\u0579\u057a"+
		"\u0005j\u0000\u0000\u057a\u057b\u0005\u0002\u0000\u0000\u057b\u057c\u0003"+
		"\u00f2y\u0000\u057c\u057d\u0005\u0003\u0000\u0000\u057d\u05f3\u0001\u0000"+
		"\u0000\u0000\u057e\u057f\u0005k\u0000\u0000\u057f\u0580\u0005\u0002\u0000"+
		"\u0000\u0580\u0581\u0003\u00f2y\u0000\u0581\u0582\u0005\u0003\u0000\u0000"+
		"\u0582\u05f3\u0001\u0000\u0000\u0000\u0583\u0584\u0005l\u0000\u0000\u0584"+
		"\u0585\u0005\u0002\u0000\u0000\u0585\u0586\u0003\u00f2y\u0000\u0586\u0587"+
		"\u0005\u0003\u0000\u0000\u0587\u05f3\u0001\u0000\u0000\u0000\u0588\u0589"+
		"\u0005m\u0000\u0000\u0589\u058a\u0005\u0002\u0000\u0000\u058a\u058b\u0003"+
		"\u00f2y\u0000\u058b\u058c\u0005\u0003\u0000\u0000\u058c\u05f3\u0001\u0000"+
		"\u0000\u0000\u058d\u058e\u0005n\u0000\u0000\u058e\u058f\u0005\u0002\u0000"+
		"\u0000\u058f\u0590\u0003\u00f2y\u0000\u0590\u0591\u0005\u0003\u0000\u0000"+
		"\u0591\u05f3\u0001\u0000\u0000\u0000\u0592\u0593\u0005o\u0000\u0000\u0593"+
		"\u0594\u0005\u0002\u0000\u0000\u0594\u0595\u0003\u00f2y\u0000\u0595\u0596"+
		"\u0005\u0003\u0000\u0000\u0596\u05f3\u0001\u0000\u0000\u0000\u0597\u0598"+
		"\u0005p\u0000\u0000\u0598\u05f3\u0005\u00a5\u0000\u0000\u0599\u059a\u0005"+
		"q\u0000\u0000\u059a\u05f3\u0005\u00a5\u0000\u0000\u059b\u059c\u0005r\u0000"+
		"\u0000\u059c\u05f3\u0005\u00a5\u0000\u0000\u059d\u059e\u0005w\u0000\u0000"+
		"\u059e\u059f\u0005\u0002\u0000\u0000\u059f\u05a0\u0003\u00f2y\u0000\u05a0"+
		"\u05a1\u0005\u0003\u0000\u0000\u05a1\u05f3\u0001\u0000\u0000\u0000\u05a2"+
		"\u05a3\u0005s\u0000\u0000\u05a3\u05a4\u0005\u0002\u0000\u0000\u05a4\u05a5"+
		"\u0003\u00f2y\u0000\u05a5\u05a6\u0005\u0003\u0000\u0000\u05a6\u05f3\u0001"+
		"\u0000\u0000\u0000\u05a7\u05a8\u0005t\u0000\u0000\u05a8\u05a9\u0005\u0002"+
		"\u0000\u0000\u05a9\u05aa\u0003\u00f2y\u0000\u05aa\u05ab\u0005\u0003\u0000"+
		"\u0000\u05ab\u05f3\u0001\u0000\u0000\u0000\u05ac\u05ad\u0005u\u0000\u0000"+
		"\u05ad\u05ae\u0005\u0002\u0000\u0000\u05ae\u05af\u0003\u00f2y\u0000\u05af"+
		"\u05b0\u0005\u0003\u0000\u0000\u05b0\u05f3\u0001\u0000\u0000\u0000\u05b1"+
		"\u05b2\u0005v\u0000\u0000\u05b2\u05b3\u0005\u0002\u0000\u0000\u05b3\u05b4"+
		"\u0003\u00f2y\u0000\u05b4\u05b5\u0005\u0003\u0000\u0000\u05b5\u05f3\u0001"+
		"\u0000\u0000\u0000\u05b6\u05b7\u0005x\u0000\u0000\u05b7\u05f3\u0003\u0098"+
		"L\u0000\u05b8\u05b9\u0005y\u0000\u0000\u05b9\u05ba\u0005\u0002\u0000\u0000"+
		"\u05ba\u05bb\u0003\u00f2y\u0000\u05bb\u05bc\u0005\b\u0000\u0000\u05bc"+
		"\u05bd\u0003\u00f2y\u0000\u05bd\u05be\u0005\b\u0000\u0000\u05be\u05bf"+
		"\u0003\u00f2y\u0000\u05bf\u05c0\u0005\u0003\u0000\u0000\u05c0\u05f3\u0001"+
		"\u0000\u0000\u0000\u05c1\u05c2\u0005z\u0000\u0000\u05c2\u05c3\u0005\u0002"+
		"\u0000\u0000\u05c3\u05c4\u0003\u00f2y\u0000\u05c4\u05c5\u0005\b\u0000"+
		"\u0000\u05c5\u05c6\u0003\u00f2y\u0000\u05c6\u05c7\u0005\u0003\u0000\u0000"+
		"\u05c7\u05f3\u0001\u0000\u0000\u0000\u05c8\u05c9\u0005{\u0000\u0000\u05c9"+
		"\u05ca\u0005\u0002\u0000\u0000\u05ca\u05cb\u0003\u00f2y\u0000\u05cb\u05cc"+
		"\u0005\b\u0000\u0000\u05cc\u05cd\u0003\u00f2y\u0000\u05cd\u05ce\u0005"+
		"\u0003\u0000\u0000\u05ce\u05f3\u0001\u0000\u0000\u0000\u05cf\u05d0\u0005"+
		"|\u0000\u0000\u05d0\u05d1\u0005\u0002\u0000\u0000\u05d1\u05d2\u0003\u00f2"+
		"y\u0000\u05d2\u05d3\u0005\b\u0000\u0000\u05d3\u05d4\u0003\u00f2y\u0000"+
		"\u05d4\u05d5\u0005\u0003\u0000\u0000\u05d5\u05f3\u0001\u0000\u0000\u0000"+
		"\u05d6\u05d7\u0005}\u0000\u0000\u05d7\u05d8\u0005\u0002\u0000\u0000\u05d8"+
		"\u05d9\u0003\u00f2y\u0000\u05d9\u05da\u0005\u0003\u0000\u0000\u05da\u05f3"+
		"\u0001\u0000\u0000\u0000\u05db\u05dc\u0005~\u0000\u0000\u05dc\u05dd\u0005"+
		"\u0002\u0000\u0000\u05dd\u05de\u0003\u00f2y\u0000\u05de\u05df\u0005\u0003"+
		"\u0000\u0000\u05df\u05f3\u0001\u0000\u0000\u0000\u05e0\u05e1\u0005\u007f"+
		"\u0000\u0000\u05e1\u05e2\u0005\u0002\u0000\u0000\u05e2\u05e3\u0003\u00f2"+
		"y\u0000\u05e3\u05e4\u0005\u0003\u0000\u0000\u05e4\u05f3\u0001\u0000\u0000"+
		"\u0000\u05e5\u05e6\u0005\u0080\u0000\u0000\u05e6\u05e7\u0005\u0002\u0000"+
		"\u0000\u05e7\u05e8\u0003\u00f2y\u0000\u05e8\u05e9\u0005\u0003\u0000\u0000"+
		"\u05e9\u05f3\u0001\u0000\u0000\u0000\u05ea\u05eb\u0005\u0081\u0000\u0000"+
		"\u05eb\u05ec\u0005\u0002\u0000\u0000\u05ec\u05ed\u0003\u00f2y\u0000\u05ed"+
		"\u05ee\u0005\u0003\u0000\u0000\u05ee\u05f3\u0001\u0000\u0000\u0000\u05ef"+
		"\u05f3\u0003\u0118\u008c\u0000\u05f0\u05f3\u0003\u0120\u0090\u0000\u05f1"+
		"\u05f3\u0003\u0122\u0091\u0000\u05f2\u04f4\u0001\u0000\u0000\u0000\u05f2"+
		"\u04f5\u0001\u0000\u0000\u0000\u05f2\u04fa\u0001\u0000\u0000\u0000\u05f2"+
		"\u04fb\u0001\u0000\u0000\u0000\u05f2\u0502\u0001\u0000\u0000\u0000\u05f2"+
		"\u0507\u0001\u0000\u0000\u0000\u05f2\u050c\u0001\u0000\u0000\u0000\u05f2"+
		"\u0511\u0001\u0000\u0000\u0000\u05f2\u0516\u0001\u0000\u0000\u0000\u05f2"+
		"\u051e\u0001\u0000\u0000\u0000\u05f2\u0520\u0001\u0000\u0000\u0000\u05f2"+
		"\u0525\u0001\u0000\u0000\u0000\u05f2\u052a\u0001\u0000\u0000\u0000\u05f2"+
		"\u052f\u0001\u0000\u0000\u0000\u05f2\u0534\u0001\u0000\u0000\u0000\u05f2"+
		"\u0536\u0001\u0000\u0000\u0000\u05f2\u0537\u0001\u0000\u0000\u0000\u05f2"+
		"\u053c\u0001\u0000\u0000\u0000\u05f2\u053d\u0001\u0000\u0000\u0000\u05f2"+
		"\u0542\u0001\u0000\u0000\u0000\u05f2\u0547\u0001\u0000\u0000\u0000\u05f2"+
		"\u054c\u0001\u0000\u0000\u0000\u05f2\u0553\u0001\u0000\u0000\u0000\u05f2"+
		"\u055a\u0001\u0000\u0000\u0000\u05f2\u0561\u0001\u0000\u0000\u0000\u05f2"+
		"\u0568\u0001\u0000\u0000\u0000\u05f2\u056f\u0001\u0000\u0000\u0000\u05f2"+
		"\u0574\u0001\u0000\u0000\u0000\u05f2\u0579\u0001\u0000\u0000\u0000\u05f2"+
		"\u057e\u0001\u0000\u0000\u0000\u05f2\u0583\u0001\u0000\u0000\u0000\u05f2"+
		"\u0588\u0001\u0000\u0000\u0000\u05f2\u058d\u0001\u0000\u0000\u0000\u05f2"+
		"\u0592\u0001\u0000\u0000\u0000\u05f2\u0597\u0001\u0000\u0000\u0000\u05f2"+
		"\u0599\u0001\u0000\u0000\u0000\u05f2\u059b\u0001\u0000\u0000\u0000\u05f2"+
		"\u059d\u0001\u0000\u0000\u0000\u05f2\u05a2\u0001\u0000\u0000\u0000\u05f2"+
		"\u05a7\u0001\u0000\u0000\u0000\u05f2\u05ac\u0001\u0000\u0000\u0000\u05f2"+
		"\u05b1\u0001\u0000\u0000\u0000\u05f2\u05b6\u0001\u0000\u0000\u0000\u05f2"+
		"\u05b8\u0001\u0000\u0000\u0000\u05f2\u05c1\u0001\u0000\u0000\u0000\u05f2"+
		"\u05c8\u0001\u0000\u0000\u0000\u05f2\u05cf\u0001\u0000\u0000\u0000\u05f2"+
		"\u05d6\u0001\u0000\u0000\u0000\u05f2\u05db\u0001\u0000\u0000\u0000\u05f2"+
		"\u05e0\u0001\u0000\u0000\u0000\u05f2\u05e5\u0001\u0000\u0000\u0000\u05f2"+
		"\u05ea\u0001\u0000\u0000\u0000\u05f2\u05ef\u0001\u0000\u0000\u0000\u05f2"+
		"\u05f0\u0001\u0000\u0000\u0000\u05f2\u05f1\u0001\u0000\u0000\u0000\u05f3"+
		"\u0117\u0001\u0000\u0000\u0000\u05f4\u05f5\u0005\u0082\u0000\u0000\u05f5"+
		"\u05f6\u0005\u0002\u0000\u0000\u05f6\u05f7\u0003\u00f2y\u0000\u05f7\u05f8"+
		"\u0005\b\u0000\u0000\u05f8\u05fb\u0003\u00f2y\u0000\u05f9\u05fa\u0005"+
		"\b\u0000\u0000\u05fa\u05fc\u0003\u00f2y\u0000\u05fb\u05f9\u0001\u0000"+
		"\u0000\u0000\u05fb\u05fc\u0001\u0000\u0000\u0000\u05fc\u05fd\u0001\u0000"+
		"\u0000\u0000\u05fd\u05fe\u0005\u0003\u0000\u0000\u05fe\u0119\u0001\u0000"+
		"\u0000\u0000\u05ff\u0600\u0005Q\u0000\u0000\u0600\u0601\u0005\u0002\u0000"+
		"\u0000\u0601\u0602\u0003\u00f2y\u0000\u0602\u0603\u0005\u0003\u0000\u0000"+
		"\u0603\u011b\u0001\u0000\u0000\u0000\u0604\u0605\u0005\u0083\u0000\u0000"+
		"\u0605\u0606\u0005\u0002\u0000\u0000\u0606\u0607\u0003\u00f2y\u0000\u0607"+
		"\u0608\u0005\b\u0000\u0000\u0608\u060b\u0003\u00f2y\u0000\u0609\u060a"+
		"\u0005\b\u0000\u0000\u060a\u060c\u0003\u00f2y\u0000\u060b\u0609\u0001"+
		"\u0000\u0000\u0000\u060b\u060c\u0001\u0000\u0000\u0000\u060c\u060d\u0001"+
		"\u0000\u0000\u0000\u060d\u060e\u0005\u0003\u0000\u0000\u060e\u011d\u0001"+
		"\u0000\u0000\u0000\u060f\u0610\u0005\u0084\u0000\u0000\u0610\u0611\u0005"+
		"\u0002\u0000\u0000\u0611\u0612\u0003\u00f2y\u0000\u0612\u0613\u0005\b"+
		"\u0000\u0000\u0613\u0614\u0003\u00f2y\u0000\u0614\u0615\u0005\b\u0000"+
		"\u0000\u0615\u0618\u0003\u00f2y\u0000\u0616\u0617\u0005\b\u0000\u0000"+
		"\u0617\u0619\u0003\u00f2y\u0000\u0618\u0616\u0001\u0000\u0000\u0000\u0618"+
		"\u0619\u0001\u0000\u0000\u0000\u0619\u061a\u0001\u0000\u0000\u0000\u061a"+
		"\u061b\u0005\u0003\u0000\u0000\u061b\u011f\u0001\u0000\u0000\u0000\u061c"+
		"\u061d\u0005\u0085\u0000\u0000\u061d\u061e\u0003n7\u0000\u061e\u0121\u0001"+
		"\u0000\u0000\u0000\u061f\u0620\u0005N\u0000\u0000\u0620\u0621\u0005\u0085"+
		"\u0000\u0000\u0621\u0622\u0003n7\u0000\u0622\u0123\u0001\u0000\u0000\u0000"+
		"\u0623\u0624\u0005\u0086\u0000\u0000\u0624\u0626\u0005\u0002\u0000\u0000"+
		"\u0625\u0627\u0005!\u0000\u0000\u0626\u0625\u0001\u0000\u0000\u0000\u0626"+
		"\u0627\u0001\u0000\u0000\u0000\u0627\u062a\u0001\u0000\u0000\u0000\u0628"+
		"\u062b\u0005\u0001\u0000\u0000\u0629\u062b\u0003\u00f2y\u0000\u062a\u0628"+
		"\u0001\u0000\u0000\u0000\u062a\u0629\u0001\u0000\u0000\u0000\u062b\u062c"+
		"\u0001\u0000\u0000\u0000\u062c\u066c\u0005\u0003\u0000\u0000\u062d\u062e"+
		"\u0005\u0087\u0000\u0000\u062e\u0630\u0005\u0002\u0000\u0000\u062f\u0631"+
		"\u0005!\u0000\u0000\u0630\u062f\u0001\u0000\u0000\u0000\u0630\u0631\u0001"+
		"\u0000\u0000\u0000\u0631\u0632\u0001\u0000\u0000\u0000\u0632\u0633\u0003"+
		"\u00f2y\u0000\u0633\u0634\u0005\u0003\u0000\u0000\u0634\u066c\u0001\u0000"+
		"\u0000\u0000\u0635\u0636\u0005\u0088\u0000\u0000\u0636\u0638\u0005\u0002"+
		"\u0000\u0000\u0637\u0639\u0005!\u0000\u0000\u0638\u0637\u0001\u0000\u0000"+
		"\u0000\u0638\u0639\u0001\u0000\u0000\u0000\u0639\u063a\u0001\u0000\u0000"+
		"\u0000\u063a\u063b\u0003\u00f2y\u0000\u063b\u063c\u0005\u0003\u0000\u0000"+
		"\u063c\u066c\u0001\u0000\u0000\u0000\u063d\u063e\u0005\u0089\u0000\u0000"+
		"\u063e\u0640\u0005\u0002\u0000\u0000\u063f\u0641\u0005!\u0000\u0000\u0640"+
		"\u063f\u0001\u0000\u0000\u0000\u0640\u0641\u0001\u0000\u0000\u0000\u0641"+
		"\u0642\u0001\u0000\u0000\u0000\u0642\u0643\u0003\u00f2y\u0000\u0643\u0644"+
		"\u0005\u0003\u0000\u0000\u0644\u066c\u0001\u0000\u0000\u0000\u0645\u0646"+
		"\u0005\u008a\u0000\u0000\u0646\u0648\u0005\u0002\u0000\u0000\u0647\u0649"+
		"\u0005!\u0000\u0000\u0648\u0647\u0001\u0000\u0000\u0000\u0648\u0649\u0001"+
		"\u0000\u0000\u0000\u0649\u064a\u0001\u0000\u0000\u0000\u064a\u064b\u0003"+
		"\u00f2y\u0000\u064b\u064c\u0005\u0003\u0000\u0000\u064c\u066c\u0001\u0000"+
		"\u0000\u0000\u064d\u064e\u0005\u008b\u0000\u0000\u064e\u0650\u0005\u0002"+
		"\u0000\u0000\u064f\u0651\u0005!\u0000\u0000\u0650\u064f\u0001\u0000\u0000"+
		"\u0000\u0650\u0651\u0001\u0000\u0000\u0000\u0651\u0652\u0001\u0000\u0000"+
		"\u0000\u0652\u0653\u0003\u00f2y\u0000\u0653\u0654\u0005\u0003\u0000\u0000"+
		"\u0654\u066c\u0001\u0000\u0000\u0000\u0655\u0656\u0005\u008c\u0000\u0000"+
		"\u0656\u0658\u0005\u0002\u0000\u0000\u0657\u0659\u0005!\u0000\u0000\u0658"+
		"\u0657\u0001\u0000\u0000\u0000\u0658\u0659\u0001\u0000\u0000\u0000\u0659"+
		"\u065a\u0001\u0000\u0000\u0000\u065a\u065b\u0003\u00f2y\u0000\u065b\u065c"+
		"\u0005\u0003\u0000\u0000\u065c\u066c\u0001\u0000\u0000\u0000\u065d\u065e"+
		"\u0005+\u0000\u0000\u065e\u0660\u0005\u0002\u0000\u0000\u065f\u0661\u0005"+
		"!\u0000\u0000\u0660\u065f\u0001\u0000\u0000\u0000\u0660\u0661\u0001\u0000"+
		"\u0000\u0000\u0661\u0662\u0001\u0000\u0000\u0000\u0662\u0667\u0003\u00f2"+
		"y\u0000\u0663\u0664\u0005\u0006\u0000\u0000\u0664\u0665\u0005\u008d\u0000"+
		"\u0000\u0665\u0666\u0005\u0014\u0000\u0000\u0666\u0668\u0003\u0134\u009a"+
		"\u0000\u0667\u0663\u0001\u0000\u0000\u0000\u0667\u0668\u0001\u0000\u0000"+
		"\u0000\u0668\u0669\u0001\u0000\u0000\u0000\u0669\u066a\u0005\u0003\u0000"+
		"\u0000\u066a\u066c\u0001\u0000\u0000\u0000\u066b\u0623\u0001\u0000\u0000"+
		"\u0000\u066b\u062d\u0001\u0000\u0000\u0000\u066b\u0635\u0001\u0000\u0000"+
		"\u0000\u066b\u063d\u0001\u0000\u0000\u0000\u066b\u0645\u0001\u0000\u0000"+
		"\u0000\u066b\u064d\u0001\u0000\u0000\u0000\u066b\u0655\u0001\u0000\u0000"+
		"\u0000\u066b\u065d\u0001\u0000\u0000\u0000\u066c\u0125\u0001\u0000\u0000"+
		"\u0000\u066d\u066f\u0003\u0136\u009b\u0000\u066e\u0670\u0003\u0096K\u0000"+
		"\u066f\u066e\u0001\u0000\u0000\u0000\u066f\u0670\u0001\u0000\u0000\u0000"+
		"\u0670\u0127\u0001\u0000\u0000\u0000\u0671\u0675\u0003\u0134\u009a\u0000"+
		"\u0672\u0676\u0005\u0094\u0000\u0000\u0673\u0674\u0005\u001b\u0000\u0000"+
		"\u0674\u0676\u0003\u0136\u009b\u0000\u0675\u0672\u0001\u0000\u0000\u0000"+
		"\u0675\u0673\u0001\u0000\u0000\u0000\u0675\u0676\u0001\u0000\u0000\u0000"+
		"\u0676\u0129\u0001\u0000\u0000\u0000\u0677\u067b\u0003\u012c\u0096\u0000"+
		"\u0678\u067b\u0003\u012e\u0097\u0000\u0679\u067b\u0003\u0130\u0098\u0000"+
		"\u067a\u0677\u0001\u0000\u0000\u0000\u067a\u0678\u0001\u0000\u0000\u0000"+
		"\u067a\u0679\u0001\u0000\u0000\u0000\u067b\u012b\u0001\u0000\u0000\u0000"+
		"\u067c\u067d\u0007\u0003\u0000\u0000\u067d\u012d\u0001\u0000\u0000\u0000"+
		"\u067e\u067f\u0007\u0004\u0000\u0000\u067f\u012f\u0001\u0000\u0000\u0000"+
		"\u0680\u0681\u0007\u0005\u0000\u0000\u0681\u0131\u0001\u0000\u0000\u0000"+
		"\u0682\u0683\u0007\u0006\u0000\u0000\u0683\u0133\u0001\u0000\u0000\u0000"+
		"\u0684\u0685\u0007\u0007\u0000\u0000\u0685\u0135\u0001\u0000\u0000\u0000"+
		"\u0686\u0688\u0005\u0095\u0000\u0000\u0687\u0686\u0001\u0000\u0000\u0000"+
		"\u0687\u0688\u0001\u0000\u0000\u0000\u0688\u068b\u0001\u0000\u0000\u0000"+
		"\u0689\u068c\u0003\u013c\u009e\u0000\u068a\u068c\u0003\u0138\u009c\u0000"+
		"\u068b\u0689\u0001\u0000\u0000\u0000\u068b\u068a\u0001\u0000\u0000\u0000"+
		"\u068c\u0137\u0001\u0000\u0000\u0000\u068d\u0690\u0003\u013e\u009f\u0000"+
		"\u068e\u0690\u0003\u0140\u00a0\u0000\u068f\u068d\u0001\u0000\u0000\u0000"+
		"\u068f\u068e\u0001\u0000\u0000\u0000\u0690\u0139\u0001\u0000\u0000\u0000"+
		"\u0691\u0692\u0007\b\u0000\u0000\u0692\u013b\u0001\u0000\u0000\u0000\u0693"+
		"\u0694\u0005\u008e\u0000\u0000\u0694\u013d\u0001\u0000\u0000\u0000\u0695"+
		"\u0696\u0005\u0090\u0000\u0000\u0696\u013f\u0001\u0000\u0000\u0000\u0697"+
		"\u0698\u0005\u008f\u0000\u0000\u0698\u0141\u0001\u0000\u0000\u0000\u00a5"+
		"\u0144\u014d\u0153\u0155\u0163\u0170\u0175\u0178\u017c\u018b\u0194\u019a"+
		"\u019e\u01a4\u01a7\u01ac\u01b0\u01b8\u01c1\u01cb\u01d0\u01d3\u01d6\u01d9"+
		"\u01df\u01e7\u01ec\u01f2\u01f8\u01fd\u0203\u0205\u0209\u020c\u0210\u0213"+
		"\u0217\u021a\u021e\u0221\u0225\u0228\u022c\u022f\u0231\u023e\u0244\u0246"+
		"\u0253\u0257\u025c\u0260\u0266\u026c\u0272\u027a\u0282\u0296\u029a\u029d"+
		"\u02a2\u02b2\u02b6\u02b9\u02c2\u02cd\u02d1\u02d4\u02d8\u02df\u02e6\u02e8"+
		"\u02ed\u02f2\u02f7\u02fc\u02ff\u0304\u0306\u0310\u031b\u032c\u0333\u033d"+
		"\u0341\u0347\u0350\u0355\u035c\u0366\u036f\u0377\u037e\u0383\u038c\u0391"+
		"\u0395\u039c\u039e\u03a6\u03a9\u03b1\u03b5\u03ba\u03c1\u03cc\u03cf\u03d4"+
		"\u03d8\u03e7\u03ee\u03fa\u0402\u0407\u040c\u0415\u0421\u0431\u043a\u043d"+
		"\u0440\u0447\u0449\u044f\u0457\u0461\u0469\u046f\u0473\u0477\u047b\u0485"+
		"\u048e\u0496\u04ad\u04b5\u04bd\u04c5\u04ca\u04d1\u04d6\u04e5\u04ee\u051c"+
		"\u05f2\u05fb\u060b\u0618\u0626\u062a\u0630\u0638\u0640\u0648\u0650\u0658"+
		"\u0660\u0667\u066b\u066f\u0675\u067a\u0687\u068b\u068f";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}