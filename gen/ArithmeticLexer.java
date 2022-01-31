// Generated from C:/Users/Joao/Desktop/Mestrado/ERASMUS/Winter Semester/qlever_local/qlever/third_party/antlr4/runtime/CSharp/tests/issue-3079\Arithmetic.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ArithmeticLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.9.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		VARIABLE=1, SCIENTIFIC_NUMBER=2, LPAREN=3, RPAREN=4, PLUS=5, MINUS=6, 
		TIMES=7, DIV=8, GT=9, LT=10, EQ=11, POINT=12, POW=13, SEMI=14, WS=15;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"VARIABLE", "SCIENTIFIC_NUMBER", "LPAREN", "RPAREN", "PLUS", "MINUS", 
			"TIMES", "DIV", "GT", "LT", "EQ", "POINT", "POW", "SEMI", "WS", "VALID_ID_START", 
			"VALID_ID_CHAR", "NUMBER", "UNSIGNED_INTEGER", "E", "SIGN"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, null, null, "'('", "')'", "'+'", "'-'", "'*'", "'/'", "'>'", "'<'", 
			"'='", "'.'", "'^'", "';'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "VARIABLE", "SCIENTIFIC_NUMBER", "LPAREN", "RPAREN", "PLUS", "MINUS", 
			"TIMES", "DIV", "GT", "LT", "EQ", "POINT", "POW", "SEMI", "WS"
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


	public ArithmeticLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Arithmetic.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\21y\b\1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\3\2\3\2\7\2\60\n\2\f\2\16\2\63"+
		"\13\2\3\3\3\3\3\3\5\38\n\3\3\3\3\3\5\3<\n\3\3\4\3\4\3\5\3\5\3\6\3\6\3"+
		"\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17"+
		"\3\17\3\20\6\20W\n\20\r\20\16\20X\3\20\3\20\3\21\5\21^\n\21\3\22\3\22"+
		"\5\22b\n\22\3\23\6\23e\n\23\r\23\16\23f\3\23\3\23\6\23k\n\23\r\23\16\23"+
		"l\5\23o\n\23\3\24\6\24r\n\24\r\24\16\24s\3\25\3\25\3\26\3\26\2\2\27\3"+
		"\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37"+
		"\21!\2#\2%\2\'\2)\2+\2\3\2\6\5\2\13\f\17\17\"\"\5\2C\\aac|\4\2GGgg\4\2"+
		"--//\2{\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2"+
		"\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3"+
		"\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\3-\3\2\2\2"+
		"\5\64\3\2\2\2\7=\3\2\2\2\t?\3\2\2\2\13A\3\2\2\2\rC\3\2\2\2\17E\3\2\2\2"+
		"\21G\3\2\2\2\23I\3\2\2\2\25K\3\2\2\2\27M\3\2\2\2\31O\3\2\2\2\33Q\3\2\2"+
		"\2\35S\3\2\2\2\37V\3\2\2\2!]\3\2\2\2#a\3\2\2\2%d\3\2\2\2\'q\3\2\2\2)u"+
		"\3\2\2\2+w\3\2\2\2-\61\5!\21\2.\60\5#\22\2/.\3\2\2\2\60\63\3\2\2\2\61"+
		"/\3\2\2\2\61\62\3\2\2\2\62\4\3\2\2\2\63\61\3\2\2\2\64;\5%\23\2\65\67\5"+
		")\25\2\668\5+\26\2\67\66\3\2\2\2\678\3\2\2\289\3\2\2\29:\5\'\24\2:<\3"+
		"\2\2\2;\65\3\2\2\2;<\3\2\2\2<\6\3\2\2\2=>\7*\2\2>\b\3\2\2\2?@\7+\2\2@"+
		"\n\3\2\2\2AB\7-\2\2B\f\3\2\2\2CD\7/\2\2D\16\3\2\2\2EF\7,\2\2F\20\3\2\2"+
		"\2GH\7\61\2\2H\22\3\2\2\2IJ\7@\2\2J\24\3\2\2\2KL\7>\2\2L\26\3\2\2\2MN"+
		"\7?\2\2N\30\3\2\2\2OP\7\60\2\2P\32\3\2\2\2QR\7`\2\2R\34\3\2\2\2ST\7=\2"+
		"\2T\36\3\2\2\2UW\t\2\2\2VU\3\2\2\2WX\3\2\2\2XV\3\2\2\2XY\3\2\2\2YZ\3\2"+
		"\2\2Z[\b\20\2\2[ \3\2\2\2\\^\t\3\2\2]\\\3\2\2\2^\"\3\2\2\2_b\5!\21\2`"+
		"b\4\62;\2a_\3\2\2\2a`\3\2\2\2b$\3\2\2\2ce\4\62;\2dc\3\2\2\2ef\3\2\2\2"+
		"fd\3\2\2\2fg\3\2\2\2gn\3\2\2\2hj\7\60\2\2ik\4\62;\2ji\3\2\2\2kl\3\2\2"+
		"\2lj\3\2\2\2lm\3\2\2\2mo\3\2\2\2nh\3\2\2\2no\3\2\2\2o&\3\2\2\2pr\4\62"+
		";\2qp\3\2\2\2rs\3\2\2\2sq\3\2\2\2st\3\2\2\2t(\3\2\2\2uv\t\4\2\2v*\3\2"+
		"\2\2wx\t\5\2\2x,\3\2\2\2\r\2\61\67;X]aflns\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}