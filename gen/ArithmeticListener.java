// Generated from C:/Users/Joao/Desktop/Mestrado/ERASMUS/Winter Semester/qlever_local/qlever/third_party/antlr4/runtime/CSharp/tests/issue-3079\Arithmetic.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ArithmeticParser}.
 */
public interface ArithmeticListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ArithmeticParser#file}.
	 * @param ctx the parse tree
	 */
	void enterFile(ArithmeticParser.FileContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParser#file}.
	 * @param ctx the parse tree
	 */
	void exitFile(ArithmeticParser.FileContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(ArithmeticParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(ArithmeticParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParser#atom}.
	 * @param ctx the parse tree
	 */
	void enterAtom(ArithmeticParser.AtomContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParser#atom}.
	 * @param ctx the parse tree
	 */
	void exitAtom(ArithmeticParser.AtomContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParser#scientific}.
	 * @param ctx the parse tree
	 */
	void enterScientific(ArithmeticParser.ScientificContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParser#scientific}.
	 * @param ctx the parse tree
	 */
	void exitScientific(ArithmeticParser.ScientificContext ctx);
	/**
	 * Enter a parse tree produced by {@link ArithmeticParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterVariable(ArithmeticParser.VariableContext ctx);
	/**
	 * Exit a parse tree produced by {@link ArithmeticParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitVariable(ArithmeticParser.VariableContext ctx);
}