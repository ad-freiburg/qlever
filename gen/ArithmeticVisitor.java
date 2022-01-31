// Generated from C:/Users/Joao/Desktop/Mestrado/ERASMUS/Winter Semester/qlever_local/qlever/third_party/antlr4/runtime/CSharp/tests/issue-3079\Arithmetic.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ArithmeticParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ArithmeticVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ArithmeticParser#file}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFile(ArithmeticParser.FileContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParser#expression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpression(ArithmeticParser.ExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParser#atom}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAtom(ArithmeticParser.AtomContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParser#scientific}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitScientific(ArithmeticParser.ScientificContext ctx);
	/**
	 * Visit a parse tree produced by {@link ArithmeticParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable(ArithmeticParser.VariableContext ctx);
}