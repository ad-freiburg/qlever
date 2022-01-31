// Generated from C:/Users/Joao/Desktop/Mestrado/ERASMUS/Winter Semester/qlever_local/qlever/third_party/antlr4/runtime/Swift/Tests/Antlr4Tests\Threading.g4 by ANTLR 4.9.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ThreadingParser}.
 */
public interface ThreadingListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ThreadingParser#s}.
	 * @param ctx the parse tree
	 */
	void enterS(ThreadingParser.SContext ctx);
	/**
	 * Exit a parse tree produced by {@link ThreadingParser#s}.
	 * @param ctx the parse tree
	 */
	void exitS(ThreadingParser.SContext ctx);
	/**
	 * Enter a parse tree produced by the {@code add}
	 * labeled alternative in {@link ThreadingParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterAdd(ThreadingParser.AddContext ctx);
	/**
	 * Exit a parse tree produced by the {@code add}
	 * labeled alternative in {@link ThreadingParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitAdd(ThreadingParser.AddContext ctx);
	/**
	 * Enter a parse tree produced by the {@code number}
	 * labeled alternative in {@link ThreadingParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterNumber(ThreadingParser.NumberContext ctx);
	/**
	 * Exit a parse tree produced by the {@code number}
	 * labeled alternative in {@link ThreadingParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitNumber(ThreadingParser.NumberContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multiply}
	 * labeled alternative in {@link ThreadingParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterMultiply(ThreadingParser.MultiplyContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multiply}
	 * labeled alternative in {@link ThreadingParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitMultiply(ThreadingParser.MultiplyContext ctx);
}