// Generated from /home/sven/7/Projects/Open/qlever-code/src/parser/sparqlParser/generated/SparqlAutomatic.g4 by ANTLR 4.13.1
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SparqlAutomaticParser}.
 */
public interface SparqlAutomaticListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#queryOrUpdate}.
	 * @param ctx the parse tree
	 */
	void enterQueryOrUpdate(SparqlAutomaticParser.QueryOrUpdateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#queryOrUpdate}.
	 * @param ctx the parse tree
	 */
	void exitQueryOrUpdate(SparqlAutomaticParser.QueryOrUpdateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#query}.
	 * @param ctx the parse tree
	 */
	void enterQuery(SparqlAutomaticParser.QueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#query}.
	 * @param ctx the parse tree
	 */
	void exitQuery(SparqlAutomaticParser.QueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#prologue}.
	 * @param ctx the parse tree
	 */
	void enterPrologue(SparqlAutomaticParser.PrologueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#prologue}.
	 * @param ctx the parse tree
	 */
	void exitPrologue(SparqlAutomaticParser.PrologueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#baseDecl}.
	 * @param ctx the parse tree
	 */
	void enterBaseDecl(SparqlAutomaticParser.BaseDeclContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#baseDecl}.
	 * @param ctx the parse tree
	 */
	void exitBaseDecl(SparqlAutomaticParser.BaseDeclContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#prefixDecl}.
	 * @param ctx the parse tree
	 */
	void enterPrefixDecl(SparqlAutomaticParser.PrefixDeclContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#prefixDecl}.
	 * @param ctx the parse tree
	 */
	void exitPrefixDecl(SparqlAutomaticParser.PrefixDeclContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#selectQuery}.
	 * @param ctx the parse tree
	 */
	void enterSelectQuery(SparqlAutomaticParser.SelectQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#selectQuery}.
	 * @param ctx the parse tree
	 */
	void exitSelectQuery(SparqlAutomaticParser.SelectQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#subSelect}.
	 * @param ctx the parse tree
	 */
	void enterSubSelect(SparqlAutomaticParser.SubSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#subSelect}.
	 * @param ctx the parse tree
	 */
	void exitSubSelect(SparqlAutomaticParser.SubSelectContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void enterSelectClause(SparqlAutomaticParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#selectClause}.
	 * @param ctx the parse tree
	 */
	void exitSelectClause(SparqlAutomaticParser.SelectClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#varOrAlias}.
	 * @param ctx the parse tree
	 */
	void enterVarOrAlias(SparqlAutomaticParser.VarOrAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#varOrAlias}.
	 * @param ctx the parse tree
	 */
	void exitVarOrAlias(SparqlAutomaticParser.VarOrAliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#alias}.
	 * @param ctx the parse tree
	 */
	void enterAlias(SparqlAutomaticParser.AliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#alias}.
	 * @param ctx the parse tree
	 */
	void exitAlias(SparqlAutomaticParser.AliasContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#aliasWithoutBrackets}.
	 * @param ctx the parse tree
	 */
	void enterAliasWithoutBrackets(SparqlAutomaticParser.AliasWithoutBracketsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#aliasWithoutBrackets}.
	 * @param ctx the parse tree
	 */
	void exitAliasWithoutBrackets(SparqlAutomaticParser.AliasWithoutBracketsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#constructQuery}.
	 * @param ctx the parse tree
	 */
	void enterConstructQuery(SparqlAutomaticParser.ConstructQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#constructQuery}.
	 * @param ctx the parse tree
	 */
	void exitConstructQuery(SparqlAutomaticParser.ConstructQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#describeQuery}.
	 * @param ctx the parse tree
	 */
	void enterDescribeQuery(SparqlAutomaticParser.DescribeQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#describeQuery}.
	 * @param ctx the parse tree
	 */
	void exitDescribeQuery(SparqlAutomaticParser.DescribeQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#askQuery}.
	 * @param ctx the parse tree
	 */
	void enterAskQuery(SparqlAutomaticParser.AskQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#askQuery}.
	 * @param ctx the parse tree
	 */
	void exitAskQuery(SparqlAutomaticParser.AskQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#datasetClause}.
	 * @param ctx the parse tree
	 */
	void enterDatasetClause(SparqlAutomaticParser.DatasetClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#datasetClause}.
	 * @param ctx the parse tree
	 */
	void exitDatasetClause(SparqlAutomaticParser.DatasetClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#defaultGraphClause}.
	 * @param ctx the parse tree
	 */
	void enterDefaultGraphClause(SparqlAutomaticParser.DefaultGraphClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#defaultGraphClause}.
	 * @param ctx the parse tree
	 */
	void exitDefaultGraphClause(SparqlAutomaticParser.DefaultGraphClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#namedGraphClause}.
	 * @param ctx the parse tree
	 */
	void enterNamedGraphClause(SparqlAutomaticParser.NamedGraphClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#namedGraphClause}.
	 * @param ctx the parse tree
	 */
	void exitNamedGraphClause(SparqlAutomaticParser.NamedGraphClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#sourceSelector}.
	 * @param ctx the parse tree
	 */
	void enterSourceSelector(SparqlAutomaticParser.SourceSelectorContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#sourceSelector}.
	 * @param ctx the parse tree
	 */
	void exitSourceSelector(SparqlAutomaticParser.SourceSelectorContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void enterWhereClause(SparqlAutomaticParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#whereClause}.
	 * @param ctx the parse tree
	 */
	void exitWhereClause(SparqlAutomaticParser.WhereClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#solutionModifier}.
	 * @param ctx the parse tree
	 */
	void enterSolutionModifier(SparqlAutomaticParser.SolutionModifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#solutionModifier}.
	 * @param ctx the parse tree
	 */
	void exitSolutionModifier(SparqlAutomaticParser.SolutionModifierContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#groupClause}.
	 * @param ctx the parse tree
	 */
	void enterGroupClause(SparqlAutomaticParser.GroupClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#groupClause}.
	 * @param ctx the parse tree
	 */
	void exitGroupClause(SparqlAutomaticParser.GroupClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#groupCondition}.
	 * @param ctx the parse tree
	 */
	void enterGroupCondition(SparqlAutomaticParser.GroupConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#groupCondition}.
	 * @param ctx the parse tree
	 */
	void exitGroupCondition(SparqlAutomaticParser.GroupConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void enterHavingClause(SparqlAutomaticParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#havingClause}.
	 * @param ctx the parse tree
	 */
	void exitHavingClause(SparqlAutomaticParser.HavingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#havingCondition}.
	 * @param ctx the parse tree
	 */
	void enterHavingCondition(SparqlAutomaticParser.HavingConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#havingCondition}.
	 * @param ctx the parse tree
	 */
	void exitHavingCondition(SparqlAutomaticParser.HavingConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#orderClause}.
	 * @param ctx the parse tree
	 */
	void enterOrderClause(SparqlAutomaticParser.OrderClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#orderClause}.
	 * @param ctx the parse tree
	 */
	void exitOrderClause(SparqlAutomaticParser.OrderClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#orderCondition}.
	 * @param ctx the parse tree
	 */
	void enterOrderCondition(SparqlAutomaticParser.OrderConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#orderCondition}.
	 * @param ctx the parse tree
	 */
	void exitOrderCondition(SparqlAutomaticParser.OrderConditionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#limitOffsetClauses}.
	 * @param ctx the parse tree
	 */
	void enterLimitOffsetClauses(SparqlAutomaticParser.LimitOffsetClausesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#limitOffsetClauses}.
	 * @param ctx the parse tree
	 */
	void exitLimitOffsetClauses(SparqlAutomaticParser.LimitOffsetClausesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void enterLimitClause(SparqlAutomaticParser.LimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#limitClause}.
	 * @param ctx the parse tree
	 */
	void exitLimitClause(SparqlAutomaticParser.LimitClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#offsetClause}.
	 * @param ctx the parse tree
	 */
	void enterOffsetClause(SparqlAutomaticParser.OffsetClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#offsetClause}.
	 * @param ctx the parse tree
	 */
	void exitOffsetClause(SparqlAutomaticParser.OffsetClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#textLimitClause}.
	 * @param ctx the parse tree
	 */
	void enterTextLimitClause(SparqlAutomaticParser.TextLimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#textLimitClause}.
	 * @param ctx the parse tree
	 */
	void exitTextLimitClause(SparqlAutomaticParser.TextLimitClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#valuesClause}.
	 * @param ctx the parse tree
	 */
	void enterValuesClause(SparqlAutomaticParser.ValuesClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#valuesClause}.
	 * @param ctx the parse tree
	 */
	void exitValuesClause(SparqlAutomaticParser.ValuesClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#update}.
	 * @param ctx the parse tree
	 */
	void enterUpdate(SparqlAutomaticParser.UpdateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#update}.
	 * @param ctx the parse tree
	 */
	void exitUpdate(SparqlAutomaticParser.UpdateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#update1}.
	 * @param ctx the parse tree
	 */
	void enterUpdate1(SparqlAutomaticParser.Update1Context ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#update1}.
	 * @param ctx the parse tree
	 */
	void exitUpdate1(SparqlAutomaticParser.Update1Context ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#load}.
	 * @param ctx the parse tree
	 */
	void enterLoad(SparqlAutomaticParser.LoadContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#load}.
	 * @param ctx the parse tree
	 */
	void exitLoad(SparqlAutomaticParser.LoadContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#clear}.
	 * @param ctx the parse tree
	 */
	void enterClear(SparqlAutomaticParser.ClearContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#clear}.
	 * @param ctx the parse tree
	 */
	void exitClear(SparqlAutomaticParser.ClearContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#drop}.
	 * @param ctx the parse tree
	 */
	void enterDrop(SparqlAutomaticParser.DropContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#drop}.
	 * @param ctx the parse tree
	 */
	void exitDrop(SparqlAutomaticParser.DropContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#create}.
	 * @param ctx the parse tree
	 */
	void enterCreate(SparqlAutomaticParser.CreateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#create}.
	 * @param ctx the parse tree
	 */
	void exitCreate(SparqlAutomaticParser.CreateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#add}.
	 * @param ctx the parse tree
	 */
	void enterAdd(SparqlAutomaticParser.AddContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#add}.
	 * @param ctx the parse tree
	 */
	void exitAdd(SparqlAutomaticParser.AddContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#move}.
	 * @param ctx the parse tree
	 */
	void enterMove(SparqlAutomaticParser.MoveContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#move}.
	 * @param ctx the parse tree
	 */
	void exitMove(SparqlAutomaticParser.MoveContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#copy}.
	 * @param ctx the parse tree
	 */
	void enterCopy(SparqlAutomaticParser.CopyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#copy}.
	 * @param ctx the parse tree
	 */
	void exitCopy(SparqlAutomaticParser.CopyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#insertData}.
	 * @param ctx the parse tree
	 */
	void enterInsertData(SparqlAutomaticParser.InsertDataContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#insertData}.
	 * @param ctx the parse tree
	 */
	void exitInsertData(SparqlAutomaticParser.InsertDataContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#deleteData}.
	 * @param ctx the parse tree
	 */
	void enterDeleteData(SparqlAutomaticParser.DeleteDataContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#deleteData}.
	 * @param ctx the parse tree
	 */
	void exitDeleteData(SparqlAutomaticParser.DeleteDataContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#deleteWhere}.
	 * @param ctx the parse tree
	 */
	void enterDeleteWhere(SparqlAutomaticParser.DeleteWhereContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#deleteWhere}.
	 * @param ctx the parse tree
	 */
	void exitDeleteWhere(SparqlAutomaticParser.DeleteWhereContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#modify}.
	 * @param ctx the parse tree
	 */
	void enterModify(SparqlAutomaticParser.ModifyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#modify}.
	 * @param ctx the parse tree
	 */
	void exitModify(SparqlAutomaticParser.ModifyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#deleteClause}.
	 * @param ctx the parse tree
	 */
	void enterDeleteClause(SparqlAutomaticParser.DeleteClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#deleteClause}.
	 * @param ctx the parse tree
	 */
	void exitDeleteClause(SparqlAutomaticParser.DeleteClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#insertClause}.
	 * @param ctx the parse tree
	 */
	void enterInsertClause(SparqlAutomaticParser.InsertClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#insertClause}.
	 * @param ctx the parse tree
	 */
	void exitInsertClause(SparqlAutomaticParser.InsertClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#usingClause}.
	 * @param ctx the parse tree
	 */
	void enterUsingClause(SparqlAutomaticParser.UsingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#usingClause}.
	 * @param ctx the parse tree
	 */
	void exitUsingClause(SparqlAutomaticParser.UsingClauseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphOrDefault}.
	 * @param ctx the parse tree
	 */
	void enterGraphOrDefault(SparqlAutomaticParser.GraphOrDefaultContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphOrDefault}.
	 * @param ctx the parse tree
	 */
	void exitGraphOrDefault(SparqlAutomaticParser.GraphOrDefaultContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphRef}.
	 * @param ctx the parse tree
	 */
	void enterGraphRef(SparqlAutomaticParser.GraphRefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphRef}.
	 * @param ctx the parse tree
	 */
	void exitGraphRef(SparqlAutomaticParser.GraphRefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphRefAll}.
	 * @param ctx the parse tree
	 */
	void enterGraphRefAll(SparqlAutomaticParser.GraphRefAllContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphRefAll}.
	 * @param ctx the parse tree
	 */
	void exitGraphRefAll(SparqlAutomaticParser.GraphRefAllContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#quadPattern}.
	 * @param ctx the parse tree
	 */
	void enterQuadPattern(SparqlAutomaticParser.QuadPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#quadPattern}.
	 * @param ctx the parse tree
	 */
	void exitQuadPattern(SparqlAutomaticParser.QuadPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#quadData}.
	 * @param ctx the parse tree
	 */
	void enterQuadData(SparqlAutomaticParser.QuadDataContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#quadData}.
	 * @param ctx the parse tree
	 */
	void exitQuadData(SparqlAutomaticParser.QuadDataContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#quads}.
	 * @param ctx the parse tree
	 */
	void enterQuads(SparqlAutomaticParser.QuadsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#quads}.
	 * @param ctx the parse tree
	 */
	void exitQuads(SparqlAutomaticParser.QuadsContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#quadsNotTriples}.
	 * @param ctx the parse tree
	 */
	void enterQuadsNotTriples(SparqlAutomaticParser.QuadsNotTriplesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#quadsNotTriples}.
	 * @param ctx the parse tree
	 */
	void exitQuadsNotTriples(SparqlAutomaticParser.QuadsNotTriplesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#triplesTemplate}.
	 * @param ctx the parse tree
	 */
	void enterTriplesTemplate(SparqlAutomaticParser.TriplesTemplateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#triplesTemplate}.
	 * @param ctx the parse tree
	 */
	void exitTriplesTemplate(SparqlAutomaticParser.TriplesTemplateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#groupGraphPattern}.
	 * @param ctx the parse tree
	 */
	void enterGroupGraphPattern(SparqlAutomaticParser.GroupGraphPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#groupGraphPattern}.
	 * @param ctx the parse tree
	 */
	void exitGroupGraphPattern(SparqlAutomaticParser.GroupGraphPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#groupGraphPatternSub}.
	 * @param ctx the parse tree
	 */
	void enterGroupGraphPatternSub(SparqlAutomaticParser.GroupGraphPatternSubContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#groupGraphPatternSub}.
	 * @param ctx the parse tree
	 */
	void exitGroupGraphPatternSub(SparqlAutomaticParser.GroupGraphPatternSubContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphPatternNotTriplesAndMaybeTriples}.
	 * @param ctx the parse tree
	 */
	void enterGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser.GraphPatternNotTriplesAndMaybeTriplesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphPatternNotTriplesAndMaybeTriples}.
	 * @param ctx the parse tree
	 */
	void exitGraphPatternNotTriplesAndMaybeTriples(SparqlAutomaticParser.GraphPatternNotTriplesAndMaybeTriplesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#triplesBlock}.
	 * @param ctx the parse tree
	 */
	void enterTriplesBlock(SparqlAutomaticParser.TriplesBlockContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#triplesBlock}.
	 * @param ctx the parse tree
	 */
	void exitTriplesBlock(SparqlAutomaticParser.TriplesBlockContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphPatternNotTriples}.
	 * @param ctx the parse tree
	 */
	void enterGraphPatternNotTriples(SparqlAutomaticParser.GraphPatternNotTriplesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphPatternNotTriples}.
	 * @param ctx the parse tree
	 */
	void exitGraphPatternNotTriples(SparqlAutomaticParser.GraphPatternNotTriplesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#optionalGraphPattern}.
	 * @param ctx the parse tree
	 */
	void enterOptionalGraphPattern(SparqlAutomaticParser.OptionalGraphPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#optionalGraphPattern}.
	 * @param ctx the parse tree
	 */
	void exitOptionalGraphPattern(SparqlAutomaticParser.OptionalGraphPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphGraphPattern}.
	 * @param ctx the parse tree
	 */
	void enterGraphGraphPattern(SparqlAutomaticParser.GraphGraphPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphGraphPattern}.
	 * @param ctx the parse tree
	 */
	void exitGraphGraphPattern(SparqlAutomaticParser.GraphGraphPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#serviceGraphPattern}.
	 * @param ctx the parse tree
	 */
	void enterServiceGraphPattern(SparqlAutomaticParser.ServiceGraphPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#serviceGraphPattern}.
	 * @param ctx the parse tree
	 */
	void exitServiceGraphPattern(SparqlAutomaticParser.ServiceGraphPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#bind}.
	 * @param ctx the parse tree
	 */
	void enterBind(SparqlAutomaticParser.BindContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#bind}.
	 * @param ctx the parse tree
	 */
	void exitBind(SparqlAutomaticParser.BindContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#inlineData}.
	 * @param ctx the parse tree
	 */
	void enterInlineData(SparqlAutomaticParser.InlineDataContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#inlineData}.
	 * @param ctx the parse tree
	 */
	void exitInlineData(SparqlAutomaticParser.InlineDataContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#dataBlock}.
	 * @param ctx the parse tree
	 */
	void enterDataBlock(SparqlAutomaticParser.DataBlockContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#dataBlock}.
	 * @param ctx the parse tree
	 */
	void exitDataBlock(SparqlAutomaticParser.DataBlockContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#inlineDataOneVar}.
	 * @param ctx the parse tree
	 */
	void enterInlineDataOneVar(SparqlAutomaticParser.InlineDataOneVarContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#inlineDataOneVar}.
	 * @param ctx the parse tree
	 */
	void exitInlineDataOneVar(SparqlAutomaticParser.InlineDataOneVarContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#inlineDataFull}.
	 * @param ctx the parse tree
	 */
	void enterInlineDataFull(SparqlAutomaticParser.InlineDataFullContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#inlineDataFull}.
	 * @param ctx the parse tree
	 */
	void exitInlineDataFull(SparqlAutomaticParser.InlineDataFullContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#dataBlockSingle}.
	 * @param ctx the parse tree
	 */
	void enterDataBlockSingle(SparqlAutomaticParser.DataBlockSingleContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#dataBlockSingle}.
	 * @param ctx the parse tree
	 */
	void exitDataBlockSingle(SparqlAutomaticParser.DataBlockSingleContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#dataBlockValue}.
	 * @param ctx the parse tree
	 */
	void enterDataBlockValue(SparqlAutomaticParser.DataBlockValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#dataBlockValue}.
	 * @param ctx the parse tree
	 */
	void exitDataBlockValue(SparqlAutomaticParser.DataBlockValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#minusGraphPattern}.
	 * @param ctx the parse tree
	 */
	void enterMinusGraphPattern(SparqlAutomaticParser.MinusGraphPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#minusGraphPattern}.
	 * @param ctx the parse tree
	 */
	void exitMinusGraphPattern(SparqlAutomaticParser.MinusGraphPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#groupOrUnionGraphPattern}.
	 * @param ctx the parse tree
	 */
	void enterGroupOrUnionGraphPattern(SparqlAutomaticParser.GroupOrUnionGraphPatternContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#groupOrUnionGraphPattern}.
	 * @param ctx the parse tree
	 */
	void exitGroupOrUnionGraphPattern(SparqlAutomaticParser.GroupOrUnionGraphPatternContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#filterR}.
	 * @param ctx the parse tree
	 */
	void enterFilterR(SparqlAutomaticParser.FilterRContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#filterR}.
	 * @param ctx the parse tree
	 */
	void exitFilterR(SparqlAutomaticParser.FilterRContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#constraint}.
	 * @param ctx the parse tree
	 */
	void enterConstraint(SparqlAutomaticParser.ConstraintContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#constraint}.
	 * @param ctx the parse tree
	 */
	void exitConstraint(SparqlAutomaticParser.ConstraintContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#functionCall}.
	 * @param ctx the parse tree
	 */
	void enterFunctionCall(SparqlAutomaticParser.FunctionCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#functionCall}.
	 * @param ctx the parse tree
	 */
	void exitFunctionCall(SparqlAutomaticParser.FunctionCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#argList}.
	 * @param ctx the parse tree
	 */
	void enterArgList(SparqlAutomaticParser.ArgListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#argList}.
	 * @param ctx the parse tree
	 */
	void exitArgList(SparqlAutomaticParser.ArgListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#expressionList}.
	 * @param ctx the parse tree
	 */
	void enterExpressionList(SparqlAutomaticParser.ExpressionListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#expressionList}.
	 * @param ctx the parse tree
	 */
	void exitExpressionList(SparqlAutomaticParser.ExpressionListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#constructTemplate}.
	 * @param ctx the parse tree
	 */
	void enterConstructTemplate(SparqlAutomaticParser.ConstructTemplateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#constructTemplate}.
	 * @param ctx the parse tree
	 */
	void exitConstructTemplate(SparqlAutomaticParser.ConstructTemplateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#constructTriples}.
	 * @param ctx the parse tree
	 */
	void enterConstructTriples(SparqlAutomaticParser.ConstructTriplesContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#constructTriples}.
	 * @param ctx the parse tree
	 */
	void exitConstructTriples(SparqlAutomaticParser.ConstructTriplesContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#triplesSameSubject}.
	 * @param ctx the parse tree
	 */
	void enterTriplesSameSubject(SparqlAutomaticParser.TriplesSameSubjectContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#triplesSameSubject}.
	 * @param ctx the parse tree
	 */
	void exitTriplesSameSubject(SparqlAutomaticParser.TriplesSameSubjectContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#propertyList}.
	 * @param ctx the parse tree
	 */
	void enterPropertyList(SparqlAutomaticParser.PropertyListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#propertyList}.
	 * @param ctx the parse tree
	 */
	void exitPropertyList(SparqlAutomaticParser.PropertyListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#propertyListNotEmpty}.
	 * @param ctx the parse tree
	 */
	void enterPropertyListNotEmpty(SparqlAutomaticParser.PropertyListNotEmptyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#propertyListNotEmpty}.
	 * @param ctx the parse tree
	 */
	void exitPropertyListNotEmpty(SparqlAutomaticParser.PropertyListNotEmptyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#verb}.
	 * @param ctx the parse tree
	 */
	void enterVerb(SparqlAutomaticParser.VerbContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#verb}.
	 * @param ctx the parse tree
	 */
	void exitVerb(SparqlAutomaticParser.VerbContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#objectList}.
	 * @param ctx the parse tree
	 */
	void enterObjectList(SparqlAutomaticParser.ObjectListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#objectList}.
	 * @param ctx the parse tree
	 */
	void exitObjectList(SparqlAutomaticParser.ObjectListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#objectR}.
	 * @param ctx the parse tree
	 */
	void enterObjectR(SparqlAutomaticParser.ObjectRContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#objectR}.
	 * @param ctx the parse tree
	 */
	void exitObjectR(SparqlAutomaticParser.ObjectRContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#triplesSameSubjectPath}.
	 * @param ctx the parse tree
	 */
	void enterTriplesSameSubjectPath(SparqlAutomaticParser.TriplesSameSubjectPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#triplesSameSubjectPath}.
	 * @param ctx the parse tree
	 */
	void exitTriplesSameSubjectPath(SparqlAutomaticParser.TriplesSameSubjectPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#propertyListPath}.
	 * @param ctx the parse tree
	 */
	void enterPropertyListPath(SparqlAutomaticParser.PropertyListPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#propertyListPath}.
	 * @param ctx the parse tree
	 */
	void exitPropertyListPath(SparqlAutomaticParser.PropertyListPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#propertyListPathNotEmpty}.
	 * @param ctx the parse tree
	 */
	void enterPropertyListPathNotEmpty(SparqlAutomaticParser.PropertyListPathNotEmptyContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#propertyListPathNotEmpty}.
	 * @param ctx the parse tree
	 */
	void exitPropertyListPathNotEmpty(SparqlAutomaticParser.PropertyListPathNotEmptyContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#verbPath}.
	 * @param ctx the parse tree
	 */
	void enterVerbPath(SparqlAutomaticParser.VerbPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#verbPath}.
	 * @param ctx the parse tree
	 */
	void exitVerbPath(SparqlAutomaticParser.VerbPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#verbSimple}.
	 * @param ctx the parse tree
	 */
	void enterVerbSimple(SparqlAutomaticParser.VerbSimpleContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#verbSimple}.
	 * @param ctx the parse tree
	 */
	void exitVerbSimple(SparqlAutomaticParser.VerbSimpleContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#tupleWithoutPath}.
	 * @param ctx the parse tree
	 */
	void enterTupleWithoutPath(SparqlAutomaticParser.TupleWithoutPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#tupleWithoutPath}.
	 * @param ctx the parse tree
	 */
	void exitTupleWithoutPath(SparqlAutomaticParser.TupleWithoutPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#tupleWithPath}.
	 * @param ctx the parse tree
	 */
	void enterTupleWithPath(SparqlAutomaticParser.TupleWithPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#tupleWithPath}.
	 * @param ctx the parse tree
	 */
	void exitTupleWithPath(SparqlAutomaticParser.TupleWithPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#verbPathOrSimple}.
	 * @param ctx the parse tree
	 */
	void enterVerbPathOrSimple(SparqlAutomaticParser.VerbPathOrSimpleContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#verbPathOrSimple}.
	 * @param ctx the parse tree
	 */
	void exitVerbPathOrSimple(SparqlAutomaticParser.VerbPathOrSimpleContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#objectListPath}.
	 * @param ctx the parse tree
	 */
	void enterObjectListPath(SparqlAutomaticParser.ObjectListPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#objectListPath}.
	 * @param ctx the parse tree
	 */
	void exitObjectListPath(SparqlAutomaticParser.ObjectListPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#objectPath}.
	 * @param ctx the parse tree
	 */
	void enterObjectPath(SparqlAutomaticParser.ObjectPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#objectPath}.
	 * @param ctx the parse tree
	 */
	void exitObjectPath(SparqlAutomaticParser.ObjectPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#path}.
	 * @param ctx the parse tree
	 */
	void enterPath(SparqlAutomaticParser.PathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#path}.
	 * @param ctx the parse tree
	 */
	void exitPath(SparqlAutomaticParser.PathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathAlternative}.
	 * @param ctx the parse tree
	 */
	void enterPathAlternative(SparqlAutomaticParser.PathAlternativeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathAlternative}.
	 * @param ctx the parse tree
	 */
	void exitPathAlternative(SparqlAutomaticParser.PathAlternativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathSequence}.
	 * @param ctx the parse tree
	 */
	void enterPathSequence(SparqlAutomaticParser.PathSequenceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathSequence}.
	 * @param ctx the parse tree
	 */
	void exitPathSequence(SparqlAutomaticParser.PathSequenceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathElt}.
	 * @param ctx the parse tree
	 */
	void enterPathElt(SparqlAutomaticParser.PathEltContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathElt}.
	 * @param ctx the parse tree
	 */
	void exitPathElt(SparqlAutomaticParser.PathEltContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathEltOrInverse}.
	 * @param ctx the parse tree
	 */
	void enterPathEltOrInverse(SparqlAutomaticParser.PathEltOrInverseContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathEltOrInverse}.
	 * @param ctx the parse tree
	 */
	void exitPathEltOrInverse(SparqlAutomaticParser.PathEltOrInverseContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathMod}.
	 * @param ctx the parse tree
	 */
	void enterPathMod(SparqlAutomaticParser.PathModContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathMod}.
	 * @param ctx the parse tree
	 */
	void exitPathMod(SparqlAutomaticParser.PathModContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void enterPathPrimary(SparqlAutomaticParser.PathPrimaryContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathPrimary}.
	 * @param ctx the parse tree
	 */
	void exitPathPrimary(SparqlAutomaticParser.PathPrimaryContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathNegatedPropertySet}.
	 * @param ctx the parse tree
	 */
	void enterPathNegatedPropertySet(SparqlAutomaticParser.PathNegatedPropertySetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathNegatedPropertySet}.
	 * @param ctx the parse tree
	 */
	void exitPathNegatedPropertySet(SparqlAutomaticParser.PathNegatedPropertySetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pathOneInPropertySet}.
	 * @param ctx the parse tree
	 */
	void enterPathOneInPropertySet(SparqlAutomaticParser.PathOneInPropertySetContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pathOneInPropertySet}.
	 * @param ctx the parse tree
	 */
	void exitPathOneInPropertySet(SparqlAutomaticParser.PathOneInPropertySetContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#integer}.
	 * @param ctx the parse tree
	 */
	void enterInteger(SparqlAutomaticParser.IntegerContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#integer}.
	 * @param ctx the parse tree
	 */
	void exitInteger(SparqlAutomaticParser.IntegerContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#triplesNode}.
	 * @param ctx the parse tree
	 */
	void enterTriplesNode(SparqlAutomaticParser.TriplesNodeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#triplesNode}.
	 * @param ctx the parse tree
	 */
	void exitTriplesNode(SparqlAutomaticParser.TriplesNodeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#blankNodePropertyList}.
	 * @param ctx the parse tree
	 */
	void enterBlankNodePropertyList(SparqlAutomaticParser.BlankNodePropertyListContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#blankNodePropertyList}.
	 * @param ctx the parse tree
	 */
	void exitBlankNodePropertyList(SparqlAutomaticParser.BlankNodePropertyListContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#triplesNodePath}.
	 * @param ctx the parse tree
	 */
	void enterTriplesNodePath(SparqlAutomaticParser.TriplesNodePathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#triplesNodePath}.
	 * @param ctx the parse tree
	 */
	void exitTriplesNodePath(SparqlAutomaticParser.TriplesNodePathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#blankNodePropertyListPath}.
	 * @param ctx the parse tree
	 */
	void enterBlankNodePropertyListPath(SparqlAutomaticParser.BlankNodePropertyListPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#blankNodePropertyListPath}.
	 * @param ctx the parse tree
	 */
	void exitBlankNodePropertyListPath(SparqlAutomaticParser.BlankNodePropertyListPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#collection}.
	 * @param ctx the parse tree
	 */
	void enterCollection(SparqlAutomaticParser.CollectionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#collection}.
	 * @param ctx the parse tree
	 */
	void exitCollection(SparqlAutomaticParser.CollectionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#collectionPath}.
	 * @param ctx the parse tree
	 */
	void enterCollectionPath(SparqlAutomaticParser.CollectionPathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#collectionPath}.
	 * @param ctx the parse tree
	 */
	void exitCollectionPath(SparqlAutomaticParser.CollectionPathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphNode}.
	 * @param ctx the parse tree
	 */
	void enterGraphNode(SparqlAutomaticParser.GraphNodeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphNode}.
	 * @param ctx the parse tree
	 */
	void exitGraphNode(SparqlAutomaticParser.GraphNodeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphNodePath}.
	 * @param ctx the parse tree
	 */
	void enterGraphNodePath(SparqlAutomaticParser.GraphNodePathContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphNodePath}.
	 * @param ctx the parse tree
	 */
	void exitGraphNodePath(SparqlAutomaticParser.GraphNodePathContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#varOrTerm}.
	 * @param ctx the parse tree
	 */
	void enterVarOrTerm(SparqlAutomaticParser.VarOrTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#varOrTerm}.
	 * @param ctx the parse tree
	 */
	void exitVarOrTerm(SparqlAutomaticParser.VarOrTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#varOrIri}.
	 * @param ctx the parse tree
	 */
	void enterVarOrIri(SparqlAutomaticParser.VarOrIriContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#varOrIri}.
	 * @param ctx the parse tree
	 */
	void exitVarOrIri(SparqlAutomaticParser.VarOrIriContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#var}.
	 * @param ctx the parse tree
	 */
	void enterVar(SparqlAutomaticParser.VarContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#var}.
	 * @param ctx the parse tree
	 */
	void exitVar(SparqlAutomaticParser.VarContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#graphTerm}.
	 * @param ctx the parse tree
	 */
	void enterGraphTerm(SparqlAutomaticParser.GraphTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#graphTerm}.
	 * @param ctx the parse tree
	 */
	void exitGraphTerm(SparqlAutomaticParser.GraphTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterExpression(SparqlAutomaticParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitExpression(SparqlAutomaticParser.ExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#conditionalOrExpression}.
	 * @param ctx the parse tree
	 */
	void enterConditionalOrExpression(SparqlAutomaticParser.ConditionalOrExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#conditionalOrExpression}.
	 * @param ctx the parse tree
	 */
	void exitConditionalOrExpression(SparqlAutomaticParser.ConditionalOrExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#conditionalAndExpression}.
	 * @param ctx the parse tree
	 */
	void enterConditionalAndExpression(SparqlAutomaticParser.ConditionalAndExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#conditionalAndExpression}.
	 * @param ctx the parse tree
	 */
	void exitConditionalAndExpression(SparqlAutomaticParser.ConditionalAndExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#valueLogical}.
	 * @param ctx the parse tree
	 */
	void enterValueLogical(SparqlAutomaticParser.ValueLogicalContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#valueLogical}.
	 * @param ctx the parse tree
	 */
	void exitValueLogical(SparqlAutomaticParser.ValueLogicalContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#relationalExpression}.
	 * @param ctx the parse tree
	 */
	void enterRelationalExpression(SparqlAutomaticParser.RelationalExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#relationalExpression}.
	 * @param ctx the parse tree
	 */
	void exitRelationalExpression(SparqlAutomaticParser.RelationalExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#numericExpression}.
	 * @param ctx the parse tree
	 */
	void enterNumericExpression(SparqlAutomaticParser.NumericExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#numericExpression}.
	 * @param ctx the parse tree
	 */
	void exitNumericExpression(SparqlAutomaticParser.NumericExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#additiveExpression}.
	 * @param ctx the parse tree
	 */
	void enterAdditiveExpression(SparqlAutomaticParser.AdditiveExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#additiveExpression}.
	 * @param ctx the parse tree
	 */
	void exitAdditiveExpression(SparqlAutomaticParser.AdditiveExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#multiplicativeExpressionWithSign}.
	 * @param ctx the parse tree
	 */
	void enterMultiplicativeExpressionWithSign(SparqlAutomaticParser.MultiplicativeExpressionWithSignContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#multiplicativeExpressionWithSign}.
	 * @param ctx the parse tree
	 */
	void exitMultiplicativeExpressionWithSign(SparqlAutomaticParser.MultiplicativeExpressionWithSignContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#plusSubexpression}.
	 * @param ctx the parse tree
	 */
	void enterPlusSubexpression(SparqlAutomaticParser.PlusSubexpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#plusSubexpression}.
	 * @param ctx the parse tree
	 */
	void exitPlusSubexpression(SparqlAutomaticParser.PlusSubexpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#minusSubexpression}.
	 * @param ctx the parse tree
	 */
	void enterMinusSubexpression(SparqlAutomaticParser.MinusSubexpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#minusSubexpression}.
	 * @param ctx the parse tree
	 */
	void exitMinusSubexpression(SparqlAutomaticParser.MinusSubexpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#multiplicativeExpressionWithLeadingSignButNoSpace}.
	 * @param ctx the parse tree
	 */
	void enterMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser.MultiplicativeExpressionWithLeadingSignButNoSpaceContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#multiplicativeExpressionWithLeadingSignButNoSpace}.
	 * @param ctx the parse tree
	 */
	void exitMultiplicativeExpressionWithLeadingSignButNoSpace(SparqlAutomaticParser.MultiplicativeExpressionWithLeadingSignButNoSpaceContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#multiplicativeExpression}.
	 * @param ctx the parse tree
	 */
	void enterMultiplicativeExpression(SparqlAutomaticParser.MultiplicativeExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#multiplicativeExpression}.
	 * @param ctx the parse tree
	 */
	void exitMultiplicativeExpression(SparqlAutomaticParser.MultiplicativeExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#multiplyOrDivideExpression}.
	 * @param ctx the parse tree
	 */
	void enterMultiplyOrDivideExpression(SparqlAutomaticParser.MultiplyOrDivideExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#multiplyOrDivideExpression}.
	 * @param ctx the parse tree
	 */
	void exitMultiplyOrDivideExpression(SparqlAutomaticParser.MultiplyOrDivideExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#multiplyExpression}.
	 * @param ctx the parse tree
	 */
	void enterMultiplyExpression(SparqlAutomaticParser.MultiplyExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#multiplyExpression}.
	 * @param ctx the parse tree
	 */
	void exitMultiplyExpression(SparqlAutomaticParser.MultiplyExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#divideExpression}.
	 * @param ctx the parse tree
	 */
	void enterDivideExpression(SparqlAutomaticParser.DivideExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#divideExpression}.
	 * @param ctx the parse tree
	 */
	void exitDivideExpression(SparqlAutomaticParser.DivideExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#unaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterUnaryExpression(SparqlAutomaticParser.UnaryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#unaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitUnaryExpression(SparqlAutomaticParser.UnaryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void enterPrimaryExpression(SparqlAutomaticParser.PrimaryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#primaryExpression}.
	 * @param ctx the parse tree
	 */
	void exitPrimaryExpression(SparqlAutomaticParser.PrimaryExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#brackettedExpression}.
	 * @param ctx the parse tree
	 */
	void enterBrackettedExpression(SparqlAutomaticParser.BrackettedExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#brackettedExpression}.
	 * @param ctx the parse tree
	 */
	void exitBrackettedExpression(SparqlAutomaticParser.BrackettedExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#builtInCall}.
	 * @param ctx the parse tree
	 */
	void enterBuiltInCall(SparqlAutomaticParser.BuiltInCallContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#builtInCall}.
	 * @param ctx the parse tree
	 */
	void exitBuiltInCall(SparqlAutomaticParser.BuiltInCallContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#regexExpression}.
	 * @param ctx the parse tree
	 */
	void enterRegexExpression(SparqlAutomaticParser.RegexExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#regexExpression}.
	 * @param ctx the parse tree
	 */
	void exitRegexExpression(SparqlAutomaticParser.RegexExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#langExpression}.
	 * @param ctx the parse tree
	 */
	void enterLangExpression(SparqlAutomaticParser.LangExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#langExpression}.
	 * @param ctx the parse tree
	 */
	void exitLangExpression(SparqlAutomaticParser.LangExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#substringExpression}.
	 * @param ctx the parse tree
	 */
	void enterSubstringExpression(SparqlAutomaticParser.SubstringExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#substringExpression}.
	 * @param ctx the parse tree
	 */
	void exitSubstringExpression(SparqlAutomaticParser.SubstringExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#strReplaceExpression}.
	 * @param ctx the parse tree
	 */
	void enterStrReplaceExpression(SparqlAutomaticParser.StrReplaceExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#strReplaceExpression}.
	 * @param ctx the parse tree
	 */
	void exitStrReplaceExpression(SparqlAutomaticParser.StrReplaceExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#existsFunc}.
	 * @param ctx the parse tree
	 */
	void enterExistsFunc(SparqlAutomaticParser.ExistsFuncContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#existsFunc}.
	 * @param ctx the parse tree
	 */
	void exitExistsFunc(SparqlAutomaticParser.ExistsFuncContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#notExistsFunc}.
	 * @param ctx the parse tree
	 */
	void enterNotExistsFunc(SparqlAutomaticParser.NotExistsFuncContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#notExistsFunc}.
	 * @param ctx the parse tree
	 */
	void exitNotExistsFunc(SparqlAutomaticParser.NotExistsFuncContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#aggregate}.
	 * @param ctx the parse tree
	 */
	void enterAggregate(SparqlAutomaticParser.AggregateContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#aggregate}.
	 * @param ctx the parse tree
	 */
	void exitAggregate(SparqlAutomaticParser.AggregateContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#iriOrFunction}.
	 * @param ctx the parse tree
	 */
	void enterIriOrFunction(SparqlAutomaticParser.IriOrFunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#iriOrFunction}.
	 * @param ctx the parse tree
	 */
	void exitIriOrFunction(SparqlAutomaticParser.IriOrFunctionContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#rdfLiteral}.
	 * @param ctx the parse tree
	 */
	void enterRdfLiteral(SparqlAutomaticParser.RdfLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#rdfLiteral}.
	 * @param ctx the parse tree
	 */
	void exitRdfLiteral(SparqlAutomaticParser.RdfLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteral(SparqlAutomaticParser.NumericLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteral(SparqlAutomaticParser.NumericLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#numericLiteralUnsigned}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteralUnsigned(SparqlAutomaticParser.NumericLiteralUnsignedContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#numericLiteralUnsigned}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteralUnsigned(SparqlAutomaticParser.NumericLiteralUnsignedContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#numericLiteralPositive}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteralPositive(SparqlAutomaticParser.NumericLiteralPositiveContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#numericLiteralPositive}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteralPositive(SparqlAutomaticParser.NumericLiteralPositiveContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#numericLiteralNegative}.
	 * @param ctx the parse tree
	 */
	void enterNumericLiteralNegative(SparqlAutomaticParser.NumericLiteralNegativeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#numericLiteralNegative}.
	 * @param ctx the parse tree
	 */
	void exitNumericLiteralNegative(SparqlAutomaticParser.NumericLiteralNegativeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#booleanLiteral}.
	 * @param ctx the parse tree
	 */
	void enterBooleanLiteral(SparqlAutomaticParser.BooleanLiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#booleanLiteral}.
	 * @param ctx the parse tree
	 */
	void exitBooleanLiteral(SparqlAutomaticParser.BooleanLiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#string}.
	 * @param ctx the parse tree
	 */
	void enterString(SparqlAutomaticParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#string}.
	 * @param ctx the parse tree
	 */
	void exitString(SparqlAutomaticParser.StringContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#iri}.
	 * @param ctx the parse tree
	 */
	void enterIri(SparqlAutomaticParser.IriContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#iri}.
	 * @param ctx the parse tree
	 */
	void exitIri(SparqlAutomaticParser.IriContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#prefixedName}.
	 * @param ctx the parse tree
	 */
	void enterPrefixedName(SparqlAutomaticParser.PrefixedNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#prefixedName}.
	 * @param ctx the parse tree
	 */
	void exitPrefixedName(SparqlAutomaticParser.PrefixedNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#blankNode}.
	 * @param ctx the parse tree
	 */
	void enterBlankNode(SparqlAutomaticParser.BlankNodeContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#blankNode}.
	 * @param ctx the parse tree
	 */
	void exitBlankNode(SparqlAutomaticParser.BlankNodeContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#iriref}.
	 * @param ctx the parse tree
	 */
	void enterIriref(SparqlAutomaticParser.IrirefContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#iriref}.
	 * @param ctx the parse tree
	 */
	void exitIriref(SparqlAutomaticParser.IrirefContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pnameLn}.
	 * @param ctx the parse tree
	 */
	void enterPnameLn(SparqlAutomaticParser.PnameLnContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pnameLn}.
	 * @param ctx the parse tree
	 */
	void exitPnameLn(SparqlAutomaticParser.PnameLnContext ctx);
	/**
	 * Enter a parse tree produced by {@link SparqlAutomaticParser#pnameNs}.
	 * @param ctx the parse tree
	 */
	void enterPnameNs(SparqlAutomaticParser.PnameNsContext ctx);
	/**
	 * Exit a parse tree produced by {@link SparqlAutomaticParser#pnameNs}.
	 * @param ctx the parse tree
	 */
	void exitPnameNs(SparqlAutomaticParser.PnameNsContext ctx);
}