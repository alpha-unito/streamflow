# Generated from ECMAScript.g4 by ANTLR 4.10
from antlr4 import *

if __name__ is not None and "." in __name__:
    from .ECMAScriptParser import ECMAScriptParser
else:
    from ECMAScriptParser import ECMAScriptParser


# This class defines a complete listener for a parse tree produced by ECMAScriptParser.
class ECMAScriptListener(ParseTreeListener):

    # Enter a parse tree produced by ECMAScriptParser#program.
    def enterProgram(self, ctx: ECMAScriptParser.ProgramContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#program.
    def exitProgram(self, ctx: ECMAScriptParser.ProgramContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#sourceElements.
    def enterSourceElements(self, ctx: ECMAScriptParser.SourceElementsContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#sourceElements.
    def exitSourceElements(self, ctx: ECMAScriptParser.SourceElementsContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#sourceElement.
    def enterSourceElement(self, ctx: ECMAScriptParser.SourceElementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#sourceElement.
    def exitSourceElement(self, ctx: ECMAScriptParser.SourceElementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#statement.
    def enterStatement(self, ctx: ECMAScriptParser.StatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#statement.
    def exitStatement(self, ctx: ECMAScriptParser.StatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#block.
    def enterBlock(self, ctx: ECMAScriptParser.BlockContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#block.
    def exitBlock(self, ctx: ECMAScriptParser.BlockContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#statementList.
    def enterStatementList(self, ctx: ECMAScriptParser.StatementListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#statementList.
    def exitStatementList(self, ctx: ECMAScriptParser.StatementListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#variableStatement.
    def enterVariableStatement(self, ctx: ECMAScriptParser.VariableStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#variableStatement.
    def exitVariableStatement(self, ctx: ECMAScriptParser.VariableStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#variableDeclarationList.
    def enterVariableDeclarationList(self, ctx: ECMAScriptParser.VariableDeclarationListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#variableDeclarationList.
    def exitVariableDeclarationList(self, ctx: ECMAScriptParser.VariableDeclarationListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#variableDeclaration.
    def enterVariableDeclaration(self, ctx: ECMAScriptParser.VariableDeclarationContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#variableDeclaration.
    def exitVariableDeclaration(self, ctx: ECMAScriptParser.VariableDeclarationContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#initialiser.
    def enterInitialiser(self, ctx: ECMAScriptParser.InitialiserContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#initialiser.
    def exitInitialiser(self, ctx: ECMAScriptParser.InitialiserContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#emptyStatement_.
    def enterEmptyStatement_(self, ctx: ECMAScriptParser.EmptyStatement_Context):
        pass

    # Exit a parse tree produced by ECMAScriptParser#emptyStatement_.
    def exitEmptyStatement_(self, ctx: ECMAScriptParser.EmptyStatement_Context):
        pass

    # Enter a parse tree produced by ECMAScriptParser#expressionStatement.
    def enterExpressionStatement(self, ctx: ECMAScriptParser.ExpressionStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#expressionStatement.
    def exitExpressionStatement(self, ctx: ECMAScriptParser.ExpressionStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ifStatement.
    def enterIfStatement(self, ctx: ECMAScriptParser.IfStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ifStatement.
    def exitIfStatement(self, ctx: ECMAScriptParser.IfStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#DoStatement.
    def enterDoStatement(self, ctx: ECMAScriptParser.DoStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#DoStatement.
    def exitDoStatement(self, ctx: ECMAScriptParser.DoStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#WhileStatement.
    def enterWhileStatement(self, ctx: ECMAScriptParser.WhileStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#WhileStatement.
    def exitWhileStatement(self, ctx: ECMAScriptParser.WhileStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ForStatement.
    def enterForStatement(self, ctx: ECMAScriptParser.ForStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ForStatement.
    def exitForStatement(self, ctx: ECMAScriptParser.ForStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ForVarStatement.
    def enterForVarStatement(self, ctx: ECMAScriptParser.ForVarStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ForVarStatement.
    def exitForVarStatement(self, ctx: ECMAScriptParser.ForVarStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ForInStatement.
    def enterForInStatement(self, ctx: ECMAScriptParser.ForInStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ForInStatement.
    def exitForInStatement(self, ctx: ECMAScriptParser.ForInStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ForVarInStatement.
    def enterForVarInStatement(self, ctx: ECMAScriptParser.ForVarInStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ForVarInStatement.
    def exitForVarInStatement(self, ctx: ECMAScriptParser.ForVarInStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#continueStatement.
    def enterContinueStatement(self, ctx: ECMAScriptParser.ContinueStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#continueStatement.
    def exitContinueStatement(self, ctx: ECMAScriptParser.ContinueStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#breakStatement.
    def enterBreakStatement(self, ctx: ECMAScriptParser.BreakStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#breakStatement.
    def exitBreakStatement(self, ctx: ECMAScriptParser.BreakStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#returnStatement.
    def enterReturnStatement(self, ctx: ECMAScriptParser.ReturnStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#returnStatement.
    def exitReturnStatement(self, ctx: ECMAScriptParser.ReturnStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#withStatement.
    def enterWithStatement(self, ctx: ECMAScriptParser.WithStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#withStatement.
    def exitWithStatement(self, ctx: ECMAScriptParser.WithStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#switchStatement.
    def enterSwitchStatement(self, ctx: ECMAScriptParser.SwitchStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#switchStatement.
    def exitSwitchStatement(self, ctx: ECMAScriptParser.SwitchStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#caseBlock.
    def enterCaseBlock(self, ctx: ECMAScriptParser.CaseBlockContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#caseBlock.
    def exitCaseBlock(self, ctx: ECMAScriptParser.CaseBlockContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#caseClauses.
    def enterCaseClauses(self, ctx: ECMAScriptParser.CaseClausesContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#caseClauses.
    def exitCaseClauses(self, ctx: ECMAScriptParser.CaseClausesContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#caseClause.
    def enterCaseClause(self, ctx: ECMAScriptParser.CaseClauseContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#caseClause.
    def exitCaseClause(self, ctx: ECMAScriptParser.CaseClauseContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#defaultClause.
    def enterDefaultClause(self, ctx: ECMAScriptParser.DefaultClauseContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#defaultClause.
    def exitDefaultClause(self, ctx: ECMAScriptParser.DefaultClauseContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#labelledStatement.
    def enterLabelledStatement(self, ctx: ECMAScriptParser.LabelledStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#labelledStatement.
    def exitLabelledStatement(self, ctx: ECMAScriptParser.LabelledStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#throwStatement.
    def enterThrowStatement(self, ctx: ECMAScriptParser.ThrowStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#throwStatement.
    def exitThrowStatement(self, ctx: ECMAScriptParser.ThrowStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#tryStatement.
    def enterTryStatement(self, ctx: ECMAScriptParser.TryStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#tryStatement.
    def exitTryStatement(self, ctx: ECMAScriptParser.TryStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#catchProduction.
    def enterCatchProduction(self, ctx: ECMAScriptParser.CatchProductionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#catchProduction.
    def exitCatchProduction(self, ctx: ECMAScriptParser.CatchProductionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#finallyProduction.
    def enterFinallyProduction(self, ctx: ECMAScriptParser.FinallyProductionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#finallyProduction.
    def exitFinallyProduction(self, ctx: ECMAScriptParser.FinallyProductionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#debuggerStatement.
    def enterDebuggerStatement(self, ctx: ECMAScriptParser.DebuggerStatementContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#debuggerStatement.
    def exitDebuggerStatement(self, ctx: ECMAScriptParser.DebuggerStatementContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#functionDeclaration.
    def enterFunctionDeclaration(self, ctx: ECMAScriptParser.FunctionDeclarationContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#functionDeclaration.
    def exitFunctionDeclaration(self, ctx: ECMAScriptParser.FunctionDeclarationContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#formalParameterList.
    def enterFormalParameterList(self, ctx: ECMAScriptParser.FormalParameterListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#formalParameterList.
    def exitFormalParameterList(self, ctx: ECMAScriptParser.FormalParameterListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#functionBody.
    def enterFunctionBody(self, ctx: ECMAScriptParser.FunctionBodyContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#functionBody.
    def exitFunctionBody(self, ctx: ECMAScriptParser.FunctionBodyContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#arrayLiteral.
    def enterArrayLiteral(self, ctx: ECMAScriptParser.ArrayLiteralContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#arrayLiteral.
    def exitArrayLiteral(self, ctx: ECMAScriptParser.ArrayLiteralContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#elementList.
    def enterElementList(self, ctx: ECMAScriptParser.ElementListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#elementList.
    def exitElementList(self, ctx: ECMAScriptParser.ElementListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#elision.
    def enterElision(self, ctx: ECMAScriptParser.ElisionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#elision.
    def exitElision(self, ctx: ECMAScriptParser.ElisionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#objectLiteral.
    def enterObjectLiteral(self, ctx: ECMAScriptParser.ObjectLiteralContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#objectLiteral.
    def exitObjectLiteral(self, ctx: ECMAScriptParser.ObjectLiteralContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#propertyNameAndValueList.
    def enterPropertyNameAndValueList(self, ctx: ECMAScriptParser.PropertyNameAndValueListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#propertyNameAndValueList.
    def exitPropertyNameAndValueList(self, ctx: ECMAScriptParser.PropertyNameAndValueListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PropertyExpressionAssignment.
    def enterPropertyExpressionAssignment(self, ctx: ECMAScriptParser.PropertyExpressionAssignmentContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PropertyExpressionAssignment.
    def exitPropertyExpressionAssignment(self, ctx: ECMAScriptParser.PropertyExpressionAssignmentContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PropertyGetter.
    def enterPropertyGetter(self, ctx: ECMAScriptParser.PropertyGetterContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PropertyGetter.
    def exitPropertyGetter(self, ctx: ECMAScriptParser.PropertyGetterContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PropertySetter.
    def enterPropertySetter(self, ctx: ECMAScriptParser.PropertySetterContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PropertySetter.
    def exitPropertySetter(self, ctx: ECMAScriptParser.PropertySetterContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#propertyName.
    def enterPropertyName(self, ctx: ECMAScriptParser.PropertyNameContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#propertyName.
    def exitPropertyName(self, ctx: ECMAScriptParser.PropertyNameContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#propertySetParameterList.
    def enterPropertySetParameterList(self, ctx: ECMAScriptParser.PropertySetParameterListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#propertySetParameterList.
    def exitPropertySetParameterList(self, ctx: ECMAScriptParser.PropertySetParameterListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#arguments.
    def enterArguments(self, ctx: ECMAScriptParser.ArgumentsContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#arguments.
    def exitArguments(self, ctx: ECMAScriptParser.ArgumentsContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#argumentList.
    def enterArgumentList(self, ctx: ECMAScriptParser.ArgumentListContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#argumentList.
    def exitArgumentList(self, ctx: ECMAScriptParser.ArgumentListContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#expressionSequence.
    def enterExpressionSequence(self, ctx: ECMAScriptParser.ExpressionSequenceContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#expressionSequence.
    def exitExpressionSequence(self, ctx: ECMAScriptParser.ExpressionSequenceContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#TernaryExpression.
    def enterTernaryExpression(self, ctx: ECMAScriptParser.TernaryExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#TernaryExpression.
    def exitTernaryExpression(self, ctx: ECMAScriptParser.TernaryExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#LogicalAndExpression.
    def enterLogicalAndExpression(self, ctx: ECMAScriptParser.LogicalAndExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#LogicalAndExpression.
    def exitLogicalAndExpression(self, ctx: ECMAScriptParser.LogicalAndExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PreIncrementExpression.
    def enterPreIncrementExpression(self, ctx: ECMAScriptParser.PreIncrementExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PreIncrementExpression.
    def exitPreIncrementExpression(self, ctx: ECMAScriptParser.PreIncrementExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ObjectLiteralExpression.
    def enterObjectLiteralExpression(self, ctx: ECMAScriptParser.ObjectLiteralExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ObjectLiteralExpression.
    def exitObjectLiteralExpression(self, ctx: ECMAScriptParser.ObjectLiteralExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#InExpression.
    def enterInExpression(self, ctx: ECMAScriptParser.InExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#InExpression.
    def exitInExpression(self, ctx: ECMAScriptParser.InExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#LogicalOrExpression.
    def enterLogicalOrExpression(self, ctx: ECMAScriptParser.LogicalOrExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#LogicalOrExpression.
    def exitLogicalOrExpression(self, ctx: ECMAScriptParser.LogicalOrExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#NotExpression.
    def enterNotExpression(self, ctx: ECMAScriptParser.NotExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#NotExpression.
    def exitNotExpression(self, ctx: ECMAScriptParser.NotExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PreDecreaseExpression.
    def enterPreDecreaseExpression(self, ctx: ECMAScriptParser.PreDecreaseExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PreDecreaseExpression.
    def exitPreDecreaseExpression(self, ctx: ECMAScriptParser.PreDecreaseExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ArgumentsExpression.
    def enterArgumentsExpression(self, ctx: ECMAScriptParser.ArgumentsExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ArgumentsExpression.
    def exitArgumentsExpression(self, ctx: ECMAScriptParser.ArgumentsExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ThisExpression.
    def enterThisExpression(self, ctx: ECMAScriptParser.ThisExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ThisExpression.
    def exitThisExpression(self, ctx: ECMAScriptParser.ThisExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#FunctionExpression.
    def enterFunctionExpression(self, ctx: ECMAScriptParser.FunctionExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#FunctionExpression.
    def exitFunctionExpression(self, ctx: ECMAScriptParser.FunctionExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#UnaryMinusExpression.
    def enterUnaryMinusExpression(self, ctx: ECMAScriptParser.UnaryMinusExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#UnaryMinusExpression.
    def exitUnaryMinusExpression(self, ctx: ECMAScriptParser.UnaryMinusExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#AssignmentExpression.
    def enterAssignmentExpression(self, ctx: ECMAScriptParser.AssignmentExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#AssignmentExpression.
    def exitAssignmentExpression(self, ctx: ECMAScriptParser.AssignmentExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PostDecreaseExpression.
    def enterPostDecreaseExpression(self, ctx: ECMAScriptParser.PostDecreaseExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PostDecreaseExpression.
    def exitPostDecreaseExpression(self, ctx: ECMAScriptParser.PostDecreaseExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#TypeofExpression.
    def enterTypeofExpression(self, ctx: ECMAScriptParser.TypeofExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#TypeofExpression.
    def exitTypeofExpression(self, ctx: ECMAScriptParser.TypeofExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#InstanceofExpression.
    def enterInstanceofExpression(self, ctx: ECMAScriptParser.InstanceofExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#InstanceofExpression.
    def exitInstanceofExpression(self, ctx: ECMAScriptParser.InstanceofExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#UnaryPlusExpression.
    def enterUnaryPlusExpression(self, ctx: ECMAScriptParser.UnaryPlusExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#UnaryPlusExpression.
    def exitUnaryPlusExpression(self, ctx: ECMAScriptParser.UnaryPlusExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#DeleteExpression.
    def enterDeleteExpression(self, ctx: ECMAScriptParser.DeleteExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#DeleteExpression.
    def exitDeleteExpression(self, ctx: ECMAScriptParser.DeleteExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#EqualityExpression.
    def enterEqualityExpression(self, ctx: ECMAScriptParser.EqualityExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#EqualityExpression.
    def exitEqualityExpression(self, ctx: ECMAScriptParser.EqualityExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#BitXOrExpression.
    def enterBitXOrExpression(self, ctx: ECMAScriptParser.BitXOrExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#BitXOrExpression.
    def exitBitXOrExpression(self, ctx: ECMAScriptParser.BitXOrExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#MultiplicativeExpression.
    def enterMultiplicativeExpression(self, ctx: ECMAScriptParser.MultiplicativeExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#MultiplicativeExpression.
    def exitMultiplicativeExpression(self, ctx: ECMAScriptParser.MultiplicativeExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#BitShiftExpression.
    def enterBitShiftExpression(self, ctx: ECMAScriptParser.BitShiftExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#BitShiftExpression.
    def exitBitShiftExpression(self, ctx: ECMAScriptParser.BitShiftExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ParenthesizedExpression.
    def enterParenthesizedExpression(self, ctx: ECMAScriptParser.ParenthesizedExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ParenthesizedExpression.
    def exitParenthesizedExpression(self, ctx: ECMAScriptParser.ParenthesizedExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#AdditiveExpression.
    def enterAdditiveExpression(self, ctx: ECMAScriptParser.AdditiveExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#AdditiveExpression.
    def exitAdditiveExpression(self, ctx: ECMAScriptParser.AdditiveExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#RelationalExpression.
    def enterRelationalExpression(self, ctx: ECMAScriptParser.RelationalExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#RelationalExpression.
    def exitRelationalExpression(self, ctx: ECMAScriptParser.RelationalExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#PostIncrementExpression.
    def enterPostIncrementExpression(self, ctx: ECMAScriptParser.PostIncrementExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#PostIncrementExpression.
    def exitPostIncrementExpression(self, ctx: ECMAScriptParser.PostIncrementExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#BitNotExpression.
    def enterBitNotExpression(self, ctx: ECMAScriptParser.BitNotExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#BitNotExpression.
    def exitBitNotExpression(self, ctx: ECMAScriptParser.BitNotExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#NewExpression.
    def enterNewExpression(self, ctx: ECMAScriptParser.NewExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#NewExpression.
    def exitNewExpression(self, ctx: ECMAScriptParser.NewExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#LiteralExpression.
    def enterLiteralExpression(self, ctx: ECMAScriptParser.LiteralExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#LiteralExpression.
    def exitLiteralExpression(self, ctx: ECMAScriptParser.LiteralExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#ArrayLiteralExpression.
    def enterArrayLiteralExpression(self, ctx: ECMAScriptParser.ArrayLiteralExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#ArrayLiteralExpression.
    def exitArrayLiteralExpression(self, ctx: ECMAScriptParser.ArrayLiteralExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#MemberDotExpression.
    def enterMemberDotExpression(self, ctx: ECMAScriptParser.MemberDotExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#MemberDotExpression.
    def exitMemberDotExpression(self, ctx: ECMAScriptParser.MemberDotExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#MemberIndexExpression.
    def enterMemberIndexExpression(self, ctx: ECMAScriptParser.MemberIndexExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#MemberIndexExpression.
    def exitMemberIndexExpression(self, ctx: ECMAScriptParser.MemberIndexExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#IdentifierExpression.
    def enterIdentifierExpression(self, ctx: ECMAScriptParser.IdentifierExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#IdentifierExpression.
    def exitIdentifierExpression(self, ctx: ECMAScriptParser.IdentifierExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#BitAndExpression.
    def enterBitAndExpression(self, ctx: ECMAScriptParser.BitAndExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#BitAndExpression.
    def exitBitAndExpression(self, ctx: ECMAScriptParser.BitAndExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#BitOrExpression.
    def enterBitOrExpression(self, ctx: ECMAScriptParser.BitOrExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#BitOrExpression.
    def exitBitOrExpression(self, ctx: ECMAScriptParser.BitOrExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#AssignmentOperatorExpression.
    def enterAssignmentOperatorExpression(self, ctx: ECMAScriptParser.AssignmentOperatorExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#AssignmentOperatorExpression.
    def exitAssignmentOperatorExpression(self, ctx: ECMAScriptParser.AssignmentOperatorExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#VoidExpression.
    def enterVoidExpression(self, ctx: ECMAScriptParser.VoidExpressionContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#VoidExpression.
    def exitVoidExpression(self, ctx: ECMAScriptParser.VoidExpressionContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#assignmentOperator.
    def enterAssignmentOperator(self, ctx: ECMAScriptParser.AssignmentOperatorContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#assignmentOperator.
    def exitAssignmentOperator(self, ctx: ECMAScriptParser.AssignmentOperatorContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#literal.
    def enterLiteral(self, ctx: ECMAScriptParser.LiteralContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#literal.
    def exitLiteral(self, ctx: ECMAScriptParser.LiteralContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#numericLiteral.
    def enterNumericLiteral(self, ctx: ECMAScriptParser.NumericLiteralContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#numericLiteral.
    def exitNumericLiteral(self, ctx: ECMAScriptParser.NumericLiteralContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#identifierName.
    def enterIdentifierName(self, ctx: ECMAScriptParser.IdentifierNameContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#identifierName.
    def exitIdentifierName(self, ctx: ECMAScriptParser.IdentifierNameContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#reservedWord.
    def enterReservedWord(self, ctx: ECMAScriptParser.ReservedWordContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#reservedWord.
    def exitReservedWord(self, ctx: ECMAScriptParser.ReservedWordContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#keyword.
    def enterKeyword(self, ctx: ECMAScriptParser.KeywordContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#keyword.
    def exitKeyword(self, ctx: ECMAScriptParser.KeywordContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#futureReservedWord.
    def enterFutureReservedWord(self, ctx: ECMAScriptParser.FutureReservedWordContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#futureReservedWord.
    def exitFutureReservedWord(self, ctx: ECMAScriptParser.FutureReservedWordContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#getter.
    def enterGetter(self, ctx: ECMAScriptParser.GetterContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#getter.
    def exitGetter(self, ctx: ECMAScriptParser.GetterContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#setter.
    def enterSetter(self, ctx: ECMAScriptParser.SetterContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#setter.
    def exitSetter(self, ctx: ECMAScriptParser.SetterContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#eos.
    def enterEos(self, ctx: ECMAScriptParser.EosContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#eos.
    def exitEos(self, ctx: ECMAScriptParser.EosContext):
        pass

    # Enter a parse tree produced by ECMAScriptParser#eof.
    def enterEof(self, ctx: ECMAScriptParser.EofContext):
        pass

    # Exit a parse tree produced by ECMAScriptParser#eof.
    def exitEof(self, ctx: ECMAScriptParser.EofContext):
        pass


del ECMAScriptParser
