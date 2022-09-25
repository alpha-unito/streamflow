from typing import Any, MutableSequence, Optional, Set

import antlr4
from cwl_utils.sandboxjs import JSEngine, code_fragment_to_js, segment_re

from streamflow.core.utils import NamesStack
from streamflow.cwl.antlr.ECMAScriptLexer import ECMAScriptLexer
from streamflow.cwl.antlr.ECMAScriptListener import ECMAScriptListener
from streamflow.cwl.antlr.ECMAScriptParser import ECMAScriptParser


def _extract_key(next_seg: str) -> str:
    if next_seg[0] == ".":
        return next_seg[1:]
    elif next_seg[1] in ("'", '"'):
        return next_seg[2:-2].replace("\\'", "'").replace('\\"', '"')


class CWLDependencyListener(ECMAScriptListener):

    def __init__(self):
        self.deps: Set[str] = set()
        self.names: NamesStack = NamesStack()
        self.names.add_name("inputs")

    @staticmethod
    def _get_index(ctx: antlr4.ParserRuleContext) -> Optional[str]:
        token = ctx.getToken(ECMAScriptParser.StringLiteral, 0)
        return token.symbol.text if token else None

    @staticmethod
    def _get_name(ctx: antlr4.ParserRuleContext) -> Optional[str]:
        token = ctx.getToken(ECMAScriptParser.Identifier, 0)
        return token.symbol.text if token else None

    def enterFunctionDeclaration(self, ctx: ECMAScriptParser.FunctionDeclarationContext):
        self.names.add_scope()
        parameters = ctx.formalParameterList()
        if parameters:
            for param in parameters.Identifier():
                if (name := param.symbol.text) in self.names:
                    self.names.add_name(name)

    def exitFunctionDeclaration(self, ctx: ECMAScriptParser.FunctionDeclarationContext):
        self.names.delete_scope()

    def enterAssignmentExpression(self, ctx: ECMAScriptParser.AssignmentExpressionContext):
        left = ctx.getChild(0)
        right = ctx.getChild(2)
        if isinstance(left, ECMAScriptParser.SingleExpressionContext):
            left_name = self._get_name(left)
            if left_name:
                if left_name in self.names:
                    if isinstance(right, ECMAScriptParser.SingleExpressionContext):
                        right_name = self._get_name(right)
                        if right_name and right_name not in self.names:
                            self.names.delete_name(left_name)
                    else:
                        self.names.delete_name(left_name)
                elif isinstance(right, ECMAScriptParser.SingleExpressionContext):
                    right_name = self._get_name(right)
                    if right_name in self.names:
                        self.names.add_name(left_name)

    def enterMemberDotExpression(self, ctx: ECMAScriptParser.MemberDotExpressionContext):
        if self._get_name(ctx.singleExpression()) in self.names.global_names():
            if dep := self._get_name(ctx.identifierName()):
                self.deps.add(dep)

    def enterMemberIndexExpression(self, ctx: ECMAScriptParser.MemberIndexExpressionContext):
        if self._get_name(ctx.singleExpression()) in self.names.global_names():
            for expr in ctx.expressionSequence().singleExpression():
                if dep := self._get_index(expr.literal()).strip("'\""):
                    self.deps.add(dep)


class DependencyResolver(JSEngine):

    def __init__(self):
        self.deps = set()

    def eval(self,
             scan: str,
             jslib: str = "",
             **kwargs: Any) -> Optional[Any]:
        code = code_fragment_to_js(scan, jslib)
        lexer = ECMAScriptLexer(antlr4.InputStream(code))
        parser = ECMAScriptParser(antlr4.CommonTokenStream(lexer))
        listener = CWLDependencyListener()
        walker = antlr4.ParseTreeWalker()
        walker.walk(listener, parser.program())
        self.deps = listener.deps
        return None

    def regex_eval(self,
                   parsed_string: str,
                   remaining_string: str,
                   current_value: Any,
                   **kwargs: Any) -> Optional[Any]:
        if parsed_string != "inputs":
            return None
        elif remaining_string:
            if not (m := segment_re.match(remaining_string)):
                return None
            key = _extract_key(m.group(1))
            if isinstance(current_value, MutableSequence) and key == "length" and not remaining_string[m.end(1):]:
                return None
            self.deps = {key}
            return None
        else:
            return None
