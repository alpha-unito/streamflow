import json
import re
from abc import abstractmethod, ABC
from enum import Enum
from typing import Optional, MutableMapping, Any, Mapping, MutableSequence, Set

import antlr4
import cwltool.sandboxjs
from cwltool.sandboxjs import JavascriptException
from schema_salad.utils import json_dumps

from streamflow.core.exception import WorkflowDefinitionException, WorkflowExecutionException
from streamflow.core.utils import NamesStack
from streamflow.cwl.antlr.ECMAScriptLexer import ECMAScriptLexer
from streamflow.cwl.antlr.ECMAScriptListener import ECMAScriptListener
from streamflow.cwl.antlr.ECMAScriptParser import ECMAScriptParser


class _State(Enum):
    DEFAULT = 0
    DOLLAR = 1
    PAREN = 2
    BRACE = 3
    SINGLE_QUOTE = 4
    DOUBLE_QUOTE = 5
    ESCAPE = 6


_SEG_SYMBOL_STR = r"""\w+"""
_SEG_SINGLE_STR = r"""\['([^']|\\')+'\]"""
_SEG_DOUBLE_STR = r"""\["([^"]|\\")+"\]"""
_SEG_INDEX_STR = r"""\[[0-9]+\]"""
_SEGMENTS_STR = fr"(\.{_SEG_SYMBOL_STR}|{_SEG_SINGLE_STR}|{_SEG_DOUBLE_STR}|{_SEG_INDEX_STR})"
_REGEX_SEGMENT = re.compile(_SEGMENTS_STR, flags=re.UNICODE)
_PARAM_STR = fr"\(({_SEG_SYMBOL_STR}){_SEGMENTS_STR}*\)$"
_REGEX_PARAM = re.compile(_PARAM_STR, flags=re.UNICODE)


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


class Evaluator(ABC):

    def __init__(self,
                 jslib: str,
                 rootvars: MutableMapping[str, Any],
                 timeout: float,
                 full_js: bool = False):
        self.jslib: str = jslib
        self.rootvars: MutableMapping[str, Any] = rootvars
        self.timeout: float = timeout
        self.full_js: bool = full_js

    @abstractmethod
    async def _ecmascript_evaluate(self, expr: str) -> Optional[Any]:
        ...

    @abstractmethod
    async def _regex_evaluate(self,
                              parsed_string: str,
                              remaining_string: str,
                              current_value: Any) -> Optional[Any]:
        ...

    def evaluate(self, expr: str) -> Optional[Any]:
        expression_parse_exception = None
        if (match := _REGEX_PARAM.match(expr)) is not None:
            first_symbol = match.group(1)
            first_symbol_end = match.end(1)
            if first_symbol_end + 1 == len(expr) and first_symbol == "null":
                return None
            try:
                if first_symbol not in self.rootvars:
                    raise WorkflowExecutionException("{} is not defined".format(first_symbol))
                return self._regex_evaluate(first_symbol, expr[first_symbol_end:-1], self.rootvars[first_symbol])

            except WorkflowExecutionException as wexc:
                expression_parse_exception = wexc
        if self.full_js:
            return self._ecmascript_evaluate(expr)
        else:
            if expression_parse_exception is not None:
                raise JavascriptException(
                    "Syntax error in parameter reference '{}': {}. This could be "
                    "due to using Javascript code without specifying "
                    "InlineJavascriptRequirement.".format(expr[1:-1], expression_parse_exception))
            else:
                raise JavascriptException(
                    "Syntax error in parameter reference '{}'. This could be due "
                    "to using Javascript code without specifying "
                    "InlineJavascriptRequirement.".format(expr))


class DependencyResolver(Evaluator):

    def _ecmascript_evaluate(self, expr: str) -> Optional[Any]:
        code = cwltool.sandboxjs.code_fragment_to_js(expr, self.jslib)
        lexer = ECMAScriptLexer(antlr4.InputStream(code))
        parser = ECMAScriptParser(antlr4.CommonTokenStream(lexer))
        listener = CWLDependencyListener()
        walker = antlr4.ParseTreeWalker()
        walker.walk(listener, parser.program())
        return listener.deps

    def _regex_evaluate(self,
                        parsed_string: str,
                        remaining_string: str,
                        current_value: Any) -> Optional[Any]:
        if parsed_string != "inputs":
            return set()
        elif remaining_string:
            if not (m := _REGEX_SEGMENT.match(remaining_string)):
                return set()
            key = _extract_key(m.group(1))
            if isinstance(current_value, MutableSequence) and key == "length" and not remaining_string[m.end(1):]:
                return set()
            return {key}
        else:
            return set()


class RuntimeEvaluator(Evaluator):

    def _ecmascript_evaluate(self, expr: str) -> Optional[Any]:
        return cwltool.sandboxjs.execjs(expr, self.jslib, self.timeout)

    def _regex_evaluate(self,
                        parsed_string: str,
                        remaining_string: str,
                        current_value: Any) -> Optional[Any]:
        if remaining_string:
            if not (m := _REGEX_SEGMENT.match(remaining_string)):
                return current_value
            next_seg = m.group(1)
            key = _extract_key(next_seg)
            if key is not None:
                if isinstance(current_value, MutableSequence) and key == "length" and not remaining_string[m.end(1):]:
                    return len(current_value)
                if not isinstance(current_value, MutableMapping):
                    raise WorkflowExecutionException("{} is a {}, cannot index on string '{}'".format(
                        parsed_string, type(current_value).__name__, key))
                if key not in current_value:
                    raise WorkflowExecutionException("{} does not contain key '{}'".format(
                        parsed_string, key))
            else:
                try:
                    key = int(next_seg[1:-1])
                except ValueError as v:
                    raise WorkflowExecutionException(str(v)) from v
                if not isinstance(current_value, MutableSequence):
                    raise WorkflowExecutionException("{} is a {}, cannot index on int '{}'".format(
                        parsed_string, type(current_value).__name__, key))
                if key and key >= len(current_value):
                    raise WorkflowExecutionException("{} list index {} out of range".format(
                        parsed_string, key))
            if (isinstance(current_value, Mapping) or
                    (isinstance(current_value, MutableSequence) and isinstance(key, int))):
                try:
                    return self._regex_evaluate(
                        ''.join([parsed_string, remaining_string]),
                        remaining_string[m.end(1):],
                        current_value[key])
                except KeyError:
                    raise WorkflowExecutionException("{} doesn't have property {}".format(
                        parsed_string, key))
            else:
                raise WorkflowExecutionException("{} doesn't have property {}".format(
                    parsed_string, key))
        else:
            return current_value


class Scanner(object):

    def __init__(self, text: str):
        self.text: str = text
        self.stack = [_State.DEFAULT]
        self.start = 0
        self.end = 0
        self.index = 0
        self.parts = []

    def _scan_container(self, c: str, open_char: str, close_char: str, state: _State) -> Optional[str]:
        if c == open_char:
            self.stack.append(state)
        elif c == close_char:
            self.stack.pop()
            if self.stack[-1] == _State.DOLLAR:
                self.stack = [_State.DEFAULT]
                if self.end < self.start - 1:
                    self.parts.append(self.text[self.end:self.start - 1])
                self.end = self.index
                return self.text[self.start:self.index]
        elif c == "'":
            self.stack.append(_State.SINGLE_QUOTE)
        elif c == '"':
            self.stack.append(_State.DOUBLE_QUOTE)

    def _scan_brace(self, c: str) -> Optional[str]:
        return self._scan_container(c, "{", "}", _State.BRACE)

    def _scan_default(self, c: str) -> None:
        if c == "$":
            self.stack.append(_State.DOLLAR)
        elif c == "\\":
            self.stack.append(_State.ESCAPE)

    def _scan_dollar(self, c: str) -> None:
        if c == "(":
            self.start = self.index - 1
            self.stack.append(_State.PAREN)
        elif c == "{":
            self.start = self.index - 1
            self.stack.append(_State.BRACE)
        else:
            self.stack.pop()

    def _scan_double_quote(self, c: str) -> None:
        self._scan_quote(c, '"')

    def _scan_escape(self, c: str) -> None:
        self.stack.pop()
        if self.stack[-1] == _State.DEFAULT:
            if c == "\\" or (c == "$" and self.text[self.index] in ["(", "{"]):
                if self.end < self.index - 2:
                    self.parts.append(self.text[self.end:self.index - 2])
                self.end = self.index - 1

    def _scan_paren(self, c: str) -> Optional[str]:
        return self._scan_container(c, "(", ")", _State.PAREN)

    def _scan_quote(self, c: str, quote_char: str):
        if c == quote_char:
            self.stack.pop()
        elif c == "\\":
            self.stack.append(_State.ESCAPE)

    def _scan_single_quote(self, c: str) -> None:
        self._scan_quote(c, "'")

    def add_part(self, part: str):
        self.parts.append(part)

    def get_result(self):
        return ''.join(self.parts)

    def scan(self) -> Optional[str]:
        while self.index < len(self.text):
            state = self.stack[-1]
            c = self.text[self.index]
            self.index += 1
            if state == _State.DEFAULT:
                self._scan_default(c)
            elif state == _State.ESCAPE:
                self._scan_escape(c)
            elif state == _State.DOLLAR:
                self._scan_dollar(c)
            elif state == _State.PAREN:
                if (expr := self._scan_paren(c)) is not None:
                    return expr
            elif state == _State.BRACE:
                if (expr := self._scan_brace(c)) is not None:
                    return expr
            elif state == _State.SINGLE_QUOTE:
                self._scan_single_quote(c)
            elif state == _State.DOUBLE_QUOTE:
                self._scan_double_quote(c)
        if len(self.stack) > 1 and not (len(self.stack) == 2 and self.stack[1] in (_State.ESCAPE, _State.DOLLAR)):
            raise WorkflowDefinitionException(
                "Substitution error, unfinished block starting at position {}: '{}' stack was {}".format(
                    self.start, self.text[self.start:], self.stack))
        else:
            if self.end < len(self.text) is not None:
                self.parts.append(self.text[self.end:])
            return None


def interpolate(
        text: str,
        rootvars: MutableMapping[str, Any],
        timeout: float = 600,
        full_js: bool = False,
        jslib: str = "",
        strip_whitespace: bool = True,
        resolve_dependencies: bool = False) -> Optional[Any]:
    if strip_whitespace:
        text = text.strip()
    scanner = Scanner(text)
    if resolve_dependencies:
        evaluator = DependencyResolver(
            jslib=jslib,
            rootvars=rootvars,
            timeout=timeout,
            full_js=full_js)
        dependencies = set()
        while (expr := scanner.scan()) is not None:
            dependencies.update(evaluator.evaluate(expr))
        return dependencies
    else:
        evaluator = RuntimeEvaluator(
            jslib=jslib,
            rootvars=rootvars,
            timeout=timeout,
            full_js=full_js)
        while (expr := scanner.scan()) is not None:
            result = evaluator.evaluate(expr)
            if not scanner.parts and scanner.index == len(text):
                return result
            leaf = json_dumps(result, sort_keys=True)
            if leaf[0] == '"':
                leaf = json.loads(leaf)
            scanner.add_part(leaf)
        return scanner.get_result()
