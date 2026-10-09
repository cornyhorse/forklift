"""Whitelist-based expression compiler/interpreter for calculated columns.

Expressions are parsed with :mod:`ast` and *interpreted* node by node; ``eval``/``exec``/
``compile`` are never used, so an expression cannot reach Python objects, attributes or builtins.
Only the following syntax is accepted:

* literals: strings, integers, floats, ``True``/``False``/``None``
* names: column names, the constants (``PI``, ``E``, ``TRUE``, ``FALSE``, ``NULL``)
* arithmetic ``+ - * / // % **`` and unary ``+ -``
* comparisons ``== != < <= > >=``, ``in``/``not in``, ``is None``/``is not None``
* boolean ``and``/``or``/``not`` and the conditional ``a if cond else b``
* calls ``name(arg, ..., key=value)`` where ``name`` is a function from the whitelist
* tuple/list displays (as call arguments or the right side of ``in``)

Everything else (attribute access, subscripts, lambdas, comprehensions, f-strings, starred
arguments, walrus, ...) is rejected with an :class:`ExpressionError` when the expression is
compiled. Compiled expressions are cached, so each expression is parsed once.

Null semantics (SQL-like): an arithmetic operation or an ordering comparison (``< <= > >=``)
with a ``None`` operand yields ``None``. ``==``/``!=`` and the boolean operators keep Python
semantics.
"""

from __future__ import annotations

import ast
import functools
import operator
from typing import Any, Callable, FrozenSet, Mapping, Tuple

from .limits import (
    MAX_CALL_ARGUMENTS,
    MAX_EXPRESSION_LENGTH,
    MAX_EXPRESSION_NODES,
    ExpressionError,
    check_result,
    checked_mul,
    checked_pow,
)

_BINARY_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}
_UNARY_OPERATORS = {ast.USub: operator.neg, ast.UAdd: operator.pos}
_ORDERING_OPERATORS = {ast.Lt: operator.lt, ast.LtE: operator.le, ast.Gt: operator.gt}
_ORDERING_OPERATORS[ast.GtE] = operator.ge

# Node classes that may appear in an expression (operator/context nodes included).
_ALLOWED_NODES = (
    ast.Expression,
    ast.BinOp,
    ast.UnaryOp,
    ast.BoolOp,
    ast.Compare,
    ast.IfExp,
    ast.Call,
    ast.keyword,
    ast.Constant,
    ast.Name,
    ast.Tuple,
    ast.List,
    ast.Load,
    ast.And,
    ast.Or,
    ast.Not,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.USub,
    ast.UAdd,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Is,
    ast.IsNot,
)

_CONSTANT_TYPES = (str, int, float, bool, type(None))

_REJECTED_SYNTAX = {
    "Attribute": "attribute access ('.')",
    "Subscript": "indexing or slicing ('[]')",
    "Slice": "slicing",
    "Lambda": "lambda expressions",
    "ListComp": "comprehensions",
    "SetComp": "comprehensions",
    "DictComp": "comprehensions",
    "GeneratorExp": "generator expressions",
    "JoinedStr": "f-strings",
    "FormattedValue": "f-strings",
    "Starred": "star-arguments ('*')",
    "NamedExpr": "assignment expressions (':=')",
    "Dict": "dict displays",
    "Set": "set displays",
    "Await": "'await'",
    "Yield": "'yield'",
    "YieldFrom": "'yield'",
    "MatMult": "the '@' operator",
    "LShift": "shift operators",
    "RShift": "shift operators",
    "BitOr": "bitwise operators",
    "BitXor": "bitwise operators",
    "BitAnd": "bitwise operators",
    "Invert": "the '~' operator",
}


def _describe(node: ast.AST) -> str:
    name = type(node).__name__
    return _REJECTED_SYNTAX.get(name, f"'{name}' syntax")


def _is_dunder(name: str) -> bool:
    return name.startswith("__")


class CompiledExpression:
    """A validated expression tree plus the names it references.

    Attributes:
        source: The original expression text.
        names: Names used as values (columns or constants), in order of first use.
        function_names: Names used as call targets.
    """

    __slots__ = ("source", "_tree", "names", "function_names")

    def __init__(
        self,
        source: str,
        tree: ast.Expression,
        names: Tuple[str, ...],
        function_names: Tuple[str, ...],
    ):
        self.source = source
        self._tree = tree
        self.names = names
        self.function_names = function_names

    def evaluate(
        self,
        variables: Mapping[str, Any],
        functions: Mapping[str, Callable],
        constants: Mapping[str, Any],
    ) -> Any:
        """Evaluate against one row.

        Args:
            variables: Column values for this row.
            functions: Whitelisted functions callable from the expression.
            constants: Named constants (take precedence over columns of the same name).

        Raises:
            ExpressionError: On unknown names/functions or any evaluation failure.
        """
        try:
            return _Interpreter(variables, functions, constants).visit(self._tree.body)
        except RecursionError:
            raise ExpressionError("Expression is nested too deeply") from None


def compile_expression(
    source: str,
    max_length: int = MAX_EXPRESSION_LENGTH,
    max_nodes: int = MAX_EXPRESSION_NODES,
) -> CompiledExpression:
    """Parse and validate an expression (cached).

    Raises:
        ExpressionError: If the expression is too long/complex, malformed, or uses syntax that
            is not on the whitelist.
    """
    if not isinstance(source, str):
        raise ExpressionError("Expression must be a string")
    return _compile_cached(source, max_length, max_nodes)


@functools.lru_cache(maxsize=1024)
def _compile_cached(source: str, max_length: int, max_nodes: int) -> CompiledExpression:
    if len(source) > max_length:
        raise ExpressionError(f"Expression is longer than the limit of {max_length} characters")

    text = source.strip()
    if not text:
        raise ExpressionError("Expression is empty")

    try:
        tree = ast.parse(text, mode="eval")
    except (SyntaxError, ValueError, RecursionError, MemoryError) as exc:
        detail = getattr(exc, "msg", None) or type(exc).__name__
        raise ExpressionError(f"Expression is not valid: {detail}") from None

    names = []
    function_names = []
    node_count = 0
    for node in ast.walk(tree):
        if isinstance(node, (ast.expr, ast.keyword)):
            node_count += 1
            if node_count > max_nodes:
                raise ExpressionError(
                    f"Expression is more complex than the limit of {max_nodes} nodes"
                )
        if not isinstance(node, _ALLOWED_NODES):
            raise ExpressionError(f"Expression uses unsupported syntax: {_describe(node)}")
        _check_node(node, names, function_names)

    return CompiledExpression(
        source,
        tree,
        tuple(dict.fromkeys(names)),
        tuple(dict.fromkeys(function_names)),
    )


def _check_node(node: ast.AST, names: list, function_names: list) -> None:
    """Node-specific validation that goes beyond the type whitelist."""
    if isinstance(node, ast.Constant):
        if type(node.value) not in _CONSTANT_TYPES:
            raise ExpressionError(
                f"Expression uses an unsupported literal of type '{type(node.value).__name__}'"
            )
    elif isinstance(node, ast.Name):
        if _is_dunder(node.id):
            raise ExpressionError("Names starting with double underscores are not allowed")
        names.append(node.id)
    elif isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise ExpressionError("Only plain function names can be called (no 'obj.method()')")
        if _is_dunder(node.func.id):
            raise ExpressionError("Names starting with double underscores are not allowed")
        if len(node.args) + len(node.keywords) > MAX_CALL_ARGUMENTS:
            raise ExpressionError(f"A call may have at most {MAX_CALL_ARGUMENTS} arguments")
        for keyword in node.keywords:
            if keyword.arg is None:
                raise ExpressionError("Keyword-argument unpacking ('**') is not allowed")
            if _is_dunder(keyword.arg):
                raise ExpressionError("Names starting with double underscores are not allowed")
        function_names.append(node.func.id)
    elif isinstance(node, ast.Compare):
        for op, comparator in zip(node.ops, node.comparators):
            if isinstance(op, (ast.Is, ast.IsNot)) and not _is_null_literal(comparator):
                raise ExpressionError("'is' and 'is not' can only be used with None/NULL")
    elif isinstance(node, ast.BoolOp) and len(node.values) < 2:
        raise ExpressionError("Malformed boolean expression")


def _is_null_literal(node: ast.AST) -> bool:
    return (isinstance(node, ast.Constant) and node.value is None) or (
        isinstance(node, ast.Name) and node.id == "NULL"
    )


class _Interpreter:
    """Evaluates one validated expression tree for one row."""

    def __init__(
        self,
        variables: Mapping[str, Any],
        functions: Mapping[str, Callable],
        constants: Mapping[str, Any],
    ):
        self._variables = variables
        self._functions = functions
        self._constants = constants

    def visit(self, node: ast.AST) -> Any:
        method = getattr(self, "_visit_" + type(node).__name__, None)
        if method is None:  # unreachable for compiled expressions; defence in depth
            raise ExpressionError(f"Expression uses unsupported syntax: {_describe(node)}")
        return method(node)

    # -- leaves ---------------------------------------------------------------------------

    def _visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def _visit_Name(self, node: ast.Name) -> Any:
        name = node.id
        if name in self._constants:
            return self._constants[name]
        if name in self._variables:
            return self._variables[name]
        if name in self._functions:
            raise ExpressionError(f"Function '{name}' must be called with parentheses")
        raise ExpressionError(f"Unknown name '{name}'")

    def _visit_Tuple(self, node: ast.Tuple) -> Any:
        return tuple(self.visit(element) for element in node.elts)

    def _visit_List(self, node: ast.List) -> Any:
        return [self.visit(element) for element in node.elts]

    # -- operators ------------------------------------------------------------------------

    def _visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        if left is None or right is None:
            return None  # SQL-style null propagation

        op = type(node.op)
        try:
            if op is ast.Mult:
                return checked_mul(left, right)
            if op is ast.Pow:
                return checked_pow(left, right)
            if op is ast.Mod and isinstance(left, (str, bytes)):
                raise ExpressionError("String formatting with '%' is not supported")
            return check_result(_BINARY_OPERATORS[op](left, right), (left, right))
        except ExpressionError:
            raise
        except (TypeError, ArithmeticError) as exc:
            raise ExpressionError(f"Operator failed: {type(exc).__name__}: {exc}") from None

    def _visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        operand = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not operand
        if operand is None:
            return None
        try:
            return _UNARY_OPERATORS[type(node.op)](operand)
        except TypeError as exc:
            raise ExpressionError(f"Operator failed: {type(exc).__name__}: {exc}") from None

    def _visit_BoolOp(self, node: ast.BoolOp) -> Any:
        is_and = isinstance(node.op, ast.And)
        result = None
        for value_node in node.values:
            result = self.visit(value_node)
            if is_and and not result:
                return result
            if not is_and and result:
                return result
        return result

    def _visit_IfExp(self, node: ast.IfExp) -> Any:
        return self.visit(node.body if self.visit(node.test) else node.orelse)

    def _visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)
            outcome = self._compare(op, left, right)
            if outcome is None:
                return None
            if not outcome:
                return False
            left = right
        return True

    @staticmethod
    def _compare(op: ast.cmpop, left: Any, right: Any) -> Any:
        try:
            if isinstance(op, ast.Eq):
                return left == right
            if isinstance(op, ast.NotEq):
                return left != right
            if isinstance(op, ast.Is):
                return left is None
            if isinstance(op, ast.IsNot):
                return left is not None
            if isinstance(op, (ast.In, ast.NotIn)):
                if right is None:
                    return None
                contained = left in right
                return contained if isinstance(op, ast.In) else not contained
            if left is None or right is None:
                return None  # ordering comparison with NULL
            return _ORDERING_OPERATORS[type(op)](left, right)
        except TypeError as exc:
            raise ExpressionError(f"Comparison failed: {type(exc).__name__}: {exc}") from None

    # -- calls ----------------------------------------------------------------------------

    def _visit_Call(self, node: ast.Call) -> Any:
        name = node.func.id  # type: ignore[attr-defined]  # validated to be a Name
        function = self._functions.get(name)
        if function is None:
            raise ExpressionError(f"Unknown function '{name}'")

        args = [self.visit(arg) for arg in node.args]
        kwargs = {keyword.arg: self.visit(keyword.value) for keyword in node.keywords}
        try:
            result = function(*args, **kwargs)
        except ExpressionError:
            raise
        except (TypeError, ArithmeticError) as exc:
            # These messages describe types/arity only, never cell values.
            raise ExpressionError(
                f"Function '{name}' failed: {type(exc).__name__}: {exc}"
            ) from None
        except Exception as exc:  # e.g. ValueError("invalid literal ...: '<cell value>'")
            raise ExpressionError(f"Function '{name}' failed: {type(exc).__name__}") from None
        return check_result(result, list(args) + list(kwargs.values()))


# Names accepted by the compiler that callers may want to know about.
SUPPORTED_NODE_NAMES: FrozenSet[str] = frozenset(cls.__name__ for cls in _ALLOWED_NODES)
