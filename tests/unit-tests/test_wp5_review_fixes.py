"""Regression tests for the WP5 review fixes in ``forklift.processors``.

Sections (one per review item):
    1. Expression sandbox (no ``eval``; AST whitelist + limits)
    2. Null propagation
    3. Constant columns
    4. Built-in functions
    5. Calculated-columns processor / factory
    6. Row hash
    7. Regex handling
    8. data_validation
    9. schema_validator / validation_factory / constraint_validator
    10. Enhanced processor / constraint validation
    11. Column mapper
    12. Bad-rows handlers
    13. Write-time validator, quality, transformations, shared base classes
"""

import ast
import hashlib
import json
import logging
import os
import time
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from forklift.processors._regex import (
    MAX_PATTERN_LENGTH,
    UnsafeRegexError,
    compile_pattern,
    pattern_matches,
)
from forklift.processors.bad_rows_handler import BadRowsConfig as TopLevelBadRowsConfig
from forklift.processors.bad_rows_handler import BadRowsHandler as TopLevelBadRowsHandler
from forklift.processors.calculated_columns import (
    CalculatedColumn,
    CalculatedColumnsConfig,
    CalculatedColumnsProcessor,
    ConstantColumn,
    ExpressionColumn,
    ExpressionEvaluator,
    get_available_functions,
    safe_eval,
)
from forklift.processors.calculated_columns_factory import (
    _parse_data_type,
    create_calculated_columns_processor_from_schema,
)
from forklift.processors.column_mapper import ColumnMapper, ColumnMappingConfig
from forklift.processors.constraint_validator import (
    ConstraintConfig,
    ConstraintValidator,
    ErrorMode,
    create_constraint_config_from_schema,
)
from forklift.processors.data_validation import (
    BadRowsConfig,
    BadRowsHandler,
    DataValidationProcessor,
    DateValidation,
    EnumValidation,
    FieldValidationRule,
    RangeValidation,
    StringValidation,
    ValidationConfig,
    ValidationRules,
)
from forklift.processors.data_validation.data_validation_processor import (
    BadRowsThresholdExceededError,
    ValidationProcessingError,
)
from forklift.processors.enhanced_processor import create_enhanced_processor_from_schema_file
from forklift.processors.pipeline import ProcessorPipeline
from forklift.processors.quality import DataQualityProcessor
from forklift.processors.row_hash import (
    HASH_VERSION_METADATA_KEY,
    RowHashConfig,
    RowHashProcessor,
)
from forklift.processors.row_hash_factory import create_row_hash_processor_from_schema
from forklift.processors.schema_validator import (
    SchemaValidationMode,
    SchemaValidator,
    SchemaValidatorConfig,
)
from forklift.processors.schema_validator.constraints import (
    ConstraintValidator as ValueConstraints,
)
from forklift.processors.schema_validator.type_converter import (
    TypeConverter,
    parse_arrow_type,
)
from forklift.processors.transformations import ColumnTransformer, uppercase
from forklift.processors.validation_factory import (
    ValidationFactory,
    create_validation_processor_from_schema,
)
from forklift.processors.write_time_validator import WriteTimeConfig, WriteTimeValidator


def _batch(**columns):
    return pa.RecordBatch.from_pydict(columns)


def _processor(*cols, **config_kwargs):
    return CalculatedColumnsProcessor(CalculatedColumnsConfig(columns=list(cols), **config_kwargs))


# ---------------------------------------------------------------------------------------------
# 1. Expression sandbox
# ---------------------------------------------------------------------------------------------

ESCAPE_PAYLOADS = [
    # attribute access / dunder chains
    "abs.__globals__",
    "abs.__globals__['__builtins__']['__import__']('os').popen('id').read()",
    "().__class__.__bases__[0].__subclasses__()",
    "x.__class__",
    "'a'.__class__.__mro__[1].__subclasses__()",
    "upper.__code__",
    "'{0.__class__}'.format(x)",
    "''.join(['a'])",
    # imports / builtins by name
    "__import__('os').system('id')",
    "__builtins__",
    "__class__",
    # lambdas
    "(lambda: __import__('os'))()",
    "(lambda x: x)(1)",
    # comprehensions / generators
    "[c for c in ().__class__.__mro__]",
    "{k: 1 for k in (1, 2)}",
    "{k for k in (1, 2)}",
    "sum(i for i in (1, 2, 3))",
    # f-strings, walrus, starred, subscript, containers
    "f'{abs.__globals__}'",
    "(y := 1)",
    "[*(1, 2)]",
    "max(*(1, 2))",
    "round(x, **{'digits': 1})",
    "x[0]",
    "'abc'[::-1]",
    "{'a': 1}",
    "{1, 2}",
    # non-whitelisted literals / operators
    "b'abc'",
    "1j",
    "...",
    "x << 2",
    "x & 1",
    "x | 1",
    "~x",
    "x @ x",
    "await x",
    "x is 5",
    # not a plain-name callee
    "abs(x)(1)",
    "(abs if x else upper)(x)",
]


class TestExpressionSandbox:
    @pytest.mark.parametrize("payload", ESCAPE_PAYLOADS)
    def test_compile_rejects_escape_payloads(self, payload):
        evaluator = ExpressionEvaluator()
        with pytest.raises(ValueError, match="Expression evaluation failed"):
            evaluator.compile(payload)

    @pytest.mark.parametrize("payload", ESCAPE_PAYLOADS)
    def test_processor_rejects_escape_payloads_at_configuration(self, payload):
        # Unsafe expressions are configuration errors: they raise even with fail_on_error=False
        for fail_on_error in (True, False):
            with pytest.raises(ValueError, match="Invalid expression for column 'bad'"):
                _processor(
                    CalculatedColumn(name="bad", expression=payload), fail_on_error=fail_on_error
                )

    @pytest.mark.parametrize("payload", ESCAPE_PAYLOADS)
    def test_validate_expression_is_false_for_escape_payloads(self, payload):
        assert ExpressionEvaluator().validate_expression(payload, {"x": 1}) is False

    def test_attribute_escape_never_runs_a_command(self, tmp_path):
        marker = tmp_path / "marker"
        payload = (
            "abs.__globals__['__builtins__']['__import__']('os')" f".system('touch {marker}')"
        )
        batch = _batch(x=[1])
        evaluator = ExpressionEvaluator()
        with pytest.raises(ValueError):
            evaluator.evaluate_expression(batch, 0, payload)
        with pytest.raises(ValueError):
            evaluator.calculate_column_values(batch, CalculatedColumn("c", payload))
        assert not marker.exists()

    @pytest.mark.parametrize(
        "expression",
        ["eval('1')", "exec('x=1')", "open('/etc/passwd')", "globals()", "getattr(x, 'real')"],
    )
    def test_builtins_are_not_callable(self, expression):
        # Syntactically fine, but only the whitelisted functions exist
        batch = _batch(x=[1])
        evaluator = ExpressionEvaluator()
        with pytest.raises(ValueError, match="Unknown function"):
            evaluator.evaluate_expression(batch, 0, expression)
        assert ExpressionEvaluator().validate_expression(expression, {"x": 1}) is False

    def test_function_used_without_call_is_an_error_not_a_function_object(self):
        batch = _batch(x=[1])
        with pytest.raises(ValueError, match="must be called"):
            ExpressionEvaluator().evaluate_expression(batch, 0, "abs")

    def test_no_eval_in_evaluator_sources(self):
        import inspect

        from forklift.processors.calculated_columns import evaluator
        from forklift.processors.calculated_columns import safe_eval as se

        for module in (evaluator, se):
            tree = ast.parse(inspect.getsource(module))
            called = {
                n.func.id
                for n in ast.walk(tree)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            }
            assert not called & {"eval", "exec", "compile", "__import__"}

    def test_documented_expressions_keep_working(self):
        batch = _batch(
            first_name=["Ann"],
            last_name=["Lee"],
            order_total=[200.0],
            discount_percent=[10],
            street=["  1 Main St "],
            city=["Oslo"],
            birth=[date(2000, 2, 3)],
            x=[3],
            y=[4],
        )
        ev = ExpressionEvaluator()
        cases = {
            "first_name + ' ' + last_name": "Ann Lee",
            "order_total * (discount_percent / 100.0)": 20.0,
            "upper(trim(street)) + ', ' + upper(city)": "1 MAIN ST, OSLO",
            "year(birth)": 2000,
            "x ** 2 + y ** 2": 25,
            "x // 2": 1,
            "x % 2": 1,
            "-x": -3,
            "if_then_else(greater_than(x, 2), multiply(x, y), add(x, y))": 12,
            "'big' if x > 2 else 'small'": "big",
            "x > 2 and y > 3": True,
            "x > 5 or y > 3": True,
            "not (x > 5)": True,
            "1 < x < 5": True,
            "coalesce(NULL, x)": 3,
            "round(PI, 2)": 3.14,
            "TRUE": True,
            "x in (1, 2, 3)": True,
            "concat(first_name, '-', last_name)": "Ann-Lee",
            "substring(first_name, 1, 2)": "nn",
        }
        for expression, expected in cases.items():
            assert ev.evaluate_expression(batch, 0, expression) == expected, expression

    def test_column_value_wins_over_function_of_same_name(self):
        # A column called "sum" or "day" used to be shadowed by the function of that name
        batch = _batch(sum=[5], day=[2])
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(batch, 0, "sum + day") == 7
        assert ev.evaluate_expression(batch, 0, "sum(sum, day)") == 7

    def test_expression_is_parsed_once_per_expression(self, monkeypatch):
        calls = []
        real_parse = ast.parse
        monkeypatch.setattr(
            safe_eval.ast, "parse", lambda *a, **k: calls.append(a) or real_parse(*a, **k)
        )
        batch = _batch(x=list(range(200)))
        ev = ExpressionEvaluator()
        values = ev.calculate_column_values(
            batch, CalculatedColumn("c", "x * 31337 + 1", pa.int64())
        )
        assert values.to_pylist() == [i * 31337 + 1 for i in range(200)]
        assert len(calls) == 1

    # -- limits ------------------------------------------------------------------------------

    def test_expression_length_limit(self):
        with pytest.raises(ValueError, match="longer than the limit"):
            ExpressionEvaluator(max_expression_length=20).compile("x + " + "1 + " * 10 + "1")

    def test_node_count_limit(self):
        with pytest.raises(ValueError, match="more complex than the limit"):
            ExpressionEvaluator(max_expression_nodes=10).compile(" + ".join(["x"] * 20))

    def test_default_limits_reject_huge_expressions(self):
        with pytest.raises(ValueError):
            ExpressionEvaluator().compile(" + ".join(["x"] * 1000))
        with pytest.raises(ValueError):
            ExpressionEvaluator().compile("-" * 3000 + "x")

    def test_empty_expression_rejected(self):
        with pytest.raises(ValueError, match="empty"):
            ExpressionEvaluator().compile("   ")

    def test_syntax_error_is_value_error(self):
        with pytest.raises(ValueError, match="not valid"):
            ExpressionEvaluator().compile("CASE WHEN a THEN b END")

    @pytest.mark.parametrize(
        "expression",
        [
            "2 ** 100000",
            "10 ** 10 ** 10",
            "x ** 3000000",
            "power(7, 3000000)",
            "power(x, 20000)",
            "'ab' * 10 ** 9",
            "multiply('ab', 10 ** 9)",
            "replace('abc', '', 'x' * 900000)",
            "round(5, -1000000000)",
            "round(x, 10 ** 9)",
        ],
    )
    def test_resource_bombs_are_rejected_quickly(self, expression):
        batch = _batch(x=[7])
        start = time.perf_counter()
        with pytest.raises(ValueError):
            ExpressionEvaluator().evaluate_expression(batch, 0, expression)
        assert time.perf_counter() - start < 0.5

    def test_large_but_bounded_integer_products_still_work(self):
        batch = _batch(x=[7])
        assert ExpressionEvaluator().evaluate_expression(batch, 0, "x" + " * x" * 60) == 7**61

    def test_string_growth_through_repeated_concatenation_is_bounded(self):
        ev = ExpressionEvaluator()
        # Genuinely large cell values keep working (result may exceed the limit by the input size)
        batch = _batch(s=["a" * 400_000])
        assert len(ev.evaluate_expression(batch, 0, "s + s + s")) == 1_200_000
        assert len(ev.evaluate_expression(batch, 0, "upper(s)")) == 400_000
        # ... but amplification beyond that is refused
        with pytest.raises(ValueError, match="maximum length"):
            ev.evaluate_expression(batch, 0, "('x' * 900000) * 3")
        with pytest.raises(ValueError, match="maximum length"):
            ev.evaluate_expression(batch, 0, "concat(" + ", ".join(["'x' * 999999"] * 4) + ")")

    def test_string_percent_formatting_is_rejected(self):
        with pytest.raises(ValueError, match="not supported"):
            ExpressionEvaluator().evaluate_expression(_batch(x=[1]), 0, "'%999999999d' % x")

    def test_negative_base_fractional_power_is_not_complex(self):
        with pytest.raises(ValueError, match="not a real number"):
            ExpressionEvaluator().evaluate_expression(_batch(x=[-8]), 0, "x ** 0.5")

    def test_reasonable_powers_still_work(self):
        ev = ExpressionEvaluator()
        batch = _batch(x=[2])
        assert ev.evaluate_expression(batch, 0, "x ** 64") == 2**64
        assert ev.evaluate_expression(batch, 0, "power(10, 18)") == 10**18
        assert ev.evaluate_expression(batch, 0, "x ** -2") == 0.25

    def test_error_messages_do_not_contain_cell_values(self):
        batch = _batch(s=["secret-value-123"])
        ev = ExpressionEvaluator()
        for expression in ("to_int(s)", "s + 1", "s < 3", "to_float(s)", "to_bool(s)"):
            with pytest.raises(ValueError) as exc_info:
                ev.evaluate_expression(batch, 0, expression)
            assert "secret-value-123" not in str(exc_info.value)

    def test_conversion_error_does_not_leak_values(self):
        batch = _batch(s=["secret-value-123"])
        ev = ExpressionEvaluator()
        with pytest.raises(ValueError) as exc_info:
            ev.calculate_column_values(batch, CalculatedColumn("c", "s", pa.int64()))
        assert "secret-value-123" not in str(exc_info.value)
        assert "cannot be converted" in str(exc_info.value)


# ---------------------------------------------------------------------------------------------
# 2. Null propagation
# ---------------------------------------------------------------------------------------------


class TestNullPropagation:
    def test_name_containing_function_name_still_propagates_null(self):
        # "cost" contains "cos", which used to disable the null handling entirely
        batch = _batch(price=[None, 10.0], cost=[2.0, 3.0])
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(batch, 0, "price - cost") is None
        assert ev.evaluate_expression(batch, 1, "price - cost") == 7.0

    @pytest.mark.parametrize(
        "name", ["cost", "order_total", "birthday", "minutes", "day_count", "summary", "ceiling"]
    )
    def test_null_propagation_does_not_depend_on_column_names(self, name):
        batch = _batch(**{name: [None, 4], "q": [1, 2]})
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(batch, 0, f"{name} + q") is None
        assert ev.evaluate_expression(batch, 0, f"q * {name}") is None
        assert ev.evaluate_expression(batch, 1, f"{name} + q") == 6

    def test_same_result_with_and_without_function_names_in_columns(self):
        with_function_like = _batch(price=[None], cost=[1.0])
        plain = _batch(p=[None], q=[1.0])
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(with_function_like, 0, "price - cost") is None
        assert ev.evaluate_expression(plain, 0, "p - q") is None

    def test_all_arithmetic_operators_propagate_null(self):
        batch = _batch(a=[None], b=[3])
        ev = ExpressionEvaluator()
        for op in ("+", "-", "*", "/", "//", "%", "**"):
            assert ev.evaluate_expression(batch, 0, f"a {op} b") is None, op
            assert ev.evaluate_expression(batch, 0, f"b {op} a") is None, op
        assert ev.evaluate_expression(batch, 0, "-a") is None

    def test_ordering_comparisons_with_null_are_null(self):
        batch = _batch(a=[None], b=[3])
        ev = ExpressionEvaluator()
        for op in ("<", "<=", ">", ">="):
            assert ev.evaluate_expression(batch, 0, f"a {op} b") is None, op
            assert ev.evaluate_expression(batch, 0, f"b {op} a") is None, op

    def test_equality_keeps_python_semantics(self):
        batch = _batch(a=[None], b=[3])
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(batch, 0, "a == b") is False
        assert ev.evaluate_expression(batch, 0, "a != b") is True
        assert ev.evaluate_expression(batch, 0, "a == NULL") is True
        assert ev.evaluate_expression(batch, 0, "a is None") is True
        assert ev.evaluate_expression(batch, 0, "b is not None") is True

    def test_boolean_operators_keep_python_semantics(self):
        batch = _batch(a=[None], b=[3])
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(batch, 0, "a or b") == 3
        assert ev.evaluate_expression(batch, 0, "a and b") is None
        assert ev.evaluate_expression(batch, 0, "not a") is True

    def test_null_in_function_arguments_is_handled_by_the_function(self):
        batch = _batch(a=[None], b=[3])
        ev = ExpressionEvaluator()
        assert ev.evaluate_expression(batch, 0, "coalesce(a, 0) + b") == 3
        assert ev.evaluate_expression(batch, 0, "upper(a)") is None

    def test_null_propagation_through_processor(self):
        proc = _processor(CalculatedColumn("net", "price - cost", pa.float64()))
        out, results = proc.process_batch(_batch(price=[None, 10.0], cost=[2.0, 3.0]))
        assert out.column("net").to_pylist() == [None, 7.0]
        assert results == []


# ---------------------------------------------------------------------------------------------
# 3. Constant columns
# ---------------------------------------------------------------------------------------------


class TestConstantColumns:
    @pytest.mark.parametrize(
        "value",
        [
            "O'Brien",
            "C:\\new\\table",
            "x' + upper('y') + 'z",
            "__import__('os').system('id')",
            "line1\nline2",
            'say "hi"',
            "tab\tchar",
            "",
        ],
    )
    def test_string_constants_are_returned_verbatim(self, value):
        proc = _processor(
            ConstantColumn(name="c", value=value, data_type=pa.string()).to_calculated_column()
        )
        out, results = proc.process_batch(_batch(x=[1, 2]))
        assert out.column("c").to_pylist() == [value, value]
        assert results == []

    def test_injection_payload_does_not_execute(self):
        value = "x' + upper('y') + 'z"
        proc = _processor(
            ConstantColumn(name="c", value=value, data_type=pa.string()).to_calculated_column()
        )
        out, _ = proc.process_batch(_batch(x=[1]))
        assert out.column("c").to_pylist() == [value]
        assert out.column("c").to_pylist() != ["xYz"]

    def test_backslash_sequences_are_not_interpreted(self):
        value = "C:\\new\\table"
        proc = _processor(
            ConstantColumn(name="c", value=value, data_type=pa.string()).to_calculated_column()
        )
        out, _ = proc.process_batch(_batch(x=[1]))
        assert out.column("c")[0].as_py() == "C:\\new\\table"
        assert "\n" not in out.column("c")[0].as_py()

    def test_date_constant_is_a_date_not_arithmetic(self):
        col = ConstantColumn(name="d", value=date(2024, 1, 31), data_type=pa.date32())
        proc = _processor(col.to_calculated_column())
        out, _ = proc.process_batch(_batch(x=[1, 2]))
        assert out.column("d").to_pylist() == [date(2024, 1, 31)] * 2
        assert out.schema.field("d").type == pa.date32()

    @pytest.mark.parametrize(
        "value,data_type",
        [(42, pa.int64()), (3.5, pa.float64()), (True, pa.bool_()), (None, pa.string())],
    )
    def test_scalar_constants(self, value, data_type):
        col = ConstantColumn(name="c", value=value, data_type=data_type)
        proc = _processor(col.to_calculated_column())
        out, _ = proc.process_batch(_batch(x=[1, 2, 3]))
        assert out.column("c").to_pylist() == [value] * 3

    def test_expression_rendering_is_a_safe_literal(self):
        for value in ("O'Brien", "C:\\new\\table", "x' + upper('y') + 'z", 42, True, None, 1.5):
            calc = ConstantColumn(name="c", value=value).to_calculated_column()
            assert calc.is_constant
            assert ast.literal_eval(calc.expression) == value

    def test_constant_via_schema_factory(self):
        proc = create_calculated_columns_processor_from_schema(
            {"constants": [{"name": "who", "value": "O'Brien", "dataType": "string"}]}
        )
        out, _ = proc.process_batch(_batch(x=[1]))
        assert out.column("who").to_pylist() == ["O'Brien"]

    def test_constant_can_be_used_by_later_expression(self):
        proc = _processor(
            ConstantColumn(
                name="who", value="O'Brien", data_type=pa.string()
            ).to_calculated_column(),
            CalculatedColumn("greeting", "'Hi ' + who", pa.string(), dependencies=["who"]),
        )
        out, _ = proc.process_batch(_batch(x=[1]))
        assert out.column("greeting").to_pylist() == ["Hi O'Brien"]


# ---------------------------------------------------------------------------------------------
# 4. Built-in functions
# ---------------------------------------------------------------------------------------------


class TestFunctions:
    def setup_method(self):
        self.f = get_available_functions()

    @pytest.mark.parametrize("text", ["false", "False", "FALSE", " no ", "0", "n", "off", "f"])
    def test_to_bool_false_strings(self, text):
        assert self.f["to_bool"](text) is False

    @pytest.mark.parametrize("text", ["true", "True", "yes", "1", "y", "on", "t"])
    def test_to_bool_true_strings(self, text):
        assert self.f["to_bool"](text) is True

    def test_to_bool_other_values(self):
        assert self.f["to_bool"](None) is None
        assert self.f["to_bool"]("") is None
        assert self.f["to_bool"](1) is True
        assert self.f["to_bool"](0) is False
        assert self.f["to_bool"](True) is True
        with pytest.raises(ValueError):
            self.f["to_bool"]("maybe")

    def test_to_bool_unknown_string_is_error_in_expression(self):
        batch = _batch(s=["maybe"])
        with pytest.raises(ValueError, match="to_bool"):
            ExpressionEvaluator().evaluate_expression(batch, 0, "to_bool(s)")

    def test_substring_without_length(self):
        assert self.f["substring"]("hello", 1) == "ello"
        assert self.f["substring"]("hello", 1, 3) == "ell"
        assert self.f["substring"]("hello", 10) == ""
        assert self.f["substring"](None, 1) is None
        assert self.f["substring"]("hello", None) is None
        assert self.f["substring"]("hello", -3) == "llo"
        assert self.f["substring"]("hello", -3, 2) == "ll"

    def test_substring_in_expression(self):
        batch = _batch(s=["hello"])
        assert ExpressionEvaluator().evaluate_expression(batch, 0, "substring(s, 2)") == "llo"

    def test_right_and_left_with_zero_or_negative(self):
        assert self.f["right"]("hello", 0) == ""
        assert self.f["right"]("hello", -2) == ""
        assert self.f["right"]("hello", 2) == "lo"
        assert self.f["right"]("hello", 99) == "hello"
        assert self.f["left"]("hello", 0) == ""
        assert self.f["left"]("hello", -1) == ""
        assert self.f["left"]("hello", 2) == "he"
        assert self.f["right"](None, 2) is None
        assert self.f["left"]("hello", None) is None

    def test_sum_of_all_nulls_is_null(self):
        assert self.f["sum"](None, None) is None
        assert self.f["sum"]() is None
        assert self.f["sum"](1, None, 2) == 3
        # consistent with min/max/avg
        assert self.f["min"](None, None) is None
        assert self.f["max"](None, None) is None
        assert self.f["avg"](None, None) is None

    def test_multiply_and_power_are_capped(self):
        start = time.perf_counter()
        with pytest.raises(ValueError):
            self.f["power"](7, 3_000_000)
        with pytest.raises(ValueError):
            self.f["multiply"]("ab", 10**9)
        assert time.perf_counter() - start < 0.5
        assert self.f["power"](2, 10) == 1024
        assert self.f["multiply"](6, 7) == 42
        assert self.f["power"](None, 2) is None

    def test_replace_growth_is_capped(self):
        with pytest.raises(ValueError):
            self.f["replace"]("a" * 500_000, "", "xyz")
        assert self.f["replace"]("hello", "l", "L") == "heLLo"

    def test_round_uses_bankers_rounding(self):
        # documented behaviour: ties go to the even neighbour (Python's round)
        assert self.f["round"](0.5) == 0
        assert self.f["round"](1.5) == 2
        assert self.f["round"](2.5) == 2
        assert self.f["round"](3.14159, 2) == 3.14

    def test_now_and_today_are_one_snapshot_per_batch(self):
        proc = _processor(
            CalculatedColumn("t1", "now()", pa.timestamp("us")),
            CalculatedColumn("t2", "now()", pa.timestamp("us")),
            CalculatedColumn("d", "today()", pa.date32()),
        )
        out, _ = proc.process_batch(_batch(x=list(range(2000))))
        t1 = set(out.column("t1").to_pylist())
        t2 = set(out.column("t2").to_pylist())
        assert len(t1) == 1 and t1 == t2
        assert out.column("d").to_pylist()[0] == next(iter(t1)).date()

    def test_snapshot_changes_between_batches(self):
        proc = _processor(CalculatedColumn("t", "now()", pa.timestamp("us")))
        first, _ = proc.process_batch(_batch(x=[1]))
        time.sleep(0.01)
        second, _ = proc.process_batch(_batch(x=[1]))
        assert first.column("t")[0].as_py() < second.column("t")[0].as_py()

    def test_now_outside_a_run_still_works(self):
        before = datetime.now()
        value = ExpressionEvaluator().evaluate_expression(_batch(x=[1]), 0, "now()")
        assert before <= value <= datetime.now()


# ---------------------------------------------------------------------------------------------
# 5. Calculated-columns processor and factory
# ---------------------------------------------------------------------------------------------


class TestCalculatedColumnsProcessorFixes:
    def test_calculated_column_named_like_input_column_raises(self):
        proc = _processor(CalculatedColumn("x", "x + 1", pa.int64()))
        with pytest.raises(ValueError, match="collides"):
            proc.process_batch(_batch(x=[1]))

    def test_duplicate_calculated_column_names_raise_at_configuration(self):
        with pytest.raises(ValueError, match="Duplicate calculated column name 'a'"):
            _processor(CalculatedColumn("a", "x + 1"), CalculatedColumn("a", "x + 2"))

    def test_collision_raises_even_without_fail_on_error(self):
        proc = _processor(CalculatedColumn("x", "x + 1"), fail_on_error=False)
        with pytest.raises(ValueError, match="collides"):
            proc.process_batch(_batch(x=[1]))

    def test_fail_on_error_true_raises_instead_of_returning_original_batch(self):
        proc = _processor(CalculatedColumn("c", "to_int(s)", pa.int64()))
        with pytest.raises(ValueError, match="Failed to calculate column 'c'"):
            proc.process_batch(_batch(s=["abc"]))

    def test_fail_on_error_false_still_yields_null_column_and_result(self):
        proc = _processor(CalculatedColumn("c", "to_int(s)", pa.int64()), fail_on_error=False)
        out, results = proc.process_batch(_batch(s=["abc", "7"]))
        # Row-level failures become NULL when fail_on_error is off
        assert out.column("c").to_pylist() == [None, 7]
        assert results == []

    def test_failure_does_not_emit_a_partially_processed_batch(self):
        proc = _processor(
            CalculatedColumn("ok", "x + 1", pa.int64()),
            CalculatedColumn("bad", "unknown_fn(x)", pa.int64()),
        )
        with pytest.raises(ValueError):
            proc.process_batch(_batch(x=[1]))

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("int", pa.int64()),
            ("integer", pa.int64()),
            ("INT", pa.int64()),
            ("bigint", pa.int64()),
            ("smallint", pa.int16()),
            ("float", pa.float64()),
            ("decimal(10,2)", pa.decimal128(10, 2)),
            ("decimal(10, 2)", pa.decimal128(10, 2)),
            ("numeric(12,4)", pa.decimal128(12, 4)),
            ("decimal128(10,2)", pa.decimal128(10, 2)),
            ("date", pa.date32()),
            ("datetime", pa.timestamp("us")),
            ("timestamp[us]", pa.timestamp("us")),
            ("str", pa.string()),
            ("varchar(20)", pa.string()),
            ("list<int>", pa.list_(pa.int64())),
        ],
    )
    def test_data_type_aliases(self, text, expected):
        assert _parse_data_type(text) == expected

    @pytest.mark.parametrize(
        "text", ["intt", "decimal", "decimal(a,b)", "decimal(99,2)", "wibble", "list<wibble>"]
    )
    def test_unknown_data_types_raise(self, text):
        with pytest.raises(ValueError, match="dataType|Unknown|Invalid"):
            _parse_data_type(text)

    def test_unknown_data_type_in_schema_raises(self):
        with pytest.raises(ValueError):
            create_calculated_columns_processor_from_schema(
                {"expressions": [{"name": "c", "expression": "x", "dataType": "no_such_type"}]}
            )

    def test_schema_data_type_alias_drives_output_type(self):
        proc = create_calculated_columns_processor_from_schema(
            {"expressions": [{"name": "c", "expression": "x * 2", "dataType": "integer"}]}
        )
        out, _ = proc.process_batch(_batch(x=[1, 2]))
        assert out.schema.field("c").type == pa.int64()
        assert out.column("c").to_pylist() == [2, 4]

    def test_expression_column_round_trip_still_works(self):
        col = ExpressionColumn(name="c", expression="x + 1", data_type=pa.int64())
        proc = _processor(col.to_calculated_column())
        out, _ = proc.process_batch(_batch(x=[1]))
        assert out.column("c").to_pylist() == [2]

    def test_validate_expressions_uses_safe_evaluator(self):
        proc = _processor(
            CalculatedColumn("ok", "x + 1"),
            ConstantColumn(name="k", value="v").to_calculated_column(),
        )
        results = proc.validate_expressions({"x": 1})
        assert [r.is_valid for r in results] == [True, True]


# ---------------------------------------------------------------------------------------------
# 6. Row hash
# ---------------------------------------------------------------------------------------------


def _hash_of(batch, **config_kwargs):
    processor = RowHashProcessor(RowHashConfig(enabled=True, **config_kwargs))
    out, _ = processor.process_batch(batch)
    return out.column("row_hash").to_pylist()


class TestRowHashEncoding:
    def test_separator_collision_is_gone(self):
        a = _batch(f=["Ann"], g=["Lee||X"], h=["111"])
        b = _batch(f=["Ann||Lee"], g=["X"], h=["111"])
        assert _hash_of(a) != _hash_of(b)
        # ... while the legacy encoding still collides (documented, reproducible)
        assert _hash_of(a, legacy_encoding=True) == _hash_of(b, legacy_encoding=True)

    def test_null_marker_differs_from_the_string_null(self):
        null_row = pa.RecordBatch.from_arrays([pa.array([None], pa.string())], names=["s"])
        text_row = _batch(s=["NULL"])
        assert _hash_of(null_row) != _hash_of(text_row)
        assert _hash_of(null_row, legacy_encoding=True) == _hash_of(text_row, legacy_encoding=True)

    def test_empty_bytes_are_not_null(self):
        empty = pa.RecordBatch.from_arrays([pa.array([b""], pa.binary())], names=["b"])
        null = pa.RecordBatch.from_arrays([pa.array([None], pa.binary())], names=["b"])
        assert _hash_of(empty) != _hash_of(null)
        assert _hash_of(empty, legacy_encoding=True) == _hash_of(null, legacy_encoding=True)

    def test_type_tag_separates_int_and_string(self):
        assert _hash_of(_batch(v=[1])) != _hash_of(_batch(v=["1"]))
        assert _hash_of(_batch(v=[1]), legacy_encoding=True) == _hash_of(
            _batch(v=["1"]), legacy_encoding=True
        )
        assert _hash_of(_batch(v=[True])) != _hash_of(_batch(v=["True"]))
        assert _hash_of(_batch(v=[1.0])) != _hash_of(_batch(v=[1]))

    def test_column_names_are_part_of_the_hash(self):
        assert _hash_of(_batch(a=["x"], b=["y"])) != _hash_of(_batch(c=["x"], d=["y"]))
        # swapping values between columns changes the hash
        assert _hash_of(_batch(a=["x"], b=["y"])) != _hash_of(_batch(a=["y"], b=["x"]))
        # column order matters
        assert _hash_of(_batch(a=["x"], b=["y"])) != _hash_of(_batch(b=["y"], a=["x"]))

    def test_integer_width_does_not_change_the_hash(self):
        small = pa.RecordBatch.from_arrays([pa.array([5], pa.int32())], names=["n"])
        wide = pa.RecordBatch.from_arrays([pa.array([5], pa.int64())], names=["n"])
        assert _hash_of(small) == _hash_of(wide)

    def test_hash_is_deterministic_and_row_wise(self):
        batch = _batch(id=[1, 2, 1], name=["a", "b", "a"])
        hashes = _hash_of(batch)
        assert hashes[0] == hashes[2] != hashes[1]
        assert hashes == _hash_of(batch)
        assert all(len(h) == 64 for h in hashes)

    def test_float_edge_values(self):
        nan1 = pa.RecordBatch.from_arrays([pa.array([float("nan")])], names=["f"])
        nan2 = pa.RecordBatch.from_arrays([pa.array([-float("nan")])], names=["f"])
        zero = pa.RecordBatch.from_arrays([pa.array([0.0])], names=["f"])
        neg_zero = pa.RecordBatch.from_arrays([pa.array([-0.0])], names=["f"])
        assert _hash_of(nan1) == _hash_of(nan2)
        assert _hash_of(zero) == _hash_of(neg_zero)
        assert _hash_of(zero) != _hash_of(nan1)

    def test_other_types_are_hashable(self):
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([Decimal("1.50")], pa.decimal128(5, 2)),
                pa.array([date(2024, 1, 31)], pa.date32()),
                pa.array([datetime(2024, 1, 31, 12, 0, 0)], pa.timestamp("ns")),
                pa.array([[1, 2]], pa.list_(pa.int64())),
                pa.array(["x"], pa.string()).dictionary_encode(),
            ],
            names=["d", "dt", "ts", "l", "dict"],
        )
        hashes = _hash_of(batch)
        assert len(hashes) == 1 and len(hashes[0]) == 64
        # dictionary-encoded strings hash like plain strings
        plain = pa.RecordBatch.from_arrays([pa.array(["x"])], names=["dict"])
        encoded = pa.RecordBatch.from_arrays([pa.array(["x"]).dictionary_encode()], names=["dict"])
        assert _hash_of(plain) == _hash_of(encoded)

    def test_legacy_encoding_reproduces_the_original_preimage(self):
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, None]),
                pa.array(["Alice", "Bob"]),
                pa.array([b"\x01\xff", b""], pa.binary()),
            ],
            names=["id", "name", "blob"],
        )
        expected = [
            hashlib.sha256("1||Alice||01ff".encode()).hexdigest(),
            hashlib.sha256("NULL||Bob||NULL".encode()).hexdigest(),
        ]
        assert _hash_of(batch, legacy_encoding=True) == expected

        custom = _hash_of(batch, legacy_encoding=True, separator="|", null_value="<null>")
        assert custom[1] == hashlib.sha256("<null>|Bob|<null>".encode()).hexdigest()

    def test_legacy_encoding_with_weak_algorithm_matches_old_values(self):
        batch = _batch(a=["x"], b=["y"])
        out = _hash_of(batch, legacy_encoding=True, algorithm="md5", allow_weak_hash=True)
        assert out == [hashlib.md5(b"x||y").hexdigest()]


class TestRowHashVersioning:
    def test_hash_version_in_config(self):
        assert RowHashConfig().hash_version == 2
        assert RowHashConfig(legacy_encoding=True).hash_version == 1

    def test_hash_version_recorded_on_output_field(self):
        for legacy, version in ((False, b"2"), (True, b"1")):
            processor = RowHashProcessor(RowHashConfig(enabled=True, legacy_encoding=legacy))
            out, _ = processor.process_batch(_batch(a=[1]))
            field = out.schema.field("row_hash")
            assert field.metadata[HASH_VERSION_METADATA_KEY] == version
            assert field.metadata[b"forklift.row_hash.algorithm"] == b"sha256"
            schema_field = processor.get_output_schema(_batch(a=[1]).schema).field("row_hash")
            assert schema_field.metadata[HASH_VERSION_METADATA_KEY] == version
            assert processor.get_hash_info()["hash_version"] == int(version)

    def test_hash_version_survives_parquet(self, tmp_path):
        processor = RowHashProcessor(RowHashConfig(enabled=True))
        out, _ = processor.process_batch(_batch(a=[1, 2]))
        path = tmp_path / "out.parquet"
        pq.write_table(pa.Table.from_batches([out]), path)
        field = pq.read_schema(path).field("row_hash")
        assert field.metadata[HASH_VERSION_METADATA_KEY] == b"2"

    def test_factory_options(self):
        processor = create_row_hash_processor_from_schema({"enabled": True})
        assert processor.config.hash_version == 2
        processor = create_row_hash_processor_from_schema({"enabled": True, "hashVersion": 1})
        assert processor.config.legacy_encoding is True
        processor = create_row_hash_processor_from_schema(
            {"enabled": True, "legacyEncoding": True}
        )
        assert processor.config.hash_version == 1
        with pytest.raises(ValueError, match="hashVersion"):
            create_row_hash_processor_from_schema({"enabled": True, "hashVersion": 3})
        with pytest.raises(ValueError, match="contradict"):
            create_row_hash_processor_from_schema(
                {"enabled": True, "hashVersion": 2, "legacyEncoding": True}
            )

    @pytest.mark.parametrize("algorithm", ["md5", "sha1"])
    def test_weak_algorithms_need_explicit_opt_in(self, algorithm):
        with pytest.raises(ValueError, match="allow_weak_hash"):
            RowHashConfig(enabled=True, algorithm=algorithm)
        assert RowHashConfig(enabled=True, algorithm=algorithm, allow_weak_hash=True)
        with pytest.raises(ValueError, match="allow_weak_hash"):
            create_row_hash_processor_from_schema({"enabled": True, "algorithm": algorithm})
        processor = create_row_hash_processor_from_schema(
            {"enabled": True, "algorithm": algorithm, "allowWeakHash": True}
        )
        assert processor.config.algorithm == algorithm

    @pytest.mark.parametrize("algorithm", ["sha256", "sha384", "sha512"])
    def test_strong_algorithms_need_no_opt_in(self, algorithm):
        assert RowHashConfig(enabled=True, algorithm=algorithm).allow_weak_hash is False

    def test_default_algorithm_is_strong(self):
        assert RowHashConfig().algorithm == "sha256"


class TestRowHashMetadata:
    def test_row_counters_restart_for_every_source(self):
        processor = RowHashProcessor(RowHashConfig(row_number_enabled=True))
        processor.set_source_context("a.csv", 0)
        first, _ = processor.process_batch(_batch(x=[1, 2, 3]))
        processor.set_source_context("b.csv", 0)
        second, _ = processor.process_batch(_batch(x=[1, 2]))
        assert first.column("_rownum_in_source_file").to_pylist() == [1, 2, 3]
        assert second.column("_rownum_in_source_file").to_pylist() == [1, 2]
        assert second.column("_rownum").to_pylist() == [1, 2]

    def test_input_hash_fires_through_the_pipeline(self):
        config = RowHashConfig(enabled=True, input_hash_enabled=True)
        pipeline = ProcessorPipeline(
            [ColumnTransformer({"name": [uppercase]}), RowHashProcessor(config)]
        )
        batch = _batch(id=[1], name=["alice"])
        out, results = pipeline.process_batch(batch)
        assert results == []
        assert "_input_hash" in out.schema.names

        # _input_hash is the hash of the row *before* the transformation ...
        reference = RowHashProcessor(RowHashConfig(enabled=True, input_hash_enabled=True))
        ref_out, _ = reference.process_batch(batch, input_batch=batch)
        assert out.column("_input_hash").to_pylist() == ref_out.column("_input_hash").to_pylist()
        # ... and differs from the hash of the transformed row
        assert out.column("_input_hash").to_pylist() != out.column("row_hash").to_pylist()

    def test_pipeline_still_works_for_processors_without_input_batch(self):
        pipeline = ProcessorPipeline([ColumnTransformer({"name": [uppercase]})])
        out, _ = pipeline.process_batch(_batch(name=["a"]))
        assert out.column("name").to_pylist() == ["A"]

    def test_failure_raises_instead_of_dropping_the_hash_column(self):
        processor = RowHashProcessor(RowHashConfig(enabled=True))
        processor._compute_row_hashes = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        with pytest.raises(RuntimeError):
            processor.process_batch(_batch(x=[1]))

    @pytest.mark.parametrize(
        "kwargs,existing",
        [
            ({"enabled": True}, "row_hash"),
            ({"source_uri_enabled": True}, "_source_uri"),
            ({"row_number_enabled": True}, "_rownum"),
            ({"row_number_enabled": True}, "_rownum_in_source_file"),
            ({"ingested_at_enabled": True}, "_ingested_at_utc"),
        ],
    )
    def test_existing_metadata_column_name_raises(self, kwargs, existing):
        processor = RowHashProcessor(RowHashConfig(**kwargs))
        processor.set_source_context("f.csv")
        with pytest.raises(ValueError, match="already has a column"):
            processor.process_batch(_batch(**{"x": [1], existing: [1]}))

    def test_two_metadata_columns_with_one_name_raise(self):
        config = RowHashConfig(
            enabled=True, input_hash_enabled=True, input_hash_column_name="row_hash"
        )
        processor = RowHashProcessor(config)
        batch = _batch(x=[1])
        with pytest.raises(ValueError, match="already has a column"):
            processor.process_batch(batch, input_batch=batch)

    def test_duplicate_input_columns_cannot_be_hashed_ambiguously(self):
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1]), pa.array([2])],
            schema=pa.schema([("a", pa.int64()), ("a", pa.int64())]),
        )
        processor = RowHashProcessor(RowHashConfig(enabled=True))
        with pytest.raises(ValueError, match="more than once"):
            processor.process_batch(batch)


# ---------------------------------------------------------------------------------------------
# 7. Regex handling
# ---------------------------------------------------------------------------------------------


REDOS_PATTERNS = [
    r"^(a+)+$",
    r"(.*)*",
    r"(\w+)*x",
    r"^(a*)*$",
    r"((a+))+",
    r"(a|b+)*c",
    r"(?:x+y?)+z",
    r"^([a-z]+\s?)+$",
]


class TestRegexHelper:
    @pytest.mark.parametrize("pattern", REDOS_PATTERNS)
    def test_nested_quantifiers_are_rejected(self, pattern):
        with pytest.raises(UnsafeRegexError, match="nested unbounded quantifiers"):
            compile_pattern(pattern)

    @pytest.mark.parametrize("pattern", REDOS_PATTERNS)
    def test_allow_unsafe_regex_opts_in(self, pattern):
        assert compile_pattern(pattern, allow_unsafe_regex=True) is not None

    @pytest.mark.parametrize(
        "pattern",
        [
            r"^\d{3}-\d{2}-\d{4}$",
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            r"^(foo|bar)+$",
            r"[A-Z]{2}\d+",
            r"^(\d+,){0,3}$",
            r"\bTOTAL\b",
        ],
    )
    def test_ordinary_patterns_are_accepted(self, pattern):
        assert compile_pattern(pattern) is not None

    def test_invalid_pattern_is_value_error(self):
        with pytest.raises(ValueError, match="Invalid regular expression"):
            compile_pattern("[unclosed")
        with pytest.raises(ValueError):
            compile_pattern(None)

    def test_pattern_length_cap(self):
        with pytest.raises(ValueError, match="longer than"):
            compile_pattern("a" * (MAX_PATTERN_LENGTH + 1))
        assert compile_pattern("a" * MAX_PATTERN_LENGTH)

    def test_compile_is_cached(self):
        assert compile_pattern(r"^abc\d+$") is compile_pattern(r"^abc\d+$")

    def test_catastrophic_pattern_is_refused_before_it_can_run(self):
        # ^(a+)+$ on "a" * 28 + "!" took ~12 s with the old per-cell re.match
        start = time.perf_counter()
        with pytest.raises(ValueError):
            StringValidation(pattern=r"^(a+)+$")
        assert time.perf_counter() - start < 0.5

    def test_search_semantics_documented_rule(self):
        # One rule everywhere: unanchored search (JSON Schema); anchor to match the whole value
        loose = compile_pattern(r"\d{9}")
        assert pattern_matches(loose, "123456789")
        assert pattern_matches(loose, "id 123456789 end")
        assert not pattern_matches(loose, "12345")
        anchored = compile_pattern(r"^\d{9}$")
        assert pattern_matches(anchored, "123456789")
        assert not pattern_matches(anchored, "123456789 and junk")
        assert not pattern_matches(anchored, "junk 123456789")

    def test_trailing_newline_does_not_satisfy_dollar(self):
        ssn = compile_pattern(r"^\d{3}-\d{2}-\d{4}$")
        assert pattern_matches(ssn, "123-45-6789")
        assert not pattern_matches(ssn, "123-45-6789\n")
        # an escaped dollar and multiline patterns keep their meaning
        assert pattern_matches(compile_pattern(r"^cost\$"), "cost$")
        assert pattern_matches(compile_pattern(r"(?m)^a$"), "b\na\nc")


class TestRegexAtEverySite:
    def test_data_validation_rejects_bad_patterns_at_configuration(self):
        with pytest.raises(ValueError, match="Invalid regular expression"):
            StringValidation(pattern="[unclosed")
        with pytest.raises(ValueError, match="nested unbounded"):
            StringValidation(pattern=r"(\w+)*$")
        assert StringValidation(pattern=r"(\w+)*$", allow_unsafe_regex=True)

    def test_data_validation_uses_the_shared_semantics(self):
        rule = StringValidation(pattern=r"^\d{3}-\d{2}-\d{4}$")
        assert ValidationRules.validate_string("ssn", "123-45-6789", rule) is None
        assert ValidationRules.validate_string("ssn", "123-45-6789\n", rule) is not None
        assert ValidationRules.validate_string("ssn", "123-45-6789 junk", rule) is not None

    def test_quality_processor_compiles_patterns_up_front(self):
        with pytest.raises(ValueError, match="Invalid regular expression"):
            DataQualityProcessor({"column_rules": {"a": {"pattern": "[unclosed"}}})
        with pytest.raises(ValueError, match="nested unbounded"):
            DataQualityProcessor({"column_rules": {"a": {"pattern": r"^(a+)+$"}}})
        assert DataQualityProcessor(
            {"column_rules": {"a": {"pattern": r"^(a+)+$", "allow_unsafe_regex": True}}}
        )
        assert DataQualityProcessor(
            {"column_rules": {"a": {"pattern": r"^(a+)+$"}}}, allow_unsafe_regex=True
        )

    def test_quality_processor_semantics(self):
        processor = DataQualityProcessor(
            {"column_rules": {"ssn": {"pattern": r"^\d{3}-\d{2}-\d{4}$"}}}
        )
        batch = _batch(ssn=["123-45-6789", "123-45-6789\n", "123-45-6789 x", None])
        _, results = processor.process_batch(batch)
        assert [r.row_index for r in results] == [1, 2]

    def test_quality_processor_handles_large_string_columns(self):
        processor = DataQualityProcessor(
            {"column_rules": {"s": {"pattern": r"^a+$", "min_length": 2, "max_length": 3}}}
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["a", "aaaa", "b"], pa.large_string())], ["s"]
        )
        _, results = processor.process_batch(batch)
        codes = sorted(r.error_code for r in results)
        assert codes == [
            "MAX_LENGTH_VIOLATION",
            "MIN_LENGTH_VIOLATION",
            "MIN_LENGTH_VIOLATION",
            "PATTERN_VIOLATION",
        ]

    def test_quality_numeric_range_covers_decimals(self):
        processor = DataQualityProcessor({"column_rules": {"d": {"min_value": 0.01}}})
        batch = pa.RecordBatch.from_arrays(
            [pa.array([Decimal("0.01"), Decimal("0.00")], pa.decimal128(5, 2))], ["d"]
        )
        _, results = processor.process_batch(batch)
        assert [r.row_index for r in results] == [1]


# ---------------------------------------------------------------------------------------------
# 8. data_validation
# ---------------------------------------------------------------------------------------------


def _validation_processor(*rules, strategy="first_wins", bad_rows=None, **config_kwargs):
    bad_rows = bad_rows or BadRowsConfig(fail_on_exceed_threshold=False)
    return DataValidationProcessor(
        ValidationConfig(
            field_validations=list(rules),
            bad_rows_config=bad_rows,
            uniqueness_strategy=strategy,
            **config_kwargs,
        )
    )


class TestUniquenessStrategies:
    def test_first_wins_keeps_the_first(self):
        proc = _validation_processor(FieldValidationRule("id", unique=True))
        out, results = proc.process_batch(_batch(id=[1, 1, 1], v=["a", "b", "c"]))
        assert out.column("v").to_pylist() == ["a"]
        assert [r.row_index for r in results] == [1, 2]

    def test_last_wins_keeps_the_last(self):
        proc = _validation_processor(FieldValidationRule("id", unique=True), strategy="last_wins")
        out, results = proc.process_batch(_batch(id=[1, 2, 1, 1], v=["a", "b", "c", "d"]))
        assert out.column("v").to_pylist() == ["b", "d"]
        assert [r.row_index for r in results] == [0, 2]
        assert all("last_wins" in r.error_message for r in results)

    def test_mark_all_duplicates_rejects_every_copy(self):
        proc = _validation_processor(
            FieldValidationRule("id", unique=True), strategy="mark_all_duplicates"
        )
        out, results = proc.process_batch(_batch(id=[1, 2, 1, 3, 1], v=list("abcde")))
        assert out.column("v").to_pylist() == ["b", "d"]
        assert sorted(r.row_index for r in results) == [0, 2, 4]

    def test_three_equal_ids_do_not_all_pass_any_more(self):
        for strategy, expected in (
            ("first_wins", 1),
            ("fail_on_duplicate", 1),
            ("last_wins", 1),
            ("mark_all_duplicates", 0),
        ):
            proc = _validation_processor(FieldValidationRule("id", unique=True), strategy=strategy)
            out, _ = proc.process_batch(_batch(id=[1, 1, 1]))
            assert out.num_rows == expected, strategy

    def test_fail_on_duplicate_message(self):
        proc = _validation_processor(
            FieldValidationRule("id", unique=True), strategy="fail_on_duplicate"
        )
        _, results = proc.process_batch(_batch(id=[1, 1]))
        assert "violates uniqueness constraint" in results[0].error_message

    @pytest.mark.parametrize(
        "strategy", ["first_wins", "fail_on_duplicate", "last_wins", "mark_all_duplicates"]
    )
    def test_rejected_row_does_not_poison_a_later_valid_row(self, strategy):
        rules = [
            FieldValidationRule("id", unique=True),
            FieldValidationRule("age", range_validation=RangeValidation(0, 120)),
        ]
        proc = _validation_processor(*rules, strategy=strategy)
        out, results = proc.process_batch(_batch(id=[7, 7], age=[999, 30]))
        assert out.column("age").to_pylist() == [30]
        assert [r.row_index for r in results] == [0]
        assert "unique" not in results[0].error_message

    def test_key_of_rejected_row_is_not_registered_for_later_batches(self):
        rules = [
            FieldValidationRule("id", unique=True),
            FieldValidationRule("age", range_validation=RangeValidation(0, 120)),
        ]
        proc = _validation_processor(*rules)
        proc.process_batch(_batch(id=[7], age=[999]))
        out, _ = proc.process_batch(_batch(id=[7], age=[30]))
        assert out.num_rows == 1
        assert proc.get_validation_summary()["unique_values_counts"] == {"id": 1}
        again, results = proc.process_batch(_batch(id=[7], age=[31]))
        assert again.num_rows == 0 and len(results) == 1

    def test_row_rejected_by_one_unique_field_does_not_claim_the_other(self):
        proc = _validation_processor(
            FieldValidationRule("a", unique=True), FieldValidationRule("b", unique=True)
        )
        proc.process_batch(_batch(a=[1], b=[1]))
        out, _ = proc.process_batch(_batch(a=[2], b=[1]))  # b duplicate -> rejected, a=2 unclaimed
        assert out.num_rows == 0
        out, _ = proc.process_batch(_batch(a=[2], b=[3]))
        assert out.num_rows == 1

    def test_mark_all_duplicates_remembers_marked_keys_across_batches(self):
        proc = _validation_processor(
            FieldValidationRule("id", unique=True), strategy="mark_all_duplicates"
        )
        proc.process_batch(_batch(id=[1, 1]))
        out, _ = proc.process_batch(_batch(id=[1, 2]))
        assert out.column("id").to_pylist() == [2]

    def test_last_wins_cannot_retract_rows_of_earlier_batches(self):
        proc = _validation_processor(FieldValidationRule("id", unique=True), strategy="last_wins")
        proc.process_batch(_batch(id=[1]))
        out, results = proc.process_batch(_batch(id=[1, 2]))
        assert out.column("id").to_pylist() == [2]
        assert "earlier batch" in results[0].error_message

    def test_null_and_blank_values_are_not_keys(self):
        proc = _validation_processor(FieldValidationRule("id", unique=True), strategy="last_wins")
        out, _ = proc.process_batch(
            pa.RecordBatch.from_arrays([pa.array([None, None, "", " "])], ["id"])
        )
        assert out.num_rows == 4


class TestDataValidationFailClosed:
    def test_internal_error_raises_and_does_not_return_the_unfiltered_batch(self):
        proc = _validation_processor(FieldValidationRule("id", required=True))
        proc._validate_row = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        with pytest.raises(ValidationProcessingError, match="batch was not emitted"):
            proc.process_batch(_batch(id=[1, 2]))

    def test_internal_error_message_has_no_cell_values(self):
        proc = _validation_processor(FieldValidationRule("id", required=True))
        proc._validate_row = lambda *a, **k: (_ for _ in ()).throw(ValueError("secret-123"))
        with pytest.raises(ValidationProcessingError) as exc_info:
            proc.process_batch(_batch(id=["secret-123"]))
        assert "secret-123" not in str(exc_info.value)

    def test_threshold_is_honoured(self):
        bad_rows = BadRowsConfig(max_bad_rows_percent=10.0, fail_on_exceed_threshold=True)
        proc = _validation_processor(FieldValidationRule("id", required=True), bad_rows=bad_rows)
        with pytest.raises(BadRowsThresholdExceededError, match="exceed threshold"):
            proc.process_batch(_batch(id=[1, None, None, 4]))

    def test_threshold_not_exceeded_passes(self):
        bad_rows = BadRowsConfig(max_bad_rows_percent=50.0, fail_on_exceed_threshold=True)
        proc = _validation_processor(FieldValidationRule("id", required=True), bad_rows=bad_rows)
        out, _ = proc.process_batch(_batch(id=[1, None, 3, 4]))
        assert out.num_rows == 3

    def test_threshold_applies_when_bad_row_collection_is_disabled(self):
        bad_rows = BadRowsConfig(enabled=False, max_bad_rows_percent=10.0)
        proc = _validation_processor(FieldValidationRule("id", required=True), bad_rows=bad_rows)
        with pytest.raises(BadRowsThresholdExceededError):
            proc.process_batch(_batch(id=[None, None]))
        assert proc.get_validation_summary()["bad_rows_count"] == 2

    def test_fail_on_exceed_threshold_false_keeps_processing(self):
        proc = _validation_processor(FieldValidationRule("id", required=True))
        out, results = proc.process_batch(_batch(id=[None, None, 3]))
        assert out.num_rows == 1 and len(results) == 2

    def test_required_rule_on_missing_column_is_an_error(self):
        proc = _validation_processor(FieldValidationRule("Email", required=True))
        with pytest.raises(ValidationProcessingError, match="Email"):
            proc.process_batch(_batch(email=["a@b.c"]))

    def test_non_required_rule_on_missing_column_is_skipped(self):
        proc = _validation_processor(FieldValidationRule("Email", unique=True))
        out, _ = proc.process_batch(_batch(email=["a@b.c"]))
        assert out.num_rows == 1

    def test_invalid_range_bounds_fail_at_configuration(self):
        with pytest.raises(ValueError, match="Range bounds"):
            _validation_processor(
                FieldValidationRule("a", range_validation=RangeValidation(min_value="abc"))
            )


class TestAllowEmptyAndNulls:
    def test_allow_empty_false_rejects_blank_values(self):
        rule = FieldValidationRule("name", string_validation=StringValidation(allow_empty=False))
        proc = _validation_processor(rule)
        out, results = proc.process_batch(_batch(name=["ok", "", "   "]))
        assert out.column("name").to_pylist() == ["ok"]
        assert all("cannot be empty" in r.error_message for r in results)

    def test_min_length_applies_to_empty_strings(self):
        rule = FieldValidationRule("name", string_validation=StringValidation(min_length=3))
        out, _ = _validation_processor(rule).process_batch(_batch(name=["abc", ""]))
        assert out.column("name").to_pylist() == ["abc"]

    def test_enum_rejects_empty_string_unless_allowed(self):
        enum = EnumValidation(allowed_values=["a", "b"])
        out, _ = _validation_processor(
            FieldValidationRule("e", enum_validation=enum)
        ).process_batch(_batch(e=["a", ""]))
        assert out.column("e").to_pylist() == ["a"]
        allowed = EnumValidation(allowed_values=["a", ""])
        out, _ = _validation_processor(
            FieldValidationRule("e", enum_validation=allowed)
        ).process_batch(_batch(e=["a", ""]))
        assert out.num_rows == 2

    def test_null_still_skips_non_required_rules(self):
        rule = FieldValidationRule(
            "name",
            string_validation=StringValidation(min_length=3, allow_empty=False),
            enum_validation=EnumValidation(allowed_values=["abc"]),
        )
        arr = pa.RecordBatch.from_arrays([pa.array(["abc", None])], ["name"])
        out, _ = _validation_processor(rule).process_batch(arr)
        assert out.num_rows == 2

    def test_required_still_rejects_null_and_blank(self):
        proc = _validation_processor(FieldValidationRule("n", required=True))
        out, _ = proc.process_batch(
            pa.RecordBatch.from_arrays([pa.array(["x", None, " "])], ["n"])
        )
        assert out.num_rows == 1


class TestDateAndRangeRules:
    def test_date_formats_are_used(self):
        rule = DateValidation(formats=["%d/%m/%Y"], min_date="2024-01-01", max_date="2024-12-31")
        assert ValidationRules.validate_date("d", "31/01/2024", rule) is None
        assert "not a valid date" in ValidationRules.validate_date("d", "2024-01-31", rule)
        assert "after maximum" in ValidationRules.validate_date("d", "01/02/2025", rule)
        # default format is still ISO
        assert ValidationRules.validate_date("d", "2024-01-31", DateValidation()) is None
        assert ValidationRules.validate_date("d", "31/01/2024", DateValidation()) is not None

    def test_multiple_date_formats_in_order(self):
        rule = DateValidation(formats=["%d/%m/%Y", "%Y.%m.%d"])
        assert ValidationRules.validate_date("d", "2024.01.31", rule) is None
        assert ValidationRules.validate_date("d", "31/01/2024", rule) is None

    def test_invalid_date_bound_is_a_configuration_error(self):
        with pytest.raises(ValueError, match="min_date"):
            ValidationRules.validate_date("d", "2024-01-01", DateValidation(min_date="nonsense"))

    def test_range_validation_on_dates(self):
        rule = RangeValidation(min_value="2020-01-01", max_value="2020-12-31")
        assert ValidationRules.validate_range("d", "2020-06-15", rule) is None
        assert ValidationRules.validate_range("d", date(2020, 6, 15), rule) is None
        assert ValidationRules.validate_range("d", datetime(2020, 12, 31, 23, 0), rule) is None
        assert "below minimum" in ValidationRules.validate_range("d", "2019-12-31", rule)
        assert "above maximum" in ValidationRules.validate_range("d", date(2021, 1, 1), rule)
        assert "cannot be converted to a date" in ValidationRules.validate_range("d", "abc", rule)

    def test_range_validation_with_date_object_bounds(self):
        rule = RangeValidation(min_value=date(2020, 1, 1), inclusive=False)
        assert "not greater than" in ValidationRules.validate_range("d", "2020-01-01", rule)
        assert ValidationRules.validate_range("d", "2020-01-02", rule) is None

    @pytest.mark.parametrize("value", [float("nan"), "nan", "NaN", Decimal("NaN")])
    def test_nan_fails_every_range_check(self, value):
        for rule in (
            RangeValidation(min_value=0),
            RangeValidation(max_value=10),
            RangeValidation(min_value=0, max_value=10),
        ):
            error = ValidationRules.validate_range("x", value, rule)
            assert error is not None and "NaN" in error

    def test_infinity_is_compared_normally(self):
        rule = RangeValidation(max_value=10)
        assert "above maximum" in ValidationRules.validate_range("x", float("inf"), rule)
        assert ValidationRules.validate_range("x", float("-inf"), rule) is None

    def test_decimal_boundary_is_exact(self):
        rule = RangeValidation(min_value=0.01, max_value=0.1)
        assert ValidationRules.validate_range("x", Decimal("0.01"), rule) is None
        assert ValidationRules.validate_range("x", Decimal("0.10"), rule) is None
        assert ValidationRules.validate_range("x", Decimal("0.009"), rule) is not None
        assert ValidationRules.validate_range("x", Decimal("0.101"), rule) is not None

    def test_strings_keep_their_precision(self):
        rule = RangeValidation(max_value="9007199254740993.5")
        assert ValidationRules.validate_range("x", "9007199254740993.5", rule) is None
        assert ValidationRules.validate_range("x", "9007199254740993.6", rule) is not None
        big = RangeValidation(min_value="12345678901234567890")
        assert ValidationRules.validate_range("x", "12345678901234567890", big) is None
        assert ValidationRules.validate_range("x", "12345678901234567889", big) is not None

    def test_decimal_column_through_the_processor(self):
        rule = FieldValidationRule("price", range_validation=RangeValidation(min_value=0.01))
        batch = pa.RecordBatch.from_arrays(
            [pa.array([Decimal("0.01"), Decimal("0.00")], pa.decimal128(5, 2))], ["price"]
        )
        out, results = _validation_processor(rule).process_batch(batch)
        assert out.num_rows == 1 and [r.row_index for r in results] == [1]

    def test_numeric_strings_and_ints_still_work(self):
        rule = RangeValidation(min_value=10, max_value=100)
        assert ValidationRules.validate_range("x", "50", rule) is None
        assert ValidationRules.validate_range("x", " 50.5 ", rule) is None
        assert ValidationRules.validate_range("x", 100, rule) is None
        assert "cannot be converted to numeric" in ValidationRules.validate_range("x", "abc", rule)


class TestErrorMessagesAndValues:
    SECRET = "secret-value-42"

    def _messages(self, **config_kwargs):
        rules = [
            FieldValidationRule("n", range_validation=RangeValidation(0, 10)),
            FieldValidationRule("s", string_validation=StringValidation(pattern=r"^[a-z]+$")),
            FieldValidationRule("e", enum_validation=EnumValidation(["a", "b"])),
            FieldValidationRule("d", date_validation=DateValidation(max_date="2000-01-01")),
        ]
        proc = _validation_processor(*rules, **config_kwargs)
        batch = _batch(n=[self.SECRET], s=[self.SECRET], e=[self.SECRET], d=[self.SECRET])
        _, results = proc.process_batch(batch)
        return [r.error_message for r in results]

    def test_values_are_not_in_messages_by_default(self):
        messages = self._messages()
        assert len(messages) == 4
        assert not any(self.SECRET in m for m in messages)

    def test_values_can_be_enabled(self):
        messages = self._messages(include_values_in_errors=True)
        assert all(self.SECRET in m for m in messages)

    def test_rule_helpers_default_to_no_values(self):
        assert self.SECRET not in ValidationRules.validate_enum(
            "e", self.SECRET, EnumValidation(["a"])
        )
        assert self.SECRET in ValidationRules.validate_enum(
            "e", self.SECRET, EnumValidation(["a"]), include_values=True
        )

    def test_duplicate_messages_have_no_values(self):
        proc = _validation_processor(FieldValidationRule("id", unique=True))
        _, results = proc.process_batch(_batch(id=[self.SECRET, self.SECRET]))
        assert self.SECRET not in results[0].error_message


class TestDataValidationBadRows:
    def _batch_with_underscore_columns(self):
        return _batch(_id=[1, 2], name=["a", "b"], _validation_errors=["old", "old2"])

    def test_underscore_columns_are_kept(self):
        handler = BadRowsHandler(BadRowsConfig())
        handler.add_bad_row(_batch(_id=[10], name=["x"]), 0, ["bad"])
        out = handler.get_bad_rows_batch()
        assert out.column("_id").to_pylist() == [10]
        assert out.column("name").to_pylist() == ["x"]
        assert out.column("_validation_errors").to_pylist() == ["bad"]

    def test_colliding_original_column_is_renamed_not_overwritten(self):
        handler = BadRowsHandler(BadRowsConfig())
        batch = self._batch_with_underscore_columns()
        handler.add_bad_row(batch, 0, ["bad"])
        out = handler.get_bad_rows_batch()
        assert out.column("_validation_errors").to_pylist() == ["bad"]
        assert out.column("original__validation_errors").to_pylist() == ["old"]
        assert out.column("_id").to_pylist() == [1]

    def test_include_original_row_false_leaves_out_the_data(self):
        handler = BadRowsHandler(BadRowsConfig(include_original_row=False))
        handler.add_bad_row(_batch(name=["Alice"], ssn=["123-45-6789"]), 0, ["bad"], row_number=41)
        row = handler.bad_rows[0]
        assert "name" not in row and "ssn" not in row
        out = handler.get_bad_rows_batch()
        assert out.schema.names == [
            "_row_number",
            "_validation_errors",
            "_error_count",
            "_processed_timestamp",
        ]
        assert out.column("_row_number").to_pylist() == [41]
        assert "Alice" not in str(out.to_pydict())

    def test_include_original_row_false_through_the_processor(self):
        bad_rows = BadRowsConfig(include_original_row=False, fail_on_exceed_threshold=False)
        proc = _validation_processor(FieldValidationRule("id", required=True), bad_rows=bad_rows)
        proc.process_batch(_batch(id=[1, None], name=["a", "b"]))
        proc.process_batch(_batch(id=[None], name=["c"]))
        assert [r["_row_number"] for r in proc.bad_rows] == [1, 2]

    def test_bad_rows_with_different_columns_use_the_union(self):
        handler = BadRowsHandler(BadRowsConfig(include_validation_errors=False))
        handler.add_bad_row(_batch(a=[1]), 0, ["e"])
        handler.add_bad_row(_batch(a=[2], b=["x"]), 0, ["e"])
        out = handler.get_bad_rows_batch()
        assert out.schema.names == ["a", "b"]
        assert out.column("b").to_pylist() == [None, "x"]

    def test_non_primitive_values_do_not_break_the_batch(self):
        handler = BadRowsHandler(BadRowsConfig())
        handler.add_bad_row(_batch(d=[date(2024, 1, 31)], n=[1]), 0, ["e"])
        handler.add_bad_row(_batch(d=[date(2024, 2, 1)], n=[2.5]), 0, ["e"])
        out = handler.get_bad_rows_batch()
        assert out.column("d").to_pylist() == ["2024-01-31", "2024-02-01"]
        assert out.column("n").to_pylist() == [1.0, 2.5]


# ---------------------------------------------------------------------------------------------
# 9. schema_validator / validation_factory / constraint_validator
# ---------------------------------------------------------------------------------------------


def _schema_validator(columns, **config_kwargs):
    return SchemaValidator({"columns": columns}, SchemaValidatorConfig(**config_kwargs))


class TestTypeConverter:
    def test_unknown_type_raises_instead_of_becoming_string(self):
        with pytest.raises(ValueError, match="Unknown data type"):
            TypeConverter.string_to_arrow_type("nonsense")
        with pytest.raises(ValueError):
            parse_arrow_type("")

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("int16", pa.int16()),
            ("decimal(18,2)", pa.decimal128(18, 2)),
            ("decimal128(18, 2)", pa.decimal128(18, 2)),
            ("timestamp[ms]", pa.timestamp("ms")),
            ("timestamp[us, tz=UTC]", pa.timestamp("us", tz="UTC")),
            ("date32[day]", pa.date32()),
            ("list<item: string>", pa.list_(pa.string())),
            ("large_string", pa.large_string()),
            ("number", pa.float64()),
            ("bool_", pa.bool_()),
        ],
    )
    def test_parse_arrow_type(self, text, expected):
        assert parse_arrow_type(text) == expected

    def test_arrow_type_strings_round_trip(self):
        # (Arrow prints float32 as "float", which this parser reads as the SQL alias for double)
        for arrow_type in (pa.int8(), pa.float64(), pa.decimal128(10, 3), pa.timestamp("ns")):
            assert parse_arrow_type(str(arrow_type)) == arrow_type

    def test_specific_types_match_exactly(self):
        assert TypeConverter.is_type_compatible(pa.decimal128(18, 2), "decimal(18,2)")
        assert not TypeConverter.is_type_compatible(pa.decimal128(18, 3), "decimal(18,2)")
        assert TypeConverter.is_type_compatible(pa.int16(), "int16")
        assert not TypeConverter.is_type_compatible(pa.int32(), "int16")
        assert TypeConverter.is_type_compatible(pa.timestamp("ms"), "timestamp[ms]")
        assert not TypeConverter.is_type_compatible(pa.int64(), "no_such_type")

    def test_family_names_still_accept_any_member(self):
        assert TypeConverter.is_type_compatible(pa.int8(), "int")
        assert TypeConverter.is_type_compatible(pa.large_string(), "string")
        assert TypeConverter.is_type_compatible(pa.decimal128(5, 2), "number")

    def test_dict_schema_with_decimal_and_int16_validates(self):
        validator = _schema_validator(
            [{"name": "amount", "type": "decimal(18,2)"}, {"name": "n", "type": "int16"}]
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array([Decimal("1.50")], pa.decimal128(18, 2)), pa.array([1], pa.int16())],
            ["amount", "n"],
        )
        _, results = validator.process_batch(batch)
        assert [r for r in results if r.error_code == "TYPE_MISMATCH"] == []

    def test_unknown_type_in_dict_schema_raises_at_construction(self):
        with pytest.raises(ValueError, match="Unknown data type"):
            _schema_validator([{"name": "a", "type": "wibble"}])


class TestSchemaValidatorCoercion:
    def test_without_coercion_a_string_column_is_a_type_mismatch(self):
        validator = _schema_validator([{"name": "n", "type": "int64"}])
        out, results = validator.process_batch(_batch(n=["1", "2"]))
        assert [r.error_code for r in results] == ["TYPE_MISMATCH"]
        assert out.schema.field("n").type == pa.string()

    def test_coercion_really_casts(self):
        validator = _schema_validator([{"name": "n", "type": "int64"}], allow_type_coercion=True)
        out, results = validator.process_batch(_batch(n=["1", "2", "30"]))
        assert results == []
        assert out.schema.field("n").type == pa.int64()
        assert out.column("n").to_pylist() == [1, 2, 30]

    def test_values_that_cannot_be_converted_are_violations_not_silently_accepted(self):
        validator = _schema_validator([{"name": "n", "type": "int64"}], allow_type_coercion=True)
        out, results = validator.process_batch(_batch(n=["1", "abc", "3", "4.5"]))
        failed = [r for r in results if r.error_code == "COERCION_FAILED"]
        assert [r.row_index for r in failed] == [1, 3]
        assert all(not r.is_valid and r.column_name == "n" for r in failed)
        assert out.column("n").to_pylist() == [1, None, 3, None]
        assert "abc" not in " ".join(r.error_message for r in results)

    def test_safe_cast_refuses_truncation(self):
        validator = _schema_validator([{"name": "n", "type": "int32"}], allow_type_coercion=True)
        batch = pa.RecordBatch.from_arrays([pa.array([1.0, 2.5, 3e12])], ["n"])
        out, results = validator.process_batch(batch)
        assert [r.row_index for r in results if r.error_code == "COERCION_FAILED"] == [1, 2]
        assert out.column("n").to_pylist() == [1, None, None]

    def test_number_to_string_and_coerce_mode(self):
        validator = SchemaValidator(
            {"columns": [{"name": "s", "type": "string"}]},
            SchemaValidatorConfig(validation_mode=SchemaValidationMode.COERCE),
        )
        out, results = validator.process_batch(_batch(s=[1, 2]))
        assert out.column("s").to_pylist() == ["1", "2"]
        assert not [r for r in results if r.error_code == "TYPE_MISMATCH"]

    def test_uncastable_type_is_still_reported(self):
        validator = _schema_validator([{"name": "n", "type": "int64"}], allow_type_coercion=True)
        out, results = validator.process_batch(_batch(n=[True, False]))
        assert "TYPE_MISMATCH_NO_COERCION" in [r.error_code for r in results]

    def test_coerced_values_are_validated_by_the_constraints(self):
        validator = _schema_validator(
            [{"name": "n", "type": "int64", "constraints": {"max": 10}}], allow_type_coercion=True
        )
        _, results = validator.process_batch(_batch(n=["5", "50"]))
        assert [r.row_index for r in results if r.error_code == "MAX_VALUE_VIOLATION"] == [1]


class TestSchemaValidatorOther:
    def test_case_insensitive_column_matching(self):
        columns = [{"name": "id", "type": "int64", "nullable": False}]
        batch = pa.RecordBatch.from_arrays([pa.array([1, None])], ["ID"])

        strict = _schema_validator(columns)
        codes = [r.error_code for r in strict.process_batch(batch)[1]]
        assert "MISSING_COLUMN" in codes and "EXTRA_COLUMN" in codes

        relaxed = _schema_validator(columns, case_sensitive=False)
        results = relaxed.process_batch(batch)[1]
        assert [r.error_code for r in results] == ["NULL_IN_REQUIRED_FIELD"]
        assert results[0].row_index == 1

    def test_case_insensitive_schema_with_clashing_names_is_rejected(self):
        with pytest.raises(ValueError, match="differ only by case"):
            _schema_validator(
                [{"name": "id", "type": "int64"}, {"name": "ID", "type": "int64"}],
                case_sensitive=False,
            )

    def test_max_null_percentage_on_an_empty_batch(self):
        validator = _schema_validator([{"name": "a", "type": "int64"}], max_null_percentage=10.0)
        empty = pa.RecordBatch.from_arrays([pa.array([], pa.int64())], ["a"])
        _, results = validator.process_batch(empty)
        assert results == []

    def test_range_constraints_cover_dates_timestamps_and_decimals(self):
        date_col = pa.array([date(2020, 1, 1), date(2021, 6, 1), date(2022, 1, 1)])
        results = ValueConstraints.validate_range_constraints(
            date_col, "d", {"min": "2020-06-01", "max": "2021-12-31"}
        )
        assert sorted((r.error_code, r.row_index) for r in results) == [
            ("MAX_VALUE_VIOLATION", 2),
            ("MIN_VALUE_VIOLATION", 0),
        ]

        ts = pa.array([datetime(2020, 1, 1, 12), datetime(2024, 1, 1, 12)], pa.timestamp("ns"))
        results = ValueConstraints.validate_range_constraints(ts, "t", {"max": "2021-01-01"})
        assert [r.row_index for r in results] == [1]

        dec = pa.array([Decimal("0.01"), Decimal("0.00")], pa.decimal128(5, 2))
        results = ValueConstraints.validate_range_constraints(dec, "x", {"min": 0.01})
        assert [r.row_index for r in results] == [1]

    def test_range_constraint_with_invalid_bound_raises(self):
        with pytest.raises(ValueError, match="not numeric"):
            ValueConstraints.validate_range_constraints(pa.array([1]), "x", {"min": "abc"})
        with pytest.raises(ValueError, match="not a date"):
            ValueConstraints.validate_range_constraints(
                pa.array([date(2020, 1, 1)]), "x", {"min": "abc"}
            )

    def test_string_constraints_cover_large_string(self):
        column = pa.array(["a", "abcdef", "ok1"], pa.large_string())
        lengths = ValueConstraints.validate_length_constraints(
            column, "s", {"minLength": 2, "maxLength": 5}
        )
        assert sorted(r.row_index for r in lengths) == [0, 1]
        patterns = ValueConstraints.validate_pattern_constraints(column, "s", r"^[a-z]+$")
        assert [r.row_index for r in patterns] == [2]

    def test_pattern_constraint_is_checked_when_the_validator_is_created(self):
        columns = [{"name": "s", "type": "string", "constraints": {"pattern": r"^(a+)+$"}}]
        with pytest.raises(ValueError, match="nested unbounded"):
            _schema_validator(columns)
        assert _schema_validator(columns, allow_unsafe_regex=True)
        with pytest.raises(ValueError, match="Invalid regular expression"):
            _schema_validator([{"name": "s", "type": "string", "constraints": {"pattern": "["}}])

    def test_pattern_constraint_semantics(self):
        validator = _schema_validator(
            [
                {
                    "name": "ssn",
                    "type": "string",
                    "constraints": {"pattern": r"^\d{3}-\d{2}-\d{4}$"},
                }
            ]
        )
        _, results = validator.process_batch(_batch(ssn=["123-45-6789", "123-45-6789\n", "x"]))
        assert [r.row_index for r in results if r.error_code == "PATTERN_VIOLATION"] == [1, 2]


class TestFactoriesFailLoudly:
    def test_invalid_configuration_is_not_turned_into_no_validation(self):
        with pytest.raises(ValueError, match="Invalid type for column 'a'"):
            create_validation_processor_from_schema(
                {"type": "schema", "schema": {"a": "no_such_type"}}
            )
        with pytest.raises(TypeError):
            create_validation_processor_from_schema({"type": "schema"})  # schema missing
        with pytest.raises(ValueError):
            create_validation_processor_from_schema({"type": "nonsense"})

    def test_empty_configuration_still_means_no_validation(self):
        assert create_validation_processor_from_schema({}) is None
        assert create_validation_processor_from_schema(None) is None

    def test_dict_schema_types(self):
        validator = ValidationFactory.create_validator(
            "schema", {"schema": {"a": "int64", "b": "string", "c": "bool_", "d": pa.float32()}}
        )
        assert validator.schema.field("c").type == pa.bool_()
        assert validator.schema.field("d").type == pa.float32()

    def test_write_time_hash_option_is_passed_through(self):
        validator = ValidationFactory.create_validator(
            "write_time", {"primary_key_columns": ["id"], "hash_primary_keys": True}
        )
        assert validator.config.hash_primary_keys is True

    def test_mistyped_error_mode_raises(self):
        with pytest.raises(ValueError, match="errorMode"):
            create_constraint_config_from_schema(
                {"x-constraintHandling": {"errorMode": "bad_row"}}
            )
        assert (
            create_constraint_config_from_schema({"x-constraintHandling": {}}).error_mode
            == ErrorMode.BAD_ROWS
        )


# ---------------------------------------------------------------------------------------------
# 10. ConstraintValidator and EnhancedDataProcessor really validate
# ---------------------------------------------------------------------------------------------


def _constraints(error_mode=ErrorMode.BAD_ROWS, check=None, unique=None):
    return ConstraintValidator(
        ConstraintConfig(
            error_mode=error_mode, check_constraints=check or {}, unique_constraints=unique or []
        )
    )


class TestConstraintValidatorImplementation:
    def test_range_violations_are_found_and_rows_removed(self):
        validator = _constraints(check={"age_range": {"column": "age", "min": 0, "max": 120}})
        batch = _batch(id=[1, 2, 3, 4], age=[30, -1, 121, None])
        out, results = validator.process_batch(batch)
        assert out.column("id").to_pylist() == [1, 4]  # NULL is not a range violation
        assert sorted(v.row_index for v in validator.violations) == [1, 2]
        assert {v.violation_type for v in validator.violations} == {"range"}
        assert all(not r.is_valid for r in results) and len(results) == 2
        assert validator.violations[0].constraint_name == "age_range"

    def test_enum_pattern_length_and_nullable(self):
        validator = _constraints(
            check={
                "c_enum": {"column": "c", "enum": ["a", "b"]},
                "c_pat": {"column": "p", "pattern": r"^\d+$"},
                "c_len": {"column": "s", "minLength": 2, "maxLength": 3},
                "c_null": {"column": "n", "nullable": False},
            }
        )
        batch = _batch(
            c=["a", "z", "b", "a"],
            p=["1", "2", "x", "4"],
            s=["ab", "abc", "abcd", "a"],
            n=[1, None, 3, 4],
        )
        out, _ = validator.process_batch(batch)
        by_row = {}
        for v in validator.violations:
            by_row.setdefault(v.row_index, set()).add(v.violation_type)
        assert by_row == {1: {"enum", "null"}, 2: {"pattern", "length"}, 3: {"length"}}
        assert out.num_rows == 1

    def test_all_violations_of_a_row_are_recorded(self):
        validator = _constraints(
            check={
                "r": {"column": "x", "max": 10},
                "e": {"column": "x", "enum": [1, 2]},
            }
        )
        validator.process_batch(_batch(x=[99]))
        assert len(validator.violations) == 2

    def test_unique_constraint_within_and_across_batches(self):
        validator = _constraints(unique=["id"])
        out, _ = validator.process_batch(_batch(id=[1, 2, 1]))
        assert out.column("id").to_pylist() == [1, 2]
        out, _ = validator.process_batch(_batch(id=[2, 3, None, None]))
        assert out.column("id").to_pylist() == [3, None, None]  # NULL keys are not compared
        assert [v.violation_type for v in validator.violations] == ["unique", "unique"]

    def test_composite_unique_constraint(self):
        validator = _constraints(unique=[("a", "b")])
        out, _ = validator.process_batch(_batch(a=[1, 1, 1], b=["x", "y", "x"]))
        assert out.num_rows == 2

    def test_rejected_row_does_not_claim_a_unique_key(self):
        validator = _constraints(check={"age": {"column": "age", "max": 120}}, unique=["id"])
        out, _ = validator.process_batch(_batch(id=[7, 7], age=[999, 30]))
        assert out.column("age").to_pylist() == [30]

    def test_violations_accumulate_across_batches(self):
        validator = _constraints(check={"r": {"column": "x", "max": 1}})
        validator.process_batch(_batch(x=[5]))
        validator.process_batch(_batch(x=[6, 7]))
        assert len(validator.get_all_violations()) == 3
        assert len(validator.batch_violations) == 2

    def test_fail_complete_keeps_rows_and_finalize_raises(self):
        validator = _constraints(ErrorMode.FAIL_COMPLETE, check={"r": {"column": "x", "max": 1}})
        out, results = validator.process_batch(_batch(x=[5, 0]))
        assert out.num_rows == 2 and len(results) == 1
        validator.process_batch(_batch(x=[9]))
        with pytest.raises(ValueError, match="2 violations"):
            validator.finalize()

    def test_fail_fast_raises_on_the_first_violation(self):
        validator = _constraints(ErrorMode.FAIL_FAST, check={"r": {"column": "x", "max": 1}})
        validator.process_batch(_batch(x=[0]))
        with pytest.raises(ValueError, match="'r' violated"):
            validator.process_batch(_batch(x=[5]))

    def test_violation_messages_contain_no_cell_values(self):
        validator = _constraints(
            check={
                "e": {"column": "x", "enum": ["a"]},
                "p": {"column": "x", "pattern": "^a$"},
            },
            unique=["x"],
        )
        validator.process_batch(_batch(x=["secret-77", "secret-77"]))
        assert validator.violations
        assert not any("secret-77" in v.error_message for v in validator.violations)

    def test_patterns_are_checked_at_configuration(self):
        with pytest.raises(ValueError):
            _constraints(check={"p": {"column": "x", "pattern": "["}})

    def test_missing_columns_are_skipped(self):
        validator = _constraints(check={"r": {"column": "nope", "max": 1}}, unique=["nope"])
        out, results = validator.process_batch(_batch(x=[5]))
        assert out.num_rows == 1 and results == []

    def test_config_from_schema_includes_enum_pattern_length(self):
        config = create_constraint_config_from_schema(
            {
                "properties": {
                    "c": {"type": "string", "enum": ["a"], "pattern": "^a", "minLength": 1},
                    "n": {"type": "integer", "minimum": 0},
                }
            }
        )
        assert set(config.check_constraints) == {"c_enum", "c_pattern", "c_length", "n_range"}


SCHEMA_FILE = {
    "type": "object",
    "required": ["id", "name"],
    "properties": {
        "id": {"type": "integer", "minimum": 1, "x-unique": True},
        "name": {"type": "string", "minLength": 2, "pattern": "^[A-Za-z]+$"},
        "status": {"type": "string", "enum": ["active", "inactive"]},
        "age": {"type": "integer", "minimum": 0, "maximum": 120},
    },
}


def _enhanced(tmp_path, schema=SCHEMA_FILE, **kwargs):
    schema_path = tmp_path / "schema.json"
    schema_path.write_text(json.dumps(schema))
    return create_enhanced_processor_from_schema_file(
        schema_path, bad_rows_output_path=tmp_path / "bad_rows.parquet", **kwargs
    )


class TestEnhancedDataProcessor:
    def test_schema_file_constraints_are_enforced(self, tmp_path):
        processor = _enhanced(tmp_path)
        batch = _batch(
            id=[1, 2, 0, 1, 5],
            name=["Ann", "Bob", "Cy", "Dee", "E1"],
            status=["active", "wrong", "active", "inactive", "active"],
            age=[30, 40, 50, 130, 20],
        )
        out, results = processor.process_batch(batch)
        # row 1: bad status; row 2: id below minimum; row 3: duplicate id 1 and age too high;
        # row 4: name pattern
        assert out.column("name").to_pylist() == ["Ann"]
        assert sorted({r.row_index for r in results if not r.is_valid}) == [1, 2, 3, 4]
        assert processor.bad_rows_handler.get_bad_row_count() == 4

    def test_valid_batch_no_longer_contains_bad_rows(self, tmp_path):
        processor = _enhanced(tmp_path)
        out, _ = processor.process_batch(
            _batch(id=[1, 2], name=["Ann", "Bob"], status=["x", "active"], age=[1, 2])
        )
        assert out.column("id").to_pylist() == [2]

    def test_null_in_required_column_rejects_the_row(self, tmp_path):
        processor = _enhanced(tmp_path)
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2]),
                pa.array(["Ann", None]),
                pa.array(["active", "active"]),
                pa.array([1, 2]),
            ],
            ["id", "name", "status", "age"],
        )
        out, results = processor.process_batch(batch)
        assert out.column("id").to_pylist() == [1]
        assert any(r.error_code == "NULL_IN_REQUIRED_FIELD" and r.row_index == 1 for r in results)

    def test_row_rejected_by_the_schema_does_not_claim_a_unique_key(self, tmp_path):
        processor = _enhanced(tmp_path)
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([7, 7]),
                pa.array([None, "Bob"]),  # row 0: NULL in a required column
                pa.array(["active", "active"]),
                pa.array([1, 2]),
            ],
            ["id", "name", "status", "age"],
        )
        out, results = processor.process_batch(batch)
        assert out.column("name").to_pylist() == ["Bob"]
        assert [r.row_index for r in results if not r.is_valid] == [0]

    def test_bad_row_indices_are_global_across_batches(self, tmp_path):
        processor = _enhanced(tmp_path)
        first = _batch(id=[1, 2], name=["Ann", "Bob"], status=["active", "nope"], age=[1, 2])
        second = _batch(id=[3, 4], name=["Cy", "Di"], status=["active", "nope"], age=[1, 2])
        processor.process_batch(first)
        processor.process_batch(second)
        assert [row["row_index"] for row in processor.bad_rows_handler.bad_rows] == [1, 3]

    def test_every_violation_of_a_row_is_kept(self, tmp_path):
        processor = _enhanced(tmp_path)
        processor.process_batch(
            _batch(id=[1], name=["Ann"], status=["nope"], age=[500])  # enum + maximum
        )
        errors = processor.bad_rows_handler.bad_rows[0]["errors"]
        assert len([e for e in errors if e["type"] == "constraint_violation"]) == 2

    def test_results_use_positions_in_the_original_batch(self, tmp_path):
        processor = _enhanced(tmp_path)
        batch = pa.RecordBatch.from_arrays(
            [
                pa.array([1, 2, 3]),
                pa.array([None, "Bob", "Cy"]),  # row 0 rejected by the schema
                pa.array(["active", "nope", "active"]),  # row 1 rejected by a constraint
                pa.array([1, 2, 3]),
            ],
            ["id", "name", "status", "age"],
        )
        out, results = processor.process_batch(batch)
        assert out.column("id").to_pylist() == [3]
        assert sorted({r.row_index for r in results if not r.is_valid}) == [0, 1]
        rows = {row["row_index"] for row in processor.bad_rows_handler.bad_rows}
        assert rows == {0, 1}

    def test_finalize_writes_the_bad_rows_file(self, tmp_path):
        processor = _enhanced(tmp_path)
        processor.process_batch(
            _batch(id=[1, 2], name=["Ann", "Bob"], status=["x", "active"], age=[1, 2])
        )
        summary = processor.finalize()
        assert summary["has_bad_rows"] is True
        assert pq.read_table(summary["bad_rows_file"]).num_rows == 1
        assert summary["constraint_violations"] == 1

    def test_fail_complete_mode_keeps_rows_and_raises_in_finalize(self, tmp_path):
        processor = _enhanced(tmp_path, error_mode="fail_complete")
        out, _ = processor.process_batch(
            _batch(id=[1, 2], name=["Ann", "Bob"], status=["x", "active"], age=[1, 2])
        )
        assert out.num_rows == 2
        with pytest.raises(ValueError, match="violations"):
            processor.finalize()

    def test_fail_fast_mode_raises_immediately(self, tmp_path):
        processor = _enhanced(tmp_path, error_mode="fail_fast")
        with pytest.raises(ValueError):
            processor.process_batch(_batch(id=[1], name=["Ann"], status=["x"], age=[1]))

    def test_mistyped_error_mode_raises(self, tmp_path):
        with pytest.raises(ValueError, match="errorMode"):
            _enhanced(tmp_path, error_mode="bad_row")

    def test_clean_batches_pass_unchanged(self, tmp_path):
        processor = _enhanced(tmp_path)
        batch = _batch(id=[1, 2], name=["Ann", "Bob"], status=["active", "inactive"], age=[1, 2])
        out, results = processor.process_batch(batch)
        assert out.num_rows == 2 and not [r for r in results if not r.is_valid]
        assert processor.bad_rows_handler.has_bad_rows() is False


# ---------------------------------------------------------------------------------------------
# 11. Column mapper
# ---------------------------------------------------------------------------------------------


def _mapper(**kwargs):
    return ColumnMapper(ColumnMappingConfig(**kwargs))


class TestColumnMapperFixes:
    def test_convention_collisions_raise_naming_the_sources(self):
        mapper = _mapper(naming_convention="snake_case")
        batch = _batch(FirstName=[1], first_name=[2], firstName=[3], other=[4])
        with pytest.raises(ValueError) as exc_info:
            mapper.process_batch(batch)
        message = str(exc_info.value)
        assert "first_name" in message
        for source in ("FirstName", "first_name", "firstName"):
            assert source in message

    def test_explicit_mapping_onto_existing_column_raises(self):
        mapper = _mapper(explicit_mappings={"A": "id"})
        with pytest.raises(ValueError, match="'id' <- "):
            mapper.process_batch(_batch(A=[1], id=[2]))

    def test_distinct_outputs_are_fine(self):
        out, results = _mapper(explicit_mappings={"A": "x"}).process_batch(_batch(A=[1], B=[2]))
        assert out.schema.names == ["x", "B"] and results == []

    def test_dropped_column_does_not_collide(self):
        mapper = _mapper(explicit_mappings={"A": "id"}, drop_unmapped=True)
        out, _ = mapper.process_batch(_batch(A=[1], id=[2]))
        assert out.schema.names == ["id"]
        assert out.column("id").to_pylist() == [1]

    def test_custom_transform_must_return_a_name(self):
        mapper = _mapper(custom_transform=lambda name: None)
        with pytest.raises(ValueError, match="custom_transform"):
            mapper.process_batch(_batch(a=[1]))

    def test_drop_unmapped_alone_drops(self):
        mapper = _mapper(explicit_mappings={"A": "x"}, drop_unmapped=True)  # allow_unmapped=True
        out, _ = mapper.process_batch(_batch(A=[1], B=[2], C=[3]))
        assert out.schema.names == ["x"]

    def test_explicit_identity_mapping_counts_as_mapped(self):
        mapper = _mapper(explicit_mappings={"A": "A"}, drop_unmapped=True)
        out, _ = mapper.process_batch(_batch(A=[1], B=[2]))
        assert out.schema.names == ["A"]

    def test_convention_renamed_column_is_still_unmapped(self):
        mapper = _mapper(
            explicit_mappings={"keepMe": "kept"},
            naming_convention="snake_case",
            drop_unmapped=True,
        )
        out, _ = mapper.process_batch(_batch(keepMe=[1], OtherColumn=[2]))
        assert out.schema.names == ["kept"]

    def test_case_insensitive_explicit_mapping_counts_as_mapped(self):
        mapper = _mapper(explicit_mappings={"abc": "x"}, case_sensitive=False, drop_unmapped=True)
        out, _ = mapper.process_batch(_batch(ABC=[1], B=[2]))
        assert out.schema.names == ["x"]

    def test_unmapped_columns_are_kept_by_default(self):
        out, _ = _mapper(explicit_mappings={"A": "x"}).process_batch(_batch(A=[1], B=[2]))
        assert out.schema.names == ["x", "B"]

    @pytest.mark.parametrize(
        "name,camel,pascal,snake",
        [
            ("firstName", "firstName", "FirstName", "first_name"),
            ("StateID", "stateId", "StateId", "state_id"),
            ("First Name", "firstName", "FirstName", "first_name"),
            ("first-name", "firstName", "FirstName", "first_name"),
            ("first.name", "firstName", "FirstName", "first_name"),
            ("FIRST_NAME", "firstName", "FirstName", "first_name"),
            ("XMLParser", "xmlParser", "XmlParser", "xml_parser"),
            ("address2Line", "address2Line", "Address2Line", "address2_line"),
            ("already_camelCase", "alreadyCamelCase", "AlreadyCamelCase", "already_camel_case"),
            ("simple", "simple", "Simple", "simple"),
        ],
    )
    def test_case_conversions(self, name, camel, pascal, snake):
        mapper = _mapper()
        assert mapper._to_camel_case(name) == camel
        assert mapper._to_pascal_case(name) == pascal
        assert mapper._to_snake_case(name) == snake

    def test_snake_case_keeps_leading_and_trailing_underscores(self):
        mapper = _mapper()
        assert mapper._to_snake_case("_rownum") == "_rownum"
        assert mapper._to_snake_case("_source_uri") == "_source_uri"
        assert mapper._to_snake_case("class_") == "class_"
        assert mapper._to_snake_case("__x__") == "__x__"

    def test_conversions_through_the_mapper(self):
        for convention, expected in (
            ("snake_case", ["first_name", "state_id"]),
            ("camelCase", ["firstName", "stateId"]),
            ("PascalCase", ["FirstName", "StateId"]),
        ):
            out, _ = _mapper(naming_convention=convention).process_batch(
                _batch(**{"First Name": [1], "StateID": [2]})
            )
            assert out.schema.names == expected


# ---------------------------------------------------------------------------------------------
# 12. Top-level bad rows handler
# ---------------------------------------------------------------------------------------------


class TestBadRowsHandlerFixes:
    def test_zero_cap_collects_nothing(self):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig(max_bad_rows=0))
        for i in range(3):
            handler.add_bad_row({"id": i}, i)
        assert handler.has_bad_rows() is False
        assert handler.bad_row_count == 3 and handler.dropped_bad_row_count == 3

    def test_cap_keeps_counting_so_percentages_stay_right(self):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig(max_bad_rows=2))
        for i in range(5):
            handler.add_bad_row({"id": i}, i)
        handler.increment_row_count(5)
        summary = handler.get_summary()
        assert summary["bad_rows_count"] == 5
        assert summary["bad_rows_collected"] == 2 and summary["bad_rows_dropped"] == 3
        assert summary["bad_rows_percentage"] == 100.0

    def test_cap_warns_once_not_per_row(self, caplog):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig(max_bad_rows=1))
        with caplog.at_level(logging.WARNING, logger="forklift.processors.bad_rows_handler"):
            for i in range(6):
                handler.add_bad_row({"id": i}, i)
        warnings = [r for r in caplog.records if "limit" in r.getMessage()]
        assert len(warnings) == 1

    def test_no_cap_by_default(self):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig())
        for i in range(50):
            handler.add_bad_row({"id": i}, i)
        assert len(handler.bad_rows) == 50

    def test_add_bad_rows_from_batch_uses_global_indices(self):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig())
        handler.add_bad_rows_from_batch(_batch(id=[1, 2]), [1], [])
        handler.increment_row_count(2)
        handler.add_bad_rows_from_batch(_batch(id=[3, 4]), [1], [])
        assert [row["row_index"] for row in handler.bad_rows] == [1, 3]

    def test_default_file_names_are_unique_and_honour_the_directory(self, tmp_path):
        handler = TopLevelBadRowsHandler(
            TopLevelBadRowsConfig(output_path=tmp_path / "out", output_format="json")
        )
        handler.add_bad_row({"id": 1}, 0)
        # a directory that does not exist yet needs a trailing separator to be recognised
        (tmp_path / "out").mkdir()
        first = handler.write_bad_rows()
        second = handler.write_bad_rows()
        assert first.parent == tmp_path / "out" == second.parent
        assert first != second and first.exists() and second.exists()
        assert str(os.getpid()) in first.name

    def test_explicit_file_path_is_used_as_is(self, tmp_path):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig(output_format="json"))
        handler.add_bad_row({"id": 1}, 0)
        target = tmp_path / "sub" / "mine.json"
        assert handler.write_bad_rows(target) == target and target.exists()

    @pytest.mark.parametrize("prefix", ["=", "+", "-", "@"])
    def test_csv_formula_injection_is_neutralised(self, tmp_path, prefix):
        handler = TopLevelBadRowsHandler(
            TopLevelBadRowsConfig(output_format="csv", create_summary=False)
        )
        handler.add_bad_row({"name": f"{prefix}CMD|' /C calc'!A0", "n": 5}, 0)
        path = handler.write_bad_rows(tmp_path / "bad.csv")
        content = path.read_text()
        assert f"'{prefix}CMD" in content
        assert f'"{prefix}CMD' not in content

    def test_csv_formula_protection_can_be_disabled(self, tmp_path):
        handler = TopLevelBadRowsHandler(
            TopLevelBadRowsConfig(
                output_format="csv", create_summary=False, csv_formula_protection=False
            )
        )
        handler.add_bad_row({"name": "=1+1"}, 0)
        path = handler.write_bad_rows(tmp_path / "bad.csv")
        assert '"=1+1"' in path.read_text()

    def test_rows_with_different_columns_are_written_with_all_columns(self, tmp_path):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig(create_summary=False))
        handler.add_bad_row({"a": 1}, 0)
        handler.add_bad_row({"a": 2, "b": "later column"}, 1)
        table = pq.read_table(handler.write_bad_rows(tmp_path / "bad.parquet"))
        assert "original_b" in table.column_names
        assert table.column("original_b").to_pylist() == [None, "later column"]

    def test_mixed_types_in_a_column_do_not_break_the_write(self, tmp_path):
        handler = TopLevelBadRowsHandler(TopLevelBadRowsConfig(create_summary=False))
        handler.add_bad_row({"a": 1}, 0)
        handler.add_bad_row({"a": "x"}, 1)
        table = pq.read_table(handler.write_bad_rows(tmp_path / "bad.parquet"))
        assert table.column("original_a").to_pylist() == ["1", "x"]


# ---------------------------------------------------------------------------------------------
# 13. write-time validator, checked column lookups, schema transformer, shared base classes
# ---------------------------------------------------------------------------------------------


def _write_validator(**kwargs):
    return WriteTimeValidator(WriteTimeConfig(**kwargs))


class TestWriteTimeValidatorFixes:
    def test_in_batch_duplicates_are_reported_once(self):
        validator = _write_validator(primary_key_columns=["id"], check_duplicate_rows=True)
        _, results = validator.process_batch(_batch(id=[1, 2, 2, 3]))
        duplicates = [r for r in results if r.error_code == "DUPLICATE_PRIMARY_KEYS"]
        assert len(duplicates) == 1
        assert "Found 1 duplicate" in duplicates[0].error_message
        assert "[2]" in duplicates[0].error_message

    def test_two_null_keys_count_once(self):
        validator = _write_validator(primary_key_columns=["id"], check_duplicate_rows=True)
        batch = pa.RecordBatch.from_arrays([pa.array([None, None, 1])], ["id"])
        _, results = validator.process_batch(batch)
        duplicates = [r for r in results if r.error_code == "DUPLICATE_PRIMARY_KEYS"]
        assert "Found 1 duplicate" in duplicates[0].error_message
        assert "[1]" in duplicates[0].error_message

    def test_duplicates_across_batches(self):
        validator = _write_validator(primary_key_columns=["id"], check_duplicate_rows=True)
        validator.process_batch(_batch(id=[1, 2]))
        _, results = validator.process_batch(_batch(id=[2, 3, 3]))
        message = [r for r in results if r.error_code == "DUPLICATE_PRIMARY_KEYS"][0].error_message
        assert "Found 2 duplicate" in message

    def test_hashed_key_mode_detects_the_same_duplicates(self):
        validator = _write_validator(
            primary_key_columns=["id", "name"], check_duplicate_rows=True, hash_primary_keys=True
        )
        validator.process_batch(_batch(id=[1, 2], name=["a", "b"]))
        _, results = validator.process_batch(_batch(id=[1, 2, 3], name=["a", "x", "c"]))
        assert "Found 1 duplicate" in results[0].error_message
        assert all(
            isinstance(token, bytes) and len(token) == 16 for token in validator._seen_primary_keys
        )

    def test_a_short_final_batch_does_not_fail(self):
        validator = _write_validator(min_row_count=5)
        _, first = validator.process_batch(_batch(x=list(range(5))))
        _, last = validator.process_batch(_batch(x=[1]))
        assert not [r for r in first + last if r.error_code == "INSUFFICIENT_ROWS"]
        assert validator.finalize() == []

    def test_finalize_checks_the_total_row_count(self):
        validator = _write_validator(min_row_count=10)
        validator.process_batch(_batch(x=[1, 2, 3]))
        results = validator.finalize()
        assert [r.error_code for r in results] == ["INSUFFICIENT_ROWS"]
        assert "3 rows" in results[0].error_message

    def test_finalize_reports_an_empty_table(self):
        validator = _write_validator()
        validator.process_batch(pa.RecordBatch.from_arrays([pa.array([], pa.int64())], ["x"]))
        assert [r.error_code for r in validator.finalize()] == ["EMPTY_TABLE"]

    def test_finalize_respects_check_empty_tables(self):
        validator = _write_validator(check_empty_tables=False)
        assert validator.finalize() == []

    def test_reset_state_restarts_the_row_total(self):
        validator = _write_validator(min_row_count=2)
        validator.process_batch(_batch(x=[1, 2, 3]))
        validator.reset_state()
        assert [r.error_code for r in validator.finalize()] == ["EMPTY_TABLE"]

    def test_duplicate_primary_key_column_names_are_an_error(self):
        validator = _write_validator(
            primary_key_columns=["id"], check_duplicate_rows=True, check_null_primary_keys=True
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1]), pa.array([1])],
            schema=pa.schema([("id", pa.int64()), ("id", pa.int64())]),
        )
        _, results = validator.process_batch(batch)
        assert any(r.error_code == "WRITE_VALIDATION_ERROR" for r in results)


class TestCheckedColumnLookups:
    def _duplicated(self):
        return pa.RecordBatch.from_arrays(
            [pa.array(["a"]), pa.array(["b"])],
            schema=pa.schema([("c", pa.string()), ("c", pa.string())]),
        )

    def test_quality_processor_raises_on_duplicate_names(self):
        processor = DataQualityProcessor({"column_rules": {"c": {"min_length": 5}}})
        with pytest.raises(ValueError, match="ambiguous"):
            processor.process_batch(self._duplicated())

    def test_column_transformer_raises_on_duplicate_names(self):
        transformer = ColumnTransformer({"c": [uppercase]})
        with pytest.raises(ValueError, match="ambiguous"):
            transformer.process_batch(self._duplicated())

    def test_schema_transformer_raises_on_duplicate_names(self):
        from forklift.processors.transformations import SchemaBasedTransformer

        transformer = SchemaBasedTransformer(
            {
                "x-transformations": {
                    "column_transformations": {
                        "c": {"string_replace": {"enabled": True, "old": "a", "new": "z"}}
                    }
                }
            }
        )
        with pytest.raises(ValueError, match="ambiguous"):
            transformer.process_batch(self._duplicated())

    def test_unique_and_missing_names_still_work(self):
        processor = DataQualityProcessor(
            {"column_rules": {"c": {"min_length": 5}, "zz": {"min_length": 1}}}
        )
        _, results = processor.process_batch(_batch(c=["abc"]))
        assert [r.error_code for r in results] == ["MIN_LENGTH_VIOLATION"]
        out, _ = ColumnTransformer({"c": [uppercase]}).process_batch(_batch(c=["abc"]))
        assert out.column("c").to_pylist() == ["ABC"]


class TestSchemaBasedTransformerFixes:
    SSN_SCHEMA = {
        "properties": {"ssn": {"type": "string", "x-special-type": "ssn"}},
        "x-transformations": {
            "column_transformations": {
                "ssn": {
                    "regex_replace": {"enabled": True, "pattern": r"^SSN:\s*", "replacement": ""}
                }
            }
        },
    }

    def test_explicit_transformations_run_before_the_special_type_step(self):
        from forklift.processors.transformations import SchemaBasedTransformer

        transformer = SchemaBasedTransformer(self.SSN_SCHEMA)
        steps = transformer.column_transformations["ssn"]
        assert [getattr(step, "special_type", None) for step in steps] == [None, "ssn"]
        out, results = transformer.process_batch(_batch(ssn=["SSN: 123-45-6789", "123456789"]))
        assert out.column("ssn").to_pylist() == ["123-45-6789", "123-45-6789"]
        assert results == []

    def test_invalid_special_values_are_reported(self):
        from forklift.processors.transformations import SchemaBasedTransformer

        transformer = SchemaBasedTransformer(self.SSN_SCHEMA)
        batch = pa.RecordBatch.from_arrays(
            [pa.array(["123-45-6789", "garbage", None, "SSN: 000"])], ["ssn"]
        )
        out, results = transformer.process_batch(batch)
        assert [r.row_index for r in results] == [1, 3]
        assert all(
            not r.is_valid and r.error_code == "INVALID_SPECIAL_VALUE" and r.column_name == "ssn"
            for r in results
        )
        assert out.column("ssn").to_pylist()[1] is None
        assert "garbage" not in " ".join(r.error_message for r in results)

    def test_configuration_errors_raise(self):
        from forklift.processors.transformations import SchemaBasedTransformer

        with pytest.raises(ValueError, match="no_such_transform.*col"):
            SchemaBasedTransformer(
                {
                    "x-transformations": {
                        "column_transformations": {"col": {"no_such_transform": {"enabled": True}}}
                    }
                }
            )

    def test_failing_transformation_raises_instead_of_passing_data_through(self):
        from forklift.processors.transformations import SchemaBasedTransformer

        transformer = SchemaBasedTransformer({})

        def broken(column):
            raise RuntimeError("secret-value-9")

        transformer.column_transformations = {"c": [broken]}
        with pytest.raises(ValueError, match="failed for column 'c'") as exc_info:
            transformer.process_batch(_batch(c=["secret-value-9"]))
        assert "secret-value-9" not in str(exc_info.value)


class TestSharedBaseClasses:
    def test_schema_validator_uses_the_shared_classes(self):
        from forklift.processors.base import BaseProcessor as SharedProcessor
        from forklift.processors.base import ValidationResult as SharedResult
        from forklift.processors.schema_validator import base_local

        assert base_local.BaseProcessor is SharedProcessor
        assert base_local.ValidationResult is SharedResult
        assert issubclass(SchemaValidator, SharedProcessor)

    def test_results_of_different_processors_are_interchangeable(self):
        from forklift.processors.base import ValidationResult as SharedResult

        validator = _schema_validator([{"name": "a", "type": "int64"}])
        _, results = validator.process_batch(_batch(b=[1]))
        assert results and all(isinstance(r, SharedResult) for r in results)

    def test_pipeline_accepts_schema_validator(self):
        pipeline = ProcessorPipeline(
            [_schema_validator([{"name": "a", "type": "int64"}], extra_columns_allowed=True)]
        )
        _, results = pipeline.process_batch(_batch(a=[1]))
        assert results == []


class TestShimsAreGone:
    def test_package_imports_still_work(self):
        from forklift.processors.calculated_columns import CalculatedColumnsProcessor
        from forklift.processors.data_validation import DataValidationProcessor
        from forklift.processors.schema_validator import SchemaValidator as Validator
        from forklift.processors.transformations import SchemaBasedTransformer

        assert all([CalculatedColumnsProcessor, DataValidationProcessor, Validator])
        assert SchemaBasedTransformer

    def test_dead_shim_files_were_removed(self):
        import forklift.processors as processors

        directory = os.path.dirname(processors.__file__)
        for name in (
            "calculated_columns",
            "schema_validator",
            "transformations",
            "data_validation",
        ):
            assert not os.path.exists(os.path.join(directory, f"{name}.py"))
            assert os.path.isdir(os.path.join(directory, name))
