"""Parity harness: real Spark records outcomes as golden files, every engine is checked.

One process runs one engine, so a same-process pair is impossible. Instead, a run on
real Spark with SPARKWARP_RECORD=1 writes the goldens, and a run on any engine compares
its outcome with them.
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import pytest
from _pytest.mark import ParameterSet
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.utils import is_remote

from corpus.gaps import KnownGap
from sparkwarp.runtime import Runtime

ENGINE_ERROR_MODULES = frozenset(
    {"pyspark.errors.exceptions.captured", "pyspark.errors.exceptions.connect"}
)
RECORD_ENV_VAR = "SPARKWARP_RECORD"
RECORD_ON = "1"
GOLDEN_DIR = Path(__file__).parent / "goldens"
FLOAT_DIGITS = 12
JSON_INDENT = 1

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
Builder = Callable[[], object]


class TypeTag:
    """Prefixes that keep non-JSON values distinguishable after encoding."""

    DECIMAL = "decimal:"
    TIMEDELTA = "timedelta:"
    BYTES = "bytes:"
    SPECIAL_FLOAT = "float:"


class ValueEncoder:
    """Turns collected Spark values into JSON values that compare across engines.

    Rows encode as `[name, value]` pairs by position, so duplicate column names
    after a join are kept and compared.
    """

    @classmethod
    def encode(cls, value: object) -> JsonValue:
        if value is None or isinstance(value, bool | int | str):
            return value
        if isinstance(value, float):
            return cls._float(value)
        if isinstance(value, Row):
            return [
                [name, cls.encode(item)]
                for name, item in zip(value.__fields__, value, strict=True)
            ]
        if isinstance(value, dict):
            return {str(k): cls.encode(v) for k, v in sorted(value.items(), key=str)}
        if isinstance(value, list | tuple):
            return [cls.encode(v) for v in value]
        return cls._scalar(value)

    @staticmethod
    def _float(value: float) -> JsonValue:
        """Rounds to FLOAT_DIGITS significant digits; adding 0.0 folds -0.0 into 0.0."""
        is_special = value != value or value in (float("inf"), float("-inf"))
        if is_special:
            return TypeTag.SPECIAL_FLOAT + repr(value)
        return float(f"{value:.{FLOAT_DIGITS}g}") + 0.0

    @staticmethod
    def _scalar(value: object) -> JsonValue:
        if isinstance(value, decimal.Decimal):
            return f"{TypeTag.DECIMAL}{value}"
        if isinstance(value, datetime.datetime | datetime.date):
            return f"{type(value).__name__}:{value.isoformat()}"
        if isinstance(value, datetime.timedelta):
            return f"{TypeTag.TIMEDELTA}{value.total_seconds()}"
        if isinstance(value, bytes | bytearray):
            return f"{TypeTag.BYTES}{base64.b64encode(bytes(value)).decode()}"
        return f"{type(value).__name__}:{value}"


class SchemaRelaxer:
    """Drops nullability and metadata, as `assertDataFrameEqual` does by default."""

    FLAGS: ClassVar[frozenset[str]] = frozenset(
        {"nullable", "containsNull", "valueContainsNull", "metadata"}
    )

    @classmethod
    def erase(cls, value: JsonValue) -> JsonValue:
        if isinstance(value, dict):
            return {k: cls.erase(v) for k, v in value.items() if k not in cls.FLAGS}
        if isinstance(value, list):
            return [cls.erase(v) for v in value]
        return value


@dataclass(frozen=True)
class Outcome:
    """What a case produced: a schema and rows, or the error raised.

    `condition` is Spark's error condition (e.g. CAST_INVALID_INPUT) when the engine
    reports one; it is compared only when both sides have it.
    """

    schema: JsonValue = None
    rows: JsonValue = None
    error: str | None = None
    condition: str | None = None
    from_client: bool = field(default=False, compare=False)

    @property
    def raised(self) -> bool:
        return self.error is not None

    @classmethod
    def of(cls, build: Builder, ordered: bool, deterministic: bool) -> Outcome:
        try:
            result = build()
            if not isinstance(result, DataFrame):
                return cls(rows=ValueEncoder.encode(result))
            schema: JsonValue = result.schema.jsonValue()
            rows = [ValueEncoder.encode(row) for row in result.collect()]
        except Exception as error:
            return cls.of_error(error)
        if not deterministic:
            return cls(schema=schema)
        if not ordered:
            rows.sort(key=lambda row: json.dumps(row, sort_keys=True))
        return cls(schema=schema, rows=rows)

    @classmethod
    def of_error(cls, error: Exception) -> Outcome:
        condition_of = getattr(error, "getCondition", None)
        condition = condition_of() if callable(condition_of) else None
        from_client = type(error).__module__ not in ENGINE_ERROR_MODULES
        return cls(
            error=type(error).__name__,
            condition=condition or None,
            from_client=from_client,
        )

    def relaxed(self) -> Outcome:
        """The outcome as `assertDataFrameEqual` compares it by default."""
        return Outcome(
            schema=SchemaRelaxer.erase(self.schema),
            rows=self.rows,
            error=self.error,
            condition=self.condition,
        )

    def matches(self, expected: Outcome) -> bool:
        both_report_condition = (
            self.condition is not None and expected.condition is not None
        )
        same_condition = (
            not both_report_condition or self.condition == expected.condition
        )
        same_result = self._exact((self.schema, self.rows, self.error)) == self._exact(
            (expected.schema, expected.rows, expected.error)
        )
        return same_result and same_condition

    @staticmethod
    def _exact(value: object) -> str:
        """JSON text, so 1, 1.0 and True stay distinct; Python's == equates them."""
        return json.dumps(value, sort_keys=True)

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "schema": self.schema,
            "rows": self.rows,
            "error": self.error,
            "condition": self.condition,
        }

    @classmethod
    def from_json(cls, data: dict[str, JsonValue]) -> Outcome:
        error, condition = data.get("error"), data.get("condition")
        return cls(
            schema=data.get("schema"),
            rows=data.get("rows"),
            error=str(error) if error else None,
            condition=str(condition) if condition else None,
        )


@dataclass
class GoldenStore:
    """Golden outcomes, one JSON file per corpus module."""

    directory: Path = GOLDEN_DIR
    _loaded: dict[str, dict[str, dict[str, JsonValue]]] = field(default_factory=dict)
    _dirty: set[str] = field(default_factory=set)

    def get(self, module: str, case: str) -> Outcome | None:
        stored = self._module(module).get(case)
        return Outcome.from_json(stored) if stored is not None else None

    def put(self, module: str, case: str, outcome: Outcome) -> None:
        self._module(module)[case] = outcome.to_json()
        self._dirty.add(module)

    def flush(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        for module in sorted(self._dirty):
            content = dict(sorted(self._loaded[module].items()))
            path = self.directory / f"{module}.json"
            path.write_text(
                json.dumps(content, indent=JSON_INDENT, sort_keys=True) + "\n"
            )
        self._dirty.clear()

    def _module(self, module: str) -> dict[str, dict[str, JsonValue]]:
        if module not in self._loaded:
            path = self.directory / f"{module}.json"
            self._loaded[module] = json.loads(path.read_text()) if path.exists() else {}
        return self._loaded[module]


@dataclass(frozen=True)
class Case:
    """One corpus entry: a named builder plus how to compare its result.

    `expects_error` states the intent. Recording refuses a golden whose outcome kind
    contradicts it, so a broken case cannot become a golden.
    """

    name: str
    build: Callable[[SparkSession], object]
    ordered: bool = False
    deterministic: bool = True
    strict_nullable: bool = False
    expects_error: bool = False
    gap: KnownGap | None = None

    @property
    def param(self) -> ParameterSet:
        marks = [] if self.gap is None else [self.gap.mark(on_sail=self.on_sail())]
        return pytest.param(self, id=self.name, marks=marks)

    @staticmethod
    def on_sail() -> bool:
        """True when the sparkwarp plugin started an engine for this run."""
        return Runtime.current() is not None

    @staticmethod
    def params(cases: list[Case]) -> list[ParameterSet]:
        return [case.param for case in cases]


class ParityChecker:
    """Checks one case against its golden, or records the golden on real Spark."""

    def __init__(self, store: GoldenStore, module: str, case_id: str) -> None:
        self._store = store
        self._module = module
        self._case_id = case_id

    @property
    def recording(self) -> bool:
        record_requested = os.environ.get(RECORD_ENV_VAR) == RECORD_ON
        if record_requested and is_remote():
            pytest.fail(
                f"{RECORD_ENV_VAR}=1 records goldens from real Spark; unset SPARKWARP"
            )
        return record_requested

    def check(self, case: Case, spark: SparkSession) -> None:
        actual = Outcome.of(lambda: case.build(spark), case.ordered, case.deterministic)
        if self.recording:
            self._record(case, actual)
            return
        expected = self._store.get(self._module, self._case_id)
        if expected is None:
            pytest.fail(
                f"no golden for {self._case_id}; "
                f"record it on real Spark with {RECORD_ENV_VAR}=1"
            )
        if not case.strict_nullable:
            actual, expected = actual.relaxed(), expected.relaxed()
        assert actual.matches(
            expected
        ), f"{self._case_id}: differs from the Spark golden"

    def _record(self, case: Case, actual: Outcome) -> None:
        intent_mismatch = actual.raised != case.expects_error
        if intent_mismatch:
            pytest.fail(
                f"{self._case_id}: expects_error={case.expects_error} but real Spark "
                f"gave {actual.error or 'a result'}; fix the case before recording"
            )
        if actual.from_client:
            pytest.fail(
                f"{self._case_id}: {actual.error} came from the Python client, not the "
                "engine, so the case tests nothing; fix the case before recording"
            )
        self._store.put(self._module, self._case_id, actual)
