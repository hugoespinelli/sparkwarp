"""Java regex features that Sail's Rust regex rejects, evaluated in Python instead.

Spark uses `java.util.regex`; Sail uses Rust's `regex`, which has no look-around,
backreferences or atomic groups. When a regex function gets such a pattern as a literal,
sparkwarp plans it as an Arrow Python UDF built on `re`, with the pattern translated to
Python syntax. A pattern whose meaning could differ between the two engines, and every
pattern Rust accepts, keeps Sail's native path.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from types import MethodType
from typing import ClassVar, cast

from pyspark.sql.connect.client.core import SparkConnectClient
from pyspark.sql.connect.column import Column as ConnectColumn
from pyspark.sql.connect.expressions import (
    Expression,
    LiteralExpression,
    UnresolvedFunction,
)
from pyspark.sql.connect.functions import lit
from pyspark.sql.connect.proto import expressions_pb2
from pyspark.sql.connect.udf import UserDefinedFunction
from pyspark.util import PythonEvalType

from sparkwarp.patches import ClassPatch

Init = Callable[..., None]
Evaluator = Callable[..., object]


class PatternScan:
    """One pass over a Java pattern, translating only syntax both engines read alike.

    Anything outside that subset, `\\b` and octal escapes among it, leaves no
    translation, so the call keeps Sail's native path and fails loudly there.
    """

    LINE_TERMINATORS: ClassVar[str] = "\\n\\r\\u0085\\u2028\\u2029"
    JAVA_DOT: ClassVar[str] = f"[^{LINE_TERMINATORS}]"
    JAVA_END: ClassVar[str] = (
        f"(?:(?=(?:\\r\\n|[{LINE_TERMINATORS}])?\\Z)(?!(?<=\\r)\\n\\Z))"
    )
    PLAIN: ClassVar[dict[str, str]] = {".": JAVA_DOT, "$": JAVA_END}
    ESCAPES: ClassVar[dict[str, str]] = {"Z": JAVA_END, "z": "\\Z"}
    SAME_LETTER_ESCAPES: ClassVar[frozenset[str]] = frozenset("dDwWsStnrfA")
    RUST_REJECTED_GROUPS: ClassVar[tuple[str, ...]] = (
        "(?=",
        "(?!",
        "(?<=",
        "(?<!",
        "(?>",
    )
    POSSESSIVE_AFTER_GROUP: ClassVar[re.Pattern[str]] = re.compile(
        r"(?:[?*+]|\{[0-9]+(?:,[0-9]*)?\})\+"
    )
    SURROGATES: ClassVar[range] = range(0xD800, 0xE000)
    CODE_POINT_DIGITS: ClassVar[int] = 16
    SAME_GROUPS: ClassVar[tuple[str, ...]] = ("(?:", "(?i:")
    CASE_INSENSITIVE: ClassVar[str] = "(?i)"
    GUARDED: ClassVar[str] = "guarded"
    CAPTURE: ClassVar[str] = "capture"
    PLAIN_GROUP: ClassVar[str] = "plain"
    NAMED_GROUP: ClassVar[re.Pattern[str]] = re.compile(r"\(\?<([A-Za-z][A-Za-z0-9]*)>")
    NAMED_BACKREFERENCE: ClassVar[re.Pattern[str]] = re.compile(
        r"\\k<([A-Za-z][A-Za-z0-9]*)>"
    )
    NUMBERED_BACKREFERENCE: ClassVar[re.Pattern[str]] = re.compile(r"\\[1-9](?![0-9])")
    CODE_POINT: ClassVar[re.Pattern[str]] = re.compile(
        r"\\x[0-9a-fA-F]{2}|\\u[0-9a-fA-F]{4}"
    )
    QUANTIFIER: ClassVar[re.Pattern[str]] = re.compile(r"\{[0-9]+(?:,[0-9]*)?\}")

    def __init__(self, text: str) -> None:
        self._text = text
        self._out: list[str] = []
        self.rust_rejects = False
        self._translatable = True
        self._open_groups: list[str] = []
        index = 0
        while index < len(text) and self._translatable:
            index = self._step(index)

    @property
    def python_text(self) -> str | None:
        return "".join(self._out) if self._translatable else None

    def _step(self, index: int) -> int:
        char = self._text[index]
        if char == "[":
            return self._character_class(index)
        if char == "\\":
            return self._escape(index)
        if char == "(":
            return self._group(index)
        if char == "{":
            return self._copy(self.QUANTIFIER.match(self._text, index))
        if char == ")":
            return self._close_group(index)
        self._out.append(self.PLAIN.get(char, char))
        return index + 1

    def _escape(self, index: int) -> int:
        named = self.NAMED_BACKREFERENCE.match(self._text, index)
        if named is not None:
            self.rust_rejects = True
            self._out.append(f"(?P={named.group(1)})")
            return named.end()
        numbered = self.NUMBERED_BACKREFERENCE.match(self._text, index)
        self.rust_rejects = self.rust_rejects or numbered is not None
        escape = numbered or self._code_point(index)
        if escape is not None:
            return self._copy(escape)
        escaped = self._text[index + 1 : index + 2]
        if escaped in self.ESCAPES:
            self.rust_rejects = self.rust_rejects or escaped == "Z"
            self._out.append(self.ESCAPES[escaped])
            return index + 2
        self._translatable = self._translatable and self._reads_alike(escaped)
        self._out.append(self._text[index : index + 2])
        return index + 2

    def _group(self, index: int) -> int:
        rejected = self._first_prefix(self.RUST_REJECTED_GROUPS, index)
        if rejected is not None:
            self.rust_rejects = True
            self._open_groups.append(self.GUARDED)
            self._out.append(rejected)
            return index + len(rejected)
        named = self.NAMED_GROUP.match(self._text, index)
        if named is not None:
            self._open_capture()
            self._out.append(f"(?P<{named.group(1)}>")
            return named.end()
        opener = self._shared_group(index)
        self._translatable = self._translatable and opener is not None
        if opener == "(":
            self._open_capture()
        elif opener != self.CASE_INSENSITIVE:
            self._open_groups.append(self.PLAIN_GROUP)
        self._out.append(opener or "")
        return index + len(opener or "(")

    def _open_capture(self) -> None:
        """Java keeps captures made in look-around or atomic groups; Python does not."""
        self._translatable = (
            self._translatable and self.GUARDED not in self._open_groups
        )
        self._open_groups.append(self.CAPTURE)

    def _close_group(self, index: int) -> int:
        """Possessive quantifiers on groups also keep captures that Python resets."""
        if self._open_groups:
            self._open_groups.pop()
        possessive = self.POSSESSIVE_AFTER_GROUP.match(self._text, index + 1)
        self._translatable = self._translatable and possessive is None
        self._out.append(")")
        return index + 1

    def _shared_group(self, index: int) -> str | None:
        """A capturing, non-capturing or `(?i)` group, which both engines read alike."""
        same = self._first_prefix(self.SAME_GROUPS, index)
        if same is not None:
            return same
        leading_flag = index == 0 and self._text.startswith(self.CASE_INSENSITIVE)
        if leading_flag:
            return self.CASE_INSENSITIVE
        is_plain_group = not self._text.startswith("(?", index)
        return "(" if is_plain_group else None

    def _first_prefix(self, prefixes: tuple[str, ...], index: int) -> str | None:
        return next((p for p in prefixes if self._text.startswith(p, index)), None)

    def _character_class(self, index: int) -> int:
        """A class of literals, ranges and shared escapes; nothing nested."""
        end = index + 1
        end += self._text.startswith("^", end)
        end += self._text.startswith("]", end)
        while end < len(self._text) and self._text[end] != "]":
            member_end = self._class_member(end)
            if member_end is None:
                self._translatable = False
                return len(self._text)
            end = member_end
        self._translatable = self._translatable and end < len(self._text)
        self._out.append(self._text[index : end + 1])
        return end + 1

    def _class_member(self, index: int) -> int | None:
        """Where a class member ends, or None when Python would read it differently."""
        nested_or_intersection = self._text.startswith(("[", "&&"), index)
        if nested_or_intersection:
            return None
        if self._text[index] != "\\":
            return index + 1
        code_point = self._code_point(index)
        if code_point is not None:
            return code_point.end()
        escaped = self._text[index + 1 : index + 2]
        return index + 2 if self._reads_alike(escaped) else None

    def _code_point(self, index: int) -> re.Match[str] | None:
        """A `\\xhh` or `\\uhhhh` escape; a surrogate half stops the translation."""
        escape = self.CODE_POINT.match(self._text, index)
        if escape is None:
            return None
        is_surrogate = (
            int(escape.group()[2:], self.CODE_POINT_DIGITS) in self.SURROGATES
        )
        self._translatable = self._translatable and not is_surrogate
        return escape

    def _copy(self, token: re.Match[str] | None) -> int:
        self._translatable = self._translatable and token is not None
        if token is None:
            return len(self._text)
        self._out.append(token.group())
        return token.end()

    def _reads_alike(self, escaped: str) -> bool:
        """`\\d`-style classes, control escapes and escaped ASCII punctuation."""
        is_punctuation = escaped.isascii() and bool(escaped) and not escaped.isalnum()
        return escaped in self.SAME_LETTER_ESCAPES or is_punctuation


class JavaPattern:
    """A `java.util.regex` pattern: whether Rust runs it, and its Python translation."""

    def __init__(self, text: str) -> None:
        self.text = text
        self._scan = PatternScan(text)

    @property
    def needs_python(self) -> bool:
        return self._scan.rust_rejects

    def compiled(self) -> re.Pattern[str] | None:
        """The pattern in Python, or None when Python cannot express it the same way."""
        python_text = self._scan.python_text
        if python_text is None:
            return None
        try:
            return re.compile(python_text, re.ASCII)
        except re.error:
            return None

    def matcher(self) -> JavaMatcher | None:
        """A Java-style matcher, or None when the native path should run."""
        compiled = self.compiled() if self.needs_python else None
        if compiled is None:
            return None
        return JavaMatcher(compiled)


class OutsideBmpError(ValueError):
    """Java would read this text as UTF-16 code units, where Python sees code points."""


@dataclass(frozen=True)
class JavaMatcher:
    """`Matcher.find` semantics on top of `re`.

    After an empty match Java searches again from the next character, where Python's
    `finditer` may start a longer match at the same place. Java matches text as UTF-16
    code units, so text with a character outside the Basic Multilingual Plane raises
    instead of risking a different value.
    """

    pattern: re.Pattern[str]

    LAST_BMP_CODE_POINT: ClassVar[int] = 0xFFFF
    OUTSIDE_BMP_MESSAGE: ClassVar[str] = (
        "sparkwarp's regex fallback cannot match Java on text with a character outside "
        "the Basic Multilingual Plane"
    )

    def first(self, value: str) -> re.Match[str] | None:
        self._check_text(value)
        return self.pattern.search(value)

    def all(self, value: str) -> list[re.Match[str]]:
        self._check_text(value)
        matches: list[re.Match[str]] = []
        position = 0
        while position <= len(value):
            match = self.pattern.search(value, position)
            if match is None:
                break
            matches.append(match)
            is_empty = match.start() == match.end()
            position = match.end() + 1 if is_empty else match.end()
        return matches

    def _check_text(self, value: str) -> None:
        outside_bmp = any(ord(char) > self.LAST_BMP_CODE_POINT for char in value)
        if outside_bmp:
            raise OutsideBmpError(self.OUTSIDE_BMP_MESSAGE)


class JavaReplacement:
    """A replacement string read the way `Matcher.appendReplacement` reads it."""

    MISSING_ESCAPE: ClassVar[str] = "character to be escaped is missing"
    BAD_REFERENCE: ClassVar[str] = "Illegal group reference"
    NO_GROUP: ClassVar[str] = "No group {group}"
    ASCII_DIGITS: ClassVar[str] = "0123456789"

    def __init__(self, text: str) -> None:
        self.text = text

    def expand(self, match: re.Match[str]) -> str:
        parts: list[str] = []
        index = 0
        while index < len(self.text):
            char = self.text[index]
            if char == "\\":
                escaped = self.text[index + 1 : index + 2]
                if not escaped:
                    raise ValueError(self.MISSING_ESCAPE)
                parts.append(escaped)
                index += 2
            elif char == "$":
                group, index = self._reference(match, index + 1)
                parts.append(group)
            else:
                parts.append(char)
                index += 1
        return "".join(parts)

    def _reference(self, match: re.Match[str], index: int) -> tuple[str, int]:
        """The group a `$` refers to, and where the replacement text continues."""
        if self.text.startswith("{", index):
            return self._named_group(match, index)
        return self._numbered_group(match, index)

    def _named_group(self, match: re.Match[str], index: int) -> tuple[str, int]:
        end = self.text.find("}", index)
        name = self.text[index + 1 : end]
        is_known_group = end >= 0 and name in match.re.groupindex
        if not is_known_group:
            raise ValueError(self.BAD_REFERENCE)
        return match.group(name) or "", end + 1

    def _numbered_group(self, match: re.Match[str], index: int) -> tuple[str, int]:
        """Java keeps reading digits while they still name an existing group."""
        digit = self.text[index : index + 1]
        is_digit = len(digit) == 1 and digit in self.ASCII_DIGITS
        if not is_digit:
            raise ValueError(self.BAD_REFERENCE)
        group, index = int(digit), index + 1
        if group > match.re.groups:
            raise IndexError(self.NO_GROUP.format(group=group))
        while index < len(self.text) and self.text[index] in self.ASCII_DIGITS:
            longer = group * 10 + int(self.text[index])
            if longer > match.re.groups:
                break
            group, index = longer, index + 1
        return match.group(group) or "", index


@dataclass(frozen=True)
class ExtractFirst:
    """`regexp_extract`: group `index` of the first match, "" when nothing matches."""

    matcher: JavaMatcher
    index: int

    @classmethod
    def of(cls, matcher: JavaMatcher, rest: Sequence[object]) -> Evaluator | None:
        index = rest[0] if rest else None
        is_group_index = isinstance(index, int) and not isinstance(index, bool)
        return cls(matcher, int(str(index))) if is_group_index else None

    def __call__(self, value: str | None, *literals: object) -> str | None:
        if value is None:
            return None
        match = self.matcher.first(value)
        return (match.group(self.index) or "") if match else ""


@dataclass(frozen=True)
class ExtractAll:
    """`regexp_extract_all`: group `index` of every match."""

    matcher: JavaMatcher
    index: int

    @classmethod
    def of(cls, matcher: JavaMatcher, rest: Sequence[object]) -> Evaluator | None:
        index = rest[0] if rest else RegexFunction.DEFAULT_GROUP
        is_group_index = isinstance(index, int) and not isinstance(index, bool)
        return cls(matcher, int(str(index))) if is_group_index else None

    def __call__(self, value: str | None, *literals: object) -> list[str] | None:
        if value is None:
            return None
        return [match.group(self.index) or "" for match in self.matcher.all(value)]


@dataclass(frozen=True)
class ReplaceAll:
    """`regexp_replace` from the first character, with Java's replacement syntax."""

    matcher: JavaMatcher
    replacement: JavaReplacement

    @classmethod
    def of(cls, matcher: JavaMatcher, rest: Sequence[object]) -> Evaluator | None:
        replacement = rest[0] if rest else None
        starts_at_first_character = rest[1:] in ([], [RegexFunction.FIRST_POSITION])
        if not isinstance(replacement, str) or not starts_at_first_character:
            return None
        return cls(matcher, JavaReplacement(replacement))

    def __call__(self, value: str | None, *literals: object) -> str | None:
        if value is None:
            return None
        parts: list[str] = []
        copied_up_to = 0
        for match in self.matcher.all(value):
            parts.append(value[copied_up_to : match.start()])
            parts.append(self.replacement.expand(match))
            copied_up_to = match.end()
        parts.append(value[copied_up_to:])
        return "".join(parts)


@dataclass(frozen=True)
class Finds:
    """`rlike`, `regexp`, `regexp_like`: whether the pattern matches anywhere."""

    matcher: JavaMatcher

    @classmethod
    def of(cls, matcher: JavaMatcher, rest: Sequence[object]) -> Evaluator | None:
        return cls(matcher) if not rest else None

    def __call__(self, value: str | None, *literals: object) -> bool | None:
        if value is None:
            return None
        return self.matcher.first(value) is not None


@dataclass(frozen=True)
class RegexFunction:
    """How one Spark regex function is rebuilt: its evaluator and its printed name."""

    printed_name: str
    return_type: str
    evaluator: Callable[[JavaMatcher, Sequence[object]], Evaluator | None]
    arity_without_default: int | None = None

    DEFAULT_GROUP: ClassVar[int] = 1
    FIRST_POSITION: ClassVar[int] = 1


REGEX_FUNCTIONS: dict[str, RegexFunction] = {
    "regexp_extract": RegexFunction("regexp_extract", "string", ExtractFirst.of),
    "regexp_extract_all": RegexFunction(
        "regexp_extract_all", "array<string>", ExtractAll.of, arity_without_default=2
    ),
    "regexp_replace": RegexFunction(
        "regexp_replace", "string", ReplaceAll.of, arity_without_default=3
    ),
    "rlike": RegexFunction("RLIKE", "boolean", Finds.of),
    "regexp": RegexFunction("REGEXP", "boolean", Finds.of),
    "regexp_like": RegexFunction("REGEXP_LIKE", "boolean", Finds.of),
}


class RegexCall:
    """A regex call whose literal pattern Rust rejects, rebuilt as a Python UDF."""

    def __init__(self, function: UnresolvedFunction) -> None:
        self._spec = REGEX_FUNCTIONS.get(function._name)
        self._args: Sequence[Expression] = function._args

    def column(self) -> ConnectColumn | None:
        """The UDF column, or None when the native path should run."""
        evaluator = self._evaluator()
        if evaluator is None or self._spec is None:
            return None
        udf = UserDefinedFunction(
            evaluator,
            returnType=self._spec.return_type,
            name=self._spec.printed_name,
            evalType=PythonEvalType.SQL_ARROW_BATCHED_UDF,
        )
        column: ConnectColumn = udf(*self._printed_args(self._spec))
        return column

    def _printed_args(self, spec: RegexFunction) -> list[ConnectColumn]:
        """The call's arguments, plus the default Spark prints in the column name."""
        args = [ConnectColumn(arg) for arg in self._args]
        omits_default = len(args) == spec.arity_without_default
        if omits_default:
            args.append(cast(ConnectColumn, lit(RegexFunction.DEFAULT_GROUP)))
        return args

    def _evaluator(self) -> Evaluator | None:
        if self._spec is None:
            return None
        literals = [self._literal(arg) for arg in self._args[1:]]
        all_literal = bool(literals) and None not in literals
        pattern_text = literals[0] if all_literal else None
        if not isinstance(pattern_text, str):
            return None
        matcher = JavaPattern(pattern_text).matcher()
        if matcher is None:
            return None
        return self._spec.evaluator(matcher, literals[1:])

    @staticmethod
    def _literal(arg: Expression) -> object | None:
        return arg._value if isinstance(arg, LiteralExpression) else None


class PythonPlan:
    """The plan of one regex call, built from its Python UDF instead."""

    def __init__(self, column: ConnectColumn) -> None:
        self._column = column

    def __call__(self, session: SparkConnectClient) -> expressions_pb2.Expression:
        plan: expressions_pb2.Expression = self._column._expr.to_plan(session)
        return plan


class FallbackInit:
    """Stands in for `UnresolvedFunction.__init__`; a rejected regex gets a Python plan.

    Only those calls get a different `to_plan`, set on the instance, so other functions
    plan exactly as before and keep their recursion depth.
    """

    PLAN_METHOD: ClassVar[str] = "to_plan"

    def __init__(self, original: Init) -> None:
        self.original = original

    def __get__(
        self, function: UnresolvedFunction | None, owner: type | None = None
    ) -> Callable[..., None] | FallbackInit:
        if function is None:
            return self
        return MethodType(self, function)

    def __call__(
        self, function: UnresolvedFunction, *args: object, **kwargs: object
    ) -> None:
        self.original(function, *args, **kwargs)
        replacement = RegexCall(function).column()
        if replacement is not None:
            setattr(function, self.PLAN_METHOD, PythonPlan(replacement))


class RegexFallbackPatch(ClassPatch):
    """Plans regex functions with Rust-rejected literal patterns as Python UDFs."""

    TARGET = UnresolvedFunction
    ATTRIBUTE = "__init__"

    def _replacement(self, previous: object) -> object:
        return FallbackInit(cast(Init, previous))
