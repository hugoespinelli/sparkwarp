"""Known gaps as values. Each id must be in KNOWN_GAPS.md, so typos fail early."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import pytest

REGISTRY_PATH = Path(__file__).resolve().parent.parent / "KNOWN_GAPS.md"
GAP_ROW = re.compile(r"^\| ((?:GAP|ORDER|ADOPT)-\d{3}) \|", re.MULTILINE)


class UnknownGapError(ValueError):
    """A test refers to a gap id that KNOWN_GAPS.md does not list."""


@dataclass(frozen=True)
class KnownGap:
    """A registered difference from real Spark, turned into an xfail on Sail.

    `stable=False` is for differences that vary between runs (row order), which a
    strict xfail cannot pin. The xfail accepts only assertion failures, so a fixture
    error or a bug in the test itself is not mistaken for the known gap. An engine
    error inside a case becomes part of its outcome and is compared like any result.
    """

    gap_id: str
    stable: bool = True

    REASON: ClassVar[str] = "known gap {gap_id}, see KNOWN_GAPS.md"
    _registered: ClassVar[frozenset[str] | None] = None

    def __post_init__(self) -> None:
        if self.gap_id not in self.registered_ids():
            raise UnknownGapError(
                f"{self.gap_id} is not listed in {REGISTRY_PATH.name}"
            )

    @classmethod
    def registered_ids(cls) -> frozenset[str]:
        if cls._registered is None:
            cls._registered = frozenset(GAP_ROW.findall(REGISTRY_PATH.read_text()))
        return cls._registered

    def mark(self, on_sail: bool) -> pytest.MarkDecorator:
        return pytest.mark.xfail(
            condition=on_sail,
            reason=self.REASON.format(gap_id=self.gap_id),
            strict=self.stable,
            raises=AssertionError,
        )
