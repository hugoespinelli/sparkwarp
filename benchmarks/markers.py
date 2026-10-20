"""Markers that record a known engine gap without hiding it."""

from __future__ import annotations

import pytest

from sparkwarp.runtime import Runtime


class KnownGap:
    """A Sail-vs-Spark difference, registered as a strict xfail on Sail only."""

    def __init__(self, gap_id: str) -> None:
        self.gap_id = gap_id

    @property
    def marker(self) -> pytest.MarkDecorator:
        running_on_sail = Runtime.current() is not None
        return pytest.mark.xfail(
            condition=running_on_sail,
            reason=f"known Sail gap {self.gap_id}, see benchmarks/KNOWN_GAPS.md",
            strict=True,
        )


def known_sail_gap(gap_id: str) -> pytest.MarkDecorator:
    """Strict xfail on Sail only. The test still runs and fails once the gap closes."""
    return KnownGap(gap_id).marker
