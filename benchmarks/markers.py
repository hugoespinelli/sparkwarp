"""The benchmark's known-gap marker, shared with the corpus."""

from __future__ import annotations

import pytest

from corpus.gaps import KnownGap
from sparkwarp.runtime import Runtime


def known_sail_gap(gap_id: str) -> pytest.MarkDecorator:
    """Strict xfail on Sail only. The test still runs and fails once the gap closes."""
    return KnownGap(gap_id).mark(on_sail=Runtime.current() is not None)
