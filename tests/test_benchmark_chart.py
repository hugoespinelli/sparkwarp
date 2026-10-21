"""The README charts are drawn from data, so their geometry and labels are tested."""

import xml.etree.ElementTree as ET

import pytest

from benchmarks.chart import (
    DARK,
    LIGHT,
    EngineNumbers,
    NiceScale,
    PerTestChart,
    Results,
    SuiteChart,
)

SPARK = EngineNumbers(wall_median=31.74, test_medians=[0.3, 0.36, 0.4, 11.9])
SAIL = EngineNumbers(wall_median=2.83, test_medians=[0.02, 0.025, 0.03, 0.146])
RESULTS = Results(spark=SPARK, sail=SAIL, runs=3)
SVG_TAG = "{http://www.w3.org/2000/svg}svg"


@pytest.mark.parametrize(
    ("value", "target", "expected"),
    [(31.75, 4, (40, 10)), (2.83, 4, (3.0, 1)), (0.9, 4, (1.0, 0.5))],
)
def test_nice_scale_rounds_up_to_a_1_2_5_step(
    value: float, target: int, expected: tuple[float, float]
) -> None:
    assert NiceScale.for_max(value, target) == pytest.approx(expected)


def test_per_test_median_handles_even_and_odd_counts() -> None:
    assert SPARK.per_test_median == pytest.approx(0.38)
    assert EngineNumbers(1.0, [0.1, 0.2, 0.9]).per_test_median == pytest.approx(0.2)


def test_speedup_is_spark_over_sail() -> None:
    assert RESULTS.speedup == pytest.approx(31.74 / 2.83)


@pytest.mark.parametrize("theme", [LIGHT, DARK], ids=["light", "dark"])
@pytest.mark.parametrize("chart", [SuiteChart, PerTestChart], ids=["suite", "per-test"])
def test_charts_are_valid_svg_with_an_accessible_title(
    chart: type[SuiteChart] | type[PerTestChart], theme: object
) -> None:
    root = ET.fromstring(chart(RESULTS, theme).render())  # type: ignore[arg-type]
    assert root.tag == SVG_TAG
    assert root.find("{http://www.w3.org/2000/svg}title") is not None


def test_suite_chart_labels_both_engines_and_the_speedup() -> None:
    svg = SuiteChart(RESULTS, LIGHT).render()
    assert "31.7 s" in svg
    assert "2.8 s" in svg
    assert "11x faster" in svg
