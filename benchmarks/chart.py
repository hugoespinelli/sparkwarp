"""Draws the README benchmark charts from benchmarks/results.json, light and dark.

Usage: `python -m benchmarks.chart` after `python -m benchmarks.run`.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar
from xml.sax.saxutils import escape

BENCH_DIR = Path(__file__).parent
RESULTS_PATH = BENCH_DIR / "results.json"
ASSET_DIR = BENCH_DIR.parent / "docs" / "assets"
FONT = "-apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
SPARK, SAIL = "spark", "sail"
MS_PER_SECOND = 1000
SVG_NS = "http://www.w3.org/2000/svg"


@dataclass(frozen=True)
class Theme:
    """Chart surface, ink and the two series colors for one color scheme."""

    name: str
    surface: str
    ink: str
    secondary: str
    muted: str
    grid: str
    axis: str
    spark: str
    sail: str


LIGHT = Theme(
    "light",
    "#fcfcfb",
    "#0b0b0b",
    "#52514e",
    "#898781",
    "#e1e0d9",
    "#c3c2b7",
    "#eb6834",
    "#2a78d6",
)
DARK = Theme(
    "dark",
    "#1a1a19",
    "#f0efec",
    "#c3c2b7",
    "#898781",
    "#2c2c2a",
    "#383835",
    "#d95926",
    "#3987e5",
)


@dataclass(frozen=True)
class EngineNumbers:
    """One engine's results, read from results.json."""

    wall_median: float
    test_medians: list[float]

    @property
    def per_test_median(self) -> float:
        ordered = sorted(self.test_medians)
        middle = len(ordered) // 2
        is_even = len(ordered) % 2 == 0
        return (
            (ordered[middle - 1] + ordered[middle]) / 2 if is_even else ordered[middle]
        )


@dataclass(frozen=True)
class Results:
    """Both engines plus how they were measured."""

    spark: EngineNumbers
    sail: EngineNumbers
    runs: int

    @classmethod
    def load(cls, path: Path) -> Results:
        data = json.loads(path.read_text())
        engines = data["engines"]
        return cls(
            cls._engine(engines[SPARK]), cls._engine(engines[SAIL]), data["runs"]
        )

    @staticmethod
    def _engine(raw: dict[str, object]) -> EngineNumbers:
        medians = raw["test_median_seconds"]
        assert isinstance(medians, dict)
        wall = raw["wall_median_seconds"]
        assert isinstance(wall, float)
        return EngineNumbers(wall, [float(v) for v in medians.values()])

    @property
    def test_count(self) -> int:
        return len(self.spark.test_medians)

    @property
    def speedup(self) -> float:
        return self.spark.wall_median / self.sail.wall_median


class Svg:
    """A tiny SVG writer: collects elements, renders a document with a surface."""

    def __init__(
        self, width: int, height: int, theme: Theme, title: str, desc: str
    ) -> None:
        self.width, self.height, self.theme = width, height, theme
        self._parts = [
            f"<title>{escape(title)}</title>",
            f"<desc>{escape(desc)}</desc>",
            f'<rect width="{width}" height="{height}" rx="8" fill="{theme.surface}"/>',
        ]

    def add(self, element: str) -> None:
        self._parts.append(element)

    def text(
        self,
        x: float,
        y: float,
        content: str,
        color: str,
        size: int = 13,
        anchor: str = "start",
        weight: int = 400,
    ) -> None:
        self.add(
            f'<text x="{x:.1f}" y="{y:.1f}" fill="{color}" font-size="{size}" '
            f'font-weight="{weight}" text-anchor="{anchor}">{escape(content)}</text>'
        )

    def line(
        self, x1: float, y1: float, x2: float, y2: float, color: str, width: float = 1
    ) -> None:
        self.add(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'stroke="{color}" stroke-width="{width}"/>'
        )

    def render(self) -> str:
        body = "\n  ".join(self._parts)
        return (
            f'<svg xmlns="{SVG_NS}" width="{self.width}" height="{self.height}" '
            f'viewBox="0 0 {self.width} {self.height}" '
            f'font-family="{FONT}" role="img">\n  {body}\n</svg>\n'
        )


class Layout:
    """Shared geometry of both charts."""

    WIDTH = 760
    PLOT_LEFT = 200
    TITLE_Y = 30
    SUBTITLE_Y = 50
    PLOT_TOP = 76
    ROW_HEIGHT = 52
    LABEL_GAP = 14
    SUB_LABEL_OFFSET = 16


class RowLabels:
    """Engine names left of the plot, with a muted line saying what each one is."""

    ROWS: ClassVar[tuple[tuple[str, str], ...]] = (
        ("real Spark", "local[1] JVM, tuned"),
        ("sparkwarp", "Sail, no JVM"),
    )

    @classmethod
    def draw(cls, svg: Svg, centers: tuple[float, float]) -> None:
        x = Layout.PLOT_LEFT - Layout.LABEL_GAP
        for (name, detail), center in zip(cls.ROWS, centers, strict=True):
            svg.text(x, center - 2, name, svg.theme.ink, 14, "end", 600)
            svg.text(
                x,
                center + Layout.SUB_LABEL_OFFSET - 2,
                detail,
                svg.theme.muted,
                12,
                "end",
            )


class SuiteChart:
    """Horizontal bars: whole-suite wall time per engine. The hero chart."""

    HEIGHT = 222
    PLOT_RIGHT_GAP = 170
    BAR = 24
    RADIUS = 4
    TICK_TARGET = 4

    def __init__(self, results: Results, theme: Theme) -> None:
        self.results, self.theme = results, theme
        self.plot_width = Layout.WIDTH - Layout.PLOT_LEFT - self.PLOT_RIGHT_GAP
        self.axis_max, self.step = NiceScale.for_max(
            results.spark.wall_median, self.TICK_TARGET
        )

    def render(self) -> str:
        r = self.results
        title = f"{r.test_count} PySpark tests, whole suite"
        desc = (
            f"Real Spark {r.spark.wall_median:.1f} s, "
            f"sparkwarp {r.sail.wall_median:.1f} s, {r.speedup:.1f}x faster."
        )
        svg = Svg(Layout.WIDTH, self.HEIGHT, self.theme, title, desc)
        svg.text(24, Layout.TITLE_Y, title, self.theme.ink, 17, weight=600)
        svg.text(
            24,
            Layout.SUBTITLE_Y,
            f"Median wall time of {r.runs} runs, same test code. Lower is better.",
            self.theme.secondary,
        )
        centers = (
            Layout.PLOT_TOP + Layout.ROW_HEIGHT / 2,
            Layout.PLOT_TOP + 1.5 * Layout.ROW_HEIGHT,
        )
        self._grid(svg)
        self._bar(
            svg,
            centers[0],
            r.spark.wall_median,
            self.theme.spark,
            f"{r.spark.wall_median:.1f} s",
        )
        self._bar(
            svg,
            centers[1],
            r.sail.wall_median,
            self.theme.sail,
            f"{r.sail.wall_median:.1f} s  ·  {r.speedup:.0f}x faster",
        )
        RowLabels.draw(svg, centers)
        return svg.render()

    def _x(self, seconds: float) -> float:
        return Layout.PLOT_LEFT + seconds / self.axis_max * self.plot_width

    def _grid(self, svg: Svg) -> None:
        bottom = Layout.PLOT_TOP + 2 * Layout.ROW_HEIGHT
        ticks = int(round(self.axis_max / self.step))
        for i in range(ticks + 1):
            x = self._x(i * self.step)
            color = self.theme.axis if i == 0 else self.theme.grid
            svg.line(x, Layout.PLOT_TOP, x, bottom, color)
            svg.text(
                x, bottom + 18, f"{i * self.step:g} s", self.theme.muted, 12, "middle"
            )

    def _bar(
        self, svg: Svg, center: float, seconds: float, color: str, label: str
    ) -> None:
        x0, x1 = self._x(0), self._x(seconds)
        y, h, r = center - self.BAR / 2, self.BAR, self.RADIUS
        svg.add(
            f'<path d="M{x0:.1f},{y:.1f} H{x1 - r:.1f} '
            f"Q{x1:.1f},{y:.1f} {x1:.1f},{y + r:.1f} V{y + h - r:.1f} "
            f"Q{x1:.1f},{y + h:.1f} {x1 - r:.1f},{y + h:.1f} "
            f'H{x0:.1f} Z" fill="{color}"/>'
        )
        svg.text(x1 + 10, center + 5, label, self.theme.ink, 14, weight=600)


class NiceScale:
    """Axis maximum and step from a 1-2-5 sequence, about `target` ticks."""

    MULTIPLIERS: ClassVar[tuple[float, ...]] = (1, 2, 5, 10)

    @classmethod
    def for_max(cls, value: float, target: int) -> tuple[float, float]:
        raw_step = value / target
        magnitude = 10 ** math.floor(math.log10(raw_step))
        step = next(m * magnitude for m in cls.MULTIPLIERS if m * magnitude >= raw_step)
        return math.ceil(value / step) * step, step


class PerTestChart:
    """Every test as a dot on a log time axis, one row per engine, median marked."""

    HEIGHT = 246
    PLOT_RIGHT = Layout.WIDTH - 40
    DOMAIN = (0.005, 20.0)
    TICKS: ClassVar[tuple[tuple[float, str], ...]] = (
        (0.01, "10 ms"),
        (0.1, "100 ms"),
        (1, "1 s"),
        (10, "10 s"),
    )
    DOT_RADIUS = 4.5
    RING = 1.5
    JITTER_STEPS = 9
    JITTER_PX = 2.4

    def __init__(self, results: Results, theme: Theme) -> None:
        self.results, self.theme = results, theme

    def render(self) -> str:
        r = self.results
        title = "Every test, one dot each"
        desc = (
            "Median time per test: real Spark "
            f"{r.spark.per_test_median * MS_PER_SECOND:.0f} ms, "
            f"sparkwarp {r.sail.per_test_median * MS_PER_SECOND:.0f} ms."
        )
        svg = Svg(Layout.WIDTH, self.HEIGHT, self.theme, title, desc)
        svg.text(24, Layout.TITLE_Y, title, self.theme.ink, 17, weight=600)
        svg.text(
            24,
            Layout.SUBTITLE_Y,
            f"Median of {r.runs} runs per test, log scale. "
            "Each row's slowest dot is the first test (session start).",
            self.theme.secondary,
        )
        centers = (
            Layout.PLOT_TOP + 8 + Layout.ROW_HEIGHT / 2,
            Layout.PLOT_TOP + 8 + 1.5 * Layout.ROW_HEIGHT,
        )
        self._grid(svg)
        self._row(svg, centers[0], r.spark, self.theme.spark)
        self._row(svg, centers[1], r.sail, self.theme.sail)
        RowLabels.draw(svg, centers)
        return svg.render()

    def _x(self, seconds: float) -> float:
        low, high = (math.log10(v) for v in self.DOMAIN)
        share = (math.log10(seconds) - low) / (high - low)
        return Layout.PLOT_LEFT + share * (self.PLOT_RIGHT - Layout.PLOT_LEFT)

    def _grid(self, svg: Svg) -> None:
        top, bottom = Layout.PLOT_TOP, Layout.PLOT_TOP + 16 + 2 * Layout.ROW_HEIGHT
        for seconds, label in self.TICKS:
            x = self._x(seconds)
            svg.line(x, top, x, bottom, self.theme.grid)
            svg.text(x, bottom + 18, label, self.theme.muted, 12, "middle")

    def _row(self, svg: Svg, center: float, numbers: EngineNumbers, color: str) -> None:
        for index, seconds in enumerate(sorted(numbers.test_medians)):
            offset = (
                index % self.JITTER_STEPS - self.JITTER_STEPS // 2
            ) * self.JITTER_PX
            svg.add(
                f'<circle cx="{self._x(seconds):.1f}" cy="{center + offset:.1f}" '
                f'r="{self.DOT_RADIUS}" fill="{color}" '
                f'stroke="{self.theme.surface}" stroke-width="{self.RING}"/>'
            )
        median = numbers.per_test_median
        x = self._x(median)
        svg.line(x, center - 16, x, center + 16, self.theme.ink, 2)
        svg.text(
            x,
            center - 21,
            f"median {median * MS_PER_SECOND:.0f} ms",
            self.theme.ink,
            12,
            "middle",
            600,
        )


class ChartCli:
    """Writes both charts in both themes into docs/assets."""

    CHARTS: ClassVar[tuple[tuple[str, type[SuiteChart] | type[PerTestChart]], ...]] = (
        ("benchmark-suite", SuiteChart),
        ("benchmark-per-test", PerTestChart),
    )

    def run(self) -> int:
        results = Results.load(RESULTS_PATH)
        ASSET_DIR.mkdir(parents=True, exist_ok=True)
        for stem, chart in self.CHARTS:
            for theme in (LIGHT, DARK):
                path = ASSET_DIR / f"{stem}-{theme.name}.svg"
                path.write_text(chart(results, theme).render())
                print(f"wrote {path.relative_to(BENCH_DIR.parent)}")
        return 0


if __name__ == "__main__":
    sys.exit(ChartCli().run())
