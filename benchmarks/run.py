"""Runs the benchmark suite on each engine in its own process and writes RESULTS.md."""

from __future__ import annotations

import argparse
import math
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from importlib.metadata import version
from pathlib import Path

from sparkwarp.config import ACTIVATION_ENV_VAR, CONNECT_MODE_ENV_VAR, REMOTE_ENV_VAR


class EngineName(str, Enum):
    """The two sides of the comparison. Sail via sparkwarp, Spark via the conftest."""

    SAIL = "sail"
    SPARK = "spark"


SPARKWARP_SWITCH = {EngineName.SAIL: "1", EngineName.SPARK: "0"}

BENCH_DIR = Path(__file__).parent
PROJECT_DIR = BENCH_DIR.parent
DEFAULT_OUTPUT = BENCH_DIR / "RESULTS.md"
SPARK_HOME_ENV_VAR = "SPARK_HOME"
JAVA_HOME_ENV_VAR = "JAVA_HOME"
PATH_ENV_VAR = "PATH"
ENV_VARS_TO_CLEAR = (
    SPARK_HOME_ENV_VAR,
    REMOTE_ENV_VAR,
    CONNECT_MODE_ENV_VAR,
    ACTIVATION_ENV_VAR,
)
RUN_TIMEOUT_SECONDS = 600
P90 = 0.9
SLOWEST_TO_SHOW = 3
TAIL = 2000
MS = 1000
HEADER = (
    "| Engine | Suite wall time | Median per test | p90 per test "
    "| Slowest test | Status |"
)


@dataclass
class RunTiming:
    wall_seconds: float
    test_seconds: dict[str, float]
    passed: int
    failed: int
    known_gaps: int


@dataclass
class EngineResult:
    engine: EngineName
    runs: list[RunTiming] = field(default_factory=list)

    @property
    def wall_median(self) -> float:
        return statistics.median(r.wall_seconds for r in self.runs)

    @property
    def test_medians(self) -> dict[str, float]:
        names = self.runs[0].test_seconds
        return {
            n: statistics.median(r.test_seconds[n] for r in self.runs) for n in names
        }

    @property
    def per_test_median(self) -> float:
        return statistics.median(self.test_medians.values())

    @property
    def per_test_p90(self) -> float:
        times = sorted(self.test_medians.values())
        return times[math.ceil(len(times) * P90) - 1]

    @property
    def slowest(self) -> list[tuple[str, float]]:
        return sorted(self.test_medians.items(), key=lambda kv: kv[1], reverse=True)[
            :SLOWEST_TO_SHOW
        ]

    @property
    def all_green(self) -> bool:
        return all(r.failed == 0 for r in self.runs)


class JUnitReport:
    """Reads per-test durations and the pass/fail count from a pytest junit file."""

    def __init__(self, path: Path) -> None:
        self._root = ET.parse(path).getroot()

    def timing(self, wall_seconds: float) -> RunTiming:
        cases = list(self._root.iter("testcase"))
        failed = sum(
            1
            for c in cases
            if c.find("failure") is not None or c.find("error") is not None
        )
        gaps = sum(1 for c in cases if self._is_expected_failure(c))
        seconds = {
            f"{c.get('classname')}::{c.get('name')}": float(c.get("time", "0"))
            for c in cases
        }
        return RunTiming(
            wall_seconds, seconds, len(cases) - failed - gaps, failed, gaps
        )

    @staticmethod
    def _is_expected_failure(case: ET.Element) -> bool:
        skipped = case.find("skipped")
        return skipped is not None and skipped.get("type") == "pytest.xfail"


class PytestInvocation:
    """One pytest process pinned to one engine."""

    def __init__(self, engine: EngineName, java_home: str | None) -> None:
        self._engine = engine
        self._java_home = java_home

    def environment(self) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if k not in ENV_VARS_TO_CLEAR}
        env[ACTIVATION_ENV_VAR] = SPARKWARP_SWITCH[self._engine]
        if self._java_home:
            java_bin = f"{self._java_home}/bin"
            env[JAVA_HOME_ENV_VAR] = self._java_home
            env[PATH_ENV_VAR] = f"{java_bin}:{env.get(PATH_ENV_VAR, '')}"
        return env

    def run(self) -> RunTiming:
        with tempfile.TemporaryDirectory() as tmp:
            junit = Path(tmp) / "junit.xml"
            argv = [
                sys.executable,
                "-m",
                "pytest",
                str(BENCH_DIR),
                "-q",
                "-p",
                "no:cacheprovider",
                f"--junitxml={junit}",
            ]
            started = time.perf_counter()
            proc = subprocess.run(
                argv,
                cwd=PROJECT_DIR,
                env=self.environment(),
                capture_output=True,
                timeout=RUN_TIMEOUT_SECONDS,
                text=True,
            )
            wall = time.perf_counter() - started
            if not junit.exists():
                tail = f"{proc.stdout[-TAIL:]}\n{proc.stderr[-TAIL:]}"
                raise RuntimeError(f"no report for {self._engine.value}:\n{tail}")
            return JUnitReport(junit).timing(wall)


class MachineInfo:
    @staticmethod
    def render() -> str:
        cpu = platform.processor() or platform.machine()
        return (
            f"{platform.system()} {platform.machine()}, {os.cpu_count()} CPUs ({cpu}), "
            f"Python {platform.python_version()}, pyspark {version('pyspark')}, "
            f"pysail {version('pysail')}"
        )


class ResultsTable:
    """Renders the comparison as the markdown block the README embeds."""

    def __init__(self, results: list[EngineResult], runs: int) -> None:
        self._results = results
        self._runs = runs

    def render(self) -> str:
        n_tests = len(self._results[0].runs[0].test_seconds)
        lines = [
            f"Benchmark: {n_tests} tests, median of {self._runs} runs, "
            "one pytest process per engine.",
            f"Machine: {MachineInfo.render()}.",
            "",
            HEADER,
            "|---|---|---|---|---|---|",
        ]
        for r in self._results:
            name, secs = r.slowest[0]
            status = self._status(r)
            test_name = name.split("::")[-1]
            lines.append(
                f"| {r.engine.value} | {r.wall_median:.1f} s "
                f"| {r.per_test_median * MS:.0f} ms | {r.per_test_p90 * MS:.0f} ms "
                f"| {secs * MS:.0f} ms ({test_name}) | {status} |"
            )
        lines.append("")
        lines.append(self._speedup_line())
        return "\n".join(lines)

    @staticmethod
    def _status(result: EngineResult) -> str:
        if not result.all_green:
            return "FAILURES"
        gaps = result.runs[0].known_gaps
        return "all green" if gaps == 0 else f"green, {gaps} known gaps (xfail)"

    def _speedup_line(self) -> str:
        by_name = {r.engine: r for r in self._results}
        if EngineName.SPARK not in by_name or EngineName.SAIL not in by_name:
            return ""
        spark, sail = by_name[EngineName.SPARK], by_name[EngineName.SAIL]
        return (
            f"Speedup: suite {spark.wall_median / sail.wall_median:.1f}x, "
            f"median test {spark.per_test_median / sail.per_test_median:.0f}x."
        )


class SpeedGate:
    """Fails the run when an engine's median suite time exceeds its budget."""

    def __init__(self, max_sail_seconds: float | None) -> None:
        self._max_sail_seconds = max_sail_seconds

    def violations(self, results: list[EngineResult]) -> list[str]:
        problems = [
            f"{r.engine.value}: test failures" for r in results if not r.all_green
        ]
        if self._max_sail_seconds is None:
            return problems
        sail = next((r for r in results if r.engine is EngineName.SAIL), None)
        if sail is None:
            return [*problems, "a Sail budget was set but sail was not measured"]
        too_slow = sail.wall_median > self._max_sail_seconds
        if too_slow:
            problems.append(
                f"sail: {sail.wall_median:.1f} s, "
                f"over the {self._max_sail_seconds} s budget"
            )
        return problems


class BenchmarkCli:
    """Runs each engine, writes the results table, and applies the speed gate."""

    DEFAULT_RUNS = 3
    DEFAULT_ENGINES = "sail,spark"
    ENGINE_SEPARATOR = ","
    EXIT_GATE_FAILED = 1

    def run(self, argv: list[str]) -> int:
        args = self._parser().parse_args(argv)
        results = [
            self._measure(name, args)
            for name in args.engines.split(self.ENGINE_SEPARATOR)
        ]
        table = ResultsTable(results, args.runs).render()
        args.output.write_text(table + "\n")
        print(table)
        problems = SpeedGate(args.max_sail_seconds).violations(results)
        for problem in problems:
            print(f"GATE FAILED: {problem}")
        return self.EXIT_GATE_FAILED if problems else 0

    def _parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--runs", type=int, default=self.DEFAULT_RUNS)
        parser.add_argument("--engines", default=self.DEFAULT_ENGINES)
        parser.add_argument("--java-home", default=os.environ.get(JAVA_HOME_ENV_VAR))
        parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
        parser.add_argument("--max-sail-seconds", type=float, default=None)
        return parser

    def _measure(self, name: str, args: argparse.Namespace) -> EngineResult:
        engine = EngineName(name.strip())
        result = EngineResult(engine)
        java_home = args.java_home if engine is EngineName.SPARK else None
        invocation = PytestInvocation(engine, java_home)
        for i in range(args.runs):
            timing = invocation.run()
            result.runs.append(timing)
            print(
                f"[{engine.value}] run {i + 1}/{args.runs}: "
                f"{timing.wall_seconds:.1f}s, {timing.passed} passed, "
                f"{timing.failed} failed, {timing.known_gaps} known gaps",
                flush=True,
            )
        return result


if __name__ == "__main__":
    sys.exit(BenchmarkCli().run(sys.argv[1:]))
