# Changelog

## 0.1.0 (2026-10-21)

First release. Run an existing PySpark 4 test suite on Sail, with no JVM.

- `SPARKWARP=1` or `pytest --sparkwarp` routes the suite's own `SparkSession.builder`
  sessions to an in-process Sail server. No test edits for the common conftest and
  `unittest` shapes. `SPARKWARP=0` leaves the suite on real Spark: the parity lane.
- A `spark` fixture for suites without one. `sparkwarp.session()` and `sparkwarp.stop()`
  outside pytest.
- Session contract: Spark 4 semantics, ANSI on, timezone UTC unless the suite sets one.
- `@pytest.mark.spark_only` skips a test, class or module on Sail.
- Failure reports get a hint when a test hits a feature that Spark Connect or Sail does
  not support.
- `sparkwarp.clean_environ()` for tests that start subprocesses.
- Works with pytest-xdist, pytest-env, nested and in-process pytest runs.
- Corpus of 329 cases recorded on real Spark; 301 match on Sail. Every difference is
  listed in `KNOWN_GAPS.md` and pinned by a test.
- Benchmark: a 42-test lakehouse-shaped suite runs about 12x faster than on local Spark.
