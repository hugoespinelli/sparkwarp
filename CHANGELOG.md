# Changelog

## 0.3.0 (unreleased)

- `df.rdd.isEmpty()` works under sparkwarp. Other `rdd` members still fail loudly
  (ADOPT-005). Code that detects Spark Connect with `try: df.rdd` now takes the classic
  branch; use `pyspark.sql.utils.is_remote()` instead.
- Results that Sail returns as Arrow `string_view` or `binary_view`, such as `md5()` before
  `distinct()` or a union, now collect: sparkwarp casts them to plain strings and binaries
  before PySpark reads them (GAP-018 closed). Needs pyarrow 18 or newer.
- New registered gap: `md5()` passed to `unhex`, `split` or `regexp_extract` fails inside
  Sail (GAP-019); cast it to string first.

## 0.2.0 (2026-10-21)

Fixes from the first real-world run: an 8,181-test lakehouse suite on PySpark 4.0.0.

- Common conftest lines now work under sparkwarp: `spark.sparkContext.setLogLevel(...)`
  (plus `applicationId`, `defaultParallelism`, `version`), `spark.catalog.clearCache()`,
  and `spark.stop()` in teardown. Other `sparkContext` members still fail loudly.
- `spark.stop()` from user code no longer closes the shared (default) session while
  sparkwarp owns it, so libraries that cache a Spark Connect session at import (such as
  cuallee) keep working; sparkwarp closes it when the run ends. Any other session is stopped
  for real. A held stop keeps conf values and temp views (ADOPT-008).
- `localCheckpoint()` and `checkpoint()` work: each run gets a temporary checkpoint
  directory. A `SAIL_EXECUTION__CHECKPOINT__PATH` you set yourself wins.
- Failures caused by columns nested too deep for the Spark Connect client get the
  sparkwarp hint.
- PySpark 4.0 and 4.1: sparkwarp holds the shared release-pool shutdown while it owns the
  engine and runs it once at engine stop, so a garbage-collected session no longer
  deadlocks or breaks the live one. PySpark 4.2 fixed the same bug with an idle-only
  shutdown.
- Tested on PySpark 4.0, 4.1 and 4.2 in CI.
- Corpus grows to 342 cases. New registered gaps: array index from a column or expression
  (GAP-013), windows ordered by a `CASE WHEN` on a function result (GAP-014), `is_cached`
  after `unpersist()` (GAP-015), `input_file_name()` (GAP-016), `unionByName` of string and
  boolean (GAP-017), `md5()` before `distinct()` or a union (GAP-018), and the protobuf
  nesting limit (ADOPT-007).
- README: a "Before your first run" checklist.

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
