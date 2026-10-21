# sparkwarp

Run existing PySpark DataFrame unit tests in milliseconds, with no JVM.

Status: pre-release. Not on PyPI yet.

## Quickstart

```bash
pip install sparkwarp            # from a local wheel until the first release
```

Requires PySpark 4 (it installs `pyspark[connect]>=4`) and Python 3.10+. sparkwarp is off
until you switch it on.

**Shape 1: your conftest already builds a session.** Run it unchanged, except for
`spark.sparkContext` calls (see below):

```bash
SPARKWARP=1 pytest
```

Your existing `SparkSession.builder.master("local[1]")...getOrCreate()` now returns a
session on Sail. sparkwarp drops the `master` setting for you, because PySpark refuses a
master URL in Connect mode. `unittest` classes that build the session in `setUpClass` work
the same way.

**Shape 2: no session fixture yet.** Use the one the plugin provides:

```python
def test_filter(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, name string")
    assert df.filter("id > 1").count() == 1
```

```bash
pytest --sparkwarp
```

**Parity lane: the same suite on real Spark.** Leave sparkwarp off and your own fixture
runs on the JVM as before. sparkwarp never starts a JVM itself.

```bash
SPARKWARP=0 pytest        # or simply: pytest
```

**Outside pytest:**

```python
import sparkwarp

spark = sparkwarp.session()                        # Sail, UTC, ANSI on
sparkwarp.stop()                                   # optional; runs at exit anyway
spark = sparkwarp.session(timezone="Asia/Tokyo")   # a new session in another timezone
```

Inside a sparkwarp pytest run the plugin owns the session. `sparkwarp.stop()` there is
ignored with a warning, and the plugin stops everything when the run ends.

**The `spark` fixture is function-scoped**, so each test gets the live session even if
older code stopped it. A session-scoped fixture of your own should build its session with
`SparkSession.builder...getOrCreate()` instead of requesting `spark`. That lands on Sail
when sparkwarp is on and on your JVM when it is off.

**`spark.sparkContext` does not exist on Spark Connect.** A conftest line such as
`spark.sparkContext.setLogLevel("ERROR")` fails under sparkwarp. Remove it, or guard it
with `pyspark.sql.utils.is_remote()`. A shim for `setLogLevel` is planned for 0.2.

**Subprocesses.** A child process inherits `SPARK_REMOTE` but not sparkwarp's builder
patch, so a child that builds its own session fails in Connect mode. Pass
`env=sparkwarp.clean_environ()` to `subprocess.run`: the child gets the environment as it
was before sparkwarp, and runs real Spark.

| Switch | Effect |
|---|---|
| `SPARKWARP=1` | Run on Sail |
| `--sparkwarp` | Run on Sail; wins over `SPARKWARP=0` |
| `SPARKWARP=0` or unset | Off: your own fixture, real Spark |

Contract on Sail: Spark 4 semantics with ANSI mode on and session timezone UTC. A
timezone you set on your own builder or with `spark.conf.set` wins.
Known differences from Spark are listed in `KNOWN_GAPS.md`; each one is a test.

## How faithful is it?

A corpus of 329 cases checks Sail against real Spark: ten semantic traps (null
logic, coercion, ANSI casts, arbitrary rows from deduplication, window frames, Java regex,
timestamps, schema, joins, SQL), the `F.*` functions lakehouse code uses, the DataFrame
methods and actions, and row order. Real
Spark recorded every expected result; CI re-checks them on both engines.

| Area | Cases | Match Spark on Sail |
|---|---|---|
| Functions | 170 | 165 |
| DataFrame methods and actions | 59 | 54 |
| Semantic traps | 88 | 74 |
| Row order | 12 | 8 |

Matches are compared the way `assertDataFrameEqual` compares by default. Every difference
is a test and is listed with its severity in [`KNOWN_GAPS.md`](KNOWN_GAPS.md); the full
per-case table is [`docs/SUPPORT.md`](docs/SUPPORT.md). Five gaps change results without
an error (four value gaps plus row order), so keep a `SPARKWARP=0` lane on CI. The [FAQ](docs/FAQ.md) explains why.

## Tests that need real Spark

```python
@pytest.mark.spark_only
def test_merge_into_delta(spark): ...
```

`spark_only` works on a test, a class, or a module (`pytestmark`). sparkwarp skips such
tests on Sail and gives the reason (shown with `pytest -rs`); they run when sparkwarp is off. When a test fails on Sail
because a feature is unsupported (`df.rdd`, `spark.sparkContext`, an unknown function),
the failure report gets a short sparkwarp section that points to `KNOWN_GAPS.md` and
`spark_only`.

## Benchmark: the same tests on real Spark and on Sail

`benchmarks/` is a 42-test suite shaped like a real lakehouse repo: transformer classes with a
`process(df) -> DataFrame` method, tested with `createDataFrame`, `F.*`, `Window`,
`groupBy().agg()`, joins, `unionByName`, `from_json` + `explode`, `regexp_replace`,
`date_trunc`, `spark.sql()` on temp views, and `assertDataFrameEqual`. The test bodies are
identical for both engines, and so is the session fixture in `benchmarks/conftest.py`:
it is an ordinary `SparkSession.builder.master("local[1]")...getOrCreate()`. Only the
`SPARKWARP` switch differs between the two rows.

Benchmark: 42 tests, median of 3 runs, one pytest process per engine.
Machine: Linux x86_64, 16 CPUs (x86_64), Python 3.11.13, pyspark 4.2.0, pysail 0.7.2.

| Engine | Suite wall time | Median per test | p90 per test | Slowest test | Status |
|---|---|---|---|---|---|
| sail | 2.5 s | 27 ms | 38 ms | 101 ms (test_totals_per_country) | green, 2 known gaps (xfail) |
| spark | 29.1 s | 375 ms | 676 ms | 10143 ms (test_totals_per_country) | all green |

Speedup: suite 11.8x, median test 14x.

Notes on reading it:

- Spark numbers already include every tuning the guides recommend (`local[1]`, UI off,
  adaptive off, `shuffle.partitions=1`, whole-stage codegen off, Arrow off). The floor is the
  engine, not the config.
- The slowest test on each engine is the first one to run; it carries session start
  (about 7 s of JVM start on Spark; on Sail, imports plus a 6 ms server start).
- "Known gaps" are tests that pass on Spark and are a strict `xfail` on Sail, listed in
  `KNOWN_GAPS.md`. They run on both engines and fail loudly the day Sail fixes them.

Run it yourself (Java 17+ is needed only for the Spark side):

```bash
uv sync --extra test --group dev
uv run python -m benchmarks.run --runs 3 --java-home /path/to/jdk17   # both engines
SPARKWARP=1 uv run pytest benchmarks -q                                 # Sail
SPARKWARP=0 uv run pytest benchmarks -q                                 # real Spark
```

The runner starts one pytest process per engine, unsets `SPARK_HOME` so PySpark uses its own
jars, and writes `benchmarks/RESULTS.md`.

## Developing sparkwarp

```bash
uv sync --extra test --group dev
uv run pytest -q                                   # package tests (Sail)
SPARKWARP=1 uv run pytest corpus -q                # corpus on Sail
SPARKWARP=0 uv run pytest corpus -q                # corpus on real Spark (Java 17+)
SPARKWARP=0 SPARKWARP_RECORD=1 uv run pytest corpus -q   # re-record goldens from real Spark
uv run python -m corpus.support_page               # regenerate docs/SUPPORT.md
```

A new corpus case is one `Case(...)` line in `corpus/test_*.py`. Record its golden on real
Spark, then run it on Sail. If Sail differs, add the gap to `KNOWN_GAPS.md` and pass
`gap=KnownGap("GAP-0NN")`. Draft upstream reports for Sail live in
[`docs/upstream-issues.md`](docs/upstream-issues.md).

## License

Apache-2.0.
