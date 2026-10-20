# sparkwarp

Run existing PySpark DataFrame unit tests in milliseconds, with no JVM.

Status: pre-release (0.1.0.dev0). Not on PyPI yet.

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
Known differences from Spark are listed in `benchmarks/KNOWN_GAPS.md`; each one is a test.

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
  `benchmarks/KNOWN_GAPS.md`. They run on both engines and fail loudly the day Sail fixes them.

Run it yourself (Java 17+ is needed only for the Spark side):

```bash
uv sync --extra test --group dev
uv run python -m benchmarks.run --runs 3 --java-home /path/to/jdk17   # both engines
SPARKWARP=1 uv run pytest benchmarks -q                                 # Sail
SPARKWARP=0 uv run pytest benchmarks -q                                 # real Spark
```

The runner starts one pytest process per engine, unsets `SPARK_HOME` so PySpark uses its own
jars, and writes `benchmarks/RESULTS.md`.

## Spike 1 verdict (2026-10-20): Spark Connect client + Sail server

Question: can `pyspark.sql.connect` be the API layer, so the package only supplies an engine?

Answer: yes. The engine already exists. [Sail](https://github.com/lakehq/sail) (`pysail`) is a
Rust Spark Connect server on Arrow + DataFusion. PySpark 4.1.1 client + pysail 0.7.2 ran the
brief's filter test unchanged, including `createDataFrame`, `F.*`, `Window`, `spark.sql`,
temp views, a Python UDF and `assertDataFrameEqual`.

What the client does without any server (verified, `spikes/01_...py`):

- `F.col("a") + F.lit(1)` builds a pure-Python protobuf `Expression`. No JVM, no socket.
- `SparkSession.builder.remote(...).getOrCreate()` DOES contact the server on PySpark 4.1.1
  (`_cleanup_ml_cache -> get_configs`). A session needs a live gRPC server, so a JVM-free
  engine must speak Spark Connect gRPC. Sail does.

Measured on this laptop (`spikes/02_...py`, `spikes/bench/`), one process, Sail in-process:

| Step | Cost |
|---|---|
| `import pyspark.sql.connect` + pysail | 0.6 to 1.6 s, once |
| Sail server start (background thread) | 6 ms |
| Session create | 4 to 6 ms |
| `createDataFrame` (4 rows, explicit schema) | 18 to 28 ms first, then ~2 ms |
| One `count()` on a warm session | 7 ms median (Spark: 300 to 400 ms) |
| Brief's filter test with `assertDataFrameEqual` | 23 ms median |
| 300 parametrized filter tests under pytest | 10.5 s wall, incl. 1.6 s imports (~30 ms/test) |

Semantic traps from the brief, Sail 0.7.2 vs Spark 4 ANSI defaults:

| Trap | Sail | Matches Spark 4? |
|---|---|---|
| `when(null > 1, "a").otherwise("b")` -> "b"; `v != 5` drops null | yes | yes |
| `cast("abc" as int)` raises; `try_cast` -> null; `ansi.enabled=false` -> null | yes | yes |
| `lit(1)/lit(2)` -> double; `createDataFrame` int -> bigint; nullable flags kept | yes | yes |
| `size(null)` -> null; Java regex `\d`; `trunc` -> date; running-sum default frame | yes | yes (Spark 3.5 non-ANSI gives -1 for size) |
| `coalesce(int_col, "x")` schema | `int` | NO: Spark 4 ANSI gives `bigint` (both raise on "x" at runtime) |
| Unsupported: `df.rdd`, unknown SQL function | raises loud | yes |

Not measured here: real Spark on the same machine (Spark 4 needs Java 17; only Java 11 here).
Spark figures come from the brief.

Open gaps to carry into the paired corpus: `coalesce` least-common-type, Spark 3.5 vs 4
behaviour switches (`sizeOfNull`, ANSI), Delta `MERGE` (Sail claims support; untested),
`assertDataFrameEqual` needs `pandas<3` on PySpark 4.1.1.

## Reproduce

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python "pyspark[connect]==4.1.1" "pandas<3" pysail pytest
.venv/bin/python spikes/01_connect_client_without_server.py   # steps 1-5 only; STEP6=1 shows the hang
.venv/bin/python spikes/02_sail_timing_and_semantics.py
cd spikes/bench && ../../.venv/bin/python -m pytest -q .
```
