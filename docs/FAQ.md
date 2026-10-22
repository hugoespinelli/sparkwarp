# FAQ

## Why are Spark unit tests slow, even on tiny data?

Every action pays a fixed cost before any data moves. PySpark talks to a JVM through Py4J,
one round trip per call. Catalyst then plans the query, and the scheduler builds stages and
tasks as it would for a cluster. On a laptop that is 300 to 400 ms per action, whether the
input has 4 rows or 4 billion. A test with two actions costs about a second. Config tuning
(`local[1]`, UI off, codegen off) shrinks execution, not this fixed path.

## What does sparkwarp change?

It swaps the engine, not the API. PySpark 4 ships a Spark Connect client: `F.col`,
`DataFrame` and `Window` build a query plan in Python and send it to a server in one call.
sparkwarp starts [Sail](https://github.com/lakehq/sail), a Spark Connect server written in
Rust, inside the pytest process, and points PySpark at it. Your tests keep calling the same
PySpark API. The JVM, Py4J and the scheduler drop out, and an action takes 5 to 10 ms.

## Do I need to change my tests?

Usually not. Set `SPARKWARP=1`. Your conftest's `SparkSession.builder...getOrCreate()`
lands on Sail. sparkwarp also accepts the common conftest lines: `setLogLevel`,
`catalog.clearCache()`, `spark.stop()` in teardown and `localCheckpoint()`. A few Spark
Connect client differences can still need attention, each with a workaround in the adoption
table of `KNOWN_GAPS.md`: `spark.sparkContext` and `df.rdd` beyond their stubs, `df.toJSON()`
returning rows, `isinstance` checks on the session or on `groupBy()` results, columns nested
too deep for protobuf, and tests that start subprocesses. The README's "Before your first
run" section is the checklist.

## Can I trust a green run?

Only together with a parity lane. Sail matches real Spark on 307 of the 344 corpus cases.
Most of the rest raise loudly or differ only in schema details, but five gaps change
values silently (GAP-004, GAP-009, GAP-010, GAP-012, GAP-015), and row order without
`orderBy` differs (ORDER-001). Run the same suite with `SPARKWARP=0` on CI, nightly or on merge, and the
parity lane catches the rest. sparkwarp is a fast inner loop, not a replacement for real
Spark in CI.

## Why Sail, and not sqlframe, Sparkless or pysparkling?

| Option | How it works | Why not the base for sparkwarp |
|---|---|---|
| pysparkling | Pure-Python reimplementation of the RDD/DataFrame API | Abandoned around Spark 2; reimplementing the API is a full-time job |
| sqlframe | Compiles the DataFrame API to SQL (DuckDB and others) | Replaces the `pyspark` import; DuckDB's SQL dialect and type rules differ from Spark's |
| Sparkless | Reimplements the API on Polars | Replaces the import; Polars null and type rules differ from Spark's |
| Spark Connect + real Spark | Real engine on a long-lived server | Keeps the JVM and the per-action cost |
| Sail (sparkwarp) | Implements Spark's own Connect protocol and Spark SQL semantics in Rust | Keeps the real PySpark client; no API to reimplement |

## Does it support Spark 3.5?

No. sparkwarp targets PySpark 4 and Spark 4 semantics with ANSI mode on, the Spark 4
default. Spark 3.5 legacy behaviour (casts returning null, `size(null) = -1`) is out of scope.

## What about Delta Lake, RDDs, streaming and UDFs?

Plain Python UDFs work. `df.rdd` (beyond `isEmpty()`), streaming and the `DeltaTable`
Python API do not. Mark those tests `@pytest.mark.spark_only`: sparkwarp skips them on
Sail, and your parity lane runs them on real Spark.

## Does sparkwarp touch production code?

No. It is a test dependency. It changes nothing unless `SPARKWARP=1` or `--sparkwarp` is set.

## How do I check the claims myself?

Clone the repo and run the benchmark and the corpus on both engines. The README has the
commands. Java 17+ is needed only for the real-Spark side.
