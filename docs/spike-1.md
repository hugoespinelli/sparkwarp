# Spike 1 (2026-10-20): Spark Connect client + Sail server

Kept for the record. These numbers predate the package; the README has current ones.


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

The spike scripts live in the repository's `spikes/` folder; they are not in the sdist.

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python "pyspark[connect]==4.1.1" "pandas<3" pysail pytest
.venv/bin/python spikes/01_connect_client_without_server.py   # steps 1-5 only; STEP6=1 shows the hang
.venv/bin/python spikes/02_sail_timing_and_semantics.py
cd spikes/bench && ../../.venv/bin/python -m pytest -q .
```
