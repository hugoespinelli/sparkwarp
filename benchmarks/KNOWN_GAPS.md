# Known gaps between Sail and Spark 4 (ANSI), found by the benchmark suite

Each gap is a strict `xfail` on Sail in the benchmark, so it runs on both engines and starts
failing loudly the day the gap closes. Found with PySpark 4.1.1 and pysail 0.7.2 on 2026-10-20;
GAP-001 and GAP-002 still reproduce on PySpark 4.2.0.

## GAP-001: `unionByName(allowMissingColumns=True)` types a missing column as NullType

Test: `benchmarks/test_joins_and_unions.py::test_union_by_name_fills_missing_columns`

```python
base = spark.createDataFrame([(1, "a")], "id int, name string")
extra = spark.createDataFrame([("b", 2, "x")], "name string, id int, note string")
base.unionByName(extra, allowMissingColumns=True).schema["note"].dataType
```

| Engine | Result |
|---|---|
| Spark 4.1.1 | `StringType()` (the missing column takes the type of the side that has it) |
| Sail 0.7.2 | `NullType()` |

Effect: a later `assertDataFrameEqual` against an explicit schema fails, and any string
function on the column resolves against NullType.

## GAP-002: `datediff` returns LongType instead of IntegerType

Test: `benchmarks/test_time_nulls_sql.py::test_event_day_is_truncated_to_midnight`

```python
spark.range(1).select(F.datediff(F.to_date(F.lit("2024-05-20")), F.to_date(F.lit("2024-05-17")))).schema
```

| Engine | Result |
|---|---|
| Spark 4.1.1 | `IntegerType()` |
| Sail 0.7.2 | `LongType()` |

Effect: values agree (3), schema comparison fails.

## Not a gap: left-semi join puts the join key first

Both engines return `["country", "order_id", "customer_id", "amount"]` for
`orders.join(lookup, on=["country"], how="left_semi")`. Spark's USING-join projection is
`leftKeys ++ leftUniqueColumns` for semi and anti joins. The first version of the benchmark
asserted the input order and was wrong on both engines.

## ADOPT-001: `isinstance(spark, pyspark.sql.SparkSession)` is False under Connect

Test: `tests/test_session_smoke.py::test_session_is_a_classic_sparksession_instance`

This is a PySpark gap, not a Sail gap. Under Spark Connect, `DataFrame`, `Column` and
`Window` objects pass `isinstance` checks against the `pyspark.sql` classes, but the session
is `pyspark.sql.connect.session.SparkSession`, which does not subclass
`pyspark.sql.SparkSession` (PySpark 4.1.1). Type hints are unaffected at runtime. Code that
branches on `isinstance(spark, SparkSession)` needs to check
`pyspark.sql.utils.is_remote()` or accept both classes.

## ADOPT-002: subprocesses inherit Connect routing but not the builder patch

Test: `tests/test_runtime.py::test_children_inherit_routing_unless_given_the_clean_environ`

sparkwarp routes PySpark through two env vars, and child processes inherit them. The
builder patch that drops `.master("local[1]")` lives only in the parent, so a child that
builds `SparkSession.builder.master(...)` fails with `CANNOT_CONFIGURE_SPARK_CONNECT_MASTER`.
Workaround: `subprocess.run(..., env=sparkwarp.clean_environ())`; the child then runs on
real Spark.

## ADOPT-003: `spark.sparkContext` is not available under Spark Connect

PySpark's Connect session has no `sparkContext`, so a conftest line such as
`spark.sparkContext.setLogLevel("ERROR")` raises `PySparkAttributeError
[JVM_ATTRIBUTE_NOT_SUPPORTED]` under sparkwarp. Remove the line or guard it with
`pyspark.sql.utils.is_remote()`. Backlog A2.6 plans a narrow `setLogLevel` shim for 0.2.
