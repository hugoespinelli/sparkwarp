# Known gaps

Everything sparkwarp knows to differ from real Spark 4 (ANSI mode), each pinned by a test.

- Engine gaps are corpus cases that run on both engines. Real Spark recorded the expected
  results; on Sail each case is a strict `xfail`, so it fails the day the gap closes.
  ORDER-001 is the exception: Sail's order varies between runs, so it is a non-strict `xfail`.
- Adoption gaps ADOPT-001, ADOPT-002 and ADOPT-006 are pinned in the package's own tests, which run on
  Sail only. ADOPT-003 to ADOPT-005 are corpus cases like the engine gaps: they run on both
  engines against Spark goldens and are strict `xfail`s on Sail.

Versions: PySpark 4.2.0, pysail 0.7.2, checked 2026-10-21.

**Severity.** *Silent*: Sail returns a result where Spark returns a different one or raises,
so a test can pass on Sail and fail on Spark. *Loud*: Sail raises where Spark does not, or
both raise with different classes, so the difference shows up as an error on Sail.
*Schema*: values agree, types or flags differ.

## Engine gaps (Sail)

| ID | Severity | What differs | Pinned by |
|---|---|---|---|
| GAP-004 | silent | Mixing string and int in `when` branches or `array(...)` widens to string on Sail. Spark ANSI casts to int and raises on non-numeric text. | `corpus/test_traps.py`: `coerce_when_branches_string_int`, `coerce_array_int_and_string` |
| GAP-009 | silent | `isnan(null)` is `null` on Sail, `false` on Spark. | `corpus/test_functions.py`: `fn_isnan` |
| GAP-010 | silent | `percentile_approx` interpolates and returns `double` on Sail. Spark returns an actual value of the column's type. | `corpus/test_functions.py`: `fn_percentile_approx` |
| GAP-012 | silent | `describe()` and `summary()` return the same statistics in a different row order (`stddev, count, max, ...` instead of Spark's `count, mean, stddev, min, max`). Code that reads rows by position gets a different statistic. Pinned as a non-strict `xfail`. | `corpus/test_methods.py`: `df_describe`, `df_summary` |
| ORDER-001 | silent | Row order without `orderBy` after `groupBy`, `distinct`, `dropDuplicates` and joins differs from local Spark, and on Sail it can change from run to run. Spark guarantees no order there, but tests often rely on it by accident. Such a test is flaky on Sail. Pinned as a non-strict `xfail`, because a varying order cannot be pinned strictly. | `corpus/test_ordering.py`: `accidental_*` |
| GAP-005 | loud | `lit("2") + lit(3)` raises `AnalysisException` on Sail. Spark returns `5` as `long`. | `corpus/test_traps.py`: `coerce_string_plus_int` |
| GAP-006 | loud | Both raise, with different classes: int overflow, bad date cast, bad `to_date` format, `element_at` out of range. | `corpus/test_traps.py`, `corpus/test_functions.py` |
| GAP-007 | loud | Java-only regex features (lookahead, backreferences) raise on Sail, which uses Rust regex. | `corpus/test_traps.py`: `regex_lookahead`, `regex_backreference` |
| GAP-011 | loud | `explode` over a `create_map(...)` literal crashes inside Sail and raises. | `corpus/test_functions.py`: `fn_explode_map` |
| GAP-001 | schema | `unionByName(allowMissingColumns=True)` types the missing column as `void`. Spark uses the other side's type. | `corpus/test_traps.py`, `benchmarks/test_joins_and_unions.py` |
| GAP-002 | schema | `datediff` returns `long`. Spark returns `int`. | `corpus/test_traps.py`, `corpus/test_functions.py`, `benchmarks/test_time_nulls_sql.py` |
| GAP-003 | schema | `coalesce(int_col, "7")` is `int`. Spark ANSI widens to `long`. | `corpus/test_traps.py`: `coerce_coalesce_int_string_schema` |
| GAP-008 | schema | Nullability flags and field metadata differ for many expressions. `assertDataFrameEqual` ignores both by default, so only direct `df.schema ==` comparisons notice. | `corpus/test_traps.py`: `nullable_flag_*`, `metadata_of_dict_agg_alias` |

## Adoption gaps (PySpark's Spark Connect client)

These come from the Spark Connect client that sparkwarp uses, not from Sail. They would
appear against any Spark Connect server.

| ID | What differs | Workaround | Pinned by |
|---|---|---|---|
| ADOPT-001 | `isinstance(spark, pyspark.sql.SparkSession)` is `False`. `DataFrame`, `Column` and `Window` pass. | Check `pyspark.sql.utils.is_remote()`, or accept both session classes. | `tests/test_session_smoke.py` (strict xfail) |
| ADOPT-006 | `isinstance(df.groupBy(...), pyspark.sql.GroupedData)` is `False`, like the session in ADOPT-001. `StructType`, `Row`, `Column` and `WindowSpec` pass. | Avoid `isinstance` checks on grouped data, or accept both classes. | `tests/test_session_smoke.py` (strict xfail) |
| ADOPT-002 | Child processes inherit the Connect routing but not sparkwarp's builder patch. | `subprocess.run(..., env=sparkwarp.clean_environ())` | `tests/test_runtime.py` (asserts the inherited routing and the clean env) |
| ADOPT-003 | `spark.sparkContext` does not exist, so `setLogLevel` in a conftest fails. | Remove the line or guard it with `is_remote()`. A shim is planned (backlog A2.6). | `corpus/test_methods.py`: `action_spark_context_is_unsupported` |
| ADOPT-004 | `df.toJSON()` returns a DataFrame of `value` rows, not string rows. | `[r.value for r in df.select(F.to_json(F.struct("*")).alias("value")).collect()]` gives the same strings on both engines. | `corpus/test_methods.py`: `action_to_json` |
| ADOPT-005 | `df.rdd` is not implemented. | Out of scope; mark such tests `@pytest.mark.spark_only`. | `corpus/test_methods.py`: `action_rdd_is_unsupported` |

## Advice

- Keep a `SPARKWARP=0` parity lane on CI. It is the only guard against the silent gaps.
- Always `orderBy` before asserting row order, or use `assertDataFrameEqual`, which ignores
  order by default.
- For GAP-006, assert `pytest.raises(Exception)`. The class and the message both differ.
- For GAP-007 and ADOPT-005, mark the test `@pytest.mark.spark_only`.
