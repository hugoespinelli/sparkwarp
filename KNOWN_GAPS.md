# Known gaps

Everything sparkwarp knows to differ from real Spark 4 (ANSI mode), each pinned by a test.

- Engine gaps are corpus cases that run on both engines. Real Spark recorded the expected
  results; on Sail each case is a strict `xfail`, so it fails the day the gap closes.
  ORDER-001 is the exception: Sail's order varies between runs, so it is a non-strict `xfail`.
- Adoption gaps ADOPT-001, ADOPT-002 and ADOPT-006 are pinned in the package's own tests, which
  run on Sail only. ADOPT-003, ADOPT-004, ADOPT-005 and ADOPT-007 are corpus cases like the
  engine gaps: they run on both engines against Spark goldens and are strict `xfail`s on Sail.
- Behaviour that sparkwarp itself chooses (ADOPT-008) is pinned in the package's own tests.

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
| GAP-015 | silent | `df.is_cached` stays `True` after `df.unpersist()`. Sail treats persist and unpersist as no-ops. | `corpus/test_methods.py`: `action_is_cached_after_unpersist` |
| ORDER-001 | silent | Row order without `orderBy` after `groupBy`, `distinct`, `dropDuplicates` and joins differs from local Spark, and on Sail it can change from run to run. Spark guarantees no order there, but tests often rely on it by accident. Such a test is flaky on Sail. Pinned as a non-strict `xfail`, because a varying order cannot be pinned strictly. | `corpus/test_ordering.py`: `accidental_*` |
| GAP-005 | loud | `lit("2") + lit(3)` raises `AnalysisException` on Sail. Spark returns `5` as `long`. | `corpus/test_traps.py`: `coerce_string_plus_int` |
| GAP-006 | loud | Both raise, with different classes: int overflow, bad date cast, bad `to_date` format, `element_at` out of range. | `corpus/test_traps.py`, `corpus/test_functions.py` |
| GAP-007 | loud | Java-only regex features (lookahead, backreferences) raise on Sail, which uses Rust regex. | `corpus/test_traps.py`: `regex_lookahead`, `regex_backreference` |
| GAP-011 | loud | `explode` over a `create_map(...)` literal crashes inside Sail and raises. | `corpus/test_functions.py`: `fn_explode_map` |
| GAP-013 | loud | An array index that is a column or an expression fails: `F.col("parts")[F.col("i")]` raises a cast error and `F.col("parts")[F.size("parts") - 1]` raises "extraction must be a literal". `F.element_at(arr, col)` and `F.get(arr, col)` work. | `corpus/test_functions.py`: `fn_array_index_from_column`, `fn_array_index_from_expression` |
| GAP-014 | loud | A window ordered by a `CASE WHEN` whose condition compares a numeric function result, such as `F.when(F.length("name") > 3, 0).otherwise(1)`, fails to plan (`BoundedWindowAggExec`), for any window function. A `CASE WHEN` on a plain column or a string comparison works. | `corpus/test_traps.py`: `window_order_by_case_on_function_result` |
| GAP-016 | loud | `F.input_file_name()` is not implemented. On real Spark it returns an empty string for in-memory data. | `corpus/test_functions.py`: `fn_input_file_name` |
| GAP-017 | loud | `unionByName` of a string column with a boolean column fails to plan on Sail ("Incompatible inputs for Union"). Spark 4 in ANSI mode casts the string to boolean (and raises only on text like `"x"`). | `corpus/test_traps.py`: `union_by_name_string_and_boolean` |
| GAP-018 | loud | `F.md5(...)` followed by `distinct()`, `union()` or `unionByName()` fails on `collect()` with `UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION: string_view`. Sail returns `md5` as Arrow `Utf8View` and set operations pass that type through to the client; `sha1`, `sha2` and other string functions return plain strings. `toPandas()` is not affected. Workaround: `F.md5(c).cast("string")` before the set operation. | `corpus/test_functions.py`: `fn_md5_then_distinct`, `fn_md5_then_union` |
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
| ADOPT-003 | `spark.sparkContext` does not exist under Spark Connect. Since 0.2.0, sparkwarp provides a stub: `setLogLevel`, `applicationId`, `defaultParallelism` and `version` work; any other member still raises. Code that detects Connect with `try: spark.sparkContext` / `except` now takes the classic branch; check `pyspark.sql.utils.is_remote()` instead. | Mark tests that need the real `SparkContext` (`parallelize`, `wholeTextFiles`, Hadoop config) `spark_only`. | `corpus/test_methods.py`: `action_spark_context_is_unsupported` |
| ADOPT-004 | `df.toJSON()` returns a DataFrame of `value` rows, not string rows. | `[r.value for r in df.select(F.to_json(F.struct("*")).alias("value")).collect()]` gives the same strings on both engines. | `corpus/test_methods.py`: `action_to_json` |
| ADOPT-005 | `df.rdd` is not implemented. Since 0.3.0, sparkwarp provides a stub: `df.rdd.isEmpty()` works; any other member still raises. Code that detects Connect with `try: df.rdd` / `except` now takes the classic branch; check `pyspark.sql.utils.is_remote()` instead. | Use `df.isEmpty()`, or mark tests that need a real RDD (`map`, `collect`, `getNumPartitions`) `@pytest.mark.spark_only`. | `corpus/test_methods.py`: `action_rdd_is_unsupported`; the stub by `action_rdd_is_empty`, `action_rdd_is_empty_on_empty_frame` and `tests/test_compat.py` |
| ADOPT-007 | The Spark Connect client serializes a column as nested protobuf messages, limited to 100 levels. About 48 nested function calls fit (47 when the column also gets an `.alias()`); more raises `DecodeError: ... MaxDepth`. Real Spark has no such limit. | Collapse character-by-character chains into one call (`F.translate` for single-character maps, or one `regexp_replace` with an alternation), or mark the test `spark_only`. | `corpus/test_functions.py`: `fn_regexp_replace_chain_at_the_limit` (47 deep plus an alias, works), `fn_deep_regexp_replace_chain` (60 deep, fails) |

## sparkwarp behaviour (not Spark Connect)

These come from choices sparkwarp makes, not from the Spark Connect client or from Sail.

| ID | What differs | Workaround | Pinned by |
|---|---|---|---|
| ADOPT-008 | Under sparkwarp, `spark.stop()` on the shared session is held, so state carries over: `spark.conf.set(...)` values and temp views survive into later test classes. Real Spark's `stop()` gives the next `getOrCreate()` a fresh session. | Reset what a test changes in its own teardown (`spark.conf.unset(key)`, `spark.catalog.dropTempView(name)`), or give temp views unique names. | `tests/test_compat.py` (asserts the carry-over) |

## Advice

- Keep a `SPARKWARP=0` parity lane on CI. It is the only guard against the silent gaps.
- Always `orderBy` before asserting row order, or use `assertDataFrameEqual`, which ignores
  order by default.
- For GAP-006, assert `pytest.raises(Exception)`. The class and the message both differ.
- For GAP-007, and for ADOPT-005 beyond `df.rdd.isEmpty()`, mark the test `@pytest.mark.spark_only`.
