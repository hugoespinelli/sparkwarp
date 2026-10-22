# Known gaps

Everything sparkwarp knows to differ from real Spark 4 (ANSI mode), each pinned by a test.

- Engine gaps are corpus cases that run on both engines. Real Spark recorded the expected
  results; on Sail each case is a strict `xfail`, so it fails the day the gap closes.
  ORDER-001 is the exception: Sail's order varies between runs, so it is a non-strict `xfail`.
- Adoption gaps ADOPT-001, ADOPT-002 and ADOPT-006, and the out-of-scope ADOPT-011 and
  ADOPT-012, are pinned in the package's own tests, which run on Sail only. ADOPT-003,
  ADOPT-004 and ADOPT-005 are corpus cases like the engine gaps: they run on both engines
  against Spark goldens and are strict `xfail`s on Sail.
- Behaviour that sparkwarp itself chooses (ADOPT-008 to ADOPT-010, ADOPT-013) is pinned in the package's
  own tests, as far as a test can run in reasonable time; the row says what is measured only.

Versions: PySpark 4.2.0, pysail 0.7.2, checked 2026-10-22.

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
| GAP-007 | loud | Java-only regex features (look-around, backreferences, atomic groups, `\Z`) raise on Sail, which uses Rust regex. Since 0.3.0, sparkwarp runs `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `rlike`, `regexp` and `regexp_like` with such a literal pattern as Python UDFs on `re`, with Spark's results and column names (but see ADOPT-010). It translates only syntax both engines read alike: literals, `.`, `^`, `$`, groups (capturing, `(?:`, named, look-around, atomic), quantifiers, simple character classes, `\d \w \s` and their negations, control escapes, `\xhh` and `\uhhhh`, single-digit and named backreferences, `\A \Z \z`, and a leading `(?i)`; line terminators keep Java's meaning for `.`, `$` and `\Z`. Anything else keeps Sail's native path and still raises: `\b`, octal escapes, `\p{...}`, `\G`, `\Q...\E`, nested classes and `&&`, comments and conditionals, flags other than a leading `(?i)`, `{,n}`, variable-length look-behind, multi-digit backreferences or references to a missing group, a capturing group inside look-around or an atomic group, a possessive quantifier on a group, `\u` escapes of surrogate halves, atomic groups and possessive quantifiers on Python 3.10. Also still raising: the same patterns inside SQL text (`F.expr`, `selectExpr`, `spark.sql`), patterns held in a column, other regex functions (`split`, `regexp_count`, `regexp_substr`, `regexp_instr`), `regexp_replace` with a start position, a group index that is not a literal, and a subject that is not a string. An invalid replacement (`$x`) or group index fails with `PythonException` instead of Spark's error class. | `corpus/test_traps.py`: `regex_lookahead_in_sql_text`; the fallback by `regex_lookahead`, `regex_backreference`, `regex_lookbehind_extract`, `regex_lookahead_extract_all` and `tests/test_regex_fallback.py` |
| GAP-011 | loud | `explode` over a `create_map(...)` literal crashes inside Sail and raises. | `corpus/test_functions.py`: `fn_explode_map` |
| GAP-013 | loud | An array index that is a column or an expression fails: `F.col("parts")[F.col("i")]` raises a cast error and `F.col("parts")[F.size("parts") - 1]` raises "extraction must be a literal". `F.element_at(arr, col)` and `F.get(arr, col)` work. Fixed upstream in lakehq/sail#2840 (open), not in a pysail release yet. Since 0.4.0, with Java installed, sparkwarp runs a self-contained query that hits this on the JVM instead (README, "The JVM for what Sail is bad at"); the corpus measures Sail with routing off. | `corpus/test_functions.py`: `fn_array_index_from_column`, `fn_array_index_from_expression` |
| GAP-014 | loud | A window ordered by a `CASE WHEN` whose condition compares a numeric function result, such as `F.when(F.length("name") > 3, 0).otherwise(1)`, fails to plan (`BoundedWindowAggExec`), for any window function. A `CASE WHEN` on a plain column or a string comparison works. Fixed upstream in lakehq/sail#2841 (open), not in a pysail release yet. Since 0.4.0, with Java installed, sparkwarp runs a self-contained query that hits this on the JVM instead (README, "The JVM for what Sail is bad at"); the corpus measures Sail with routing off. | `corpus/test_traps.py`: `window_order_by_case_on_function_result` |
| GAP-016 | loud | `F.input_file_name()` is not implemented. On real Spark it returns an empty string for in-memory data. Fixed in Sail `main` (lakehq/sail#2571), not in a pysail release yet. Since 0.4.0, with Java installed, sparkwarp runs a self-contained query that hits this on the JVM instead (README, "The JVM for what Sail is bad at"); the corpus measures Sail with routing off. | `corpus/test_functions.py`: `fn_input_file_name` |
| GAP-017 | loud | `unionByName` of a string column with a boolean column fails to plan on Sail ("Incompatible inputs for Union"). Spark 4 in ANSI mode casts the string to boolean (and raises only on text like `"x"`). Since 0.4.0, with Java installed, sparkwarp runs a self-contained query that hits this on the JVM instead (README, "The JVM for what Sail is bad at"); the corpus measures Sail with routing off. | `corpus/test_traps.py`: `union_by_name_string_and_boolean` |
| GAP-019 | loud | `F.md5(...)` passed to `unhex`, `split` or `regexp_extract` fails inside Sail (`could not cast array of type Utf8View` / `Could not downcast arguments`). Sail returns `md5` as Arrow `Utf8View`, which these functions do not accept. Other string functions (`upper`, `concat`, `substring`, `sha2`, `like`) are fine. Workaround: `F.md5(c).cast("string")`. Still present in Sail `main`. | `corpus/test_functions.py`: `fn_unhex_of_md5`, `fn_split_of_md5`, `fn_regexp_extract_of_md5` |
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

## Sail does not implement (out of scope)

These raise on Sail but work on a real Spark 4 Spark Connect server (with Spark ML, or with
the Delta Connect plugin). sparkwarp does not plan to cover them.

| ID | What differs | Workaround | Pinned by |
|---|---|---|---|
| ADOPT-011 | Spark ML (`pyspark.ml`) under Spark Connect sends ML relations and ML commands, which Sail does not implement: a transformer fails with `UnsupportedOperationException: ML relation`, an estimator's `fit` with `ml command`. | Out of scope; mark such tests `@pytest.mark.spark_only`. | `tests/test_out_of_scope.py`: `ml_transformer`, `ml_estimator` |
| ADOPT-012 | Delta's `DeltaTable` Python API (delta-spark) sends a Spark Connect extension relation, which Sail does not implement: `DeltaTable.forPath(...)` fails with `UnsupportedOperationException: extension relation`. Delta tables read and written through `spark.read.format("delta")` and `df.write.format("delta")` work on Sail. | Out of scope; mark such tests `@pytest.mark.spark_only`. | `tests/test_out_of_scope.py`: `connect_extension_relation` (a hand-built extension relation; delta-spark is not a test dependency) and `test_delta_tables_round_trip_through_read_and_write` |

## sparkwarp behaviour (not Spark Connect)

These come from choices sparkwarp makes, not from the Spark Connect client or from Sail.

| ID | What differs | Workaround | Pinned by |
|---|---|---|---|
| ADOPT-008 | Under sparkwarp, `spark.stop()` on the shared session is held, so state carries over: `spark.conf.set(...)` values and temp views survive into later test classes. Real Spark's `stop()` gives the next `getOrCreate()` a fresh session. | Reset what a test changes in its own teardown (`spark.conf.unset(key)`, `spark.catalog.dropTempView(name)`), or give temp views unique names. | `tests/test_compat.py` (asserts the carry-over) |
| ADOPT-009 | Columns nested 64 levels or deeper (`SPARKWARP_JVM_DEPTH`), such as one `regexp_replace` per character. Sail plans them very slowly (150 calls about 4 s, 300 about 50 s). With Java installed, sparkwarp runs a self-contained query like this on the JVM (300 calls in about 0.3 s once the server is up); over a temp view or table it stays on Sail and is slow. Without Java, with `SPARKWARP_JVM=0`, or when the server fails to start, such a column fails fast with `NOT_IMPLEMENTED` and the sparkwarp hint. Separately, real Spark raises `StackOverflowError` from about 350 nested calls, and PySpark's client raises `RecursionError` past about 495 (990 on Python 3.12+). | Collapse a long chain into one `F.translate`, install Java so the JVM runs it, or mark the test `spark_only`; `SPARKWARP_JVM_DEPTH=0` runs it on Sail, slowly. | `tests/test_router.py`, `tests/test_deep_columns.py`, `tests/test_pytest_plugin.py`: `test_without_a_jvm_a_too_deep_column_fails_fast_with_the_hint` |
| ADOPT-010 | sparkwarp's regex fallback (GAP-007) fails with `PythonException` ("cannot match Java on text with a character outside the Basic Multilingual Plane") on any text containing such a character, an emoji for instance. Java matches UTF-16 code units, so look-behind, empty matches and `.` can treat an emoji as two characters there; rather than risk a different value, the fallback refuses. Spark returns a value. | Mark such tests `spark_only`, or keep emoji out of text that a Java-only regex runs on. | `tests/test_regex_fallback.py`: `test_text_outside_the_bmp_fails_where_java_would_differ` |
| ADOPT-013 | Under sparkwarp 0.4.0 with Java, the queries routed to the JVM (ADOPT-009, GAP-013, GAP-014, GAP-016, GAP-017) get real Spark's results and errors, and every other query gets Sail's, so one test can see both engines. A routed query sees `spark.conf` settings but not temp views, tables, checkpoints, functions registered by name or the working directory, and its Python workers do not see the test's imports or monkeypatches, which is why only self-contained queries move: inline data, or built-in file formats at absolute paths, with no SQL text and no inline Python code. The first routed query in a run waits about 10 s for the JVM server. | `SPARKWARP_JVM=0` with `SPARKWARP_JVM_DEPTH=0` keeps everything on Sail. | `tests/test_router.py` |

## Closed gaps

Kept so that IDs in old logs and reports still lead somewhere. The IDs are in backticks so
that a corpus case cannot be tagged with a closed gap.

| ID | Closed in | How |
|---|---|---|
| `ADOPT-007` | 0.3.0 | The Spark Connect client's protobuf backend (upb) stopped columns nested more than about 48 calls deep with `DecodeError: ... MaxDepth`. While sparkwarp runs, it lifts that limit and gives Sail's threads a 32 MB stack. upb's only switch is process-wide and also lifts its 64 MB message cap for every protobuf message in the process. What remains is ADOPT-009. |
| `GAP-018` | 0.3.0 | `F.md5(...)` before `distinct()` or a union failed on `collect()` with `string_view is not supported`. sparkwarp now casts Arrow view types in every result to plain types before PySpark reads them, with pyarrow 18 or newer (older pyarrow cannot cast view types, and the old error stays). Also fixed in Sail `main` (lakehq/sail#2837), not yet released. The same `Utf8View` still breaks some functions inside Sail: GAP-019. |

## Advice

- Keep a `SPARKWARP=0` parity lane on CI. It is the only guard against the silent gaps.
- Always `orderBy` before asserting row order, or use `assertDataFrameEqual`, which ignores
  order by default.
- For GAP-006, assert `pytest.raises(Exception)`. The class and the message both differ.
- For GAP-007 cases sparkwarp cannot route to Python, for ADOPT-005 beyond
  `df.rdd.isEmpty()`, and for ADOPT-011 and ADOPT-012, mark the test
  `@pytest.mark.spark_only`.
