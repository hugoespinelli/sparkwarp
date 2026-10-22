# Draft issues for lakehq/sail

Drafts only. Nothing here has been filed. Each draft is a minimal reproduction against
pysail 0.7.2 with PySpark 4.2.0 as the client, compared with Apache Spark 4.2 in its
default ANSI mode. File them one per issue; link each back into `KNOWN_GAPS.md`.

Shared setup for every repro:

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession, functions as F

server = SparkConnectServer("127.0.0.1", 0)
server.start(background=True)
spark = SparkSession.builder.remote("sc://%s:%d" % server.listening_address).getOrCreate()
```

## 1. Mixed string/int in CASE WHEN and array() widens to string instead of int (GAP-004)

```python
df = spark.createDataFrame([(5,), (0,)], "v int")
df.select(F.when(F.col("v") > 1, "a").otherwise(0).alias("r")).collect()
spark.range(1).select(F.array(F.lit(1), F.lit("a"))).collect()
```

Spark 4 (ANSI): both raise `NumberFormatException` (`CAST_INVALID_INPUT`), because the least
common type of STRING and INT is the numeric type. Sail: returns `['a', '0']` and
`['1', 'a']` as strings. This is a silent difference.

## 2. isnan(NULL) returns NULL instead of false (GAP-009)

```python
spark.createDataFrame([(None,)], "d double").select(F.isnan("d")).collect()
```

Spark: `[Row(isnan(d)=False)]`, non-nullable. Sail: `[Row(isnan(d)=None)]`.

## 3. percentile_approx interpolates and returns double (GAP-010)

```python
spark.createDataFrame([(25,), (30,)], "age int").agg(F.percentile_approx("age", 0.5)).collect()
```

Spark: returns an element of the input with the input's type (`25`, int). Sail: `27.5`, double.

## 4. explode over a map literal panics (GAP-011)

```python
spark.range(1).select(F.explode(F.create_map(F.lit("k"), F.lit(1)))).collect()
```

Spark: `[Row(key='k', value=1)]`. Sail: the Rust worker panics with
`ListArray expected data type Struct("key": non-null Utf8, "value": Int32) got
Struct("key": non-null Utf8, "value": non-null Int32)`.

## 5. string + int raises instead of coercing (GAP-005)

```python
spark.range(1).select(F.lit("2") + F.lit(3)).collect()
```

Spark: `5` as BIGINT. Sail: `AnalysisException`.

## 6. unionByName(allowMissingColumns=True) types the missing column as void (GAP-001)

```python
a = spark.createDataFrame([(1, "a")], "id int, name string")
b = spark.createDataFrame([("b", 2, "x")], "name string, id int, note string")
a.unionByName(b, allowMissingColumns=True).schema["note"].dataType
```

Spark: `StringType()`. Sail: `NullType()`.

## 7. datediff returns BIGINT instead of INT (GAP-002)

```python
spark.range(1).select(F.datediff(F.lit("2024-05-20").cast("date"), F.lit("2024-05-17").cast("date"))).schema
```

Spark: `IntegerType`. Sail: `LongType`.

## 8. coalesce(int, string) is INT instead of BIGINT (GAP-003)

```python
spark.createDataFrame([(None,)], "v int").select(F.coalesce("v", F.lit("7"))).schema
```

Spark 4 ANSI: `LongType` (least common type of INT and STRING is BIGINT). Sail: `IntegerType`.

## 9. Exception classes differ from Spark for runtime errors (GAP-006)

| Expression | Spark | Sail |
|---|---|---|
| `F.lit(2**31).cast("int")` | `ArithmeticException` | `NumberFormatException` |
| `F.lit("2024-02-30").cast("date")` | `DateTimeException` | `AnalysisException` |
| `F.to_date(F.lit("not a date"), "yyyy-MM-dd")` | `DateTimeException` | `AnalysisException` |
| `F.element_at(F.array(F.lit(5)), 3)` | `ArrayIndexOutOfBoundsException` | `AnalysisException` |

Tests that `pytest.raises` a specific class see the difference.

## 10. Java regex features: lookaround and backreferences (GAP-007)

```python
spark.range(1).select(F.regexp_replace(F.lit("price100usd"), r"\d+(?=usd)", "N")).collect()
spark.range(1).select(F.lit("abab").rlike(r"^(ab)\1$")).collect()
```

Spark (java.util.regex): `priceNusd` and `True`. Sail: raises. Likely a known design limit of
the Rust `regex` crate; worth a documented note or a `fancy-regex` fallback.

## 11. describe() and summary() return statistic rows in a different order (GAP-012)

```python
spark.createDataFrame([(25,), (30,)], "age int").describe("age").collect()
```

Spark: rows in the order `count, mean, stddev, min, max`, which the API documents. Sail:
the same values in another order, and the order changes from run to run (five runs
gave five different orders; one was `stddev, count, max, min, mean`).

## 12. Array index from a column or an expression fails (GAP-013)

```python
df = spark.createDataFrame([(["a", "b", "c"], 1)], "parts array<string>, i int")
df.select(F.col("parts")[F.col("i")]).collect()
df.select(F.col("parts")[F.size("parts") - 1]).collect()
```

Spark: `b` and `c`. Sail: the first raises `Cast error: Cannot cast string 'i' to value of
Int64 type`, the second raises `invalid argument: extraction must be a literal`.
`F.element_at("parts", F.col("i") + 1)` and `F.get("parts", F.col("i"))` work.

## 13. Window ordered by a CASE WHEN on a function result fails to plan (GAP-014)

```python
df = spark.createDataFrame([(1, "bob", "eng"), (2, "carol", "eng")], "id int, name string, dept string")
w = Window.partitionBy("dept").orderBy(F.when(F.length("name") > 3, 0).otherwise(1))
df.select(F.row_number().over(w)).collect()
```

Spark: row numbers. Sail: `Error during planning: Plan: ["BoundedWindowAggExec: ...`. It
fails for `row_number`, `rank` and `sum` alike. A `CASE WHEN` on a plain column, or on a
string comparison such as `F.upper("name") == "BOB"`, plans fine.

## 14. unionByName of string and boolean columns fails to plan (GAP-017)

```python
a = spark.createDataFrame([("true",)], "c string")
b = spark.createDataFrame([(False,)], "c boolean")
a.unionByName(b).collect()
```

Spark 4 (ANSI): casts the string side to boolean, `[True, False]`. Sail: `Incompatible inputs
for Union: Previous inputs were of type Utf8, but got incompatible ...`.

## 15. is_cached stays True after unpersist (GAP-015)

```python
df = spark.range(3)
df.cache(); df.count(); df.unpersist()
df.is_cached
```

Spark: `False`. Sail: `True`. Persist and unpersist are logged as no-ops.

## 16. input_file_name() is not implemented (GAP-016)

```python
spark.range(1).select(F.input_file_name()).collect()
```

Spark: `[Row(input_file_name()='')]` for in-memory data, and the file path for file sources.
Sail: `UnsupportedOperationException: function: input_file_name`.

## 17. catalog.clearCache() is unsupported

```python
spark.catalog.clearCache()
```

Spark: no-op when nothing is cached. Sail: `UnsupportedOperationException:
PlanNode::ClearCache`. Since persist is already a no-op on Sail, accepting `ClearCache` as a
no-op would match. sparkwarp works around it today.

## 18. md5 returns Utf8View, which set operations pass to the client (GAP-018)

Status: fixed in Sail `main` by lakehq/sail#2837 (not yet released); do not file. sparkwarp
casts view types on the client since 0.3.0.

```python
df = spark.createDataFrame([("a",), ("b",)], "k string")
df.select(F.md5("k").alias("v")).distinct().collect()
df.select(F.md5("k").alias("v")).union(df.select("k")).collect()
```

Spark: two rows of hex strings. Sail: PySpark 4.0, 4.1 and 4.2 all raise
`[UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION] string_view is not supported in conversion to
Arrow` during `collect()`. `df.schema` says `string`; only the Arrow batch carries `Utf8View`.
A plain projection, `orderBy`, `groupBy` and joins are fine, and `sha1`, `sha2` and `lower`
return `Utf8`. Suggested fix: return `Utf8` from `md5` like the other hash functions, or cast
`Utf8View` to `Utf8` at the Spark Connect result boundary.

## 19. md5's Utf8View breaks unhex, split and regexp_extract (GAP-019)

```python
df = spark.createDataFrame([("a",)], "s string")
df.select(F.unhex(F.md5("s"))).collect()
df.select(F.split(F.md5("s"), "a")).collect()
df.select(F.regexp_extract(F.md5("s"), "([0-9a-f]{4})", 1)).collect()
```

Spark: one row each. Sail (0.7.2 and `main` after #2837): `could not cast array of type
Utf8View to ...GenericByteArray` for `unhex`, and `Could not downcast arguments to arrow
arrays` for `split` and `regexp_extract`. `upper`, `concat`, `substring`, `sha2` and `like`
accept the same input. Suggested fix: return `Utf8` from `md5` like `sha1` and `sha2`, or
accept `Utf8View` in these kernels.
