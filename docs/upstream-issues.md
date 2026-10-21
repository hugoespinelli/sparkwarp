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
