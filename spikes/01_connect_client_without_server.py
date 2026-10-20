import time, sys, os
os.environ.pop("SPARK_HOME", None)
t0 = time.perf_counter()
from pyspark.sql.connect import functions as F
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
print(f"[import] {time.perf_counter()-t0:.3f}s")

# 1) Column with no session at all
c = (F.col("a") + F.lit(1)).alias("b")
print("[column type]", type(c).__module__, type(c).__name__)
print("[column repr]", c)
expr = c._expr
print("[expr type]", type(expr).__name__)
proto = expr.to_plan(None)
print("[proto Expression, no session]\n", proto)

# 2) Session with a remote URL that nothing listens on -> does builder connect?
t0 = time.perf_counter()
spark = SparkSession.builder.remote("sc://localhost:65000").getOrCreate()
print(f"[session built] {time.perf_counter()-t0:.3f}s  client={type(spark._client).__name__}")

# 3) createDataFrame with explicit schema -> pure local?
schema = StructType([StructField("id", IntegerType()), StructField("name", StringType()), StructField("age", IntegerType())])
t0 = time.perf_counter()
try:
    df = spark.createDataFrame([(1, "alice", 30), (2, "bob", 25)], schema)
    print(f"[createDataFrame explicit schema] OK {time.perf_counter()-t0:.3f}s plan={type(df._plan).__name__}")
except Exception as e:
    print(f"[createDataFrame explicit schema] FAILED {type(e).__name__}: {str(e)[:120]}")

# 4) createDataFrame w/o schema -> infers locally or asks server?
t0 = time.perf_counter()
try:
    df2 = spark.createDataFrame([(1, "alice", 30), (2, "bob", 25)], ["id", "name", "age"])
    print(f"[createDataFrame names only] OK {time.perf_counter()-t0:.3f}s")
except Exception as e:
    print(f"[createDataFrame names only] FAILED {type(e).__name__}: {str(e)[:160]}")

# 5) transformations: pure proto?
try:
    out = df.filter((F.col("age") >= 30) & F.col("name").isin(["bob", "carol"])).select("id", (F.col("age") * 2).alias("age2"))
    rel = out._plan.to_proto(spark._client)
    print("[relation proto, no server]\n", rel)
except Exception as e:
    print(f"[transform] FAILED {type(e).__name__}: {str(e)[:160]}")

# 6) what hits the server?
if os.environ.get("STEP6"):
    for name, fn in [("df.schema", lambda: df.schema), ("df.columns", lambda: df.columns), ("df.collect", lambda: df.collect())]:
      t0 = time.perf_counter()
      try:
          fn(); print(f"[{name}] no server call?!")
      except Exception as e:
          print(f"[{name}] -> server call ({type(e).__name__}) after {time.perf_counter()-t0:.2f}s")
