import time, os
T = time.perf_counter
t = T()
from pysail.spark import SparkConnectServer
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.testing.utils import assertDataFrameEqual
print(f"[imports] {T()-t:.3f}s")

t = T(); server = SparkConnectServer("127.0.0.1", 0); server.start(background=True)
ip, port = server.listening_address; print(f"[sail server start] {T()-t:.3f}s on {ip}:{port}")
t = T(); spark = SparkSession.builder.remote(f"sc://{ip}:{port}").getOrCreate(); print(f"[session] {T()-t:.3f}s")

schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
t = T()
df = spark.createDataFrame([(1, "alice", 30), (2, "bob", 25), (3, "carol", 40), (4, "bob", 45)], schema)
print(f"[createDataFrame] {T()-t:.4f}s")
t = T(); n = df.filter(F.col("age") >= 30).count(); print(f"[first action count={n}] {T()-t:.4f}s")

def filter_test():
    result = df.filter((F.col("age") >= 30) & F.col("name").isin(["bob", "carol"]))
    expected = spark.createDataFrame([(3, "carol", 40), (4, "bob", 45)], schema)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))
t = T(); filter_test(); print(f"[brief filter test, 1st] {T()-t:.4f}s")
ts = []
for _ in range(20):
    t = T(); filter_test(); ts.append(T()-t)
ts.sort(); print(f"[brief filter test x20] median={ts[10]*1000:.1f}ms min={ts[0]*1000:.1f}ms max={ts[-1]*1000:.1f}ms")
ts = []
for _ in range(50):
    t = T(); df.filter(F.col("age") >= 30).count(); ts.append(T()-t)
ts.sort(); print(f"[single count x50] median={ts[25]*1000:.1f}ms min={ts[0]*1000:.1f}ms")
ts = []
for _ in range(50):
    t = T(); df.withColumn("x", F.coalesce(F.col("age"), F.lit(0))).select("id","x").collect(); ts.append(T()-t)
ts.sort(); print(f"[withColumn+collect x50] median={ts[25]*1000:.1f}ms")
t = T(); print("[schema]", df.schema.simpleString(), f"{T()-t:.4f}s")

# semantic traps
def probe(label, fn):
    try:
        r = fn(); print(f"[trap] {label}: {r}")
    except Exception as e:
        print(f"[trap] {label}: RAISED {type(e).__name__}: {str(e).splitlines()[0][:110]}")
nd = spark.createDataFrame([(1, None), (2, 5)], StructType([StructField("id", IntegerType()), StructField("v", IntegerType())]))
probe("when(null>1,'a').otherwise('b')", lambda: [r[0] for r in nd.select(F.when(F.col("v") > 1, "a").otherwise("b")).orderBy(F.lit(1)).collect()])
probe("filter v != 5 (null dropped?)", lambda: nd.filter(F.col("v") != 5).count())
probe("coalesce(int,'x') type", lambda: nd.select(F.coalesce(F.col("v"), F.lit("x")).alias("c")).schema.simpleString())
probe("lit(1)/lit(2)", lambda: spark.range(1).select((F.lit(1)/F.lit(2)).alias("d")).schema.simpleString())
probe("cast('abc' as int)", lambda: spark.range(1).select(F.lit("abc").cast("int")).collect()[0][0])
probe("cast 'abc' try_cast", lambda: spark.range(1).select(F.try_cast if False else F.lit("abc").try_cast("int")).collect()[0][0])
probe("size(null)", lambda: spark.range(1).select(F.size(F.lit(None).cast("array<int>"))).collect()[0][0])
probe("regexp_replace java \\d", lambda: spark.range(1).select(F.regexp_replace(F.lit("a1b22"), r"\d+", "#")).collect()[0][0])
probe("createDataFrame int inference", lambda: spark.createDataFrame([(1,"a")], ["i","s"]).schema.simpleString())
probe("row_number window", lambda: [tuple(r) for r in nd.select("id", F.row_number().over(__import__("pyspark.sql.connect.window", fromlist=["Window"]).Window.partitionBy(F.lit(1)).orderBy("id")).alias("rn")).orderBy("id").collect()])
probe("sum over orderBy default frame", lambda: [r[1] for r in nd.select("id", F.sum("id").over(__import__("pyspark.sql.connect.window", fromlist=["Window"]).Window.orderBy("id")).alias("s")).orderBy("id").collect()])
probe("from_json ddl", lambda: spark.range(1).select(F.from_json(F.lit('{"a":1}'), "a INT").alias("j")).collect()[0][0])
probe("spark.sql temp view", lambda: (nd.createOrReplaceTempView("t"), spark.sql("SELECT count(*) AS c FROM t WHERE v IS NULL").collect()[0][0])[1])
probe("unionByName+dropDuplicates", lambda: nd.unionByName(nd).dropDuplicates().count())
probe("explode", lambda: spark.range(1).select(F.explode(F.array(F.lit(1),F.lit(2)))).count())
probe("left_anti join", lambda: nd.join(nd.filter(F.col("id")==1), on="id", how="left_anti").count())
probe("struct/concat_ws/trim/lower", lambda: spark.range(1).select(F.concat_ws("-", F.trim(F.lit(" A ")), F.lower(F.lit("B"))), F.struct(F.lit(1).alias("x"))).collect()[0])
probe("date_trunc MM type", lambda: spark.range(1).select(F.trunc(F.lit("2024-05-17").cast("date"), "MM").alias("d")).schema.simpleString())
probe("current_timestamp", lambda: type(spark.range(1).select(F.current_timestamp()).collect()[0][0]).__name__)
probe("python udf", lambda: spark.range(3).select(F.udf(lambda x: x*2, IntegerType())("id")).collect()[2][0])
server.stop(); print("[server stopped]")
