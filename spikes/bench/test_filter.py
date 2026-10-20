import pytest
from pyspark.sql.connect import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.testing.utils import assertDataFrameEqual

SCHEMA = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True), StructField("age", IntegerType(), True)])

@pytest.mark.parametrize("i", range(300))
def test_filter_and(spark, i):
    df = spark.createDataFrame([(1, "alice", 30), (2, "bob", 25), (3, "carol", 40), (4, "bob", 45)], SCHEMA)
    result = df.filter((F.col("age") >= 30) & F.col("name").isin(["bob", "carol"])).withColumn("age", F.col("age") + F.lit(i) - F.lit(i))
    expected = spark.createDataFrame([(3, "carol", 40), (4, "bob", 45)], SCHEMA)
    assertDataFrameEqual(result.orderBy("id"), expected.orderBy("id"))
