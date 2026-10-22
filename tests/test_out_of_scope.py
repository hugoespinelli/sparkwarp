"""Spark ML and Connect extensions (Delta's Python API): Sail rejects them loudly."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pyspark.sql.connect.proto as proto
import pytest
from pyspark.errors.exceptions.connect import UnsupportedOperationException
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.client.core import SparkConnectClient
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.plan import LogicalPlan

from sparkwarp.engines import SailEngine
from sparkwarp.pytest_plugin import UnsupportedFeature

DELTA = "delta"
DELTA_RELATION_TYPE = "type.googleapis.com/delta.connect.DeltaRelation"


class ExtensionRelation(LogicalPlan):
    """The kind of relation delta-spark's `DeltaTable` sends: a Connect extension."""

    def __init__(self) -> None:
        super().__init__(None)

    def plan(self, session: SparkConnectClient) -> proto.Relation:
        relation: proto.Relation = self._create_proto_relation()
        relation.extension.type_url = DELTA_RELATION_TYPE
        return relation


@dataclass(frozen=True)
class OutOfScope:
    """One call Sail does not implement, and the message it fails with."""

    name: str
    run: Callable[[SparkSession, DataFrame], object]
    message: str


OUT_OF_SCOPE = [
    OutOfScope(
        "ml_transformer",
        lambda s, frame: VectorAssembler(inputCols=["x", "y"], outputCol="v")
        .transform(frame)
        .collect(),
        "ML relation",
    ),
    OutOfScope(
        "ml_estimator",
        lambda s, frame: StringIndexer(inputCol="c", outputCol="i").fit(frame),
        "ml command",
    ),
    OutOfScope(
        "connect_extension_relation",
        lambda s, frame: ConnectDataFrame(ExtensionRelation(), s).collect(),
        "extension relation",
    ),
]


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


@pytest.fixture
def features(spark: SparkSession) -> DataFrame:
    """Numeric and string columns for ML stages."""
    return spark.createDataFrame(
        [(1.0, 2.0, "a"), (3.0, 4.0, "b")], "x double, y double, c string"
    )


@pytest.mark.parametrize("case", OUT_OF_SCOPE, ids=[case.name for case in OUT_OF_SCOPE])
def test_sail_rejects_it_loudly_with_the_sparkwarp_hint(
    spark: SparkSession, features: DataFrame, case: OutOfScope
) -> None:
    with pytest.raises(UnsupportedOperationException, match=case.message) as raised:
        case.run(spark, features)
    assert UnsupportedFeature.is_unsupported(raised.value)


def test_delta_tables_round_trip_through_read_and_write(
    spark: SparkSession, tmp_path: Path
) -> None:
    table = str(tmp_path / DELTA)
    spark.range(3).write.format(DELTA).save(table)
    rows = spark.read.format(DELTA).load(table).orderBy("id").collect()
    assert [row.id for row in rows] == [0, 1, 2]
