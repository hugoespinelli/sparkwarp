"""Arrow view types in results: converted to plain types before PySpark reads them."""

from collections.abc import Iterator

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.connect.client.core import SparkConnectClient

from sparkwarp.arrow_views import ArrowViewTypes, ViewCastingFetch, ViewTypesPatch
from sparkwarp.engines import SailEngine

needs_view_casts = pytest.mark.skipif(
    not ArrowViewTypes.castable(), reason="pyarrow below 18 cannot cast view types"
)
A_MD5 = "0cc175b9c0f1b6a831c399e269772661"
B_MD5 = "92eb5ffee6ae2fec3ad71c777531578f"
FETCH = ViewTypesPatch.ATTRIBUTE
VIEW_COLUMNS = [
    pa.array(["a", None], pa.string_view()),
    pa.array([b"a", None], pa.binary_view()),
    pa.array([["a"], None], pa.list_(pa.string_view())),
    pa.array([[b"a"], None], pa.large_list(pa.binary_view())),
    pa.array([["a", "b"], None], pa.list_(pa.string_view(), 2)),
    pa.array([{"h": "a"}, None], pa.struct([("h", pa.string_view())])),
    pa.array([[("k", b"v")], None], pa.map_(pa.string_view(), pa.binary_view())),
]
PLAIN_TYPES = [
    (pa.string_view(), pa.string()),
    (pa.binary_view(), pa.binary()),
    (pa.list_(pa.string_view()), pa.list_(pa.string())),
    (pa.large_list(pa.binary_view()), pa.large_list(pa.binary())),
    (pa.list_(pa.string_view(), 2), pa.list_(pa.string(), 2)),
    (
        pa.struct([("h", pa.string_view()), ("n", pa.int64())]),
        pa.struct([("h", pa.string()), ("n", pa.int64())]),
    ),
    (pa.map_(pa.string_view(), pa.binary_view()), pa.map_(pa.string(), pa.binary())),
    (
        pa.map_(pa.string(), pa.int64(), keys_sorted=True),
        pa.map_(pa.string(), pa.int64(), keys_sorted=True),
    ),
    (pa.int64(), pa.int64()),
]


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    engine = SailEngine()
    yield engine.start_session()
    engine.stop()


@pytest.mark.parametrize(("view", "plain"), PLAIN_TYPES)
def test_view_types_map_to_their_plain_types(
    view: pa.DataType, plain: pa.DataType
) -> None:
    assert ArrowViewTypes.plain(view) == plain


def test_batch_without_view_types_is_returned_as_is() -> None:
    batch = pa.record_batch({"n": pa.array([1, 2])})
    assert ArrowViewTypes.to_plain(batch) is batch


@needs_view_casts
def test_batch_with_view_types_keeps_values_and_metadata() -> None:
    schema = pa.schema([("h", pa.string_view())], metadata={b"k": b"v"})
    batch = pa.record_batch([pa.array(["a", None], pa.string_view())], schema=schema)
    plain = ArrowViewTypes.to_plain(batch)
    assert plain.schema.field("h").type == pa.string()
    assert plain.schema.metadata == {b"k": b"v"}
    assert plain.column(0).to_pylist() == ["a", None]


@needs_view_casts
def test_md5_then_distinct_collects(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a",), ("a",), ("b",)], "s string")
    hashes = frame.select(F.md5("s").alias("h")).distinct()
    assert sorted(row.h for row in hashes.collect()) == [A_MD5, B_MD5]


@needs_view_casts
def test_nested_view_values_collect(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("a",)], "s string")
    nested = frame.select(
        F.array(F.md5("s")).alias("a"), F.struct(F.md5("s").alias("x")).alias("st")
    ).distinct()
    row = nested.collect()[0]
    assert row.a == [A_MD5]
    assert row.st.x == A_MD5


@needs_view_casts
def test_engine_stop_restores_the_fetch() -> None:
    original = vars(SparkConnectClient)[FETCH]
    engine = SailEngine()
    try:
        engine.start_session()
        assert vars(SparkConnectClient)[FETCH] is not original
    finally:
        engine.stop()
    assert vars(SparkConnectClient)[FETCH] is original


@needs_view_casts
@pytest.mark.parametrize("column", VIEW_COLUMNS, ids=lambda c: str(c.type))
def test_each_view_kind_casts_with_its_values(column: pa.Array) -> None:
    batch = pa.record_batch([column], names=["c"])
    plain = ArrowViewTypes.to_plain(batch)
    assert plain.schema.field("c").type == ArrowViewTypes.plain(column.type)
    assert plain.column(0).to_pylist() == column.to_pylist()


class FakeStream:
    """Yields what a client stream yields: a schema-like object, then Arrow batches."""

    MARKER = "schema"

    def __init__(self, batch: pa.RecordBatch) -> None:
        self._batch = batch

    def __call__(self, client: object) -> Iterator[object]:
        yield self.MARKER
        yield self._batch


@needs_view_casts
def test_the_fetch_wrapper_casts_batches_and_passes_the_rest() -> None:
    batch = pa.record_batch([pa.array([b"a"], pa.binary_view())], names=["b"])
    fetch = ViewCastingFetch(FakeStream(batch))
    marker, cast_batch = list(fetch(object()))
    assert marker == FakeStream.MARKER
    assert cast_batch.schema.field("b").type == pa.binary()
    assert cast_batch.column(0).to_pylist() == [b"a"]


def test_the_patch_applies_only_where_pyarrow_can_cast_view_types() -> None:
    assert ViewTypesPatch().applies is ArrowViewTypes.castable()
