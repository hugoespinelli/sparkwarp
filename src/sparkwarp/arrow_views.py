"""Arrow view types in Sail's results, converted to the plain types PySpark reads.

Sail can return Arrow `string_view` and `binary_view` columns (after `md5()` and a set
operation, for one). PySpark's Arrow-to-Spark type conversion rejects them, so sparkwarp
casts each result batch to the matching plain types before PySpark sees it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from types import MethodType
from typing import cast

import pyarrow as pa
from pyspark.sql.connect.client.core import SparkConnectClient

from sparkwarp.patches import ClassPatch

Fetch = Callable[..., Iterator[object]]


class ArrowViewTypes:
    """Maps `string_view` and `binary_view`, also nested in lists, structs and maps, to
    plain types with the same values."""

    @classmethod
    def plain(cls, data_type: pa.DataType) -> pa.DataType:
        if pa.types.is_string_view(data_type):
            return pa.string()
        if pa.types.is_binary_view(data_type):
            return pa.binary()
        if pa.types.is_struct(data_type):
            return pa.struct([cls._plain_field(field) for field in data_type])
        if pa.types.is_map(data_type):
            return pa.map_(
                cls._plain_field(data_type.key_field),
                cls._plain_field(data_type.item_field),
                keys_sorted=data_type.keys_sorted,
            )
        is_list = pa.types.is_list(data_type)
        is_large_list = pa.types.is_large_list(data_type)
        is_fixed_size_list = pa.types.is_fixed_size_list(data_type)
        if is_list or is_large_list or is_fixed_size_list:
            return cls._plain_list(data_type, large=is_large_list)
        return data_type

    @staticmethod
    def castable() -> bool:
        """Whether pyarrow casts view types (they exist from 16, the casts from 18)."""
        try:
            pa.array([], pa.string_view()).cast(pa.string())
        except (AttributeError, pa.ArrowNotImplementedError):
            return False
        return True

    @classmethod
    def to_plain(cls, batch: pa.RecordBatch) -> pa.RecordBatch:
        """The batch with plain types; the same object when it holds no view type."""
        schema = pa.schema(
            [cls._plain_field(field) for field in batch.schema],
            metadata=batch.schema.metadata,
        )
        has_view_types = not schema.equals(batch.schema)
        return batch.cast(schema) if has_view_types else batch

    @classmethod
    def _plain_list(cls, data_type: pa.DataType, large: bool) -> pa.DataType:
        values = cls._plain_field(data_type.value_field)
        if pa.types.is_fixed_size_list(data_type):
            return pa.list_(values, data_type.list_size)
        return pa.large_list(values) if large else pa.list_(values)

    @classmethod
    def _plain_field(cls, field: pa.Field) -> pa.Field:
        return field.with_type(cls.plain(field.type))


class ViewCastingFetch:
    """Stands in for the client's result stream and casts each Arrow batch it yields."""

    def __init__(self, original: Fetch) -> None:
        self.original = original

    def __get__(
        self, client: SparkConnectClient | None, owner: type | None = None
    ) -> Fetch | ViewCastingFetch:
        if client is None:
            return self
        return MethodType(self, client)

    def __call__(
        self, client: SparkConnectClient, *args: object, **kwargs: object
    ) -> Iterator[object]:
        for response in self.original(client, *args, **kwargs):
            is_batch = isinstance(response, pa.RecordBatch)
            yield ArrowViewTypes.to_plain(response) if is_batch else response


class ViewTypesPatch(ClassPatch):
    """Casts view types in every result PySpark fetches, while sparkwarp runs."""

    TARGET = SparkConnectClient
    ATTRIBUTE = "_execute_and_fetch_as_iterator"

    @property
    def applies(self) -> bool:
        return ArrowViewTypes.castable() and self.ATTRIBUTE in vars(self.TARGET)

    def _replacement(self, previous: object) -> object:
        return ViewCastingFetch(cast(Fetch, previous))
