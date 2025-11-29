"""Schema management for Iceberg tables."""

from __future__ import annotations

from typing import Any

import polars as pl
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
)


# Mapping from Polars dtypes to Iceberg types
POLARS_TO_ICEBERG: dict[type, type] = {
    pl.Int8: IntegerType,
    pl.Int16: IntegerType,
    pl.Int32: IntegerType,
    pl.Int64: LongType,
    pl.UInt8: IntegerType,
    pl.UInt16: IntegerType,
    pl.UInt32: LongType,
    pl.UInt64: LongType,
    pl.Float32: FloatType,
    pl.Float64: DoubleType,
    pl.Boolean: BooleanType,
    pl.String: StringType,
    pl.Utf8: StringType,
    pl.Binary: BinaryType,
    pl.Date: DateType,
    pl.Time: TimeType,
    pl.Datetime: TimestamptzType,
    pl.Duration: LongType,
}


def polars_dtype_to_iceberg(dtype: pl.DataType, field_id: int = 0) -> Any:
    """Convert a Polars dtype to an Iceberg type."""
    # Handle base types
    dtype_class = type(dtype)

    if dtype_class in POLARS_TO_ICEBERG:
        return POLARS_TO_ICEBERG[dtype_class]()

    # Handle Datetime with timezone
    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone:
            return TimestamptzType()
        return TimestampType()

    # Handle Decimal
    if isinstance(dtype, pl.Decimal):
        precision = dtype.precision or 38
        scale = dtype.scale or 0
        return DecimalType(precision, scale)

    # Handle List
    if isinstance(dtype, pl.List):
        inner_type = polars_dtype_to_iceberg(dtype.inner, field_id + 1)
        return ListType(
            element_id=field_id + 1,
            element=inner_type,
            element_required=False,
        )

    # Handle Struct
    if isinstance(dtype, pl.Struct):
        fields = []
        for i, field in enumerate(dtype.fields):
            iceberg_type = polars_dtype_to_iceberg(field.dtype, field_id + i + 1)
            fields.append(
                NestedField(
                    field_id=field_id + i + 1,
                    name=field.name,
                    field_type=iceberg_type,
                    required=False,
                )
            )
        return StructType(*fields)

    # Default to string for unknown types
    return StringType()


def schema_from_polars(
    df: pl.DataFrame,
    required_columns: list[str] | None = None,
) -> Schema:
    """
    Create an Iceberg Schema from a Polars DataFrame.

    Args:
        df: Polars DataFrame
        required_columns: List of column names that should be required (non-nullable)

    Returns:
        Iceberg Schema
    """
    required_columns = required_columns or []
    fields = []

    for i, (name, dtype) in enumerate(df.schema.items()):
        field_id = i + 1
        iceberg_type = polars_dtype_to_iceberg(dtype, field_id)
        is_required = name in required_columns

        fields.append(
            NestedField(
                field_id=field_id,
                name=name,
                field_type=iceberg_type,
                required=is_required,
            )
        )

    return Schema(*fields)


class SchemaBuilder:
    """Builder for creating Iceberg schemas programmatically."""

    def __init__(self) -> None:
        self._fields: list[NestedField] = []
        self._field_id = 0

    def _next_id(self) -> int:
        """Get the next field ID."""
        self._field_id += 1
        return self._field_id

    def add_integer(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add an integer field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=IntegerType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_long(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a long (bigint) field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=LongType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_float(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a float field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=FloatType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_double(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a double field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=DoubleType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_boolean(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a boolean field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=BooleanType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_string(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a string field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=StringType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_binary(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a binary field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=BinaryType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_date(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a date field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=DateType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_time(
        self,
        name: str,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a time field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=TimeType(),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_timestamp(
        self,
        name: str,
        with_timezone: bool = True,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a timestamp field."""
        ts_type = TimestamptzType() if with_timezone else TimestampType()
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=ts_type,
                required=required,
                doc=doc,
            )
        )
        return self

    def add_decimal(
        self,
        name: str,
        precision: int = 38,
        scale: int = 0,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a decimal field."""
        self._fields.append(
            NestedField(
                field_id=self._next_id(),
                name=name,
                field_type=DecimalType(precision, scale),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_list(
        self,
        name: str,
        element_type: Any,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a list field."""
        element_id = self._next_id()
        field_id = self._next_id()

        self._fields.append(
            NestedField(
                field_id=field_id,
                name=name,
                field_type=ListType(
                    element_id=element_id,
                    element=element_type,
                    element_required=False,
                ),
                required=required,
                doc=doc,
            )
        )
        return self

    def add_map(
        self,
        name: str,
        key_type: Any,
        value_type: Any,
        required: bool = False,
        doc: str | None = None,
    ) -> "SchemaBuilder":
        """Add a map field."""
        key_id = self._next_id()
        value_id = self._next_id()
        field_id = self._next_id()

        self._fields.append(
            NestedField(
                field_id=field_id,
                name=name,
                field_type=MapType(
                    key_id=key_id,
                    key_type=key_type,
                    value_id=value_id,
                    value_type=value_type,
                    value_required=False,
                ),
                required=required,
                doc=doc,
            )
        )
        return self

    def build(self) -> Schema:
        """Build the schema."""
        return Schema(*self._fields)


def merge_schemas(schema1: Schema, schema2: Schema) -> Schema:
    """
    Merge two schemas, keeping all fields from both.

    Fields from schema2 are added if they don't exist in schema1.
    """
    existing_names = {field.name for field in schema1.fields}

    new_fields = list(schema1.fields)
    max_id = max(field.field_id for field in schema1.fields)

    for field in schema2.fields:
        if field.name not in existing_names:
            max_id += 1
            new_fields.append(
                NestedField(
                    field_id=max_id,
                    name=field.name,
                    field_type=field.field_type,
                    required=field.required,
                    doc=field.doc,
                )
            )

    return Schema(*new_fields)
