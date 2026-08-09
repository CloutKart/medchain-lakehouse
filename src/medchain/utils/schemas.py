"""Build Spark schemas from the ``conf/sources.yaml`` data contract.

Bronze ingestion is schema-on-read but never schema-*inferred*. Inference reads a
sample of each file and guesses; two batches of the same source can therefore land
with different types (an all-integer column one day, one with a null the next),
which silently corrupts the archive. Declaring types once here makes Bronze
reproducible and makes a contract change a visible diff.
"""

from __future__ import annotations

import re
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.IGNORECASE)

_SIMPLE_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}


def parse_type(spec: str) -> DataType:
    """Convert a YAML type string such as ``decimal(18,2)`` into a Spark type."""
    spec = spec.strip()
    match = _DECIMAL.match(spec)
    if match:
        return DecimalType(int(match.group(1)), int(match.group(2)))
    try:
        return _SIMPLE_TYPES[spec.lower()]
    except KeyError:
        raise ValueError(
            f"Unsupported type {spec!r} in sources.yaml. "
            f"Supported: {sorted(_SIMPLE_TYPES)} plus decimal(p,s)."
        ) from None


def source_schema(source_cfg: dict[str, Any], *, all_strings: bool = False) -> StructType:
    """Spark schema for one source's declared columns.

    ``all_strings`` reads every column as a string. Bronze uses this so that a
    malformed value (a date typed as ``31/02/2024``, an amount with a stray comma)
    is preserved verbatim rather than nulled out by the reader. Typing happens in
    Silver, where a failed cast can be quarantined with its original value intact.
    """
    columns = source_cfg.get("columns") or {}
    fields = [
        StructField(
            name,
            StringType() if all_strings else parse_type(spec["type"]),
            nullable=bool(spec.get("nullable", True)),
        )
        for name, spec in columns.items()
    ]
    if not fields:
        raise ValueError("Source config declares no columns")
    return StructType(fields)


def column_names(source_cfg: dict[str, Any]) -> list[str]:
    return list((source_cfg.get("columns") or {}).keys())


def typed_columns(source_cfg: dict[str, Any]) -> dict[str, DataType]:
    """Mapping of column name to its *declared* (post-Silver) Spark type."""
    return {
        name: parse_type(spec["type"]) for name, spec in (source_cfg.get("columns") or {}).items()
    }
