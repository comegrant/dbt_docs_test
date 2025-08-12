from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from aligned.sources.databricks import convert_pyspark_type

if TYPE_CHECKING:
    from pyspark.sql.types import StructField, StructType


@dataclass
class SchemaChange:
    adding: list[StructField]
    changes: dict[str, SchemaChange | StructField]
    deletes: list[StructField]

    @property
    def has_changes(self) -> bool:
        if self.adding or self.deletes:
            return True
        return bool(self.changes)

    def to_spark_sql_struct(self, column: str, table_identifier: str) -> str:
        raw_sql = ""

        alter_table = f"ALTER TABLE {table_identifier}"

        for add in self.adding:
            raw_sql += f"{alter_table} ADD COLUMN {column}.{add.name} {add.dataType.simpleString()};\n"

        for spark_field in self.changes.values():
            assert not isinstance(spark_field, SchemaChange)
            raw_sql += f"{alter_table} DROP COLUMN {column}.{spark_field.name};\n"
            raw_sql += f"{alter_table} ADD COLUMN {column}.{spark_field.name} {spark_field.dataType.simpleString()};\n"

        return raw_sql

    def to_spark_sql(self, table_identifier: str) -> str:
        raw_sql = ""
        alter_table = f"ALTER TABLE {table_identifier}"

        for add in self.adding:
            raw_sql += f"{alter_table} ADD COLUMN {add.name} {add.dataType.simpleString()};\n"

        for name, changes in self.changes.items():
            if isinstance(changes, SchemaChange):
                raw_sql += changes.to_spark_sql_struct(name, table_identifier)
            else:
                raw_sql += f"{alter_table} ALTER COLUMN {name} TYPE {changes.dataType.simpleString()};\n"

        return raw_sql


def pyspark_schema_changes(from_schema: StructType, to_schema: StructType) -> SchemaChange:
    from pyspark.sql.types import ArrayType, StructType

    from_map = {field.name: field for field in from_schema.fields}
    to_map = {field.name: field for field in to_schema.fields}

    new_fields: list[StructField] = []
    alter_field = {}
    deletes: list[StructField] = []

    for new_field in to_map.values():
        if new_field.name not in from_map:
            new_fields.append(new_field)
            continue

        existing_field = from_map[new_field.name]

        if isinstance(new_field.dataType, StructType) and isinstance(existing_field.dataType, StructType):
            changes = pyspark_schema_changes(existing_field.dataType, new_field.dataType)
            if changes.has_changes:
                alter_field[new_field.name] = changes

        elif isinstance(new_field.dataType, ArrayType) and isinstance(existing_field.dataType, ArrayType):
            new_sub_el = new_field.dataType.elementType
            old_sub_el = existing_field.dataType.elementType

            if isinstance(new_sub_el, StructType) and isinstance(old_sub_el, StructType):
                changes = pyspark_schema_changes(old_sub_el, new_sub_el)
                if changes.has_changes:
                    alter_field[new_field.name] = changes

            elif new_sub_el != old_sub_el:
                alter_field[new_field.name] = new_field

        elif new_field.dataType != existing_field.dataType:
            to_type = convert_pyspark_type(new_field.dataType)
            from_type = convert_pyspark_type(existing_field.dataType)

            if to_type.dtype.is_numeric and from_type.dtype.is_numeric:
                continue

            alter_field[new_field.name] = new_field

    for from_field in from_map.values():
        if from_field.name not in to_map:
            deletes.append(from_field)

    return SchemaChange(adding=new_fields, changes=alter_field, deletes=deletes)
