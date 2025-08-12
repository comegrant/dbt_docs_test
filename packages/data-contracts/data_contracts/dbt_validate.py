import asyncio
import json
import logging
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from aligned import ContractStore
from aligned.compiler.feature_factory import FeatureFactory
from aligned.schemas.feature import Feature, FeatureType
from aligned.schemas.feature_view import CompiledFeatureView
from aligned.sources.databricks import UCSqlSource, UCTableSource
from aligned.sources.renamer import Renamer
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ManifestNode, ModelNode
from pydantic import BaseModel, Field
from pyspark.sql.types import CharType, DecimalType, VarcharType

from data_contracts.tags import Tags

logger = logging.getLogger(__name__)


DBTModels = dict[str, dict[str, ColumnInfo]]


class Issue(Protocol):
    def describe(self) -> str: ...


@dataclass
class MissingModel:
    contract: CompiledFeatureView
    expected_table: str
    schema_mapping: dict[str, str] = field(default_factory=lambda: {"bronze": "silver"})

    def describe(self) -> str:
        message = f"*'{self.contract.name}'* uses **'{self.expected_table}'**"

        if self.contract.tags:
            message += "\nTags: " + ", ".join([f"`{tag}`" for tag in self.contract.tags])

        action = f"Make sure that '{self.expected_table}' exists and that it is not an ephemeral model."

        for from_schema, to_schema in self.schema_mapping.items():
            if self.expected_table.startswith(f"{from_schema}."):
                stripped = self.expected_table.lstrip(from_schema)
                new_model = f"{to_schema}{stripped}"
                action = f"Consider changing to *'{new_model}'*."

        message += f"\n\n> [!TIP]\n> {action}"

        return message


@dataclass
class MissingColumn:
    column_name: str
    data_type: str | None


@dataclass
class MismatchingDataType:
    column_name: str
    expected_data_type: str
    current_data_type: str


@dataclass
class BrokenContract:
    dbt_model: str
    contract: CompiledFeatureView
    missing_columns: list[MissingColumn]
    mismatching_data_types: list[MismatchingDataType]

    def describe(self) -> str:
        message = f"**'{self.contract.name}'** is not fulfilled.\n\n"

        if self.contract.tags:
            message += "\nTags: " + ", ".join([f"`{tag}`" for tag in self.contract.tags])

        if self.contract.contacts:
            contacts = "\n- ".join([contact.name for contact in self.contract.contacts])

            message += f"Contacts:\n- {contacts}\n\n"

        if self.missing_columns:
            message += "Missing columns.\n"

            for col in self.missing_columns:
                if col.data_type:
                    message += f"- '{col.column_name}' of type '{col.data_type}'\n"
                else:
                    message += f"- '{col.column_name}'\n"

            message += (
                f"\n> [!TIP]\n> Add the columns to the *'{self.dbt_model}'* model, or stop using the columns in Python."
            )

        if self.mismatching_data_types:
            message += "Conflicting data types.\n"
            message += "\n".join(
                [
                    f"- '{col.column_name}' expected data type '{col.expected_data_type}'"
                    f" but got '{col.current_data_type}'"
                    for col in self.mismatching_data_types
                ]
            )

        return message


@dataclass
class Conflicts:
    missing_models: list[MissingModel]
    broken_contracts: list[BrokenContract]

    def describe(self) -> str:
        if not self.missing_models and not self.broken_contracts:
            return ""

        message = "Found some conflicts between our dbt setup and Python data contracts.\n\n"

        if self.missing_models:
            message += f"⁉️  {len(self.missing_models)} dbt models are missing.\n\n"

            message += "".join([f"{miss.describe()}\n---\n\n" for miss in self.missing_models])
            message += "\n\n"

        if self.broken_contracts:
            message += f"🤝❗ Found {len(self.broken_contracts)} broken contracts between dbt and Python.\n\n"

            message += "".join([f"{miss.describe()}\n---\n\n" for miss in self.broken_contracts])
        return message


def read_manifest_from(path: str | Path) -> Manifest:
    if not isinstance(path, Path):
        path = Path(path)

    return Manifest.from_dict(json.loads(path.read_text()))


def columns_per_table(nodes: Iterable[ManifestNode], prefix: str = "~matsei_") -> DBTModels:
    return {  # type: ignore
        f"{node.schema.removeprefix(prefix)}.{node.identifier}": node.columns
        for node in nodes
        if isinstance(node, ModelNode) and not node.is_ephemeral_model
    }


def feature_type_from(data_type: str) -> FeatureType:
    from aligned.sources.databricks import convert_pyspark_type
    from pyspark.sql.types import (
        BooleanType,
        ByteType,
        DataType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        TimestampNTZType,
        TimestampType,
    )

    if data_type == "array":
        data_type = "array<string>"

    if data_type.startswith("array"):
        rest = data_type.removeprefix("array")

        if rest:
            return FeatureType.array(feature_type_from(rest[1:-1]))
        else:
            return FeatureType.array()

    if data_type.startswith("struct"):
        struct_content = data_type.removeprefix("struct")[1:-1]

        raw_fields = [prop.split(":") for prop in struct_content.split(",")]
        fields = {vals[0]: feature_type_from(vals[1]) for vals in raw_fields}
        return FeatureType.struct(fields)

    if data_type.startswith("map"):
        return FeatureType.json()

    if data_type.startswith("varchar"):
        args = data_type.removeprefix("varchar")
        spark_type = VarcharType(int(args[1:-1]))
    elif data_type.startswith("char"):
        args = data_type.removeprefix("char")
        spark_type = CharType(int(args[1:-1]))
    elif data_type.startswith("decimal"):
        args = data_type.removeprefix("decimal")
        int_args = [int(val) for val in args[1:-1].split(",") if val]
        spark_type = DecimalType(*int_args)
    else:
        data_type = data_type.replace(" - not_null", "")

        simple_types: list[DataType] = [
            BooleanType(),
            ByteType(),
            DateType(),
            DoubleType(),
            FloatType(),
            IntegerType(),
            LongType(),
            ShortType(),
            StringType(),
            TimestampType(),
            TimestampNTZType(),
        ]
        simple_map = {dtype.simpleString(): dtype for dtype in simple_types}
        spark_type = simple_map[data_type]
    return convert_pyspark_type(spark_type).dtype


def issues_for_sql(
    source: UCSqlSource, view: CompiledFeatureView, dbt_models: DBTModels
) -> list[MissingModel | BrokenContract] | None:
    logger.info(f"Found source. Checking for {view.name}")
    column_selects = selected_columns_in(source.query)

    issues: list[MissingModel | BrokenContract] = []

    for identifier in column_selects:
        if identifier not in dbt_models:
            issues.append(MissingModel(view, identifier))
            continue

        dbt_model = dbt_models[identifier]
        selects = column_selects[identifier]

        if not dbt_model:
            continue

        missing_columns: list[MissingColumn] = []

        for col in selects:
            if col not in dbt_model:
                missing_columns.append(MissingColumn(col, data_type=None))

        if missing_columns:
            issues.append(
                BrokenContract(
                    dbt_model=identifier, contract=view, missing_columns=missing_columns, mismatching_data_types=[]
                )
            )

    if issues:
        return issues
    else:
        return None


def issues_for_table(
    source: UCTableSource, view: CompiledFeatureView, dbt_models: DBTModels
) -> MissingModel | BrokenContract | None:
    logger.info(f"Found source. Checking for {view.name}")

    needed_columns = view.features.union(view.entities)
    if view.event_timestamp:
        needed_columns.add(view.event_timestamp.as_feature())

    identifier = f"{source.table.schema.read()}.{source.table.table.read()}"

    if identifier not in dbt_models:
        logger.info(f"Unable to find identifier {identifier}")

        if Tags.is_dbt_model in (view.tags or []):
            return MissingModel(view, identifier)
        else:
            return None

    dbt_columns = dbt_models[identifier]

    missing_columns: list[MissingColumn] = []
    mismatching_types: list[MismatchingDataType] = []

    inverse_renamer = Renamer.noop()
    if source.renamer:
        inverse_renamer = source.renamer.inverse()

    for feat in needed_columns:
        source_name = inverse_renamer.rename(feat.name)

        if source_name not in dbt_columns:
            missing_columns.append(MissingColumn(source_name, feat.dtype.name))
            continue

        column = dbt_columns[source_name]
        if not column.data_type:
            continue

        if column.data_type == "int" and feat.dtype.name.startswith("int"):
            # ignore this case, as we do not have the granularity in dbt right now
            continue

        stored_dtype = feature_type_from(column.data_type)

        if stored_dtype != feat.dtype and not (
            feat.dtype.name.startswith("float") and stored_dtype.name.startswith("float")
        ):
            mismatching_types.append(MismatchingDataType(source_name, feat.dtype.name, stored_dtype.name))

    if missing_columns or mismatching_types:
        return BrokenContract(
            identifier, view, missing_columns=missing_columns, mismatching_data_types=mismatching_types
        )
    else:
        return None


def issues_for_contract(view: CompiledFeatureView, dbt_models: DBTModels) -> list[MissingModel | BrokenContract] | None:
    source = view.source

    if isinstance(source, UCTableSource):
        issue = issues_for_table(source, view, dbt_models)
        if issue:
            return [issue]
    elif isinstance(source, UCSqlSource):
        return issues_for_sql(source, view, dbt_models)

    return None


def contract_from_spark_sql(query: str, file_name: str, directory: Path) -> CompiledFeatureView:
    from sqlglot import exp, parse_one

    from data_contracts.sources import databricks_config

    parsed = parse_one(query, read="spark")

    select = parsed.find(exp.Select)
    assert select

    selected_names = select.named_selects

    return CompiledFeatureView(
        name=file_name,
        source=databricks_config.sql(query),
        tags=[directory.as_posix()],
        entities=set(),
        features={Feature(name=name, dtype=FeatureType.string()) for name in selected_names},
        derived_features=set(),
    )


def conflicts_for(store: ContractStore, dbt_models: DBTModels) -> Conflicts | None:
    conflicts = Conflicts([], [])

    all_contracts = list(store.feature_views.values())
    all_contracts.extend([model.predictions_view.as_view(model.name) for model in store.models.values()])

    for view in all_contracts:
        if view.tags and Tags.skip_dbt_check in view.tags:
            continue
        issues = issues_for_contract(view, dbt_models)

        conflicts.missing_models.extend(issue for issue in issues or [] if isinstance(issue, MissingModel))
        conflicts.broken_contracts.extend(issue for issue in issues or [] if isinstance(issue, BrokenContract))

    if conflicts.broken_contracts or conflicts.missing_models:
        return conflicts
    else:
        return None


def view_for_model(model: ModelNode) -> CompiledFeatureView:
    from data_contracts.sources import databricks_catalog

    def feature_for(column: ColumnInfo) -> Feature:
        return Feature(
            column.name,
            dtype=feature_type_from(column.data_type) if column.data_type else FeatureType.string(),
            tags=column.tags,
            description=column.description,
        )

    schema = model.config.schema or model.schema

    return CompiledFeatureView(
        name=model.name,
        source=databricks_catalog.schema(schema).table(model.identifier),
        entities=set(),
        features={feature_for(col) for col in model.columns.values()},
        derived_features=set(),
        tags=[*model.tags, "auto-generated"],
        description=model.description,
    )


def code_for_view(view: CompiledFeatureView) -> str:
    from data_contracts.helper import snake_to_pascal

    source = view.source
    assert isinstance(source, UCTableSource)

    table_config = source.table

    def feature_code_for(fact: FeatureFactory) -> str:
        code = f"{fact._name} = {fact.__class__.__name__}()"

        if fact.tags:
            for tag in fact.tags:
                code += f'.with_tag("{tag}")'

        return code

    feature_facts: list[FeatureFactory] = []

    for feature in sorted(view.features, key=lambda feat: feat.name):
        fact = feature.dtype.feature_factory
        fact._name = feature.name

        if feature.tags:
            fact.tags = set(feature.tags)

        if feature.description:
            fact = fact.description(feature.description)

        feature_facts.append(fact)

    feature_code = "\n    ".join(feature_code_for(fact) for fact in feature_facts)

    all_feature_types = {feat.__class__.__name__ for feat in feature_facts}
    all_feature_types.add("feature_view")

    all_feature_imports = ", ".join(sorted(all_feature_types))

    aligned_import = f"from aligned import {all_feature_imports}"

    return f"""{aligned_import}
from data_contracts.sources import databricks_catalog


@feature_view(
    source=databricks_catalog.schema("{table_config.schema.read()}").table("{table_config.table.read()}"),
    tags={view.tags}
)
class {snake_to_pascal(view.name)}:
    \"\"\"{view.description}\"\"\"

    {feature_code}
"""


def dbt_sources_for_views(
    views: list[CompiledFeatureView],
    schema: str,
) -> dict:
    return {"sources": [{"name": schema, "schema": schema, "tables": [dbt_table_for_view(view) for view in views]}]}


def dbt_table_for_view(view: CompiledFeatureView) -> dict:
    source = view.materialized_source or view.source

    if not isinstance(source, UCTableSource):
        raise ValueError(f"Expected unity catalog source, but got {type(source)}.")

    all_features = view.features.union(view.entities)
    if view.event_timestamp:
        all_features.add(view.event_timestamp.as_feature())

    description = view.description or ""
    description += f"\nAuto-generated model at {datetime.now(timezone.utc).isoformat()}"

    source = {
        "name": view.name,
        "identifier": source.table.table.read(),
        "description": description,
        "columns": [
            {"name": feat.name, "description": feat.description, "data_type": feat.dtype.spark_type.simpleString()}
            for feat in all_features
        ],
        "tags": ["auto-generated", "aligned"],
    }

    if view.event_timestamp:
        source["loaded_at_field"] = view.event_timestamp.name

        freshness_config = {}

        if view.acceptable_freshness:
            freshness_config["warn_after"] = {
                "count": int(view.acceptable_freshness.total_seconds() / 60),
                "period": "minute",
            }

        if view.unacceptable_freshness:
            freshness_config["error_after"] = {
                "count": int(view.unacceptable_freshness.total_seconds() / 60),
                "period": "minute",
            }

        if freshness_config:
            source["freshness"] = freshness_config

    metadata = {}

    if view.contacts:
        metadata["owner"] = ", ".join([contact.name for contact in view.contacts])

    if metadata:
        source["meta"] = metadata

    return source


def dbt_model_for_view(view: CompiledFeatureView) -> dict:
    all_features = view.features.union(view.entities)
    if view.event_timestamp:
        all_features.add(view.event_timestamp.as_feature())

    description = view.description or ""
    description += f"\nAuto-generated model at {datetime.now(timezone.utc).isoformat()}"

    return {
        "name": view.name,
        "description": view.description,
        "columns": [
            {"name": feat.name, "description": feat.description, "data_type": feat.dtype.spark_type.simpleString()}
            for feat in all_features
        ],
        "config": {"tags": ["auto-generated", "aligned"]},
    }


def selected_columns_in(query: str) -> dict[str, list[str]]:
    from sqlglot import exp, parse_one

    tree = parse_one(query, read="spark")

    ctes = tree.find_all(exp.CTE)
    tables = tree.find_all(exp.Table)

    cte_names = [cte.alias_or_name for cte in ctes]
    table_selects = [(table, table.parent_select) for table in tables if table.name not in cte_names]
    needed_columns: dict[str, list[str]] = {}

    for table, table_select in table_selects:
        assert table_select
        table_name = f"{table.db}.{table.name}"

        tables_in_select = list(table_select.find_all(exp.Table))

        columns: list[exp.Column] = list(table_select.find_all(exp.Column))
        filter_exp = table_select.find(exp.Where)
        if filter_exp:
            columns.extend(filter_exp.find_all(exp.Column))

        if len(tables_in_select) > 1:
            needed_columns[table_name] = [col.name for col in columns if col.table == table.alias_or_name]
        else:
            needed_columns[table_name] = [col.name for col in columns]

    return needed_columns


class ValidateArgs(BaseModel):
    manifest_file: Path = Field(
        default_factory=lambda: Path.cwd().parent.parent / "projects/data-model/transform/target/manifest.json"
    )
    sql_dirs: list[Path] | None = Field(None)
    output_file: Path | None = Field(None)


def sql_contracts_from_directory(directory: Path) -> ContractStore:
    from sqlglot.errors import ParseError, TokenError

    abs_path = directory.absolute()
    sql_files = abs_path.glob("**/*.sql")

    all_contracts = ContractStore.empty()

    for file in sql_files:
        sql_query = (
            file.read_text()
            .replace("{env}.", "")
            .replace("'{", "'")
            .replace("}'", "'")
            .replace("{", "'")
            .replace("}", "'")
        )
        if not sql_query:
            continue

        with suppress(ParseError, TokenError):
            sql_contract = contract_from_spark_sql(sql_query, file.relative_to(abs_path).as_posix(), directory)
            all_contracts.add_compiled_view(sql_contract)

    return all_contracts


def generate_contracts_from_dbt(manifest_content: Manifest, schema: str = "gold") -> None:
    generate_path = Path("data_contracts/dbt")
    generate_path.mkdir(exist_ok=True)

    for model in manifest_content.nodes.values():
        if not isinstance(model, ModelNode):
            continue

        if model.config.schema != schema:
            continue

        dir_path = generate_path / schema

        dir_path.mkdir(exist_ok=True)
        view = view_for_model(model)

        if not view.features:
            continue

        code = code_for_view(view)
        file_path = dir_path / f"{view.name}.py"
        file_path.write_text(code)


async def validate(args: ValidateArgs) -> None:
    """
    Validates that the different contracts is compatible with the dbt models
    """
    manifest_content = read_manifest_from(args.manifest_file)
    models = columns_per_table(manifest_content.nodes.values())

    contracts = await ContractStore.from_dir(".", exclude_glob=["tests/*.py", "tests/**/*.py"])

    if args.sql_dirs:
        for sql_dir in args.sql_dirs:
            print(sql_dir.absolute())  # noqa
            contracts = contracts.combined_with(sql_contracts_from_directory(sql_dir))

    issues = conflicts_for(contracts, models)

    if issues:
        if args.output_file:
            args.output_file.write_text(issues.describe())
        else:
            print(issues.describe())  # noqa
    else:
        print("Found no issues")  # noqa


if __name__ == "__main__":
    from pydantic_argparser import parse_args

    asyncio.run(validate(parse_args(ValidateArgs)))
