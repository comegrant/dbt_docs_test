import asyncio
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from aligned import FeatureStore
from aligned.data_source.batch_data_source import BatchDataSource
from aligned.feature_store import FeatureLocation
from aligned.schemas.feature import Feature
from aligned.schemas.feature_view import CompiledFeatureView
from aligned.schemas.model import Model


@dataclass
class MermadeGraph:
    nodes: list[str]
    edges: list[str]

    def markdown(self, layout: str = "TB") -> str:
        format_nodes = "\n    ".join(self.nodes)
        format_edge = "\n    ".join(self.edges)
        return f"""```mermaid
%%{{init: {{"flowchart": {{"defaultRenderer": "elk"}}}} }}%%
flowchart {layout}
    {format_nodes}

    {format_edge}
```
        """


def mermade_node(location: FeatureLocation) -> str:
    if location.location == "model":
        return f"{location.location}_{location.name}[\\{location.name}/]"
    else:
        return f"{location.location}_{location.name}({location.name})"


def mermade_subgraph_node(location: FeatureLocation, features: list[str]) -> str:
    feature_nodes = "\n    ".join(
        [f"{location.location}_{location.name}.{feat}({feat})" for feat in features]
    )

    return f"""
subgraph {location.location}_{location.name}
{feature_nodes}
end
    """


def mermade_edge(from_node: FeatureLocation, to_node: FeatureLocation) -> str:
    return f"{mermade_node(from_node)} --> {mermade_node(to_node)}"


def mermade_graph(
    from_locations: dict[FeatureLocation, list[str]], to_model: Model
) -> MermadeGraph:
    model_output = to_model.predictions_view

    model_location = FeatureLocation.model(to_model.name)
    nodes_collection = from_locations.copy()
    nodes_collection[model_location] = [
        feat.name for feat in model_output.features.union(model_output.derived_features)
    ]

    model_function = FeatureLocation.model("model")

    nodes = [
        mermade_subgraph_node(location, nodes_collection[location])
        for location in nodes_collection.keys()
    ]
    nodes.append(mermade_node(model_function))

    edges = [
        mermade_edge(location, model_function) for location in from_locations.keys()
    ]
    edges.append(mermade_edge(model_function, model_location))

    pred_view = to_model.predictions_view
    if pred_view.source:
        source_node = mermade_source_node(pred_view.source, model_location)
        nodes.append(source_node)
        edges.append(f"{mermade_node(model_location)} -->|Stored at| {source_node}")

    if pred_view.application_source:
        source_node = mermade_source_node(pred_view.application_source, model_location)
        nodes.append(source_node)
        edges.append(f"{mermade_node(model_location)} -->|Stored at| {source_node}")

    return MermadeGraph(
        nodes=nodes,
        edges=edges,
    )


def data_source_markdown(source: BatchDataSource) -> str:
    from cheffelo_personalization.sql_server import MarkdownDescribable

    if isinstance(source, MarkdownDescribable):
        return source.as_markdown()
    else:
        return f"Type: *{source.type_name}*"


def feature_markdown(feature: Feature) -> str:
    return f"""<details>
<summary><b>{feature.name}</b></summary>

Data Type: **`{feature.dtype.name}`**

Description: *{feature.description}*

Constraints: *{feature.constraints}*

</details>"""


def mermade_source_node(source: BatchDataSource, location: FeatureLocation) -> str:
    return f"{location.location}_{source.type_name}[({source.type_name})]"


def feature_view_readme(view: CompiledFeatureView) -> str:
    location = FeatureLocation.feature_view(view.name)
    entity_md = "\n".join([f"- {ent.name}" for ent in view.entities])

    features_md = "\n".join([feature_markdown(feat) for feat in view.features])

    derived_md = "\n".join([feature_markdown(feat) for feat in view.derived_features])

    nodes: set[str] = set()
    edges: set[str] = set()

    source_node = mermade_source_node(view.source, location)
    nodes.add(source_node)

    nodes.add(mermade_node(location))
    edges.add(f"{source_node} --> {location.name}")

    if view.materialized_source:
        mat_source = mermade_source_node(view.materialized_source, location)
        nodes.add(mat_source)
        edges.add(f"{location.name} --> {mat_source}")

        source_node = mat_source

    graph = MermadeGraph(nodes=list(nodes), edges=list(edges))

    source_md = f"""## Source
This section describes the different sources used.

### Data Flow

Below is a chart that shows how the data flows.
If you are not able to display it here. Open [Mermaid.live](https://mermaid.live/edit), and past in the graph.
{graph.markdown("LR")}

### Main Source
This is the source of truth for our features, but can sometimes be slow to access.

{data_source_markdown(view.source)}"""

    if view.materialized_source:
        source_md += f"""

### Materialized Source
This is the default source to use, as it is intended to be a fast access data source for large data volumes.

{data_source_markdown(view.materialized_source)}"""

    return f"""# {view.name}

## Description
{view.description or "No description is define, concider adding one in `feature_view`."}

## Data Content

This section describes which data is expected to exist in the data source.

### Entities
{entity_md}

### Features
{features_md}

### Transformed Features
{derived_md}

{source_md}
"""


def model_readme(model: Model, load_feature_entities: set[Feature]) -> str:
    grouped_input_features: dict[FeatureLocation, list[str]] = defaultdict(list)

    for feature in model.features:
        grouped_input_features[feature.location].append(feature.name)

    input_features_markdown = "\n".join([f" - {feat.name}" for feat in model.features])

    graph = mermade_graph(grouped_input_features, model)

    output_view = model.predictions_view

    entities_markdown = "\n".join(
        [
            f"""<details>
<summary><b>{ent.name}</b></summary>

Data Type: **`{ent.dtype.name}`**

</details>"""
            for ent in output_view.entities
        ]
    )
    output_features_md = "\n".join(
        [
            feature_markdown(feat)
            for feat in output_view.features.union(output_view.derived_features)
        ]
    )

    entity_input_example = "\n    ".join(
        [f'"{ent.name}": [...]' for ent in load_feature_entities]
    )

    entity_output_example = "\n    ".join(
        [f'"{ent.name}": [...]' for ent in output_view.entities]
    )

    contacts_md = ""

    if model.contacts:
        contacts_md = "\n## Contacts / Owners\n"
        contacts_md += "\n".join(model.contacts)

    stored_at_md = ""
    if output_view.source:
        stored_at_md = f"""### Stored at

The features are stored in the following data source.

{data_source_markdown(output_view.source)}
"""
        if output_view.application_source:
            stored_at_md += f"""
#### Application Source

The application source often contains the predictions used by our applications. Therefore, often no historical data.
For our `{model.name}` model contains an application source at.

{data_source_markdown(output_view.application_source)}"""

    return f"""# {model.name}
{contacts_md}

## Description
{model.description or "No description given yet. Consider adding it in the model_contract."}

## Output

The model will output the following data.

### Features

{output_features_md}

### Entities

To fetch a prediction, specify the following information.

{entities_markdown}

## Input Features
{input_features_markdown}

Below is a chart that shows how the data flows.
If you are not able to display it here. Open [Mermaid.live](https://mermaid.live/edit), and past in the graph.
{graph.markdown()}

### Load features

To run the model, you will need some features. Therefore, the following code shows how to fetch them.

```python
from aligned import FeatureStore

store = await FeatureStore.from_dir(".")

entities = {{
    {entity_input_example}
}}

features = await store.model("{model.name}")\\
    .features_for(entities)\\
    .to_pandas()

```

### Load predictions

{stored_at_md}

**All Predictions**

```python
from aligned import FeatureStore

store = await FeatureStore.from_dir(".")

predictions = await store.model("{model.name}")\\
    .all_predictions()\\
    .to_pandas()
```

**Specific Prediction**

You can select specific predictions by filling in the values in the `entities` variable.

```python
from aligned import FeatureStore

store = await FeatureStore.from_dir(".")

entities = {{
    {entity_output_example}
}}

predictions = await store.model("{model.name}")\\
    .predictions_for(entities)\\
    .to_pandas()
```

"""


def generate_model_read_me(store: FeatureStore) -> dict[str, str]:
    from aligned.feature_store import RawStringFeatureRequest

    readmes: dict[str, str] = {}

    for model_name in store.models.keys():
        model = store.models[model_name]
        raw_feature_request = RawStringFeatureRequest(
            {feat_ref.identifier for feat_ref in model.features}
        )
        request = store.requests_for(raw_feature_request)
        readmes[model_name] = model_readme(model, request.request_result.entities)

    return readmes


def generate_feature_view_read_me(store: FeatureStore) -> dict[str, str]:
    readmes: dict[str, str] = {}

    for view_name in store.feature_views.keys():
        view = store.feature_views[view_name]
        readmes[view_name] = feature_view_readme(view)

    return readmes


def view_overview_readme(store: FeatureStore, view_path: Path) -> str:
    views = list(store.feature_views.keys())
    view_md = "\n".join(
        [f"- [{view}](/{(view_path / view).as_posix()}/README.md)" for view in views]
    )

    return f"""# Views

This page shows the available views / dimensions.

{view_md}
"""


def model_overview_readme(store: FeatureStore, model_path: Path) -> str:
    models = list(store.models.keys())
    models_md = "\n".join(
        [
            f"- [{model}](/{(model_path / model).as_posix()}/README.md)"
            for model in models
        ]
    )

    nodes: set[str] = set()
    edges: list[str] = []

    added_nodes: set[FeatureLocation] = set()
    depends_on: set[FeatureLocation] = set()

    for model_name in models:
        location = FeatureLocation.model(model_name)
        added_nodes.add(location)
        nodes.add(mermade_node(location))

        model = store.model(model_name).model
        if model.predictions_view.source:
            pred_source = model.predictions_view.source
            mermade_source_node = (
                f"model_{pred_source.type_name}[({pred_source.type_name})]"
            )

            nodes.add(mermade_source_node)
            edges.append(f"{mermade_node(location)} --> {mermade_source_node}")

        if model.predictions_view.application_source:
            pred_source = model.predictions_view.application_source
            mermade_source_node = f"{pred_source.type_name}[({pred_source.type_name})]"

            nodes.add(mermade_source_node)
            edges.append(f"{mermade_node(location)} --> {mermade_source_node}")

        dependent_on = list(store.model(model_name).depends_on())
        depends_on.update(dependent_on)
        nodes.update([mermade_node(loc) for loc in dependent_on])

        mermade_edges = [mermade_edge(node, location) for node in dependent_on]
        edges.extend(mermade_edges)

    for location in depends_on - added_nodes:
        view = store.feature_view(location.name).view
        source = view.materialized_source or view.source
        mermade_source = f"view_{source.type_name}[({source.type_name})]"
        nodes.add(mermade_source)

        edges.append(f"view_{source.type_name} --> {mermade_node(location)}")

    graph = MermadeGraph(list(nodes), edges)

    return f"""# Models

This page shows the available models in this repo.

{models_md}

## Data Flow

Below is a chart that shows how the data flows.
If you are not able to display it here. Open [Mermaid.live](https://mermaid.live/edit), and past in the graph.
{graph.markdown()}"""


def write_readmes(write_dir: Path, store: FeatureStore) -> None:
    model_dir = write_dir / "models"
    model_dir.mkdir(exist_ok=True)
    model_overview = model_overview_readme(store, model_dir)
    (model_dir / "README.md").write_text(model_overview)

    readmes = generate_model_read_me(store)

    for model_name, readme in readmes.items():
        file = model_dir / model_name / "README.md"
        file.parent.mkdir(exist_ok=True)
        file.write_text(readme)

    view_dir = write_dir / "views"
    view_dir.mkdir(exist_ok=True)

    view_overview = view_overview_readme(store, view_dir)
    (view_dir / "README.md").write_text(view_overview)

    readmes = generate_feature_view_read_me(store)

    for view_name, readme in readmes.items():
        file = view_dir / view_name / "README.md"
        file.parent.mkdir(exist_ok=True)
        file.write_text(readme)

    overview = f"""# Overview

#{model_overview}

#{view_overview}"""

    (write_dir / "README.md").write_text(overview)


async def main():
    store = await FeatureStore.from_dir(".")
    write_dir = Path("./docs")

    write_dir.mkdir(exist_ok=True)
    write_readmes(write_dir, store)


if __name__ == "__main__":
    asyncio.run(main())
