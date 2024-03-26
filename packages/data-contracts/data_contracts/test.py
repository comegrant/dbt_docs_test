from aligned import FileSource, feature_view

from data_contracts.sources import adb


@feature_view(
    name="some_view",
    source=adb.table("some_table"),
    materialized_source=FileSource.delta_at("some_dir.delta"),
)
class SomeViewBronze:
    pass
