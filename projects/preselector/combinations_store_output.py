from aligned import UUID, Int32, List, String, Timestamp, feature_view
from aligned.schemas.date_formatter import DateFormatter
from data_contracts.sources import azure_dl_creds


# Define feature view that specifies folder and file storage details
@feature_view(
    source=azure_dl_creds.directory(  # Define the container and a subdir to write to
        "data-science/dev/combinations_app/",
    ).parquet_at(  # Define the name of the file
        "output_combinations.parquet", date_formatter=DateFormatter.iso_8601()
    ),
)
class CombinationsAppOutput:
    identifier = UUID().as_entity()
    session_info = UUID()
    time = Timestamp()
    company_id = UUID()
    year = Int32()
    week = Int32()
    recipes = List(sub_type=Int32())
    rating = String()
    attributes = List(sub_type=String())
    taste_preferences = List(sub_type=String())
    number_of_recipes = Int32()
    portion_size = Int32()
    comment = String().is_optional()
