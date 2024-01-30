from aligned import Bool, Float, Int32, String, feature_view
from aligned.compiler.feature_factory import List

from data_contracts.azure_blob import AzureBlobConfig

azure_dl_creds = AzureBlobConfig(
    account_name_env="DATALAKE_SERVICE_ACCOUNT_NAME",
    account_id_env="DATALAKE_STORAGE_ACCOUNT_KEY",
    tenent_id_env="AZURE_TENANT_ID",
    client_id_env="DATALAKE_SERVICE_PRINCIPAL_CLIENT_ID",
    client_secret_env="DATALAKE_SERVICE_PRINCIPAL_CLIENT_SECRET",
)
embedding_dir = azure_dl_creds.directory("data-science/recipe_text_embedding/latest")


@feature_view(
    name="recipe_embedding",
    source=embedding_dir.parquet_at("recipe_text_GL.parquet"),
)
class RecipeEmbedding:
    recipe_id = Int32().as_entity()

    company_id = String()
    is_active = Bool()
    recipe_name = String()
    text_embedding = List(Float())
