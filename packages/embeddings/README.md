# Embeddings

A package to simplify the usage of embeddings

## Usage
Run the following command to install this package:

```bash
chef add embeddings --extras=openai
```

Then you can create embeddings with the following

```python
import polars as pl
from embeddings import OpenAI, Embedder

model: Embedder = OpenAI(api_key="...") # or define nothing as it will read the `OPENAI_KEY` env var.

input_df = pl.DataFrame({
    "text": ["Hello", "world"]
})
embedded_df = await model.embed(input_df, input_column="text")

print(embedded_df)

# This will output something roughly like this

# |-------------------------------------------------------|
# | text  | embedded_text   | embedded_text_model_version |
# |-------------------------------------------------------|
# | Hello | [0.003, -0.323] | text-embedding-3-small      |
# | world | [0.435, -0.124] | text-embedding-3-small      |
```
