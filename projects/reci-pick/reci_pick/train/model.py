import logging

import numpy as np
import pandas as pd
import tensorflow as tf
from reci_pick.helpers import get_dict_values_as_array


def train_nn_model(
    user_embeddings_input: np.array,
    recipe_embeddings_input: np.array,
    target: np.array,
) -> tuple[tf.keras.Model, float, float]:
    embedding_dim = user_embeddings_input.shape[1]
    model = create_model(input_embedding_dim=embedding_dim)
    early_stopping = tf.keras.callbacks.EarlyStopping(monitor="val_loss", patience=5, restore_best_weights=True)
    history = model.fit(
        x={
            "user_profile": user_embeddings_input,
            "recipe_profile": recipe_embeddings_input,
        },
        y=target,
        batch_size=256,
        epochs=100,
        validation_split=0.2,
        callbacks=[early_stopping],
    )

    # Get the last epoch's loss and accuracy
    last_loss = history.history["loss"][-1]
    last_accuracy = history.history["accuracy"][-1]

    return model, last_loss, last_accuracy


def create_model(
    input_embedding_dim: int,
) -> tf.keras.Model:
    """
    A two tower architecture processed by fully connected layers
    """
    user_input = tf.keras.layers.Input(shape=(input_embedding_dim,), name="user_profile")
    recipe_input = tf.keras.layers.Input(shape=(input_embedding_dim,), name="recipe_profile")
    #  Add processing layers
    user_processed = tf.keras.layers.Dense(128, activation="relu")(user_input)
    recipe_processed = tf.keras.layers.Dense(128, activation="relu")(recipe_input)

    user_processed2 = tf.keras.layers.Dense(64, activation="relu")(user_processed)
    recipe_processed2 = tf.keras.layers.Dense(64, activation="relu")(recipe_processed)

    # dot product
    dot_product = tf.keras.layers.Dot(axes=1)([user_processed2, recipe_processed2])
    output_layer = tf.keras.layers.Dense(1, activation="sigmoid")(dot_product)

    model = tf.keras.Model(inputs=[user_input, recipe_input], outputs=output_layer)

    model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
    return model


def predict_recipe_scores(
    recipe_ids_to_predict: list[int],
    user_billing_agreements: list[int],
    user_embeddings_pooled_dict: dict,
    id_to_embedding_lookup: dict,
    model: tf.keras.Model,
) -> pd.DataFrame:
    # Remove the recipes that we cannot predict
    mask = np.array([i in id_to_embedding_lookup for i in recipe_ids_to_predict])
    if (1 - mask).sum() > 0:
        logging.warning("Some recipes to be predicted are not mapped to an embedding. Skipping them.")
        recipe_ids_to_predict = recipe_ids_to_predict[mask]

    recipe_inputs = get_dict_values_as_array(look_up_dict=id_to_embedding_lookup, key_list=recipe_ids_to_predict)

    score_dict = {}
    for billing_agreement in user_billing_agreements:
        # We need to repeat the user profile n times
        user_inputs = np.tile(user_embeddings_pooled_dict[billing_agreement], (len(recipe_ids_to_predict), 1))
        scores = model.predict(x={"user_profile": user_inputs, "recipe_profile": recipe_inputs})
        score_dict[billing_agreement] = scores.flatten()

    score_df = pd.DataFrame(list(score_dict.items()), columns=["billing_agreement_id", "score"])
    score_df["main_recipe_id"] = [recipe_ids_to_predict] * len(score_df)
    score_df_exploded = score_df.explode(column=["main_recipe_id"])
    score_df_exploded["score"] = score_df.explode(column=["score"])["score"]
    return score_df_exploded
