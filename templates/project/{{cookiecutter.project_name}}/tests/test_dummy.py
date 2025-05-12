import pandas as pd
import pytest
from jobs.run import SampleTableArgs, main
from key_vault.interface import NoopVault
from model_registry import ModelMetadata
from model_registry.in_mem import InMemoryRegistry
from pytest_mock import MockFixture


@pytest.mark.asyncio
async def test_run_job(mocker: MockFixture) -> None:

    test_input = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [2, 3, 4],
    })

    run_url = "some-run"

    # Makes sure we never connect to Spark
    mocker.patch("jobs.run.training_dataset", return_value=(test_input, ["some_ref.column"]))
    mocker.patch("jobs.run.run_url", return_value=run_url)

    args = SampleTableArgs(
        table="mloutputs.attribute_scoring",
        username=None,
        environment="dev",
        model_name="some_model_name"
    )
    registry = InMemoryRegistry()

    await main(
        args=args,
        registry=registry,
        vault=NoopVault()
    )
    assert registry.model_name == args.model_name
    assert registry.model is not None
    assert registry.registry._signature is not None

    preds = registry.model.predict(test_input)
    assert preds.shape[0] == test_input.shape[0]

    assert registry.registry._metadata[ModelMetadata.training_run_url] == run_url
