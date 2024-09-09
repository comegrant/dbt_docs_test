# Cheffelo Logging

A package to simplify logging for local and production environments

## Usage
Run the following command to install this package:

```bash
chef add cheffelo_logging
```

### Push logs to DataDog

We often want to log to a service in order to debug our logs later on.
Therefore, we can log to DataDog by using the following source.

> [!NOTE]
> All loggers should be setup in a function, not on import!
>
> Not how to do it ❌
> ```python
> import logging
>
> logger = logging.getLogger(__name__)
>
> logger.basicConfig(logging.INFO)
> ```
>
> How to do it correctly ✅
> ```python
> import logging
>
> logger = logging.getLogger(__name__)
>
> def setup_logger():
>     logger.basicConfig(logging.INFO)
> ```

The code bellow showcases how to setup DataDog logging for a whole project.

```python
import logging
from cheffelo_logging import setup_datadog, DataDogConfig

def main():
    # The root logger, so we log everything to datadog
    logger = logging.getLogger('')

    # Missing args, will be loaded from environment variables
    config = DataDogConfig(
        datadog_service_name="preselector",
        datadog_tags="env:testing",
        datadog_app_key="app_key"
    )
    setup_datadog(logger, config)

    logger.info("Hello DataDog")

if __name__ == "__main__":
    main()
```

## Develop

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.


### Testing
Run the following command to test that the changes do not impact critical functionality:

```bash
chef test
```
This will spin up a Docker image and run your code in a similar environment as our production code.

### Linting
We have added linting which automatically enforces coding style guides.
You may as a result get some failing pipelines from this.

Run the following command to fix and reproduce the linting errors:

```bash
chef lint
```

You can also install `pre-commit` if you want linting to be applied on commit.
