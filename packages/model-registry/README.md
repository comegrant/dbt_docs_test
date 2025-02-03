# Model Registry

A package to make it easier to integrate and test model registries

## Usage
Run the following command to install this package:

```bash
chef add model-registry
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
