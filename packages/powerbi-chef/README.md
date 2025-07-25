# PowerBI Chef

A package to assist with the local development of PowerBI reports. It provides utilities to standardize and automate common PowerBI development tasks.

## Installation

In order to use the package, you need to have a poetry environment up and running. You can do this by running the following commands:

```bash
cd packages/powerbi-chef
poetry shell
poetry install
```

## Features

### Update Opening Page

Ensures consistency across PowerBI reports by setting the active page to always be the first page in the page order:

```bash
cd projects/powerbi  # Navigate to your PowerBI directory
powerbi-chef fix-opening-page
```

This command:
- Searches for all `pages.json` files in your PowerBI directory
- Updates the `activePageName` in each file to match the first page in the `pageOrder` array
- Displays a summary of changes made

## Development

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
