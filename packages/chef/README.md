# Chef

A cli to simplify the development of Cheffelo projects and packages.

## Installation

Install the cli with `poetry install`.


## Usage

If you have activated the python venv in your shell, run `chef <command>`.

Otherwise try activate the shell with `poetry shell` or run `poetry run chef <command>`.

## Commands

Bellow contains the different commands available.

### New
Create a new project or package.

`chef new <project | package>`

This will start up an interactable wisard that fills in basic information.

### Add
Adds an internal or external package to the project.

`chef add lmkgroup-ds-utils`

If you define a package that matches one of the internal packages will the internal package be added.
Otherwise will it assume it is an external package, and add that one.

### List internal deps
Lists the internal dependencies that exists.

`chef list-internal-deps`

### Setup
Setup the dependencies and pre-commit hooks.

`chef setup`


### Install
Install the deps defined in the package or project.

`chef install`

### Pin
Pins an internal dependency to a spesified version.
If no versions are defined, will it select the current version on origin/main.

`chef pin lmkgroup-ds-utils`

### Expose
Exposes a port to a public url.
Can be used to showcase experimental projects to some other data chefs.

`chef expose <port>`

### Build
Builds a project into a Docker image. Will default to the name of the project.

`chef build`

### Run
Runs a command in a build Docker image.

`chef run echo "Hello World"`

### Bash
Opens up an interactable bash shell in a Docker image.

`chef bash`

### Test
Runs all tests in the current package or project

`chef test`

### Lint
Run a linter in the project.

`chef lint`

### Format
Runs a formatter on the codebase

`chef format`
