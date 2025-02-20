# Chef

A cli to simplify the development of Cheffelo projects and packages.

## Installation

Install the cli with `poetry install`.


## Usage

If you have activated the python venv in your shell, run `chef <command>`.

Otherwise try activate the shell with `poetry shell` or run `poetry run chef <command>`.

## Commands

Below are the different commands available.

### Create
Create a new project or package.

`chef create <project | package>`

This will start up an interactable wizard that fills in basic information.

### Add
Adds an internal or external package to the project.

`chef add lmkgroup-ds-utils`

If you define a package that matches one of the internal packages, the internal package will be added.
Otherwise it will assume it is an external package, and add that one.

### Remove
Removes a dependency from the project.

`chef remove lmkgroup-ds-utils`

### List packages
Lists the internal packages that exists.

`chef list-packages`

### Pin
Pins an internal dependency to a specified version.
If no versions are defined, it will select the current version on origin/main.

`chef pin lmkgroup-ds-utils`

### Expose
Exposes a port to a public url.
Can be used to showcase experimental projects to some other data chefs.

`chef expose <port>`

### Build
Builds a project into a Docker image. Will default to the name of the project.

`chef build`


### Up
Spins up a complete environment of the project.
This is supposed to simulate the project in it's final form.

`chef up`

You can also specify a `--profile` if you would like to spin up a subset of services.

### Down
To take down all services after running `chef up` can you use `chef down`

### Run
Runs a command in the projects environment.

`chef run echo "Hello World"`

### Shell
Opens up an interactable bash shell in a Docker image.

`chef shell`

This can be very useful for debugging Docker images, or spesific commands.

### Test
Runs all tests in the current package or project.

This assumes that all tests are located at `./tests`.

`chef test`

### Generate Dotenv
Generates a `.env` file that contains the expected settings for the project.

`chef generate-dotenv`

If you do not provide a `--settings-path`, then it will default to look for a `settings.py` file and a `pydantic_settings.BaseSettings` class called `Settings`.

### Lint
Run a linter in the project.

`chef lint`

### Format
Runs a formatter on the codebase

`chef format`

### Doctor
Checks the project for common issues.

`chef doctor`

You can also pass a `--project` argument to check a specific project. For example `chef doctor --project data-model`

Note: when testing the `data-model` project, you can't be in a directory lower than the root `data-model` directory.
