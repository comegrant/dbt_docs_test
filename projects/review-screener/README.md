# Review Screener

A Python project by Cheffelo.

## Structure

This project is structured with the following assumptions in mind.

- `review_screener`: Where all our business logic is stored.
- `jobs`: Where all our orchestrated jobs exists. E.i. a wrapper around functionality that exists in `review_screener`.
- `tests`: Where we put any unit or integration tests.
- `docker`: Where we build our containers for different environments.

## Development

To get *auto completion* support, make sure to have a python virtual environment setup with the project's Python dependencies installed.

This can be done with the following CLI commands:

```bash
poetry shell
poetry install
```

Then open your favourite editor and start typing :rocket:

## Running the code

### First time setup

Before you start editing.
Make sure you have an `.env` file in the root of the `sous-chef` repo.

This is used to connect to Databricks and our data lake when developing locally through a container.

### Local run

To run the code locally, is it recommended to use `docker`.
This makes sure we can replicate the production environment as closely as possible, and therefore replicate issues with ease.

Then run `docker compose run <your-service-to-run>`.

> [!NOTE]
> The default template contains a service called `run` in the `docker-compose.yaml` file which should run the `job/run.py` file with some default arguments.

### Run in Databricks

If you rather want to use Databricks compute to run the code. Then it is recommended to do the following.

1. Create a new PR in GitHub. This will deploy your new `jobs` workflows to the Databricks `dev` [workspace](https://adb-4291784437205825.5.azuredatabricks.net/jobs?o=4291784437205825).
2. Find the workflow that you created. Should be named similar to `[dev <some uuid>] <your-job-name>`.


## Troubleshooting

Here are some common issues and how to fix them.

### I get a `Module not found error` locally
This often happens when a dependency is either added or updated, but not reflected in the container.

Therefore, try running `docker compose build <your-service>` and then run again.

If it is still not working, then make sure the dependencies are copied correctly in the associated `Dockerfile`.

### Unable to find my job in Databricks
Make sure you have a `.yaml` file associated with your python file, and not only the python file that.

If you still can not find your workflow in Databricks, make sure the `databricks.yml` reference the yaml file in the `include` section.

If this do not work. Check the Databricks DABs validation logs in GitHub actions to find more information about why it is failing.

### My code is not updating in Databricks
Make sure you have pushed a new Docker image with the latest source code.
This should happen automatically if you push a change to an open PR.

Furthermore, if you still have issues. Try to manually trigger the workflow and manually define the Docker image url as a parameter to make sure you are using the correct codebase.
