# Preselector

The preselector project selects out the **optimal combination** of recipes through out a week.

## The Problem to solve

Even though we recommend the default selection that a customer will get for a week, is this not a classical recommendation problem.
This is due the the nature where the recommendation of the recipe $n$ is dependent on the recipes $0 \rightarrow n - 1$.

Therefore, at the time of this writing is this more of an optimalisation problem then a ml problem, even tho a few of the optimalisation dimentions are outputs of other ml models.

This have lead to an approach where we do a bredth first search through a multi dimentional space.

## Get started

Before you manage to get started you will need to set the following environment variables in a `.env` file.

```
DATALAKE_SERVICE_ACCOUNT_NAME="gganalyticsdatalake"
DATALAKE_STORAGE_ACCOUNT_KEY="..."
ADB_CONNECTION="DRIVER=ODBC Driver 18 for SQL Server;DATABASE=AnalyticsDB;UID=...;SERVER=gg-analytics-fog.database.windows.net;PORT=1433;PWD=..."
```

There are multiple apps but to get started and understand the pre-selector do we recommend to startup the `combination-app` in the `docker-compose.yaml` file.


Run one of the following to start the app:

Through `chef`
```bash
chef up combination-app
```

Through `docker-compose`
```bash
docker compose up combination-app
```

This should start an app at `http://localhost:8506` which makes it possible to experiment with different combinations and explain why the pre-selector outputs what it does.

## Suggested Development

It is recommended to develop everything locally through `docker`. This is due to the combination of Databricks and Azure resources, as they serve different purposes.

If you do any local changes, startup the `combination-app` and run the app to see if fix the issue.

## Debugging
If any customers have any issues in production, then startup the `debug` app, again through `docker compose`.
This will find the generation request that the customer had, and try to reproduce the results as closely as possible.
There is a posibility of data drift, so a 1:1 recreation is probably not possible.

## Cost of Food
There is a app to measure Cost of Food as well. This can be started with `docker compose cof-app`.

## Inspect data
If you want to inspect some of the data used in the pre-selector, then start up the catalog with `chef catalog`. This will start an app that enables you to run SQL queries and explore the data in the `data-contracts` package.

You may need to setup a `.env` in the root of `sous-chef`.
