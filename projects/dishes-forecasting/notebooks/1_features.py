# Databricks notebook source
import sys
import logging


from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# packages = [
#     "../",
#     "../../../packages/pydantic-argparser/",
#     "../../../packages/constants/",
# ]

# sys.path.extend(packages)

# COMMAND ----------
from dishes_forecasting.run_features import run_features, Args

args = Args(company="GL", env="dev")
run_features(args=args)
