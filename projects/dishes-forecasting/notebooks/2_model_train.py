# Databricks notebook source
import pandas as pd

import sys
import logging

from databricks_env import auto_setup_env

auto_setup_env()

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# COMMAND ----------
from dishes_forecasting.run_train import run_train, Args

args = Args(company="GL", env="dev")
run_train(args=args)
