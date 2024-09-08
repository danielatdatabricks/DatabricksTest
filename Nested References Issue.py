# Databricks notebook source
from config.setup import sum_two_numbers
# from config.settings.functions import multi_two_numbers
import dlt


# COMMAND ----------

import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )
a = sum_two_numbers(1,2)
# res = multi_two_numbers(a, 3)
