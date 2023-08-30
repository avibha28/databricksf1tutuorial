# Databricks notebook source
# MAGIC %md
# MAGIC Method 1: Pyspark

# COMMAND ----------

from delta.tables import *
Deltatable.create(spark) \
    .tableName("employee_demo") \
    .addColumn("emp_id", "INT") \
    .addColumn("emp_name", "STRING") \
    .addColumn("gender", "STRING") \
    .addCOlumn("salary", "INT") \
    .addColumn("Dept", "STRING") \
    .property("Description", "Table created for demo purposes.") \
    .location()