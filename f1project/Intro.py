# Databricks notebook source
# MAGIC %md
# MAGIC # this is intro

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %sql
# MAGIC select "hello world"

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    if files.name.endswith('/'):
        print(files.name)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dummydatasbx.dfs.core.windows.net",
    "dAqrHcJPNHcv/MPIW+OyDzktxB/2SoLoppRo1xF22zQnKvP+NnLLpTR3RRTpE4lIPBddCOZA52T/+AStR1Sm6w==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://rawdata@dummydatasbx.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://rawdata@dummydatasbx.dfs.core.windows.net/circuits.csv"))