# Databricks notebook source
# MAGIC %store -r adls key
# MAGIC print(adls, key)

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{adls}.dfs.core.windows.net",
    key)

# COMMAND ----------

from pyspark.sql.types import *
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/races.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col
races_df = df.withColumn("ingestion_date", current_timestamp()) \
             .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) 
display(races_df)


# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df = races_df.select(col("raceId").alias('race_id'), col("year").alias('race_year'), col("round"), col("circuitId").alias('circuit_id'), col("name"), col("ingestion_date"), col("race_timestamp"))
display(races_selected_df)

# COMMAND ----------

races_selected_df.write.parquet(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/races", mode="overwrite", partitionBy="race_year")

# COMMAND ----------

display(dbutils.fs.ls(f'abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/races'))

# COMMAND ----------

df1 = spark.read.parquet(f"abfss://rawdata@{adls}.dfs.core.windows.net/f1/processed/races")
df1.show()