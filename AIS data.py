# Databricks notebook source
import pandas as pd
import urllib
import zipfile
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS stuart.lv.csv

# COMMAND ----------

dates = pd.DataFrame(data=pd.date_range(start="2024-12-10", end="2024-12-19"))
df = spark.createDataFrame(dates)
display(df)

# COMMAND ----------

urls = df.withColumn(
  "file_name",
  F.concat(
    F.lit("AIS_"), F.date_format("0", "yyyy_MM_dd"), F.lit(".zip")
  )
).withColumn(
  "url",
  F.concat(
    F.lit("https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"), F.year("0"), F.lit("/"),
    F.col("file_name")
  )
).select("url", "file_name")
display(urls)

# COMMAND ----------

dbutils.fs.mkdirs("/tmp/vessels_zips")
dbutils.fs.mkdirs("/tmp/vessels_ais")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType

def get_ais_data(data: pd.DataFrame) -> pd.DataFrame:
  try:
    url = data["url"].iloc[0]
    file_name = data["file_name"].iloc[0]
    file = urllib.request.urlopen(url)
    zip_file_name = f'/dbfs/tmp/vessels_zips/{file_name}'
    out_dir = f'/Volumes/stuart/lv/csv/'
    with open(zip_file_name, 'wb') as output:
      output.write(file.read())
    with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
      zip_ref.extractall(out_dir)
    return pd.DataFrame(data=["OK"])
  except Exception as e:
    return pd.DataFrame(data=[f"Error: {e}"])

# COMMAND ----------

to_run = urls.withColumn(
  "id", F.monotonically_increasing_id()
).repartition(int(dates.size), F.col("id")).groupBy("id").applyInPandas(get_ais_data, schema="struct<f1: string>")

# COMMAND ----------

to_run.collect()

# COMMAND ----------

# MAGIC %sh ls -lah '/Volumes/stuart/lv/csv'
