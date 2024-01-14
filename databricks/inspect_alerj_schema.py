# Databricks notebook source
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pathlib import Path
import json

dirs = dbutils.fs.ls("gs://bossa-bucket-coutj/trusted", )

files = []
for folder in dirs:
    files.extend([file_name.path for file_name in dbutils.fs.ls(folder.path)])

# COMMAND ----------


dict_schema = {}
for file in files:
    file_name = Path(file).name
    print("Reading schema:", file_name)
    dict_schema[file_name] = spark.read.parquet(file).columns


# COMMAND ----------

# Create dataframe

# get max column list len
max_len = 0
for v in dict_schema.values():
    actual_len = len(v)
    if actual_len > max_len:
        max_len = actual_len

# Set list to a fix len
dict_schema_size_fix = {}
for k,v in dict_schema.items():
    dict_schema_size_fix[k] = [ v[idx] if idx < len(v) else "" for idx in range(max_len) ]

data2 = []
for idx in range(max_len):
    new_list = []
    for l in dict_schema_size_fix.values():    
        new_list.append(l[idx])
    data2.append(new_list)

schema = StructType([ StructField(col_name, StringType(), True) for col_name in dict_schema_size_fix.keys()])

df = spark.createDataFrame(data=data2, schema=schema)

display(df)

# COMMAND ----------

#Get list of distinct column names for each file

distinct_values = {}

for idx, list_values in enumerate(data2):
    distinct_values[f"row_{idx}"] = list(set(list_values))

print(json.dumps(distinct_values))

