# Databricks notebook source
from pathlib import Path
# dbutils.fs.help("ls")
dirs = dbutils.fs.ls("gs://bossa-bucket-coutj/trusted", )

files = []
for folder in dirs:
    files.extend([file_name.path for file_name in dbutils.fs.ls(folder.path)])

print(files)

# COMMAND ----------


dict_schema = {}
for file in files:
    file_name = Path(file).name
    print("Reading schema:", file_name)
    schema = sqlContext.read.parquet(file).schema.fields
    field_names = [field.name for field in schema]
    dict_schema[file_name] = field_names


# fields = sqlContext.read.parquet("gs://bossa-bucket-coutj/trusted/2016/folha_2016_1.parquet").schema.fields

# dict_schema = {}
# field_names = [field.name for field in fields]
# # for field in fields:
# dict_schema["2016"] = field_names
#     # print(field.name)

print(dict_schema)



# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
max_len = 0

for k,v in dict_schema.items():
    actual_len = len(v)
    if actual_len > max_len:
        max_len = actual_len

print(max_len)

dict_schema_size_fix = {}
for k,v in dict_schema.items():
    dict_schema_size_fix[k] = [v[idx] if idx < (len(v)) else "" for idx in range(max_len + 1) ]

# print(dict_schema_size_fix)


# data = [ tuple(value_list) for value_list in dict_schema.values()]
data2 = []
for idx in range(max_len):
    new_list = []
    for l in dict_schema.values():    
        new_list.append(l[idx])
    data2.append(new_list)

# print(data2)
schema = StructType([ StructField(col_name, StringType(), True) for col_name in dict_schema_size_fix.keys()])

# print(data)
# print(schema)

df = spark.createDataFrame(data=data2, schema=schema)

display(df)
