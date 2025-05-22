# Databricks notebook source
# DBTITLE 0,--i18n-b24dbbe7-205a-4a6a-8b35-ef14ee51f01c
# MAGIC %md
# MAGIC # 데이터 수집 랩
# MAGIC
# MAGIC 제품 데이터가 포함된 CSV 파일을 읽습니다.
# MAGIC
# MAGIC ##### 작업
# MAGIC 1. 추론 스키마로 읽기
# MAGIC 2. 사용자 정의 스키마로 읽기
# MAGIC 3. DDL 형식 문자열로 스키마를 읽기
# MAGIC 4. 델타 형식으로 쓰기

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.08L

# COMMAND ----------

# DBTITLE 0,--i18n-13d2d883-def7-49df-b634-910428adc5a2
# MAGIC %md
# MAGIC
# MAGIC ### 1. 스키마 추론을 사용하여 읽기
# MAGIC - DBUtils 메서드 **`fs.head`**를 사용하여 첫 번째 CSV 파일을 보고, **`single_product_csv_file_path`** 변수에 파일 경로를 지정합니다.
# MAGIC - **`products_csv_path`** 변수에 파일 경로가 지정된 CSV 파일을 읽어 **`products_df`**를 생성합니다.
# MAGIC - 첫 번째 줄을 헤더로 사용하고 스키마를 추론하는 옵션을 구성합니다.

# COMMAND ----------

# TODO

single_product_csv_file_path = f"{DA.paths.products_csv}/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(single_product_csv_file_path))

products_csv_path = DA.paths.products_csv
products_df = spark.read.csv(products_csv_path, header=True, inferSchema=True)

products_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-3ca1ef04-e7a4-4e9b-a49e-adba3180d9a4
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-d7f8541b-1691-4565-af41-6c8f36454e95
# MAGIC %md
# MAGIC
# MAGIC ### 2. 사용자 정의 스키마로 읽기
# MAGIC 열 이름과 데이터 유형을 사용하여 **StructType`**을 생성하여 스키마를 정의합니다.

# COMMAND ----------

# TODO
from pyspark.sql.types import LongType, StringType, StructType, DoubleType, StructField

user_defined_schema = StructType([StructField("item_id", StringType(), True), StructField("name", StringType(), True), StructField("price", DoubleType(), True)
])

products_df2 = spark.read.schema(user_defined_schema).csv(products_csv_path, header=True)

# COMMAND ----------

# DBTITLE 0,--i18n-af9a4134-5b88-4c6b-a3bb-8a1ae7b00e53
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

assert(user_defined_schema.fieldNames() == ["item_id", "name", "price"])
print("All test pass")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = products_df2.first()

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0a52c971-8cff-4d89-b9fb-375e1c48364d
# MAGIC %md
# MAGIC
# MAGIC ### 3. DDL로 포맷된 문자열 읽기

# COMMAND ----------

# TODO
ddl_schema = "item_id string, name string, price double"

products_df3 = spark.read.schema(ddl_schema).csv(products_csv_path, header=True)

# COMMAND ----------

# DBTITLE 0,--i18n-733b1e61-b319-4da6-9626-3f806435eec5
# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0b7e2b59-3fb1-4179-9793-bddef0159c89
# MAGIC %md
# MAGIC
# MAGIC ### 4. Delta에 쓰기
# MAGIC **`products_output_path`** 변수에 제공된 파일 경로에 **`products_df`**를 씁니다.

# COMMAND ----------

# TODO
products_output_path = DA.paths.working_dir + "/delta/products"
products_df.write.format("delta").mode("overwrite").save(products_output_path)

# COMMAND ----------

# DBTITLE 0,--i18n-fb47127c-018d-4da0-8d6d-8584771ccd64
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-42fb4bd4-287f-4863-b6ea-f635f315d8ec
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()