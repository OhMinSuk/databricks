# Databricks notebook source
# MAGIC %md
# MAGIC # 연습 #2 - 일괄 처리
# MAGIC
# MAGIC 이 연습에서는 2017년, 2018년, 2019년 각각 하나씩, 총 세 개의 주문 일괄 처리를 처리합니다.
# MAGIC
# MAGIC 각 일괄 처리를 처리할 때마다 새로운 델타 테이블에 추가하여 모든 데이터 세트를 하나의 단일 데이터 세트로 통합합니다.
# MAGIC
# MAGIC 매년 다른 개인과 다른 표준이 사용되어 데이터 세트가 약간씩 달라졌습니다.
# MAGIC * 2017년 백업은 고정 너비 텍스트 파일로 작성되었습니다.
# MAGIC * 2018년 백업은 탭으로 구분된 텍스트 파일로 작성되었습니다.
# MAGIC * 2019년 백업은 "표준" 쉼표로 구분된 텍스트 파일로 작성되었지만, 열 이름 형식은 변경되었습니다.
# MAGIC
# MAGIC 여기서 우리의 유일한 목표는 모든 데이터 세트를 통합하는 동시에 추가 문제가 발생할 경우 각 레코드의 출처(처리된 파일 이름 및 처리된 타임스탬프)를 추적하는 것입니다.
# MAGIC
# MAGIC 이 단계에서는 데이터 수집에만 집중하므로 대부분의 열은 단순 문자열로 수집되며, 향후 연습에서는 다양한 변환을 통해 이 문제(및 기타 문제)를 해결할 것입니다.
# MAGIC
# MAGIC 진행하면서 몇 가지 "현실 확인"을 통해 제대로 진행되고 있는지 확인할 수 있습니다. 해당 솔루션을 구현한 후 해당 명령을 실행하기만 하면 됩니다.
# MAGIC
# MAGIC 이 연습은 3단계로 나뉩니다.
# MAGIC * 연습 2.A - 고정 너비 파일 데이터 수집
# MAGIC * 연습 2.B - 탭으로 구분된 파일 데이터 수집
# MAGIC * 연습 2.C - 쉼표로 구분된 파일 데이터 수집

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 2번 설정</h2>
# MAGIC
# MAGIC 시작하려면 다음 셀을 실행하여 이 연습을 설정하고, 연습별 변수와 함수를 선언하세요.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-02

# COMMAND ----------

# MAGIC %md 이 연습에서 처리할 파일 목록을 미리 보려면 다음 셀을 실행하세요.

# COMMAND ----------

files = dbutils.fs.ls(f"{working_dir}/raw/orders/batch") # List all the files
display(files)                                           # Display the list of files

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #2.A - 고정 너비 파일 수집</h2>
# MAGIC
# MAGIC **이 단계에서는 다음 작업을 수행해야 합니다.**
# MAGIC 1. 필요한 경우 **`batch_2017_path`** 변수와 **`dbutils.fs.head`** 변수를 사용하여 2017 배치 파일을 조사합니다.
# MAGIC 2. **`batch_2017_path`**로 식별된 텍스트 파일을 수집하도록 **`DataFrameReader`**를 구성합니다. 이렇게 하면 줄당 하나의 레코드가 제공되고, **`value`**라는 단일 열이 있어야 합니다.
# MAGIC 3. **`fixed_width_column_defs`**(또는 사전 자체)의 정보를 사용하여 **`value`** 열을 사용하여 적절한 길이의 새 열을 추출합니다.<br/>
# MAGIC * 사전의 키는 열 이름입니다.
# MAGIC * 사전의 값에 있는 첫 번째 요소는 해당 열 데이터의 시작 위치입니다.
# MAGIC * 사전의 값에 있는 두 번째 요소는 해당 열 데이터의 길이입니다.
# MAGIC 4. **`value`** 열에 대한 작업이 완료되면 제거합니다.
# MAGIC 5. 3단계에서 생성된 각 새 열에 대해 선행 공백을 제거합니다.
# MAGIC * **`value`** 열에서 고정 너비 값을 추출할 때 \[선행\] 공백이 발생할 수 있습니다.
# MAGIC 6. 3단계에서 생성된 각 새 열에 대해 모든 빈 문자열을 **`null`**로 바꿉니다.
# MAGIC * 공백을 제거한 후, 원본 데이터세트에 값이 지정되지 않은 열은 모두 빈 문자열이 됩니다.
# MAGIC 7. 데이터를 읽어온 파일 이름인 **`ingest_file_name`**이라는 새 열을 추가합니다.
# MAGIC * 이 부분은 하드코딩되어서는 안 됩니다.
# MAGIC * 해당 함수는 <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> 모듈을 참조하세요.
# MAGIC 8. 데이터가 DataFrame으로 수집된 타임스탬프인 **`ingested_at`**이라는 새 열을 추가합니다.
# MAGIC * 이 부분은 하드코딩되어서는 안 됩니다.
# MAGIC * 적절한 함수는 <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> 모듈을 참조하세요.
# MAGIC 9. 해당 **`DataFrame`**을 "델타" 형식으로 **`batch_target_path`**에 지정된 위치에 작성합니다.
# MAGIC
# MAGIC **참고 사항:**
# MAGIC * **`fixed_width_column_defs`** 사전을 사용하여 프로그래밍 방식으로 각 열을 추출할 수 있지만, 이 단계를 하드 코딩하여 한 번에 한 열씩 추출해도 괜찮습니다.
# MAGIC * **`SparkSession`**은 이미 **`spark`**의 인스턴스로 제공됩니다.
# MAGIC * 이 연습에 필요한 클래스/메서드는 다음과 같습니다.
# MAGIC * **`pyspark.sql.DataFrameReader`**: 데이터 수집
# MAGIC * **`pyspark.sql.DataFrameWriter`**: 데이터 수집
# MAGIC * **`pyspark.sql.Column`**: 데이터 변환
# MAGIC * **`pyspark.sql.functions`** 모듈의 다양한 함수
# MAGIC * **`pyspark.sql.DataFrame`**의 다양한 변환 및 작업
# MAGIC * 다음 메서드를 사용하여 Databricks 파일 시스템(DBFS)을 조사하고 조작할 수 있습니다.
# MAGIC * **`dbutils.fs.ls(..)`**: 파일 나열
# MAGIC * **`dbutils.fs.rm(..)`**: 파일 제거
# MAGIC * **`dbutils.fs.head(..)`**: 파일의 처음 N바이트 보기
# MAGIC
# MAGIC **추가 요구 사항:**
# MAGIC * 통합 배치 데이터 세트는 "델타" 형식으로 디스크에 기록되어야 합니다.
# MAGIC * 통합 배치 데이터 세트의 스키마는 다음과 같아야 합니다.
# MAGIC   * **`submitted_at`**:**`string`**
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`customer_id`**:**`string`**
# MAGIC   * **`sales_rep_id`**:**`string`**
# MAGIC   * **`sales_rep_ssn`**:**`string`**
# MAGIC   * **`sales_rep_first_name`**:**`string`**
# MAGIC   * **`sales_rep_last_name`**:**`string`**
# MAGIC   * **`sales_rep_address`**:**`string`**
# MAGIC   * **`sales_rep_city`**:**`string`**
# MAGIC   * **`sales_rep_state`**:**`string`**
# MAGIC   * **`sales_rep_zip`**:**`string`**
# MAGIC   * **`shipping_address_attention`**:**`string`**
# MAGIC   * **`shipping_address_address`**:**`string`**
# MAGIC   * **`shipping_address_city`**:**`string`**
# MAGIC   * **`shipping_address_state`**:**`string`**
# MAGIC   * **`shipping_address_zip`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`string`**
# MAGIC   * **`product_sold_price`**:**`string`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### 고정 너비 메타데이터
# MAGIC
# MAGIC 다음 사전은 참조 및/또는 구현을 위해 제공됩니다.<br/>
# MAGIC (선택한 전략에 따라 다름).
# MAGIC
# MAGIC 다음 셀을 실행하여 인스턴스화하세요.

# COMMAND ----------

fixed_width_column_defs = {
  "submitted_at": (1, 15),
  "order_id": (16, 40),
  "customer_id": (56, 40),
  "sales_rep_id": (96, 40),
  "sales_rep_ssn": (136, 15),
  "sales_rep_first_name": (151, 15),
  "sales_rep_last_name": (166, 15),
  "sales_rep_address": (181, 40),
  "sales_rep_city": (221, 20),
  "sales_rep_state": (241, 2),
  "sales_rep_zip": (243, 5),
  "shipping_address_attention": (248, 30),
  "shipping_address_address": (278, 40),
  "shipping_address_city": (318, 20),
  "shipping_address_state": (338, 2),
  "shipping_address_zip": (340, 5),
  "product_id": (345, 40),
  "product_quantity": (385, 5),
  "product_sold_price": (390, 20)
}

print(fixed_width_column_defs)

# COMMAND ----------

# MAGIC %md ### 연습문제 #2.A 구현하기
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

from pyspark.sql.functions import col, substring, trim, when, input_file_name, current_timestamp
from pyspark.sql.types import StringType
# TODO
df_2017 = (spark
           .read
           .option('header', 'false')
           .text(batch_2017_path))

for col_name, (start, length) in fixed_width_column_defs.items():
  df_2017 = df_2017.withColumn(col_name, substring(col("value"), start, length)) 

df_2017 = df_2017.drop("value")

for col_name in df_2017.columns:
  df_2017 = df_2017.withColumn(col_name, trim(col(col_name)))

for col_name in df_2017.columns:
  df_2017 = df_2017.withColumn(col_name, when(col(col_name) == "",None).otherwise(col(col_name)))

df_2017 = df_2017.withColumn("ingest_file_name", input_file_name())
df_2017 = df_2017.withColumn("ingested_at", current_timestamp())

df_2017.write.format("delta").mode("overwrite").save(batch_target_path)
# Use this cell to complete your solution
display(df_2017)

# COMMAND ----------

# MAGIC %md ### Reality Check #2.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #2.B - 탭으로 구분된 파일 수집</h2>
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC 1. 필요한 경우 **`batch_2018_path`** 변수와 **`dbutils.fs.head`** 변수를 사용하여 2018 배치 파일을 조사합니다.
# MAGIC 2. **`batch_2018_path`**로 식별된 탭으로 구분된 파일을 수집하도록 **`DataFrameReader`**를 구성합니다.
# MAGIC 3. 데이터를 읽어온 파일의 이름인 **`ingest_file_name`**이라는 새 열을 추가합니다. 이 이름은 하드 코딩되어서는 안 됩니다.
# MAGIC 4. 데이터가 DataFrame으로 수집된 시점의 타임스탬프인 새 열 **`ingested_at`**을 추가합니다. 이 값은 하드코딩되어서는 안 됩니다.
# MAGIC 5. 이전에 생성된 데이터세트의 **`batch_target_path`**에 지정된 값에 해당하는 **`DataFrame`**을 **추가**합니다.
# MAGIC
# MAGIC **추가 요구 사항**
# MAGIC * CSV 파일에 있는 **"null"** 문자열은 SQL 값 **null**로 대체해야 합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #2.b를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
df_2018 = (spark
           .read
           .option('header', 'true')
           .option("sep","\t")
           .csv(batch_2018_path))

df_2018 = df_2018.withColumn("ingest_file_name",input_file_name())
df_2018 = df_2018.withColumn("ingested_at",current_timestamp())

for col_name in df_2018.columns:
  df_2018 = df_2018.withColumn(col_name, when(col(col_name) == "null",None).otherwise(col(col_name)))

df_2018.write.format("delta").mode("append").save(batch_target_path)
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #2.B
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_02_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> 연습 #2.C - 쉼표로 구분된 파일 수집</h2>
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC 1. 필요한 경우 **`batch_2019_path`** 변수와 **`dbutils.fs.head`** 변수를 사용하여 2019 배치 파일을 조사합니다.
# MAGIC 2. **`batch_2019_path`**로 식별된 쉼표로 구분된 파일을 수집하도록 **`DataFrameReader`**를 구성합니다.
# MAGIC 3. 데이터를 읽어온 파일의 이름인 **`ingest_file_name`**이라는 새 열을 추가합니다. 이 이름은 하드 코딩되어서는 안 됩니다.
# MAGIC 4. 데이터가 DataFrame으로 수집된 시점의 타임스탬프인 새 열 **`ingested_at`**을 추가합니다. 이 값은 하드 코딩되어서는 안 됩니다.
# MAGIC 5. 이전에 생성된 데이터세트의 **`batch_target_path`**에 지정된 타임스탬프에 해당 **`DataFrame`**을 **추가**합니다.<br/>
# MAGIC
# MAGIC 참고: 이 데이터세트의 열 이름은 연습 #2.A에서 정의된 스키마에 맞게 업데이트해야 합니다. 이를 위한 몇 가지 전략이 있습니다.
# MAGIC * 수집 시 이름을 변경하는 스키마를 제공합니다.
# MAGIC * 한 번에 한 열씩 수동으로 이름을 바꿉니다.
# MAGIC * **`fixed_width_column_defs`**를 사용하여 프로그래밍 방식으로 한 번에 한 열씩 이름을 바꿉니다.
# MAGIC * **`DataFrame`** 클래스에서 제공하는 변환을 사용하여 한 번의 작업으로 모든 열의 이름을 바꿉니다.
# MAGIC
# MAGIC **추가 요구 사항**
# MAGIC * CSV 파일의 **"null"** 문자열은 SQL 값 **null**로 대체해야 합니다.<br/>

# COMMAND ----------

# MAGIC %md ### 연습문제 #2.C를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

# TODO
df_2019 = (spark
           .read
           .option('header', 'true')
           .option("sep",",")
           .csv(batch_2019_path))

for new_col in fixed_width_column_defs:
  for old_col in df_2019.columns:
    if old_col.lower().replace("_", "") == new_col.replace("_", ""):
      df_2019 = df_2019.withColumnRenamed(old_col,new_col)

df_2019 = df_2019.withColumn("ingest_file_name",input_file_name())
df_2019 = df_2019.withColumn("ingested_at",current_timestamp())

for col_name in df_2019.columns:
  df_2019 = df_2019.withColumn(col_name, when(col(col_name) == "null",None).otherwise(col(col_name)))

df_2019.write.format("delta").mode("append").save(batch_target_path)

display(df_2019)
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### 현실 확인 #2.C
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_02_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2 - Final Check</h2>
# MAGIC
# MAGIC 다음 명령을 실행하여 이 연습이 완료되었는지 확인하세요.

# COMMAND ----------

reality_check_02_final()