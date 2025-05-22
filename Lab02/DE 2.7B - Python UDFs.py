# Databricks notebook source
# DBTITLE 0,--i18n-7c0e5ecf-c2e4-4a89-a418-76faa15ce226
# MAGIC %md
# MAGIC
# MAGIC # Python 사용자 정의 함수
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. 함수 정의
# MAGIC 1. UDF 생성 및 적용
# MAGIC 1. Python 데코레이터 구문을 사용하여 UDF 생성 및 등록
# MAGIC 1. Pandas(벡터화된) UDF 생성 및 적용
# MAGIC
# MAGIC ##### 메서드
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html" target="_blank">Pandas UDF 데코레이터</a>: **`@udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html" target="_blank">Pandas UDF 데코레이터</a>: **`@pandas_udf`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.7B

# COMMAND ----------

# DBTITLE 0,--i18n-1e94c419-dd84-4f8d-917a-019b15fc6700
# MAGIC %md
# MAGIC
# MAGIC ### 사용자 정의 함수(UDF)
# MAGIC 사용자 정의 열 변환 함수
# MAGIC
# MAGIC - Catalyst Optimizer로 최적화할 수 없음
# MAGIC - 함수가 직렬화되어 실행기로 전송됨
# MAGIC - 행 데이터는 Spark의 네이티브 바이너리 형식에서 역직렬화되어 UDF로 전달되고, 결과는 Spark의 네이티브 형식으로 다시 직렬화됨
# MAGIC - Python UDF의 경우, 실행기와 각 워커 노드에서 실행되는 Python 인터프리터 간에 추가적인 프로세스 간 통신 오버헤드가 발생함

# COMMAND ----------

# DBTITLE 0,--i18n-4d1eb639-23fb-42fa-9b62-c407a0ccde2d
# MAGIC %md
# MAGIC
# MAGIC 이 데모에서는 판매 데이터를 사용합니다.

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05043672-b02a-4194-ba44-75d544f6af07
# MAGIC %md
# MAGIC
# MAGIC ### 함수 정의
# MAGIC
# MAGIC **`email`** 필드에서 문자열의 첫 글자를 가져오는 함수를 (드라이버에서) 정의합니다.

# COMMAND ----------

def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")

# COMMAND ----------

# DBTITLE 0,--i18n-17f25aa9-c20f-41da-bac5-95ebb413dcd4
# MAGIC %md
# MAGIC
# MAGIC ### UDF 생성 및 적용
# MAGIC 함수를 UDF로 등록합니다. 이렇게 하면 함수가 직렬화되어 실행기로 전송되어 DataFrame 레코드를 변환할 수 있습니다.

# COMMAND ----------

first_letter_udf = udf(first_letter_function)

# COMMAND ----------

# DBTITLE 0,--i18n-75abb6ee-291b-412f-919d-be646cf1a580
# MAGIC %md
# MAGIC
# MAGIC **`email`** 열에 UDF를 적용합니다.

# COMMAND ----------

from pyspark.sql.functions import col

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-26f93012-a994-4b6a-985e-01720dbecc25
# MAGIC %md
# MAGIC
# MAGIC ### 데코레이터 구문 사용(Python 전용)
# MAGIC
# MAGIC 또는 <a href="https://realpython.com/primer-on-python-decorators/" target="_blank">Python 데코레이터 구문</a>을 사용하여 UDF를 정의하고 등록할 수 있습니다. **`@udf`** 데코레이터 매개변수는 함수가 반환하는 Column 데이터 유형입니다.
# MAGIC
# MAGIC 더 이상 로컬 Python 함수를 호출할 수 없습니다(예: **`first_letter_udf("annagray@kaufman.com")`**는 작동하지 않습니다).
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="참고"> 이 예제에서는 Python 3.5에 도입된 <a href="https://docs.python.org/3/library/typing.html" target="_blank">Python 타입 힌트</a>도 사용합니다. 이 예제에서는 타입 힌트가 필수는 아니지만, 개발자가 함수를 올바르게 사용할 수 있도록 돕는 "문서" 역할을 합니다. 이 예제에서 타입 힌트는 UDF가 한 번에 하나의 레코드를 처리하여 단일 **`str`** 인수를 받고 **`str`** 값을 반환함을 강조하기 위해 사용됩니다.

# COMMAND ----------

# Our input/output is a string
@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

# DBTITLE 0,--i18n-4d628fe1-2d94-4d86-888d-7b9df4107dba
# MAGIC %md
# MAGIC
# MAGIC 여기서 데코레이터 UDF를 사용해 보겠습니다.

# COMMAND ----------

from pyspark.sql.functions import col

sales_df = spark.table("sales")
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-3ae354c0-0b10-4e8c-8cf6-da68e8fba9f2
# MAGIC %md
# MAGIC
# MAGIC ### Pandas/벡터화된 UDF
# MAGIC
# MAGIC Pandas UDF는 UDF의 효율성을 향상시키기 위해 Python에서 제공됩니다. Pandas UDF는 Apache Arrow를 활용하여 계산 속도를 높입니다.
# MAGIC
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">Blog post</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">Documentation</a>
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="Benchmark" width ="500" height="1500">
# MAGIC
# MAGIC 사용자 정의 함수는 다음을 사용하여 실행됩니다.
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>: Spark에서 JVM과 Python 프로세스 간에 데이터를 효율적으로 전송하는 데 사용되는 메모리 내 열 형식 데이터 포맷으로, (역)직렬화 비용이 거의 들지 않습니다.
# MAGIC * Pandas 인스턴스 및 API와 함께 작동하도록 함수 내부에 Pandas를 사용합니다.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="경고"> Spark 3.0부터는 **항상** Python 타입 힌트를 사용하여 Pandas UDF를 정의해야 합니다.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-9a2fb1b1-8060-4e50-a759-f30dc73ce1a1
# MAGIC %md
# MAGIC
# MAGIC 이러한 Pandas UDF를 SQL 네임스페이스에 등록할 수 있습니다.

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# DBTITLE 0,--i18n-5e506b8d-a488-4373-af9a-9ebb14834b1b
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()