# Databricks notebook source
# DBTITLE 0,--i18n-df421470-173e-44c6-a85a-1d48d8a14d42
# MAGIC %md
# MAGIC # 추가 함수
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. 내장 함수를 적용하여 새 열에 대한 데이터 생성
# MAGIC 1. DataFrame NA 함수를 적용하여 Null 값 처리
# MAGIC 1. DataFrame 조인
# MAGIC
# MAGIC ##### 메서드
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame 메서드</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**, **`drop`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">내장 함수</a>:
# MAGIC - 집계: **`collect_set`**
# MAGIC - 컬렉션: **`explode`**
# MAGIC - 비집계 및 기타: **`col`**, **`lit`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.11

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c80fc2ec-34e6-459f-b5eb-afa660db9491
# MAGIC %md
# MAGIC
# MAGIC ### 비집계 함수 및 기타 함수
# MAGIC 다음은 몇 가지 추가 비집계 함수 및 기타 내장 함수입니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | col / column | 주어진 열 이름을 기반으로 열을 반환합니다. |
# MAGIC | lit | 리터럴 값의 열을 생성합니다. |
# MAGIC | isnull | 열이 null이면 true를 반환합니다. |
# MAGIC | rand | [0.0, 1.0]에 균등하게 분포하는 독립적이고 동일 분포(i.i.d.) 샘플을 갖는 난수 열을 생성합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-bae51fa6-6275-46ec-854d-40ba81788bac
# MAGIC %md
# MAGIC
# MAGIC **`col`** 함수를 사용하여 특정 열을 선택할 수 있습니다.

# COMMAND ----------

gmail_accounts = sales_df.filter(col("email").endswith("gmail.com"))

display(gmail_accounts)

# COMMAND ----------

# DBTITLE 0,--i18n-a88d37a6-5e98-40ad-9045-bb8fd4d36331
# MAGIC %md
# MAGIC
# MAGIC **`lit`**은 값으로 열을 생성하는 데 사용할 수 있으며, 열을 추가할 때 유용합니다.

# COMMAND ----------

display(gmail_accounts.select("email", lit(True).alias("gmail user")))

# COMMAND ----------

# DBTITLE 0,--i18n-d7436832-7254-4bfa-845b-2ab10170f171
# MAGIC %md
# MAGIC
# MAGIC ### DataFrameNaFunctions
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>는 null 값을 처리하는 메서드를 포함하는 DataFrame 하위 모듈입니다. DataFrame의 **`na`** 속성에 접근하여 DataFrameNaFunctions의 인스턴스를 가져옵니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | drop | 열의 선택적 하위 집합을 고려하여 null 값이 있는 행을 일부, 전체 또는 지정된 개수만큼 제외하고 새 DataFrame을 반환합니다. |
# MAGIC | fill | 열의 선택적 하위 집합에 대해 null 값을 지정된 값으로 바꿉니다. |
# MAGIC | replace | 열의 선택적 하위 집합을 고려하여 값을 다른 값으로 바꾸고 새 DataFrame을 반환합니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-da0ccd36-e2b5-4f79-ae61-cb8252e5da7c
# MAGIC %md
# MAGIC 여기서는 null/NA 값이 있는 행을 삭제하기 전과 삭제한 후의 행 수를 확인합니다.

# COMMAND ----------

print(sales_df.count())
print(sales_df.na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-aef560b8-7bb6-4985-a43d-38541ba78d33
# MAGIC %md
# MAGIC 행 개수가 같으므로 null이 없는 열이 있습니다. items.coupon과 같은 열에서 null을 찾으려면 항목을 분해해야 합니다.

# COMMAND ----------

sales_exploded_df = sales_df.withColumn("items", explode(col("items")))
display(sales_exploded_df.select("items.coupon"))
print(sales_exploded_df.select("items.coupon").count())
print(sales_exploded_df.select("items.coupon").na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-4c01038b-2afa-41d8-a390-fad45d1facfe
# MAGIC %md
# MAGIC
# MAGIC 누락된 쿠폰 코드는 **`na.fill`**을 사용하여 채울 수 있습니다.

# COMMAND ----------

display(sales_exploded_df.select("items.coupon").na.fill("NO COUPON"))

# COMMAND ----------

# DBTITLE 0,--i18n-8a65ceb6-7bc2-4147-be66-71b63ae374a1
# MAGIC %md
# MAGIC
# MAGIC ### DataFrame 결합
# MAGIC DataFrame의 <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank">**`join`**</a> 메서드는 주어진 조인 표현식을 기반으로 두 DataFrame을 결합합니다.
# MAGIC
# MAGIC 여러 유형의 조인이 지원됩니다.
# MAGIC
# MAGIC "name"이라는 공유 열의 값이 같은 경우를 기준으로 하는 내부 조인(즉, 동등 조인)<br/>
# MAGIC **`df1.join(df2, "name")`**
# MAGIC
# MAGIC "name"과 "age"라는 공유 열의 값이 같은 경우를 기준으로 하는 내부 조인<br/>
# MAGIC **`df1.join(df2, ["name", "age"])`**
# MAGIC
# MAGIC "name"이라는 공유 열의 값이 같은 경우를 기준으로 하는 전체 외부 조인<br/>
# MAGIC **`df1.join(df2, "name", "outer")`**
# MAGIC
# MAGIC 명시적 열 표현식을 기준으로 하는 왼쪽 외부 조인<br/>
# MAGIC **`df1.join(df2, df1["customer_name"] == df2["account_name"], "left_outer")`**

# COMMAND ----------

# DBTITLE 0,--i18n-67001b92-91e6-4137-9138-b7f00950b450
# MAGIC %md
# MAGIC 위의 Gmail 계정으로 가입하기 위해 사용자 데이터를 로드하겠습니다.

# COMMAND ----------

users_df = spark.table("users")
display(users_df)

# COMMAND ----------

joined_df = gmail_accounts.join(other=users_df, on='email', how = "inner")
display(joined_df)

# COMMAND ----------

# DBTITLE 0,--i18n-9a47f04c-ce9c-4892-b457-1a2e57176721
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()