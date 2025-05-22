# Databricks notebook source
# DBTITLE 0,--i18n-5b46ceba-8f87-4062-91cc-6f02f3303258
# MAGIC %md
# MAGIC # 구매 수익 Lab
# MAGIC
# MAGIC 구매 수익이 있는 이벤트 데이터세트를 준비합니다.
# MAGIC
# MAGIC ##### 작업
# MAGIC 1. 각 이벤트의 구매 수익 추출
# MAGIC 2. 수익이 null이 아닌 이벤트 필터링
# MAGIC 3. 수익이 있는 이벤트 유형 확인
# MAGIC 4. 불필요한 열 삭제
# MAGIC
# MAGIC ##### 메서드
# MAGIC - DataFrame: **`select`**, **`drop`**, **`withColumn`**, **`filter`**, **`dropDuplicates`**
# MAGIC - Column: **`isNotNull`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.04L

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-412840ac-10d6-473e-a3ea-8e9e92446b80
# MAGIC %md
# MAGIC
# MAGIC ### 1. 각 이벤트에 대한 구매 수익을 추출합니다.
# MAGIC **`ecommerce.purchase_revenue_in_usd`**를 추출하여 새 열 **`revenue`**를 추가합니다.

# COMMAND ----------

# TODO
revenue_df = events_df.withColumn('revenue',events_df['ecommerce.purchase_revenue_in_usd'])
display(revenue_df)

# COMMAND ----------

# DBTITLE 0,--i18n-66dfc9f4-0a59-482e-a743-cfdbc897aee8
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.functions import col
expected1 = [4351.5, 4044.0, 3985.0, 3946.5, 3885.0, 3590.0, 3490.0, 3451.5, 3406.5, 3385.0]
result1 = [row.revenue for row in revenue_df.sort(col("revenue").desc_nulls_last()).limit(10).collect()]
print(result1)
assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cb49af43-880a-4834-be9c-62f65581e67a
# MAGIC %md
# MAGIC
# MAGIC ### 2. 매출이 null이 아닌 이벤트 필터링
# MAGIC **`매출`**이 **`null`**이 아닌 레코드 필터링

# COMMAND ----------

# TODO
purchases_df = revenue_df.filter(col("revenue").isNotNull())
display(purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-3363869f-e2f4-4ec6-9200-9919dc38582b
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

assert purchases_df.filter(col("revenue").isNull()).count() == 0, "Nulls in 'revenue' column"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-6dd8d228-809d-4a3b-8aba-60da65c53f1c
# MAGIC %md
# MAGIC
# MAGIC ### 3. 수익이 발생한 이벤트 유형 확인
# MAGIC 두 가지 방법 중 하나로 **`purchases_df`**에서 고유한 **`event_name`** 값을 찾습니다.
# MAGIC - "event_name"을 선택하고 고유한 레코드를 가져옵니다.
# MAGIC - "event_name"만 기준으로 중복 레코드를 삭제합니다.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="힌트"> 수익과 연결된 이벤트는 하나뿐입니다.

# COMMAND ----------

# TODO
distinct_df1 = purchases_df.select("event_name").distinct()
display(distinct_df1)

distinct_df2 = purchases_df.dropDuplicates(["event_name"])
display(distinct_df2)

# COMMAND ----------

# DBTITLE 0,--i18n-f0d53260-4525-4942-b901-ce351f55d4c9
# MAGIC %md
# MAGIC ### 4. 불필요한 열 삭제
# MAGIC 이벤트 유형이 하나뿐이므로 **`purchases_df`**에서 **`event_name`**을 삭제합니다.

# COMMAND ----------

# TODO
final_df = purchases_df.drop("event_name")
display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8ea4b4df-c55e-4015-95ee-1caccafa44d6
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(final_df.columns) == expected_columns)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-ed143b89-079a-44e9-87f3-9c8d242f09d2
# MAGIC %md
# MAGIC
# MAGIC ### 5. 3단계를 제외한 위의 모든 단계를 연결합니다.

# COMMAND ----------

# TODO
final_df = (events_df
  .withColumn('revenue',events_df['ecommerce.purchase_revenue_in_usd'])
  .filter(col("revenue").isNotNull())
  .drop("event_name")
)

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d7b35e13-8c38-4e17-b676-2146b64045fe
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

assert(final_df.count() == 9056)
print("All test pass")

# COMMAND ----------

expected_columns = {"device", "ecommerce", "event_previous_timestamp", "event_timestamp",
                    "geo", "items", "revenue", "traffic_source",
                    "user_first_touch_timestamp", "user_id"}
assert(set(final_df.columns) == expected_columns)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-03e7e278-385e-4afe-8268-229a1984a654
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()