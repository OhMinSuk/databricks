# Databricks notebook source
# DBTITLE 0,--i18n-ab2602b5-4183-4f33-8063-cfc03fcb1425
# MAGIC %md
# MAGIC # DataFrame & Column
# MAGIC ##### 목표
# MAGIC 1. 열 생성
# MAGIC 1. 열 부분 집합 생성
# MAGIC 1. 열 추가 또는 교체
# MAGIC 1. 행 부분 집합 생성
# MAGIC 1. 행 정렬
# MAGIC
# MAGIC ##### 방법
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, **`operators`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.03

# COMMAND ----------

# DBTITLE 0,--i18n-ef990348-e991-4edb-bf45-84de46a34759
# MAGIC %md
# MAGIC
# MAGIC events 데이터 세트를 사용해 보겠습니다.

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4ea9a278-1eb6-45ad-9f96-34e0fd0da553
# MAGIC %md
# MAGIC
# MAGIC ## 열 표현식
# MAGIC
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">열</a>은 표현식을 사용하여 DataFrame의 데이터를 기반으로 계산되는 논리적 구문입니다.
# MAGIC
# MAGIC DataFrame의 기존 열을 기반으로 새 열을 생성합니다.

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 0,--i18n-d87b8303-8f78-416e-99b0-b037caf2107a
# MAGIC %md
# MAGIC Scala는 DataFrame의 기존 열을 기반으로 새 열을 생성하는 추가 구문을 지원합니다.

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------

# DBTITLE 0,--i18n-64238a77-0877-4bd4-af46-a9a8bd4763c6
# MAGIC %md
# MAGIC
# MAGIC ### 열 연산자 및 메서드
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | 수학 및 비교 연산자 |
# MAGIC | ==, != | 같음 및 같지 않음 테스트(Scala 연산자는 **`===`** 및 **`=!=`**입니다) |
# MAGIC | alias | 열에 별칭을 지정합니다 |
# MAGIC | cast, astype | 열을 다른 데이터 유형으로 변환합니다 |
# MAGIC | isNull, isNotNull, isNan | null인지, null이 아닌지, NaN인지 |
# MAGIC | asc, desc | 열의 오름차순/내림차순 정렬 표현식을 반환합니다 |

# COMMAND ----------

# DBTITLE 0,--i18n-6d68007e-3dbf-4f18-bde4-6990299ef086
# MAGIC %md
# MAGIC
# MAGIC columns, operators, and methods를 사용하여 복잡한 표현식을 만듭니다.

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# DBTITLE 0,--i18n-7c1c0688-8f9f-4247-b8b8-bb869414b276
# MAGIC %md
# MAGIC 다음은 DataFrame 컨텍스트에서 이러한 열 표현식을 사용하는 예입니다.

# COMMAND ----------

rev_df = (events_df
         .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
         .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
         .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
         .sort(col("avg_purchase_revenue").desc())
        )

display(rev_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7ba60230-ecd3-49dd-a4c8-d964addc6692
# MAGIC %md
# MAGIC
# MAGIC ## DataFrame 변환 메서드
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | **`select`** | 각 요소에 대해 주어진 표현식을 계산하여 새 DataFrame을 반환합니다. |
# MAGIC | **`drop`** | 열을 삭제한 새 DataFrame을 반환합니다. |
# MAGIC | **`withColumnRenamed`** | 열 이름이 변경된 새 DataFrame을 반환합니다. |
# MAGIC | **`withColumn`** | 열을 추가하거나 이름이 같은 기존 열을 대체하여 새 DataFrame을 반환합니다. |
# MAGIC | **`filter`**, **`where`** | 주어진 조건을 사용하여 행을 필터링합니다. |
# MAGIC | **`sort`**, **`orderBy`** | 주어진 표현식으로 정렬된 새 DataFrame을 반환합니다. |
# MAGIC | **`dropDuplicates`**, **`distinct`** | 중복 행을 제거한 새 DataFrame을 반환합니다. |
# MAGIC | **`limit`** | 처음 n개 행을 가져와 새 DataFrame을 반환합니다. |
# MAGIC | **`groupBy`** | 지정된 열을 사용하여 DataFrame을 그룹화하여 해당 열에 대한 집계를 실행할 수 있습니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-3e95eb92-30e4-44aa-8ee0-46de94c2855e
# MAGIC %md
# MAGIC
# MAGIC ### 열 부분 집합
# MAGIC DataFrame 변환을 사용하여 열 부분 집합 만들기

# COMMAND ----------

# DBTITLE 0,--i18n-987cfd99-8e06-447f-b1c7-5f104cd5ed2f
# MAGIC %md
# MAGIC
# MAGIC #### **`select()`**
# MAGIC 열 목록 또는 열 기반 표현식을 선택합니다.

# COMMAND ----------

devices_df = events_df.select("user_id", "device")
display(devices_df)

# COMMAND ----------

from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id", 
    col("geo.city").alias("city"), 
    col("geo.state").alias("state")
)
display(locations_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8d556f84-bfcd-436a-a3dd-893143ce620e
# MAGIC %md
# MAGIC
# MAGIC #### **`selectExpr()`**
# MAGIC SQL 표현식 목록을 선택합니다.

# COMMAND ----------

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# DBTITLE 0,--i18n-452f7fb3-3866-4835-827f-6d359f364046
# MAGIC %md
# MAGIC
# MAGIC #### **`drop()`**
# MAGIC 주어진 열을 삭제한 후 새 DataFrame을 반환합니다. 문자열 또는 Column 객체로 지정됩니다.
# MAGIC
# MAGIC 문자열을 사용하여 여러 열을 지정합니다.

# COMMAND ----------

anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

no_sales_df = events_df.drop(col("ecommerce"))
display(no_sales_df)


# COMMAND ----------

# DBTITLE 0,--i18n-b11609a3-11d5-453b-b713-15131b277066
# MAGIC %md
# MAGIC
# MAGIC ### 열 추가 또는 바꾸기
# MAGIC DataFrame 변환을 사용하여 열을 추가하거나 바꿉니다.

# COMMAND ----------

# DBTITLE 0,--i18n-f29a47d9-9567-40e5-910b-73c640cc61ca
# MAGIC %md
# MAGIC
# MAGIC #### **`withColumn()`**
# MAGIC 같은 이름의 열을 추가하거나 기존 열을 대체하여 새 DataFrame을 반환합니다.

# COMMAND ----------

mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

# COMMAND ----------

purchase_quantity_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchase_quantity_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-969c0d9f-202f-405a-8c66-ef29076b48fc
# MAGIC %md
# MAGIC
# MAGIC #### **`withColumnRenamed()`**
# MAGIC 열 이름이 변경된 새 DataFrame을 반환합니다.

# COMMAND ----------

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# DBTITLE 0,--i18n-23b0a9ef-58d5-4973-a610-93068a998d5e
# MAGIC %md
# MAGIC
# MAGIC ### 행 부분 집합
# MAGIC DataFrame 변환을 사용하여 행 부분 집합을 만듭니다.

# COMMAND ----------

# DBTITLE 0,--i18n-4ada6444-7345-41f7-aaa2-1de2d729483f
# MAGIC %md
# MAGIC
# MAGIC #### **`filter()`**
# MAGIC 주어진 SQL 표현식 또는 열 기반 조건을 사용하여 행을 필터링합니다.
# MAGIC
# MAGIC ##### 별칭: **`where`**

# COMMAND ----------

purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
display(purchases_df)

# COMMAND ----------

revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenue_df)

# COMMAND ----------

android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4d6a79eb-3989-43e1-8c28-5b976a513f5f
# MAGIC %md
# MAGIC
# MAGIC #### **`dropDuplicates()`**
# MAGIC 중복 행을 제거한 새 DataFrame을 반환합니다. 선택적으로 일부 열만 고려합니다.
# MAGIC
# MAGIC ##### 별칭: **`distinct`**

# COMMAND ----------

display(events_df.distinct())

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(["user_id"])
display(distinct_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-433c57f4-ce40-48c9-8d04-d3a13c398082
# MAGIC %md
# MAGIC
# MAGIC #### **`limit()`**
# MAGIC 처음 n개 행을 가져와 새 DataFrame을 반환합니다.

# COMMAND ----------

limit_df = events_df.limit(100)
display(limit_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d4117305-e742-497e-964d-27a7b0c395cd
# MAGIC %md
# MAGIC
# MAGIC ### 행 정렬
# MAGIC DataFrame 변환을 사용하여 행을 정렬합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-16b3c7fe-b5f2-4564-9e8e-4f677777c50c
# MAGIC %md
# MAGIC
# MAGIC #### **`sort()`**
# MAGIC 주어진 열 또는 표현식을 기준으로 정렬된 새 DataFrame을 반환합니다.
# MAGIC
# MAGIC ##### 별칭: **`orderBy`**

# COMMAND ----------

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
display(decrease_timestamp_df)

# COMMAND ----------

increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increase_sessions_df)

# COMMAND ----------

decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decrease_sessions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-555c663e-3f62-4478-9d76-c9ee090beca1
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()