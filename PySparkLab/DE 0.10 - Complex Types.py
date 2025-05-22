# Databricks notebook source
# DBTITLE 0,--i18n-10b1d2c4-b58e-4a1c-a4be-29c3c07c7832
# MAGIC %md
# MAGIC # 복합 타입
# MAGIC
# MAGIC 컬렉션 및 문자열 작업을 위한 내장 함수를 살펴보세요.
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. 배열 처리에 컬렉션 함수 적용
# MAGIC 1. DataFrames 결합
# MAGIC
# MAGIC ##### 방법
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:**`union`**, **`unionByName`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">내장 함수</a>:
# MAGIC - 집계: **`collect_set`**
# MAGIC - 컬렉션: **`array_contains`**, **`element_at`**, **`explode`**
# MAGIC - 문자열: **`split`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.10

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.table("sales")

display(df)

# COMMAND ----------

# You will need this DataFrame for a later exercise
details_df = (df
              .withColumn("items", explode("items"))
              .select("email", "items.item_name")
              .withColumn("details", split(col("item_name"), " "))
             )
display(details_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4306b462-66db-488e-8106-66e1bbbd30d9
# MAGIC %md
# MAGIC
# MAGIC ### 문자열 함수
# MAGIC 다음은 문자열을 조작하는 데 사용할 수 있는 몇 가지 내장 함수입니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | translate | src의 모든 문자를 replaceString의 문자로 변환합니다. |
# MAGIC | regexp_replace | 지정된 문자열 값에서 regexp와 일치하는 모든 부분 문자열을 rep로 바꿉니다. |
# MAGIC | regexp_extract | 지정된 문자열 열에서 Java 정규식과 일치하는 특정 그룹을 추출합니다. |
# MAGIC | ltrim | 지정된 문자열 열에서 선행 공백 문자를 제거합니다. |
# MAGIC | lower | 문자열 열을 소문자로 변환합니다. |
# MAGIC | split | 주어진 패턴과 일치하는 문자열을 중심으로 str을 나눕니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-12dcf4bd-35e7-4316-b03f-ec076e9739c7
# MAGIC %md
# MAGIC 예를 들어, **`email`** 열을 구문 분석해야 한다고 가정해 보겠습니다. **`split`** 함수를 사용하여 도메인을 분할하고 처리할 것입니다.

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

display(df.select(split(df.email, '@', 0).alias('email_handle')))

# COMMAND ----------

# DBTITLE 0,--i18n-4be5a98f-61e3-483b-b7a6-af4b671eb057
# MAGIC %md
# MAGIC
# MAGIC ### 컬렉션 함수
# MAGIC
# MAGIC 다음은 배열 작업에 사용할 수 있는 몇 가지 내장 함수입니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | array_contains | 배열이 null이면 null을 반환하고, 배열에 값이 있으면 true를 반환하고, 그렇지 않으면 false를 반환합니다. |
# MAGIC | element_at | 주어진 인덱스에 있는 배열의 요소를 반환합니다. 배열 요소는 **1**부터 번호가 매겨집니다. |
# MAGIC | explode | 주어진 배열 또는 맵 열의 각 요소에 대해 새 행을 생성합니다. |
# MAGIC | collect_set | 중복 요소가 제거된 객체 집합을 반환합니다. |

# COMMAND ----------

mattress_df = (details_df
               .filter(array_contains(col("details"), "Mattress"))
               .withColumn("size", element_at(col("details"), 2)))
display(mattress_df)

# COMMAND ----------

# DBTITLE 0,--i18n-110c4036-291a-4ca8-a61c-835e2abb1ffc
# MAGIC %md
# MAGIC
# MAGIC ### 집계 함수
# MAGIC
# MAGIC 다음은 일반적으로 GroupedData에서 배열을 생성하는 데 사용할 수 있는 몇 가지 내장 집계 함수입니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | collect_list | 그룹 내의 모든 값으로 구성된 배열을 반환합니다. |
# MAGIC | collect_set | 그룹 내의 모든 고유 값으로 구성된 배열을 반환합니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-1e0888f3-4334-4431-a486-c58f6560210f
# MAGIC %md
# MAGIC
# MAGIC 이메일 주소별로 주문된 매트리스 크기를 확인하고 싶다고 가정해 보겠습니다. 이를 위해 **collect_set** 함수를 사용할 수 있습니다.

# COMMAND ----------

size_df = mattress_df.groupBy("email").agg(collect_set("size").alias("size options"))

display(size_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7304a528-9b97-4806-954f-56cbf7bed6dc
# MAGIC %md
# MAGIC
# MAGIC ##Union 및 unionByName
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="경고"> DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.union.html" target="_blank">**`union`**</a> 메서드는 표준 SQL처럼 위치를 기준으로 열을 확인합니다. 두 DataFrame의 스키마가 열 순서를 포함하여 정확히 동일한 경우에만 사용해야 합니다. 반면, DataFrame <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.unionByName.html" target="_blank">**`unionByName`**</a> 메서드는 이름을 기준으로 열을 확인합니다. 이는 SQL의 UNION ALL과 동일합니다. 둘 다 중복을 제거하지 않습니다.
# MAGIC
# MAGIC 아래는 두 데이터프레임에 **`union`**이 적합한 일치하는 스키마가 있는지 확인하는 코드입니다.

# COMMAND ----------

mattress_df.schema==size_df.schema

# COMMAND ----------

# DBTITLE 0,--i18n-7bb80944-614a-487f-85b8-bb3983e259ed
# MAGIC %md
# MAGIC
# MAGIC 간단한 **`select`** 문으로 두 스키마를 일치시킬 수 있다면, **`union`**을 사용할 수 있습니다.

# COMMAND ----------

union_count = mattress_df.select("email").union(size_df.select("email")).count()

mattress_count = mattress_df.count()
size_count = size_df.count()

mattress_count + size_count == union_count

# COMMAND ----------

# DBTITLE 0,--i18n-52fdd386-f0dc-4850-87c7-4775fd2c64d4
# MAGIC %md
# MAGIC
# MAGIC ## Clean up classroom

# COMMAND ----------

DA.cleanup()