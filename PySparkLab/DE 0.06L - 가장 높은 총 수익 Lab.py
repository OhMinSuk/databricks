# Databricks notebook source
# DBTITLE 0,--i18n-8382e200-81c0-4bc3-9bdb-6aee604b0a8c
# MAGIC %md
# MAGIC # 가장 높은 총 수익 Lab
# MAGIC 가장 높은 총 수익을 창출하는 3개의 트래픽 소스를 확인하세요.
# MAGIC 1. 트래픽 소스별 매출 집계
# MAGIC 2. 총 매출 기준 상위 3개 트래픽 소스 가져오기
# MAGIC 3. 매출 열을 소수점 둘째 자리까지 정리
# MAGIC
# MAGIC ##### 메서드
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**, **`sort`**, **`limit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`alias`**, **`desc`**, **`cast`**, **`operators`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">내장 함수</a>: **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.06L

# COMMAND ----------

# DBTITLE 0,--i18n-b6ac5716-1668-4b34-8343-ee2d5c77cfad
# MAGIC %md
# MAGIC ### 설정
# MAGIC 아래 셀을 실행하여 시작 DataFrame **`df`**를 만듭니다.

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.table("events")
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-78acde42-f2b7-4b0f-9760-65fba886ef5b
# MAGIC %md
# MAGIC
# MAGIC ### 1. 트래픽 소스별 총 수익
# MAGIC - **`traffic_source`**로 그룹화
# MAGIC - **`revenue`**의 합계를 **`total_rev`**로 구합니다. 소수점 첫째 자리까지 반올림합니다(예: `nnnnn.n`).
# MAGIC - **`revenue`**의 평균을 **`avg_rev`**로 구합니다.
# MAGIC
# MAGIC 필요한 내장 함수를 모두 import하는 것을 잊지 마세요.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col, sum, avg

traffic_df = (df.groupBy("traffic_source")
              .agg(sum("revenue").alias("total_rev"),avg("revenue").alias("avg_rev"))
)

display(traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0ef1149d-9690-49a3-b717-ae2c38a166ed
# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

from pyspark.sql.functions import round

expected1 = [(620096.0, 1049.2318), (4026578.5, 986.1814), (1200591.0, 1067.192), (2322856.0, 1093.1087), (826921.0, 1086.6242), (404911.0, 1091.4043)]
test_df = traffic_df.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f5c20afb-2891-4fa2-8090-cca7e313354d
# MAGIC %md
# MAGIC
# MAGIC ### 2. 총 수익 기준 상위 3개 트래픽 소스 가져오기
# MAGIC - **`total_rev`**를 기준으로 내림차순 정렬
# MAGIC - 처음 3개 행으로 제한

# COMMAND ----------

# TODO
top_traffic_df = (traffic_df.sort(col("total_rev").desc()).limit(3)
)
display(top_traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-2ef23b0a-fc13-48ca-b1f3-9bc425023024
# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

expected2 = [(4026578.5, 986.1814), (2322856.0, 1093.1087), (1200591.0, 1067.192)]
test_df = top_traffic_df.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected2 == result2)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-04399ae3-d2ab-4ada-9a51-9f8cc21cc45a
# MAGIC %md
# MAGIC
# MAGIC ### 3. 매출 열의 소수점 이하 두 자리까지 제한
# MAGIC - **`avg_rev`** 및 **`total_rev`** 열을 수정하여 소수점 이하 두 자리까지 포함하는 숫자를 포함하도록 합니다.
# MAGIC - 동일한 이름의 **`withColumn()`**을 사용하여 이러한 열을 바꿉니다.
# MAGIC - 소수점 이하 두 자리까지 제한하려면 각 열에 100을 곱하고 long으로 변환한 후 100으로 나눕니다.

# COMMAND ----------

# TODO
final_df = (top_traffic_df
    .withColumn("avg_rev", (col("avg_rev") * 100).cast("long") / 100)
    .withColumn("total_rev", (col("total_rev") * 100).cast("long") / 100)
)

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d28b2d3a-6db6-4ba0-8a2c-a773635a69a4
# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**

# COMMAND ----------

expected3 = [(4026578.5, 986.18), (2322856.0, 1093.1), (1200591.0, 1067.19)]
result3 = [(row.total_rev, row.avg_rev) for row in final_df.collect()]

assert(expected3 == result3)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-4e2d3b62-bee6-497e-b6af-44064f759451
# MAGIC %md
# MAGIC ### 4. 보너스: 내장 수학 함수를 사용하여 다시 작성하세요.
# MAGIC 지정된 소수점 이하 자릿수로 반올림하는 내장 수학 함수를 찾으세요.

# COMMAND ----------

# TODO
bonus_df = (top_traffic_df
            .withColumn("avg_rev", round(col("avg_rev"), 2))
            .withColumn("total_rev", round(col("total_rev"), 2))
)

display(bonus_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6514f89e-1920-4804-96e4-a73998026023
# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

expected4 = [(4026578.5, 986.18), (2322856.0, 1093.11), (1200591.0, 1067.19)]
result4 = [(row.total_rev, row.avg_rev) for row in bonus_df.collect()]

assert(expected4 == result4)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-8f19689f-4cf7-4031-bc4c-eb2ece7cb56d
# MAGIC %md
# MAGIC
# MAGIC ### 5. 위의 모든 단계를 연결하세요

# COMMAND ----------

# TODO
chain_df = (df
            .groupBy("traffic_source")
            .agg(sum("revenue").alias("total_rev"),avg("revenue").alias("avg_rev"))
            .sort(col("total_rev").desc()).limit(3)
            .withColumn("avg_rev", round(col("avg_rev"), 2))
            .withColumn("total_rev", round(col("total_rev"), 2))
)

display(chain_df)

# COMMAND ----------

# DBTITLE 0,--i18n-53c0b070-d2bc-45e9-a3ea-da25a375d6f3
# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

expected5 = [(4026578.5, 986.18), (2322856.0, 1093.11), (1200591.0, 1067.19)]
result5 = [(row.total_rev, row.avg_rev) for row in chain_df.collect()]

assert(expected5 == result5)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f8095ac2-c3cf-4bbb-b20b-eed1891489e0
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()