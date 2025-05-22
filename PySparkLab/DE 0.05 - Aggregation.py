# Databricks notebook source
# DBTITLE 0,--i18n-3fbfc7bd-6ef2-4fea-b8a2-7f949cd84044
# MAGIC %md
# MAGIC # 집계
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. 지정된 열을 기준으로 데이터 그룹화
# MAGIC 1. 그룹화된 데이터 메서드를 적용하여 데이터 집계
# MAGIC 1. 내장 함수를 적용하여 데이터 집계
# MAGIC
# MAGIC ##### 메서드
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">그룹화된 데이터</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">내장 함수</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.05

# COMMAND ----------

# DBTITLE 0,--i18n-88095892-40a1-46dd-a809-19186953d968
# MAGIC %md
# MAGIC
# MAGIC BedBricks 이벤트 데이터 세트를 사용해 보겠습니다.

# COMMAND ----------

df = spark.table("events")
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-a04aa8bd-35f0-43df-b137-6e34aebcded1
# MAGIC %md
# MAGIC
# MAGIC ### Grouping data

# COMMAND ----------

# DBTITLE 0,--i18n-cd0936f7-cd8a-4277-bbaf-d3a6ca2c29ec
# MAGIC %md
# MAGIC
# MAGIC ### groupBy
# MAGIC DataFrame의 **`groupBy`** 메서드를 사용하여 그룹화된 데이터 객체를 생성합니다.
# MAGIC
# MAGIC 이 그룹화된 데이터 객체는 Scala에서는 **`RelationalGroupedDataset`**, Python에서는 **`GroupedData`**라고 합니다.

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# DBTITLE 0,--i18n-7918f032-d001-4e38-bd75-51eb68c41ffa
# MAGIC %md
# MAGIC
# MAGIC ### 그룹화된 데이터 메서드
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a> 객체에서 다양한 집계 메서드를 사용할 수 있습니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | agg | 일련의 집계 열을 지정하여 집계를 계산합니다. |
# MAGIC | avg | 각 그룹의 각 숫자 열에 대한 평균값을 계산합니다. |
# MAGIC | count | 각 그룹의 행 수를 센다. |
# MAGIC | max | 각 그룹의 각 숫자 열에 대한 최대값을 계산합니다. |
# MAGIC | mean | 각 그룹의 각 숫자 열에 대한 평균값을 계산합니다. |
# MAGIC | min | 각 그룹의 각 숫자 열에 대한 최소값을 계산합니다. |
# MAGIC | pivot | 현재 DataFrame의 열을 피벗(행의 값을 열로 바꾸기)하고 지정된 집계를 수행합니다. |
# MAGIC | sum | 각 그룹의 각 숫자 열에 대한 합계를 계산합니다. |

# COMMAND ----------

event_counts_df = df.groupBy("event_name").count()
display(event_counts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-bf63efea-c4f7-4ff9-9d42-4de245617d97
# MAGIC %md
# MAGIC
# MAGIC 여기서는 각 항목의 평균 구매 수익을 구합니다.

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-b11167f4-c270-4f7b-b967-75538237c915
# MAGIC %md
# MAGIC 여기에는 각 주와 도시의 조합에 대한 총 수량과 구매 수익 합계가 나와 있습니다.

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

# DBTITLE 0,--i18n-62a4e852-249a-4dbf-b47a-e85a64cbc258
# MAGIC %md
# MAGIC
# MAGIC ## 내장 함수
# MAGIC DataFrame 및 Column 변환 메서드 외에도 Spark의 내장 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL 함수</a> 모듈에는 유용한 함수가 많이 있습니다.
# MAGIC
# MAGIC Scala에서는 <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>이고, Python에서는 <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a>입니다. 이 모듈의 함수는 코드로 가져와야 합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-68f06736-e457-4893-8c0d-be83c818bd91
# MAGIC %md
# MAGIC
# MAGIC ### 집계 함수
# MAGIC
# MAGIC 다음은 집계에 사용할 수 있는 몇 가지 기본 제공 함수입니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | 그룹 내 고유 항목의 대략적인 개수를 반환합니다. |
# MAGIC | avg | 그룹 내 값의 평균을 반환합니다. |
# MAGIC | collect_list | 중복된 객체 목록을 반환합니다. |
# MAGIC | corr | 두 숫자형 열의 상관계수를 반환합니다. |
# MAGIC | max | 각 그룹의 각 숫자 열에 대한 최댓값을 계산합니다. |
# MAGIC | mean | 각 그룹의 각 숫자 열에 대한 평균값을 계산합니다. |
# MAGIC | stddev_samp | 그룹 내 표현식의 표본 표준 편차를 반환합니다. |
# MAGIC | sumDistinct | 표현식 내 고유 값의 합계를 반환합니다. |
# MAGIC | var_pop | 그룹 내 값의 모분산을 반환합니다. |
# MAGIC
# MAGIC 그룹화된 데이터 메서드 <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a>를 사용하여 내장 집계 함수를 적용합니다.
# MAGIC
# MAGIC 이렇게 하면 결과 열에 <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>와 같은 다른 변환을 적용할 수 있습니다.

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-875e6ef8-fec3-4468-ab86-f3f6946b281f
# MAGIC %md
# MAGIC
# MAGIC 그룹화된 데이터에 여러 집계 함수 적용

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6bb4a15f-4f5d-4f70-bf50-4be00167d9fa
# MAGIC %md
# MAGIC
# MAGIC ### 수학 함수
# MAGIC 다음은 수학 연산을 위한 내장 함수입니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | ceil | 주어진 열의 상한값을 계산합니다. |
# MAGIC | cos | 주어진 값의 코사인을 계산합니다. |
# MAGIC | log | 주어진 값의 자연 로그를 계산합니다. |
# MAGIC | round | HALF_UP 반올림 모드를 사용하여 e 열의 값을 소수점 이하 0자리로 반올림하여 반환합니다. |
# MAGIC | sqrt | 지정된 부동 소수점 값의 제곱근을 계산합니다. |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

# DBTITLE 0,--i18n-d03fb77f-5e4c-43b8-a293-884cd7cb174c
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()