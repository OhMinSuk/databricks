# Databricks notebook source
# DBTITLE 0,--i18n-15802400-50d0-40e5-854c-89b08b50c14e
# MAGIC %md
# MAGIC
# MAGIC # 쿼리 최적화
# MAGIC
# MAGIC 논리적 최적화와 조건자 푸시다운을 사용하거나 사용하지 않는 예제를 포함하여 여러 예제에 대한 쿼리 계획 및 최적화를 살펴보겠습니다.
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. 논리적 최적화
# MAGIC 1. 조건자 푸시다운
# MAGIC 1. 조건자 푸시다운 사용 안 함
# MAGIC
# MAGIC ##### 방법
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.explain.html#pyspark.sql.DataFrame.explain" target="_blank">DataFrame</a>: **`explain`**

# COMMAND ----------

# DBTITLE 0,--i18n-8cb4efc1-cf1b-42a5-9cf3-109ccc0b5bb5
# MAGIC %md
# MAGIC
# MAGIC 설정된 셀을 실행하고 초기 DataFrame을 **`df`** 변수에 저장해 보겠습니다. 이 DataFrame을 표시하면 이벤트 데이터가 표시됩니다.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.13

# COMMAND ----------

df = spark.read.table("events")
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-63293e50-d68e-468d-a3c2-08608c66fb1d
# MAGIC %md
# MAGIC
# MAGIC ### 논리적 최적화
# MAGIC
# MAGIC **`explain(..)`**은 쿼리 계획을 출력하며, 선택적으로 지정된 설명 모드에 따라 형식이 지정됩니다. 다음 논리적 계획과 물리적 계획을 비교하면서 Catalyst가 여러 **`filter`** 변환을 어떻게 처리했는지 확인하세요.

# COMMAND ----------

from pyspark.sql.functions import col

limit_events_df = (df
                   .filter(col("event_name") != "reviews")
                   .filter(col("event_name") != "checkout")
                   .filter(col("event_name") != "register")
                   .filter(col("event_name") != "email_coupon")
                   .filter(col("event_name") != "cc_info")
                   .filter(col("event_name") != "delivery")
                   .filter(col("event_name") != "shipping_info")
                   .filter(col("event_name") != "press")
                  )

limit_events_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-cc9b8d61-bb89-4961-819d-d135ec4f4aac
# MAGIC %md
# MAGIC 물론, 원래는 단일 **`filter`** 조건을 사용하여 쿼리를 직접 작성할 수도 있었습니다. 이전 쿼리 계획과 다음 쿼리 계획을 비교해 보세요.

# COMMAND ----------

better_df = (df
             .filter((col("event_name").isNotNull()) &
                     (col("event_name") != "reviews") &
                     (col("event_name") != "checkout") &
                     (col("event_name") != "register") &
                     (col("event_name") != "email_coupon") &
                     (col("event_name") != "cc_info") &
                     (col("event_name") != "delivery") &
                     (col("event_name") != "shipping_info") &
                     (col("event_name") != "press"))
            )

better_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-27a81fc2-4aec-46bf-89c0-bb8b90fa9e17
# MAGIC %md
# MAGIC 물론, 의도적으로 다음 코드를 작성하지는 않겠지만, 길고 복잡한 쿼리에서는 중복된 필터 조건을 알아차리지 못할 수도 있습니다. Catalyst가 이 쿼리에서 어떤 작업을 수행하는지 살펴보겠습니다.

# COMMAND ----------

stupid_df = (df
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
             .filter(col("event_name") != "finalize")
            )

stupid_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-90d320e9-9295-4869-8042-217652fe355b
# MAGIC %md
# MAGIC ### 캐싱
# MAGIC
# MAGIC 기본적으로 DataFrame의 데이터는 쿼리 처리 중에만 Spark 클러스터에 존재하며, 이후 클러스터에 자동으로 저장되지 않습니다. (Spark는 데이터 저장 시스템이 아니라 데이터 처리 엔진입니다.) Spark의 **cache** 메서드를 호출하여 클러스터에 DataFrame을 저장하도록 명시적으로 요청할 수 있습니다.
# MAGIC
# MAGIC DataFrame을 캐싱하는 경우, 더 이상 필요하지 않으면 **unpersist**를 호출하여 캐시에서 명시적으로 제거해야 합니다.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_best_32.png" alt="모범 사례"> 다음과 같이 동일한 DataFrame을 여러 번 사용할 것이 확실한 경우 DataFrame을 캐싱하는 것이 적합할 수 있습니다.
# MAGIC
# MAGIC - 탐색적 데이터 분석
# MAGIC - 머신 러닝 모델 학습
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="경고"> 이러한 사용 사례 외에는 DataFrame을 캐싱하지 **않아야 합니다.** 애플리케이션의 성능이 *저하*될 가능성이 높습니다.
# MAGIC
# MAGIC - 캐싱은 작업 실행에 사용될 수 있는 클러스터 리소스를 소모합니다.
# MAGIC - 다음 예에서 볼 수 있듯이 캐싱으로 인해 Spark가 쿼리 최적화를 수행하지 못할 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-2256e20c-d69c-4ce8-ae74-c513a8d673f5
# MAGIC %md
# MAGIC
# MAGIC ### 조건자 푸시다운
# MAGIC
# MAGIC 다음은 JDBC 소스에서 읽어오는 예제입니다. Catalyst는 *조건자 푸시다운*이 발생할 수 있다고 판단합니다.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Ensure that the driver class is loaded
# MAGIC Class.forName("org.postgresql.Driver")

# COMMAND ----------

jdbc_url = "jdbc:postgresql://server1.training.databricks.com/training"

# Username and Password w/read-only rights
conn_properties = {
    "user" : "training",
    "password" : "training"
}

pp_df = (spark
         .read
         .jdbc(url=jdbc_url,                 # the JDBC URL
               table="training.people_1m",   # the name of the table
               column="id",                  # the name of a column of an integral type that will be used for partitioning
               lowerBound=1,                 # the minimum value of columnName used to decide partition stride
               upperBound=1000000,           # the maximum value of columnName used to decide partition stride
               numPartitions=8,              # the number of partitions/connections
               properties=conn_properties    # the connection properties
              )
         .filter(col("gender") == "M")   # Filter the data by gender
        )

pp_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-b067b782-e86b-4284-80f4-4faedfb0953e
# MAGIC %md
# MAGIC
# MAGIC **Scan**에 **Filter**가 없고 **PushedFilters**가 있다는 점에 유의하세요. 필터 작업은 데이터베이스에 푸시되고 일치하는 레코드만 Spark로 전송됩니다. 이렇게 하면 Spark가 수집해야 하는 데이터 양을 크게 줄일 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-e378204a-cce7-4903-a1e4-f2f3e387c4f5
# MAGIC %md
# MAGIC
# MAGIC ### 조건문 푸시다운 없음
# MAGIC
# MAGIC 반면에, 필터링 전에 데이터를 캐싱하면 조건문 푸시다운이 발생할 가능성이 없어집니다.

# COMMAND ----------

cached_df = (spark
            .read
            .jdbc(url=jdbc_url,
                  table="training.people_1m",
                  column="id",
                  lowerBound=1,
                  upperBound=1000000,
                  numPartitions=8,
                  properties=conn_properties
                 )
            )

cached_df.cache()
filtered_df = cached_df.filter(col("gender") == "M")

filtered_df.explain(True)

# COMMAND ----------

# DBTITLE 0,--i18n-7923a69e-43bd-4a4d-8de9-ac83d6eee749
# MAGIC %md
# MAGIC
# MAGIC 이전 예제에서 보았던 **Scan**(JDBC 읽기) 외에도, 여기서는 설명 계획에서 **Filter**가 뒤따르는 **InMemoryTableScan**도 볼 수 있습니다.
# MAGIC
# MAGIC 즉, Spark는 데이터베이스에서 모든 데이터를 읽고 캐시한 다음, 캐시에서 스캔하여 필터 조건과 일치하는 레코드를 찾아야 했습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-20c1b03f-3627-40bf-b426-f24cb3111430
# MAGIC %md
# MAGIC 사용 후 깨끗이 치우는 걸 잊지 마세요!

# COMMAND ----------

cached_df.unpersist()

# COMMAND ----------

# DBTITLE 0,--i18n-be8bb4b0-cdcc-4457-baa3-145a71d04b35
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()