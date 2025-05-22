# Databricks notebook source
# DBTITLE 0,--i18n-da4e23df-1911-4f58-9030-65da697d7b61
# MAGIC %md
# MAGIC # Spark SQL 랩
# MAGIC
# MAGIC ##### 작업
# MAGIC 1. **`events`** 테이블에서 DataFrame 생성
# MAGIC 1. DataFrame 표시 및 스키마 검사
# MAGIC 1. **`macOS`** 이벤트 필터링 및 정렬에 변환 적용
# MAGIC 1. 결과 개수 계산 및 처음 5개 행 가져오기
# MAGIC 1. SQL 쿼리를 사용하여 동일한 DataFrame 생성
# MAGIC
# MAGIC ##### 메서드
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> 변환: **`select`**, **`where`**, **`orderBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a> 작업: **`select`**, **`count`**, **`take`**
# MAGIC - 기타 <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> 메서드: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.02L

# COMMAND ----------

# DBTITLE 0,--i18n-e0f3f405-8c97-46d1-8550-fb8ff14e5bd6
# MAGIC %md
# MAGIC
# MAGIC ### 1. **`events`** 테이블에서 DataFrame 생성
# MAGIC - SparkSession을 사용하여 **`events`** 테이블에서 DataFrame을 생성합니다.

# COMMAND ----------

# TODO
events_df = spark.table("events")

# COMMAND ----------

# DBTITLE 0,--i18n-fb5458a0-b475-4d77-b06b-63bb9a18d586
# MAGIC %md
# MAGIC
# MAGIC ### 2. DataFrame 표시 및 스키마 검사
# MAGIC - 위의 메서드를 사용하여 DataFrame 내용과 스키마를 검사합니다.

# COMMAND ----------

# TODO
events_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-76adfcb2-f182-485c-becd-9e569d4148b6
# MAGIC %md
# MAGIC
# MAGIC ### 3. **`macOS`** 이벤트 필터링 및 정렬에 변환 적용
# MAGIC - **`device`**가 **`macOS`**인 행 필터링
# MAGIC - **`event_timestamp`**로 행 정렬
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="힌트"> 필터 SQL 표현식에 작은따옴표와 큰따옴표를 사용하세요.

# COMMAND ----------

# TODO
mac_df = (events_df
          .where("device = 'macOS'")
          .sort("event_timestamp")
         )

# COMMAND ----------

# DBTITLE 0,--i18n-81f8748d-a154-468b-b02e-ef1a1b6b2ba8
# MAGIC %md
# MAGIC
# MAGIC ### 4. 결과 개수를 세고 처음 5개 행 가져오기
# MAGIC - DataFrame 액션을 사용하여 행 개수를 세고 가져오기

# COMMAND ----------

# TODO
num_rows = mac_df.count()
rows = mac_df.take(5)

# COMMAND ----------

# DBTITLE 0,--i18n-4e340689-5d23-499a-9cd2-92509a646de6
# MAGIC %md
# MAGIC
# MAGIC **4.1: 과제 확인**

# COMMAND ----------

from pyspark.sql import Row

assert(num_rows == 97150)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cbb03650-db3b-42b3-96ee-54ea9b287ab5
# MAGIC %md
# MAGIC
# MAGIC ### 5. SQL 쿼리를 사용하여 동일한 DataFrame 생성
# MAGIC - SparkSession을 사용하여 **`events`** 테이블에 SQL 쿼리 실행
# MAGIC - SQL 명령을 사용하여 이전에 사용한 것과 동일한 필터 및 정렬 쿼리 작성

# COMMAND ----------

# TODO
mac_sql_df = spark.sql("select * from events where device = 'macOS' order by event_timestamp")

display(mac_sql_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1d203e4e-e835-4778-a245-daf30cc9f4bc
# MAGIC %md
# MAGIC
# MAGIC **5.1: 작업 확인**
# MAGIC - **device`** 열에는 **macOS`** 값만 표시되어야 합니다.
# MAGIC - 다섯 번째 행은 타임스탬프가 **1592539226602157`**인 이벤트여야 합니다.

# COMMAND ----------

verify_rows = mac_sql_df.take(5)
assert (mac_sql_df.select("device").distinct().count() == 1 and len(verify_rows) == 5 and verify_rows[0]['device'] == "macOS"), "Incorrect filter condition"
assert (verify_rows[4]['event_timestamp'] == 1592540419446946), "Incorrect sorting"
del verify_rows
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-5b3843b3-e615-4dc6-aec4-c8ce4d684464
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()