-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad42144-605b-486f-ad65-ca24b47b1924
-- MAGIC %md
-- MAGIC
-- MAGIC # 데이터 정리
-- MAGIC
-- MAGIC 데이터를 검사하고 정리할 때 데이터세트에 적용할 변환을 표현하기 위해 다양한 열 표현식과 쿼리를 작성해야 합니다.
-- MAGIC
-- MAGIC 열 표현식은 기존 열, 연산자 및 내장 함수로 구성됩니다. **SELECT`** 문에서 열 표현식을 사용하여 새 열을 생성하는 변환을 표현할 수 있습니다.
-- MAGIC
-- MAGIC Spark SQL에서는 변환을 표현하는 데 사용할 수 있는 많은 표준 SQL 쿼리 명령(예: **DISTINCT`**, **WHERE`**, **GROUP BY`** 등)을 사용할 수 있습니다.
-- MAGIC
-- MAGIC 이 노트북에서는 기존 시스템과 다를 수 있는 몇 가지 개념을 살펴보고 일반적인 작업에 유용한 몇 가지 함수를 소개합니다.
-- MAGIC
-- MAGIC **NULL`** 값과 관련된 동작과 문자열 및 날짜/시간 필드의 서식 지정에 특히 주의 깊게 살펴보겠습니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 수업을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - 데이터세트 요약 및 null 동작 설명
-- MAGIC - 중복 항목 검색 및 제거
-- MAGIC - 예상 개수, 누락된 값 및 중복 레코드에 대한 데이터세트 검증
-- MAGIC - 일반적인 변환을 적용하여 데이터 정리 및 변환

-- COMMAND ----------

-- DBTITLE 0,--i18n-2a604768-1aac-40e2-8396-1e15de60cc96
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설치 실행
-- MAGIC
-- MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-31202e20-c326-4fa0-8892-ab9308b4b6f0
-- MAGIC %md
-- MAGIC ## 데이터 개요
-- MAGIC
-- MAGIC 다음 스키마를 갖는 **`users_dirty`** 테이블의 새 사용자 레코드를 살펴보겠습니다.
-- MAGIC
-- MAGIC | field | type | description |
-- MAGIC |---|---|---|
-- MAGIC | user_id | string | unique identifier |
-- MAGIC | user_first_touch_timestamp | long | epoch 이후 사용자 레코드가 생성된 시간(마이크로초) |
-- MAGIC | email | string | 사용자가 작업을 완료하기 위해 제공한 가장 최근 이메일 주소 |
-- MAGIC | updated | timestamp | 이 레코드가 마지막으로 업데이트된 시간 |
-- MAGIC
-- MAGIC 먼저 데이터의 각 필드에 있는 값을 세어 보겠습니다.

-- COMMAND ----------

SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
FROM users_dirty

-- COMMAND ----------

-- DBTITLE 0,--i18n-c414c24e-3b72-474b-810d-c3df32032c26
-- MAGIC %md
-- MAGIC
-- MAGIC ## 누락된 데이터 검사
-- MAGIC
-- MAGIC 위의 계산 결과를 보면 모든 필드에 적어도 몇 개의 null 값이 있는 것으로 보입니다.
-- MAGIC
-- MAGIC **참고:** `count()`를 포함한 일부 수학 함수에서 null 값이 제대로 작동하지 않습니다.
-- MAGIC
-- MAGIC - **`count(col)`**는 특정 열이나 표현식의 개수를 셀 때 **`NULL`** 값을 건너뜁니다.
-- MAGIC - **`count(*)`**는 총 행 수(`NULL`** 값만 있는 행 포함)를 계산하는 특수한 경우입니다.
-- MAGIC
-- MAGIC 필드에서 null 값을 계산하려면 해당 필드가 null인 레코드를 필터링해야 합니다. 다음 중 하나를 사용합니다.
-- MAGIC **`count_if(col IS NULL)`** 또는 **`count(*)`**를 `col IS NULL`** 필터와 함께 사용합니다.
-- MAGIC
-- MAGIC 아래 두 명령문 모두 이메일이 누락된 레코드를 올바르게 계산합니다.

-- COMMAND ----------

SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("users_dirty")
-- MAGIC
-- MAGIC usersDF.selectExpr("count_if(email IS NULL)")
-- MAGIC usersDF.where(col("email").isNull()).count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-ea1ca35c-6421-472b-b70b-4f36bdab6d79
-- MAGIC %md
-- MAGIC
-- MAGIC ## 중복 행 제거
-- MAGIC **`DISTINCT *`**를 사용하면 전체 행에 동일한 값이 포함된 실제 중복 레코드를 제거할 수 있습니다.

-- COMMAND ----------

SELECT DISTINCT(*) FROM users_dirty

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().display()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5da6599b-756c-4d22-85cd-114ff02fc19d
-- MAGIC %md
-- MAGIC
-- MAGIC ## 특정 열 기준 중복 행 제거
-- MAGIC
-- MAGIC 아래 코드는 **`GROUP BY`**를 사용하여 **`user_id`** 및 **`user_first_touch_timestamp`** 열 값을 기준으로 중복 레코드를 제거합니다. (이 두 필드는 모두 특정 사용자를 처음 만났을 때 생성되므로 고유한 튜플을 형성합니다.)
-- MAGIC
-- MAGIC 여기서는 집계 함수 **`max`**를 사용하여 다음을 수행합니다.
-- MAGIC - 그룹화 결과에서 **`email`** 및 **`updated`** 열의 값을 유지합니다.
-- MAGIC - 여러 레코드가 있는 경우 null이 아닌 이메일을 캡처합니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (usersDF
-- MAGIC     .where(col("user_id").isNotNull())
-- MAGIC     .groupBy("user_id", "user_first_touch_timestamp")
-- MAGIC     .agg(max("email").alias("email"), 
-- MAGIC          max("updated").alias("updated"))
-- MAGIC     )
-- MAGIC
-- MAGIC dedupedDF.count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5e2c98db-ea2d-44dc-b2ae-680dfd85c74b
-- MAGIC %md
-- MAGIC
-- MAGIC 고유한 **user_id`** 및 **user_first_touch_timestamp`** 값을 기반으로 중복 제거 후 남은 레코드의 예상 개수가 있는지 확인해 보겠습니다.

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (usersDF
-- MAGIC     .dropDuplicates(["user_id", "user_first_touch_timestamp"])
-- MAGIC     .filter(col("user_id").isNotNull())
-- MAGIC     .count())

-- COMMAND ----------

-- DBTITLE 0,--i18n-776b4ee7-9f29-4a19-89da-1872a1f8cafa
-- MAGIC %md
-- MAGIC
-- MAGIC ## 데이터셋 검증
-- MAGIC 위의 수동 검토를 바탕으로, 예상대로 개수가 계산됨을 시각적으로 확인했습니다.
-- MAGIC
-- MAGIC 간단한 필터와 **`WHERE`** 절을 사용하여 프로그래밍 방식으로 검증을 수행할 수도 있습니다.
-- MAGIC
-- MAGIC 각 행의 **`user_id`**가 고유한지 검증합니다.

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .groupBy("user_id")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-d405e7cd-9add-44e3-976a-e56b8cdf9d83
-- MAGIC %md
-- MAGIC
-- MAGIC 각 이메일이 최대 하나의 **`user_id`**와 연결되어 있는지 확인하세요.

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .where(col("email").isNotNull())
-- MAGIC     .groupby("email")
-- MAGIC     .agg(count("user_id").alias("user_id_count"))
-- MAGIC     .select((max("user_id_count") <= 1).alias("at_most_one_id")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-8630c04d-0752-404f-bfd1-bb96f7b06ffa
-- MAGIC %md
-- MAGIC
-- MAGIC ## 날짜 형식 및 정규 표현식
-- MAGIC 이제 null 필드와 중복 항목을 제거했으므로 데이터에서 추가 값을 추출할 수 있습니다.
-- MAGIC
-- MAGIC 아래 코드:
-- MAGIC - **`user_first_touch_timestamp`**를 올바른 크기로 조정하고 유효한 타임스탬프로 변환합니다.
-- MAGIC - 이 타임스탬프에 대한 달력 날짜 및 시간을 사람이 읽을 수 있는 형식으로 추출합니다.
-- MAGIC - **`regexp_extract`**를 사용하여 정규 표현식을 사용하여 이메일 열에서 도메인을 추출합니다.

-- COMMAND ----------

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_format, regexp_extract
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .withColumn("first_touch", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
-- MAGIC     .withColumn("first_touch_date", date_format("first_touch", "MMM d, yyyy"))
-- MAGIC     .withColumn("first_touch_time", date_format("first_touch", "HH:mm:ss"))
-- MAGIC     .withColumn("email_domain", regexp_extract("email", "(?<=@).+", 0))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-c9e02918-f105-4c12-b553-3897fa7387cc
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()