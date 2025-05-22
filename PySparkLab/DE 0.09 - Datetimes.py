# Databricks notebook source
# DBTITLE 0,--i18n-0eeddecf-4f2c-4599-960f-8fefe777281f
# MAGIC %md
# MAGIC # 날짜/시간 함수
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. 타임스탬프로 변환
# MAGIC 2. 날짜/시간 형식 지정
# MAGIC 3. 타임스탬프에서 추출
# MAGIC 4. 날짜/시간으로 변환
# MAGIC 5. 날짜/시간 조작
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">Built-In Functions</a>: **`date_format`**, **`to_date`**, **`date_add`**, **`year`**, **`month`**, **`dayofweek`**, **`minute`**, **`second`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.09

# COMMAND ----------

# DBTITLE 0,--i18n-6d2e9c6a-0561-4426-ae88-a8ebca06c61b
# MAGIC %md
# MAGIC
# MAGIC 이벤트 데이터셋의 일부를 사용하여 날짜/시간 작업을 연습해 보겠습니다.

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table("events").select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-34540d5e-7a9b-496d-b9d7-1cf7de580f23
# MAGIC %md
# MAGIC
# MAGIC ### 내장 함수: 날짜/시간 함수
# MAGIC Spark에서 날짜와 시간을 조작하는 몇 가지 내장 함수는 다음과 같습니다.
# MAGIC
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | **`add_months`** | startDate로부터 numMonths 후의 날짜를 반환합니다. |
# MAGIC | **`current_timestamp`** | 쿼리 실행 시작 시 현재 타임스탬프를 타임스탬프 열로 반환합니다. |
# MAGIC | **`date_format`** | 날짜/타임스탬프/문자열을 두 번째 인수로 지정된 날짜 형식의 문자열 값으로 변환합니다. |
# MAGIC | **`dayofweek`** | 주어진 날짜/타임스탬프/문자열에서 일자를 정수로 추출합니다. |
# MAGIC | **`from_unixtime`** | 유닉스 시대(1970-01-01 00:00:00 UTC)의 초 수를 현재 시스템 시간대의 해당 시점의 타임스탬프를 yyyy-MM-dd HH:mm:ss 형식으로 나타내는 문자열로 변환합니다. |
# MAGIC | **`minute`** | 주어진 날짜/타임스탬프/문자열에서 분을 정수로 추출합니다. |
# MAGIC | **`unix_timestamp`** | 주어진 패턴을 갖는 시간 문자열을 유닉스 타임스탬프(초)로 변환합니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-fa5f62a7-e690-48c8-afa0-b446d3bc7aa6
# MAGIC %md
# MAGIC
# MAGIC ### 타임스탬프로 변환
# MAGIC
# MAGIC #### **`cast()`**
# MAGIC 문자열 표현이나 DataType을 사용하여 지정된 다른 데이터 유형으로 열을 변환합니다.

# COMMAND ----------

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestamp_df)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType()))
display(timestamp_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6c9cb2b0-ef18-48c4-b1ed-3fad453172c1
# MAGIC %md
# MAGIC
# MAGIC ### 날짜/시간
# MAGIC
# MAGIC Spark에서 날짜/시간을 사용하는 몇 가지 일반적인 시나리오는 다음과 같습니다.
# MAGIC
# MAGIC - CSV/JSON 데이터 소스는 날짜/시간 콘텐츠의 구문 분석 및 형식 지정에 패턴 문자열을 사용합니다.
# MAGIC - StringType과 DateType 또는 TimestampType 간의 변환과 관련된 날짜/시간 함수(예: **`unix_timestamp`**, **`date_format`**, **`from_unixtime`**, **`to_date`**, **`to_timestamp`** 등)
# MAGIC
# MAGIC #### 형식 지정 및 구문 분석을 위한 날짜/시간 패턴
# MAGIC Spark는 <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">날짜 및 타임스탬프 구문 분석 및 형식 지정에 패턴 문자</a>를 사용합니다. 이러한 패턴의 일부는 다음과 같습니다.
# MAGIC
# MAGIC | 기호 | 의미 | 표현 | 예시 |
# MAGIC | ------ | --------------- | ------------ | ------------ |
# MAGIC | G | 시대 | 텍스트 | 서기; 기원후 |
# MAGIC | y | 년 | 년 | 2020; 20 |
# MAGIC | D | 일 | 숫자(3) | 189 |
# MAGIC | M/L | 월 | 월 | 7; 07; 7월; 7월 |
# MAGIC | d | 일 | 숫자(3) | 28 |
# MAGIC | Q/q | 분기 | 숫자/텍스트 | 3; 03; 3분기 |
# MAGIC | E | 요일 | 텍스트 | 화; 화요일 |
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="경고"> Spark 3.0 버전에서 날짜 및 타임스탬프 처리 방식이 변경되었으며, 이러한 값을 구문 분석하고 서식을 지정하는 데 사용되는 패턴도 변경되었습니다. 이러한 변경 사항에 대한 설명은 <a href="https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html" target="_blank">이 Databricks 블로그 게시물</a>을 참조하세요.

# COMMAND ----------

# DBTITLE 0,--i18n-6bc9e089-fc58-4d8f-b118-d5162b747dc6
# MAGIC %md
# MAGIC
# MAGIC #### 날짜 형식 지정
# MAGIC
# MAGIC #### **`date_format()`**
# MAGIC 날짜/타임스탬프/문자열을 주어진 날짜/시간 패턴으로 형식화된 문자열로 변환합니다.

# COMMAND ----------

from pyspark.sql.functions import date_format

formatted_df = (timestamp_df
                .withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
                .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
               )
display(formatted_df)

# COMMAND ----------

# DBTITLE 0,--i18n-adc065e9-e241-424e-ad6e-db1e2cb9b1e6
# MAGIC %md
# MAGIC
# MAGIC #### 타임스탬프에서 날짜/시간 속성 추출
# MAGIC
# MAGIC #### **`year`**
# MAGIC 주어진 날짜/타임스탬프/문자열에서 연도를 정수로 추출합니다.
# MAGIC
# MAGIC ##### 유사 메서드: **`month`**, **`dayofweek`**, **`minute`**, **`second`** 등

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, minute, second

datetime_df = (timestamp_df
               .withColumn("year", year(col("timestamp")))
               .withColumn("month", month(col("timestamp")))
               .withColumn("dayofweek", dayofweek(col("timestamp")))
               .withColumn("minute", minute(col("timestamp")))
               .withColumn("second", second(col("timestamp")))
              )
display(datetime_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f06bd91b-c4f4-4909-98dd-680fbfdf56cd
# MAGIC %md
# MAGIC
# MAGIC #### 날짜로 변환
# MAGIC
# MAGIC #### **`to_date`**
# MAGIC 규칙을 DateType으로 캐스팅하여 열을 DateType으로 변환합니다.

# COMMAND ----------

from pyspark.sql.functions import to_date

date_df = timestamp_df.withColumn("date", to_date(col("timestamp")))
display(date_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8367af41-fc35-44ba-8ab1-df721452e6f3
# MAGIC %md
# MAGIC
# MAGIC ### 날짜/시간 조작
# MAGIC #### **`date_add`**
# MAGIC 시작일로부터 주어진 일수 후의 날짜를 반환합니다.

# COMMAND ----------

from pyspark.sql.functions import date_add

plus_2_df = timestamp_df.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus_2_df)

# COMMAND ----------

# DBTITLE 0,--i18n-3669ec6f-2f26-4607-9f58-656d463308b5
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()