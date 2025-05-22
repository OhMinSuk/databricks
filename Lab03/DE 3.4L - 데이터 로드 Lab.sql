-- Databricks notebook source
-- DBTITLE 0,--i18n-02080b66-d11c-47fb-a096-38ce02af4dbb
-- MAGIC %md
-- MAGIC
-- MAGIC # 데이터 로드 랩
-- MAGIC
-- MAGIC 이 랩에서는 새 델타 테이블과 기존 델타 테이블에 데이터를 로드합니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 랩을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - 제공된 스키마를 사용하여 빈 델타 테이블 생성
-- MAGIC - 기존 테이블의 레코드를 델타 테이블에 삽입
-- MAGIC - CTAS 문을 사용하여 파일에서 델타 테이블 생성

-- COMMAND ----------

-- DBTITLE 0,--i18n-50357195-09c5-4ab4-9d60-ca7fd44feecc
-- MAGIC %md
-- MAGIC
-- MAGIC ## 실행 설정
-- MAGIC
-- MAGIC 이 레슨의 변수와 데이터 세트를 구성하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- MAGIC %python
-- MAGIC kafka_events = DA.paths.kafka_events
-- MAGIC print(kafka_events)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5b20c9b2-6658-4536-b79b-171d984b3b1e
-- MAGIC %md
-- MAGIC
-- MAGIC ## 데이터 개요
-- MAGIC
-- MAGIC JSON 파일로 작성된 원시 Kafka 데이터 샘플을 사용합니다.
-- MAGIC
-- MAGIC 각 파일에는 5초 간격 동안 사용된 모든 레코드가 포함되어 있으며, 전체 Kafka 스키마와 함께 다중 레코드 JSON 파일로 저장됩니다.
-- MAGIC
-- MAGIC 테이블 스키마는 다음과 같습니다.
-- MAGIC
-- MAGIC | field | type | description |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key | BINARY | **`user_id`** 필드는 키로 사용됩니다. 세션/쿠키 정보에 해당하는 고유한 영숫자 필드입니다. |
-- MAGIC | offset | LONG | 각 파티션에 대해 단조롭게 증가하는 고유한 값입니다. |
-- MAGIC | partition | INTEGER | 현재 Kafka 구현에서는 2개의 파티션(0과 1)만 사용합니다. |
-- MAGIC | timestamp | LONG | 이 타임스탬프는 epoch 이후 밀리초 단위로 기록되며, 프로듀서가 파티션에 레코드를 추가하는 시간을 나타냅니다. |
-- MAGIC | topic | STRING | Kafka 서비스는 여러 토픽을 호스팅하지만, **`클릭스트림`** 토픽의 레코드만 여기에 포함됩니다. |
-- MAGIC | 값 | 이진 | 전체 데이터 페이로드(나중에 설명)이며 JSON으로 전송됩니다. |

-- COMMAND ----------

-- DBTITLE 0,--i18n-5140f012-898a-43ac-bed9-b7e01a916505
-- MAGIC %md
-- MAGIC
-- MAGIC ## 빈 델타 테이블에 대한 스키마 정의
-- MAGIC 동일한 스키마를 사용하여 **`events_raw`**라는 이름의 빈 관리형 델타 테이블을 생성합니다.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE events_raw (
  key BINARY,
  offset LONG,
  partition INT,
  timestamp LONG,
  topic STRING,
  value BINARY
);

DESCRIBE EXTENDED events_raw;

-- COMMAND ----------

-- DBTITLE 0,--i18n-70f4dbf1-f4cb-4ec6-925a-a939cbc71bd5
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀을 실행하여 테이블이 올바르게 생성되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Define Schema")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 0, "The table should have 0 records")
-- MAGIC
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "key", "BinaryType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "offset", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "partition", "IntegerType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "timestamp", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "topic", "StringType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "value", "BinaryType")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite

-- COMMAND ----------

-- DBTITLE 0,--i18n-7b4e55f2-737f-4996-a51b-4f18c2fc6eb7
-- MAGIC %md
-- MAGIC
-- MAGIC ## 델타 테이블에 원시 이벤트 삽입
-- MAGIC
-- MAGIC 추출된 데이터와 델타 테이블이 준비되면 **`events_json`** 테이블의 JSON 레코드를 새 **`events_raw`** 델타 테이블에 삽입합니다.

-- COMMAND ----------

-- TODO
INSERT INTO events_raw
SELECT * FROM JSON.`${DA.paths.kafka_events}`

-- COMMAND ----------

insert into events_raw
select * from json.`${da.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-65ffce96-d821-4792-b545-5725814003a0
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 내용을 직접 검토하여 데이터가 예상대로 작성되었는지 확인하세요.

-- COMMAND ----------

-- TODO
SELECT * FROM events_raw LIMIT 100

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cbff40d-e916-487c-958f-2eb7807c5aeb
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀을 실행하여 데이터가 올바르게 로드되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate events_raw")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 2252, "The table should have 2252 records")
-- MAGIC
-- MAGIC first_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").asc()).limit(5).collect()]
-- MAGIC suite.test_sequence(first_five, [1593879303631, 1593879304224, 1593879305465, 1593879305482, 1593879305746], True, "First 5 values are correct")
-- MAGIC
-- MAGIC last_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").desc()).limit(5).collect()]
-- MAGIC suite.test_sequence(last_five, [1593881096290, 1593881095799, 1593881093452, 1593881093394, 1593881092076], True, "Last 5 values are correct")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3c62fea-b75d-41d6-8214-660f9cfa3acd
-- MAGIC %md
-- MAGIC
-- MAGIC ## 쿼리 결과에서 델타 테이블 생성
-- MAGIC
-- MAGIC 새로운 이벤트 데이터 외에도, 나중에 사용할 제품 세부 정보를 제공하는 작은 조회 테이블도 로드해 보겠습니다.
-- MAGIC CTAS ​​문을 사용하여 아래에 제공된 Parquet 디렉터리에서 데이터를 추출하는 **`item_lookup`**이라는 관리형 델타 테이블을 생성합니다.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE item_lookup AS 
SELECT * FROM PARQUET.`${da.paths.datasets}/ecommerce/raw/item-lookup`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a971532-0003-4665-9064-26196cd31e89
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀을 실행하여 조회 테이블이 올바르게 로드되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate item_lookup")
-- MAGIC expected_table = lambda: spark.table("item_lookup")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"item_lookup\"")
-- MAGIC
-- MAGIC actual_values = lambda: [r["item_id"] for r in expected_table().collect()]
-- MAGIC expected_values = ['M_PREM_Q','M_STAN_F','M_PREM_F','M_PREM_T','M_PREM_K','P_DOWN_S','M_STAN_Q','M_STAN_K','M_STAN_T','P_FOAM_S','P_FOAM_K','P_DOWN_K']
-- MAGIC suite.test_sequence(actual_values, expected_values, False, "Contains the 12 expected item IDs")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-4db73493-3920-44e2-a19b-f335aa650f76
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()