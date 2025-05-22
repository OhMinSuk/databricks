-- Databricks notebook source
-- DBTITLE 0,--i18n-037a5204-a995-4945-b6ed-7207b636818c
-- MAGIC %md
-- MAGIC
-- MAGIC # 데이터 추출 실습
-- MAGIC
-- MAGIC 이 실습에서는 JSON 파일에서 원시 데이터를 추출합니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 실습을 마치면 다음을 수행할 수 있습니다.
-- MAGIC - JSON 파일에서 데이터를 추출하기 위해 외부 테이블을 등록합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-3b401203-34ae-4de5-a706-0dbbd5c987b7
-- MAGIC %md
-- MAGIC
-- MAGIC ## 실행 설정
-- MAGIC
-- MAGIC 이 레슨의 변수와 데이터 세트를 구성하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-9adc06e2-7298-4306-a062-7ff70adb8032
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
-- MAGIC | value | BINARY | 전체 데이터 페이로드(나중에 설명)이며 JSON으로 전송됩니다. |

-- COMMAND ----------

-- DBTITLE 0,--i18n-8cca978a-3034-4339-8e6e-6c48d389cce7
-- MAGIC %md
-- MAGIC
-- MAGIC ## JSON 파일에서 원시 이벤트 추출
-- MAGIC 이 데이터를 Delta에 제대로 로드하려면 먼저 올바른 스키마를 사용하여 JSON 데이터를 추출해야 합니다.
-- MAGIC
-- MAGIC 아래 제공된 파일 경로에 있는 JSON 파일에 대한 외부 테이블을 생성합니다. 이 테이블의 이름을 **`events_json`**으로 지정하고 위의 스키마를 선언합니다.

-- COMMAND ----------

select * from csv.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-33231985-3ff1-4f44-8098-b7b862117689
-- MAGIC %md
-- MAGIC
-- MAGIC **참고**: 랩 전체에서 Python을 사용하여 간헐적으로 검사를 실행합니다. 지침을 따르지 않은 경우 다음 셀에서 변경해야 할 사항에 대한 메시지와 함께 오류가 반환됩니다. 셀 실행 결과 출력이 없으면 이 단계가 완료되었음을 의미합니다.

-- COMMAND ----------

DROP TABLE IF EXISTS events_json;


-- COMMAND ----------

CREATE TABLE events_json
(key BINARY, offset BIGINT, partition INT, timestamp bigint, topic string, value binary)
USING json
LOCATION '${DA.paths.kafka_events}';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key','offset',  'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6919d58a-89e4-4c02-812c-98a15bb6f239
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()