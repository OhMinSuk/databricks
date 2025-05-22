# Databricks notebook source
# DBTITLE 0,--i18n-a0d28fb8-0d0f-4354-9720-79ce468b5ea8
# MAGIC %md
# MAGIC
# MAGIC # Spark SQL을 사용하여 파일에서 직접 데이터 추출
# MAGIC
# MAGIC 이 노트북에서는 Databricks에서 Spark SQL을 사용하여 파일에서 직접 데이터를 추출하는 방법을 알아봅니다.
# MAGIC
# MAGIC 다양한 파일 형식에서 이 옵션을 지원하지만, Parquet 및 JSON과 같은 자체 설명적 데이터 형식에 가장 유용합니다.
# MAGIC
# MAGIC ## 학습 목표
# MAGIC 이 과정을 마치면 다음을 수행할 수 있습니다.
# MAGIC - Spark SQL을 사용하여 데이터 파일을 직접 쿼리합니다.
# MAGIC - 레이어 뷰 및 CTE를 사용하여 데이터 파일을 더 쉽게 참조합니다.
# MAGIC - **`text`** 및 **`binaryFile`** 메서드를 사용하여 원시 파일 내용을 검토합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-73162404-8907-47f6-9b3e-dd17819d71c9
# MAGIC %md
# MAGIC
# MAGIC ## 설치 실행
# MAGIC
# MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.1

# COMMAND ----------

# DBTITLE 0,--i18n-480bfe0b-d36d-4f67-8242-6a6d3cca38dd
# MAGIC %md
# MAGIC
# MAGIC ## 데이터 개요
# MAGIC
# MAGIC 이 예제에서는 JSON 파일로 작성된 원시 Kafka 데이터 샘플을 사용합니다.
# MAGIC
# MAGIC 각 파일에는 5초 간격 동안 사용된 모든 레코드가 포함되어 있으며, 전체 Kafka 스키마와 함께 다중 레코드 JSON 파일로 저장됩니다.
# MAGIC
# MAGIC | field | type | description |
# MAGIC | --- | --- | --- |
# MAGIC | key | BINARY | **`user_id`** 필드는 키로 사용됩니다. 세션/쿠키 정보에 해당하는 고유한 영숫자 필드입니다. |
# MAGIC | value | BINARY | JSON으로 전송되는 전체 데이터 페이로드(나중에 설명)입니다. |
# MAGIC | topic | STRING | Kafka 서비스는 여러 토픽을 호스팅하지만, **`clickstream`** 토픽의 레코드만 여기에 포함됩니다. |
# MAGIC | partition | INTEGER | 현재 Kafka 구현에서는 2개의 파티션(0과 1)만 사용합니다. |
# MAGIC | offset | LONG | 각 파티션에 대해 단조롭게 증가하는 고유한 값입니다. |
# MAGIC | 타임스탬프 | LONG | 이 타임스탬프는 에포크 이후 밀리초 단위로 기록되며, 프로듀서가 파티션에 레코드를 추가하는 시간을 나타냅니다.

# COMMAND ----------

# DBTITLE 0,--i18n-65941466-ca87-4c29-903e-658e24e48cee
# MAGIC %md
# MAGIC
# MAGIC 소스 디렉터리에 많은 JSON 파일이 포함되어 있습니다.

# COMMAND ----------

print(DA.paths.kafka_events)

files = dbutils.fs.ls(DA.paths.kafka_events)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-f1ddfb40-9c95-4b9a-84e5-2958ac01166d
# MAGIC %md
# MAGIC
# MAGIC 여기서는 DBFS 루트에 기록된 데이터의 상대 파일 경로를 사용합니다.
# MAGIC
# MAGIC 대부분의 워크플로에서는 사용자가 외부 클라우드 스토리지 위치의 데이터에 액세스해야 합니다.
# MAGIC
# MAGIC 대부분의 회사에서는 작업 공간 관리자가 이러한 스토리지 위치에 대한 액세스를 구성할 책임을 맡습니다.
# MAGIC
# MAGIC 이러한 위치를 구성하고 액세스하는 방법에 대한 지침은 "클라우드 아키텍처 및 시스템 통합"이라는 제목의 클라우드 공급업체별 자습 과정에서 확인할 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
# MAGIC %md
# MAGIC
# MAGIC ## 단일 파일 쿼리
# MAGIC
# MAGIC 단일 파일에 포함된 데이터를 쿼리하려면 다음 패턴으로 쿼리를 실행하세요.
# MAGIC
# MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
# MAGIC
# MAGIC 경로를 작은따옴표가 아닌 백틱(')으로 묶어야 합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

# DBTITLE 0,--i18n-5c2891f1-e055-4fde-8bf9-3f448e4cdb2b
# MAGIC %md
# MAGIC
# MAGIC 미리보기에 소스 파일의 321개 행이 모두 표시됩니다.

# COMMAND ----------

# DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
# MAGIC %md
# MAGIC
# MAGIC ## 파일 디렉터리 쿼리
# MAGIC
# MAGIC 디렉토리에 있는 모든 파일의 형식과 스키마가 동일하다고 가정할 경우, 개별 파일 대신 디렉터리 경로를 지정하여 모든 파일을 동시에 쿼리할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-6921da25-dc10-4bd9-9baa-7e589acd3139
# MAGIC %md
# MAGIC
# MAGIC 기본적으로 이 쿼리는 처음 1000개 행만 표시합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
# MAGIC %md
# MAGIC
# MAGIC ## 파일 참조 생성
# MAGIC 파일과 디렉터리를 직접 쿼리할 수 있는 이 기능을 통해 파일에 대한 쿼리에 추가적인 Spark 로직을 연결할 수 있습니다.
# MAGIC
# MAGIC 경로에 대한 쿼리에서 뷰를 생성하면 이후 쿼리에서 이 뷰를 참조할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW event_view
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-5c29b73b-b4b0-48ab-afbb-7b1422fce6e4
# MAGIC %md
# MAGIC
# MAGIC 사용자에게 뷰와 기본 저장소 위치에 대한 액세스 권한이 있는 한, 해당 사용자는 이 뷰 정의를 사용하여 기본 데이터를 쿼리할 수 있습니다. 이는 작업 공간의 여러 사용자, 여러 노트북 및 여러 클러스터에 적용됩니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM event_view

# COMMAND ----------

# DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
# MAGIC %md
# MAGIC ## 파일에 대한 임시 참조 만들기
# MAGIC
# MAGIC 임시 뷰는 마찬가지로 쿼리에 별칭을 지정하여 이후 쿼리에서 참조하기 쉬운 이름을 지정합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW events_temp_view
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-a9f9827b-2258-4481-a9d9-6fecf55aeb9b
# MAGIC %md
# MAGIC
# MAGIC 임시 뷰는 현재 SparkSession에만 존재합니다. Databricks에서는 임시 뷰가 현재 노트북, 작업 또는 DBSQL 쿼리에 격리됨을 의미합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM events_temp_view

# COMMAND ----------

# DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
# MAGIC %md
# MAGIC ## 쿼리 내 참조에 CTE 적용
# MAGIC 공통 테이블 표현식(CTE)은 쿼리 결과에 대한 단기간 동안 사람이 읽을 수 있는 참조가 필요할 때 매우 유용합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_json
# MAGIC AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
# MAGIC SELECT * FROM cte_json

# COMMAND ----------

# DBTITLE 0,--i18n-c85e1553-f643-47b8-b909-0d10d2177437
# MAGIC %md
# MAGIC CTE는 쿼리가 계획되고 실행되는 동안에만 쿼리 결과에 별칭을 지정합니다.
# MAGIC
# MAGIC 따라서 **다음 셀은 실행 시 오류를 발생시킵니다**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM cte_json

# COMMAND ----------

# DBTITLE 0,--i18n-106214eb-2fec-4a27-b692-035a86b8ec8d
# MAGIC %md
# MAGIC
# MAGIC ## 텍스트 파일을 원시 문자열로 추출
# MAGIC
# MAGIC 텍스트 기반 파일(JSON, CSV, TSV, TXT 형식 포함)을 사용할 때, **`text`** 형식을 사용하여 파일의 각 줄을 **`value`**라는 문자열 열 하나를 가진 행으로 로드할 수 있습니다. 이 기능은 데이터 소스가 손상되기 쉽고 사용자 지정 텍스트 구문 분석 함수를 사용하여 텍스트 필드에서 값을 추출할 때 유용할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-732e648b-4274-48f4-86e9-8b42fd5a26bd
# MAGIC %md
# MAGIC
# MAGIC ## 파일의 원시 바이트 및 메타데이터 추출
# MAGIC
# MAGIC 이미지나 비정형 데이터를 처리하는 경우처럼 일부 워크플로에서는 전체 파일을 처리해야 할 수 있습니다. **`binaryFile`**을 사용하여 디렉터리를 쿼리하면 파일 콘텐츠의 이진 표현과 함께 파일 메타데이터가 제공됩니다.
# MAGIC
# MAGIC 생성된 필드는 **`path`**, **`modificationTime`**, **`length`** 및 **`content`**를 나타냅니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

# COMMAND ----------

# DBTITLE 0,--i18n-9ac20d39-ae6a-400e-9e13-14af5d4c91df
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()