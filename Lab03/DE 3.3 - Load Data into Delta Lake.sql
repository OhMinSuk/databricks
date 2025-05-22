-- Databricks notebook source
-- DBTITLE 0,--i18n-a518bafd-59bd-4b23-95ee-5de2344023e4
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lake에 데이터 로드하기
-- MAGIC Delta Lake 테이블은 클라우드 객체 스토리지의 데이터 파일로 지원되는 테이블에 ACID 호환 업데이트를 제공합니다.
-- MAGIC
-- MAGIC 이 노트북에서는 Delta Lake에서 업데이트를 처리하는 SQL 구문을 살펴보겠습니다. 많은 작업이 표준 SQL이지만, Spark 및 Delta Lake 실행에 맞게 약간의 변형이 있습니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - **`INSERT OVERWRITE`**를 사용하여 데이터 테이블 덮어쓰기
-- MAGIC - **`INSERT INTO`**를 사용하여 테이블에 추가
-- MAGIC - **`MERGE INTO`**를 사용하여 테이블에 추가, 업데이트 및 삭제
-- MAGIC - **`COPY INTO`**를 사용하여 테이블에 증분 방식으로 데이터 수집

-- COMMAND ----------

-- DBTITLE 0,--i18n-af486892-a86c-4ef2-9996-2ace24b5737c
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설치 실행
-- MAGIC
-- MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.5

-- COMMAND ----------

-- DBTITLE 0,--i18n-04a35896-fb09-4a99-8d00-313480e5c6a1
-- MAGIC %md
-- MAGIC
-- MAGIC ## 완전 덮어쓰기
-- MAGIC
-- MAGIC 덮어쓰기를 사용하면 테이블의 모든 데이터를 원자적으로 바꿀 수 있습니다. 테이블을 삭제하고 다시 만드는 대신 테이블을 덮어쓰면 여러 가지 이점이 있습니다.
-- MAGIC - 테이블 덮어쓰기는 디렉터리를 재귀적으로 나열하거나 파일을 삭제할 필요가 없기 때문에 훨씬 빠릅니다.
-- MAGIC - 이전 버전의 테이블이 여전히 존재하므로 시간 여행을 통해 이전 데이터를 쉽게 검색할 수 있습니다.
-- MAGIC - 원자적 연산입니다. 테이블을 삭제하는 동안에도 동시 쿼리가 테이블을 읽을 수 있습니다.
-- MAGIC - ACID 트랜잭션 보장으로 인해 테이블 ​​덮어쓰기가 실패하더라도 테이블은 이전 상태로 유지됩니다.
-- MAGIC
-- MAGIC Spark SQL은 완전 덮어쓰기를 수행하는 두 가지 간단한 방법을 제공합니다.
-- MAGIC
-- MAGIC 일부 학생들은 CTAS 문에 대한 이전 수업에서 실제로 CRAS 문을 사용했다는 것을 알아차렸을 것입니다(셀을 여러 번 실행할 경우 발생할 수 있는 오류를 방지하기 위해).
-- MAGIC
-- MAGIC **`CREATE OR REPLACE TABLE`**(CRAS) 문은 실행될 때마다 테이블의 내용을 완전히 바꿉니다.

-- COMMAND ----------

CREATE OR REPLACE TABLE events AS
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`

-- COMMAND ----------

-- DBTITLE 0,--i18n-8f767697-33e6-4b5b-ac09-862076f77033
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 기록을 검토한 결과, 이 테이블의 이전 버전이 대체된 것으로 나타났습니다.

-- COMMAND ----------

DESCRIBE HISTORY events

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb68d513-240c-41e1-902c-3c3add9c0a75
-- MAGIC %md
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`**는 위와 거의 동일한 결과를 제공합니다. 대상 테이블의 데이터가 쿼리의 데이터로 대체됩니다.
-- MAGIC
-- MAGIC **`INSERT OVERWRITE`**:
-- MAGIC
-- MAGIC - 기존 테이블만 덮어쓸 수 있으며, CRAS 문처럼 새 테이블을 만들 수는 없습니다.
-- MAGIC - 현재 테이블 스키마와 일치하는 새 레코드로만 덮어쓸 수 있습니다. 따라서 다운스트림 소비자를 방해하지 않고 기존 테이블을 덮어쓸 수 있는 "더 안전한" 기법입니다.
-- MAGIC - 개별 파티션을 덮어쓸 수 있습니다.

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical/`

-- COMMAND ----------

-- DBTITLE 0,--i18n-cfefb85f-f762-43db-be9b-cb536a06c842
-- MAGIC %md
-- MAGIC
-- MAGIC CRAS 명령문과는 다른 메트릭이 표시되며, 테이블 기록에도 작업이 다르게 기록됩니다.

-- COMMAND ----------

DESCRIBE HISTORY sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-40769b04-c72b-4740-9d27-ea2d1b8700f3
-- MAGIC %md
-- MAGIC
-- MAGIC 여기서 가장 큰 차이점은 Delta Lake가 스키마 온 쓰기(schema on write)를 적용하는 방식과 관련이 있습니다.
-- MAGIC
-- MAGIC CRAS 문을 사용하면 대상 테이블의 내용을 완전히 재정의할 수 있지만, 스키마를 변경하려고 하면 (선택적인 설정을 제공하지 않는 한) **`INSERT OVERWRITE`**가 실패합니다.
-- MAGIC
-- MAGIC 아래 셀의 주석 처리를 제거하고 실행하면 예상되는 오류 메시지가 생성됩니다.

-- COMMAND ----------

-- INSERT OVERWRITE sales
-- SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`

-- COMMAND ----------

-- DBTITLE 0,--i18n-ceb78e46-6362-4c3b-b63d-54f42d38dd1f
-- MAGIC %md
-- MAGIC
-- MAGIC ## 행 추가
-- MAGIC
-- MAGIC **`INSERT INTO`**를 사용하여 기존 Delta 테이블에 새로운 행을 원자적으로 추가할 수 있습니다. 이렇게 하면 기존 테이블에 증분 업데이트를 적용할 수 있으므로 매번 덮어쓰는 것보다 훨씬 효율적입니다.
-- MAGIC
-- MAGIC **`INSERT INTO`**를 사용하여 **`sales`** 테이블에 새로운 판매 레코드를 추가합니다.

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-30m`

-- COMMAND ----------

-- DBTITLE 0,--i18n-171f9cf2-e0e5-4f8d-9dc7-bf4770b6d8e5
-- MAGIC %md
-- MAGIC
-- MAGIC **`INSERT INTO`**에는 동일한 레코드를 여러 번 삽입하는 것을 방지하는 기본 제공 기능이 없습니다. 위 셀을 다시 실행하면 대상 테이블에 동일한 레코드가 기록되어 중복 레코드가 생성됩니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ad4ab1f-a7c1-439d-852e-ff504dd16307
-- MAGIC %md
-- MAGIC
-- MAGIC ## 업데이트 병합
-- MAGIC
-- MAGIC **`MERGE`** SQL 작업을 사용하여 소스 테이블, 뷰 또는 DataFrame의 데이터를 대상 Delta 테이블에 업서트할 수 있습니다. Delta Lake는 **`MERGE`**에서 삽입, 업데이트 및 삭제를 지원하며, 고급 사용 사례를 지원하기 위해 SQL 표준을 넘어서는 확장된 구문을 지원합니다.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC MERGE INTO target a<br/>
-- MAGIC USING source b<br/>
-- MAGIC ON {merge_condition}<br/>
-- MAGIC WHEN MATCHED THEN {matched_action}<br/>
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC **`MERGE`** 작업을 사용하여 업데이트된 이메일과 새로운 사용자를 포함하여 이전 사용자 데이터를 업데이트합니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-30m`

-- COMMAND ----------

-- DBTITLE 0,--i18n-4732ea19-2857-45fe-9ca2-c2475015ef47
-- MAGIC %md
-- MAGIC
-- MAGIC **`MERGE`**의 주요 이점:
-- MAGIC * 업데이트, 삽입 및 삭제가 단일 트랜잭션으로 완료됩니다.
-- MAGIC * 일치하는 필드 외에도 여러 조건을 추가할 수 있습니다.
-- MAGIC * 사용자 지정 로직을 구현하기 위한 광범위한 옵션을 제공합니다.
-- MAGIC
-- MAGIC 아래에서는 현재 행에 **`NULL`** 이메일이 있고 새 행에는 없는 경우에만 레코드를 업데이트합니다.
-- MAGIC
-- MAGIC 새 배치에서 일치하지 않는 모든 레코드가 삽입됩니다.

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-5cae1734-7eaf-4a53-a9b5-c093a8d73cc9
-- MAGIC %md
-- MAGIC
-- MAGIC **`MATCHED`** 및 **`NOT MATCHED`** 조건 모두에 대해 이 함수의 동작을 명시적으로 지정합니다. 여기에 제시된 예는 적용 가능한 논리의 예일 뿐이며, 모든 **`MERGE`** 동작을 나타내는 것은 아닙니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-d7d2c7fd-2c83-4ed2-aa78-c37992751881
-- MAGIC %md
-- MAGIC
-- MAGIC ## 중복 제거를 위한 삽입 전용 병합
-- MAGIC
-- MAGIC 일반적인 ETL 사용 사례는 일련의 추가 작업을 통해 로그 또는 기타 모든 추가 데이터 세트를 델타 테이블에 수집하는 것입니다.
-- MAGIC
-- MAGIC 많은 소스 시스템에서 중복 레코드가 생성될 수 있습니다. 병합을 사용하면 삽입 전용 병합을 수행하여 중복 레코드 삽입을 방지할 수 있습니다.
-- MAGIC
-- MAGIC 이 최적화된 명령은 동일한 **`MERGE`** 구문을 사용하지만 **`WHEN NOT MATCHED`** 절만 제공합니다.
-- MAGIC
-- MAGIC 아래에서는 이 절을 사용하여 동일한 **`user_id`** 및 **`event_timestamp`**를 가진 레코드가 **`events`** 테이블에 이미 있는지 확인합니다.

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-75891a95-c6f2-4f00-b30e-3df2df858c7c
-- MAGIC %md
-- MAGIC
-- MAGIC ## 증분 로드
-- MAGIC
-- MAGIC **`COPY INTO`**는 SQL 엔지니어에게 외부 시스템에서 데이터를 증분 방식으로 수집할 수 있는 멱등 옵션을 제공합니다.
-- MAGIC
-- MAGIC 이 작업에는 몇 가지 예상 사항이 있습니다.
-- MAGIC - 데이터 스키마는 일관성이 있어야 합니다.
-- MAGIC - 중복 레코드는 제외되거나 다운스트림에서 처리되어야 합니다.
-- MAGIC
-- MAGIC 이 작업은 예측 가능하게 증가하는 데이터의 경우 전체 테이블 스캔보다 훨씬 저렴할 수 있습니다.
-- MAGIC
-- MAGIC 여기서는 정적 디렉터리에서 간단한 실행을 보여주지만, 진정한 가치는 시간이 지남에 따라 여러 번 실행하여 소스에서 새 파일을 자동으로 가져오는 것입니다.

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd65fe71-cdaf-47a8-85ec-fa9769c11708
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()