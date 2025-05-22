-- Databricks notebook source
-- DBTITLE 0,--i18n-c6ad28ff-5ff6-455d-ba62-30880beee5cd
-- MAGIC %md
-- MAGIC
-- MAGIC # 델타 테이블 설정
-- MAGIC
-- MAGIC 외부 데이터 소스에서 데이터를 추출한 후, Databricks 플랫폼의 모든 이점을 최대한 활용할 수 있도록 Lakehouse에 데이터를 로드합니다.
-- MAGIC
-- MAGIC 조직마다 Databricks에 데이터를 처음 로드하는 방법에 대한 정책이 다를 수 있지만, 일반적으로 초기 테이블은 대부분 원시 데이터 버전을 나타내고 유효성 검사 및 보강은 이후 단계에서 수행하는 것이 좋습니다. 이 패턴을 사용하면 데이터 유형이나 열 이름과 관련하여 데이터가 예상과 일치하지 않더라도 데이터가 삭제되지 않으므로 프로그래밍 방식이나 수동 작업을 통해 부분적으로 손상되었거나 유효하지 않은 상태의 데이터를 복구할 수 있습니다.
-- MAGIC
-- MAGIC 이 과정에서는 대부분의 테이블 생성에 사용되는 패턴인 **`CREATE TABLE _ AS SELECT`**(CTAS) 문에 주로 중점을 둡니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - CTAS 문을 사용하여 Delta Lake 테이블 생성
-- MAGIC - 기존 뷰 또는 테이블에서 새 테이블 생성
-- MAGIC - 추가 메타데이터로 로드된 데이터 보강
-- MAGIC - 생성된 열과 설명 주석을 사용하여 테이블 스키마 선언
-- MAGIC - 데이터 위치, 품질 적용 및 파티셔닝을 제어하는 ​​고급 옵션 설정
-- MAGIC - 얕은 복제본 및 깊은 복제본 생성

-- COMMAND ----------

-- DBTITLE 0,--i18n-fe1b3b29-37fb-4e5b-8a50-e2baf7381d24
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설치 실행
-- MAGIC
-- MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-36f64593-7d14-4841-8b1a-fabad267ba22
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC
-- MAGIC **`CREATE TABLE AS SELECT`** 명령문은 입력 쿼리에서 검색된 데이터를 사용하여 델타 테이블을 만들고 채웁니다.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

-- DBTITLE 0,--i18n-1d3d7f45-be4f-4459-92be-601e55ff0063
-- MAGIC %md
-- MAGIC
-- MAGIC CTAS ​​문은 쿼리 결과에서 스키마 정보를 자동으로 유추하며, 수동 스키마 선언은 지원하지 않습니다.
-- MAGIC
-- MAGIC 즉, CTAS 문은 Parquet 파일 및 테이블과 같이 스키마가 명확하게 정의된 소스에서 외부 데이터를 수집하는 데 유용합니다.
-- MAGIC
-- MAGIC CTAS ​​문은 추가 파일 옵션 지정도 지원하지 않습니다.
-- MAGIC
-- MAGIC CSV 파일에서 데이터를 수집할 때 이러한 제약이 얼마나 큰지 알 수 있습니다.

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ec18c0e-c56b-4b41-8364-4826d1f34dcf
-- MAGIC %md
-- MAGIC
-- MAGIC 이 데이터를 Delta Lake 테이블에 올바르게 수집하려면 옵션을 지정할 수 있는 파일 참조를 사용해야 합니다.
-- MAGIC
-- MAGIC 이전 과정에서는 외부 테이블을 등록하여 이 작업을 수행하는 방법을 살펴보았습니다. 여기서는 이 구문을 약간 수정하여 임시 뷰에 옵션을 지정한 다음, 이 임시 뷰를 CTAS 명령문의 소스로 사용하여 Delta 테이블을 성공적으로 등록해 보겠습니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- DBTITLE 0,--i18n-96f21158-ccb9-4fd3-9dd2-c7c7fce1a6e9
-- MAGIC %md
-- MAGIC
-- MAGIC ## 기존 테이블의 열 필터링 및 이름 바꾸기
-- MAGIC
-- MAGIC 열 이름을 변경하거나 대상 테이블에서 열을 제외하는 것과 같은 간단한 변환은 테이블 생성 중에 쉽게 수행할 수 있습니다.
-- MAGIC
-- MAGIC 다음 명령문은 **sales`** 테이블의 열 하위 집합을 포함하는 새 테이블을 생성합니다.
-- MAGIC
-- MAGIC 여기서는 사용자를 식별하거나 항목별 구매 세부 정보를 제공하는 정보를 의도적으로 생략한다고 가정합니다. 또한 다운스트림 시스템의 명명 규칙이 원본 데이터와 다르다는 가정 하에 필드 이름을 바꿉니다.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-02026f25-a1cf-42e5-b9c3-75b9c1c7ef11
-- MAGIC %md
-- MAGIC
-- MAGIC 아래에 보이는 것처럼 뷰를 사용해서도 같은 목표를 달성할 수 있었습니다.

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-23f77f7e-21d5-4977-93bc-45800b28535f
-- MAGIC %md
-- MAGIC
-- MAGIC ## 생성된 열로 스키마 선언
-- MAGIC
-- MAGIC 앞서 언급했듯이 CTAS 명령문은 스키마 선언을 지원하지 않습니다. 앞서 언급했듯이 timestamp 열은 Unix 타임스탬프의 변형으로 보이며, 분석가가 인사이트를 얻는 데 가장 유용하지 않을 수 있습니다. 이러한 상황에서는 생성된 열이 유용합니다.
-- MAGIC
-- MAGIC 생성된 열은 Delta 테이블(DBR 8.3에 도입됨)의 다른 열에 대해 사용자 지정 함수를 기반으로 값이 자동으로 생성되는 특수한 유형의 열입니다.
-- MAGIC
-- MAGIC 아래 코드는 새 테이블을 생성하는 동안 다음을 수행하는 방법을 보여줍니다.
-- MAGIC 1. 열 이름 및 유형 지정
-- MAGIC 1. 날짜를 계산하기 위해 <a href="https://docs.databricks.com/delta/delta-batch.html#deltausegeneratedcolumns" target="_blank">생성된 열</a> 추가
-- MAGIC 1. 생성된 열에 대한 설명적인 열 주석 제공

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

-- DBTITLE 0,--i18n-33e94ae0-f443-4cc9-9691-30b8b08179aa
-- MAGIC %md
-- MAGIC
-- MAGIC **`date`**는 생성된 열이므로, **`date`** 열에 값을 제공하지 않고 **`purchase_dates`**에 값을 입력하면 Delta Lake에서 자동으로 계산합니다.
-- MAGIC
-- MAGIC **참고**: 아래 셀은 Delta Lake의 **`MERGE`** 문을 사용할 때 열을 생성할 수 있도록 설정을 구성합니다. 이 구문에 대해서는 이 과정의 후반부에서 자세히 살펴보겠습니다.

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

-- DBTITLE 0,--i18n-8da2bdf3-a0e1-4c5d-a016-d9bc38167f50
-- MAGIC %md
-- MAGIC
-- MAGIC 아래에서 볼 수 있듯이, 데이터가 삽입될 때 모든 날짜가 올바르게 계산되었지만, 원본 데이터나 삽입 쿼리 모두 이 필드에 값을 지정하지 않았습니다.
-- MAGIC
-- MAGIC 다른 Delta Lake 소스와 마찬가지로, 쿼리는 모든 쿼리에 대해 테이블의 가장 최신 스냅샷을 자동으로 읽습니다. **REFRESH TABLE**을 실행할 필요가 없습니다.

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- DBTITLE 0,--i18n-3bb038ec-4e33-40a1-b8ff-33b388e5dda1
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블에 삽입할 때 생성될 필드가 포함될 경우, 제공된 값이 생성된 열을 정의하는 데 사용된 로직에서 도출되는 값과 정확히 일치하지 않으면 삽입이 실패합니다.
-- MAGIC
-- MAGIC 아래 셀의 주석 처리를 제거하고 실행하면 이 오류를 확인할 수 있습니다.

-- COMMAND ----------

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- COMMAND ----------

-- DBTITLE 0,--i18n-43e1ab8b-0b34-4693-9d49-29d1f899c210
-- MAGIC %md
-- MAGIC
-- MAGIC ## 테이블 제약 조건 추가
-- MAGIC
-- MAGIC 위의 오류 메시지는 **`CHECK 제약 조건`**을 나타냅니다. 생성된 열은 CHECK 제약 조건의 특수 구현입니다.
-- MAGIC
-- MAGIC Delta Lake는 쓰기 시 스키마를 적용하므로 Databricks는 테이블에 추가되는 데이터의 품질과 무결성을 보장하기 위해 표준 SQL 제약 조건 관리 절을 지원할 수 있습니다.
-- MAGIC
-- MAGIC Databricks는 현재 두 가지 유형의 제약 조건을 지원합니다.
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#not-null-constraint" target="_blank">**`NOT NULL`** 제약 조건</a>
-- MAGIC * <a href="https://docs.databricks.com/delta/delta-constraints.html#check-constraint" target="_blank">**`CHECK`** 제약 조건</a>
-- MAGIC
-- MAGIC 두 경우 모두 제약 조건을 정의하기 전에 제약 조건을 위반하는 데이터가 테이블에 이미 있는지 확인해야 합니다. 테이블에 제약 조건이 추가되면 제약 조건을 위반하는 데이터는 쓰기 실패로 이어집니다.
-- MAGIC
-- MAGIC 아래에서는 테이블의 **date` 열에 **CHECK`** 제약 조건을 추가합니다. **CHECK`** 제약 조건은 데이터 세트를 필터링하는 데 사용할 수 있는 표준 **WHERE`** 절과 유사합니다.

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

-- DBTITLE 0,--i18n-32fe077c-4e4d-4830-9a80-9a6a2b5d2a61
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 제약 조건은 **TBLPROPERTIES`** 필드에 표시됩니다.

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates

-- COMMAND ----------

-- DBTITLE 0,--i18n-07f549c0-71af-4271-a8f5-91b4237d89e4
-- MAGIC %md
-- MAGIC
-- MAGIC ## 추가 옵션 및 메타데이터로 테이블 강화
-- MAGIC
-- MAGIC 지금까지 Delta Lake 테이블을 강화하는 옵션은 간략하게 살펴보았습니다.
-- MAGIC
-- MAGIC 아래에서는 여러 추가 구성 및 메타데이터를 포함하도록 CTAS 문을 개선하는 방법을 보여줍니다.
-- MAGIC
-- MAGIC **`SELECT`** 절은 파일 수집에 유용한 두 가지 내장 Spark SQL 명령을 활용합니다.
-- MAGIC * **`current_timestamp()`**는 로직이 실행될 때의 타임스탬프를 기록합니다.
-- MAGIC * **`input_file_name()`**은 테이블의 각 레코드에 대한 소스 데이터 파일을 기록합니다.
-- MAGIC
-- MAGIC 또한 소스의 타임스탬프 데이터에서 파생된 새 날짜 열을 생성하는 로직도 포함합니다.
-- MAGIC
-- MAGIC **`CREATE TABLE`** 절에는 여러 옵션이 포함되어 있습니다.
-- MAGIC * 테이블 내용을 더 쉽게 검색할 수 있도록 **`COMMENT`**가 추가되었습니다.
-- MAGIC * **`LOCATION`**이 지정되어 관리형 테이블이 아닌 외부 테이블이 생성됩니다.
-- MAGIC * 테이블은 날짜 열로 **`PARTITIONED BY`**됩니다. 즉, 각 데이터의 데이터는 대상 저장소 위치의 자체 디렉터리에 저장됩니다.
-- MAGIC
-- MAGIC **참고**: 여기서는 주로 구문과 그 영향을 보여주기 위해 파티셔닝을 설명합니다. 대부분의 Delta Lake 테이블(특히 중소 규모 데이터)은 파티셔닝의 이점을 얻지 못합니다. 파티셔닝은 데이터 파일을 물리적으로 분리하기 때문에, 이 방식은 작은 파일 문제를 야기하고 파일 압축 및 효율적인 데이터 건너뛰기를 방해할 수 있습니다. Hive 또는 HDFS에서 관찰되는 이점은 Delta Lake에는 적용되지 않으므로, 테이블을 파티셔닝하기 전에 경험이 풍부한 Delta Lake 설계자와 상의해야 합니다.
-- MAGIC
-- MAGIC **Delta Lake를 사용할 때는 대부분의 사용 사례에서 기본적으로 파티셔닝되지 않은 테이블을 사용하는 것이 가장 좋습니다.**

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

-- DBTITLE 0,--i18n-431d473d-162f-4a97-9afc-df47a787f409
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블에 추가된 메타데이터 필드는 레코드가 삽입된 시점과 위치를 파악하는 데 유용한 정보를 제공합니다. 특히 원본 데이터의 문제를 해결해야 할 때 유용합니다.
-- MAGIC
-- MAGIC 해당 테이블의 모든 주석과 속성은 **`DESCRIBE TABLE EXTENDED`**를 사용하여 검토할 수 있습니다.
-- MAGIC
-- MAGIC **참고**: Delta Lake는 테이블 생성 시 여러 테이블 속성을 자동으로 추가합니다.

-- COMMAND ----------

DESCRIBE EXTENDED users_pii

-- COMMAND ----------

-- DBTITLE 0,--i18n-227fec69-ab44-47b4-aef3-97a14eb4384a
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블에 사용된 위치를 나열하면 파티션 열 **`first_touch_date`**의 고유 값이 데이터 디렉터리를 생성하는 데 사용된다는 것을 알 수 있습니다.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-d188161b-3bc6-4095-aec9-508c09c14e0c
-- MAGIC %md
-- MAGIC
-- MAGIC ## 델타 레이크 테이블 복제
-- MAGIC 델타 레이크에는 델타 레이크 테이블을 효율적으로 복사하는 두 가지 옵션이 있습니다.
-- MAGIC
-- MAGIC **`DEEP CLONE`**은 소스 테이블의 데이터와 메타데이터를 타겟 테이블로 완전히 복사합니다. 이 복사는 증분 방식으로 수행되므로 이 명령을 다시 실행하면 소스 테이블의 변경 사항을 타겟 테이블로 동기화할 수 있습니다.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-c0aa62a8-7448-425c-b9de-45284ea87f8c
-- MAGIC %md
-- MAGIC
-- MAGIC 모든 데이터 파일을 복사해야 하므로 대용량 데이터 세트의 경우 시간이 꽤 오래 걸릴 수 있습니다.
-- MAGIC
-- MAGIC 현재 테이블을 수정할 위험 없이 변경 사항을 적용해 보기 위해 테이블의 복사본을 빠르게 생성하려면 **`SHALLOW CLONE`**가 좋은 선택이 될 수 있습니다. 얕은 복제는 델타 트랜잭션 로그만 복사하므로 데이터가 이동하지 않습니다.

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

-- DBTITLE 0,--i18n-045bfc09-41cc-4710-ab67-acab3881f128
-- MAGIC %md
-- MAGIC
-- MAGIC 두 경우 모두 복제된 테이블에 적용된 데이터 수정 사항은 원본과 별도로 추적 및 저장됩니다. 복제는 개발 중에 SQL 코드를 테스트하기 위해 테이블을 설정하는 좋은 방법입니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-32c272d6-cbe8-43ba-b051-9fe8e5586990
-- MAGIC %md
-- MAGIC
-- MAGIC ## 요약
-- MAGIC
-- MAGIC 이 노트북에서는 주로 Delta Lake 테이블을 생성하는 DDL과 구문에 중점을 두었습니다. 다음 노트북에서는 테이블에 업데이트를 작성하는 옵션을 살펴보겠습니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-c87f570b-e175-4000-8706-c571aa1cf6e1
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()