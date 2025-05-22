-- Databricks notebook source
-- DBTITLE 0,--i18n-5ec757b3-50cf-43ac-a74d-6902d3e18983
-- MAGIC %md
-- MAGIC
-- MAGIC # SQL UDF 및 제어 흐름
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC * SQL UDF 정의 및 등록
-- MAGIC * SQL UDF 공유에 사용되는 보안 모델 설명
-- MAGIC * SQL 코드에서 **`CASE`** / **`WHEN`** 문 사용
-- MAGIC * 사용자 지정 제어 흐름을 위해 SQL UDF에서 **`CASE`** / **`WHEN`** 문 활용

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd5d37b8-b720-4a88-a2cf-9b3c43f697eb
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설정 실행
-- MAGIC 다음 셀을 실행하여 환경을 설정하세요.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.7A

-- COMMAND ----------

-- DBTITLE 0,--i18n-e8fa445b-db52-43c4-a649-9904526c6a04
-- MAGIC %md
-- MAGIC
-- MAGIC ## 사용자 정의 함수
-- MAGIC
-- MAGIC Spark SQL의 사용자 정의 함수(UDF)를 사용하면 사용자 지정 SQL 로직을 데이터베이스에 함수로 등록하여 Databricks에서 SQL을 실행할 수 있는 모든 곳에서 재사용할 수 있습니다. 이러한 함수는 SQL에 기본적으로 등록되며, 대규모 데이터세트에 사용자 지정 로직을 적용할 때 Spark의 모든 최적화를 유지합니다.
-- MAGIC
-- MAGIC SQL UDF를 생성하려면 최소한 함수 이름, 선택적 매개변수, 반환될 유형, 그리고 몇 가지 사용자 지정 로직이 필요합니다.
-- MAGIC
-- MAGIC 아래의 **`sale_announcement`**라는 간단한 함수는 **`item_name`**과 **`item_price`**를 매개변수로 받습니다. 이 함수는 품목을 원래 가격의 80%로 판매한다는 것을 알리는 문자열을 반환합니다.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a5dfa8f-f9e7-4b5f-b229-30bed4497009
-- MAGIC %md
-- MAGIC
-- MAGIC 이 함수는 Spark 처리 엔진 내에서 열의 모든 값에 병렬로 적용됩니다. SQL UDF는 Databricks에서 실행되도록 최적화된 사용자 지정 로직을 정의하는 효율적인 방법입니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9735833-a4f3-4966-8739-eb351025dc28
-- MAGIC %md
-- MAGIC
-- MAGIC ## SQL UDF의 범위 및 권한
-- MAGIC SQL 사용자 정의 함수:
-- MAGIC - 실행 환경(노트북, DBSQL 쿼리, 작업 등) 간에 유지됩니다.
-- MAGIC - 메타스토어에 객체로 존재하며 데이터베이스, 테이블 또는 뷰와 동일한 테이블 ACL에 의해 관리됩니다.
-- MAGIC - SQL UDF를 **생성**하려면 카탈로그에 **`USE CATALOG`**, 스키마에 **`USE SCHEMA`** 및 **`CREATE FUNCTION`**이 필요합니다.
-- MAGIC - SQL UDF를 **사용**하려면 카탈로그에 **`USE CATALOG`**, 스키마에 **`USE SCHEMA`**, 함수에 **`EXECUTE`**가 필요합니다.
-- MAGIC
-- MAGIC **`DESCRIBE FUNCTION`**을 사용하면 함수가 등록된 위치와 예상 입력 및 반환되는 내용에 대한 기본 정보를 확인할 수 있습니다(**`DESCRIBE FUNCTION EXTENDED`**를 사용하면 더 자세한 정보를 얻을 수 있습니다).

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

-- DBTITLE 0,--i18n-091c02b4-07b5-4b2c-8e1e-8cb561eed5a3
-- MAGIC %md
-- MAGIC
-- MAGIC 함수 설명 하단의 **`Body`** 필드는 함수 자체에서 사용된 SQL 로직을 보여줍니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf549dbc-edb7-465f-a310-0f5c04cfbe0a
-- MAGIC %md
-- MAGIC
-- MAGIC ## 간단한 제어 흐름 함수
-- MAGIC
-- MAGIC SQL UDF를 **`CASE`** / **`WHEN`** 절 형태의 제어 흐름과 결합하면 SQL 워크로드 내에서 제어 흐름 실행을 최적화할 수 있습니다. 표준 SQL 구문 구조인 **`CASE`** / **`WHEN`**을 사용하면 테이블 내용에 따라 다른 결과를 갖는 여러 조건문을 평가할 수 있습니다.
-- MAGIC
-- MAGIC 여기서는 SQL을 실행할 수 있는 모든 곳에서 재사용 가능한 함수로 이 제어 흐름 논리를 래핑하는 방법을 보여줍니다.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-14f5f9df-d17e-4e6a-90b0-d22bbc4e1e10
-- MAGIC %md
-- MAGIC
-- MAGIC 여기에 제공된 예제는 간단하지만, 동일한 기본 원칙을 사용하여 Spark SQL에서 네이티브 실행을 위한 사용자 지정 계산 및 로직을 추가할 수 있습니다.
-- MAGIC
-- MAGIC 특히 정의된 프로시저나 사용자 지정 수식이 많은 시스템에서 사용자를 마이그레이션하는 기업의 경우, SQL UDF를 사용하면 소수의 사용자가 일반적인 보고 및 분석 쿼리에 필요한 복잡한 로직을 정의할 수 있습니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-451ef10d-9e38-4b71-ad69-9c2ed74601b5
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()