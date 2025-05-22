-- Databricks notebook source
-- DBTITLE 0,--i18n-4c4121ee-13df-479f-be62-d59452a5f261
-- MAGIC
-- MAGIC %md
-- MAGIC
-- MAGIC # Databricks의 스키마 및 테이블
-- MAGIC 이 데모에서는 스키마와 테이블을 생성하고 살펴보겠습니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 수업을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC * Spark SQL DDL을 사용하여 스키마 및 테이블 정의
-- MAGIC * **`LOCATION`** 키워드가 기본 저장소 디렉터리에 미치는 영향 설명
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Schemas and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-acb0c723-a2bf-4d00-b6cb-6e9aef114985
-- MAGIC %md
-- MAGIC
-- MAGIC ## 수업 설정
-- MAGIC 다음 스크립트는 이 데모의 이전 실행 내용을 삭제하고 SQL 쿼리에 사용될 일부 Hive 변수를 구성합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-cc3d2766-764e-44bb-a04b-b03ae9530b6d
-- MAGIC %md
-- MAGIC
-- MAGIC ## 스키마
-- MAGIC 스키마(데이터베이스)를 만드는 것부터 시작해 보겠습니다.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_custom_location LOCATION '${da.paths.working_dir}/${da.schema_name}_custom_location.db';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.working_dir, DA.schema_name)

-- COMMAND ----------

-- DBTITLE 0,--i18n-427db4b9-fa6c-47aa-ae70-b95087298362
-- MAGIC %md
-- MAGIC
-- MAGIC 첫 번째 스키마(데이터베이스)의 위치는 기본 위치인 **`dbfs:/user/hive/warehouse/`**에 있으며, 스키마 디렉터리는 스키마 이름에 **`.db`** 확장자가 붙습니다.

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

DESCRIBE SCHEMA 1dt035_wwoo_da_delp_default_location

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0fda220-4a73-419b-969f-664dd4b80024
-- MAGIC %md
-- MAGIC ## 관리형 테이블
-- MAGIC
-- MAGIC 위치 경로를 지정하지 않고 **관리형** 테이블을 생성합니다.
-- MAGIC
-- MAGIC 위에서 생성한 스키마(데이터베이스)에 테이블을 생성합니다.
-- MAGIC
-- MAGIC 테이블의 열과 데이터 유형을 유추할 데이터가 없으므로 테이블 스키마를 정의해야 합니다.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
INSERT INTO managed_table 
VALUES (3, 2, 1);
SELECT * FROM managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c422056-45b4-419d-b4a6-2c3252e82575
-- MAGIC %md
-- MAGIC
-- MAGIC 확장된 테이블 설명을 확인하여 위치를 찾을 수 있습니다(결과에서 아래로 스크롤해야 합니다).

-- COMMAND ----------

DESCRIBE DETAIL managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bdc6475c-1c77-46a5-9ea1-04d5a538c225
-- MAGIC %md
-- MAGIC
-- MAGIC 기본적으로, 지정된 위치가 없는 스키마의 **관리되는** 테이블은 **dbfs:/user/hive/warehouse/<schema_name>.db/`** 디렉터리에 생성됩니다.
-- MAGIC
-- MAGIC 예상대로 테이블의 데이터와 메타데이터가 해당 위치에 저장되어 있는 것을 확인할 수 있습니다.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
-- MAGIC # tbl_location = dbfs:/user/hive/warehouse/santiago_ortiz_be24_da_delp_default_location.db/managed_table
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-507a84a5-f60f-4923-8f48-475ee3270dbd
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블을 삭제하세요.
-- MAGIC
-- MAGIC  

-- COMMAND ----------

DROP TABLE managed_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-0b390bf4-3e3b-4d1a-bcb8-296fa1a7edb8
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 디렉터리와 로그 및 데이터 파일이 삭제됩니다. 스키마(데이터베이스) 디렉터리만 남습니다.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC  
-- MAGIC ## 외부 테이블
-- MAGIC 다음으로, 샘플 데이터에서 **외부** (관리되지 않는) 테이블을 생성합니다.
-- MAGIC
-- MAGIC 사용할 데이터는 CSV 형식입니다. 원하는 디렉터리에 **`LOCATION`**을 입력하여 Delta 테이블을 생성하려고 합니다.

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- 잘못된 줄이 발견되면 RuntimeException으로 파일 구문 분석을 중단합니다.
);
CREATE OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
  SELECT * FROM temp_delays;

SELECT * FROM external_table; 

-- COMMAND ----------

-- DBTITLE 0,--i18n-6b5d7597-1fc1-4747-b5bb-07f67d806c2b
-- MAGIC %md
-- MAGIC
-- MAGIC 이번 강의의 작업 디렉터리에서 테이블 데이터의 위치를 ​​확인해 보겠습니다.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-72f7bef4-570b-4c20-9261-b763b66b6942
-- MAGIC %md
-- MAGIC
-- MAGIC 이제 테이블을 삭제합니다.

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- DBTITLE 0,--i18n-f71374ea-db51-4a2c-8920-9f8a000850df
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 정의는 더 이상 메타스토어에 존재하지 않지만, 기본 데이터는 그대로 유지됩니다.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-7defc948-a8e4-4019-9633-0886d653b7c6
-- MAGIC %md
-- MAGIC
-- MAGIC ## 정리
-- MAGIC 스키마를 삭제합니다.

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb4a8ae9-450b-479f-9e16-a76f1131bd1a
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()