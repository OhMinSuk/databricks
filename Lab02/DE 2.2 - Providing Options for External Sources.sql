-- Databricks notebook source
-- DBTITLE 0,--i18n-ac3b4f33-4b00-4169-a663-000fddc1fb9d
-- MAGIC %md
-- MAGIC
-- MAGIC # 외부 소스 옵션 제공
-- MAGIC 자체 설명적 형식에서는 파일을 직접 쿼리하는 것이 효과적이지만, 많은 데이터 소스는 레코드를 제대로 수집하기 위해 추가 구성이나 스키마 선언이 필요합니다.
-- MAGIC
-- MAGIC 이번 레슨에서는 외부 데이터 소스를 사용하여 테이블을 생성합니다. 이러한 테이블은 아직 Delta Lake 형식으로 저장되지 않으므로 Lakehouse에 최적화되지 않지만, 이 기법은 다양한 외부 시스템에서 데이터를 추출하는 데 도움이 됩니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 레슨을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - Spark SQL을 사용하여 외부 소스에서 데이터를 추출하는 옵션을 구성합니다.
-- MAGIC - 다양한 파일 형식에 대해 외부 데이터 소스에 대한 테이블을 생성합니다.
-- MAGIC - 외부 소스에 대해 정의된 테이블을 쿼리할 때의 기본 동작을 설명합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-d0bd783f-524d-4953-ac3c-3f1191d42a9e
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설치 실행
-- MAGIC
-- MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-90830ba2-82d9-413a-974e-97295b7246d0
-- MAGIC %md
-- MAGIC
-- MAGIC ## 직접 쿼리가 작동하지 않는 경우
-- MAGIC
-- MAGIC CSV 파일은 가장 일반적인 파일 형식 중 하나이지만, 이러한 파일에 대한 직접 쿼리를 실행해도 원하는 결과를 얻는 경우는 드뭅니다.

-- COMMAND ----------

SELECT * FROM csv.`${DA.paths.sales_csv}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-2de59e8e-3bc3-4609-96ad-e8985b250154
-- MAGIC %md
-- MAGIC
-- MAGIC 위에서 다음을 확인할 수 있습니다.
-- MAGIC 1. 헤더 행이 테이블 행으로 추출됩니다.
-- MAGIC 1. 모든 열이 단일 열로 로드됩니다.
-- MAGIC 1. 파일이 파이프(**`|`**)로 구분됩니다.
-- MAGIC 1. 마지막 열에 잘리는 중첩 데이터가 포함되어 있는 것으로 보입니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-3eae3be1-134c-4f3b-b423-d081fb780914
-- MAGIC %md
-- MAGIC
-- MAGIC ## 읽기 옵션을 사용하여 외부 데이터에 테이블 등록
-- MAGIC
-- MAGIC Spark는 기본 설정을 사용하여 일부 자체 설명적 데이터 소스를 효율적으로 추출하지만, 많은 형식에서는 스키마 선언이나 기타 옵션이 필요합니다.
-- MAGIC
-- MAGIC 외부 소스에 대해 테이블을 생성할 때 설정할 수 있는 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">추가 구성</a>이 많지만, 아래 구문은 대부분의 형식에서 데이터를 추출하는 데 필요한 필수 구성 요소를 보여줍니다.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION = path<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC 옵션은 키는 따옴표 없이, 값은 따옴표로 묶어 전달됩니다. Spark는 사용자 지정 옵션을 사용하여 다양한 <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">데이터 소스</a>를 지원하며, 일부 시스템은 외부 <a href="https://docs.databricks.com/libraries/index.html" target="_blank">라이브러리</a>를 통해 비공식적인 지원을 받을 수 있습니다.
-- MAGIC
-- MAGIC **참고**: 작업 공간 설정에 따라 라이브러리를 로드하고 일부 데이터 소스에 필요한 보안 설정을 구성하는 데 관리자의 도움이 필요할 수 있습니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-1c947c25-bb8b-4cad-acca-29651e191108
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀은 Spark SQL DDL을 사용하여 외부 CSV 소스에 대한 테이블을 생성하는 방법을 보여줍니다. 다음을 지정합니다.
-- MAGIC 1. 열 이름 및 유형
-- MAGIC 1. 파일 형식
-- MAGIC 1. 필드를 구분하는 데 사용되는 구분 기호
-- MAGIC 1. 헤더 존재 여부
-- MAGIC 1. 이 데이터가 저장된 경로

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${DA.paths.sales_csv}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-4631ecfc-06b5-494a-904f-8577e345c98d
-- MAGIC %md
-- MAGIC
-- MAGIC **참고:** PySpark에서 외부 소스에 대한 테이블을 생성하려면 이 SQL 코드를 **spark.sql()`** 함수로 래핑할 수 있습니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS sales_csv
-- MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC   header = "true",
-- MAGIC   delimiter = "|"
-- MAGIC )
-- MAGIC LOCATION "{DA.paths.sales_csv}"
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 0,--i18n-964019da-1d24-4a60-998a-bbf23ffc64a6
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 선언 중에 데이터가 이동하지 않았음을 유의하세요.
-- MAGIC
-- MAGIC 파일을 직접 쿼리하여 뷰를 생성했을 때와 마찬가지로, 여전히 외부 위치에 저장된 파일을 가리키고 있습니다.
-- MAGIC
-- MAGIC 다음 셀을 실행하여 데이터가 올바르게 로드되는지 확인하세요.

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-d11afb5e-08d3-42c3-8904-14cdddfe5431
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 선언 시 전달된 모든 메타데이터와 옵션은 메타스토어에 저장되므로 해당 위치의 데이터는 항상 이러한 옵션을 사용하여 읽힙니다.
-- MAGIC
-- MAGIC **참고**: CSV를 데이터 소스로 사용하는 경우, 소스 디렉터리에 추가 데이터 파일이 추가되더라도 열 순서가 변경되지 않도록 해야 합니다. 이 데이터 형식은 스키마를 엄격하게 적용하지 않으므로 Spark는 테이블 선언 시 지정된 순서대로 열을 로드하고 열 이름과 데이터 유형을 적용합니다.
-- MAGIC
-- MAGIC 테이블에서 **`DESCRIBE EXTENDED`**를 실행하면 테이블 정의와 관련된 모든 메타데이터가 표시됩니다.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdbb45bc-72b3-4610-97a6-accd30ec8fec
-- MAGIC %md
-- MAGIC
-- MAGIC ## 외부 데이터 소스를 사용한 테이블의 한계
-- MAGIC
-- MAGIC Databricks에서 다른 과정을 수강했거나 회사 자료를 살펴보셨다면 Delta Lake와 Lakehouse에 대해 들어보셨을 것입니다. 외부 데이터 소스에 대한 테이블이나 쿼리를 정의할 때 Delta Lake 및 Lakehouse와 관련된 성능 보장을 기대할 수 **없습니다**.
-- MAGIC
-- MAGIC 예를 들어, Delta Lake 테이블은 항상 최신 버전의 소스 데이터를 쿼리하도록 보장하지만, 다른 데이터 소스에 등록된 테이블은 이전에 캐시된 버전을 나타낼 수 있습니다.
-- MAGIC
-- MAGIC 아래 셀은 테이블의 기반 파일을 직접 업데이트하는 외부 시스템을 나타내는 것으로 생각할 수 있는 몇 가지 로직을 실행합니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC       .option("header", "true")
-- MAGIC       .option("delimiter", "|")
-- MAGIC       .csv(DA.paths.sales_csv)
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(DA.paths.sales_csv, header="true"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-28b2112b-4eb2-4bd4-ad76-131e010dfa44
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블의 현재 레코드 수를 살펴보면, 표시되는 숫자는 새로 삽입된 행을 반영하지 않습니다.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-bede6aed-2b6b-4ee7-8017-dfce2217e8b4
-- MAGIC %md
-- MAGIC
-- MAGIC 이전에 이 데이터 소스를 쿼리했을 때 Spark는 로컬 저장소에 기본 데이터를 자동으로 캐시했습니다. 이를 통해 후속 쿼리에서 Spark는 이 로컬 캐시만 쿼리하여 최적의 성능을 제공합니다.
-- MAGIC
-- MAGIC 외부 데이터 소스는 Spark에 이 데이터를 새로 고침하도록 구성되어 있지 않습니다.
-- MAGIC
-- MAGIC `REFRESH TABLE` 명령을 실행하여 데이터 캐시를 수동으로 새로 고칠 수 있습니다.

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-656a9929-a4e5-4fb1-bfe6-6c3cc7137598
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블을 새로 고치면 캐시가 무효화되므로 원래 데이터 소스를 다시 스캔하고 모든 데이터를 메모리로 다시 가져와야 합니다.
-- MAGIC
-- MAGIC 매우 큰 데이터 세트의 경우 상당한 시간이 소요될 수 있습니다.

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-ee1ac9ff-add1-4247-bc44-2e71f0447390
-- MAGIC %md
-- MAGIC
-- MAGIC ## SQL 데이터베이스에서 데이터 추출
-- MAGIC SQL 데이터베이스는 매우 일반적인 데이터 소스이며, Databricks는 다양한 SQL 버전에 연결하기 위한 표준 JDBC 드라이버를 제공합니다.
-- MAGIC
-- MAGIC 이러한 연결을 생성하는 일반적인 구문은 다음과 같습니다.
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC 아래 코드 샘플에서는 <a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>에 연결합니다.
-- MAGIC
-- MAGIC **참고:** SQLite는 로컬 파일을 사용하여 데이터베이스를 저장하며 포트, 사용자 이름 또는 비밀번호를 요구하지 않습니다.
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png"> **경고**: JDBC 서버의 백엔드 구성은 이 노트북을 단일 노드 클러스터에서 실행한다고 가정합니다. 여러 워커가 있는 클러스터에서 실행하는 경우, 실행기에서 실행 중인 클라이언트는 드라이버에 연결할 수 없습니다.

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)

-- COMMAND ----------

-- DBTITLE 0,--i18n-33fb962c-707d-43b8-8a37-41ebb5d83b2f
-- MAGIC %md
-- MAGIC
-- MAGIC 이제 마치 로컬에 정의된 것처럼 이 테이블을 쿼리할 수 있습니다.

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- DBTITLE 0,--i18n-3576239e-8f73-4ef9-982e-e42542d4fc70
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 메타데이터를 살펴보면 외부 시스템에서 스키마 정보를 캡처했음을 알 수 있습니다.
-- MAGIC
-- MAGIC 저장소 속성(연결과 관련된 사용자 이름 및 비밀번호 포함)은 자동으로 삭제됩니다.

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- DBTITLE 0,--i18n-c20051d4-f3c3-483c-b4fa-ff2d356fcd5e
-- MAGIC %md
-- MAGIC
-- MAGIC 지정된 위치의 콘텐츠를 나열하면 로컬에 데이터가 저장되지 않음을 확인할 수 있습니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC location = spark.sql("DESCRIBE EXTENDED users_jdbc").filter(F.col("col_name") == "Location").first()["data_type"]
-- MAGIC print(location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(location)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cb11f07-755c-4fb2-a122-1eb340033712
-- MAGIC %md
-- MAGIC
-- MAGIC 데이터웨어하우스와 같은 일부 SQL 시스템에는 사용자 지정 드라이버가 있습니다. Spark는 다양한 외부 데이터베이스와 서로 다르게 상호 작용하지만, 두 가지 기본적인 접근 방식은 다음과 같습니다.
-- MAGIC 1. 전체 소스 테이블을 Databricks로 이동한 다음 현재 활성 클러스터에서 로직을 실행합니다.
-- MAGIC 1. 쿼리를 외부 SQL 데이터베이스로 푸시하고 결과만 Databricks로 다시 전송합니다.
-- MAGIC
-- MAGIC 두 경우 모두 외부 SQL 데이터베이스에서 매우 큰 데이터 세트를 사용하면 다음과 같은 이유로 상당한 오버헤드가 발생할 수 있습니다.
-- MAGIC 1. 공용 인터넷을 통해 모든 데이터를 이동하는 데 따른 네트워크 전송 지연 시간
-- MAGIC 1. 빅데이터 쿼리에 최적화되지 않은 소스 시스템에서 쿼리 로직을 실행합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-c973af61-2b79-4c55-8e32-f6a8176ea9e8
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()