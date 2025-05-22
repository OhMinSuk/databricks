-- Databricks notebook source
-- DBTITLE 0,--i18n-0d527322-1a21-4a91-bc34-e7957e052a75
-- MAGIC %md
-- MAGIC
-- MAGIC # Delta Lake의 버전 관리, 최적화, VACUUM
-- MAGIC
-- MAGIC 이제 Delta Lake에서 기본적인 데이터 작업을 수행하는 데 익숙해지셨으니, Delta Lake의 몇 가지 고유한 기능에 대해 알아보겠습니다.
-- MAGIC
-- MAGIC 여기서 사용된 일부 키워드는 표준 ANSI SQL에 포함되지 않지만, 모든 Delta Lake 작업은 SQL을 사용하여 Databricks에서 실행할 수 있습니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC * **`OPTIMIZE`**를 사용하여 작은 파일을 압축합니다.
-- MAGIC * **`ZORDER`**를 사용하여 테이블을 인덱싱합니다.
-- MAGIC * Delta Lake 파일의 디렉터리 구조를 설명합니다.
-- MAGIC * 테이블 트랜잭션 기록을 검토합니다.
-- MAGIC * 이전 테이블 버전으로 쿼리하고 롤백합니다.
-- MAGIC * **`VACUUM`**을 사용하여 오래된 데이터 파일을 정리합니다.
-- MAGIC
-- MAGIC **참고자료**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks 문서</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">델타 Vacuum - Databricks 문서</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef1115dd-7242-476a-a929-a16aa09ce9c1
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설정 실행
-- MAGIC 먼저 설정 스크립트를 실행합니다. 이 스크립트는 각 사용자별로 적용되는 사용자 이름, 사용자 홈, 스키마를 정의합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.2 

-- COMMAND ----------

-- DBTITLE 0,--i18n-b10dbe8f-e936-4ca3-9d1e-8b471c4bc162
-- MAGIC %md
-- MAGIC
-- MAGIC ## 히스토리가 포함된 델타 테이블 생성
-- MAGIC
-- MAGIC 이 쿼리가 실행되기를 기다리는 동안 실행 중인 총 트랜잭션 수를 확인할 수 있는지 확인하세요.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-e932d675-aa26-42a7-9b55-654ac9896dab
-- MAGIC %md
-- MAGIC
-- MAGIC ## 테이블 세부 정보 확인
-- MAGIC
-- MAGIC Databricks는 기본적으로 Hive 메타스토어를 사용하여 스키마, 테이블 및 뷰를 등록합니다.
-- MAGIC
-- MAGIC `DESCRIBE EXTENDED`를 사용하면 테이블에 대한 중요한 메타데이터를 확인할 수 있습니다.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be5873-30b3-4e7e-9333-2c1e6f1cbe25
-- MAGIC %md
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`**은 테이블 메타데이터를 탐색할 수 있는 또 다른 명령입니다.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd7b24fa-7a2d-4f31-ab7c-fcc28d617d75
-- MAGIC %md
-- MAGIC
-- MAGIC **`Location`** 필드에 주목하세요.
-- MAGIC
-- MAGIC 지금까지 테이블을 스키마 내의 관계형 엔티티로만 생각해 왔지만, Delta Lake 테이블은 실제로 클라우드 객체 스토리지에 저장된 파일 컬렉션으로 백업됩니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ff9d64a-f0c4-4ee6-a007-888d4d082abe
-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Lake 파일 살펴보기
-- MAGIC
-- MAGIC Databricks Utilities 함수를 사용하여 Delta Lake 테이블을 뒷받침하는 파일을 확인할 수 있습니다.
-- MAGIC
-- MAGIC **참고**: Delta Lake를 사용하기 위해 지금 당장 이러한 파일에 대한 모든 것을 알 필요는 없지만, 기술이 어떻게 구현되는지 더 잘 이해하는 데 도움이 될 것입니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-1a84bb11-649d-463b-85ed-0125dc599524
-- MAGIC %md
-- MAGIC
-- MAGIC 디렉토리에는 여러 개의 Parquet 데이터 파일과 **`_delta_log`**라는 디렉터리가 있습니다.
-- MAGIC
-- MAGIC Delta Lake 테이블의 레코드는 Parquet 파일에 데이터로 저장됩니다.
-- MAGIC
-- MAGIC Delta Lake 테이블에 대한 트랜잭션은 **`_delta_log`**에 기록됩니다.
-- MAGIC
-- MAGIC 자세한 내용은 **`_delta_log`**를 참조하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-dbcbd76a-c740-40be-8893-70e37bd5e0d2
-- MAGIC %md
-- MAGIC
-- MAGIC 각 트랜잭션은 Delta Lake 트랜잭션 로그에 새로운 JSON 파일을 기록합니다. 여기에서 이 테이블에는 총 8개의 트랜잭션이 있음을 확인할 수 있습니다(Delta Lake는 인덱스가 0개입니다).

-- COMMAND ----------

-- DBTITLE 0,--i18n-101dffc0-260a-4078-97db-cb1de8d705a8
-- MAGIC %md
-- MAGIC
-- MAGIC ## 데이터 파일 추론
-- MAGIC
-- MAGIC 아주 작은 테이블에 많은 데이터 파일이 있는 것을 방금 보았습니다.
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`**을 사용하면 파일 개수를 포함하여 델타 테이블에 대한 다른 세부 정보를 볼 수 있습니다.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-cb630727-afad-4dde-9d71-bcda9e579de9
-- MAGIC %md
-- MAGIC
-- MAGIC 여기서 현재 테이블에는 현재 버전에 4개의 데이터 파일이 포함되어 있음을 알 수 있습니다. 그렇다면 다른 Parquet 파일들은 테이블 디렉터리에서 어떤 역할을 할까요?
-- MAGIC
-- MAGIC Delta Lake는 변경된 데이터가 포함된 파일을 덮어쓰거나 즉시 삭제하는 대신, 트랜잭션 로그를 사용하여 현재 버전의 테이블에서 파일이 유효한지 여부를 나타냅니다.
-- MAGIC
-- MAGIC 여기에서는 위의 **`MERGE`** 문에 해당하는 트랜잭션 로그를 살펴보겠습니다. 이 로그에서는 레코드가 삽입, 업데이트 및 삭제되었습니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3221b77b-6d57-4654-afc3-dcb9dfa62be8
-- MAGIC %md
-- MAGIC
-- MAGIC **`add`** 열에는 테이블에 새로 작성된 모든 파일 목록이 포함됩니다. **remove`** 열은 테이블에 더 이상 포함되지 않아야 할 파일을 나타냅니다.
-- MAGIC
-- MAGIC Delta Lake 테이블을 쿼리할 때 쿼리 엔진은 트랜잭션 로그를 사용하여 현재 버전에서 유효한 모든 파일을 확인하고 다른 모든 데이터 파일은 무시합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc6dee2e-406c-48b2-9780-74408c93162d
-- MAGIC %md
-- MAGIC
-- MAGIC ## 작은 파일 압축 및 인덱싱
-- MAGIC
-- MAGIC 작은 파일은 다양한 이유로 발생할 수 있습니다. 이 경우에는 하나 또는 여러 개의 레코드만 삽입되는 여러 작업을 수행했습니다.
-- MAGIC
-- MAGIC **`OPTIMIZE`** 명령을 사용하면 파일이 테이블 크기에 따라 크기가 조정되는 최적의 크기로 결합됩니다.
-- MAGIC
-- MAGIC **`OPTIMIZE`**는 레코드를 결합하고 결과를 다시 작성하여 기존 데이터 파일을 대체합니다.
-- MAGIC
-- MAGIC **`OPTIMIZE`**를 실행할 때 사용자는 선택적으로 **`ZORDER`** 인덱싱을 위해 하나 또는 여러 개의 필드를 지정할 수 있습니다. Z-order의 구체적인 계산 방식은 중요하지 않지만, 제공된 필드를 기준으로 필터링할 때 데이터 파일 내에서 유사한 값을 가진 데이터를 함께 배치하여 데이터 검색 속도를 높입니다.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- DBTITLE 0,--i18n-5f412c12-88c7-4e43-bda2-60ec5c749b2a
-- MAGIC %md
-- MAGIC
-- MAGIC 데이터가 너무 작기 때문에 **`ZORDER`**는 아무런 이점도 제공하지 않지만, 이 작업으로 생성된 모든 지표를 확인할 수 있습니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad93f7e-4bb1-4051-8b9c-b685164e3b45
-- MAGIC %md
-- MAGIC
-- MAGIC ## 델타 레이크 트랜잭션 검토
-- MAGIC
-- MAGIC 델타 레이크 테이블의 모든 변경 사항은 트랜잭션 로그에 저장되므로 <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">테이블 기록</a>을 쉽게 검토할 수 있습니다.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- DBTITLE 0,--i18n-ed297545-7997-4e75-8bf6-0c204a707956
-- MAGIC %md
-- MAGIC
-- MAGIC 예상대로 **`OPTIMIZE`**는 테이블의 다른 버전을 생성했습니다. 즉, 버전 8이 최신 버전입니다.
-- MAGIC
-- MAGIC 트랜잭션 로그에서 제거된 것으로 표시된 모든 추가 데이터 파일을 기억하시나요? 이 파일들을 통해 테이블의 이전 버전을 쿼리할 수 있습니다.
-- MAGIC
-- MAGIC 이러한 시간 이동 쿼리는 정수 버전 또는 타임스탬프를 지정하여 수행할 수 있습니다.
-- MAGIC
-- MAGIC **참고**: 대부분의 경우 타임스탬프를 사용하여 원하는 시점의 데이터를 다시 생성합니다. 이 데모에서는 버전을 사용하는데, 이는 결정적이므로(이 데모는 나중에 언제든지 실행할 수 있습니다) 버전을 사용합니다.

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- DBTITLE 0,--i18n-d1d03156-6d88-4d4c-ae8e-ddfe49d957d7
-- MAGIC %md
-- MAGIC
-- MAGIC 시간 여행에서 중요한 점은 현재 버전에 대한 트랜잭션을 취소하여 테이블의 이전 상태를 재생성하는 것이 아니라, 지정된 버전에서 유효하다고 표시된 모든 데이터 파일을 쿼리하는 것입니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-78cf75b0-0403-4aa5-98c7-e3aabbef5d67
-- MAGIC %md
-- MAGIC
-- MAGIC ## 롤백 버전
-- MAGIC
-- MAGIC 테이블에서 일부 레코드를 수동으로 삭제하는 쿼리를 입력하다가 실수로 다음 상태에서 이 쿼리를 실행했다고 가정해 보겠습니다.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-7f7936c3-3aa2-4782-8425-78e6e7634d79
-- MAGIC %md
-- MAGIC
-- MAGIC 삭제에 의해 영향을 받은 행 수에 **`-1`**이 표시되면 전체 데이터 디렉터리가 삭제되었음을 의미합니다.
-- MAGIC
-- MAGIC 아래에서 이를 확인해 보겠습니다.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-9d3908f4-e6bb-40a6-92dd-d7d12d28a032
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블의 모든 레코드를 삭제하는 것은 바람직하지 않을 것입니다. 다행히 이 커밋을 롤백할 수 있습니다.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- DBTITLE 0,--i18n-902966c3-830a-44db-9e59-dec82b98a9c2
-- MAGIC %md
-- MAGIC
-- MAGIC **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">명령</a>은 트랜잭션으로 기록됩니다. 실수로 테이블의 모든 레코드를 삭제했다는 사실을 완전히 숨길 수는 없지만, 작업을 실행 취소하고 테이블을 원하는 상태로 되돌릴 수는 있습니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-847452e6-2668-463b-afdf-52c1f512b8d3
-- MAGIC %md
-- MAGIC
-- MAGIC ## 오래된 파일 정리
-- MAGIC
-- MAGIC Databricks는 Delta Lake 테이블에서 오래된 로그 파일(기본적으로 30일 이상)을 자동으로 정리합니다.
-- MAGIC 체크포인트가 기록될 때마다 Databricks는 이 보존 기간보다 오래된 로그 항목을 자동으로 정리합니다.
-- MAGIC
-- MAGIC Delta Lake 버전 관리 및 시간 이동 기능은 최신 버전을 쿼리하고 쿼리를 롤백하는 데 유용하지만, 모든 버전의 대규모 운영 테이블에 대한 데이터 파일을 무기한으로 보관하는 것은 비용이 많이 들고(개인 식별 정보가 있는 경우 규정 준수 문제로 이어질 수 있음) 비용이 많이 듭니다.
-- MAGIC
-- MAGIC 오래된 데이터 파일을 수동으로 삭제하려면 **`VACUUM`** 작업을 사용하면 됩니다.
-- MAGIC
-- MAGIC 다음 셀의 주석 처리를 제거하고 **`0 HOURS`** 보존 기간을 설정하여 실행하면 현재 버전만 유지됩니다.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3af389e-e93f-433c-8d47-b38f8ded5ecd
-- MAGIC %md
-- MAGIC
-- MAGIC 기본적으로 **`VACUUM`**은 7일 미만의 파일은 삭제하지 못하도록 하여 장기 실행 작업이 삭제할 파일을 참조하지 않도록 합니다. Delta 테이블에서 **`VACUUM`**을 실행하면 지정된 데이터 보존 기간보다 이전 버전으로 시간 여행을 할 수 없습니다. 데모에서는 Databricks가 **`0 HOURS`** 보존 기간을 지정하는 코드를 실행하는 것을 볼 수 있습니다. 이는 단순히 기능을 보여주기 위한 것이며 일반적으로 프로덕션 환경에서는 실행되지 않습니다.
-- MAGIC
-- MAGIC 다음 셀에서 다음을 수행합니다.
-- MAGIC 1. 데이터 파일의 조기 삭제를 방지하기 위해 확인란을 해제합니다.
-- MAGIC 1. **`VACUUM`** 명령의 로깅이 활성화되어 있는지 확인합니다.
-- MAGIC 1. **`DRY RUN`** 버전의 vacuum을 사용하여 삭제할 모든 레코드를 출력합니다.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c825ee6-e584-48a1-8d75-f616d7ed53ac
-- MAGIC %md
-- MAGIC
-- MAGIC **`VACUUM`**을 실행하고 위의 10개 파일을 삭제하면, 해당 파일이 구현되어야 하는 테이블 버전에 대한 액세스가 영구적으로 제거됩니다.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-6574e909-c7ee-4b0b-afb8-8bac83dacdd3
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 디렉터리를 확인하여 파일이 성공적으로 삭제되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3437d5f0-c0e2-4486-8142-413a1849bc40
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()