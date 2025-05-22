-- Databricks notebook source
-- DBTITLE 0,--i18n-65583202-79bf-45b7-8327-d4d5562c831d
-- MAGIC %md
-- MAGIC
-- MAGIC # 델타 테이블 조작 실습
-- MAGIC
-- MAGIC 이 노트북은 델타 레이크가 데이터 레이크하우스에 제공하는 몇 가지 난해한 기능들을 직접 살펴보는 데 도움이 됩니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 실습을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - 테이블 기록 검토
-- MAGIC - 이전 테이블 버전 쿼리 및 특정 버전으로 테이블 롤백
-- MAGIC - 파일 압축 및 Z-오더 인덱싱 수행
-- MAGIC - 영구 삭제로 표시된 파일 미리 보기 및 삭제 커밋

-- COMMAND ----------

-- DBTITLE 0,--i18n-065e2f94-2251-4701-b0b6-f4b86323dec8
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설정
-- MAGIC 다음 스크립트를 실행하여 필요한 변수를 설정하고 이 노트북의 이전 실행을 삭제하세요. 이 셀을 다시 실행하면 랩을 처음부터 다시 시작할 수 있습니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-56940be8-afa9-49d8-8949-b4bcdb343f9d
-- MAGIC %md
-- MAGIC
-- MAGIC ## Bean 컬렉션 히스토리 생성
-- MAGIC
-- MAGIC 아래 셀에는 다양한 테이블 작업이 포함되어 있으며, **`beans`** 테이블에 대한 스키마는 다음과 같습니다.
-- MAGIC
-- MAGIC | 필드 이름 | 필드 유형 |
-- MAGIC | --- | --- |
-- MAGIC | 이름 | 문자열 |
-- MAGIC | 색상 | 문자열 |
-- MAGIC | 그램 | 부동 소수점 |
-- MAGIC | 맛있는 | 부울 |

-- COMMAND ----------

DROP TABLE IF EXISTS beans

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf6ff074-4166-4d51-92e5-67e7f2084c9b
-- MAGIC %md
-- MAGIC
-- MAGIC ## 테이블 기록 검토
-- MAGIC
-- MAGIC Delta Lake의 트랜잭션 로그는 테이블의 내용이나 설정을 수정하는 각 트랜잭션에 대한 정보를 저장합니다.
-- MAGIC
-- MAGIC 아래의 **`beans`** 테이블 기록을 검토하세요.

-- COMMAND ----------

-- TODO
DESCRIBE HISTORY beans

-- COMMAND ----------

describe history beans;

-- COMMAND ----------

-- DBTITLE 0,--i18n-fb56d746-8889-41c1-ba73-576282582534
-- MAGIC %md
-- MAGIC
-- MAGIC 이전 작업이 모두 설명된 대로 완료되면 테이블의 7개 버전이 표시됩니다(**참고**: Delta Lake 버전 관리는 0부터 시작하므로 최대 버전 번호는 6입니다).
-- MAGIC
-- MAGIC 작업은 다음과 같습니다.
-- MAGIC
-- MAGIC | version | operation |
-- MAGIC | --- | --- |
-- MAGIC | 0 | CREATE TABLE |
-- MAGIC | 1 | WRITE |
-- MAGIC | 2 | WRITE |
-- MAGIC | 3 | UPDATE |
-- MAGIC | 4 | UPDATE |
-- MAGIC | 5 | DELETE |
-- MAGIC | 6 | MERGE |
-- MAGIC
-- MAGIC **`operationsParameters`** 열은 업데이트, 삭제 및 병합에 사용되는 조건자를 검토할 수 있도록 합니다. **operationMetrics`** 열은 각 작업에서 추가된 행과 파일 수를 나타냅니다.
-- MAGIC
-- MAGIC Delta Lake 기록을 검토하여 특정 트랜잭션과 일치하는 테이블 버전을 파악하십시오.
-- MAGIC
-- MAGIC **참고**: **`version`** 열은 특정 트랜잭션이 완료된 후 테이블의 상태를 나타냅니다. **`readVersion`** 열은 작업이 실행된 테이블의 버전을 나타냅니다. 이 간단한 데모(동시 트랜잭션 없음)에서는 이 관계가 항상 1씩 증가합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-00d8e251-9c9e-4be3-b8e7-6e38b07fac55
-- MAGIC %md
-- MAGIC
-- MAGIC ## 특정 버전 쿼리
-- MAGIC
-- MAGIC 테이블 기록을 검토한 후, 첫 번째 데이터가 삽입된 이후의 테이블 상태를 확인하고 싶다고 생각합니다.
-- MAGIC
-- MAGIC 아래 쿼리를 실행하여 확인하세요.

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 4

-- COMMAND ----------

-- DBTITLE 0,--i18n-90e3c115-6bed-4b83-bb37-dd45fb92aec5
-- MAGIC %md
-- MAGIC
-- MAGIC 이제 데이터의 현재 상태를 검토해 보세요.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-f073a6d9-3aca-41a0-9452-a278fb87fa8c
-- MAGIC %md
-- MAGIC
-- MAGIC 레코드를 삭제하기 전에 빈의 무게를 검토하려고 합니다.
-- MAGIC
-- MAGIC 아래 문장을 입력하여 데이터가 삭제되기 직전 버전의 임시 뷰를 등록한 후, 다음 셀을 실행하여 뷰를 쿼리합니다.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
SELECT * FROM beans VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-bad13c31-d91f-454e-a14e-888d255dc8a4
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀을 실행하여 올바른 버전을 캡처했는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- DBTITLE 0,--i18n-8450d1ef-c49b-4c67-9390-3e0550c9efbc
-- MAGIC %md
-- MAGIC
-- MAGIC ## 이전 버전 복원
-- MAGIC
-- MAGIC 오해가 있었던 것 같습니다. 친구가 준 빈을 컬렉션에 병합했는데, 해당 빈은 보관할 의도가 아니었습니다.
-- MAGIC
-- MAGIC 이 **`MERGE`** 명령문이 완료되기 전 버전으로 테이블을 되돌리세요.

-- COMMAND ----------

-- TODO
restore table beans to version as of 5;

-- COMMAND ----------

-- DBTITLE 0,--i18n-405edc91-49e8-412b-99e7-96cc60aab32d
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 기록을 검토하세요. 이전 버전으로 복원하면 다른 테이블 버전이 추가된다는 점에 유의하세요.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- DBTITLE 0,--i18n-d430fe1c-32f1-44c0-907a-62ef8a5ca07b
-- MAGIC %md
-- MAGIC
-- MAGIC ## 파일 압축
-- MAGIC 리버전 작업 중 트랜잭션 메트릭을 살펴보면, 이렇게 작은 데이터 집합에 이렇게 많은 파일이 있다는 사실에 놀랐습니다.
-- MAGIC
-- MAGIC 이 크기의 테이블에 인덱싱을 적용해도 성능이 향상될 가능성은 낮지만, 시간이 지남에 따라 Bean 컬렉션이 기하급수적으로 증가할 것으로 예상하여 **`name`** 필드에 Z-order 인덱스를 추가하기로 했습니다.
-- MAGIC
-- MAGIC 아래 셀을 사용하여 파일 압축 및 Z-order 인덱싱을 수행합니다.

-- COMMAND ----------

-- TODO
OPTIMIZE beans
ZORDER BY name

-- COMMAND ----------

optimize beans
zorder by name

-- COMMAND ----------

-- DBTITLE 0,--i18n-8ef4ffb6-c958-4798-b564-fd2e65d4fa0e
-- MAGIC %md
-- MAGIC
-- MAGIC 데이터가 단일 파일로 압축되어야 합니다. 다음 셀을 실행하여 수동으로 확인하세요.

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-8a63081c-1423-43f2-9608-fe846a4a58bb
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀을 실행하여 테이블이 성공적으로 최적화되고 인덱싱되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6432b28c-18c1-4402-864c-ea40abca50e1
-- MAGIC %md
-- MAGIC
-- MAGIC ## 오래된 데이터 파일 정리
-- MAGIC
-- MAGIC 모든 데이터가 현재 하나의 데이터 파일에 저장되어 있지만, 이전 버전의 테이블 데이터 파일은 여전히 ​​이 파일과 함께 저장되어 있습니다. 테이블에서 **`VACUUM`**을 실행하여 이러한 파일을 제거하고 이전 버전의 테이블에 대한 액세스를 제거하려고 합니다.
-- MAGIC
-- MAGIC **`VACUUM`**을 실행하면 테이블 디렉터리에서 가비지 정리가 수행됩니다. 기본적으로 7일의 보존 임계값이 적용됩니다.
-- MAGIC
-- MAGIC 아래 셀은 일부 Spark 구성을 수정합니다. 첫 번째 명령은 보존 임계값 확인을 재정의하여 데이터를 영구적으로 제거하는 방법을 보여줍니다.
-- MAGIC
-- MAGIC **참고**: 보존 기간이 짧은 프로덕션 테이블을 vaccuming하면 데이터 손상 및/또는 장기 실행 쿼리 실패로 이어질 수 있습니다. 이는 데모용이며, 이 설정을 비활성화할 때는 각별히 주의해야 합니다.
-- MAGIC
-- MAGIC 두 번째 명령은 **`spark.databricks.delta.vacuum.logging.enabled`**를 **`true`**로 설정하여 **`VACUUM`** 작업이 트랜잭션 로그에 기록되도록 합니다.
-- MAGIC
-- MAGIC **참고**: 클라우드마다 스토리지 프로토콜이 약간씩 다르기 때문에 DBR 9.1부터 일부 클라우드에서는 **`VACUUM`** 명령 로깅이 기본적으로 활성화되어 있지 않습니다.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- DBTITLE 0,--i18n-04f27ab4-7848-4418-ac79-c339f9843b23
-- MAGIC %md
-- MAGIC
-- MAGIC 데이터 파일을 영구적으로 삭제하기 전에 **DRY RUN** 옵션을 사용하여 수동으로 검토하세요.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb9ce589-09ae-47b8-b6a6-4ab8e4dc70e7
-- MAGIC %md
-- MAGIC
-- MAGIC 현재 버전의 테이블에 없는 모든 데이터 파일은 위 미리보기에 표시됩니다.
-- MAGIC
-- MAGIC **`DRY RUN`** 없이 명령을 다시 실행하면 해당 파일이 영구적으로 삭제됩니다.
-- MAGIC
-- MAGIC **참고**: 이전 버전의 테이블에는 더 이상 액세스할 수 없습니다.

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-1630420a-94f5-43eb-b37c-ccbb46c9ba40
-- MAGIC %md
-- MAGIC
-- MAGIC 중요한 데이터 세트에 **`VACUUM`**은 매우 파괴적인 작업일 수 있으므로, 보존 기간 확인을 다시 활성화하는 것이 좋습니다. 이 설정을 다시 활성화하려면 아래 셀을 실행하세요.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- DBTITLE 0,--i18n-8d72840f-49f1-4983-92e4-73845aa98086
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블 기록에는 **VACUUM`** 작업을 완료한 사용자, 삭제된 파일 수, 그리고 이 작업 중에 보존 확인이 비활성화되었다는 로그가 표시됩니다.

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-875b39be-103c-4c70-8a2b-43eaa4a513ee
-- MAGIC %md
-- MAGIC
-- MAGIC 테이블을 다시 쿼리하여 현재 버전에 계속 액세스할 수 있는지 확인하세요.

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdb81194-2e3e-4a00-bfb6-97e822ae9ec3
-- MAGIC %md
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png"> 델타 캐시는 현재 세션에서 쿼리된 파일의 복사본을 현재 활성 클러스터에 배포된 스토리지 볼륨에 저장하므로 이전 테이블 버전에 일시적으로 액세스할 수 있습니다(단, 시스템은 이러한 동작을 예상하도록 설계되어서는 안 됩니다).
-- MAGIC
-- MAGIC 클러스터를 다시 시작하면 캐시된 데이터 파일이 영구적으로 삭제됩니다.
-- MAGIC
-- MAGIC 다음 셀의 주석 처리를 제거하고 실행하면 이러한 예를 확인할 수 있습니다. 이 셀은 캐시 상태에 따라 실패할 수도 있고 그렇지 않을 수도 있습니다.

-- COMMAND ----------

SELECT * FROM beans@v5

-- COMMAND ----------

-- DBTITLE 0,--i18n-a5cfd876-c53a-4d60-96e2-cdbc2b00c19f
-- MAGIC %md
-- MAGIC
-- MAGIC 이 실습을 완료하면 이제 다음 작업에 익숙해질 것입니다.
-- MAGIC * 표준 Delta Lake 테이블 생성 및 데이터 조작 명령 완료
-- MAGIC * 테이블 기록을 포함한 테이블 메타데이터 검토
-- MAGIC * 스냅샷 쿼리 및 롤백을 위해 Delta Lake 버전 관리 활용
-- MAGIC * 작은 파일 압축 및 테이블 인덱싱
-- MAGIC * **`VACUUM`**을 사용하여 삭제 표시된 파일 검토 및 삭제 커밋

-- COMMAND ----------

-- DBTITLE 0,--i18n-b541b92b-03a9-4f3c-b41c-fdb0ce4f2271
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()