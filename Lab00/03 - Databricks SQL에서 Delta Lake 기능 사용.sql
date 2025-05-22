-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 03 - Databricks SQL에서 Delta Lake 기능 사용
-- MAGIC
-- MAGIC 이 레슨에서는 Databricks SQL의 테이블 작업을 위해 Delta Lake가 제공하는 일부 기능을 살펴봅니다. 이를 위해 테이블을 생성하고 조작한 다음 로그에서 결과를 확인하는 과정을 진행합니다.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1부: 테이블 생성 및 데이터 추가
-- MAGIC 먼저, **delta_students**라는 이름의 테이블을 스키마에 생성합니다.

-- COMMAND ----------

USE CATALOG databricks_1dt035_2;
USE SCHEMA v01;

CREATE
OR REPLACE TABLE delta_students (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  name STRING,
  grade FLOAT,
  country STRING
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ID 필드는 **BIGINT** 유형이며 그 값은 단조 증가하는 숫자로 자동 생성됩니다. 이 값을 **INSERT** 하는 동안 전달하지 않으므로 테이블의 기본 키로 간주할 수 있습니다.
-- MAGIC
-- MAGIC 다음으로, 몇 가지 값을 삽입하고 데이터를 검토하겠습니다.

-- COMMAND ----------

INSERT INTO
  delta_students (name, grade, country)
VALUES
  ('Lucas', 4.0, "Italy"),
  ("Ana", 3.5, "Germany");

SELECT
  *
FROM
  delta_students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 그런 다음 테이블에 몇 개의 추가 레코드를 추가합니다.

-- COMMAND ----------

INSERT INTO
  delta_students (name, grade, country)
VALUES
  ('Mary', 2.0, "Finland"),
  ("John", 3.5, "USA"),
  ("Judith", 4.0, "Singapore");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 다음으로, 데이터에 몇 가지 업데이트를 수행하겠습니다. 한 학생의 국가를 변경하고 다른 학생의 등급을 편집합니다. 마지막으로, 현재 데이터 세트를 지금 상태로 확인하겠습니다.

-- COMMAND ----------

UPDATE
  delta_students
SET
  country = "France"
WHERE
  name = 'Lucas';

UPDATE
  delta_students
SET
  grade = 2.5
WHERE
  id = 3;

SELECT
  *
FROM
  delta_students
ORDER BY
  id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2부: DESCRIBE HISTORY 및 VERSION AS OF
-- MAGIC **DESCRIBE HISTORY** 및 **VERSION AS OF** 를 사용하여 테이블의 메타데이터를 자세히 살펴보겠습니다.
-- MAGIC
-- MAGIC 먼저 `delta_students` 테이블의 기록을 검토하기 위해 **DESCRIBE HISTORY** 를 사용합니다.

-- COMMAND ----------

DESCRIBE HISTORY delta_students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 기록에서 볼 수 있듯이, Delta에 의해 테이블에서 자동으로 실행된 최적화와 함께 우리가 만든 모든 변경 사항과 삽입이 기록됩니다. **VERSION AS OF** 를 사용하여 기록 테이블에서 주어진 버전의 데이터가 어떻게 보였는지 볼 수 있습니다.
-- MAGIC
-- MAGIC 이제 첫 번째 두 레코드를 삽입한 **버전 1**의 테이블을 살펴보겠습니다. 

-- COMMAND ----------

SELECT * FROM delta_students VERSION AS OF 1 ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 그런 다음 **버전 2** 를 살펴보십시오.

-- COMMAND ----------

SELECT * FROM delta_students VERSION AS OF 4 ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 하지만 ID가 1인 학생의 위치를 살펴보면, 업데이트 중 하나에서 프랑스로 이동했음에도 불구하고 여전히 이탈리아에 있습니다. 다음 명령은 현재 테이블을 보여줍니다.

-- COMMAND ----------

SELECT * FROM delta_students WHERE id = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3단계: 테이블 복원
-- MAGIC
-- MAGIC 테이블에 무슨 일이 발생했다고 가정해 보겠습니다. 아마도 동료가 실수를 해서 테이블의 모든 레코드를 삭제했을 수도 있습니다.

-- COMMAND ----------

DELETE FROM delta_students;

-- COMMAND ----------

SELECT * FROM delta_students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이제 테이블에 의존하는 하위 라인 보고서가 실행되기 전에 문제를 해결해야 합니다. 따라서 **DESCRIBE HISTORY** 를 사용하여 **DELETE** 가 발생하기 전의 버전을 찾습니다.

-- COMMAND ----------

DESCRIBE HISTORY delta_students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 버전 목록에 따르면 마지막 업데이트는 버전 5이므로 **RESTORE TABLE** 명령을 사용하여 테이블을 해당 버전으로 복원합니다.

-- COMMAND ----------

RESTORE TABLE delta_students VERSION AS OF 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이제 테이블을 확인하면, 동료가 실수로 삭제하기 전 상태로 돌아갔습니다.

-- COMMAND ----------

SELECT * FROM delta_students ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC #### 결론
-- MAGIC
-- MAGIC 보시다시피, Delta는 테이블 작업에 여러 가지 이점을 제공하며, 문제가 발생할 경우 과거 상태로 돌아가 테이블을 복원할 수 있는 기능도 있습니다.