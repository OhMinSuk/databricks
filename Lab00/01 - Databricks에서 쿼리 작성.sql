-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 01 - Databricks에서 query 작성
-- MAGIC
-- MAGIC 이 레슨에서는 카탈로그 탐색기를 사용하여 데이터를 검색하고 Databricks 노트북을 사용하여 해당 데이터를 탐색하고 조작하기 위한 query를 개발하는 방법을 배울 것입니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1부: 데이터 탐색
-- MAGIC 데이터 분석 작업을 수행하려면 먼저 적절한 데이터가 필요합니다. 
-- MAGIC 실습을 위한 데이터를 구성합니다. 
-- MAGIC
-- MAGIC - 1단계: 사이드바 탐색에서 **카탈로그** 를 선택합니다.
-- MAGIC - 2단계: 카탈로그 선택기에서 **생성한 datbricks** 이름으로 생성되어 있는 카탈로그를 선택합니다.
-- MAGIC - 3단계: **v01** 스키마를 생성 후 강사에게 제공받은 데이터로 다음 세 개의 테이블을 생성합니다.
-- MAGIC     - customers
-- MAGIC     - sales
-- MAGIC     - sales_orders
-- MAGIC
-- MAGIC
-- MAGIC 이제 이 수업을 위한 데이터가 준비되었습니다.
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2부: SQL 개발을 위한 노트북 사용
-- MAGIC
-- MAGIC **01 - Databricks에서 쿼리 작성** 노트북의 **2부** 로 이동합니다. 여기에서 1부에서 식별한 데이터를 쿼리하고 탐색하기 시작합니다.
-- MAGIC
-- MAGIC 노트북을 열면 실행 중인 컴퓨팅 클러스터에 연결되어 있는지 확인합니다. 이는 Serverless Databricks SQL 웨어하우스이어야 합니다.
-- MAGIC
-- MAGIC SQL 웨어하우스는 SQL 워크로드 최적화를 위해 특별히 설계되었습니다. Serverless는 Databricks에서 성능 대비 최고의 가격을 제공하지만 규제 요구 사항에 따라 비서버리스 컴퓨팅 옵션을 사용해야 하는 경우 Databricks는 Databricks SQL 웨어하우스의 Pro 버전을 제공합니다. 그러나 Pro Databricks SQL 웨어하우스는 기업용으로 준비되어 있으며 확장성 있는 성능을 최적화했지만 Databricks Serverless 컴퓨팅과 동일한 시작 속도 또는 확장성을 제공하지 않습니다.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이제 노트북에서 코드를 작성할 셀을 만들어야 합니다. 이 텍스트 바로 아래에 있는 **+Code** 버튼을 사용하여 새 셀을 생성합니다. 이 셀과 다음 셀 사이의 공간에 마우스를 올려야 **+Code** 버튼이 보일 수 있습니다.
-- MAGIC
-- MAGIC 다음 SQL 명령을 생성한 셀에 복사하여 붙여넣습니다.
-- MAGIC
-- MAGIC `SHOW TABLES IN xxxxx.v01;`

-- COMMAND ----------

show tables in databricks_1dt035_2.v01;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이제, 아래에 또 다른 코드 셀을 추가하고 다음 SQL 명령을 입력합니다.
-- MAGIC
-- MAGIC `SELECT * FROM xxxxx.v01.customers;`
-- MAGIC
-- MAGIC 노트북 설정에서 했던 것처럼 각 셀을 하나씩 실행하거나 노트북 상단의 "모두 실행" 옵션을 선택하여 노트북의 모든 셀을 순차적으로 실행할 수 있습니다.
-- MAGIC
-- MAGIC **3단계 네임스페이스:** 아마도 눈치채셨겠지만, 이 수업의 데이터 테이블을 참조하기 위해 3단계 네임스페이스 `catalog.schema.table`을 사용했습니다.

-- COMMAND ----------

select * from databricks_1dt035_2.v01.customers limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC **참고:** 이 노트북에서 새 셀은 기본적으로 SQL로 설정되어 있습니다(오른쪽 상단 모서리에 표시됨). 이는 노트북 자체가 창 상단의 노트북 이름 옆에 표시된 대로 SQL로 설정되어 있기 때문입니다. 이러한 토글을 사용하면 노트북 또는 셀의 코드 유형을 변경할 수 있습니다. 노트북 기본 설정 이외의 옵션(예: Python)을 선택하면 셀에 표시기(예: %python 또는 %md)가 추가되어 다른 유형의 코드로 실행됨을 나타냅니다. SQL 웨어하우스는 SQL 전용으로 최적화되어 있으며 다른 항목은 실행하지 않습니다. 따라서 SQL 웨어하우스에 연결된 노트북에는 오류가 발생하지 않도록 SQL 또는 마크다운 셀만 포함되어야 합니다.
-- MAGIC
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3부: 노트북에서 셀 포커스 모드 사용
-- MAGIC
-- MAGIC 이제 **포커스 모드**에 대해 설명하겠습니다. 포커스 모드를 사용하면 결과 패널이 화면의 절반을 차지하는 확장된 셀을 사용할 수 있습니다. 다음 셀의 오른쪽 모서리에 있는 **포커스 모드** 아이콘을 사용하여 셀을 포커스 모드로 열고 주석에 있는 세부 정보를 따르십시오.

-- COMMAND ----------

-- Focus 모드를 살펴보세요. 다음을 수행할 수 있습니다:
-- * 셀 간 이동
-- * 어시스턴트 전환
-- * 셀 주석 또는 버전 기록 검토
-- * 터미널 또는 출력 창 확장

USE CATALOG databricks_1dt035_2;
USE SCHEMA v01;
SELECT current_catalog(), current_database();

-- 현재 카탈로그와 스키마가 표시됩니다.
-- 완료되면 위쪽의 Focus 모드 종료 버튼을 선택하여 Focus 모드를 종료합니다.

-- COMMAND ----------

-- 이제 다음 코드를 실행하여 스키마의 고객 테이블을 확인합니다.
SELECT * FROM customers;

-- 이 코드는 이 노트북의 앞부분에서 설정한 기본 카탈로그 및 스키마를 사용하고 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 결과 패널에서 데이터가 테이블 형태로 표시됩니다.
-- MAGIC
-- MAGIC 간단한 시각화를 생성할 것입니다. 그러나 이 과정에서 나중에 시각화를 훨씬 더 자세히 다룰 예정입니다. 다음 단계를 완료하십시오:
-- MAGIC
-- MAGIC 1. query 편집기의 결과 섹션에서 **+** 버튼을 클릭하고 **시각화** 를 선택합니다.
-- MAGIC 2. **시각화 유형** 을 **막대** 로 유지합니다.
-- MAGIC 3. **X 열** 드롭다운을 사용하여 **state** 를 선택합니다.
-- MAGIC 4. **Y 열** 헤더 아래에서 **열 추가** 를 클릭하고 __*__ 를 선택합니다.
-- MAGIC 5. **Count** 를 기본적으로 선택된 상태로 유지합니다.
-- MAGIC 6. 오른쪽 하단 모서리에 있는 **저장** 을 클릭합니다.
-- MAGIC
-- MAGIC 시각화는 query 아래의 결과 옆에 추가됩니다. query 자체에서 그룹화 또는 집계를 수행할 필요가 없지만 시각화는 상태별로 그룹화된 개수를 표시합니다.
-- MAGIC
-- MAGIC 이제 차트 아래의 **시각화 편집** 옵션을 클릭하여 시각화를 편집하거나 제목을 클릭하여 시각화에 이름을 지정할 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---