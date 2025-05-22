-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 02 - Databricks Assistant 사용
-- MAGIC
-- MAGIC 이 레슨에서는 쿼리 개발 및 테스트를 지원하기 위해 Databricks Assistant를 사용하는 방법을 배웁니다.
-- MAGIC
-- MAGIC 이 레슨에서는 `xxxxx.v01`의 다음 리소스를 사용합니다.
-- MAGIC * **sales** 테이블
-- MAGIC * **customers** 테이블
-- MAGIC
-- MAGIC

-- COMMAND ----------

USE CATALOG databricks_1dt035_2; 
USE SCHEMA v01;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1부: 작성
-- MAGIC 다음 셀에서 쿼리를 작성하도록 Assistant를 사용합니다. 아래 셀을 선택하면 **생성** 옵션이 제공됩니다. 이를 선택하면 다음 프롬프트를 입력할 수 있습니다.
-- MAGIC
-- MAGIC `databricks_1dt035_2의 "sales" 테이블에서 "total_price"가 10000이 넘는제품에 대한 "high sales" 뷰를 만드는 SQL 쿼리를 작성해 주세요.`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ### 2부: 디버깅
-- MAGIC 다음 몇 개의 셀에는 실행되지 않는 잘못 작성된 쿼리가 포함되어 있습니다. 쿼리에 어떤 문제가 있는지 알아내고 적절하게 다시 작성하여 실행되도록 도와주는 Assistant를 사용할 수 있습니다. 먼저 셀을 실행해 보세요. "오류 진단"을 선택하여 문제 해결을 지원하는 Databricks Assistant를 호출하세요.

-- COMMAND ----------

SELECT * from databricks_1dt035_2.v01.sales where customer_id = 17372531

-- COMMAND ----------

SELECT * 
FROM databricks_1dt035_2.v01.sales 
WHERE total_price = 'total_price';

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 이들은 모두 코드를 간단히 검토하여 디버깅할 수 있는 간단한 예이지만, 올바르게 실행되지 않는 훨씬 더 복잡한 쿼리를 상상할 수 있습니다. Databricks Assistant는 무엇이 잘못되었는지 빠르게 파악하고 이를 수정하기 위한 솔루션을 제공하는 데 큰 도움이 될 수 있습니다.
-- MAGIC
-- MAGIC ---
-- MAGIC **추가 질문:** Databricks에서 데이터 분석 작업을 지원하기 위해 Databricks Assistant를 사용할 수 있는 다른 사용 사례는 무엇입니까?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>