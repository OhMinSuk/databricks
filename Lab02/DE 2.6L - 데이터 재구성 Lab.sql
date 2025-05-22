-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-af5bea55-ebfc-4d31-a91f-5d6cae2bc270
-- MAGIC %md
-- MAGIC
-- MAGIC # 데이터 재구성 랩
-- MAGIC
-- MAGIC 이 랩에서는 각 사용자가 **events`**에서 특정 액션을 수행한 횟수를 집계하는 **clickpaths`** 테이블을 생성하고, 이 정보를 **transactions`**의 평면화된 뷰와 조인하여 각 사용자의 액션과 최종 구매에 대한 레코드를 생성합니다.
-- MAGIC
-- MAGIC **clickpaths`** 테이블에는 **transactions`**의 모든 필드와 **events`**의 모든 **event_name`**의 개수가 별도의 열에 포함되어야 합니다. 이 테이블에는 구매를 완료한 각 사용자에 대한 단일 행이 포함되어야 합니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 랩을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - 테이블을 피벗 및 조인하여 각 사용자에 대한 클릭 경로를 생성합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-5258fa9b-065e-466d-9983-89f0be627186
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설치 실행
-- MAGIC
-- MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-082bfa19-8e4e-49f7-bd5d-cd833c471109
-- MAGIC %md
-- MAGIC
-- MAGIC 랩 전체에서 Python을 사용하여 간헐적으로 검사를 실행합니다. 아래 도우미 함수는 지침을 따르지 않은 경우 변경해야 할 사항에 대한 메시지와 함께 오류를 반환합니다. 출력이 없으면 이 단계를 완료한 것입니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-2799ea4f-4a8e-4ad4-8dc1-a8c1f807c6d7
-- MAGIC %md
-- MAGIC
-- MAGIC ## 각 사용자의 이벤트 수를 얻기 위한 피벗 이벤트
-- MAGIC
-- MAGIC 먼저 **`events`** 테이블을 피벗하여 각 **`event_name`**에 대한 수를 구해 보겠습니다.
-- MAGIC
-- MAGIC 각 사용자가 **`event_name`** 열에 지정된 특정 이벤트를 수행한 횟수를 집계하려고 합니다. 이를 위해 **`user_id`**로 그룹화하고 **`event_name`**을 피벗하여 각 이벤트 유형의 수를 각 열에 제공합니다. 그러면 아래 스키마가 생성됩니다. 대상 스키마에서 **`user_id`**는 **`user`**로 이름이 변경되었습니다.
-- MAGIC
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC
-- MAGIC 이벤트 이름 목록은 아래의 TODO 셀에 제공됩니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d995af9-e6f3-47b0-8b78-44bda953fa37
-- MAGIC %md
-- MAGIC
-- MAGIC ### SQL로 해결

-- COMMAND ----------

select * from events

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW events_pivot
<FILL_IN>
("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
"register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
"cc_info", "foam", "reviews", "original", "delivery", "premium")

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW events_pivot AS
SELECT *
FROM (
  SELECT user_id as user , event_name
  FROM events
)
PIVOT (
  COUNT(*) FOR event_name IN (
    'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 
    'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 
    'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'
  )
)

-- COMMAND ----------

select * from events_pivot

-- COMMAND ----------

-- DBTITLE 0,--i18n-afd696e4-049d-47e1-b266-60c7310b169a
-- MAGIC %md
-- MAGIC
-- MAGIC ### 파이썬으로 풀기

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC (spark.read.table("events")
-- MAGIC     .groupBy("user_id")
-- MAGIC     .pivot("event_name")
-- MAGIC     .count()
-- MAGIC     .withColumnRenamed("user_id", "user")
-- MAGIC     .createOrReplaceTempView("events_pivot"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-2fe0f24b-2364-40a3-9656-c124d6515c4d
-- MAGIC %md
-- MAGIC
-- MAGIC ### 작업 확인
-- MAGIC 아래 셀을 실행하여 뷰가 올바르게 생성되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("events_pivot", 204586, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-eaac4506-501a-436a-b2f3-3788c689e841
-- MAGIC %md
-- MAGIC
-- MAGIC ## 모든 사용자의 이벤트 수와 거래를 조인합니다.
-- MAGIC
-- MAGIC 다음으로, **`events_pivot`**과 **`transactions`**를 조인하여 **`clickpaths`** 테이블을 만듭니다. 이 테이블에는 위에서 만든 **`events_pivot`** 테이블과 동일한 이벤트 이름 열이 있어야 하며, 그 뒤에 **`transactions`** 테이블의 열이 있어야 합니다(아래 참조).
-- MAGIC
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

-- DBTITLE 0,--i18n-03571117-301e-4c35-849a-784621656a83
-- MAGIC %md
-- MAGIC
-- MAGIC ### SQL로 해결

-- COMMAND ----------

select * from transactions

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TEMP VIEW clickpaths AS
<FILL_IN>

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW clickpaths AS
select * 
FROM transactions 
join events_pivot
on transactions.user_id = events_pivot.user

-- COMMAND ----------

-- DBTITLE 0,--i18n-e0ad84c7-93f0-4448-9846-4b56ba71acf8
-- MAGIC %md
-- MAGIC
-- MAGIC ### 파이썬으로 풀기

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC from pyspark.sql.functions import col
-- MAGIC (spark.read.table("events_pivot")
-- MAGIC     .join(spark.table("transactions"),col("events_pivot.user") == col("transactions.user_id"), "inner")
-- MAGIC     .createOrReplaceTempView("clickpaths"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-ac19c8e1-0ab9-4558-a0eb-a6c954e84167
-- MAGIC %md
-- MAGIC
-- MAGIC ### 작업 확인
-- MAGIC 아래 셀을 실행하여 뷰가 올바르게 생성되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("clickpaths", 9085, ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium', 'user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K'])

-- COMMAND ----------

-- DBTITLE 0,--i18n-f352b51d-72ce-48d9-9944-a8f4c0a2a5ce
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()