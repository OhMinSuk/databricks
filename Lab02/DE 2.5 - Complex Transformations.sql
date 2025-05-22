-- Databricks notebook source
-- DBTITLE 0,--i18n-daae326c-e59e-429b-b135-5662566b6c34
-- MAGIC %md
-- MAGIC
-- MAGIC # 복잡한 변환
-- MAGIC
-- MAGIC Spark SQL을 사용하면 데이터 레이크하우스에 저장된 테이블 형식 데이터를 쉽고 효율적이며 빠르게 쿼리할 수 있습니다.
-- MAGIC
-- MAGIC 데이터 구조가 불규칙해지거나, 단일 쿼리에 여러 테이블을 사용해야 하거나, 데이터 형태를 크게 변경해야 하는 경우 쿼리는 더욱 복잡해집니다. 이 노트북에서는 엔지니어가 가장 복잡한 변환 작업도 완료할 수 있도록 Spark SQL에 있는 여러 함수를 소개합니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC - **`.`** 및 **`:`** 구문을 사용하여 중첩된 데이터를 쿼리합니다.
-- MAGIC - JSON 문자열을 구조체로 구문 분석합니다.
-- MAGIC - 배열과 구조체를 평면화하고 압축 해제합니다.
-- MAGIC - 조인을 사용하여 데이터 세트를 결합합니다.
-- MAGIC - 피벗 테이블을 사용하여 데이터 형태를 변경합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-b01af8a2-da4a-4c8f-896e-790a60dc8d0c
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설치 실행
-- MAGIC
-- MAGIC 설치 스크립트는 이 노트북의 나머지 부분을 실행하는 데 필요한 데이터를 생성하고 값을 선언합니다.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.5

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be8b8a-1c1f-40dd-a71c-8e91ae079b5c
-- MAGIC %md
-- MAGIC
-- MAGIC ## 데이터 개요
-- MAGIC
-- MAGIC **`events_raw`** 테이블은 Kafka 페이로드를 나타내는 데이터에 등록되었습니다. 대부분의 경우 Kafka 데이터는 바이너리로 인코딩된 JSON 값입니다.
-- MAGIC
-- MAGIC **`key`**와 **`value`**를 문자열로 변환하여 사람이 읽을 수 있는 형식으로 값을 표시해 보겠습니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC events_stringsDF = (spark
-- MAGIC     .table("events_raw")
-- MAGIC     .select(col("key").cast("string"), 
-- MAGIC             col("value").cast("string"))
-- MAGIC     )
-- MAGIC display(events_stringsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-67712d1a-cae1-41dc-8f7b-cc97e933128e
-- MAGIC %md
-- MAGIC ## Manipulate Complex Types

-- COMMAND ----------

-- DBTITLE 0,--i18n-c6a0cd9e-3bdc-463a-879a-5551fa9a8449
-- MAGIC %md
-- MAGIC
-- MAGIC ### 중첩 데이터 작업
-- MAGIC
-- MAGIC 아래 코드 셀은 변환된 문자열을 쿼리하여 null 필드가 없는 JSON 객체 예를 보여줍니다(다음 섹션에서 이 코드가 필요합니다).
-- MAGIC
-- MAGIC **참고:** Spark SQL에는 JSON 문자열 또는 구조체 타입으로 저장된 중첩 데이터와 직접 상호 작용하는 기본 제공 기능이 있습니다.
-- MAGIC - JSON 문자열의 하위 필드에 액세스하려면 쿼리에서 **`:`** 구문을 사용하세요.
-- MAGIC - 구조체 타입의 하위 필드에 액세스하려면 쿼리에서 **`.`** 구문을 사용하세요.

-- COMMAND ----------

SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(events_stringsDF
-- MAGIC     .where("value:event_name = 'finalize'")
-- MAGIC     .orderBy("key")
-- MAGIC     .limit(1)
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-914b04cd-a1c1-4a91-aea3-ecd87714ea7d
-- MAGIC %md
-- MAGIC 위의 JSON 문자열 예제를 사용하여 스키마를 도출한 다음, 전체 JSON 열을 구조체 유형으로 구문 분석해 보겠습니다.
-- MAGIC - **`schema_of_json()`**은 예제 JSON 문자열에서 도출된 스키마를 반환합니다.
-- MAGIC - **`from_json()`**은 지정된 스키마를 사용하여 JSON 문자열이 포함된 열을 구조체 유형으로 구문 분석합니다.
-- MAGIC
-- MAGIC JSON 문자열을 구조체 유형으로 압축 해제한 후, 모든 구조체 필드를 열로 압축 해제하고 평면화해 보겠습니다.
-- MAGIC - **`*`** 압축 해제는 구조체를 평면화하는 데 사용할 수 있습니다. **`col_name.*`**은 **`col_name`**의 하위 필드를 자체 열로 추출합니다.

-- COMMAND ----------

SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}') AS schema

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_events AS SELECT json.* FROM (
SELECT from_json(value, 'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS json 
FROM events_strings);

SELECT * FROM parsed_events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json
-- MAGIC
-- MAGIC json_string = """
-- MAGIC {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
-- MAGIC """
-- MAGIC parsed_eventsDF = (events_stringsDF
-- MAGIC     .select(from_json("value", schema_of_json(json_string)).alias("json"))
-- MAGIC     .select("json.*")
-- MAGIC )
-- MAGIC
-- MAGIC display(parsed_eventsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ca54e9c-dcb7-4177-99ab-77377ce8d899
-- MAGIC %md
-- MAGIC ### 배열 조작
-- MAGIC
-- MAGIC Spark SQL에는 배열 데이터를 조작하는 여러 함수가 있으며, 여기에는 다음이 포함됩니다.
-- MAGIC - **`explode()`**는 배열의 요소를 여러 행으로 분리합니다. 각 요소에 대해 새 행을 생성합니다.
-- MAGIC - **`size()`**는 각 행에 대한 배열의 요소 개수를 계산합니다.
-- MAGIC
-- MAGIC 아래 코드는 **`items`** 필드(구조체 배열)를 여러 행으로 분리하고 3개 이상의 항목이 있는 배열을 포함하는 이벤트를 표시합니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC
-- MAGIC exploded_eventsDF = (parsed_eventsDF
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC display(exploded_eventsDF.where(size("items") > 2))

-- COMMAND ----------

DESCRIBE exploded_events

-- COMMAND ----------

-- DBTITLE 0,--i18n-0810444d-1ce9-4cb7-9ba9-f4596e84d895
-- MAGIC %md
-- MAGIC 아래 코드는 배열 변환을 결합하여 고유한 작업 컬렉션과 사용자 장바구니에 있는 항목을 보여주는 표를 생성합니다.
-- MAGIC - **`collect_set()`**은 배열 내의 필드를 포함하여 필드에 대한 고유한 값을 수집합니다.
-- MAGIC - **`flatten()`**은 여러 배열을 하나의 배열로 결합합니다.
-- MAGIC - **`array_distinct()`**는 배열에서 중복 요소를 제거합니다.

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM exploded_events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import array_distinct, collect_set, flatten
-- MAGIC
-- MAGIC display(exploded_eventsDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(collect_set("event_name").alias("event_history"),
-- MAGIC             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history"))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-8744b315-393b-4f8b-a8c1-3d6f9efa93b0
-- MAGIC %md
-- MAGIC
-- MAGIC ## 데이터 결합 및 재구성

-- COMMAND ----------

-- DBTITLE 0,--i18n-15407508-ba1c-4aef-bd40-1c8eb244ed83
-- MAGIC %md
-- MAGIC
-- MAGIC ### 테이블 조인
-- MAGIC
-- MAGIC Spark SQL은 표준 **`JOIN`** 연산(inner, outer, left, right, anti, cross, semi)을 지원합니다.
-- MAGIC 여기서는 분해된 이벤트 데이터셋을 조회 테이블과 조인하여 표준 인쇄 항목 이름을 가져옵니다.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW item_purchases AS

SELECT * 
FROM (SELECT *, explode(items) AS item FROM sales) a
INNER JOIN item_lookup b
ON a.item.item_id = b.item_id;

SELECT * FROM item_purchases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_salesDF = (spark
-- MAGIC     .table("sales")
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC itemsDF = spark.table("item_lookup")
-- MAGIC
-- MAGIC item_purchasesDF = (exploded_salesDF
-- MAGIC     .join(itemsDF, exploded_salesDF.item.item_id == itemsDF.item_id)
-- MAGIC )
-- MAGIC
-- MAGIC display(item_purchasesDF)

-- COMMAND ----------

-- MAGIC %md --i18n-6c1f0e6f-c4f0-4b86-bf02-783160ea00f7
-- MAGIC ### Pivot Tables
-- MAGIC
-- MAGIC **`PIVOT`**을 사용하면 지정된 피벗 열의 고유 값을 집계 함수에 따라 여러 열로 회전하여 데이터를 다양한 관점에서 볼 수 있습니다.
-- MAGIC - **`PIVOT`** 절은 **`FROM`** 절에 지정된 테이블 이름 또는 하위 쿼리 뒤에 오며, 이는 피벗 테이블의 입력값입니다.
-- MAGIC - 피벗 열의 고유 값은 제공된 집계 표현식을 사용하여 그룹화 및 집계되어 결과 피벗 테이블에 각 고유 값에 대한 별도의 열이 생성됩니다.
-- MAGIC
-- MAGIC 다음 코드 셀은 **`PIVOT`**을 사용하여 **`sales`** 데이터세트에서 파생된 여러 필드에 포함된 품목 구매 정보를 평면화합니다. 이렇게 평면화된 데이터 형식은 대시보드에 유용할 뿐만 아니라 추론이나 예측을 위한 머신 러닝 알고리즘 적용에도 유용합니다.

-- COMMAND ----------

SELECT *
FROM item_purchases
PIVOT (
  sum(item.quantity) FOR item_id IN (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactionsDF = (item_purchasesDF
-- MAGIC     .groupBy("order_id", 
-- MAGIC         "email",
-- MAGIC         "transaction_timestamp", 
-- MAGIC         "total_item_quantity", 
-- MAGIC         "purchase_revenue_in_usd", 
-- MAGIC         "unique_items",
-- MAGIC         "items",
-- MAGIC         "item",
-- MAGIC         "name",
-- MAGIC         "price")
-- MAGIC     .pivot("item_id")
-- MAGIC     .sum("item.quantity")
-- MAGIC )
-- MAGIC display(transactionsDF)

-- COMMAND ----------

-- DBTITLE 0,--i18n-b89c0c3e-2352-4a82-973d-7e655276bede
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()