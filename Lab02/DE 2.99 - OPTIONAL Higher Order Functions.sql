-- Databricks notebook source
-- DBTITLE 0,--i18n-a51f84ef-37b4-4341-a3cc-b85c491339a8
-- MAGIC %md
-- MAGIC
-- MAGIC # Spark SQL의 고차 함수
-- MAGIC
-- MAGIC Spark SQL의 고차 함수를 사용하면 배열이나 맵 타입 객체와 같은 복잡한 데이터 타입을 원래 구조를 유지하면서 변환할 수 있습니다. 다음은 그 예입니다.
-- MAGIC - **`FILTER()`**는 주어진 람다 함수를 사용하여 배열을 필터링합니다.
-- MAGIC - **`EXIST()`**는 배열의 하나 이상의 요소에 대해 특정 명령문이 참인지 여부를 테스트합니다.
-- MAGIC - **`TRANSFORM()`**은 주어진 람다 함수를 사용하여 배열의 모든 요소를 ​​변환합니다.
-- MAGIC - **`REDUCE()`**는 두 개의 람다 함수를 사용하여 배열의 요소를 버퍼에 병합하여 단일 값으로 축소하고, 최종 버퍼에 마무리 함수를 적용합니다.
-- MAGIC
-- MAGIC ## 학습 목표
-- MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
-- MAGIC * 고차 함수를 사용하여 배열 작업 수행

-- COMMAND ----------

-- DBTITLE 0,--i18n-b295e5de-82bb-41c0-a470-2d8c6bbacc09
-- MAGIC %md
-- MAGIC
-- MAGIC ## 설정 실행
-- MAGIC 다음 셀을 실행하여 환경을 설정하세요.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.99

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc1d8e11-d1ff-4aa0-b4e9-3c2703826cd1
-- MAGIC %md
-- MAGIC ## 필터
-- MAGIC **`FILTER`** 함수를 사용하면 제공된 조건에 따라 각 배열의 값을 제외하는 새 열을 만들 수 있습니다.
-- MAGIC 이 함수를 사용하여 **sales`** 데이터세트의 모든 레코드에서 **items`** 열에 있는 제품 중 킹 사이즈가 아닌 제품을 제거해 보겠습니다.
-- MAGIC
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC
-- MAGIC 위 명령문에서:
-- MAGIC - **`FILTER`**: 고차 함수의 이름 <br>
-- MAGIC - **`items`**: 입력 배열의 이름 <br>
-- MAGIC - **`i`**: 반복자 변수의 이름. 이 이름을 선택한 다음 람다 함수에서 사용합니다. 배열을 반복하며 각 값을 한 번에 하나씩 함수로 순환합니다.<br>
-- MAGIC - **`->`** : 함수의 시작을 나타냅니다. <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : 이것이 함수입니다. 각 값은 대문자 K로 끝나는지 확인합니다. 대문자 K로 끝나면 새 열인 **`king_items`**로 필터링됩니다.
-- MAGIC
-- MAGIC **참고:** 생성된 열에 빈 배열이 많이 생성되는 필터를 작성할 수 있습니다. 이 경우, 반환된 열에 비어 있지 않은 배열 값만 표시하기 위해 **`WHERE`** 절을 사용하는 것이 유용할 수 있습니다.

-- COMMAND ----------

SELECT * FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0

-- COMMAND ----------

-- DBTITLE 0,--i18n-3e2f5be3-1f8b-4a54-9556-dd72c3699a21
-- MAGIC %md
-- MAGIC
-- MAGIC ## 변환
-- MAGIC
-- MAGIC **`TRANSFORM()`** 고차 함수는 기존 함수를 배열의 각 요소에 적용하려는 경우 특히 유용합니다.
-- MAGIC 이 함수를 적용하여 **`items`** 배열 열에 포함된 요소를 변환하여 **`item_revenues`**라는 새 배열 열을 만들어 보겠습니다.
-- MAGIC
-- MAGIC 아래 쿼리에서 **`items`**는 입력 배열의 이름이고, **`i`**는 반복자 변수의 이름입니다(이 이름을 선택한 후 람다 함수에서 사용합니다. 배열을 반복하면서 각 값을 한 번에 하나씩 함수로 순환합니다). **`->`**는 함수의 시작을 나타냅니다.

-- COMMAND ----------

SELECT *,
  TRANSFORM (
    items, i -> CAST(i.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-ccfac343-4884-497a-a759-fc14b1666d6b
-- MAGIC %md
-- MAGIC
-- MAGIC 위에서 지정한 람다 함수는 각 값의 **`item_revenue_in_usd`** 하위 필드를 가져와서 100을 곱하고, 정수로 변환한 후 결과를 새 배열 열인 **`item_revenues`**에 포함합니다.

-- COMMAND ----------

-- DBTITLE 0,--i18n-9a5d0a06-c033-4541-b06e-4661804bf3c5
-- MAGIC %md
-- MAGIC
-- MAGIC ## Exists Lab
-- MAGIC 여기서는 고차 함수 **`EXISTS`**를 **`sales`** 테이블의 데이터와 함께 사용하여 구매한 품목이 매트리스인지 베개인지를 나타내는 부울 열 **`mattress`**와 **`pillow`**를 생성합니다.
-- MAGIC
-- MAGIC 예를 들어, **`items`** 열의 **`item_name`**이 문자열 **`"Mattress"`**로 끝나면, **`mattress`**의 열 값은 **`true`**이고 **`pillow`**의 값은 **`false`**여야 합니다. 다음은 몇 가지 품목과 결과 값의 예입니다.
-- MAGIC
-- MAGIC | items | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC
-- MAGIC <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a> 함수에 대한 설명서를 참조하세요.
-- MAGIC 조건식 **`item_name LIKE "%Mattress"`**를 사용하면 문자열 **`item_name`**이 "Mattress"라는 단어로 끝나는지 확인할 수 있습니다.

-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE sales_product_flags AS
<FILL_IN>
EXISTS <FILL_IN>.item_name LIKE "%Mattress"
EXISTS <FILL_IN>.item_name LIKE "%Pillow"



-- COMMAND ----------

-- TODO
CREATE OR REPLACE TABLE sales_product_flags AS
select items,
EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
from sales

-- COMMAND ----------

select * from sales_product_flags

-- COMMAND ----------

-- DBTITLE 0,--i18n-3dbc22b0-1092-40c9-a6cb-76ed364a4aae
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 도우미 함수는 지침을 따르지 않은 경우 변경해야 할 사항에 대한 메시지와 함께 오류를 반환합니다. 출력이 없으면 이 단계를 완료한 것입니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-caed8962-3717-4931-8ed2-910caf97740a
-- MAGIC %md
-- MAGIC
-- MAGIC 아래 셀을 실행하여 테이블이 올바르게 생성되었는지 확인하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", 10510, ['items', 'mattress', 'pillow'])
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 9986, 'num_pillow': 1384}, "There should be 9986 rows where mattress is true, and 1384 where pillow is true"

-- COMMAND ----------

-- DBTITLE 0,--i18n-ffcde68f-163a-4a25-85d1-c5027c664985
-- MAGIC %md
-- MAGIC
-- MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()