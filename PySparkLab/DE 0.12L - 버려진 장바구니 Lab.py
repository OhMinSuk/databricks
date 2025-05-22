# Databricks notebook source
# DBTITLE 0,--i18n-c52349f9-afd2-4532-804b-2d50a67839fa
# MAGIC %md
# MAGIC # 버려진 장바구니 랩
# MAGIC 구매하지 않고 버려진 장바구니 항목을 이메일로 받아보세요.
# MAGIC 1. 거래에서 전환된 사용자의 이메일 가져오기
# MAGIC 2. 사용자 ID로 이메일 병합
# MAGIC 3. 각 사용자의 장바구니 항목 내역 가져오기
# MAGIC 4. 이메일로 장바구니 항목 내역 병합
# MAGIC 5. 장바구니에서 버려진 항목이 있는 이메일 필터링
# MAGIC
# MAGIC ##### 방법
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">내장 함수</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# DBTITLE 0,--i18n-f1f59329-e456-4268-836a-898d3736f378
# MAGIC %md
# MAGIC ### 설정
# MAGIC 아래 셀을 실행하여 **`sales_df`**, **`users_df`**, **`events_df`** 데이터 프레임을 생성하세요.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.12L

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.table("users")
display(users_df)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c2065783-4c56-4d12-bdf8-2e0fce22016f
# MAGIC %md
# MAGIC
# MAGIC ### 1: 거래에서 전환된 사용자의 이메일 가져오기
# MAGIC - **`sales_df`**에서 **`email`** 열을 선택하고 중복 항목을 제거합니다.
# MAGIC - 모든 행에 **`True`** 값을 가진 새 **`converted`** 열을 추가합니다.
# MAGIC
# MAGIC 결과를 **`converted_users_df`**로 저장합니다.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import lit

sales_df = sales_df.select('email').distinct()
converted_users_df = sales_df.withColumn("converted", lit(True))

display(converted_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4becd415-94d5-4e58-a995-d17ef1be87d0
# MAGIC %md
# MAGIC
# MAGIC #### 1.1: 작업 확인
# MAGIC
# MAGIC 다음 셀을 실행하여 솔루션이 제대로 작동하는지 확인하세요.

# COMMAND ----------

expected_columns = ["email", "converted"]

expected_count = 10510

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-72c8bd3a-58ae-4c30-8e3e-007d9608aea3
# MAGIC %md
# MAGIC ### 2: 사용자 ID로 이메일을 조인합니다.
# MAGIC - **`converted_users_df`**와 **`users_df`**에서 **`email`** 필드를 사용하여 외부 조인을 수행합니다.
# MAGIC - **`email`**이 null이 아닌 사용자를 필터링합니다.
# MAGIC - **`converted`**의 null 값을 **`False`**로 채웁니다.
# MAGIC
# MAGIC 결과를 **`conversions_df`**로 저장합니다.

# COMMAND ----------

# TODO
conversions_df = (users_df
                  .join(converted_users_df, on='email',how='outer')
                  .filter(col('email').isNotNull())
                  .na.fill({'converted': False})
                 )
display(conversions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-48691094-e17f-405d-8f91-286a42ce55d7
# MAGIC %md
# MAGIC
# MAGIC #### 2.1: 작업 확인
# MAGIC
# MAGIC 다음 셀을 실행하여 솔루션이 제대로 작동하는지 확인하세요.

# COMMAND ----------

expected_columns = ['email', 'user_id', 'user_first_touch_timestamp', 'updated', 'converted']

expected_count = 38939

expected_false_count = 28429

assert conversions_df.columns == expected_columns, "Columns are not correct"

assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversions_df.count() == expected_count, "There is an incorrect number of rows"

assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-8a92bfe3-cad0-40af-a283-3241b810fc20
# MAGIC %md
# MAGIC ### 3: 각 사용자의 장바구니 항목 내역 가져오기
# MAGIC - 기존 **`items`** 필드를 결과로 대체하여 **`events_df`**의 **`items`** 필드를 분해합니다.
# MAGIC - **`user_id`**로 그룹화합니다.
# MAGIC - 각 사용자에 대한 모든 **`items.item_id`** 객체 집합을 수집하고 해당 열의 별칭을 "cart"로 지정합니다.
# MAGIC
# MAGIC 결과를 **`carts_df`**로 저장합니다.

# COMMAND ----------

# TODO
carts_df = (events_df
            .withColumn("items", explode(col("items")))
            .groupBy("user_id")
            .agg(collect_set("items.item_id").alias('cart'))
)
display(carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d0ed8434-7016-44c0-a865-4b55a5001194
# MAGIC %md
# MAGIC
# MAGIC #### 3.1: 작업 확인
# MAGIC
# MAGIC 다음 셀을 실행하여 솔루션이 제대로 작동하는지 확인하세요.

# COMMAND ----------

expected_columns = ["user_id", "cart"]

expected_count = 24574

assert carts_df.columns == expected_columns, "Incorrect columns"

assert carts_df.count() == expected_count, "Incorrect number of rows"

assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-97b01bbb-ada0-4c4f-a253-7c578edadaf9
# MAGIC %md
# MAGIC ### 4: 장바구니 항목 내역을 이메일과 연결
# MAGIC - **`user_id`** 필드에서 **`conversions_df`**와 **`carts_df`**에 대한 left join을 수행합니다.
# MAGIC
# MAGIC 결과를 **`email_carts_df`**로 저장합니다.

# COMMAND ----------

# TODO
email_carts_df = conversions_df.join(carts_df,on='user_id',how='left')
display(email_carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0cf80000-eb4c-4f0f-a3f8-7e938b99f1ef
# MAGIC %md
# MAGIC
# MAGIC #### 4.1: 작업 확인
# MAGIC
# MAGIC 다음 셀을 실행하여 솔루션이 제대로 작동하는지 확인하세요.

# COMMAND ----------

email_carts_df.filter(col("cart").isNull()).count()

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 38939

expected_cart_null_count = 19671

assert email_carts_df.columns == expected_columns, "Columns do not match"

assert email_carts_df.count() == expected_count, "Counts do not match"

assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-380e3eda-3543-4f67-ae11-b883e5201dba
# MAGIC %md
# MAGIC ### 5: 장바구니에 버려진 상품이 있는 이메일 필터링
# MAGIC - **`converted`가 False인 사용자에 대해 **`email_carts_df`** 필터링
# MAGIC - null이 아닌 장바구니가 있는 사용자 필터링
# MAGIC
# MAGIC 결과를 **`abandoned_carts_df`**로 저장합니다.

# COMMAND ----------

# TODO
abandoned_carts_df = (email_carts_df
                      .filter(col("converted") == False)
                      .filter(col("cart").isNotNull())
)
display(abandoned_carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05ff2599-c1e6-404f-a38b-262fb0a055fa
# MAGIC %md
# MAGIC
# MAGIC #### 5.1: 작업 확인
# MAGIC
# MAGIC 다음 셀을 실행하여 솔루션이 제대로 작동하는지 확인하세요.

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 10212

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"

assert abandoned_carts_df.count() == expected_count, "Counts do not match"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-e2f48480-5c42-490a-9f14-9b92d29a9823
# MAGIC %md
# MAGIC ### 6: 보너스 활동
# MAGIC 제품별 장바구니 포기 항목 수를 표시합니다.

# COMMAND ----------

# TODO
abandoned_items_df = (abandoned_carts_df
                      .select(explode(col("cart")).alias("items"))
                      .groupBy("items")
                      .agg(count("items").alias("count"))
                     )
display(abandoned_items_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a08b8c26-6a94-4a20-a096-e74721824eac
# MAGIC %md
# MAGIC
# MAGIC #### 6.1: 작업 확인
# MAGIC
# MAGIC 다음 셀을 실행하여 솔루션이 제대로 작동하는지 확인하세요.

# COMMAND ----------

expected_columns = ["items", "count"]

expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"

assert abandoned_items_df.columns == expected_columns, "Columns do not match"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f2f44609-5139-465a-ad7d-d87f3f06a380
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()