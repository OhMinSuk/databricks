# Databricks notebook source
# MAGIC %md
# MAGIC # 연습 #3 - 팩트 테이블과 디멘션 테이블 만들기
# MAGIC
# MAGIC 이제 3년간의 주문이 단일 데이터세트로 통합되었으므로 데이터 변환 프로세스를 시작할 수 있습니다.
# MAGIC
# MAGIC 한 레코드에는 실제로 네 개의 하위 데이터세트가 있습니다.
# MAGIC * 다른 세 데이터세트를 집계하는 주문 자체.
# MAGIC * 각 주문의 개별 항목. 각 특정 품목의 가격과 수량을 포함합니다.
# MAGIC * 주문을 하는 영업 담당자.
# MAGIC * 주문을 하는 고객 - 설명을 간소화하기 위해 이 데이터세트를 분리하여 주문의 일부로 두지 **않겠습니다**.
# MAGIC
# MAGIC 다음으로 할 일은 모든 데이터(고객 데이터 제외)를 해당 데이터세트로 추출하는 것입니다.
# MAGIC
# MAGIC 즉, 이 경우 데이터 중복을 줄이기 위해 데이터를 정규화합니다.
# MAGIC
# MAGIC 이 연습은 5단계로 나뉩니다.
# MAGIC * 연습 3.A - 데이터베이스 생성 및 사용
# MAGIC * 연습 3.B - 배치 주문 로드 및 캐시
# MAGIC * 연습 3.C - 영업 담당자 추출
# MAGIC * 연습 3.D - 주문 추출
# MAGIC * 연습 3.E - 개별 항목 추출

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 3번 설정</h2>
# MAGIC
# MAGIC 시작하려면 다음 셀을 실행하여 이 연습을 설정하고, 연습별 변수와 함수를 선언하세요.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-03

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #3.A - 데이터베이스 생성 및 사용</h2>
# MAGIC
# MAGIC 특정 데이터베이스를 사용하면 작업 공간의 다른 사용자가 사용 중일 수 있는 공통으로 명명된 테이블과의 충돌을 방지할 수 있습니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * **`user_db`** 변수로 식별되는 데이터베이스를 생성합니다.
# MAGIC * 이 노트북에서 생성된 테이블이 **`default`** 데이터베이스에 **추가되지** 않도록 **`user_db`** 변수로 식별되는 데이터베이스를 사용합니다.
# MAGIC
# MAGIC **특별 참고 사항**
# MAGIC * 데이터베이스 이름을 하드코딩하지 마십시오. 경우에 따라 유효성 검사 오류가 발생할 수 있습니다.
# MAGIC * 데이터베이스 생성을 위한 SQL 명령에 대한 도움말은 Databricks 문서 웹사이트의 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-database.html" target="_blank">CREATE DATABASE</a>를 참조하세요.
# MAGIC * 데이터베이스 사용에 대한 SQL 명령에 대한 도움말은 Databricks 문서 웹사이트의 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-usedb.html" target="_blank">USE DATABASE</a>를 참조하세요.

# COMMAND ----------

# MAGIC %md ### 연습문제 #3.A를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
spark.sql(f"create database if not exists {user_db}")

spark.sql(f"use {user_db}")
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.A
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_03_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #3.B - 배치 오더 로드 및 캐시</h2>
# MAGIC
# MAGIC 다음으로, 이전 연습의 배치 오더를 로드한 후 이 연습의 후반부에서 데이터를 변환할 준비를 위해 캐시해야 합니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 이전 연습에서 생성한 델타 데이터세트를 로드합니다. 이 데이터세트는 **`batch_source_path`** 변수로 식별됩니다.
# MAGIC * 동일한 데이터세트를 사용하여 **`batch_temp_view`** 변수로 식별되는 임시 뷰를 생성합니다.
# MAGIC * 임시 뷰를 캐시합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #3.B를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
df_batch = spark.read.format("delta").load(batch_source_path)

df_batch.createOrReplaceTempView(batch_temp_view)

spark.sql(f"cache table {batch_temp_view}")
# Use this cell to complete your solution

# COMMAND ----------

# 1. 델타 데이터셋 로드
df_batch = spark.read.format("delta").load(batch_source_path)

# 2. 임시 뷰 생성 (batch_temp_view는 문자열 변수로 주어져야 함)
df_batch.createOrReplaceTempView(batch_temp_view)

# 3. 캐시 (임시 뷰 이름으로 SQL API 사용)
spark.sql(f"CACHE TABLE {batch_temp_view}")


# COMMAND ----------

# MAGIC %md ### Reality Check #3.B
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_03_b()

# COMMAND ----------

# MAGIC
# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #3.C - 영업 담당자 추출</h2>
# MAGIC
# MAGIC 연습 #2에서 일괄 처리된 주문에는 수천 개의 주문이 포함되어 있으며, 각 주문에는 주문을 담당하는 영업 담당자의 이름, 사회보장번호, 주소 및 기타 정보가 포함되어 있습니다.
# MAGIC
# MAGIC 이 데이터를 사용하여 영업 담당자만 포함된 표를 만들 수 있습니다.
# MAGIC
# MAGIC 영업 담당자는 약 100명 정도이지만 주문은 수천 개라는 점을 고려하면 이 공간에 중복 데이터가 많이 발생할 것입니다.
# MAGIC
# MAGIC 이 데이터 세트의 또 다른 특징은 사회보장번호가 항상 삭제되지 않았다는 것입니다. 즉, 경우에 따라 하이픈으로 표시되기도 하고 그렇지 않기도 했습니다. 이 부분은 여기서 해결해야 할 문제입니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * **`batched_orders`** 테이블(**`batch_temp_view`** 변수로 식별됨)을 로드합니다.
# MAGIC * SSN 번호에 추적하려는 오류가 있습니다. **`boolean`** 열 **`_error_ssn_format`**을 추가합니다. **`sales_rep_ssn`**에 하이픈이 있는 경우 이 값을 **`true`**로 설정하고, 그렇지 않으면 **`false`**로 설정합니다.
# MAGIC * 다양한 열을 문자열 표현에서 지정된 유형으로 변환합니다.
# MAGIC * **`sales_rep_ssn`** 열은 **`Long`**으로 표현해야 합니다. (참고: 먼저 일부 레코드에서 불필요한 하이픈을 제거하여 열을 정리해야 합니다.)
# MAGIC * **`sales_rep_zip`** 열은 **`Integer`**로 표현해야 합니다.
# MAGIC * sales-rep 레코드와 직접 관련 없는 열을 제거합니다.
# MAGIC * 관련 없는 ID 열: **`submitted_at`**, **`order_id`**, **`customer_id`**
# MAGIC * 배송 주소 열: **`shipping_address_attention`**, **`shipping_address_address`**, **`shipping_address_city`**, **`shipping_address_state`**, **`shipping_address_zip`**
# MAGIC * 제품 열: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * 주문한 제품당 하나의 레코드(주문당 여러 제품)가 있고, 한 명의 영업 담당자가 여러 주문을 하기 때문에(영업 담당자당 여러 주문) 영업 담당자에 대한 중복 레코드가 발생합니다. 모든 중복 레코드를 제거하고, **`ingest_file_name`** 및 **`ingested_at`**을 중복 레코드 평가에서 제외해야 합니다.
# MAGIC * 데이터 세트를 관리형 델타 테이블 **`sales_rep_scd`**(변수 **`sales_reps_table`**로 식별됨)에 로드합니다.
# MAGIC
# MAGIC **추가 요구 사항:**<br/>
# MAGIC **`sales_rep_scd`** 테이블의 스키마는 다음과 같아야 합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #3.C를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

from pyspark.sql.functions import col, instr, regexp_replace
from pyspark.sql.types import LongType, IntegerType
# TODO
df_orders = spark.table(batch_temp_view)

df_orders = (df_orders.
             withColumn("_error_ssn_format",instr(col("sales_rep_ssn"),"-")>0)
             .withColumn("sales_rep_ssn",regexp_replace(col("sales_rep_ssn"),"-","").cast(LongType()))
             .withColumn("sales_rep_zip",col("sales_rep_zip").cast(IntegerType()))
             )

df_orders = df_orders.drop("submitted_at", "order_id", "customer_id","shipping_address_attention",
                            "shipping_address_address","shipping_address_city", "shipping_address_state",
                            "shipping_address_zip","product_id", "product_quantity", "product_sold_price")

dup_cols = ['sales_rep_id','sales_rep_ssn','sales_rep_first_name','sales_rep_last_name',
            'sales_rep_address','sales_rep_city','sales_rep_state','sales_rep_zip','_error_ssn_format']

df_orders = df_orders.dropDuplicates(dup_cols)

df_orders.write.format("delta").mode("overwrite").saveAsTable(sales_reps_table)

display(df_orders)
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### 현실 확인 #3.C
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_03_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #3.D - 주문 추출</h2>
# MAGIC
# MAGIC 연습 02의 일괄 주문에는 제품당 한 줄이 포함되어 있으므로 주문당 여러 개의 레코드가 있습니다.
# MAGIC
# MAGIC 이 단계의 목표는 영업 담당자와 라인 항목을 제외한 주문 세부 정보만 추출하는 것입니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * **`batched_orders`** 테이블(**`batch_temp_view`** 변수로 식별됨)을 로드합니다.
# MAGIC * 다양한 열을 문자열 표현에서 지정된 유형으로 변환합니다.
# MAGIC * **`submitted_at`** 열은 "unix epoch"(1970년 1월 1일 00:00:00 UTC 이후의 초 수)이며 **`Timestamp`**로 표현해야 합니다.
# MAGIC * **`shipping_address_zip`** 열은 **`Integer`**로 표현해야 합니다.
# MAGIC * 주문 레코드와 직접 관련되지 않은 열을 제거합니다.
# MAGIC * 영업 담당자 열: **`sales_rep_ssn`**, **`sales_rep_first_name`**, **`sales_rep_last_name`**, **`sales_rep_address`** **`sales_rep_city`**, **`sales_rep_state`**, **`sales_rep_zip`**
# MAGIC * 제품 열: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * 주문된 제품당 레코드가 하나(주문당 여러 제품)이므로 각 주문에 대해 중복 레코드가 발생합니다. 모든 중복 레코드를 제거하고, **`ingest_file_name`** 및 **`ingested_at`**을 중복 레코드 평가에서 제외해야 합니다.
# MAGIC * **`submitted_at`**에서 파생된 **`문자열`**이며 "**yyyy-MM**" 형식인 **`submitted_yyyy_mm`** 열을 추가합니다.
# MAGIC * 관리형 델타 테이블 **`orders`**(변수 **`orders_table`**로 식별됨)에 데이터 세트를 로드합니다.
# MAGIC * 이 경우, 데이터는 **`submitted_yyyy_mm`**으로 분할되어야 합니다.
# MAGIC
# MAGIC **추가 요구 사항:**
# MAGIC * **`orders`** 테이블의 스키마는 다음과 같아야 합니다.
# MAGIC   * **`submitted_at:timestamp`**
# MAGIC   * **`submitted_yyyy_mm`** using the format "**yyyy-MM**"
# MAGIC   * **`order_id:string`**
# MAGIC   * **`customer_id:string`**
# MAGIC   * **`sales_rep_id:string`**
# MAGIC   * **`shipping_address_attention:string`**
# MAGIC   * **`shipping_address_address:string`**
# MAGIC   * **`shipping_address_city:string`**
# MAGIC   * **`shipping_address_state:string`**
# MAGIC   * **`shipping_address_zip:integer`**
# MAGIC   * **`ingest_file_name:string`**
# MAGIC   * **`ingested_at:timestamp`**

# COMMAND ----------

# MAGIC %md ### 연습문제 #3.D를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, date_format, to_timestamp
# TODO
df_orders = spark.table(batch_temp_view)

df_orders = (df_orders
             .withColumn("submitted_at",to_timestamp(from_unixtime(col("submitted_at"))))
             .withColumn("submitted_yyyy_mm",date_format(col("submitted_at"),"yyyy-MM"))
             .withColumn("shipping_address_zip",col("shipping_address_zip").cast(IntegerType()))
             )

df_orders = df_orders.drop("sales_rep_ssn", "sales_rep_first_name", "sales_rep_last_name", 
                           "sales_rep_address", "sales_rep_city", "sales_rep_state", "sales_rep_zip",
                           "product_id", "product_quantity", "product_sold_price")

dedup_cols = [col for col in df_orders.columns if col not in ["ingest_file_name", "ingested_at"]]

df_orders = df_orders.dropDuplicates(dedup_cols)

df_orders.write.format("delta").mode("overwrite").partitionBy("submitted_yyyy_mm").saveAsTable(orders_table)

display(df_orders)
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.D
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_03_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #3.E - 라인 항목 추출</h2>
# MAGIC
# MAGIC 이제 영업 담당자와 주문을 추출했으니, 각 주문의 특정 라인 항목을 추출해 보겠습니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * **`batched_orders`** 테이블(**`batch_temp_view`** 변수로 식별)을 로드합니다.
# MAGIC * 다음 열을 유지합니다(아래 스키마 참조).
# MAGIC * 상관 ID 열: **`order_id`** 및 **`product_id`**
# MAGIC * 두 제품별 열: **`product_quantity`** 및 **`product_sold_price`**
# MAGIC * 두 수집 열: **`ingest_file_name`** 및 **`ingested_at`**
# MAGIC * 다양한 열을 문자열 표현에서 지정된 유형으로 변환합니다.
# MAGIC * **`product_quantity`** 열은 **`Integer`**로 표현해야 합니다.
# MAGIC * **`product_sold_price`** 열은 **`decimal(10,2)`**처럼 소수점 이하 두 자릿수를 사용하는 **`Decimal`**로 표현해야 합니다.
# MAGIC * 관리형 델타 테이블 **`line_items`**(변수로 식별)에 데이터 세트를 로드합니다. **`line_items_table`**)
# MAGIC
# MAGIC **추가 요구 사항:**
# MAGIC * **`line_items`** 테이블의 스키마는 다음과 같아야 합니다.
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`integer`**
# MAGIC   * **`product_sold_price`**:**`decimal(10,2)`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### 연습문제 #3.E를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
from pyspark.sql.types import DecimalType, IntegerType
df_orders = spark.table(batch_temp_view)

drop_cols = [col for col in df_orders.columns if col not in ["order_id","product_id","product_quantity",
                                                            "product_sold_price","ingest_file_name","ingested_at"]]
df_orders = df_orders.drop(*drop_cols)

df_orders = (df_orders
             .withColumn("product_quantity",col("product_quantity").cast(IntegerType()))
             .withColumn("product_sold_price",col("product_sold_price").cast(DecimalType(10,2)))
             )

df_orders.write.format("delta").mode("overwrite").saveAsTable(line_items_table)

display(df_orders)
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #3.E
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_03_e()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #3 - 최종 확인</h2>
# MAGIC
# MAGIC 이 연습이 완료되었는지 확인하려면 다음 명령을 실행하세요.

# COMMAND ----------

reality_check_03_final()