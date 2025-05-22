# Databricks notebook source
# MAGIC %md 
# MAGIC # 연습 #5 - 주문 스트리밍
# MAGIC
# MAGIC 네 개의 과거 데이터세트가 제대로 로드되었으므로 이제 "현재" 주문 처리를 시작할 수 있습니다.
# MAGIC
# MAGIC 이 경우, 새로운 "시스템"은 주문당 하나의 JSON 파일을 클라우드 저장소에 저장합니다.
# MAGIC
# MAGIC 새로운 주문이 이 데이터세트에 지속적으로 추가된다는 가정 하에 이러한 JSON 파일을 주문 스트림으로 처리할 수 있습니다.
# MAGIC
# MAGIC 이 프로젝트를 단순화하기 위해 주문 "스트림"을 2020년 첫 몇 시간으로 줄였으며, 반복 작업당 하나의 파일로만 스트림을 제한할 것입니다.
# MAGIC
# MAGIC 이 연습은 3단계로 나뉩니다.
# MAGIC * 연습 5.A - 데이터베이스 사용
# MAGIC * 연습 5.B - 주문 스트림 추가
# MAGIC * 연습 5.C - 개별 항목 스트림 추가
# MAGIC
# MAGIC ## 친절한 조언...
# MAGIC
# MAGIC 각 레코드는 대략 다음과 같은 구조를 가진 JSON 객체입니다.
# MAGIC
# MAGIC * **`customerID`**
# MAGIC * **`orderId`**
# MAGIC * **`products`**
# MAGIC   * array
# MAGIC     * **`productId`**
# MAGIC     * **`quantity`**
# MAGIC     * **`soldPrice`**
# MAGIC * **`salesRepId`**
# MAGIC * **`shippingAddress`**
# MAGIC   * **`address`**
# MAGIC   * **`attention`**
# MAGIC   * **`city`**
# MAGIC   * **`state`**
# MAGIC   * **`zip`**
# MAGIC * **`submittedAt`**
# MAGIC
# MAGIC 이 데이터를 수집할 때는 기존 **`orders`** 테이블의 스키마와 **`line_items`** 테이블의 스키마에 맞게 변환해야 합니다.
# MAGIC
# MAGIC 스트림으로 데이터를 수집하기 전에, 다음과 같은 다양한 문제를 해결할 수 있도록 정적 **`DataFrame`**으로 시작하는 것이 좋습니다.
# MAGIC * 열 이름 변경 및 평면화
# MAGIC * products 배열 분해
# MAGIC * **`submittedAt`** 열을 **`timestamp`**로 구문 분석
# MAGIC * **`orders`** 및 **`line_items`** 스키마 준수 - 이 테이블들은 델타 테이블이므로 스키마가 올바르지 않으면 스키마를 추가할 수 없습니다.
# MAGIC
# MAGIC 또한, JSON 파일에서 스트림을 생성하려면 먼저 스키마를 지정해야 합니다. 스트림을 시작하기 전에 일부 JSON 파일에서 스키마를 "속여" 유추할 수 있습니다.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 5번 설정</h2>
# MAGIC
# MAGIC 시작하려면 다음 셀을 실행하여 이 연습을 설정하고, 연습별 변수와 함수를 선언하세요.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-05

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #5.A - 데이터베이스 사용</h2>
# MAGIC
# MAGIC 각 노트북은 서로 다른 Spark 세션을 사용하며, 처음에는 **`default`** 데이터베이스를 사용합니다.
# MAGIC
# MAGIC 이전 연습과 마찬가지로, 사용자별 데이터베이스를 사용하면 공통으로 명명된 테이블에 대한 경합을 피할 수 있습니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 이 노트북에서 생성된 테이블이 **`default`** 데이터베이스에 **추가되지 않도록** 변수 **`user_db`**로 식별된 데이터베이스를 사용합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #5.A를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
spark.sql(f"use {user_db}")
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #5.A
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_05_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #5.B - 스트림 추가 주문</h2>
# MAGIC
# MAGIC 스트림에서 수집된 모든 JSON 파일은 하나의 주문과 해당 주문에서 구매된 제품의 열거된 목록을 나타냅니다.
# MAGIC
# MAGIC 목표는 간단합니다. 데이터를 수집하고, **`orders`** 테이블 스키마에 따라 데이터를 변환한 후, 새 레코드를 기존 테이블에 추가하는 것입니다.
# MAGIC
# MAGIC **이 단계에서는 다음 작업을 수행해야 합니다.**
# MAGIC
# MAGIC * JSON 파일 스트림을 수집합니다.
# MAGIC * **`stream_path`**로 식별된 경로에서 스트림을 시작합니다.
# MAGIC * **`maxFilesPerTrigger`** 옵션을 사용하여 반복당 하나의 파일만 처리하도록 스트림을 조절합니다.
# MAGIC * 수집 메타데이터를 추가합니다(다른 데이터 세트와 동일):
# MAGIC * **`ingested_at`**:**`timestamp`**
# MAGIC * **`ingest_file_name`**:**`string`**
# MAGIC * **`submitted_at`**을 유효한 **`timestamp`**로 올바르게 구문 분석합니다.
# MAGIC * "**yyyy-MM**" 형식을 사용하여 **`submitted_yyyy_mm`** 열을 추가합니다.
# MAGIC * **`orders`** 테이블의 스키마를 따르도록 열 이름과 데이터 유형에 필요한 기타 변경 작업을 수행합니다.
# MAGIC
# MAGIC * 스트림을 Delta **테이블**에 기록합니다.:
# MAGIC * 테이블의 형식은 "**delta**"여야 합니다.
# MAGIC * **`submitted_yyyy_mm`** 열을 기준으로 데이터를 분할합니다.
# MAGIC * 레코드는 **`orders_table`** 변수로 식별되는 테이블에 추가해야 합니다.
# MAGIC * 쿼리는 변수로 식별되는 테이블과 동일한 이름을 가져야 합니다. **`orders_table`**
# MAGIC * 쿼리는 **`orders_checkpoint_path`** 변수로 식별된 체크포인트 위치를 사용해야 합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #5.B를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

# TODO
from pyspark.sql.functions import (
    input_file_name, current_timestamp, to_timestamp, date_format, col
)

# 1. 정적 DataFrame으로 스키마 추론
schema = spark.read.format("json").load(stream_path).schema

# 2. 스트림 읽기 및 컬럼 처리
df_stream_cleaned = (
    spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .schema(schema)
        .load(stream_path)
        .withColumn("submitted_at", to_timestamp(col("submittedAt")))  # ISO 8601 처리
        .withColumn("submitted_yyyy_mm", date_format(col("submitted_at"), "yyyy-MM"))
        .withColumn("shipping_address_attention", col("shippingAddress.attention"))
        .withColumn("shipping_address_address", col("shippingAddress.address"))
        .withColumn("shipping_address_city", col("shippingAddress.city"))
        .withColumn("shipping_address_state", col("shippingAddress.state"))
        .withColumn("shipping_address_zip", col("shippingAddress.zip").cast("int"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("ingest_file_name", input_file_name())
        .select(
            col("submitted_at"),
            col("orderId").alias("order_id"),
            col("customerId").alias("customer_id"),
            col("salesRepId").alias("sales_rep_id"),
            col("shipping_address_attention"),
            col("shipping_address_address"),
            col("shipping_address_city"),
            col("shipping_address_state"),
            col("shipping_address_zip"),
            col("submitted_yyyy_mm")
        )
)

# 3. 스트리밍 델타 테이블 쓰기
(
    df_stream_cleaned.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", orders_checkpoint_path)
        .partitionBy("submitted_yyyy_mm")
        .queryName(orders_table)
        .table(orders_table)
)

# Use this cell to complete your solution

# COMMAND ----------

spark.table(orders_table).printSchema()

# COMMAND ----------

df_stream.printSchema()


# COMMAND ----------

# MAGIC %md ### Reality Check.B
# MAGIC 다음 명령을 실행하여 제대로 작동하는지 확인하세요.
# MAGIC
# MAGIC **주의**: 위 셀에서는 델타 테이블에 데이터를 추가하며, 최종 레코드 수는 아래에서 검증됩니다. 스트림을 다시 시작하면 필연적으로 이 테이블에 중복 레코드가 추가되어 검증이 실패하게 됩니다. 이 시나리오에서는 두 가지 사항을 해결해야 합니다.
# MAGIC * **연습 #3**을 다시 실행하여 중복 데이터 문제를 해결하세요. 이 연습에서는 데이터 세트를 삭제하거나 덮어쓰고 이 연습의 기본 상태로 되돌릴 수 있습니다.
# MAGIC * 스트림의 상태 문제(처리된 파일 기억)를 해결하려면 *`orders_checkpoint_path`*로 식별된 디렉터리를 삭제하세요.

# COMMAND ----------

reality_check_05_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #5.C - 스트림 추가 라인 항목</h2>
# MAGIC
# MAGIC 이전 스트림에서 처리했던 JSON 파일에는 라인 항목도 포함되어 있습니다. 이제 이 라인 항목을 추출하여 기존 **`line_items`** 테이블에 추가해야 합니다.
# MAGIC
# MAGIC 앞서와 마찬가지로 목표는 간단합니다. 데이터를 수집하고, **`line_items`** 테이블 스키마에 따라 데이터를 변환한 후, 새 레코드를 기존 테이블에 추가하는 것입니다.
# MAGIC
# MAGIC 참고: 동일한 스트림을 두 번 처리합니다. 이 작업을 더 효율적으로 수행하는 다른 패턴도 있지만, 이 연습에서는 디자인을 단순하게 유지하려고 합니다.<br/>
# MAGIC 다행히 이전 단계의 코드 대부분을 복사하여 바로 시작할 수 있습니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC
# MAGIC * JSON 파일 스트림 수집:
# MAGIC * **`stream_path`**로 식별된 경로에서 스트림을 시작합니다.
# MAGIC * **`maxFilesPerTrigger`** 옵션을 사용하여 반복당 하나의 파일만 처리하도록 스트림을 조절합니다.
# MAGIC * 수집 메타데이터를 추가합니다(다른 데이터세트와 동일):
# MAGIC * **`ingested_at`**:**`timestamp`**
# MAGIC * **`ingest_file_name`**:**`string`**
# MAGIC * **`line_items`** 테이블의 스키마를 따르도록 열 이름과 데이터 유형에 필요한 기타 변경 작업을 수행합니다.
# MAGIC * 가장 중요한 변환은 **`products`** 열에 대한 변환입니다.
# MAGIC * **`products`** 열은 요소 배열이므로 분해해야 합니다(**`pyspark.sql.functions`** 참조).
# MAGIC * 한 가지 해결책은 다음과 같습니다.
# MAGIC 1. **`order_id`**를 선택하고 **`products`**를 분해한 후 이름을 **`product`**로 바꿉니다.
# MAGIC 2. **`product`** 열의 중첩된 값을 평면화합니다.
# MAGIC 3. 수집 메타데이터(**`ingest_file_name`** 및 **`ingested_at`**)를 추가합니다.
# MAGIC 4. **`line_items`** 테이블 스키마에 따라 데이터 유형을 변환합니다.
# MAGIC
# MAGIC * 델타 싱크에 스트림을 작성합니다.
# MAGIC * 싱크의 형식은 "**delta**"여야 합니다.
# MAGIC * 레코드는 **`line_items_table`** 변수로 식별되는 테이블에 추가되어야 합니다.
# MAGIC * 쿼리 이름은 테이블 이름과 동일해야 하며, **`line_items_table`** 변수로 식별되어야 합니다.
# MAGIC * 쿼리는 **`line_items_checkpoint_path`** 변수로 식별되는 체크포인트 위치를 사용해야 합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #5.C를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

# TODO
from pyspark.sql.functions import (
    explode, col, input_file_name, current_timestamp
)

# 1. 정적 데이터프레임으로 스키마 추론
schema = spark.read.format("json").load(stream_path).schema

# 2. 스트리밍 JSON 데이터 로드
df_stream = (
    spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .schema(schema)
        .load(stream_path)
)

# 3. products 배열 분해 및 정제
df_line_items = (
    df_stream
        .select(
            col("orderId").alias("order_id"),
            explode(col("products")).alias("product"),
            input_file_name().alias("ingest_file_name"),
            current_timestamp().alias("ingested_at")
        )
        .select(
            col("order_id"),
            col("product.productId").alias("product_id"),
            col("product.quantity").cast("integer").alias("product_quantity"),
            col("product.soldPrice").alias("product_sold_price").cast("decimal(10,2)"),
            col("ingest_file_name"),
            col("ingested_at")
        )
)

# 4. 스트리밍 델타 테이블로 기록
(
    df_line_items.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", line_items_checkpoint_path)
        .queryName(line_items_table)
        .table(line_items_table)
)
# Use this cell to complete your solution

# COMMAND ----------

spark.table(line_items_table).printSchema()

# COMMAND ----------

df_line_items.printSchema()

# COMMAND ----------

# MAGIC %md ### Reality Check.C
# MAGIC 다음 명령을 실행하여 제대로 작동하는지 확인하세요.

# COMMAND ----------

reality_check_05_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5 - Final Check</h2>
# MAGIC
# MAGIC 다음 명령을 실행하여 이 연습이 완료되었는지 확인하세요.

# COMMAND ----------

reality_check_05_final()