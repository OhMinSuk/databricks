# Databricks notebook source
# MAGIC %md 
# MAGIC # Exercise #4 - XML 수집, 제품 테이블
# MAGIC
# MAGIC 영업 담당자가 판매하는 제품은 XML 문서에 항목별로 정리되어 있으며, 이를 로드해야 합니다.
# MAGIC
# MAGIC CSV, JSON, Parquet, Delta와 달리 Apache Spark 기본 배포판에는 XML 지원이 포함되어 있지 않습니다.
# MAGIC
# MAGIC XML 문서를 로드하기 전에 XML 파일을 처리할 수 있는 **DataFrameReader**에 대한 추가 지원이 필요합니다.
# MAGIC
# MAGIC **spark-xml** 라이브러리가 클러스터에 설치되면 XML 문서를 로드하고 다른 변환 작업을 진행할 수 있습니다.
# MAGIC
# MAGIC 이 연습은 4단계로 나뉩니다.
# MAGIC * 연습 4.A - 데이터베이스 사용
# MAGIC * 연습 4.B - 라이브러리 설치
# MAGIC * 연습 4.C - 제품 로드
# MAGIC * 연습 4.D - 제품 품목 로드

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #4 설정</h2>
# MAGIC 시작하려면 다음 셀을 실행하여 이 연습을 설정하고, 연습별 변수와 함수를 선언하세요.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-04

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #4.A - 데이터베이스 사용</h2>
# MAGIC
# MAGIC 각 노트북은 서로 다른 Spark 세션을 사용하며, 처음에는 **`default`** 데이터베이스를 사용합니다.
# MAGIC
# MAGIC 이전 연습과 마찬가지로, 사용자별 데이터베이스를 사용하면 공통으로 명명된 테이블에 대한 경합을 피할 수 있습니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 이 노트북에서 생성된 테이블이 **`default`** 데이터베이스에 **추가되지 않도록** 변수 **`user_db`**로 식별된 데이터베이스를 사용합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #4.A를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
spark.sql(f"use {user_db}")
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #4.A
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_04_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #4.C - 제품 로드</h2>
# MAGIC
# MAGIC **spark-xml** 라이브러리가 설치된 경우, XML 문서를 처리하는 것은 제공된 특정 옵션을 제외하고는 다른 데이터세트를 처리하는 것과 동일합니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 다음 매개변수를 사용하여 XML 문서를 로드합니다.
# MAGIC * 형식: **xml**
# MAGIC * 옵션:
# MAGIC * **`rootTag`** = **`products`** - XML ​​문서의 루트 태그를 식별합니다. 이 경우 "products"입니다.
# MAGIC * **`rowTag`** = **`product`** - 루트 태그 아래 각 행의 태그를 식별합니다. 이 경우 "product"입니다.
# MAGIC * **`inferSchema`** = **`True`** - 파일 크기가 작고 한 번만 실행하면 됩니다. 스키마를 유추하면 시간을 절약할 수 있습니다.
# MAGIC * 파일 경로: **`products_xml_path`** 변수로 지정됨
# MAGIC
# MAGIC * 다음 사양을 준수하도록 스키마를 업데이트합니다.
# MAGIC * **`product_id`**:**`string`**
# MAGIC * **`color`**:**`string`**
# MAGIC * **`model_name`**:**`string`**
# MAGIC * **`model_number`**:**`string`**
# MAGIC * **`base_price`**:**`double`**
# MAGIC * **`color_adj`**:**`double`**
# MAGIC * **`size_adj`**:**`double`**
# MAGIC * **`price`**:**`double`**
# MAGIC * **`size`**:**`string`**
# MAGIC
# MAGIC * **`price`**가 포함되지 않은 레코드를 제외합니다. 이러한 레코드는 아직 판매할 수 없는 제품을 나타냅니다.
# MAGIC * 관리형 델타 테이블 **`products`**(변수 **`products_table`**로 식별됨)에 데이터 세트를 로드합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #4.C를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

# TODO
from pyspark.sql.functions import col

df_products = (spark.read.format("xml")
               .option("rootTag", "products")
               .option("rowTag", "product")
               .option("inferSchema", "true")
               .load(products_xml_path))
               
df_products = (
    df_products
    .withColumn("product_id", col("_product_id").cast("string"))
    .withColumn("color", col("color").cast("string"))
    .withColumn("model_name", col("model_name").cast("string"))
    .withColumn("model_number", col("model_number").cast("string"))
    .withColumn("base_price", col("price._base_price").cast("double"))
    .withColumn("color_adj", col("price._color_adj").cast("double"))
    .withColumn("size_adj", col("price._size_adj").cast("double"))
    .withColumn("price", col("price.usd").cast("double"))
    .withColumn("product_id", col("_product_id").cast("string"))
    .drop("_product_id")
    .filter(col("price").isNotNull())
)

df_products.write.format("delta").mode("overwrite").saveAsTable(products_table)

display(df_products)

# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #4.C
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_04_c()