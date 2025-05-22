# Databricks notebook source
# DBTITLE 0,--i18n-ad7af192-ab00-41a3-b683-5de4856cacb0
# MAGIC %md
# MAGIC # Spark SQL
# MAGIC
# MAGIC DataFrame API를 사용하여 Spark SQL의 기본 개념을 설명합니다.
# MAGIC
# MAGIC ##### 목표
# MAGIC 1. SQL 쿼리 실행
# MAGIC 1. 테이블에서 DataFrame 생성
# MAGIC 1. DataFrame 변환을 사용하여 동일한 쿼리 작성
# MAGIC 1. DataFrame 액션을 사용하여 계산 트리거
# MAGIC 1. DataFrame과 SQL 간 변환
# MAGIC
# MAGIC ##### 방법
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:
# MAGIC - 변환: **`select`**, **`where`**, **`orderBy`**
# MAGIC - 액션: **`show`**, **`count`**, **`take`**
# MAGIC - 기타 방법: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.01

# COMMAND ----------

# DBTITLE 0,--i18n-3ad6c2cb-bfa4-4af5-b637-ba001a9ef54b
# MAGIC %md
# MAGIC
# MAGIC ## 다중 인터페이스
# MAGIC Spark SQL은 다중 인터페이스를 갖춘 구조화된 데이터 처리를 위한 모듈입니다.
# MAGIC
# MAGIC Spark SQL은 두 가지 방법으로 상호 작용할 수 있습니다.
# MAGIC 1. SQL 쿼리 실행
# MAGIC 1. DataFrame API 사용

# COMMAND ----------

# DBTITLE 0,--i18n-236a9dcf-8e89-4b08-988a-67c3ca31bb71
# MAGIC %md
# MAGIC **Method 1: SQL 쿼리 실행**
# MAGIC
# MAGIC 이것은 기본적인 SQL 쿼리입니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price

# COMMAND ----------

# DBTITLE 0,--i18n-58f7e711-13f5-4015-8cff-c18ec5b305c6
# MAGIC %md
# MAGIC
# MAGIC **Method 2: DataFrame API 사용**
# MAGIC
# MAGIC DataFrame API를 사용하여 Spark SQL 쿼리를 표현할 수도 있습니다.
# MAGIC 다음 셀은 위에서 검색된 결과와 동일한 결과를 포함하는 DataFrame을 반환합니다.

# COMMAND ----------

display(spark
        .table("products")
        .select("name", "price")
        .where("price < 200")
        .orderBy("price")
       )

# COMMAND ----------

# DBTITLE 0,--i18n-5b338899-be0c-46ae-92d9-8cfc3c2c3fb8
# MAGIC %md
# MAGIC
# MAGIC DataFrame API의 구문은 이 강의 후반부에서 살펴보겠지만, 이 빌더 디자인 패턴을 사용하면 SQL에서 볼 수 있는 것과 매우 유사한 일련의 연산을 연결할 수 있다는 것을 알 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-bb02bfff-cf98-4639-af21-76bec5c8d95b
# MAGIC %md
# MAGIC
# MAGIC ## 쿼리 실행
# MAGIC 어떤 인터페이스를 사용하든 동일한 쿼리를 표현할 수 있습니다. Spark SQL 엔진은 Spark 클러스터에서 최적화하고 실행하는 데 사용되는 것과 동일한 쿼리 계획을 생성합니다.
# MAGIC
# MAGIC ![쿼리 실행 엔진](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="참고"> 복원력 있는 분산 데이터셋(RDD)은 Spark 클러스터에서 처리되는 데이터셋의 저수준 표현입니다. 초기 버전의 Spark에서는 <a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html" target="_blank">RDD를 직접 조작하는 코드</a>를 작성해야 했습니다. 최신 버전의 Spark에서는 더 높은 수준의 DataFrame API를 사용해야 합니다. Spark는 이를 자동으로 저수준 RDD 작업으로 컴파일합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-fbaea5c1-fefc-4b3b-a645-824ffa77bbd5
# MAGIC %md
# MAGIC
# MAGIC ## Spark API Documentation
# MAGIC
# MAGIC Spark SQL에서 DataFrames를 사용하는 방법을 알아보기 위해 먼저 Spark API 문서를 살펴보겠습니다.
# MAGIC Spark의 주요 <a href="https://spark.apache.org/docs/latest/" target="_blank">문서</a> 페이지에는 각 Spark 버전에 대한 API 문서 링크와 유용한 가이드가 포함되어 있습니다.
# MAGIC
# MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html" target="_blank">Scala API</a>와 <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Python API</a>가 가장 일반적으로 사용되며, 두 언어의 문서를 모두 참조하는 것이 도움이 되는 경우가 많습니다.
# MAGIC Scala 문서는 일반적으로 더 포괄적이고 Python 문서는 더 많은 코드 예제를 포함하는 경향이 있습니다.
# MAGIC
# MAGIC #### Spark SQL 모듈 문서 탐색
# MAGIC Scala API에서는 **`org.apache.spark.sql`**, Python API에서는 **`pyspark.sql`**로 이동하여 Spark SQL 모듈을 찾으세요.
# MAGIC 이 모듈에서 살펴볼 첫 번째 클래스는 **`SparkSession`** 클래스입니다. 검색창에 "SparkSession"을 입력하면 찾을 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-24790eda-96df-49bb-af34-b1ed839fa80a
# MAGIC %md
# MAGIC ## SparkSession
# MAGIC **`SparkSession`** 클래스는 DataFrame API를 사용하는 Spark의 모든 기능에 대한 단일 진입점입니다.
# MAGIC
# MAGIC Databricks 노트북에서는 SparkSession이 자동으로 생성되어 **`spark`**라는 변수에 저장됩니다.

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 0,--i18n-4f5934fb-12b9-4bf2-b821-5ab17d627309
# MAGIC %md
# MAGIC
# MAGIC 이 수업의 시작 부분에 나온 예제에서는 SparkSession의 **table` 메서드를 사용하여 **products`** 테이블에서 DataFrame을 생성했습니다. 이 DataFrame을 **products_df`** 변수에 저장해 보겠습니다.

# COMMAND ----------

products_df = spark.table("products")

# COMMAND ----------

# DBTITLE 0,--i18n-f9968eff-ed08-4ed6-9fe7-0252b94bf50a
# MAGIC %md
# MAGIC 다음은 DataFrame을 생성하는 데 사용할 수 있는 몇 가지 추가 메서드입니다. 이 모든 메서드는 **SparkSession`**에 대한 <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.html" target="_blank">문서</a>에서 확인할 수 있습니다.
# MAGIC
# MAGIC #### **`SparkSession`** 메서드
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | sql | 주어진 쿼리의 결과를 나타내는 DataFrame을 반환합니다. |
# MAGIC | table | 지정된 테이블을 DataFrame으로 반환합니다. |
# MAGIC | read | DataFrame으로 데이터를 읽는 데 사용할 수 있는 DataFrameReader를 반환합니다. |
# MAGIC | range | 시작부터 끝까지(제외) 범위에 있는 요소를 포함하는 열과 단계 값 및 파티션 개수를 가진 DataFrame을 생성합니다. |
# MAGIC | createDataFrame | 주로 테스트용으로 사용되는 튜플 목록에서 DataFrame을 생성합니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-2277e250-91f9-489a-940b-97d17e75c7f5
# MAGIC %md
# MAGIC
# MAGIC SparkSession 메서드를 사용하여 SQL을 실행해 보겠습니다.

# COMMAND ----------

result_df = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

display(result_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f2851702-3573-4cb4-9433-ec31d4ceb0f2
# MAGIC %md
# MAGIC
# MAGIC ## DataFrames
# MAGIC DataFrame API의 메서드를 사용하여 쿼리를 표현하면 결과가 DataFrame으로 반환된다는 점을 기억하세요. 이 결과를 **budget_df** 변수에 저장해 보겠습니다.
# MAGIC
# MAGIC **DataFrame**은 명명된 열로 그룹화된 데이터의 분산된 컬렉션입니다.

# COMMAND ----------

budget_df = (spark
             .table("products")
             .select("name", "price")
             .where("price < 200")
             .orderBy("price")
            )

# COMMAND ----------

# DBTITLE 0,--i18n-d538680a-1d7a-433c-9715-7fd975d4427b
# MAGIC %md
# MAGIC
# MAGIC **`display()`**를 사용하여 데이터프레임의 결과를 출력할 수 있습니다.

# COMMAND ----------

display(budget_df)

# COMMAND ----------

# DBTITLE 0,--i18n-ea532d26-a607-4860-959a-00a2eca34305
# MAGIC %md
# MAGIC
# MAGIC **스키마**는 데이터프레임의 열 이름과 유형을 정의합니다.
# MAGIC
# MAGIC **`schema`** 속성을 사용하여 데이터프레임의 스키마에 액세스합니다.

# COMMAND ----------

budget_df.schema

# COMMAND ----------

# DBTITLE 0,--i18n-4212166a-a200-44b5-985c-f7f1b33709a3
# MAGIC %md
# MAGIC
# MAGIC **`printSchema()`** 메서드를 사용하여 이 스키마의 더 나은 출력을 확인하세요.

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-7ad577db-093a-40fb-802e-99bbc5a4435b
# MAGIC %md
# MAGIC
# MAGIC ## 변환
# MAGIC **`budget_df`**를 생성할 때 **`select`**, **`where`**, **`orderBy`**와 같은 일련의 DataFrame 변환 메서드를 사용했습니다.
# MAGIC
# MAGIC <strong><code>products_df
# MAGIC &nbsp; .select("name", "price")
# MAGIC &nbsp; .where("price < 200")
# MAGIC &nbsp; .orderBy("price")
# MAGIC </code></strong>
# MAGIC
# MAGIC 변환은 DataFrame에서 동작하고 DataFrame을 반환하므로, 변환 메서드를 연결하여 새로운 DataFrame을 생성할 수 있습니다.
# MAGIC 하지만 이러한 연산은 단독으로 실행될 수 없습니다. 변환 메서드는 **지연 평가**되기 때문입니다.
# MAGIC
# MAGIC 다음 셀을 실행해도 계산이 트리거되지 않습니다.

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price"))

# COMMAND ----------

# DBTITLE 0,--i18n-56f40b55-842f-44cf-b34a-b0fd17a962d4
# MAGIC %md
# MAGIC
# MAGIC ## 동작
# MAGIC 반대로, DataFrame 동작은 **계산을 트리거**하는 메서드입니다.
# MAGIC 동작은 DataFrame 변환의 실행을 트리거하는 데 필요합니다.
# MAGIC
# MAGIC **`show`** 동작은 다음 셀에서 변환을 실행하도록 합니다.

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
  .show())

# COMMAND ----------

# DBTITLE 0,--i18n-6f574091-0026-4dd2-9763-c6d4c3b9c4fe
# MAGIC %md
# MAGIC
# MAGIC 아래는 <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#dataframe-apis" target="_blank">DataFrame</a> 작업의 몇 가지 예입니다.
# MAGIC
# MAGIC ### DataFrame 작업 메서드
# MAGIC | 메서드 | 설명 |
# MAGIC | --- | --- |
# MAGIC | show | DataFrame의 상위 n개 행을 표 형식으로 표시합니다. |
# MAGIC | count | DataFrame의 행 수를 반환합니다. |
# MAGIC | describe, summary | 숫자 및 문자열 열에 대한 기본 통계를 계산합니다. |
# MAGIC | first, head | 첫 번째 행을 반환합니다. |
# MAGIC | collect | 이 DataFrame의 모든 행을 포함하는 배열을 반환합니다. |
# MAGIC | take | DataFrame의 처음 n개 행을 포함하는 배열을 반환합니다. |

# COMMAND ----------

# DBTITLE 0,--i18n-7e725f41-43bc-4e56-9c44-d46becd375a0
# MAGIC %md
# MAGIC **`count`**는 DataFrame의 레코드 수를 반환합니다.

# COMMAND ----------

budget_df.count()

# COMMAND ----------

# DBTITLE 0,--i18n-12ea69d5-587e-4953-80d9-81955eeb9d7b
# MAGIC %md
# MAGIC **`collect`**는 DataFrame의 모든 행을 배열로 반환합니다.

# COMMAND ----------

budget_df.collect()

# COMMAND ----------

# DBTITLE 0,--i18n-983f5da8-b456-42b5-b21c-b6f585c697b4
# MAGIC %md
# MAGIC
# MAGIC ## DataFrame과 SQL 간 변환

# COMMAND ----------

# DBTITLE 0,--i18n-0b6ceb09-86dc-4cdd-9721-496d01e8737f
# MAGIC %md
# MAGIC **`createOrReplaceTempView`**는 DataFrame을 기반으로 임시 뷰를 생성합니다. 임시 뷰의 수명은 DataFrame을 생성하는 데 사용된 SparkSession에 연결됩니다.

# COMMAND ----------

budget_df.createOrReplaceTempView("budget")

# COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

# DBTITLE 0,--i18n-81ff52ca-2160-4bfd-a78f-b3ba2f8b4933
# MAGIC %md
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()