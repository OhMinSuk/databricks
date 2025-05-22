# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC # Reader & Writer
# MAGIC ##### 목표
# MAGIC 1. CSV 파일에서 읽기
# MAGIC 1. JSON 파일에서 읽기
# MAGIC 1. DataFrame을 파일에 쓰기
# MAGIC 1. DataFrame을 테이블에 쓰기
# MAGIC 1. DataFrame을 Delta 테이블에 쓰기
# MAGIC
# MAGIC ##### 방법
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameReader</a>: **`csv`**, **`json`**, **`option`**, **`schema`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#input-and-output" target="_blank">DataFrameWriter</a>: **`mode`**, **`option`**, **`parquet`**, **`format`**, **`saveAsTable`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html#pyspark.sql.types.StructType" target="_blank">구조체 유형</a>: **`toDDL`**
# MAGIC
# MAGIC ##### Spark 유형
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types" target="_blank">유형</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.07

# COMMAND ----------

# DBTITLE 0,--i18n-24a8edc0-6f58-4530-a256-656e2b577e3e
# MAGIC %md
# MAGIC
# MAGIC ## DataFrameReader
# MAGIC 외부 저장소 시스템에서 DataFrame을 로드하는 데 사용되는 인터페이스
# MAGIC
# MAGIC **`spark.read.parquet("path/to/files")`**
# MAGIC
# MAGIC DataFrameReader는 SparkSession 속성인 **`read`**를 통해 액세스할 수 있습니다. 이 클래스에는 다양한 외부 저장소 시스템에서 DataFrame을 로드하는 메서드가 포함되어 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-108685bb-e26b-47db-a974-7e8de357085f
# MAGIC %md
# MAGIC
# MAGIC ### CSV 파일에서 읽기
# MAGIC DataFrameReader의 **`csv`** 메서드와 다음 옵션을 사용하여 CSV에서 읽기:
# MAGIC
# MAGIC Tab separator, use first line as header, infer schema

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .option("inferSchema", True)
           .csv(DA.paths.users_csv)
          )

users_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-86642c4a-e773-4856-b03a-13b359fa499f
# MAGIC %md
# MAGIC Spark의 Python API를 사용하면 DataFrameReader 옵션을 **`csv`** 메서드의 매개변수로 지정할 수도 있습니다.

# COMMAND ----------

users_df = (spark
           .read
           .csv(DA.paths.users_csv, sep="\t", header=True, inferSchema=True)
          )

users_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-8827b582-0b26-407c-ba78-cb64666d7a6b
# MAGIC %md
# MAGIC
# MAGIC 열 이름과 데이터 유형을 사용하여 **StructType**을 생성하여 스키마를 수동으로 정의합니다.

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("email", StringType(), True)
])

# COMMAND ----------

# DBTITLE 0,--i18n-d2e7e50d-afa1-4e65-826f-7eefc0a70640
# MAGIC %md
# MAGIC
# MAGIC 스키마를 유추하는 대신 이 사용자 정의 스키마를 사용하여 CSV에서 읽습니다.

# COMMAND ----------

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(user_defined_schema)
           .csv(DA.paths.users_csv)
          )

# COMMAND ----------

# DBTITLE 0,--i18n-0e098586-6d6c-41a6-9196-640766212724
# MAGIC %md
# MAGIC
# MAGIC 또는 <a href="https://en.wikipedia.org/wiki/Data_definition_language" target="_blank">데이터 정의 언어(DDL)</a> 구문을 사용하여 스키마를 정의합니다.

# COMMAND ----------

ddl_schema = "user_id string, user_first_touch_timestamp long, email string"

users_df = (spark
           .read
           .option("sep", "\t")
           .option("header", True)
           .schema(ddl_schema)
           .csv(DA.paths.users_csv)
          )

# COMMAND ----------

# DBTITLE 0,--i18n-bbc2fc78-c2d4-42c5-91c0-652154ce9f89
# MAGIC %md
# MAGIC
# MAGIC ### JSON 파일에서 읽기
# MAGIC
# MAGIC DataFrameReader의 **`json`** 메서드와 infer schema 옵션을 사용하여 JSON에서 읽기

# COMMAND ----------

events_df = (spark
            .read
            .option("inferSchema", True)
            .json(DA.paths.events_json)
           )

events_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-509e0bc1-1ffd-4c22-8188-c3317215d5e0
# MAGIC %md
# MAGIC
# MAGIC 스키마 이름과 데이터 유형을 사용하여 **`StructType`**을 생성하여 데이터를 더 빠르게 읽습니다.

# COMMAND ----------

from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([
        StructField("purchaseRevenue", DoubleType(), True),
        StructField("total_item_quantity", LongType(), True),
        StructField("unique_items", LongType(), True)
    ]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(
        StructType([
            StructField("coupon", StringType(), True),
            StructField("item_id", StringType(), True),
            StructField("item_name", StringType(), True),
            StructField("item_revenue_in_usd", DoubleType(), True),
            StructField("price_in_usd", DoubleType(), True),
            StructField("quantity", LongType(), True)
        ])
    ), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

events_df = (spark
            .read
            .schema(user_defined_schema)
            .json(DA.paths.events_json)
           )

# COMMAND ----------

# DBTITLE 0,--i18n-ae248126-23f3-49e2-ab43-63700049405c
# MAGIC %md
# MAGIC
# MAGIC 스칼라의 **`StructType`** 메서드인 **`toDDL`**을 사용하면 DDL 형식의 문자열을 자동으로 생성할 수 있습니다.
# MAGIC
# MAGIC 이 기능은 CSV 및 JSON 데이터를 처리하기 위해 DDL 형식의 문자열을 가져와야 하지만 문자열을 직접 작성하거나 스키마의 **`StructType`** 변형을 원하지 않을 때 편리합니다.
# MAGIC
# MAGIC 하지만 Python에서는 이 기능을 사용할 수 없지만, 노트북의 강력한 기능을 통해 두 언어를 모두 사용할 수 있습니다.

# COMMAND ----------

# Step 1 - 공유된 spark-config를 사용하여 Python과 Scala 간에 값(데이터 세트 경로)을 전송하려면 이 트릭을 사용하세요.
spark.conf.set("com.whatever.your_scope.events_path", DA.paths.events_json)

# COMMAND ----------

# DBTITLE 0,--i18n-5a35a507-1eff-4f5f-b6b9-6a254b61b38f
# MAGIC %md
# MAGIC 이와 같은 Python 노트북에서 Scala 셀을 생성하여 데이터를 삽입하고 DDL 형식의 스키마를 생성합니다.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Step 2 - config에서 값을 끌어오거나 복사하여 붙여넣습니다.
# MAGIC val eventsJsonPath = spark.conf.get("com.whatever.your_scope.events_path")
# MAGIC
# MAGIC // Step 3 - JSON을 읽지만 스키마를 추론하게 합니다.
# MAGIC val eventsSchema = spark.read
# MAGIC                         .option("inferSchema", true)
# MAGIC                         .json(eventsJsonPath)
# MAGIC                         .schema.toDDL
# MAGIC
# MAGIC // Step 4 - 스키마를 print하고, 선택한 후 복사합니다.
# MAGIC println("="*80)
# MAGIC println(eventsSchema)
# MAGIC println("="*80)

# COMMAND ----------

# Step 5 - 위의 스키마를 붙여넣고 여기에서 볼 수 있듯이 변수에 할당합니다.
events_schema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

# Step 6 - 새로운 DDL 형식 문자열을 사용하여 JSON 데이터를 읽습니다.
events_df = (spark.read
                 .schema(events_schema)
                 .json(DA.paths.events_json))

display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1a79ce6b-d803-4d25-a1c2-c24a70a0d6bf
# MAGIC %md
# MAGIC 이것은 완전히 새로운 데이터 세트에 대한 스키마를 생성하고 개발 속도를 높이는 데 매우 유용한 "트릭"입니다.
# MAGIC
# MAGIC 완료되면(예: 7단계) 임시 코드를 삭제하세요.
# MAGIC
# MAGIC 경고: **운영 환경에서는 이 트릭을 사용하지 마세요.**</br>
# MAGIC 스키마 추론은 스키마를 추론하기 위해 소스 데이터 세트를 모두 읽어야 하므로 매우 느릴 수 있습니다.<br/>

# COMMAND ----------

# DBTITLE 0,--i18n-f57b5940-857f-4e37-a2e4-030b27b3795a
# MAGIC %md
# MAGIC
# MAGIC ## DataFrameWriter
# MAGIC 외부 저장소 시스템에 DataFrame을 쓰는 데 사용되는 인터페이스
# MAGIC
# MAGIC <strong><code>
# MAGIC (df  
# MAGIC &nbsp;  .write                         
# MAGIC &nbsp;  .option("compression", "snappy")  
# MAGIC &nbsp;  .mode("overwrite")      
# MAGIC &nbsp;  .parquet(output_dir)       
# MAGIC )
# MAGIC </code></strong>
# MAGIC
# MAGIC DataFrameWriter는 SparkSession 속성인 **`write`**를 통해 접근할 수 있습니다. 이 클래스에는 다양한 외부 저장소 시스템에 DataFrame을 쓰는 메서드가 포함되어 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-8799bf1d-1d80-4412-b093-ad3ba71d73b8
# MAGIC %md
# MAGIC
# MAGIC ### 파일에 DataFrame 쓰기
# MAGIC
# MAGIC DataFrameWriter의 **`parquet`** 메서드와 다음 구성을 사용하여 **`users_df`**를 parquet에 쓰기:
# MAGIC
# MAGIC Snappy compression, overwrite mode

# COMMAND ----------

users_output_dir = DA.paths.working_dir + "/users.parquet"

(users_df
 .write
 .option("compression", "snappy")
 .mode("overwrite")
 .parquet(users_output_dir)
)

# COMMAND ----------

display(
    dbutils.fs.ls(users_output_dir)
)

# COMMAND ----------

# DBTITLE 0,--i18n-a2f733f5-8afc-48f0-aebb-52df3a6e461f
# MAGIC %md
# MAGIC DataFrameReader와 마찬가지로 Spark의 Python API를 사용하면 **parquet** 메서드의 매개변수로 DataFrameWriter 옵션을 지정할 수도 있습니다.

# COMMAND ----------

(users_df
 .write
 .parquet(users_output_dir, compression="snappy", mode="overwrite")
)

# COMMAND ----------

# DBTITLE 0,--i18n-61a5a982-f46e-4cf6-bce2-dd8dd68d9ed5
# MAGIC %md
# MAGIC
# MAGIC ### 테이블에 DataFrame 쓰기
# MAGIC
# MAGIC DataFrameWriter 메서드인 **saveAsTable`**을 사용하여 테이블에 **`events_df`**를 쓰기
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="참고"> 이 메서드는 DataFrame 메서드인 **`createOrReplaceTempView`**로 생성되는 로컬 뷰와 달리 전역 테이블을 생성합니다.

# COMMAND ----------

events_df.write.mode("overwrite").saveAsTable("events")

# COMMAND ----------

# DBTITLE 0,--i18n-abcfcd19-ba89-4d97-a4dd-2fa3a380a953
# MAGIC %md
# MAGIC
# MAGIC 이 테이블은 교실 설정에서 생성된 데이터베이스에 저장되었습니다. 아래에 인쇄된 데이터베이스 이름을 확인하세요.

# COMMAND ----------

print(DA.schema_name)

# COMMAND ----------

# DBTITLE 0,--i18n-9a929c69-4b77-4c4f-b7b6-2fca645037aa
# MAGIC %md
# MAGIC ## Delta Lake
# MAGIC
# MAGIC 거의 모든 경우, 특히 Databricks 작업 공간에서 데이터를 참조할 경우 Delta Lake 형식을 사용하는 것이 가장 좋습니다.
# MAGIC
# MAGIC <a href="https://delta.io/" target="_blank">Delta Lake</a>는 Spark와 함께 작동하여 데이터 레이크의 안정성을 높이도록 설계된 오픈 소스 기술입니다.
# MAGIC
# MAGIC
# MAGIC #### Delta Lake의 주요 기능
# MAGIC - ACID 트랜잭션
# MAGIC - 확장 가능한 메타데이터 처리
# MAGIC - 통합 스트리밍 및 일괄 처리
# MAGIC - 시간 이동(데이터 버전 관리)
# MAGIC - 스키마 적용 및 진화
# MAGIC - 감사 기록
# MAGIC - Parquet 형식
# MAGIC - Apache Spark API와 호환

# COMMAND ----------

# DBTITLE 0,--i18n-ba1e0aa1-bd35-4594-9eb7-a16b65affec1
# MAGIC %md
# MAGIC ### 델타 테이블에 결과 쓰기
# MAGIC
# MAGIC DataFrameWriter의 **`save`** 메서드와 다음 구성을 사용하여 **`events_df`**를 작성합니다. 델타 형식 및 덮어쓰기 모드.

# COMMAND ----------

events_output_path = DA.paths.working_dir + "/delta/events"

(events_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(events_output_path)
)

# COMMAND ----------

# DBTITLE 0,--i18n-331a0d38-4573-4987-9aa6-ebfc9476f85d
# MAGIC %md
# MAGIC
# MAGIC ### Clean up classroom

# COMMAND ----------

DA.cleanup()