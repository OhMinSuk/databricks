# Databricks notebook source
# MAGIC %md # 연습 #1 - 프로젝트 개요 및 데이터셋 설치
# MAGIC
# MAGIC 이 프로젝트는 Apache Spark 및 DataFrame API와 관련된 기본적인 기술을 평가하는 것을 목표로 합니다.
# MAGIC
# MAGIC 본 프로젝트는 다음 엔티티에 익숙하고 어느 정도 경험이 있다고 가정합니다.
# MAGIC * **`SparkContext`**
# MAGIC * **`SparkSession`**
# MAGIC * **`DataFrame`**
# MAGIC * **`DataFrameReader`**
# MAGIC * **`DataFrameWriter`**
# MAGIC * **`pyspark.sql.functions`** 모듈에 있는 다양한 함수
# MAGIC
# MAGIC 이 프로젝트 전반에 걸쳐 구체적인 지침이 제공되며, 기존 지식과 <a href="https://spark.apache.org/docs/latest/api.html" target="_blank">Spark API 문서</a>와 같은 다른 자료를 활용하여 이러한 지침을 완료할 수 있기를 기대합니다.
# MAGIC
# MAGIC 프로젝트, 데이터 세트 및 다양한 연습 문제를 검토한 후,<br/>
# MAGIC 데이터 세트를 Databricks 작업 공간에 설치하여 프로젝트를 진행할 수 있도록 하겠습니다.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">프로젝트 개요</h2>
# MAGIC
# MAGIC * 프로젝트 - 이 프로젝트 소개
# MAGIC
# MAGIC * 데이터 - 이 프로젝트의 데이터세트 소개
# MAGIC
# MAGIC * 연습 - 이 프로젝트의 다양한 연습 개요

# COMMAND ----------

# MAGIC %md ### 프로젝트
# MAGIC
# MAGIC 이 프로젝트의 핵심은 구매 시스템에서 데이터를 수집하여 추가 분석을 위해 데이터 레이크에 로드하는 것입니다.
# MAGIC
# MAGIC 각 연습은 더 작은 단계, 즉 마일스톤으로 나뉩니다.
# MAGIC
# MAGIC 각 마일스톤 후에는 예상대로 진행되고 있는지 확인할 수 있도록 "Reality Check"을 제공합니다.
# MAGIC
# MAGIC 각 연습은 이전 연습을 기반으로 구성되므로 다음 연습으로 넘어가기 전에 모든 연습을 완료하고 "Reality Check"을 통과하는 것이 중요합니다.
# MAGIC
# MAGIC 이 프로젝트의 마지막 연습으로, 로드한 데이터를 사용하여 몇 가지 간단한 비즈니스 질문에 답해 보겠습니다.

# COMMAND ----------

# MAGIC %md ### 데이터
# MAGIC 원시 데이터는 세 가지 형태로 제공됩니다.
# MAGIC
# MAGIC 1. 2017년, 2018년, 2019년에 처리된 주문
# MAGIC * 각 연도별로 해당 연도 주문의 별도 배치(또는 백업)가 생성되었습니다.
# MAGIC * 세 파일의 형식은 유사하지만, 완전히 동일하게 생성된 것은 아닙니다.
# MAGIC * 2017년 파일은 고정 너비 텍스트 파일 형식입니다.
# MAGIC * 2018년 파일은 탭으로 구분된 텍스트 파일입니다.
# MAGIC * 2019년 파일은 쉼표로 구분된 텍스트 파일입니다.
# MAGIC * 각 주문은 네 가지 주요 데이터 요소로 구성됩니다.
# MAGIC 0. 주문 - 최상위 집계
# MAGIC 0. 개별 품목 - 주문에서 구매한 개별 제품
# MAGIC 0. 영업 담당자 - 주문자
# MAGIC 0. 고객 - 품목을 구매하고 배송한 사람
# MAGIC * 세 가지 배치 모두 개별 품목당 하나의 레코드가 있어 주문, 담당자 및 고객 간에 상당한 양의 중복 데이터가 생성된다는 점에서 일관성이 있습니다.
# MAGIC * 모든 엔터티는 일반적으로 order_id, customer_id 등의 ID로 참조됩니다.
# MAGIC
# MAGIC 2. 이 회사에서 판매하는 모든 제품(SKU)은 단일 XML 파일로 표현됩니다.
# MAGIC
# MAGIC 3. 2020년에 이 회사는 시스템을 전환하여 모든 주문에 대해 클라우드 스토리지에 단일 JSON 파일을 저장합니다.
# MAGIC * 이 주문은 2017년부터 2019년까지의 일괄 처리 데이터를 간소화한 버전으로, 주문 세부 정보, 개별 품목 및 관련 ID만 포함합니다.
# MAGIC * 영업 담당자의 데이터는 더 이상 주문과 함께 표현되지 않습니다.

# COMMAND ----------

# MAGIC %md ### 연습
# MAGIC
# MAGIC * **연습 #1**(이 노트)에서는 등록 절차, 데이터세트 설치, 그리고 이 프로젝트의 진행에 도움이 되는 현실 확인 방법을 소개합니다.
# MAGIC
# MAGIC * **연습 #2**에서는 2017년부터 2019년까지의 배치 데이터를 수집하여 향후 처리를 위해 단일 데이터세트로 결합합니다.
# MAGIC
# MAGIC * **연습 #3**에서는 **연습 #2**의 통합 배치 데이터를 가져와 정제한 후, 주문, 라인 품목, 영업 담당자라는 세 개의 새로운 데이터세트로 추출합니다. 편의상 고객 데이터는 주문과 함께 분리하지 않습니다.
# MAGIC
# MAGIC * **연습 #4**에서는 모든 프로젝트가 포함된 XML 문서를 수집하여 라인 품목과 결합하여 또 다른 데이터세트인 제품 라인 품목을 생성합니다.
# MAGIC
# MAGIC * **연습 #5**에서는 2020년 주문 스트림을 처리하고, 필요에 따라 해당 데이터 스트림을 기존 데이터 세트에 추가합니다.
# MAGIC
# MAGIC * **연습 #6**에서는 모든 새 데이터 세트를 사용하여 몇 가지 비즈니스 질문에 답합니다.
# MAGIC

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #1 - 데이터셋 설치</h2>
# MAGIC
# MAGIC 이 프로젝트의 데이터셋은 공개 객체 저장소에 저장됩니다.
# MAGIC
# MAGIC 이 프로젝트를 진행하기 전에 Databricks 작업 공간에 다운로드하여 설치해야 합니다.
# MAGIC
# MAGIC 하지만 그 전에 이 프로젝트에 적합한 클러스터를 구성해야 합니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC 1. 클러스터 구성(런타임 버전을 반드시 15.4로 구성 필요)
# MAGIC 2. 이 노트북을 클러스터에 연결
# MAGIC 3. 이 연습의 설치 노트북 실행
# MAGIC 4. 데이터셋 설치
# MAGIC 5. 데이터셋이 올바르게 설치되었는지 확인하기 위한 현실 확인 실행
# MAGIC

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-01

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #1 - Install Datasets</h2>
# MAGIC
# MAGIC 다음 명령을 실행하여 데이터셋을 작업 공간에 설치하세요.

# COMMAND ----------

# 이 프로젝트 진행 중 언제든지 reinstall=True를 설정하여 소스 데이터 세트를 다시 설치할 수 있습니다.
# 이 노트북을 다시 실행해도 이러한 데이터 세트는 자동으로
# 다시 설치되지 않으므로 시간을 절약할 수 있습니다.
install_datasets(reinstall=False)

# COMMAND ----------

# MAGIC %md ### Reality Check #1
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_install()