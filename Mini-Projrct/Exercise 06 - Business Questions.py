# Databricks notebook source
# MAGIC %md
# MAGIC # 연습 #6 - 비즈니스 질문
# MAGIC
# MAGIC 마지막 연습에서는 네 개의 테이블(**`orders`**, **`line_items`**, **`sales_reps`**, **`products`**)에 대해 다양한 조인을 실행하여 기본적인 비즈니스 질문에 답해 보겠습니다.
# MAGIC
# MAGIC 이 연습은 3단계로 나뉩니다.
# MAGIC * 연습 6.A - 데이터베이스 사용
# MAGIC * 연습 6.B - 질문 1
# MAGIC * 연습 6.C - 질문 2
# MAGIC * 연습 6.D - 질문 3

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #6 설정</h2>
# MAGIC
# MAGIC 시작하려면 다음 셀을 실행하여 이 연습을 설정하고, 연습별 변수와 함수를 선언하세요.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-06

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #6.A - 데이터베이스 사용</h2>
# MAGIC
# MAGIC 각 노트북은 서로 다른 Spark 세션을 사용하며, 처음에는 **`default`** 데이터베이스를 사용합니다.
# MAGIC
# MAGIC 이전 연습과 마찬가지로, 사용자별 데이터베이스를 사용하면 공통으로 명명된 테이블에 대한 경합을 피할 수 있습니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 이 노트북에서 생성된 테이블이 **`default`** 데이터베이스에 **추가되지 않도록**, **`user_db`** 변수로 식별된 데이터베이스를 사용합니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #6.A를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #6.A
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_06_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습 #6.B - 문제 #1</h2>
# MAGIC ## 각 주에 배송된 주문은 몇 개입니까?
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * **`shipping_address_state`**로 주문을 집계합니다.
# MAGIC * 각 주의 레코드 수를 계산합니다.
# MAGIC * **`count`**로 결과를 내림차순으로 정렬합니다.
# MAGIC * 결과를 임시 뷰인 **`question_1_results`**에 저장합니다. 이 뷰는 **`question_1_results_table`** 변수로 식별됩니다.

# COMMAND ----------

# MAGIC %md ### 연습문제 #6.B를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #6.B
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_06_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> 연습문제 #6.C - 문제 #2</h2>
# MAGIC ## 영업 담당자가 잘못된 사회보장번호(SSN)를 제출한 노스캐롤라이나주에 판매된 친환경 제품의 평균, 최소, 최대 판매 가격은 얼마입니까?
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 네 테이블 모두에 대해 조인을 실행합니다.
# MAGIC * **`orders`**, **`orders_table`** 변수로 식별됨
# MAGIC * **`line_items`**, **`line_items_table`** 변수로 식별됨
# MAGIC * **`products`**, **`products_table`** 변수로 식별됨
# MAGIC * **`sales_reps`**, **`sales_reps_table`** 변수로 식별됨
# MAGIC * 결과를 green products(**`color`**)으로만 제한합니다.
# MAGIC * 결과를 North Carolina로 배송된 주문으로 제한합니다(**`shipping_address_state`**)
# MAGIC * 결과를 처음에 잘못된 형식의 주민등록번호(**`_error_ssn_format`**)를 제출한 영업 담당자로 제한합니다.
# MAGIC * **`product_sold_price`**의 평균, 최소값, 최대값을 계산합니다. 계산 후 이 열의 이름을 변경하지 마세요.
# MAGIC * 결과를 임시 뷰인 **`question_2_results`**에 저장합니다. 이 뷰는 **`question_2_results_table`** 변수로 식별됩니다.
# MAGIC * 임시 뷰에는 다음 세 개의 열이 있어야 합니다. **`avg(product_sold_price)`**, **`min(product_sold_price)`**, **`max(product_sold_price)`**
# MAGIC * 결과를 운전자에게 수집합니다.
# MAGIC * 다음 로컬 변수에 평균, 최소값, 최대값을 할당합니다. 이 변수는 유효성 검사를 위해 현실 확인에 전달됩니다.
# MAGIC * **`ex_avg`** - 평균값을 저장하는 지역 변수
# MAGIC * **`ex_min`** - 최소값을 저장하는 지역 변수
# MAGIC * **`ex_max`** - 최대값을 저장하는 지역 변수

# COMMAND ----------

# MAGIC %md ### 연습문제 #6.C를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해답을 구현하세요.

# COMMAND ----------

# TODO
# Use this cell to complete your solution

ex_avg = 0 # FILL_IN
ex_min = 0 # FILL_IN
ex_max = 0 # FILL_IN

# COMMAND ----------

# MAGIC %md ### Reality Check #6.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_c(ex_avg, ex_min, ex_max)

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png">연습문제 #6.D - 문제 #3</h2>
# MAGIC ## 순매출 기준으로 가장 많은 수입을 올리는 영업 담당자의 성과 이름은 무엇입니까?
# MAGIC
# MAGIC 이 시나리오에서는...
# MAGIC * 가장 많은 수입을 올리는 영업 담당자는 가장 큰 수익을 창출하는 사람으로 정의됩니다.
# MAGIC * 이익은 **`product_sold_price`**와 **`price`**의 차이로 정의됩니다.<br/>
# MAGIC **(product_sold_price - price) * product_quantity**에서 볼 수 있듯이 이 차이에 **`product_quantity`**를 곱합니다.
# MAGIC
# MAGIC **이 단계에서는 다음을 수행해야 합니다.**
# MAGIC * 네 테이블 모두에 대해 조인을 실행합니다.
# MAGIC * **`orders`**, **`orders_table`** 변수로 식별됨
# MAGIC * **`line_items`**, **`line_items_table`** 변수로 식별됨
# MAGIC * **`products`**, **`products_table`** 변수로 식별됨
# MAGIC * **`sales_reps`**, **`sales_reps_table`** 변수로 식별됨
# MAGIC * 위에서 설명한 대로 주문의 각 라인 항목에 대한 이익을 계산합니다.
# MAGIC * 영업 담당자의 first &amp; last name을 기준으로 결과를 집계한 다음 각 담당자의 총 이익을 합산합니다.
# MAGIC * 가장 큰 수익을 낸 영업 담당자에 대한 데이터 세트를 단일 행으로 줄입니다.
# MAGIC * 결과를 임시 뷰인 **`question_3_results`**에 저장합니다. 이 뷰는 **`question_3_results_table`** 변수로 식별됩니다.
# MAGIC * 임시 뷰에는 다음 세 개의 열이 있어야 합니다.
# MAGIC * **`sales_rep_first_name`** - 집계 기준이 되는 첫 번째 열
# MAGIC * **`sales_rep_last_name`** - 집계 기준이 되는 두 번째 열
# MAGIC * **`sum(total_profit)`** - **`total_profit`** 열의 합계

# COMMAND ----------

# MAGIC %md ### 연습문제 #6.D를 구현하세요.
# MAGIC
# MAGIC 다음 셀에 해결책을 구현하세요.

# COMMAND ----------

# TODO
# Use this cell to complete your solution

# COMMAND ----------

# MAGIC %md ### Reality Check #6.D
# MAGIC 다음 명령을 실행하여 제대로 진행되고 있는지 확인하세요.

# COMMAND ----------

reality_check_06_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6 - Final Check</h2>
# MAGIC
# MAGIC 다음 명령을 실행하여 이 연습이 완료되었는지 확인하세요.

# COMMAND ----------

reality_check_06_final()

# COMMAND ----------

# MAGIC %md
# MAGIC # 프로젝트를 완료하신 것을 축하드립니다!