# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-4212c527-b1c1-4cce-b629-b5ffb5c57d68
# MAGIC %md
# MAGIC
# MAGIC # Getting Started with the Databricks Platform
# MAGIC
# MAGIC 이 노트북은 Databricks Data Science and Engineering Workspace의 몇 가지 기본 기능을 직접 살펴보는 기능을 제공합니다.
# MAGIC
# MAGIC ## 학습 목표
# MAGIC 이 랩을 마치면 다음을 수행할 수 있어야 합니다.
# MAGIC - 노트북 이름 변경 및 기본 언어 변경
# MAGIC - 클러스터 연결
# MAGIC - **`%run`** 매직 명령어 사용
# MAGIC - Python 및 SQL 셀 실행
# MAGIC - 마크다운 셀 생성

# COMMAND ----------

# DBTITLE 0,--i18n-eb166a05-9a22-4a74-b54e-3f9e5779f342
# MAGIC %md
# MAGIC
# MAGIC # 노트북 이름 변경
# MAGIC
# MAGIC 노트북 이름을 변경하는 것은 간단합니다. 이 페이지 상단의 이름을 클릭한 다음 이름을 변경하세요. 나중에 이 노트북으로 쉽게 돌아갈 수 있도록 기존 이름 끝에 짧은 테스트 문자열을 추가하세요.

# COMMAND ----------

# DBTITLE 0,--i18n-a975b60c-9871-4736-9b4f-194577d730f0
# MAGIC %md
# MAGIC
# MAGIC # 클러스터 연결
# MAGIC
# MAGIC 노트북에서 셀을 실행하려면 클러스터에서 제공하는 컴퓨팅 리소스가 필요합니다. 노트북에서 셀을 처음 실행할 때, 클러스터가 아직 연결되지 않은 경우 연결하라는 메시지가 표시됩니다.
# MAGIC
# MAGIC 이 페이지 오른쪽 상단의 드롭다운을 클릭하여 지금 이 노트북에 클러스터를 연결하세요. 이전에 생성한 클러스터를 선택하세요. 노트북의 실행 상태가 지워지고 선택한 클러스터에 노트북이 연결됩니다.
# MAGIC
# MAGIC 드롭다운 메뉴에서는 필요에 따라 클러스터를 시작하거나 다시 시작할 수 있습니다. 또한 한 번의 작업으로 클러스터를 분리했다가 다시 연결할 수도 있습니다. 이는 필요에 따라 실행 상태를 지우는 데 유용합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-4cd4b089-e782-4e81-9b88-5c0abd02d03f
# MAGIC %md
# MAGIC
# MAGIC # %run 사용
# MAGIC
# MAGIC 모든 유형의 복잡한 프로젝트는 더 간단하고 재사용 가능한 구성 요소로 분할하는 기능의 이점을 누릴 수 있습니다.
# MAGIC
# MAGIC Databricks Notebooks에서는 **`%run`** 매직 명령어를 통해 이 기능을 사용할 수 있습니다.
# MAGIC
# MAGIC 이렇게 사용하면 변수, 함수 및 코드 블록이 현재 프로그래밍 컨텍스트의 일부가 됩니다.
# MAGIC
# MAGIC 다음 예를 살펴보겠습니다.
# MAGIC
# MAGIC **`Notebook_A`**에는 네 가지 명령이 있습니다.
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`%run ./Notebook_B`**
# MAGIC 4. **`print(f"Welcome back {full_name}`**
# MAGIC
# MAGIC **`Notebook_B`**에는 단 하나의 명령만 있습니다.
# MAGIC 1. **`full_name = f"{name} Doe"`**
# MAGIC
# MAGIC **`Notebook_B`**를 실행하면 **`name`** 변수가 **`Notebook_B`**에 정의되어 있지 않기 때문에 실행에 실패합니다.
# MAGIC
# MAGIC 마찬가지로, **`Notebook_A`**도 **`Notebook_A`**에 정의되어 있지 않은 **`full_name`** 변수를 사용하기 때문에 실패할 것이라고 생각할 수 있지만, 실제로는 그렇지 않습니다!
# MAGIC
# MAGIC 실제로는 아래에서 볼 수 있듯이 두 노트북이 병합된 후 **다음** 실행:
# MAGIC 1. **`name = "John"`**
# MAGIC 2. **`print(f"Hello {name}")`**
# MAGIC 3. **`full_name = f"{name} Doe"`**
# MAGIC 4. **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC 따라서 예상 동작은 다음과 같습니다.
# MAGIC * **`Hello John`**
# MAGIC * **`Welcome back John Doe`**

# COMMAND ----------

# DBTITLE 0,--i18n-40ca42ab-4275-4d92-b151-995429e54486
# MAGIC %md
# MAGIC
# MAGIC 이 노트북이 있는 폴더에는 **`ExampleSetupFolder`**라는 하위 폴더가 있으며, 이 폴더에는 **`example-setup`**이라는 노트북이 있습니다.
# MAGIC
# MAGIC 이 간단한 노트북은 **`my_name`** 변수를 선언하고 **`None`**으로 설정한 다음 **`example_df`**라는 DataFrame을 생성합니다.
# MAGIC
# MAGIC **`example-setup`** 노트북을 열고 **`my_name`**이 **`None`**이 아니라 따옴표로 묶인 사용자 이름(또는 다른 사람의 이름)이 되도록 수정합니다. 이렇게 하면 다음 두 셀이 **`AssertionError`**를 발생시키지 않고 실행됩니다.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> 이 구성 과정에 사용되는 추가 참조 **`_utility-methods`** 및 **`DBAcademyHelper`**가 표시되며, 이 연습에서는 무시해야 합니다.

# COMMAND ----------

# MAGIC %run ../ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-e5ef8dff-bfa6-4f9e-8ad3-d5ef322b978d
# MAGIC %md
# MAGIC
# MAGIC ## Python 셀 실행
# MAGIC
# MAGIC 다음 셀을 실행하여 **example_df` 데이터 프레임을 표시하여 **example-setup`** 노트북이 실행되었는지 확인하세요. 이 테이블은 증가하는 값의 16개 행으로 구성됩니다.

# COMMAND ----------

display(example_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6cb46bcc-9797-4782-931c-a7b8350146b2
# MAGIC %md
# MAGIC
# MAGIC # 언어 변경
# MAGIC
# MAGIC 이 노트북의 기본 언어가 Python으로 설정되어 있습니다. 노트북 이름 오른쪽에 있는 **Python** 버튼을 클릭하여 언어를 변경하세요. 기본 언어를 SQL로 변경하세요.
# MAGIC
# MAGIC Python 셀의 유효성을 유지하기 위해 셀 앞에 <strong><code>&#37;python</code></strong> 매직 명령어가 자동으로 추가됩니다. 이 작업을 수행하면 실행 상태도 초기화됩니다.

# COMMAND ----------

# DBTITLE 0,--i18n-478faa69-6814-4725-803b-3414a1a803ae
# MAGIC %md
# MAGIC
# MAGIC # 마크다운 셀 만들기
# MAGIC
# MAGIC 이 셀 아래에 새 셀을 추가합니다. 다음 요소 이상을 포함하는 마크다운으로 채웁니다.
# MAGIC * 헤더
# MAGIC * 글머리 기호
# MAGIC * 링크(HTML 또는 마크다운 규칙 사용)

# COMMAND ----------

# DBTITLE 0,--i18n-55b2a6c6-2fc6-4c57-8d6d-94bba244d86e
# MAGIC %md
# MAGIC
# MAGIC ## SQL 셀 실행
# MAGIC
# MAGIC SQL을 사용하여 Delta 테이블에 쿼리를 실행하려면 다음 셀을 실행하세요. 이 셀은 모든 DBFS 설치에 포함된 Databricks 제공 예제 데이터세트를 기반으로 하는 테이블에 대한 간단한 쿼리를 실행합니다.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.datasets}/nyctaxi-with-zipcodes/data`

# COMMAND ----------

# DBTITLE 0,--i18n-9993ed50-d8cf-4f37-bc76-6b18789447d6
# MAGIC %md
# MAGIC
# MAGIC 이 테이블을 뒷받침하는 기본 파일을 보려면 다음 셀을 실행하세요.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-c31a3318-a114-46e8-a744-18e8f8aa071e
# MAGIC %md
# MAGIC
# MAGIC # 노트북 상태 지우기
# MAGIC
# MAGIC 노트북에 정의된 모든 변수를 지우고 처음부터 다시 시작하는 것이 유용할 때가 있습니다. 이는 셀을 분리하여 테스트하거나 실행 상태를 재설정하려는 경우에 유용할 수 있습니다.
# MAGIC
# MAGIC **실행** 메뉴에서 **상태 및 출력 지우기**를 선택하세요.
# MAGIC
# MAGIC 이제 아래 셀을 실행해 보세요. 위의 이전 셀을 다시 실행하기 전까지는 이전에 정의된 변수가 더 이상 정의되지 않은 것을 확인할 수 있습니다.

# COMMAND ----------

print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-1e11bea0-7be9-4df7-be4e-b525c625dfee
# MAGIC %md
# MAGIC
# MAGIC # 변경 사항 검토
# MAGIC
# MAGIC Databricks Repo를 사용하여 이 자료를 작업 공간으로 가져왔다고 가정하고, 이 페이지 왼쪽 상단의 **`게시됨`** 브랜치 버튼을 클릭하여 Repo 대화 상자를 엽니다. 세 가지 변경 사항이 표시됩니다.
# MAGIC 1. 이전 노트북 이름으로 **제거됨**
# MAGIC 1. 새 노트북 이름으로 **추가됨**
# MAGIC 1. 위에 마크다운 셀을 생성하기 위해 **수정됨**
# MAGIC
# MAGIC 대화 상자를 사용하여 변경 사항을 되돌리고 이 노트북을 원래 상태로 복원합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-9947d429-2c10-4047-811f-3f5128527c6d
# MAGIC %md
# MAGIC
# MAGIC ## 마무리
# MAGIC
# MAGIC 이 실습을 완료하면 이제 노트북을 조작하고, 새로운 셀을 생성하고, 노트북 내에서 노트북을 실행하는 데 익숙해지셨을 것입니다.