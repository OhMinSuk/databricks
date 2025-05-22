# Databricks notebook source
# DBTITLE 0,--i18n-2d4e57a0-a2d5-4fad-80eb-98ff30d09a37
# MAGIC %md
# MAGIC
# MAGIC # 노트북 기본 사항
# MAGIC
# MAGIC 노트북은 Databricks에서 대화형으로 코드를 개발하고 실행하는 주요 수단입니다. 이 과정에서는 Databricks 노트북 작업에 대한 기본적인 소개를 제공합니다.
# MAGIC
# MAGIC 이전에 Databricks 노트북을 사용해 봤지만 Databricks Repos에서 노트북을 처음 실행하는 경우 기본 기능은 동일하다는 것을 알 수 있습니다. 다음 과정에서는 Databricks Repos가 노트북에 추가하는 몇 가지 기능을 살펴보겠습니다.
# MAGIC
# MAGIC ## 학습 목표
# MAGIC 이 과정을 마치면 다음을 수행할 수 있어야 합니다.
# MAGIC * 클러스터에 노트북 연결
# MAGIC * 노트북의 셀 실행
# MAGIC * 노트북 언어 설정
# MAGIC * 매직 명령 설명 및 사용
# MAGIC * SQL 셀 생성 및 실행
# MAGIC * Python 셀 생성 및 실행
# MAGIC * 마크다운 셀 생성
# MAGIC * Databricks 노트북 내보내기
# MAGIC * Databricks 노트북 모음 내보내기

# COMMAND ----------

# DBTITLE 0,--i18n-e7c4cc85-5ab7-46c1-9da3-f9181d77d118
# MAGIC %md
# MAGIC
# MAGIC ## 클러스터에 연결
# MAGIC
# MAGIC 이전 레슨에서 클러스터를 배포했거나 관리자가 사용하도록 구성한 클러스터를 확인했을 것입니다.
# MAGIC
# MAGIC 화면 오른쪽 상단의 클러스터 선택기("연결" 버튼)를 클릭하고 드롭다운 메뉴에서 클러스터를 선택합니다. 노트북이 클러스터에 연결되면 이 버튼에 클러스터 이름이 표시됩니다.
# MAGIC
# MAGIC **참고**: 클러스터 배포에는 몇 분 정도 걸릴 수 있습니다. 리소스가 배포되면 클러스터 이름 왼쪽에 녹색 원이 나타납니다. 클러스터 왼쪽에 빈 회색 원이 있는 경우, <a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">클러스터 시작</a> 지침을 따라야 합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-23aed87f-d375-4b81-8c3c-d4375cda0384
# MAGIC %md
# MAGIC
# MAGIC ## 노트북 기본 사항
# MAGIC
# MAGIC 노트북은 셀 단위 코드 실행을 제공합니다. 여러 언어를 하나의 노트북에 혼합하여 사용할 수 있습니다. 사용자는 플롯, 이미지, 마크다운 텍스트를 추가하여 코드를 더욱 풍부하게 만들 수 있습니다.
# MAGIC
# MAGIC 이 과정에서 노트북은 학습 도구로 설계되었습니다. 노트북은 Databricks를 사용하여 프로덕션 코드로 쉽게 배포할 수 있으며, 데이터 탐색, 보고 및 대시보드를 위한 강력한 도구 세트를 제공합니다.
# MAGIC
# MAGIC ### 셀 실행
# MAGIC * 다음 옵션 중 하나를 사용하여 아래 셀을 실행하세요.
# MAGIC * **CTRL+ENTER** 
# MAGIC * **SHIFT+ENTER** 를 사용하여 셀을 실행하고 다음 셀로 이동하세요.
# MAGIC

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# DBTITLE 0,--i18n-5b14b4c2-c009-4786-8058-a3ddb61fa41d
# MAGIC %md
# MAGIC
# MAGIC **참고**: 셀 ​​단위 코드 실행은 셀을 여러 번 또는 순서 없이 실행할 수 있음을 의미합니다. 명시적으로 지시되지 않는 한, 이 과정의 노트북은 한 번에 한 셀씩 위에서 아래로 실행되도록 설계되었다고 가정해야 합니다. 오류가 발생하면 문제 해결을 시도하기 전에 셀 앞뒤의 텍스트를 읽어 의도적인 학습 과정으로 인한 오류가 아닌지 확인하십시오. 대부분의 오류는 노트북에서 누락된 이전 셀을 실행하거나 전체 노트북을 처음부터 다시 실행하여 해결할 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-9be4ac54-8411-45a0-ad77-7173ec7402f8
# MAGIC %md
# MAGIC
# MAGIC ### 기본 노트북 언어 설정
# MAGIC
# MAGIC 현재 노트북의 기본 언어가 Python으로 설정되어 있으므로 위 셀은 Python 명령을 실행합니다.
# MAGIC
# MAGIC Databricks 노트북은 Python, SQL, Scala, R을 지원합니다. 노트북을 생성할 때 언어를 선택할 수 있지만, 언제든지 변경할 수 있습니다.
# MAGIC
# MAGIC 기본 언어는 페이지 상단의 노트북 제목 오른쪽에 표시됩니다. 이 과정에서는 SQL과 Python 노트북을 함께 사용합니다.
# MAGIC
# MAGIC 이 노트북의 기본 언어를 SQL로 변경합니다.
# MAGIC
# MAGIC 단계:
# MAGIC * 화면 상단의 노트북 제목 옆에 있는 **Python**을 클릭합니다.
# MAGIC * 팝업 UI의 드롭다운 목록에서 **SQL**을 선택합니다.
# MAGIC
# MAGIC **참고**: 이 셀 바로 앞의 셀에 <strong><code>&#37;python</code></strong>이 포함된 새 줄이 나타납니다. 이 부분에 대해서는 잠시 후에 설명하겠습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-3185e9b5-fcba-40aa-916b-5f3daa555cf5
# MAGIC %md
# MAGIC
# MAGIC ### SQL 셀 생성 및 실행
# MAGIC
# MAGIC * 이 셀 아래에서 **`%+코드`** " 를 눌러 아래에 새 셀을 만드세요.
# MAGIC * 아래 코드를 아래 셀에 복사한 후 셀을 실행하세요.
# MAGIC
# MAGIC **`%sql`**<br/>
# MAGIC **`SELECT "I'm running SQL!"`**
# MAGIC
# MAGIC **참고**: GUI 옵션 및 키보드 단축키를 포함하여 셀을 추가, 이동 및 삭제하는 다양한 방법이 있습니다. 자세한 내용은 <a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">문서</a>를 참조하세요.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "I'm running SQL!"

# COMMAND ----------

# DBTITLE 0,--i18n-5046f81c-cdbf-42c3-9b39-3be0721d837e
# MAGIC %md
# MAGIC
# MAGIC ## 매직 명령
# MAGIC * 매직 명령은 Databricks 노트북에만 적용됩니다.
# MAGIC * 유사한 노트북 제품에 있는 매직 명령과 매우 유사합니다.
# MAGIC * 노트북 언어와 관계없이 동일한 결과를 제공하는 기본 제공 명령입니다.
# MAGIC * 셀 시작 부분의 퍼센트(%) 기호는 매직 명령을 나타냅니다.
# MAGIC * 셀당 하나의 매직 명령만 사용할 수 있습니다.
# MAGIC * 매직 명령은 셀의 첫 번째 항목이어야 합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-39d2c50e-4b92-46ef-968c-f358114685be
# MAGIC %md
# MAGIC
# MAGIC ### 언어 매직
# MAGIC 언어 매직 명령을 사용하면 노트북의 기본 언어 이외의 언어로 코드를 실행할 수 있습니다. 이 과정에서는 다음과 같은 언어 매직을 살펴보겠습니다.
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC 현재 설정된 노트북 유형에 언어 매직을 추가할 필요는 없습니다.
# MAGIC
# MAGIC 위에서 노트북 언어를 Python에서 SQL로 변경했을 때, Python으로 작성된 기존 셀에는 <strong><code>&#37;python</code></strong> 명령이 추가되었습니다.
# MAGIC
# MAGIC **참고**: 노트북의 기본 언어를 계속 변경하는 대신, 기본 언어를 유지하고 다른 언어로 코드를 실행하는 데 필요한 경우에만 언어 매직을 사용해야 합니다.

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# DBTITLE 0,--i18n-94da1696-d0cf-418f-ba5a-d105a5ecdaac
# MAGIC %md
# MAGIC
# MAGIC ### 마크다운
# MAGIC
# MAGIC 마법의 명령어 **&percnt;md**를 사용하면 셀에 마크다운을 렌더링할 수 있습니다.
# MAGIC * 이 셀을 두 번 클릭하여 편집을 시작합니다.
# MAGIC * 그런 다음 **`Esc`**를 눌러 편집을 중지합니다.
# MAGIC
# MAGIC # 제목 1
# MAGIC ## 제목 2
# MAGIC ### 제목 3
# MAGIC
# MAGIC 이것은 비상 방송 시스템 테스트입니다. 테스트일 뿐입니다.
# MAGIC
# MAGIC 이 텍스트에는 **굵게** 표시된 단어가 있습니다.
# MAGIC
# MAGIC 이 텍스트에는 *기울임꼴* 단어가 있습니다.
# MAGIC
# MAGIC 순서 있는 목록입니다.
# MAGIC 1. 하나
# MAGIC 1. 둘
# MAGIC 1. 셋
# MAGIC
# MAGIC 순서 없는 목록입니다.
# MAGIC * 사과
# MAGIC * 복숭아
# MAGIC * 바나나
# MAGIC
# MAGIC 링크/임베디드 HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">마크다운 - 위키백과</a>
# MAGIC
# MAGIC 이미지:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC 물론 표도 있습니다.
# MAGIC
# MAGIC | 이름 | 값 |
# MAGIC |--------|-------|
# MAGIC | Yi | 1 |
# MAGIC | Ali | 2 |
# MAGIC | Selina | 3 |

# COMMAND ----------

# DBTITLE 0,--i18n-537e86bc-782f-4167-9899-edb3bd2b9e38
# MAGIC %md
# MAGIC
# MAGIC ### %run
# MAGIC * **%run** 명령어를 사용하여 다른 노트북에서 노트북을 실행할 수 있습니다.
# MAGIC * 실행할 노트북은 상대 경로로 지정됩니다.
# MAGIC * 참조된 노트북은 현재 노트북의 일부인 것처럼 실행되므로 호출하는 노트북에서 임시 뷰 및 기타 로컬 선언을 사용할 수 있습니다.

# COMMAND ----------

# DBTITLE 0,--i18n-d5c27671-b3c8-4b8c-a559-40cf7988f92f
# MAGIC %md
# MAGIC
# MAGIC 다음 셀의 주석 처리를 제거하고 실행하면 다음 오류가 발생합니다.<br/>
# MAGIC **`SQL 문 오류: AnalysisException: 테이블 또는 뷰를 찾을 수 없습니다: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-d0df4a17-abb4-42d3-ba37-c9a78f4fc9c0
# MAGIC %md
# MAGIC
# MAGIC 하지만 다음 셀을 실행하여 이 셀과 다른 몇 가지 변수 및 함수를 선언할 수 있습니다.

# COMMAND ----------

%run /Workspace/Users/1dt035@msacademy.msai.kr/Lab01/Includes/Classroom-Setup-01.2

# COMMAND ----------

# DBTITLE 0,--i18n-6a001755-5259-4fef-a5d3-2661d5301237
# MAGIC %md
# MAGIC
# MAGIC 참고한 **`../Includes/Classroom-Setup-01.2`** 노트북에는 스키마를 생성하고 **`USE`**하는 로직과 임시 뷰 **`demo_temp_vw`**를 생성하는 로직이 포함되어 있습니다.
# MAGIC
# MAGIC 다음 쿼리를 실행하면 현재 노트북 세션에서 이 임시 뷰를 사용할 수 있습니다.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-c28ecc03-8919-488f-bce7-e2fc0a451870
# MAGIC %md
# MAGIC
# MAGIC 이러한 "설정" 노트북 패턴을 수업 전반에 걸쳐 사용하여 수업 및 실습 환경을 구성합니다.
# MAGIC
# MAGIC 이러한 "제공된" 변수, 함수 및 기타 객체는 **DBAcademyHelper`**의 인스턴스인 **DA`** 객체의 일부이므로 쉽게 식별할 수 있습니다.
# MAGIC
# MAGIC 이를 염두에 두고 대부분의 수업에서는 사용자 이름에서 파생된 변수를 사용하여 파일과 스키마를 구성합니다.
# MAGIC
# MAGIC 이 패턴을 사용하면 공유 작업 공간에서 다른 사용자와의 충돌을 방지할 수 있습니다.
# MAGIC
# MAGIC 아래 셀은 Python을 사용하여 이 노트북의 설정 스크립트에 이전에 정의된 일부 변수를 출력합니다.

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

# DBTITLE 0,--i18n-1145175f-c51e-4cf5-a4a5-b0e3290a73a2
# MAGIC %md
# MAGIC
# MAGIC 이 외에도, 동일한 변수들이 SQL 문에서 사용할 수 있도록 SQL 컨텍스트에 "주입"됩니다.
# MAGIC
# MAGIC 이에 대해서는 나중에 더 자세히 설명하겠지만, 다음 셀에서 간단한 예를 볼 수 있습니다.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png"> 이 두 예에서 **`da`**와 **`DA`**라는 단어의 대소문자 표기에 미묘하지만 중요한 차이가 있음을 주목하세요.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.schema_name}' as schema_name

# COMMAND ----------

# DBTITLE 0,--i18n-8330ad3c-8d48-42fa-9b6f-72e818426ed4
# MAGIC %md
# MAGIC
# MAGIC ## Databricks Utilities
# MAGIC Databricks 노트북에는 환경 구성 및 상호 작용을 위한 다양한 유틸리티 명령을 제공하는 **`dbutils`** 객체가 포함되어 있습니다. <a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC
# MAGIC 이 과정에서는 Python 셀의 파일 디렉터리를 나열하기 위해 **`dbutils.fs.ls()`**를 가끔씩 사용합니다.

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

# DBTITLE 0,--i18n-25eb75f8-6e75-41a6-a304-d140844fa3e6
# MAGIC %md
# MAGIC
# MAGIC ## display()
# MAGIC
# MAGIC 셀에서 SQL 쿼리를 실행하면 결과는 항상 렌더링된 표 형식으로 표시됩니다.
# MAGIC
# MAGIC Python 셀에서 반환된 표 형식 데이터가 있는 경우, **display`**를 호출하여 동일한 유형의 미리보기를 얻을 수 있습니다.
# MAGIC
# MAGIC 여기서는 파일 시스템에서 이전 list 명령을 **display`**로 래핑합니다.

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-42b624ff-8cc9-4318-bedc-e3b340cb1b81
# MAGIC %md
# MAGIC
# MAGIC **`display()`** 명령은 다음과 같은 기능과 제한 사항을 가집니다.
# MAGIC * 결과 미리보기는 1000개 레코드로 제한됩니다.
# MAGIC * 결과 데이터를 CSV로 다운로드할 수 있는 버튼을 제공합니다.
# MAGIC * 플롯 렌더링을 허용합니다.

# COMMAND ----------

# DBTITLE 0,--i18n-c7f02dde-9b21-4edb-bded-5ce0eed56d03
# MAGIC %md
# MAGIC
# MAGIC ## 노트북 다운로드
# MAGIC
# MAGIC 개별 노트북 또는 노트북 모음을 다운로드하는 데는 여러 가지 옵션이 있습니다.
# MAGIC
# MAGIC 여기에서는 이 노트북과 이 과정의 모든 노트북 모음을 다운로드하는 과정을 안내합니다.
# MAGIC
# MAGIC ### 노트북 다운로드
# MAGIC
# MAGIC 단계:
# MAGIC * 노트북 왼쪽 상단에서 **파일** 옵션을 클릭합니다.
# MAGIC * 나타나는 메뉴에서 **내보내기** 위에 마우스를 올린 후 **소스 파일**을 선택합니다.
# MAGIC
# MAGIC 노트북이 개인 노트북에 다운로드됩니다. 현재 노트북 이름과 기본 언어의 파일 확장자가 지정됩니다. 모든 파일 편집기에서 이 노트북을 열어 Databricks 노트북의 원본 콘텐츠를 확인할 수 있습니다.
# MAGIC
# MAGIC 이러한 소스 파일은 모든 Databricks 작업 공간에 업로드할 수 있습니다.
# MAGIC
# MAGIC ### 노트북 모음 다운로드
# MAGIC
# MAGIC **참고**: 다음 지침은 **Repos**를 사용하여 이러한 자료를 가져온 것으로 가정합니다.
# MAGIC
# MAGIC 단계:
# MAGIC * 왼쪽 사이드바에서 ![](https://files.training.databricks.com/images/repos-icon.png) **Repos**를 클릭하세요.
# MAGIC * 이 노트북의 상위 디렉터리를 미리 볼 수 있습니다.
# MAGIC * 화면 중앙의 디렉터리 미리보기 왼쪽에 왼쪽 화살표가 있습니다. 파일 계층 구조에서 위로 이동하려면 이 화살표를 클릭하세요.
# MAGIC * **Data Engineer Learning Path**라는 디렉터리가 표시됩니다. 아래쪽 화살표/셰브론을 클릭하여 메뉴를 엽니다.
# MAGIC * 메뉴에서 **내보내기** 위에 마우스를 올리고 **DBC 아카이브**를 선택하세요.
# MAGIC
# MAGIC 다운로드된 DBC(Databricks Cloud) 파일에는 이 과정의 디렉터리와 노트북이 압축되어 포함되어 있습니다. 사용자는 이 DBC 파일을 로컬에서 편집해서는 안 되지만, 모든 Databricks 작업 공간에 안전하게 업로드하여 노트북 콘텐츠를 이동하거나 공유할 수 있습니다.
# MAGIC
# MAGIC **참고**: DBC 모음을 다운로드하면 결과 미리보기와 플롯도 함께 내보내집니다. 소스 노트북을 다운로드하면 코드만 저장됩니다.

# COMMAND ----------

# DBTITLE 0,--i18n-30e63e01-ca85-461a-b980-ea401904731f
# MAGIC %md
# MAGIC
# MAGIC ## Learning More
# MAGIC
# MAGIC Databricks 플랫폼과 노트북의 다양한 기능에 대해 자세히 알아보려면 설명서를 살펴보시기 바랍니다.
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Getting Started with Databricks</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">User Guide / Notebooks</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">Importing notebooks - Supported Formats</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">Administration Guide</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">Cluster Configuration</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">Release Notes</a>

# COMMAND ----------

# DBTITLE 0,--i18n-9987fd58-1023-4dbd-8319-40332f909181
# MAGIC %md
# MAGIC
# MAGIC ## One more note! 
# MAGIC
# MAGIC 각 레슨이 끝나면 **`DA.cleanup()`** 명령어가 표시됩니다.
# MAGIC
# MAGIC 이 메서드는 작업 공간을 깔끔하게 유지하고 각 레슨의 불변성을 유지하기 위해 레슨별 스키마와 작업 디렉터리를 삭제합니다.
# MAGIC
# MAGIC 이 레슨과 관련된 테이블과 파일을 삭제하려면 다음 셀을 실행하세요.

# COMMAND ----------

DA.cleanup()