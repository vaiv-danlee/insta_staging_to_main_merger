# Instagram 데이터 BigQuery 테이블 병합 Cloud Function

## 프로젝트 목적

이 Cloud Function은 Google BigQuery에 저장된 Instagram 데이터의 스테이징 테이블에서 메인 테이블로 주기적으로 데이터를 병합(MERGE)하는 역할을 합니다. Post 데이터와 Profile 데이터를 각각 처리하며, 최신 데이터를 기준으로 업데이트합니다.

## 주요 기능

*   **Post 데이터 병합**: `POST_STAGING_TABLE_ID`의 데이터를 `POST_MAIN_TABLE_ID`로 병합합니다.
    *   고유 키: `media_id`
    *   파티션 필터 컬럼: `publish_date` (MERGE 시 ON 조건에 사용)
    *   최신 데이터 업데이트 기준 컬럼: `crawl_date`
*   **Profile 데이터 병합**: `PROFILE_STAGING_TABLE_ID`의 데이터를 `PROFILE_MAIN_TABLE_ID`로 병합합니다.
    *   고유 키: `user_id`
    *   최신 데이터 업데이트 기준 컬럼: `crawl_date`
*   **스테이징 테이블 중복 처리**: 병합 대상 스테이징 테이블에 고유 키 (및 파티션 키)가 동일한 레코드가 여러 개 있을 경우, `crawl_date`가 가장 최신인 레코드 하나만 선택하여 병합을 수행합니다. (중복으로 인한 MERGE 오류 방지)
*   **테이블 자동 생성**: 메인 테이블이 존재하지 않을 경우, 정의된 스키마에 따라 자동으로 생성합니다.
*   **스테이징 테이블 비우기**: 데이터 병합 성공 후, 해당 스테이징 테이블의 모든 데이터를 삭제(TRUNCATE)합니다.
*   **Cloud Logging**: 작업 진행 상황 및 오류를 Google Cloud Logging을 통해 기록합니다.
*   **HTTP 트리거**: Cloud Functions의 HTTP 트리거를 통해 실행됩니다.

## 환경 변수

Cloud Function 배포 시 다음 환경 변수를 설정해야 합니다.

*   `GCP_PROJECT`: Google Cloud 프로젝트 ID (기본값: `mmm-lab`)
*   `BQ_DATASET`: BigQuery 데이터셋 ID (기본값: `sns`)
*   `POST_STAGING_TABLE_ID`: Post 데이터 스테이징 테이블 ID (기본값: `insta_media_mmm_v2_staging`)
*   `POST_MAIN_TABLE_ID`: Post 데이터 메인 테이블 ID (기본값: `insta_media_mmm_v2`)
*   `PROFILE_STAGING_TABLE_ID`: Profile 데이터 스테이징 테이블 ID (기본값: `insta_profile_mmm_v2_staging`)
*   `PROFILE_MAIN_TABLE_ID`: Profile 데이터 메인 테이블 ID (기본값: `insta_profile_mmm_v2`)

## 실행 방법

1.  `main.py`의 코드를 Google Cloud Functions에 배포합니다.
    *   진입점(Entry point)은 `trigger_merge_tables`로 설정합니다.
    *   트리거 유형은 `HTTP`로 설정합니다.
2.  필요한 환경 변수를 설정합니다.
3.  (권장) Google Cloud Scheduler와 같은 스케줄링 서비스를 사용하여 배포된 Cloud Function의 HTTP 엔드포인트를 주기적으로 호출하도록 설정하여 자동 병합을 구성합니다.

## 스키마 정보

*   **Post 데이터 스키마**: `_get_bq_schema_for_post()` 함수에서 정의됩니다. 주요 필드는 `media_id`, `user_id`, `caption`, `publish_date`, `crawl_date` 등입니다.
*   **Profile 데이터 스키마**: `_get_bq_schema_for_profile()` 함수에서 정의됩니다. 주요 필드는 `user_id`, `username`, `biography`, `media_count`, `crawl_date` 등입니다.

## 핵심 로직

*   `merge_table(dataset_id, staging_table_id, main_table_id, unique_key_column, main_table_schema_fields, partition_filter_column=None, crawl_date_column=None)`:
    *   주어진 정보를 바탕으로 BigQuery MERGE SQL 문을 생성하고 실행합니다.
    *   **소스 데이터 중복 제거**: `crawl_date_column`이 제공된 경우, `USING` 절의 서브쿼리에서 `QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key_column [, partition_filter_column] ORDER BY crawl_date_column DESC) = 1` 조건을 사용하여 스테이징 테이블(Source)에서 각 고유 키별로 가장 최신의 `crawl_date`를 가진 행만 선택합니다. 이를 통해 "UPDATE/MERGE must match at most one source row for each target row" 오류를 방지합니다.
    *   `unique_key_column`을 기준으로 (중복 제거된) 스테이징 테이블(Source)과 메인 테이블(Target)을 조인합니다.
    *   `partition_filter_column`이 제공되면 MERGE 문의 `ON` 조건에 추가되어 특정 파티션에 대해서만 병합을 시도합니다.
    *   `crawl_date_column`이 제공되면 `WHEN MATCHED` 조건에 추가되어, 스테이징 테이블의 `crawl_date`가 메인 테이블의 `crawl_date`보다 최신일 경우에만 업데이트를 수행합니다.
    *   `WHEN NOT MATCHED THEN INSERT`를 통해 새로운 데이터를 메인 테이블에 삽입합니다.
    *   병합 성공 후 스테이징 테이블을 비웁니다.
*   `trigger_merge_tables(request)`:
    *   HTTP 요청을 받아 Post 데이터와 Profile 데이터에 대한 병합 작업을 순차적으로 호출합니다.
    *   모든 작업이 완료되면 성공 메시지와 HTTP 상태 코드 200을 반환합니다. 