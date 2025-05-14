import os
import logging
import google.cloud.logging # Google Cloud Logging 클라이언트
from google.cloud import bigquery
import functions_framework # 추가됨

# --- Cloud Logging 설정 ---
try:
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging(log_level=logging.INFO)
    logging.info("Cloud Logging client successfully set up.")
except Exception as e:
    logging.basicConfig(level=logging.INFO)
    logging.warning(f"Could not set up Cloud Logging client: {e}. Falling back to basicConfig.")
# --- 로깅 설정 끝 ---

GCP_PROJECT = os.environ.get("GCP_PROJECT", "mmm-lab")
BQ_DATASET = os.environ.get("BQ_DATASET", "sns")

POST_STAGING_TABLE_ID = os.environ.get("POST_STAGING_TABLE_ID", "insta_media_mmm_v2_staging")
POST_MAIN_TABLE_ID = os.environ.get("POST_MAIN_TABLE_ID", "insta_media_mmm_v2")

PROFILE_STAGING_TABLE_ID = os.environ.get("PROFILE_STAGING_TABLE_ID", "insta_profile_mmm_v2_staging")
PROFILE_MAIN_TABLE_ID = os.environ.get("PROFILE_MAIN_TABLE_ID", "insta_profile_mmm_v2")

bigquery_client = bigquery.Client(project=GCP_PROJECT if GCP_PROJECT else None)


# --- insta_data_router에서 가져온 함수들 ---
def _get_bq_schema_for_post():
    """Post 데이터의 BigQuery 스키마를 반환합니다."""
    return [
        bigquery.SchemaField("media_id", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("caption", "STRING"),
        bigquery.SchemaField("hashtags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("comment_count", "INTEGER"),
        bigquery.SchemaField("like_count", "INTEGER"),
        bigquery.SchemaField("is_video", "BOOLEAN"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("display_src", "STRING"),
        bigquery.SchemaField("publish_date", "TIMESTAMP"),
        bigquery.SchemaField("publish_short_date", "STRING"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP"),
        bigquery.SchemaField("p_id", "STRING"),
        bigquery.SchemaField("like_and_view_counts_disabled", "BOOLEAN"),
        bigquery.SchemaField("tagged_accounts", "STRING", mode="REPEATED"),
        bigquery.SchemaField("tagged_account_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("coauthor_names", "STRING", mode="REPEATED"),
        bigquery.SchemaField("coauthor_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("video_url", "STRING"),
        bigquery.SchemaField("video_view_count", "INTEGER"),
        bigquery.SchemaField("img_count", "INTEGER"),
        bigquery.SchemaField("is_paid_partnership", "BOOLEAN"),
        bigquery.SchemaField("sponsored_by", "STRING"),
    ]

def _get_bq_schema_for_profile():
    """Profile 데이터의 BigQuery 스키마를 반환합니다. (media_timeline 제외)"""
    return [
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("biography", "STRING"),
        bigquery.SchemaField("bio_hashtags", "STRING", mode="REPEATED"),
        bigquery.SchemaField("category_name", "STRING"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("external_url", "STRING"),
        bigquery.SchemaField("followed_by", "INTEGER"),
        bigquery.SchemaField("follows", "INTEGER"),
        bigquery.SchemaField("media_count", "INTEGER"),
        bigquery.SchemaField("profile_pic_url", "STRING"),
        bigquery.SchemaField("related_accounts", "STRING", mode="REPEATED"),
        bigquery.SchemaField("crawl_date", "TIMESTAMP"),
        bigquery.SchemaField("account_status", "STRING"),
    ]

def _ensure_table_exists(table_ref, schema):
    """테이블이 없으면 지정된 스키마로 생성합니다."""
    try:
        bigquery_client.get_table(table_ref)
        logging.info(f"Table {table_ref.path} already exists.")
    except Exception: # google.cloud.exceptions.NotFound
        table_id_str = f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}"
        logging.info(f"Table {table_id_str} not found, creating table...")
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        logging.info(f"Table {table_id_str} created with specified schema.")
# --- 가져온 함수들 끝 ---


def merge_table(dataset_id, staging_table_id, main_table_id, unique_key_column, main_table_schema_fields, partition_filter_column=None, crawl_date_column=None):
    """
    스테이징 테이블의 데이터를 메인 테이블로 MERGE 합니다.
    메인 테이블이 없으면 main_table_schema_fields를 사용하여 생성합니다.
    partition_filter_column이 제공되면 MERGE 문의 ON 조건에 추가됩니다.
    crawl_date_column이 제공되면 WHEN MATCHED 조건에 추가되어 최신 데이터만 업데이트합니다.
    """
    main_table_ref_obj = bigquery_client.dataset(dataset_id).table(main_table_id)
    
    # 메인 테이블 존재 확인 및 생성
    _ensure_table_exists(main_table_ref_obj, main_table_schema_fields)

    staging_table_full_id = f"`{GCP_PROJECT}.{dataset_id}.{staging_table_id}`"
    main_table_full_id = f"`{GCP_PROJECT}.{dataset_id}.{main_table_id}`"

    # MERGE 문에 사용될 컬럼 목록 (스키마 필드에서 직접 가져옴)
    main_table_columns = [field.name for field in main_table_schema_fields]

    update_set_statements = []
    for col_name in main_table_columns:
        if col_name != unique_key_column: # 고유 키는 업데이트 대상에서 제외
             # 컬럼 이름에 예약어나 특수문자가 포함될 수 있으므로 백틱으로 감쌉니다.
            update_set_statements.append(f"Target.`{col_name}` = Source.`{col_name}`")
    
    update_set_clause = ", ".join(update_set_statements)
    
    # INSERT 절에 사용될 컬럼명과 VALUES에 사용될 컬럼명 (Source에서 가져옴)
    # 컬럼 이름에 예약어나 특수문자가 포함될 수 있으므로 백틱으로 감쌉니다.
    insert_columns_clause = ", ".join([f"`{col}`" for col in main_table_columns])
    source_columns_clause = ", ".join([f"Source.`{col}`" for col in main_table_columns])

    # 기본 ON 조건
    on_condition = f"Target.`{unique_key_column}` = Source.`{unique_key_column}`"

    # partition_filter_column이 제공되면 ON 조건에 추가
    if partition_filter_column:
        on_condition += f" AND Target.`{partition_filter_column}` = Source.`{partition_filter_column}`"

    # WHEN MATCHED 조건 (crawl_date_column이 제공된 경우)
    when_matched_condition = ""
    if crawl_date_column:
        when_matched_condition = f" AND Source.`{crawl_date_column}` > Target.`{crawl_date_column}`"

    # 소스 테이블 표현식: crawl_date_column이 제공되면 중복 제거
    source_table_expression = f"`{GCP_PROJECT}.{dataset_id}.{staging_table_id}`"
    if crawl_date_column:
        partition_by_cols = [f"`{unique_key_column}`"]
        if partition_filter_column:
            partition_by_cols.append(f"`{partition_filter_column}`")
        
        partition_by_clause = ", ".join(partition_by_cols)
        
        source_table_expression = f"""(
      SELECT *
      FROM `{GCP_PROJECT}.{dataset_id}.{staging_table_id}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY {partition_by_clause} ORDER BY `{crawl_date_column}` DESC) = 1
    )"""

    merge_sql = f"""
    MERGE {main_table_full_id} AS Target
    USING {source_table_expression} AS Source
    ON {on_condition}
    WHEN MATCHED{when_matched_condition} THEN
      UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns_clause})
      VALUES ({source_columns_clause})
    """

    logging.info(f"Executing MERGE statement for {main_table_full_id} from {staging_table_id} on `{unique_key_column}` (deduplicated by {crawl_date_column if crawl_date_column else 'N/A'})")
    try:
        query_job = bigquery_client.query(merge_sql)
        query_job.result()
        logging.info(f"Successfully merged data from {staging_table_id} to {main_table_id}.")

        # (선택 사항) MERGE 성공 후 스테이징 테이블 비우기
        logging.info(f"Truncating staging table {staging_table_full_id}...")
        truncate_sql = f"TRUNCATE TABLE {staging_table_full_id}"
        truncate_job = bigquery_client.query(truncate_sql)
        truncate_job.result()
        logging.info(f"Successfully truncated staging table {staging_table_full_id}.")

    except Exception as e:
        logging.exception(f"Error during MERGE operation for {main_table_id}:")


# @functions_framework.cloud_event # HTTP 트리거로 변경하므로 이 데코레이터 제거
@functions_framework.http # HTTP 트리거를 위한 데코레이터 추가
def trigger_merge_tables(request):
    """
    HTTP 요청을 받아 테이블 병합을 트리거합니다.
    Cloud Scheduler에 의해 호출되는 것을 가정합니다.
    """
    # HTTP 요청 정보 로깅 (선택 사항)
    # logging.info(f"Received HTTP request: Method={request.method}, Headers={request.headers}, Body={request.data}")
    logging.info(f"Received HTTP request to trigger merge.")

    # Post 데이터 병합
    post_schema = _get_bq_schema_for_post()
    logging.info(f"Starting merge for POST data ({POST_STAGING_TABLE_ID} -> {POST_MAIN_TABLE_ID}).")
    # Post 테이블의 고유 키는 'media_id'로, 파티션 키는 'publish_date'로 가정합니다.
    merge_table(BQ_DATASET, POST_STAGING_TABLE_ID, POST_MAIN_TABLE_ID, "media_id", post_schema, partition_filter_column="publish_date", crawl_date_column="crawl_date")

    # Profile 데이터 병합
    profile_schema = _get_bq_schema_for_profile()
    logging.info(f"Starting merge for PROFILE data ({PROFILE_STAGING_TABLE_ID} -> {PROFILE_MAIN_TABLE_ID}).")
    # Profile 테이블의 고유 키는 'user_id'로 가정합니다. (파티션 필터 컬럼 없음)
    merge_table(BQ_DATASET, PROFILE_STAGING_TABLE_ID, PROFILE_MAIN_TABLE_ID, "user_id", profile_schema, crawl_date_column="crawl_date")

    logging.info("All merge operations completed.")
    return ("Merge process successfully triggered.", 200) # HTTP 응답 추가