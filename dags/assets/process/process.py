import dagster as dg

from ccfc_yt.processor import Processor
from dags.assets.process.utils import (
    get_comment_keys_for_partition,
    read_and_process_comment,
    combine_processed_comments,
    upload_processed_comments
)
from dags.resources import s3Resource
from datetime import datetime

monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

@dg.asset(
    partitions_def=monthly_partitions,
    deps=[dg.AssetKey("fetch_comments")]
)
def process_comments(context: dg.AssetExecutionContext, s3: s3Resource):
    """Process and flatten the JSON comments responses for downstream sentiment analysis"""
    processor = Processor()
    s3_client = s3._client
    partition_key_dt = datetime.strptime(context.partition_key, "%Y-%m-%d")

    s3_prefix = f"raw/comments/year={partition_key_dt.year}/month={partition_key_dt.month}"
    context.log.info(f"Listing objects with prefix: {s3_prefix}")
    
    comment_keys = get_comment_keys_for_partition(s3_client, "ccfcyoutube", s3_prefix)
    context.log.info(f"Found {len(comment_keys)} comment files")

    dfs = []
    for key in comment_keys:
        context.log.info(f"Processing {key}")
        df = read_and_process_comment(s3_client, "ccfcyoutube", key, processor)
        dfs.append(df)

    context.log.info("Combining all comment DataFrames")
    combined_df = combine_processed_comments(dfs)

    dest_key = f"processed/year={partition_key_dt.year}/month={partition_key_dt.month}/comments.jsonl"
    context.log.info(f"Uploading processed comments to {dest_key}")
    upload_processed_comments(s3_client, "ccfcyoutube", dest_key, combined_df)