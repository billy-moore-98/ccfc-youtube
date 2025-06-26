import dagster as dg
import io
import pandas as pd

from ccfc_yt.processor import Processor
from dags.resources import s3Resource
from datetime import datetime

monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

@dg.asset(
    partitions_def=monthly_partitions,
    deps=[dg.AssetKey("fetch_comments")]
)
def process_comments(context: dg.AssetExecutionContext, s3: s3Resource):
    processor = Processor()
    dfs = []
    partition_key_dt = datetime.strptime(context.partition_key, "%Y-%m-%d")
    # formulate key prefix for month partition
    s3_key_prefix = f"raw/comments/year={partition_key_dt.year}/month={partition_key_dt.month}"
    context.log.info(f"Processing comments for partition {partition_key_dt.date} now")
    context.log.info(f"s3 key prefix is {s3_key_prefix}, listing objects now")
    # list s3 objects for partition
    comment_objects = s3._client.list_objects_v2(Bucket="bmooreawsbucket", Prefix=s3_key_prefix)
    context.log.info(f"{len(comment_objects["Contents"])} objects found, looping through now")
    # loop through the listed objects, obtain key and process comments response
    for comment_object in comment_objects["Contents"]:
        comment_key: str = comment_object["Key"]
        if not comment_key.endswith('comments.jsonl'):
            continue
        context.log.info(f"Processing {comment_key} now")
        response = s3._client.get_object(Bucket="bmooreawsbucket", Key=comment_key)
        comment = response["Body"].read().decode("utf-8")
        comment_processed = processor.run_comments_processing(comment)
        context.log.info("Processed successfully")
        dfs.append(comment_processed)
    # combine dfs and write to a jsonl file for the month partition
    context.log.info("Combining comment dfs")
    all_comments = pd.concat(dfs, ignore_index=True)
    buffer = io.StringIO()
    all_comments.to_json(buffer, orient="records", lines=True, force_ascii=False)
    buffer.seek(0)
    destination_key = f"processed/year={partition_key_dt.year}/{partition_key_dt.month}/comments.jsonl"
    context.log.info("Uploading processed comments to s3")
    s3._client.put_object(
        Bucket="bmooreawsbucket",
        Key=destination_key,
        Body=buffer.getvalue().encode("utf-8")
    )