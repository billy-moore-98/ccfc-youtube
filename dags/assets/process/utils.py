import dagster as dg
import io
import pandas as pd

from ccfc_yt.processor import Processor
from dags.resources import s3Resource

def get_comment_keys_for_partition(s3: s3Resource, bucket: str, prefix: str) -> list[str]:
    response = s3._client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    return [obj["Key"] for obj in contents if obj["Key"].endswith("comments.jsonl")]

def read_and_process_comment(s3: s3Resource, bucket: str, key: str, processor: Processor) -> pd.DataFrame:
    response = s3._client.get_object(Bucket=bucket, Key=key)
    comment = response["Body"].read().decode("utf-8")
    return processor.run_comments_processing(comment)

def combine_processed_comments(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(dfs, ignore_index=True)

def upload_processed_comments(s3: s3Resource, bucket: str, key: str, df: pd.DataFrame):
    buffer = io.StringIO()
    df.to_json(buffer, orient="records", lines=True, force_ascii=False)
    buffer.seek(0)
    s3._client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue().encode("utf-8"))