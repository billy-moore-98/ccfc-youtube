import botocore.exceptions
import json

import dagster as dg

from ccfc_yt.exceptions import QuotaExceededError
from datetime import datetime, timedelta
from dags.resources import s3Resource
from typing import Dict
from zoneinfo import ZoneInfo

def get_partition_info(date_partition: str, asset_type: str):
    dt = datetime.strptime(date_partition, "%Y-%m-%d")
    s3_prefix = f"raw/{asset_type}/year={dt.year}/month={dt.month}"
    return dt, s3_prefix

# safely load or initialise state file
def load_or_init_state(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    bucket: str,
    s3_key_prefix: str,
    default_state: Dict
    ) -> Dict | None:
    state_key = f"{s3_key_prefix}/state.json"
    context.log.info(f"Loading state from {state_key} now")
    try:
        response = s3._client.get_object(Bucket=bucket, Key=state_key)
        state = json.loads(response["Body"].read().decode("utf-8"))
        return state
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            context.log.info("State not found, initialising")
            return default_state
        else:
            raise e

def save_state_file(s3: s3Resource, s3_prefix: str, state: dict):
    s3._client.put_object(
        Bucket="ccfcyoutube",
        Key=f"{s3_prefix}/state.json",
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json"
    )

def handle_quota_exceeded(context: dg.AssetExecutionContext, exception: QuotaExceededError):
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    midnight_pt = now_pt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    wait_seconds = int((midnight_pt - now_pt).total_seconds())
    context.log.warning("YouTube API quota exceeded, retrying after reset.")
    raise dg.RetryRequested(max_retries=1, seconds_to_wait=wait_seconds) from exception
