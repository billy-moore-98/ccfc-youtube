import botocore.exceptions
import calendar
import json
import re

from datetime import datetime
from dags.resources import s3Resource

# helper function to return end of month date from start of month partition
def get_end_of_month(first_day: datetime) -> datetime:
    last_day_num = calendar.monthrange(first_day.year, first_day.month)[1]
    return first_day.replace(day=last_day_num)

# helper function to safely load state file
def load_state_file(s3: s3Resource, bucket: str, s3_key: str) -> dict:
    try:
        response = s3._client.get_object(Bucket=bucket, Key=s3_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        else:
            raise e

def get_videos_list(s3: s3Resource, partition: datetime) -> list[str]:
    video_ids = []
    paginator = s3._client.get_paginator("list_objects_v2")
    s3_partition_prefix = f"comments/year={partition.year}/month={partition.month}"
    for page in paginator.paginate(Bucket="bmooreawsbucket", Prefix=s3_partition_prefix):
        for obj in page.get("Contents", []):
            key = obj["key"]
            match = re.match(r"video_id=(.+?)\.json$", key)
            if match:
                video_ids.append(match.group(1))
    return video_ids
