import botocore.exceptions
import calendar
import io
import json
import re

from datetime import datetime
from dags.resources import s3Resource

# return end of month date from start of month partition
def get_end_of_month(first_day: datetime) -> datetime:
    last_day_num = calendar.monthrange(first_day.year, first_day.month)[1]
    return first_day.replace(day=last_day_num)

# safely load state file
def load_state_file(s3: s3Resource, bucket: str, s3_key: str) -> dict:
    try:
        response = s3._client.get_object(Bucket=bucket, Key=s3_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        else:
            raise e
        
# get list of video ids for relevant comments asset partition
def get_videos_list(s3: s3Resource, partition: datetime) -> list[str]:
    video_ids = []
    paginator = s3._client.get_paginator("list_objects_v2")
    s3_partition_prefix = f"raw/videos/year={partition.year}/month={partition.month}"
    for page in paginator.paginate(Bucket="bmooreawsbucket", Prefix=s3_partition_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = re.search(r"video_id=(.+?)\.json$", key)
            if match:
                video_ids.append(match.group(1))
    return video_ids

# append new comments objects to the comments file stored on s3
def append_comments_to_comments_file(s3: s3Resource, bucket: str, key: str, new_items: list[dict]):
    # get object if it exists, if not return empty
    try:
        response = s3._client.get_object(Bucket=bucket, Key=key)
        obj = response["Body"].read().decode("utf-8")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            obj = ""
        else:
            raise
    # write items to string buffer
    str_buffer = io.StringIO()
    if obj:
        str_buffer.write(obj)
        if not obj.endswith("\n"):
            str_buffer.write("\n")
    for item in new_items:
        str_buffer.write(json.dumps(item) + "\n")
    # reupload final object
    s3._client.put_object(Bucket=bucket, Key=key, Body=str_buffer.getvalue(), ContentType="application/json")
    
