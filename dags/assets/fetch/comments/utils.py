import botocore.exceptions
import dagster as dg
import io
import json
import re

from ccfc_yt.exceptions import CommentsDisabledError
from dags.assets.fetch.utils import load_or_init_state, save_state_file
from dags.resources import s3Resource, YtResource
from datetime import datetime

# get list of video ids for relevant comments asset partition
def get_videos_list(s3: s3Resource, partition: datetime) -> list[str]:
    video_ids = []
    paginator = s3._client.get_paginator("list_objects_v2")
    s3_partition_prefix = f"raw/videos/year={partition.year}/month={partition.month}"
    for page in paginator.paginate(Bucket="ccfcyoutube", Prefix=s3_partition_prefix):
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
    

def fetch_and_store_comments_for_video(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    yt: YtResource,
    video_id: str,
    s3_prefix: str
):
    s3_key_prefix = f"{s3_prefix}/video_id={video_id}"
    state = load_or_init_state(
        context,
        s3,
        "ccfcyoutube",
        s3_key_prefix=s3_key_prefix,
        default_state={
            "videoId": video_id,
            "complete": False,
            "pagesFetched": 0,
            "nextPageToken": None
        }
    )
    context.log.info(f"State is: {state}")
    if state["complete"]:
        context.log.info(f"Comments already fetched for video {video_id}, skipping")
        return
    try:
        for page in yt._client.get_comments_thread(
            video_id=video_id,
            max_results=20,
            optional_params={"order": "relevance"},
            paginate=True,
            stream=True,
            page_token=state.get("nextPageToken"),
            max_pages=(5 - state["pagesFetched"])
        ):
            comments = page.get("items", [])
            if not comments:
                context.log.info(f"No comments found for video {video_id}, continuing.")
                continue

            context.log.info(f"Uploading {len(comments)} comments for video {video_id}")
            append_comments_to_comments_file(
                s3=s3,
                bucket="ccfcyoutube",
                key=f"{s3_key_prefix}/comments.jsonl",
                new_items=comments
            )

            state["pagesFetched"] += 1
            state["nextPageToken"] = page.get("nextPageToken")

            if not state["nextPageToken"] or state["pagesFetched"] >= 5:
                state["complete"] = True

            save_state_file(s3, s3_key_prefix, state)

    except CommentsDisabledError:
        context.log.warning(f"Comments disabled for video {video_id}, skipping.")