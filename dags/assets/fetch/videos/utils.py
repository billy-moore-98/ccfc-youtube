import calendar
import dagster as dg
import json

from dags.assets.fetch.utils import save_state_file
from dags.resources import s3Resource, YtResource
from datetime import datetime, timezone
from typing import Tuple

def get_partition_bounds(published_after: datetime) -> Tuple[datetime, datetime]:
    last_day_num = calendar.monthrange(published_after.year, published_after.month)[1]
    published_before = published_after.replace(day=last_day_num)
    return published_after, published_before

def upload_video_to_s3(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    s3_key_prefix: str,
    item: dict
):
    video_id = item.get("id", {}).get("videoId")
    if not video_id:
        return
    s3_key = f"{s3_key_prefix}/video_id={video_id}.json"
    context.log.info(f"Uploading video {video_id} to {s3_key}")
    s3._client.put_object(
        Bucket="ccfcyoutube",
        Body=json.dumps(item),
        Key=s3_key,
        ContentType="application/json"
    )

def process_page(
    context:dg.AssetExecutionContext,
    s3: s3Resource,
    s3_key_prefix: str,
    page: dict,
    state: dict
):
    items = page.get("items", [])
    context.log.info("Processing page with {len(items)} items")
    for item in items:
        upload_video_to_s3(context, s3, s3_key_prefix, item)
    state["pagesFetched"] += 1
    state["nextPageToken"] = page.get("nextPageToken")
    save_state_file(s3, s3_key_prefix, state)


def fetch_and_store_videos(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    yt: YtResource,
    s3_key_prefix: str,
    state: dict,
    query: str
):
    optional_params = {
        "publishedAfter": state["publishedAfter"],
        "publishedBefore": state["publishedBefore"],
        "order": "viewCount"
    }
    for page in yt._client.get_videos_search(
        query=query,
        max_results=20,
        optional_params=optional_params,
        stream=True,
        paginate=True,
        page_token=state.get("nextPageToken"),
        max_pages=(5 - state["pagesFetched"])
    ):
        process_page(context, s3, s3_key_prefix, page, state)

