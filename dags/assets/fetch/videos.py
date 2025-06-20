import dagster as dg
import json

from ccfc_yt.exceptions import QuotaExceededError
from dags.assets.fetch.utils import get_end_of_month, load_state_file
from dags.resources import s3Resource, YtResource
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# set a monthly partition definition at the beginning of the 24/25 season
monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

@dg.asset(partitions_def=monthly_partitions)
def fetch_videos(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    yt: YtResource
):
    """Fetches coventry city fc videos from the YouTube API with full pagination support"""
    # date partiton formatting
    date_partition = context.partition_key
    context.log.info(f"Fetching videos for partition: {date_partition}")
    published_after = datetime.strptime(date_partition, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    published_before = get_end_of_month(published_after)
    # setting request params
    optional_params = {
        "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishedBefore":published_before.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "order": "viewCount"
    }
    query = "coventry city fc"
    # set s3 key prefix
    s3_key_prefix = f"videos/year={published_after.year}/month={published_after.month}"
    # load state file if exists, populate if it doesn't
    context.log.info(f"Loading state now from : {s3_key_prefix}/state.json")
    state = load_state_file(s3, "bmooreawsbucket", s3_key_prefix+"/state.json")
    if not state:
        context.log.info("State file not found, initializing now")
        state = {
            "query": query,
            "partition": date_partition,
            "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "publishedBefore": published_before.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "nextPageToken": None,
            "pagesFetched": 5
        }
    # begin fetch for maximum of 5 pages for project deployment
    # in production can expand this for all videos if necessary
    context.log.info("Beginning video fetch")
    try:
        for page in yt._client.get_videos_search(
            query=query,
            max_results=10, # limit number of results for project deployment
            optional_params=optional_params,
            stream=True,
            paginate=True,
            page_token=state.get("nextPageToken", None),
            max_pages=5 # 10 results * 5 pages returns 50 videos, a good amount per month
        ):
            context.log.info(f"Processing page with {len(page.get('items', []))} items")
            items = page.get("items", [])
            # loop through video items and upload to s3
            for item in items:
                video_id = item.get("id", {}).get("videoId")
                s3_key = f"{s3_key_prefix}/video_id={video_id}.json"
                context.log.info(f"Uploading video item with ID: {video_id} to S3 at {s3_key}")
                s3._client.put_object(
                    Bucket="bmooreawsbucket",
                    Body=json.dumps(item),
                    Key=s3_key,
                    ContentType="application/json"
                )
            # update state file with pages fetched
            state["pagesFetched"] += 1
            # update state file with next page token
            next_page_token = page.get("nextPageToken", None)
            state["nextPageToken"] = next_page_token
            # update state file
            s3._client.put_object(
                Bucket="bmooreawsbucket",
                Body=json.dumps(state),
                Key=f"{s3_key_prefix}/state.json",
                ContentType="application/json"
            )
    except QuotaExceededError as e:
        # YouTube Data API quota resets at midnight PT
        # Schedule a retry for this time
        context.log.warning("YouTube API quota exceeded, scheduling retry for tomorrow")
        now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
        midnight_pt = now_pt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        seconds_to_wait = int((midnight_pt - now_pt).total_seconds())
        raise dg.RetryRequested(max_retries=1, seconds_to_wait=seconds_to_wait) from e
    except Exception as e:
        context.log.error(f"An unexpected error occured: {e}")
        raise
