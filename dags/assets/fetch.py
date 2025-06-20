import botocore.exceptions
import calendar
import dagster as dg
import json

from ccfc_yt.exceptions import QuotaExceededError
from dags.resources import s3Resource, YtResource
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# set a monthly partition definition at the beginning of the 24/25 season
monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

# helper function to return end of month date from start of month partition
def _get_end_of_month(first_day: datetime) -> datetime:
    last_day_num = calendar.monthrange(first_day.year, first_day.month)[1]
    return first_day.replace(day=last_day_num)

# helper function to safely load state file
def _load_state_file(s3: s3Resource, bucket: str, s3_key: str) -> dict:
    try:
        response = s3._client.get_object(Bucket=bucket, Key=s3_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        else:
            raise e

@dg.asset(partitions_def=monthly_partitions)
def fetch_videos(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    yt: YtResource
):
    """Fetches coventry city fc videos from the YouTube API with full pagination support"""
    # date partiton formatting
    date_partition = context.partition_key
    published_after = datetime.strptime(date_partition, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    published_before = _get_end_of_month(published_after)
    # setting request params
    optional_params = {
        "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishedBefore":published_before.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    query = "coventry city fc"
    # set s3 key prefix
    s3_key_prefix = f"videos/year={published_after.year}/month={published_after.month}"
    # load state file if exists, populate if it doesn't
    state = _load_state_file(s3, "bmooreawsbucket", s3_key_prefix+"/state.json")
    if not state:
        state = {
            "query": query,
            "partition": date_partition,
            "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "publishedBefore": published_before.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "nextPageToken": None
        }
    # begin fetch
    try:
        for page in yt._client.get_videos_search(
            query=query,
            optional_params=optional_params,
            stream=True,
            paginate=True,
            page_token=state.get("nextPageToken", None)
        ):
            items = page.get("items", [])
            # loop through video items and upload to s3
            for item in items:
                video_id = item.get("id", {}).get("videoId")
                s3_key = f"{s3_key_prefix}/video_id={video_id}.json"
                s3._client.put_object(
                    Bucket="bmooreawsbucket",
                    Body=json.dumps(item),
                    Key=s3_key,
                    ContentType="application/json"
                )
            # update state file with next page token
            next_page_token = page.get("nextPageToken")
            if next_page_token:
                state["nextPageToken"] = next_page_token
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