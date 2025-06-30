import dagster as dg
import json

from ccfc_yt.exceptions import CommentsDisabledError, QuotaExceededError
from dags.assets.fetch.utils import append_comments_to_comments_file, get_videos_list, load_state_file
from dags.resources import s3Resource, YtResource
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# set a monthly partition definition at the beginning of the 24/25 season
monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

@dg.asset(
    partitions_def=monthly_partitions,
    deps=[dg.AssetKey("fetch_videos")]
)
def fetch_comments(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    yt: YtResource
):
    """Fetches comments posted on relevant coventry city fc videos"""
    # date partition formatting
    date_partition = context.partition_key
    context.log.info(f"Fetching comments for partition: {date_partition}")
    partition_date_obj = datetime.strptime(date_partition, "%Y-%m-%d")
    s3_partition_prefix = f"raw/comments/year={partition_date_obj.year}/month={partition_date_obj.month}"
    # setting request params
    optional_params = {"order": "relevance"}
    # pull list of video ids for partition
    videos = get_videos_list(s3, partition_date_obj)
    context.log.info(f"Beginning comments fetch for {len(videos)} videos")
    # loop through video ids
    try:
        for video in videos:
            s3_key_prefix = f"{s3_partition_prefix}/video_id={video}"
            # pull state file for video, initialise if not found
            context.log.info(f"Loading state now from : {s3_key_prefix}/state.json")
            state = load_state_file(s3, "ccfcyoutube", f"{s3_key_prefix}/state.json")
            if not state:
                context.log.info("State file not found, initializing now")
                state = {
                    "videoId": video,
                    "complete": False,
                    "pagesFetched": 0,
                    "nextPageToken": None
                }
            # go to next video if comment fetching already completed
            if state["complete"]:
                context.log.info("Comments fetch already completed, skipping")
                continue
            try:
                for page in yt._client.get_comments_thread(
                    video_id=video,
                    max_results=20,
                    optional_params=optional_params,
                    paginate=True,
                    stream=True,
                    page_token=state.get("nextPageToken", None),
                    max_pages=(5-state["pagesFetched"]) # 20 results * 5 pages = max 100 comments per video
                ):
                    if not page.get("items", []):
                        context.log.info(f"No comments for video id: {video}, continuing")
                        continue
                    context.log.info(f"Processing page with {len(page.get('items', []))} items for video id: {video}")
                    # process comment and append to comments.jsonl file at s3 key
                    context.log.info(f"Uploading {len(page['items'])} comments for video id: {video} to S3 at {s3_key_prefix}/comments.jsonl")
                    append_comments_to_comments_file(
                        s3=s3,
                        bucket="ccfcyoutube",
                        key=f"{s3_key_prefix}/comments.jsonl",
                        new_items=page.get("items", [])
                    )
                    # update state file with pages fetched
                    state["pagesFetched"] += 1
                    # update state file with next page token
                    next_page_token = page.get("nextPageToken", None)
                    state["nextPageToken"] = next_page_token
                    # update state file completion
                    if not next_page_token or state["pagesFetched"] >= 5:
                        state["complete"] = True
                    # update state file
                    context.log.info(f"Updating state file for video id: {video} at {s3_key_prefix}/state.json")
                    s3._client.put_object(
                        Bucket="ccfcyoutube",
                        Key=f"{s3_key_prefix}/state.json",
                        Body=json.dumps(state),
                        ContentType="application/json"
                    )
            except CommentsDisabledError as e:
                context.log.warning(f"Comments have been disabled for video with id {video}, skipping now")
                continue
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