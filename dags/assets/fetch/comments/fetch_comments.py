import dagster as dg
from datetime import datetime

from ccfc_yt.exceptions import QuotaExceededError
from dags.resources import s3Resource, YtResource
from dags.assets.fetch.comments.utils import fetch_and_store_comments_for_video, get_videos_list
from dags.assets.fetch.utils import get_partition_info, handle_quota_exceeded

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
    """Fetches comments posted on Coventry City FC videos."""
    partition_key = context.partition_key
    context.log.info(f"Fetching comments for partition: {partition_key}")

    partition_dt, s3_prefix = get_partition_info(partition_key, "comments")
    videos = get_videos_list(s3, partition_dt)

    context.log.info(f"Found {len(videos)} videos to fetch comments for.")

    try:
        for video_id in videos:
            fetch_and_store_comments_for_video(
                context=context,
                s3=s3,
                yt=yt,
                video_id=video_id,
                s3_key_prefix=s3_prefix
            )
    except QuotaExceededError as e:
        handle_quota_exceeded(context, e)
    except Exception as e:
        context.log.error(f"Unexpected error: {e}")
        raise
