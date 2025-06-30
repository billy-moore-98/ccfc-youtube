import dagster as dg

from ccfc_yt.exceptions import QuotaExceededError
from dags.assets.fetch.utils import get_partition_info, handle_quota_exceeded, load_or_init_state
from dags.assets.fetch.videos.utils import fetch_and_store_videos, get_partition_bounds
from dags.resources import s3Resource, YtResource

monthly_partitions = dg.MonthlyPartitionsDefinition("2024-08-01")

@dg.asset(partitions_def=monthly_partitions)
def fetch_videos(
    context: dg.AssetExecutionContext,
    s3: s3Resource,
    yt: YtResource
):
    """Fetches Coventry City FC videos from the YouTube API with pagination and state tracking."""
    partition_key = context.partition_key
    context.log.info(f"Fetching videos for partition: {partition_key}")

    partition_dt, s3_prefix = get_partition_info(partition_key, "videos")
    published_after, published_before = get_partition_bounds(partition_dt)

    optional_params = {
        "publishedAfter": published_after.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishedBefore": published_before.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "order": "viewCount"
    }

    query = "coventry city fc"
    state = load_or_init_state(
        context=context,
        s3=s3,
        bucket="ccfcyoutube",
        s3_key_prefix=s3_prefix,
        default_state={
            "query": query,
            "partition": partition_key,
            "publishedAfter": optional_params["publishedAfter"],
            "publishedBefore": optional_params["publishedBefore"],
            "nextPageToken": None,
            "pagesFetched": 0
        }
    )

    try:
        fetch_and_store_videos(
            context=context,
            s3=s3,
            yt=yt,
            s3_key_prefix=s3_prefix,
            state=state,
            query=query
        )
    except QuotaExceededError as e:
        handle_quota_exceeded(context, e)
    except Exception as e:
        context.log.error(f"Unexpected error: {e}")
        raise
