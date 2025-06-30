import os

from dagster import Definitions, load_assets_from_modules
from dotenv import load_dotenv

from dags.assets.fetch.comments import fetch_comments
from dags.assets.fetch.videos import fetch_videos
from dags.assets.infer import infer
from dags.assets.process import process
from dags.jobs import comments_sentiment_analysis
from dags.resources import s3Resource, YtResource
from dags.schedules import monthly_schedule

load_dotenv()

all_assets = load_assets_from_modules([fetch_comments, infer, process, fetch_videos])

defs = Definitions(
    assets=all_assets,
    resources={
        "yt": YtResource(key=os.getenv("YOUTUBE_API_KEY")),
        "s3": s3Resource(
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    },
    jobs=[comments_sentiment_analysis],
    schedules=[monthly_schedule]
)
