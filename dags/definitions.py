import os

from dagster import Definitions, load_assets_from_modules
from dotenv import load_dotenv

from dags.assets import process
from dags.assets.fetch import comments, videos
from dags.jobs import process_comments_job
from dags.resources import s3Resource, YtResource
from dags.sensors import fetch_comments_sensor

load_dotenv()

all_assets = load_assets_from_modules([comments, process, videos])

defs = Definitions(
    assets=all_assets,
    resources={
        "yt": YtResource(key=os.getenv("YOUTUBE_API_KEY")),
        "s3": s3Resource(
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    },
    jobs=[process_comments_job],
    sensors=[fetch_comments_sensor]
)
