import os

from dagster import Definitions, load_assets_from_modules
from dotenv import load_dotenv

from ccfc_yt_dags import assets
from ccfc_yt_dags.resources import s3, Yt

load_dotenv()

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "yt": Yt(key=os.getenv("YOUTUBE_API_KEY")),
        "s3": s3(
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    }
)
