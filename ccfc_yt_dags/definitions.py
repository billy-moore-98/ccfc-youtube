import os

from dagster import Definitions, load_assets_from_modules
from dotenv import load_dotenv

from ccfc_yt_dags import assets
from ccfc_yt_dags.resources import Yt

load_dotenv()

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"yt": Yt(key=os.getenv("YOUTUBE_API_KEY"))}
)
