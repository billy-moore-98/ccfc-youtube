import dagster as dg

from ccfc_yt.client import YoutubeClient
from pydantic import PrivateAttr

class Yt(dg.ConfigurableResource):
    key: str
    _client: YoutubeClient = PrivateAttr() # Private attribute ignored by pydantic to hold the YoutubeClient instance 