import boto3
import dagster as dg

from ccfc_yt.client import YoutubeClient
from pydantic import PrivateAttr

class Yt(dg.ConfigurableResource):
    key: str
    _client: YoutubeClient = PrivateAttr() # Private attribute ignored by pydantic to hold the YoutubeClient instance

    def __init__(self, **data):
        super().__init__(**data)
        self._client = YoutubeClient(api_key=self.key)  # Create API client so we can access it in assets

class s3(dg.ConfigurableResource):
    access_key_id: str
    secret_access_key: str
    _client: boto3.client = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        self._client = boto3.client(
            's3',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )