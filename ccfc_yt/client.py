import logging
import requests

from ccfc_yt.exceptions import QuotaExceededError
from requests.adapters import HTTPAdapter
from typing import Generator, Union
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)

# Class built using bare requests module instead of google-api-python-client for maximum control and testability

class YoutubeClient:
    BASE_URL = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key: str):
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("API key must be provided")
        self.session = self._create_session()

    def _create_session(
            self,
            total_retries: int = 3,
            backoff_factor: float = 0.5,
            status_forcelist: list = [429, 500, 502, 503, 504]
    ) -> requests.Session:
        """Return a requests.Session object with a custom retry strategy for certain status codes"""
        session = requests.Session()
        retry_strategy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        http_adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", http_adapter)
        return session
    
    def _get_request(self, endpoint: str, params: dict = None) -> dict:
        """Helper method to make GET requests to the YouTube API"""
        url = f"{self.BASE_URL}/{endpoint}"
        params["key"] = self.api_key
        try:
            logger.info(f"Making get request to endpoint: {endpoint} now")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            status_code = response.status_code
            error_json = response.json()
            if status_code == 403:
                reason = error_json.get("error", {}).get("errors", [{}])[0].get("reason")
                if reason == "quotaExceeded":
                    raise QuotaExceededError
            else:
                raise
        except Exception as e:
            logger.error(f"Unknown error occurred: {e}")
            raise

    def _paginate(self, endpoint: str, params: dict) -> list[dict]:
        """Helper method to handle full pagination"""
        items = []
        next_page_token = None
        while True:
            logger.info(f"Paginating through results for endpoint: {endpoint}")
            if next_page_token:
                params['pageToken'] = next_page_token
            response = self._get_request(endpoint, params)
            logger.info(f"Response received, appending and checking for next page token")
            items.extend(response)
            next_page_token = response.get("nextPageToken")
            logger.info(f"Next page token: {next_page_token}")
            if not next_page_token:
                logger.info("No more pages to fetch, breaking loop")
                break
        logger.info(f"Total items fetched: {len(items)}")
        return items
    
    def _stream_paginate(self, endpoint: str, params: dict, page_token: str = None) -> Generator[dict, None, None]:
        """Helper method to stream results from pagination"""
        next_page_token = page_token
        while True:
            logger.info(f"Streaming results for endpoint: {endpoint}")
            if next_page_token:
                params['pageToken'] = next_page_token
            response = self._get_request(endpoint, params)
            logger.info(f"Response received, yielding items")
            yield response
            next_page_token = response.get("nextPageToken")
            logger.info(f"Next page token: {next_page_token}")
            if not next_page_token:
                logger.info("No more pages to fetch, breaking loop")
                break
    
    def get_videos_search(
        self,
        query: str,
        max_results: int = 50,
        optional_params: dict = None,
        paginate: bool = False,
        stream: bool = False,
        page_token: str = None
    ) -> Union[dict, list[dict], Generator[dict, None, None]]:
        """
        Conducts a youtube api search for videos based on a query string
        Several methods of pagination are supported

        :param query: the search query string
        :param max_results: the maximum number of results to return (default 50)
        :param optional_params: additional parameters to provide in the request
        :param paginate: whether to paginate through all results (default False)
        :param stream: whether to stream pages and act as a generator (default False)
        
        :return: a list of search Resource items
        """
        if stream and not paginate:
            raise ValueError("Cannot stream results without pagination")
        
        endpoint = "search"
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": max_results
        }
        if optional_params:
            params.update(optional_params)
        if paginate and stream:
            return self._stream_paginate(endpoint, params, page_token=page_token)
        elif paginate:
            return self._paginate(endpoint, params)
        else:
            return self._get_request(endpoint, params)
        
    def get_comments_thread(
        self,
        video_id: str,
        max_results: int = 50,
        optional_params: dict = None,
        paginate: bool = False,
        stream: bool = False,
        page_token: str = None
    ) -> Union[dict, list[dict], Generator[dict, None, None]]:
        """
        Fetches the comments thread for a given video ID

        :param video_id: the ID of the video to fetch comments for
        :param max_results: the maximum number of results to return (default 50)
        :param optional_params: additional parameters to provide in the request
        :param paginate: whether to paginate through all results (default False)
        :param stream: whether to stream pages and act as a generator (default False)

        :return: a list of comment thread Resource items
        """
        if stream and not paginate:
            raise ValueError("Cannot stream results without pagination")
        
        endpoint = "commentThreads"
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max_results
        }
        if optional_params:
            params.update(optional_params)
        if paginate and stream:
            return self._stream_paginate(endpoint, params, page_token=page_token)
        elif paginate:
            return self._paginate(endpoint, params)
        else:
            return self._get_request(endpoint, params)