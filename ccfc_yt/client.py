import logging
import requests

from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

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
                    raise ValueError("Quota exceeded for today. Try again tomorrow.")
            else:
                raise
        except Exception as e:
            logger.error(f"Unknown error occurred: {e}")
            raise

    def _paginate(self, endpoint: str, params: dict) -> list[dict]:
        """Helper method to handle pagination"""
        items = []
        next_page_token = None
        params['key'] = self.api_key
        while True:
            logger.info(f"Paginating through results for endpoint: {endpoint}")
            if next_page_token:
                params['pageToken'] = next_page_token
            response = self._get_request(endpoint, params)
            logger.info(f"Response received, appending and checking for next page token")
            items.extend(response.get("items", []))
            next_page_token = response.get("nextPageToken")
            logger.info(f"Next page token: {next_page_token}")
            if not next_page_token:
                logger.info("No more pages to fetch, breaking loop")
                break
        logger.info(f"Total items fetched: {len(items)}")
        return items
    
    def get_videos_search(
        self,
        query: str,
        max_results: int = 50,
        paginate: bool = False,
        optional_params: dict = None
    ):
        """
        Conducts a youtube api search for videos based on a query string

        :param query: the search query string
        :param max_results: the maximum number of results to return (default 50)
        :param paginate: whether to paginate through all results (default False)
        :param optional_params: additional parameters to provide in the request
        
        :return: a list of search Resource items
        """
        endpoint = "search"
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": max_results
        }
        if optional_params:
            params.update(optional_params)
        if paginate:
            return self._paginate(endpoint, params)
        else:
            return self._get_request(endpoint, params).get("items", [])
        
    def get_comments_thread(
        self,
        video_id: str,
        max_results: int = 50,
        paginate: bool = False,
        optional_params: dict = None
    ) -> list[dict]:
        endpoint = "commentThreads"
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": max_results
        }
        if optional_params:
            params.update(optional_params)
        if paginate:
            return self._paginate(endpoint, params)
        else:
            return self._get_request(endpoint, params).get("items", [])