import logging

from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

class YoutubeClient:

    def __init__(self, api_key: str):
        self.api_key = api_key
        
    def _service(self):
        return build("youtube", "v3", developerKey=self.api_key, static_discovery=False)
    
    def search(self, query: str, max_results: int = 10) -> dict:
        service = self._service()
        try:
            request = service.search().list(
                q=query,
                part="snippet",
                maxResults=max_results,
                type="video"
            )
            response = request.execute()
        except Exception as e:
            logger.error(f"An error occured while searching: {e}")
        else:
            return response
        finally:
            service.close()