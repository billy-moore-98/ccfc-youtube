import logging

from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

class YoutubeClient:

    def __init__(self, api_key: str):
        self.api_key = api_key
        
    def _service(self):
        return build("youtube", "v3", developerKey=self.api_key, static_discovery=False)
    
    def search(self, query: str, max_results: int = 10, **kwargs) -> dict:
        service = self._service()
        try:
            request = service.search().list(
                q=query,
                part="snippet",
                maxResults=max_results,
                type="video",
                **kwargs
            )
            response = request.execute()
        except Exception as e:
            logger.error(f"An error occured while searching: {e}")
            raise
        else:
            return response
        finally:
            service.close()

    def get_comments(self, video_id: str, max_results: int = 100, **kwargs) -> dict:
        service = self._service()
        try:
            request = service.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=max_results,
                **kwargs
            )
            response = request.execute()
        except Exception as e:
            logger.error(f"An error occurred while fetching comments: {e}")
            raise
        else:
            return response
        finally:
            service.close()