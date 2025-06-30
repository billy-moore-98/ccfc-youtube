class QuotaExceededError(Exception):
    """Custom exception raised when YouTube API quota is exceeded"""
    def __init__(self, message="Quota exceeded for today. Try again tomorrow."):
        super().__init__(message)
        self.message = message

class CommentsDisabledError(Exception):
    """Custom exception raised when a video has disabled comments"""
    def __init__(self, message="Video has disabled comments"):
        super().__init__(message)
        self.message = message