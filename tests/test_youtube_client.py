import pytest

from ccfc_yt.client import YoutubeClient

def test_no_api_key():
    """Ensure ValueError is raised when no API key provdied"""
    with pytest.raises(ValueError, match="API key must be provided"):
        YoutubeClient(api_key=None)