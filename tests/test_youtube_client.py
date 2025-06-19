import pytest
import requests

from ccfc_yt.client import YoutubeClient
from unittest.mock import patch, MagicMock

def test_no_api_key():
    """Ensure ValueError is raised when no API key provdied"""
    with pytest.raises(ValueError, match="API key must be provided"):
        YoutubeClient(api_key=None)

def test_valid_init():
    """Ensure YoutubeClient initializes with valid API key"""
    client = YoutubeClient(api_key="VALID_KEY")
    assert client.api_key == "VALID_KEY"
    assert isinstance(client.session, requests.Session)

@patch('ccfc_yt.client.requests.Session.get')
def test_get_request_success(mock_get):
    """Ensure _get_request returns expected data on success"""
    client = YoutubeClient(api_key="VALID_KEY")
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": id, "items": [{"id": "video1"}, {"id": "video2"}]}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    response = client._get_request("videos", params={"part": "snippet", "id": "video1,video2"})
    mock_get.assert_called_once_with(
        f"{client.BASE_URL}/videos",
        params={"part": "snippet", "id": "video1,video2", "key": "VALID_KEY"}
    )
    assert response == {"id": id, "items": [{"id": "video1"}, {"id": "video2"}]}

@patch('ccfc_yt.client.requests.Session.get')
def test_get_request_403_quota_exceeded(mock_get):
    """Ensure _get_request catches 403 quoteExceeded error and raises ValueError"""
    client = YoutubeClient(api_key="VALID_KEY")
    mock_response = MagicMock()
    mock_response.status_code = 403
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError
    mock_response.json.return_value = {"error": {"errors": [{"reason": "quotaExceeded", "message": "Quota exceeded"}]}}
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="Quota exceeded for today. Try again tomorrow."):
        client._get_request("videos", params={"part": "snippet", "id": "video1,video2"})

@patch.object(YoutubeClient, '_get_request')
def test_paginate(mock_client_get):
    """Ensure _paginate works correctly with multiple pages"""
    client = YoutubeClient(api_key="VALID_KEY")
    mock_client_get.side_effect = [
        {"items": [{"id": "video1"}], "nextPageToken": "token1"},
        {"items": [{"id": "video2"}], "nextPageToken": None}
    ]

    items = client._paginate("videos", params={"q": "test"})

    assert len(items) == 2
    assert mock_client_get.call_count == 2