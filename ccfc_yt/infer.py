import aiohttp
import asyncio
import backoff
import json
import logging

from typing import Dict, List

logger = logging.getLogger(__name__)

class OpenRouterAsyncClient:

    def __init__(self, api_key: str, max_concurrent_requests: int = 50):
        self.base_url = "https://openrouter.ai/api/v1"
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("API key must be set")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        self.max_concurrent_requests = max_concurrent_requests
        self._sempaphore = asyncio.Semaphore(max_concurrent_requests)

    @backoff.on_exception(
        backoff.expo,
        exception=(aiohttp.ClientConnectionError, aiohttp.ClientResponseError),
        max_tries=5,
        # giveup if it is a ClientResponseError that is not a 429 (rate limiting) status code
        giveup=lambda e: e.status != 429 if isinstance(e, aiohttp.ClientResponseError) else False,
        # set up some logging when backing off
        on_backoff=lambda details: logger.warning(
            f"Backing off {details['wait']:.1f}s after {details['tries']} tries calling {details['target'].__name__}"
        )
    )
    async def _post(self, session: aiohttp.ClientSession, endpoint: str, data: Dict) -> Dict:
        """Async helper post method to post to OpenRouter API"""
        response = await session.post(
            url=f"{self.base_url}{endpoint}",
            headers=self.headers,
            json=data
        )
        response.raise_for_status()
        return await response.json()
    
    async def chat_completion(self, model: str, messages_batch: List[Dict]) -> None:
        """
        Take an input of comments and infer sentiment using the specified model with async requests

        :param model: the LLM model to use
        :param messages_batch: the batch of LLM message prompts with associated comment_id
            must take the schema [
                {
                    "comment_id": id,
                    "messages": [{"role": "user", "content": "Some text"}]
                },
                ...
            ]
        """
        logger.info("Beginning session now")
        # set session level request timeout of 30 seconds
        timeout = aiohttp.ClientTimeout(30)
        results = []
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = []
            logger.info("Generating tasks list now")
            for batch in messages_batch:
                comment_id = batch["comment_id"]
                messages = batch["messages"]
                # formulate payload for post request
                data = {
                    "model": model,
                    "messages": messages
                }
                logger.info(f"Posting for omment id: {comment_id} with payload: {data}")
                tasks.append(
                    (comment_id, self._post(session, endpoint="/chat/completions", data=data))
                )
            # gather tasks together using gather
            responses = await asyncio.gather(
                *[t[1] for t in tasks], return_exceptions=True
            )
            # append response with associated comment_id
            for (comment_id, _), res in zip(tasks, responses):
                results.append({
                    "comment_id": comment_id,
                    "response": res
                })
            return results